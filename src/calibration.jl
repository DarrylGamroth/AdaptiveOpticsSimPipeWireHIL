"""
One actual held-probe exposure. `timestamp` is the actual AOS model timestamp;
`identity` is the complete PipeWire acquisition identity. Header PTS remains a
monotonic publication-clock reading and must not be used as model time.
"""
struct PipeWireCalibrationExposure
    identity::AcquisitionIdentity
    probe_sequence::UInt64
    timestamp::AlgorithmGraphs.ModelTimestamp
    exposure_duration_nanoseconds::UInt64
end

mutable struct CalibrationState{Transport,Command,Node,StreamState}
    transport::Transport
    wire_command::Command
    signal::CompletionSignal
    expected_probe::Atomic{UInt64}
    last_probe_sequence::UInt64
    pending_exposure::Union{Nothing,PipeWireCalibrationExposure}
    failed::Atomic{Bool}
    progress_requested::Atomic{Bool}
    frame_node_id::Atomic{UInt32}
    command_node_id::Node
    frame_stream_state::Atomic{Int32}
    command_stream_state::StreamState
    frame_driving::Atomic{Bool}
    frame_publication_cycle_sequence::Atomic{UInt64}
end

function _new_calibration_domain()
    value = uuid4().value
    return AcquisitionDomain(ntuple(
        index -> UInt8((value >> (8 * (ACQUISITION_DOMAIN_SIZE - index))) & 0xff),
        Val(ACQUISITION_DOMAIN_SIZE),
    ))
end

_map_calibration_commands(f, transport::ExchangeState) = f(transport)
_map_calibration_commands(f, transport::NamedTuple) = map(f, transport)
_calibration_port(value, ::Nothing) = value
_calibration_port(values::NamedTuple, ::Val{Name}) where {Name} = values[Name]

function CalibrationState(transport, domain::AcquisitionDomain)
    for command in _command_states(transport)
        command.acquisition_domain = domain
    end
    return CalibrationState(
        transport, _map_calibration_commands(command -> zeros(Float32, length(command.command)), transport), CompletionSignal(),
        Atomic{UInt64}(0), UInt64(0), nothing,
        Atomic{Bool}(false), Atomic{Bool}(false), Atomic{UInt32}(_INVALID_NODE_ID),
        _map_calibration_commands(_ -> Atomic{UInt32}(_INVALID_NODE_ID), transport),
        Atomic{Int32}(PipeWireAO.LibPipeWire.PW_STREAM_STATE_UNCONNECTED),
        _map_calibration_commands(_ -> Atomic{Int32}(PipeWireAO.LibPipeWire.PW_STREAM_STATE_UNCONNECTED), transport),
        Atomic{Bool}(false),
        Atomic{UInt64}(0),
    )
end

_calibration_commands_received(state::CalibrationState, sequence::UInt64) =
    all(command -> command.command_ready[] == sequence, _command_states(state.transport))

function _fail_calibration!(state::CalibrationState)
    state.failed[] = true
    _signal_completion!(state.signal)
    return nothing
end

function _require_calibration_quiescent(state::CalibrationState)
    state.failed[] && throw(PipeWireHILError(
        "calibration.lifecycle", "the transport failed; close and reconstruct its endpoints",
    ))
    state.pending_exposure === nothing || throw(PipeWireHILError(
        "calibration.exposure", "the prior exposure still requires correlated WFS completion",
    ))
    state.expected_probe[] == 0 && _calibration_commands_received(state, UInt64(0)) || throw(
        PipeWireHILError("calibration.probe", "a probe receipt or adoption is pending"),
    )
    transport = _frame_state(state.transport)
    transport.frame_ready[] == transport.frame_published[] || throw(
        PipeWireHILError("calibration.exposure", "a frame publication is pending"),
    )
    return nothing
end

function _arm_calibration_probe!(state::CalibrationState, sequence::UInt64)
    _require_calibration_quiescent(state)
    sequence > state.last_probe_sequence || throw(PipeWireHILError(
        "calibration.probe", "probe Header sequence must be positive and strictly advance for the endpoint lifetime",
    ))
    # Consume the token before submission. An unknown publication outcome must
    # never allow this wire token to identify a later probe, including after reset.
    state.last_probe_sequence = sequence
    state.expected_probe[] = sequence
    return sequence
end

function _copy_calibration_probe!(
    state::CalibrationState, values::AbstractVector{Float32}, sequence::UInt64, port=nothing,
)
    state.failed[] && throw(PipeWireHILError("calibration.probe", "transport failed"))
    sequence != 0 && sequence == state.expected_probe[] || throw(PipeWireHILError(
        "calibration.probe", "command Header sequence does not match the armed probe",
    ))
    transport = _calibration_port(state.transport, port)
    transport.command_ready[] == 0 || throw(PipeWireHILError(
        "calibration.probe", "a duplicate command arrived before probe adoption",
    ))
    _copy_command!(transport, values)
    # Preserve the exact demanded wire values only after the complete command
    # and fixed unit conversion validate. The mapped view remains borrowed.
    copyto!(_calibration_port(state.wire_command, port), values)
    return nothing
end

function _publish_calibration_probe_receipt!(state::CalibrationState, sequence::UInt64, port=nothing)
    _calibration_port(state.transport, port).command_ready[] = sequence
    _signal_completion!(state.signal)
    return nothing
end

struct CalibrationCommandProcess{State<:CalibrationState,Port}
    state::State
    buffer::StreamBuffer
    port::Port
end

function (process::CalibrationCommandProcess)(stream::Stream)
    state = process.state
    try
        dequeue_buffer!(process.buffer, stream) || return nothing
        queued = false
        try
            header = buffer_header(process.buffer)
            header === nothing && throw(PipeWireHILError(
                "calibration.probe", "command buffer has no Header sequence",
            ))
            _copy_calibration_probe!(
                state, _command_values(_calibration_port(state.transport, process.port), buffer_data(process.buffer)),
                header.sequence, process.port,
            )
            queue_buffer!(process.buffer, stream)
            queued = true
            _publish_calibration_probe_receipt!(state, header.sequence, process.port)
        finally
            queued || return_buffer!(process.buffer, stream)
        end
    catch
        _fail_calibration!(state)
        rethrow()
    end
    return nothing
end

struct CalibrationFrameProcess{State<:CalibrationState}
    state::State
    buffer::StreamBuffer
end

function (process::CalibrationFrameProcess)(stream::Stream)
    state = process.state
    transport = _frame_state(state.transport)
    try
        sequence = transport.frame_ready[]
        (sequence == 0 || sequence == transport.frame_published[]) && return nothing
        dequeue_buffer!(process.buffer, stream) || return nothing
        queued = false
        try
            _copy_frame!(transport, buffer_data(process.buffer))
            _set_frame_metadata!(transport, process.buffer, sequence)
            queue_buffer!(process.buffer, stream)
            queued = true
            transport.frame_published[] = sequence
            _signal_completion!(state.signal)
        finally
            queued || return_buffer!(process.buffer, stream)
        end
    catch
        _fail_calibration!(state)
        rethrow()
    end
    return nothing
end

struct CalibrationFramePublicationCycleProcess{State}
    state::State
end

function _complete_calibration_publication_cycle!(state::CalibrationState)
    state.failed[] ||
        (state.frame_publication_cycle_sequence[] = _frame_state(state.transport).frame_published[])
    _signal_completion!(state.signal)
    return nothing
end

function (process::CalibrationFramePublicationCycleProcess)(::Stream)
    return _complete_calibration_publication_cycle!(process.state)
end

function _observe_calibration_stream!(state::CalibrationState, ::Val{:frame}, stream, current, ::Nothing=nothing)
    state.frame_node_id[] = node_id(stream)
    state.frame_stream_state[] = current
    state.frame_driving[] = is_driving(stream)
    return nothing
end

function _observe_calibration_stream!(state::CalibrationState, ::Val{:command}, stream, current, port=nothing)
    _calibration_port(state.command_node_id, port)[] = node_id(stream)
    _calibration_port(state.command_stream_state, port)[] = current
    return nothing
end

struct CalibrationStreamStateProcess{State<:CalibrationState,Role,Port}
    state::State
    role::Role
    port::Port
end
CalibrationStreamStateProcess(state, role) = CalibrationStreamStateProcess(state, role, nothing)

function _calibration_stream_transition!(state::CalibrationState, old, current)
    if current == PipeWireAO.LibPipeWire.PW_STREAM_STATE_ERROR ||
       (current == PipeWireAO.LibPipeWire.PW_STREAM_STATE_UNCONNECTED && old != current)
        _fail_calibration!(state)
    end
    _signal_completion!(state.signal)
    return nothing
end

function (process::CalibrationStreamStateProcess)(stream::Stream, old, current, message)
    try
        _observe_calibration_stream!(process.state, process.role, stream, current, process.port)
        _calibration_stream_transition!(process.state, old, current)
    catch
        _fail_calibration!(process.state)
        rethrow()
    end
    return nothing
end

struct CalibrationStreamIOProcess{State<:CalibrationState,Role,Port}
    state::State
    role::Role
    port::Port
end
CalibrationStreamIOProcess(state, role) = CalibrationStreamIOProcess(state, role, nothing)

function (process::CalibrationStreamIOProcess)(stream::Stream, io::StreamIO)
    try
        _observe_calibration_stream!(process.state, process.role, stream, stream_state(stream), process.port)
        _signal_completion!(process.state.signal)
    catch
        _fail_calibration!(process.state)
        rethrow()
    end
    return nothing
end

"""
Own PipeWire frame/command endpoints for an AOS calibration boundary. One
serialized control owner calls every lifecycle, probe, exposure, ACK, reset and
status operation outside callbacks. Callbacks own only bounded transport state.
"""
mutable struct PreparedPipeWireCalibration{
    Boundary<:AlgorithmGraphs.PreparedGraphCalibrationBoundary,State<:CalibrationState,
    Configuration<:PipeWireHILConfiguration,Commands,
}
    boundary::Boundary
    configuration::Configuration
    state::State
    loop::ThreadLoop
    context::Context
    core::CoreConnection
    frame_stream::Stream
    command_stream::Commands
    started::Bool
    closed::Bool
end

function _with_loop_access(f, prepared::PreparedPipeWireCalibration)
    if isrunning(prepared.loop)
        return with_thread_loop_lock(prepared.loop) do _
            f()
        end
    end
    return f()
end

function _check_calibration_failure(prepared::PreparedPipeWireCalibration)
    prepared.state.failed[] || return nothing
    # Detailed native/callback errors are read after leaving the condition lock.
    _with_loop_access(prepared) do
        stream_state(prepared.frame_stream)
        _command_stream_states(prepared.command_stream)
    end
    throw(PipeWireHILError("calibration.lifecycle", "a stream callback or connection failed"))
end

function _calibration_timeout_ns(configuration::PipeWireHILConfiguration, timeout_ns::UInt64)
    0 < timeout_ns <= configuration.timeout_ns && timeout_ns <= UInt64(typemax(Int64)) || throw(
        PipeWireHILError("timeout_ns", "must be positive, fit Int64 nanoseconds, and not exceed the configured timeout"),
    )
    return timeout_ns
end

_calibration_timeout_ns(configuration::PipeWireHILConfiguration, timeout_ns) =
    throw(PipeWireHILError("timeout_ns", "operation timeout must be UInt64"))

function _calibration_remaining_ns(started::UInt64, timeout_ns::UInt64, field::String)
    elapsed = time_ns() - started
    elapsed < timeout_ns || throw(PipeWireHILError(
        field, "operation completion deadline expired",
    ))
    return timeout_ns - elapsed
end

function _await_calibration_completion(
    ready, prepared::PreparedPipeWireCalibration, field::String;
    started::UInt64=time_ns(), timeout_ns::UInt64=prepared.configuration.timeout_ns,
    drive::Bool=false, refresh::Bool=false,
)
    state = prepared.state
    state.progress_requested[] = false
    retry_timer = nothing
    try
        if drive || refresh
            # Bounded control-path progress requests, never scientific exposures
            # or evidence of success. Native inspection stays outside Condition.
            retry_timer = Timer(0.001; interval=0.001) do _
                state.progress_requested[] = true
                _signal_completion!(state.signal)
            end
        end
        while true
            remaining = _calibration_remaining_ns(started, timeout_ns, field)
            _await_completion(
                () -> state.failed[] || ready() || state.progress_requested[],
                state.signal, remaining, field,
            )
            _check_calibration_failure(prepared)
            _calibration_remaining_ns(started, timeout_ns, field)
            ready() && return nothing
            if Threads.atomic_xchg!(state.progress_requested, false)
                _with_loop_access(prepared) do
                    if refresh
                        _refresh_calibration_streams!(prepared)
                    end
                    drive && trigger_process!(prepared.frame_stream)
                end
            end
        end
    catch
        _fail_calibration!(state)
        rethrow()
    finally
        retry_timer === nothing || close(retry_timer)
        state.progress_requested[] = false
    end
end

function _prepare_calibration_command_stream(core, state, name, port)
    return Stream(core, name;
        properties=_stream_properties(name),
        on_process=CalibrationCommandProcess(state, StreamBuffer(), port),
        on_state_changed=CalibrationStreamStateProcess(state, Val(:command), port),
        on_io_changed=CalibrationStreamIOProcess(state, Val(:command), port))
end

function _prepare_calibration_command_streams(core, state, names::AbstractString)
    return _prepare_calibration_command_stream(core, state, names, nothing)
end

function _prepare_calibration_command_streams(core, state, names::NamedTuple)
    created = ()
    try
        # Boundary order owns the command set; configuration is selected by name.
        for key in keys(state.transport)
            created = (created..., _prepare_calibration_command_stream(core, state, names[key], Val(key)))
        end
        return NamedTuple{keys(state.transport)}(created)
    catch exception
        errors = Any[exception]
        for stream in reverse(created)
            _attempt_cleanup!(() -> close(stream), errors)
        end
        length(errors) == 1 && rethrow()
        throw(CompositeException(errors))
    end
end

function _refresh_calibration_commands!(state, stream::Stream)
    _observe_calibration_stream!(state, Val(:command), stream, stream_state(stream))
    return nothing
end

function _refresh_calibration_commands!(state, streams::NamedTuple)
    for key in keys(streams)
        stream = streams[key]
        _observe_calibration_stream!(state, Val(:command), stream, stream_state(stream), Val(key))
    end
    return nothing
end

function _refresh_calibration_streams!(prepared::PreparedPipeWireCalibration)
    _observe_calibration_stream!(prepared.state, Val(:frame), prepared.frame_stream, stream_state(prepared.frame_stream))
    _refresh_calibration_commands!(prepared.state, prepared.command_stream)
    return nothing
end

"""
    prepare_pipewire_calibration(boundary, configuration;
                                acquisition_domain=unique UUID domain,
                                reference_clock=METADATA_CLOCK_REALTIME)

Connect inactive frame source and demanded-command sink endpoints. Reuse the
ordinary HIL format, encoding, and fixed unit conversion. No exposure is generated.
Named commands use the ordinary matching named node/schema/scale configuration;
every command port must receive the armed token before the complete probe adopts.
The domain identifies this endpoint lifetime; supplying one makes that identity
an explicit owner responsibility. Close the prepared endpoints deterministically.
"""
function prepare_pipewire_calibration(
    boundary::AlgorithmGraphs.PreparedGraphCalibrationBoundary,
    configuration::PipeWireHILConfiguration;
    acquisition_domain::AcquisitionDomain=_new_calibration_domain(),
    reference_clock::MetadataClock=METADATA_CLOCK_REALTIME,
)
    reference_clock in (METADATA_CLOCK_MONOTONIC,METADATA_CLOCK_REALTIME,METADATA_CLOCK_TAI) || throw(ArgumentError("unsupported host reference clock"))
    status = AlgorithmGraphs.hil_boundary_status(boundary)
    (!status.failed && status.frame_sequence == 0 && status.active_probe_sequence == 0) || throw(
        PipeWireHILError("calibration.boundary", "preparation requires an unstepped boundary with no adopted probe"),
    )
    configuration.timeout_ns <= UInt64(typemax(Int64)) || throw(PipeWireHILError(
        "timeout_ns", "calibration completion timeout must fit Int64 nanoseconds",
    ))
    configuration.exposure_duration_ns <= UInt64(typemax(Int64)) || throw(PipeWireHILError(
        "exposure_duration_ns", "calibration exposure duration must fit model-time range",
    ))
    state = CalibrationState(_prepare_exchange_state(
        _frame_exchange_buffer(hil_frame_buffer(boundary), "calibration.frame"),
        hil_command_buffer(boundary), configuration,
    ), acquisition_domain)
    _set_exchange_reference_clock!(state.transport,reference_clock)
    loop = context = core = frame_stream = command_stream = nothing
    try
        loop = ThreadLoop("AdaptiveOpticsSimPipeWireCalibration")
        context = Context(loop)
        core = CoreConnection(context; properties=Dict("remote.name" => configuration.remote))
        frame_stream = Stream(
            core, configuration.frame_node_name;
            properties=_stream_properties(configuration.frame_node_name; driver=true,header_pts_clock="monotonic"),
            on_process=CalibrationFrameProcess(state, StreamBuffer()),
            on_state_changed=CalibrationStreamStateProcess(state, Val(:frame)),
            on_io_changed=CalibrationStreamIOProcess(state, Val(:frame)),
            on_trigger_done=CalibrationFramePublicationCycleProcess(state),
        )
        command_stream = _prepare_calibration_command_streams(core, state, configuration.command_node_name)
        prepared = PreparedPipeWireCalibration(
            boundary, configuration, state, loop, context, core,
            frame_stream, command_stream, false, false,
        )
        flags = STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT
        transport = _frame_state(state.transport)
        connect!(frame_stream, :output; flags=flags | STREAM_DRIVER, params=_stream_parameters(
            _format(transport.frame, configuration.rate, eltype(transport.frame_transport)),
            configuration.frame_schema,
        ))
        _connect_command_streams!(command_stream, state.transport, configuration, 2, flags;
            metadata=(MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP),))
        start!(loop)
        _await_calibration_completion(prepared, "calibration.nodes") do
            state.frame_node_id[] != _INVALID_NODE_ID &&
                all(id -> id[] != _INVALID_NODE_ID, _command_values_tuple(state.command_node_id))
        end
        return prepared
    catch exception
        errors = _close_pipewire_resources!(loop, context, core, frame_stream, command_stream)
        isempty(errors) || throw(CompositeException(Any[exception; errors]))
        rethrow()
    end
end

function _require_calibration_started(prepared::PreparedPipeWireCalibration)
    prepared.closed && throw(PipeWireHILError("calibration.lifecycle", "endpoints are closed"))
    prepared.started || throw(PipeWireHILError("calibration.lifecycle", "endpoints are not started"))
    _require_calibration_quiescent(prepared.state)
    return nothing
end

function _wait_for_calibration_driver(
    prepared::PreparedPipeWireCalibration, started::UInt64, timeout_ns::UInt64,
)
    state = prepared.state
    _with_loop_access(prepared) do
        _refresh_calibration_streams!(prepared)
    end
    _await_calibration_completion(prepared, "calibration.driver"; started, timeout_ns, refresh=true) do
        state.frame_stream_state[] == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING &&
        all(current -> current[] == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING,
            _command_values_tuple(state.command_stream_state)) && state.frame_driving[]
    end
    return nothing
end

_no_probe_submission() = nothing

function _submit_calibration_probe!(state::CalibrationState, sequence::UInt64, submit)
    applicable(submit) || throw(PipeWireHILError("calibration.submit", "submit must be callable without arguments"))
    _arm_calibration_probe!(state, sequence)
    try
        submit()
    catch
        _fail_calibration!(state)
        rethrow()
    end
    return nothing
end

"""
    adopt_pipewire_probe!(prepared, probe_sequence::UInt64; submit=() -> nothing,
                          timeout_ns=prepared.configuration.timeout_ns)

Arm a strictly advancing command Header token, promptly invoke `submit()`, wait
for that exact demanded command, then validate/adopt it through the AOS boundary.
`submit` must promptly enqueue publication; a separately coordinated producer
may instead publish after arming. No exposure executes. Unknown submission,
receipt, or adoption failures stop admission; close/reconstruct after failure.
Probe wire tokens are never reused by reset. `timeout_ns` must be a positive
UInt64 no greater than the configured timeout, fitting Int64 nanoseconds. One
deadline covers driver readiness, submission, receipt and adoption admission.
"""
function adopt_pipewire_probe!(
    prepared::PreparedPipeWireCalibration, sequence::UInt64; submit=_no_probe_submission,
    timeout_ns=prepared.configuration.timeout_ns,
)
    timeout_ns = _calibration_timeout_ns(prepared.configuration, timeout_ns)
    applicable(submit) || throw(PipeWireHILError("calibration.submit", "submit must be callable without arguments"))
    _require_calibration_started(prepared)
    sequence > prepared.state.last_probe_sequence || throw(PipeWireHILError(
        "calibration.probe", "probe Header sequence must be positive and strictly advance for the endpoint lifetime",
    ))
    started = time_ns()
    _wait_for_calibration_driver(prepared, started, timeout_ns)
    state = prepared.state
    try
        _submit_calibration_probe!(state, sequence, submit)
        _calibration_remaining_ns(started, timeout_ns, "calibration.probe")
        _with_loop_access(prepared) do
            trigger_process!(prepared.frame_stream)
        end
        _await_calibration_completion(prepared, "calibration.probe"; started, timeout_ns, drive=true) do
            _calibration_commands_received(state, sequence)
        end
        _check_calibration_failure(prepared)
        _calibration_remaining_ns(started, timeout_ns, "calibration.probe")
        return _adopt_received_calibration_probe!(prepared.boundary, state, sequence)
    catch
        _fail_calibration!(state)
        rethrow()
    end
end

function _adopt_received_calibration_probe!(boundary, state::CalibrationState, sequence::UInt64)
    state.failed[] && throw(PipeWireHILError("calibration.probe", "transport failed"))
    sequence != 0 && state.expected_probe[] == sequence &&
        _calibration_commands_received(state, sequence) || throw(PipeWireHILError(
            "calibration.probe", "no complete matching probe is awaiting adoption",
        ))
    try
        AlgorithmGraphs.adopt_hil_probe!(boundary, sequence)
        state.failed[] && throw(PipeWireHILError("calibration.probe", "transport failed during adoption"))
    catch
        _fail_calibration!(state)
        rethrow()
    end
    for command in _command_states(state.transport)
        command.command_ready[] = 0
    end
    state.expected_probe[] = 0
    return sequence
end

adopt_pipewire_probe!(prepared::PreparedPipeWireCalibration, sequence; kwargs...) =
    throw(PipeWireHILError("calibration.probe", "probe Header sequence must be UInt64"))

function _calibration_exposure_timestamp(driver, duration::UInt64)
    duration <= UInt64(typemax(Int64)) || throw(PipeWireHILError(
        "calibration.exposure", "exposure duration exceeds Int64 model-time range",
    ))
    timestamp = AlgorithmGraphs.next_model_timestamp(driver)
    ns = AlgorithmGraphs.model_nanoseconds(timestamp)
    ns >= 0 || throw(PipeWireHILError(
        "calibration.exposure", "calibration model timestamps must be nonnegative",
    ))
    try
        Base.checked_add(ns, Int64(duration))
    catch
        throw(PipeWireHILError("calibration.exposure", "model exposure end exceeds Int64 nanoseconds"))
    end
    return timestamp
end

_no_exposure_arming(::PipeWireCalibrationExposure) = nothing

function _stage_calibration_exposure!(boundary, state::CalibrationState, driver, before_publish)
    _require_calibration_quiescent(state)
    transport = _frame_state(state.transport)
    try
        _calibration_exposure_timestamp(driver, transport.exposure_duration_ns)
        result = AlgorithmGraphs.step_hil_exposure_at!(boundary, driver)
        exposure = PipeWireCalibrationExposure(
            AcquisitionIdentity(transport.acquisition_domain, transport.acquisition_generation, result.sequence),
            AlgorithmGraphs.hil_boundary_status(boundary).active_probe_sequence,
            result.timestamp, transport.exposure_duration_ns,
        )
        state.pending_exposure = exposure
        _stage_frame_transport!(transport)
        before_publish(exposure)
        return exposure
    catch
        _fail_calibration!(state)
        rethrow()
    end
end

"""
    step_pipewire_exposure!(prepared, driver; before_publish=exposure -> nothing,
                            timeout_ns=prepared.configuration.timeout_ns)
        -> PipeWireCalibrationExposure

Execute one AOS exposure at the actual model boundary and publish its complete
host frame. Return its probe token, model timestamp, duration and full acquisition
identity. Publication is not WFS success: another probe, exposure, or reset is
blocked until [`complete_pipewire_exposure!`](@ref) acknowledges correlated WFS
completion. The caller owns response validation and settling/aggregation policy. Calibration
model timestamps must be nonnegative, with exposure end fitting Int64 nanoseconds.

`before_publish(exposure)` runs exactly once on the control owner, outside the
ThreadLoop lock and callbacks, after the actual AOS step, pending record and host
frame staging, before publication becomes possible. Use it to arm an external
collector with the actual complete identity. Failure retains the completed model
step and pending record, exposes no frame, and stops admission until close.
`timeout_ns` must be a positive UInt64 no greater than the configured timeout,
fitting Int64 nanoseconds. One deadline covers readiness, execution, the owner
callable and publication; the owner callable must return promptly.
"""
function step_pipewire_exposure!(
    prepared::PreparedPipeWireCalibration, driver; before_publish=_no_exposure_arming,
    timeout_ns=prepared.configuration.timeout_ns,
)
    timeout_ns = _calibration_timeout_ns(prepared.configuration, timeout_ns)
    _require_calibration_started(prepared)
    status = AlgorithmGraphs.hil_boundary_status(prepared.boundary)
    status.failed && throw(PipeWireHILError("calibration.exposure", "AOS boundary failed"))
    status.active_probe_sequence != 0 || throw(PipeWireHILError(
        "calibration.exposure", "an exposure requires an adopted probe",
    ))
    started = time_ns()
    _wait_for_calibration_driver(prepared, started, timeout_ns)
    state = prepared.state
    transport = _frame_state(state.transport)
    try
        _calibration_remaining_ns(started, timeout_ns, "calibration.exposure")
        exposure = _stage_calibration_exposure!(prepared.boundary, state, driver, before_publish)
        _calibration_remaining_ns(started, timeout_ns, "calibration.publication")
        transport.exposure_record = PipeWireExposureRecord(
            exposure.identity.sequence, transport.exposure_duration_ns,
        )
        transport.frame_ready[] = exposure.identity.sequence
        _with_loop_access(prepared) do
            trigger_process!(prepared.frame_stream)
        end
        _await_calibration_completion(prepared, "calibration.publication"; started, timeout_ns, drive=true) do
            transport.frame_published[] == exposure.identity.sequence
        end
        return exposure
    catch
        _fail_calibration!(state)
        rethrow()
    end
end

function _complete_calibration_exposure!(state::CalibrationState, identity::AcquisitionIdentity)
    state.failed[] && throw(PipeWireHILError("calibration.exposure", "transport failed"))
    pending = state.pending_exposure
    pending === nothing && throw(PipeWireHILError("calibration.exposure", "no exposure is awaiting WFS completion"))
    isequal(identity, pending.identity) || throw(PipeWireHILError(
        "calibration.exposure", "WFS completion does not match the full pending acquisition identity",
    ))
    _frame_state(state.transport).frame_published[] == identity.sequence || throw(PipeWireHILError(
        "calibration.exposure", "the pending frame has not been published",
    ))
    state.pending_exposure = nothing
    return identity
end

"""
    complete_pipewire_exposure!(prepared, identity_or_exposure)

Acknowledge a complete, caller-validated WFS response for the exact domain,
generation, and sequence of the pending exposure. Source queueing alone cannot
acknowledge completion. Duplicate, stale, foreign, and sequence-only ACKs reject.
"""
function complete_pipewire_exposure!(prepared::PreparedPipeWireCalibration, identity::AcquisitionIdentity)
    prepared.closed && throw(PipeWireHILError("calibration.lifecycle", "endpoints are closed"))
    return _complete_calibration_exposure!(prepared.state, identity)
end

complete_pipewire_exposure!(prepared::PreparedPipeWireCalibration, exposure::PipeWireCalibrationExposure) =
    complete_pipewire_exposure!(prepared, exposure.identity)

complete_pipewire_exposure!(prepared::PreparedPipeWireCalibration, identity) =
    throw(PipeWireHILError("calibration.exposure", "completion requires a full AcquisitionIdentity or exposure record"))

function _reset_calibration_state!(state::CalibrationState)
    _require_calibration_quiescent(state)
    transport = _frame_state(state.transport)
    transport.acquisition_generation < typemax(UInt64) || throw(PipeWireHILError(
        "calibration.acquisition", "acquisition generation is exhausted",
    ))
    generation = transport.acquisition_generation + UInt64(1)
    for command in _command_states(state.transport)
        command.acquisition_generation = generation
    end
    transport.frame_ready[] = 0
    transport.frame_published[] = 0
    state.frame_publication_cycle_sequence[] = 0
    fill!(transport.frame_transport, zero(eltype(transport.frame_transport)))
    foreach(command -> fill!(command, 0.0f0), _command_values_tuple(state.wire_command))
    return nothing
end

"""Reset a quiescent calibration boundary/driver; preserve lifetime probe wire tokens."""
_reset_calibration_boundary!(boundary, ::Nothing) = AlgorithmGraphs.reset_hil_boundary!(boundary)
_reset_calibration_boundary!(boundary, driver) = AlgorithmGraphs.reset_hil_boundary!(boundary, driver)

function reset_pipewire_calibration!(prepared::PreparedPipeWireCalibration, driver=nothing)
    prepared.closed && throw(PipeWireHILError("calibration.lifecycle", "endpoints are closed"))
    _with_loop_access(prepared) do
        _require_calibration_quiescent(prepared.state)
        _frame_state(prepared.state.transport).acquisition_generation < typemax(UInt64) || throw(
            PipeWireHILError("calibration.acquisition", "acquisition generation is exhausted"),
        )
        try
            _reset_calibration_boundary!(prepared.boundary, driver)
            _reset_calibration_state!(prepared.state)
        catch
            _fail_calibration!(prepared.state)
            rethrow()
        end
    end
    return prepared
end

function _calibration_probe_values(boundary, state::CalibrationState)
    state.failed[] && throw(PipeWireHILError("calibration.probe", "transport failed"))
    state.expected_probe[] == 0 && _calibration_commands_received(state, UInt64(0)) || throw(
        PipeWireHILError("calibration.probe", "a probe receipt or adoption is pending"),
    )
    status = AlgorithmGraphs.hil_boundary_status(boundary)
    !status.failed && state.last_probe_sequence != 0 &&
        status.active_probe_sequence == state.last_probe_sequence || throw(PipeWireHILError(
            "calibration.probe", "no successfully adopted probe is available",
        ))
    return state.wire_command
end

"""
    pipewire_probe_values(prepared) -> Vector{Float32} or NamedTuple

Borrow the exact demanded wire values of the successfully adopted probe, before
fixed unit conversion. Access rejects a pending probe receipt/adoption, failed
transport or AOS boundary, reset boundary, or closed endpoints. The probe Header
token is `pipewire_calibration_status(prepared).last_probe_sequence`.
For a named boundary, return one vector per port in its declared command order.
All returned vectors belong to the same successfully adopted probe token.

The sole serialized control owner MUST NOT mutate the borrowed vector. Its
contents are valid until the owner begins another probe adoption, reset or close;
copy them first when longer retention is required. No mapped native memory is
returned, and exposures under the held probe leave the values unchanged.
"""
function pipewire_probe_values(prepared::PreparedPipeWireCalibration)
    prepared.closed && throw(PipeWireHILError("calibration.lifecycle", "endpoints are closed"))
    return _calibration_probe_values(prepared.boundary, prepared.state)
end

"""
Return bounded mailbox/publication state without treating publication as WFS completion.
`received_probe_sequence` is a UInt64 for a scalar command or a NamedTuple of
per-port UInt64 receipts in the boundary's declared order for a named command set.

`frame_publication_cycle_sequence` is the latest published frame sequence observed
when the native source's `on_trigger_done` callback ran. Source queueing can finish
before that callback. Match this witness to the pending exposure's sequence in
the current acquisition generation when source-cycle ordering is required. It
does not establish downstream WFS completion or acknowledge the exposure. Reset
clears it to zero. A callback observing failed transport leaves it unchanged;
the witness never clears failure or the pending exposure fence.
"""
function pipewire_calibration_status(prepared::PreparedPipeWireCalibration)
    state = prepared.state
    transport = _frame_state(state.transport)
    return (
        acquisition_domain=transport.acquisition_domain,
        acquisition_generation=transport.acquisition_generation,
        expected_probe_sequence=state.expected_probe[],
        received_probe_sequence=_map_calibration_commands(command -> command.command_ready[], state.transport),
        last_probe_sequence=state.last_probe_sequence,
        frame_publication_cycle_sequence=state.frame_publication_cycle_sequence[],
        pending_exposure=state.pending_exposure,
        failed=state.failed[] || AlgorithmGraphs.hil_boundary_status(prepared.boundary).failed,
    )
end

function start!(prepared::PreparedPipeWireCalibration)
    prepared.closed && throw(PipeWireHILError("calibration.lifecycle", "endpoints are closed"))
    prepared.started && return prepared
    _require_calibration_quiescent(prepared.state)
    _with_loop_access(prepared) do
        _set_command_active!(prepared.command_stream, true)
        try
            set_active!(prepared.frame_stream, true)
        catch
            _set_command_active!(prepared.command_stream, false)
            rethrow()
        end
    end
    prepared.started = true
    return prepared
end

function stop!(prepared::PreparedPipeWireCalibration)
    prepared.closed && return prepared
    prepared.started || return prepared
    _with_loop_access(prepared) do
        set_active!(prepared.frame_stream, false)
        _set_command_active!(prepared.command_stream, false)
    end
    prepared.started = false
    return prepared
end

Base.close(prepared::PreparedPipeWireCalibration) = _close_pipewire_hil!(prepared)
