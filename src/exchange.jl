const _NO_SEQUENCE = UInt64(0)
const _INVALID_NODE_ID = typemax(UInt32)
# PipeWire's standard dummy driver has priority 200000. The lockstep HIL
# source must own this development graph's explicitly triggered frame cadence.
const _HIL_DRIVER_PRIORITY = "210000"

# Native driver cycles and model frames have independent lifetimes. Only the
# serialized exchange owner changes epoch/pending; the callback advances the
# completion observation. A timeout leaves the cycle unresolved and fails the
# owner rather than attributing a late event to a replacement cycle.
mutable struct DriverCycle
    completed::Atomic{UInt64}
    epoch::UInt64
    pending::Bool
end
DriverCycle() = DriverCycle(Atomic{UInt64}(0), UInt64(0), false)

function _complete_driver_cycle!(cycle::DriverCycle)
    Base.Threads.atomic_add!(cycle.completed, UInt64(1))
    return nothing
end

function _issue_driver_cycle!(issue!::F, cycle::DriverCycle) where {F}
    if cycle.pending
        cycle.completed[] == cycle.epoch && return false
        cycle.pending = false
    end
    cycle.epoch = cycle.completed[]
    cycle.pending = true
    issue!()
    return true
end

function _await_driver_cycle!(check!::F, cycle::DriverCycle, timeout_ns::UInt64,
                              idle_strategy=SimRunner.current_strategy()) where {F}
    start = time_ns()
    check_at = start
    while cycle.pending && cycle.completed[] == cycle.epoch
        if time_ns() >= check_at
            check!()
            check_at = time_ns() + UInt64(1_000_000)
        end
        time_ns() - start <= timeout_ns || throw(PipeWireHILError(
            "wfs_frame.cycle", "native driver cycle did not complete before the development timeout",
        ))
        SimRunner.idle!(idle_strategy)
    end
    cycle.pending = false
    return nothing
end

function _require_quiescent_driver_cycle(cycle::DriverCycle)
    cycle.pending && throw(PipeWireHILError("lifecycle", "a native driver cycle is unresolved"))
    return nothing
end

function _reset_driver_cycle!(cycle::DriverCycle)
    _require_quiescent_driver_cycle(cycle)
    cycle.epoch = cycle.completed[]
    return nothing
end

struct FrameTriggerDone
    cycle::DriverCycle
end
(callback::FrameTriggerDone)(::Stream) = _complete_driver_cycle!(callback.cycle)

"""
    PipeWireFrameCommandTiming

One completed source-publication-to-command-receipt observation for a WFS
frame sequence. All timestamps use Julia's monotonic `time_ns()` clock. The
observation begins at the frame Header PTS and ends when the command callback
receives the corresponding command buffer.
"""
struct PipeWireFrameCommandTiming
    sequence::UInt64
    source_published_nanoseconds::Int64
    command_received_nanoseconds::Int64
    end_to_end_latency_nanoseconds::UInt64

    function PipeWireFrameCommandTiming(
        sequence::UInt64,
        source_published_nanoseconds::Int64,
        command_received_nanoseconds::Int64,
    )
        sequence != _NO_SEQUENCE || throw(ArgumentError("sequence zero is reserved"))
        source_published_nanoseconds >= 0 || throw(ArgumentError(
            "source publication timestamp must be non-negative",
        ))
        command_received_nanoseconds >= source_published_nanoseconds || throw(
            ArgumentError("command receipt precedes source publication"),
        )
        return new(
            sequence,
            source_published_nanoseconds,
            command_received_nanoseconds,
            UInt64(command_received_nanoseconds - source_published_nanoseconds),
        )
    end
end

"""
    PipeWireExchangeTiming()

Reusable owner-thread phase timestamps for `exchange_frame!(...; timing)`.
All fields are `UInt64`; timestamps use host-monotonic `time_ns()`. `ready_ns`
follows the initial driver checks, `step_finished_ns` includes the completed
AOS step and host frame copy, `staged_ns` follows exposure and transport staging,
and `adoption_started_ns` follows publication, matching command receipt and the
final driver-cycle wait. `finished_ns` follows command adoption.

The owner resets the record on entry. Only successful completion sets positive
`generation` and `sequence`; a failed exchange retains sequence zero and any
completed phase timestamps. Reuse and read this record only on the exchange
owner thread. It does not publish telemetry or establish a deadline guarantee.
The default `timing=nothing` adds no timing clock reads.
"""
mutable struct PipeWireExchangeTiming
    started_ns::UInt64
    ready_ns::UInt64
    step_started_ns::UInt64
    step_finished_ns::UInt64
    staged_ns::UInt64
    adoption_started_ns::UInt64
    finished_ns::UInt64
    generation::UInt64
    sequence::UInt64
end
PipeWireExchangeTiming() = PipeWireExchangeTiming(0, 0, 0, 0, 0, 0, 0, 0, 0)

@inline _exchange_timestamp!(::Nothing, ::Val) = nothing
@inline function _exchange_timestamp!(timing::PipeWireExchangeTiming, ::Val{Field}) where {Field}
    setfield!(timing, Field, time_ns())
    return nothing
end

@inline _begin_exchange_timing!(::Nothing) = nothing
@inline function _begin_exchange_timing!(timing::PipeWireExchangeTiming)
    timing.sequence = 0
    timing.generation = 0
    timing.ready_ns = 0
    timing.step_started_ns = 0
    timing.step_finished_ns = 0
    timing.staged_ns = 0
    timing.adoption_started_ns = 0
    timing.finished_ns = 0
    _exchange_timestamp!(timing, Val(:started_ns))
    return nothing
end

@inline _complete_exchange_timing!(::Nothing, generation::UInt64, sequence::UInt64) = nothing
@inline function _complete_exchange_timing!(timing::PipeWireExchangeTiming,
        generation::UInt64, sequence::UInt64)
    _exchange_timestamp!(timing, Val(:finished_ns))
    timing.generation = generation
    timing.sequence = sequence
    return nothing
end

mutable struct ExchangeState{Frame<:Union{Array{Float32},Array{UInt16}},Command<:Array{Float32},Element<:Union{Float32,UInt16}}
    frame::Frame
    frame_transport::Vector{Element}
    command::Command
    frame_bytes::Int
    frame_chunk_stride::Int32
    command_bytes::Int
    command_scale::Float32
    frame_ready::Atomic{UInt64}
    frame_published::Atomic{UInt64}
    command_ready::Atomic{UInt64}
    timing_source_sequence::Atomic{UInt64}
    timing_source_published_nanoseconds::Atomic{UInt64}
    timing_last_event_nanoseconds::Atomic{UInt64}
    timing_completed_sequence::Atomic{UInt64}
    timing_completed_source_published_nanoseconds::Atomic{UInt64}
    timing_completed_command_received_nanoseconds::Atomic{UInt64}
    acquisition_domain::AcquisitionDomain
    acquisition_generation::UInt64
    exposure_duration_ns::UInt64
    exposure_record::PipeWireExposureRecord
    reference_clock::MetadataClock
end

function _frame_chunk_stride(format::NdArrayFormat{N}) where {N}
    # Use the packed extent of the innermost storage axis.
    element_bytes = payload_size(format) ÷ prod(Int, format.shape)
    axis = format.layout == NdArray.ROW_MAJOR ? N : 1
    stride = Int(format.shape[axis]) * element_bytes
    stride <= typemax(Int32) || throw(PipeWireHILError(
        "wfs_frame",
        "packed frame chunk stride does not fit Int32",
    ))
    return Int32(stride)
end

function ExchangeState(
    frame::Frame,
    command::Command,
    exposure_duration_ns::UInt64,
    frame_encoding::Symbol=:float32,
    command_scale::Float32=1.0f0;
    frame_transport=nothing,
    reference_clock::MetadataClock=METADATA_CLOCK_REALTIME,
) where {Frame<:Union{Array{Float32},Array{UInt16}},Command<:Array{Float32}}
    element = if frame_encoding === :float32
        Float32
    elseif frame_encoding === :uint16
        UInt16
    else
        throw(PipeWireHILError("frame_encoding", "must be :float32 or :uint16"))
    end
    transport = frame_transport === nothing ? Vector{element}(undef, length(frame)) : frame_transport
    transport isa Vector{element} && length(transport) == length(frame) || throw(PipeWireHILError(
        "wfs_frame", "shared frame transport must match the prepared frame encoding and extent"))
    domain = AcquisitionDomain(ntuple(
        index -> index == 1 ? UInt8(0xa0) : UInt8(0),
        Val(ACQUISITION_DOMAIN_SIZE),
    ))
    return ExchangeState(
        frame,
        transport,
        command,
        sizeof(element) * length(frame),
        _frame_chunk_stride(_format(frame, nothing, element)),
        sizeof(Float32) * length(command),
        command_scale,
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        Atomic{UInt64}(_NO_SEQUENCE),
        domain,
        UInt64(1),
        exposure_duration_ns,
        PipeWireExposureRecord(0, exposure_duration_ns),
        reference_clock,
    )
end

# A command set owns one concrete per-port receipt state. Publication atomics
# belong to the one frame source; each callback owns only its command and timing.
_frame_state(state::ExchangeState) = state
_frame_state(states::NamedTuple) = first(values(states))
_command_states(state::ExchangeState) = (state,)
_command_states(states::NamedTuple) = values(states)
_command_streams(streams::NamedTuple) = values(streams)
_command_streams(stream) = (stream,) # deterministic cleanup also supports test resources

function _prepare_exchange_state(frame, command::Array{Float32}, configuration)
    configuration.command_node_name isa AbstractString || throw(PipeWireHILError(
        "correction_command", "a single command buffer requires scalar command configuration"))
    Base.mightalias(frame, command) && throw(PipeWireHILError(
        "correction_command", "must not alias the WFS frame buffer"))
    return ExchangeState(frame, command, configuration.exposure_duration_ns,
        configuration.frame_encoding, configuration.command_scale)
end

function _prepare_exchange_state(frame, commands::NamedTuple{Keys}, configuration) where {Keys}
    configuration.command_node_name isa NamedTuple &&
        Set(keys(configuration.command_node_name)) == Set(Keys) || throw(PipeWireHILError(
        "correction_command", "command configuration must name every boundary command port"))
    1 <= length(Keys) <= 16 || throw(PipeWireHILError(
        "correction_command", "a command set requires 1:16 ports"))
    buffers = values(commands)
    for (index, command) in enumerate(buffers)
        command isa Array{Float32} || throw(PipeWireHILError(
            "correction_command", "each command requires a dense host Array{Float32}"))
        Base.mightalias(frame, command) && throw(PipeWireHILError(
            "correction_command", "command buffers must not alias the WFS frame"))
        any(prior -> Base.mightalias(prior, command), buffers[1:(index - 1)]) &&
            throw(PipeWireHILError("correction_command", "command buffers must not alias"))
    end
    source = ExchangeState(frame, commands[first(Keys)], configuration.exposure_duration_ns,
        configuration.frame_encoding, configuration.command_scale[first(Keys)])
    states = NamedTuple{Keys}(map(Keys) do key
        key === first(Keys) ? source : ExchangeState(frame, commands[key], configuration.exposure_duration_ns,
            configuration.frame_encoding, configuration.command_scale[key];
            frame_transport=source.frame_transport)
    end)
    for state in values(states)
        state.frame_ready = source.frame_ready
        state.frame_published = source.frame_published
        state.timing_source_sequence = source.timing_source_sequence
        state.timing_source_published_nanoseconds = source.timing_source_published_nanoseconds
    end
    return states
end

_prepare_exchange_state(frame, command, configuration) = throw(PipeWireHILError(
    "correction_command", "requires a dense host Array{Float32} or a named set of dense host Array{Float32} buffers"))

function _stage_command_context!(states)
    source = _frame_state(states)
    for state in _command_states(states)
        state.acquisition_domain = source.acquisition_domain
        state.acquisition_generation = source.acquisition_generation
        state.exposure_record = source.exposure_record
    end
    return nothing
end

@inline _monotonic_nanoseconds() = Int64(time_ns() & UInt64(typemax(Int64)))

function _record_frame_publication!(
    state::ExchangeState,
    sequence::UInt64,
    source_published_nanoseconds::Int64,
)
    sequence > state.timing_source_sequence[] || throw(PipeWireHILError(
        "wfs_frame.timing",
        "published sequence $sequence does not advance the timing observation",
    ))
    source_published_nanoseconds >= 0 || throw(PipeWireHILError(
        "wfs_frame.timing",
        "Header PTS is negative in the monotonic timing domain",
    ))
    source = UInt64(source_published_nanoseconds)
    source >= state.timing_last_event_nanoseconds[] || throw(PipeWireHILError(
        "wfs_frame.timing",
        "Header PTS precedes the prior timing event",
    ))
    state.timing_source_published_nanoseconds[] = source
    state.timing_source_sequence[] = sequence
    state.timing_last_event_nanoseconds[] = source
    return nothing
end

function _validate_frame_command_timing(
    state::ExchangeState,
    sequence::UInt64,
    command_received_nanoseconds::Int64,
)
    sequence == state.timing_source_sequence[] || throw(PipeWireHILError(
        "correction_command.timing",
        "received sequence $sequence has no matching published WFS frame",
    ))
    command_received_nanoseconds >= 0 || throw(PipeWireHILError(
        "correction_command.timing",
        "command receipt is negative in the monotonic timing domain",
    ))
    source = state.timing_source_published_nanoseconds[]
    received = UInt64(command_received_nanoseconds)
    received >= source || throw(PipeWireHILError(
        "correction_command.timing",
        "command receipt precedes the matching frame Header PTS",
    ))
    received >= state.timing_last_event_nanoseconds[] || throw(PipeWireHILError(
        "correction_command.timing",
        "command receipt precedes the prior timing event",
    ))
    sequence > state.timing_completed_sequence[] || throw(PipeWireHILError(
        "correction_command.timing",
        "completed sequence $sequence does not advance the timing observation",
    ))

    return nothing
end

function _complete_frame_command_timing!(
    state::ExchangeState,
    sequence::UInt64,
    command_received_nanoseconds::Int64,
)
    _validate_frame_command_timing(state, sequence, command_received_nanoseconds)
    source = state.timing_source_published_nanoseconds[]
    received = UInt64(command_received_nanoseconds)
    # The sequence is the atomic publication marker. Write every value first so
    # readers only observe a completed, self-consistent timing observation.
    state.timing_completed_source_published_nanoseconds[] = source
    state.timing_completed_command_received_nanoseconds[] = received
    state.timing_last_event_nanoseconds[] = received
    state.timing_completed_sequence[] = sequence
    return nothing
end

function _frame_command_timing(state::ExchangeState)
    while true
        sequence = state.timing_completed_sequence[]
        sequence == _NO_SEQUENCE && return nothing
        source = state.timing_completed_source_published_nanoseconds[]
        received = state.timing_completed_command_received_nanoseconds[]
        sequence == state.timing_completed_sequence[] || continue
        return PipeWireFrameCommandTiming(sequence, Int64(source), Int64(received))
    end
end

struct FrameProcess{State<:ExchangeState}
    state::State
    buffer::StreamBuffer
end

function _copy_frame!(state::ExchangeState, data)
    capacity(data) >= state.frame_bytes || throw(PipeWireHILError(
        "wfs_frame",
        "negotiated buffer capacity is smaller than the complete WFS frame",
    ))
    destination = reinterpret(eltype(state.frame_transport), buffer_memory(data, state.frame_bytes))
    copyto!(destination, state.frame_transport)
    set_chunk!(data; size=state.frame_bytes, stride=state.frame_chunk_stride)
    return nothing
end

function _set_frame_metadata!(
    state::ExchangeState,
    buffer::StreamBuffer,
    sequence::UInt64,
)
    now = _monotonic_nanoseconds()
    record = state.exposure_record
    record.sequence == sequence || throw(PipeWireHILError(
        "wfs_frame.acquisition", "the staged acquisition record does not match the publication"))
    metadata = initialize_metadata!(buffer)
    set_buffer_header!(buffer, BufferHeader(UInt32(0), UInt32(0), now, Int64(0), sequence))
    record.has_optical_classification && set_optical_classification!(metadata, record.optical_classification)
    if record.trigger_state != ACQUISITION_TRIGGER_ABSENT || metadata_record(metadata,SPA.META_ELECTRICAL_TRIGGER) !== nothing
        set_trigger_classification!(metadata, record.trigger_state)
    end
    set_acquisition_identity!(
        metadata,
        AcquisitionIdentity(
            state.acquisition_domain,
            state.acquisition_generation,
            sequence,
        ),
    )
    set_exposure_timing!(metadata, ExposureTiming(;duration_nanoseconds=record.duration_nanoseconds))
    set_reference_timestamp!(metadata,reference_timestamp_now(state.reference_clock))
    return now
end

function (process::FrameProcess)(stream::Stream)
    state = process.state
    sequence = state.frame_ready[]
    (sequence == _NO_SEQUENCE || sequence == state.frame_published[]) && return nothing
    dequeue_buffer!(process.buffer, stream) || return nothing
    queued = false
    try
        data = buffer_data(process.buffer)
        _copy_frame!(state, data)
        source_published_nanoseconds = _set_frame_metadata!(state, process.buffer, sequence)
        # queue_buffer! makes this buffer visible to downstream processing.
        # Publish its Header-PTS timing record first so the command callback
        # always finds its matching source sequence.
        _record_frame_publication!(state, sequence, source_published_nanoseconds)
        queue_buffer!(process.buffer, stream)
        queued = true
        state.frame_published[] = sequence
    finally
        queued || return_buffer!(process.buffer, stream)
    end
    return nothing
end

struct CommandProcess{State<:ExchangeState}
    state::State
    buffer::StreamBuffer
end

function _validate_command_sequence(state::ExchangeState, sequence::UInt64)
    state.command_ready[] == _NO_SEQUENCE || throw(PipeWireHILError(
        "correction_command.sequence",
        "received a second command while one command is awaiting adoption",
    ))
    sequence != _NO_SEQUENCE || throw(PipeWireHILError(
        "correction_command.sequence",
        "sequence zero is reserved",
    ))
    sequence == state.frame_ready[] && sequence == state.timing_source_sequence[] &&
        sequence > state.timing_completed_sequence[] || throw(PipeWireHILError(
            "correction_command.sequence",
            "received sequence $sequence does not match an outstanding published frame",
        ))
    return nothing
end

function _validate_command_acquisition(
    state::ExchangeState,
    metadata::Union{Nothing,MetadataRecords},
    sequence::UInt64,
)
    metadata !== nothing && metadata_records_valid(metadata) || throw(PipeWireHILError(
            "correction_command.acquisition",
            "received command requires valid metadata records",
        ))
    identity = acquisition_identity(metadata)
    identity !== nothing && identity.domain == state.acquisition_domain &&
        identity.generation == state.acquisition_generation &&
        identity.sequence == sequence || throw(PipeWireHILError(
            "correction_command.acquisition",
            "received command acquisition identity does not match the outstanding frame",
        ))
    record = state.exposure_record
    record.sequence == sequence &&
        _exposure_duration(metadata) == record.duration_nanoseconds &&
        something(trigger_classification(metadata),ACQUISITION_TRIGGER_ABSENT) == record.trigger_state &&
        optical_classification(metadata) == (record.has_optical_classification ? record.optical_classification : nothing) || throw(PipeWireHILError(
            "correction_command.acquisition",
            "received command exposure classification does not match the outstanding frame",
        ))
    return nothing
end

function _validate_command_storage(state::ExchangeState)
    sizeof(state.command) == state.command_bytes || throw(PipeWireHILError(
        "correction_command", "prepared command storage was resized; close and reconstruct the endpoint"))
    return nothing
end

function _copy_command!(state::ExchangeState, values::AbstractVector{Float32})
    _validate_command_storage(state)
    length(values) == length(state.command) || throw(PipeWireHILError(
        "correction_command",
        "received $(length(values)) elements; expected $(length(state.command))",
    ))
    # Validate the entire borrowed command and its unit conversion before the
    # first write to the host plant-command buffer.
    for value in values
        isfinite(value) || throw(PipeWireHILError(
            "correction_command",
            "received command contains a non-finite value",
        ))
        isfinite(value * state.command_scale) || throw(PipeWireHILError(
            "correction_command",
            "scaled command exceeds the finite Float32 range",
        ))
    end
    if ndims(state.command) == 1
        for index in eachindex(state.command, values)
            state.command[index] = values[index] * state.command_scale
        end
    else
        source = 1
        for index in Iterators.product(reverse(axes(state.command))...)
            state.command[reverse(index)...] = values[source] * state.command_scale
            source += 1
        end
    end
    return nothing
end

function _command_values(state::ExchangeState, data)
    _validate_command_storage(state)
    chunk = chunk_info(data)
    Int(chunk.size) == state.command_bytes || throw(PipeWireHILError(
        "correction_command",
        "received $(chunk.size) bytes; expected $(state.command_bytes)",
    ))
    Int(chunk.offset) + state.command_bytes <= capacity(data) || throw(PipeWireHILError(
        "correction_command",
        "received chunk exceeds the mapped buffer capacity",
    ))
    return reinterpret(Float32, bytes(data))
end

function _receive_command!(
    state::ExchangeState,
    values::AbstractVector{Float32},
    sequence::UInt64,
    command_received_nanoseconds::Int64,
    acquisition::Union{Nothing,MetadataRecords}=nothing,
)
    _validate_command_sequence(state, sequence)
    _validate_command_acquisition(state, acquisition, sequence)
    _validate_frame_command_timing(state, sequence, command_received_nanoseconds)
    _copy_command!(state, values)
    _complete_frame_command_timing!(state, sequence, command_received_nanoseconds)
    state.command_ready[] = sequence
    return nothing
end

function (process::CommandProcess)(stream::Stream)
    state = process.state
    state.command_ready[] == _NO_SEQUENCE || throw(PipeWireHILError(
        "correction_command.sequence",
        "received a second command while one command is awaiting adoption",
    ))
    dequeue_buffer!(process.buffer, stream) || return nothing
    queued = false
    try
        header = buffer_header(process.buffer)
        header === nothing && throw(PipeWireHILError(
            "correction_command.sequence",
            "received buffer has no header sequence",
        ))
        acquisition = metadata_records(process.buffer)
        acquisition === nothing && throw(PipeWireHILError("correction_command.acquisition", "received command lacks metadata records"))
        identity = acquisition_identity(acquisition)
        identity === nothing && throw(PipeWireHILError("correction_command.acquisition", "received command lacks physical identity"))
        sequence = identity.sequence
        command_received_nanoseconds = _monotonic_nanoseconds()
        _receive_command!(
            state,
            _command_values(state, buffer_data(process.buffer)),
            sequence,
            command_received_nanoseconds,
            acquisition,
        )
        queue_buffer!(process.buffer, stream)
        queued = true
    finally
        queued || return_buffer!(process.buffer, stream)
    end
    return nothing
end

"""
    PreparedPipeWireHIL

Own the mandatory frame-source and command-sink PipeWire stream nodes, optional adopted-command and image
sources, and bounded single-frame exchange state for one AdaptiveOpticsSim
lockstep HIL boundary. Close it explicitly.
"""
mutable struct PreparedPipeWireHIL{
    Boundary,State,Configuration<:PipeWireHILConfiguration,
    ContextType<:Context,CoreType<:CoreConnection,
    FrameStream<:Stream,FrameLoan<:Union{Nothing,StreamBuffer},CommandStream,AcquisitionSource,AppliedSource,
    ImageSources<:Tuple,ImageOwner,Acquisition,
}
    boundary::Boundary
    acquisition_source::AcquisitionSource
    configuration::Configuration
    state::State
    free_running::Acquisition
    driver_cycle::DriverCycle
    loop::ThreadLoop
    context::ContextType
    core::CoreType
    frame_stream::FrameStream
    frame_loan::FrameLoan
    command_stream::CommandStream
    applied_command_source::AppliedSource
    image_sources::ImageSources
    image_owner::ImageOwner
    started::Bool
    closed::Bool
end

function _with_loop_access(f, prepared::PreparedPipeWireHIL)
    if isrunning(prepared.loop)
        return with_thread_loop_lock(prepared.loop) do _
            f()
        end
    end
    return f()
end

"""
    frame_command_timing(prepared) -> Union{Nothing, PipeWireFrameCommandTiming}

Return the latest completed source-publication-to-command-receipt observation,
or `nothing` until a same-sequence command has been received. A named command
set returns matching per-port timing observations. This is a
single-frame development-profile observation, not a latency distribution or a
real-time deadline claim.
"""
frame_command_timing(prepared::PreparedPipeWireHIL) =
    _with_loop_access(prepared) do
        _frame_command_timing(prepared.state)
    end

"""Access the source stream under its PipeWire loop lock for bounded control work.

Do not retain the stream beyond the prepared endpoint lifetime or hold this
lock across simulation, exchange, reset, filesystem work or waits.
"""
function with_frame_stream(f::F, prepared::PreparedPipeWireHIL) where {F}
    return _with_loop_access(prepared) do
        f(prepared.frame_stream)
    end
end

"Return the current frame acquisition generation under the endpoint loop lock."
frame_acquisition_generation(prepared::PreparedPipeWireHIL) =
    _with_loop_access(prepared) do
        _frame_state(prepared.state).acquisition_generation
    end

_frame_exchange_buffer(values::Union{Array{Float32},Array{UInt16}}, role::String) = values
_frame_exchange_buffer(values, role::String) = throw(PipeWireHILError(
    role, "requires a dense host Array{Float32} or Array{UInt16} frame buffer",
))

function _format(array::Union{Array{Float32},Array{UInt16}}, rate::Union{Nothing,SPA.Fraction}, ::Type{T}=eltype(array)) where {T<:Union{Float32,UInt16}}
    return NdArrayFormat(
        _wire_element_type(T),
        size(array);
        layout=NdArray.ROW_MAJOR,
        rate,
    )
end

function _frame_buffer_count(buffers; field="frame_buffers")
    buffers isa Integer && !(buffers isa Bool) && 2 <= buffers <= 64 ||
        throw(PipeWireHILError(field, "must be an integer in 2:64"))
    return Int(buffers)
end

const _ACQUISITION_METADATA = (
    MetadataRequirement(SPA.META_ACQUISITION_IDENTITY;required=true),
    MetadataRequirement(SPA.META_EXPOSURE_TIMING;required=true),
    MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP;required=true),
)
_exposure_duration(metadata) = begin
    timing = exposure_timing(metadata)
    timing === nothing || !timing.has_duration ? nothing : timing.duration_nanoseconds
end
function _stream_parameters(format::NdArrayFormat, schema::String;buffers=2,metadata::Tuple=_ACQUISITION_METADATA)
    count=_frame_buffer_count(buffers)
    params=Pod[ndarray_format(format;schema),
        Pod(buffers_param(size=payload_size(format),buffers=count,metadata_types=Int32(1 << SPA.META_HEADER))),
        Pod(header_metadata_param())]
    append!(params,(Pod(metadata_param(requirement)) for requirement in metadata))
    return params
end

function _stream_properties(node_name::String; driver::Bool=false,header_pts_clock::String="unavailable")
    properties = Dict(
        "node.name" => node_name,
        "media.type" => "Application",
        "media.category" => "Filter",
        "media.role" => "DSP",
        "node.description" => node_name,
        "pipewireao.header.pts.clock" => header_pts_clock,
    )
    if driver
        properties["node.want-driver"] = "true"
        properties["priority.driver"] = _HIL_DRIVER_PRIORITY
    end
    return properties
end

function _wait_for_node(
    prepared::PreparedPipeWireHIL,
    stream::Stream,
    timeout_ns::UInt64,
    field::String,
)
    start = time_ns()
    while true
        id = _with_loop_access(prepared) do
            node_id(stream)
        end
        id != _INVALID_NODE_ID && return id
        time_ns() - start <= timeout_ns || throw(PipeWireHILError(
            field,
            "PipeWire node did not become inspectable before the development timeout",
        ))
        sleep(0.001)
    end
end

_camera_buffer_loan(boundary, configuration) = nothing

# Staged exchange also supports the released AOS boundary.
@static if isdefined(AlgorithmGraphs, :PreparedBorrowedFrameHILBoundary)
    function _camera_buffer_loan(boundary::AlgorithmGraphs.PreparedBorrowedFrameHILBoundary, configuration)
        frame = hil_frame_buffer(boundary)
        frame isa Matrix{UInt16} && configuration.frame_encoding === :uint16 ||
            throw(PipeWireHILError("wfs_frame", "direct loans require a packed UInt16 matrix"))
        AlgorithmGraphs.hil_frame_exposure_nanoseconds(boundary) == configuration.exposure_duration_ns ||
            throw(PipeWireHILError("wfs_frame.acquisition", "the configured duration differs from the prepared camera exposure"))
        ENDIAN_BOM == 0x04030201 ||
            throw(PipeWireHILError("wfs_frame", "direct UInt16 loans require a little-endian host"))
        return StreamBuffer()
    end
end

"""
    prepare_pipewire_hil(boundary, configuration; frame_properties=nothing,
        frame_params=(), on_frame_param_changed=nothing,
        frame_param_buffer=nothing, on_frame_param_overflow=nothing,
        frame_buffers=2, command_buffers=2, applied_command_node_name=nothing,
        applied_command_buffers=2, applied_command_properties=nothing,
        image_sources=())

Create and connect inactive WFS-frame source and correction-command sink nodes
on the configured private PipeWire core. The boundary must use distinct dense
host `Array{Float32}` command buffers and `Array{Float32}` or `Array{UInt16}`
frame buffers. No simulation frame is executed during preparation.

A buffered plant provides `hil_frame_buffer` and `hil_command_buffer` from
`AdaptiveOpticsSim.AlgorithmGraphs`. `step_hil_frame!` stages one frame and
returns its positive UInt64 sequence. `adopt_hil_command!` consumes the
command written into the command buffer for that sequence. The exchange
owner calls both methods outside the native process callbacks. If the caller
uses reset, the plant also provides `hil_boundary_status` and
`reset_hil_boundary!`.

A prepared borrowed-frame boundary writes directly to the dequeued camera
buffer and requires a caller-owned model-time driver and UInt16 transport.
The exchange owner holds the loan through frame generation and metadata
publication. If no buffer is available, the model waits before advancing.

Optional source parameter hooks use the public PipeWireAO `Stream` contract.
They may stage bounded controls; they must not run simulation, change admission,
or reset an exchange. The owner applies controls after `exchange_frame!` returns.
Additional properties cannot override the transport's existing properties.

`frame_buffers` selects the source pool's fixed cardinality, an integer in
`2:64`, before any link negotiates. A deployment using the public capacity-one
PipeWireAO observation queue selects three. This does not create an observation
branch or permit direct observer attachment; topology, isolation and validation
remain the deployment owner's responsibilities. `command_buffers` selects the
command sink pool with the same bounds; a capacity-one command observation queue
also requires three. Defaults remain two for both pools. The exchange still
admits exactly one frame and one coherent command set at a time.

`applied_command_node_name` opts into a separate simulator-adopted command
source. Physical-DM commands use schema `pipewireao/applied-dm-command-opd-um-f32/1`, the declared actuator-vector shape and
Float32 transport. It publishes a copied command only after successful matched
adoption, with the matching frame Header sequence and original ingress PTS.
Values are µm OPD, converted from the adopted metre-OPD plant command.
Preparation requires a known OPD command schema and its corresponding input
scale; arbitrary external mandatory exchange schemas remain supported without
this optional source. Tip/tilt commands with schema
`pipewireao/tip-tilt-opd-coefficients-m-f32/1` retain their two normalized-pupil
XY coefficients in metre OPD and their schema, with input scale `1.0f0`.
All selected observation contracts are checked before native connection.
Each source retains one prepared snapshot and drops optional
observations when busy. The session owner must clock this independent source;
the exchange never triggers it or waits for its consumers. The node is present
at preparation, before the first frame. `applied_command_buffers` is in `2:64`.
`applied_command_properties` adds owner-defined stream properties, such as
`node.group` and `node.loop.name`, before connection. It cannot override the
transport's existing properties, including `node.name` and `media.schema`.
"""
function prepare_pipewire_hil(
    boundary,
    configuration::PipeWireHILConfiguration,
    ; frame_properties=nothing, frame_params=(), on_frame_param_changed=nothing,
    frame_param_buffer=nothing, on_frame_param_overflow=nothing, frame_buffers=2,
    command_buffers=2,
    acquisition_source=nothing,
    applied_command_node_name=nothing, applied_command_buffers=2,
    applied_command_properties=nothing,
    image_sources::Tuple=(),
    reference_clock::MetadataClock=METADATA_CLOCK_REALTIME,
)
    frame_loan = _camera_buffer_loan(boundary, configuration)
    source = _prepare_acquisition_source(acquisition_source, boundary, configuration)
    source_buffers = _frame_buffer_count(frame_buffers)
    sink_buffers = _frame_buffer_count(command_buffers; field="command_buffers")
    observation_buffers = _frame_buffer_count(applied_command_buffers; field="applied_command_buffers")
    frame = _frame_exchange_buffer(hil_frame_buffer(boundary), "wfs_frame")
    command = hil_command_buffer(boundary)
    state = _prepare_exchange_state(frame, command, configuration)
    _set_exchange_reference_clock!(state,reference_clock)
    free_running = _prepare_free_running(boundary, state)
    applied_contracts = applied_command_node_name === nothing ? nothing :
        _applied_command_contracts(configuration, command)
    loop = context = core = frame_stream = command_stream = applied_command_source = nothing
    prepared_images = ()
    image_owner = nothing
    try
        loop = ThreadLoop("AdaptiveOpticsSimPipeWireHIL")
        context = Context(loop;
            properties=Dict("application.name" => "AOS simulator"))
        core = CoreConnection(
            context;
            properties=Dict("remote.name" => configuration.remote),
        )
        frame_process = frame_loan === nothing ? FrameProcess(_frame_state(state), StreamBuffer()) : nothing
        driver_cycle = DriverCycle()
        frame_trigger_done = FrameTriggerDone(driver_cycle)
        properties = _stream_properties(configuration.frame_node_name; driver=true,header_pts_clock="monotonic")
        if frame_properties !== nothing
            for (key, value) in frame_properties
                haskey(properties, key) && throw(ArgumentError("frame property overrides transport: $key"))
                properties[key] = value
            end
        end
        frame_stream = Stream(
            core,
            configuration.frame_node_name;
            properties=properties,
            on_process=frame_process,
            on_trigger_done=frame_trigger_done,
            on_param_changed=on_frame_param_changed,
            param_buffer=frame_param_buffer,
            on_param_overflow=on_frame_param_overflow,
        )
        command_stream = _prepare_command_streams(core, state, configuration, free_running)
        # Compile the exact callbacks before admitting the first model frame.
        frame_process === nothing || precompile(frame_process, (typeof(frame_stream),))
        precompile(frame_trigger_done, (typeof(frame_stream),))
        applied_command_source = _prepare_applied_command_source(
            core, configuration, command, applied_command_node_name, observation_buffers,
            applied_command_properties, applied_contracts,
        )
        _set_applied_reference_clock!(applied_command_source,reference_clock)
        image_owner = _prepare_image_source_owner(configuration, _frame_state(state), image_sources,
            applied_command_node_name)
        prepared_images = image_owner === nothing ? () : image_owner.sources
        prepared = PreparedPipeWireHIL(
            boundary,
            source,
            configuration,
            state,
            free_running,
            driver_cycle,
            loop,
            context,
            core,
            frame_stream,
            frame_loan,
            command_stream,
            applied_command_source,
            prepared_images,
            image_owner,
            false,
            false,
        )
        common_flags = STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT
        connect!(
            frame_stream,
            :output;
            flags=common_flags | STREAM_DRIVER,
            params=(_stream_parameters(
                _format(frame, configuration.rate, eltype(_frame_state(state).frame_transport)),
                configuration.frame_schema; buffers=source_buffers,
            )..., frame_params...),
        )
        _connect_command_streams!(command_stream, state, configuration, sink_buffers, common_flags)
        start!(loop)
        _wait_for_node(
            prepared,
            frame_stream,
            configuration.timeout_ns,
            "frame_node_name",
        )
        for stream in _command_streams(command_stream)
            _wait_for_node(prepared, stream, configuration.timeout_ns, "command_node_name")
        end
        if applied_command_source !== nothing
            for stream in _command_streams(_applied_source_streams(applied_command_source))
                _wait_for_node(prepared, stream, configuration.timeout_ns, "applied_command_node_name")
            end
        end
        return prepared
    catch exception
        observation_stream = _applied_source_streams(applied_command_source)
        errors = _close_image_owner!(image_owner)
        append!(errors, _close_pipewire_resources!(loop, context, core, frame_stream,
            command_stream, observation_stream))
        isempty(errors) || throw(CompositeException(Any[exception; errors]))
        rethrow()
    end
end

function _prepare_command_streams(core, state::ExchangeState, configuration, acquisition)
    return _prepare_command_stream(core, state, configuration.command_node_name, _command_receiver(acquisition, 1))
end
function _prepare_command_streams(core, states::NamedTuple{Keys}, configuration, acquisition) where {Keys}
    created = ()
    try
        for (index, key) in enumerate(Keys)
            created = (created..., _prepare_command_stream(core, states[key],
                configuration.command_node_name[key], _command_receiver(acquisition, index)))
        end
        return NamedTuple{Keys}(created)
    catch exception
        errors = Any[exception]
        for stream in reverse(created)
            _attempt_cleanup!(() -> close(stream), errors)
        end
        length(errors) == 1 && rethrow()
        throw(CompositeException(errors))
    end
end
_command_receiver(::Nothing, index) = nothing

function _prepare_command_stream(core, state, name, receiver)
    callback = if receiver === nothing
        CommandProcess(state, StreamBuffer())
    else
        ArrayStreamProcess(receiver, StreamBuffer(), true, Int(_frame_chunk_stride(_format(state.command, nothing))))
    end
    stream = Stream(core, name; properties=_stream_properties(name), on_process=callback)
    try
        precompile(callback, (typeof(stream),))
        return stream
    catch
        close(stream)
        rethrow()
    end
end
function _connect_command_streams!(stream::Stream, state, configuration, buffers, flags;metadata::Tuple=_ACQUISITION_METADATA)
    connect!(stream, :input; flags, params=_stream_parameters(
        _format(state.command, configuration.rate), configuration.command_schema;
        buffers,metadata))
    return nothing
end
function _connect_command_streams!(streams::NamedTuple, states, configuration, buffers, flags;metadata::Tuple=_ACQUISITION_METADATA)
    for key in keys(streams)
        connect!(streams[key], :input; flags, params=_stream_parameters(
            _format(states[key].command, configuration.rate), configuration.command_schema[key];
            buffers,metadata))
    end
    return nothing
end
_command_stream_states(streams) = map(stream_state, _command_streams(streams))
function _set_command_active!(streams, active)
    errors = Any[]
    for stream in _command_streams(streams)
        _attempt_cleanup!(() -> set_active!(stream, active), errors)
    end
    if !isempty(errors)
        # A partial start must leave every successfully activated sink inactive.
        if active
            for stream in _command_streams(streams)
                _attempt_cleanup!(() -> set_active!(stream, false), errors)
            end
        end
        throw(length(errors) == 1 ? only(errors) : CompositeException(errors))
    end
    return nothing
end

function _wait_for_commands(prepared, sequence, idle_strategy)
    return _await_command_set(_command_states(prepared.state), sequence,
        prepared.configuration.timeout_ns, idle_strategy) do
        _with_loop_access(prepared) do
            stream_state(prepared.frame_stream)
            _command_stream_states(prepared.command_stream)
            _issue_driver_cycle!(prepared.driver_cycle) do
                trigger_process!(prepared.frame_stream)
            end
        end
    end
end

# One deadline covers the whole command set, including a missing final port.
function _await_command_set(retry!::F, states::Tuple, sequence::UInt64, timeout_ns::UInt64,
                            idle_strategy=SimRunner.current_strategy()) where {F}
    start = time_ns()
    retry_at = start
    while !all(state -> _sequence_complete(state.command_ready, sequence,
            "correction_command.sequence", false), states)
        if time_ns() >= retry_at
            retry!() === false || (retry_at = time_ns() + UInt64(1_000_000))
        end
        time_ns() - start <= timeout_ns || throw(PipeWireHILError(
            "correction_command.sequence", "command set $sequence did not complete before the development timeout"))
        SimRunner.idle!(idle_strategy)
    end
    return sequence
end

_frame_command_timing(states::NamedTuple) = map(_frame_command_timing, states)
function _require_quiescent_exchange(states::NamedTuple)
    foreach(_require_quiescent_exchange, values(states))
    return nothing
end
function _reset_exchange_state!(states::NamedTuple)
    _require_quiescent_exchange(states)
    foreach(_clear_exchange_state!, values(states))
    return nothing
end

function _wait_for_frame_driver(prepared::PreparedPipeWireHIL, idle_strategy=SimRunner.current_strategy())
    start = time_ns()
    frame_state = PipeWireAO.LibPipeWire.PW_STREAM_STATE_UNCONNECTED
    command_state = PipeWireAO.LibPipeWire.PW_STREAM_STATE_UNCONNECTED
    while true
        frame_state, command_state, driving = _with_loop_access(prepared) do
            (
                stream_state(prepared.frame_stream),
                _command_stream_states(prepared.command_stream),
                is_driving(prepared.frame_stream),
            )
        end
        if frame_state == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING &&
           all(==(PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING), command_state) &&
           driving
            return nothing
        end
        time_ns() - start <= prepared.configuration.timeout_ns || throw(PipeWireHILError(
            "wfs_frame.driver",
            "HIL graph did not become ready; observed frame state " *
            "$frame_state, command state $command_state, and frame driving=$driving",
        ))
        _wait_for_exchange_poll!(idle_strategy)
    end
end

"""Activate both HIL endpoint streams without stepping the AOS boundary."""
function start!(prepared::PreparedPipeWireHIL)
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    prepared.started && return prepared
    _with_loop_access(prepared) do
        _set_command_active!(prepared.command_stream, true)
        try
            set_active!(prepared.frame_stream, true)
        catch
            _set_command_active!(prepared.command_stream, false)
            rethrow()
        end
        _set_applied_command_active!(prepared.applied_command_source, true)
    end
    _set_image_owner_active!(prepared.image_owner, true)
    prepared.started = true
    return prepared
end

"""Deactivate both HIL endpoint streams while retaining AOS and RTC state."""
function stop!(prepared::PreparedPipeWireHIL)
    prepared.closed && return prepared
    prepared.started || return prepared
    _with_loop_access(prepared) do
        set_active!(prepared.frame_stream, false)
        _set_command_active!(prepared.command_stream, false)
        _set_applied_command_active!(prepared.applied_command_source, false)
        _reset_applied_command!(prepared.applied_command_source)
    end
    _set_image_owner_active!(prepared.image_owner, false)
    _reset_image_owner!(prepared.image_owner)
    prepared.started = false
    return prepared
end

function _sequence_complete(sequence::Atomic{UInt64}, expected::UInt64, field::String, allow_prior::Bool)
    # Use one observation: publication between two reads must not turn the
    # matching expected sequence into a reported sequence error.
    observed = sequence[]
    observed == expected && return true
    if observed != _NO_SEQUENCE && (!allow_prior || observed > expected)
        throw(PipeWireHILError(field, "expected sequence $expected, received $observed"))
    end
    return false
end

function _wait_for_sequence(
    prepared::PreparedPipeWireHIL,
    sequence::Atomic{UInt64},
    expected::UInt64,
    field::String,
    idle_strategy;
    allow_prior::Bool=false,
    drive::Bool=false,
)
    retry! = () -> _with_loop_access(prepared) do
        stream_state(prepared.frame_stream)
        _command_stream_states(prepared.command_stream)
        if drive
            return _issue_driver_cycle!(prepared.driver_cycle) do
                trigger_process!(prepared.frame_stream)
            end
        end
        return true
    end
    return _await_sequence(sequence, expected, field,
        prepared.configuration.timeout_ns, allow_prior, retry!, idle_strategy)
end

function _await_sequence(sequence::Atomic{UInt64}, expected::UInt64, field::String,
                         timeout_ns::UInt64, allow_prior::Bool, retry!::F,
                         idle_strategy=SimRunner.current_strategy()) where {F}
    start = time_ns()
    retry_at = start
    while !_sequence_complete(sequence, expected, field, allow_prior)
        now = time_ns()
        if now >= retry_at
            # A pending native cycle can suppress a trigger attempt. Only an
            # issued trigger (or a health-only check) consumes the 1 ms retry
            # cadence; completion is still checked on every pass.
            retry!() === false || (retry_at = time_ns() + UInt64(1_000_000))
        end
        time_ns() - start <= timeout_ns || throw(PipeWireHILError(
            field,
            "sequence $expected did not complete before the development timeout",
        ))
        SimRunner.idle!(idle_strategy)
    end
    return expected
end

function _wait_for_exchange_poll!(idle_strategy)
    start = time_ns()
    while time_ns() - start < UInt64(1_000_000)
        SimRunner.idle!(idle_strategy)
    end
    return nothing
end

function _await_driver_cycle!(prepared::PreparedPipeWireHIL, idle_strategy)
    return _await_driver_cycle!(prepared.driver_cycle, prepared.configuration.timeout_ns, idle_strategy) do
        _with_loop_access(prepared) do
            stream_state(prepared.frame_stream)
            _command_stream_states(prepared.command_stream)
            nothing
        end
    end
end

@inline _encode_frame_value(::Type{T}, value::T) where {T<:Union{Float32,UInt16}} = value
@inline _encode_frame_value(::Type{Float32}, value::UInt16) = Float32(value)

@inline function _encode_frame_value(::Type{UInt16}, value::Float32)
    isfinite(value) && 0 <= value <= typemax(UInt16) || throw(PipeWireHILError(
        "wfs_frame",
        "UInt16 transport requires finite raw ADC codes in 0:65535",
    ))
    return round(UInt16, value)
end

function _stage_frame_transport!(state::ExchangeState)
    element = eltype(state.frame_transport)
    if ndims(state.frame) == 1
        for index in eachindex(state.frame_transport, state.frame)
            state.frame_transport[index] = _encode_frame_value(element, state.frame[index])
        end
        return nothing
    end
    destination = 1
    for index in Iterators.product(reverse(axes(state.frame))...)
        state.frame_transport[destination] = _encode_frame_value(element, state.frame[reverse(index)...])
        destination += 1
    end
    return nothing
end

"""
    exchange_frame!(prepared[, model_time_driver]; timing=nothing, idle_strategy) -> sequence

Step one complete AOS frame, publish it, wait for the same-sequence complete
correction command on every declared port, and adopt that complete command set
for the next AOS frame. Exactly one frame and one coherent command set may be in flight.
With a caller-owned model-time driver, step at its next model-time boundary;
the adapter does not pace wall-clock execution.
An optional caller-owned [`PipeWireExchangeTiming`](@ref) records bounded phase
timestamps on the exchange owner thread. It is invalidated on entry, and its
generation/sequence are assigned only after successful command adoption.
The optional `idle_strategy` controls the exchange owner's completion waits.
"""
function exchange_frame!(prepared::PreparedPipeWireHIL, model_time_driver=nothing;
        timing::Union{Nothing,PipeWireExchangeTiming}=nothing,
        idle_strategy=SimRunner.current_strategy())
    return _exchange_frame!(prepared, model_time_driver, timing, idle_strategy)
end

function _exchange_frame!(prepared::PreparedPipeWireHIL, model_time_driver, timing, idle_strategy)
    prepared.free_running === nothing ||
        throw(ArgumentError("Use step_free_running! with a held-command boundary"))
    _begin_exchange_timing!(timing)
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    prepared.started || throw(PipeWireHILError("lifecycle", "HIL endpoints are not started"))
    _require_model_time_driver(prepared.frame_loan, model_time_driver)
    _wait_for_frame_driver(prepared, idle_strategy)
    _await_driver_cycle!(prepared, idle_strategy)
    _exchange_timestamp!(timing, Val(:ready_ns))
    state = _frame_state(prepared.state)
    state.frame_ready[] == state.frame_published[] || throw(PipeWireHILError(
        "wfs_frame.sequence",
        "the prior frame has not been published",
    ))
    all(port -> port.command_ready[] == _NO_SEQUENCE, _command_states(prepared.state)) ||
        throw(PipeWireHILError("correction_command.sequence", "a prior command has not been adopted"))

    sequence = _step_and_publish_frame!(prepared, model_time_driver, timing, prepared.frame_loan, idle_strategy)
    _wait_for_commands(prepared, sequence, idle_strategy)
    _await_driver_cycle!(prepared, idle_strategy)
    # Readiness can already be complete when the owner starts waiting. Check
    # stored native callback failures even when no polling iteration was needed.
    _with_loop_access(prepared) do
        stream_state(prepared.frame_stream)
        _command_stream_states(prepared.command_stream)
    end
    _exchange_timestamp!(timing, Val(:adoption_started_ns))
    _adopt_received_command!(adopt_hil_command!, prepared.boundary, prepared.state,
        prepared.applied_command_source, sequence)
    _complete_exchange_timing!(timing, state.acquisition_generation, sequence)
    return sequence
end

function _step_and_publish_frame!(prepared, model_time_driver, timing, ::Nothing, idle_strategy)
    state = _frame_state(prepared.state)
    _exchange_timestamp!(timing, Val(:step_started_ns))
    sequence = if model_time_driver === nothing
        step_hil_frame!(prepared.boundary)
    else
        AlgorithmGraphs.step_hil_frame_at!(prepared.boundary, model_time_driver).sequence
    end
    _exchange_timestamp!(timing, Val(:step_finished_ns))
    _stage_exposure_record!(state, prepared.acquisition_source, sequence)
    _stage_frame_transport!(state)
    _stage_command_context!(prepared.state)
    _exchange_timestamp!(timing, Val(:staged_ns))
    state.frame_ready[] = sequence
    _with_loop_access(prepared) do
        _issue_driver_cycle!(prepared.driver_cycle) do
            trigger_process!(prepared.frame_stream)
        end
    end
    # A cycle can finish without publishing when no free buffer was available.
    # Retry only after completion, using the same staged frame and sequence.
    _wait_for_sequence(
        prepared,
        state.frame_published,
        sequence,
        "wfs_frame.sequence",
        idle_strategy;
        allow_prior=true,
        drive=true,
    )
    return sequence
end

function _camera_loan_destination(state::ExchangeState, data)
    capacity(data) >= state.frame_bytes || throw(PipeWireHILError(
        "wfs_frame", "negotiated buffer capacity is smaller than the complete WFS frame",
    ))
    data_flags(data) & SPA.DATA_FLAG_WRITABLE != 0 || throw(PipeWireHILError(
        "wfs_frame", "the mapped frame buffer is read-only",
    ))
    address = data_pointer(data)
    UInt(address) % Base.datatype_alignment(UInt16) == 0 || throw(PipeWireHILError(
        "wfs_frame", "the mapped frame buffer is not UInt16-aligned",
    ))
    return unsafe_wrap(Array, Ptr{UInt16}(address), reverse(size(state.frame)); own=false)
end

_require_model_time_driver(loan, driver) = nothing
_require_model_time_driver(::StreamBuffer, ::Nothing) =
    throw(PipeWireHILError("wfs_frame.driver", "a direct camera loan requires a model-time driver"))

function _step_and_publish_frame!(prepared, model_time_driver, timing, loan::StreamBuffer, idle_strategy)
    # Establish explicit native triggering before queueing from the owner.
    # PipeWire's legacy mode otherwise triggers implicitly on the first queue.
    if iszero(prepared.driver_cycle.completed[])
        _with_loop_access(prepared) do
            _issue_driver_cycle!(prepared.driver_cycle) do
                trigger_process!(prepared.frame_stream)
            end
        end
        _await_driver_cycle!(prepared, idle_strategy)
    end
    started = time_ns()
    while true
        sequence = _with_loop_access(prepared) do
            stream = prepared.frame_stream
            if dequeue_buffer!(loan, stream)
                return _step_into_camera_loan!(prepared, model_time_driver, timing, loan)
            end
            _issue_driver_cycle!(prepared.driver_cycle) do
                trigger_process!(stream)
            end
            return nothing
        end
        sequence === nothing || return sequence
        time_ns() - started <= prepared.configuration.timeout_ns || throw(PipeWireHILError(
            "wfs_frame", "no camera output loan became available before the development timeout",
        ))
        _await_driver_cycle!(prepared, idle_strategy)
        _wait_for_exchange_poll!(idle_strategy)
    end
end

function _step_into_camera_loan!(prepared, model_time_driver, timing, loan::StreamBuffer)
    state = _frame_state(prepared.state)
    stream = prepared.frame_stream
    queued = false
    # The owner holds the native loop lock throughout the loan so a local
    # renegotiation cannot unmap its storage while the simulator is writing.
    sequence = try
        data = buffer_data(loan)
        destination = _camera_loan_destination(state, data)
        _exchange_timestamp!(timing, Val(:step_started_ns))
        sample = AlgorithmGraphs.step_hil_frame_at!(prepared.boundary, model_time_driver, destination)
        _exchange_timestamp!(timing, Val(:step_finished_ns))
        sequence = sample.sequence
        _stage_exposure_record!(state, prepared.acquisition_source, sequence)
        _stage_command_context!(prepared.state)
        set_chunk!(data; size=state.frame_bytes, stride=state.frame_chunk_stride)
        _exchange_timestamp!(timing, Val(:staged_ns))
        state.frame_ready[] = sequence
        now = _set_frame_metadata!(state, loan, sequence)
        _record_frame_publication!(state, sequence, now)
        queue_buffer!(loan, stream)
        queued = true
        state.frame_published[] = sequence
        sequence
    finally
        queued || return_buffer!(loan, stream)
    end
    _issue_driver_cycle!(prepared.driver_cycle) do
        trigger_process!(stream)
    end
    return sequence
end

function _require_quiescent_exchange(state::ExchangeState)
    state.frame_ready[] == state.frame_published[] == state.timing_completed_sequence[] &&
        state.command_ready[] == _NO_SEQUENCE || throw(PipeWireHILError(
            "lifecycle",
            "cannot reset while a frame or command exchange is pending",
        ))
    state.acquisition_generation < typemax(UInt64) || throw(PipeWireHILError(
        "wfs_frame.acquisition",
        "acquisition generation cannot advance beyond UInt64 range",
    ))
    return nothing
end

function _reset_exchange_state!(state::ExchangeState)
    _require_quiescent_exchange(state)
    return _clear_exchange_state!(state)
end

function _clear_exchange_state!(state::ExchangeState)
    state.exposure_record = PipeWireExposureRecord(0, state.exposure_duration_ns)
    state.acquisition_generation += UInt64(1)
    state.frame_ready[] = _NO_SEQUENCE
    state.frame_published[] = _NO_SEQUENCE
    state.command_ready[] = _NO_SEQUENCE
    state.timing_source_sequence[] = _NO_SEQUENCE
    state.timing_source_published_nanoseconds[] = _NO_SEQUENCE
    state.timing_last_event_nanoseconds[] = _NO_SEQUENCE
    state.timing_completed_sequence[] = _NO_SEQUENCE
    state.timing_completed_source_published_nanoseconds[] = _NO_SEQUENCE
    state.timing_completed_command_received_nanoseconds[] = _NO_SEQUENCE
    fill!(state.frame_transport, zero(eltype(state.frame_transport)))
    return nothing
end

"""
    reset_pipewire_hil!(prepared[, model_time_driver]; idle_strategy) -> prepared

Reset a quiescent HIL boundary and optional caller-owned model-time driver.
Advance the acquisition generation and clear exchange sequences and timing
observations while holding the PipeWire loop lock. The owner must serialize
this call with exposure steps; endpoints may remain started. In lockstep,
pending publication, command receipt, or command adoption prevents reset.
Free-running drains the outstanding driver cycle using the optional
`idle_strategy` and clears pending frames and command groups.
"""
function reset_pipewire_hil!(prepared::PreparedPipeWireHIL, model_time_driver=nothing;
                             idle_strategy=SimRunner.current_strategy())
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    prepared.free_running === nothing ||
        return _reset_free_running!(prepared, model_time_driver, idle_strategy)
    _require_model_time_driver(prepared.frame_loan, model_time_driver)
    _with_loop_access(prepared) do
        state = prepared.state
        _require_quiescent_driver_cycle(prepared.driver_cycle)
        _require_quiescent_exchange(state)
        AlgorithmGraphs.hil_boundary_status(prepared.boundary).command_response_required &&
            throw(PipeWireHILError("lifecycle", "the AOS boundary has a pending command response"))
        if model_time_driver === nothing
            AlgorithmGraphs.reset_hil_boundary!(prepared.boundary)
        else
            AlgorithmGraphs.reset_hil_boundary!(prepared.boundary, model_time_driver)
        end
        _reset_exchange_state!(state)
        _reset_driver_cycle!(prepared.driver_cycle)
        _reset_applied_command!(prepared.applied_command_source)
    end
    _reset_image_owner!(prepared.image_owner)
    return prepared
end

function _attempt_cleanup!(f, errors)
    try
        f()
    catch exception
        push!(errors, exception)
    end
    return nothing
end

function _close_pipewire_resources!(loop, context, core, frame_stream, command_stream,
    observation_stream=nothing, image_streams=())
    errors = Any[]
    close_children = () -> begin
        for resource in (reverse(image_streams)..., reverse(_command_streams(observation_stream))...,
            reverse(_command_streams(command_stream))..., frame_stream, core, context)
            resource === nothing && continue
            _attempt_cleanup!(errors) do
                close(resource)
            end
        end
    end
    if loop === nothing
        close_children()
        return errors
    end
    children_attempted = false
    _attempt_cleanup!(errors) do
        if isrunning(loop)
            with_thread_loop_lock(loop) do _
                children_attempted = true
                close_children()
            end
        else
            children_attempted = true
            close_children()
        end
    end
    stopped = false
    _attempt_cleanup!(errors) do
        stop!(loop)
        stopped = true
    end
    # If native lock acquisition failed, retry child teardown after a successful
    # stop, when callbacks cannot race object destruction.
    if !children_attempted && stopped
        close_children()
    end
    _attempt_cleanup!(errors) do
        close(loop)
    end
    return errors
end

function _close_pipewire_hil!(prepared)
    prepared.closed && return nothing
    errors = Any[]
    _attempt_cleanup!(errors) do
        prepared.started && stop!(prepared)
    end
    append!(errors, _close_image_owner!(_image_owner(prepared)))
    append!(errors, _close_pipewire_resources!(
        prepared.loop, prepared.context, prepared.core,
        prepared.frame_stream, prepared.command_stream,
        _applied_command_resource(prepared),
    ))
    prepared.started = false
    prepared.closed = all(resource -> resource === nothing || !isopen(resource), (
        _image_resources(prepared)...,
        _command_streams(_applied_command_resource(prepared))...,
        _command_streams(prepared.command_stream)..., prepared.frame_stream, prepared.core,
        prepared.context, prepared.loop,
    ))
    isempty(errors) || throw(length(errors) == 1 ? only(errors) : CompositeException(errors))
    return nothing
end

Base.close(prepared::PreparedPipeWireHIL) = _close_pipewire_hil!(prepared)

_set_exchange_reference_clock!(state::ExchangeState,clock::MetadataClock) = (state.reference_clock=clock;state)
_set_exchange_reference_clock!(states::NamedTuple,clock::MetadataClock) = foreach(state -> _set_exchange_reference_clock!(state,clock),values(states))

_set_applied_reference_clock!(::Nothing,clock)=nothing
_set_applied_reference_clock!(source,clock)=(source.state.reference_clock=clock;nothing)
_set_applied_reference_clock!(sources::NamedTuple,clock)=foreach(source -> _set_applied_reference_clock!(source,clock),values(sources))

function _header_pts_property(key::String,value::String)
    if key=="pipewireao.header.pts.clock"
        value in ("monotonic","realtime","tai","model","unavailable") || throw(ArgumentError("invalid Header PTS clock label"))
        return true
    elseif key=="pipewireao.header.pts.absolute-conversion"
        throw(ArgumentError("Header PTS conversion properties are unsupported"))
    end
    return false
end
