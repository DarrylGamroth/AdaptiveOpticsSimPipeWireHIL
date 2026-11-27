"Schema for the optional simulator-adopted PDM command observation."
const APPLIED_PDM_COMMAND_SCHEMA = Schemas.APPLIED_DM_COMMAND_OPD_UM_V1
const _APPLIED_PDM_COMMAND_SCALE_M = 1.0f-6

# The exchange owner and observation callback each try once to claim this
# snapshot. Neither spins or waits. The snapshot is owned here, never borrowed
# from the command transport or the simulator, and contains one adopted command.
mutable struct AppliedCommandState
    values::Vector{Float32}
    busy::Atomic{Bool}
    sequence::UInt64
    pts::Int64
    pending::Bool
    dropped::Atomic{UInt64}
    failed::Atomic{Bool}
    reference_clock::MetadataClock
end

AppliedCommandState(count::Integer) = AppliedCommandState(
    zeros(Float32, count), Atomic{Bool}(false),
    UInt64(0), Int64(0), false, Atomic{UInt64}(0), Atomic{Bool}(false),METADATA_CLOCK_REALTIME,
)

struct PreparedAppliedCommandSource{S<:Stream}
    stream::S
    state::AppliedCommandState
    node_name::String
    format::NdArrayFormat{1}
    schema::String
    media_name::String
    units::Symbol
    command_scale::Float32
end

function _stage_applied_command!(
    state::AppliedCommandState, command::Vector{Float32},
    scale::Float32, sequence::UInt64, pts::Int64,
)
    if state.failed[] || Base.Threads.atomic_cas!(state.busy, false, true)
        Base.Threads.atomic_add!(state.dropped, UInt64(1))
        return false
    end
    try
        # A rounded transport->plant conversion can overflow on inversion at
        # Float32's upper boundary. Drop that optional observation atomically.
        for value in command
            if !isfinite(value / scale)
                Base.Threads.atomic_add!(state.dropped, UInt64(1))
                return false
            end
        end
        state.pending && Base.Threads.atomic_add!(state.dropped, UInt64(1))
        for index in eachindex(state.values, command)
            state.values[index] = command[index] / scale
        end
        state.sequence = sequence
        state.pts = pts
        state.pending = true
        return true
    finally
        state.busy[] = false
    end
end

_stage_applied_command!(::Nothing, ::ExchangeState, ::UInt64) = false
function _stage_applied_command!(observation::AppliedCommandState, state::ExchangeState, sequence::UInt64)
    return _stage_applied_command!(
        observation, state.command, _APPLIED_PDM_COMMAND_SCALE_M, sequence,
        Int64(state.timing_source_published_nanoseconds[]),
    )
end
function _stage_applied_command!(source::PreparedAppliedCommandSource, state::ExchangeState, sequence::UInt64)
    return _stage_applied_command!(source.state, state.command, source.command_scale, sequence,
        Int64(state.timing_source_published_nanoseconds[]))
end

struct AppliedCommandProcess{B}
    state::AppliedCommandState
    buffer::B
end

function _copy_applied_command!(state::AppliedCommandState, buffer)
    data = buffer_data(buffer)
    extent = sizeof(Float32) * length(state.values)
    capacity(data) >= extent || throw(PipeWireHILError(
        "simulator_applied_command", "negotiated buffer cannot hold the complete adopted command",
    ))
    copyto!(reinterpret(Float32, buffer_memory(data, extent)), state.values)
    set_chunk!(data; offset=0, size=extent, stride=sizeof(Float32))
    set_buffer_size!(buffer, length(state.values))
    initialize_metadata!(buffer)
    set_reference_timestamp!(buffer,reference_timestamp_now(state.reference_clock))
    set_buffer_header!(buffer, BufferHeader(0, 0, state.pts, 0, state.sequence))
    return nothing
end

function (process::AppliedCommandProcess)(stream)
    state = process.state
    state.failed[] && return nothing
    Base.Threads.atomic_cas!(state.busy, false, true) && return nothing
    try
        state.pending || return nothing
        dequeue_buffer!(process.buffer, stream) || return nothing
        queued = false
        try
            _copy_applied_command!(state, process.buffer)
            queue_buffer!(process.buffer, stream)
            queued = true
            state.pending = false
        finally
            queued || return_buffer!(process.buffer, stream)
        end
    catch
        # An observer failure must never poison a mandatory HIL callback or
        # exchange. This source stops producing until it is prepared again.
        state.failed[] = true
        state.pending = false
    finally
        state.busy[] = false
    end
    return nothing
end

function _applied_command_properties(name, additional, contract; driver=false)
    properties = _stream_properties(name; driver, header_pts_clock="monotonic")
    properties["media.name"] = contract.media_name
    properties["media.schema"] = contract.schema
    properties["node.virtual"] = "true"
    if additional !== nothing
        for (key, value) in additional
            haskey(properties, key) && throw(ArgumentError("applied command property overrides transport: $key"))
            properties[key] = value
        end
    end
    return properties
end

function _applied_command_contract(configuration, shape::Tuple)
    length(shape) == 1 || throw(PipeWireHILError(
        "simulator_applied_command", "adopted commands must be a vector"))
    schema = configuration.command_schema
    expected_scale = if schema == Schemas.DEMANDED_PDM_COMMAND_V1
        1.0f-6
    elseif schema in (Schemas.ACTUATOR_COMMAND_OPD_M_V1, Schemas.TIP_TILT_OPD_COEFFICIENTS_M_V1)
        1.0f0
    else
        throw(PipeWireHILError("simulator_applied_command",
            "optional observation requires a declared physical-DM or tip/tilt OPD command schema"))
    end
    configuration.command_scale == expected_scale || throw(PipeWireHILError(
        "simulator_applied_command", "command scale must convert the declared OPD units to metres"))
    if schema == Schemas.TIP_TILT_OPD_COEFFICIENTS_M_V1
        shape == (2,) || throw(PipeWireHILError(
            "simulator_applied_command", "tip/tilt OPD commands require two normalized-pupil XY coefficients"))
        return (schema=schema, media_name="tip-tilt-applied", units=:metre_opd, command_scale=1.0f0)
    end
    return (schema=APPLIED_PDM_COMMAND_SCHEMA, media_name="dm-applied",
        units=:micrometre_opd, command_scale=_APPLIED_PDM_COMMAND_SCALE_M)
end

_applied_command_contracts(configuration, command::Array{Float32}) =
    _applied_command_contract(configuration, size(command))
function _applied_command_contracts(configuration, commands::NamedTuple{Keys}) where {Keys}
    return NamedTuple{Keys}(map(Keys) do key
        _applied_command_contract(_command_port_configuration(configuration, key), size(commands[key]))
    end)
end

function _prepare_applied_command_source(core, configuration, command, node_name, buffers, additional, contract)
    node_name === nothing && return nothing
    name = _nonempty_string(node_name, "applied_command_node_name")
    name in (configuration.frame_node_name, _command_values_tuple(configuration.command_node_name)...) &&
        throw(PipeWireHILError("applied_command_node_name", "must differ from the mandatory endpoints"))
    format = _format(command, configuration.rate)
    return _prepare_applied_command_stream(core, name, format, buffers, additional, contract)
end

function _prepare_applied_command_stream(core, name, format, buffers, additional, contract; driver=false)
    state = AppliedCommandState(only(format.shape))
    process = AppliedCommandProcess(state, StreamBuffer())
    properties = _applied_command_properties(name, additional, contract; driver)
    stream = Stream(core, name; properties, on_process=process)
    try
        precompile(process, (typeof(stream),))
        flags = STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT | STREAM_NO_CONVERT
        driver && (flags |= STREAM_DRIVER)
        connect!(stream, :output; flags,
            params=_stream_parameters(format,contract.schema;buffers,metadata=(MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP;required=true),)),
        )
        return PreparedAppliedCommandSource(stream, state, name, format, contract.schema,
            contract.media_name, contract.units, contract.command_scale)
    catch
        close(stream)
        rethrow()
    end
end

_set_applied_command_active!(::Nothing, ::Bool) = nothing
function _set_applied_command_active!(source::PreparedAppliedCommandSource, active::Bool)
    try
        set_active!(source.stream, active)
    catch
        source.state.failed[] = true
    end
    return nothing
end

_reset_applied_command!(::Nothing) = nothing
function _reset_applied_command!(source::PreparedAppliedCommandSource)
    try
        flush!(source.stream)
    catch
        source.state.failed[] = true
    end
    return _reset_applied_command!(source.state)
end
function _reset_applied_command!(state::AppliedCommandState)
    # Reset is serialized with the exchange owner and holds the native loop
    # lock, so no producer or callback can own the snapshot here.
    state.sequence = UInt64(0)
    state.pts = Int64(0)
    state.pending = false
    state.dropped[] = UInt64(0)
    fill!(state.values, 0.0f0)
    return nothing
end

_applied_command_resource(::Any) = nothing
_applied_command_resource(prepared::PreparedPipeWireHIL) =
    _applied_source_streams(prepared.applied_command_source)

"""
    applied_command_source_info(prepared) -> Union{Nothing, NamedTuple}

Return the prepared optional source declaration for the session owner, before
any model frame is admitted. Physical-DM payloads use micrometre OPD, converted
from the adopted metre-OPD plant command. Tip/tilt payloads retain the two
normalized-pupil XY coefficients in metre OPD and their catalogue schema.
Preparation requires a known input schema, its declared shape, and its matching
transport-to-plant scale.
The source is independent of the mandatory exchange and needs an observation
consumer clock. Absent or slow consumers lose observations without gating HIL.
"""
function applied_command_source_info(prepared::PreparedPipeWireHIL)
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    source = prepared.applied_command_source
    source === nothing && return nothing
    return _applied_command_source_info(source)
end

function _applied_command_source_info(source::PreparedAppliedCommandSource)
    return (role="simulator-applied-command", node_name=source.node_name,
        port_name="output_1", media_name=source.media_name, schema=source.schema,
        format=source.format, shape=Tuple(Int.(source.format.shape)),
        command_scale=source.command_scale, units=source.units)
end

"""
    with_applied_command_stream(f, prepared)

Access the optional stream under the endpoint loop lock for bounded preparation
or session registration. Return `nothing` when disabled. Do not retain the
stream beyond its prepared lifetime or hold the lock across simulation or waits.
"""
function with_applied_command_stream(f::F, prepared::PreparedPipeWireHIL) where {F}
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    source = prepared.applied_command_source
    source === nothing && return nothing
    return _with_loop_access(prepared) do
        f(_applied_source_streams(source))
    end
end

# The call order is deliberately explicit: rejected adoption publishes nothing.
function _adopt_received_command!(adopt!::F, boundary, state::ExchangeState, source, sequence::UInt64) where {F}
    sequence != _NO_SEQUENCE && sequence == state.command_ready[] ==
        state.timing_completed_sequence[] == state.timing_source_sequence[] || throw(PipeWireHILError(
        "correction_command.sequence", "adoption does not match the received command",
    ))
    adopt!(boundary, sequence)
    _stage_applied_command!(source, state, sequence)
    state.command_ready[] = _NO_SEQUENCE
    return nothing
end

function _adopt_received_command!(adopt!::F, boundary, states::NamedTuple, source, sequence::UInt64) where {F}
    for state in values(states)
        sequence != _NO_SEQUENCE && sequence == state.command_ready[] ==
            state.timing_completed_sequence[] == state.timing_source_sequence[] || throw(PipeWireHILError(
                "correction_command.sequence", "adoption requires every same-sequence command port"))
    end
    adopt!(boundary, sequence)
    _stage_applied_command!(source, states, sequence)
    for state in values(states)
        state.command_ready[] = _NO_SEQUENCE
    end
    return nothing
end
_stage_applied_command!(::Nothing, ::NamedTuple, ::UInt64) = false

_applied_source_streams(::Nothing) = nothing
_applied_source_streams(source::PreparedAppliedCommandSource) = source.stream
_applied_source_streams(sources::NamedTuple) = map(_applied_source_streams, sources)
_applied_command_source_info(sources::NamedTuple) = map(_applied_command_source_info, sources)

function _command_port_configuration(configuration, key)
    return PipeWireHILConfiguration(; remote=configuration.remote,
        frame_node_name=configuration.frame_node_name,
        command_node_name=configuration.command_node_name[key],
        frame_schema=configuration.frame_schema, command_schema=configuration.command_schema[key],
        rate=configuration.rate, timeout_ns=configuration.timeout_ns,
        exposure_duration_ns=configuration.exposure_duration_ns,
        frame_encoding=configuration.frame_encoding, command_scale=configuration.command_scale[key])
end
function _prepare_applied_command_source(core, configuration, commands::NamedTuple{Keys}, names,
        buffers, additional, contracts) where {Keys}
    names === nothing && return nothing
    names isa NamedTuple && Set(keys(names)) == Set(Keys) || throw(PipeWireHILError(
        "applied_command_node_name", "must name every command port"))
    checked = map(name -> _nonempty_string(name, "applied_command_node_name"), names)
    length(unique(values(checked))) == length(Keys) || throw(PipeWireHILError(
        "applied_command_node_name", "observation node names must be distinct"))
    mandatory = (configuration.frame_node_name, values(configuration.command_node_name)...)
    any(name -> name in mandatory, values(checked)) && throw(PipeWireHILError(
        "applied_command_node_name", "must differ from every mandatory endpoint"))
    created = ()
    try
        for key in Keys
            created = (created..., _prepare_applied_command_source(core,
                _command_port_configuration(configuration, key), commands[key], checked[key], buffers, additional, contracts[key]))
        end
        return NamedTuple{Keys}(created)
    catch exception
        errors = Any[exception]
        for source in reverse(created)
            _attempt_cleanup!(() -> close(source.stream), errors)
        end
        length(errors) == 1 && rethrow()
        throw(CompositeException(errors))
    end
end
function _stage_applied_command!(sources::NamedTuple, states::NamedTuple, sequence::UInt64)
    for key in keys(sources)
        _stage_applied_command!(sources[key], states[key], sequence)
    end
    return nothing
end
function _set_applied_command_active!(sources::NamedTuple, active::Bool)
    foreach(source -> _set_applied_command_active!(source, active), values(sources))
    return nothing
end
function _reset_applied_command!(sources::NamedTuple)
    foreach(_reset_applied_command!, values(sources))
    return nothing
end
