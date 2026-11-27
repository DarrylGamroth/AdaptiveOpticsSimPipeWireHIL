const _COMMAND_RECEIVER_LIBRARY = joinpath(@__DIR__, "..", "deps", "libhil_command_receiver.so")
const _COMMAND_METADATA = (SPA.META_REFERENCE_TIMESTAMP, SPA.META_ACQUISITION_IDENTITY,
    SPA.META_EXPOSURE_TIMING, SPA.META_OPTICAL_CHOPPING, SPA.META_ELECTRICAL_TRIGGER)

# Layout shared with deps/command_receiver.c. A zero clock, generation or
# exposure duration represents an unavailable optional fact.
struct _NativeCommand
    pts::Int64
    sequence::UInt64
    duration::UInt64
    generation::UInt64
    acquisition_sequence::UInt64
    stamp::UInt64
    stamp_duration::UInt64
    lost::UInt64
    received::UInt64
    clock::UInt32
    domain::NTuple{16,UInt8}
end

mutable struct _CommandReceiver{C<:CoreConnection}
    const core::C
    handle::Ptr{Cvoid}
    stream::Ptr{PipeWireAO.LibPipeWire.pw_stream}
    record::Base.RefValue{_NativeCommand}
    status::Base.RefValue{NTuple{5,UInt64}}
end

function _CommandReceiver(core::CoreConnection, option::PipeWireArrayStreamOptions, properties, active::Bool)
    option.format.element_type == NdArray.F32_LE && length(option.format.shape) == 1 ||
        throw(ArgumentError("a synchronous command receiver requires a Float32 vector"))
    all(r -> r.type in _COMMAND_METADATA, option.metadata) ||
        throw(ArgumentError("unsupported synchronous command metadata"))
    isfile(_COMMAND_RECEIVER_LIBRARY) ||
        error("synchronous HIL command receiver is missing. Run Pkg.build(\"AdaptiveOpticsSimPipeWireHIL\").")
    requirements = PipeWireAO.LibPipeWire.spa_meta_requirement[
        PipeWireAO._native_requirement(r) for r in option.metadata]
    params, pointers = PipeWireAO._stream_params(_stream_parameters(
        option.format, option.schema; buffers=option.buffers, metadata=option.metadata))
    core_handle = PipeWireAO._retain_stream(core)
    handle = try
        native_properties = PipeWireAO._owned_native_properties(properties)
        receiver = GC.@preserve requirements params pointers ccall(
            (:hil_command_receiver_new, _COMMAND_RECEIVER_LIBRARY), Ptr{Cvoid},
            (Ptr{Cvoid}, Cstring, Ptr{Cvoid}, Ptr{Ptr{Cvoid}}, UInt32, UInt32, Cuchar,
                Ptr{PipeWireAO.LibPipeWire.spa_meta_requirement}, UInt32),
            core_handle, option.node_name, native_properties,
            pointer(pointers), length(pointers), payload_size(option.format), active,
            pointer(requirements), length(requirements))
        Base.systemerror("create synchronous HIL command receiver", receiver == C_NULL)
        receiver
    catch
        PipeWireAO._release_stream(core)
        rethrow()
    end
    stream = ccall((:hil_command_receiver_stream, _COMMAND_RECEIVER_LIBRARY),
        Ptr{PipeWireAO.LibPipeWire.pw_stream}, (Ptr{Cvoid},), handle)
    return _CommandReceiver(core, handle, stream, Ref{_NativeCommand}(), Ref{NTuple{5,UInt64}}())
end

function _command_stream(receiver::_CommandReceiver)
    isopen(receiver) || throw(InvalidStateException("the command receiver is closed", :closed))
    return receiver.stream
end

PipeWireAO.node_id(receiver::_CommandReceiver) =
    PipeWireAO.LibPipeWire.pw_stream_get_node_id(_command_stream(receiver))
PipeWireAO.stream_state(receiver::_CommandReceiver) =
    PipeWireAO.LibPipeWire.pw_stream_get_state(_command_stream(receiver), C_NULL)

function PipeWireAO.set_active!(receiver::_CommandReceiver, active::Bool=true)
    result = PipeWireAO.LibPipeWire.pw_stream_set_active(_command_stream(receiver), active)
    PipeWireAO._check_result(:pw_stream_set_active, result)
    return receiver
end

Base.isopen(receiver::_CommandReceiver) = receiver.handle != C_NULL

function Base.close(receiver::_CommandReceiver)
    isopen(receiver) || return nothing
    @ccall gc_safe=true _COMMAND_RECEIVER_LIBRARY.hil_command_receiver_free(
        receiver.handle::Ptr{Cvoid},
    )::Cvoid
    receiver.handle = C_NULL
    receiver.stream = C_NULL
    PipeWireAO._release_stream(receiver.core)
    return nothing
end

function _command_status(receiver::_CommandReceiver)
    ccall((:hil_command_receiver_status, _COMMAND_RECEIVER_LIBRARY), Cvoid,
        (Ptr{Cvoid}, Ref{NTuple{5,UInt64}}), receiver.handle, receiver.status)
    pending, dropped, gaps, rejected, failed = receiver.status[]
    return (; pending=!iszero(pending), dropped, gaps, rejected,
        failed=!iszero(failed) ||
            stream_state(receiver) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_ERROR)
end

function _drain_command!(destination::Vector{Float32}, receiver::_CommandReceiver)
    result = ccall((:hil_command_receiver_drain, _COMMAND_RECEIVER_LIBRARY), Cuchar,
        (Ptr{Cvoid}, Ptr{Float32}, Ref{_NativeCommand}),
        receiver.handle, destination, receiver.record)
    iszero(result) && return nothing
    record = receiver.record[]
    identity = iszero(record.generation) ? nothing :
        AcquisitionIdentity(AcquisitionDomain(record.domain), record.generation,
            record.acquisition_sequence)
    reference = iszero(record.clock) ? nothing : ReferenceTimestamp(record.stamp,
        MetadataClock(record.clock), record.stamp_duration != typemax(UInt64),
        record.stamp_duration)
    duration = iszero(record.duration) ? nothing : record.duration
    return PipeWireArrayReceipt(record.pts, record.sequence, duration, identity, record.lost,
        ACQUISITION_TRIGGER_ABSENT, false, nothing, reference)
end

function _reset_command!(receiver::_CommandReceiver, identity, check_only)
    domain = Ref(identity === nothing ? ntuple(_ -> UInt8(0), 16) : identity.domain.bytes)
    generation = identity === nothing ? UInt64(0) : identity.generation
    if !check_only
        result = PipeWireAO.LibPipeWire.pw_stream_flush(_command_stream(receiver), false)
        PipeWireAO._check_result(:pw_stream_flush, result)
    end
    result = ccall((:hil_command_receiver_reset, _COMMAND_RECEIVER_LIBRARY), Cint,
        (Ptr{Cvoid}, UInt64, Ref{NTuple{16,UInt8}}, Cuchar),
        receiver.handle, generation, domain, check_only)
    result == -Base.Libc.EINVAL && throw(ArgumentError("reset generation must increase"))
    PipeWireAO._check_result(:hil_command_receiver_reset, result)
    return nothing
end
