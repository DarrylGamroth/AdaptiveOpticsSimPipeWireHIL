"""
    PipeWireArrayStreamOptions(; node_name, media_name, schema, format,
        properties=(), buffers=2,
        metadata=(), reference_clock=METADATA_CLOCK_REALTIME)

Immutable, optically agnostic complete-array declaration. `format` must be a
row-major Float32 or UInt16 `NdArrayFormat`; Julia host arrays retain their usual axes.
The caller owns schema, units, plane order, coordinates and Header PTS meaning,
and publishes those descriptions in string-valued `properties`. Optional typed
per-buffer facts use `metadata`;
these are independent of cold string properties and Header publication identity.
"""
struct PipeWireArrayStreamOptions{N,P<:Tuple,R<:Tuple}
    node_name::String
    media_name::String
    schema::String
    format::NdArrayFormat{N}
    properties::P
    buffers::Int
    metadata::R
    reference_clock::MetadataClock
end
function PipeWireArrayStreamOptions(; node_name, media_name, schema, format::NdArrayFormat,
    properties=(), buffers=2,
    metadata::Tuple=(), reference_clock::MetadataClock=METADATA_CLOCK_REALTIME)
    all(item -> item isa MetadataRequirement,metadata) || throw(ArgumentError("invalid metadata declaration"))
    validate_metadata_requirements(metadata)
    format.layout == NdArray.ROW_MAJOR || throw(ArgumentError("array streams require row-major layout"))
    _array_element_type(Val(format.element_type))
    Base.ENDIAN_BOM == 0x04030201 || throw(ArgumentError("array streams require a little-endian host"))
    payload_size(format) <= typemax(Int32) || throw(ArgumentError("array payload exceeds SPA buffer-size range"))
    descriptions = properties isa NamedTuple ? pairs(properties) : pairs(Dict(properties))
    extra = Tuple(String(key) => String(value) for (key, value) in descriptions)
    reserved = _stream_properties("array"; driver=true)
    for (key, value) in extra
        _header_pts_property(key,value) && continue
        (haskey(reserved, key) || key in ("media.name", "media.schema", "node.virtual")) &&
            throw(ArgumentError("array property overrides transport: $key"))
    end
    return PipeWireArrayStreamOptions(_nonempty_string(node_name, "array.node_name"),
        _nonempty_string(media_name, "array.media_name"), _nonempty_string(schema, "array.schema"),
        format, extra, _frame_buffer_count(buffers; field="array.buffers"),
        metadata, reference_clock)
end

# Map host element types to the corresponding ndarray wire types.
_wire_element_type(::Type{Float32}) = NdArray.F32_LE
_wire_element_type(::Type{UInt16}) = NdArray.U16_LE

_array_element_type(::Val{NdArray.F32_LE}) = Float32
_array_element_type(::Val{NdArray.U16_LE}) = UInt16
_array_element_type(::Val) = throw(ArgumentError("array streams require Float32 or UInt16"))

"Immutable metadata for one copied input. `lost_samples` is cumulative since reset."
struct PipeWireArrayReceipt
    pts::Int64
    sequence::UInt64
    duration::Union{Nothing,UInt64}
    identity::Union{Nothing,AcquisitionIdentity}
    lost_samples::UInt64
    trigger_state::AcquisitionTriggerState
    has_trigger_classification::Bool
    optical_classification::Union{Nothing,OpticalClassification}
    reference_timestamp::Union{Nothing,ReferenceTimestamp}
end
PipeWireArrayReceipt(pts,duration,identity,lost_samples,trigger,has_trigger,optical,stamp::Union{Nothing,ReferenceTimestamp}) =
    PipeWireArrayReceipt(pts,identity===nothing ? UInt64(0) : identity.sequence,duration,identity,lost_samples,trigger,has_trigger,optical,stamp)
PipeWireArrayReceipt(pts,duration,identity,lost_samples,trigger,has_trigger,optical) =
    PipeWireArrayReceipt(pts,duration,identity,lost_samples,trigger,has_trigger,optical,nothing)
PipeWireArrayReceipt(pts, duration, identity, lost_samples) =
    PipeWireArrayReceipt(pts, duration, identity, lost_samples, ACQUISITION_TRIGGER_ABSENT, true, nothing)
PipeWireArrayReceipt(pts, duration, identity, lost_samples, trigger_state) =
    PipeWireArrayReceipt(pts, duration, identity, lost_samples, trigger_state, true, nothing)
PipeWireArrayReceipt(pts, duration, identity, lost_samples, trigger_state, optical_classification) =
    PipeWireArrayReceipt(pts, duration, identity, lost_samples, trigger_state, true, optical_classification)

# The callback mutates one prepared slot. Public receipts are constructed only
# when the serialized owner drains the copied payload.
mutable struct _ArrayReceipt
    pts::Int64
    sequence::UInt64
    duration::Union{Nothing,UInt64}
    identity::Union{Nothing,AcquisitionIdentity}
    lost_samples::UInt64
    trigger_state::AcquisitionTriggerState
    has_trigger_classification::Bool
    optical_classification::Union{Nothing,OpticalClassification}
    reference_timestamp::Union{Nothing,ReferenceTimestamp}
end
_ArrayReceipt()=_ArrayReceipt(0,0,nothing,nothing,0,ACQUISITION_TRIGGER_ABSENT,false,nothing,nothing)
function _set_array_receipt!(receipt,pts,sequence,duration,identity,lost_samples,trigger,has_trigger,optical,stamp)
    receipt.pts=pts
    receipt.sequence=sequence
    receipt.duration=duration
    receipt.identity=identity
    receipt.lost_samples=lost_samples
    receipt.trigger_state=trigger
    receipt.has_trigger_classification=has_trigger
    receipt.optical_classification=optical
    receipt.reference_timestamp=stamp
    return receipt
end
_clear_array_receipt!(receipt)=_set_array_receipt!(receipt,0,0,nothing,nothing,0,ACQUISITION_TRIGGER_ABSENT,false,nothing,nothing)

# One owned row-major slot; sole caller serializes stage/drain and lifecycle.
# The callback and caller only try the slot once and never wait for each other.
mutable struct ArrayStreamState{T}
    values::Vector{T}
    busy::Atomic{Bool}
    pending::Bool
    receipt::_ArrayReceipt
    identity::Union{Nothing,AcquisitionIdentity}
    publication_sequence::UInt64
    dropped::Atomic{UInt64}
    gaps::Atomic{UInt64}
    rejected::Atomic{UInt64}
    failed::Atomic{Bool}
end
function ArrayStreamState(count::Int, ::Type{T}) where {T}
    return ArrayStreamState(
        zeros(T, count), Atomic{Bool}(false), false, _ArrayReceipt(), nothing, UInt64(0),
        Atomic{UInt64}(0), Atomic{UInt64}(0), Atomic{UInt64}(0), Atomic{Bool}(false),
    )
end

function _array_identity_valid(state,identity,pts,duration,sequence=identity===nothing ? UInt64(0) : identity.sequence)
    pts isa Int64 && (pts>=0 || pts==typemin(Int64)) || return false
    duration===nothing || duration isa UInt64 && duration>0 || return false
    sequence isa UInt64 && sequence>0 || return false
    if identity===nothing
        return sequence>state.publication_sequence
    end
    identity isa AcquisitionIdentity && identity.sequence>0 && identity.generation>0 || return false
    previous=state.identity
    return previous===nothing || (identity.domain==previous.domain &&
        (identity.generation>previous.generation ||
         (identity.generation==previous.generation && identity.sequence>previous.sequence)))
end
_array_requires(metadata,type)=any(r -> r.required && r.type==type,metadata)

function _accept_array!(state, values, identity, pts, duration, trigger_state=ACQUISITION_TRIGGER_ABSENT,
    optical_classification=nothing, has_trigger_classification::Bool=true,reference_timestamp::Union{Nothing,ReferenceTimestamp}=nothing,
    sequence::UInt64=identity===nothing ? UInt64(0) : identity.sequence)
    _array_identity_valid(state, identity, pts, duration,sequence) && length(values) == length(state.values) || begin
        Base.Threads.atomic_add!(state.rejected, UInt64(1))
        return false
    end
    # Only the serialized native input callback writes this arrival identity.
    # Observe every valid arrival even when the owner holds the copy slot, so
    # contention and missing wire sequences are counted separately and exactly.
    previous = state.identity
    if identity!==nothing && previous !== nothing && previous.generation == identity.generation && previous.sequence > 0
        Base.Threads.atomic_add!(state.gaps, identity.sequence - previous.sequence - UInt64(1))
    end
    if identity===nothing && state.publication_sequence>0
        Base.Threads.atomic_add!(state.gaps,sequence-state.publication_sequence-UInt64(1))
    end
    state.identity = identity
    state.publication_sequence=sequence
    if Base.Threads.atomic_cas!(state.busy, false, true)
        Base.Threads.atomic_add!(state.dropped, UInt64(1))
        return false
    end
    try
        state.pending && Base.Threads.atomic_add!(state.dropped, UInt64(1))
        copyto!(state.values, values)
        _set_array_receipt!(state.receipt,pts,sequence,duration,identity,state.dropped[] + state.gaps[],trigger_state,has_trigger_classification,optical_classification,reference_timestamp)
        state.pending = true
        return true
    finally
        state.busy[] = false
    end
end

function _pack_array!(values::Vector{T}, array::Array{T,N}) where {T,N}
    index = 1
    for coordinate in CartesianIndices(reverse(size(array)))
        values[index] = array[reverse(Tuple(coordinate))...]
        index += 1
    end
    return values
end
function _unpack_array!(array::Array{T,N}, values::Vector{T}) where {T,N}
    index = 1
    for coordinate in CartesianIndices(reverse(size(array)))
        array[reverse(Tuple(coordinate))...] = values[index]
        index += 1
    end
    return array
end

struct ArrayStreamProcess{B,R<:Tuple,T}
    state::ArrayStreamState{T}
    buffer::B
    input::Bool
    stride::Int
    metadata::R
    reference_clock::MetadataClock
end
ArrayStreamProcess(state, buffer, input, stride) =
    ArrayStreamProcess(state, buffer, input, stride, _ACQUISITION_METADATA, METADATA_CLOCK_REALTIME)
function _write_array!(process)
    state, buffer = process.state, process.buffer
    metadata_fits(buffer,process.metadata) || throw(ArgumentError("array output metadata storage differs from its declaration"))
    receipt = something(state.receipt)
    records=metadata_records(buffer)
    declared(type)=any(r -> r.type==type,process.metadata)
    available(type)=declared(type) && metadata_record(records,type)!==nothing
    write_time=available(SPA.META_REFERENCE_TIMESTAMP)
    data = buffer_data(buffer)
    element = eltype(state.values)
    extent = sizeof(element) * length(state.values)
    copyto!(reinterpret(element, buffer_memory(data, extent)), state.values)
    set_chunk!(data; offset=0, size=extent, stride=process.stride)
    set_buffer_size!(buffer, length(state.values))
    properties = initialize_metadata!(buffer)
    set_buffer_header!(buffer, BufferHeader(0, 0, receipt.pts, 0, receipt.sequence))
    receipt.optical_classification===nothing || !available(SPA.META_OPTICAL_CHOPPING) ||
        set_optical_classification!(properties,receipt.optical_classification)
    receipt.has_trigger_classification && available(SPA.META_ELECTRICAL_TRIGGER) &&
        set_trigger_classification!(properties,receipt.trigger_state)
    receipt.identity===nothing || !available(SPA.META_ACQUISITION_IDENTITY) ||
        set_acquisition_identity!(properties,receipt.identity)
    receipt.duration===nothing || !available(SPA.META_EXPOSURE_TIMING) ||
        set_exposure_timing!(properties,ExposureTiming(;duration_nanoseconds=receipt.duration))
    if write_time
        stamp = receipt.reference_timestamp
        set_reference_timestamp!(properties, stamp === nothing ? reference_timestamp_now(process.reference_clock) : stamp)
    end
    metadata_records_valid(properties, process.metadata) ||
        throw(ArgumentError("published array lacks required metadata records"))
    return nothing
end
function _read_array!(process)
    buffer, state = process.buffer, process.state
    header = buffer_header(buffer)
    properties = metadata_records(buffer)
    data = buffer_data(buffer)
    chunk = chunk_info(data)
    element = eltype(state.values)
    if header === nothing || properties === nothing ||
            !metadata_records_valid(properties, process.metadata) ||
            header.flags & (SPA.META_HEADER_FLAG_CORRUPTED | SPA.META_HEADER_FLAG_GAP | SPA.META_HEADER_FLAG_DISCONT) != 0 ||
            chunk.flags != 0 || chunk.offset != 0 ||
            chunk.size != sizeof(element) * length(state.values) ||
            chunk.stride ∉ (0, sizeof(element), process.stride)
        Base.Threads.atomic_add!(state.rejected, UInt64(1))
        return false
    end
    identity = acquisition_identity(properties)
    identity === nothing && _array_requires(process.metadata,SPA.META_ACQUISITION_IDENTITY) && begin
        Base.Threads.atomic_add!(state.rejected, UInt64(1))
        return false
    end
    duration=_exposure_duration(properties)
    if duration===nothing && _array_requires(process.metadata,SPA.META_EXPOSURE_TIMING)
        Base.Threads.atomic_add!(state.rejected,UInt64(1))
        return false
    end
    # Header sequence identifies this publication; retained acquisitions may differ.
    classification = trigger_classification(properties)
    trigger = something(classification, ACQUISITION_TRIGGER_ABSENT)
    values = reinterpret(element, bytes(data))
    optical=optical_classification(properties)
    stamp=reference_timestamp(properties)
    # Inline admission here so the optional facts do not box at a dynamic call.
    if identity===nothing
        return @inline _accept_array!(state,values,nothing,header.pts,duration,trigger,optical,classification!==nothing,stamp,header.sequence)
    end
    return @inline _accept_array!(state,values,identity,header.pts,duration,trigger,optical,classification!==nothing,stamp,header.sequence)
end
function (process::ArrayStreamProcess)(stream)
    state = process.state
    state.failed[] && return nothing
    locked = false
    if !process.input
        locked = !Base.Threads.atomic_cas!(state.busy, false, true)
        locked || return nothing
        if !state.pending
            state.busy[] = false
            return nothing
        end
    end
    if !dequeue_buffer!(process.buffer, stream)
        locked && (state.busy[] = false)
        return nothing
    end
    queued = false
    try
        if process.input
            _read_array!(process)
        else
            _write_array!(process)
        end
        queue_buffer!(process.buffer, stream)
        queued = true
        process.input || (state.pending = false)
    catch
        state.failed[] = true
        locked && (state.pending = false)
    finally
        locked && (state.busy[] = false)
        queued || return_buffer!(process.buffer, stream)
    end
    return nothing
end

struct PreparedArrayStream{S,O,T}
    stream::S
    state::T
    options::O
    input::Bool
end
mutable struct PipeWireArrayStreams{L,C,K,S}
    loop::L
    context::C
    core::K
    streams::S
    closed::Bool
end
function _array_open(owner)
    owner.closed && throw(InvalidStateException("array streams are closed", :closed))
    return nothing
end
"Access a stream under its shared native loop lock for linking or public `trigger_process!`."
function with_array_stream(f, owner::PipeWireArrayStreams, index::Int)
    _array_open(owner)
    return with_thread_loop_lock(owner.loop) do _
        f(owner.streams[index].stream)
    end
end

"""
    PipeWireAO.trigger_process!(owner::PipeWireArrayStreams)

Drive each streaming output that PipeWire currently elected as a driver.
Connected camera outputs can share one driver even when their samples arrive
independently. Triggering only the stream whose sample was staged can leave a
non-driving camera pending indefinitely. Unstaged source callbacks publish no
sample; this operation does not create frames or change acquisition identity.
"""
function PipeWireAO.trigger_process!(owner::PipeWireArrayStreams)
    _array_open(owner)
    with_thread_loop_lock(owner.loop) do _
        foreach(_trigger_array_output!, owner.streams)
    end
    return nothing
end

function _trigger_array_output!(prepared)
    prepared.input && return nothing
    stream = prepared.stream
    stream_state(stream) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING &&
        is_driving(stream) && trigger_process!(stream)
    return nothing
end

"""
    with_array_streams(f, options::Tuple; remote, input::Tuple, active=true, synchronous_command=false)

Open complete-array streams on one shared loop and call `f(owner)`.
Pass `active=false` to prepare registered streams for later activation.
`input[i]` selects a sink; output streams are explicitly triggered drivers.
With `synchronous_command=true`, the first stream is a Float32 command-vector
input processed by a native synchronous callback. Camera output streams retain
their independent arrival and trigger semantics.
A nonempty explicit remote is required. This function never creates links.
It closes streams, connection, context and loop even when `f` throws.
"""
function with_array_streams(f, options::Tuple; remote, input::Tuple, active::Bool=true,
        synchronous_command::Bool=false)
    remote_name = _nonempty_string(remote, "array.remote")
    length(input) == length(options) > 0 && all(x -> x isa Bool, input) ||
        throw(ArgumentError("input must contain one Bool per array stream"))
    synchronous_command && !input[1] &&
        throw(ArgumentError("a synchronous command receiver requires an input as the first stream"))
    all(x -> x isa PipeWireArrayStreamOptions, options) || throw(ArgumentError("invalid array options"))
    supported=(SPA.META_ACQUISITION_IDENTITY,SPA.META_EXPOSURE_TIMING,SPA.META_REFERENCE_TIMESTAMP,
        SPA.META_OPTICAL_CHOPPING,SPA.META_ELECTRICAL_TRIGGER)
    for (option,isinput) in zip(options,input)
        !isinput && any(r -> r.required && r.type ∉ supported,option.metadata) &&
            throw(ArgumentError("array source cannot produce required metadata"))
    end
    length(Set(option.node_name for option in options)) == length(options) ||
        throw(ArgumentError("array node names must be unique"))
    loop = context = core = owner = nothing
    streams = PreparedArrayStream[]
    try
        loop = ThreadLoop("AOS-arrays")
        context = Context(loop)
        core = CoreConnection(context; properties=Dict("remote.name" => remote_name))
        for (index, (option, isinput)) in enumerate(zip(options, input))
            properties = _stream_properties(option.node_name; driver=!isinput)
            merge!(properties, Dict(option.properties))
            properties["media.name"], properties["media.schema"] = option.media_name, option.schema
            properties["node.virtual"] = "true"
            if synchronous_command && index == 1
                stream = _CommandReceiver(core, option, properties, active)
                push!(streams, PreparedArrayStream(stream, nothing, option, true))
                continue
            end
            element = _array_element_type(Val(option.format.element_type))
            state = ArrayStreamState(prod(Int.(option.format.shape)), element)
            stride = sizeof(element) * Int(last(option.format.shape))
            process = ArrayStreamProcess(state, StreamBuffer(), isinput, stride,
                option.metadata, option.reference_clock)
            stream = Stream(core, option.node_name; properties, on_process=process)
            push!(streams, PreparedArrayStream(stream, state, option, isinput))
            precompile(process, (typeof(stream),))
            flags = STREAM_MAP_BUFFERS | STREAM_DONT_RECONNECT | STREAM_NO_CONVERT
            active || (flags |= STREAM_INACTIVE)
            connect!(stream, isinput ? :input : :output; flags=isinput ? flags : flags | STREAM_DRIVER,
                params=_stream_parameters(option.format, option.schema; buffers=option.buffers,
                    metadata=option.metadata))
        end
        owner = PipeWireArrayStreams(loop, context, core, Tuple(streams), false)
        start!(loop)
        return f(owner)
    finally
        errors = _close_pipewire_resources!(loop, context, core, nothing, nothing, nothing,
            map(item -> item.stream, streams))
        owner === nothing || (owner.closed = true)
        isempty(errors) || throw(CompositeException(errors))
    end
end

"""
    stage_pipewire_array!(owner, index, array; identity, pts, duration,
        trigger_state=ACQUISITION_TRIGGER_ABSENT, optical_classification=nothing) -> Bool

Copy one CPU `Array{Float32}` into the output slot. New values overwrite a
pending complete array. The sole owner serializes calls and lifecycle; it must
not mutate `array` during the copy. Trigger publication with
`PipeWireAO.trigger_process!(owner)` so an independently arriving camera can
use its connected graph's elected driver.
Sequences must increase within a generation. A greater generation in the same
admitted domain starts a new sequence; a changed domain requires explicit reset.
Whole-exposure electrical and optical classifications are independent typed facts.
`has_trigger_classification=false` preserves a missing electrical record when
republishing an input receipt; its trigger state must then be ABSENT.
Optical classification is a separately negotiated record and is never inferred
from an electrical trigger. `nothing` means no optical fact;
`OPTICAL_ABSENT` is an explicitly supplied classification.
"""
function stage_pipewire_array!(owner::PipeWireArrayStreams, index::Int, array::Array{T};
    identity::Union{Nothing,AcquisitionIdentity}=nothing,pts::Int64,duration::Union{Nothing,UInt64}=nothing,
    sequence::UInt64=identity===nothing ? UInt64(0) : identity.sequence,
    trigger_state::AcquisitionTriggerState=ACQUISITION_TRIGGER_ABSENT,
    has_trigger_classification::Bool=true,
    optical_classification::Union{Nothing,OpticalClassification}=nothing,
    reference_timestamp::Union{Nothing,ReferenceTimestamp}=nothing) where {T<:Union{Float32,UInt16}}
    _array_open(owner)
    !has_trigger_classification && trigger_state != ACQUISITION_TRIGGER_ABSENT &&
        throw(ArgumentError("a missing trigger property cannot carry a trigger classification"))
    streams = owner.streams
    1 <= index <= length(streams) || throw(BoundsError(streams,index))
    return _with_array_item(_stage_array!,streams,index,array,identity,pts,duration,sequence,
        trigger_state,has_trigger_classification,optical_classification,reference_timestamp)
end

# Resolve the index while every stream remains concrete. Indexing a mixed
# stream tuple before dispatch boxes its members and their metadata.
@inline function _with_array_item(f::F, streams::Tuple, index::Int, args::Vararg{Any,N}) where {F,N}
    index == 1 && return f(first(streams),args...)
    return _with_array_item(f,Base.tail(streams),index-1,args...)
end

@inline _with_array_item(f, ::Tuple{}, index::Int, args...) = throw(BoundsError((),index))

function _stage_array!(item, array::Array{T}, identity, pts, duration, sequence,
        trigger_state, has_trigger_classification, optical_classification,
        reference_timestamp) where {T}
    item.input && throw(ArgumentError("cannot stage an input stream"))
    if optical_classification === nothing && any(r -> r.required && r.type==SPA.META_OPTICAL_CHOPPING,item.options.metadata)
        throw(ArgumentError("array is missing required optical classification"))
    end
    identity===nothing && _array_requires(item.options.metadata,SPA.META_ACQUISITION_IDENTITY) && throw(ArgumentError("array profile requires acquisition identity"))
    duration===nothing && _array_requires(item.options.metadata,SPA.META_EXPOSURE_TIMING) && throw(ArgumentError("array profile requires exposure duration"))
    !has_trigger_classification && _array_requires(item.options.metadata,SPA.META_ELECTRICAL_TRIGGER) && throw(ArgumentError("array profile requires electrical trigger classification"))
    state = item.state
    size(array) == item.options.format.shape || throw(DimensionMismatch("array shape differs from format"))
    T === eltype(state.values) || throw(ArgumentError("array element type differs from format"))
    state.failed[] && return false
    if Base.Threads.atomic_cas!(state.busy, false, true)
        Base.Threads.atomic_add!(state.dropped, UInt64(1))
        return false
    end
    try
        _array_identity_valid(state, identity, pts, duration,sequence) || begin
            Base.Threads.atomic_add!(state.rejected, UInt64(1))
            return false
        end
        state.pending && Base.Threads.atomic_add!(state.dropped, UInt64(1))
        _pack_array!(state.values, array)
        state.identity = identity
        state.publication_sequence=sequence
        _set_array_receipt!(state.receipt,pts,sequence,duration,identity,state.dropped[],trigger_state,has_trigger_classification,optical_classification,reference_timestamp)
        state.pending = true
        return true
    finally
        state.busy[] = false
    end
end

"Copy the latest input into owner storage, or return `nothing` immediately when unavailable."
function drain_pipewire_array!(
    destination::Array{T}, owner::PipeWireArrayStreams, index::Int
) where {T<:Union{Float32,UInt16}}
    _array_open(owner)
    streams = owner.streams
    1 <= index <= length(streams) || throw(BoundsError(streams,index))
    return _with_array_item(_drain_array_input!,streams,index,destination)
end

function _drain_array_input!(item, destination)
    item.input || throw(ArgumentError("cannot drain an output stream"))
    size(destination) == item.options.format.shape || throw(DimensionMismatch("array shape differs from format"))
    return _drain_array!(destination, item)
end

function _drain_array!(destination::Array{T}, item) where {T}
    if item.state === nothing
        T === Float32 || throw(ArgumentError("array element type differs from format"))
        return _drain_command!(destination, item.stream)
    end
    state = item.state
    T === eltype(state.values) || throw(ArgumentError("array element type differs from format"))
    state.failed[] && return nothing
    Base.Threads.atomic_cas!(state.busy, false, true) && return nothing
    try
        state.pending || return nothing
        _unpack_array!(destination, state.values)
        state.pending = false
        receipt = something(state.receipt)
        return PipeWireArrayReceipt(receipt.pts,receipt.sequence,receipt.duration,receipt.identity,
            state.dropped[] + state.gaps[], receipt.trigger_state, receipt.has_trigger_classification, receipt.optical_classification,receipt.reference_timestamp)
    finally
        state.busy[] = false
    end
end

"Return bounded counters; drops include overwrite/contention, gaps count missing sequence values."
function array_stream_status(owner::PipeWireArrayStreams, index::Int)
    _array_open(owner)
    return with_thread_loop_lock(owner.loop) do _
        _with_array_item(_array_status, owner.streams, index)
    end
end

_array_status(item::PreparedArrayStream{S,O,Nothing}) where {S,O} =
    _command_status(item.stream)

function _array_status(item::PreparedArrayStream)
    state = item.state
    return (pending=state.pending, dropped=state.dropped[], gaps=state.gaps[],
        rejected=state.rejected[], failed=state.failed[])
end

function _array_transport_failed(owner::PipeWireArrayStreams)
    _array_open(owner)
    return with_thread_loop_lock(owner.loop) do _
        any(item -> _array_status(item).failed, owner.streams)
    end
end

_array_transport_failed(owner) =
    any(index -> array_stream_status(owner, index).failed, eachindex(owner.streams))

"""
    reset_pipewire_array!(owner, index; identity=nothing)

Flush one stopped, owner-serialized stream and discard its retained receipt and
counters. An optional positive-generation, sequence-zero identity binds its new
cohort; without one the next valid complete packet establishes domain admission.
Other streams retain their independent identities. Serialize this call against
stage/drain operations and hold the upstream source before clearing admission.
"""
function reset_pipewire_array!(owner::PipeWireArrayStreams, index::Int;
        identity::Union{Nothing,AcquisitionIdentity}=nothing)
    _array_open(owner)
    identity === nothing || identity.sequence == 0 && identity.generation > 0 ||
        throw(ArgumentError("reset identity requires sequence zero and positive generation"))
    with_thread_loop_lock(owner.loop) do _
        item = owner.streams[index]
        _check_array_reset(item, identity)
        _reset_array!(item, identity)
    end
    return owner
end

"""
    reset_pipewire_arrays!(owner; identity)

Flush all pools, discard pending receipts and bind the new domain/generation.
Use sequence zero in `identity`. A generation must increase within a domain;
late buffers from the previous generation are rejected. Input also admits a
strictly newer generation automatically and returns it in the next receipt, so
the caller can invalidate incomplete work. Reconstruct after failure.
"""
function reset_pipewire_arrays!(owner::PipeWireArrayStreams; identity::AcquisitionIdentity)
    _array_open(owner)
    identity.sequence == 0 && identity.generation > 0 || throw(ArgumentError("reset identity requires sequence zero and positive generation"))
    with_thread_loop_lock(owner.loop) do _
        foreach(item -> _check_array_reset(item, identity), owner.streams)
        foreach(item -> _reset_array!(item, identity), owner.streams)
    end
    return owner
end


function _check_array_reset(item, identity)
    state = item.state
    state === nothing && return _reset_command!(item.stream, identity, true)
    previous = state.identity
    identity === nothing || previous === nothing || previous.domain != identity.domain ||
        identity.generation > previous.generation || throw(ArgumentError("reset generation must increase"))
    return nothing
end

function _reset_array!(item, identity)
    state = item.state
    state === nothing && return _reset_command!(item.stream, identity, false)
    # The loop lock excludes ordinary callbacks and the caller serializes its copy.
    state.busy[] && throw(InvalidStateException("array copy is still active", :busy))
    flush!(item.stream)
    state.pending = false
    _clear_array_receipt!(state.receipt)
    state.identity = identity
    state.publication_sequence = 0
    state.dropped[] = state.rejected[] = state.gaps[] = UInt64(0)
    return nothing
end
