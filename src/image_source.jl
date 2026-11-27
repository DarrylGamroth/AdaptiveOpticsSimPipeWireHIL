"""
    PipeWireImageSourceOptions(; node_name, media_name, schema, shape,
        rate=nothing, buffers=2, properties=nothing, channels=())

Declare one optional complete rank-two Float32 ndarray source. The optical
owner supplies the schema, shape and completed host image; this adapter assigns
no optical meaning or units. `rate=nothing` uses the HIL configuration rate.
Additional properties cannot override transport properties.
`channels` is an immutable tuple of owner-declared scalar descriptions with
`name`, zero-based row-major `index`, `title`, and `units` (String or `nothing`).
The source publishes them as immutable Node information; they change no values.
"""
struct PipeWireImageSourceOptions
    node_name::String
    media_name::String
    schema::String
    shape::Tuple{Int,Int}
    rate::Union{Nothing,SPA.Fraction}
    buffers::Int
    properties::Dict{String,String}
    channels::Tuple
    reference_clock::MetadataClock
end

function PipeWireImageSourceOptions(;
    node_name, media_name, schema, shape, rate=nothing, buffers=2, properties=nothing,
    channels=(), reference_clock::MetadataClock=METADATA_CLOCK_REALTIME,
)
    shape isa Tuple{Integer,Integer} && all(>(0), shape) &&
        all(value -> !(value isa Bool) && value <= typemax(Int), shape) ||
        throw(PipeWireHILError("image.shape", "must contain two positive integer extents"))
    rate === nothing || rate isa SPA.Fraction || throw(PipeWireHILError(
        "image.rate", "must be nothing or an SPA.Fraction",
    ))
    rate === nothing || (rate.num > 0 && rate.denom > 0) || throw(PipeWireHILError(
        "image.rate", "must be positive",
    ))
    name = _nonempty_string(node_name, "image.node_name")
    media = _nonempty_string(media_name, "image.media_name")
    schema_name = _nonempty_string(schema, "image.schema")
    extra = properties === nothing ? Dict{String,String}() : Dict{String,String}(properties)
    _image_properties(name, media, schema_name, extra)
    return PipeWireImageSourceOptions(name, media, schema_name, Int.(shape), rate,
        _frame_buffer_count(buffers; field="image.buffers"), extra, _image_channels(channels, shape),reference_clock)
end

# Each participant claims the one owned snapshot once. A pending image may be
# replaced, but no participant waits for an observer or borrows simulator storage.
mutable struct ImageSourceState
    image::Matrix{Float32}
    busy::Atomic{Bool}
    sequence::UInt64
    pts::Int64
    domain::AcquisitionDomain
    generation::UInt64
    exposure_duration_ns::UInt64
    pending::Bool
    dropped::Atomic{UInt64}
    failed::Atomic{Bool}
    reference_clock::MetadataClock
end

function ImageSourceState(shape::Tuple{Int,Int}, domain::AcquisitionDomain, duration::UInt64)
    return ImageSourceState(zeros(Float32, shape), Atomic{Bool}(false), UInt64(0),
        Int64(0), domain, UInt64(0), duration, false, Atomic{UInt64}(0), Atomic{Bool}(false),METADATA_CLOCK_REALTIME)
end

struct PreparedImageSource{S<:Stream}
    stream::S
    state::ImageSourceState
    options::PipeWireImageSourceOptions
    format::NdArrayFormat{2}
end

# All optional image streams share one independent native loop and connection.
# Image callback residence therefore cannot occupy the mandatory WFS loop.
struct ImageSourceOwner{ContextType<:Context,CoreType<:CoreConnection,Sources<:Tuple}
    loop::ThreadLoop
    context::ContextType
    core::CoreType
    sources::Sources
end

function _with_image_loop_access(f::F, owner::ImageSourceOwner) where {F}
    if isrunning(owner.loop)
        return with_thread_loop_lock(owner.loop) do _
            f()
        end
    end
    return f()
end

function _stage_image!(state::ImageSourceState, image::Matrix{Float32},
    sequence::UInt64, pts::Int64, generation::UInt64)
    if state.failed[] || Base.Threads.atomic_cas!(state.busy, false, true)
        Base.Threads.atomic_add!(state.dropped, UInt64(1))
        return false
    end
    try
        if size(image) != size(state.image) || sequence == 0 || pts < 0 || generation == 0
            Base.Threads.atomic_add!(state.dropped, UInt64(1))
            return false
        end
        state.pending && Base.Threads.atomic_add!(state.dropped, UInt64(1))
        copyto!(state.image, image)
        state.sequence = sequence
        state.pts = pts
        state.generation = generation
        state.pending = true
        return true
    finally
        state.busy[] = false
    end
end

struct ImageProcess{B}
    state::ImageSourceState
    buffer::B
end

function _copy_image!(state::ImageSourceState, buffer)
    data = buffer_data(buffer)
    extent = sizeof(Float32) * length(state.image)
    capacity(data) >= extent || throw(PipeWireHILError(
        "image", "negotiated buffer cannot hold the complete image",
    ))
    destination = reinterpret(Float32, buffer_memory(data, extent))
    # A Julia matrix is column-major; the public ndarray contract is row-major.
    index = 1
    for row in axes(state.image, 1), column in axes(state.image, 2)
        destination[index] = state.image[row, column]
        index += 1
    end
    set_chunk!(data; offset=0, size=extent, stride=sizeof(Float32) * size(state.image, 2))
    set_buffer_size!(buffer, length(state.image))
    metadata = initialize_metadata!(buffer)
    set_buffer_header!(buffer, BufferHeader(0, 0, state.pts, 0, state.sequence))
    set_acquisition_identity!(metadata,
        AcquisitionIdentity(state.domain, state.generation, state.sequence))
    set_exposure_timing!(metadata,ExposureTiming(;duration_nanoseconds=state.exposure_duration_ns))
    set_reference_timestamp!(metadata,reference_timestamp_now(state.reference_clock))
    return nothing
end

function (process::ImageProcess)(stream)
    state = process.state
    state.failed[] && return nothing
    Base.Threads.atomic_cas!(state.busy, false, true) && return nothing
    try
        state.pending || return nothing
        dequeue_buffer!(process.buffer, stream) || return nothing
        queued = false
        try
            _copy_image!(state, process.buffer)
            queue_buffer!(process.buffer, stream)
            queued = true
            state.pending = false
        finally
            queued || return_buffer!(process.buffer, stream)
        end
    catch
        state.failed[] = true
        state.pending = false
    finally
        state.busy[] = false
    end
    return nothing
end

function _image_properties(name, media_name, schema, additional)
    properties = _stream_properties(name)
    properties["media.name"] = media_name
    properties["media.schema"] = schema
    properties["node.virtual"] = "true"
    for (key, value) in additional
        if _header_pts_property(String(key),String(value))
            properties[key]=value
            continue
        end
        (haskey(properties, key) || key == _CHANNELS_PROPERTY) &&
            throw(ArgumentError("image property overrides transport: $key"))
        properties[key] = value
    end
    return properties
end

function _prepare_image_source(core, configuration, state, options::PipeWireImageSourceOptions)
    format = NdArrayFormat(NdArray.F32_LE, options.shape; layout=NdArray.ROW_MAJOR,
        rate=options.rate === nothing ? configuration.rate : options.rate)
    description = _image_channel_description(options.schema, format, options.channels)
    observation = ImageSourceState(options.shape, state.acquisition_domain, state.exposure_duration_ns)
    # An inactive prepared owner rejects public staging before reaching this
    # success path. Warm the exact production leaf on owned host storage, then
    # erase the synthetic identity before any native stream can publish it.
    _stage_image!(observation, observation.image, UInt64(1), Int64(0), UInt64(1)) ||
        throw(PipeWireHILError("image", "cold snapshot staging did not succeed"))
    _reset_image!(observation)
    observation.reference_clock=options.reference_clock
    process = ImageProcess(observation, StreamBuffer())
    properties = _image_properties(options.node_name, options.media_name, options.schema, options.properties)
    description === nothing || (properties[_CHANNELS_PROPERTY] = description)
    stream = Stream(core, options.node_name; properties, on_process=process)
    try
        precompile(process, (typeof(stream),))
        connect!(stream, :output;
            flags=STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT | STREAM_NO_CONVERT,
            params=_stream_parameters(format, options.schema; buffers=options.buffers))
        return PreparedImageSource(stream, observation, options, format)
    catch
        close(stream)
        rethrow()
    end
end

function _prepare_image_sources(core, configuration, state, options::Tuple, applied_name)
    names = Set((configuration.frame_node_name, _command_values_tuple(configuration.command_node_name)...))
    applied_name === nothing || union!(names, _command_values_tuple(applied_name))
    for option in options
        option isa PipeWireImageSourceOptions || throw(PipeWireHILError(
            "image_sources", "must be a tuple of PipeWireImageSourceOptions"))
        option.node_name in names && throw(PipeWireHILError(
            "image.node_name", "must differ from all other endpoint names"))
        push!(names, option.node_name)
    end
    sources = PreparedImageSource[]
    try
        for option in options
            push!(sources, _prepare_image_source(core, configuration, state, option))
        end
        return Tuple(sources)
    catch exception
        errors = Any[exception]
        for source in reverse(sources)
            _attempt_cleanup!(() -> close(source.stream), errors)
        end
        length(errors) == 1 && rethrow()
        throw(CompositeException(errors))
    end
end

function _prepare_image_source_owner(configuration, state, options::Tuple, applied_name)
    isempty(options) && return nothing
    loop = context = core = nothing
    sources = ()
    try
        # Linux comm admits 15 bytes; keep the image loop distinguishable from
        # the mandatory WFS loop for deployment-owned CPU placement.
        loop = ThreadLoop("AOS-HIL-images")
        context = Context(loop;
            properties=Dict("application.name" => "AOS optical observer"))
        core = CoreConnection(context; properties=Dict("remote.name" => configuration.remote))
        sources = _prepare_image_sources(core, configuration, state, options, applied_name)
        owner = ImageSourceOwner(loop, context, core, sources)
        start!(loop)
        for source in sources
            started = time_ns()
            while true
                id = _with_image_loop_access(owner) do
                    node_id(source.stream)
                end
                id != _INVALID_NODE_ID && break
                time_ns() - started <= configuration.timeout_ns || throw(PipeWireHILError(
                    "image.node_name", "PipeWire node did not become inspectable before the development timeout"))
                sleep(0.001)
            end
        end
        return owner
    catch exception
        errors = _close_pipewire_resources!(loop, context, core, nothing, nothing,
            nothing, map(source -> source.stream, sources))
        isempty(errors) || throw(CompositeException(Any[exception; errors]))
        rethrow()
    end
end

function _set_images_active!(sources::Tuple, active::Bool)
    for source in sources
        try
            set_active!(source.stream, active)
        catch
            source.state.failed[] = true
        end
    end
    return nothing
end

_set_image_owner_active!(::Nothing, ::Bool) = nothing
function _set_image_owner_active!(owner::ImageSourceOwner, active::Bool)
    try
        _with_image_loop_access(owner) do
            _set_images_active!(owner.sources, active)
        end
    catch
        for source in owner.sources
            source.state.failed[] = true
        end
    end
    return nothing
end

function _reset_image!(state::ImageSourceState)
    # Reset runs before streams exist, or with the producer serialized and the
    # independent image loop lock held.
    state.sequence = UInt64(0)
    state.pts = Int64(0)
    state.generation = UInt64(0)
    state.pending = false
    state.dropped[] = UInt64(0)
    fill!(state.image, 0.0f0)
    return nothing
end

function _reset_images!(sources::Tuple)
    for source in sources
        try
            flush!(source.stream)
        catch
            source.state.failed[] = true
        end
        _reset_image!(source.state)
    end
    return nothing
end

_reset_image_owner!(::Nothing) = nothing
function _reset_image_owner!(owner::ImageSourceOwner)
    try
        _with_image_loop_access(owner) do
            _reset_images!(owner.sources)
        end
    catch
        for source in owner.sources
            source.state.failed[] = true
        end
    end
    return nothing
end

_image_owner(::Any) = nothing
_image_owner(prepared::PreparedPipeWireHIL) = prepared.image_owner

_close_image_owner!(::Nothing) = Any[]
function _close_image_owner!(owner::ImageSourceOwner)
    return _close_pipewire_resources!(owner.loop, owner.context, owner.core, nothing, nothing,
        nothing, map(source -> source.stream, owner.sources))
end

_image_resources(::Any) = ()
function _image_resources(prepared::PreparedPipeWireHIL)
    owner = prepared.image_owner
    owner === nothing && return ()
    return (map(source -> source.stream, owner.sources)..., owner.core, owner.context, owner.loop)
end

"""
    stage_pipewire_image!(prepared, index, image, sequence, pts, generation) -> Bool

Copy one complete `Matrix{Float32}` into optional source `index`. The caller
supplies the originating frame's sequence, original WFS Header PTS and captured
acquisition generation. A stale generation, invalid image or unavailable snapshot
returns `false`; no observer can gate HIL. A new snapshot replaces a pending one.
The sole simulation owner serializes staging with reset, stop and close, and does
not mutate the image during this call. No loop lock or observer wait is taken.
"""
function stage_pipewire_image!(prepared::PreparedPipeWireHIL, index::Int,
    image::Matrix{Float32}, sequence::UInt64, pts::Int64, generation::UInt64)
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    source = prepared.image_sources[index]
    if !prepared.started || generation != _frame_state(prepared.state).acquisition_generation
        Base.Threads.atomic_add!(source.state.dropped, UInt64(1))
        return false
    end
    return _stage_image!(source.state, image, sequence, pts, generation)
end

"""
    image_source_info(prepared) -> Tuple

Return prepared declarations for independent observation session registration.
The owner supplies an observation consumer clock. Image streams are ordinary
non-driver sources; missing or slow observers lose snapshots without gating HIL.
"""
function image_source_info(prepared::PreparedPipeWireHIL)
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    return map(Tuple(enumerate(prepared.image_sources))) do (index, source)
        (index=index, role="simulator-image", node_name=source.options.node_name,
            port_name="output_1", media_name=source.options.media_name,
            schema=source.options.schema, format=source.format, shape=source.options.shape)
    end
end

"Access one optional image stream under its independent image loop lock for bounded control work."
function with_image_stream(f::F, prepared::PreparedPipeWireHIL, index::Int) where {F}
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    source = prepared.image_sources[index]
    return _with_image_loop_access(prepared.image_owner) do
        f(source.stream)
    end
end

"""
    reset_pipewire_image_sources!(prepared) -> prepared

Flush all optional image source pools and clear their owned snapshots under the
image loop lock. The simulation owner serializes this cold lifecycle call
with staging. It preserves the mandatory WFS acquisition generation and exchange
state, and can discard synthetic snapshots used for callback warmup before the
first model frame. A failed optional source remains disabled until reconstruction.
"""
function reset_pipewire_image_sources!(prepared::PreparedPipeWireHIL)
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    _reset_image_owner!(prepared.image_owner)
    return prepared
end
