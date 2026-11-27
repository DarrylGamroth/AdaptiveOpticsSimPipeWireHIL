const _CHANNELS_PROPERTY = "pipewireao.ndarray.channels"

function _channel_text(value, field)
    value isa AbstractString && isvalid(value) && !isempty(strip(value)) &&
        ncodeunits(value) <= 255 && !any(iscntrl, value) ||
        throw(ArgumentError("$field must be valid UTF-8, nonempty, without controls and at most 255 bytes"))
    return String(value)
end

function _image_channels(channels, shape)
    channels isa Tuple && length(channels) <= 64 ||
        throw(ArgumentError("image channels must be a tuple of at most 64 declarations"))
    names = Set{String}()
    indices = Set{Int}()
    extent = Base.checked_mul(shape...)
    return map(channels) do channel
        channel isa NamedTuple && Set(keys(channel)) == Set((:name, :index, :title, :units)) ||
            throw(ArgumentError("image channel requires name, index, title and units"))
        name = _channel_text(channel.name, "channel name")
        occursin(r"^[a-z][a-z0-9_]*$", name) ||
            throw(ArgumentError("channel name must use lowercase ASCII letters, digits and underscores"))
        index = channel.index
        index isa Integer && !(index isa Bool) && 0 <= index < extent ||
            throw(ArgumentError("channel index must be a zero-based scalar position within the image"))
        name ∉ names && index ∉ indices ||
            throw(ArgumentError("channel names and indices must be unique"))
        push!(names, name)
        push!(indices, Int(index))
        title = _channel_text(channel.title, "channel title")
        units = isnothing(channel.units) ? nothing : _channel_text(channel.units, "channel units")
        (name=name, index=Int(index), title=title, units=units)
    end
end

# Text is bounded, valid UTF-8 and control-free before encoding. No JSON runtime
# dependency or per-frame encoding is needed for this immutable description.
_channel_json_string(io, value) =
    print(io, '"', replace(value, "\\" => "\\\\", "\"" => "\\\""), '"')

function _image_channel_description(schema, format, channels)
    isempty(channels) && return nothing
    _channel_text(schema, "channel schema")
    io = IOBuffer()
    print(io, "{\"version\":1,\"outputs\":[{\"port\":\"output_1\",\"schema\":")
    _channel_json_string(io, schema)
    print(io, ",\"element_type\":", UInt32(format.element_type),
        ",\"layout\":", UInt32(format.layout), ",\"shape\":[")
    join(io, format.shape, ',')
    print(io, "],\"channels\":[")
    for (ordinal, channel) in enumerate(channels)
        ordinal > 1 && print(io, ',')
        print(io, "{\"name\":")
        _channel_json_string(io, channel.name)
        print(io, ",\"index\":", channel.index, ",\"title\":")
        _channel_json_string(io, channel.title)
        print(io, ",\"units\":")
        isnothing(channel.units) ? print(io, "null") : _channel_json_string(io, channel.units)
        print(io, '}')
        position(io) <= 16384 || throw(ArgumentError("image channels exceeds 16384 UTF-8 bytes"))
    end
    print(io, "]}]}")
    position(io) <= 16384 || throw(ArgumentError("image channels exceeds 16384 UTF-8 bytes"))
    return String(take!(io))
end
