"""A configuration or runtime failure at the AOS–PipeWire HIL boundary."""
struct PipeWireHILError <: Exception
    field::String
    message::String
end

function Base.showerror(io::IO, error::PipeWireHILError)
    print(io, error.field, ": ", error.message)
end

function _nonempty_string(value::AbstractString, field::AbstractString)
    text = String(value)
    isempty(text) && throw(PipeWireHILError(String(field), "must not be empty"))
    contains(text, '\0') && throw(PipeWireHILError(String(field), "must not contain NUL"))
    return text
end

"""
    PipeWireHILConfiguration(; remote, frame_node_name, command_node_name,
        frame_schema, command_schema, rate, timeout_ns=5_000_000_000,
        exposure_duration_ns, frame_encoding=:float32, command_scale=1.0f0)

Configure one non-actuating AOS HIL frame source and its command sinks. `remote` names the private
PipeWire core. Schemas identify the complete WFS frame and correction command.
For named AOS command inputs, `command_node_name`, `command_schema` and
`command_scale` are matching NamedTuples with 1:16 ports; configuration field
order is irrelevant. Scalar values retain the single-command contract.
The positive `timeout_ns` bounds control-path waits; it is not a real-time
deadline or performance claim.
`frame_encoding` selects `:float32` or `:uint16` transport for a Float32 or UInt16
AOS frame buffer. Native UInt16 codes are copied exactly to UInt16 transport.
Float32 raw ADC codes sent as UInt16 are rounded to nearest
(ties to even), rejecting values outside 0:65535. `command_scale` is a fixed,
finite positive Float32 multiplier from incoming command units to plant units;
use `1.0f-6` for micrometre OPD commands and metre OPD plant inputs.
"""
struct PipeWireHILConfiguration{Names,Schemas,Scales}
    remote::String
    frame_node_name::String
    command_node_name::Names
    frame_schema::String
    command_schema::Schemas
    rate::SPA.Fraction
    timeout_ns::UInt64
    exposure_duration_ns::UInt64
    frame_encoding::Symbol
    command_scale::Scales

    function PipeWireHILConfiguration(;
        remote::AbstractString,
        frame_node_name::AbstractString,
        command_node_name,
        frame_schema::AbstractString,
        command_schema,
        rate::SPA.Fraction,
        timeout_ns::Integer=5_000_000_000,
        exposure_duration_ns::Integer,
        frame_encoding::Symbol=:float32,
        command_scale=1.0f0,
    )
        frame_name = _nonempty_string(frame_node_name, "frame_node_name")
        command_name, schemas, scale = _command_configuration(command_node_name, command_schema, command_scale)
        frame_name in _command_values_tuple(command_name) && throw(PipeWireHILError(
            "command_node_name",
            "must differ from frame_node_name",
        ))
        timeout_ns > 0 || throw(PipeWireHILError("timeout_ns", "must be positive"))
        timeout_ns <= typemax(UInt64) || throw(PipeWireHILError(
            "timeout_ns",
            "exceeds UInt64 range",
        ))
        exposure_duration_ns > 0 || throw(PipeWireHILError(
            "exposure_duration_ns",
            "must be positive",
        ))
        exposure_duration_ns <= typemax(UInt64) || throw(PipeWireHILError(
            "exposure_duration_ns",
            "exceeds UInt64 range",
        ))
        rate.num > 0 && rate.denom > 0 || throw(PipeWireHILError(
            "rate",
            "must be positive",
        ))
        frame_encoding in (:float32, :uint16) || throw(PipeWireHILError(
            "frame_encoding",
            "must be :float32 or :uint16",
        ))
        return new{typeof(command_name),typeof(schemas),typeof(scale)}(
            _nonempty_string(remote, "remote"),
            frame_name,
            command_name,
            _nonempty_string(frame_schema, "frame_schema"),
            schemas,
            rate,
            UInt64(timeout_ns),
            UInt64(exposure_duration_ns),
            frame_encoding,
            scale,
        )
    end
end

_command_values_tuple(value) = (value,)
_command_values_tuple(value::NamedTuple) = values(value)

function _command_scale(value::Real)
    value isa Bool && throw(PipeWireHILError("command_scale", "must be a real scale, not Bool"))
    scale = Float32(value)
    isfinite(scale) && scale > 0 || throw(PipeWireHILError(
        "command_scale", "must be finite and positive when represented as Float32"))
    return scale
end

function _command_configuration(name::AbstractString, schema::AbstractString, scale::Real)
    return (_nonempty_string(name, "command_node_name"),
        _nonempty_string(schema, "command_schema"), _command_scale(scale))
end

function _command_configuration(names::NamedTuple{Keys}, schemas::NamedTuple,
        scales::NamedTuple) where {Keys}
    1 <= length(Keys) <= 16 || throw(PipeWireHILError(
        "command_node_name", "a command set requires 1:16 named ports"))
    Set(keys(schemas)) == Set(Keys) && Set(keys(scales)) == Set(Keys) || throw(PipeWireHILError(
        "correction_command", "command configuration names must match"))
    ordered_schemas = NamedTuple{Keys}(Tuple(schemas[key] for key in Keys))
    ordered_scales = NamedTuple{Keys}(Tuple(scales[key] for key in Keys))
    checked = map(name -> _nonempty_string(name, "command_node_name"), names)
    length(unique(values(checked))) == length(Keys) || throw(PipeWireHILError(
        "command_node_name", "command endpoint names must be distinct"))
    return checked, map(schema -> _nonempty_string(schema, "command_schema"), ordered_schemas),
        map(_command_scale, ordered_scales)
end

_command_configuration(names, schemas, scales) = throw(PipeWireHILError(
    "correction_command", "node names, schemas and scales must be scalars or matching NamedTuples"))
