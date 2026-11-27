"""
    validate_hil_system(system; command_input, frame_output, command_shape, frame_shape)

Validate that a prepared HIL system's boundary is bound to the named graph
ports and owns Float32 command and Float32 or UInt16 frame host buffers with
the required shapes. This check does not prepare or step the system and does
not mutate it.
"""
function validate_hil_system(
    system;
    command_input,
    frame_output::Symbol,
    command_shape,
    frame_shape::Tuple,
)
    hasproperty(system, :graph) && hasproperty(system, :boundary) ||
        throw(ArgumentError("HIL system must provide graph and boundary fields"))
    graph = getproperty(system, :graph)
    boundary = getproperty(system, :boundary)
    hasproperty(boundary, :graph) ||
        throw(ArgumentError("HIL boundary must provide its prepared graph"))
    getproperty(boundary, :graph) === graph ||
        throw(ArgumentError("HIL boundary is not bound to system.graph"))

    _validate_hil_command_binding(boundary, graph, command_input, command_shape)
    boundary.frame_output === AlgorithmGraphs.graph_output(graph, frame_output) ||
        throw(ArgumentError("HIL boundary frame output is not bound to graph output :$frame_output"))

    frame = hil_frame_buffer(boundary)
    size(frame) == frame_shape || throw(ArgumentError(
        "HIL frame buffer shape $(size(frame)) does not match required shape $frame_shape",
    ))
    eltype(frame) in (Float32, UInt16) || throw(ArgumentError(
        "HIL frame buffer element type $(eltype(frame)) is not Float32 or UInt16",
    ))
    return nothing
end

function _validate_hil_command_shape(command, shape::Tuple)
    size(command) == shape || throw(ArgumentError(
        "HIL command buffer shape $(size(command)) does not match required shape $shape"))
    eltype(command) === Float32 || throw(ArgumentError(
        "HIL command buffer element type $(eltype(command)) is not Float32"))
    return nothing
end
function _validate_hil_command_binding(boundary, graph, name::Symbol, shape::Tuple)
    boundary.command_input === AlgorithmGraphs.graph_input(graph, name) || throw(ArgumentError(
        "HIL boundary command input is not bound to graph input :$name"))
    return _validate_hil_command_shape(hil_command_buffer(boundary), shape)
end
function _validate_hil_command_binding(boundary, graph, names::Tuple{Vararg{Symbol}}, shapes::NamedTuple)
    commands = hil_command_buffer(boundary)
    commands isa NamedTuple && boundary.command_input isa NamedTuple &&
        1 <= length(names) <= 16 && length(unique(names)) == length(names) &&
        Set(names) == Set(keys(shapes)) == Set(keys(commands)) == Set(keys(boundary.command_input)) ||
        throw(ArgumentError("HIL command inputs, buffer names and required shape names must match"))
    for name in names
        boundary.command_input[name] === AlgorithmGraphs.graph_input(graph, name) || throw(ArgumentError(
            "HIL boundary command input :$name is not bound to the corresponding graph input"))
        _validate_hil_command_shape(commands[name], shapes[name])
    end
    return nothing
end
_validate_hil_command_binding(boundary, graph, names, shapes) =
    throw(ArgumentError("HIL command inputs require a Symbol and shape Tuple or named command ports and shapes"))

"""
    validate_wire_contract(frame_encoding, frame_schema, command_schema,
        command_scale, allowed_contracts)

Check for an exact match against one of the supplied instrument wire
contracts. The command scale is compared after conversion to finite positive
Float32, matching the transport configuration representation.
"""
function validate_wire_contract(
    frame_encoding::Symbol,
    frame_schema::Union{Nothing,AbstractString},
    command_schema,
    command_scale,
    allowed_contracts::Tuple,
)
    scale = _wire_command_scale(command_schema, command_scale)

    for contract in allowed_contracts
        contract.frame_encoding === frame_encoding || continue
        contract.frame_schema == frame_schema || continue
        _wire_command_equal(contract.command_schema, command_schema) || continue
        _wire_command_equal(_wire_command_scale(contract.command_schema, contract.command_scale), scale) || continue
        return nothing
    end
    throw(ArgumentError("wire format and schemas do not match a supported contract"))
end

function _wire_command_scale(schema::Union{Nothing,AbstractString}, value::Real)
    value isa Bool && throw(ArgumentError("wire command scale must not be Bool"))
    scale = Float32(value)
    isfinite(scale) && scale > 0 || throw(ArgumentError(
        "wire command scale must be finite and positive when represented as Float32"))
    return scale
end
function _wire_command_scale(schemas::NamedTuple, scales::NamedTuple)
    Set(keys(schemas)) == Set(keys(scales)) && 1 <= length(schemas) <= 16 ||
        throw(ArgumentError("wire command schema and scale names must match"))
    return NamedTuple{keys(schemas)}(map(key -> _wire_command_scale(schemas[key], scales[key]), keys(schemas)))
end
_wire_command_scale(schemas, scales) = throw(ArgumentError(
    "wire commands require scalar schemas/scales or matching named schemas/scales"))
_wire_command_equal(a, b) = a == b
function _wire_command_equal(a::NamedTuple, b::NamedTuple)
    return Set(keys(a)) == Set(keys(b)) && all(key -> a[key] == b[key], keys(a))
end
