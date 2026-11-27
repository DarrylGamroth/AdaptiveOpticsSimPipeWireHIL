"""Load portable resolved AOS simulations without an RTC runtime."""
module Simulation

using AdaptiveOpticsSim.AlgorithmGraphs
using AdaptiveOpticsSim.Backends: HostComputeDevice
using FITSIO: FITS
using PipeWireAO: SPA
using SHA: sha256
import YAML
using ..AdaptiveOpticsSimPipeWireHIL:
    SimRunner,
    PipeWireHILConfiguration,
    PipeWireHILError,
    prepare_pipewire_hil,
    exchange_frame!,
    start!

public AbstractConfiguration, Configuration, OpticalConfiguration, load, prepare, run!, managed_main

"""
    managed_main(args)

Run HIL with the optional PipeWireAORTC extension. A managed process starts
from the package's `src/managed.jl` entry point to publish Preparing before
loading science. A caller that has already loaded HIL can pass its launcher
arguments to this method.
"""
function managed_main end

"Prepare and warm a managed CPU optical configuration without connecting to PipeWire."
function prepare_optical_startup end

"Prepare and warm a sealed managed CPU lockstep configuration without connecting to PipeWire."
function prepare_lockstep_startup end

abstract type AbstractConfiguration end

const MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
const MAX_BINDING_BYTES = 256 * 1024 * 1024
const MAX_RETAINED_BYTES = 512 * 1024 * 1024

"""Owned graph YAML and exact logical binding arrays validated by [`load`](@ref).

This schema-1 lockstep record preserves the original API. Numerical preparation
is separate and opens no PipeWire core. Arrays are copied into each prepared graph.
"""
struct Configuration{Bindings,Command,Wire} <: AbstractConfiguration
    graph::String
    bindings::Bindings
    command_input::Command
    frame_output::Symbol
    period_ns::UInt64
    pipewire::Wire
end

failure(message) = throw(PipeWireHILError("simulation", message))

fields(value, allowed, required, label) = failure("$label must be a mapping")
function fields(value::AbstractDict, allowed, required, label)
    all(key -> key in allowed, keys(value)) || failure("unknown $label field")
    all(key -> haskey(value, key), required) || failure("missing $label field")
    return value
end

text(value, label) = failure("$label must be nonempty text")
function text(value::AbstractString, label)
    !isempty(value) && !contains(value, '\0') || failure("invalid $label")
    return String(value)
end

integer(value, label, limit) = failure("$label must be a positive integer")
integer(value::Bool, label, limit) = failure("$label must be an integer, not Bool")
function integer(value::Integer, label, limit)
    0 < value <= limit || failure("$label is outside 1:$limit")
    return value
end

function identifier(value, label)
    name = text(value, label)
    Base.isidentifier(name) || failure("$label must be a Julia identifier")
    return Symbol(name)
end

function mapping(constructor, node)
    # Check explicit keys before YAML merge-key flattening, which can otherwise
    # hide repeated explicit keys. Merge defaults remain ordinary YAML behavior.
    names = Set{String}()
    for (key_node, _) in node.value
        key_node.tag == "tag:yaml.org,2002:merge" && continue
        key = YAML.construct_object(constructor, key_node)
        key = text(key, "YAML mapping key")
        key in names && failure("duplicate YAML mapping key '$key'")
        push!(names, key)
    end
    return YAML.construct_mapping(
        Dict{String,Any}, constructor, node; strict_unique_keys = true
    )
end

function document(contents::String)
    constructors = Dict("tag:yaml.org,2002:map" => mapping)
    documents = collect(YAML.load_all(contents, constructors))
    length(documents) == 1 || failure("expected exactly one YAML document")
    root = only(documents)
    bounded(root, Ref(131072), 0)
    return root
end

bounded(value, remaining, depth) = nothing
function bounded(values::Union{AbstractDict,AbstractVector}, remaining, depth)
    depth <= 32 || failure("YAML nesting exceeds 32 levels")
    remaining[] -= length(values)
    remaining[] >= 0 || failure("YAML collection exceeds 131072 entries")
    for value in values
        bounded(value, remaining, depth + 1)
    end
    return nothing
end
bounded(value::Pair, remaining, depth) = bounded(last(value), remaining, depth)

function snapshot(path, limit)
    isfile(path) || failure("missing file: $path")
    stat(path).size <= limit || failure("file exceeds $limit bytes: $path")
    bytes = open(path) do io
        return read(io, limit + 1)
    end
    length(bytes) <= limit || failure("file exceeds $limit bytes: $path")
    return bytes
end

function asset(directory, entry, label, limit)
    path = text(entry["path"], "$label path")
    !isabspath(path) && !occursin('\\', path) ||
        failure("$label path must be portable and relative")
    path = normpath(joinpath(directory, path))
    isfile(path) || failure("missing $label file: $path")
    resolved = realpath(path)
    startswith(resolved, directory * "/") || failure("$label path escapes bundle directory")
    digest = text(entry["sha256"], "$label sha256")
    occursin(r"^[0-9a-f]{64}$", digest) ||
        failure("$label sha256 must be lowercase hexadecimal")
    bytes = snapshot(resolved, limit)
    bytes2hex(sha256(bytes)) == digest || failure("$label sha256 mismatch")
    return bytes
end

binding_type(value) = failure("unsupported binding eltype: $value")
binding_type(::Val{:Float32}) = Float32
binding_type(::Val{:Float64}) = Float64
binding_type(::Val{:UInt8}) = UInt8
binding_type(::Val{:UInt16}) = UInt16
binding_type(::Val{:UInt32}) = UInt32
binding_type(::Val{:Int32}) = Int32
binding_type(::Val{:Bool}) = Bool

function binding(directory, entry, retained)
    fields(
        entry,
        ("path", "sha256", "shape", "eltype"),
        ("path", "sha256", "shape", "eltype"),
        "binding",
    )
    type = binding_type(Val(Symbol(text(entry["eltype"], "binding eltype"))))
    shape = dimensions(entry["shape"])
    count = 1
    for dimension in shape
        dimension <= MAX_BINDING_BYTES ÷ sizeof(type) ÷ count ||
            failure("binding exceeds 256 MiB")
        count *= dimension
    end
    retained[] += count * sizeof(type)
    retained[] <= MAX_RETAINED_BYTES || failure("bindings exceed 512 MiB")
    bytes = asset(directory, entry, "binding", MAX_BINDING_BYTES + 1024 * 1024)
    # FITSIO requires a seekable file. Read the verified snapshot, never reopen
    # the source after hashing; this temporary file owns no bundle state.
    return mktemp() do path, io
        write(io, bytes)
        close(io)
        FITS(path) do fits
            length(fits) == 1 || failure("binding must contain exactly one FITS HDU")
            hdu = fits[1]
            size(hdu) == shape || failure("binding FITS shape differs from declaration")
            return read_binding(hdu, type)
        end
    end
end

function read_binding(hdu, ::Type{T}) where {T}
    eltype(hdu) === T || failure("binding FITS eltype differs from declaration")
    return read(hdu)
end
function read_binding(hdu, ::Type{Bool})
    eltype(hdu) === UInt8 || failure("Bool binding requires UInt8 FITS storage")
    values = read(hdu)
    all(value -> value in (0x00, 0x01), values) ||
        failure("Bool binding contains values other than 0 and 1")
    return map(Bool, values)
end

dimensions(value) = failure("binding shape must be an array")
function dimensions(values::AbstractVector)
    1 <= length(values) <= 16 || failure("binding rank must be in 1:16")
    return Tuple(
        Int(integer(value, "binding dimension", MAX_BINDING_BYTES)) for value in values
    )
end

commands(value) = identifier(value, "command_input")
function commands(values::AbstractVector)
    1 <= length(values) <= 16 || failure("command_input requires 1:16 names")
    names = Tuple(identifier(value, "command_input") for value in values)
    length(unique(names)) == length(names) || failure("duplicate command_input")
    return names
end

named(value) = value
function named(value::AbstractDict)
    names = Tuple(sort!(collect(keys(value))))
    symbols = Tuple(identifier(name, "command field") for name in names)
    return NamedTuple{symbols}(Tuple(value[name] for name in names))
end

function wire(entry)
    required = (
        "remote",
        "frame_node_name",
        "command_node_name",
        "frame_schema",
        "command_schema",
        "rate",
        "exposure_duration_ns",
    )
    fields(
        entry,
        (required..., "timeout_ns", "frame_encoding", "command_scale"),
        required,
        "pipewire",
    )
    rate = fraction(entry["rate"])
    exposure = integer(
        entry["exposure_duration_ns"], "exposure_duration_ns", typemax(UInt64)
    )
    timeout = integer(
        get(entry, "timeout_ns", 5_000_000_000), "timeout_ns", typemax(UInt64)
    )
    return PipeWireHILConfiguration(;
        remote = text(entry["remote"], "remote"),
        frame_node_name = text(entry["frame_node_name"], "frame_node_name"),
        command_node_name = named(entry["command_node_name"]),
        frame_schema = text(entry["frame_schema"], "frame_schema"),
        command_schema = named(entry["command_schema"]),
        rate,
        timeout_ns = timeout,
        exposure_duration_ns = exposure,
        frame_encoding = Symbol(
            text(get(entry, "frame_encoding", "float32"), "frame_encoding")
        ),
        command_scale = named(get(entry, "command_scale", 1.0f0)),
    )
end

fraction(value) = failure("rate must be [numerator, denominator]")
function fraction(values::AbstractVector)
    length(values) == 2 || return fraction(nothing)
    numerator, denominator = map(
        value -> UInt32(integer(value, "rate", typemax(UInt32))), values
    )
    return SPA.Fraction(numerator, denominator)
end

"""
    load(path)::AbstractConfiguration

Load resolved AOS graph YAML and exact typed FITS binding arrays. Schema 1
retains the lockstep API; schema 2 selects external-pupil or clocked optics.
Graph/binding paths are relative to the manifest directory and remain inside it
when symlinks resolve. SHA-256, file/array bounds, shape and element type are
checked before any native connection. FITS HDU 1 uses its logical dimensions
without permutation. Bool uses exact UInt8 codes 0 and 1 in FITS; other types
require exact readback without conversion. Schema 2 also retains declared,
hash-verified compiled model assets without interpreting their numerical grammar.
Unresolved instrument authoring is unsupported.
"""
function load(path::AbstractString)
    path = realpath(path)
    directory = dirname(path)
    root = document(String(snapshot(path, MAX_DOCUMENT_BYTES)))
    return load_version(configuration_version(root), directory, root)
end

configuration_version(root) = failure("simulation must be a mapping")
function configuration_version(root::AbstractDict)
    version = integer(get(root, "schema_version", nothing), "schema_version", 2)
    return Val(Int(version))
end

load_version(version, directory, root) = failure("unsupported simulation schema_version")
function load_version(::Val{1}, directory, root)
    required = ("schema_version", "graph", "hil", "pipewire")
    fields(root, (required..., "mode"), required, "simulation")
    integer(root["schema_version"], "schema_version", 1)
    get(root, "mode", "lockstep") == "lockstep" ||
        failure("only lockstep mode is supported")
    graph = fields(
        root["graph"],
        ("path", "sha256", "bindings"),
        ("path", "sha256", "bindings"),
        "graph",
    )
    contents = String(asset(directory, graph, "graph", MAX_DOCUMENT_BYTES))
    validate_graph(document(contents))
    entries = graph["bindings"]
    bindings = load_bindings(directory, entries)
    hil = fields(
        root["hil"],
        ("command_input", "frame_output", "period_ns"),
        ("command_input", "frame_output", "period_ns"),
        "hil",
    )
    command = commands(hil["command_input"])
    frame = identifier(hil["frame_output"], "frame_output")
    period = UInt64(integer(hil["period_ns"], "period_ns", typemax(Int64)))
    configuration = wire(root["pipewire"])
    validate_commands(command, configuration.command_node_name)
    return Configuration(contents, bindings, command, frame, period, configuration)
end

load_bindings(directory, entries, retained = Ref(0)) = failure("bindings must be a mapping")
function load_bindings(directory, entries::AbstractDict, retained = Ref(0))
    length(entries) <= 64 || failure("at most 64 bindings are supported")
    names = Tuple(sort!(collect(keys(entries))))
    symbols = Tuple(identifier(name, "binding name") for name in names)
    arrays = Tuple(binding(directory, entries[name], retained) for name in names)
    return NamedTuple{symbols}(arrays)
end

function validate_graph(graph; compiled = false)
    fields(
        graph,
        (
            "schema_version",
            "name",
            "nodes",
            "links",
            "delayed_links",
            "inputs",
            "outputs",
            "parameters",
        ),
        ("schema_version", "name", "nodes"),
        "AOS graph",
    )
    for node in graph["nodes"]
        !compiled && get(node, "type", nothing) in
        ("compiled_optical_paths_f32", "compiled_optical_paths_f64") && failure(
            "compiled optical-model assets are not supported by simulation schema version 1",
        )
    end
    return nothing
end

validate_commands(command::Symbol, names::AbstractString) = nothing
function validate_commands(commands::Tuple, names::NamedTuple)
    Set(commands) == Set(keys(names)) || failure("HIL and PipeWire command names differ")
    return nothing
end
validate_commands(command, names) = failure("HIL and PipeWire command forms differ")

validate_period(config, period) = nothing
function validate_period(config::AlgorithmGraphs.SecondOrderResponseNodeConfig, period)
    round(UInt64, config.sample_period_s * 1e9) == period ||
        failure("response sample period differs from the model period")
    return nothing
end
function validate_period(config::AlgorithmGraphs.MultiLayerAtmosphereOPDNodeConfig, period)
    round(UInt64, Float64(config.atmosphere_step) * 1e9) == period ||
        failure("atmosphere step differs from the model period")
    return nothing
end

"""
    prepare(configuration; target=HostComputeDevice(),
        execution=StreamGraphExecution(), node_types=builtin_graph_node_types())

Prepare owned numerical arrays, an AOS graph/HIL boundary and model-time driver.
This opens no core and creates no session authority. Returns `definition`,
`graph`, `boundary`, `driver`, and the native transport `configuration`.
"""
function prepare(
    configuration::Configuration;
    target = HostComputeDevice(),
    execution = StreamGraphExecution(),
    node_types = builtin_graph_node_types(),
)
    bindings = map(copy, configuration.bindings)
    definition = load_algorithm_graph(document(configuration.graph); bindings, node_types)
    foreach(node -> validate_period(node.config, configuration.period_ns), definition.nodes)
    graph = prepare_algorithm_graph(definition; target, execution)
    boundary = prepare_graph_hil_boundary(
        graph;
        command_input = configuration.command_input,
        frame_output = configuration.frame_output,
    )
    driver = FixedStepModelTimeDriver(
        PeriodicSchedule(; period_ns = configuration.period_ns)
    )
    return (; definition, graph, boundary, driver, configuration = configuration.pipewire)
end

"""
    run!(control, prepared; frames, idle_strategy)

Open and close HIL transport around at most `frames` exchanges. The caller's
`control(prepared, transport)::Bool` first admits the inactive endpoints, then
runs before each exchange: `true` proceeds, `false` stops. The caller owns
links and admission; configured transport timeouts bound exchange waits.
Caller-owned link proxies must use a separate connection so they cannot block
closure of the HIL connection.
The optional `idle_strategy` overrides the enclosing simulation policy or the
`PIPEWIREAO_SIM_IDLE_STRATEGY` setting.
No acquisition identity is fabricated for peers that cannot preserve it.
"""
function run!(control, prepared; frames::Integer, idle_strategy = SimRunner.configured_strategy())
    count = integer(frames, "frames", typemax(Int))
    admitted = Ref(false)
    completed = Ref(0)
    transport = prepare_pipewire_hil(prepared.boundary, prepared.configuration)
    try
        SimRunner.run!(idle_strategy) do
            if !admitted[]
                control(prepared, transport)::Bool || return nothing
                start!(transport)
                admitted[] = true
            end
            completed[] == count && return nothing
            control(prepared, transport)::Bool || return nothing
            exchange_frame!(transport, prepared.driver; idle_strategy)
            completed[] += 1
            return 1
        end
        return completed[]
    finally
        close(transport)
    end
end

include("simulation_optical.jl")

end
