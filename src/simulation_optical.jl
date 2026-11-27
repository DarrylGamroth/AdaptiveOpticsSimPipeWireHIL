# Schema 2 composes the public AOS optical and HIL execution contracts. Each
# graph role is prepared independently, even when descriptors share a graph key.
using PipeWireAO: PipeWireAO
using AdaptiveOpticsSim.Backends: AcceleratorComputeDevice, allocate_device_array
using ..AdaptiveOpticsSimPipeWireHIL:
    ExternalPupil, PipeWireArrayStreamOptions, with_array_streams

abstract type AbstractOpticalMode end
struct ExternalPupilMode <: AbstractOpticalMode end
struct ClockedOpticalMode <: AbstractOpticalMode end

"Owned schema-2 optical graph, binding and compiled-asset snapshots."
struct OpticalConfiguration{Mode<:AbstractOpticalMode,G,O,W,S} <: AbstractConfiguration
    mode::Mode
    graphs::G
    optical::O
    pipewire::W
    source::S
end

struct PreparedOptical{S,E,O}
    science::S
    execution::E
    options::O
    remote::String
end

optical_mode(::Val{:external_pupil}) = ExternalPupilMode()
optical_mode(::Val{:clocked_optical}) = ClockedOpticalMode()
optical_mode(value) = failure("unsupported schema-2 simulation mode")

function properties(entry::AbstractDict)
    return Dict{String,String}(
        text(k, "property key") => text(v, "property value") for (k, v) in entry
    )
end
properties(entry) = failure("properties must be a mapping")

function names(entry::AbstractDict, label)
    1 <= length(entry) <= 64 || failure("$label requires 1:64 entries")
    return Tuple(Symbol(text(name, label)) for name in sort!(collect(keys(entry))))
end
names(entry, label) = failure("$label must be a mapping")
function as_named(entry, symbols, operation)
    return NamedTuple{symbols}(Tuple(operation(entry[string(n)]) for n in symbols))
end

function relative_path(value, label)
    path = text(value, label)
    !isabspath(path) &&
    !occursin('\\', path) &&
    all(part -> part != "..", splitpath(path)) ||
        failure("$label must remain relative to bundle")
    return normpath(path)
end

function graph_snapshot(directory, entry, retained, cache)
    fields(
        entry,
        ("path", "sha256", "bindings", "assets"),
        ("path", "sha256", "bindings"),
        "graph",
    )
    bytes = asset(directory, entry, "graph", MAX_DOCUMENT_BYTES)
    retained[] += length(bytes)
    retained[] <= MAX_RETAINED_BYTES || failure("snapshots exceed 512 MiB")
    bindings = load_bindings(directory, entry["bindings"], retained)
    entries = get(entry, "assets", Any[])
    compiled_assets(entries::AbstractVector) = Tuple(
        map(entries) do row
            fields(row, ("path", "sha256"), ("path", "sha256"), "compiled asset")
            path = relative_path(row["path"], "compiled asset path")
            digest = text(row["sha256"], "compiled asset sha256")
            if haskey(cache, path)
                cache[path].sha256 == digest || failure("conflicting compiled asset hash")
                cache[path]
            else
                contents = asset(
                    directory, row, "compiled asset", MAX_RETAINED_BYTES - retained[]
                )
                retained[] += length(contents)
                retained[] <= MAX_RETAINED_BYTES || failure("snapshots exceed 512 MiB")
                cache[path] = (; path, sha256 = digest, bytes = contents)
            end
        end,
    )
    compiled_assets(value) = failure("graph assets must be an array")
    assets = compiled_assets(entries)
    length(unique(a.path for a in assets)) == length(assets) ||
        failure("duplicate compiled asset path")
    contents = String(bytes)
    graph = document(contents)
    validate_graph(graph; compiled = true)
    # The AOS parser remains authoritative for graph and model grammar.
    for node in get(graph, "nodes", Any[])
        if get(node, "type", nothing) in
            ("compiled_optical_paths_f32", "compiled_optical_paths_f64")
            config = node["config"]
            path = relative_path(config["model_path"], "compiled model_path")
            index = findfirst(a -> a.path == path, assets)
            index === nothing &&
                failure("compiled model_path must be declared in graph assets")
            config["model_sha256"] == assets[index].sha256 ||
                failure("compiled model_sha256 differs from declared asset")
        end
    end
    return (; graph = contents, bindings, assets)
end

function pupil(entry)
    fields(
        entry, ("schema", "shape", "properties"), ("schema", "shape", "properties"), "pupil"
    )
    shape = dimensions(entry["shape"])
    length(shape) == 3 && shape[1] == 2 && shape[2] == shape[3] ||
        failure("pupil shape must be (2,p,p)")
    return (;
        schema = text(entry["schema"], "pupil schema"),
        shape,
        properties = properties(entry["properties"]),
    )
end

positive(value, label) = failure("$label must be positive and finite")
positive(value::Bool, label) = failure("$label must be a number, not Bool")
function positive(value::Real, label)
    isfinite(value) && value > 0 || failure("$label must be positive and finite")
    return Float64(value)
end
boolean(value::Bool, label) = value
boolean(value, label) = failure("$label must be Bool")

function response(entry)
    required = (
        "graph",
        "input",
        "output",
        "normalized_to_photon_rate",
        "pixel_sampling_m",
        "exposure_duration_s",
        "frame_schema",
    )
    fields(entry, required, required, "response")
    sampling = entry["pixel_sampling_m"]
    length(sampling) == 2 || failure("pixel_sampling_m requires two pitches")
    return (;
        graph = Symbol(text(entry["graph"], "response graph")),
        input = identifier(entry["input"], "response input"),
        output = identifier(entry["output"], "response output"),
        normalized_to_photon_rate = positive(
            entry["normalized_to_photon_rate"], "normalized_to_photon_rate"
        ),
        pixel_sampling_m = Tuple(positive(v, "pixel_sampling_m") for v in sampling),
        exposure_duration_s = positive(entry["exposure_duration_s"], "exposure_duration_s"),
        frame_schema = text(entry["frame_schema"], "frame_schema"),
    )
end
function chopping(entry)
    required = ("graph", "initial_phase", "trigger_high_is_fringed")
    fields(entry, required, required, "chopping")
    phase = Symbol(text(entry["initial_phase"], "initial_phase"))
    phase in (:fringed, :unfringed) || failure("invalid initial_phase")
    return (;
        graph = Symbol(text(entry["graph"], "chopping graph")),
        initial_phase = phase,
        trigger_high_is_fringed = boolean(
            entry["trigger_high_is_fringed"], "trigger_high_is_fringed"
        ),
    )
end
function optical_path(entry)
    required = ("graph", "exposure_duration_ns", "output", "sampling_node", "sensor")
    fields(entry, (required..., "response", "chopping"), required, "optical path")
    sensor = fields(
        entry["sensor"],
        ("schema", "properties", "frame_encoding"),
        ("schema", "properties"),
        "sensor",
    )
    encoding = text(get(sensor, "frame_encoding", "float32"), "frame_encoding")
    return (;
        graph = Symbol(text(entry["graph"], "optical graph")),
        exposure_duration_ns = Int64(
            integer(entry["exposure_duration_ns"], "exposure_duration_ns", typemax(Int64))
        ),
        output = identifier(entry["output"], "optical output"),
        sampling_node = identifier(entry["sampling_node"], "sampling_node"),
        sensor = (;
            schema = text(sensor["schema"], "sensor schema"),
            frame_type = frame_type(Val(Symbol(encoding))),
            properties = properties(sensor["properties"]),
        ),
        response = haskey(entry, "response") ? response(entry["response"]) : nothing,
        chopping = haskey(entry, "chopping") ? chopping(entry["chopping"]) : nothing,
    )
end
frame_type(::Val{:float32}) = Float32
frame_type(::Val{:uint16}) = UInt16
frame_type(::Val) = failure("frame_encoding must be float32 or uint16")

reference_clock(::Val{:realtime}) = PipeWireAO.METADATA_CLOCK_REALTIME
reference_clock(::Val{:monotonic}) = PipeWireAO.METADATA_CLOCK_MONOTONIC
reference_clock(::Val{:tai}) = PipeWireAO.METADATA_CLOCK_TAI
reference_clock(value) = failure("unsupported reference_clock")

wire_fields(::ExternalPupilMode) = ("pupil_node_name",)
wire_fields(::ClockedOpticalMode) = ("command",)
wire_pupil(::ExternalPupilMode, entry) = text(entry["pupil_node_name"], "pupil_node_name")
wire_pupil(::ClockedOpticalMode, entry) = nothing

function optical_wire(mode, entry, sensors)
    required = (
        "remote",
        "sensor_node_names",
        "sensor_buffers",
        "reference_clock",
        wire_fields(mode)...,
    )
    fields(entry, required, required, "pipewire")
    nodes = entry["sensor_node_names"]
    Set(names(nodes, "sensor_node_names")) == Set(sensors) ||
        failure("sensor node names differ from optical paths")
    command = command_wire(get(entry, "command", nothing))
    return (;
        remote = text(entry["remote"], "remote"),
        pupil_node_name = wire_pupil(mode, entry),
        sensor_node_names = as_named(nodes, sensors, v -> text(v, "sensor node name")),
        sensor_buffers = Int(integer(entry["sensor_buffers"], "sensor_buffers", 64)),
        reference_clock = reference_clock(
            Val(Symbol(text(entry["reference_clock"], "reference_clock")))
        ),
        command,
    )
end
command_wire(::Nothing) = nothing
function command_wire(entry)
    required = ("node_name", "schema", "properties", "buffers")
    fields(entry, required, required, "command")
    return (;
        node_name = text(entry["node_name"], "command node_name"),
        schema = text(entry["schema"], "command schema"),
        properties = properties(entry["properties"]),
        buffers = Int(integer(entry["buffers"], "command buffers", 64)),
    )
end

function source_configuration(::ExternalPupilMode, root, directory, retained, wire)
    return if (haskey(root, "source") || wire.command !== nothing)
        failure("external_pupil cannot declare source or command")
    else
        nothing
    end
end
function source_configuration(::ClockedOpticalMode, root, directory, retained, wire)
    haskey(root, "source") && wire.command !== nothing ||
        failure("clocked_optical requires source and command")
    entry = root["source"]
    required = (
        "graph",
        "command_input",
        "amplitude",
        "pupil_axes",
        "period_ns",
        "command_limit",
        "pacing_period_ns",
    )
    fields(entry, required, required, "source")
    dimensions(entry["pupil_axes"]) == (2, 1) || failure("pupil_axes must be [2,1]")
    pacing = nonnegative_integer(entry["pacing_period_ns"], "pacing_period_ns")
    return (;
        graph = Symbol(text(entry["graph"], "source graph")),
        command_input = identifier(entry["command_input"], "command_input"),
        amplitude = binding(directory, entry["amplitude"], retained),
        pupil_axes = (2, 1),
        period_ns = Int64(integer(entry["period_ns"], "period_ns", typemax(Int64))),
        command_limit = positive(entry["command_limit"], "command_limit"),
        pacing_period_ns = Int64(pacing),
    )
end
nonnegative_integer(value, label) = failure("$label must be a nonnegative integer")
nonnegative_integer(value::Bool, label) = failure("$label must be an integer, not Bool")
function nonnegative_integer(value::Integer, label)
    0 <= value <= typemax(Int64) || failure("$label is outside signed nanoseconds")
    return Int64(value)
end

function load_version(::Val{2}, directory, root)
    required = ("schema_version", "mode", "graphs", "optical", "pipewire")
    fields(root, (required..., "source"), required, "simulation")
    mode = optical_mode(Val(Symbol(text(root["mode"], "mode"))))
    retained = Ref(0)
    graph_names = names(root["graphs"], "graphs")
    cache = Dict{
        String,NamedTuple{(:path, :sha256, :bytes),Tuple{String,String,Vector{UInt8}}}
    }()
    graphs = as_named(
        root["graphs"], graph_names, v -> graph_snapshot(directory, v, retained, cache)
    )
    optical = fields(root["optical"], ("pupil", "paths"), ("pupil", "paths"), "optical")
    path_names = names(optical["paths"], "optical paths")
    paths = as_named(optical["paths"], path_names, optical_path)
    wire = optical_wire(mode, root["pipewire"], path_names)
    source = source_configuration(mode, root, directory, retained, wire)
    config = OpticalConfiguration(
        mode, graphs, (; pupil = pupil(optical["pupil"]), paths), wire, source
    )
    for row in values(paths)
        hasproperty(graphs, row.graph) || failure("unknown optical graph")
        row.response === nothing ||
            hasproperty(graphs, row.response.graph) ||
            failure("unknown response graph")
        row.chopping === nothing ||
            hasproperty(graphs, row.chopping.graph) ||
            failure("unknown chopping graph")
    end
    source === nothing ||
        hasproperty(graphs, source.graph) ||
        failure("unknown source graph")
    return config
end

optical_execution(::HostComputeDevice, ::Nothing) = StreamGraphExecution()
optical_execution(::HostComputeDevice, execution) = execution
optical_execution(::AcceleratorComputeDevice, ::Nothing) = CapturedGraphExecution()
optical_execution(::AcceleratorComputeDevice, execution::CapturedGraphExecution) = execution
function optical_execution(::AcceleratorComputeDevice, execution)
    return failure("accelerator optics require CapturedGraphExecution")
end

function prepare_optical_graph(
    snapshot;
    target,
    execution,
    node_types,
    period = nothing,
    input_contract = nothing,
    output_contract = nothing,
)
    return mktempdir() do directory
        for asset in snapshot.assets
            path = joinpath(directory, asset.path)
            mkpath(dirname(path))
            write(path, asset.bytes)
        end
        graph = document(snapshot.graph)
        for node in get(graph, "nodes", Any[])
            get(node, "type", nothing) in
            ("compiled_optical_paths_f32", "compiled_optical_paths_f64") || continue
            config = node["config"]
            config["model_path"] = joinpath(
                directory, relative_path(config["model_path"], "compiled model_path")
            )
        end
        runtime = Set{Symbol}()
        for (section, field) in
            (("inputs", "binding"), ("parameters", "binding"), ("delayed_links", "initial"))
            union!(runtime, (Symbol(row[field]) for row in get(graph, section, Any[])))
        end
        bindings = NamedTuple{keys(snapshot.bindings)}(
            Tuple(
                name in runtime ?
                target_binding(target, getproperty(snapshot.bindings, name)) :
                copy(getproperty(snapshot.bindings, name)) for
                name in keys(snapshot.bindings)
            ),
        )
        definition = load_algorithm_graph(graph; bindings, node_types)
        input_contract === nothing ||
            AlgorithmGraphs.graph_input_contract(definition, first(input_contract)).schema ==
            last(input_contract) ||
            failure("command schema differs from its AOS graph input contract")
        output_contract === nothing ||
            AlgorithmGraphs.graph_output_contract(definition, first(output_contract)).schema ==
            last(output_contract) ||
            failure("response frame_schema differs from its AOS graph output contract")
        period === nothing ||
            foreach(node -> validate_period(node.config, period), definition.nodes)
        return prepare_algorithm_graph(definition; target, execution)
    end
end

function target_binding(target, source)
    destination = allocate_device_array(target, eltype(source), size(source)...)
    copyto!(destination, source)
    return destination
end

function prepare(
    configuration::OpticalConfiguration;
    target = HostComputeDevice(),
    execution = nothing,
    node_types = builtin_graph_node_types(),
)
    execution = optical_execution(target, execution)
    graph(
        key;
        period = nothing,
        numerical_target = target,
        numerical_execution = execution,
        input_contract = nothing,
        output_contract = nothing,
    ) = prepare_optical_graph(
        getproperty(configuration.graphs, key);
        target = numerical_target,
        execution = numerical_execution,
        node_types,
        period,
        input_contract,
        output_contract,
    )
    paths = configuration.optical.paths
    graphs = map(row -> graph(row.graph), paths)
    observations = map(paths) do row
        observation = (; output = row.output, sampling_node = row.sampling_node)
        row.response === nothing || (
            observation = merge(
                observation,
                (;
                    response = merge(
                        row.response,
                        (;
                            graph = graph(
                                row.response.graph;
                                output_contract = row.response.output =>
                                    row.response.frame_schema,
                            )
                        ),
                    )
                ),
            )
        )
        row.chopping === nothing || (
            observation = merge(
                observation,
                (;
                    chopping = (;
                        unfringed = graph(row.chopping.graph),
                        initial_phase = row.chopping.initial_phase,
                    )
                ),
            )
        )
        return observation
    end
    plant = prepare_external_pupil(
        graphs;
        exposure_durations = map(row -> ModelDuration(row.exposure_duration_ns), paths),
        observations,
    )
    pupil = configuration.optical.pupil
    pupil.shape == (2, size(plant.pupil_opd)...) ||
        failure("pupil shape differs from prepared optical input")
    sensor_values = map(keys(paths)) do name
        row = getproperty(paths, name)
        observation = getproperty(observations, name)
        response = row.response
        image = if response === nothing
            graph_output(getproperty(plant.graphs, name), row.output)
        else
            graph_output(observation.response.graph, response.output)
        end
        response === nothing ||
            row.sensor.schema == response.frame_schema ||
            failure("sensor schema differs from response frame_schema")
        row.sensor.frame_type === UInt16 && response === nothing &&
            failure("uint16 sensor encoding requires a detector response")
        row.sensor.frame_type === UInt16 && eltype(image) ∉ (Float32, UInt16) &&
            failure("uint16 sensor encoding requires Float32 ADC codes or UInt16 pixels")
        validate_sensor_properties(row.sensor.properties, image, row.exposure_duration_ns)
        properties = copy(row.sensor.properties)
        for prefix in ("optics.", "optics.unfringed.", "detector.")
            for suffix in (
                "execution",
                "captured-node-count",
                "execution-scope",
                "independent.execution",
                "independent.captured-node-count",
            )
                pop!(properties, prefix * suffix, nothing)
            end
        end
        merge!(properties, ExternalPupil.execution_properties(plant, name))
        response === nothing || merge!(
            properties,
            ExternalPupil.execution_properties(
                observation.response.graph; prefix = "detector."
            ),
        )
        row.chopping === nothing || merge!(
            properties,
            ExternalPupil.execution_properties(
                plant, name; prefix = "optics.unfringed.", unfringed = true
            ),
        )
        return (; image, schema = row.sensor.schema, properties)
    end
    sensors = NamedTuple{keys(paths)}(sensor_values)
    chopped = Tuple(
        name for name in keys(paths) if getproperty(paths, name).chopping !== nothing
    )
    chopping = NamedTuple{chopped}(
        Tuple(getproperty(paths, name).chopping for name in chopped)
    )
    science = (; plant, pupil, sensors, chopping)
    return prepare_optical_mode(configuration.mode, configuration, science, graph)
end

function validate_sensor_properties(properties, image, duration)
    for key in ("detector.roi-shape", "detector.shape")
        haskey(properties, key) &&
            properties[key] != join(size(image), ",") &&
            failure("sensor $key differs from prepared image")
    end
    haskey(properties, "optics.exposure-duration-ns") &&
        properties["optics.exposure-duration-ns"] != string(duration) &&
        failure("sensor exposure property differs from optical exposure")
    return nothing
end

function prepare_optical_mode(::ExternalPupilMode, configuration, science, graph)
    frame_types = map(row -> row.sensor.frame_type, configuration.optical.paths)
    execution = ExternalPupil.prepare(science; frame_types)
    return finish_optical(configuration, science, execution)
end
function prepare_optical_mode(::ClockedOpticalMode, configuration, science, graph)
    source = configuration.source
    source_graph = graph(
        source.graph;
        period = source.period_ns,
        numerical_target = HostComputeDevice(),
        numerical_execution = StreamGraphExecution(),
        input_contract = source.command_input => configuration.pipewire.command.schema,
    )
    science = ExternalPupil.prepare_clocked_source(
        science;
        source = (; graph = source_graph),
        command_input = source.command_input,
        amplitude = copy(source.amplitude),
        pupil_axes = source.pupil_axes,
        period_ns = source.period_ns,
    )
    count = length(science.source_command)
    execution = ExternalPupil.prepare(
        science;
        clocked = true,
        command_count = count,
        command_limit = source.command_limit,
        pacing_period_ns = source.pacing_period_ns,
        frame_types = map(row -> row.sensor.frame_type, configuration.optical.paths),
    )
    return finish_optical(configuration, science, execution)
end
input_option(::ExternalPupilMode, configuration, science) = nothing
function input_option(::ClockedOpticalMode, configuration, science)
    wire = configuration.pipewire
    command = wire.command
    return PipeWireArrayStreamOptions(;
        node_name = command.node_name,
        media_name = "Optical mirror command",
        schema = command.schema,
        format = PipeWireAO.NdArrayFormat(
            PipeWireAO.NdArray.F32_LE,
            (length(science.source_command),);
            layout = PipeWireAO.NdArray.ROW_MAJOR,
        ),
        properties = command.properties,
        buffers = command.buffers,
        reference_clock = wire.reference_clock,
        metadata = (
            PipeWireAO.MetadataRequirement(
                PipeWireAO.SPA.META_REFERENCE_TIMESTAMP; required = true
            ),
            PipeWireAO.MetadataRequirement(
                PipeWireAO.SPA.META_ACQUISITION_IDENTITY; required = false
            ),
            PipeWireAO.MetadataRequirement(
                PipeWireAO.SPA.META_EXPOSURE_TIMING; required = false
            ),
        ),
    )
end
function finish_optical(configuration, science, execution)
    wire = configuration.pipewire
    ExternalPupil.warmup!(execution)
    options = ExternalPupil.stream_options(
        science,
        wire.pupil_node_name,
        wire.sensor_node_names;
        sensor_buffers = wire.sensor_buffers,
        reference_clock = wire.reference_clock,
        input_option = input_option(configuration.mode, configuration, science),
        frame_types = NamedTuple{keys(science.sensors)}(map(eltype, execution.buffers)),
    )
    length(unique(option.node_name for option in options)) == length(options) ||
        failure("array node names must be unique")
    return PreparedOptical(science, execution, options, wire.remote)
end

"Run at most `frames` source advances under serialized caller controls and the selected idle policy."
function run!(
    control, prepared::PreparedOptical;
    frames::Integer,
    idle_strategy = SimRunner.configured_strategy(),
    kwargs...,
)
    count = integer(frames, "frames", typemax(Int))
    return with_array_streams(
        prepared.options;
        remote = prepared.remote,
        synchronous_command = prepared.execution.clocked,
        input = (true, ntuple(_ -> false, length(prepared.options) - 1)...),
    ) do streams
        ExternalPupil.prepare_transport!(prepared.execution, streams)
        return run_optical!(control, prepared.execution, streams, count; idle_strategy, kwargs...)
    end
end
function run_optical!(control, execution, streams, count; kwargs...)
    completed = Ref(0)
    previous = Ref(ExternalPupil.snapshot(execution))
    ExternalPupil.run!(execution, streams; kwargs...) do execution, transport, now_ns
        cursor = ExternalPupil.snapshot(execution)
        cursor.sequence > previous[].sequence &&
            (completed[] += Int(cursor.sequence - previous[].sequence))
        completed[] >= count && return ExternalPupil.Stop()
        decision = control(execution, transport, now_ns)
        previous[] = ExternalPupil.snapshot(execution)
        return decision
    end
    return completed[]
end
