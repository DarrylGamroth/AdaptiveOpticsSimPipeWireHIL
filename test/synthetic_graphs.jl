module SyntheticGraphs

using FITSIO: FITS
import PipeWireAORTC

export lockstep_documents, pupil_source_documents, external_pupil_documents,
    clocked_optical_documents

const Common = PipeWireAORTC.Common
const Configuration = PipeWireAORTC.InstrumentConfiguration
const PUPIL_SIZE = 4
const PUPIL_DIAMETER = 0.012
const PUPIL_SCHEMA = "test.pupil.opd/1"
const SURFACE_SCHEMA = "test.mirror.surface/1"
const COMMAND_SCHEMA = "test.mirror.command/1"

input(name, destination) = Dict("name" => name, "binding" => name, "destination" => destination)
output(name, source) = Dict("name" => name, "source" => source)
parameter(binding, destination) = Dict("binding" => binding, "destination" => destination)
node(name, type, config) = Dict("name" => name, "type" => type, "config" => config)
zeros_binding(shape...) = Dict("kind" => "zeros", "type" => "Float32", "shape" => collect(shape))

function graph(name, nodes, inputs, outputs; parameters=[], links=[])
    return Dict("schema_version" => 1, "name" => name, "nodes" => nodes,
        "inputs" => inputs, "outputs" => outputs, "parameters" => parameters, "links" => links)
end

function fits_binding(directory, name, values; datatype=eltype(values))
    filename = name * ".fits"
    path = joinpath(directory, filename)
    FITS(path, "w") do file
        write(file, values)
    end
    return Dict("kind" => "fits", "type" => string(datatype), "path" => filename,
        "hdu" => 1, "axes" => collect(1:ndims(values)), "shape" => collect(size(values)),
        "sha256" => Common.sha256_file(path))
end

function pyramid_sensor(schema)
    return Dict("kind" => "pyramid", "detector_shape" => [2, 2], "pupil_shape" => [1, 1],
        "pupil_separation_pixels" => [0, 0], "pupil_edge_pixels" => [0, 0],
        "expected_schema" => schema)
end

function write_documents(directory, graphs, bindings, sensors, source;
        components=Dict(), profile="reference", mirror=false, optical_paths=Dict())
    profile_document = profile == "reference" ? Dict("nodes" => [], "parameters" => []) :
        Dict("template" => profile)
    plane = Dict("plane" => "entrance_pupil", "diameter_m" => PUPIL_DIAMETER,
        "curvature" => "collimated", "registration" => "centered")
    physical = Dict{String,Any}(
        "schema_version" => 1, "id" => "synthetic-hil",
        "deformable_mirrors" => Dict{String,Any}(), "wavefront_sensors" => sensors,
        "optical_input" => plane, "optical_output" => plane,
        "telescope" => Dict("telescope_diameter_m" => PUPIL_DIAMETER,
            "central_obstruction_ratio" => 0.0, "pupil_reflectivity" => 1.0,
            "aperture_revision" => 1),
    )
    isempty(optical_paths) || (physical["optical_paths"] = optical_paths)
    controls = Dict{String,Any}()
    if mirror
        Common.write_json(joinpath(directory, "mirror.json"), Dict(
            "schema_version" => 1, "id" => "synthetic-mirror", "manufacturer" => "Test",
            "model" => "Two actuators", "layout" => Dict(
                "schema_version" => 1, "name" => "Test grid", "grid_shape" => [2, 2],
                "command_positions" => [[1, 1], [2, 2]], "axis_labels" => ["column", "row"],
                "command_order" => "physical")))
        physical["deformable_mirrors"]["primary"] = Dict("description" => "mirror.json",
            "pupil_registration" => Dict("actuator_pitch_pupil_radius" => 0.2),
            "units" => "m", "command_scale_m" => 1)
        controls["primary"] = Dict(
            "limiter" => Dict("kind" => "amplitude", "lower" => -1, "upper" => 1),
            "shape_control" => Dict("bases" => []))
    end
    definition = Dict{String,Any}(
        "schema_version" => 1, "id" => "synthetic-hil", "instrument" => "../instrument.yaml",
        "frame_period_s" => 0.002, "graphs" => graphs, "components" => components,
        "defaults" => Dict("profile" => profile, "pupil_resolution" => PUPIL_SIZE,
            "wavelength_um" => 1.55),
        "settings_schema" => Dict("type" => "object", "properties" => Dict(
            "profile" => Dict("type" => "string", "enum" => [profile]),
            "pupil_resolution" => Dict("type" => "integer", "minimum" => 1),
            "wavelength_um" => Dict("type" => "number", "exclusiveMinimum" => 0))),
        "profiles" => Dict(profile => profile_document),
        "bindings" => bindings, "settings_bindings" => [],
    )
    merge!(definition, get(source, "definition", Dict()))
    mkpath(joinpath(directory, "simulation"))
    mkpath(joinpath(directory, "rtc"))
    Common.write_yaml(joinpath(directory, "instrument.yaml"), physical)
    Common.write_yaml(joinpath(directory, "simulation/definition.yaml"), definition)
    Common.write_yaml(joinpath(directory, "rtc/mirror-controls.yaml"),
        Dict("schema_version" => 1, "deformable_mirrors" => controls))
    rtc = "rtc/configuration.yaml"
    Common.write_yaml(joinpath(directory, rtc), Dict("schema_version" => 1,
        "instrument" => "../instrument.yaml", "mirror_controls" => "rtc/mirror-controls.yaml",
        "dimensions" => Dict(), "publication" => "instrument.json",
        "calibration_identity" => "sha256:" * repeat("a", 64)))
    Configuration.prepare_publication(directory)
    document = merge(Dict{String,Any}(
        "plant" => Dict("definition" => "simulation/definition.yaml", "settings" => Dict()),
        "rtc_configuration" => rtc, "backend" => "cpu", "device" => 0,
    ), source["launch"])
    config = joinpath(directory, "simulation.conf")
    Common.write_json(config, document)
    document = Configuration.prepare_simulation_configuration!(directory)
    Common.write_json(config, document)
    return (; config, document)
end

function lockstep_documents(directory; total_exchanges=2)
    frame_schema = "test.modal.opd/1"
    command_schema = "test.modal.coefficients/1"
    basis = reshape(Float32.(1:8), 2, 2, 2)
    bindings = Dict("command" => zeros_binding(2),
        "basis" => fits_binding(directory, "basis", basis),
        "support" => fits_binding(directory, "support", UInt8[1 0; 1 1]; datatype=Bool))
    expansion = node("expand", "modal_opd_expansion_f32", Dict(
        "pupil_rows" => 2, "pupil_columns" => 2, "mode_count" => 2,
        "coefficients_schema" => command_schema, "opd_schema" => frame_schema,
        "basis_schema" => "test.modal.basis/1", "pupil_support_schema" => "test.modal.support/1"))
    modal = graph("modal", [expansion], [input("command", "expand.coefficients")],
        [output("frame", "expand.opd")]; parameters=[
            parameter("basis", "expand.basis"), parameter("support", "expand.pupil_support")])
    wire = Dict("frame_encoding" => "uint16", "frame_schema" => frame_schema,
        "command_schema" => command_schema, "command_scale" => 1)
    launch = merge(wire, Dict("version" => 3, "command_input" => "command",
        "frame_output" => "frame", "frame_shape" => [2, 2], "command_count" => 2,
        "rate" => 500, "exposure_ns" => 2_000_000, "total_exchanges" => total_exchanges,
        "pacing_rate_hz" => 25))
    source = Dict("launch" => launch, "definition" => Dict(
        "hil" => Dict("command" => "expand.coefficients", "frame" => "expand.opd"),
        "wire_contracts" => [wire]))
    result = write_documents(directory, Dict("production" => modal, "calibration" => modal),
        bindings, Dict("sensor" => pyramid_sensor(frame_schema)), source; mirror=true)
    return (; result..., basis)
end

function pupil_source_documents(directory)
    composition = node("pupil", "pupil_opd_composition_f32", Dict("resolution" => PUPIL_SIZE,
        "surface_opd_schema" => SURFACE_SCHEMA, "uncompensated_opd_schema" => PUPIL_SCHEMA,
        "pupil_opd_schema" => PUPIL_SCHEMA))
    plant = graph("pupil_source", [Dict("name" => "primary"), composition],
        [input("command", "primary.pdm_command"), input("residual", "pupil.uncompensated_opd")],
        [output("pupil_opd", "pupil.pupil_opd")]; links=[
            Dict("source" => "primary.surface_opd", "destination" => "pupil.surface_opd")])
    bindings = Dict("command" => zeros_binding(2),
        "residual" => zeros_binding(PUPIL_SIZE, PUPIL_SIZE),
        "pupil_amplitude" => Dict("kind" => "pupil_amplitude", "type" => "Float32",
            "resolution" => "pupil_resolution"))
    components = Dict("primary" => Dict("template" => "deformable_mirror", "mirror" => "primary",
        "coupling" => 0.15, "config" => Dict("surface_opd_schema" => SURFACE_SCHEMA,
            "pdm_command_schema" => COMMAND_SCHEMA, "actuator_grid_indices_schema" => "test.mirror.grid/1")))
    source = Dict("launch" => Dict("version" => 5, "mode" => "pupil-source",
        "plant" => Dict("definition" => "simulation/definition.yaml", "settings" => Dict(),
            "graph" => "plant"),
        "command_input" => "command", "mirror" => "primary", "rate" => 500, "pacing_rate_hz" => 1),
        "definition" => Dict("pupil_output" => Dict("opd" => "pupil_opd", "amplitude" => "pupil_amplitude")))
    return write_documents(directory, Dict("plant" => plant, "calibration" => plant),
        bindings, Dict("sensor" => pyramid_sensor("test.pupil.frame/1")), source;
        components, profile="grid_gaussian", mirror=true)
end

function camera_graph(; mirror=false)
    steps = mirror ? [Dict("op" => "add_opd", "name" => "mirror", "input" => "dm_opd",
        "size" => [4, 4], "axes" => [1, 2], "center_pixel" => [3, 3],
        "sampling_m" => [0.003, 0.003], "schema" => SURFACE_SCHEMA, "outside_opd_m" => 0.0)] :
        [Dict("op" => "multiply", "amplitude" => 0.5)]
    optics = node("optics", "proper_optical_train_f32", Dict("resolution" => 8,
        "pupil_resolution" => PUPIL_SIZE, "diameter_m" => PUPIL_DIAMETER, "wavelength_um" => 1.55,
        "steps" => steps, "pupil_opd_schema" => PUPIL_SCHEMA,
        "pupil_amplitude_schema" => "test.pupil.amplitude/1", "output_schema" => "test.camera.intensity/1"))
    inputs = [input(name, "optics." * name) for name in ("pupil_opd", "pupil_amplitude")]
    mirror && push!(inputs, input("dm_opd", "optics.dm_opd"))
    return graph("camera", [optics], inputs, [output("intensity", "optics.output")])
end

function cameras(exposures)
    sensors = Dict(name => Dict("kind" => "scc", "detector" => Dict("name" => "Synthetic " * name),
        "acquisition" => Dict("exposure_duration_s" => duration)) for (name, duration) in exposures)
    optical_paths = Dict(name => Dict("sensor" => name, "lyot_stop_port" => "transmitted")
        for name in keys(sensors))
    nodes = Dict(name => "test.camera." * name for name in keys(sensors))
    return (; sensors, optical_paths, nodes)
end

function external_pupil_documents(directory)
    (; sensors, optical_paths, nodes) = cameras(("fast" => 0.002, "slow" => 0.004))
    graphs = Dict(name => camera_graph() for name in keys(sensors))
    bindings = Dict(name => zeros_binding(PUPIL_SIZE, PUPIL_SIZE)
        for name in ("pupil_opd", "pupil_amplitude"))
    source = Dict("launch" => Dict("version" => 4, "mode" => "external-pupil",
        "pupil_node" => "test.pupil", "sensor_nodes" => nodes))
    return write_documents(directory, graphs, bindings, sensors, source; optical_paths)
end

function clocked_optical_documents(directory)
    (; sensors, optical_paths, nodes) = cameras(("fast" => 0.002,))
    mirror = node("primary", "deformable_mirror_surface_f32", Dict("actuator_count" => 2,
        "resolution" => PUPIL_SIZE, "telescope_diameter_m" => PUPIL_DIAMETER,
        "pdm_command_schema" => COMMAND_SCHEMA, "surface_opd_schema" => SURFACE_SCHEMA,
        "actuator_coordinates_schema" => "test.mirror.coordinates/1",
        "influence_functions_schema" => "test.mirror.influences/1"))
    residual = node("residual", "spatial_ao_residual_f32", Dict(
        "resolution" => PUPIL_SIZE, "telescope_diameter_m" => PUPIL_DIAMETER, "r0" => 0.15,
        "reference_wavelength_m" => 500e-9, "fractional_cn2" => [1.0],
        "wind_speed" => [1.0], "wind_direction_deg" => [0.0], "altitude" => [0.0],
        "layer_ids" => ["ground"], "atmosphere_step" => 0.002, "rng_seed" => 17,
        "control_cutoff_cycles_per_m" => 10.0, "residual_amplitude" => 0.25,
        "residual_opd_schema" => PUPIL_SCHEMA))
    plant = graph("clocked_source", [mirror, residual], [input("command", "primary.pdm_command")],
        [output("pupil_opd", "residual.residual_opd"), output("dm_opd", "primary.surface_opd")];
        parameters=[parameter("coordinates", "primary.actuator_coordinates"),
            parameter("influences", "primary.influence_functions")])
    bindings = Dict(name => zeros_binding(PUPIL_SIZE, PUPIL_SIZE)
        for name in ("pupil_opd", "pupil_amplitude", "dm_opd"))
    bindings["command"] = zeros_binding(2)
    bindings["coordinates"] = Dict("kind" => "mirror_coordinates", "mirror" => "primary")
    bindings["influences"] = zeros_binding(PUPIL_SIZE^2, 2)
    samples = reshape(Float32.(1:PUPIL_SIZE^2), PUPIL_SIZE, PUPIL_SIZE) ./ PUPIL_SIZE^2
    bindings["source_amplitude"] = fits_binding(directory, "amplitude", samples)
    source = Dict("launch" => Dict("version" => 6, "mode" => "clocked-optical",
        "sensor_nodes" => nodes, "source_graph" => "plant", "source_amplitude_binding" => "source_amplitude",
        "source_pupil_axes" => [2, 1], "source_dm_axes" => ["y", "x"], "command_node" => "test.mirror.command",
        "command_schema" => COMMAND_SCHEMA, "command_units" => "m", "command_limit" => 1.0,
        "command_input" => "command", "mirror" => "primary", "rate" => 500, "pacing_rate_hz" => 50))
    return write_documents(directory, Dict("plant" => plant, "fast" => camera_graph(; mirror=true)),
        bindings, sensors, source; optical_paths, mirror=true)
end

end
