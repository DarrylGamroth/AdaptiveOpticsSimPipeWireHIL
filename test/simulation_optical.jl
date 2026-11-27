module OpticalSimulationTests
using Test, SHA, FITSIO
using YAML: YAML
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil, PipeWireHILError
using AdaptiveOpticsSim.AlgorithmGraphs

function binding_file(directory, name, values)
    path = joinpath(directory, name * ".fits")
    FITS(path, "w") do fits
        return write(fits, eltype(values) === Bool ? UInt8.(values) : values)
    end
    return Dict(
        "path" => name * ".fits",
        "sha256" => bytes2hex(sha256(read(path))),
        "shape" => collect(size(values)),
        "eltype" => string(eltype(values)),
    )
end
function graph_file(directory, name, graph, bindings)
    path = joinpath(directory, name * ".yaml")
    YAML.write_file(path, graph)
    return Dict(
        "path" => name * ".yaml",
        "sha256" => bytes2hex(sha256(read(path))),
        "bindings" => bindings,
    )
end
function bundle(directory)
    graph = Dict(
        "schema_version" => 1,
        "name" => "optical",
        "nodes" => [
            Dict(
                "name" => "optics",
                "type" => "proper_optical_train_f32",
                "config" => Dict(
                    "resolution" => 4,
                    "pupil_resolution" => 4,
                    "diameter_m" => 0.004,
                    "wavelength_um" => 1.55,
                    "steps" => [],
                ),
            ),
        ],
        "inputs" => [
            Dict("name" => name, "destination" => "optics." * name, "binding" => name)
            for name in ("pupil_opd", "pupil_amplitude")
        ],
        "outputs" => [Dict("name" => "intensity", "source" => "optics.output")],
    )
    bindings = Dict(
        "pupil_opd" => binding_file(directory, "pupil_opd", zeros(Float32, 4, 4)),
        "pupil_amplitude" =>
            binding_file(directory, "pupil_amplitude", ones(Float32, 4, 4)),
    )
    path = Dict(
        "graph" => "optical-main",
        "exposure_duration_ns" => 4,
        "output" => "intensity",
        "sampling_node" => "optics",
        "sensor" => Dict(
            "schema" => "test.optical/1",
            "properties" => Dict("detector.roi-shape" => "4,4"),
        ),
    )
    root = Dict(
        "schema_version" => 2,
        "mode" => "external_pupil",
        "graphs" =>
            Dict("optical-main" => graph_file(directory, "optical", graph, bindings)),
        "optical" => Dict(
            "pupil" => Dict(
                "schema" => "test.pupil/1",
                "shape" => [2, 4, 4],
                "properties" => Dict(),
            ),
            "paths" => Dict("camera" => path),
        ),
        "pipewire" => Dict(
            "remote" => "unopened-optical-core",
            "pupil_node_name" => "pupil",
            "sensor_node_names" => Dict("camera" => "camera"),
            "sensor_buffers" => 3,
            "reference_clock" => "realtime",
        ),
    )
    return (;
        root,
        graph,
        path,
        bindings,
        directory,
        manifest = joinpath(directory, "simulation.yaml"),
    )
end
save(product) = (YAML.write_file(product.manifest, product.root); product.manifest)

@testset "schema-2 bounded optical declarations" begin
    mktempdir() do directory
        product = bundle(directory)
        config = Simulation.load(save(product))
        @test typeof(config) <: Simulation.AbstractConfiguration
        @test keys(config.graphs) == (Symbol("optical-main"),)
        @test config.optical.pupil.shape == (2, 4, 4)
        @test config.pipewire.remote == "unopened-optical-core"
        for change in (
            p -> p.root["mode"] = "unknown",
            p -> p.root["optical"]["pupil"]["shape"] = [2, 3, 4],
            p -> p.path["graph"] = "missing",
            p -> p.path["exposure_duration_ns"] = true,
            p ->
                p.path["chopping"] = Dict(
                    "graph" => "optical-main",
                    "initial_phase" => "mixed",
                    "trigger_high_is_fringed" => true,
                ),
            p -> p.root["pipewire"]["reference_clock"] = "unknown",
            p -> p.root["pipewire"]["sensor_node_names"] = Dict("other" => "camera"),
            p -> p.root["mode"] = "clocked_optical",
            p -> p.root["graphs"]["optical-main"]["bindings"] = [],
            p -> p.path["sensor"]["properties"] = [],
            p -> p.path["sensor"]["frame_encoding"] = "raw",
        )
            fresh = bundle(directory)
            change(fresh)
            @test_throws PipeWireHILError Simulation.load(save(fresh))
        end
        write(product.manifest, "7\n")
        @test_throws PipeWireHILError Simulation.load(product.manifest)
    end
end

function add_response!(product)
    directory = product.directory
    config = Dict(
        "shutter" => "global",
        "rows" => 4,
        "columns" => 4,
        "pixel_sampling_m" => [0.001, 0.001],
        "spectral_channel" => "reference",
        "exposure_duration_s" => 4e-9,
        "quantum_efficiency" => 0.5,
        "photon_noise" => true,
        "rng_seed" => 17,
        "photon_rate_schema" => "test.rate/1",
        "frame_schema" => "test.frame/1",
    )
    graph = Dict(
        "schema_version" => 1,
        "name" => "response",
        "nodes" => [
            Dict(
                "name" => "detector",
                "type" => "detector",
                "detector" => "ingaas",
                "config" => config,
            ),
        ],
        "inputs" => [
            Dict(
                "name" => "photon_rate",
                "destination" => "detector.photon_rate",
                "binding" => "photons",
            ),
        ],
        "outputs" => [Dict("name" => "frame", "source" => "detector.frame")],
    )
    product.root["graphs"]["response"] = graph_file(
        directory,
        "response",
        graph,
        Dict("photons" => binding_file(directory, "photons", zeros(Float32, 4, 4))),
    )
    product.path["response"] = Dict(
        "graph" => "response",
        "input" => "photon_rate",
        "output" => "frame",
        "normalized_to_photon_rate" => 1e10,
        "pixel_sampling_m" => [0.001, 0.001],
        "exposure_duration_s" => 4e-9,
        "frame_schema" => "test.frame/1",
    )
    product.path["sensor"]["schema"] = "test.frame/1"
    return graph
end

function add_source!(product)
    directory = product.directory
    config = Dict(
        "pupil_rows" => 4,
        "pupil_columns" => 4,
        "mode_count" => 1,
        "coefficients_schema" => "test.command/1",
        "opd_schema" => "test.opd/1",
        "basis_schema" => "test.basis/1",
        "pupil_support_schema" => "test.support/1",
    )
    graph = Dict(
        "schema_version" => 1,
        "name" => "source",
        "nodes" => [
            Dict(
                "name" => "mirror",
                "type" => "modal_opd_expansion_f32",
                "config" => config,
            ),
        ],
        "inputs" => [
            Dict(
                "name" => "command",
                "destination" => "mirror.coefficients",
                "binding" => "command",
            ),
        ],
        "outputs" => [
            Dict("name" => name, "source" => "mirror.opd") for
            name in ("dm_opd", "pupil_opd")
        ],
        "parameters" => [
            Dict("destination" => "mirror.basis", "binding" => "basis"),
            Dict("destination" => "mirror.pupil_support", "binding" => "support"),
        ],
    )
    bindings = Dict(
        "command" => binding_file(directory, "command", zeros(Float32, 1)),
        "basis" => binding_file(directory, "basis", fill(1.0f-9, 4, 4, 1)),
        "support" => binding_file(directory, "support", trues(4, 4)),
    )
    product.root["graphs"]["source"] = graph_file(directory, "source", graph, bindings)
    product.root["mode"] = "clocked_optical"
    delete!(product.root["pipewire"], "pupil_node_name")
    product.root["source"] = Dict(
        "graph" => "source",
        "command_input" => "command",
        "amplitude" => binding_file(directory, "amplitude", ones(Float32, 4, 4)),
        "pupil_axes" => [2, 1],
        "period_ns" => 2,
        "command_limit" => 1.0,
        "pacing_period_ns" => 0,
    )
    return product.root["pipewire"]["command"] = Dict(
        "node_name" => "command",
        "schema" => "test.command/1",
        "properties" => Dict(),
        "buffers" => 3,
    )
end

function clocked_bundle(directory)
    product = bundle(directory)
    # Source-compatible optics expose an independent physical DM input.
    product.graph["nodes"][1]["config"]["steps"] = [
        Dict(
            "op" => "add_opd",
            "name" => "dm",
            "input" => "dm_opd",
            "size" => [4, 4],
            "axes" => [1, 2],
            "sampling_m" => [0.001, 0.001],
            "center_pixel" => [3.0, 3.0],
            "schema" => "test.opd/1",
            "outside_opd_m" => 0.0,
        ),
    ]
    push!(
        product.graph["inputs"],
        Dict("name" => "dm_opd", "destination" => "optics.dm_opd", "binding" => "dm_opd"),
    )
    product.bindings["dm_opd"] = binding_file(directory, "dm_opd", zeros(Float32, 4, 4))
    product.root["graphs"]["optical-main"] = graph_file(
        directory, "optical", product.graph, product.bindings
    )
    add_source!(product)
    return product
end

function test_preparation()
    @testset "real CPU optics, detector ownership and clocked source" begin
        mktempdir() do directory
            product = bundle(directory)
            add_response!(product)
            product.root["optical"]["paths"]["second"] = deepcopy(product.path)
            product.root["pipewire"]["sensor_node_names"]["second"] = "second"
            product.path["chopping"] = Dict(
                "graph" => "optical-main",
                "initial_phase" => "fringed",
                "trigger_high_is_fringed" => true,
            )
            config = Simulation.load(save(product))
            prepared = Simulation.prepare(config)
            @test prepared.execution.sequence == 0
            @test prepared.remote == "unopened-optical-core"
            @test length(prepared.options) == 3
            @test prepared.science.plant.graphs.camera !==
                prepared.science.plant.graphs.second
            @test prepared.science.plant.observations.camera.response.graph !==
                prepared.science.plant.observations.second.response.graph
            @test prepared.science.plant.graphs.camera !==
                prepared.science.plant.observations.camera.chopping.unfringed
            @test all(iszero, prepared.execution.frames[1])
            @test all(iszero, config.graphs[Symbol("optical-main")].bindings.pupil_opd)
            # Warmup resets detector RNG: independent preparations produce exact draws.
            second = Simulation.prepare(config)
            packet = zeros(Float32, 2, 4, 4)
            packet[2, :, :] .= 1
            outputs = []
            other = []
            for (science, store) in ((prepared.science, outputs), (second.science, other))
                step_external_pupil!(
                    (name, image, metadata) -> push!(store, (name, copy(image), metadata)),
                    science.plant,
                    view(packet, 1, :, :),
                    view(packet, 2, :, :),
                    ModelTimestamp(0),
                    ModelDuration(4);
                    domain = ntuple(UInt8, 16),
                    generation = UInt64(1),
                    sequence = UInt64(1),
                    lost_samples = UInt64(0),
                )
            end
            @test outputs == other
            @test length(outputs) == 2
            product.path["sensor"]["schema"] = "wrong/1"
            @test_throws PipeWireHILError Simulation.prepare(Simulation.load(save(product)))
            # Matching wire declarations cannot rename the actual AOS output.
            product.path["response"]["frame_schema"] = "wrong/1"
            @test_throws PipeWireHILError Simulation.prepare(Simulation.load(save(product)))
        end
        mktempdir() do directory
            product = bundle(directory)
            product.path["sensor"]["frame_encoding"] = "uint16"
            @test_throws PipeWireHILError Simulation.prepare(Simulation.load(save(product)))
            detector = add_response!(product)
            config = detector["nodes"][1]["config"]
            config["bits"] = 16
            config["full_well_e"] = 1024
            bindings = product.root["graphs"]["response"]["bindings"]
            product.root["graphs"]["response"] = graph_file(
                directory, "response", detector, bindings
            )
            prepared = Simulation.prepare(Simulation.load(save(product)))
            @test eltype(only(prepared.execution.buffers)) === UInt16
            @test prepared.options[2].format.element_type == Simulation.PipeWireAO.NdArray.U16_LE
        end
        mktempdir() do directory
            product = clocked_bundle(directory)
            prepared = Simulation.prepare(Simulation.load(save(product)))
            @test prepared.execution.clocked
            @test length(prepared.execution.command) == 1
            @test model_time_sequence(prepared.science.source_driver) == 0
            @test all(iszero, prepared.science.source_command)
            @test prepared.options[1].schema == "test.command/1"
            product.root["pipewire"]["command"]["schema"] = "wrong-command/1"
            @test_throws PipeWireHILError Simulation.prepare(Simulation.load(save(product)))
        end
    end
end

@testset "clocked optical mode fields and integer declarations" begin
    mktempdir() do directory
        product = clocked_bundle(directory)
        @test Simulation.load(save(product)).pipewire.pupil_node_name === nothing
        product.root["pipewire"]["pupil_node_name"] = "unused-pupil"
        @test_throws PipeWireHILError Simulation.load(save(product))
        product = bundle(directory)
        delete!(product.root["pipewire"], "pupil_node_name")
        @test_throws PipeWireHILError Simulation.load(save(product))
        for change in (
            p -> p.root["source"]["pacing_period_ns"] = 0.0,
            p -> p.root["source"]["pupil_axes"] = [2.0, 1.0],
        )
            fresh = clocked_bundle(directory)
            change(fresh)
            @test_throws PipeWireHILError Simulation.load(save(fresh))
        end
    end
end

end
