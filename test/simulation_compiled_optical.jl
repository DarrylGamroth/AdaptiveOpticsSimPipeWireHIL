# Optional CPU compiled-model relocation and immutable-snapshot check.
using Proper
isdefined(@__MODULE__, :OpticalSimulationTests) || include("simulation_optical.jl")

module CompiledOpticalSimulationTests
using Test, SHA, FITSIO, YAML
using AdaptiveOpticsSimPipeWireHIL: Simulation, PipeWireHILError
using AdaptiveOpticsSim.AlgorithmGraphs
using ..OpticalSimulationTests: bundle, save, binding_file, graph_file

function compiled_bundle(directory)
    product = bundle(directory)
    modeldir = joinpath(directory, "models")
    mkdir(modeldir)
    arrays = Dict{String,Any}()
    assets = Any[]
    for (name, values) in (
        ("one", ones(ComplexF64, 4, 4)),
        ("identity", ComplexF64[i == j for i in 1:4, j in 1:4]),
        ("scalar", ones(ComplexF64, 1, 1)),
    )
        path = joinpath(modeldir, name * ".fits")
        FITS(path, "w") do file
            write(file, permutedims(real.(values)))
            return write(file, permutedims(imag.(values)))
        end
        digest = bytes2hex(sha256(read(path)))
        arrays[name] = Dict(
            "path" => name * ".fits",
            "sha256" => digest,
            "shape" => collect(size(values)),
            "type" => "ComplexF64",
            "axes" => [2, 1],
            "real_hdu" => 1,
            "imag_hdu" => 2,
        )
        push!(assets, Dict("path" => "models/" * name * ".fits", "sha256" => digest))
    end
    screen(name) = Dict(
        "kind" => "live_screen",
        "input" => name,
        "kernel" => "identity",
        "size" => 4,
        "dx" => 0.001,
        "lam" => 1.55e-6,
    )
    metadata = Dict(
        "schema_version" => 1,
        "array_orientation" => "yx",
        "grid" => 4,
        "image_size" => 4,
        "crop_start" => 0,
        "n_env" => 1,
        "wavelengths" => [1.55e-6],
        "channels" => [1.55e-6],
        "mode" => "sky",
        "dx_output" => 0.001,
        "live" => Dict(
            "dm_size" => 4,
            "dm_pitch" => 0.001,
            "entrance_size" => 4,
            "entrance_pitch" => 0.001,
        ),
        "arrays" => arrays,
        "propagations" => [
            Dict(
                "path_index" => 0,
                "anchor_index" => 0,
                "entrance" => "one",
                "prefix" => [
                    screen("pupil_opd"),
                    screen("dm_opd"),
                    Dict("kind" => "tf", "dx" => 0.001, "dz" => 0.004, "lam" => 1.55e-6),
                ],
                "closed" => [],
                "only" => [],
            ),
        ],
        "fast" => Dict(
            "resample_kernel" => "identity",
            "main_phase" => "one",
            "basis" => "scalar",
            "coefficient" => "scalar",
            "visibility" => ["scalar"],
            "pinhole_gain_real" => [0.3],
            "pinhole_gain_imag" => [0.0],
            "phantom_pad" => 0,
            "pixel_pad" => 0,
            "pixel_mtf" => "one",
            "apply_pixel_mtf" => false,
        ),
        "fast_paths" => [
            Dict(
                "source_index" => 0,
                "shift_transfer" => "one",
                "phase_multiplier" => ["one"],
            ),
        ],
    )
    modelpath = joinpath(modeldir, "model.yaml")
    YAML.write_file(modelpath, metadata)
    digest = bytes2hex(sha256(read(modelpath)))
    push!(assets, Dict("path" => "models/model.yaml", "sha256" => digest))
    grid = Dict(
        "size" => [4, 4],
        "axes" => [1, 2],
        "sampling_m" => [0.001, 0.001],
        "center_pixel" => [3.0, 3.0],
    )
    node = product.graph["nodes"][1]
    node["type"] = "compiled_optical_paths_f32"
    node["config"] = Dict(
        "model_path" => "models/model.yaml",
        "model_sha256" => digest,
        "dm_grid" => grid,
        "pupil_grid" => grid,
    )
    node["bindings"] = Dict("entrance_reference_amplitude" => "reference")
    product.bindings["reference"] = binding_file(
        directory, "reference", ones(Float32, 4, 4)
    )
    product.bindings["dm_opd"] = binding_file(directory, "dm_opd", zeros(Float32, 4, 4))
    push!(
        product.graph["inputs"],
        Dict("name" => "dm_opd", "destination" => "optics.dm_opd", "binding" => "dm_opd"),
    )
    product.graph["outputs"][1]["source"] = "optics.fringed"
    descriptor = graph_file(directory, "optical", product.graph, product.bindings)
    descriptor["assets"] = assets
    product.root["graphs"]["optical-main"] = descriptor
    return product
end

@testset "compiled model assets survive relocation and source mutation" begin
    mktempdir() do directory
        source = joinpath(directory, "source")
        mkdir(source)
        product = compiled_bundle(source)
        product.root["graphs"]["optical-copy"] = deepcopy(
            product.root["graphs"]["optical-main"]
        )
        save(product)
        relocated = joinpath(directory, "relocated")
        cp(source, relocated)
        rm(source; recursive = true)
        path = joinpath(relocated, "simulation.yaml")
        config = Simulation.load(path)
        @test length(config.graphs[Symbol("optical-main")].assets) == 4
        @test config.graphs[Symbol("optical-main")].assets[1].bytes ===
            config.graphs[Symbol("optical-copy")].assets[1].bytes
        prepared = Simulation.prepare(config)
        @test prepared.execution.sequence == 0
        @test eltype(prepared.science.sensors.camera.image) === Float32
        # Both factory and numerical prepare read the private verified model.
        write(joinpath(relocated, "models/model.yaml"), "tampered model")
        write(joinpath(relocated, "models/one.fits"), "tampered array")
        second = Simulation.prepare(config)
        @test second.science.sensors.camera.image == prepared.science.sensors.camera.image
        @test_throws PipeWireHILError Simulation.load(path)
        # No reusable definition exposes model paths after temporary cleanup.
        @test !hasproperty(prepared, :definition)
    end
    for change in (
        p -> pop!(p.root["graphs"]["optical-main"]["assets"]),
        p -> p.root["graphs"]["optical-main"]["assets"][1]["sha256"] = repeat("0", 64),
        p -> p.root["graphs"]["optical-main"]["assets"][1]["path"] = "../outside",
    )
        mktempdir() do directory
            product = compiled_bundle(directory)
            change(product)
            @test_throws PipeWireHILError Simulation.load(save(product))
        end
    end
end
end
