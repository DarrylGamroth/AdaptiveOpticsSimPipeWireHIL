module SimulationTests

using Test
using FITSIO: FITS
using SHA: sha256
import YAML
using AdaptiveOpticsSim.AlgorithmGraphs
import AdaptiveOpticsSimPipeWireHIL as HIL
using AdaptiveOpticsSimPipeWireHIL: Simulation, PipeWireHILError

struct StandaloneTransport
    events::Vector{Symbol}
end
HIL.prepare_pipewire_hil(transport::StandaloneTransport, ::Nothing) = transport
HIL.start!(transport::StandaloneTransport) = push!(transport.events, :start)
function HIL.exchange_frame!(transport::StandaloneTransport, ::Nothing; idle_strategy)
    @test idle_strategy === HIL.SimRunner.current_strategy()
    push!(transport.events, :exchange)
    return nothing
end
Base.close(transport::StandaloneTransport) = push!(transport.events, :close)

@testset "standalone lockstep policy and finite controls" begin
    withenv("PIPEWIREAO_SIM_IDLE_STRATEGY" => "busy-spin") do
        transport = StandaloneTransport(Symbol[])
        prepared = (; boundary = transport, configuration = nothing, driver = nothing)
        owner = current_task()
        completed = Simulation.run!(prepared; frames = 2) do science, streams
            @test current_task() === owner
            @test science === prepared && streams === transport
            @test HIL.SimRunner.name(HIL.SimRunner.current_strategy()) == "busy-spin"
            push!(streams.events, :control)
            return true
        end
        @test completed == 2
        @test transport.events == [:control, :start, :control, :exchange, :control, :exchange, :close]

        empty!(transport.events)
        yielding = HIL.SimRunner.strategy("yielding")
        @test_throws ErrorException Simulation.run!(prepared; frames = 2, idle_strategy = yielding) do _, streams
            @test HIL.SimRunner.current_strategy() === yielding
            push!(streams.events, :control)
            :start in streams.events && error("control failed")
            return true
        end
        @test transport.events == [:control, :start, :control, :close]

        empty!(transport.events)
        HIL.SimRunner.run!(yielding) do
            completed = Simulation.run!(prepared; frames = 2) do _, streams
                @test HIL.SimRunner.current_strategy() === yielding
                push!(streams.events, :control)
                return false
            end
            @test completed == 0
            return nothing
        end
        @test transport.events == [:control, :close]
    end
end

function write_bundle(directory; command_input = "command")
    graph = Dict(
        "schema_version" => 1,
        "name" => "response",
        "nodes" => [
            Dict(
                "name" => "response",
                "type" => "second_order_response_f32",
                "config" => Dict(
                    "natural_frequency_hz" => 20.0,
                    "damping_ratio" => 0.7,
                    "sample_period_s" => 0.001,
                    "count" => 2,
                    "schema" => "test.response/1",
                ),
            ),
        ],
        "inputs" => [
            Dict(
                "name" => "command",
                "destination" => "response.input",
                "binding" => "command",
            ),
        ],
        "outputs" => [Dict("name" => "frame", "source" => "response.output")],
    )
    graphpath = joinpath(directory, "graph.yaml")
    YAML.write_file(graphpath, graph)
    bindingpath = joinpath(directory, "command.fits")
    FITS(bindingpath, "w") do fits
        return write(fits, zeros(Float32, 2))
    end
    manifest = Dict(
        "schema_version" => 1,
        "graph" => Dict(
            "path" => "graph.yaml",
            "sha256" => bytes2hex(sha256(read(graphpath))),
            "bindings" => Dict(
                "command" => Dict(
                    "path" => "command.fits",
                    "sha256" => bytes2hex(sha256(read(bindingpath))),
                    "shape" => [2],
                    "eltype" => "Float32",
                ),
            ),
        ),
        "hil" => Dict(
            "command_input" => command_input,
            "frame_output" => "frame",
            "period_ns" => 1_000_000,
        ),
        "pipewire" => Dict(
            "remote" => "unopened-test-core",
            "frame_node_name" => "simulation-frame",
            "command_node_name" => "simulation-command",
            "frame_schema" => "test.response/1",
            "command_schema" => "test.response/1",
            "rate" => [1000, 1],
            "exposure_duration_ns" => 1_000_000,
        ),
    )
    manifestpath = joinpath(directory, "simulation.yaml")
    YAML.write_file(manifestpath, manifest)
    return (; manifest, manifestpath, graph, graphpath, bindingpath)
end

function save(product)
    YAML.write_file(product.manifestpath, product.manifest)
    return product.manifestpath
end

@testset "standalone numerical preparation and relocation" begin
    mktempdir() do directory
        product = write_bundle(directory)
        configuration = Simulation.load(product.manifestpath)
        @test configuration.period_ns == 1_000_000
        prepared = Simulation.prepare(configuration)
        @test prepared.configuration.remote == "unopened-test-core"
        @test prepared.boundary.frame_output === graph_output(prepared.graph, :frame)
        @test graph_step_sequence(prepared.graph) == 0
        @test hil_command_buffer(prepared.boundary) == zeros(Float32, 2)
        @test step_hil_frame!(prepared.boundary) == 1
        @test all(iszero, hil_frame_buffer(prepared.boundary))
        copyto!(hil_command_buffer(prepared.boundary), Float32[1, -2])
        adopt_hil_command!(prepared.boundary, UInt64(1))
        @test step_hil_frame!(prepared.boundary) == 2
        @test any(!iszero, hil_frame_buffer(prepared.boundary))
        reset_hil_boundary!(prepared.boundary)
        @test all(iszero, hil_command_buffer(prepared.boundary))
        # Distinct preparations retain distinct numerical input ownership.
        second = Simulation.prepare(configuration)
        fill!(hil_command_buffer(prepared.boundary), 3)
        @test all(iszero, hil_command_buffer(second.boundary))
        @test all(iszero, configuration.bindings.command)
        @test_throws PipeWireHILError Simulation.run!((_, _) -> true, second; frames = 0)
        @test_throws PipeWireHILError Simulation.run!((_, _) -> true, second; frames = true)
        # A verified configuration owns its snapshot, not paths to mutable inputs.
        write(product.graphpath, "changed after load")
        @test graph_step_sequence(Simulation.prepare(configuration).graph) == 0
        @test_throws PipeWireHILError Simulation.load(product.manifestpath)
    end
    mktempdir() do directory
        source = joinpath(directory, "source")
        destination = joinpath(directory, "relocated")
        mkdir(source)
        product = write_bundle(source)
        cp(source, destination)
        rm(source; recursive = true)
        prepared = Simulation.prepare(
            Simulation.load(joinpath(destination, "simulation.yaml"))
        )
        @test step_hil_frame!(prepared.boundary) == 1
        @test all(iszero, hil_frame_buffer(prepared.boundary))
    end
end

@testset "bounded manifest and exact assets" begin
    changes = (
        p -> p.manifest["schema_version"] = true,
        p -> p.manifest["schema_version"] = 2,
        p -> p.manifest["mode"] = "clocked-optical",
        p -> p.manifest["unexpected"] = "field",
        p -> p.manifest["graph"]["path"] = p.graphpath,
        p -> p.manifest["graph"]["sha256"] = repeat("0", 64),
        p -> p.manifest["graph"]["path"] = "missing.yaml",
        p -> p.manifest["graph"]["bindings"]["command"]["shape"] = [3],
        p -> p.manifest["graph"]["bindings"]["command"]["shape"] = [true],
        p -> p.manifest["graph"]["bindings"]["command"]["shape"] = [typemax(Int)],
        p -> p.manifest["graph"]["bindings"]["command"]["eltype"] = "Float64",
        p -> p.manifest["graph"]["bindings"]["command"]["eltype"] = "Float16",
        p -> p.manifest["graph"]["bindings"]["command"]["sha256"] = repeat("0", 64),
        p -> p.manifest["hil"]["period_ns"] = 0,
        p -> p.manifest["hil"]["command_input"] = ["command", "command"],
        p -> p.manifest["pipewire"]["rate"] = [1000, 0],
        p -> p.manifest["pipewire"]["rate"] = Any[true, 1],
        p -> p.manifest["pipewire"]["command_scale"] = -1,
    )
    for change in changes
        mktempdir() do directory
            product = write_bundle(directory)
            change(product)
            @test_throws PipeWireHILError Simulation.load(save(product))
        end
    end
    mktempdir() do directory
        bundle = joinpath(directory, "bundle")
        mkdir(bundle)
        product = write_bundle(bundle)
        outside = joinpath(directory, "outside.yaml")
        cp(product.graphpath, outside)
        product.manifest["graph"]["path"] = "../outside.yaml"
        @test_throws PipeWireHILError Simulation.load(save(product))
        symlink(outside, joinpath(bundle, "escaped.yaml"))
        product.manifest["graph"]["path"] = "escaped.yaml"
        @test_throws PipeWireHILError Simulation.load(save(product))
    end
    mktempdir() do directory
        product = write_bundle(directory)
        write(
            product.manifestpath, read(product.manifestpath, String) * "schema_version: 1\n"
        )
        @test_throws PipeWireHILError Simulation.load(product.manifestpath)
        YAML.write_file(product.manifestpath, product.manifest)
        write(
            product.manifestpath,
            read(product.manifestpath, String) * "---\nschema_version: 1\n",
        )
        @test_throws PipeWireHILError Simulation.load(product.manifestpath)
    end
end

@testset "declared rate and named command fields" begin
    mktempdir() do directory
        product = write_bundle(directory; command_input = ["command"])
        wire = product.manifest["pipewire"]
        wire["command_node_name"] = Dict("command" => "simulation-command")
        wire["command_schema"] = Dict("command" => "test.response/1")
        wire["command_scale"] = Dict("command" => 1.0)
        wire["rate"] = [30, 1]
        product.manifest["hil"]["period_ns"] = round(Int, Float64(Float32(1 / 30)) * 1e9)
        product.graph["nodes"][1]["config"]["sample_period_s"] = Float64(Float32(1 / 30))
        YAML.write_file(product.graphpath, product.graph)
        product.manifest["graph"]["sha256"] = bytes2hex(sha256(read(product.graphpath)))
        configuration = Simulation.load(save(product))
        @test configuration.pipewire.rate.num == 30
        @test configuration.command_input == (:command,)
        @test configuration.pipewire.command_scale == (command = 1.0f0,)
        @test hil_command_buffer(Simulation.prepare(configuration).boundary).command ==
            zeros(Float32, 2)
        wire["command_schema"] = Dict("other" => "test.response/1")
        @test_throws PipeWireHILError Simulation.load(save(product))
    end
end

@testset "numerical node periods match model cadence" begin
    mktempdir() do directory
        product = write_bundle(directory)
        product.manifest["hil"]["period_ns"] = 10_000_000
        @test_throws PipeWireHILError Simulation.prepare(Simulation.load(save(product)))
        product.manifest["hil"]["period_ns"] = 1_000_000
        @test graph_step_sequence(
            Simulation.prepare(Simulation.load(save(product))).graph
        ) == 0
        atmosphere = Dict(
            "name" => "atmosphere",
            "type" => "multilayer_atmosphere_opd_f32",
            "config" => Dict(
                "resolution" => 8,
                "telescope_diameter_m" => 1.0,
                "r0" => 0.15,
                "reference_wavelength_m" => 500e-9,
                "fractional_cn2" => [1.0],
                "wind_speed" => [0.0],
                "wind_direction_deg" => [0.0],
                "altitude" => [0.0],
                "layer_ids" => ["ground"],
                "atmosphere_step" => 0.01,
                "rng_seed" => 1,
                "atmosphere_opd_schema" => "test.atmosphere/1",
            ),
        )
        push!(product.graph["nodes"], atmosphere)
        YAML.write_file(product.graphpath, product.graph)
        product.manifest["graph"]["sha256"] = bytes2hex(sha256(read(product.graphpath)))
        @test_throws PipeWireHILError Simulation.prepare(Simulation.load(save(product)))
        atmosphere["config"]["atmosphere_step"] = 0.001
        YAML.write_file(product.graphpath, product.graph)
        product.manifest["graph"]["sha256"] = bytes2hex(sha256(read(product.graphpath)))
        @test graph_step_sequence(
            Simulation.prepare(Simulation.load(save(product))).graph
        ) == 0
    end
end

@testset "logical FITS axes and Bool codes" begin
    mktempdir() do directory
        product = write_bundle(directory)
        values = reshape(Float32.(1:6), 2, 3)
        FITS(product.bindingpath, "w") do fits
            write(fits, values)
        end
        entry = product.manifest["graph"]["bindings"]["command"]
        entry["shape"] = [2, 3]
        entry["sha256"] = bytes2hex(sha256(read(product.bindingpath)))
        @test Simulation.load(save(product)).bindings.command == values
        for (codes, valid) in ((UInt8[0, 1], true), (UInt8[0, 2], false))
            FITS(product.bindingpath, "w") do fits
                write(fits, codes)
            end
            entry["shape"] = [2]
            entry["eltype"] = "Bool"
            entry["sha256"] = bytes2hex(sha256(read(product.bindingpath)))
            if valid
                @test Simulation.load(save(product)).bindings.command == Bool[0, 1]
            else
                @test_throws PipeWireHILError Simulation.load(save(product))
            end
        end
    end
end

@testset "compiled optical assets fail before graph construction" begin
    mktempdir() do directory
        product = write_bundle(directory)
        product.graph["nodes"][1]["type"] = "compiled_optical_paths_f32"
        YAML.write_file(product.graphpath, product.graph)
        product.manifest["graph"]["sha256"] = bytes2hex(sha256(read(product.graphpath)))
        @test_throws PipeWireHILError Simulation.load(save(product))
    end
end

end
