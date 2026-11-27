# Run with --threads=3,1 and the matching PIPEWIREAO_HIL_TEST_PREFIX.
# Uses only an isolated private core and a direct ndarray loopback link.
isdefined(@__MODULE__, :SimulationTests) || include("simulation.jl")

module SimulationPrivateCoreTests

using Test
using PipeWireAO
using PipeWireAO_jll: PipeWireAO_jll
using AdaptiveOpticsSimPipeWireHIL: Simulation
using ..SimulationTests: write_bundle, save

include("private_core_helpers.jl")

function loopback(transport)
    loop = ThreadLoop("simulation-test-links")
    context = Context(loop)
    core = CoreConnection(
        context; properties = Dict("remote.name" => transport.configuration.remote)
    )
    registry = Registry(core)
    start!(loop)
    try
        ports = nothing
        _wait_for_hil_condition() do
            roundtrip(registry)
            outputs = find_globals(
                registry;
                interface = "PipeWire:Interface:Port",
                properties = (
                    "node.id" => string(node_id(transport.frame_stream)),
                    "port.direction" => "out",
                ),
            )
            inputs = find_globals(
                registry;
                interface = "PipeWire:Interface:Port",
                properties = (
                    "node.id" => string(node_id(transport.command_stream)),
                    "port.direction" => "in",
                ),
            )
            isempty(outputs) || isempty(inputs) || (ports = (only(outputs), only(inputs)))
            return ports !== nothing
        end
        output, input = ports
        link = with_thread_loop_lock(loop) do _
            create_object(
                core,
                "link-factory",
                Link;
                properties = Dict(
                    "link.output.node" => string(node_id(transport.frame_stream)),
                    "link.output.port" => string(output.id),
                    "link.input.node" => string(node_id(transport.command_stream)),
                    "link.input.port" => string(input.id),
                    "object.linger" => "false",
                ),
            )
        end
        return (; loop, context, core, registry, link)
    catch
        with_thread_loop_lock(loop) do _
            close(registry)
            close(core)
            close(context)
        end
        stop!(loop)
        close(loop)
        rethrow()
    end
end

function close_links(owner)
    with_thread_loop_lock(owner.loop) do _
        close(owner.link)
        close(owner.registry)
        close(owner.core)
        close(owner.context)
    end
    stop!(owner.loop)
    close(owner.loop)
    return nothing
end

@testset "standalone finite transport and control teardown" begin
    _with_private_core() do remote
        mktempdir() do directory
            product = write_bundle(directory)
            product.manifest["pipewire"]["remote"] = remote
            configuration = Simulation.load(save(product))
            for decision in (:stop, :fail, :control_fail, :run)
                prepared = Simulation.prepare(configuration)
                opened = nothing
                link = nothing
                calls = 0
                try
                    control = function (science, transport)
                        opened = transport
                        calls += 1
                        if calls == 1
                            @test !transport.started
                            decision === :fail && error("admission failed")
                            decision === :stop && return false
                            decision === :run && (link = loopback(transport))
                        end
                        decision === :control_fail && calls == 2 && error("control failed")
                        return true
                    end
                    if decision in (:fail, :control_fail)
                        @test_throws ErrorException Simulation.run!(
                            control, prepared; frames = 3
                        )
                    else
                        completed = Simulation.run!(control, prepared; frames = 3)
                        @test completed == (decision === :run ? 3 : 0)
                        @test calls == (decision === :run ? 4 : 1)
                    end
                    @test opened.closed
                    @test !isopen(opened.core)
                finally
                    link === nothing || close_links(link)
                end
            end
        end
    end
end

end
