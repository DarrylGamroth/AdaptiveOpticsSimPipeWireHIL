# Run this opt-in test in a private network and shared-memory namespace.
using Proper
include("simulation_optical.jl")

module HeartExecutorTests

using Test, Sockets, FITSIO
using YAML: YAML
using PipeWireAO, PipeWireAO_jll
using AdaptiveOpticsSim.AlgorithmGraphs: optical_sampling_m, prepared_graph_node
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil, array_stream_status,
    with_array_stream
using ..OpticalSimulationTests: clocked_bundle, save, binding_file, graph_file, add_response!

include("private_core_helpers.jl")
include("heart_helpers.jl")

include("heart_executor.jl")

const FRAME_COUNT = 6

function exchange_frames(prepared, remote)
    loop = ThreadLoop("heart-executor-test-links")
    context = Context(loop)
    core = CoreConnection(context; properties=Dict("remote.name" => remote))
    registry = Registry(core)
    start!(loop)
    links = Link[]
    frames = Matrix{UInt16}[]
    commands = Vector{Float32}[]
    opened = nothing
    rejected = 0
    try
        completed = Simulation.run!(
            prepared; frames=FRAME_COUNT * STEPS_PER_EXPOSURE + 1
        ) do execution, streams, _
            if isempty(links)
                opened = streams
                link_heart!(links, loop, core, registry, streams)
                rejected = array_stream_status(streams, 1).rejected
            end
            sequence = ExternalPupil.snapshot(execution).sequence
            if sequence > 0 && sequence % STEPS_PER_EXPOSURE == 0
                push!(frames, copy(only(execution.buffers)))
                push!(commands, copy(execution.command))
                _wait_for_hil_condition() do
                    status = array_stream_status(streams, 1)
                    status.failed && error("HEART command transport failed")
                    # The DM source flags the first packet as discontinuous.
                    status.pending || (length(frames) == 1 && status.rejected > rejected)
                end
                if length(frames) == FRAME_COUNT
                    @test array_stream_status(streams, 1).rejected == rejected + 1
                    close_heart_links!(links, loop)
                    roundtrip(registry)
                    return ExternalPupil.Stop()
                end
            end
            return ExternalPupil.Run(0)
        end
        @test completed == FRAME_COUNT * STEPS_PER_EXPOSURE
        @test opened.closed && !isopen(opened.core)
        return (; frames, commands)
    finally
        close_heart_links!(links, loop)
        with_thread_loop_lock(loop) do _
            close(registry)
            close(core)
            close(context)
        end
        stop!(loop)
        close(loop)
    end
end

@testset "full HEART rtcExec roundtrip through standalone HIL" begin
    # The command and telemetry ports are private to the outer network namespace.
    addresses = getipaddrs()
    @test isempty(addresses)
    isempty(addresses) || error("run this test in the loopback-only namespace")
    binary = abspath(ENV["HEART_TEST_EXECUTOR"])
    cli = abspath(ENV["HEART_TEST_CLI"])
    plugins = abspath(ENV["HEART_TEST_SPA_DIR"])
    @test isfile(joinpath(plugins, "heart", "libspa-heart.so"))
    configuration_for(remote) = core_configuration(
        remote, DM_PORT, WFS_PORT; pixel_type="raw", target=0, actuators=2,
        exposure_ns=RTC_EXPOSURE_NS, wfs_source=0,
    )
    _with_private_core(; configuration_for, spa_plugin_dir=plugins) do remote
        mktempdir() do directory
            configuration = heart_bundle(directory, remote; frame_encoding=:uint16,
                actuators=2, exposure_ns=RTC_EXPOSURE_NS)
            prepared = Simulation.prepare(configuration)
            with_heart_executor(directory, binary, cli) do
                result = exchange_frames(prepared, remote)
                @test length(result.frames) == FRAME_COUNT
                @test any(command -> any(!iszero, command), result.commands)
                @test any(command -> first(command) != last(command), result.commands)
                @test any(frame -> frame != first(result.frames), result.frames)
                @test any(frame -> frame != permutedims(frame), result.frames)
                received = dump_buffer(directory, cli, "HOP0.wfs.pixelsRaw")
                @test eltype(received) === UInt16
                @test received == cat(permutedims.(result.frames)...; dims=3)
                sent = dump_buffer(directory, cli, "WCC.dm0.dmOutput")
                @test size(sent) == (1, 2, FRAME_COUNT)
                sent = dropdims(sent; dims=1)
                # The DM source marks the first publication discontinuous.
                # The next WFS exposure uses the most recent admitted command.
                @test all(iszero, result.commands[1]) && all(iszero, result.commands[2])
                @test hcat(result.commands[3:end]...) ≈ sent[:, 2:(end - 1)] .* 1f-6
            end
        end
    end
end

end
