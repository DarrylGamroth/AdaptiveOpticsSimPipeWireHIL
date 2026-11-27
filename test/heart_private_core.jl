# Opt-in loopback UDP interoperability test.
using Proper
include("simulation_optical.jl")

module HeartPrivateCoreTests

using Test, Sockets, Libdl
using YAML: YAML
using PipeWireAO, PipeWireAO_jll
using AdaptiveOpticsSim.AlgorithmGraphs: optical_sampling_m, prepared_graph_node
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil, array_stream_status,
    with_array_stream
using ..OpticalSimulationTests: clocked_bundle, save, binding_file, graph_file, add_response!

include("private_core_helpers.jl")

include("heart_helpers.jl")

function send_command(socket, encoder, port, id, microns)
    packet = Vector{UInt8}(undef, 128)
    size = ccall(encoder, Cint, (Ptr{UInt8}, Csize_t, UInt32, Cfloat),
        packet, length(packet), id, microns)
    size > 0 || error("HEART command encoding failed")
    resize!(packet, size)
    send(socket, ip"127.0.0.1", port, packet)
end

function read_pixels(socket, decoder, id)
    pixels = fill(Float32(NaN), Int(ROWS) * Int(COLUMNS))
    timestamps = UInt64[]
    for segment in 1:ROWS
        task = @async recv(socket)
        _wait_for_hil_condition(() -> istaskdone(task))
        packet = fetch(task)
        timestamp = Ref{UInt64}()
        status = ccall(decoder, Cint,
            (Ptr{UInt8}, Csize_t, Ptr{Float32}, Cushort, Cushort, Cushort, UInt32, Ref{UInt64}),
            packet, length(packet), pixels, ROWS, COLUMNS, segment, id, timestamp)
        status == 0 || error("HEART rejected WFS frame $id segment $segment")
        push!(timestamps, timestamp[])
    end
    @test first(timestamps) > 0 && all(==(first(timestamps)), timestamps)
    return pixels
end

function roundtrip_heart(remote, configuration, socket, dm_port, encoder, decoder)
    prepared = Simulation.prepare(configuration)
    oracle = Simulation.prepare(configuration)
    commands = (0.0f0, 0.3f0)
    expected = Vector{Float32}[]
    for microns in commands
        emit = (_, image, _) -> push!(expected, vec(permutedims(image)))
        for _ in 1:STEPS_PER_EXPOSURE
            ExternalPupil.step_clocked_source!(
                emit, oracle.science, [Float32(Float64(microns) * 1e-6)]; generation=UInt64(1),
            )
        end
    end
    @test length(expected) == 2
    @test !isapprox(expected[1], expected[2])

    loop = ThreadLoop("heart-test-links")
    context = Context(loop)
    core = CoreConnection(context; properties=Dict("remote.name" => remote))
    registry = Registry(core)
    start!(loop)
    links = Link[]
    frame = 0
    expected_steps = length(commands) * STEPS_PER_EXPOSURE
    opened = nothing
    try
        completed = Simulation.run!(prepared; frames=expected_steps + 1) do execution, streams, _
            if isempty(links)
                opened = streams
                link_heart!(links, loop, core, registry, streams)
                # The first command is marked discontinuous on startup.
                send_command(socket, encoder, dm_port, 1, 0.0f0)
                _wait_for_hil_condition(() -> array_stream_status(streams, 1).rejected == 1)
            end
            sequence = ExternalPupil.snapshot(execution).sequence
            if sequence % STEPS_PER_EXPOSURE == 0
                if sequence > 0
                    pixels = read_pixels(socket, decoder, frame)
                    @test pixels ≈ expected[frame]
                end
                if frame == length(commands)
                    status = array_stream_status(streams, 1)
                    @test status.rejected == 1
                    @test !status.failed
                    close_heart_links!(links, loop)
                    roundtrip(registry)
                    return ExternalPupil.Stop()
                end
                frame += 1
                send_command(socket, encoder, dm_port, frame + 1, commands[frame])
                _wait_for_hil_condition(() -> array_stream_status(streams, 1).pending)
            end
            return ExternalPupil.Run(0)
        end
        @test completed == expected_steps
        @test opened.closed
        @test !isopen(opened.core)
    finally
        with_thread_loop_lock(loop) do _
            foreach(close, reverse(links))
            close(registry)
            close(core)
            close(context)
        end
        stop!(loop)
        close(loop)
    end
end

function bind_loopback(socket)
    for _ in 1:100
        port = rand(49152:65535)
        Sockets.bind(socket, ip"127.0.0.1", port) && return port
    end
    error("could not reserve a loopback UDP port")
end

@testset "HEART Standard DM and WFS roundtrip through standalone HIL" begin
    heart = joinpath(ENV["HEART_TEST_SOURCE"], "source", "device")
    plugins = abspath(ENV["HEART_TEST_SPA_DIR"])
    @test isfile(joinpath(plugins, "heart", "libspa-heart.so"))
    mktempdir() do directory
        sources = [
            joinpath(heart, "src", "hrtStd$name.c") for name in ("Dm", "Wfs", "Checksum")
        ]
        library = joinpath(directory, "libheart-wire.so")
        include_dir = joinpath(heart, "include")
        peer_source = joinpath(@__DIR__, "heart_wire.c")
        run(`cc -std=gnu11 -shared -fPIC -I$include_dir -o $library $peer_source $sources`)
        handle = dlopen(library)
        socket, reservation = UDPSocket(), UDPSocket()
        try
            wfs_port, dm_port = bind_loopback(socket), bind_loopback(reservation)
            close(reservation)
            configuration_for(remote) = core_configuration(remote, dm_port, wfs_port)
            _with_private_core(; configuration_for, spa_plugin_dir=plugins) do remote
                configuration = heart_bundle(directory, remote)
                roundtrip_heart(
                    remote, configuration, socket, dm_port,
                    dlsym(handle, :heart_dm_command), dlsym(handle, :heart_wfs_pixels),
                )
            end
        finally
            close(reservation)
            close(socket)
            dlclose(handle)
        end
    end
end

end
