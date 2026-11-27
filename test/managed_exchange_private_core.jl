module ManagedExchangeTests

using Test, PipeWireAO, PipeWireAO_jll, PipeWireAORTC
using AdaptiveOpticsSim, AdaptiveOpticsSimPipeWireHIL
using AdaptiveOpticsSim.AlgorithmGraphs
import ThreadPinning

const HIL = AdaptiveOpticsSimPipeWireHIL
const Managed = Base.get_extension(HIL, :PipeWireAORTCExt)
const C = PipeWireAORTC.Common
const IC = PipeWireAORTC.InstrumentConfiguration
const Client = PipeWireAORTC.OwnerBootstrapClient
const Codec = PipeWireAORTC.OwnerBootstrapCodec
const Source = PipeWireAORTC.SourceControl
include("private_core_helpers.jl")
include("synthetic_graphs.jl")
using .SyntheticGraphs

bootstrap_deadline() = time_ns() / 1e9 + 180
worker_cpus() = filter(>(1), findall(!iszero, ThreadPinning.getaffinity(; cutoff=nothing)) .- 1)

function observation_core_configuration(remote)
    return replace(_private_core_configuration(remote),
        "library.use-fallback = false" => """
        library.use-fallback = false
        context.data-loops = [
            { loop.name = data-loop loop.class = data.rt loop.rt-prio = 0 }
            { loop.name = telemetry-loop loop.class = [] loop.rt-prio = 0 }
        ]
        """)
end

function with_managed_source(f, config, remote; arguments=String[], entrypoint=nothing)
    directory = dirname(config)
    package = pkgdir(HIL)
    launcher = joinpath(package, "src", "managed.jl")
    project = dirname(Base.active_project())
    control_node = "test.managed.control"
    bootstrap_node = "test.managed.bootstrap"
    absolute_remote = joinpath(ENV["PIPEWIREAO_RUNTIME_DIR"], remote)
    args = [
        "--config", config, "--remote", absolute_remote, "--control-node", control_node,
        "--bootstrap-node", bootstrap_node, "--bootstrap-instance", "51",
    ]
    coverage = Base.JLOptions().code_coverage == 0 ? String[] : ["--code-coverage=@$package"]
    entry = entrypoint === nothing ? [launcher] : ["-e", entrypoint, "--"]
    cmd = `$(Base.julia_cmd()) --project=$project --startup-file=no --threads=3,0 $coverage $entry $args $arguments`
    log = joinpath(directory, "owner.log")
    process = run(pipeline(cmd; stdout=log, stderr=log); wait=false)
    client = nothing
    try
        client = Client.connect(absolute_remote, bootstrap_node, getpid(process), 51; deadline=bootstrap_deadline())
        @test Client.connect_owner!(client; deadline=bootstrap_deadline()).lifecycle === Codec.Connected
        f(client, control_node, joinpath(dirname(absolute_remote), control_node * "-report.json"))
        @test Client.quit!(client; deadline=bootstrap_deadline()).lifecycle === Codec.Stopped
        close(client)
        client = nothing
        _wait_for_hil_condition(() -> process_exited(process); timeout=20)
        @test success(process)
    catch
        println(stderr, read(log, String))
        rethrow()
    finally
        client === nothing || close(client)
        _stop_hil_process(process)
    end
end

function with_source_controls(f, client, name)
    connection = client.client
    loop, registry = connection.loop, something(connection.registry)
    codec = PropsBuffer(Source.SNAPSHOT_NAMES, Source.snapshot_values(
        Int64(1), Source.INITIAL, Int64(0), Int32(0), Int64(1), Int64(0),
        false, false, Int64(1), Int64(0),
    ))
    observed = Ref(codec.prototype)
    node = with_thread_loop_lock(loop) do _
        object = only(find_globals(registry; interface="PipeWire:Interface:Node",
            properties=("node.name" => name,)))
        proxy = bind(registry, object, Node; on_param = (_, _, id, _, _, pod) -> begin
            id == SPA.PARAM_PROPS && pod !== nothing && parse_props!(observed, codec, pod)
            nothing
        end)
        subscribe_params!(proxy, (SPA.PARAM_PROPS,))
        proxy
    end
    snapshot() = with_thread_loop_lock(_ -> observed[], loop)
    function control(pod, kind, token)
        with_thread_loop_lock(loop) do _
            set_param!(node, SPA.PARAM_PROPS, pod)
        end
        _wait_for_hil_condition(; timeout=20) do
            fields = snapshot()
            fields[3] == kind && fields[4] == token
        end
        return snapshot()
    end
    try
        f(control, snapshot)
    finally
        with_thread_loop_lock(_ -> close(node), loop)
    end
end

function connect_ports(core, registry, output, input)
    function lookup(name, direction)
        nodes = find_globals(registry; interface="PipeWire:Interface:Node",
            properties=("node.name" => name,))
        isempty(nodes) && return nothing
        ports = find_globals(registry; interface="PipeWire:Interface:Port",
            properties=("node.id" => string(only(nodes).id), "port.direction" => direction))
        return isempty(ports) ? nothing : only(ports)
    end
    ports = nothing
    _wait_for_hil_condition() do
        roundtrip(registry)
        source, sink = lookup(output, "out"), lookup(input, "in")
        (source === nothing || sink === nothing) && return false
        ports = (source, sink)
        true
    end
    return with_thread_loop_lock(main_loop(core)) do _
        create_object(core, "link-factory", Link; properties=Dict(
            "link.output.node" => ports[1].properties["node.id"], "link.output.port" => string(ports[1].id),
            "link.input.node" => ports[2].properties["node.id"], "link.input.port" => string(ports[2].id),
            "object.linger" => "false"))
    end
end

function seal_lockstep_configuration(directory)
    environment = dirname(Base.active_project())
    for file in ("Project.toml", "Manifest.toml")
        cp(joinpath(environment, file), joinpath(directory, file))
    end
    IC.stage_simulator_preparation!(directory, PipeWireAO_jll.artifact_dir)
    C.write_json(joinpath(directory, "deployment.conf"), Dict("artifacts"=>Dict()))
    PipeWireAORTC.InstrumentExport.seal!(directory)
end

struct LockstepPeer
    frame::FilterBuffer
    correction::FilterBuffer
    received::Vector{Tuple{AcquisitionIdentity,Vector{UInt16}}}
end

function (peer::LockstepPeer)(filter::Filter, position)
    frame, correction = filter.ports
    dequeue_buffer!(peer.frame, frame) || return nothing
    try
        identity = acquisition_identity(peer.frame)
        image = copy(reinterpret(UInt16, bytes(buffer_data(peer.frame))))
        dequeue_buffer!(peer.correction, correction) || error("correction buffer unavailable")
        try
            data = buffer_data(peer.correction)
            payload = reinterpret(Float32, buffer_memory(data, 8))
            payload[1], payload[2] = 2f0, 3f0
            set_chunk!(data; size=8, stride=4)
            copy_metadata!(peer.correction, peer.frame)
            set_buffer_header!(peer.correction, buffer_header(peer.frame))
        finally
            queue_buffer!(peer.correction, correction)
        end
        push!(peer.received, (identity, image))
    finally
        queue_buffer!(peer.frame, frame)
    end
    return nothing
end

function with_lockstep_peer(f, remote, control_node, options)
    loop = ThreadLoop("managed-peer")
    context = Context(loop)
    core = CoreConnection(context; properties=Dict("remote.name"=>remote))
    registry = Registry(core)
    peer = LockstepPeer(FilterBuffer(), FilterBuffer(), [])
    filter = Filter(core, "test.lockstep.peer"; properties=Dict("node.name"=>"test.lockstep.peer"),
        on_process=peer)
    links = Link[]
    try
        for (direction, name, shape, schema) in (
            (:input, "frame", (2, 2), options.frame_schema),
            (:output, "command", (2,), options.command_schema),
        )
            element = direction === :input ? NdArray.U16_LE : NdArray.F32_LE
            format = NdArrayFormat(element, shape; layout=NdArray.ROW_MAJOR,
                rate=SPA.Fraction(options.rate, 1))
            add_port!(filter, direction; flags=FILTER_PORT_MAP_BUFFERS,
                properties=Dict("port.name"=>name), params=HIL._stream_parameters(format, schema))
        end
        precompile(peer, (typeof(filter), Union{Nothing,FilterPosition}))
        connect!(filter; flags=FILTER_INACTIVE)
        start!(loop)
        push!(links, connect_ports(core, registry, control_node, "test.lockstep.peer"))
        push!(links, connect_ports(core, registry, "test.lockstep.peer", options.command_node))
        with_thread_loop_lock(_ -> set_active!(filter, true), loop)
        f(() -> with_thread_loop_lock(_ -> copy(peer.received), loop))
    finally
        with_thread_loop_lock(loop) do _
            foreach(close, reverse(links))
            close(filter)
            close(registry)
            close(core)
            close(context)
        end
        close(loop)
    end
end

@testset "managed lockstep applies one correction per frame and fences reset" begin
    mktempdir() do directory
        (; config, basis) = lockstep_documents(directory)
        options = PipeWireAORTC.AOSOwner.configuration(config)
        @test_throws ArgumentError HIL.Simulation.prepare_lockstep_startup(config)
        seal_lockstep_configuration(directory)
        @test HIL.Simulation.prepare_lockstep_startup(config) === nothing
        phase = mkpath(joinpath(directory, "phase"))
        arguments = ["--phase-timing-directory", phase, "--phase-timing-frames", "2",
            "--phase-timing-warmup", "0"]
        _with_private_core() do remote
            with_managed_source(config, remote; arguments) do client, node, report
                with_lockstep_peer(remote, node, options) do received
                    with_source_controls(client, node) do control, snapshot
                        for generation in 1:2
                            token = 2generation - 1
                            ack = control(run_control_request(token, :running), Source.RUN, token)
                            @test ack[5] == 0
                            _wait_for_hil_condition(; timeout=20) do
                                result = C.read_json(report)
                                result["generation"] == generation && result["completed"]
                            end
                            result = C.read_json(report)
                            @test result["sequence"] == 2 && !result["running"]
                            frames = last(received(), 2)
                            @test [id.sequence for (id, _) in frames] == [1, 2]
                            @test all(id.generation == generation for (id, _) in frames)
                            @test frames[1][2] == zeros(UInt16, 4)
                            # The FITS basis varies along both axes. Check row-major delivery
                            # and that frame two, but not frame one, used the preceding command.
                            expected = 2 .* basis[:, :, 1] .+ 3 .* basis[:, :, 2]
                            expected[1, 2] = 0
                            @test frames[2][2] == UInt16.(vec(permutedims(expected)))
                            timing = C.read_json(joinpath(phase, "phase-timing-$generation.json"))
                            @test timing["record_count"] == 2
                            @test [row[2] for row in timing["rows"]] == [1, 2]
                            if generation == 1
                                reset = control(reset_control_request(2), Source.RESET, 2)
                                @test reset[5] == 0 && reset[6] == 2 && reset[7] == 0
                            end
                        end
                    end
                end
            end
        end
    end
end

@testset "managed free-running camera adopts received corrections while stepping" begin
    mktempdir() do directory
        (; config, basis) = lockstep_documents(directory; total_exchanges=8)
        options = PipeWireAORTC.AOSOwner.configuration(config)
        _with_private_core() do remote
            with_managed_source(config, remote; arguments=["--acquisition", "free-running"]) do client, node, report
                with_lockstep_peer(remote, node, options) do received
                    with_source_controls(client, node) do control, _
                        @test control(run_control_request(1, :running), Source.RUN, 1)[5] == 0
                        _wait_for_hil_condition(; timeout=20) do
                            C.read_json(report)["completed"]
                        end
                        @test C.read_json(report)["sequence"] == 8
                        frames = received()
                        @test first(frames)[2] == zeros(UInt16, 4)
                        expected = 2 .* basis[:, :, 1] .+ 3 .* basis[:, :, 2]
                        expected[1, 2] = 0
                        @test any(frame -> frame[2] == UInt16.(vec(permutedims(expected))), frames)
                    end
                end
            end
        end
    end
end

include("managed_pupil_publication.jl")
include("managed_optical_diagnostics.jl")
include("managed_clocked_optical.jl")

end
