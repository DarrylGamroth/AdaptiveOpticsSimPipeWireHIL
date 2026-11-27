# Managed CPU source extension with an isolated private core.
module ManagedOpticalPrivateCoreTests

using Test,
    PipeWireAORTC, AdaptiveOpticsSim, AdaptiveOpticsSimPipeWireHIL, Proper, PipeWireAO
import PipeWireAO_jll
import ThreadPinning # Load the HIL extension alongside PipeWireAORTC.

include("synthetic_graphs.jl")
using .SyntheticGraphs: external_pupil_documents
include("private_core_helpers.jl")
const Owner = PipeWireAORTC.AOSOwner
const Client = PipeWireAORTC.OwnerBootstrapClient
const Codec = PipeWireAORTC.OwnerBootstrapCodec
const Source = PipeWireAORTC.SourceControl
using PipeWireAORTC.InstrumentExport: prepare_external_pupil_ports
deadline() = time_ns() / 1e9 + 180

@testset "managed optical observation contract" begin
    extension = Base.get_extension(AdaptiveOpticsSimPipeWireHIL, :PipeWireAORTCExt)
    @test extension !== nothing
    contracts = extension.OpticalObservation.contracts()
    @test isdefined(contracts, :imaging_configuration)
    @test isdefined(contracts, :port_contracts)
end

function external_pupil_delivery(remote, options)
    science = prepare_external_pupil_ports(options.plant.definition; settings = options.plant.settings)
    names = keys(science.sensors)
    formats = (
        science.pupil.shape, map(sensor -> size(sensor.image), values(science.sensors))...
    )
    schemas = (
        science.pupil.schema, map(sensor -> sensor.schema, values(science.sensors))...
    )
    peers = Tuple(
        PipeWireArrayStreamOptions(
            node_name = "test.optical.peer.$index",
            media_name = "test",
            # Fixed source and sink buffer pools must agree at link negotiation.
            buffers = index == 1 ? 2 : options.sensor_buffers,
            metadata = PipeWireAORTC.Common.ACQUISITION_METADATA,
            schema = schemas[index],
            format = NdArrayFormat(
                NdArray.F32_LE, formats[index]; layout = NdArray.ROW_MAJOR
            ),
        ) for index in 1:3
    )
    with_array_streams(peers; remote, input = (false, true, true)) do owner
        registry = with_array_stream(owner, 1) do _
            Registry(owner.core)
        end
        links = Link[]
        try
            function connect(output_name, input_name)
                ports = nothing
                _wait_for_hil_condition() do
                    roundtrip(registry)
                    output = find_globals(
                        registry;
                        interface = "PipeWire:Interface:Node",
                        properties = ("node.name" => output_name,),
                    )
                    input = find_globals(
                        registry;
                        interface = "PipeWire:Interface:Node",
                        properties = ("node.name" => input_name,),
                    )
                    length(output) == length(input) == 1 || return false
                    output_id, input_id = only(output).id, only(input).id
                    outputs = find_globals(
                        registry;
                        interface = "PipeWire:Interface:Port",
                        properties = (
                            "node.id" => string(output_id), "port.direction" => "out"
                        ),
                    )
                    inputs = find_globals(
                        registry;
                        interface = "PipeWire:Interface:Port",
                        properties = (
                            "node.id" => string(input_id), "port.direction" => "in"
                        ),
                    )
                    length(outputs) == length(inputs) == 1 || return false
                    ports = (output_id, only(outputs).id, input_id, only(inputs).id)
                    true
                end
                push!(
                    links,
                    with_array_stream(owner, 1) do _
                        create_object(
                            owner.core,
                            "link-factory",
                            Link;
                            properties = Dict(
                                "link.output.node" => string(ports[1]),
                                "link.output.port" => string(ports[2]),
                                "link.input.node" => string(ports[3]),
                                "link.input.port" => string(ports[4]),
                                "object.linger" => "false",
                            ),
                        )
                    end,
                )
            end
            connect("test.optical.peer.1", options.pupil_node)
            for (index, name) in enumerate(names)
                connect(
                    getproperty(options.sensor_nodes, name), "test.optical.peer.$(index+1)"
                )
            end
            packet = zeros(Float32, science.pupil.shape)
            packet[2, :, :] .= 1
            images = map(shape -> zeros(Float32, shape), formats[2:end])
            domain = AcquisitionDomain(ntuple(i -> UInt8(i), 16))
            for sequence in UInt64(1):UInt64(2)
                @test stage_pipewire_array!(
                    owner,
                    1,
                    packet;
                    identity = AcquisitionIdentity(domain, UInt64(1), sequence),
                    pts = Int64((sequence - 1) * 2_000_000),
                    duration = UInt64(2_000_000),
                )
                _wait_for_hil_condition(; timeout = 20) do
                    with_array_stream(trigger_process!, owner, 1)
                    array_stream_status(owner, 2).pending
                end
                receipt = drain_pipewire_array!(images[1], owner, 2)
                @test receipt.identity.sequence == sequence && receipt.duration == 2_000_000
                @test all(isfinite, images[1]) && sum(images[1]) > 0
                if sequence == 1
                    @test !array_stream_status(owner, 3).pending
                else
                    _wait_for_hil_condition(; timeout = 20) do ;
                        array_stream_status(owner, 3).pending
                    end
                    receipt = drain_pipewire_array!(images[2], owner, 3)
                    @test receipt.identity.sequence == 1 && receipt.duration == 4_000_000
                    @test all(isfinite, images[2]) && sum(images[2]) > 0
                end
            end
        finally
            with_array_stream(owner, 1) do _
                foreach(close, reverse(links))
                close(registry)
            end
        end
    end
end

@testset "managed external-pupil shutdown by $termination" for termination in (:quit, :interrupt)
    mktempdir() do directory
        (; config) = external_pupil_documents(directory)
        _with_private_core() do remote
            remote = joinpath(ENV["PIPEWIREAO_RUNTIME_DIR"], remote)
            control_node = "test.optical.control"
            bootstrap_node = "test.optical.bootstrap"
            options = merge(Owner.configuration(config), (; control_node))
            args = [
                "--config", config, "--remote", remote, "--control-node", control_node,
                "--bootstrap-node", bootstrap_node, "--bootstrap-instance", "51",
            ]
            package = pkgdir(AdaptiveOpticsSimPipeWireHIL)
            launcher = joinpath(package, "src", "managed.jl")
            project = dirname(Base.active_project())
            coverage = Base.JLOptions().code_coverage == 0 ? String[] : ["--code-coverage=@$package"]
            command = `$(Base.julia_cmd()) --project=$project --startup-file=no --threads=2,0 $coverage $launcher $args`
            startup_ready = joinpath(directory, "startup-ready")
            if termination === :interrupt
                # Julia wraps a signal delivered during a package initializer in InitError.
                source_dir = mkpath(joinpath(directory, "PipeWireAOOpticalStartup", "src"))
                write(joinpath(source_dir, "PipeWireAOOpticalStartup.jl"), """
                    module PipeWireAOOpticalStartup
                    function __init__()
                        touch(ENV["HIL_TEST_STARTUP_READY"])
                        sleep(120)
                    end
                    end
                    """)
                command = addenv(command,
                    "JULIA_LOAD_PATH" => join([directory; LOAD_PATH], ':'),
                    "PIPEWIREAO_OPTICAL_STARTUP" => "interrupt-test",
                    "HIL_TEST_STARTUP_READY" => startup_ready,
                )
            end
            log = joinpath(directory, "managed.log")
            process = run(pipeline(command; stdout = log, stderr = log); wait = false)
            client = source = nothing
            try
                client = Client.connect(
                    remote, bootstrap_node, getpid(process), 51; deadline = deadline()
                )
                @test Client.status(client; deadline = deadline()).lifecycle === Codec.Preparing
                if termination === :interrupt
                    _wait_for_hil_condition(; timeout = 30) do
                        process_exited(process) && error(read(log, String))
                        isfile(startup_ready)
                    end
                    kill(process, Base.SIGINT)
                    _wait_for_hil_condition(() -> process_exited(process); timeout = 15)
                    @test success(process)
                    @test !occursin("wrong context", read(log, String))
                    return
                end
                @test Client.connect_owner!(client; deadline = deadline()).lifecycle ===
                    Codec.Connected
                snapshot_codec = PropsBuffer(
                    Source.SNAPSHOT_NAMES,
                    Source.snapshot_values(
                        Int64(1),
                        Source.INITIAL,
                        Int64(0),
                        Int32(0),
                        Int64(1),
                        Int64(0),
                        false,
                        false,
                        Int64(1),
                        Int64(0),
                    ),
                )
                snapshot = Ref(snapshot_codec.prototype)
                # The public Node interface carries the same source PODs as a
                # camera Stream, with no invented detector or mirror port.
                source = with_thread_loop_lock(client.client.loop) do _
                    registry = something(client.client.registry)
                    object = only(
                        find_globals(
                            registry;
                            interface = "PipeWire:Interface:Node",
                            properties = ("node.name" => options.control_node,),
                        ),
                    )
                    node = bind(
                        registry,
                        object,
                        Node;
                        on_param = (node, seq, id, index, next, pod) -> begin
                            id == SPA.PARAM_PROPS &&
                                pod !== nothing &&
                                parse_props!(snapshot, snapshot_codec, pod)
                            nothing
                        end,
                    )
                    subscribe_params!(node, (SPA.PARAM_PROPS,))
                    node
                end
                function control(pod, kind, token)
                    with_thread_loop_lock(client.client.loop) do _
                        set_param!(source, SPA.PARAM_PROPS, pod)
                    end
                    _wait_for_hil_condition(; timeout = 10) do
                        process_exited(process) && error(read(log, String))
                        with_thread_loop_lock(client.client.loop) do _
                            snapshot[][3] == kind && snapshot[][4] == token
                        end
                    end
                    return with_thread_loop_lock(client.client.loop) do _
                        snapshot[]
                    end
                end
                running = control(run_control_request(1, :running), Source.RUN, 1)
                @test running[5] == 0 && running[8]
                external_pupil_delivery(remote, options)
                refused = control(reset_control_request(2), Source.RESET, 2)
                @test refused[5] < 0 && refused[6] == 1
                held = control(run_control_request(3, :stopped), Source.RUN, 3)
                @test held[5] == 0 && !held[8]
                reset = control(reset_control_request(4), Source.RESET, 4)
                @test reset[5] == 0 && reset[6] == 2 && reset[7] == 0 && !reset[8]
                with_thread_loop_lock(client.client.loop) do _
                    close(source)
                end
                source = nothing
                @test Client.quit!(client; deadline = deadline()).lifecycle ===
                    Codec.Stopped
                close(client)
                client = nothing
                _wait_for_hil_condition(() -> process_exited(process))
                @test success(process)
            finally
                client === nothing || with_thread_loop_lock(client.client.loop) do _
                    source === nothing || close(source)
                end
                client === nothing || close(client)
                _stop_hil_process(process)
                success(process) || println(stderr, read(log, String))
            end
        end
    end
end

end
