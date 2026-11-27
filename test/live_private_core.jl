using AdaptiveOpticsSim.AlgorithmGraphs
import PipeWireAO_jll
include("live_image_private_core.jl")
include("live_array_private_core.jl")

mutable struct LiveAppliedCapture
    buffer::StreamBuffer
    values::Vector{Float32}
    header::BufferHeader
    sequence::Base.Threads.Atomic{UInt64}
end
function (capture::LiveAppliedCapture)(stream)
    dequeue_buffer!(capture.buffer, stream) || return nothing
    try
        capture.header = something(buffer_header(capture.buffer))
        copyto!(capture.values, reinterpret(Float32, bytes(buffer_data(capture.buffer))))
        capture.sequence[] = capture.header.sequence
    finally
        queue_buffer!(capture.buffer, stream)
    end
    return nothing
end

function _test_applied_native_callback_allocation!(source, state)
    process = source.stream.callbacks.on_process
    process(source.stream)
    @test !source.state.pending
    AdaptiveOpticsSimPipeWireHIL._stage_applied_command!(source, state, UInt64(3))
    @test (@allocated process(source.stream)) == 0
    @test !source.state.pending # The measured call actually queued a full buffer.
    return nothing
end

function _test_applied_live_delivery(prepared)
    capture = LiveAppliedCapture(StreamBuffer(), zeros(Float32, 277),
        BufferHeader(0, 0, 0, 0, 0), Base.Threads.Atomic{UInt64}(0))
    source = prepared.applied_command_source
    sink = registry = link = nothing
    try
        sink, registry = with_thread_loop_lock(prepared.loop) do _
            sink = Stream(prepared.core, "test-applied-command-observer";
                properties=Dict("node.name" => "test-applied-command-observer",
                    "node.want-driver" => "true", "priority.driver" => "100"),
                on_process=capture)
            precompile(capture, (typeof(sink),))
            connect!(sink, :input;
                flags=STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT | STREAM_DRIVER,
                params=(ndarray_format(source.format; schema=APPLIED_PDM_COMMAND_SCHEMA),
                    Pod(buffers_param(size=277 * 4, buffers=2, metadata_types=Int32(
                        (1 << PipeWireAO.LibPipeWire.SPA_META_Header)))),
                    Pod(header_metadata_param()),Pod(metadata_param(MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP;required=true)))))
            return sink, Registry(prepared.core)
        end
        _wait_for_hil_condition() do
            roundtrip(registry)
            outputs = find_globals(registry; interface="PipeWire:Interface:Port",
                properties=("node.id" => string(node_id(source.stream)), "port.direction" => "out"))
            inputs = find_globals(registry; interface="PipeWire:Interface:Port",
                properties=("node.id" => string(node_id(sink)), "port.direction" => "in"))
            return !isempty(outputs) && !isempty(inputs)
        end
        output = only(find_globals(registry; interface="PipeWire:Interface:Port",
            properties=("node.id" => string(node_id(source.stream)), "port.direction" => "out")))
        input = only(find_globals(registry; interface="PipeWire:Interface:Port",
            properties=("node.id" => string(node_id(sink)), "port.direction" => "in")))
        @test output.properties["port.name"] == "output_1"
        link = with_thread_loop_lock(prepared.loop) do _
            link = create_object(prepared.core, "link-factory", Link; properties=Dict(
                "link.output.node" => string(node_id(source.stream)), "link.output.port" => string(output.id),
                "link.input.node" => string(node_id(sink)), "link.input.port" => string(input.id),
                "object.linger" => "false"))
            set_active!(sink, true)
            return link
        end
        _wait_for_hil_condition() do
            with_thread_loop_lock(prepared.loop) do _
                stream_state(sink)
                return is_driving(sink) &&
                    stream_state(source.stream) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING
            end
        end
        with_thread_loop_lock(prepared.loop) do _
            _test_applied_native_callback_allocation!(source, prepared.state)
        end
        _wait_for_hil_condition() do
            with_thread_loop_lock(prepared.loop) do _
                capture.sequence[] == 3 && return true
                trigger_process!(sink)
                return false
            end
        end
        @test capture.header.sequence == 3
        @test capture.header.pts == 300
        @test capture.values == fill(2.0f0, 277)
        @test !source.state.failed[]
        @test !source.state.pending
    finally
        with_thread_loop_lock(prepared.loop) do _
            link === nothing || close(link)
            sink === nothing || close(sink)
            registry === nothing || close(registry)
        end
    end
    return nothing
end

include("private_core_helpers.jl")

# Transport-only command-copy fixture; no RTC or instrument model is qualified.
struct LiveTransportTestDeclaration end
struct LiveTransportTestOwner{Input,Output}
    command::Input
    frame::Output
end

function AlgorithmGraphs.graph_node_ports(::Type{LiveTransportTestDeclaration}, extent::Int)
    return (
        AlgorithmGraphs.graph_port_contract(:command, :input, :data, Float32, (extent,),
            "org.adaptiveopticssim.test.command/1", :column_major),
        AlgorithmGraphs.graph_port_contract(:frame, :output, :data, Float32, (extent,),
            "org.adaptiveopticssim.test.frame/1", :column_major),
    )
end

function AlgorithmGraphs.prepare_graph_node(
    ::Type{LiveTransportTestDeclaration}, extent::Int, props,
    inputs::NamedTuple, outputs::NamedTuple, parameters::NamedTuple, target,
)
    return LiveTransportTestOwner(inputs.command, outputs.frame)
end

function AlgorithmGraphs.step_graph_node!(owner::LiveTransportTestOwner)
    copyto!(owner.frame, owner.command)
    return nothing
end
AlgorithmGraphs.reset_graph_node!(::LiveTransportTestOwner) = nothing

function _test_hil_boundary(extent=2; command=zeros(Float32, extent))
    node = algorithm_node(:plant, LiveTransportTestDeclaration, extent)
    graph = prepare_algorithm_graph(algorithm_graph(
        (node,);
        inputs=(graph_input(:command, :plant => :command, command),),
        outputs=(graph_output(:frame, :plant => :frame),),
    ))
    return prepare_graph_hil_boundary(
        graph;
        command_input=:command,
        frame_output=:frame,
    )
end

include("live_exchange_private_core.jl")
include("live_multi_command_private_core.jl")
include("live_uint16_private_core.jl")

@testset "private-core node publication" begin
    prefix = get(ENV, "PIPEWIREAO_HIL_TEST_PREFIX", String(PipeWireAO_jll.artifact_dir))
    native_libdir = isdir(joinpath(prefix, "lib", "x86_64-linux-gnu")) ?
        joinpath(prefix, "lib", "x86_64-linux-gnu") : joinpath(prefix, "lib")
    daemon_path = joinpath(prefix, "bin", "pipewire-ao")
    dump_path = joinpath(prefix, "bin", "pwao-dump")
    @test isfile(daemon_path)
    @test isfile(dump_path)

    mktempdir(prefix="adaptive-optics-sim-pipewire-hil-") do temporary
        runtime = joinpath(temporary, "runtime")
        configuration_directory = joinpath(temporary, "configuration")
        mkpath(runtime)
        mkpath(configuration_directory)
        core_name = "aos-hil-$(getpid())-$(time_ns())"
        write(
            joinpath(configuration_directory, "private-core.conf"),
            _private_core_configuration(core_name),
        )
        write(
            joinpath(configuration_directory, "client.conf"),
            _HIL_CLIENT_CONFIGURATION,
        )
        environment = Dict(
            "XDG_RUNTIME_DIR" => runtime,
            "PIPEWIREAO_RUNTIME_DIR" => runtime,
            "PIPEWIREAO_CONFIG_DIR" => configuration_directory,
            "PIPEWIREAO_MODULE_DIR" => joinpath(native_libdir, "pipewire-ao-0.3"),
            "PIPEWIREAO_SPA_PLUGIN_DIR" => joinpath(native_libdir, "spa-ao-0.2"),
            "PIPEWIREAO_DEBUG" => "0",
            "LD_LIBRARY_PATH" => native_libdir * ":" * get(ENV, "LD_LIBRARY_PATH", ""),
        )
        log_path = joinpath(temporary, "core.log")
        log = open(log_path, "w+")
        daemon = nothing
        prepared = nothing
        try
            command = addenv(`$daemon_path -c private-core.conf`, environment)
            daemon = run(pipeline(command; stdout=log, stderr=log); wait=false)
            socket = joinpath(runtime, core_name)
            _wait_for_hil_condition(() -> ispath(socket))

            configuration = PipeWireHILConfiguration(
                remote=core_name,
                frame_node_name="adaptive-optics-sim-hil-frame",
                command_node_name="adaptive-optics-sim-hil-command",
                frame_schema="org.adaptiveopticssim.test.frame/1",
                command_schema="org.adaptiveopticssim.test.command/1",
                rate=SPA.Fraction(1_000, 1),
                exposure_duration_ns=1_000_000,
            )
            withenv(environment...) do
                _test_array_live_delivery(core_name)
                @test_throws ArgumentError prepare_pipewire_hil(
                    _test_hil_boundary(), configuration;
                    frame_properties=Dict("node.name" => "invalid-override"),
                )
                control = PropsBuffer(("test.control.version", "test.control.enabled"),
                    (Int32(1), false))
                prepared = prepare_pipewire_hil(
                    _test_hil_boundary(),
                    configuration,
                    frame_properties=Dict("test.control.version" => "1"),
                    frame_params=(control.pod,),
                    frame_param_buffer=PodBuffer(512),
                )
                @test frame_acquisition_generation(prepared) == 1
                @test with_frame_stream(identity, prepared) === prepared.frame_stream
                @test (@allocated frame_acquisition_generation(prepared)) == 0
                @test (@allocated with_frame_stream(identity, prepared)) == 0
                @test prepared.image_owner === nothing
                @test image_source_info(prepared) == ()
                @test with_frame_stream(prepared) do stream
                    props!(control, (Int32(1), true))
                    update_params!(stream, control.pod)
                    true
                end
                reset_pipewire_hil!(prepared)
                @test frame_acquisition_generation(prepared) == 2
                output = read(
                    addenv(`$dump_path -r $core_name`, environment),
                    String,
                )
                @test contains(output, configuration.frame_node_name)
                @test contains(output, configuration.command_node_name)
                @test contains(output, "test.control.version")
                @test !Base.process_exited(daemon)
                close(prepared)
                timing = PipeWireExchangeTiming()
                AdaptiveOpticsSimPipeWireHIL._complete_exchange_timing!(timing, UInt64(2), UInt64(7))
                @test_throws PipeWireHILError exchange_frame!(prepared; timing)
                @test timing.started_ns > 0
                @test timing.sequence == 0 && timing.generation == 0
                @test timing.finished_ns == 0
                @test_throws PipeWireHILError exchange_frame!(prepared)
                prepared = nothing
                _test_live_frame_publication_retry(core_name)
                _test_live_multi_command(core_name)
                _test_live_uint16_frame(core_name)
                output = read(
                    addenv(`$dump_path -r $core_name`, environment),
                    String,
                )
                @test !contains(output, configuration.frame_node_name)
                @test !contains(output, configuration.command_node_name)

                @test_throws PipeWireHILError prepare_pipewire_hil(
                    _test_hil_boundary(), configuration;
                    applied_command_node_name="simulator-applied-command",
                )
                scaled_configuration = PipeWireHILConfiguration(
                    remote=core_name,
                    frame_node_name=configuration.frame_node_name,
                    command_node_name=configuration.command_node_name,
                    frame_schema=configuration.frame_schema,
                    command_schema=AdaptiveOpticsSimPipeWireHIL.Schemas.DEMANDED_PDM_COMMAND_V1,
                    rate=configuration.rate,
                    exposure_duration_ns=configuration.exposure_duration_ns,
                    command_scale=1.0f-6,
                )
                prepared = prepare_pipewire_hil(_test_hil_boundary(277), scaled_configuration;
                    applied_command_node_name="simulator-applied-command",
                    applied_command_properties=Dict("node.group" => "test-applied-observation"),
                    image_sources=(PipeWireImageSourceOptions(node_name="simulator-pupil-opd",
                        media_name="pupil-opd", schema="org.adaptiveopticssim.test.pupil-opd/1",
                        shape=(2, 3), properties=Dict("node.group" => "test-optical-observation"),
                        channels=((name="first_value", index=0, title="First value", units="nm"),
                            (name="last_value", index=5, title="Last value", units=nothing))),))
                source = prepared.applied_command_source
                info = applied_command_source_info(prepared)
                @test info.role == "simulator-applied-command"
                @test info.port_name == "output_1"
                @test info.media_name == "dm-applied"
                @test info.schema == APPLIED_PDM_COMMAND_SCHEMA
                @test info.shape == (277,)
                @test info.format.element_type == NdArray.F32_LE
                @test info.command_scale == 1.0f-6
                @test with_applied_command_stream(identity, prepared) === source.stream
                @test (@allocated with_applied_command_stream(identity, prepared)) == 0
                image_source = only(prepared.image_sources)
                @test !image_source.state.pending
                @test !image_source.state.busy[]
                @test !image_source.state.failed[]
                @test image_source.state.sequence == 0
                @test image_source.state.pts == 0
                @test image_source.state.generation == 0
                @test image_source.state.dropped[] == 0
                @test all(iszero, image_source.state.image)
                image_info = only(image_source_info(prepared))
                @test image_info.index == 1
                @test image_info.shape == (2, 3)
                @test image_info.port_name == "output_1"
                @test image_info.format.element_type == NdArray.F32_LE
                @test image_info.format.layout == NdArray.ROW_MAJOR
                @test with_image_stream(identity, prepared, 1) === image_source.stream
                with_image_stream(identity, prepared, 1)
                @test (@allocated with_image_stream(identity, prepared, 1)) == 0
                @test image_source.stream.core === prepared.image_owner.core
                @test prepared.image_owner.core !== prepared.core
                _test_image_loop_isolation(prepared)
                if Sys.islinux()
                    names = [strip(read(joinpath("/proc/self/task", tid, "comm"), String))
                        for tid in readdir("/proc/self/task")]
                    @test count(==("AOS-HIL-images"), names) == 1
                    @test "AdaptiveOpticsS" in names # Existing mandatory WFS loop.
                end
                before_generation = frame_acquisition_generation(prepared)
                @test reset_pipewire_image_sources!(prepared) === prepared
                @test frame_acquisition_generation(prepared) == before_generation
                @test !image_source.state.pending
                output = read(addenv(`$dump_path -r $core_name`, environment), String)
                @test contains(output, "simulator-applied-command")
                @test contains(output, "dm-applied")
                @test contains(output, "test-applied-observation")
                @test contains(output, "simulator-pupil-opd")
                _test_image_channel_publication(prepared)
                @test !source.state.pending # Registered before any adopted frame.
                start!(prepared) # No observer is attached or awaited.
                values = fill(2.0f0, 277)
                for expected in UInt64(1):UInt64(3)
                    sequence = step_hil_frame!(prepared.boundary)
                    @test sequence == expected
                    state = prepared.state
                    state.frame_ready[] = state.frame_published[] = sequence
                    pts = Int64(100 * sequence)
                    AdaptiveOpticsSimPipeWireHIL._record_frame_publication!(state, sequence, pts)
                    receive_test_command!(state, values, sequence, pts + 10)
                    AdaptiveOpticsSimPipeWireHIL._adopt_received_command!(
                        adopt_hil_command!, prepared.boundary, state, source, sequence)
                    @test hil_boundary_status(prepared.boundary).active_command_sequence == sequence
                    @test state.command_ready[] == 0
                    @test source.state.sequence == sequence
                    @test source.state.pts == pts
                    @test source.state.values == values
                    @test stage_pipewire_image!(prepared, 1, Float32[1 3 5; 2 4 6],
                        sequence, pts, frame_acquisition_generation(prepared))
                end
                @test source.state.dropped[] == 2 # Observer absence retains just the latest.
                _test_applied_live_delivery(prepared)
                @test image_source.state.dropped[] == 2
                _test_image_live_delivery(prepared)
                source_id = with_applied_command_stream(node_id, prepared)
                image_id = with_image_stream(node_id, prepared, 1)
                old_generation = frame_acquisition_generation(prepared)
                reset_pipewire_hil!(prepared)
                @test !source.state.pending
                @test source.state.sequence == 0
                @test with_applied_command_stream(node_id, prepared) == source_id
                @test !image_source.state.pending
                @test image_source.state.sequence == 0
                @test image_source.state.generation == 0
                @test !stage_pipewire_image!(prepared, 1, zeros(Float32, 2, 3),
                    UInt64(3), Int64(300), old_generation)
                @test !image_source.state.pending
                @test with_image_stream(node_id, prepared, 1) == image_id
                @test stage_pipewire_image!(prepared, 1, zeros(Float32, 2, 3),
                    UInt64(1), Int64(400), frame_acquisition_generation(prepared))
                stop!(prepared)
                @test !image_source.state.pending
                start!(prepared)
                @test with_applied_command_stream(node_id, prepared) == source_id
                close(prepared)
                @test !isopen(source.stream)
                @test !isopen(image_source.stream)
                @test !isopen(prepared.image_owner.core)
                @test !isopen(prepared.image_owner.context)
                @test !isopen(prepared.image_owner.loop)
                @test_throws PipeWireHILError image_source_info(prepared)
                @test_throws PipeWireHILError applied_command_source_info(prepared)
                prepared = nothing
                output = read(addenv(`$dump_path -r $core_name`, environment), String)
                @test !contains(output, "simulator-applied-command")
                @test !contains(output, "simulator-pupil-opd")

                # A tuple of images uses one independent group, rather than
                # acquiring a native loop and connection for each image.
                prepared = prepare_pipewire_hil(_test_hil_boundary(), configuration;
                    image_sources=(
                        PipeWireImageSourceOptions(node_name="image-a", media_name="image-a",
                            schema="org.adaptiveopticssim.test.image/1", shape=(2, 3)),
                        PipeWireImageSourceOptions(node_name="image-b", media_name="image-b",
                            schema="org.adaptiveopticssim.test.image/1", shape=(2, 3)),
                    ))
                @test length(image_source_info(prepared)) == 2
                @test all(source -> source.stream.core === prepared.image_owner.core,
                    prepared.image_sources)
                @test all(source -> main_loop(source.stream) === prepared.image_owner.loop,
                    prepared.image_sources)
                @test prepared.image_owner.loop !== prepared.loop
                close(prepared)
                @test prepared.closed
                @test all(source -> !isopen(source.stream), prepared.image_sources)
                @test !isopen(prepared.image_owner.core)
                @test !isopen(prepared.image_owner.context)
                @test !isopen(prepared.image_owner.loop)
                prepared = nothing
            end
        catch
            flush(log)
            seekstart(log)
            core_log = read(log, String)
            isempty(core_log) || @error "private PipeWireAO core log" core_log
            rethrow()
        finally
            prepared === nothing || close(prepared)
            _stop_hil_process(daemon)
            close(log)
        end
    end
end
