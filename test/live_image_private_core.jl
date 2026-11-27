mutable struct LiveImageCapture
    buffer::StreamBuffer
    values::Vector{Float32}
    header::BufferHeader
    identity::Union{Nothing,AcquisitionIdentity}
    exposure_start::Union{Nothing,ExposureTiming}
    timebase::Union{Nothing,MetadataClock}
    duration::UInt64
    sequence::Base.Threads.Atomic{UInt64}
end

function _test_image_channel_publication(prepared)
    source = only(prepared.image_sources)
    loop = ThreadLoop("test.channels")
    context = core = registry = node = nothing
    snapshot = Ref{Union{Nothing,NodeInfo}}(nothing)
    try
        context, core, registry = with_thread_loop_lock(loop) do _
            context = Context(loop)
            core = CoreConnection(context; properties=Dict("remote.name" => prepared.configuration.remote))
            context, core, Registry(core)
        end
        start!(loop)
        roundtrip(registry)
        global_node = only(find_globals(registry; interface="PipeWire:Interface:Node",
            properties=("node.name" => source.options.node_name,)))
        function bind_source()
            snapshot[] = nothing
            result = with_thread_loop_lock(loop) do _
                bind(registry, global_node, Node;
                    on_info=(node, info) -> (snapshot[] = info; nothing))
            end
            roundtrip(core)
            return result
        end
        node = bind_source()
        initial = snapshot[].properties["pipewireao.ndarray.channels"]
        output = only(JSON.parse(initial)["outputs"])
        @test output["port"] == "output_1"
        @test output["shape"] == collect(source.format.shape)
        @test output["schema"] == source.options.schema
        @test output["element_type"] == Int(source.format.element_type)
        @test output["layout"] == Int(source.format.layout)
        @test getindex.(output["channels"], "units") == ["nm", nothing]
        @test source.stream.callbacks.on_param_changed === nothing
        # Props cannot replace the authoritative Node information. The Stream
        # API provides no writable control surface for this description.
        with_thread_loop_lock(loop) do _
            set_param!(node, SPA.PARAM_PROPS, Pod(props_param(SPA.Props(
                "pipewireao.ndarray.channels" => "{\"version\":999}"))))
        end
        roundtrip(core)
        with_thread_loop_lock(loop) do _
            close(node)
        end
        node = bind_source()
        @test snapshot[].properties["pipewireao.ndarray.channels"] == initial
        @test !source.state.failed[]
    finally
        with_thread_loop_lock(loop) do _
            node === nothing || close(node)
            registry === nothing || close(registry)
            core === nothing || close(core)
            context === nothing || close(context)
        end
        close(loop)
    end
    return nothing
end

function (capture::LiveImageCapture)(stream)
    dequeue_buffer!(capture.buffer, stream) || return nothing
    try
        capture.header = something(buffer_header(capture.buffer))
        copyto!(capture.values, reinterpret(Float32, bytes(buffer_data(capture.buffer))))
        metadata = metadata_records(capture.buffer)
        capture.identity = acquisition_identity(metadata)
        capture.exposure_start = (timing=exposure_timing(metadata);timing===nothing || !timing.has_start ? nothing : timing)
        capture.timebase = (timing=exposure_timing(metadata);timing===nothing || !timing.has_start ? nothing : timing.clock)
        capture.duration = AdaptiveOpticsSimPipeWireHIL._exposure_duration(metadata)
        capture.sequence[] = capture.header.sequence
    finally
        queue_buffer!(capture.buffer, stream)
    end
    return nothing
end

function _test_image_native_callback_allocation!(prepared, image)
    source = only(prepared.image_sources)
    process = source.stream.callbacks.on_process
    process(source.stream)
    @test !source.state.pending
    generation = prepared.state.acquisition_generation
    stage_pipewire_image!(prepared, 1, image, UInt64(3), Int64(300), generation)
    @test (@allocated process(source.stream)) == 0
    @test !source.state.pending # The measured call queued a complete native buffer.
    @test (@allocated stage_pipewire_image!(prepared, 1, image,
        UInt64(3), Int64(300), generation)) == 0
    return nothing
end

function _test_image_live_delivery(prepared)
    image = Float32[1 3 5; 2 4 6]
    capture = LiveImageCapture(StreamBuffer(), zeros(Float32, 6),
        BufferHeader(0, 0, 0, 0, 0), nothing, nothing, nothing, 0, Base.Threads.Atomic{UInt64}(0))
    source = only(prepared.image_sources)
    owner = prepared.image_owner
    baseline_dropped = source.state.dropped[]
    sink = registry = link = nothing
    try
        sink, registry = with_thread_loop_lock(owner.loop) do _
            sink = Stream(owner.core, "test-image-observer";
                properties=Dict("node.name" => "test-image-observer",
                    "node.want-driver" => "true", "priority.driver" => "100"),
                on_process=capture)
            precompile(capture, (typeof(sink),))
            connect!(sink, :input;
                flags=STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT | STREAM_DRIVER,
                params=AdaptiveOpticsSimPipeWireHIL._stream_parameters(source.format,
                    source.options.schema; buffers=2))
            return sink, Registry(owner.core)
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
        link = with_thread_loop_lock(owner.loop) do _
            link = create_object(owner.core, "link-factory", Link; properties=Dict(
                "link.output.node" => string(node_id(source.stream)), "link.output.port" => string(output.id),
                "link.input.node" => string(node_id(sink)), "link.input.port" => string(input.id),
                "object.linger" => "false"))
            set_active!(sink, true)
            return link
        end
        _wait_for_hil_condition() do
            with_thread_loop_lock(owner.loop) do _
                stream_state(sink)
                return is_driving(sink) &&
                    stream_state(source.stream) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING
            end
        end
        with_thread_loop_lock(owner.loop) do _
            _test_image_native_callback_allocation!(prepared, image)
        end
        _wait_for_hil_condition() do
            with_thread_loop_lock(owner.loop) do _
                capture.sequence[] == 3 && !source.state.pending && return true
                trigger_process!(sink)
                return false
            end
        end
        @test capture.header.sequence == 3
        @test capture.header.pts == 300
        @test capture.values == Float32[1, 3, 5, 2, 4, 6]
        @test capture.identity == AcquisitionIdentity(prepared.state.acquisition_domain,
            prepared.state.acquisition_generation, UInt64(3))
        @test capture.duration == prepared.state.exposure_duration_ns
        @test capture.exposure_start === nothing
        @test capture.timebase === nothing
        @test source.state.dropped[] == baseline_dropped
        @test !source.state.failed[]
        @test !source.state.pending
    finally
        with_thread_loop_lock(owner.loop) do _
            link === nothing || close(link)
            sink === nothing || close(sink)
            registry === nothing || close(registry)
        end
    end
    # Viewer removal does not invalidate a new snapshot or the mandatory owner.
    @test stage_pipewire_image!(prepared, 1, image, UInt64(3), Int64(300),
        prepared.state.acquisition_generation)
    @test source.state.pending
    @test !source.state.failed[]
    return nothing
end

function _image_loop_access_probe(prepared)
    # Warm frame control access before timing; the probe measures native lock
    # independence rather than compilation or task setup.
    with_frame_stream(identity, prepared)
    held = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    serviced = Channel{Nothing}(1)
    holder = Base.Threads.@spawn begin
        # The native mutex must be released by the thread that acquired it,
        # even though this deliberate fault probe yields while holding it.
        current_task().sticky = true
        with_image_stream(prepared, 1) do _
            put!(held, nothing)
            take!(release)
        end
    end
    take!(held)
    started = time_ns()
    frame_access = Base.Threads.@spawn begin
        with_frame_stream(identity, prepared)
        put!(serviced, nothing)
    end
    try
        independent = timedwait(() -> isready(serviced), 0.05; pollint=0.001) == :ok
        remaining = 0.1 - Float64(time_ns() - started) / 1e9
        remaining > 0 && sleep(remaining)
        return independent
    finally
        put!(release, nothing)
        fetch(holder)
        fetch(frame_access)
    end
end

function _test_image_loop_isolation(prepared)
    @test main_loop(only(prepared.image_sources).stream) !== main_loop(prepared.frame_stream)
    @test _image_loop_access_probe(prepared)
    return nothing
end
