# One native producer node receives a frame and returns three distinct command
# outputs carrying that frame's sequence. This is transport evidence only.
struct LiveMultiCommandProducer
    input::FilterBuffer
    outputs::NTuple{3,FilterBuffer}
    received::Threads.Atomic{UInt64}
    header::Base.RefValue{BufferHeader}
    metadata::MetadataSnapshot
    published::NTuple{3,Threads.Atomic{UInt64}}
    queued::NTuple{3,Threads.Atomic{UInt64}}
    release_last::Threads.Atomic{Bool}
    duplicate_first::Threads.Atomic{Bool}
    calls::Threads.Atomic{UInt64}
end
function LiveMultiCommandProducer(; release_last=true)
    return LiveMultiCommandProducer(FilterBuffer(), ntuple(_ -> FilterBuffer(),3),
        Threads.Atomic{UInt64}(0), Ref(BufferHeader(0,0,0,0,0)), MetadataSnapshot(),
        ntuple(_ -> Threads.Atomic{UInt64}(0),3), ntuple(_ -> Threads.Atomic{UInt64}(0),3),
        Threads.Atomic{Bool}(release_last),
        Threads.Atomic{Bool}(false), Threads.Atomic{UInt64}(0))
end
function (producer::LiveMultiCommandProducer)(filter::Filter, position)
    Threads.atomic_add!(producer.calls,UInt64(1))
    input = filter.ports[1]
    if dequeue_buffer!(producer.input, input)
        try
            incoming = something(buffer_header(producer.input))
            _retain_test_metadata!(producer.metadata,producer.input)
            prior = producer.header[]
            if incoming.sequence != prior.sequence || incoming.pts != prior.pts
                producer.header[] = incoming
                for published in producer.published
                    published[] = 0
                end
            end
            producer.received[] = incoming.sequence
        finally
            queue_buffer!(producer.input, input)
        end
    end
    header = producer.header[]
    header.sequence == 0 && return nothing
    for index in 1:3
        index == 3 && !producer.release_last[] && continue
        duplicate = index == 1 && producer.duplicate_first[]
        producer.published[index][] == header.sequence && !duplicate && continue
        port = filter.ports[index+1]
        buffer = producer.outputs[index]
        dequeue_buffer!(buffer, port) || error("test producer has no command output buffer")
        queued = false
        try
            count = (2,3,1)[index]
            data = buffer_data(buffer)
            values = reinterpret(Float32, buffer_memory(data, count*sizeof(Float32)))
            for item in eachindex(values)
                values[item] = Float32(10index + item)
            end
            set_chunk!(data; size=count*sizeof(Float32), stride=sizeof(Float32))
            _publish_test_metadata!(buffer,producer.metadata)
            set_buffer_header!(buffer, header)
            queue_buffer!(buffer, port)
            queued = true
            Threads.atomic_add!(producer.queued[index],UInt64(1))
            producer.published[index][] = header.sequence
            duplicate && (producer.duplicate_first[] = false)
        finally
            queued || queue_buffer!(buffer, port)
        end
    end
    return nothing
end

function _live_multi_link(core, registry, output_node, output_name, input_node, input_name)
    output = only(find_globals(registry; interface="PipeWire:Interface:Port",
        properties=("node.id"=>string(output_node), "port.name"=>output_name)))
    input = only(find_globals(registry; interface="PipeWire:Interface:Port",
        properties=("node.id"=>string(input_node), "port.name"=>input_name)))
    return create_object(core,"link-factory",Link; properties=Dict(
        "link.output.node"=>string(output_node), "link.output.port"=>string(output.id),
        "link.input.node"=>string(input_node), "link.input.port"=>string(input.id),
        "object.linger"=>"false"))
end

function _prepare_live_multi_producer(prepared, callback)
    producer = registry = nothing
    links = ()
    try
        producer, registry = with_thread_loop_lock(prepared.loop) do _
            filter = Filter(prepared.core,"test-three-output-producer";
                properties=Dict("node.name"=>"test-three-output-producer",
                    "media.type"=>"Application", "media.category"=>"Filter"), on_process=callback)
            add_port!(filter,:input; flags=FILTER_PORT_MAP_BUFFERS,
                properties=Dict("port.name"=>"frame"),
                params=AdaptiveOpticsSimPipeWireHIL._stream_parameters(
                    NdArrayFormat(NdArray.F32_LE,(3,); layout=NdArray.ROW_MAJOR),"test.frame/1"))
            for (name,count) in (("woofer",2),("tweeter",3),("tiptilt",1))
                add_port!(filter,:output; flags=FILTER_PORT_MAP_BUFFERS,
                    properties=Dict("port.name"=>name),
                    params=AdaptiveOpticsSimPipeWireHIL._stream_parameters(
                        NdArrayFormat(NdArray.F32_LE,(count,); layout=NdArray.ROW_MAJOR),"test.command/1"))
            end
            precompile(callback,(typeof(filter),Union{Nothing,FilterPosition}))
            connect!(filter; flags=FILTER_INACTIVE)
            return filter, Registry(prepared.core)
        end
        _wait_for_hil_condition() do
            roundtrip(registry)
            return node_id(producer) != typemax(UInt32) &&
                length(find_globals(registry; interface="PipeWire:Interface:Port",
                    properties=("node.id"=>string(node_id(producer)),))) == 4
        end
        roundtrip(registry)
        with_thread_loop_lock(prepared.loop) do _
            links = (_live_multi_link(prepared.core,registry,node_id(prepared.frame_stream),
                "output_1",node_id(producer),"frame"),)
            for key in keys(prepared.command_stream)
                links = (links..., _live_multi_link(prepared.core,registry,node_id(producer),
                    String(key),node_id(prepared.command_stream[key]),"input_1"))
            end
            set_active!(producer,true)
        end
        return (; producer, registry, links)
    catch
        with_thread_loop_lock(prepared.loop) do _
            foreach(close, reverse(links))
            producer === nothing || close(producer)
            registry === nothing || close(registry)
        end
        rethrow()
    end
end

function _test_live_multi_command(core_name)
    _test_live_partial_command_set(core_name)
    @testset "one native producer returns a coherent three-port command set" begin
        boundary = multi_command_boundary()
        configuration = multi_command_configuration(; remote=core_name)
        prepared = prepare_pipewire_hil(boundary, configuration)
        producer = registry = nothing
        links = ()
        callback = LiveMultiCommandProducer()
        try
            native = _prepare_live_multi_producer(prepared, callback)
            producer, registry, links = native.producer, native.registry, native.links
            start!(prepared)
            @test exchange_frame!(prepared) == 1
            @test callback.received[] == 1
            @test boundary.command_input == (woofer=Float32[22,24], tweeter=Float32[63,66,69], tiptilt=Float32[124])
            @test all(state -> state.command_ready[] == 0,values(prepared.state))
            @test exchange_frame!(prepared) == 2
            @test hil_frame_buffer(boundary) == Float32[46,198,124]
            @test callback.received[] == 2
            reset_pipewire_hil!(prepared)
            @test frame_acquisition_generation(prepared) == 2
            @test all(state -> state.acquisition_generation == 2,values(prepared.state))
            @test exchange_frame!(prepared) == 1
            stop!(prepared)
        finally
            with_thread_loop_lock(prepared.loop) do _
                for link in reverse(links)
                    close(link)
                end
                producer === nothing || close(producer)
                registry === nothing || close(registry)
            end
            close(prepared)
        end
        @test all(stream -> !isopen(stream),values(prepared.command_stream))
        @test prepared.closed
    end
    @testset "mixed physical-DM and tip/tilt adopted sources and image lifetime" begin
        adapter = AdaptiveOpticsSimPipeWireHIL
        boundary = multi_command_boundary(; tiptilt_count=2)
        configuration = multi_command_configuration(; remote=core_name,
            command_schema=(woofer=adapter.Schemas.ACTUATOR_COMMAND_OPD_M_V1,
                tweeter=adapter.Schemas.ACTUATOR_COMMAND_OPD_M_V1,
                tiptilt=adapter.Schemas.TIP_TILT_OPD_COEFFICIENTS_M_V1),
            command_scale=(woofer=1f0,tweeter=1f0,tiptilt=1f0))
        names = (tiptilt="multi-applied-tiptilt",woofer="multi-applied-woofer",tweeter="multi-applied-tweeter")
        prepared = prepare_pipewire_hil(boundary,configuration; applied_command_node_name=names,
            image_sources=(PipeWireImageSourceOptions(node_name="multi-image",media_name="test-image",
                schema="test.image/1",shape=(2,3)),))
        sources = prepared.applied_command_source
        try
            declarations = applied_command_source_info(prepared)
            @test keys(declarations) == keys(hil_command_buffer(boundary))
            @test all(key -> declarations[key].node_name == names[key],keys(names))
            @test map(info -> info.shape,declarations) == (woofer=(2,),tweeter=(3,),tiptilt=(2,))
            @test declarations.woofer.schema == declarations.tweeter.schema == APPLIED_PDM_COMMAND_SCHEMA
            @test declarations.woofer.units == declarations.tweeter.units == :micrometre_opd
            @test declarations.woofer.command_scale == declarations.tweeter.command_scale == 1f-6
            @test declarations.tiptilt.schema == adapter.Schemas.TIP_TILT_OPD_COEFFICIENTS_M_V1
            @test declarations.tiptilt.units == :metre_opd
            @test declarations.tiptilt.command_scale == 1f0
            @test declarations.tiptilt.media_name == "tip-tilt-applied"
            sequence = step_hil_frame!(boundary)
            frame_state = adapter._frame_state(prepared.state)
            frame_state.frame_ready[] = frame_state.frame_published[] = sequence
            adapter._record_frame_publication!(frame_state, sequence, Int64(100))
            receive_test_command!(prepared.state.woofer, Float32[1f-6,-2f-6], sequence, Int64(110))
            receive_test_command!(prepared.state.tweeter, Float32[3f-6,-4f-6,5f-6], sequence, Int64(110))
            @test_throws PipeWireHILError adapter._adopt_received_command!(adopt_hil_command!,
                boundary, prepared.state, sources, sequence)
            @test all(source -> !source.state.pending, values(sources))
            coefficients = Float32[-0.0,4.25f-7]
            receive_test_command!(prepared.state.tiptilt, coefficients, sequence, Int64(110))
            adapter._adopt_received_command!(adopt_hil_command!, boundary, prepared.state, sources, sequence)
            @test sources.woofer.state.values == Float32[1,-2]
            @test sources.tweeter.state.values == Float32[3,-4,5]
            @test reinterpret(UInt32, sources.tiptilt.state.values) == reinterpret(UInt32, coefficients)
            @test reinterpret(UInt32, boundary.command_input.tiptilt) == reinterpret(UInt32, coefficients)
            @test all(source -> source.state.sequence == sequence && source.state.pts == 100, values(sources))
            ids = with_applied_command_stream(streams -> map(node_id,streams),prepared)
            @test length(unique(values(ids))) == 3
            start!(prepared)
            @test stage_pipewire_image!(prepared,1,ones(Float32,2,3),UInt64(1),Int64(100),
                frame_acquisition_generation(prepared))
            stop!(prepared)
            @test all(source -> !source.state.pending,values(sources))
            @test !prepared.image_sources[1].state.pending
        finally
            close(prepared)
        end
        @test all(source -> !isopen(source.stream),values(sources))
        @test !isopen(prepared.image_owner.loop)
    end
    return nothing
end

function _test_live_partial_command_set(core_name)
    adapter = AdaptiveOpticsSimPipeWireHIL
    for outcome in (:delayed, :missing, :duplicate)
        @testset "native partial command set: $outcome" begin
            boundary = multi_command_boundary()
            initial = map(copy,boundary.command_input)
            prepared = prepare_pipewire_hil(boundary,multi_command_configuration(;
                remote=core_name,timeout_ns=UInt64(2_000_000_000)))
            callback = LiveMultiCommandProducer(; release_last=false)
            native = nothing
            waiting = nothing
            try
                native = _prepare_live_multi_producer(prepared,callback)
                start!(prepared)
                waiting = @async exchange_frame!(prepared)
                _wait_for_hil_condition() do
                    return istaskdone(waiting) || (prepared.state.woofer.command_ready[] == 1 &&
                        prepared.state.tweeter.command_ready[] == 1)
                end
                @test !istaskdone(waiting)
                @test prepared.state.woofer.command_ready[] == prepared.state.tweeter.command_ready[] == 1
                @test prepared.state.tiptilt.command_ready[] == 0
                @test boundary.command_input == initial
                calls = callback.calls[]
                completed = prepared.driver_cycle.completed[]
                _wait_for_hil_condition() do
                    return istaskdone(waiting) || (callback.calls[] >= calls+3 &&
                        prepared.driver_cycle.completed[] >= completed+3)
                end
                # Empty callbacks on the already-ready ports must survive
                # additional native cycles while the final command is absent.
                @test !istaskdone(waiting)
                @test callback.calls[] >= calls+3
                @test prepared.driver_cycle.completed[] >= completed+3
                @test boundary.command_input == initial
                @test callback.queued[1][] == callback.queued[2][] == 1
                @test callback.queued[3][] == 0
                if outcome == :delayed
                    callback.release_last[] = true
                    @test fetch(waiting) == 1
                    @test boundary.command_input == (woofer=Float32[22,24],
                        tweeter=Float32[63,66,69],tiptilt=Float32[124])
                    @test all(state -> state.command_ready[] == 0,values(prepared.state))
                else
                    outcome == :duplicate && (callback.duplicate_first[] = true)
                    observed = try
                        fetch(waiting)
                    catch exception
                        exception
                    end
                    @test observed isa TaskFailedException
                    failure = waiting.result
                    @test failure isa PipeWireHILError
                    if outcome == :missing
                        @test occursin("command set 1 did not complete",failure.message)
                    else
                        @test callback.queued[1][] == 2
                        @test occursin("second command",failure.message)
                        @test prepared.command_stream.woofer.callbacks.on_process.buffer.handle == C_NULL
                    end
                    @test boundary.command_input == initial
                    @test AlgorithmGraphs.hil_boundary_status(boundary).command_response_required
                    @test_throws PipeWireHILError reset_pipewire_hil!(prepared)
                    @test_throws AlgorithmGraphs.AlgorithmGraphError step_hil_frame!(boundary)
                end
            finally
                if native !== nothing
                    with_thread_loop_lock(prepared.loop) do _
                        foreach(close,reverse(native.links))
                        close(native.producer)
                        close(native.registry)
                    end
                end
                # A genuine duplicate stores a native callback error. Closing
                # must still return every endpoint before reporting that error.
                close_error = try
                    close(prepared)
                    nothing
                catch exception
                    exception
                end
                @test close_error === nothing || close_error isa PipeWireHILError
                @test prepared.closed
                @test all(stream -> !isopen(stream),values(prepared.command_stream))
            end
        end
    end
    return nothing
end
