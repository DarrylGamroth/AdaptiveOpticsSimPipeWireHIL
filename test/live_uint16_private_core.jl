# Native detector codes and command causality across a real private-core link.
struct LiveUInt16TestDeclaration end
struct LiveUInt16TestOwner{Command,Frame}
    command::Command
    frame::Frame
end
function AlgorithmGraphs.graph_node_ports(::Type{LiveUInt16TestDeclaration}, ::Nothing)
    return (
        AlgorithmGraphs.graph_port_contract(:command, :input, :data, Float32, (2,),
            "test.native-command/1", :column_major),
        AlgorithmGraphs.graph_port_contract(:frame, :output, :data, UInt16, (2, 3),
            "test.native-detector/1", :column_major),
    )
end
function AlgorithmGraphs.prepare_graph_node(::Type{LiveUInt16TestDeclaration}, ::Nothing,
    props, inputs::NamedTuple, outputs::NamedTuple, parameters::NamedTuple, target)
    return LiveUInt16TestOwner(inputs.command, outputs.frame)
end
function AlgorithmGraphs.step_graph_node!(owner::LiveUInt16TestOwner)
    copyto!(owner.frame, (0x0000, 0x0001, 0x7fff, 0x8000, 0xffff, 0xfffe))
    return nothing
end
AlgorithmGraphs.reset_graph_node!(::LiveUInt16TestOwner) = nothing

struct LiveUInt16CommandProducer
    input::FilterBuffer
    output::FilterBuffer
    values::Vector{UInt16}
    received::Threads.Atomic{UInt64}
    metadata::MetadataSnapshot
end
function (producer::LiveUInt16CommandProducer)(filter::Filter, position)
    dequeue_buffer!(producer.input, filter.ports[1]) || return nothing
    try
        header = something(buffer_header(producer.input))
        _retain_test_metadata!(producer.metadata,producer.input)
        copyto!(producer.values, reinterpret(UInt16, bytes(buffer_data(producer.input))))
        dequeue_buffer!(producer.output, filter.ports[2]) || error("fixture has no command buffer")
        try
            data = buffer_data(producer.output)
            values = reinterpret(Float32, buffer_memory(data, 2sizeof(Float32)))
            values[1] = 11
            values[2] = 12
            set_chunk!(data; size=2sizeof(Float32), stride=sizeof(Float32))
            _publish_test_metadata!(producer.output,producer.metadata)
            set_buffer_header!(producer.output, header)
        finally
            queue_buffer!(producer.output, filter.ports[2])
        end
        producer.received[] = header.sequence
    finally
        queue_buffer!(producer.input, filter.ports[1])
    end
    return nothing
end

@static if isdefined(AlgorithmGraphs, :PreparedBorrowedFrameHILBoundary)
    struct LiveBorrowedFrame{Boundary} <: AlgorithmGraphs.PreparedBorrowedFrameHILBoundary
        boundary::Boundary
    end

    AlgorithmGraphs.hil_frame_buffer(plant::LiveBorrowedFrame) = hil_frame_buffer(plant.boundary)
    AlgorithmGraphs.hil_command_buffer(plant::LiveBorrowedFrame) = hil_command_buffer(plant.boundary)
    AlgorithmGraphs.hil_boundary_status(plant::LiveBorrowedFrame) =
        AlgorithmGraphs.hil_boundary_status(plant.boundary)
    AlgorithmGraphs.hil_frame_exposure_nanoseconds(::LiveBorrowedFrame) = UInt64(1_000_000)
    AlgorithmGraphs.adopt_hil_command!(plant::LiveBorrowedFrame, sequence) =
        adopt_hil_command!(plant.boundary, sequence)

    function AlgorithmGraphs.step_hil_frame_at!(plant::LiveBorrowedFrame, driver, destination)
        sample = AlgorithmGraphs.step_hil_frame_at!(plant.boundary, driver)
        permutedims!(destination, hil_frame_buffer(plant), (2, 1))
        return sample
    end
end

function _test_live_uint16_frame(core_name)
    modes = isdefined(@__MODULE__, :LiveBorrowedFrame) ? (:staged, :borrowed) : (:staged,)
    @testset "native UInt16 non-square frame exchange ($mode)" for mode in modes
        graph = prepare_algorithm_graph(algorithm_graph(
            (algorithm_node(:plant, LiveUInt16TestDeclaration, nothing),);
            inputs=(graph_input(:command, :plant => :command, zeros(Float32, 2)),),
            outputs=(graph_output(:frame, :plant => :frame),),
        ))
        boundary = prepare_graph_hil_boundary(graph; command_input=:command, frame_output=:frame)
        @test validate_hil_system((; graph, boundary); command_input=:command,
            frame_output=:frame, command_shape=(2,), frame_shape=(2, 3)) === nothing
        configuration = PipeWireHILConfiguration(remote=core_name,
            frame_node_name="native-uint16-frame", command_node_name="native-uint16-command",
            frame_schema="test.native-detector/1", command_schema="test.native-command/1",
            frame_encoding=:uint16, rate=SPA.Fraction(1_000, 1), exposure_duration_ns=1_000_000)
        plant = mode === :borrowed ? LiveBorrowedFrame(boundary) : boundary
        driver = mode === :borrowed ? AlgorithmGraphs.FixedStepModelTimeDriver(
            AlgorithmGraphs.PeriodicSchedule(AlgorithmGraphs.ModelDuration(1_000_000)),
        ) : nothing
        prepared = prepare_pipewire_hil(plant, configuration)
        callback = LiveUInt16CommandProducer(FilterBuffer(), FilterBuffer(), zeros(UInt16, 6),
            Threads.Atomic{UInt64}(0), MetadataSnapshot())
        producer = registry = nothing
        links = ()
        try
            producer, registry = with_thread_loop_lock(prepared.loop) do _
                filter = Filter(prepared.core, "native-uint16-command-producer";
                    properties=Dict("node.name" => "native-uint16-command-producer"), on_process=callback)
                for (direction, name, format, schema) in (
                    (:input, "frame", NdArrayFormat(NdArray.U16_LE, (2, 3); layout=NdArray.ROW_MAJOR), "test.native-detector/1"),
                    (:output, "command", NdArrayFormat(NdArray.F32_LE, (2,); layout=NdArray.ROW_MAJOR), "test.native-command/1"),
                )
                    add_port!(filter, direction; flags=FILTER_PORT_MAP_BUFFERS,
                        properties=Dict("port.name" => name),
                        params=AdaptiveOpticsSimPipeWireHIL._stream_parameters(format, schema))
                end
                precompile(callback, (typeof(filter), Union{Nothing,FilterPosition}))
                connect!(filter; flags=FILTER_INACTIVE)
                return filter, Registry(prepared.core)
            end
            _wait_for_hil_condition() do
                roundtrip(registry)
                return length(find_globals(registry; interface="PipeWire:Interface:Port",
                    properties=("node.id" => string(node_id(producer)),))) == 2
            end
            with_thread_loop_lock(prepared.loop) do _
                links = (
                    _live_multi_link(prepared.core, registry, node_id(prepared.frame_stream),
                        "output_1", node_id(producer), "frame"),
                    _live_multi_link(prepared.core, registry, node_id(producer),
                        "command", node_id(prepared.command_stream), "input_1"),
                )
                set_active!(producer, true)
            end
            start!(prepared)
            if mode === :borrowed
                @test_throws PipeWireHILError exchange_frame!(prepared)
                @test AlgorithmGraphs.model_time_sequence(driver) == 0
            end
            for sequence in UInt64(1):UInt64(2)
                @test exchange_frame!(prepared, driver) == sequence
                @test callback.received[] == sequence
                @test callback.values == UInt16[0, 32767, 65535, 1, 32768, 65534]
                @test hil_frame_buffer(boundary) == UInt16[0 32767 65535; 1 32768 65534]
                @test hil_command_buffer(boundary) == Float32[11, 12]
                @test prepared.state.command_ready[] == 0
                @test !prepared.driver_cycle.pending
            end
        finally
            with_thread_loop_lock(prepared.loop) do _
                foreach(close, reverse(links))
                producer === nothing || close(producer)
                registry === nothing || close(registry)
            end
            close(prepared)
        end
        @test prepared.closed
    end
    return nothing
end
