struct LiveUnpublishedFrameCycle{State}
    state::State
    completed::Threads.Atomic{Bool}
end

function (observer::LiveUnpublishedFrameCycle)(::Stream)
    if observer.state.frame_ready[] != observer.state.frame_published[]
        observer.completed[] = true
    end
    return nothing
end

function _test_live_frame_publication_retry(core_name)
    @testset "publication retries after an empty completed cycle" begin
        boundary = _test_hil_boundary(; command=Float32[3, 4])
        configuration = PipeWireHILConfiguration(
            remote=core_name,
            frame_node_name="publication-retry-frame",
            command_node_name="publication-retry-command",
            frame_schema="org.adaptiveopticssim.test.exchange/1",
            command_schema="org.adaptiveopticssim.test.exchange/1",
            rate=SPA.Fraction(1_000, 1),
            exposure_duration_ns=1_000_000,
            timeout_ns=5_000_000_000,
        )
        prepared = prepare_pipewire_hil(boundary, configuration)
        registry = link = listener = waiting = nothing
        loans = StreamBuffer[]
        try
            registry = with_thread_loop_lock(prepared.loop) do _
                Registry(prepared.core)
            end
            roundtrip(registry)
            output = only(find_globals(registry; interface="PipeWire:Interface:Port",
                properties=("node.id" => string(node_id(prepared.frame_stream)),
                    "port.direction" => "out")))
            input = only(find_globals(registry; interface="PipeWire:Interface:Port",
                properties=("node.id" => string(node_id(prepared.command_stream)),
                    "port.direction" => "in")))
            # A direct transport echo preserves the frame Header and Acquisition
            # identity without introducing an RTC algorithm into this fixture.
            link = with_thread_loop_lock(prepared.loop) do _
                create_object(prepared.core, "link-factory", Link; properties=Dict(
                    "link.output.node" => string(node_id(prepared.frame_stream)),
                    "link.output.port" => string(output.id),
                    "link.input.node" => string(node_id(prepared.command_stream)),
                    "link.input.port" => string(input.id),
                    "object.linger" => "false"))
            end
            start!(prepared)
            AdaptiveOpticsSimPipeWireHIL._wait_for_frame_driver(prepared)
            observer = LiveUnpublishedFrameCycle(prepared.state, Threads.Atomic{Bool}(false))
            with_thread_loop_lock(prepared.loop) do _
                listener = add_listener!(prepared.frame_stream; on_trigger_done=observer)
                while true
                    buffer = StreamBuffer()
                    dequeue_buffer!(buffer, prepared.frame_stream) || break
                    push!(loans, buffer)
                end
            end
            @test length(loans) >= 2
            completed_before = prepared.driver_cycle.completed[]
            waiting = @async exchange_frame!(prepared)
            _wait_for_hil_condition(() -> observer.completed[])
            @test prepared.driver_cycle.completed[] > completed_before
            @test prepared.state.frame_ready[] == 1
            @test prepared.state.frame_published[] == 0
            @test prepared.state.command_ready[] == 0
            with_thread_loop_lock(prepared.loop) do _
                for buffer in loans
                    return_buffer!(buffer, prepared.frame_stream)
                end
                empty!(loans)
            end
            @test fetch(waiting) == 1
            @test prepared.state.frame_ready[] == prepared.state.frame_published[] == 1
            @test prepared.state.timing_completed_sequence[] == 1
            @test prepared.state.command == Float32[3, 4]
            @test prepared.state.command_ready[] == 0 # The matched command was adopted.
            @test !prepared.driver_cycle.pending
        finally
            with_thread_loop_lock(prepared.loop) do _
                for buffer in loans
                    return_buffer!(buffer, prepared.frame_stream)
                end
                listener === nothing || close(listener)
                link === nothing || close(link)
                registry === nothing || close(registry)
            end
            close(prepared)
        end
    end
    return nothing
end
