@testset "private-core free-running command adoption and reset" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    _with_private_core() do remote
        boundary = multi_command_boundary(; calibration=true)
        configuration = multi_command_configuration(; remote)
        prepared = prepare_pipewire_hil(boundary, configuration)
        callback = LiveMultiCommandProducer()
        native = nothing
        try
            @test_throws PipeWireHILError step_free_running!(prepared)
            @test_throws ArgumentError exchange_frame!(prepared)
            native = _prepare_live_multi_producer(prepared, callback)
            start!(prepared)
            adapter._wait_for_frame_driver(prepared)
            @test step_free_running!(prepared) == 1
            @test free_running_status(prepared).applied_updates == 0
            # The plant continues stepping while the graph queues corrections.
            # The first correction can arrive in a later native cycle.
            for sequence in UInt64(2):UInt64(16)
                adapter._await_driver_cycle!(prepared, adapter.SimRunner.current_strategy())
                @test step_free_running!(prepared) == sequence
                free_running_status(prepared).applied_updates > 0 && break
            end
            status = free_running_status(prepared)
            @test 0 < status.applied_sequence <= callback.received[]
            @test status.applied_updates == 1
            @test hil_frame_buffer(boundary) == Float32[46, 198, 124]

            reset_pipewire_hil!(prepared)
            @test frame_acquisition_generation(prepared) == 2
            @test free_running_status(prepared) ==
                (; frames=0, dropped_frames=0, pending=false, applied_sequence=0, applied_updates=0)
        finally
            if native !== nothing
                with_thread_loop_lock(prepared.loop) do _
                    foreach(close, reverse(native.links))
                    close(native.producer)
                    close(native.registry)
                end
            end
            close(prepared)
        end
        @test_throws PipeWireHILError step_free_running!(prepared)
    end
end
