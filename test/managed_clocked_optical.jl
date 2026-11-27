@testset "managed clocked optics observes the held command and clears it on reset" begin
    mktempdir() do directory
        (; config, document) = clocked_optical_documents(directory)
        document["applied_command_observation"] = true
        document["pacing_rate_hz"] = 50
        C.write_json(config, document)
        options = PipeWireAORTC.AOSOwner.configuration(config)
        execution = HIL.Simulation.prepare_optical_startup(config)
        sensor = only(execution.science.sensors)
        @test HIL.ExternalPupil.snapshot(execution).sequence == 0
        @test all(iszero, execution.science.source_command)
        reference = (MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP; required=true),)
        reference_peer(name, schema, rate) = PipeWireArrayStreamOptions(;
            node_name=name, media_name=name, schema, buffers=3, metadata=reference,
            format=NdArrayFormat(NdArray.F32_LE, (2,); rate, layout=NdArray.ROW_MAJOR))
        camera_rate = SPA.Fraction(1_000_000_000, options.period_ns)
        ports = (
            reference_peer("test.clocked.command", options.command_schema, nothing),
            reference_peer("test.clocked.applied", PipeWireAORTC.Schemas.identifier("applied_pdm_command"),
                SPA.Fraction(options.rate, 1)),
            array_peer("test.clocked.camera", sensor.schema, size(sensor.image);
                rate=camera_rate, buffers=options.sensor_buffers),
        )
        links = ((ports[1].node_name, options.command_node, false),
            ("simulator-$(options.mirror)-applied", ports[2].node_name, true),
            (only(options.sensor_nodes), ports[3].node_name, true))
        _with_private_core() do remote
            with_managed_source(config, remote) do client, node, _
                with_array_peers(remote, ports, links) do owner
                    with_source_controls(client, node) do control, _
                        @test control(run_control_request(1, :running), Source.RUN, 1)[5] == 0
                        adopted = fill(Float32(NaN), 2)
                        receive_array!(adopted, owner, 2)
                        @test all(iszero, adopted)
                        demand = Float32[2e-8, -1e-8]
                        sequence = UInt64(0)
                        _wait_for_hil_condition(; timeout=20) do
                            sample = drain_pipewire_array!(adopted, owner, 2)
                            sample !== nothing && adopted == demand && return true
                            sequence += 1
                            stage_pipewire_array!(owner, 1, demand; sequence, pts=Int64(sequence))
                            with_array_stream(trigger_process!, owner, 1)
                            false
                        end
                        image = zeros(Float32, size(sensor.image))
                        first = receive_array!(image, owner, 3)
                        @test first.identity.generation == 1
                        @test all(isfinite, image) && sum(image) > 0
                        @test control(run_control_request(2, :stopped), Source.RUN, 2)[5] == 0
                        reset = control(reset_control_request(3), Source.RESET, 3)
                        @test reset[5] == 0 && reset[6] == 2 && reset[7] == 0
                        @test control(run_control_request(4, :running), Source.RUN, 4)[5] == 0
                        newest = receive_array!(image, owner, 3;
                            accept=sample -> sample.identity.generation == 2)
                        @test newest.identity.domain == first.identity.domain
                        _wait_for_hil_condition(; timeout=20) do
                            sample = drain_pipewire_array!(adopted, owner, 2)
                            sample !== nothing && all(iszero, adopted)
                        end
                    end
                end
            end
        end
    end
end
