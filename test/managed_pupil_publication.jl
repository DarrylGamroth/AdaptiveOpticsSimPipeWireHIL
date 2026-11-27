# Included by the managed exchange suite, which owns the daemon and bootstrap helpers.
function pupil_offsets(directory, options)
    matrix = Float32[1 0; 0 1]
    path = joinpath(directory, "identity.fits")
    PipeWireAORTC.InstrumentCalibration.write_fits(path, matrix)
    artifact = Dict("path"=>"identity.fits", "sha256"=>C.sha256_file(path),
        "axes"=>[2, 1], "shape"=>[2, 2])
    offset_path = joinpath(directory, "offsets.yaml")
    C.write_yaml(offset_path, Dict(
        "version"=>2, "projection"=>artifact, "inverse"=>artifact,
        "command_limit_m"=>1e-6, "calibration_identity"=>options.calibration_identity,
        "command_order"=>options.command_order,
        "branches"=>Dict("llowfs"=>Dict(
            "node_name"=>"test.pupil.offset", "schema"=>"test.offset/1", "modes"=>[1],
            "command_limit_m"=>1e-6, "coefficient_limit_m"=>1e-7,
            "max_age_ns"=>10_000_000, "silence_ns"=>1_000_000_000)),
    ))
    return offset_path
end

function with_array_peers(f, remote, ports, links)
    with_array_streams(ports; remote, input=map(last, links)) do owner
        registry = with_array_stream(_ -> Registry(owner.core), owner, 1)
        connections = Link[]
        try
            for (output, input, _) in links
                push!(connections, connect_ports(owner.core, registry, output, input))
            end
            f(owner)
        finally
            with_array_stream(owner, 1) do _
                foreach(close, reverse(connections))
                close(registry)
            end
        end
    end
end

function array_peer(name, schema, shape; rate=nothing, buffers=3)
    return PipeWireArrayStreamOptions(; node_name=name, media_name=name, schema, buffers,
        format=NdArrayFormat(NdArray.F32_LE, shape; layout=NdArray.ROW_MAJOR, rate),
        metadata=C.ACQUISITION_METADATA)
end

function receive_array!(array, owner, index; accept=Returns(true))
    receipt = Ref{Union{Nothing,PipeWireArrayReceipt}}(nothing)
    _wait_for_hil_condition(; timeout=20) do
        sample = drain_pipewire_array!(array, owner, index)
        sample === nothing && return false
        accept(sample) || return false
        receipt[] = sample
        true
    end
    return something(receipt[])
end

@testset "managed pupil publication and command observation share acquisition after reset" begin
    mktempdir() do directory
        (; config, document) = pupil_source_documents(directory)
        document["applied_command_observation"] = true
        document["pacing_rate_hz"] = 5
        C.write_json(config, document)
        options = PipeWireAORTC.AOSOwner.configuration(config)
        offsets = pupil_offsets(directory, options)
        pupil_contract = Base.get_extension(PipeWireAORTC, :PipeWireAORTCAOSExt).PupilOutput
        declaration = IC.load_simulation(options.plant.definition)
        pupil = pupil_contract.contract(declaration, IC.settings(declaration);
            simulation_graph=options.plant.graph)
        applied = Managed.PupilSource.applied_command_option(options)
        ports = (
            array_peer("test.pupil.sink", pupil.schema, Tuple(pupil.shape); buffers=2),
            array_peer("test.applied.sink", applied.schema, (2,); rate=applied.format.rate),
        )
        links = ((Managed.PupilOutput.NODE_NAME, ports[1].node_name, true),
            (applied.node_name, ports[2].node_name, true))
        arguments = ["--physical-offsets", offsets]
        cpus = worker_cpus()
        entrypoint = if isempty(cpus)
            # Exercise the actual bootstrap and PupilSource owner on CI runners
            # restricted to CPU 0/1, where deployment affinity cannot be declared.
            """
            using AdaptiveOpticsSimPipeWireHIL, PipeWireAORTC, ThreadPinning
            const HIL = AdaptiveOpticsSimPipeWireHIL
            const Ext = Base.get_extension(HIL, :PipeWireAORTCExt)
            Ext.Bootstrap.with_owner(Ext.arguments(ARGS)) do admitted
                options = Ext.Configuration.configuration(admitted.config)
                Ext.PupilSource.run_source!(merge(options, admitted,
                    (; idle_strategy=HIL.SimRunner.strategy("yielding"))))
            end
            """
        else
            append!(arguments, ["--pupil-output-cpu", string(first(cpus))])
            nothing
        end
        _with_private_core(; configuration_for=observation_core_configuration) do remote
            with_managed_source(config, remote; arguments, entrypoint) do client, node, report
                with_array_peers(remote, ports, links) do owner
                    with_source_controls(client, node) do control, _
                        packet = zeros(Float32, Tuple(pupil.shape))
                        command = fill(Float32(NaN), 2)
                        first_domain = nothing
                        for generation in 1:2
                            token = 3generation - 2
                            @test control(run_control_request(token, :running), Source.RUN, token)[5] == 0
                            in_generation = receipt -> receipt.identity.generation == generation
                            published = receive_array!(packet, owner, 1; accept=in_generation)
                            adopted = receive_array!(command, owner, 2; accept=in_generation)
                            @test published.identity.domain == adopted.identity.domain
                            @test published.duration == adopted.duration == options.period_ns
                            @test published.pts == (published.identity.sequence - 1) * options.period_ns
                            @test adopted.pts == (adopted.identity.sequence - 1) * options.period_ns
                            @test all(iszero, command)
                            @test all(iszero, view(packet, 1, :, :))
                            @test any(==(1f0), view(packet, 2, :, :))
                            @test all(x -> x in (0f0, 1f0), view(packet, 2, :, :))
                            if generation == 1
                                first_domain = published.identity.domain
                                held = control(run_control_request(2, :stopped), Source.RUN, 2)
                                @test held[5] == 0 && !held[8]
                                reset = control(reset_control_request(3), Source.RESET, 3)
                                @test reset[5] == 0 && reset[6] == 2 && reset[7] == 0
                                @test C.read_json(report)["pupil_output"]["generation"] == 2
                            else
                                @test published.identity.domain == first_domain
                            end
                        end
                    end
                end
            end
        end
    end
end
