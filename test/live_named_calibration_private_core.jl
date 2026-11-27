# Transport-only named held probes. Each test owns and closes a private core.
@testset "private-core named held calibration: $mode" for mode in (:complete, :missing, :duplicate, :stale)
    prefix = get(ENV, "PIPEWIREAO_HIL_TEST_PREFIX", String(PipeWireAO_jll.artifact_dir))
    native_libdir = isdir(joinpath(prefix, "lib", "x86_64-linux-gnu")) ?
        joinpath(prefix, "lib", "x86_64-linux-gnu") : joinpath(prefix, "lib")
    daemon_path = joinpath(prefix, "bin", "pipewire-ao")
    mktempdir(prefix="aos-named-calibration-private-") do temporary
        runtime = joinpath(temporary, "runtime")
        configuration_directory = joinpath(temporary, "configuration")
        mkpath(runtime)
        mkpath(configuration_directory)
        remote = "aos-named-calibration-$(getpid())-$(time_ns())"
        write(joinpath(configuration_directory, "private-core.conf"), _private_core_configuration(remote))
        write(joinpath(configuration_directory, "client.conf"), _HIL_CLIENT_CONFIGURATION)
        environment = Dict(
            "XDG_RUNTIME_DIR" => runtime, "PIPEWIREAO_RUNTIME_DIR" => runtime,
            "PIPEWIREAO_CONFIG_DIR" => configuration_directory,
            "PIPEWIREAO_MODULE_DIR" => joinpath(native_libdir, "pipewire-ao-0.3"),
            "PIPEWIREAO_SPA_PLUGIN_DIR" => joinpath(native_libdir, "spa-ao-0.2"),
            "PIPEWIREAO_DEBUG" => "0",
            "LD_LIBRARY_PATH" => native_libdir * ":" * get(ENV, "LD_LIBRARY_PATH", ""))
        log = open(joinpath(temporary, "core.log"), "w+")
        daemon = prepared = registry = consumer = nothing
        producers = ()
        links = PipeWireAO.Link[]
        try
            daemon = run(pipeline(addenv(`$daemon_path -c private-core.conf`, environment);
                stdout=log, stderr=log); wait=false)
            _wait_for_hil_condition(() -> ispath(joinpath(runtime, remote)))
            withenv(environment...) do
                boundary = multi_command_boundary(; calibration=true)
                initial = map(copy, boundary.command_input)
                configuration = multi_command_configuration(; remote, frame_encoding=:uint16,
                    timeout_ns=UInt64(10_000_000_000))
                prepared = prepare_pipewire_calibration(boundary, configuration)
                @test keys(prepared.command_stream) == (:woofer, :tweeter, :tiptilt)
                @test pipewire_calibration_status(prepared).received_probe_sequence ==
                    (woofer=0, tweeter=0, tiptilt=0)
                commands = (woofer=Float32[3, 4], tweeter=Float32[5, 6, 7], tiptilt=Float32[8])
                sources = map(commands) do command
                    CalibrationLiveProbeSource(copy(command), Threads.Atomic{UInt64}(0),
                        Threads.Atomic{UInt64}(0), StreamBuffer())
                end
                domain = pipewire_calibration_status(prepared).acquisition_domain
                sink = CalibrationLiveExposureSink(zeros(UInt16, 3),
                    AcquisitionIdentity(domain, UInt64(0), UInt64(0)), UInt64(0),
                    Threads.Atomic{UInt64}(0), Threads.Atomic{UInt64}(0), Threads.Atomic{Bool}(false),
                    CalibrationTransport.CompletionSignal(), StreamBuffer(),Threads.Atomic{UInt64}(0),Threads.Atomic{UInt64}(0))
                CalibrationTransport._with_loop_access(prepared) do
                    producers = map(keys(sources)) do key
                        stream = Stream(prepared.core, "named-probe-$(key)";
                            properties=Dict("node.name" => "named-probe-$(key)",
                                "node.want-driver" => "true", "priority.driver" => "100",
                                "node.reliable" => "true"), on_process=sources[key])
                        connect!(stream, :output;
                            flags=STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT | STREAM_DRIVER,
                            params=CalibrationTransport._stream_parameters(
                                CalibrationTransport._format(commands[key], configuration.rate),
                                configuration.command_schema[key];metadata=()))
                        stream
                    end
                    consumer = Stream(prepared.core, "named-calibration-wfs";
                        properties=Dict("node.name" => "named-calibration-wfs", "node.reliable" => "true"),
                        on_process=sink)
                    connect!(consumer, :input; flags=STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT,
                        params=CalibrationTransport._stream_parameters(
                            NdArrayFormat(NdArray.U16_LE, (3,); rate=configuration.rate, layout=NdArray.ROW_MAJOR),
                            configuration.frame_schema))
                    registry = Registry(prepared.core)
                end
                connections = ((prepared.frame_stream, consumer),
                    ((producer, prepared.command_stream[key]) for (key, producer) in zip(keys(sources), producers))...)
                _wait_for_hil_condition() do
                    roundtrip(registry)
                    CalibrationTransport._with_loop_access(prepared) do
                        all(connections) do (output, input)
                            outputs = find_globals(registry; interface="PipeWire:Interface:Port",
                                properties=("node.id"=>string(node_id(output)), "port.direction"=>"out"))
                            inputs = find_globals(registry; interface="PipeWire:Interface:Port",
                                properties=("node.id"=>string(node_id(input)), "port.direction"=>"in"))
                            length(outputs) == length(inputs) == 1
                        end
                    end
                end
                for (output, input) in connections
                    push!(links, calibration_live_link(prepared.core, registry, output, input))
                end
                roundtrip(prepared.core)
                CalibrationTransport._with_loop_access(prepared) do
                    set_active!(consumer, true)
                    foreach(producer -> set_active!(producer, true), producers)
                end
                start!(prepared)
                function submit_port!(key, token)
                    index = findfirst(==(key), keys(sources))
                    CalibrationTransport._with_loop_access(prepared) do
                        sources[key].ready[] = token
                        trigger_process!(producers[index])
                    end
                end
                delayed = Ref{Task}()
                submit = () -> begin
                    # Reverse receipt order; the final port is held back deliberately.
                    submit_port!(:tweeter, UInt64(7))
                    submit_port!(:woofer, UInt64(7))
                    mode === :missing && return nothing
                    delayed[] = @async begin
                        _wait_for_hil_condition(
                            () -> prepared.state.transport.woofer.command_ready[] == 7 &&
                                prepared.state.transport.tweeter.command_ready[] == 7,
                            timeout=5.0)
                        @test boundary.command_input == initial
                        @test CalibrationGraphs.graph_step_sequence(boundary.graph) == 0
                        if mode === :duplicate
                            # Requeue the same port's token instead of completing the set.
                            CalibrationTransport._with_loop_access(prepared) do
                                sources.woofer.published[] = 0
                            end
                            submit_port!(:woofer, UInt64(7))
                        else
                            submit_port!(:tiptilt, mode === :stale ? UInt64(6) : UInt64(7))
                        end
                    end
                    nothing
                end
                if mode !== :complete
                    budget = mode === :missing ? UInt64(250_000_000) : UInt64(5_000_000_000)
                    started = time_ns()
                    @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(7);
                        submit, timeout_ns=budget)
                    # One operation budget covers the whole set, including its missing port.
                    @test time_ns() - started < UInt64(10_000_000_000)
                    isassigned(delayed) && fetch(delayed[])
                    @test pipewire_calibration_status(prepared).failed
                    @test pipewire_calibration_status(prepared).last_probe_sequence == 7
                    @test pipewire_calibration_status(prepared).received_probe_sequence ==
                        (woofer=7, tweeter=7, tiptilt=0)
                    @test boundary.command_input == initial
                    @test CalibrationGraphs.graph_step_sequence(boundary.graph) == 0
                    @test_throws PipeWireHILError pipewire_probe_values(prepared)
                    @test_throws PipeWireHILError reset_pipewire_calibration!(prepared)
                    @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(8))
                else
                    @test adopt_pipewire_probe!(prepared, UInt64(7); submit) == 7
                    fetch(delayed[])
                    snapshot = pipewire_probe_values(prepared)
                    @test keys(snapshot) == keys(commands)
                    @test snapshot == commands
                    for key in keys(commands)
                        @test reinterpret(UInt32, snapshot[key]) == reinterpret(UInt32, commands[key])
                    end
                    @test boundary.command_input == (woofer=Float32[6, 8], tweeter=Float32[15, 18, 21], tiptilt=Float32[32])
                    driver = CalibrationGraphs.FixedStepModelTimeDriver(
                        CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(configuration.exposure_duration_ns)))
                    old = nothing
                    for generation in UInt64(1):UInt64(2)
                        for sequence in UInt64(1):UInt64(2)
                            exposure = step_pipewire_exposure!(prepared, driver)
                            @test exposure.identity.generation == generation
                            @test exposure.identity.sequence == sequence
                            @test exposure.probe_sequence == 6 + generation
                            @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(9))
                            @test_throws PipeWireHILError reset_pipewire_calibration!(prepared, driver)
                            CalibrationTransport._await_completion(
                                () -> sink.failed[] || (sink.generation[] == generation && sink.sequence[] == sequence),
                                sink.signal, UInt64(5_000_000_000), "test.named-WFS")
                            @test !sink.failed[]
                            @test sink.identity == exposure.identity
                            @test sink.values == UInt16[14, 54, 32]
                            old === nothing || @test_throws PipeWireHILError complete_pipewire_exposure!(prepared, old)
                            @test complete_pipewire_exposure!(prepared, sink.identity) == exposure.identity
                            old = exposure.identity
                        end
                        generation == 2 && break
                        reset_pipewire_calibration!(prepared, driver)
                        @test boundary.command_input == initial
                        @test all(command -> all(iszero, command), snapshot)
                        @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(7))
                        @test_throws PipeWireHILError pipewire_probe_values(prepared)
                        @test adopt_pipewire_probe!(prepared, UInt64(8); submit=() -> begin
                            for key in keys(sources)
                                submit_port!(key, UInt64(8))
                            end
                        end) == 8
                    end
                    stop!(prepared)
                    @test !prepared.started
                end
            end
        finally
            if prepared !== nothing
                # Disconnect callbacks may retain the original terminal failure.
                CalibrationTransport._with_loop_access(prepared) do
                    for resource in (reverse(links)..., consumer, reverse(producers)..., registry)
                        resource === nothing || close(resource)
                    end
                end
                close_error = try
                    close(prepared)
                    nothing
                catch error
                    error
                end
                if mode in (:duplicate, :stale)
                    @test close_error isa PipeWireHILError
                    @test close_error.field == "calibration.probe"
                else
                    @test close_error === nothing
                end
                @test prepared.closed
                @test all(stream -> !isopen(stream), values(prepared.command_stream))
            end
            _stop_hil_process(daemon)
            close(log)
        end
    end
end
