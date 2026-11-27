# Software transport qualification only: the plant fixture copies its command.
# No instrument optics, WFS estimator, or interaction matrix is qualified here.
mutable struct CalibrationLiveProbeSource
    values::Vector{Float32}
    ready::Threads.Atomic{UInt64}
    published::Threads.Atomic{UInt64}
    buffer::StreamBuffer
end

function (source::CalibrationLiveProbeSource)(stream::Stream)
    sequence = source.ready[]
    (sequence == 0 || sequence == source.published[]) && return nothing
    dequeue_buffer!(source.buffer, stream) || return nothing
    queued = false
    try
        data = buffer_data(source.buffer)
        copyto!(reinterpret(Float32, buffer_memory(data, sizeof(source.values))), source.values)
        set_chunk!(data; size=sizeof(source.values), stride=sizeof(Float32))
        initialize_metadata!(source.buffer)
        set_buffer_header!(source.buffer, BufferHeader(UInt32(0), UInt32(0), Int64(0), Int64(0), sequence))
        queue_buffer!(source.buffer, stream)
        queued = true
        source.published[] = sequence
    finally
        queued || return_buffer!(source.buffer, stream)
    end
    return nothing
end

mutable struct CalibrationLiveExposureSink
    values::Vector{UInt16}
    identity::AcquisitionIdentity
    duration::UInt64
    generation::Threads.Atomic{UInt64}
    sequence::Threads.Atomic{UInt64}
    failed::Threads.Atomic{Bool}
    signal::CalibrationTransport.CompletionSignal
    buffer::StreamBuffer
    attempts::Threads.Atomic{UInt64}
    empty::Threads.Atomic{UInt64}
end

function (sink::CalibrationLiveExposureSink)(stream::Stream)
    Threads.atomic_add!(sink.attempts,UInt64(1))
    try
        if !dequeue_buffer!(sink.buffer, stream)
            Threads.atomic_add!(sink.empty,UInt64(1))
            return nothing
        end
        queued = false
        try
            metadata = metadata_records(sink.buffer)
            metadata === nothing && error("fixture exposure has no acquisition metadata")
            identity = acquisition_identity(metadata)
            identity === nothing && error("fixture exposure has no identity")
            header = buffer_header(sink.buffer)
            header === nothing && error("fixture exposure has no Header")
            copyto!(sink.values, reinterpret(UInt16, bytes(buffer_data(sink.buffer))))
            sink.identity = identity
            sink.duration = CalibrationTransport._exposure_duration(metadata)
            queue_buffer!(sink.buffer, stream)
            queued = true
            sink.generation[] = identity.generation
            sink.sequence[] = identity.sequence
            CalibrationTransport._signal_completion!(sink.signal)
        finally
            queued || return_buffer!(sink.buffer, stream)
        end
    catch
        sink.failed[] = true
        CalibrationTransport._signal_completion!(sink.signal)
        rethrow()
    end
    return nothing
end

struct CalibrationLivePublicationObserver{State}
    state::State
    attempted::Threads.Atomic{Bool}
    signal::CalibrationTransport.CompletionSignal
    probe_attempted::Threads.Atomic{Bool}
    probe_signal::CalibrationTransport.CompletionSignal
end

function (observer::CalibrationLivePublicationObserver)(stream::Stream)
    transport = observer.state.transport
    if transport.frame_ready[] != transport.frame_published[]
        observer.attempted[] = true
        CalibrationTransport._signal_completion!(observer.signal)
    end
    if observer.state.expected_probe[] != 0 && transport.command_ready[] == 0
        observer.probe_attempted[] = true
        CalibrationTransport._signal_completion!(observer.probe_signal)
    end
    return nothing
end

function calibration_live_link(core, registry, output, input)
    roundtrip(registry)
    output_port = only(find_globals(registry;
        interface="PipeWire:Interface:Port",
        properties=("node.id" => string(node_id(output)), "port.direction" => "out"),
    ))
    input_port = only(find_globals(registry;
        interface="PipeWire:Interface:Port",
        properties=("node.id" => string(node_id(input)), "port.direction" => "in"),
    ))
    return with_thread_loop_lock(main_loop(core)) do _
        create_object(core, "link-factory", PipeWireAO.Link; properties=Dict(
            "link.output.node" => string(node_id(output)),
            "link.output.port" => string(output_port.id),
            "link.input.node" => string(node_id(input)),
            "link.input.port" => string(input_port.id),
            "object.linger" => "false",
        ))
    end
end

@testset "private-core held-probe command and exposure transport: $failure_mode, $frame_type" for failure_mode in (:throw, :deadline), frame_type in (Float32, UInt16)
    prefix = get(ENV, "PIPEWIREAO_HIL_TEST_PREFIX", String(PipeWireAO_jll.artifact_dir))
    native_libdir = isdir(joinpath(prefix, "lib", "x86_64-linux-gnu")) ? joinpath(prefix, "lib", "x86_64-linux-gnu") : joinpath(prefix, "lib")
    daemon_path = joinpath(prefix, "bin", "pipewire-ao")
    @test isfile(daemon_path)
    mktempdir(prefix="aos-calibration-private-") do temporary
        runtime = joinpath(temporary, "runtime")
        config_dir = joinpath(temporary, "configuration")
        mkpath(runtime)
        mkpath(config_dir)
        core_name = "aos-calibration-$(getpid())-$(time_ns())"
        write(joinpath(config_dir, "private-core.conf"), _private_core_configuration(core_name))
        write(joinpath(config_dir, "client.conf"), _HIL_CLIENT_CONFIGURATION)
        environment = Dict(
            "XDG_RUNTIME_DIR" => runtime, "PIPEWIREAO_RUNTIME_DIR" => runtime,
            "PIPEWIREAO_CONFIG_DIR" => config_dir,
            "PIPEWIREAO_MODULE_DIR" => joinpath(native_libdir, "pipewire-ao-0.3"),
            "PIPEWIREAO_SPA_PLUGIN_DIR" => joinpath(native_libdir, "spa-ao-0.2"),
            "PIPEWIREAO_DEBUG" => "0",
            "LD_LIBRARY_PATH" => native_libdir * ":" * get(ENV, "LD_LIBRARY_PATH", ""),
        )
        log = open(joinpath(temporary, "core.log"), "w+")
        daemon = prepared = registry = producer = consumer = frame_link = command_link = observer_listener = nothing
        try
            daemon = run(pipeline(addenv(`$daemon_path -c private-core.conf`, environment);
                stdout=log, stderr=log); wait=false)
            _wait_for_hil_condition(() -> ispath(joinpath(runtime, core_name)))
            withenv(environment...) do
                fixture = calibration_transport_fixture(; scale=0.5f0, frame_type)
                configuration = PipeWireHILConfiguration(
                    remote=core_name, frame_node_name="calibration-plant-frame",
                    command_node_name="calibration-demanded-command",
                    frame_schema="test.calibration.u16/1", command_schema="test.calibration.f32/1",
                    rate=SPA.Fraction(20_000_000, 1), exposure_duration_ns=50,
                    frame_encoding=:uint16, command_scale=0.5f0,
                    timeout_ns=10_000_000_000,
                )
                prepared = prepare_pipewire_calibration(fixture.boundary, configuration;reference_clock=METADATA_CLOCK_REALTIME)
                @test CalibrationTransport._frame_state(prepared.state.transport).reference_clock==METADATA_CLOCK_REALTIME
                source = CalibrationLiveProbeSource(zeros(Float32, 2), Threads.Atomic{UInt64}(0), Threads.Atomic{UInt64}(0), StreamBuffer())
                sink = CalibrationLiveExposureSink(
                    zeros(UInt16, 2), AcquisitionIdentity(prepared.state.transport.acquisition_domain, UInt64(0), UInt64(0)),
                    UInt64(0), Threads.Atomic{UInt64}(0), Threads.Atomic{UInt64}(0), Threads.Atomic{Bool}(false),
                    CalibrationTransport.CompletionSignal(), StreamBuffer(),Threads.Atomic{UInt64}(0),Threads.Atomic{UInt64}(0),
                )
                CalibrationTransport._with_loop_access(prepared) do
                    producer = Stream(prepared.core, "calibration-test-probe";
                        properties=Dict("node.name" => "calibration-test-probe", "node.want-driver" => "true", "priority.driver" => "100", "node.reliable" => "true"),
                        on_process=source,
                    )
                    consumer = Stream(prepared.core, "calibration-test-wfs";
                        properties=Dict("node.name" => "calibration-test-wfs", "node.reliable" => "true"), on_process=sink,
                    )
                    flags = STREAM_MAP_BUFFERS | STREAM_INACTIVE | STREAM_DONT_RECONNECT
                    connect!(producer, :output; flags=flags | STREAM_DRIVER,
                        params=CalibrationTransport._stream_parameters(CalibrationTransport._format(source.values, configuration.rate), configuration.command_schema;metadata=()))
                    connect!(consumer, :input; flags,
                        params=CalibrationTransport._stream_parameters(NdArrayFormat(NdArray.U16_LE, (2,); rate=configuration.rate, layout=NdArray.ROW_MAJOR), configuration.frame_schema))
                    registry = Registry(prepared.core)
                end
                roundtrip(registry)
                frame_link = calibration_live_link(prepared.core, registry, prepared.frame_stream, consumer)
                command_link = calibration_live_link(prepared.core, registry, producer, prepared.command_stream)
                roundtrip(prepared.core)
                CalibrationTransport._with_loop_access(prepared) do
                    set_active!(consumer, true)
                    set_active!(producer, true)
                end
                start!(prepared)
                @test pipewire_calibration_status(prepared).frame_publication_cycle_sequence == 0
                driver = CalibrationGraphs.FixedStepModelTimeDriver(CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50)))
                submit(sequence, values) = () -> CalibrationTransport._with_loop_access(prepared) do
                    copyto!(source.values, values)
                    source.ready[] = sequence
                    trigger_process!(producer)
                end
                observer = CalibrationLivePublicationObserver(
                    prepared.state, Threads.Atomic{Bool}(false), CalibrationTransport.CompletionSignal(),
                    Threads.Atomic{Bool}(false), CalibrationTransport.CompletionSignal(),
                )
                observer_listener = CalibrationTransport._with_loop_access(prepared) do
                    add_listener!(prepared.frame_stream; on_trigger_done=observer)
                end
                delayed_producer = Ref{Task}()
                delayed_submit = () -> begin
                    @test_throws PipeWireHILError pipewire_probe_values(prepared)
                    delayed_producer[] = @async begin
                        CalibrationTransport._await_completion(
                            () -> observer.probe_attempted[], observer.probe_signal,
                            UInt64(10_000_000_000), "fixture.first-empty-probe-cycle",
                        )
                        submit(UInt64(7), Float32[6, 8])()
                    end
                    return nothing
                end
                @test CalibrationGraphs.graph_step_sequence(fixture.graph) == 0
                @test_throws PipeWireHILError pipewire_probe_values(prepared)
                submitted = Ref(0)
                for invalid in (UInt64(0), 1, UInt32(1), configuration.timeout_ns + UInt64(1), typemax(UInt64))
                    @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(7);
                        submit=() -> (submitted[] += 1), timeout_ns=invalid)
                    @test_throws PipeWireHILError step_pipewire_exposure!(prepared, driver;
                        before_publish=exposure -> (submitted[] += 1), timeout_ns=invalid)
                    @test CalibrationGraphs.graph_step_sequence(fixture.graph) == CalibrationGraphs.model_time_sequence(driver) == 0
                    @test prepared.state.expected_probe[] == prepared.state.last_probe_sequence == 0
                    @test prepared.state.pending_exposure === nothing
                    @test !prepared.state.failed[]
                end
                @test submitted[] == 0
                @test adopt_pipewire_probe!(prepared, UInt64(7); submit=delayed_submit) == UInt64(7)
                @test fetch(delayed_producer[]) === producer
                @test observer.probe_attempted[]
                @test source.published[] == UInt64(7)
                wire_probe = pipewire_probe_values(prepared)
                @test wire_probe === prepared.state.wire_command
                @test wire_probe == Float32[6, 8]
                @test CalibrationGraphs.graph_input(fixture.graph, Val(:command)) == Float32[3, 4]
                @test CalibrationGraphs.graph_step_sequence(fixture.graph) == CalibrationGraphs.model_time_sequence(driver) == 0
                @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(8); submit=5)
                @test pipewire_calibration_status(prepared).last_probe_sequence == 7
                loans = StreamBuffer[]
                CalibrationTransport._with_loop_access(prepared) do
                    while true
                        buffer = StreamBuffer()
                        dequeue_buffer!(buffer, prepared.frame_stream) || break
                        push!(loans, buffer)
                    end
                end
                @test length(loans) >= 2
                # Returning a legitimate output loan must make it dequeueable
                # again, rather than trip the native duplicate-loan protection.
                reclaimed = StreamBuffer()
                returned = pop!(loans)
                reclaimed_available = CalibrationTransport._with_loop_access(prepared) do
                    return_buffer!(returned, prepared.frame_stream)
                    dequeue_buffer!(reclaimed, prepared.frame_stream)
                end
                @test reclaimed_available
                reclaimed_available && push!(loans, reclaimed)
                first_identity = nothing
                arming_calls = Ref(0)
                owner_task = current_task()
                before_publish = exposure -> begin
                    arming_calls[] += 1
                    @test current_task() === owner_task
                    @test prepared.state.pending_exposure === exposure
                    @test exposure.identity.sequence == CalibrationGraphs.graph_step_sequence(fixture.graph) == CalibrationGraphs.model_time_sequence(driver)
                    @test exposure.identity === AcquisitionIdentity(prepared.state.transport.acquisition_domain, UInt64(1), UInt64(2))
                    @test prepared.state.transport.frame_transport == UInt16[3, 4]
                    @test prepared.state.transport.frame_ready[] == prepared.state.transport.frame_published[] == 1
                    @test sink.sequence[] == 1
                    # A different Julia thread can acquire the native loop lock
                    # while the owner callable runs.
                    @test fetch(Threads.@spawn CalibrationTransport._with_loop_access(() -> true, prepared))
                    return nothing
                end
                for sequence in UInt64(1):UInt64(16)
                    exposure = if sequence == 1
                        waiting = @async step_pipewire_exposure!(prepared, driver)
                        CalibrationTransport._await_completion(
                            () -> observer.attempted[], observer.signal,
                            UInt64(10_000_000_000), "fixture.no-buffer-first",
                        )
                        @test prepared.state.transport.frame_published[] == 0
                        @test CalibrationGraphs.graph_step_sequence(fixture.graph) == 1
                        @test CalibrationGraphs.model_time_sequence(driver) == 1
                        CalibrationTransport._with_loop_access(prepared) do
                            for buffer in loans
                                return_buffer!(buffer, prepared.frame_stream)
                            end
                        end
                        fetch(waiting)
                    elseif sequence == 2
                        step_pipewire_exposure!(prepared, driver; before_publish, timeout_ns=UInt64(5_000_000_000))
                    else
                        step_pipewire_exposure!(prepared, driver)
                    end
                    first_identity === nothing && (first_identity = exposure.identity)
                    @test exposure.identity.sequence == sequence
                    @test exposure.timestamp == CalibrationGraphs.ModelTimestamp(Int64(sequence - 1) * 50)
                    CalibrationTransport._await_completion(
                        () -> pipewire_calibration_status(prepared).frame_publication_cycle_sequence == sequence,
                        prepared.state.signal, UInt64(10_000_000_000), "fixture.source-publication-cycle",
                    )
                    @test pipewire_calibration_status(prepared).frame_publication_cycle_sequence == sequence
                    @test prepared.state.pending_exposure === exposure
                    @test exposure.probe_sequence == 7
                    @test pipewire_probe_values(prepared) === wire_probe
                    @test wire_probe == Float32[6, 8]
                    @test_throws PipeWireHILError step_pipewire_exposure!(prepared, driver)
                    @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(8))
                    @test_throws PipeWireHILError reset_pipewire_calibration!(prepared, driver)
                    @test CalibrationGraphs.graph_step_sequence(fixture.graph) == CalibrationGraphs.model_time_sequence(driver) == sequence
                    try
                        CalibrationTransport._await_completion(
                            () -> sink.failed[] || (sink.generation[] == exposure.identity.generation && sink.sequence[] == sequence),
                            sink.signal, UInt64(10_000_000_000), "fixture.WFS",
                        )
                    catch error
                        before=(attempts=sink.attempts[],empty=sink.empty[],generation=sink.generation[],sequence=sink.sequence[])
                        CalibrationTransport._with_loop_access(prepared) do
                            sink(consumer)
                        end
                        after=(attempts=sink.attempts[],empty=sink.empty[],generation=sink.generation[],sequence=sink.sequence[])
                        @error "WFS completion diagnostic" before after expected=exposure.identity status=pipewire_calibration_status(prepared)
                        rethrow(error)
                    end
                    @test !sink.failed[]
                    @test sink.identity === exposure.identity
                    @test sink.duration == 50
                    @test sink.values == UInt16[3, 4]
                    @test complete_pipewire_exposure!(prepared, exposure) === exposure.identity
                    @test_throws PipeWireHILError complete_pipewire_exposure!(prepared, exposure)
                    @test_throws PipeWireHILError complete_pipewire_exposure!(prepared, sequence)
                end
                @test arming_calls[] == 1
                @test reset_pipewire_calibration!(prepared, driver) === prepared
                @test pipewire_calibration_status(prepared).frame_publication_cycle_sequence == 0
                @test wire_probe == zeros(Float32, 2)
                @test_throws PipeWireHILError pipewire_probe_values(prepared)
                @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(7))
                @test_throws PipeWireHILError step_pipewire_exposure!(prepared, driver)
                @test CalibrationGraphs.graph_step_sequence(fixture.graph) == CalibrationGraphs.model_time_sequence(driver) == 0
                @test adopt_pipewire_probe!(prepared, UInt64(8); submit=submit(UInt64(8), Float32[10, 12])) == UInt64(8)
                @test pipewire_probe_values(prepared) === wire_probe
                @test wire_probe == Float32[10, 12]
                for sequence in UInt64(1):UInt64(4)
                    exposure = step_pipewire_exposure!(prepared, driver)
                    @test exposure.identity.generation == 2
                    @test exposure.identity.sequence == sequence
                    @test exposure.probe_sequence == 8
                    @test exposure.timestamp == CalibrationGraphs.ModelTimestamp(Int64(sequence - 1) * 50)
                    CalibrationTransport._await_completion(
                        () -> sink.failed[] || (sink.generation[] == UInt64(2) && sink.sequence[] == sequence),
                        sink.signal, UInt64(10_000_000_000), "fixture.WFS",
                    )
                    @test !sink.failed[]
                    @test sink.values == UInt16[5, 6]
                    @test_throws PipeWireHILError complete_pipewire_exposure!(prepared, first_identity)
                    @test complete_pipewire_exposure!(prepared, sink.identity) === exposure.identity
                end
                @test !Base.process_exited(daemon)
                @test !pipewire_calibration_status(prepared).failed
                # A failed owner callable leaves the actual completed exposure
                # visible to status, while source admission remains at zero.
                reset_pipewire_calibration!(prepared, driver)
                @test adopt_pipewire_probe!(prepared, UInt64(9); submit=submit(UInt64(9), Float32[6, 8]),
                    timeout_ns=UInt64(5_000_000_000)) == UInt64(9)
                failed_calls = Ref(0)
                failure = exposure -> begin
                    failed_calls[] += 1
                    @test exposure.identity.generation == 3
                    @test prepared.state.pending_exposure === exposure
                    @test prepared.state.transport.frame_ready[] == prepared.state.transport.frame_published[] == 0
                    if failure_mode == :throw
                        error("owner collector arming failed")
                    end
                    # The owner consumes the original caller-clamped operation
                    # budget. Publication must not receive a fresh deadline.
                    sleep(1.1)
                    return nothing
                end
                if failure_mode == :throw
                    @test_throws ErrorException step_pipewire_exposure!(prepared, driver; before_publish=failure)
                else
                    @test_throws PipeWireHILError step_pipewire_exposure!(prepared, driver;
                        before_publish=failure, timeout_ns=UInt64(1_000_000_000))
                end
                @test failed_calls[] == 1
                @test pipewire_calibration_status(prepared).failed
                @test_throws PipeWireHILError pipewire_probe_values(prepared)
                @test CalibrationGraphs.graph_step_sequence(fixture.graph) == CalibrationGraphs.model_time_sequence(driver) == 1
                @test prepared.state.transport.frame_ready[] == prepared.state.transport.frame_published[] == 0
                @test sink.generation[] == 2
                @test_throws PipeWireHILError step_pipewire_exposure!(prepared, driver)
                @test_throws PipeWireHILError adopt_pipewire_probe!(prepared, UInt64(10))
                @test_throws PipeWireHILError reset_pipewire_calibration!(prepared, driver)
                @test CalibrationGraphs.graph_step_sequence(fixture.graph) == CalibrationGraphs.model_time_sequence(driver) == 1

            end
        finally
            if prepared !== nothing
                CalibrationTransport._with_loop_access(prepared) do
                    for resource in (observer_listener, command_link, frame_link, consumer, producer, registry)
                        resource === nothing || close(resource)
                    end
                end
                close(prepared)
                @test_throws PipeWireHILError pipewire_probe_values(prepared)
            end
            _stop_hil_process(daemon)
            flush(log)
            evidence = get(ENV, "PIPEWIREAO_HIL_TEST_LOG", "")
            isempty(evidence) || (seekstart(log); write(evidence, read(log, String)))
            close(log)
        end
    end
end
