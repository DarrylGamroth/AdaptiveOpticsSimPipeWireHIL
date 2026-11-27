function wire_configuration(options, remote, control_node)
    return PipeWireHILConfiguration(;
        remote,
        frame_node_name = control_node,
        command_node_name = options.command_node,
        frame_schema = options.frame_schema,
        command_schema = options.command_schema,
        rate = SPA.Fraction(UInt32(options.rate), UInt32(1)),
        exposure_duration_ns = options.exposure_ns,
        frame_encoding = options.frame_encoding,
        command_scale = options.command_scale,
    )
end

function validate_command_buffers(command::AbstractArray, count::Integer)
    size(command) == (count,) ||
        throw(DimensionMismatch("AOS command shape differs from configured count"))
    return nothing
end
function validate_command_buffers(commands::NamedTuple, counts::NamedTuple)
    Set(keys(commands)) == Set(keys(counts)) ||
        throw(ArgumentError("AOS command buffer names differ from configuration"))
    for name in keys(commands)
        validate_command_buffers(commands[name], counts[name])
    end
    return nothing
end
zero_command_buffers!(command::AbstractArray) = fill!(command, 0.0f0)
zero_command_buffers!(commands::NamedTuple) =
    foreach(zero_command_buffers!, values(commands))
# A named exchange completes after every command has arrived. Diagnostics use
# the latest receipt from that same source frame, retaining the scalar contract.
completed_frame_timing(timing) = timing
function completed_frame_timing(timings::NamedTuple)
    first_timing = first(values(timings))
    all(
        timing ->
            timing.sequence == first_timing.sequence &&
            timing.source_published_nanoseconds ==
            first_timing.source_published_nanoseconds,
        values(timings),
    ) || error("completed command timings differ from their common source frame")
    received = maximum(timing -> timing.command_received_nanoseconds, values(timings))
    return PipeWireFrameCommandTiming(
        first_timing.sequence, first_timing.source_published_nanoseconds, received
    )
end

# Pacing skips elapsed wall periods without skipping scientific model steps.
function next_deadline(deadline::Int64, period::Int64, now::Int64)
    period > 0 || throw(ArgumentError("pacing period must be positive"))
    candidate = widen(deadline) + period
    missed = now >= candidate ? div(widen(now) - candidate, period) + 1 : Int128(0)
    result = candidate + missed * period
    typemin(Int64) <= result <= typemax(Int64) ||
        throw(OverflowError("source pacing deadline exceeds signed nanoseconds"))
    return Int64(result)
end

command_mirror_properties(options, ::AbstractString, properties) = properties
function command_mirror_properties(options, names::NamedTuple, properties)
    return NamedTuple{keys(names)}(
        map(
            name -> PipeWireAORTC.InstrumentDefinition.mirror_properties(options, name),
            keys(names),
        ),
    )
end
function publish_mirror_properties!(stream, properties)
    isempty(properties) || update_properties!(stream, properties)
    return nothing
end
function publish_mirror_properties!(streams::NamedTuple, properties::NamedTuple)
    for name in keys(streams)
        publish_mirror_properties!(streams[name], properties[name])
    end
    return nothing
end

applied_command_names(name::AbstractString, enabled) =
    enabled ? "simulator-applied-command" : nothing
applied_command_names(names::NamedTuple, enabled) =
    enabled ? map(name -> name * "-applied", names) : nothing

function run_prepared!(
    options,
    science,
    pipewire,
    observation,
    mailbox,
    state,
    phase = nothing,
    pupil = nothing;
    clock::Clocks.AbstractNanoClock = Clocks.SystemNanoClock(),
    idle_strategy = SimRunner.current_strategy(),
)
    runtime = options.bootstrap_runtime
    free_running = pipewire.free_running !== nothing
    reset_owner! =
        () -> begin
            # Drain optional asynchronous snapshots before graph reset. Native
            # source controls are applied only here, after the preceding exchange.
            OpticalObservation.drain!(observation)
            PupilOutput.drain!(pupil; check = options.owner_check, clock)
            save_phase_timing!(phase)
            reset_pipewire_hil!(pipewire, science.driver; idle_strategy)
            # The plant reset has committed this empty cursor even if optional
            # observation rewarming subsequently fails before the control ACK.
            state.generation = Int64(frame_acquisition_generation(pipewire))
            state.sequence = 0
            PupilOutput.reset!(
                pupil, UInt64(state.generation); check = options.owner_check, clock
            )
            state.completed = false
            state.deadline_ns = 0
            reset_phase_timing!(phase, state.generation)
            OpticalObservation.reset!(
                observation,
                pipewire;
                check = options.owner_check,
                cpu = get(options, :optical_diagnostics_cpu, nothing),
                clock,
            )
        end
    ticket = Bootstrap.take_connect!(runtime)
    Bootstrap.check_connect!(runtime, ticket)
    start!(pipewire)
    wait_registered!(runtime, ticket) do
        with_frame_stream(pipewire) do frame
            all(registered, (frame, Configuration.command_items(pipewire.command_stream)...))
        end && PupilOutput.registered(pupil)
    end
    Bootstrap.connected!(runtime, ticket)
    Bootstrap.await_connected!(runtime, ticket)
    println("SIMULATOR_CONNECTED sequence=0 held=true")
    flush(stdout)
    return SimRunner.run!(idle_strategy) do
        Bootstrap.cancelled(runtime) && return nothing
        now_ns = SourceControl.observe_clock!(state, Clocks.time_nanos(clock))
        consume!(
            FrameAccess(pipewire), options, mailbox, state,
            max(options.pacing_period_ns, Int64(1)), reset_owner!, now_ns;
            after = () -> state.running || save_phase_timing!(phase),
        )
        Bootstrap.cancelled(runtime) && return nothing
        if !state.running
            sleep(0.001)
            return 0
        end
        options.pacing_period_ns != 0 && now_ns < state.deadline_ns && return 0
        model_ns = model_nanoseconds(next_model_timestamp(science.driver))
        deadline = state.deadline_ns
        timing = exchange_timing(phase)
        sequence = if free_running
            step_free_running!(pipewire, science.driver)
        else
            exchange_frame!(pipewire, science.driver; timing, idle_strategy)
        end
        state.sequence = sequence
        model_time_sequence(science.driver) == sequence ||
            error("model-time sequence differs from source")
        frame_timing = free_running ? nothing : completed_frame_timing(frame_command_timing(pipewire))
        PupilOutput.sample!(pupil; generation = UInt64(state.generation), model_ns)
        # Throttle admission retains the completed-exchange host measurement.
        completed_ns = SourceControl.observe_clock!(state, Clocks.time_nanos(clock))
        OpticalObservation.sample!(observation, free_running ? completed_ns : frame_timing, model_ns, completed_ns)
        # Observation enqueue/copy work finishes after its throttle decision.
        # This distinct measurement preserves saved optical-finish timing and
        # skip-to-next-phase pacing after all exchange/observation work.
        optical_finished = SourceControl.observe_clock!(state, Clocks.time_nanos(clock))
        state.deadline_ns =
            options.pacing_period_ns == 0 ? Int64(0) :
            next_deadline(state.deadline_ns, options.pacing_period_ns, optical_finished)
        timing === nothing || record_phase_timing!(
            phase, timing, frame_timing, deadline, optical_finished, state.deadline_ns
        )
        if options.total_exchanges !== nothing && sequence == options.total_exchanges
            state.running = false
            state.completed = true
            state.deadline_ns = 0
            save_phase_timing!(phase)
            report!(options, mailbox, state)
        end
        return 1
    end
end

function run_source!(
    ::Val{3}, options, config, prepared;
    clock::Clocks.AbstractNanoClock = Clocks.SystemNanoClock(),
)
    runtime = options.bootstrap_runtime
    free_running = get(options, :acquisition, "lockstep") == "free-running"
    prepared === nothing && PipeWireAORTC.InstrumentDefinition.verify_declaration(options)
    options.owner_check()
    println("SIMULATOR_PREPARING backend=$(options.backend) graph=$(options.graph)")
    flush(stdout)
    state = Cursor()
    source_instance = Int64(rand(UInt64) % UInt64(typemax(Int64))) + Int64(1)
    observation = nothing
    pupil = nothing
    phase = nothing
    pipewire = nothing
    failures = Exception[]
    try
        idle_strategy = options.idle_strategy
        options = merge(
            options,
            (;
                graph_sha256 = options.graph === nothing ? nothing :
                               PipeWireAORTC.Common.sha256_file(options.graph),
                configuration_sha256 = PipeWireAORTC.Common.sha256_file(
                    options.config_path
                ),
            ),
        )
        phase = free_running ? nothing : prepare_phase_timing(options)
        science = prepare_lockstep(config, prepared, options.owner_check)
        config = merge(config, (; options.configuration_sha256))
        pupil = PupilOutput.prepare(config, science)
        options = merge(options, (; pupil_output_status = () -> PupilOutput.status(pupil)))
        observation = prepare_observation(config, science)
        options.owner_check()
        mailbox = SourceControl.Mailbox(source_instance)
        report!(options, mailbox, state)
        mirror_properties = PipeWireAORTC.InstrumentDefinition.mirror_properties(options)
        command_properties = command_mirror_properties(
            options, options.command_node, mirror_properties
        )
        wire = wire_configuration(config, options.remote, options.control_node)
        pipewire = prepare_lockstep_transport(
            config, science, wire, mailbox, observation, mirror_properties; free_running
        )
        with_thread_loop_lock(pipewire.loop) do _
            publish_mirror_properties!(pipewire.command_stream, command_properties)
        end
        with_applied_command_stream(pipewire) do streams
            publish_mirror_properties!(streams, command_properties)
        end
        pipewire_cpu = get(options, :pipewire_cpu, nothing)
        if pipewire_cpu !== nothing
            PipeWireAORTC.Placement.pin_loop!(
                pipewire.loop, pipewire_cpu; check = options.owner_check
            )
        end
        OpticalObservation.warm!(
            observation,
            pipewire;
            check = options.owner_check,
            cpu = get(options, :optical_diagnostics_cpu, nothing),
            clock,
        )
        PupilOutput.start!(
            pupil;
            remote = options.remote,
            cpu = get(options, :pupil_output_cpu, nothing),
            check = options.owner_check,
            clock,
        )
        precompile(OpticalObservation.close!, (typeof(observation), typeof(pipewire))) ||
            error("cannot compile prepared optical cleanup before admission")
        if free_running
            precompile(step_free_running!, (typeof(pipewire), typeof(science.driver))) ||
                error("cannot compile prepared exposure step before admission")
        else
            timing = exchange_timing(phase)
            precompile(
                Core.kwcall,
                (
                    typeof((; timing, idle_strategy)),
                    typeof(exchange_frame!),
                    typeof(pipewire),
                    typeof(science.driver),
                ),
            ) || error("cannot compile prepared frame exchange before admission")
        end
        options.owner_check()
        Bootstrap.prepared!(runtime)
        println(
            "SIMULATOR_PREPARED command_count=$(options.command_count) frame_shape=$(options.frame_shape)",
        )
        flush(stdout)
        run_prepared!(
            options, science, pipewire, observation, mailbox, state, phase, pupil; clock, idle_strategy
        )
    catch error
        if !(owner_cancelled(error, runtime))
            push!(failures, error)
            Bootstrap.fault!(runtime, error)
        end
    finally
        Bootstrap.finishing!(runtime)
        generation = state.generation
        if pipewire !== nothing
            try
                observed = Int64(frame_acquisition_generation(pipewire))
                if free_running
                    options = merge(options, (; free_running_status=free_running_status(pipewire)))
                end
                if observed != state.generation
                    generation = nothing
                    push!(
                        failures,
                        ErrorException(
                            "source generation changed from $(state.generation) to $observed before its owner cursor committed",
                        ),
                    )
                end
            catch error
                generation = nothing
                push!(failures, error)
            end
        end
        try
            PupilOutput.close!(pupil)
        catch error
            push!(failures, error)
        end
        try
            OpticalObservation.close!(observation, pipewire)
        catch error
            push!(failures, error)
        end
        try
            save_phase_timing!(phase)
        catch error
            push!(failures, error)
        end
        state.running = false
        state.deadline_ns = 0
        try
            write_report(options, state, generation, source_instance; failures)
        catch error
            push!(failures, error)
        end
    end
    isempty(failures) ||
        throw(length(failures) == 1 ? only(failures) : CompositeException(failures))
    return nothing
end

owner_cancelled(::InterruptException, runtime) = Bootstrap.clean_cancel(runtime)
owner_cancelled(error, runtime) = false


function validate_invocation(::Val{3}, options)
    options.physical_offsets === nothing ||
        throw(ArgumentError("lockstep HIL does not accept --physical-offsets"))
    all(name -> name ∉ (options.control_node, options.bootstrap_node), Configuration.command_items(options.command_node)) ||
        throw(ArgumentError("source, bootstrap and command nodes must be distinct"))
    return nothing
end

struct FrameAccess{T}
    transport::T
end

(access::FrameAccess)(f) = with_frame_stream(f, access.transport)

function prepare_observation(config, science)
    config.optical_observation || return nothing
    imaging = OpticalObservation.contracts().imaging_configuration(science.declaration)
    return OpticalObservation.prepare(science.definition, science.graph; imaging)
end

function prepare_lockstep_transport(config, science, wire, mailbox, observation, mirror_properties;
        free_running=false)
    reference_clock = Common.prepare_reference_clock(config)
    boundary = free_running ? prepare_graph_calibration_boundary(
        science.graph; command_input=config.command_input, frame_output=config.frame_output,
        command_buffer=hil_command_buffer(science.boundary), frame_buffer=hil_frame_buffer(science.boundary),
    ) : science.boundary
    return prepare_pipewire_hil(
        boundary,
        wire;
        reference_clock = reference_clock,
        frame_buffers = config.detector_observation ? 3 : 2,
        command_buffers = config.command_observation ? 3 : 2,
        applied_command_node_name = applied_command_names(
            config.command_node, config.applied_command_observation
        ),
        applied_command_buffers = 3,
        applied_command_properties = merge(
            Dict(
                "node.group" => "pipewireao-applied-observation",
                "node.sync-group" => "sync.pipewireao-applied-observation",
                "node.loop.name" => "telemetry-loop",
            ),
            mirror_properties,
        ),
        image_sources = OpticalObservation.source_options(
            observation; reference_clock = reference_clock
        ),
        frame_properties = merge(
            SourceControl.source_properties(mailbox),
            simulation_properties(config, science),
        ),
        frame_params = mailbox.parameters.params,
        on_frame_param_changed = SourceControl.ParameterChanged(mailbox),
        frame_param_buffer = PodBuffer(4096),
        on_frame_param_overflow = SourceControl.ParameterOverflow(mailbox),
    )
end

function lockstep_receipt(config)
    isdefined(InstrumentConfiguration, :read_simulator_preparation) || return nothing
    return InstrumentConfiguration.read_simulator_preparation(config)
end

function Simulation.prepare_lockstep_startup(path::AbstractString)
    config = Configuration.configuration(path)
    config.version == 3 && config.backend === :cpu ||
        throw(ArgumentError("lockstep precompilation requires a managed CPU configuration"))
    prepared = lockstep_receipt(config)
    prepared === nothing && throw(ArgumentError("lockstep precompilation requires a sealed receipt"))
    load_proper(config, prepared)
    Base.invokelatest(precompile_lockstep_startup, config, prepared)
    return nothing
end

# The install workload owns only local model state and snapshots. Compile
# native transport methods from these types without opening a PipeWire core.
function precompile_lockstep_startup(config, prepared)
    science = prepare_lockstep(config, prepared, Returns(nothing))
    config = merge(config, (; configuration_sha256 = Common.sha256_file(config.config_path)))
    pupil = PupilOutput.prepare(config, science)
    observation = nothing
    try
        observation = prepare_observation(config, science)
        # The synthetic frame node must differ from every declared command node.
        name_length = maximum(ncodeunits, Configuration.command_items(config.command_node)) + 1
        wire = wire_configuration(config, "offline", repeat("_", name_length))
        mailbox = SourceControl.Mailbox(Int64(1))
        mirror_properties = PipeWireAORTC.InstrumentDefinition.mirror_properties(config)
        precompile(
            prepare_lockstep_transport,
            (typeof(config), typeof(science), typeof(wire), typeof(mailbox),
             typeof(observation), typeof(mirror_properties)),
        )
        if pupil !== nothing
            for cpu in (Nothing, Int)
                precompile(
                    Core.kwcall,
                    (NamedTuple{(:clock,),Tuple{Clocks.SystemNanoClock}},
                     typeof(PupilOutput.worker!), typeof(pupil), String, cpu),
                )
            end
        end
    finally
        try
            PupilOutput.close!(pupil)
        finally
            OpticalObservation.close!(observation)
        end
    end
    return nothing
end

Base.@nospecializeinfer function prepare_lockstep(options, prepared, @nospecialize(check))
    execution = options.backend === :cpu ? StreamGraphExecution() : CapturedGraphExecution()
    device = target(options)
    system = if prepared === nothing
        InstrumentConfiguration.prepare_system(
            options.plant.definition;
            settings = options.plant.settings,
            simulation_graph = options.plant.graph,
            target = device,
            execution,
        )
    else
        InstrumentConfiguration.prepare_system(prepared; target = device, execution)
    end
    check()
    AdaptiveOpticsSimPipeWireHIL.validate_hil_system(
        system;
        command_input = options.command_input,
        frame_output = options.frame_output,
        command_shape = Configuration.command_shapes(options.command_count),
        frame_shape = options.frame_shape,
    )
    declaration = system.declaration
    authoring = Base.get_extension(PipeWireAORTC, :PipeWireAORTCAOSExt)
    AdaptiveOpticsSimPipeWireHIL.validate_wire_contract(
        options.frame_encoding,
        options.frame_schema,
        options.command_schema,
        options.command_scale,
        authoring.configured_wire_contracts(declaration),
    )
    round(UInt64, Float64(system.frame_period_s) * 1e9) == options.period_ns ||
        throw(ArgumentError("instrument cadence differs from the application"))
    for node in system.definition.nodes
        validate_node_timing(node.config, options)
    end
    boundary = system.boundary
    driver = FixedStepModelTimeDriver(PeriodicSchedule(; period_ns = options.period_ns))
    warm = step_hil_frame_at!(boundary, driver)
    check()
    values = hil_frame_buffer(boundary)
    all(isfinite, values) || throw(ArgumentError("prepared detector frame is not finite"))
    if options.frame_encoding === :uint16
        all(value -> 0 <= value <= typemax(UInt16), values) ||
            throw(ArgumentError("prepared detector frame cannot encode as UInt16"))
    end
    validate_command_buffers(hil_command_buffer(boundary), options.command_count)
    zero_command_buffers!(hil_command_buffer(boundary))
    adopt_hil_command!(boundary, warm.sequence)
    reset_hil_boundary!(boundary, driver)
    check()
    return (; system.definition, system.graph, system.declaration, system.configured, boundary, driver)
end
