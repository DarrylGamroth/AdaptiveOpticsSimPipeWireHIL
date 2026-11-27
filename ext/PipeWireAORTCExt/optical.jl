function prepare_clocked_source(options, science; check = () -> nothing)
    source = InstrumentConfiguration.prepare_system(
        options.plant.definition;
        settings = options.plant.settings,
        simulation_graph = options.source_graph,
        target = Backends.HostComputeDevice(),
    )
    declaration = InstrumentConfiguration.load_simulation(options.plant.definition)
    configured = InstrumentConfiguration.settings(declaration, options.plant.settings)
    size(graph_input(source.graph, options.command_input)) == (options.command_count,) ||
        throw(DimensionMismatch("clocked source command differs from declared count"))
    amplitude = InstrumentConfiguration.binding_values(
        declaration,
        configured,
        declaration.document["bindings"][options.source_amplitude_binding],
    )
    return ExternalPupil.prepare_clocked_source(
        science;
        source,
        command_input = options.command_input,
        amplitude,
        pupil_axes = options.source_pupil_axes,
        period_ns = options.period_ns,
        check,
    )
end

function clocked_command_option(options)
    return PipeWireArrayStreamOptions(;
        node_name = options.command_node,
        media_name = options.mirror * " native command",
        buffers = options.command_buffers,
        reference_clock = Common.prepare_reference_clock(options),
        metadata = (
            MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP; required = true),
            MetadataRequirement(SPA.META_ACQUISITION_IDENTITY),
            MetadataRequirement(SPA.META_EXPOSURE_TIMING),
            MetadataRequirement(SPA.META_OPTICAL_CHOPPING),
            MetadataRequirement(SPA.META_ELECTRICAL_TRIGGER),
        ),
        schema = options.command_schema,
        format = NdArrayFormat(
            NdArray.F32_LE, (options.command_count,); layout = NdArray.ROW_MAJOR
        ),
        properties = Dict(
            "optics.command-units" => options.command_units,
            "optics.calibration-identity" => options.calibration_identity,
        ),
    )
end

function with_clocked_applied_command(f, transport, options)
    get(options, :applied_command_observation, false) || return f(nothing)
    HIL = AdaptiveOpticsSimPipeWireHIL
    name = "simulator-" * options.mirror * "-applied"
    name ∉ (options.command_node, options.control_node, options.bootstrap_node,
        values(options.sensor_nodes)...) || throw(ArgumentError("applied command node name collides"))
    contract = (;
        schema = PipeWireAORTC.Schemas.identifier("applied_pdm_command"),
        media_name = "dm-applied",
        units = Symbol(options.command_units),
        command_scale = 1.0f0,
    )
    format = NdArrayFormat(
        NdArray.F32_LE, (options.command_count,);
        layout = NdArray.ROW_MAJOR, rate = SPA.Fraction(options.rate, 1),
    )
    reference_clock = Common.prepare_reference_clock(options)
    properties = Dict(
        "optics.command-units" => options.command_units,
        "optics.calibration-identity" => options.calibration_identity,
        "node.loop.name" => "telemetry-loop",
        "node.group" => "pipewireao-applied-observation",
        "node.sync-group" => "sync.pipewireao-applied-observation",
    )
    source = with_thread_loop_lock(transport.loop) do _
        HIL._prepare_applied_command_stream(transport.core, name, format, 3, properties, contract;
            driver = true)
    end
    source.state.reference_clock = reference_clock
    try
        return f(source)
    finally
        with_thread_loop_lock(transport.loop) do _
            close(source.stream)
        end
    end
end

function observe_applied!(source, transport, command, sequence, pts)
    HIL = AdaptiveOpticsSimPipeWireHIL
    HIL._stage_applied_command!(source.state, command, source.command_scale, sequence, pts) ||
        return nothing
    try
        with_thread_loop_lock(transport.loop) do _
            stream = source.stream
            stream_state(stream) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING &&
                is_driving(stream) && trigger_process!(stream)
        end
    catch
        source.state.failed[] = true
    end
    return nothing
end

const OpticalMode = Union{Val{4},Val{6}}

function validate_invocation(::OpticalMode, options)
    if options.physical_offsets !== nothing || options.phase_timing_directory !== nothing ||
        options.optical_diagnostics_cpu !== nothing || options.pupil_output_cpu !== nothing
        throw(ArgumentError("managed optical mode uses its declared source and exposure ports"))
    end
    input_node = options.version == 6 ? options.command_node : options.pupil_node
    for node in (input_node, values(options.sensor_nodes)...)
        node ∉ (options.control_node, options.bootstrap_node) ||
            throw(ArgumentError("source, bootstrap and data nodes must be distinct"))
    end
    return nothing
end

# The RTC extension resolves shared authored assets while cold. HIL owns the
# resulting graph, execution state and PipeWire data ports on this task.
function prepare_optics(options, ::Val{4})
    return PipeWireAORTC.InstrumentExport.prepare_external_pupil_ports(
        options.plant.definition;
        settings = options.plant.settings,
        chopping = options.plant.chopping,
        target = target(options),
        check = options.owner_check,
    )
end

function prepare_optics(options, ::Val{6})
    science = prepare_optics(options, Val(4))
    return prepare_clocked_source(options, science; check = options.owner_check)
end

function synchronize!(state::Cursor, execution::ExternalPupil.Execution)
    cursor = ExternalPupil.snapshot(execution)
    state.generation = cursor.generation
    state.sequence = cursor.sequence
    state.deadline_ns = cursor.deadline_ns
    return nothing
end

struct OpticalControl{O,M,A,S}
    options::O
    mailbox::M
    state::Cursor
    access::A
    applied::S
end

function (control::OpticalControl)(execution, streams, now_ns::Int64)
    (; options, mailbox, state, access) = control
    synchronize!(state, execution)
    SourceControl.observe_clock!(state, now_ns)
    Bootstrap.cancelled(options.bootstrap_runtime) && return ExternalPupil.Stop()
    reset = () -> begin
        ExternalPupil.reset!(execution, streams)
        if control.applied !== nothing
            with_thread_loop_lock(streams.loop) do _
                AdaptiveOpticsSimPipeWireHIL._reset_applied_command!(control.applied)
            end
        end
        synchronize!(state, execution)
    end
    consume!(access, options, mailbox, state, max(execution.pacing_period_ns, Int64(1)), reset, now_ns)
    Bootstrap.cancelled(options.bootstrap_runtime) && return ExternalPupil.Stop()
    return state.running ? ExternalPupil.Run(state.deadline_ns) : ExternalPupil.Hold()
end

struct DiscardOpticalStep <: Exception end

# The current duty owns the graph and has not begun exposure publication.
# Status uses the last committed cursor. Hold parks the task at this boundary.
function propagation_checkpoint!(control::OpticalControl, period::Int64)
    (; options, mailbox, state, access) = control
    while true
        Bootstrap.cancelled(options.bootstrap_runtime) && throw(DiscardOpticalStep())
        if mailbox.ready[]
            kind, token, _ = access(_ -> SourceControl.pending(mailbox))
            # A held Reset needs the graph to unwind before touching its storage.
            if kind == SourceControl.RESET && !state.running && token > state.last_request_id
                throw(DiscardOpticalStep())
            end
            now = Clocks.time_nanos(Clocks.SystemNanoClock())
            consume!(
                access, options, mailbox, state, period,
                () -> error("reset requires an optical duty boundary"), now,
            )
        end
        state.running && return nothing
        sleep(0.001)
    end
end

function run_optical!(control::OpticalControl, execution, transport, observe_applied, idle_strategy)
    period = max(execution.pacing_period_ns, Int64(1))
    checkpoint = () -> propagation_checkpoint!(control, period)
    while !Bootstrap.cancelled(control.options.bootstrap_runtime)
        try
            with_propagation_checkpoint(checkpoint) do
                ExternalPupil.run!(control, execution, transport; observe_applied, idle_strategy)
            end
            return nothing
        catch error
            error isa DiscardOpticalStep || rethrow()
            # Cancellation exits. A pending Reset restarts the loop at its
            # normal control boundary, with a fresh exposure publisher.
        end
    end
    return nothing
end

function optical_properties(options, science, clocked)
    return merge(
        ExternalPupil.execution_properties(
            science.plant, first(keys(science.plant.graphs)); prefix = "aos.simulation."
        ),
        Dict(
            "aos.simulation.mode" => clocked ? "clocked-optical" : "external-pupil",
            "aos.simulation.backend" => string(options.backend),
            "aos.simulation.device" => string(options.device),
            "aos.simulation.calibration-identity" => options.calibration_identity,
            "aos.simulation.configuration-sha256" => options.configuration_sha256,
            "aos.simulation.prescription-identity" => science.identity,
        ),
    )
end

# Keep the preparation signatures shared by offline compilation and managed
# startup. The managed callback still checks the retained bootstrap owner.
struct OpticalPreparationCheck
    check::Function
end

(check::OpticalPreparationCheck)() = check.check()

function prepare_optical_startup(options, mode::OpticalMode, owner_check; warmup = true)
    check = OpticalPreparationCheck(owner_check)
    options = merge(options, (; owner_check = check))
    Configuration.verify_external_pupil_declaration(options)
    @info "Preparing optical model" owner=options.control_node backend=options.backend
    model = @timed prepare_optics(options, mode)
    @info "Optical model prepared" owner=options.control_node wall_seconds=model.time compile_seconds=model.compile_time
    clocked = mode === Val(6)
    execution = ExternalPupil.prepare(
        model.value;
        clocked,
        command_count = clocked ? options.command_count : 0,
        command_limit = clocked ? options.command_limit : Inf,
        pacing_period_ns = clocked ? options.pacing_period_ns : 0,
    )
    if warmup
        @info "Warming up optical execution" owner=options.control_node
        timing = @timed ExternalPupil.warmup!(execution; check)
        @info "Optical execution warmed up" owner=options.control_node wall_seconds=timing.time compile_seconds=timing.compile_time
    else
        check()
    end
    return execution
end

function Simulation.prepare_optical_startup(config::AbstractString)
    options = Configuration.configuration(config)
    options.backend === :cpu || throw(ArgumentError("optical precompilation requires CPU"))
    mode = Val(options.version)
    mode isa OpticalMode || throw(ArgumentError("expected a managed optical configuration"))
    Configuration.requires_proper(options) && Configuration.load_instrument(:Proper)
    options = merge(options, (; control_node = "optical precompile"))
    return Base.invokelatest(prepare_optical_startup, options, mode, Returns(nothing))
end

function run_source!(mode::OpticalMode, options, config)
    idle_strategy = options.idle_strategy
    # Pass the same configuration fields and owner label as the offline entry point.
    cold_options = merge(config, (; control_node = options.control_node))
    # Unpaced CPU playback can initialize on its first real exposure. The
    # install-time workload still covers both optical phases for precompilation.
    warmup = !(mode === Val(6) && options.backend === :cpu && iszero(options.pacing_period_ns))
    execution = prepare_optical_startup(cold_options, mode, options.owner_check; warmup)
    science = execution.science
    options = merge(options, (; configuration_sha256 = Common.sha256_file(options.config_path)))
    runtime = options.bootstrap_runtime
    clocked = mode === Val(6)
    streams = ExternalPupil.stream_options(
        science, options.pupil_node, options.sensor_nodes;
        sensor_buffers = options.sensor_buffers,
        input_option = clocked ? clocked_command_option(options) : nothing,
        reference_clock = Common.prepare_reference_clock(options),
    )
    state = Cursor()
    mailbox = source_mailbox(options, state)
    input = (true, map(_ -> false, keys(science.sensors))...)
    with_array_streams(streams; remote = options.remote, input, active = false,
            synchronous_command = clocked) do transport
        with_clocked_applied_command(transport, options) do applied
            pin_transport!(transport, options)
            properties = optical_properties(options, science, clocked)
            access = source_filter(transport, options.control_node, mailbox, properties)
            try
                # Live emitters specialize on the actual stream and graph types.
                # Complete compilation while held before accepting the Connect ticket.
                @info "Preparing optical transport" owner=options.control_node
                control = OpticalControl(options, mailbox, state, access, applied)
                observe = applied === nothing ? nothing :
                    (command, sequence, pts) -> observe_applied!(applied, transport, command, sequence, pts)
                preparation = @timed begin
                    ExternalPupil.prepare_transport!(execution, transport)
                    precompile(
                        Core.kwcall,
                        (typeof((; observe_applied = observe, idle_strategy)), typeof(ExternalPupil.run!),
                         typeof(control), typeof(execution), typeof(transport)),
                    ) || error("cannot compile managed optical execution")
                    precompile(propagation_checkpoint!, (typeof(control), Int64)) ||
                        error("cannot compile managed optical control checkpoint")
                    precompile(
                        run_optical!,
                        (typeof(control), typeof(execution), typeof(transport), typeof(observe), typeof(idle_strategy)),
                    ) || error("cannot compile managed optical control loop")
                end
                @info "Optical transport prepared" owner=options.control_node wall_seconds=preparation.time compile_seconds=preparation.compile_time
                options.owner_check()
                Bootstrap.prepared!(runtime)
                ticket = Bootstrap.take_connect!(runtime)
                Bootstrap.check_connect!(runtime, ticket)
                for index in eachindex(streams)
                    with_array_stream(transport, index) do stream
                        set_active!(stream, true)
                    end
                end
                if applied !== nothing
                    with_thread_loop_lock(transport.loop) do _
                        set_active!(applied.stream, true)
                    end
                end
                wait_registered!(runtime, ticket) do
                    applied_ready = applied === nothing || with_thread_loop_lock(transport.loop) do _
                        registered(applied.stream)
                    end
                    applied_ready && access(registered) && all(eachindex(streams)) do index
                        with_array_stream(registered, transport, index)
                    end
                end
                Bootstrap.connected!(runtime, ticket)
                Bootstrap.await_connected!(runtime, ticket)
                run_optical!(control, execution, transport, observe, idle_strategy)
            finally
                Bootstrap.finishing!(runtime)
                access(close)
            end
        end
    end
    synchronize!(state, execution)
    state.running = false
    write_report(options, state, state.generation, mailbox.instance)
    return nothing
end
