module PupilSource

using ..PipeWireAORTCExt:
    owner_cancelled, Configuration, Bootstrap, SourceControl, target,
    Cursor, PupilOutput, source_filter, registered, wait_registered!, pin_transport!,
    consume!, source_mailbox, write_report, next_deadline
using AdaptiveOpticsSim, AdaptiveOpticsSimPipeWireHIL, PipeWireAO
using AdaptiveOpticsSimPipeWireHIL: SimRunner
using AdaptiveOpticsSim.AlgorithmGraphs
using AdaptiveOpticsSim.AlgorithmGraphs:
    poll_graph_output_snapshot!,
    snapshot_values,
    release_graph_output_snapshot!,
    reset_graph_output_snapshot!
import PipeWireAORTC
import Clocks
const IC = PipeWireAORTC.InstrumentConfiguration
const Offsets = PipeWireAORTC.PhysicalOffsets
const Extension = parentmodule(@__MODULE__)
const WARMUP_TIMEOUT_NS = Int64(30_000_000_000)

function prepare(
    options;
    check = () -> nothing,
    clock::Clocks.AbstractNanoClock = Clocks.SystemNanoClock(),
)
    check()
    device = target(options)
    execution = options.backend === :cpu ? StreamGraphExecution() : CapturedGraphExecution()
    system = IC.prepare_system(
        options.plant.definition;
        settings = options.plant.settings,
        simulation_graph = options.plant.graph,
        target = device,
        execution,
    )
    system.boundary === nothing ||
        throw(ArgumentError("pupil source cannot own an HIL camera boundary"))
    command = graph_input(system.graph, options.command_input)
    eltype(command) === Float32 && size(command) == (options.command_count,) || throw(
        DimensionMismatch("pupil source command differs from physical Float32 vector")
    )
    all(iszero, Array(command)) ||
        throw(ArgumentError("pupil source physical command must start at zero"))
    driver = FixedStepModelTimeDriver(PeriodicSchedule(; period_ns = options.period_ns))
    science = merge(system, (; driver, command))
    publisher = PupilOutput.prepare(options, science)
    try
        warmup!(science, publisher; check, clock)
    catch
        PupilOutput.close!(publisher)
        rethrow()
    end
    return merge(science, (; publisher))
end

"Warm the actual graph and packet packing, then restore the first model phase."
function warmup!(
    science,
    publisher;
    check = () -> nothing,
    clock::Clocks.AbstractNanoClock = Clocks.SystemNanoClock(),
)
    try
        check()
        step_graph_at!(science.graph, science.driver)
        check()
        PupilOutput.sample!(publisher; generation = UInt64(1), model_ns = Int64(0)) ||
            error("pupil source warmup snapshot unavailable")
        previous = Clocks.time_nanos(clock)::Int64
        deadline = Base.checked_add(previous, WARMUP_TIMEOUT_NS)
        while true
            now_ns = Clocks.time_nanos(clock)::Int64
            # Readiness is an owned snapshot proof and precedes temporal refusal.
            poll_graph_output_snapshot!(publisher.snapshot) && break
            check()
            now_ns >= previous || error("pupil source warmup monotonic clock regressed")
            previous = now_ns
            now_ns < deadline || error("pupil source warmup snapshot timed out")
            yield()
        end
        try
            PupilOutput.pack!(
                publisher.packet, snapshot_values(publisher.snapshot), publisher.amplitude
            )
        finally
            release_graph_output_snapshot!(publisher.snapshot)
        end
    finally
        reset_graph_output_snapshot!(publisher.snapshot)
        reset_science!(science)
    end
    return nothing
end

function reset_science!(science)
    fill!(science.command, 0)
    reset_graph!(science.graph)
    reset_model_time!(science.driver)
    return nothing
end

function step!(science, publisher, receiver, command; generation::UInt64)
    publisher.failed[] && error("required scientific pupil output failed")
    copyto!(science.command, command)
    timestamp = step_graph_at!(science.graph, science.driver)
    sequence = model_time_sequence(science.driver)
    pts = model_nanoseconds(timestamp)
    Offsets.observe_clock!(
        receiver,
        AcquisitionIdentity(publisher.domain, generation, sequence),
        pts,
        publisher.period_ns,
    ) || error("pupil source clock regressed")
    # A busy snapshot drops this publication, never the model step. The next
    # snapshot carries its actual graph sequence and model timestamp.
    PupilOutput.sample!(publisher; generation, model_ns = pts)
    return sequence
end

function applied_command_option(options)
    options.applied_command_observation || return nothing
    properties = merge(
        PipeWireAORTC.InstrumentDefinition.mirror_properties(
            options, Symbol(options.mirror)
        ),
        Dict(
            "node.loop.name" => "telemetry-loop",
            "node.group" => "pipewireao-applied-observation",
            "node.sync-group" => "sync.pipewireao-applied-observation",
            "pipewireao.header.pts.clock" => "model",
            "aos.simulation.command-order" => options.command_order,
            "aos.simulation.calibration-identity" => options.calibration_identity,
        ),
    )
    return PipeWireArrayStreamOptions(;
        node_name = "simulator-" * options.mirror * "-applied",
        media_name = "dm-applied",
        buffers = 3,
        reference_clock = PipeWireAORTC.Common.prepare_reference_clock(options),
        metadata = PipeWireAORTC.Common.ACQUISITION_METADATA,
        schema = PipeWireAORTC.Schemas.identifier("applied_dm_command_opd_um"),
        format = NdArrayFormat(
            NdArray.F32_LE,
            (options.command_count,);
            layout = NdArray.ROW_MAJOR,
            rate = SPA.Fraction(UInt32(options.rate), UInt32(1)),
        ),
        properties,
    )
end

function observe_command!(inputs, values, command, publisher, state)
    index = inputs.applied_command
    index == 0 && return nothing
    # The completed model step adopted exactly this source-owned command.
    # Convert metres to the established applied-command micrometre schema.
    @. values = command / 1.0f-6
    identity = AcquisitionIdentity(
        publisher.domain, UInt64(state.generation), state.sequence
    )
    pts = Base.checked_sub(
        inputs.prepared.receiver.clock_end_ns, Int64(publisher.period_ns)
    )
    if stage_pipewire_array!(
        inputs.transport, index, values; identity, pts, duration = publisher.period_ns
    )
        with_array_stream(inputs.transport, index) do stream
            stream_state(stream) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING &&
                is_driving(stream) &&
                trigger_process!(stream)
        end
    end
    return nothing
end

function run!(
    options,
    science,
    prepared,
    mailbox,
    state;
    clock::Clocks.AbstractNanoClock = Clocks.SystemNanoClock(),
)
    runtime = options.bootstrap_runtime
    idle_strategy = options.idle_strategy
    publisher = science.publisher
    Bootstrap.prepared!(runtime)
    ticket = Bootstrap.take_connect!(runtime)
    Bootstrap.check_connect!(runtime, ticket)
    applied_command = applied_command_option(options)
    applied_values =
        applied_command === nothing ? Float32[] : zeros(Float32, options.command_count)
    Offsets.with_receiver(
        prepared; remote = options.remote, applied_command, clock
    ) do inputs
        pin_transport!(inputs.transport, options)
        properties = merge(
            Extension.simulation_properties(options, science),
            Dict(
                "aos.simulation.mode" => "pupil-source",
                "aos.simulation.prescription-identity" => options.prescription_identity,
                "aos.simulation.command-order" => options.command_order,
            ),
        )
        access = source_filter(inputs.transport, options.control_node, mailbox, properties)
        try
            PupilOutput.start!(
                publisher;
                remote = options.remote,
                cpu = get(options, :pupil_output_cpu, nothing),
                check = () -> Bootstrap.check_connect!(runtime, ticket),
                clock,
            )
            wait_registered!(runtime, ticket) do
                access(registered) && all(eachindex(inputs.transport.streams)) do index
                    with_array_stream(registered, inputs.transport, index)
                end
            end
            Bootstrap.connected!(runtime, ticket)
            Bootstrap.await_connected!(runtime, ticket)
            reset_owner! =
                () -> begin
                    PupilOutput.drain!(publisher; check = options.owner_check, clock)
                    Offsets.reset_inputs!(inputs)
                    reset_science!(science)
                    state.generation = Base.checked_add(state.generation, Int64(1))
                    inputs.applied_command == 0 || reset_pipewire_array!(
                        inputs.transport,
                        inputs.applied_command;
                        identity = AcquisitionIdentity(
                            publisher.domain, UInt64(state.generation), UInt64(0)
                        ),
                    )
                    fill!(applied_values, 0)
                    state.sequence = 0
                    state.completed = false
                    state.deadline_ns = 0
                    PupilOutput.reset!(
                        publisher,
                        UInt64(state.generation);
                        check = options.owner_check,
                        clock,
                    )
                end
            SimRunner.run!(idle_strategy) do
                Bootstrap.cancelled(runtime) && return nothing
                now_ns = SourceControl.observe_clock!(state, Clocks.time_nanos(clock))
                consume!(
                    access, options, mailbox, state,
                    max(options.pacing_period_ns, Int64(1)), reset_owner!, now_ns,
                )
                Bootstrap.cancelled(runtime) && return nothing
                if !state.running
                    sleep(0.001)
                    return 0
                end
                options.pacing_period_ns != 0 && now_ns < state.deadline_ns && return 0
                command = Offsets.poll!(inputs; now_ns)
                state.sequence = step!(
                    science,
                    publisher,
                    prepared.inputs.receiver,
                    command;
                    generation = UInt64(state.generation),
                )
                observe_command!(inputs, applied_values, command, publisher, state)
                # Separate exchange completion retains the existing missed-period policy.
                finished_ns = SourceControl.observe_clock!(state, Clocks.time_nanos(clock))
                state.deadline_ns =
                    options.pacing_period_ns == 0 ? Int64(0) :
                    next_deadline(
                        state.deadline_ns, options.pacing_period_ns, finished_ns
                    )
                return 1
            end
        finally
            Bootstrap.finishing!(runtime)
            PupilOutput.drain!(publisher; check = () -> nothing, clock)
            access(close)
        end
    end
    return nothing
end

function run_source!(
    options; clock::Clocks.AbstractNanoClock = Clocks.SystemNanoClock()
)
    options = merge(
        options,
        (; configuration_sha256 = PipeWireAORTC.Common.sha256_file(options.config_path)),
    )
    Configuration.verify_pupil_source_declaration(options)
    state = Cursor()
    mailbox = nothing
    failures = Exception[]
    science = nothing
    try
        prepared = Offsets.prepare(
            options.physical_offsets;
            options.command_count,
            options.command_order,
            options.calibration_identity,
        )
        names = [
            row["node_name"] for row in values(prepared.inputs.declaration["branches"])
        ]
        reserved = (
            options.control_node,
            options.bootstrap_node,
            PupilOutput.NODE_NAME,
            (
                options.applied_command_observation ?
                ("simulator-" * options.mirror * "-applied",) : ()
            )...,
        )
        all(name -> name ∉ reserved, names) ||
            throw(ArgumentError("pupil source native node names collide"))
        @info "Preparing pupil source" owner=options.control_node backend=options.backend
        preparation = @timed prepare(options; check = options.owner_check, clock)
        science = preparation.value
        @info "Pupil source prepared" owner=options.control_node wall_seconds=preparation.time compile_seconds=preparation.compile_time
        options = merge(
            options, (; pupil_output_status = () -> PupilOutput.status(science.publisher))
        )
        mailbox = source_mailbox(options, state)
        run!(options, science, prepared, mailbox, state; clock)
    catch error
        if !(owner_cancelled(error, options.bootstrap_runtime))
            push!(failures, error)
            Bootstrap.fault!(options.bootstrap_runtime, error)
        end
    finally
        Bootstrap.finishing!(options.bootstrap_runtime)
        if science !== nothing
            try
                fill!(science.command, 0)
                PupilOutput.close!(science.publisher)
            catch error
                push!(failures, error)
            end
        end
        state.running = false
        try
            mailbox === nothing || write_report(options, state, state.generation, mailbox.instance; failures)
        catch error
            push!(failures, error)
        end
    end
    isempty(failures) ||
        throw(length(failures) == 1 ? only(failures) : CompositeException(failures))
    return nothing
end
end
