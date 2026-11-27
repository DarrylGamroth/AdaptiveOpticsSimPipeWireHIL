"""
Prepared external-pupil and clocked optical simulation over public PipeWire streams.

AOS owns numerical graphs and model time. This module owns single-writer simulation
execution and publication. Callers own stream lifetime, admission and controls;
no simulation work runs in a PipeWire processing callback.
"""
module ExternalPupil

using AdaptiveOpticsSim, AdaptiveOpticsSim.AlgorithmGraphs, PipeWireAO
using AdaptiveOpticsSim.AlgorithmGraphs: optical_execution_graph
using UUIDs: uuid4
import Clocks
using ..AdaptiveOpticsSimPipeWireHIL:
    SimRunner,
    PipeWireArrayStreamOptions,
    PipeWireArrayReceipt,
    stage_pipewire_array!,
    drain_pipewire_array!,
    _array_transport_failed,
    reset_pipewire_array!,
    _encode_frame_value,
    _wire_element_type

public Execution,
    prepare,
    prepare_clocked_source,
    stream_options,
    emitter,
    warmup!,
    prepare_transport!,
    snapshot,
    reset!,
    run!,
    Run,
    Hold,
    Stop,
    feed!,
    trigger_state,
    sensor_optical_classification,
    sensor_sequence,
    ExposurePublisher,
    stage_exposure!,
    publish_exposures!,
    execution_properties,
    pack_pupil!

const Backends = AdaptiveOpticsSim.Backends

const ACQUISITION_METADATA = (
    MetadataRequirement(SPA.META_ACQUISITION_IDENTITY; required = true),
    MetadataRequirement(SPA.META_EXPOSURE_TIMING; required = true),
    MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP; required = true),
)

"""
    execution_properties(plant, name; prefix="optics.", unfringed=false)

Describe the actual prepared AOS optical execution policy and capture count.
When AOS shares an execution, include the independent graph under `independent.`
and declare `execution-scope=prepared`. This reports preparation, not per-frame
selection or timing qualification.
"""
function execution_properties(plant, name; prefix = "optics.", unfringed = false)
    graph = optical_execution_graph(plant, name; unfringed)
    independent = unfringed ? getproperty(plant.observations, name).chopping.unfringed :
        getproperty(plant.graphs, name)
    properties = execution_properties(graph; prefix)
    if graph !== independent
        properties[prefix * "execution-scope"] = "prepared"
        merge!(properties, execution_properties(independent; prefix = prefix * "independent."))
    end
    return properties
end

function execution_properties(graph; prefix)
    return Dict(
        prefix * "execution" => string(nameof(typeof(graph_execution_policy(graph)))),
        prefix * "captured-node-count" => string(captured_graph_node_count(graph)),
    )
end

"Build native input and sensor options from prepared physical contracts and exposure rates."
function stream_options(
    science,
    pupil_node,
    sensor_nodes;
    sensor_buffers = 3,
    input_option = nothing,
    reference_clock = PipeWireAO.METADATA_CLOCK_REALTIME,
    frame_types = map(_ -> Float32, science.sensors),
)
    pupil =
        input_option === nothing ?
        PipeWireArrayStreamOptions(;
            node_name = pupil_node,
            media_name = "External pupil field",
            schema = science.pupil.schema,
            format = NdArrayFormat(
                NdArray.F32_LE, science.pupil.shape; layout = NdArray.ROW_MAJOR
            ),
            properties = science.pupil.properties,
            metadata = ACQUISITION_METADATA,
            reference_clock,
        ) : input_option
    keys(frame_types) == keys(science.sensors) ||
        throw(ArgumentError("frame types must match the declared sensor names and order"))
    sensors = map(keys(science.sensors)) do name
        sensor = getproperty(science.sensors, name)
        duration = getproperty(science.plant.exposures, name).duration_ns
        duration > 0 || throw(ArgumentError("sensor exposure duration must be positive"))
        rate = 1_000_000_000 // duration
        PipeWireArrayStreamOptions(;
            node_name = getproperty(sensor_nodes, name),
            media_name = string(name),
            buffers = sensor_buffers,
            reference_clock,
            metadata = (
                PipeWireAO.MetadataRequirement(
                    PipeWireAO.SPA.META_ACQUISITION_IDENTITY; required = true
                ),
                PipeWireAO.MetadataRequirement(
                    PipeWireAO.SPA.META_EXPOSURE_TIMING; required = true
                ),
                PipeWireAO.MetadataRequirement(
                    PipeWireAO.SPA.META_REFERENCE_TIMESTAMP; required = true
                ),
                (
                    hasproperty(science.chopping, name) ?
                    (
                        PipeWireAO.MetadataRequirement(
                            PipeWireAO.SPA.META_OPTICAL_CHOPPING; required = true
                        ),
                        PipeWireAO.MetadataRequirement(
                            PipeWireAO.SPA.META_ELECTRICAL_TRIGGER; required = true
                        ),
                    ) : ()
                )...,
            ),
            schema = sensor.schema,
            format = NdArrayFormat(
                _wire_element_type(getproperty(frame_types, name)),
                size(sensor.image);
                layout = NdArray.ROW_MAJOR,
                rate = SPA.Fraction(numerator(rate), denominator(rate)),
            ),
            properties = merge(
                sensor.properties, Dict("pipewireao.header.pts.clock" => "model")
            ),
        )
    end
    return (pupil, sensors...)
end

"Adapt the public transport receipt without changing physical axes or units."
function feed!(emit, plant, packet::Array{Float32,3}, receipt)
    size(packet) == (2, size(plant.pupil_opd)...) ||
        throw(DimensionMismatch("external pupil packet shape"))
    identity = receipt.identity
    return step_external_pupil!(
        emit,
        plant,
        view(packet, 1, :, :),
        view(packet, 2, :, :),
        ModelTimestamp(receipt.pts),
        ModelDuration(receipt.duration);
        domain = identity.domain.bytes,
        generation = identity.generation,
        sequence = identity.sequence,
        lost_samples = receipt.lost_samples,
    )
end

# Runtime startup exercises the same feed and emitter as acquisition. Publication
# remains disabled until all synthetic optical/response history has been reset.
# Detector graph reset restores its construction RNG seed (Host or counter RNG).
function warmup!(emit, science, packet, frames, buffers; check = () -> nothing)
    durations = map(exposure -> exposure.duration_ns, values(science.plant.exposures))
    step = reduce(gcd, durations)
    horizon = Base.checked_mul(maximum(durations), Int64(2))
    samples = div(horizon, step)
    0 < samples <= 4096 ||
        throw(ArgumentError("external-pupil warmup exceeds 4096 samples"))
    try
        fill!(packet, 0)
        view(packet, 2, :, :) .= 1.0f0
        # Local receipt only, matching live feed specialization. Its isolated
        # identity never reaches transport or changes the live owner identity.
        identity_domain = AcquisitionDomain(ntuple(index -> UInt8(index), 16))
        for sequence in 1:samples
            check()
            receipt = PipeWireArrayReceipt(
                Int64((sequence - 1) * step),
                UInt64(step),
                AcquisitionIdentity(identity_domain, UInt64(1), UInt64(sequence)),
                UInt64(0),
            )
            feed!(emit, science.plant, packet, receipt) ||
                error("external-pupil warmup sample rejected")
        end
    finally
        reset_external_pupil!(science.plant)
        fill!(packet, 0)
        foreach(frame -> fill!(frame, 0), frames)
        foreach(buffer -> fill!(buffer, 0), buffers)
    end
    return nothing
end

"Return the declared electrical trigger state for an emitted optical exposure."
function trigger_state(science, name, metadata)
    hasproperty(science.chopping, name) || return ACQUISITION_TRIGGER_ABSENT
    kind = get(metadata, :chopped_image_kind, nothing)
    kind === :mixed && return ACQUISITION_TRIGGER_MIXED
    kind in (:fringed, :unfringed) ||
        throw(ArgumentError("chopped exposure lacks its actual optical phase"))
    high =
        (kind === :fringed) == getproperty(science.chopping, name).trigger_high_is_fringed
    return high ? ACQUISITION_TRIGGER_HIGH : ACQUISITION_TRIGGER_LOW
end

# Optical exposure classification is authored by the optics, independently of
# the explicitly authored electrical trigger mapping.
"Return the actual optical phase independently of electrical trigger mapping."
function sensor_optical_classification(science, name, metadata)
    hasproperty(science.chopping, name) || return PipeWireAO.OPTICAL_ABSENT
    kind = get(metadata, :chopped_image_kind, nothing)
    kind === :fringed && return PipeWireAO.OPTICAL_FRINGED
    kind === :unfringed && return PipeWireAO.OPTICAL_UNFRINGED
    kind === :mixed && return PipeWireAO.OPTICAL_MIXED
    throw(ArgumentError("chopped exposure lacks its actual optical classification"))
end

"Return the source sequence, or the aligned model-window ordinal for chopped optics."
function sensor_sequence(metadata, chopped::Bool)
    chopped || return metadata.sequence
    # AOS complete exposures occupy aligned windows of its zero-origin model
    # clock. The wire identity records the observed window ordinal so missing
    # whole windows cannot appear adjacent. Optical phase is never inferred here.
    pts, duration = metadata.pts, metadata.duration
    validate_chopped_window(pts, duration)
    return Base.checked_add(div(UInt64(pts), duration), UInt64(1))
end

function domain()
    value = uuid4().value
    return AcquisitionDomain(ntuple(i -> UInt8((value >> (8(i - 1))) & 0xff), 16))
end
"Single-writer publication boundary; a failed publisher requires transport teardown."
mutable struct ExposurePublisher{O}
    owner::O
    active::Bool
    pending::Bool
    failed::Bool
end
ExposurePublisher(owner) = ExposurePublisher(owner, false, false, false)

"Stage a completed exposure inside the current optical source step."
function stage_exposure!(
    publisher::ExposurePublisher,
    index,
    buffer,
    identity,
    pts,
    duration,
    classification;
    optical_classification = nothing,
)
    publisher.failed &&
        throw(ArgumentError("failed exposure publisher requires transport teardown"))
    publisher.active ||
        throw(ArgumentError("exposure publication requires a source-step boundary"))
    properties = optical_classification === nothing ? (;) : (; optical_classification)
    # A rejected newer exposure can leave an older sample pending for retry.
    publisher.pending = true
    try
        return stage_pipewire_array!(
            publisher.owner,
            index,
            buffer;
            identity,
            pts,
            duration,
            trigger_state = classification,
            properties...,
        )
    catch
        publisher.failed = true
        rethrow()
    end
end

"""
    publish_exposures!(operation, publisher)

Stage completed exposures during `operation()`, then trigger the connected graph
once. A step without exposures does not trigger. A nonthrowing staging rejection
still retries retained HIL samples. Staging is not transactional: after an error,
the publisher cannot be reused and its transport must be closed, so partial
staging cannot escape during a later source step.
"""
function publish_exposures!(operation, publisher::ExposurePublisher)
    publisher.failed &&
        throw(ArgumentError("failed exposure publisher requires transport teardown"))
    publisher.active &&
        throw(ArgumentError("exposure publication boundaries cannot be nested"))
    publisher.active = true
    publisher.pending = false
    try
        result = operation()
        publisher.failed &&
            throw(ArgumentError("failed exposure publisher requires transport teardown"))
        publisher.pending && trigger_process!(publisher.owner)
        return result
    catch
        publisher.failed = true
        rethrow()
    finally
        publisher.active = false
        publisher.pending = false
    end
end

function validate_chopped_window(pts::Int64, duration::UInt64)
    pts >= 0 && 0 < duration <= typemax(Int64) && UInt64(pts) % duration == 0 || throw(
        ArgumentError(
            "chopped exposure must occupy an aligned nonnegative model-time window"
        ),
    )
    return nothing
end
validate_chopped_window(pts, duration) = throw(
    ArgumentError("chopped exposure must occupy an aligned nonnegative model-time window"),
)


include("clocked_optical_source.jl")

"Single-writer buffers and scientific cursor for one prepared optical simulation."
mutable struct Execution{S,P,C,F,B,D,R}
    science::S
    packet::P
    command::C
    incoming_command::C
    frames::F
    buffers::B
    domains::D
    readback_contexts::R
    clocked::Bool
    command_limit::Float64
    pacing_period_ns::Int64
    generation::Int64
    sequence::UInt64
    deadline_ns::Int64
    upstream_generation::UInt64
    host_ns::Union{Nothing,Int64}
end

"""
    prepare(science; clocked=false, command_count=0, command_limit=Inf,
        pacing_period_ns=0, frame_types=...)

Own buffers for a prepared `science` containing `plant`, `pupil`, `sensors` and
`chopping`. Clocked science also contains the result of `prepare_clocked_source`.
Preparation opens no streams and consumes no scientific samples. Call `warmup!`
before admission and `prepare_transport!` after streams have been opened.
"""
function prepare(
    science;
    clocked::Bool = false,
    command_count::Integer = 0,
    command_limit::Real = Inf,
    pacing_period_ns::Integer = 0,
    frame_types = map(_ -> Float32, science.sensors),
)
    0 <= command_count <= typemax(Int) || throw(ArgumentError("invalid command count"))
    0 <= pacing_period_ns <= typemax(Int64) || throw(ArgumentError("invalid pacing period"))
    command_limit > 0 || throw(ArgumentError("command limit must be positive"))
    clocked &&
        (command_count == 0 || length(science.source_command) != command_count) &&
        throw(DimensionMismatch("clocked source command count differs from preparation"))
    packet = zeros(Float32, science.pupil.shape)
    command = clocked ? zeros(Float32, command_count) : Float32[]
    frames = map(
        sensor -> zeros(eltype(sensor.image), size(sensor.image)), values(science.sensors)
    )
    keys(frame_types) == keys(science.sensors) ||
        throw(ArgumentError("frame types must match the declared sensor names and order"))
    buffers = map(frames, values(frame_types)) do frame, T
        _wire_element_type(T)
        T === eltype(frame) ? frame : zeros(T, size(frame))
    end
    domains = map(_ -> domain(), keys(science.sensors))
    readback_contexts = map(name -> _exposure_readback_context(science.plant, name),
        keys(science.sensors))
    return Execution(
        science,
        packet,
        command,
        similar(command),
        frames,
        buffers,
        domains,
        readback_contexts,
        clocked,
        Float64(command_limit),
        Int64(pacing_period_ns),
        Int64(1),
        UInt64(0),
        Int64(0),
        UInt64(0),
        nothing,
    )
end

struct QuietEmitter{E}
    execution::E
end
struct LiveEmitter{E,P}
    execution::E
    publisher::P
end

"""
    emitter(execution, publisher=nothing)

Return a concrete callable for AOS emissions. The quiet emitter copies and
validates products without publication. The live emitter stages owned copies
inside `publish_exposures!`; it never retains borrowed AOS image storage.
"""
emitter(execution::Execution, ::Nothing = nothing) = QuietEmitter(execution)
emitter(execution::Execution, publisher::ExposurePublisher) =
    LiveEmitter(execution, publisher)

_exposure_readback_context(plant, name) = nothing
function _exposure_readback_context(plant::PreparedExternalPupil, name)
    response = get(getproperty(plant.observations, name), :response, nothing)
    response === nothing && return nothing
    return _response_readback_context(Backends.compute_device(response.graph), response.graph)
end

_response_readback_context(target, graph) = nothing
function _response_readback_context(
    ::Backends.AcceleratorComputeDevice{Backends.CUDABackend}, graph
)
    return graph.context
end

_copy_exposure_frame!(::Nothing, destination::Array, image::AbstractArray) =
    copyto!(destination, image)

function _copy_exposure_frame!(context, destination::Array{T,N}, image::AbstractArray{U,N}) where {T,U,N}
    GC.@preserve destination image begin
        Backends._with_prepared_device_execution_context(context) do
            try
                Backends._copy_prepared_device_buffer!(context, destination, image)
            finally
                Backends._synchronize_prepared_device_execution_context_blocking!(context)
            end
        end
    end
    return destination
end

function copy_exposure!(execution, name, image, metadata)
    index = findfirst(==(name), keys(execution.science.sensors))
    index === nothing && throw(ArgumentError("unknown optical sensor $name"))
    _copy_exposure_frame!(execution.readback_contexts[index], execution.frames[index], image)
    buffer = execution.buffers[index]
    buffer === execution.frames[index] || copy_frame!(buffer, execution.frames[index])
    all(isfinite, buffer) ||
        throw(ArgumentError("optical product exceeds Float32 transport range"))
    sequence = sensor_sequence(metadata, hasproperty(execution.science.chopping, name))
    classification = trigger_state(execution.science, name, metadata)
    return index, sequence, classification
end
copy_frame!(buffer::Array{Float32}, frame) = copyto!(buffer, frame)
function copy_frame!(buffer::Array{UInt16}, frame)
    for index in eachindex(buffer, frame)
        buffer[index] = _encode_frame_value(UInt16, frame[index])
    end
    return buffer
end

function (emit::QuietEmitter)(name, image, metadata)
    copy_exposure!(emit.execution, name, image, metadata)
    return nothing
end
function (emit::LiveEmitter)(name, image, metadata)
    execution = emit.execution
    index, sequence, classification = copy_exposure!(execution, name, image, metadata)
    identity = AcquisitionIdentity(
        execution.domains[index], UInt64(execution.generation), sequence
    )
    stage_exposure!(
        emit.publisher,
        index + 1,
        execution.buffers[index],
        identity,
        metadata.pts,
        metadata.duration,
        classification;
        optical_classification = sensor_optical_classification(
            execution.science, name, metadata
        ),
    )
    return nothing
end

"Exercise prepared optics and restore initial model, exposure and detector state."
function warmup!(execution::Execution; check = () -> nothing)
    try
        if execution.clocked
            warmup_clocked_source!(
                emitter(execution), execution.science, execution.command; check
            )
        else
            warmup!(
                emitter(execution),
                execution.science,
                execution.packet,
                execution.frames,
                execution.buffers;
                check,
            )
        end
    finally
        fill!(execution.packet, 0)
        fill!(execution.command, 0)
        fill!(execution.incoming_command, 0)
        foreach(frame -> fill!(frame, 0), execution.frames)
        foreach(buffer -> fill!(buffer, 0), execution.buffers)
    end
    return nothing
end

"Compile concrete transport operations without staging synthetic camera frames."
function prepare_transport!(execution::Execution, streams)
    input = execution.clocked ? execution.incoming_command : execution.packet
    publisher = ExposurePublisher(streams)
    emit = emitter(execution, publisher)
    # Quiet warmup has a different concrete emitter. Compile the actual live
    # AOS path without stepping optics or staging synthetic exposures.
    if execution.clocked
        precompile(
            Core.kwcall,
            (
                NamedTuple{(:generation,),Tuple{UInt64}},
                typeof(step_clocked_source!),
                typeof(emit),
                typeof(execution.science),
                typeof(execution.command),
            ),
        ) || error("clocked optical live execution preparation failed")
    else
        precompile(
            feed!,
            (
                typeof(emit),
                typeof(execution.science.plant),
                typeof(execution.packet),
                PipeWireArrayReceipt,
            ),
        ) || error("external-pupil live execution preparation failed")
    end
    precompile(drain_pipewire_array!, (typeof(input), typeof(streams), Int)) ||
        error("external-pupil input transport preparation failed")
    for buffer in execution.buffers
        precompile(
            stage_exposure!,
            (
                typeof(publisher),
                Int,
                typeof(buffer),
                AcquisitionIdentity,
                Int64,
                UInt64,
                typeof(ACQUISITION_TRIGGER_ABSENT),
            ),
        ) || error("external-pupil output transport preparation failed")
        precompile(
            Core.kwcall,
            (
                NamedTuple{
                    (:optical_classification,),Tuple{PipeWireAO.OpticalClassification}
                },
                typeof(stage_exposure!),
                typeof(publisher),
                Int,
                typeof(buffer),
                AcquisitionIdentity,
                Int64,
                UInt64,
                typeof(ACQUISITION_TRIGGER_ABSENT),
            ),
        ) || error("external-pupil optical property transport preparation failed")
    end
    return nothing
end

"Return an immutable scientific cursor snapshot; control code does not own this state."
snapshot(execution::Execution) = (;
    generation = execution.generation,
    sequence = execution.sequence,
    deadline_ns = execution.deadline_ns,
    upstream_generation = execution.upstream_generation,
)

"""
    reset!(execution, streams)

Reset optics, clocked source and transport receipts at a serialized control
boundary. Begin a new local generation; retained publications are discarded.
The caller must close the transport if reset fails.
"""
function reset!(execution::Execution, streams)
    execution.generation = Base.checked_add(execution.generation, Int64(1))
    reset_external_pupil!(execution.science.plant)
    if execution.clocked
        reset_clocked_source!(execution.science)
        fill!(execution.command, 0)
        fill!(execution.incoming_command, 0)
    end
    execution.sequence = 0
    execution.deadline_ns = 0
    for index in eachindex(streams.streams)
        reset_pipewire_array!(streams, index)
    end
    execution.upstream_generation = 0
    return nothing
end

"Permit one duty cycle, with the earliest permitted clocked model-step time."
struct Run
    deadline::Int64
end
"Hold model stepping while continuing bounded input draining and control polling."
struct Hold end
"Return from the runner; the caller remains responsible for closing streams."
struct Stop end

function observe_clock!(execution, now_ns::Int64)
    previous = execution.host_ns
    previous === nothing ||
        now_ns >= previous ||
        throw(ArgumentError("optical execution clock regressed"))
    execution.host_ns = now_ns
    return now_ns
end

function process!(
    execution, streams, emit, publisher, decision::Run, now_ns, clock, observe_applied
)
    if execution.clocked
        now_ns < decision.deadline && execution.pacing_period_ns != 0 && return 0
        execution.sequence = publish_exposures!(publisher) do
            step_clocked_source!(
                emit,
                execution.science,
                execution.command;
                generation = UInt64(execution.generation),
            )
        end
        finished_ns = observe_clock!(execution, Clocks.time_nanos(clock)::Int64)
        if observe_applied !== nothing
            observe_applied(execution.science.source_command, execution.sequence, finished_ns)
        end
        execution.deadline_ns =
            execution.pacing_period_ns == 0 ? Int64(0) :
            next_release(decision.deadline, now_ns, finished_ns, execution.pacing_period_ns)
        return 1
    end
    receipt = drain_pipewire_array!(execution.packet, streams, 1)
    receipt === nothing && return 0
    if execution.upstream_generation != 0 &&
        execution.upstream_generation != receipt.identity.generation
        execution.generation = Base.checked_add(execution.generation, Int64(1))
    end
    execution.upstream_generation = receipt.identity.generation
    accepted = publish_exposures!(publisher) do
        feed!(emit, execution.science.plant, execution.packet, receipt)
    end
    accepted && (execution.sequence = Base.checked_add(execution.sequence, UInt64(1)))
    return 1
end
function process!(execution, streams, emit, publisher, ::Hold, now_ns, clock, observe_applied)
    drained = !execution.clocked && drain_pipewire_array!(execution.packet, streams, 1) !== nothing
    sleep(0.001)
    return Int(drained)
end

_with_reclamation_timer(f, device) = f()

function _with_optical_execution(f, execution::Execution)
    graph = first(values(execution.science.plant.graphs))
    device = Backends.compute_device(graph)
    return Backends._with_compute_device(device) do
        execution.clocked ? _with_reclamation_timer(f, device) : f()
    end
end

"""
    run!(control, execution, streams; clock=Clocks.SystemNanoClock(), idle_strategy)

Run a prepared single-writer simulation on the caller's Julia task. Each duty
samples `clock` once and calls `control(execution, streams, now_ns)`, which must
return `Run(deadline::Int64)`, `Hold()` or `Stop()`. Controls may call `reset!`
there and inspect `snapshot`; they must not advance or reset AOS independently.
Clocked sources advance the release deadline on a fixed schedule. The worker
polls controls and commands while waiting and skips elapsed release slots
after an overrun. External-pupil time comes from input receipts.
`observe_applied(command, sequence, pts)`, if supplied, observes the applied
command after each completed clocked optical step.
`idle_strategy` selects how to wait for active deadlines and input receipts.
It defaults to the enclosing simulation policy or the
`PIPEWIREAO_SIM_IDLE_STRATEGY` setting.
The caller owns admission and stream lifetime. A failure requires transport
teardown; this function never opens links or installs native control callbacks.
"""
function run!(
    control::F,
    execution::Execution,
    streams;
    clock::Clocks.AbstractNanoClock = Clocks.SystemNanoClock(),
    observe_applied = nothing,
    idle_strategy = SimRunner.configured_strategy(),
) where {F}
    return _with_optical_execution(execution) do
        _run_optical!(control, execution, streams, clock, observe_applied, idle_strategy)
    end
end

function _run_optical!(control::F, execution, streams, clock, observe_applied, idle_strategy) where {F}
    publisher = ExposurePublisher(streams)
    emit = emitter(execution, publisher)
    return SimRunner.run!(idle_strategy) do
        now_ns = observe_clock!(execution, Clocks.time_nanos(clock)::Int64)
        decision = control(execution, streams, now_ns)
        optical_duty!(decision, execution, streams, emit, publisher, now_ns, clock, observe_applied)
    end
end

optical_duty!(::Stop, execution, streams, emit, publisher, now_ns, clock, observe_applied) = nothing
function optical_duty!(
    decision::Union{Run,Hold}, execution, streams, emit, publisher, now_ns, clock, observe_applied
)
    _array_transport_failed(streams) && error("external-pupil array transport failed")
    command_work = 0
    if execution.clocked
        receipt = drain_pipewire_array!(execution.incoming_command, streams, 1)
        if receipt !== nothing
            all(isfinite, execution.incoming_command) &&
            maximum(abs, execution.incoming_command) <= execution.command_limit || throw(
                ArgumentError(
                    "native optical command exceeds its declared finite amplitude domain",
                ),
            )
            copyto!(execution.command, execution.incoming_command)
            command_work = 1
        end
    end
    accept_deadline!(execution, decision)
    return command_work +
        process!(execution, streams, emit, publisher, decision, now_ns, clock, observe_applied)
end
function next_release(deadline, started, finished, period)
    release = iszero(deadline) ? started : deadline
    # Move to the first future slot without running a burst after an overrun.
    periods = Base.checked_add(div(finished - release, period), 1)
    return Base.checked_add(release, Base.checked_mul(periods, period))
end

accept_deadline!(execution, decision::Run) =
    (execution.deadline_ns = decision.deadline; nothing)
accept_deadline!(execution, ::Hold) = (execution.deadline_ns = 0; nothing)


"One-based host snapshot [x,y] becomes [quantity,y,x]; no display transform."
function pack_pupil!(
    packet::Array{Float32,3}, opd::AbstractMatrix{Float32}, amplitude::Matrix{Float32}
)
    Base.require_one_based_indexing(opd)
    axes(opd) == axes(amplitude) && size(packet) == (2, size(opd, 2), size(opd, 1)) ||
        throw(DimensionMismatch("scientific pupil packet axes differ"))
    for x in axes(opd, 1), y in axes(opd, 2)
        value = opd[x, y]
        isfinite(value) || throw(ArgumentError("scientific pupil OPD must be finite"))
        packet[1, y, x] = value
        packet[2, y, x] = amplitude[x, y]
    end
    return packet
end

end
