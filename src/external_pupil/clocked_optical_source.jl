"""
    prepare_clocked_source(science; source, command_input, amplitude, pupil_axes,
        period_ns, check=()->nothing)

Compose an already prepared host source graph with prepared optics. Validate
physical mirror and entrance-pupil planes without reading application assets.
"""
function prepare_clocked_source(
    science;
    source,
    command_input,
    amplitude,
    pupil_axes,
    period_ns::Integer,
    check = () -> nothing,
)
    0 < period_ns <= typemax(Int64) ||
        throw(ArgumentError("source period must be positive signed nanoseconds"))
    command = graph_input(source.graph, command_input)
    eltype(command) === Float32 &&
    size(command) == (length(command),) &&
    all(iszero, command) || throw(
        ArgumentError(
            "clocked optical mirror requires a zero-initialized native Float32 command"
        ),
    )
    opd = graph_output(source.graph, :pupil_opd)
    eltype(amplitude) === Float32 &&
    all(value -> isfinite(value) && 0 <= value <= 1, amplitude) || throw(
        ArgumentError("clocked source pupil amplitude must be finite passive Float32")
    )
    eltype(opd) === Float32 ||
        throw(ArgumentError("clocked source pupil OPD must be Float32"))
    dm_opd = graph_output(source.graph, :dm_opd)
    eltype(dm_opd) === Float32 ||
        throw(ArgumentError("clocked source mirror OPD must be Float32"))
    pupil_opd = permutedims(opd, pupil_axes)
    size(pupil_opd) == size(amplitude) == science.pupil.shape[2:3] || throw(
        DimensionMismatch("clocked source entrance pupil differs from optical interface"),
    )
    for graph in (
        values(science.plant.graphs)...,
        (
            row.chopping.unfringed for
            row in values(science.plant.observations) if hasproperty(row, :chopping)
        )...,
    )
        eltype(graph_input(graph, :dm_opd)) === Float32 &&
        size(graph_input(graph, :dm_opd)) == size(dm_opd) || throw(
            DimensionMismatch("physical mirror OPD differs from optical DM-plane input")
        )
    end
    driver = FixedStepModelTimeDriver(PeriodicSchedule(; period_ns = period_ns))
    check()
    return merge(
        science,
        (;
            source,
            source_command = command,
            source_opd = opd,
            source_pupil_opd = pupil_opd,
            source_pupil_axes = pupil_axes,
            source_amplitude = amplitude,
            source_dm_opd = dm_opd,
            source_driver = driver,
            source_domain = domain(),
            source_period_ns = period_ns,
        ),
    )
end

function reset_clocked_source!(science)
    fill!(science.source_command, 0)
    reset_graph!(science.source.graph)
    reset_model_time!(science.source_driver)
    return nothing
end

"Advance one source-owned model sample, preserving physical DM and entrance planes."
function step_clocked_source!(emit::F, science, command; generation::UInt64) where {F}
    copyto!(science.source_command, command)
    timestamp = step_graph_at!(science.source.graph, science.source_driver)
    dm_opd = science.source_dm_opd
    foreach(values(science.plant.graphs)) do graph
        AlgorithmGraphs.copyto_graph_input!(graph, :dm_opd, dm_opd)
    end
    foreach(values(science.plant.observations)) do observation
        if hasproperty(observation, :chopping)
            AlgorithmGraphs.copyto_graph_input!(
                observation.chopping.unfringed, :dm_opd, dm_opd
            )
        end
    end
    sequence = model_time_sequence(science.source_driver)
    permutedims!(science.source_pupil_opd, science.source_opd, science.source_pupil_axes)
    accepted = step_external_pupil!(
        emit,
        science.plant,
        science.source_pupil_opd,
        science.source_amplitude,
        timestamp,
        ModelDuration(science.source_period_ns);
        domain = science.source_domain.bytes,
        generation,
        sequence,
        lost_samples = UInt64(0),
    )
    accepted || throw(ArgumentError("clocked optical source sample rejected"))
    return sequence
end

function warmup_clocked_source!(emit, science, command; check = () -> nothing)
    durations = map(exposure -> exposure.duration_ns, values(science.plant.exposures))
    samples = cld(2maximum(durations), science.source_period_ns)
    0 < samples <= 4096 ||
        throw(ArgumentError("clocked optical warmup exceeds 4096 samples"))
    try
        fill!(command, 0)
        for _ in 1:samples
            check()
            step_clocked_source!(emit, science, command; generation = UInt64(1))
        end
    finally
        reset_clocked_source!(science)
        reset_external_pupil!(science.plant)
    end
    return nothing
end
