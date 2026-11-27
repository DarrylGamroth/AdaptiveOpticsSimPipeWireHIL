"The private invocation directory owns this small operational report."
report_path(options) =
    joinpath(dirname(options.remote), options.control_node * "-report.json")

"""Save an atomic operational cursor record, without scientific arrays or timing claims."""
function write_report(options, state, generation, source_instance; failures = ())
    generation === nothing &&
        isempty(failures) &&
        throw(ArgumentError("an unknown report generation requires a recorded failure"))
    record = (;
        version = 1,
        kind = "pipewireao.rtc.aos-owner-report/1",
        bootstrap_instance = Configuration.integer(
            options.bootstrap_instance, "bootstrap_instance", 1, typemax(Int64)
        ),
        source_instance = Configuration.integer(source_instance, "source_instance", 1, typemax(Int64)),
        owner_pid = getpid(),
        generation = generation === nothing ? nothing :
                     Configuration.integer(generation, "generation", 1, typemax(Int64)),
        sequence = Configuration.integer(state.sequence, "sequence", 0, typemax(Int64)),
        running = state.running,
        completed = state.completed,
        backend = string(options.backend),
        graph = options.graph,
        graph_sha256 = get(options, :graph_sha256, nothing),
        configuration = get(options, :config_path, nothing),
        configuration_sha256 = get(options, :configuration_sha256, nothing),
        plant = get(options, :plant, nothing),
        failures = [
            first(replace(sprint(showerror, error), '\0' => '?'), 1024) for
            error in failures
        ],
    )
    calibration_identity = get(options, :calibration_identity, nothing)
    calibration_identity === nothing ||
        (record = merge(record, (; startup_calibration_identity = calibration_identity)))
    pupil_output = get(options, :pupil_output_status, () -> nothing)()
    pupil_output === nothing || (record = merge(record, (; pupil_output)))
    idle_strategy = get(options, :idle_strategy, nothing)
    idle_strategy === nothing || (record = merge(record, (; idle_strategy = SimRunner.name(idle_strategy))))
    acquisition = get(options, :acquisition, "lockstep")
    acquisition == "free-running" &&
        (record = merge(record, (; acquisition, free_running=get(options, :free_running_status, nothing))))
    path = report_path(options)
    mkpath(dirname(path))
    temporary, io = mktemp(dirname(path))
    errors = Exception[]
    try
        close(io)
        Common.write_json(temporary, record)
        # Same-directory rename replaces the complete file atomically and
        # preserves a conflicting directory when replacement fails.
        Base.rename(temporary, path)
    catch error
        push!(errors, error)
    finally
        try
            isopen(io) && close(io)
        catch error
            push!(errors, error)
        end
        try
            ispath(temporary) && rm(temporary)
        catch error
            push!(errors, error)
        end
    end
    isempty(errors) ||
        throw(length(errors) == 1 ? only(errors) : CompositeException(errors))
    return nothing
end
