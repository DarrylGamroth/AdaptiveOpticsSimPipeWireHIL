"Parse the complete native bootstrap invocation without loading science."
function arguments(args)
    values = Dict{String,String}()
    required = (
        "--config", "--remote", "--control-node", "--bootstrap-node", "--bootstrap-instance"
    )
    optional = (
        "--phase-timing-directory", "--phase-timing-frames", "--phase-timing-warmup"
    )
    allowed = (
        required...,
        optional...,
        "--pipewire-cpu",
        "--optical-diagnostics-cpu",
        "--pupil-output-cpu",
        "--physical-offsets",
        "--acquisition",
        "--idle-strategy",
    )
    iseven(length(args)) || throw(ArgumentError("each managed HIL source option requires a value"))
    for index in 1:2:length(args)
        key, value = args[index], args[index+1]
        key in allowed || throw(ArgumentError("unknown managed HIL source option $key"))
        haskey(values, key) && throw(ArgumentError("duplicate $key"))
        values[key] = Configuration.text(value, key)
    end
    all(key -> haskey(values, key), required) || throw(
        ArgumentError(
            "managed HIL source requires --config, --remote, --control-node, --bootstrap-node and --bootstrap-instance",
        ),
    )
    remote = values["--remote"]
    isabspath(remote) ||
        throw(ArgumentError("managed HIL source requires an absolute private remote"))
    control_node = Configuration.node_name(values["--control-node"], "control node")
    bootstrap_node = Configuration.node_name(values["--bootstrap-node"], "bootstrap node")
    control_node != bootstrap_node ||
        throw(ArgumentError("source and bootstrap nodes must differ"))
    instance = tryparse(Int64, values["--bootstrap-instance"])
    instance !== nothing && instance > 0 ||
        throw(ArgumentError("bootstrap instance must be positive"))
    acquisition = get(values, "--acquisition", "lockstep")
    acquisition in ("lockstep", "free-running") ||
        throw(ArgumentError("--acquisition must be lockstep or free-running"))
    idle = get(values, "--idle-strategy", nothing)
    (idle === nothing || idle in ("yielding", "busy-spin")) ||
        throw(ArgumentError("--idle-strategy must be yielding or busy-spin"))
    pipewire_cpu = nothing
    if haskey(values, "--pipewire-cpu")
        pipewire_cpu = tryparse(Int, values["--pipewire-cpu"])
        pipewire_cpu !== nothing && pipewire_cpu >= 2 || throw(
            ArgumentError("PipeWire loop CPU must be an integer excluding CPUs 0 and 1")
        )
    end
    optical_diagnostics_cpu = nothing
    if haskey(values, "--optical-diagnostics-cpu")
        optical_diagnostics_cpu = tryparse(Int, values["--optical-diagnostics-cpu"])
        optical_diagnostics_cpu !== nothing && optical_diagnostics_cpu >= 2 ||
            throw(ArgumentError("optical diagnostics CPU must exclude CPUs 0 and 1"))
    end
    requested_timing = any(key -> haskey(values, key), optional)
    pupil_output_cpu = nothing
    if haskey(values, "--pupil-output-cpu")
        pupil_output_cpu = tryparse(Int, values["--pupil-output-cpu"])
        pupil_output_cpu !== nothing && pupil_output_cpu >= 2 ||
            throw(ArgumentError("pupil output CPU must exclude CPUs 0 and 1"))
        pupil_output_cpu != optical_diagnostics_cpu || throw(
            ArgumentError("pupil output and optical diagnostics require distinct CPUs")
        )
    end
    timing_directory = nothing
    timing_frames = timing_warmup = 0
    if requested_timing
        all(key -> haskey(values, key), optional[1:2]) || throw(
            ArgumentError(
                "phase timing requires --phase-timing-directory and --phase-timing-frames",
            ),
        )
        timing_directory = values["--phase-timing-directory"]
        isabspath(timing_directory) ||
            throw(ArgumentError("phase timing directory must be absolute"))
        timing_directory = abspath(timing_directory)
        timing_frames = tryparse(Int, values["--phase-timing-frames"])
        timing_warmup = tryparse(Int, get(values, "--phase-timing-warmup", "512"))
        timing_frames !== nothing && 1 <= timing_frames <= 65_536 ||
            throw(ArgumentError("phase timing frames must be in 1:65536"))
        timing_warmup !== nothing && 0 <= timing_warmup <= 65_536 ||
            throw(ArgumentError("phase timing warmup must be in 0:65536"))
    end
    return (;
        remote,
        control_node,
        bootstrap_node,
        bootstrap_instance = instance,
        acquisition,
        idle_strategy_name = idle,
        config = abspath(values["--config"]),
        phase_timing_directory = timing_directory,
        phase_timing_frames = timing_frames,
        phase_timing_warmup = timing_warmup,
        pipewire_cpu,
        optical_diagnostics_cpu,
        pupil_output_cpu,
        physical_offsets = haskey(values, "--physical-offsets") ?
                           abspath(values["--physical-offsets"]) : nothing,
    )
end
