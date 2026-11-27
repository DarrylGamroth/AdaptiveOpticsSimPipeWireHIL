function PipeWireAORTC.InstrumentExport.owner_scheduling!(::Val{:hil}, owner, process)
    workers = process["workers"]
    Set(keys(workers)) ⊆ Set(["pipewire", "optical-diagnostics", "pupil-output"]) ||
        throw(ArgumentError("unknown HIL worker placement"))
    argv = owner["argv"]
    any(
        arg -> first(split(arg, '=')) in (
            "--pipewire-cpu",
            "--optical-diagnostics-cpu",
            "--pupil-output-cpu",
            "--transport-cpu",
        ),
        argv,
    ) && throw(ArgumentError("launcher owns HIL worker arguments"))
    haskey(owner["environment"], "PIPEWIREAO_OPTICAL_CPU") &&
        throw(ArgumentError("obsolete optical CPU environment setting"))
    PipeWireAORTC.InstrumentExport.julia_threads!(argv, haskey(workers, "pupil-output") ? 3 : 2)
    append!(argv, ["--pipewire-cpu", string(worker_cpu(workers, "pipewire"))])
    if haskey(workers, "optical-diagnostics")
        cpu = worker_cpu(workers, "optical-diagnostics")
        cpu != process["leader-cpu"] ||
            throw(ArgumentError("optical diagnostics worker must differ from source CPU"))
        append!(argv, ["--optical-diagnostics-cpu", string(cpu)])
    end
    if haskey(workers, "pupil-output")
        cpu = worker_cpu(workers, "pupil-output")
        cpu != process["leader-cpu"] ||
            throw(ArgumentError("pupil output worker must differ from source CPU"))
        (
            !haskey(workers, "optical-diagnostics") ||
            cpu != worker_cpu(workers, "optical-diagnostics")
        ) || throw(
            ArgumentError("pupil output and optical diagnostics require distinct CPUs")
        )
        append!(argv, ["--pupil-output-cpu", string(cpu)])
    end
    # The managed Julia entry point unwinds SIGINT through its owner scope.
    owner["stop-signal"] = "SIGINT"
    return nothing
end

function worker_cpu(workers, name)
    haskey(workers, name) || throw(ArgumentError("HIL process requires $name worker placement"))
    return only(workers[name])
end
