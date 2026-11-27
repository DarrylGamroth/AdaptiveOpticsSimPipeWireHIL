# A control-path waiter for prepared callback state. The predicate is the
# evidence; notification (including a deadline timer) only wakes its owner.
struct CompletionSignal
    condition::Threads.Condition
    waiting::Atomic{Bool}
end

CompletionSignal() = CompletionSignal(Threads.Condition(), Atomic{Bool}(false))

function _signal_completion!(signal::CompletionSignal)
    signal.waiting[] || return nothing
    lock(signal.condition) do
        notify(signal.condition; all=true)
    end
    return nothing
end

function _await_completion(
    ready, signal::CompletionSignal, timeout_ns::UInt64, field::String,
)
    0 < timeout_ns <= UInt64(typemax(Int64)) || throw(PipeWireHILError(
        field, "completion timeout must be positive and fit Int64 nanoseconds",
    ))
    Threads.atomic_cas!(signal.waiting, false, true) && throw(PipeWireHILError(
        field, "a completion waiter is already active",
    ))
    started = time_ns()
    timer = nothing
    try
        # Julia's timers use millisecond resolution. Round up so a timer cannot
        # wake early and leave an otherwise idle waiter without a deadline wake.
        delay = cld(timeout_ns, UInt64(1_000_000)) / 1000
        timer = Timer(_ -> _signal_completion!(signal), delay)
        lock(signal.condition) do
            while true
                # A completion observed after the deadline is not accepted.
                time_ns() - started >= timeout_ns && throw(PipeWireHILError(
                    field, "completion deadline expired",
                ))
                ready() && return nothing
                wait(signal.condition)
            end
        end
    finally
        signal.waiting[] = false
        timer === nothing || close(timer)
    end
    return nothing
end
