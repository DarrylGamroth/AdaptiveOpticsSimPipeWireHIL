"""Drive a live simulation on the task that owns its prepared plant and device context."""
module SimRunner

import Agent
using Base.ScopedValues: ScopedValue, with

public strategy, current_strategy, configured_strategy, name, run!, idle!

const Strategy = Union{Agent.BusySpinIdleStrategy,Agent.YieldingIdleStrategy}
const ACTIVE = ScopedValue{Strategy}()

"""Resolve the owner setting once, before entering its polling loop."""
function strategy(name = get(ENV, "PIPEWIREAO_SIM_IDLE_STRATEGY", "yielding"))
    name == "yielding" && return Agent.YieldingIdleStrategy()
    name == "busy-spin" && return Agent.BusySpinIdleStrategy()
    throw(ArgumentError("simulator idle strategy must be yielding or busy-spin, got $(repr(name))"))
end

current_strategy() = isassigned(ACTIVE) ? ACTIVE[] : Agent.YieldingIdleStrategy()
configured_strategy() = isassigned(ACTIVE) ? ACTIVE[] : strategy()
name(::Agent.YieldingIdleStrategy) = "yielding"
name(::Agent.BusySpinIdleStrategy) = "busy-spin"

"""Wait once for an outstanding frame or command while permitting Julia GC."""
@inline function idle!(selected::Strategy)
    GC.safepoint()
    Agent.idle(selected)
    return nothing
end

struct SimulationAgent{F}
    duty::F
end

@inline function Agent.do_work(agent::SimulationAgent)
    count = agent.duty()
    count === nothing && return -1
    count isa Int && count >= 0 || throw(ArgumentError("expected a nonnegative work count or nothing"))
    return count
end

"""
    run!(duty, selected=configured_strategy())

Run on the calling task, using Agent.jl's caller-owned invoker. A duty returns
the number of items processed, zero to idle, or `nothing` to finish. Errors
propagate to the owner so it can tear down its streams. Busy-spin requires
independent CPU time for the threads that supply commands and controls.
"""
function run!(duty::F, selected::Strategy = configured_strategy()) where {F}
    with(ACTIVE => selected) do
        invoker = Agent.AgentInvoker(SimulationAgent(duty))
        Agent.start(invoker)
        try
            while true
                count = Agent.invoke_unchecked(invoker)
                count < 0 && break
                GC.safepoint()
                Agent.idle(selected, count)
            end
        finally
            close(invoker)
        end
    end
    return nothing
end

end
