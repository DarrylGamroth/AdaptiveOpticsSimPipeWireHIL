using AdaptiveOpticsSimPipeWireHIL: SimRunner

@testset "live simulator idle strategies" begin
    @test_throws ArgumentError SimRunner.strategy("spin")
    withenv("PIPEWIREAO_SIM_IDLE_STRATEGY" => nothing) do
        @test SimRunner.name(SimRunner.strategy()) == "yielding"
    end
    for policy in ("yielding", "busy-spin")
        selected = SimRunner.strategy(policy)
        original_task = current_task()
        cycles = Ref(0)
        when_scheduled = Ref(0)
        sibling = Ref{Task}()
        SimRunner.run!(selected) do
            @test current_task() === original_task
            @test SimRunner.current_strategy() === selected
            cycles[] += 1
            if cycles[] == 1
                sibling[] = @async (when_scheduled[] = cycles[])
                return 0
            end
            return nothing
        end
        wait(sibling[])
        @test when_scheduled[] == (policy == "yielding" ? 1 : 2)
        @test SimRunner.name(SimRunner.current_strategy()) == "yielding"
    end
    @test_throws ErrorException SimRunner.run!(() -> error("source failed"), SimRunner.strategy("busy-spin"))
    @test_throws ArgumentError SimRunner.run!(() -> -1)
    @test SimRunner.name(SimRunner.current_strategy()) == "yielding"
end
