const CompletionAdapter = AdaptiveOpticsSimPipeWireHIL

@testset "completion notifications do not constitute evidence" begin
    signal = CompletionAdapter.CompletionSignal()
    ready = Threads.Atomic{Bool}(false)
    @test CompletionAdapter._signal_completion!(signal) === nothing
    ready[] = true
    @test CompletionAdapter._await_completion(
        () -> ready[], signal, UInt64(1_000_000_000), "test",
    ) === nothing
    @test !signal.waiting[]
    ready[] = false
    waiter = @async CompletionAdapter._await_completion(
        () -> ready[], signal, UInt64(1_000_000_000), "test",
    )
    yield()
    @test signal.waiting[]
    @test_throws CompletionAdapter.PipeWireHILError CompletionAdapter._await_completion(
        () -> ready[], signal, UInt64(1_000_000_000), "test",
    )
    CompletionAdapter._signal_completion!(signal)
    yield()
    @test !istaskdone(waiter)
    ready[] = true
    CompletionAdapter._signal_completion!(signal)
    @test fetch(waiter) === nothing
    @test !signal.waiting[]

    ready[] = false
    @test_throws CompletionAdapter.PipeWireHILError CompletionAdapter._await_completion(
        () -> ready[], signal, UInt64(2_000_000), "test",
    )
    @test !signal.waiting[]
    @test_throws CompletionAdapter.PipeWireHILError CompletionAdapter._await_completion(
        () -> true, signal, UInt64(0), "test",
    )
    @test_throws CompletionAdapter.PipeWireHILError CompletionAdapter._await_completion(
        () -> true, signal, typemax(UInt64), "test",
    )
    @test_throws ErrorException CompletionAdapter._await_completion(
        () -> error("callback failure"), signal, UInt64(1_000_000_000), "test",
    )
    @test !signal.waiting[]
end
