function exchange_wait_bytes()
    runner = AdaptiveOpticsSimPipeWireHIL.SimRunner
    strategy = runner.current_strategy()
    runner.idle!(strategy)
    return @allocated begin
        for _ in 1:1000
            runner.idle!(strategy)
        end
    end
end

function complete_in_retry!(sequence)
    sequence[] = 2
    return nothing
end

function awaited_sequence_bytes(sequence, retry!)
    sequence[] = 0
    return @allocated AdaptiveOpticsSimPipeWireHIL._await_sequence(
        sequence,UInt64(2),"test",UInt64(100_000_000),false,retry!)
end

function suppressed_retry_bytes(sequence, cycle, retry!::F) where {F}
    AdaptiveOpticsSimPipeWireHIL._await_driver_cycle!(() -> nothing, cycle, UInt64(100_000_000))
    sequence[] = 0
    AdaptiveOpticsSimPipeWireHIL._issue_driver_cycle!(() -> nothing, cycle)
    return @allocated AdaptiveOpticsSimPipeWireHIL._await_sequence(sequence, UInt64(2), "test",
        UInt64(100_000_000), false, retry!)
end

function suppressed_retry_fixture()
    cycle = AdaptiveOpticsSimPipeWireHIL.DriverCycle()
    sequence = Base.Threads.Atomic{UInt64}(0)
    issue! = () -> begin
        sequence[] = 2
        AdaptiveOpticsSimPipeWireHIL._complete_driver_cycle!(cycle)
        nothing
    end
    retry! = () -> begin
        issued = AdaptiveOpticsSimPipeWireHIL._issue_driver_cycle!(issue!, cycle)
        issued || AdaptiveOpticsSimPipeWireHIL._complete_driver_cycle!(cycle)
        return issued
    end
    return sequence, cycle, retry!
end

@testset "bounded completion wait and retry cadence" begin
    sequence = Base.Threads.Atomic{UInt64}(0)
    await = AdaptiveOpticsSimPipeWireHIL._await_sequence
    retry! = () -> complete_in_retry!(sequence)
    @test await(sequence,UInt64(2),"test",UInt64(100_000_000),false,retry!) == 2
    awaited_sequence_bytes(sequence,retry!)
    @test awaited_sequence_bytes(sequence,retry!) == 0
    sequence[] = 0
    @test_throws PipeWireHILError await(sequence,UInt64(2),"test",UInt64(1_000_000),false,()->nothing)
    sequence[] = 1
    @test await(sequence,UInt64(2),"test",UInt64(100_000_000),true,retry!) == 2
    sequence[] = 3
    @test_throws PipeWireHILError await(sequence,UInt64(2),"test",UInt64(100_000_000),true,retry!)
    sequence[] = 0
    @test_throws ErrorException await(sequence,UInt64(2),"test",UInt64(100_000_000),false,()->error("stream failed"))
    for retry_result in (nothing, true)
        retries = UInt64[]
        retry_delayed! = () -> begin
            if isempty(retries)
                until = time_ns()+UInt64(3_000_000)
                while time_ns()<until
                    GC.safepoint()
                end
            end
            push!(retries,time_ns())
            length(retries)==2 && (sequence[]=2)
            return retry_result
        end
        sequence[] = 0
        @test await(sequence,UInt64(2),"test",UInt64(100_000_000),false,retry_delayed!) == 2
        @test length(retries)==2
        @test retries[2]-retries[1] >= 1_000_000
    end
end

@testset "suppressed driver trigger does not consume retry cadence" begin
    hil = AdaptiveOpticsSimPipeWireHIL
    delays = UInt64[]
    for _ in 1:8
        cycle = hil.DriverCycle()
        hil._issue_driver_cycle!(() -> nothing, cycle)
        sequence = Base.Threads.Atomic{UInt64}(0)
        attempted = Ref(UInt64(0))
        issued_at = Ref(UInt64(0))
        suppressed = Ref(0)
        retry! = () -> begin
            attempted[] == 0 && (attempted[] = time_ns())
            issued = hil._issue_driver_cycle!(cycle) do
                issued_at[] = time_ns()
                sequence[] = 2
                hil._complete_driver_cycle!(cycle)
            end
            if !issued
                suppressed[] += 1
                # The outstanding cycle completes after the first attempt was
                # suppressed. No new native trigger has consumed a retry slot.
                hil._complete_driver_cycle!(cycle)
            end
            return issued
        end
        @test hil._await_sequence(sequence, UInt64(2), "test",
            UInt64(100_000_000), false, retry!) == 2
        @test suppressed[] == 1
        push!(delays, issued_at[] - attempted[])
    end
    # Use several trials to tolerate an isolated host scheduling pause. An
    # unconditional cooldown makes every trial take at least one millisecond.
    @test minimum(delays) < 1_000_000

    allocation_sequence, allocation_cycle, allocation_retry! = suppressed_retry_fixture()
    suppressed_retry_bytes(allocation_sequence, allocation_cycle, allocation_retry!)
    @test suppressed_retry_bytes(allocation_sequence, allocation_cycle, allocation_retry!) == 0
end

@testset "suppressed retries retain yielding, timeout and failure" begin
    sequence = Base.Threads.Atomic{UInt64}(0)
    attempts = Ref(0)
    progressed = Ref(false)
    producer = @async (progressed[] = true)
    retry! = () -> begin
        attempts[] += 1
        return false
    end
    @test_throws PipeWireHILError AdaptiveOpticsSimPipeWireHIL._await_sequence(
        sequence, UInt64(2), "test", UInt64(10_000_000), false, retry!)
    @test attempts[] > 1
    @test progressed[]
    @test fetch(producer)

    failing_attempts = Ref(0)
    failing_retry! = () -> begin
        failing_attempts[] += 1
        failing_attempts[] == 1 && return false
        error("stream failed after a suppressed trigger")
    end
    @test_throws ErrorException AdaptiveOpticsSimPipeWireHIL._await_sequence(
        sequence, UInt64(2), "test", UInt64(100_000_000), false, failing_retry!)
    @test failing_attempts[] == 2
end

@testset "completion polling yields without per-poll timers" begin
    exchange_wait_bytes()
    @test exchange_wait_bytes() == 0
    progressed = Ref(false)
    task = @async begin
        progressed[] = true
    end
    runner = AdaptiveOpticsSimPipeWireHIL.SimRunner
    strategy = runner.current_strategy()
    while !istaskdone(task)
        runner.idle!(strategy)
    end
    @test progressed[]
    @test fetch(task)
end

@testset "completion checks use one published sequence" begin
    atomic = Base.Threads.Atomic{UInt64}(0)
    ready = AdaptiveOpticsSimPipeWireHIL._sequence_complete
    @test !ready(atomic,UInt64(2),"test",false)
    atomic[] = 1
    @test !ready(atomic,UInt64(2),"test",true)
    @test_throws PipeWireHILError ready(atomic,UInt64(2),"test",false)
    atomic[] = 2
    @test ready(atomic,UInt64(2),"test",false)
    @test ready(atomic,UInt64(2),"test",true)
    atomic[] = 3
    @test_throws PipeWireHILError ready(atomic,UInt64(2),"test",false)
    @test_throws PipeWireHILError ready(atomic,UInt64(2),"test",true)
end
