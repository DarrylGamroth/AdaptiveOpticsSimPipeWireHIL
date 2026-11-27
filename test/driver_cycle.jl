const DriverCycle = AdaptiveOpticsSimPipeWireHIL.DriverCycle
const issue_driver_cycle! = AdaptiveOpticsSimPipeWireHIL._issue_driver_cycle!
const complete_driver_cycle! = AdaptiveOpticsSimPipeWireHIL._complete_driver_cycle!
const await_driver_cycle! = AdaptiveOpticsSimPipeWireHIL._await_driver_cycle!
const require_quiescent_driver_cycle = AdaptiveOpticsSimPipeWireHIL._require_quiescent_driver_cycle
const reset_driver_cycle! = AdaptiveOpticsSimPipeWireHIL._reset_driver_cycle!

function immediate_driver_cycle_bytes(cycle, issue!)
    return @allocated begin
        issue_driver_cycle!(issue!, cycle)
        await_driver_cycle!(() -> nothing, cycle, UInt64(1_000_000))
    end
end

function issue_and_complete_driver_cycle!(cycle)
    complete_driver_cycle!(cycle)
    return nothing
end

@testset "driver cycle ownership and completion" begin
    cycle_a = DriverCycle()
    cycle_b = DriverCycle()
    @test issue_driver_cycle!(() -> nothing, cycle_a)
    @test issue_driver_cycle!(() -> nothing, cycle_b)
    complete_driver_cycle!(cycle_a)
    await_driver_cycle!(() -> nothing, cycle_a, UInt64(10_000_000))
    @test cycle_a.completed[] == 1
    @test cycle_b.completed[] == 0
    @test cycle_b.pending
    complete_driver_cycle!(cycle_b)
    await_driver_cycle!(() -> nothing, cycle_b, UInt64(10_000_000))
end

@testset "driver cycle only accepts completion after issue" begin
    cycle = DriverCycle()
    complete_driver_cycle!(cycle)
    calls = Ref(0)
    @test issue_driver_cycle!(() -> (calls[] += 1), cycle)
    @test calls[] == 1
    @test_throws PipeWireHILError await_driver_cycle!(() -> nothing, cycle, UInt64(1_000_000))
    @test cycle.pending
    complete_driver_cycle!(cycle)
    await_driver_cycle!(() -> nothing, cycle, UInt64(10_000_000))
    @test !cycle.pending

    immediate = DriverCycle()
    @test issue_driver_cycle!(() -> complete_driver_cycle!(immediate), immediate)
    await_driver_cycle!(() -> nothing, immediate, UInt64(10_000_000))
    @test !immediate.pending
end

@testset "driver cycle trigger serialization and issuer failure" begin
    cycle = DriverCycle()
    calls = Ref(0)
    @test issue_driver_cycle!(() -> (calls[] += 1), cycle)
    @test !issue_driver_cycle!(() -> (calls[] += 1), cycle)
    @test calls[] == 1
    complete_driver_cycle!(cycle)
    await_driver_cycle!(() -> nothing, cycle, UInt64(10_000_000))
    @test issue_driver_cycle!(() -> (calls[] += 1), cycle)
    @test calls[] == 2
    complete_driver_cycle!(cycle)
    await_driver_cycle!(() -> nothing, cycle, UInt64(10_000_000))

    failed = DriverCycle()
    @test_throws ErrorException issue_driver_cycle!(() -> error("issuer failed"), failed)
    @test failed.pending
    @test_throws PipeWireHILError require_quiescent_driver_cycle(failed)
end

@testset "driver cycle failures retain pending state" begin
    timed_out = DriverCycle()
    @test issue_driver_cycle!(() -> nothing, timed_out)
    @test_throws PipeWireHILError await_driver_cycle!(() -> nothing, timed_out, UInt64(1_000_000))
    @test timed_out.pending
    @test_throws PipeWireHILError require_quiescent_driver_cycle(timed_out)

    check_failed = DriverCycle()
    @test issue_driver_cycle!(() -> nothing, check_failed)
    @test_throws ErrorException await_driver_cycle!(() -> error("health check failed"), check_failed, UInt64(10_000_000))
    @test check_failed.pending
end

@testset "driver cycle reset requires quiescence and preserves counter" begin
    cycle = DriverCycle()
    @test issue_driver_cycle!(() -> nothing, cycle)
    before = (cycle.completed[], cycle.epoch, cycle.pending)
    @test_throws PipeWireHILError reset_driver_cycle!(cycle)
    @test (cycle.completed[], cycle.epoch, cycle.pending) == before
    complete_driver_cycle!(cycle)
    await_driver_cycle!(() -> nothing, cycle, UInt64(10_000_000))
    @test cycle.completed[] == 1
    reset_driver_cycle!(cycle)
    @test cycle.completed[] == 1
    @test cycle.epoch == 1
    @test !cycle.pending
    @test issue_driver_cycle!(() -> nothing, cycle)
    complete_driver_cycle!(cycle)
    await_driver_cycle!(() -> nothing, cycle, UInt64(10_000_000))
    @test cycle.completed[] == 2
end

@testset "driver cycle wait yields and immediate completion allocates nothing" begin
    cycle = DriverCycle()
    issue! = () -> issue_and_complete_driver_cycle!(cycle)
    immediate_driver_cycle_bytes(cycle, issue!)
    @test immediate_driver_cycle_bytes(cycle, issue!) == 0

    waiting = DriverCycle()
    @test issue_driver_cycle!(() -> nothing, waiting)
    progressed = Ref(false)
    task = @async begin
        complete_driver_cycle!(waiting)
        progressed[] = true
    end
    await_driver_cycle!(() -> nothing, waiting, UInt64(100_000_000))
    @test progressed[]
    @test fetch(task)
end


function pending_driver_cycle_wait_bytes(check!, cycle)
    return @allocated await_driver_cycle!(check!, cycle, UInt64(100_000_000))
end

function pending_driver_cycle_wait_fixture()
    cycle = DriverCycle()
    issue_driver_cycle!(() -> nothing, cycle)
    checked = Ref(0)
    released = Base.Threads.Atomic{Bool}(false)
    check! = () -> begin
        checked[] += 1
        released[] = true
        nothing
    end
    producer = @async begin
        while !released[]
            yield()
        end
        complete_driver_cycle!(cycle)
    end
    return cycle, checked, producer, check!
end

@testset "driver cycle pending wait allocation" begin
    # The test measures only the owner-side await call. Its cooperative producer
    # is created first and cannot complete until a health check has run.
    warm_cycle, _, warm_producer, warm_check! = pending_driver_cycle_wait_fixture()
    pending_driver_cycle_wait_bytes(warm_check!, warm_cycle)
    fetch(warm_producer)

    cycle, checks, producer, check! = pending_driver_cycle_wait_fixture()
    bytes = pending_driver_cycle_wait_bytes(check!, cycle)
    fetch(producer)
    @test checks[] > 0
    @test bytes == 0
    @test !cycle.pending
end
