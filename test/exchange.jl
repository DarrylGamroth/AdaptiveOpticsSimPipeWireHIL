@testset "frame-to-command timing observation" begin
    state = AdaptiveOpticsSimPipeWireHIL.ExchangeState(
        zeros(Float32, 2),
        zeros(Float32, 2),
        UInt64(1),
    )

    @test isbitstype(PipeWireFrameCommandTiming)
    @test hasmethod(frame_command_timing, Tuple{PreparedPipeWireHIL})
    @test AdaptiveOpticsSimPipeWireHIL._frame_command_timing(state) === nothing

    AdaptiveOpticsSimPipeWireHIL._record_frame_publication!(state, UInt64(1), 100)
    @test AdaptiveOpticsSimPipeWireHIL._frame_command_timing(state) === nothing
    AdaptiveOpticsSimPipeWireHIL._complete_frame_command_timing!(state, UInt64(1), 140)
    timing = AdaptiveOpticsSimPipeWireHIL._frame_command_timing(state)
    @test timing == PipeWireFrameCommandTiming(UInt64(1), 100, 140)
    @test timing.sequence == UInt64(1)
    @test timing.source_published_nanoseconds == 100
    @test timing.command_received_nanoseconds == 140
    @test timing.end_to_end_latency_nanoseconds == UInt64(40)

    AdaptiveOpticsSimPipeWireHIL._record_frame_publication!(state, UInt64(2), 160)
    AdaptiveOpticsSimPipeWireHIL._complete_frame_command_timing!(state, UInt64(2), 190)
    @test AdaptiveOpticsSimPipeWireHIL._frame_command_timing(state) ==
          PipeWireFrameCommandTiming(UInt64(2), 160, 190)

    invalid = AdaptiveOpticsSimPipeWireHIL.ExchangeState(
        zeros(Float32, 1),
        zeros(Float32, 1),
        UInt64(1),
    )
    @test_throws PipeWireHILError AdaptiveOpticsSimPipeWireHIL._complete_frame_command_timing!(
        invalid,
        UInt64(3),
        120,
    )
    AdaptiveOpticsSimPipeWireHIL._record_frame_publication!(invalid, UInt64(3), 100)
    @test_throws PipeWireHILError AdaptiveOpticsSimPipeWireHIL._complete_frame_command_timing!(
        invalid,
        UInt64(4),
        120,
    )
    @test_throws PipeWireHILError AdaptiveOpticsSimPipeWireHIL._complete_frame_command_timing!(
        invalid,
        UInt64(3),
        99,
    )
    @test_throws ArgumentError PipeWireFrameCommandTiming(UInt64(1), 100, 99)
end

function _test_complete_exchange_timing!(timing)
    HIL = AdaptiveOpticsSimPipeWireHIL
    HIL._begin_exchange_timing!(timing)
    HIL._exchange_timestamp!(timing, Val(:ready_ns))
    HIL._exchange_timestamp!(timing, Val(:step_started_ns))
    HIL._exchange_timestamp!(timing, Val(:step_finished_ns))
    HIL._exchange_timestamp!(timing, Val(:staged_ns))
    HIL._exchange_timestamp!(timing, Val(:adoption_started_ns))
    HIL._complete_exchange_timing!(timing, UInt64(2), UInt64(7))
    return nothing
end

@testset "owner exchange phase timing and successful sequence attribution" begin
    HIL = AdaptiveOpticsSimPipeWireHIL
    timing = PipeWireExchangeTiming()
    @test fieldnames(PipeWireExchangeTiming) == (:started_ns, :ready_ns,
        :step_started_ns, :step_finished_ns, :staged_ns, :adoption_started_ns,
        :finished_ns, :generation, :sequence)
    @test all(type -> type === UInt64, fieldtypes(PipeWireExchangeTiming))
    @test all(index -> getfield(timing, index) == 0, 1:fieldcount(PipeWireExchangeTiming))
    @test _test_complete_exchange_timing!(timing) === nothing
    phases = ntuple(index -> getfield(timing, index), 7)
    @test first(phases) > 0
    @test issorted(phases)
    @test timing.generation == 2
    @test timing.sequence == 7

    # Starting a new exchange invalidates attribution before any operation can
    # fail and clears timestamps belonging to the preceding successful sample.
    HIL._begin_exchange_timing!(timing)
    @test timing.started_ns >= last(phases)
    @test timing.sequence == 0 && timing.generation == 0
    @test all(index -> getfield(timing, index) == 0, 2:7)
    @test_throws ErrorException begin
        HIL._exchange_timestamp!(timing, Val(:ready_ns))
        HIL._exchange_timestamp!(timing, Val(:step_started_ns))
        error("plant step failed before completing")
    end
    @test timing.started_ns <= timing.ready_ns <= timing.step_started_ns
    @test timing.sequence == 0 && timing.generation == 0
    @test all(index -> getfield(timing, index) == 0, 4:7)

    _test_complete_exchange_timing!(timing)
    _test_complete_exchange_timing!(nothing)
    @test (@allocated _test_complete_exchange_timing!(timing)) == 0
    @test (@allocated _test_complete_exchange_timing!(nothing)) == 0
end

struct CameraLoanPlane
    address::Ptr{UInt8}
    capacity::Int
    flags::UInt32
end

PipeWireAO.capacity(plane::CameraLoanPlane) = plane.capacity
PipeWireAO.data_flags(plane::CameraLoanPlane) = plane.flags
PipeWireAO.data_pointer(plane::CameraLoanPlane) = plane.address

@testset "camera loan uses writable, aligned native storage" begin
    HIL = AdaptiveOpticsSimPipeWireHIL
    state = HIL.ExchangeState(zeros(UInt16, 2, 3), zeros(Float32, 1), UInt64(1), :uint16)
    bytes = zeros(UInt16, 6)
    GC.@preserve bytes begin
        address = Ptr{UInt8}(pointer(bytes))
        writable = SPA.DATA_FLAG_WRITABLE
        destination = HIL._camera_loan_destination(state, CameraLoanPlane(address, 12, writable))
        @test size(destination) == (3, 2)
        destination[3, 2] = 0xffff
        @test bytes[6] == 0xffff
        @test_throws PipeWireHILError HIL._camera_loan_destination(
            state, CameraLoanPlane(address, 10, writable))
        @test_throws PipeWireHILError HIL._camera_loan_destination(
            state, CameraLoanPlane(address, 12, SPA.DATA_FLAG_READABLE))
        @test_throws PipeWireHILError HIL._camera_loan_destination(
            state, CameraLoanPlane(address + 1, 12, writable))
    end
end
