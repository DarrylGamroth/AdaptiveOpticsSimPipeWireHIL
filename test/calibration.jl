const CalibrationTransport = AdaptiveOpticsSimPipeWireHIL
const CalibrationGraphs = CalibrationTransport.AlgorithmGraphs

struct CalibrationTransportTestDeclaration end
struct CalibrationTransportTestOwner{Input,Output}
    input::Input
    output::Output
end

function CalibrationGraphs.graph_node_ports(::Type{CalibrationTransportTestDeclaration}, specification::Tuple{Int,DataType})
    extent, frame_type = specification
    port(name, direction, element) = CalibrationGraphs.graph_port_contract(
        name, direction, :data, element, (extent,), "test.calibration.f32/1", :column_major,
    )
    return (port(:command, :input, Float32), port(:frame, :output, frame_type))
end

function CalibrationGraphs.prepare_graph_node(
    ::Type{CalibrationTransportTestDeclaration}, specification::Tuple{Int,DataType}, props,
    inputs::NamedTuple, outputs::NamedTuple, parameters::NamedTuple, target,
)
    return CalibrationTransportTestOwner(inputs.command, outputs.frame)
end

function CalibrationGraphs.step_graph_node!(owner::CalibrationTransportTestOwner)
    copyto!(owner.output, owner.input)
    return nothing
end
CalibrationGraphs.reset_graph_node!(::CalibrationTransportTestOwner) = nothing

function calibration_transport_fixture(; scale=1.0f-6, encoding=:uint16, frame_type=Float32)
    graph = CalibrationGraphs.prepare_algorithm_graph(CalibrationGraphs.algorithm_graph(
        (CalibrationGraphs.algorithm_node(:plant, CalibrationTransportTestDeclaration, (2, frame_type)),);
        inputs=(CalibrationGraphs.graph_input(:command, :plant => :command, Float32[1, 2]),),
        outputs=(CalibrationGraphs.graph_output(:frame, :plant => :frame),),
    ))
    boundary = CalibrationGraphs.prepare_graph_calibration_boundary(
        graph; command_input=:command, frame_output=:frame,
    )
    state = CalibrationTransport.CalibrationState(CalibrationTransport.ExchangeState(
        CalibrationGraphs.hil_frame_buffer(boundary), CalibrationGraphs.hil_command_buffer(boundary),
        UInt64(50), encoding, scale,
    ), CalibrationTransport._new_calibration_domain())
    return (; graph, boundary, state)
end

function receive_calibration_test_probe!(state, values, sequence)
    CalibrationTransport._copy_calibration_probe!(state, values, sequence)
    # Synthetic queue completion: receipt publication is a separate operation.
    CalibrationTransport._publish_calibration_probe_receipt!(state, sequence)
    return nothing
end

@testset "calibration probe mailbox correlation and validation" begin
    (; graph, boundary, state) = calibration_transport_fixture()
    @test state.pending_exposure === nothing
    @test state.expected_probe[] == 0
    @test state.last_probe_sequence == 0
    @test state.transport.frame_ready[] == 0
    @test CalibrationGraphs.hil_boundary_status(boundary).active_probe_sequence == 0
    @test state.transport.command == Float32[1, 2]
    @test state.transport.acquisition_domain != CalibrationTransport._new_calibration_domain()
    @test_throws PipeWireHILError CalibrationTransport._submit_calibration_probe!(state, UInt64(1), 5)
    @test state.expected_probe[] == state.last_probe_sequence == 0
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(0))
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[8, 9], UInt64(1))
    @test state.transport.command == Float32[1, 2]
    @test CalibrationTransport._arm_calibration_probe!(state, UInt64(7)) == UInt64(7)
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    for invalid_sequence in (UInt64(0), UInt64(6), UInt64(8))
        @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(
            state, Float32[8, 9], invalid_sequence,
        )
        @test state.transport.command == Float32[1, 2]
        @test state.expected_probe[] == 7
        @test state.transport.command_ready[] == 0
    end
    for invalid in (NaN32, Inf32, -Inf32)
        @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[8, invalid], UInt64(7))
        @test state.transport.command == Float32[1, 2]
        @test !state.failed[]
    end
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[8], UInt64(7))
    @test_throws PipeWireHILError CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
    @test @inferred(CalibrationTransport._copy_calibration_probe!(state, Float32[8, 9], UInt64(7))) === nothing
    @test state.transport.command_ready[] == 0 # Copy does not acknowledge queue completion.
    @test CalibrationGraphs.graph_input(graph, Val(:command)) == Float32[1, 2]
    @test state.transport.command == Float32[8, 9] .* 1.0f-6
    CalibrationTransport._publish_calibration_probe_receipt!(state, UInt64(7))
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[99, 99], UInt64(7))
    @test_throws PipeWireHILError CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(8))
    @test @inferred(CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))) == UInt64(7)
    @test state.transport.command_ready[] == state.expected_probe[] == 0
    @test CalibrationGraphs.graph_input(graph, Val(:command)) == Float32[8, 9] .* 1.0f-6
    @test CalibrationGraphs.graph_step_sequence(graph) == 0
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[99, 99], UInt64(7))
    for stale in (UInt64(0), UInt64(6), UInt64(7))
        @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, stale)
    end

    overflow = calibration_transport_fixture(; scale=2.0f0).state
    CalibrationTransport._arm_calibration_probe!(overflow, UInt64(1))
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(overflow, Float32[8, floatmax(Float32)], UInt64(1))
    @test overflow.transport.command == Float32[1, 2]
end

@testset "source publication-cycle witness is separate from exposure completion" begin
    (; boundary, state) = calibration_transport_fixture(; scale=1.0f0)
    @test state.frame_publication_cycle_sequence[] == 0
    CalibrationTransport._complete_calibration_publication_cycle!(state)
    @test state.frame_publication_cycle_sequence[] == 0
    CalibrationTransport._submit_calibration_probe!(state, UInt64(1),
        () -> receive_calibration_test_probe!(state, Float32[3,4], UInt64(1)))
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(1))
    driver = CalibrationGraphs.FixedStepModelTimeDriver(
        CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50)))
    exposure = CalibrationTransport._stage_calibration_exposure!(boundary,state,driver, _ -> nothing)
    state.transport.frame_ready[] = exposure.identity.sequence
    state.transport.frame_published[] = exposure.identity.sequence
    @test state.frame_publication_cycle_sequence[] == 0 # Queueing is not cycle completion.
    @test @inferred(CalibrationTransport._complete_calibration_publication_cycle!(state)) === nothing
    @test state.frame_publication_cycle_sequence[] == exposure.identity.sequence
    @test state.pending_exposure === exposure
    @test_throws PipeWireHILError CalibrationTransport._require_calibration_quiescent(state)
    CalibrationTransport._complete_calibration_exposure!(state,exposure.identity)
    CalibrationTransport._reset_calibration_boundary!(boundary,driver)
    CalibrationTransport._reset_calibration_state!(state)
    @test state.frame_publication_cycle_sequence[] == 0
    # A delayed source event after reset cannot resurrect the prior generation.
    CalibrationTransport._complete_calibration_publication_cycle!(state)
    @test state.frame_publication_cycle_sequence[] == 0
    state.transport.frame_published[] = 3
    CalibrationTransport._fail_calibration!(state)
    CalibrationTransport._complete_calibration_publication_cycle!(state)
    @test state.frame_publication_cycle_sequence[] == 0
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
end

@testset "held exposure completion fence and reset generation" begin
    (; graph, boundary, state) = calibration_transport_fixture(; scale=1.0f0)
    driver = CalibrationGraphs.FixedStepModelTimeDriver(
        CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50)),
    )
    CalibrationTransport._submit_calibration_probe!(state, UInt64(9),
        () -> receive_calibration_test_probe!(state, Float32[3, 4], UInt64(9)),
    )
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(9))
    result = CalibrationGraphs.step_hil_exposure_at!(boundary, driver)
    identity = AcquisitionIdentity(state.transport.acquisition_domain, UInt64(1), result.sequence)
    exposure = PipeWireCalibrationExposure(identity, UInt64(9), result.timestamp, UInt64(50))
    @test isbitstype(typeof(exposure))
    @test exposure.timestamp == CalibrationGraphs.ModelTimestamp(0)
    @test exposure.exposure_duration_nanoseconds == 50
    state.pending_exposure = exposure
    CalibrationTransport._stage_frame_transport!(state.transport)
    @test state.transport.frame_transport == UInt16[3, 4]
    state.transport.frame_ready[] = result.sequence
    @test_throws PipeWireHILError CalibrationTransport._require_calibration_quiescent(state)
    @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, identity)
    state.transport.frame_published[] = result.sequence
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(10))
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
    for invalid in (
        AcquisitionIdentity(CalibrationTransport._new_calibration_domain(), UInt64(1), UInt64(1)),
        AcquisitionIdentity(identity.domain, UInt64(0), UInt64(1)),
        AcquisitionIdentity(identity.domain, UInt64(2), UInt64(1)),
        AcquisitionIdentity(identity.domain, UInt64(1), UInt64(0)),
        AcquisitionIdentity(identity.domain, UInt64(1), UInt64(2)),
    )
        @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, invalid)
        @test state.pending_exposure === exposure
    end
    @test @inferred(CalibrationTransport._complete_calibration_exposure!(state, identity)) === identity
    @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, identity)
    @test CalibrationTransport._require_calibration_quiescent(state) === nothing
    # A second actual graph exposure holds probe 9 without another command.
    result = CalibrationGraphs.step_hil_exposure_at!(boundary, driver)
    @test result.sequence == UInt64(2)
    @test result.timestamp == CalibrationGraphs.ModelTimestamp(50)
    @test CalibrationGraphs.hil_frame_buffer(boundary) == Float32[3, 4]
    identity2 = AcquisitionIdentity(identity.domain, UInt64(1), result.sequence)
    state.pending_exposure = PipeWireCalibrationExposure(identity2, UInt64(9), result.timestamp, UInt64(50))
    state.transport.frame_ready[] = state.transport.frame_published[] = result.sequence
    @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, identity)
    CalibrationTransport._complete_calibration_exposure!(state, identity2)
    CalibrationTransport._reset_calibration_boundary!(boundary, driver)
    CalibrationTransport._reset_calibration_state!(state)
    @test state.transport.acquisition_generation == 2
    @test state.transport.frame_ready[] == state.transport.frame_published[] == 0
    @test state.last_probe_sequence == 9
    @test CalibrationGraphs.model_time_sequence(driver) == 0
    @test CalibrationGraphs.hil_boundary_status(boundary).active_probe_sequence == 0
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(9))
    CalibrationTransport._submit_calibration_probe!(state, UInt64(10),
        () -> receive_calibration_test_probe!(state, Float32[5, 6], UInt64(10)),
    )
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(10))
    result = CalibrationGraphs.step_hil_exposure_at!(boundary, driver)
    identity3 = AcquisitionIdentity(identity.domain, UInt64(2), result.sequence)
    state.pending_exposure = PipeWireCalibrationExposure(identity3, UInt64(10), result.timestamp, UInt64(50))
    state.transport.frame_ready[] = state.transport.frame_published[] = result.sequence
    @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, identity)
    @test state.pending_exposure.identity === identity3
    CalibrationTransport._complete_calibration_exposure!(state, identity3)
    state.transport.acquisition_generation = typemax(UInt64)
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
    @test state.transport.acquisition_generation == typemax(UInt64)
end

@testset "submission failures and unknown outcomes fail closed" begin
    (; graph, boundary, state) = calibration_transport_fixture()
    @test_throws ErrorException CalibrationTransport._submit_calibration_probe!(state, UInt64(1), () -> error("submit failed"))
    @test state.failed[]
    @test state.last_probe_sequence == state.expected_probe[] == 1
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(2))
    @test CalibrationGraphs.graph_step_sequence(graph) == 0

    (; graph, boundary, state) = calibration_transport_fixture()
    consumed_then_failed = () -> begin
        receive_calibration_test_probe!(state, Float32[8, 9], UInt64(5))
        error("producer consumed before reporting failure")
    end
    @test_throws ErrorException CalibrationTransport._submit_calibration_probe!(state, UInt64(5), consumed_then_failed)
    @test state.transport.command_ready[] == 5
    @test state.last_probe_sequence == 5
    @test state.failed[]
    @test_throws PipeWireHILError CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(5))
    @test CalibrationGraphs.graph_input(graph, Val(:command)) == Float32[1, 2]
    @test CalibrationGraphs.hil_boundary_status(boundary).active_probe_sequence == 0

    (; graph, boundary, state) = calibration_transport_fixture()
    CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    receive_calibration_test_probe!(state, Float32[8, 9], UInt64(8))
    state.transport.command[2] = NaN32 # Deliberate mailbox corruption before AOS adoption.
    @test_throws CalibrationGraphs.AlgorithmGraphError CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(8))
    @test state.failed[]
    @test CalibrationGraphs.graph_input(graph, Val(:command)) == Float32[1, 2]
end

function calibration_probe_copy_allocations(state, values, sequence, port=nothing)
    CalibrationTransport._copy_calibration_probe!(state, values, sequence, port)
    return @allocated CalibrationTransport._copy_calibration_probe!(state, values, sequence, port)
end

@testset "calibration borrowed command copy and notification allocations" begin
    (; state) = calibration_transport_fixture()
    CalibrationTransport._arm_calibration_probe!(state, UInt64(1))
    fixture = transport_plane_fixture(zeros(UInt8, state.transport.command_bytes))
    GC.@preserve fixture begin
        data = buffer_data(fixture.buffer)
        copyto!(buffer_memory(data), reinterpret(UInt8, Float32[8, 9]))
        set_chunk!(data; size=state.transport.command_bytes, stride=4)
        values = CalibrationTransport._command_values(state.transport, data)
        @test calibration_probe_copy_allocations(state, values, UInt64(1)) == 0
        @test state.transport.command == Float32[8, 9] .* 1.0f-6
    end
    CalibrationTransport._publish_calibration_probe_receipt!(state, UInt64(1))
    @test @allocated(CalibrationTransport._publish_calibration_probe_receipt!(state, UInt64(1))) == 0
    configuration = PipeWireHILConfiguration(remote="unused-private-core", frame_node_name="frame",
        command_node_name="command", frame_schema="test.frame/1", command_schema="test.command/1",
        rate=SPA.Fraction(1,1), exposure_duration_ns=50)
    @test_throws PipeWireHILError CalibrationTransport._prepare_exchange_state(
        state.transport.frame, zeros(Float64, 2), configuration)
end

@testset "calibration model exposure range validates before stepping" begin
    for negative in (typemin(Int64), Int64(-1))
        @test_throws CalibrationGraphs.AlgorithmGraphError CalibrationGraphs.ModelTimestamp(negative)
    end
    for origin in (typemax(Int64) - Int64(49),)
        driver = CalibrationGraphs.FixedStepModelTimeDriver(
            CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50));
            origin=CalibrationGraphs.ModelTimestamp(origin),
        )
        @test_throws PipeWireHILError CalibrationTransport._calibration_exposure_timestamp(driver, UInt64(50))
        @test CalibrationGraphs.model_time_sequence(driver) == 0
    end
    driver = CalibrationGraphs.FixedStepModelTimeDriver(
        CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50));
        origin=CalibrationGraphs.ModelTimestamp(typemax(Int64) - Int64(50)),
    )
    @test CalibrationTransport._calibration_exposure_timestamp(driver, UInt64(50)) ==
        CalibrationGraphs.ModelTimestamp(typemax(Int64) - Int64(50))
    @test CalibrationGraphs.model_time_sequence(driver) == 0
    @test_throws PipeWireHILError CalibrationTransport._calibration_exposure_timestamp(driver, typemax(UInt64))
    @test CalibrationGraphs.model_time_sequence(driver) == 0
end

@testset "calibration transport preparation requires a fresh AOS owner" begin
    (; boundary) = calibration_transport_fixture()
    configuration = PipeWireHILConfiguration(
        remote="unused-private-core", frame_node_name="frame", command_node_name="command",
        frame_schema="test.calibration.f32/1", command_schema="test.calibration.f32/1",
        rate=SPA.Fraction(1, 1), exposure_duration_ns=1,
    )
    CalibrationGraphs.adopt_hil_probe!(boundary, UInt64(1))
    @test_throws PipeWireHILError prepare_pipewire_calibration(boundary, configuration)
    CalibrationGraphs.step_hil_exposure!(boundary)
    @test_throws PipeWireHILError prepare_pipewire_calibration(boundary, configuration)
end

@testset "exposure owner arming precedes source admission" begin
    for arm in (true, false)
        (; graph, boundary, state) = calibration_transport_fixture(; scale=1.0f0)
        driver = CalibrationGraphs.FixedStepModelTimeDriver(
            CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50)),
        )
        CalibrationTransport._submit_calibration_probe!(state, UInt64(7),
            () -> receive_calibration_test_probe!(state, Float32[3, 4], UInt64(7)),
        )
        CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
        calls = Ref(0)
        before_publish = exposure -> begin
            calls[] += 1
            @test CalibrationGraphs.graph_step_sequence(graph) == CalibrationGraphs.model_time_sequence(driver) == 1
            @test state.pending_exposure === exposure
            @test exposure.identity === AcquisitionIdentity(state.transport.acquisition_domain, UInt64(1), UInt64(1))
            @test exposure.probe_sequence == 7
            @test exposure.timestamp == CalibrationGraphs.ModelTimestamp(0)
            @test exposure.exposure_duration_nanoseconds == 50
            @test state.transport.frame_transport == UInt16[3, 4]
            @test state.transport.frame_ready[] == state.transport.frame_published[] == 0
            return nothing
        end
        exposure = CalibrationTransport._stage_calibration_exposure!(
            boundary, state, driver, arm ? before_publish : CalibrationTransport._no_exposure_arming,
        )
        @test calls[] == (arm ? 1 : 0)
        @test state.pending_exposure === exposure
        @test CalibrationGraphs.graph_step_sequence(graph) == CalibrationGraphs.model_time_sequence(driver) == 1
        @test state.transport.frame_transport == UInt16[3, 4]
        @test state.transport.frame_ready[] == state.transport.frame_published[] == 0
        @test !state.failed[]
    end
end

@testset "exposure owner arming failure retains one step and no source frame" begin
    (; graph, boundary, state) = calibration_transport_fixture(; scale=1.0f0)
    driver = CalibrationGraphs.FixedStepModelTimeDriver(
        CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50)),
    )
    CalibrationTransport._submit_calibration_probe!(state, UInt64(7),
        () -> receive_calibration_test_probe!(state, Float32[3, 4], UInt64(7)),
    )
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
    calls = Ref(0)
    throwing_owner = exposure -> begin
        calls[] += 1
        @test state.pending_exposure === exposure
        error("collector arming failed")
    end
    @test_throws ErrorException CalibrationTransport._stage_calibration_exposure!(boundary, state, driver, throwing_owner)
    @test calls[] == 1
    @test state.failed[]
    @test state.pending_exposure.identity.sequence == 1
    @test CalibrationGraphs.graph_step_sequence(graph) == CalibrationGraphs.model_time_sequence(driver) == 1
    @test state.transport.frame_ready[] == state.transport.frame_published[] == 0
    @test_throws PipeWireHILError CalibrationTransport._stage_calibration_exposure!(boundary, state, driver, throwing_owner)
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, state.pending_exposure.identity)
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
    @test CalibrationGraphs.graph_step_sequence(graph) == CalibrationGraphs.model_time_sequence(driver) == calls[] == 1
end

@testset "calibration operation budget validation and original deadline" begin
    configuration = PipeWireHILConfiguration(
        remote="unused-private-core", frame_node_name="frame", command_node_name="command",
        frame_schema="test.calibration.f32/1", command_schema="test.calibration.f32/1",
        rate=SPA.Fraction(1, 1), exposure_duration_ns=1, timeout_ns=1_000_000_000,
    )
    for invalid in (UInt64(0), 1, 1.0, nothing, UInt32(1), UInt64(1_000_000_001), typemax(UInt64))
        @test_throws PipeWireHILError CalibrationTransport._calibration_timeout_ns(configuration, invalid)
    end
    @test CalibrationTransport._calibration_timeout_ns(configuration, UInt64(1)) == UInt64(1)
    @test CalibrationTransport._calibration_timeout_ns(configuration, configuration.timeout_ns) == configuration.timeout_ns
    oversized_configuration = PipeWireHILConfiguration(
        remote="unused-private-core", frame_node_name="frame", command_node_name="command",
        frame_schema="test.calibration.f32/1", command_schema="test.calibration.f32/1",
        rate=SPA.Fraction(1, 1), exposure_duration_ns=1, timeout_ns=typemax(UInt64),
    )
    @test_throws PipeWireHILError CalibrationTransport._calibration_timeout_ns(oversized_configuration, UInt64(typemax(Int64)) + UInt64(1))
    @test CalibrationTransport._calibration_timeout_ns(oversized_configuration, UInt64(typemax(Int64))) == UInt64(typemax(Int64))
    # A completed earlier phase consumes the same budget as every later wait.
    timeout_ns = UInt64(1_000_000_000)
    started = time_ns() - UInt64(500_000_000)
    remaining = CalibrationTransport._calibration_remaining_ns(started, timeout_ns, "test.first-phase")
    @test 0 < remaining <= UInt64(500_000_000)
    @test CalibrationTransport._calibration_remaining_ns(started, timeout_ns, "test.next-phase") <= remaining
    expired_started = time_ns() - timeout_ns
    for phase in ("calibration.driver", "calibration.probe", "calibration.publication")
        @test_throws PipeWireHILError CalibrationTransport._calibration_remaining_ns(expired_started, timeout_ns, phase)
    end
end

@testset "adopted probe readback preserves demanded wire bits" begin
    (; graph, boundary, state) = calibration_transport_fixture()
    @test state.wire_command == zeros(Float32, 2)
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    # This finite Float32 loses a bit when multiplication by the configured
    # scale is reversed. A signed zero also exercises exact wire preservation.
    values = Float32[reinterpret(Float32, UInt32(0x3ff42401)), -0.0f0]
    original_bits = copy(reinterpret(UInt32, values))
    CalibrationTransport._arm_calibration_probe!(state, UInt64(7))
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    CalibrationTransport._copy_calibration_probe!(state, values, UInt64(7))
    @test reinterpret(UInt32, state.wire_command) == original_bits
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    CalibrationTransport._publish_calibration_probe_receipt!(state, UInt64(7))
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    values[1] = 99.0f0 # The cache owns a copy, rather than borrowing producer storage.
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
    borrowed = @inferred CalibrationTransport._calibration_probe_values(boundary, state)
    @test borrowed === state.wire_command
    @test borrowed !== values
    @test borrowed !== state.transport.command
    @test borrowed !== CalibrationGraphs.graph_input(graph, Val(:command))
    @test reinterpret(UInt32, borrowed) == original_bits
    scaled = CalibrationGraphs.graph_input(graph, Val(:command))
    @test scaled[1] / state.transport.command_scale != borrowed[1]
    @test reinterpret(UInt32, scaled[2]) == UInt32(0x80000000)
    CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[3, NaN], UInt64(8))
    @test reinterpret(UInt32, borrowed) == original_bits # Rejection does not partially overwrite the cache.
    receive_calibration_test_probe!(state, Float32[5, 6], UInt64(8))
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(8))
    @test CalibrationTransport._calibration_probe_values(boundary, state) === borrowed
    @test borrowed == Float32[5, 6]
    CalibrationTransport._reset_calibration_boundary!(boundary, nothing)
    CalibrationTransport._reset_calibration_state!(state)
    @test borrowed == zeros(Float32, 2)
    @test state.last_probe_sequence == 8
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    state.failed[] = true
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
end

@testset "calibration disconnect fences readback and exposure acknowledgement" begin
    native = PipeWireAO.LibPipeWire
    (; boundary, state) = calibration_transport_fixture()
    CalibrationTransport._calibration_stream_transition!(state,
        native.PW_STREAM_STATE_UNCONNECTED, native.PW_STREAM_STATE_UNCONNECTED)
    @test !state.failed[] # The initial unconnected construction state is harmless.
    CalibrationTransport._arm_calibration_probe!(state, UInt64(7))
    receive_calibration_test_probe!(state, Float32[3, 4], UInt64(7))
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
    driver = CalibrationGraphs.FixedStepModelTimeDriver(
        CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(50)))
    exposure = CalibrationTransport._stage_calibration_exposure!(boundary, state, driver, _ -> nothing)
    state.transport.frame_ready[] = state.transport.frame_published[] = exposure.identity.sequence
    @test CalibrationTransport._calibration_probe_values(boundary, state) == Float32[3, 4]
    CalibrationTransport._calibration_stream_transition!(state,
        native.PW_STREAM_STATE_PAUSED, native.PW_STREAM_STATE_UNCONNECTED)
    @test state.failed[]
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, exposure.identity)
    @test state.pending_exposure === exposure
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
end

@testset "calibration command configuration must match the boundary" begin
    (; boundary) = calibration_transport_fixture()
    configuration = PipeWireHILConfiguration(
        remote="unused-private-core", frame_node_name="frame",
        command_node_name=(first="first-command",second="second-command"),
        frame_schema="test.calibration.f32/1",
        command_schema=(first="test.calibration.f32/1",second="test.calibration.f32/1"),
        command_scale=(first=1f0,second=1f0),
        rate=SPA.Fraction(1,1),exposure_duration_ns=1)
    observed = try
        prepare_pipewire_calibration(boundary,configuration)
    catch exception
        exception
    end
    @test observed isa PipeWireHILError
    @test observed.field == "correction_command"
    @test CalibrationGraphs.hil_boundary_status(boundary).frame_sequence == 0
end

function named_calibration_transport_fixture()
    boundary = multi_command_boundary(; calibration=true)
    configuration = multi_command_configuration()
    states = CalibrationTransport._prepare_exchange_state(CalibrationGraphs.hil_frame_buffer(boundary),
        CalibrationGraphs.hil_command_buffer(boundary), configuration)
    state = CalibrationTransport.CalibrationState(states, CalibrationTransport._new_calibration_domain())
    return (; boundary, configuration, state)
end

@testset "named calibration adopts only a complete same-token command set" begin
    (; boundary, configuration, state) = named_calibration_transport_fixture()
    initial = map(copy, boundary.command_input)
    commands = (woofer=Float32[3, 4], tweeter=Float32[5, 6, 7], tiptilt=Float32[8])
    @test keys(state.transport) == keys(state.wire_command) == (:woofer, :tweeter, :tiptilt)
    @test keys(configuration.command_node_name) == (:tiptilt, :woofer, :tweeter)
    @test all(command -> command.acquisition_domain == state.transport.woofer.acquisition_domain,
        values(state.transport))
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    CalibrationTransport._arm_calibration_probe!(state, UInt64(7))
    for key in (:tweeter, :woofer)
        CalibrationTransport._copy_calibration_probe!(state, commands[key], UInt64(7), Val(key))
        @test state.transport[key].command_ready[] == 0
        @test_throws PipeWireHILError CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
        CalibrationTransport._publish_calibration_probe_receipt!(state, UInt64(7), Val(key))
        @test_throws PipeWireHILError CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
        @test boundary.command_input == initial
        @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, commands[key], UInt64(7), Val(key))
    end
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    for token in (UInt64(0), UInt64(6), UInt64(8))
        @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, commands.tiptilt, token, Val(:tiptilt))
        @test state.transport.tiptilt.command == initial.tiptilt
    end
    @test_throws FieldError CalibrationTransport._copy_calibration_probe!(state, commands.tiptilt, UInt64(7), Val(:foreign))
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[NaN], UInt64(7), Val(:tiptilt))
    @test_throws PipeWireHILError CalibrationTransport._copy_calibration_probe!(state, Float32[8, 9], UInt64(7), Val(:tiptilt))
    CalibrationTransport._copy_calibration_probe!(state, commands.tiptilt, UInt64(7), Val(:tiptilt))
    @test_throws PipeWireHILError CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))
    CalibrationTransport._publish_calibration_probe_receipt!(state, UInt64(7), Val(:tiptilt))
    @test @inferred(CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(7))) == 7
    @test state.expected_probe[] == 0
    @test all(command -> command.command_ready[] == 0, values(state.transport))
    @test boundary.command_input == (woofer=commands.woofer .* 2f0, tweeter=commands.tweeter .* 3f0,
        tiptilt=commands.tiptilt .* 4f0)
    wire = @inferred CalibrationTransport._calibration_probe_values(boundary, state)
    @test wire === state.wire_command
    @test wire == commands
    @test CalibrationGraphs.graph_step_sequence(boundary.graph) == 0

    driver = CalibrationGraphs.FixedStepModelTimeDriver(
        CalibrationGraphs.PeriodicSchedule(CalibrationGraphs.ModelDuration(configuration.exposure_duration_ns)))
    exposure = CalibrationTransport._stage_calibration_exposure!(boundary, state, driver, _ -> nothing)
    frame = CalibrationTransport._frame_state(state.transport)
    frame.frame_ready[] = frame.frame_published[] = exposure.identity.sequence
    @test frame.frame == Float32[14, 54, 32]
    @test exposure.probe_sequence == 7
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
    foreign = AcquisitionIdentity(CalibrationTransport._new_calibration_domain(), exposure.identity.generation,
        exposure.identity.sequence)
    @test_throws PipeWireHILError CalibrationTransport._complete_calibration_exposure!(state, foreign)
    @test state.pending_exposure === exposure
    @test wire == commands
    CalibrationTransport._complete_calibration_exposure!(state, exposure.identity)
    second = CalibrationTransport._stage_calibration_exposure!(boundary, state, driver, _ -> nothing)
    @test second.probe_sequence == 7
    @test second.identity.sequence == 2
    @test frame.frame == Float32[14, 54, 32]
    frame.frame_ready[] = frame.frame_published[] = second.identity.sequence
    CalibrationTransport._complete_calibration_exposure!(state, second.identity)
    CalibrationTransport._reset_calibration_boundary!(boundary, driver)
    CalibrationTransport._reset_calibration_state!(state)
    @test boundary.command_input == initial
    @test all(command -> command.acquisition_generation == 2, values(state.transport))
    @test all(values -> all(iszero, values), wire)
    @test state.last_probe_sequence == 7
    @test_throws PipeWireHILError CalibrationTransport._arm_calibration_probe!(state, UInt64(7))
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    CalibrationTransport._arm_calibration_probe!(state, UInt64(8))
    @test calibration_probe_copy_allocations(state, commands.woofer, UInt64(8), Val(:woofer)) == 0
    for key in keys(commands)
        CalibrationTransport._copy_calibration_probe!(state, commands[key], UInt64(8), Val(key))
        CalibrationTransport._publish_calibration_probe_receipt!(state, UInt64(8), Val(key))
    end
    CalibrationTransport._adopt_received_calibration_probe!(boundary, state, UInt64(8))
    @test CalibrationTransport._calibration_probe_values(boundary, state) == commands
    CalibrationTransport._fail_calibration!(state)
    @test_throws PipeWireHILError CalibrationTransport._calibration_probe_values(boundary, state)
    @test_throws PipeWireHILError CalibrationTransport._reset_calibration_state!(state)
end

@testset "calibration clock policy validates before connection" begin
    fixture=calibration_transport_fixture()
    configuration=PipeWireHILConfiguration(remote="intentionally-absent",frame_node_name="frame",
        command_node_name="command",frame_schema="test.frame/1",command_schema="test.command/1",
        rate=SPA.Fraction(1,1),exposure_duration_ns=50)
    @test_throws ArgumentError prepare_pipewire_calibration(fixture.boundary,configuration;
        reference_clock=METADATA_CLOCK_MODEL)
    @test CalibrationGraphs.hil_boundary_status(fixture.boundary).frame_sequence==0
end
