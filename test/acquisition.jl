using AdaptiveOpticsSimPipeWireHIL, PipeWireAO, Test

struct ObservedExposureFixture
    record::PipeWireExposureRecord
end

AdaptiveOpticsSimPipeWireHIL.acquisition_record(source::ObservedExposureFixture, ::UInt64) = source.record

struct BufferedExposureFixture end

struct StatusExposureFixture
    frame_sequence::UInt64
end

import AdaptiveOpticsSim.AlgorithmGraphs: hil_boundary_status
hil_boundary_status(plant::StatusExposureFixture) =
    (; plant.frame_sequence)

@testset "configured exposure works with or without boundary status" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    configuration = (; exposure_duration_ns=UInt64(10))
    source = adapter._prepare_acquisition_source(nothing, BufferedExposureFixture(), configuration)
    @test adapter.acquisition_record(source, UInt64(1)) == PipeWireExposureRecord(1, 10)

    source = adapter._prepare_acquisition_source(nothing, StatusExposureFixture(2), configuration)
    state = adapter.ExchangeState(zeros(Float32, 2), zeros(Float32, 1), UInt64(10))
    @test_throws PipeWireHILError adapter._stage_exposure_record!(state, source, UInt64(1))
    adapter._stage_exposure_record!(state, source, UInt64(2))
    @test state.exposure_record == PipeWireExposureRecord(2, 10)
end

@testset "observed exposure staging before publication" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    state = adapter.ExchangeState(zeros(Float32, 2), zeros(Float32, 468), UInt64(10), :float32, 1f0)
    @test isbitstype(PipeWireExposureRecord)
    @test_throws ArgumentError PipeWireExposureRecord(1, 0)
    @test_throws ArgumentError PipeWireExposureRecord(-1, 10)
    mixed = PipeWireExposureRecord(1, 10, ACQUISITION_TRIGGER_MIXED)
    adapter._stage_exposure_record!(state, ObservedExposureFixture(mixed), UInt64(1))
    @test state.exposure_record == mixed
    @test state.frame_ready[] == 0
    for invalid in (PipeWireExposureRecord(2, 10, ACQUISITION_TRIGGER_HIGH),
            PipeWireExposureRecord(1, 11, ACQUISITION_TRIGGER_LOW))
        @test_throws PipeWireHILError adapter._stage_exposure_record!(state,
            ObservedExposureFixture(invalid), UInt64(1))
        @test state.exposure_record == mixed
        @test state.frame_ready[] == 0
    end
    adapter._reset_exchange_state!(state)
    @test state.exposure_record.sequence == 0
    @test state.exposure_record.trigger_state == ACQUISITION_TRIGGER_ABSENT
end

function exposure_metadata_fixture()
    native = PipeWireAO.LibPipeWire
    snapshot=MetadataSnapshot((MetadataRequirement(SPA.META_HEADER),MetadataRequirement(SPA.META_ACQUISITION_IDENTITY),
        MetadataRequirement(SPA.META_EXPOSURE_TIMING),MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP),
        MetadataRequirement(SPA.META_OPTICAL_CHOPPING),MetadataRequirement(SPA.META_ELECTRICAL_TRIGGER)))
    properties=snapshot.storage
    metadata=snapshot.descriptors
    spa=Ref(native.spa_buffer(snapshot.count,0,pointer(metadata),C_NULL))
    packet=Ref(native.pw_buffer(Base.unsafe_convert(Ptr{native.spa_buffer},spa),C_NULL,0,0,0))
    return (;snapshot,properties,metadata,spa,packet,buffer=StreamBuffer(Base.unsafe_convert(Ptr{native.pw_buffer},packet)))
end

@testset "publication time is not simulated exposure start" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    for trigger in (ACQUISITION_TRIGGER_ABSENT, ACQUISITION_TRIGGER_MIXED)
        state = adapter.ExchangeState(zeros(Float32, 2), zeros(Float32, 468), UInt64(10), :float32, 1f0)
        state.exposure_record = PipeWireExposureRecord(7, 10,
            trigger)
        fixture = exposure_metadata_fixture()
        GC.@preserve fixture begin
            published = adapter._set_frame_metadata!(state, fixture.buffer, UInt64(7))
            @test buffer_header(fixture.buffer).pts == published > 0
            @test buffer_header(fixture.buffer).sequence == 7
            record = metadata_records(fixture.buffer)
            @test acquisition_identity(record).sequence == 7
            @test !exposure_timing(record).has_start
            @test reference_timestamp(record).clock == METADATA_CLOCK_REALTIME
            @test exposure_timing(record).duration_nanoseconds == 10
            @test trigger_classification(record) == state.exposure_record.trigger_state
            @test_throws PipeWireHILError adapter._set_frame_metadata!(state, fixture.buffer, UInt64(8))
        end
    end
end

function outstanding_acquisition_state(; trigger=ACQUISITION_TRIGGER_HIGH)
    adapter = AdaptiveOpticsSimPipeWireHIL
    state = adapter.ExchangeState(zeros(Float32, 2), fill(-9f0, 468), UInt64(10), :float32, 1f0)
    state.acquisition_generation = 2
    state.exposure_record = PipeWireExposureRecord(7, 10, trigger)
    state.frame_ready[] = 7
    state.frame_published[] = 7
    adapter._record_frame_publication!(state, UInt64(7), Int64(100))
    return state
end

function command_acquisition_fixture(state; domain=state.acquisition_domain,
    generation=state.acquisition_generation, sequence=UInt64(7), duration=UInt64(10),
    trigger=ACQUISITION_TRIGGER_HIGH, optical_classification=nothing, identity=true, valid=true)
    fixture = exposure_metadata_fixture()
    GC.@preserve fixture begin
        metadata = initialize_metadata!(fixture.buffer)
        optical_classification === nothing || set_optical_classification!(metadata, optical_classification)
        set_trigger_classification!(metadata, trigger)
        identity && set_acquisition_identity!(metadata, AcquisitionIdentity(domain, generation, sequence))
        duration === nothing || set_exposure_timing!(metadata, ExposureTiming(;duration_nanoseconds=duration))
        valid || fill!(fixture.properties, typemax(UInt64))
    end
    return fixture
end

function command_publication_state(state)
    return (state.command_ready[], state.timing_last_event_nanoseconds[],
        state.timing_completed_sequence[], state.timing_completed_source_published_nanoseconds[],
        state.timing_completed_command_received_nanoseconds[], state.frame_ready[], state.frame_published[])
end

function command_acquisition_allocations(state, metadata)
    AdaptiveOpticsSimPipeWireHIL._validate_command_acquisition(state, metadata, UInt64(7))
    return @allocated AdaptiveOpticsSimPipeWireHIL._validate_command_acquisition(state, metadata, UInt64(7))
end

@testset "command acquisition records before numerical adoption" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    values = fill(0.25f0, 468)
    wrong_domain = AcquisitionDomain(ntuple(index -> index == 1 ? UInt8(0xb0) : UInt8(0), 16))
    for mismatch in ((; generation=UInt64(1)), (; domain=wrong_domain),
            (; sequence=UInt64(8)), (; valid=false), (; identity=false),
            (; duration=nothing), (; duration=UInt64(11)),
            (; trigger=ACQUISITION_TRIGGER_MIXED), (; trigger=ACQUISITION_TRIGGER_LOW),
            (; trigger=ACQUISITION_TRIGGER_ABSENT))
        state = outstanding_acquisition_state()
        before = command_publication_state(state)
        fixture = command_acquisition_fixture(state; mismatch...)
        GC.@preserve fixture begin
            metadata = metadata_records(fixture.buffer)
            @test_throws PipeWireHILError adapter._receive_command!(
                state, values, UInt64(7), Int64(120), metadata)
        end
        @test all(==(-9f0), state.command)
        @test command_publication_state(state) == before
    end

    state = outstanding_acquisition_state()
    before = command_publication_state(state)
    @test_throws PipeWireHILError adapter._receive_command!(state, values, UInt64(7), Int64(120))
    @test all(==(-9f0), state.command)
    @test command_publication_state(state) == before

    state = outstanding_acquisition_state()
    state.exposure_record = PipeWireExposureRecord(8, 10, ACQUISITION_TRIGGER_HIGH)
    fixture = command_acquisition_fixture(state)
    GC.@preserve fixture begin
        @test_throws PipeWireHILError adapter._receive_command!(
            state, values, UInt64(7), Int64(120), metadata_records(fixture.buffer))
    end
    @test all(==(-9f0), state.command)
    @test command_publication_state(state) == before

    for trigger in (ACQUISITION_TRIGGER_HIGH, ACQUISITION_TRIGGER_LOW, ACQUISITION_TRIGGER_MIXED)
        state = outstanding_acquisition_state(; trigger)
        fixture = command_acquisition_fixture(state; trigger)
        GC.@preserve fixture begin
            metadata = metadata_records(fixture.buffer)
            @test command_acquisition_allocations(state, metadata) == 0
            adapter._receive_command!(state, values, UInt64(7), Int64(120), metadata)
        end
        @test state.command == values
        @test state.command_ready[] == 7
        @test state.timing_completed_sequence[] == 7
        @test state.timing_completed_command_received_nanoseconds[] == 120
    end

    state = outstanding_acquisition_state()
    old_fixture = command_acquisition_fixture(state)
    GC.@preserve old_fixture begin
        old_metadata = metadata_records(old_fixture.buffer)
        adapter._receive_command!(state, values, UInt64(7), Int64(120), old_metadata)
        state.command_ready[] = 0 # The owner has adopted the first command.
        adapter._reset_exchange_state!(state)
        @test state.acquisition_generation == 3
        state.exposure_record = PipeWireExposureRecord(7, 10, ACQUISITION_TRIGGER_HIGH)
        state.frame_ready[] = 7
        state.frame_published[] = 7
        adapter._record_frame_publication!(state, UInt64(7), Int64(200))
        before = command_publication_state(state)
        @test_throws PipeWireHILError adapter._receive_command!(
            state, fill(0.5f0, 468), UInt64(7), Int64(220), old_metadata)
        @test state.command == values
        @test command_publication_state(state) == before
        current_fixture = command_acquisition_fixture(state)
        GC.@preserve current_fixture begin
            adapter._receive_command!(state, values, UInt64(7), Int64(220),
                metadata_records(current_fixture.buffer))
        end
        @test state.command_ready[] == 7
        @test state.timing_completed_command_received_nanoseconds[] == 220
    end
end

function receive_test_command!(state, values, sequence, received_ns)
    record = state.exposure_record
    state.exposure_record = PipeWireExposureRecord(sequence, record.duration_nanoseconds, record.trigger_state,
        record.has_optical_classification ? record.optical_classification : nothing)
    record = state.exposure_record
    fixture = command_acquisition_fixture(state; sequence, trigger=record.trigger_state,
        duration=record.duration_nanoseconds, optical_classification=
            record.has_optical_classification ? record.optical_classification : nothing)
    GC.@preserve fixture AdaptiveOpticsSimPipeWireHIL._receive_command!(state, values, sequence,
        received_ns, metadata_records(fixture.buffer))
end

@testset "sample reference time does not reset acquisition identity" begin
    state=AdaptiveOpticsSimPipeWireHIL.ExchangeState(zeros(Float32,2),zeros(Float32,2),UInt64(10))
    state.exposure_record=PipeWireExposureRecord(1,10)
    fixture=exposure_metadata_fixture()
    GC.@preserve fixture begin
        set_reference_timestamp!(writable_metadata_records(fixture.buffer),ReferenceTimestamp(typemax(UInt64)-1))
        AdaptiveOpticsSimPipeWireHIL._set_frame_metadata!(state,fixture.buffer,UInt64(1))
        @test reference_timestamp(fixture.buffer).clock==METADATA_CLOCK_REALTIME
        @test reference_timestamp(fixture.buffer).timestamp_nanoseconds < typemax(UInt64)-1
        @test acquisition_identity(fixture.buffer)==AcquisitionIdentity(state.acquisition_domain,state.acquisition_generation,1)
        @test !exposure_timing(fixture.buffer).has_start
        call!(state,buffer)=AdaptiveOpticsSimPipeWireHIL._set_frame_metadata!(state,buffer,UInt64(1))
        call!(state,fixture.buffer)
        @test @allocated(call!(state,fixture.buffer))==0
    end
end
