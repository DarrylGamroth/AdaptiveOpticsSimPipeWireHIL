using AdaptiveOpticsSim.AlgorithmGraphs
using AdaptiveOpticsSim.AlgorithmGraphs: ModelDuration, ModelTimestamp

@testset "PipeWire acquisition model-time capture" begin
    domain=AcquisitionDomain(ntuple(UInt8,16))
    requirements=(MetadataRequirement(SPA.META_HEADER),MetadataRequirement(SPA.META_ACQUISITION_IDENTITY),
        MetadataRequirement(SPA.META_EXPOSURE_TIMING),MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP))
    first,second=MetadataSnapshot(requirements),MetadataSnapshot(requirements)
    a,b=writable_metadata_records(first),writable_metadata_records(second)
    set_acquisition_identity!(a,AcquisitionIdentity(domain,4,81))
    set_acquisition_identity!(b,AcquisitionIdentity(domain,4,82))
    timing(start;clock=METADATA_CLOCK_MODEL,duration=1000)=
        ExposureTiming(start;clock,duration_nanoseconds=duration)
    set_exposure_timing!(a,timing(1_000_000))
    set_exposure_timing!(b,timing(1_001_020))
    origin=capture_pipewire_model_time_origin(first)
    first_capture=capture_pipewire_model_timestamp(first,origin)
    second_capture=capture_pipewire_model_timestamp(second,origin)
    @test model_timestamp(first_capture)==ModelTimestamp(0)
    @test model_timestamp(second_capture)==ModelTimestamp(1020)
    @test model_time_uncertainty(second_capture)===nothing
    provenance=model_time_provenance(second_capture)
    @test provenance.identity.sequence==82
    @test provenance.exposure.start_nanoseconds==1_001_020
    @test provenance.exposure.clock==METADATA_CLOCK_MODEL
    @test !provenance.has_reference_timestamp
    @test provenance.exposure.duration_nanoseconds==1000
    @test isbitstype(typeof(provenance))
    replay=prepare_captured_model_time_driver((first_capture,second_capture))
    @test advance_model_time!(replay)==ModelTimestamp(0)
    @test advance_model_time!(replay)==ModelTimestamp(1020)
    @test model_time_exhausted(replay)
    capture_pipewire_model_timestamp(second,origin)
    @test @allocated(capture_pipewire_model_timestamp(second,origin))==0
    for invalid in (timing(999_999),timing(1_002_000;clock=METADATA_CLOCK_MONOTONIC),
            timing(1_002_000;duration=nothing))
        set_exposure_timing!(b,invalid)
        @test_throws AlgorithmGraphError capture_pipewire_model_timestamp(second,origin)
    end
    set_exposure_timing!(b,timing(1_002_000))
    for identity in (AcquisitionIdentity(AcquisitionDomain(ntuple(i -> UInt8(i+1),16)),4,82),
            AcquisitionIdentity(domain,5,82))
        set_acquisition_identity!(b,identity)
        @test_throws AlgorithmGraphError capture_pipewire_model_timestamp(second,origin)
    end
    set_acquisition_identity!(b,AcquisitionIdentity(domain,4,82))
    stamp=ReferenceTimestamp(typemax(UInt64)-1;clock=METADATA_CLOCK_MODEL)
    set_reference_timestamp!(b,stamp)
    stamped=capture_pipewire_model_timestamp(second,origin)
    @test model_time_provenance(stamped).has_reference_timestamp
    @test model_time_provenance(stamped).reference_timestamp==stamp
    # Optional timestamp presence can change without changing the replay type.
    mixed=prepare_captured_model_time_driver((first_capture,stamped))
    @test advance_model_time!(mixed)==ModelTimestamp(0)
    @test advance_model_time!(mixed)==ModelTimestamp(2000)
end
