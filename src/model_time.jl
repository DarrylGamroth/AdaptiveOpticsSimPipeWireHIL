"The acquisition clock and source cohort used as captured model-time zero."
struct PipeWireAcquisitionTimeOrigin
    nanoseconds::Int64
    clock::MetadataClock
    domain::AcquisitionDomain
    generation::UInt64
end

"Owned source identity and timing retained with captured model time."
struct PipeWireAcquisitionProvenance
    identity::AcquisitionIdentity
    exposure::ExposureTiming
    has_reference_timestamp::Bool
    reference_timestamp::ReferenceTimestamp
end
PipeWireAcquisitionProvenance(identity,exposure,stamp) = PipeWireAcquisitionProvenance(
    identity,exposure,stamp !== nothing,something(stamp,ReferenceTimestamp(0)))

@noinline function _capture_error(message::AbstractString)
    throw(AlgorithmGraphs.AlgorithmGraphError(String(message)))
end
function _acquisition_clock(metadata)
    timing=exposure_timing(metadata)
    timing !== nothing && timing.has_start || _capture_error("PipeWire capture requires an explicit exposure start")
    return timing
end
function capture_pipewire_model_time_origin(metadata)
    identity=acquisition_identity(metadata)
    identity===nothing && _capture_error("PipeWire model-time capture requires acquisition identity")
    timing=_acquisition_clock(metadata)
    return PipeWireAcquisitionTimeOrigin(timing.start_nanoseconds,timing.clock,identity.domain,identity.generation)
end
function capture_pipewire_model_timestamp(metadata,origin::PipeWireAcquisitionTimeOrigin)
    identity=acquisition_identity(metadata)
    identity===nothing && _capture_error("PipeWire model-time capture requires acquisition identity")
    timing=_acquisition_clock(metadata)
    timing.has_duration || _capture_error("PipeWire model-time capture requires exposure duration")
    timing.clock==origin.clock && identity.domain==origin.domain && identity.generation==origin.generation ||
        _capture_error("PipeWire exposure clock or source cohort differs from the captured origin")
    delta=Int128(timing.start_nanoseconds)-origin.nanoseconds
    0 <= delta <= typemax(Int64) || _capture_error("PipeWire exposure timestamp is outside the captured model-time range")
    return AlgorithmGraphs.CapturedModelTimestamp(AlgorithmGraphs.ModelTimestamp(Int64(delta)),
        nothing,PipeWireAcquisitionProvenance(identity,timing,reference_timestamp(metadata)))
end
