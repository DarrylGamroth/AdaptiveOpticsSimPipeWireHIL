"""
    PipeWireExposureRecord(sequence, duration_nanoseconds, trigger_state, optical_classification=nothing)

Owned, immutable metadata observed from one completed simulated exposure.
The sequence must come from committed scientific state, not the requested identity.
Exposure start is deliberately absent: model time has no implicit physical-clock mapping.
Optional optical classification is independent of the electrical trigger.
`has_optical_classification` distinguishes no optical property from explicit ABSENT.
"""
struct PipeWireExposureRecord
    sequence::UInt64
    duration_nanoseconds::UInt64
    trigger_state::AcquisitionTriggerState
    has_optical_classification::Bool
    optical_classification::OpticalClassification

    function PipeWireExposureRecord(sequence::Integer, duration_nanoseconds::Integer,
            trigger_state::AcquisitionTriggerState=ACQUISITION_TRIGGER_ABSENT,
            optical_classification::Union{Nothing,OpticalClassification}=nothing)
        0 <= sequence <= typemax(UInt64) || throw(ArgumentError("exposure sequence is outside UInt64 range"))
        0 < duration_nanoseconds <= typemax(UInt64) || throw(ArgumentError("exposure duration must be positive UInt64 nanoseconds"))
        return new(UInt64(sequence), UInt64(duration_nanoseconds), trigger_state,
            optical_classification !== nothing, something(optical_classification, OPTICAL_ABSENT))
    end
end

"Validate the exact scientific graph/product binding of an acquisition source during preparation."
function validate_acquisition_source end

"Observe a completed exposure; scientific mutation, stepping and publication are forbidden."
function acquisition_record end

struct _ConfiguredExposureSource{Boundary}
    boundary::Boundary
    duration_nanoseconds::UInt64
end

# Buffered boundaries need only provide a committed sequence from their step.
# Where the boundary exposes status, continue checking the sequence independently.
acquisition_record(source::_ConfiguredExposureSource{Nothing}, sequence::UInt64) =
    PipeWireExposureRecord(sequence, source.duration_nanoseconds)

function acquisition_record(source::_ConfiguredExposureSource, ::UInt64)
    status = AlgorithmGraphs.hil_boundary_status(source.boundary)
    return PipeWireExposureRecord(status.frame_sequence, source.duration_nanoseconds)
end

function _prepare_acquisition_source(::Nothing, boundary, configuration)
    observed_boundary = applicable(AlgorithmGraphs.hil_boundary_status, boundary) ? boundary : nothing
    return _ConfiguredExposureSource(observed_boundary, configuration.exposure_duration_ns)
end

function _prepare_acquisition_source(source, boundary, configuration)
    validate_acquisition_source(source, boundary, configuration)
    return source
end

function _stage_exposure_record!(state, source, sequence::UInt64)
    record = acquisition_record(source, sequence)::PipeWireExposureRecord
    record.sequence == sequence || throw(PipeWireHILError(
        "wfs_frame.acquisition", "observed exposure sequence does not match the completed frame",
    ))
    record.duration_nanoseconds == state.exposure_duration_ns || throw(PipeWireHILError(
        "wfs_frame.acquisition", "observed exposure duration differs from the negotiated configuration",
    ))
    state.exposure_record = record
    return nothing
end
