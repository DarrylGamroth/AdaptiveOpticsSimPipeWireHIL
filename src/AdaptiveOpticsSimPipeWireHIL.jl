"""
    AdaptiveOpticsSimPipeWireHIL

Expose AdaptiveOpticsSim models through ordinary PipeWireAO streams: lockstep
HIL boundaries, external-pupil optics and independently clocked optical sources.
Numerical models remain in AOS; callers retain session admission and controls.
"""
module AdaptiveOpticsSimPipeWireHIL

using Base.Threads: Atomic
using AdaptiveOpticsSim: AlgorithmGraphs
using AdaptiveOpticsSim.AlgorithmGraphs: adopt_hil_command!
using AdaptiveOpticsSim.AlgorithmGraphs: hil_command_buffer, hil_frame_buffer
using AdaptiveOpticsSim.AlgorithmGraphs: step_hil_frame!
using UUIDs: uuid4
using PipeWireAO
import PipeWireAO: start!, stop!

export PipeWireHILConfiguration, PreparedPipeWireHIL
export PipeWireExposureRecord
public validate_acquisition_source, acquisition_record
public validate_hil_system, validate_wire_contract
export PipeWireHILError, prepare_pipewire_hil, exchange_frame!
export step_free_running!, free_running_status
export reset_pipewire_hil!
export with_frame_stream, frame_acquisition_generation
export PreparedPipeWireCalibration, PipeWireCalibrationExposure
export prepare_pipewire_calibration, adopt_pipewire_probe!, pipewire_probe_values
export step_pipewire_exposure!, complete_pipewire_exposure!
export reset_pipewire_calibration!, pipewire_calibration_status
export PipeWireFrameCommandTiming, frame_command_timing
export PipeWireExchangeTiming
export PipeWireAcquisitionProvenance, PipeWireAcquisitionTimeOrigin
export capture_pipewire_model_time_origin, capture_pipewire_model_timestamp
export APPLIED_PDM_COMMAND_SCHEMA, applied_command_source_info, with_applied_command_stream
export PipeWireImageSourceOptions, stage_pipewire_image!, image_source_info, with_image_stream
export reset_pipewire_image_sources!
export PipeWireArrayStreamOptions, PipeWireArrayReceipt, with_array_streams, with_array_stream
export stage_pipewire_array!, drain_pipewire_array!, array_stream_status, reset_pipewire_arrays!
export reset_pipewire_array!

public SimRunner
include("sim_runner.jl")
include("schemas.jl")
include("configuration.jl")
include("validation.jl")
include("completion.jl")
include("model_time.jl")
include("acquisition.jl")
include("exchange.jl")
include("applied_command.jl")
include("source_channels.jl")
include("image_source.jl")
include("array_streams.jl")
include("free_running.jl")
include("command_receiver.jl")
include("calibration.jl")

public ExternalPupil
include("external_pupil/ExternalPupil.jl")

public Simulation
include("simulation.jl")

end
