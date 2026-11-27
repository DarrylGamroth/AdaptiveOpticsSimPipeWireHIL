module PipeWireAORTCExt

using AdaptiveOpticsSimPipeWireHIL
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil, SimRunner
using AdaptiveOpticsSim.AlgorithmGraphs
using AdaptiveOpticsSim: Backends
using AdaptiveOpticsSim.Optics: with_propagation_checkpoint
using PipeWireAO
import Clocks
import PipeWireAORTC
using PipeWireAORTC: Common, SourceControl, InstrumentConfiguration, Placement

const Bootstrap = PipeWireAORTC.OwnerBootstrapRuntime
const Configuration = PipeWireAORTC.AOSOwner

compute_target(::Val{:cpu}, device) = Backends.HostComputeDevice()
compute_target(::Val{:cuda}, device) =
    Backends.AcceleratorComputeDevice(Backends.CUDABackend(), device)
compute_target(::Val{:amdgpu}, device) =
    Backends.AcceleratorComputeDevice(Backends.AMDGPUBackend(), device)

function target(options)
    device = compute_target(Val(options.backend), options.device)
    availability = Backends.compute_device_availability(device)
    Backends.compute_device_is_available(availability) || throw(
        ArgumentError(Backends.compute_device_unavailable_reason(availability)),
    )
    return device
end

load_backend(::Val{:cpu}) = nothing
load_backend(::Val{:cuda}) = Configuration.load_instrument(:CUDA)
load_backend(::Val{:amdgpu}) = Configuration.load_instrument(:AMDGPU)

include("arguments.jl")
include("report.jl")
include("source_control.jl")
include("optical.jl")
include("metadata.jl")
include("phase_timing.jl")
include("exchange_timing.jl")
include("optical_observation.jl")
include("pupil_output.jl")
include("lockstep_timing.jl")
include("lockstep.jl")
include("pupil_source.jl")
include("placement.jl")

function validate_invocation(::Val{5}, options)
    options.physical_offsets !== nothing && options.pupil_output_cpu !== nothing ||
        throw(ArgumentError("managed pupil source requires offset inputs and pupil output placement"))
    options.phase_timing_directory === nothing && options.optical_diagnostics_cpu === nothing ||
        throw(ArgumentError("managed pupil source has no camera exchange or image diagnostics"))
    return nothing
end

run_source!(::Val{5}, options) = PupilSource.run_source!(options)

function Simulation.managed_main(args::AbstractVector)
    invocation = arguments(args)
    Bootstrap.with_owner(Simulation.managed_main, invocation)
    return 0
end

function Simulation.managed_main(admitted::NamedTuple)
    admitted.owner_check()
    config = Configuration.configuration(admitted.config)
    selection = get(admitted, :idle_strategy_name, nothing)
    idle_strategy = selection === nothing ? SimRunner.strategy() : SimRunner.strategy(selection)
    options = merge(config, admitted, (; idle_strategy))
    mode = Val(options.version)
    get(options, :acquisition, "lockstep") == "free-running" && mode !== Val(3) &&
        throw(ArgumentError("Free-running acquisition requires a graph-backed WFS configuration"))
    validate_invocation(mode, options)
    load_backend(Val(options.backend))
    options.owner_check()
    prepared = mode === Val(3) ? lockstep_receipt(config) : nothing
    load_proper(config, prepared)
    options.owner_check()
    if mode === Val(3)
        Base.invokelatest(run_source!, mode, options, config, prepared)
    else
        Base.invokelatest(run_source!, mode, options, config)
    end
    return nothing
end

function load_proper(config, prepared)
    needs_proper = if prepared === nothing
        Configuration.requires_proper(config)
    else
        any(prepared.document["nodes"]) do node
            node["type"] in (
                "proper_optical_train_f32",
                "proper_optical_train_f64",
                "compiled_optical_paths_f32",
                "compiled_optical_paths_f64",
            )
        end
    end
    needs_proper && Configuration.load_instrument(:Proper)
    return nothing
end

run_source!(mode, options, config) = run_source!(mode, options)

validate_invocation(::Val{V}, options) where {V} =
    throw(ArgumentError("unsupported managed HIL configuration version $V"))

end
