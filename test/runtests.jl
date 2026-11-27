using AdaptiveOpticsSimPipeWireHIL
using Aqua
using PipeWireAO
using Test

@testset "AdaptiveOpticsSimPipeWireHIL" begin
    include("configuration.jl")
    include("simulation.jl")
    include("simulation_optical.jl")
    include("validation.jl")
    include("completion.jl")
    include("sim_runner.jl")

    include("acquisition.jl")
    include("exchange.jl")
    include("applied_command.jl")
    include("image_source.jl")
    include("array_streams.jl")
    include("source_channels.jl")
    include("exchange_wait.jl")
    include("driver_cycle.jl")
    include("transport.jl")
    include("lifecycle.jl")
    include("multi_command.jl")
    include("calibration.jl")
    include("optical_exposure_publication.jl")
    include("external_pupil_execution.jl")

    @testset "row-major frame transport" begin
        frame = reshape(Float32.(1:6), 2, 3)
        state = AdaptiveOpticsSimPipeWireHIL.ExchangeState(
            frame,
            zeros(Float32, 1),
            UInt64(1),
        )
        AdaptiveOpticsSimPipeWireHIL._stage_frame_transport!(state)
        @test state.frame_transport == Float32[1, 3, 5, 2, 4, 6]
    end

    Aqua.test_all(AdaptiveOpticsSimPipeWireHIL)
end

include("model_time.jl")
include("live_private_core.jl")
include("live_free_running_private_core.jl")
include("live_command_receiver.jl")

include("live_calibration_private_core.jl")
include("live_named_calibration_private_core.jl")

@testset "Standalone CPU simulation" begin
    include("external_pupil_stream_formats.jl")
    include("simulation_optical_preparation.jl")
    include("simulation_compiled_optical.jl")
    include("simulation_private_core.jl")
    include("simulation_optical_private_core.jl")
end

@testset "Managed CPU simulation" begin
    include("managed_contracts.jl")
    include("managed_phase_timing.jl")
    include("optical_startup_image.jl")
    include("managed_optical_private_core.jl")
end

@testset "Managed exchange and observation workers" begin
    # Deployment's pupil worker owns Julia thread three. Keep the existing
    # transport fixtures on their original thread pools.
    project = dirname(Base.active_project())
    package = pkgdir(AdaptiveOpticsSimPipeWireHIL)
    coverage = Base.JLOptions().code_coverage == 0 ? String[] : ["--code-coverage=@$package"]
    script = joinpath(@__DIR__, "managed_exchange_private_core.jl")
    @test success(`$(Base.julia_cmd()) --startup-file=no --project=$project --threads=3,0 $coverage $script`)
end
