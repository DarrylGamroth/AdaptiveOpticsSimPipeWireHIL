module ManagedContractsTests

using AdaptiveOpticsSimPipeWireHIL, Aqua, Test
using AdaptiveOpticsSim.AlgorithmGraphs: graph_step_sequence, model_time_sequence
import PipeWireAORTC
import ThreadPinning

const Managed = Base.get_extension(AdaptiveOpticsSimPipeWireHIL, :PipeWireAORTCExt)
const Observation = Managed.OpticalObservation

@testset "Aqua for the RTC extension" begin
    @test isempty(Test.detect_ambiguities(Managed; recursive=true))
    Aqua.test_unbound_args(Managed)
    # These hooks belong to the parent package and are implemented by this extension.
    simulation = AdaptiveOpticsSimPipeWireHIL.Simulation
    Aqua.test_piracies(Managed; treat_as_own=[
        simulation.managed_main, simulation.prepare_lockstep_startup, simulation.prepare_optical_startup,
    ])
end

@testset "managed invocation and worker placement" begin
    mktempdir() do directory
        args = [
            "--config", joinpath(directory, "simulation.conf"),
            "--remote", joinpath(directory, "private-core"),
            "--control-node", "sim-control",
            "--bootstrap-node", "sim-bootstrap",
            "--bootstrap-instance", "7",
        ]
        options = Managed.arguments(args)
        @test options.acquisition == "lockstep"
        @test all(isnothing, (options.pipewire_cpu, options.optical_diagnostics_cpu, options.pupil_output_cpu))
        @test options.bootstrap_instance == 7

        owner = Dict("argv" => ["julia"], "environment" => Dict{String,String}())
        process = Dict("leader-cpu" => 2, "workers" =>
            Dict("pipewire" => [3], "optical-diagnostics" => [4], "pupil-output" => [5]))
        PipeWireAORTC.InstrumentExport.owner_scheduling!(Val(:hil), owner, process)
        options = Managed.arguments([args; owner["argv"][end-5:end]])
        @test (options.pipewire_cpu, options.optical_diagnostics_cpu, options.pupil_output_cpu) == (3, 4, 5)
        @test owner["stop-signal"] == "SIGINT"

        options = Managed.arguments([args; "--phase-timing-directory"; directory;
            "--phase-timing-frames"; "12"; "--phase-timing-warmup"; "3"])
        @test options.phase_timing_directory == directory
        @test options.phase_timing_frames == 12
        @test options.phase_timing_warmup == 3

        for suffix in (
            ["--phase-timing-frames", "12"],
            ["--phase-timing-directory", "relative", "--phase-timing-frames", "12"],
            ["--phase-timing-directory", directory, "--phase-timing-frames", "0"],
            ["--optical-diagnostics-cpu", "3", "--pupil-output-cpu", "3"],
            ["--pipewire-cpu", "1"],
            ["--acquisition", "unsupported"],
            ["--idle-strategy", "unsupported"],
            ["--config", "second.conf"],
            ["--unknown", "value"],
            ["--remote"],
        )
            @test_throws ArgumentError Managed.arguments([args; suffix])
        end
    end
end

@testset "managed optical diagnostics retain physical axes and pupil mask" begin
    intensity = Float32[1 2 3; 4 5 6]
    image = zeros(Float32, 3, 2)
    @test Observation.form_focal_image!(image, intensity) === image
    @test image == Float32[3 6; 2 5; 1 4]
    @test_throws DimensionMismatch Observation.form_focal_image!(zeros(Float32, 2, 3), intensity)
    square = ones(Float32, 2, 2)
    @test_throws ArgumentError Observation.form_focal_image!(square, square)

    opd = [1.0 NaN 3.0; 5.0 NaN 7.0] .* 1e-9
    pupil = Bool[1 0 1; 1 0 1]
    @test Observation.form_wavefront!(image, opd, pupil) === image
    @test image[[1, 3], :] ≈ Float32[-1 3; -3 1]
    @test all(isnan, image[2, :])
    @test Observation.pupil_rms_nm(opd, pupil) ≈ sqrt(5f0)
    @test_throws ArgumentError Observation.form_wavefront!(image, opd, trues(2, 3))
    @test_throws ArgumentError Observation.pupil_rms_nm(opd, falses(2, 3))
    @test_throws DimensionMismatch Observation.pupil_rms_nm(opd, trues(3, 2))
end

@testset "managed lockstep aligns named receipts and skips elapsed pacing periods" begin
    timing(seq, published, received) =
        PipeWireFrameCommandTiming(UInt64(seq), Int64(published), Int64(received))
    receipts = (; fast=timing(4, 10, 12), slow=timing(4, 10, 24))
    @test Managed.completed_frame_timing(receipts) == receipts.slow
    @test_throws "differ from their common source frame" Managed.completed_frame_timing(
        (; receipts.fast, slow=timing(5, 10, 24)))
    @test_throws "differ from their common source frame" Managed.completed_frame_timing(
        (; receipts.fast, slow=timing(4, 11, 24)))

    @test Managed.next_deadline(Int64(-20), Int64(10), Int64(-11)) == -10
    @test Managed.next_deadline(Int64(-20), Int64(10), Int64(-10)) == 0
    @test Managed.next_deadline(Int64(100), Int64(10), Int64(135)) == 140
    @test_throws ArgumentError Managed.next_deadline(Int64(0), Int64(0), Int64(0))
    @test_throws OverflowError Managed.next_deadline(typemax(Int64), Int64(1), typemax(Int64))

    commands = (; dm=Float32[1, 2], steering=Float32[3])
    @test Managed.validate_command_buffers(commands, (; steering=1, dm=2)) === nothing
    @test_throws DimensionMismatch Managed.validate_command_buffers(commands, (; dm=2, steering=2))
    @test_throws ArgumentError Managed.validate_command_buffers(commands, (; dm=2, other=1))
    Managed.zero_command_buffers!(commands)
    @test all(iszero, commands.dm) && all(iszero, commands.steering)
    @test Managed.applied_command_names((; dm="command", steering="steering"), true) ==
        (; dm="command-applied", steering="steering-applied")
end

include("synthetic_graphs.jl")
using .SyntheticGraphs: pupil_source_documents

@testset "managed pupil source warmup restores model and command state" begin
    mktempdir() do directory
        (; config) = pupil_source_documents(directory)
        options = PipeWireAORTC.AOSOwner.configuration(config)
        science = Managed.PupilSource.prepare(options)
        publisher = science.publisher
        try
            @test model_time_sequence(science.driver) == graph_step_sequence(science.graph) == 0
            @test all(iszero, science.command)
            @test all(iszero, view(publisher.packet, 1, :, :))
            @test view(publisher.packet, 2, :, :) == publisher.amplitude
            status = Managed.PupilOutput.status(publisher)
            @test !status.failed
            @test status.completed == status.dropped == 0
            @test status.generation == 1
        finally
            Managed.PupilOutput.close!(publisher)
        end
    end
end

end
