# Run with --threads=3,1 and a matching PIPEWIREAO_HIL_TEST_PREFIX.
# Owns its isolated private core; creates no user-session links or controls.
using Proper
isdefined(@__MODULE__, :OpticalSimulationTests) || include("simulation_optical.jl")

module OpticalSimulationPrivateCoreTests
using Test, PipeWireAO, PipeWireAO_jll
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil
using ..OpticalSimulationTests: clocked_bundle, save
include("private_core_helpers.jl")

@testset "standalone finite optical transport closes on completion and controls" begin
    _with_private_core() do remote
        mktempdir() do directory
            product = clocked_bundle(directory)
            product.root["pipewire"]["remote"] = remote
            configuration = Simulation.load(save(product))
            for mode in (:run, :stop, :fail)
                prepared = Simulation.prepare(configuration)
                opened = Ref{Any}(nothing)
                duties = Ref(0)
                control = function (execution, streams, now_ns)
                    opened[] = streams
                    duties[] += 1
                    mode === :fail && error("optical control failed")
                    mode === :stop && return ExternalPupil.Stop()
                    if duties[] == 1
                        return ExternalPupil.Hold()
                    elseif duties[] == 3
                        ExternalPupil.reset!(execution, streams)
                    end
                    ExternalPupil.Run(0)
                end
                if mode === :fail
                    @test_throws ErrorException Simulation.run!(
                        control, prepared; frames = 2
                    )
                else
                    completed = Simulation.run!(control, prepared; frames = 2)
                    @test completed == (mode === :run ? 2 : 0)
                    @test duties[] == (mode === :run ? 3 : 1)
                end
                @test opened[].closed
                @test !isopen(opened[].core)
            end
        end
    end
end
end
