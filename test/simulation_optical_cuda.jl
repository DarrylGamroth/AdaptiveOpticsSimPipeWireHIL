# Optional real CUDA capture test. No core, service, or device control is opened.
using CUDA, Proper
CUDA.allowscalar(false)
include("simulation_optical.jl")

module OpticalSimulationCUDATests
using Test, CUDA
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil, PipeWireHILError
using AdaptiveOpticsSim.AlgorithmGraphs
using AdaptiveOpticsSim.Backends:
    AcceleratorComputeDevice, CUDABackend, HostComputeDevice, compute_device
using ..OpticalSimulationTests: bundle, clocked_bundle, add_response!, graph_file, save

function image_after_sample(prepared)
    images = []
    step_external_pupil!(
        (name, image, metadata) -> push!(images, Array(image)),
        prepared.science.plant,
        zeros(Float32, 4, 4),
        ones(Float32, 4, 4),
        ModelTimestamp(0),
        ModelDuration(4);
        domain = ntuple(UInt8, 16),
        generation = UInt64(1),
        sequence = UInt64(1),
        lost_samples = UInt64(0),
    )
    return only(images)
end

@testset "standalone CUDA optics use captured device bindings and host source" begin
    @test CUDA.functional()
    target = AcceleratorComputeDevice(CUDABackend(), 0)
    mktempdir() do directory
        product = bundle(directory)
        product.path["sensor"]["properties"]["optics.execution"] = "stale"
        configuration = Simulation.load(save(product))
        @test_throws PipeWireHILError Simulation.prepare(
            configuration; target, execution = StreamGraphExecution()
        )
        cpu = Simulation.prepare(configuration)
        gpu = Simulation.prepare(configuration; target)
        @test graph_execution_policy(gpu.science.plant.graphs.camera) ===
            CapturedGraphExecution()
        @test compute_device(graph_input(gpu.science.plant.graphs.camera, :pupil_opd)) ==
            target
        @test gpu.science.sensors.camera.properties["optics.execution"] ==
            "CapturedGraphExecution"
        @test parse(
            Int, gpu.science.sensors.camera.properties["optics.captured-node-count"]
        ) > 0
        @test image_after_sample(gpu) ≈ image_after_sample(cpu) rtol = 1e-5
    end
    mktempdir() do directory
        product = clocked_bundle(directory)
        response = add_response!(product)
        response["nodes"][1]["config"]["photon_noise"] = false
        descriptor = product.root["graphs"]["response"]
        product.root["graphs"]["response"] = graph_file(
            directory, "response", response, descriptor["bindings"]
        )
        prepared = Simulation.prepare(Simulation.load(save(product)); target)
        @test compute_device(prepared.science.source.graph) == HostComputeDevice()
        @test graph_execution_policy(prepared.science.source.graph) ===
            StreamGraphExecution()
        @test prepared.science.sensors.camera.properties["detector.execution"] ==
            "CapturedGraphExecution"
        @test parse(
            Int, prepared.science.sensors.camera.properties["detector.captured-node-count"]
        ) > 0
        @test model_time_sequence(prepared.science.source_driver) == 0
        @test all(iszero, prepared.execution.command)
    end
end
end
