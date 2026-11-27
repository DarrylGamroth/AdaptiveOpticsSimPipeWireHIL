using Test, AdaptiveOpticsSimPipeWireHIL, AdaptiveOpticsSim, PipeWireAO
using AdaptiveOpticsSim.AlgorithmGraphs
import Clocks
const OpticalExecution = AdaptiveOpticsSimPipeWireHIL.ExternalPupil

# Tiny transport-neutral AOS nodes exercise the real graph/model/exposure owners.
# Transport doubles keep these runner tests independent of a PipeWire daemon.
struct RunnerOpticsNode end
struct RunnerOpticsOwner{I,O}
    inputs::I
    output::O
end
function AlgorithmGraphs.graph_node_ports(::Type{RunnerOpticsNode}, config)
    port(name, direction) = AlgorithmGraphs.graph_port_contract(
        name, direction, :data, Float32, (2, 2), "test.runner.pupil/1", :column_major
    )
    return (
        port(:pupil_opd, :input),
        port(:pupil_amplitude, :input),
        port(:dm_opd, :input),
        port(:output, :output),
    )
end
AlgorithmGraphs.prepare_graph_node(
    ::Type{RunnerOpticsNode},
    config,
    props,
    inputs::NamedTuple,
    outputs::NamedTuple,
    parameters::NamedTuple,
    target,
) = RunnerOpticsOwner(inputs, outputs.output)
AlgorithmGraphs.step_graph_node!(owner::RunnerOpticsOwner) =
    (owner.output .= owner.inputs.pupil_amplitude .+ owner.inputs.dm_opd; nothing)
AlgorithmGraphs.reset_graph_node!(::RunnerOpticsOwner) = nothing
AlgorithmGraphs.optical_sampling_m(::RunnerOpticsOwner) = 1.0

struct RunnerSourceNode end
struct RunnerSourceOwner{I,O}
    inputs::I
    outputs::O
end
function AlgorithmGraphs.graph_node_ports(::Type{RunnerSourceNode}, config)
    return (
        AlgorithmGraphs.graph_port_contract(
            :command, :input, :data, Float32, (1,), "test.runner.command/1", :column_major
        ),
        AlgorithmGraphs.graph_port_contract(
            :pupil_opd,
            :output,
            :data,
            Float32,
            (2, 2),
            "test.runner.pupil/1",
            :column_major,
        ),
        AlgorithmGraphs.graph_port_contract(
            :dm_opd, :output, :data, Float32, (2, 2), "test.runner.pupil/1", :column_major
        ),
    )
end
AlgorithmGraphs.prepare_graph_node(
    ::Type{RunnerSourceNode},
    config,
    props,
    inputs::NamedTuple,
    outputs::NamedTuple,
    parameters::NamedTuple,
    target,
) = RunnerSourceOwner(inputs, outputs)
function AlgorithmGraphs.step_graph_node!(owner::RunnerSourceOwner)
    fill!(owner.outputs.pupil_opd, 0)
    fill!(owner.outputs.dm_opd, only(owner.inputs.command))
    return nothing
end
AlgorithmGraphs.reset_graph_node!(::RunnerSourceOwner) = nothing

function runner_science(; duration_ns = 2)
    node = algorithm_node(:optics, RunnerOpticsNode, nothing)
    graph = prepare_algorithm_graph(
        algorithm_graph(
            (node,);
            inputs = (
                graph_input(:pupil_opd, :optics => :pupil_opd, zeros(Float32, 2, 2)),
                graph_input(
                    :pupil_amplitude, :optics => :pupil_amplitude, ones(Float32, 2, 2)
                ),
                graph_input(:dm_opd, :optics => :dm_opd, zeros(Float32, 2, 2)),
            ),
            outputs = (graph_output(:intensity, :optics => :output),),
        ),
    )
    plant = prepare_external_pupil(
        (camera = graph,);
        exposure_durations = (camera = ModelDuration(duration_ns),),
        observations = (camera = (; output = :intensity, sampling_node = :optics),),
    )
    return (;
        plant,
        chopping = (;),
        pupil = (;
            shape = (2, 2, 2),
            schema = "test.runner.pupil/1",
            properties = Dict{String,String}(),
        ),
        sensors = (
            camera = (;
                image = plant.exposures.camera.image,
                schema = "test.runner.camera/1",
                properties = Dict{String,String}(),
            ),
        ),
    )
end
function runner_clocked_science()
    node = algorithm_node(:source, RunnerSourceNode, nothing)
    graph = prepare_algorithm_graph(
        algorithm_graph(
            (node,);
            inputs = (graph_input(:command, :source => :command, zeros(Float32, 1)),),
            outputs = (
                graph_output(:pupil_opd, :source => :pupil_opd),
                graph_output(:dm_opd, :source => :dm_opd),
            ),
        ),
    )
    return OpticalExecution.prepare_clocked_source(
        runner_science();
        source = (; graph),
        command_input = :command,
        amplitude = ones(Float32, 2, 2),
        pupil_axes = (2, 1),
        period_ns = 2,
    )
end

mutable struct RunnerTransport
    streams::Tuple{Nothing,Nothing}
    inputs::Vector{Tuple{Array{Float32},PipeWireArrayReceipt}}
    events::Vector{Any}
    resets::Vector{Int}
    failed::Bool
end
RunnerTransport(inputs = Tuple{Array{Float32},PipeWireArrayReceipt}[]) =
    RunnerTransport((nothing, nothing), inputs, Any[], Int[], false)
AdaptiveOpticsSimPipeWireHIL.with_array_streams(f, options::Tuple{RunnerTransport,Nothing}; kwargs...) =
    f(first(options))
function AdaptiveOpticsSimPipeWireHIL.drain_pipewire_array!(
    buffer, transport::RunnerTransport, index
)
    isempty(transport.inputs) && return nothing
    values, receipt = popfirst!(transport.inputs)
    copyto!(buffer, values)
    return receipt
end
function AdaptiveOpticsSimPipeWireHIL.stage_pipewire_array!(
    transport::RunnerTransport, index, image; kwargs...
)
    push!(transport.events, (; index, image = copy(image), metadata = (; kwargs...)))
    return true
end
PipeWireAO.trigger_process!(transport::RunnerTransport) =
    (push!(transport.events, :trigger); nothing)
AdaptiveOpticsSimPipeWireHIL.array_stream_status(transport::RunnerTransport, index) =
    (; failed = transport.failed)
AdaptiveOpticsSimPipeWireHIL.reset_pipewire_array!(transport::RunnerTransport, index) =
    (push!(transport.resets, index); nothing)
mutable struct RunnerClock <: Clocks.AbstractNanoClock
    values::Vector{Int64}
    samples::Int
end
RunnerClock(values) = RunnerClock(Int64[values...], 0)
Clocks.time_nanos(clock::RunnerClock) = (clock.samples += 1; clock.values[clock.samples])
function runner_receipt(sequence; generation = 1, pts = 2(sequence - 1), duration = 2)
    return PipeWireArrayReceipt(
        Int64(pts),
        UInt64(duration),
        AcquisitionIdentity(
            AcquisitionDomain(ntuple(UInt8, 16)), UInt64(generation), UInt64(sequence)
        ),
        UInt64(0),
    )
end
function pupil_packet()
    packet = zeros(Float32, 2, 2, 2)
    packet[2, :, :] .= 1
    return packet
end

@testset "external-pupil preparation, warmup and owned emissions" begin
    execution = OpticalExecution.prepare(runner_science())
    checks = Ref(0)
    OpticalExecution.warmup!(execution; check = () -> (checks[] += 1))
    @test checks[] == 2
    @test execution.science.plant.state.sequence == 0
    @test execution.science.plant.state.domain === nothing
    @test all(iszero, execution.packet)
    @test all(buffer -> all(iszero, buffer), execution.buffers)
    @test_throws InterruptException OpticalExecution.warmup!(
        execution; check = () -> throw(InterruptException())
    )
    @test execution.science.plant.state.sequence == 0
    @test all(iszero, execution.packet)
    transport = RunnerTransport()
    OpticalExecution.prepare_transport!(execution, transport)
    @test isempty(transport.events)
    publisher = OpticalExecution.ExposurePublisher(transport)
    metadata = (; sequence = UInt64(1), pts = Int64(0), duration = UInt64(2))
    image = fill(3.0, 2, 2)
    OpticalExecution.publish_exposures!(publisher) do
        OpticalExecution.emitter(execution, publisher)(:camera, image, metadata)
    end
    fill!(image, 99)
    @test transport.events[1].image == fill(3.0f0, 2, 2)
    @test eltype(transport.events[1].image) === Float32
    @test transport.events[1].metadata.identity.generation == 1
    @test last(transport.events) === :trigger
    options = OpticalExecution.stream_options(
        execution.science, "pupil", (camera = "camera",)
    )
    @test options[1].format.shape == (2, 2, 2)
    @test options[2].format.rate.num == 500_000_000
    @test options[2].format.rate.denom == 1
end

@testset "external-pupil Run Hold Stop and upstream generation ownership" begin
    execution = OpticalExecution.prepare(runner_science())
    packet = pupil_packet()
    transport = RunnerTransport([
        (packet, runner_receipt(1)),
        (packet, runner_receipt(2)),
        (packet, runner_receipt(1; generation = 2)),
    ])
    decisions = (
        OpticalExecution.Hold(),
        OpticalExecution.Run(0),
        OpticalExecution.Run(0),
        OpticalExecution.Stop(),
    )
    duties = Ref(0)
    clock = RunnerClock((10, 20, 30, 40))
    runner = AdaptiveOpticsSimPipeWireHIL.SimRunner
    withenv("PIPEWIREAO_SIM_IDLE_STRATEGY" => "busy-spin") do
        OpticalExecution.run!(execution, transport; clock) do current, streams, now
            duties[] += 1
            @test current === execution && streams === transport
            @test now == duties[] * 10
            @test runner.name(runner.current_strategy()) == "busy-spin"
            decisions[duties[]]
        end
    end
    @test clock.samples == 4
    @test isempty(transport.inputs)
    @test OpticalExecution.snapshot(execution).sequence == 2
    @test OpticalExecution.snapshot(execution).generation == 2
    @test OpticalExecution.snapshot(execution).upstream_generation == 2
    @test count(==(:trigger), transport.events) == 2
    OpticalExecution.reset!(execution, transport)
    @test OpticalExecution.snapshot(execution).generation == 3
    @test OpticalExecution.snapshot(execution).sequence == 0
    @test OpticalExecution.snapshot(execution).deadline_ns == 0
    @test OpticalExecution.snapshot(execution).upstream_generation == 0
    @test transport.resets == [1, 2]
    @test execution.science.plant.state.sequence == 0
end

@testset "clocked commands remain held and pacing follows fixed releases" begin
    science = runner_clocked_science()
    @test_throws DimensionMismatch OpticalExecution.prepare(
        science; clocked = true, command_count = 2
    )
    execution = OpticalExecution.prepare(
        science;
        clocked = true,
        command_count = 1,
        command_limit = 1.0f-6,
        pacing_period_ns = 5,
    )
    OpticalExecution.warmup!(execution)
    @test model_time_sequence(science.source_driver) == 0
    @test all(iszero, science.source_command)
    transport = RunnerTransport([(Float32[1.0f-7], runner_receipt(1))])
    OpticalExecution.prepare_transport!(execution, transport)
    @test isempty(transport.events)
    @test model_time_sequence(science.source_driver) == 0
    duties = Ref(0)
    clock = RunnerClock((10, 20, 26, 30, 40))
    adopted = []
    runner = AdaptiveOpticsSimPipeWireHIL.SimRunner
    idle_strategy = runner.strategy("busy-spin")
    observe_applied = (command, sequence, pts) -> begin
        @test runner.current_strategy() === idle_strategy
        push!(adopted, (copy(command), sequence, pts))
    end
    OpticalExecution.run!(execution, transport; clock, observe_applied, idle_strategy) do current, streams, now
        duties[] += 1
        if duties[] == 1
            return OpticalExecution.Hold()
        elseif duties[] == 2
            @test current.command == Float32[1.0f-7]
            return OpticalExecution.Run(0)
        elseif duties[] == 3
            @test OpticalExecution.snapshot(current).sequence == 1
            @test OpticalExecution.snapshot(current).deadline_ns == 30
            return OpticalExecution.Hold()
        end
        @test OpticalExecution.snapshot(current).deadline_ns == 0
        OpticalExecution.Stop()
    end
    @test clock.samples == 5
    @test adopted == [(Float32[1.0f-7], UInt64(1), Int64(26))]
    @test count(==(:trigger), transport.events) == 1
    @test science.source_command == Float32[1.0f-7]
    @test graph_output(science.source.graph, :dm_opd) == fill(1.0f-7, 2, 2)
    @test graph_output(science.source.graph, :pupil_opd) == zeros(Float32, 2, 2)
    @test model_time_sequence(science.source_driver) == 1
    OpticalExecution.reset!(execution, transport)
    @test OpticalExecution.snapshot(execution) == (;
        generation = Int64(2),
        sequence = UInt64(0),
        deadline_ns = Int64(0),
        upstream_generation = UInt64(0),
    )
    @test all(iszero, science.source_command) && all(iszero, execution.command)
    @test model_time_sequence(science.source_driver) == 0
    bad = RunnerTransport([(Float32[Inf], runner_receipt(1))])
    @test_throws ArgumentError OpticalExecution.run!(
        (args...) -> OpticalExecution.Hold(), execution, bad; clock = RunnerClock((50,))
    )
end

@testset "runner refuses clock regression and transport failure" begin
    execution = OpticalExecution.prepare(runner_science())
    @test_throws ArgumentError OpticalExecution.run!(
        (args...) -> OpticalExecution.Hold(),
        execution,
        RunnerTransport();
        clock = RunnerClock((20, 10)),
    )
    execution = OpticalExecution.prepare(runner_science())
    transport = RunnerTransport()
    transport.failed = true
    @test_throws ErrorException OpticalExecution.run!(
        (args...) -> OpticalExecution.Run(0),
        execution,
        transport;
        clock = RunnerClock((20,)),
    )
    @test !any(
        id -> id.name in ("Proper", "CUDA", "AMDGPU", "PipeWireAORTC"),
        keys(Base.loaded_modules),
    )
end

@testset "clocked pacing waits without advancing model time" begin
    execution = OpticalExecution.prepare(
        runner_clocked_science();
        clocked = true,
        command_count = 1,
        command_limit = 1.0f-6,
        pacing_period_ns = 5,
    )
    transport = RunnerTransport([(Float32[1.0f-7], runner_receipt(1))])
    clock = RunnerClock((10, 20, 30))
    duties = Ref(0)
    runner = AdaptiveOpticsSimPipeWireHIL.SimRunner
    idle_strategy = runner.strategy("busy-spin")
    owner = current_task()
    OpticalExecution.run!(execution, transport; clock, idle_strategy) do current, streams, now
        @test current_task() === owner
        @test runner.current_strategy() === idle_strategy
        duties[] += 1
        duties[] < 3 ? OpticalExecution.Run(100) : OpticalExecution.Stop()
    end
    @test clock.samples == 3
    @test execution.command == Float32[1.0f-7]
    @test OpticalExecution.snapshot(execution).deadline_ns == 100
    @test OpticalExecution.snapshot(execution).sequence == 0
    @test model_time_sequence(execution.science.source_driver) == 0
    @test isempty(transport.events)
    excessive = RunnerTransport([(Float32[2.0f-6], runner_receipt(2))])
    @test_throws ArgumentError OpticalExecution.run!(
        (args...) -> OpticalExecution.Run(0),
        execution,
        excessive;
        clock = RunnerClock((40,)),
    )
    @test execution.command == Float32[1.0f-7]
    @test isempty(excessive.events)
end

@testset "optical phase and electrical mapping remain independent" begin
    science = (; chopping = (camera = (; trigger_high_is_fringed = false),))
    metadata = (;
        sequence = UInt64(99),
        pts = Int64(8),
        duration = UInt64(4),
        chopped_image_kind = :fringed,
    )
    @test OpticalExecution.sensor_sequence(metadata, true) == 3
    @test OpticalExecution.trigger_state(science, :camera, metadata) ===
        ACQUISITION_TRIGGER_LOW
    @test OpticalExecution.sensor_optical_classification(science, :camera, metadata) ===
        OPTICAL_FRINGED
    @test OpticalExecution.sensor_sequence(metadata, false) == 99
    @test_throws ArgumentError OpticalExecution.sensor_sequence(
        merge(metadata, (; pts = Int64(7))), true
    )
    @test_throws ArgumentError OpticalExecution.trigger_state(
        science, :camera, merge(metadata, (; chopped_image_kind = :unknown))
    )
end

@testset "standalone optical budget follows source progress through controls" begin
    simulation = AdaptiveOpticsSimPipeWireHIL.Simulation
    runner = AdaptiveOpticsSimPipeWireHIL.SimRunner
    # An accepted upstream generation change advances the finite budget.
    execution = OpticalExecution.prepare(runner_science())
    packet = zeros(Float32, 2, 2, 2)
    packet[2, :, :] .= 1
    transport = RunnerTransport([
        (copy(packet), runner_receipt(1)),
        (copy(packet), runner_receipt(1; generation = 2, pts = 0)),
        (copy(packet), runner_receipt(2; generation = 2, pts = 2)),
    ])
    prepared = simulation.PreparedOptical(execution.science, execution, (transport, nothing), "mock")
    duties = Ref(0)
    count = withenv("PIPEWIREAO_SIM_IDLE_STRATEGY" => "busy-spin") do
        simulation.run!(prepared; frames = 2, clock = RunnerClock((10, 20, 30))) do current, streams, now
            @test runner.name(runner.current_strategy()) == "busy-spin"
            duties[] += 1
            OpticalExecution.Run(0)
        end
    end
    @test count == 2
    @test duties[] == 2
    @test length(transport.inputs) == 1
    # Hold and a serialized reset consume no source budget.
    execution = OpticalExecution.prepare(
        runner_clocked_science(); clocked = true, command_count = 1, pacing_period_ns = 5
    )
    transport = RunnerTransport()
    duties[] = 0
    count = simulation.run_optical!(
        execution, transport, 2; clock = RunnerClock((10, 20, 30, 40, 50, 60))
    ) do current, streams, now
        duties[] += 1
        if duties[] == 1
            return OpticalExecution.Hold()
        elseif duties[] == 3
            OpticalExecution.reset!(current, streams)
        end
        OpticalExecution.Run(0)
    end
    @test count == 2
    @test duties[] == 3
    @test execution.generation == 2
end

@testset "external pupil stream declarations share the selected reference clock" begin
    science = runner_science()
    for reference_clock in
        (METADATA_CLOCK_REALTIME, METADATA_CLOCK_MONOTONIC, METADATA_CLOCK_TAI)
        options = OpticalExecution.stream_options(
            science, "pupil", (; camera = "camera"); reference_clock
        )
        @test all(option -> option.reference_clock === reference_clock, options)
    end
end

@testset "fixed optical release schedule" begin
    next_release = AdaptiveOpticsSimPipeWireHIL.ExternalPupil.next_release
    @test next_release(100, 101, 107, 10) == 110
    @test next_release(100, 101, 110, 10) == 120
    @test next_release(100, 101, 126, 10) == 130
    @test next_release(0, 101, 107, 10) == 111
    next_release(100, 101, 107, 10)
    @test Base.allocated(next_release, 100, 101, 107, 10) == 0
end
