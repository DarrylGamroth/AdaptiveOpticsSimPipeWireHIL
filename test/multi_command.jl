using AdaptiveOpticsSim: AlgorithmGraphs
using AdaptiveOpticsSim.AlgorithmGraphs
using AdaptiveOpticsSim.AlgorithmGraphs: graph_port_contract, reset_hil_boundary!

# Transport-only graph: distinct command lengths make accidental flattening,
# port swaps and incomplete adoption observable without an instrument model.
struct MultiCommandTestDeclaration end
struct MultiCommandTestOwner{Inputs,Frame}
    inputs::Inputs
    frame::Frame
end
function AlgorithmGraphs.graph_node_ports(::Type{MultiCommandTestDeclaration}, tiptilt_count::Int)
    return (
        graph_port_contract(:woofer, :input, :data, Float32, (2,), "test.command/1", :column_major),
        graph_port_contract(:tweeter, :input, :data, Float32, (3,), "test.command/1", :column_major),
        graph_port_contract(:tiptilt, :input, :data, Float32, (tiptilt_count,), "test.command/1", :column_major),
        graph_port_contract(:frame, :output, :data, Float32, (3,), "test.frame/1", :column_major),
    )
end
function AlgorithmGraphs.prepare_graph_node(::Type{MultiCommandTestDeclaration}, tiptilt_count::Int,
        props, inputs::NamedTuple, outputs::NamedTuple, parameters::NamedTuple, target)
    return MultiCommandTestOwner(inputs, outputs.frame)
end
function AlgorithmGraphs.step_graph_node!(owner::MultiCommandTestOwner)
    owner.frame[1] = sum(owner.inputs.woofer)
    owner.frame[2] = sum(owner.inputs.tweeter)
    owner.frame[3] = sum(owner.inputs.tiptilt)
    return nothing
end
AlgorithmGraphs.reset_graph_node!(::MultiCommandTestOwner) = nothing

function multi_command_boundary(; tiptilt_count=1, calibration=false)
    node = algorithm_node(:plant, MultiCommandTestDeclaration, tiptilt_count)
    graph = prepare_algorithm_graph(algorithm_graph((node,);
        inputs=(graph_input(:woofer, :plant => :woofer, fill(10f0, 2)),
            graph_input(:tweeter, :plant => :tweeter, fill(20f0, 3)),
            graph_input(:tiptilt, :plant => :tiptilt, fill(30f0, tiptilt_count))),
        outputs=(graph_output(:frame, :plant => :frame),)))
    prepare = calibration ? prepare_graph_calibration_boundary : prepare_graph_hil_boundary
    return prepare(graph;
        command_input=(:woofer, :tweeter, :tiptilt), frame_output=:frame)
end
function multi_command_configuration(; remote="unused-private-core", kwargs...)
    return PipeWireHILConfiguration(; remote, frame_node_name="multi-frame",
        command_node_name=(tiptilt="multi-tiptilt", woofer="multi-woofer", tweeter="multi-tweeter"),
        frame_schema="test.frame/1",
        command_schema=(tweeter="test.command/1", tiptilt="test.command/1", woofer="test.command/1"),
        command_scale=(woofer=2f0, tweeter=3f0, tiptilt=4f0),
        rate=SPA.Fraction(1000, 1), exposure_duration_ns=1_000_000, kwargs...)
end
function outstanding_multi_command(; frame_sequence=1)
    adapter = AdaptiveOpticsSimPipeWireHIL
    boundary = multi_command_boundary()
    configuration = multi_command_configuration()
    states = adapter._prepare_exchange_state(hil_frame_buffer(boundary),
        hil_command_buffer(boundary), configuration)
    sequence = step_hil_frame!(boundary)
    for index in 2:frame_sequence
        adopt_hil_command!(boundary, sequence)
        sequence = step_hil_frame!(boundary)
    end
    source = adapter._frame_state(states)
    source.frame_ready[] = sequence
    source.frame_published[] = sequence
    source.exposure_record = PipeWireExposureRecord(sequence, configuration.exposure_duration_ns,
        ACQUISITION_TRIGGER_ABSENT)
    adapter._stage_command_context!(states)
    adapter._record_frame_publication!(source, sequence, Int64(100))
    return (; boundary, configuration, states, sequence)
end

@testset "free-running groups retain and adopt complete acquisitions" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    boundary = multi_command_boundary(; calibration=true)
    states = adapter._prepare_exchange_state(hil_frame_buffer(boundary),
        hil_command_buffer(boundary), multi_command_configuration())
    acquisition = adapter._prepare_free_running(boundary, states)
    source = adapter._frame_state(states)
    prepared = (; state=states, boundary, applied_command_source=nothing)
    source.frame_published[] = 2
    function send(port, sequence, value)
        receiver = acquisition.receivers[port]
        identity = AcquisitionIdentity(source.acquisition_domain, 1, sequence)
        @test adapter._accept_array!(receiver, fill(Float32(value), length(receiver.values)),
            identity, Int64(sequence), source.exposure_duration_ns)
    end
    poll() = adapter._collect_free_running_commands!(acquisition, states)
    send(1, 1, 1)
    @test poll() === nothing
    @test step_hil_exposure!(boundary) == 1
    @test hil_frame_buffer(boundary) == [20f0, 60f0, 30f0]
    send(1, 2, 2)
    send(2, 1, 1)
    @test poll() === nothing
    send(3, 1, 1)
    adapter._adopt_free_running!(prepared, acquisition, poll())
    @test acquisition.applied_sequence == 1
    @test step_hil_exposure!(boundary) == 2
    @test hil_frame_buffer(boundary) == [4f0, 9f0, 4f0]
    send(3, 2, 2)
    @test poll() === nothing
    send(2, 2, 2)
    adapter._adopt_free_running!(prepared, acquisition, poll())
    @test acquisition.applied_sequence == 2
    @test step_hil_exposure!(boundary) == 3
    @test hil_frame_buffer(boundary) == [8f0, 18f0, 8f0]
end

@testset "named command configuration and admission" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    configuration = multi_command_configuration()
    @test isconcretetype(typeof(configuration))
    @test all(isconcretetype, fieldtypes(typeof(configuration)))
    @test configuration.command_scale.woofer == 2f0
    @test keys(configuration.command_schema) == keys(configuration.command_node_name)
    common = (remote="unused", frame_node_name="frame", frame_schema="test.frame/1",
        rate=SPA.Fraction(1000, 1), exposure_duration_ns=1)
    for extra in (
            (command_node_name=(a="a", b="b"), command_schema=(a="test/1",), command_scale=(a=1f0,b=1f0)),
            (command_node_name=(a="a", b="a"), command_schema=(a="test/1", b="test/1"), command_scale=(a=1f0,b=1f0)),
            (command_node_name=(a="frame",), command_schema=(a="test/1",), command_scale=(a=1f0,)),
            (command_node_name=(a="a",), command_schema=(a="test/1",), command_scale=(a=true,)),
            (command_node_name=(a="a",), command_schema=(a="test/1",), command_scale=(a=Inf,)),
            (command_node_name=(a="a",), command_schema="test/1", command_scale=1f0),
            (command_node_name=NamedTuple(), command_schema=NamedTuple(), command_scale=NamedTuple()))
        @test_throws PipeWireHILError PipeWireHILConfiguration(; common..., extra...)
    end
    @test_throws PipeWireHILError PipeWireHILConfiguration(; common...,
        command_node_name="a", command_schema="test/1", command_scale=true)
    names17 = ntuple(index -> Symbol("port",index),17)
    @test_throws PipeWireHILError PipeWireHILConfiguration(; common...,
        command_node_name=NamedTuple{names17}(ntuple(index -> "command-$index",17)),
        command_schema=NamedTuple{names17}(ntuple(_ -> "test/1",17)),
        command_scale=NamedTuple{names17}(ntuple(_ -> 1f0,17)))
    boundary = multi_command_boundary()
    commands = hil_command_buffer(boundary)
    frame = hil_frame_buffer(boundary)
    @test_throws PipeWireHILError adapter._prepare_exchange_state(frame,
        (woofer=commands.woofer,tweeter=commands.tweeter),configuration)
    @test_throws PipeWireHILError adapter._prepare_exchange_state(frame,
        merge(commands,(tiptilt=commands.woofer,)),configuration)
    @test_throws PipeWireHILError adapter._prepare_exchange_state(frame,
        merge(commands,(tiptilt=frame,)),configuration)
    system = (; graph=boundary.graph, boundary)
    shapes = (tiptilt=(1,), tweeter=(3,), woofer=(2,))
    @test validate_hil_system(system; command_input=(:tweeter,:woofer,:tiptilt),
        frame_output=:frame, command_shape=shapes, frame_shape=(3,)) === nothing
    for (names, command_shape) in (((:woofer,:tweeter), (woofer=(2,),tweeter=(3,))),
            ((:woofer,:tweeter,:tiptilt), merge(shapes, (tiptilt=(2,),))),
            ((:woofer,:woofer,:tiptilt), shapes))
        @test_throws ArgumentError validate_hil_system(system; command_input=names,
            frame_output=:frame, command_shape, frame_shape=(3,))
    end
    allowed = ((frame_encoding=:float32, frame_schema="test.frame/1",
        command_schema=(woofer="test.command/1", tweeter="test.command/1", tiptilt="test.command/1"),
        command_scale=(tiptilt=4.0, tweeter=3.0, woofer=2.0)),)
    @test validate_wire_contract(:float32, "test.frame/1", configuration.command_schema,
        configuration.command_scale, allowed) === nothing
    @test_throws ArgumentError validate_wire_contract(:float32, "test.frame/1",
        configuration.command_schema, (woofer=2f0, tweeter=3f0), allowed)
    @test_throws ArgumentError validate_wire_contract(:float32, "test.frame/1",
        configuration.command_schema, merge(configuration.command_scale, (tiptilt=5f0,)), allowed)
end

@testset "coherent command set adoption and reordered callbacks" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    fixture = outstanding_multi_command()
    (; boundary, states, sequence) = fixture
    initial = map(copy, boundary.command_input)
    @test all(state -> state.frame_transport === states.woofer.frame_transport, values(states))
    receive_test_command!(states.tiptilt, Float32[7], sequence, Int64(120))
    receive_test_command!(states.woofer, Float32[1, 2], sequence, Int64(130))
    @test boundary.command_input == initial
    @test_throws PipeWireHILError adapter._adopt_received_command!(adopt_hil_command!,
        boundary, states, nothing, sequence)
    @test boundary.command_input == initial
    @test_throws PipeWireHILError adapter._await_command_set(() -> nothing, values(states),
        sequence, UInt64(1_000_000))
    @test boundary.command_input == initial
    @test_throws PipeWireHILError adapter._reset_exchange_state!(states)
    @test_throws AlgorithmGraphs.AlgorithmGraphError step_hil_frame!(boundary)
    receive_test_command!(states.tweeter, Float32[3, 4, 5], sequence, Int64(140))
    @test adapter._await_command_set(() -> nothing, values(states), sequence, UInt64(1)) == sequence
    adapter._adopt_received_command!(adopt_hil_command!, boundary, states, nothing, sequence)
    @test boundary.command_input == (woofer=Float32[2, 4], tweeter=Float32[9, 12, 15], tiptilt=Float32[28])
    @test all(state -> state.command_ready[] == 0, values(states))
    @test map(t -> t.sequence, adapter._frame_command_timing(states)) == (woofer=1, tweeter=1, tiptilt=1)
    @test step_hil_frame!(boundary) == 2
    @test hil_frame_buffer(boundary) == Float32[6,36,28]
    # Finish the second boundary step solely to verify reset lifecycle.
    adopt_hil_command!(boundary, UInt64(2))
    reset_hil_boundary!(boundary)
    adapter._reset_exchange_state!(states)
    @test all(state -> state.acquisition_generation == 2, values(states))
    @test all(state -> state.frame_ready[] == state.command_ready[] == state.timing_completed_sequence[] == 0,
        values(states))
    @test all(isnothing, values(adapter._frame_command_timing(states)))
end

@testset "command set rejects duplicate stale wrong and invalid final ports" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    for invalid in (:duplicate, :zero, :stale, :future, :length, :nonfinite, :overflow)
        (; boundary, states, sequence) = outstanding_multi_command(; frame_sequence=2)
        initial = map(copy, boundary.command_input)
        receive_test_command!(states.woofer, Float32[1,2], sequence, Int64(120))
        receive_test_command!(states.tweeter, Float32[3,4,5], sequence, Int64(130))
        target = invalid == :duplicate ? states.woofer : states.tiptilt
        before = copy(target.command)
        values = invalid == :length ? Float32[6,7] : invalid == :nonfinite ? Float32[NaN] :
            invalid == :overflow ? Float32[floatmax(Float32)] : invalid == :duplicate ? Float32[1,2] : Float32[6]
        token = invalid == :zero ? UInt64(0) : invalid == :stale ? sequence-1 : invalid == :future ? sequence+1 : sequence
        @test_throws PipeWireHILError receive_test_command!(target, values, token, Int64(140))
        @test target.command == before
        @test boundary.command_input == initial
        @test_throws PipeWireHILError adapter._adopt_received_command!(adopt_hil_command!, boundary,
            states, nothing, sequence)
        @test boundary.command_input == initial
    end
end

@testset "each named command validates acquisition identity" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    for key in (:woofer,:tweeter,:tiptilt)
        (; boundary, states, sequence) = outstanding_multi_command()
        state = states[key]
        fixture = command_acquisition_fixture(state; sequence, generation=UInt64(8),
            duration=state.exposure_duration_ns)
        GC.@preserve fixture begin
            @test_throws PipeWireHILError adapter._receive_command!(state, ones(Float32,length(state.command)),
                sequence, Int64(120), metadata_records(fixture.buffer))
        end
        @test state.command_ready[] == 0
    end
end

@testset "all named command streams close despite one cleanup error" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    events = Symbol[]
    loop = FakeHILResource(:loop, events; running=true)
    context = FakeHILResource(:context, events)
    core = FakeHILResource(:core, events)
    frame = FakeHILResource(:frame, events)
    streams = (woofer=FakeHILResource(:woofer,events), tweeter=FakeHILResource(:tweeter,events),
        tiptilt=FakeHILResource(:tiptilt,events))
    streams.tweeter.fail_close = true
    errors = adapter._close_pipewire_resources!(loop,context,core,frame,streams)
    @test length(errors) == 1
    @test all(resource -> !isopen(resource), (values(streams)...,frame,core,context,loop))
    @test events == [:lock,:tiptilt,:tweeter,:woofer,:frame,:core,:context,:unlock,:join,:loop]
end

@testset "named adopted observations follow coherent adoption" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    (; boundary, states, sequence) = outstanding_multi_command()
    sources = (woofer=adapter.AppliedCommandState(2), tweeter=adapter.AppliedCommandState(3),
        tiptilt=adapter.AppliedCommandState(1))
    receive_test_command!(states.woofer, Float32[1,2], sequence, Int64(120))
    @test_throws PipeWireHILError adapter._adopt_received_command!(adopt_hil_command!, boundary,
        states, sources, sequence)
    @test all(source -> !source.pending, values(sources))
    receive_test_command!(states.tiptilt, Float32[3], sequence, Int64(130))
    receive_test_command!(states.tweeter, Float32[4,5,6], sequence, Int64(140))
    adapter._adopt_received_command!(adopt_hil_command!, boundary, states, sources, sequence)
    @test all(source -> source.pending && source.sequence == sequence && source.pts == 100, values(sources))
    @test sources.woofer.values == Float32[2,4] ./ 1f-6
    @test sources.tweeter.values == Float32[12,15,18] ./ 1f-6
    @test sources.tiptilt.values == Float32[12] ./ 1f-6
end

@testset "resized named command storage rejects before copy" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    for delta in (-1,1), key in (:woofer,:tweeter,:tiptilt)
        (; boundary, states, sequence) = outstanding_multi_command()
        initial = map(copy,boundary.command_input)
        state = states[key]
        expected_length = length(state.command)
        resize!(state.command,expected_length+delta)
        fill!(state.command,-9f0)
        @test_throws PipeWireHILError receive_test_command!(state,ones(Float32,expected_length),
            sequence,Int64(120))
        @test_throws PipeWireHILError receive_test_command!(state,ones(Float32,length(state.command)),
            sequence,Int64(120))
        @test_throws PipeWireHILError adapter._validate_command_storage(state)
        @test all(==(-9f0),state.command)
        @test state.command_ready[] == 0
        @test boundary.command_input == initial
    end
end

@testset "all named observation contracts validate before native connection" begin
    adapter = AdaptiveOpticsSimPipeWireHIL
    boundary = multi_command_boundary(; tiptilt_count=2)
    configuration = multi_command_configuration(
        command_schema=(woofer=adapter.Schemas.ACTUATOR_COMMAND_OPD_M_V1,
            tweeter=adapter.Schemas.ACTUATOR_COMMAND_OPD_M_V1, tiptilt="external/unknown-f32/1"),
        command_scale=(woofer=1f0,tweeter=1f0,tiptilt=1f0))
    failure = try
        prepare_pipewire_hil(boundary, configuration;
            applied_command_node_name=(woofer="applied-woofer",tweeter="applied-tweeter",tiptilt="applied-tiptilt"))
    catch exception
        exception
    end
    @test failure isa PipeWireHILError
    @test failure.field == "simulator_applied_command"
    @test occursin("declared physical-DM or tip/tilt OPD command schema", failure.message)
    @test AlgorithmGraphs.hil_boundary_status(boundary).frame_sequence == 0
end
