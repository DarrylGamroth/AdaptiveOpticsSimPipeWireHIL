using AdaptiveOpticsSim: AlgorithmGraphs
using AdaptiveOpticsSimPipeWireHIL: validate_hil_system, validate_wire_contract
using AdaptiveOpticsSim.AlgorithmGraphs: algorithm_graph, algorithm_node
using AdaptiveOpticsSim.AlgorithmGraphs: graph_input, graph_output
using AdaptiveOpticsSim.AlgorithmGraphs: prepare_algorithm_graph, prepare_graph_hil_boundary
using AdaptiveOpticsSim.AlgorithmGraphs: hil_command_buffer, hil_frame_buffer

struct ValidationTestDeclaration end

struct ValidationTestOwner{CommandA,CommandB,FrameA,FrameB}
    command_a::CommandA
    command_b::CommandB
    frame_a::FrameA
    frame_b::FrameB
end

function AlgorithmGraphs.graph_node_ports(
    ::Type{ValidationTestDeclaration}, types::Tuple{DataType,DataType},
)
    command_type, frame_type = types
    return (
        AlgorithmGraphs.graph_port_contract(:command_a, :input, :data, command_type, (2,), "test.command-a/1", :column_major),
        AlgorithmGraphs.graph_port_contract(:command_b, :input, :data, command_type, (2,), "test.command-b/1", :column_major),
        AlgorithmGraphs.graph_port_contract(:frame_a, :output, :data, frame_type, (2,), "test.frame-a/1", :column_major),
        AlgorithmGraphs.graph_port_contract(:frame_b, :output, :data, frame_type, (2,), "test.frame-b/1", :column_major),
    )
end

function AlgorithmGraphs.prepare_graph_node(
    ::Type{ValidationTestDeclaration}, types::Tuple{DataType,DataType}, props,
    inputs::NamedTuple, outputs::NamedTuple, parameters::NamedTuple, target,
)
    return ValidationTestOwner(inputs.command_a, inputs.command_b,
        outputs.frame_a, outputs.frame_b)
end

function _validation_test_system(command_type::Type=Float32, frame_type::Type=Float32;
    command=:command_a, frame=:frame_a)
    node = algorithm_node(:plant, ValidationTestDeclaration, (command_type, frame_type))
    graph = prepare_algorithm_graph(algorithm_graph(
        (node,);
        inputs=(
            graph_input(:command_a, :plant => :command_a, zeros(command_type, 2)),
            graph_input(:command_b, :plant => :command_b, zeros(command_type, 2)),
        ),
        outputs=(
            graph_output(:frame_a, :plant => :frame_a),
            graph_output(:frame_b, :plant => :frame_b),
        ),
    ))
    boundary = prepare_graph_hil_boundary(graph;
        command_input=command, frame_output=frame)
    return (; graph, boundary)
end

@testset "cold HIL admission validation" begin
    system = _validation_test_system()
    command = hil_command_buffer(system.boundary)
    frame = hil_frame_buffer(system.boundary)
    fill!(command, 3.0f0)
    fill!(frame, 5.0f0)
    command_before = copy(command)
    frame_before = copy(frame)
    @test validate_hil_system(system;
        command_input=:command_a, frame_output=:frame_a,
        command_shape=(2,), frame_shape=(2,)) === nothing
    @test command == command_before
    @test frame == frame_before

    @test validate_hil_system(_validation_test_system(Float32, UInt16);
        command_input=:command_a, frame_output=:frame_a,
        command_shape=(2,), frame_shape=(2,)) === nothing

    other = _validation_test_system()
    other_command = hil_command_buffer(other.boundary)
    other_frame = hil_frame_buffer(other.boundary)
    other_command_before = copy(other_command)
    other_frame_before = copy(other_frame)
    @test_throws ArgumentError validate_hil_system((; graph=system.graph, boundary=other.boundary);
        command_input=:command_a, frame_output=:frame_a,
        command_shape=(2,), frame_shape=(2,))
    @test_throws ArgumentError validate_hil_system(system;
        command_input=:command_b, frame_output=:frame_a,
        command_shape=(2,), frame_shape=(2,))
    @test_throws ArgumentError validate_hil_system(system;
        command_input=:command_a, frame_output=:frame_b,
        command_shape=(2,), frame_shape=(2,))
    @test_throws ArgumentError validate_hil_system(system;
        command_input=:command_a, frame_output=:frame_a,
        command_shape=(3,), frame_shape=(2,))
    @test_throws ArgumentError validate_hil_system(system;
        command_input=:command_a, frame_output=:frame_a,
        command_shape=(2,), frame_shape=(3,))
    @test_throws ArgumentError validate_hil_system(_validation_test_system(Float64, Float32);
        command_input=:command_a, frame_output=:frame_a,
        command_shape=(2,), frame_shape=(2,))
    @test_throws ArgumentError validate_hil_system(_validation_test_system(Float32, Float64);
        command_input=:command_a, frame_output=:frame_a,
        command_shape=(2,), frame_shape=(2,))
    @test command == command_before
    @test frame == frame_before
    @test other_command == other_command_before
    @test other_frame == other_frame_before

    allowed = ((
        frame_encoding=:uint16,
        frame_schema="test.frame.u16/1",
        command_schema="test.command.f32/1",
        command_scale=1.0f-6,
    ), (
        frame_encoding=:float32,
        frame_schema="test.frame.f32/1",
        command_schema="test.command.f32/1",
        command_scale=1.0f0,
    ))
    @test validate_wire_contract(:uint16, "test.frame.u16/1", "test.command.f32/1", 1.0e-6, allowed) === nothing
    double_scale = (merge(first(allowed), (command_scale=1.0e-6,)),)
    @test validate_wire_contract(:uint16, "test.frame.u16/1", "test.command.f32/1", 1.0f-6, double_scale) === nothing
    @test validate_wire_contract(:float32, "test.frame.f32/1", "test.command.f32/1", 1, allowed) === nothing
    @test_throws ArgumentError validate_wire_contract(:uint16, nothing, "test.command.f32/1", 1.0e-6, allowed)
    @test_throws ArgumentError validate_wire_contract(:uint16, "test.frame.f32/1", "test.command.u16/1", 1.0e-6, allowed)
    @test_throws ArgumentError validate_wire_contract(:uint16, "test.frame.u16/1", "test.command.f32/1", 1.0e-5, allowed)
    for scale in (0, -1, NaN, Inf, 1.0e100, 1.0e-100)
        @test_throws ArgumentError validate_wire_contract(:uint16, "test.frame.u16/1", "test.command.f32/1", scale, allowed)
    end
end
