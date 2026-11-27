const HILApplied = AdaptiveOpticsSimPipeWireHIL

@testset "applied command owner properties" begin
    additional = Dict("node.group" => "pipewireao-applied-observation",
        "node.loop.name" => "observer-loop")
    contract = HILApplied._applied_command_contract((command_schema=HILApplied.Schemas.DEMANDED_PDM_COMMAND_V1,
        command_scale=1.0f-6), (277,))
    properties = HILApplied._applied_command_properties("simulator-applied-command", additional, contract)
    @test properties["node.group"] == additional["node.group"]
    @test properties["node.loop.name"] == additional["node.loop.name"]
    @test properties["node.name"] == "simulator-applied-command"
    @test properties["media.name"] == "dm-applied"
    @test properties["media.schema"] == APPLIED_PDM_COMMAND_SCHEMA
    @test additional == Dict("node.group" => "pipewireao-applied-observation",
        "node.loop.name" => "observer-loop")
    for key in keys(HILApplied._applied_command_properties("source", nothing, contract))
        @test_throws ArgumentError HILApplied._applied_command_properties("source", Dict(key => "override"), contract)
    end
end

mutable struct FakeAppliedBuffer
    memory::Vector{UInt8}
    header::BufferHeader
    extent::Int
    stride::Int
    count::Int
    properties_initialized::Bool
end
mutable struct FakeAppliedStream
    available::Bool
    fail_queue::Bool
    borrowed::Bool
    queued::Int
    returned::Int
end
FakeAppliedStream() = FakeAppliedStream(true, false, false, 0, 0)
FakeAppliedBuffer() = FakeAppliedBuffer(zeros(UInt8, 277 * 4), BufferHeader(0, 0, 0, 0, 0), 0, 0, 0, false)

PipeWireAO.set_reference_timestamp!(::FakeAppliedBuffer,stamp)=nothing
PipeWireAO.initialize_metadata!(buffer::FakeAppliedBuffer) = (buffer.properties_initialized = true; nothing)
PipeWireAO.buffer_data(buffer::FakeAppliedBuffer) = buffer
PipeWireAO.capacity(buffer::FakeAppliedBuffer) = length(buffer.memory)
PipeWireAO.buffer_memory(buffer::FakeAppliedBuffer, ::Int) = buffer.memory
function PipeWireAO.set_chunk!(buffer::FakeAppliedBuffer; offset=0, size, stride)
    buffer.extent = size
    buffer.stride = stride
    return nothing
end
PipeWireAO.set_buffer_header!(buffer::FakeAppliedBuffer, header::BufferHeader) = (buffer.header = header; nothing)
PipeWireAO.set_buffer_size!(buffer::FakeAppliedBuffer, count) = (buffer.count = count; nothing)
function PipeWireAO.dequeue_buffer!(::FakeAppliedBuffer, stream::FakeAppliedStream)
    stream.available || return false
    stream.borrowed && error("buffer already borrowed")
    stream.borrowed = true
    return true
end
function PipeWireAO.queue_buffer!(::FakeAppliedBuffer, stream::FakeAppliedStream)
    stream.fail_queue && error("observer queue failure")
    stream.borrowed || error("no borrowed buffer")
    stream.borrowed = false
    stream.queued += 1
    return nothing
end
function PipeWireAO.return_buffer!(::FakeAppliedBuffer, stream::FakeAppliedStream)
    stream.borrowed || error("no borrowed buffer")
    stream.borrowed = false
    stream.returned += 1
    return nothing
end

function _applied_fixture()
    state = HILApplied.ExchangeState(zeros(Float32, 2), zeros(Float32, 277), UInt64(1), :float32, 1.0f-6)
    observation = HILApplied.AppliedCommandState(length(state.command))
    buffer = FakeAppliedBuffer()
    stream = FakeAppliedStream()
    process = HILApplied.AppliedCommandProcess(observation, buffer)
    return state, observation, buffer, stream, process
end

function _receive_applied_fixture!(state, sequence, pts, values)
    state.frame_ready[] = sequence
    HILApplied._record_frame_publication!(state, sequence, pts)
    receive_test_command!(state, values, sequence, pts + 10)
    return nothing
end
_accept_applied_fixture(::Nothing, ::UInt64) = nothing
_reject_applied_fixture(::Nothing, ::UInt64) = throw(PipeWireHILError("adoption", "rejected"))

function _applied_allocation_cycle!(state, observation, stream, process)
    HILApplied._stage_applied_command!(observation, state, UInt64(17))
    process(stream)
    return nothing
end

@testset "simulator adopted command observation" begin
    @test APPLIED_PDM_COMMAND_SCHEMA == "pipewireao/applied-dm-command-opd-um-f32/1"
    state, observation, buffer, stream, process = _applied_fixture()
    values = Float32.(range(-2, 2; length=277))
    _receive_applied_fixture!(state, UInt64(1), Int64(100), values)
    @test !observation.pending
    process(stream)
    @test stream.queued == 0 # Receipt is not adoption.
    @test_throws PipeWireHILError HILApplied._adopt_received_command!(
        _reject_applied_fixture, nothing, state, observation, UInt64(1))
    @test !observation.pending
    @test state.command_ready[] == 1
    @test_throws PipeWireHILError HILApplied._adopt_received_command!(
        _accept_applied_fixture, nothing, state, observation, UInt64(2))
    @test !observation.pending

    HILApplied._adopt_received_command!(_accept_applied_fixture, nothing, state, observation, UInt64(1))
    @test state.command_ready[] == 0
    @test observation.pending
    @test observation.values ≈ values
    fill!(state.command, 123.0f0) # Snapshot owns its values independently.
    stream.available = false
    process(stream)
    @test observation.pending
    @test stream.queued == 0
    stream.available = true
    process(stream)
    @test !observation.pending
    @test stream.queued == 1
    @test buffer.properties_initialized
    @test !stream.borrowed
    @test buffer.header.sequence == 1
    @test buffer.header.pts == 100 # Original ingress PTS, not adoption time.
    @test buffer.extent == 277 * sizeof(Float32)
    @test buffer.stride == sizeof(Float32)
    @test buffer.count == 277
    @test reinterpret(Float32, buffer.memory) ≈ values
    process(stream)
    @test stream.queued == 1

    _receive_applied_fixture!(state, UInt64(2), Int64(200), values)
    observation.busy[] = true
    HILApplied._adopt_received_command!(_accept_applied_fixture, nothing, state, observation, UInt64(2))
    @test state.command_ready[] == 0 # Busy observers cannot gate adoption.
    @test observation.dropped[] == 1
    process(stream)
    @test !stream.borrowed
    observation.busy[] = false

    @test HILApplied._stage_applied_command!(observation, state, UInt64(2))
    @test HILApplied._stage_applied_command!(observation, state, UInt64(3))
    @test observation.dropped[] == 2 # Newest adopted snapshot replaces pending one.
    @test observation.sequence == 3
    HILApplied._reset_applied_command!(observation)
    @test !observation.pending
    @test observation.sequence == 0
    @test observation.pts == 0
    @test observation.dropped[] == 0
    @test all(iszero, observation.values)
    process(stream)
    @test stream.queued == 1

    _applied_allocation_cycle!(state, observation, stream, process)
    @test (@allocated _applied_allocation_cycle!(state, observation, stream, process)) == 0
    state.command_ready[] = UInt64(2)
    HILApplied._adopt_received_command!(_accept_applied_fixture, nothing, state, observation, UInt64(2))
    state.command_ready[] = UInt64(2)
    @test (@allocated HILApplied._adopt_received_command!(
        _accept_applied_fixture, nothing, state, observation, UInt64(2))) == 0
    @test !observation.busy[]
    @test HILApplied._stage_applied_command!(observation, state, UInt64(4))
    stream.fail_queue = true
    @test process(stream) === nothing
    @test observation.failed[]
    @test !observation.pending
    @test !observation.busy[]
    @test !stream.borrowed
    @test stream.returned == 1
    @test !HILApplied._stage_applied_command!(observation, state, UInt64(5))
end

@testset "concurrent copied adopted snapshots" begin
    _, observation, buffer, stream, process = _applied_fixture()
    command = zeros(Float32, 277)
    done = Base.Threads.Atomic{Bool}(false)
    coherent = Base.Threads.Atomic{Bool}(true)
    consumer = Base.Threads.@spawn begin
        previous = 0
        while !done[]
            process(stream)
            if stream.queued != previous
                sequence = buffer.header.sequence
                if buffer.header.pts != 3 * sequence ||
                        !all(==(Float32(sequence)), reinterpret(Float32, buffer.memory))
                    coherent[] = false
                end
                previous = stream.queued
            end
            yield()
        end
        process(stream)
    end
    producer = Base.Threads.@spawn begin
        try
            for sequence in UInt64(1):UInt64(5000)
                fill!(command, Float32(sequence))
                HILApplied._stage_applied_command!(observation, command, 1.0f0,
                    sequence, Int64(3 * sequence))
                sequence % 16 == 0 && yield()
            end
        finally
            done[] = true
        end
    end
    fetch(producer)
    fetch(consumer)
    @test coherent[]
    @test stream.queued > 0
    @test !stream.borrowed
    @test !observation.busy[]
    @test !observation.failed[]
end

@testset "applied observation reports micrometre OPD for metre input" begin
    state = HILApplied.ExchangeState(zeros(Float32, 2), fill(1.0f-6, 277), UInt64(1), :float32, 1.0f0)
    observation = HILApplied.AppliedCommandState(length(state.command))
    @test HILApplied._stage_applied_command!(observation, state, UInt64(1))
    @test all(==(1.0f0), observation.values)
end


@testset "optional DM observation qualifies physical command meaning" begin
    for (schema, scale) in ((HILApplied.Schemas.DEMANDED_PDM_COMMAND_V1, 1.0f-6),
            (HILApplied.Schemas.ACTUATOR_COMMAND_OPD_M_V1, 1.0f0))
        @test HILApplied._applied_command_contract((;command_schema=schema, command_scale=scale), (277,)).schema == APPLIED_PDM_COMMAND_SCHEMA
        @test_throws PipeWireHILError HILApplied._applied_command_contract(
            (;command_schema=schema, command_scale=2.0f0), (277,))
    end
    @test_throws PipeWireHILError HILApplied._applied_command_contract(
        (;command_schema="external/voltage-f32/1", command_scale=1.0f0), (277,))
    for count in (3, 17, 277)
        state = HILApplied.ExchangeState(zeros(Float32, 2), fill(1.0f-6, count), UInt64(1), :float32, 1.0f0)
        observation = HILApplied.AppliedCommandState(count)
        @test HILApplied._stage_applied_command!(observation, state, UInt64(1))
        @test observation.values == ones(Float32, count)
    end
end

@testset "tip/tilt observation preserves normalized-pupil coefficient meaning" begin
    schema = HILApplied.Schemas.TIP_TILT_OPD_COEFFICIENTS_M_V1
    contract = HILApplied._applied_command_contract((command_schema=schema, command_scale=1f0), (2,))
    @test contract.schema == schema
    @test contract.media_name == "tip-tilt-applied"
    @test contract.units == :metre_opd
    @test contract.command_scale == 1f0
    @test HILApplied.Schemas.schema_element_type(schema) === Float32
    for shape in ((1,), (3,), (1, 2))
        @test_throws PipeWireHILError HILApplied._applied_command_contract(
            (command_schema=schema, command_scale=1f0), shape)
    end
    @test_throws PipeWireHILError HILApplied._applied_command_contract(
        (command_schema=schema, command_scale=1f-6), (2,))
    values = Float32[-0.0, 4.25f-7]
    observation = HILApplied.AppliedCommandState(2)
    @test HILApplied._stage_applied_command!(observation, values, contract.command_scale, UInt64(7), Int64(100))
    @test reinterpret(UInt32, observation.values) == reinterpret(UInt32, values)
    buffer = FakeAppliedBuffer()
    process = HILApplied.AppliedCommandProcess(observation, buffer)
    process(FakeAppliedStream())
    @test buffer.extent == 2sizeof(Float32)
    @test reinterpret(UInt32, buffer.memory)[1:2] == reinterpret(UInt32, values)
    @test buffer.header.sequence == 7
    @test buffer.header.pts == 100
end
