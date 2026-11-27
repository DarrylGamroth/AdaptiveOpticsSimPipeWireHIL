const HILImage = AdaptiveOpticsSimPipeWireHIL

mutable struct FakeImageAcquisition
    identity::Union{Nothing,AcquisitionIdentity}
    pts::Union{Nothing,Int64}
    duration::UInt64
end
mutable struct FakeImageBuffer
    memory::Vector{UInt8}
    header::BufferHeader
    acquisition::FakeImageAcquisition
    extent::Int
    stride::Int
    count::Int
end
FakeImageBuffer(shape=(2, 3)) = FakeImageBuffer(zeros(UInt8, prod(shape) * 4),
    BufferHeader(0, 0, 0, 0, 0), FakeImageAcquisition(nothing, 0, 0), 0, 0, 0)
PipeWireAO.buffer_data(buffer::FakeImageBuffer) = buffer
PipeWireAO.capacity(buffer::FakeImageBuffer) = length(buffer.memory)
PipeWireAO.buffer_memory(buffer::FakeImageBuffer, ::Int) = buffer.memory
function PipeWireAO.set_chunk!(buffer::FakeImageBuffer; offset=0, size, stride)
    buffer.extent = size
    buffer.stride = stride
    return nothing
end
PipeWireAO.set_buffer_size!(buffer::FakeImageBuffer, count) = (buffer.count = count; nothing)
PipeWireAO.set_buffer_header!(buffer::FakeImageBuffer, header::BufferHeader) = (buffer.header = header; nothing)
PipeWireAO.metadata_records(buffer::FakeImageBuffer)=buffer.acquisition
PipeWireAO.initialize_metadata!(buffer::FakeImageBuffer)=initialize_metadata!(buffer.acquisition)
PipeWireAO.set_reference_timestamp!(::FakeImageAcquisition,stamp)=nothing
function PipeWireAO.initialize_metadata!(record::FakeImageAcquisition)
    metadata = record
    metadata.identity = nothing
    metadata.pts = nothing
    metadata.duration = 0
    return metadata
end
PipeWireAO.set_acquisition_identity!(metadata::FakeImageAcquisition, identity::AcquisitionIdentity) =
    (metadata.identity = identity; metadata)
PipeWireAO.set_exposure_timing!(metadata::FakeImageAcquisition,timing::ExposureTiming) =
    (metadata.duration = timing.duration_nanoseconds;metadata)
function PipeWireAO.dequeue_buffer!(::FakeImageBuffer, stream::FakeAppliedStream)
    stream.available || return false
    stream.borrowed && error("buffer already borrowed")
    stream.borrowed = true
    return true
end
function PipeWireAO.queue_buffer!(::FakeImageBuffer, stream::FakeAppliedStream)
    stream.fail_queue && error("observer queue failure")
    stream.borrowed || error("no borrowed buffer")
    stream.borrowed = false
    stream.queued += 1
    return nothing
end
function PipeWireAO.return_buffer!(::FakeImageBuffer, stream::FakeAppliedStream)
    stream.borrowed || error("no borrowed buffer")
    stream.borrowed = false
    stream.returned += 1
    return nothing
end

function _image_fixture()
    domain = AcquisitionDomain(ntuple(index -> index == 1 ? UInt8(0xa0) : UInt8(0),
        Val(ACQUISITION_DOMAIN_SIZE)))
    state = HILImage.ImageSourceState((2, 3), domain, UInt64(1000))
    buffer = FakeImageBuffer()
    stream = FakeAppliedStream()
    process = HILImage.ImageProcess(state, buffer)
    return state, buffer, stream, process
end

function _image_allocation_cycle!(state, image, stream, process)
    HILImage._stage_image!(state, image, UInt64(17), Int64(30), UInt64(1))
    process(stream)
    return nothing
end

@testset "optional image declarations" begin
    option = PipeWireImageSourceOptions(node_name="pupil-opd", media_name="pupil-opd",
        schema="example.pupil-opd/1", shape=(2, 3), buffers=3,
        properties=Dict("node.group" => "observation", "node.loop.name" => "observer-loop"))
    @test option.shape == (2, 3)
    @test option.buffers == 3
    @test option.rate === nothing
    properties = HILImage._image_properties(option.node_name, option.media_name,
        option.schema, option.properties)
    @test properties["node.group"] == "observation"
    @test properties["node.loop.name"] == "observer-loop"
    @test properties["media.schema"] == option.schema
    @test !haskey(properties, "node.want-driver")
    for key in keys(HILImage._image_properties("image", "image", "example.image/1", Dict()))
        @test_throws ArgumentError PipeWireImageSourceOptions(node_name="image", media_name="image",
            schema="example.image/1", shape=(2, 3), properties=Dict(key => "override"))
    end
    for shape in ((0, 3), (2,), (2, 3, 4), (true, 3), (-1, 3))
        @test_throws PipeWireHILError PipeWireImageSourceOptions(node_name="image", media_name="image",
            schema="example.image/1", shape=shape)
    end
    @test_throws PipeWireHILError PipeWireImageSourceOptions(node_name="image", media_name="image",
        schema="example.image/1", shape=(2, 3), buffers=1)
    configuration = PipeWireHILConfiguration(remote="private-core",
        frame_node_name="wfs", command_node_name="command", frame_schema="example.wfs/1",
        command_schema="example.command/1", rate=SPA.Fraction(1000, 1), exposure_duration_ns=1000)
    state, _, _, _ = _image_fixture()
    @test HILImage._prepare_image_sources(nothing, configuration, state, (), nothing) == ()
    @test_throws PipeWireHILError HILImage._prepare_image_sources(nothing, configuration,
        state, (option, option), nothing)
    @test_throws PipeWireHILError HILImage._prepare_image_sources(nothing, configuration,
        state, (option,), option.node_name)
end

@testset "copied row-major images and originating acquisition" begin
    state, buffer, stream, process = _image_fixture()
    image = Float32[1 3 5; 2 4 6]
    process(stream)
    @test stream.queued == 0
    @test HILImage._stage_image!(state, image, UInt64(1), Int64(20), UInt64(7))
    fill!(image, -1)
    stream.available = false
    process(stream)
    @test state.pending
    @test !stream.borrowed
    stream.available = true
    process(stream)
    @test reinterpret(Float32, buffer.memory) == Float32[1, 3, 5, 2, 4, 6]
    @test buffer.header.sequence == 1
    @test buffer.header.pts == 20
    @test buffer.acquisition.identity == AcquisitionIdentity(state.domain, UInt64(7), UInt64(1))
    @test buffer.acquisition.pts === nothing
    @test buffer.acquisition.duration == 1000
    @test buffer.extent == 24
    @test buffer.stride == 12
    @test buffer.count == 6
    @test !state.pending
    @test stream.queued == 1
    @test state.dropped[] == 0
    process(stream)
    @test stream.queued == 1

    state.busy[] = true
    @test !HILImage._stage_image!(state, image, UInt64(2), Int64(30), UInt64(7))
    process(stream)
    @test !stream.borrowed
    state.busy[] = false
    @test state.dropped[] == 1
    @test HILImage._stage_image!(state, image, UInt64(2), Int64(30), UInt64(7))
    @test HILImage._stage_image!(state, image, UInt64(3), Int64(40), UInt64(7))
    @test state.dropped[] == 2
    @test !HILImage._stage_image!(state, zeros(Float32, 3, 2), UInt64(4), Int64(50), UInt64(7))
    @test state.sequence == 3
    @test !HILImage._stage_image!(state, image, UInt64(0), Int64(50), UInt64(7))
    @test !HILImage._stage_image!(state, image, UInt64(4), Int64(-1), UInt64(7))
    @test !HILImage._stage_image!(state, image, UInt64(4), Int64(50), UInt64(0))

    HILImage._reset_image!(state)
    @test !state.pending
    @test state.sequence == 0
    @test state.pts == 0
    @test state.generation == 0
    @test state.dropped[] == 0
    @test all(iszero, state.image)
    process(stream)
    @test stream.queued == 1
    _image_allocation_cycle!(state, image, stream, process)
    @test (@allocated _image_allocation_cycle!(state, image, stream, process)) == 0
    @test HILImage._stage_image!(state, image, UInt64(4), Int64(50), UInt64(8))
    stream.fail_queue = true
    @test process(stream) === nothing
    @test state.failed[]
    @test !state.busy[]
    @test !state.pending
    @test !stream.borrowed
    @test stream.returned == 1
    @test !HILImage._stage_image!(state, image, UInt64(5), Int64(60), UInt64(8))
end

@testset "coherent concurrent image snapshots" begin
    state, buffer, stream, process = _image_fixture()
    image = zeros(Float32, 2, 3)
    done = Base.Threads.Atomic{Bool}(false)
    coherent = Base.Threads.Atomic{Bool}(true)
    consumer = Base.Threads.@spawn begin
        previous = 0
        while !done[]
            process(stream)
            if stream.queued != previous
                sequence = buffer.header.sequence
                if buffer.header.pts != 3 * sequence ||
                        buffer.acquisition.identity != AcquisitionIdentity(state.domain, UInt64(7), sequence) ||
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
                fill!(image, Float32(sequence))
                HILImage._stage_image!(state, image, sequence, Int64(3 * sequence), UInt64(7))
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
    @test !state.busy[]
    @test !state.failed[]
end
