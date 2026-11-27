const ArrayTransport = AdaptiveOpticsSimPipeWireHIL

mutable struct FakeArrayMetadata
    identity::Union{Nothing,AcquisitionIdentity}
    duration::Union{Nothing,UInt64}
    optical_classification::Union{Nothing,OpticalClassification}
    trigger_state::AcquisitionTriggerState
    has_trigger_classification::Bool
    reference_timestamp::Union{Nothing,ReferenceTimestamp}
    allocated::Vector{UInt32}
end
mutable struct FakeArrayBuffer
    memory::Vector{UInt8}
    header::Union{Nothing,BufferHeader}
    metadata::FakeArrayMetadata
    extent::Int
    stride::Int
    flags::UInt32
end
FakeArrayBuffer(shape) = FakeArrayBuffer(zeros(UInt8, 4 * prod(shape)), nothing,
    FakeArrayMetadata(nothing, nothing, nothing, ACQUISITION_TRIGGER_ABSENT, false,nothing,UInt32[SPA.META_ACQUISITION_IDENTITY,SPA.META_EXPOSURE_TIMING,
        SPA.META_REFERENCE_TIMESTAMP,SPA.META_OPTICAL_CHOPPING,SPA.META_ELECTRICAL_TRIGGER]),
    4 * prod(shape), 4 * last(shape), UInt32(0))
mutable struct FakeArrayStream
    available::Bool
    borrowed::Bool
    queued::Int
    returned::Int
    fail_queue::Bool
end
FakeArrayStream() = FakeArrayStream(true, false, 0, 0, false)
PipeWireAO.buffer_data(buffer::FakeArrayBuffer) = buffer
PipeWireAO.buffer_header(buffer::FakeArrayBuffer) = buffer.header
PipeWireAO.metadata_records(buffer::FakeArrayBuffer) = buffer.metadata
PipeWireAO.metadata_record(metadata::FakeArrayMetadata,type::UInt32)=type in metadata.allocated ? type : nothing
PipeWireAO.metadata_fits(buffer::FakeArrayBuffer,requirements::Tuple{Vararg{MetadataRequirement}};header::Bool=true)=
    all(r -> !r.required || r.type in buffer.metadata.allocated,requirements)

PipeWireAO.initialize_metadata!(buffer::FakeArrayBuffer)=initialize_metadata!(buffer.metadata)
PipeWireAO.metadata_records_valid(::FakeArrayMetadata, requirements::Tuple=()) = true
PipeWireAO.optical_classification(metadata::FakeArrayMetadata) = metadata.optical_classification
PipeWireAO.set_optical_classification!(metadata::FakeArrayMetadata, value::OpticalClassification) = (metadata.optical_classification = value; metadata)
PipeWireAO.buffer_memory(buffer::FakeArrayBuffer, extent::Int) = @view buffer.memory[1:extent]
PipeWireAO.bytes(buffer::FakeArrayBuffer) = @view buffer.memory[1:buffer.extent]
PipeWireAO.chunk_info(buffer::FakeArrayBuffer) = (offset=0, size=buffer.extent, stride=buffer.stride, flags=buffer.flags)
PipeWireAO.acquisition_identity(metadata::FakeArrayMetadata) = metadata.identity
PipeWireAO.exposure_timing(metadata::FakeArrayMetadata) = metadata.duration===nothing ? nothing : ExposureTiming(;duration_nanoseconds=metadata.duration)
PipeWireAO.trigger_classification(metadata::FakeArrayMetadata) = metadata.has_trigger_classification ? metadata.trigger_state : nothing
PipeWireAO.set_trigger_classification!(metadata::FakeArrayMetadata, state::AcquisitionTriggerState) =
    (metadata.trigger_state = state; metadata.has_trigger_classification = true; metadata)
PipeWireAO.set_buffer_header!(buffer::FakeArrayBuffer, header::BufferHeader) = (buffer.header = header; nothing)
PipeWireAO.set_buffer_size!(buffer::FakeArrayBuffer, count::Int) = nothing
function PipeWireAO.set_chunk!(buffer::FakeArrayBuffer; offset, size, stride)
    @assert offset == 0
    buffer.extent, buffer.stride = size, stride
    return nothing
end
function PipeWireAO.initialize_metadata!(metadata::FakeArrayMetadata)
    metadata.identity, metadata.duration, metadata.optical_classification = nothing, nothing, nothing
    metadata.trigger_state = ACQUISITION_TRIGGER_ABSENT
    metadata.has_trigger_classification = false
    metadata.reference_timestamp=nothing
    return metadata
end
PipeWireAO.set_acquisition_identity!(metadata::FakeArrayMetadata, identity::AcquisitionIdentity) = (metadata.identity = identity; metadata)
PipeWireAO.set_exposure_timing!(metadata::FakeArrayMetadata,timing::ExposureTiming) = (metadata.duration=timing.duration_nanoseconds;metadata)
PipeWireAO.set_reference_timestamp!(metadata::FakeArrayMetadata,stamp::ReferenceTimestamp) = (metadata.reference_timestamp=stamp;metadata)
PipeWireAO.reference_timestamp(metadata::FakeArrayMetadata)=metadata.reference_timestamp
function PipeWireAO.dequeue_buffer!(::FakeArrayBuffer, stream::FakeArrayStream)
    stream.available || return false
    @assert !stream.borrowed
    stream.borrowed = true
    return true
end
function PipeWireAO.queue_buffer!(::FakeArrayBuffer, stream::FakeArrayStream)
    stream.fail_queue && error("injected queue failure")
    @assert stream.borrowed
    stream.borrowed = false
    stream.queued += 1
    return nothing
end
function PipeWireAO.return_buffer!(::FakeArrayBuffer, stream::FakeArrayStream)
    @assert stream.borrowed
    stream.borrowed = false
    stream.returned += 1
    return nothing
end

const TEST_ARRAY_SENSOR_METADATA=(ArrayTransport._ACQUISITION_METADATA...,
    MetadataRequirement(SPA.META_OPTICAL_CHOPPING),MetadataRequirement(SPA.META_ELECTRICAL_TRIGGER))

function array_fixture(shape=(2, 3, 4); metadata=TEST_ARRAY_SENSOR_METADATA, allocated=nothing,
        reference_clock=METADATA_CLOCK_REALTIME, element_type=NdArray.F32_LE)
    format = NdArrayFormat(element_type, shape; layout=NdArray.ROW_MAJOR)
    element = ArrayTransport._array_element_type(Val(element_type))
    stride = sizeof(element) * last(shape)
    option = PipeWireArrayStreamOptions(node_name="test-array", media_name="test array",
        schema="test.array/1", format=format, metadata=metadata,reference_clock=reference_clock,properties=Dict("test.axes" => "plane,row,column"))
    source = ArrayTransport.ArrayStreamState(prod(shape), element)
    sink = ArrayTransport.ArrayStreamState(prod(shape), element)
    buffer, output, input = FakeArrayBuffer(shape), FakeArrayStream(), FakeArrayStream()
    allocated===nothing || (buffer.metadata.allocated=UInt32[allocated...])
    source_process = ArrayTransport.ArrayStreamProcess(source, buffer, false, stride,metadata,reference_clock)
    sink_process = ArrayTransport.ArrayStreamProcess(sink, buffer, true, stride,metadata,reference_clock)
    owner = ArrayTransport.PipeWireArrayStreams(nothing, nothing, nothing,
        (ArrayTransport.PreparedArrayStream(output, source, option, false),
         ArrayTransport.PreparedArrayStream(input, sink, option, true)), false)
    domain = AcquisitionDomain(ntuple(i -> UInt8(i), Val(ACQUISITION_DOMAIN_SIZE)))
    identity(sequence, generation=1) = AcquisitionIdentity(domain, UInt64(generation), UInt64(sequence))
    return (; owner, source, sink, buffer, output, input, source_process, sink_process, identity)
end

@testset "complete arrays preserve observed whole-exposure trigger state" begin
    f = array_fixture((2,3))
    image = reshape(Float32.(1:6),2,3)
    for (sequence, trigger_state) in enumerate((ACQUISITION_TRIGGER_HIGH,
            ACQUISITION_TRIGGER_LOW, ACQUISITION_TRIGGER_MIXED, ACQUISITION_TRIGGER_ABSENT))
        @test stage_pipewire_array!(f.owner,1,image; identity=f.identity(sequence),
            pts=Int64(10sequence),duration=UInt64(10),trigger_state)
        f.source_process(f.output)
        @test f.buffer.metadata.trigger_state == trigger_state
        f.sink_process(f.input)
        received = similar(image)
        receipt = drain_pipewire_array!(received,f.owner,2)
        @test received == image
        @test receipt.trigger_state == trigger_state
        @test receipt.identity == f.identity(sequence)
        @test receipt.pts == 10sequence && receipt.duration == 10 && receipt.lost_samples == 0
    end
    # A stale trigger must not survive the next explicit absent classification.
    @test f.sink.receipt.trigger_state == ACQUISITION_TRIGGER_ABSENT
    @test PipeWireArrayReceipt(Int64(0),UInt64(10),f.identity(1),UInt64(0)).trigger_state == ACQUISITION_TRIGGER_ABSENT
    @test PipeWireArrayReceipt(0,10,f.identity(1),0) == PipeWireArrayReceipt(Int64(0),UInt64(10),f.identity(1),UInt64(0))
end

@testset "array input requires physical identity and duration independent of Header" begin
    f = array_fixture((2,3))
    image = ones(Float32,2,3)
    @test stage_pipewire_array!(f.owner,1,image; identity=f.identity(1),pts=Int64(0),duration=UInt64(10))
    f.source_process(f.output)
    f.buffer.header = BufferHeader(0,0,100,0,1000)
    @test ArrayTransport._read_array!(f.sink_process)
    @test f.sink.receipt.identity == f.identity(1)
    f.sink.identity = nothing
    f.buffer.metadata.identity = nothing
    @test !ArrayTransport._read_array!(f.sink_process)
    f.buffer.metadata.identity = f.identity(2)
    f.buffer.metadata.duration = nothing
    @test !ArrayTransport._read_array!(f.sink_process)
    @test f.sink.rejected[] == 2
end

@testset "UInt16 array transport" begin
    f = array_fixture((2, 3); element_type=NdArray.U16_LE)
    pixels = UInt16[0 255 65535; 256 1024 42]
    @test stage_pipewire_array!(f.owner, 1, pixels;
        identity=f.identity(1), pts=Int64(1), duration=UInt64(2))
    @test_throws ArgumentError stage_pipewire_array!(f.owner, 1, Float32.(pixels);
        identity=f.identity(2), pts=Int64(2), duration=UInt64(2))
    f.source_process(f.output)
    @test f.buffer.extent == sizeof(pixels)
    @test f.buffer.stride == 3 * sizeof(UInt16)
    @test reinterpret(UInt16, bytes(f.buffer)) == UInt16[0, 255, 65535, 256, 1024, 42]
    f.sink_process(f.input)
    @test_throws ArgumentError drain_pipewire_array!(zeros(Float32, size(pixels)), f.owner, 2)
    received = similar(pixels)
    @test drain_pipewire_array!(received, f.owner, 2).identity == f.identity(1)
    @test received == pixels
end

@testset "immutable optically agnostic array declarations" begin
    fixture = array_fixture()
    option = fixture.owner.streams[1].options
    @test option.properties isa Tuple
    @test Dict(option.properties)["test.axes"] == "plane,row,column"
    properties = Dict("test.axes" => "a,b")
    option2 = PipeWireArrayStreamOptions(node_name="x", media_name="x", schema="x/1",
        format=NdArrayFormat(NdArray.F32_LE, (2, 3); layout=NdArray.ROW_MAJOR), properties=properties)
    properties["test.axes"] = "changed"
    @test Dict(option2.properties)["test.axes"] == "a,b"
    @test_throws ArgumentError PipeWireArrayStreamOptions(node_name="x", media_name="x", schema="x/1",
        format=NdArrayFormat(NdArray.F64_LE, (2, 3); layout=NdArray.ROW_MAJOR))
    @test_throws ArgumentError PipeWireArrayStreamOptions(node_name="x", media_name="x", schema="x/1",
        format=NdArrayFormat(NdArray.F32_LE, (2, 3); layout=NdArray.ROW_MAJOR), properties=Dict("node.name" => "override"))
    @test_throws PipeWireHILError with_array_streams(identity, (option,); remote="", input=(true,))
    @test_throws ArgumentError with_array_streams(identity, (option,); remote="unused-private", input=())
end

@testset "complete rank-three arrays and acquisition receipts" begin
    f = array_fixture()
    array = reshape(Float32.(1:24), 2, 3, 4)
    expected = copy(array)
    @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(1), pts=Int64(123), duration=UInt64(45))
    fill!(array, -1)
    f.source_process(f.output)
    @test !f.source.pending
    @test reinterpret(Float32, f.buffer.memory) == [expected[p,r,c] for p in 1:2 for r in 1:3 for c in 1:4]
    @test f.buffer.header.pts == 123
    @test f.buffer.header.sequence == 1
    @test f.buffer.metadata.identity == f.identity(1)
    @test f.buffer.metadata.duration == 45
    f.sink_process(f.input)
    fill!(f.buffer.memory, 0) # No borrowed native storage survives the callback.
    receipt = drain_pipewire_array!(array, f.owner, 2)
    @test array == expected
    @test receipt isa PipeWireArrayReceipt
    @test receipt.pts == 123
    @test receipt.duration == 45
    @test receipt.identity == f.identity(1)
    @test receipt.lost_samples == 0
    @test receipt.trigger_state == ACQUISITION_TRIGGER_ABSENT
    @test drain_pipewire_array!(array, f.owner, 2) === nothing
    @test !f.output.borrowed && !f.input.borrowed
    @test !stage_pipewire_array!(f.owner, 1, array; identity=f.identity(1), pts=Int64(123), duration=UInt64(45))
    @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(1, 2), pts=Int64(0), duration=UInt64(45))
    f.source_process(f.output)
    f.sink_process(f.input)
    receipt = drain_pipewire_array!(array, f.owner, 2)
    @test receipt.identity == f.identity(1, 2)
    @test receipt.lost_samples == 0
    @test f.sink.gaps[] == 0
    @test !stage_pipewire_array!(f.owner, 1, array; identity=f.identity(2), pts=Int64(123), duration=UInt64(45))
    f.buffer.header = BufferHeader(0, 0, 123, 0, 2)
    f.buffer.metadata.identity = f.identity(2)
    f.sink_process(f.input)
    @test f.sink.rejected[] == 1
    @test drain_pipewire_array!(array, f.owner, 2) === nothing
    @test_throws DimensionMismatch stage_pipewire_array!(f.owner, 1, zeros(Float32, 2, 3); identity=f.identity(2), pts=Int64(1), duration=UInt64(1))
end

@testset "latest-only overwrite, gaps and contention recycle" begin
    f = array_fixture((2, 3))
    array = zeros(Float32, 2, 3)
    for sequence in 1:3
        fill!(array, sequence)
        @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(sequence), pts=Int64(sequence), duration=UInt64(1))
    end
    @test f.source.dropped[] == 2
    f.output.available = false
    f.source_process(f.output)
    @test f.source.pending
    f.output.available = true
    f.source_process(f.output)
    f.sink_process(f.input)
    for sequence in (4, 7)
        fill!(array, sequence)
        @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(sequence), pts=Int64(sequence), duration=UInt64(1))
        f.source_process(f.output)
        f.sink_process(f.input)
    end
    @test f.sink.dropped[] == 2
    @test f.sink.gaps[] == 2
    @test drain_pipewire_array!(array, f.owner, 2).lost_samples == 4
    @test all(==(7), array)
    @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(8), pts=Int64(8), duration=UInt64(1))
    f.source_process(f.output)
    f.sink.busy[] = true
    f.sink_process(f.input)
    @test !f.input.borrowed
    f.sink.busy[] = false
    @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(9), pts=Int64(9), duration=UInt64(1))
    f.source_process(f.output)
    f.sink_process(f.input)
    @test drain_pipewire_array!(array, f.owner, 2).lost_samples == 5 # Busy loss not counted twice as a gap.
    @test f.sink.gaps[] == 2
    f.source.busy[] = true
    f.source_process(f.output)
    @test !f.output.borrowed
    f.source.busy[] = false
end

@testset "invalid buffers and fail-closed callbacks" begin
    f = array_fixture()
    f.sink_process(f.input)
    @test f.sink.rejected[] == 1
    @test !f.input.borrowed
    array = zeros(Float32, 2, 3, 4)
    @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(1), pts=Int64(0), duration=UInt64(1))
    f.source_process(f.output)
    f.buffer.stride = 8
    f.sink_process(f.input)
    @test f.sink.rejected[] == 2
    f.buffer.stride = 16
    f.buffer.metadata.duration = nothing
    f.sink_process(f.input)
    @test f.sink.rejected[] == 3
    f.buffer.metadata.duration = UInt64(1)
    f.sink.identity = f.identity(0, 2) # Same cold epoch binding used by reset.
    f.sink_process(f.input)
    @test f.sink.rejected[] == 4
    @test !f.sink.pending
    @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(2), pts=Int64(1), duration=UInt64(1))
    f.output.fail_queue = true
    f.source_process(f.output)
    @test f.source.failed[]
    @test !f.output.borrowed
    @test f.output.returned == 1
    @test !f.source.busy[]
    f.owner.closed = true
    @test_throws InvalidStateException stage_pipewire_array!(f.owner, 1, array; identity=f.identity(3), pts=Int64(2), duration=UInt64(1))
    @test_throws InvalidStateException drain_pipewire_array!(array, f.owner, 2)
end

@testset "packed native chunk forms and invalid Header flags" begin
    f = array_fixture()
    array = zeros(Float32, 2, 3, 4)
    for (sequence, stride) in enumerate((0, 4, 16))
        @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(sequence), pts=Int64(sequence), duration=UInt64(1))
        f.source_process(f.output)
        f.buffer.stride = stride
        f.sink_process(f.input)
        @test drain_pipewire_array!(array, f.owner, 2).identity == f.identity(sequence)
    end
    for (sequence, flag) in enumerate((SPA.META_HEADER_FLAG_CORRUPTED, SPA.META_HEADER_FLAG_GAP))
        f.buffer.header = BufferHeader(flag, 0, 1, 0, sequence + 3)
        f.buffer.metadata.identity = f.identity(sequence + 3)
        f.sink_process(f.input)
    end
    @test f.sink.rejected[] == 2
    @test !f.sink.pending
end

@testset "explicit Header discontinuity cannot bridge an exposure" begin
    f = array_fixture()
    array = zeros(Float32, 2, 3, 4)
    for sequence in 1:3
        @test stage_pipewire_array!(f.owner, 1, array; identity=f.identity(sequence), pts=Int64(sequence), duration=UInt64(1))
        f.source_process(f.output)
        if sequence == 2
            f.buffer.header = BufferHeader(SPA.META_HEADER_FLAG_DISCONT, 0, 2, 0, 2)
        end
        f.sink_process(f.input)
        receipt = drain_pipewire_array!(array, f.owner, 2)
        if sequence == 2
            @test receipt === nothing
        else
            @test receipt.identity == f.identity(sequence)
            @test receipt.lost_samples == (sequence == 1 ? 0 : 1)
        end
    end
    @test f.sink.rejected[] == 1
    @test f.sink.gaps[] == 1
end

@testset "coherent concurrent standalone array snapshots" begin
    f = array_fixture()
    array, destination = zeros(Float32, 2, 3, 4), zeros(Float32, 2, 3, 4)
    done = Base.Threads.Atomic{Bool}(false)
    coherent = Base.Threads.Atomic{Bool}(true)
    received = Base.Threads.Atomic{Int}(0)
    function adopt()
        receipt = drain_pipewire_array!(destination, f.owner, 2)
        if receipt !== nothing
            sequence = receipt.identity.sequence
            coherent[] &= receipt.pts == 3 * sequence && receipt.duration == sequence + 1 &&
                all(==(Float32(sequence)), destination)
            Base.Threads.atomic_add!(received, 1)
        end
        return nothing
    end
    consumer = Base.Threads.@spawn begin
        while !done[]
            f.source_process(f.output)
            f.sink_process(f.input)
            yield()
        end
        f.source_process(f.output)
        f.sink_process(f.input)
    end
    producer = Base.Threads.@spawn begin
        try
            for sequence in UInt64(1):UInt64(2000)
                fill!(array, Float32(sequence))
                stage_pipewire_array!(f.owner, 1, array; identity=f.identity(sequence),
                    pts=Int64(3 * sequence), duration=sequence + UInt64(1))
                adopt()
                sequence % 8 == 0 && yield()
            end
        finally
            done[] = true
        end
    end
    fetch(producer)
    fetch(consumer)
    adopt()
    @test coherent[]
    @test received[] > 0
    @test !f.source.failed[] && !f.sink.failed[]
    @test !f.source.busy[] && !f.sink.busy[]
    @test !f.output.borrowed && !f.input.borrowed
end

@testset "independent per-port metadata layouts" begin
    format=NdArrayFormat(NdArray.F32_LE,(2,3);layout=NdArray.ROW_MAJOR)
    options=PipeWireArrayStreamOptions(node_name="metadata",media_name="metadata",schema="test/1",format=format)
    @test all(item -> item isa MetadataRequirement,options.metadata)
    @test options.reference_clock==METADATA_CLOCK_REALTIME
    @test isempty(options.metadata)
    @test_throws ArgumentError PipeWireArrayStreamOptions(node_name="metadata",media_name="metadata",schema="test/1",format=format,metadata=(MetadataRequirement(0x900,4096),MetadataRequirement(0x901,8)))
end

@testset "array receipts preserve the source reference timestamp" begin
    f=array_fixture((2,3);reference_clock=METADATA_CLOCK_MODEL)
    source=ReferenceTimestamp(typemax(UInt64)-1;clock=METADATA_CLOCK_MODEL,duration_nanoseconds=17)
    values=ones(Float32,2,3)
    @test stage_pipewire_array!(f.owner,1,values;identity=f.identity(1),pts=Int64(7),duration=UInt64(10),reference_timestamp=source)
    f.source_process(f.output)
    f.sink_process(f.input)
    receipt=drain_pipewire_array!(similar(values),f.owner,2)
    @test receipt.reference_timestamp==source
end

@testset "command profiles retain publication sequence without camera facts" begin
    f=array_fixture((2,3))
    profile=(MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP;required=true),
        MetadataRequirement(SPA.META_ACQUISITION_IDENTITY),MetadataRequirement(SPA.META_EXPOSURE_TIMING))
    process=ArrayTransport.ArrayStreamProcess(f.sink,f.buffer,true,12,profile,METADATA_CLOCK_REALTIME)
    f.buffer.header=BufferHeader(0,0,typemin(Int64),0,UInt64(7))
    f.buffer.metadata.reference_timestamp=ReferenceTimestamp(123)
    @test ArrayTransport._read_array!(process)
    receipt=drain_pipewire_array!(zeros(Float32,2,3),f.owner,2)
    @test receipt.sequence==7
    @test receipt.identity===nothing
    @test receipt.duration===nothing
    @test receipt.reference_timestamp==ReferenceTimestamp(123)
    @test !ArrayTransport._read_array!(process)
end

function _array_read_allocation(process)
    process.state.identity=nothing
    process.state.publication_sequence=0
    ArrayTransport._read_array!(process)
    process.state.identity=nothing
    process.state.publication_sequence=0
    return @allocated ArrayTransport._read_array!(process)
end
@testset "warmed callback metadata uses its prepared receipt slot" begin
    optional=Tuple(MetadataRequirement(r.type,r.size) for r in TEST_ARRAY_SENSOR_METADATA)
    for has_identity in (false,true), stamp in (nothing,ReferenceTimestamp(typemax(UInt64)-1))
        f=array_fixture((2,3);metadata=optional)
        identity=has_identity ? f.identity(1) : nothing
        stage_pipewire_array!(f.owner,1,ones(Float32,2,3);identity,sequence=UInt64(1),pts=Int64(7),duration=UInt64(10))
        f.source_process(f.output)
        f.buffer.metadata.reference_timestamp=stamp
        @test _array_read_allocation(f.sink_process)==0
        @test f.sink.receipt.reference_timestamp==stamp
        @test f.sink.receipt.identity==identity
    end
end

@testset "array writers honor negotiated optional capabilities" begin
    image=ones(Float32,2,3)
    header_only=array_fixture((2,3);metadata=(),allocated=(),reference_clock=METADATA_CLOCK_MODEL)
    @test stage_pipewire_array!(header_only.owner,1,image;identity=header_only.identity(100),sequence=UInt64(7),
        pts=Int64(9),duration=UInt64(10),optical_classification=OPTICAL_FRINGED)
    header_only.source_process(header_only.output)
    @test header_only.output.queued==1 && !header_only.source.failed[]
    @test header_only.buffer.header.sequence==7
    @test header_only.buffer.metadata.identity===nothing && header_only.buffer.metadata.duration===nothing
    @test header_only.buffer.metadata.reference_timestamp===nothing
    @test !header_only.buffer.metadata.has_trigger_classification && header_only.buffer.metadata.optical_classification===nothing

    reference=(MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP;required=true),)
    command=array_fixture((2,3);metadata=reference,allocated=(SPA.META_REFERENCE_TIMESTAMP,))
    @test stage_pipewire_array!(command.owner,1,image;sequence=UInt64(19),pts=Int64(9))
    command.source_process(command.output)
    @test command.output.queued==1 && !command.source.failed[]
    @test command.buffer.header.sequence==19
    @test command.buffer.metadata.identity===nothing && command.buffer.metadata.duration===nothing
    @test command.buffer.metadata.reference_timestamp.clock==METADATA_CLOCK_REALTIME

    optional=Tuple(MetadataRequirement(r.type,r.size) for r in ArrayTransport._ACQUISITION_METADATA)
    negotiated=array_fixture((2,3);metadata=optional,allocated=(SPA.META_REFERENCE_TIMESTAMP,))
    @test stage_pipewire_array!(negotiated.owner,1,image;identity=negotiated.identity(100),sequence=UInt64(8),
        pts=Int64(9),duration=UInt64(10),optical_classification=OPTICAL_FRINGED)
    negotiated.source_process(negotiated.output)
    @test negotiated.output.queued==1 && !negotiated.source.failed[]
    @test negotiated.buffer.metadata.identity===nothing && negotiated.buffer.metadata.duration===nothing
    @test !negotiated.buffer.metadata.has_trigger_classification && negotiated.buffer.metadata.optical_classification===nothing
    @test negotiated.buffer.metadata.reference_timestamp!==nothing

    omitted=array_fixture((2,3);metadata=optional,allocated=())
    @test stage_pipewire_array!(omitted.owner,1,image;sequence=UInt64(9),pts=Int64(9))
    omitted.source_process(omitted.output)
    @test omitted.output.queued==1 && omitted.buffer.metadata.reference_timestamp===nothing

    missing=array_fixture((2,3);metadata=reference,allocated=())
    @test stage_pipewire_array!(missing.owner,1,image;sequence=UInt64(9),pts=Int64(9))
    missing.source_process(missing.output)
    @test missing.source.failed[] && missing.output.queued==0 && missing.output.returned==1
    @test all(iszero,missing.buffer.memory)
end

function _array_write_allocation(process)
    ArrayTransport._write_array!(process)
    return @allocated ArrayTransport._write_array!(process)
end
@testset "warmed array publication metadata allocates no heap" begin
    f=array_fixture((2,3))
    stage_pipewire_array!(f.owner,1,ones(Float32,2,3);identity=f.identity(1),pts=Int64(7),duration=UInt64(10))
    @test _array_write_allocation(f.source_process)==0
end
