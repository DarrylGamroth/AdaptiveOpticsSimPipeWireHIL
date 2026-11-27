const HILTransport = AdaptiveOpticsSimPipeWireHIL

@testset "cold source buffer cardinality" begin
    format = NdArrayFormat(NdArray.U16_LE, (2, 3); layout=NdArray.ROW_MAJOR)
    native = PipeWireAO.LibPipeWire
    for count in (2, 3, 64)
        @test HILTransport._frame_buffer_count(count) == count
        parameters = HILTransport._stream_parameters(format, "test.detector/1"; buffers=count)
        object = pod_value(SPA.Parameter, parameters[2]).object
        @test pod_value(Int32, object[native.SPA_PARAM_BUFFERS_buffers].value) == count
        @test pod_value(Int32, object[native.SPA_PARAM_BUFFERS_size].value) == 12
        @test length(parameters) == 6
    end
    default = pod_value(SPA.Parameter, HILTransport._stream_parameters(format, "test.detector/1")[2]).object
    @test pod_value(Int32, default[native.SPA_PARAM_BUFFERS_buffers].value) == 2
    for invalid in (true, false, 1, 0, -1, 65, typemax(UInt64), 3.0, "3", nothing)
        @test_throws PipeWireHILError HILTransport._frame_buffer_count(invalid)
        @test_throws PipeWireHILError HILTransport._stream_parameters(format, "test.detector/1"; buffers=invalid)
    end
end

function transport_plane_fixture(storage)
    native = PipeWireAO.LibPipeWire
    chunk = Ref(native.spa_chunk(UInt32(0), UInt32(0), Int32(0), UInt32(0)))
    planes = native.spa_data[native.spa_data(
        UInt32(native.SPA_DATA_MemPtr), UInt32(0), Int64(-1), UInt32(0),
        UInt32(sizeof(storage)), Ptr{Cvoid}(pointer(storage)),
        Base.unsafe_convert(Ptr{native.spa_chunk}, chunk),
    )]
    spa_buffer = Ref(native.spa_buffer(UInt32(0), UInt32(1), C_NULL, pointer(planes)))
    native_buffer = Ref(native.pw_buffer(
        Base.unsafe_convert(Ptr{native.spa_buffer}, spa_buffer),
        C_NULL, UInt64(0), UInt64(0), UInt64(0),
    ))
    buffer = StreamBuffer(Base.unsafe_convert(Ptr{native.pw_buffer}, native_buffer))
    return (; storage, chunk, planes, spa_buffer, native_buffer, buffer)
end

function outstanding_transport_state(; command=zeros(Float32, 3), scale=1.0f-6)
    state = HILTransport.ExchangeState(zeros(Float32, 2), command, UInt64(1), :float32, scale)
    state.frame_ready[] = UInt64(2)
    state.frame_published[] = UInt64(2)
    HILTransport._record_frame_publication!(state, UInt64(2), Int64(100))
    return state
end

@testset "borrowed frame and command buffers" begin
    for (encoding, element) in ((:float32, Float32), (:uint16, UInt16))
        state = HILTransport.ExchangeState(
            reshape(Float32.(1:6), 2, 3), zeros(Float32, 3), UInt64(1), encoding,
        )
        HILTransport._stage_frame_transport!(state)
        storage = zeros(UInt8, state.frame_bytes + 8)
        fixture = transport_plane_fixture(storage)
        GC.@preserve fixture storage begin
            data = buffer_data(fixture.buffer)
            HILTransport._copy_frame!(state, data)
            @test reinterpret(element, bytes(data)) == state.frame_transport
            @test chunk_info(data).size == state.frame_bytes
            @test chunk_info(data).stride == size(state.frame, ndims(state.frame)) * sizeof(element)
            @test all(iszero, buffer_memory(data)[(state.frame_bytes + 1):end])
        end
        too_small = transport_plane_fixture(zeros(UInt8, state.frame_bytes - 1))
        GC.@preserve too_small begin
            @test_throws PipeWireHILError HILTransport._copy_frame!(state, buffer_data(too_small.buffer))
        end
    end

    state = outstanding_transport_state(; command=Float32[7, 8, 9])
    storage = zeros(UInt8, state.command_bytes + 8)
    fixture = transport_plane_fixture(storage)
    GC.@preserve fixture storage begin
        data = buffer_data(fixture.buffer)
        # Read a bounded, nonzero-offset command chunk through PipeWireAO.
        copyto!(buffer_memory(data), 5, reinterpret(UInt8, Float32[1, 2, 3]), 1, state.command_bytes)
        set_chunk!(data; offset=4, size=state.command_bytes, stride=sizeof(Float32))
        incoming = HILTransport._command_values(state, data)
        @test incoming == Float32[1, 2, 3]
        @test state.command == Float32[7, 8, 9]
        receive_test_command!(state, incoming, UInt64(2), Int64(120))
        @test state.command == Float32[1, 2, 3] .* 1.0f-6
        set_chunk!(data; size=state.command_bytes - 1)
        @test_throws PipeWireHILError HILTransport._command_values(state, data)
        fixture.chunk[] = PipeWireAO.LibPipeWire.spa_chunk(
            UInt32(12), UInt32(state.command_bytes), Int32(4), UInt32(0),
        )
        @test_throws PipeWireHILError HILTransport._command_values(state, data)
    end
end

function frame_copy_allocations(state, data)
    HILTransport._copy_frame!(state, data)
    return @allocated HILTransport._copy_frame!(state, data)
end

@testset "packed frame chunk stride" begin
    for shape in ((6,), (2, 3), (2, 3, 4), (352, 352))
        for (encoding, element) in ((:float32, Float32), (:uint16, UInt16))
            frame = reshape(Float32.(mod.(0:(prod(shape) - 1), 65536)), shape)
            state = HILTransport.ExchangeState(frame, zeros(Float32, 1), UInt64(1), encoding)
            @test state.frame_chunk_stride == last(shape) * sizeof(element)
            HILTransport._stage_frame_transport!(state)
            fixture = transport_plane_fixture(zeros(UInt8, state.frame_bytes))
            GC.@preserve fixture begin
                data = buffer_data(fixture.buffer)
                HILTransport._copy_frame!(state, data)
                @test chunk_info(data).stride == last(shape) * sizeof(element)
                @test chunk_info(data).size == state.frame_bytes
                @test reinterpret(element, bytes(data)) == state.frame_transport
                @test frame_copy_allocations(state, data) == 0
            end
        end
    end
end

@testset "ndarray layout chunk stride" begin
    for (layout, shape, stride) in (
        (NdArray.ROW_MAJOR, (6,), 12),
        (NdArray.COLUMN_MAJOR, (6,), 12),
        (NdArray.ROW_MAJOR, (2, 3), 6),
        (NdArray.COLUMN_MAJOR, (2, 3), 4),
        (NdArray.ROW_MAJOR, (2, 3, 4), 8),
        (NdArray.COLUMN_MAJOR, (2, 3, 4), 4),
    )
        format = NdArrayFormat(NdArray.U16_LE, shape; layout)
        @test HILTransport._frame_chunk_stride(format) === Int32(stride)
    end
    oversized_row = NdArrayFormat(NdArray.U16_LE, (1, typemax(Int32)); layout=NdArray.ROW_MAJOR)
    oversized_column = NdArrayFormat(NdArray.U16_LE, (typemax(Int32), 1); layout=NdArray.COLUMN_MAJOR)
    @test_throws PipeWireHILError HILTransport._frame_chunk_stride(oversized_row)
    @test_throws PipeWireHILError HILTransport._frame_chunk_stride(oversized_column)
end

@testset "Float32 and UInt16 frame encoding" begin
    frame = reshape(Float32[0, 0.5, 1.5, 2.5, 65534.5, 65535], 2, 3)
    original = copy(frame)
    float_state = HILTransport.ExchangeState(frame, zeros(Float32, 1), UInt64(1))
    integer_state = HILTransport.ExchangeState(frame, zeros(Float32, 1), UInt64(1), :uint16)
    @test float_state.frame_transport isa Vector{Float32}
    @test integer_state.frame_transport isa Vector{UInt16}
    @test float_state.frame_bytes == 24
    @test integer_state.frame_bytes == 12
    HILTransport._stage_frame_transport!(float_state)
    HILTransport._stage_frame_transport!(integer_state)
    @test float_state.frame_transport == Float32[0, 1.5, 65534.5, 0.5, 2.5, 65535]
    @test integer_state.frame_transport == UInt16[0, 2, 65534, 0, 2, 65535]
    @test frame == original

    vector_state = HILTransport.ExchangeState(Float32[0, 65535], zeros(Float32, 1), UInt64(1), :uint16)
    HILTransport._stage_frame_transport!(vector_state)
    @test vector_state.frame_transport == UInt16[0, 65535]
    for value in (NaN32, Inf32, -Inf32, -0.01f0, 65535.5f0)
        vector_state.frame[end] = value
        @test_throws PipeWireHILError HILTransport._stage_frame_transport!(vector_state)
        @test vector_state.frame_ready[] == 0
    end

    cube_state = HILTransport.ExchangeState(reshape(Float32.(1:8), 2, 2, 2), zeros(Float32, 1), UInt64(1), :uint16)
    HILTransport._stage_frame_transport!(cube_state)
    @test cube_state.frame_transport == UInt16[1, 5, 3, 7, 2, 6, 4, 8]

    rate = SPA.Fraction(997, 1)
    for (element, encoding, payload) in ((Float32, NdArray.F32_LE, 24), (UInt16, NdArray.U16_LE, 12))
        format = HILTransport._format(frame, rate, element)
        @test format.element_type == encoding
        @test format.shape == (Int32(2), Int32(3))
        @test format.layout == NdArray.ROW_MAJOR
        @test format.rate == rate
        @test payload_size(format) == payload
    end
end

@testset "command units and validation before mutation" begin
    state = outstanding_transport_state()
    incoming = Float32[1, -2, 3]
    receive_test_command!(state, incoming, UInt64(2), Int64(120))
    @test state.command == incoming .* 1.0f-6
    @test incoming == Float32[1, -2, 3]
    @test state.command_ready[] == 2
    @test HILTransport._frame_command_timing(state) == PipeWireFrameCommandTiming(UInt64(2), 100, 120)

    unity = outstanding_transport_state(; scale=1.0f0)
    receive_test_command!(unity, incoming, UInt64(2), Int64(120))
    @test unity.command == incoming

    matrix = outstanding_transport_state(; command=zeros(Float32, 2, 3))
    receive_test_command!(matrix, Float32[1, 3, 5, 2, 4, 6], UInt64(2), Int64(120))
    @test matrix.command == reshape(Float32.(1:6), 2, 3) .* 1.0f-6

    for values in (Float32[1, 2, NaN], Float32[1, 2, Inf], Float32[1, 2, -Inf], Float32[1, 2], Float32[1, 2, 3, 4])
        invalid = outstanding_transport_state(; command=Float32[7, 8, 9])
        @test_throws PipeWireHILError receive_test_command!(invalid, values, UInt64(2), Int64(120))
        @test invalid.command == Float32[7, 8, 9]
        @test invalid.command_ready[] == 0
        @test HILTransport._frame_command_timing(invalid) === nothing
    end
    overflow = outstanding_transport_state(; command=Float32[7, 8, 9], scale=2.0f0)
    @test_throws PipeWireHILError receive_test_command!(overflow, Float32[1, 2, floatmax(Float32)], UInt64(2), Int64(120))
    @test overflow.command == Float32[7, 8, 9]

    for sequence in UInt64[0, 1, 3]
        invalid = outstanding_transport_state(; command=Float32[7, 8, 9])
        @test_throws PipeWireHILError receive_test_command!(invalid, incoming, sequence, Int64(120))
        @test invalid.command == Float32[7, 8, 9]
        @test invalid.command_ready[] == 0
    end
    invalid_timing = outstanding_transport_state(; command=Float32[7, 8, 9])
    @test_throws PipeWireHILError receive_test_command!(invalid_timing, incoming, UInt64(2), Int64(99))
    @test invalid_timing.command == Float32[7, 8, 9]

    received = copy(state.command)
    @test_throws PipeWireHILError receive_test_command!(state, zeros(Float32, 3), UInt64(2), Int64(130))
    @test state.command == received
    state.command_ready[] = UInt64(0) # Owner has adopted the command.
    @test_throws PipeWireHILError receive_test_command!(state, zeros(Float32, 3), UInt64(2), Int64(130))
    @test state.command == received

    unpublished = outstanding_transport_state(; command=Float32[7, 8, 9])
    unpublished.timing_source_sequence[] = UInt64(0)
    @test_throws PipeWireHILError receive_test_command!(unpublished, incoming, UInt64(2), Int64(120))
    @test unpublished.command == Float32[7, 8, 9]
end

@testset "quiescent transport reset" begin
    state = outstanding_transport_state()
    @test_throws PipeWireHILError HILTransport._reset_exchange_state!(state)
    @test state.acquisition_generation == 1
    state.frame_published[] = UInt64(1)
    @test_throws PipeWireHILError HILTransport._reset_exchange_state!(state)
    state.frame_published[] = UInt64(2)
    receive_test_command!(state, Float32[1, 2, 3], UInt64(2), Int64(120))
    @test_throws PipeWireHILError HILTransport._reset_exchange_state!(state)
    state.command_ready[] = UInt64(0)
    HILTransport._reset_exchange_state!(state)
    @test state.acquisition_generation == 2
    @test all(field -> getfield(state, field)[] == 0, (
        :frame_ready, :frame_published, :command_ready, :timing_source_sequence,
        :timing_source_published_nanoseconds, :timing_last_event_nanoseconds,
        :timing_completed_sequence, :timing_completed_source_published_nanoseconds,
        :timing_completed_command_received_nanoseconds,
    ))
    @test all(iszero, state.frame_transport)
    @test HILTransport._frame_command_timing(state) === nothing
    state.acquisition_generation = typemax(UInt64)
    @test_throws PipeWireHILError HILTransport._reset_exchange_state!(state)
    @test state.acquisition_generation == typemax(UInt64)
    @test hasmethod(reset_pipewire_hil!, Tuple{PreparedPipeWireHIL})
    @test hasmethod(exchange_frame!, Tuple{PreparedPipeWireHIL, Any})
end

@testset "native UInt16 frame transport preserves detector codes" begin
    frame = UInt16[0 32767 65535; 1 32768 65534]
    original = copy(frame)
    state = HILTransport.ExchangeState(frame, zeros(Float32, 2), UInt64(1), :uint16)
    @test HILTransport._stage_frame_transport!(state) === nothing
    @test state.frame_transport == UInt16[0, 32767, 65535, 1, 32768, 65534]
    @test frame == original
    @test state.frame_bytes == 12
    @test state.frame_chunk_stride == 6
    @test HILTransport._format(frame, nothing).element_type == NdArray.U16_LE
    storage = zeros(UInt8, state.frame_bytes)
    fixture = transport_plane_fixture(storage)
    GC.@preserve fixture storage begin
        HILTransport._copy_frame!(state, buffer_data(fixture.buffer))
        @test reinterpret(UInt16, bytes(buffer_data(fixture.buffer))) ==
            UInt16[0, 32767, 65535, 1, 32768, 65534]
    end
    HILTransport._stage_frame_transport!(state)
    @test (@allocated HILTransport._stage_frame_transport!(state)) == 0
    float_state = HILTransport.ExchangeState(frame, zeros(Float32, 2), UInt64(1), :float32)
    HILTransport._stage_frame_transport!(float_state)
    @test float_state.frame_transport == Float32[0, 32767, 65535, 1, 32768, 65534]
end
