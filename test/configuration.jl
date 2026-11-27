@testset "configuration" begin
    configuration = PipeWireHILConfiguration(
        remote="aos-hil-test",
        frame_node_name="aos-wfs",
        command_node_name="aos-command",
        frame_schema="org.adaptiveopticssim.hil.shwfs-frame.f32/1",
        command_schema="org.adaptiveopticssim.hil.pdm-command.f32/1",
        rate=SPA.Fraction(1_000, 1),
        exposure_duration_ns=1_000_000,
    )
    @test configuration.remote == "aos-hil-test"
    @test configuration.rate == SPA.Fraction(1_000, 1)
    @test configuration.timeout_ns == 5_000_000_000
    @test configuration.frame_encoding === :float32
    @test configuration.command_scale === 1.0f0

    valid = (
        remote=configuration.remote,
        frame_node_name=configuration.frame_node_name,
        command_node_name=configuration.command_node_name,
        frame_schema=configuration.frame_schema,
        command_schema=configuration.command_schema,
        rate=configuration.rate,
        timeout_ns=configuration.timeout_ns,
        exposure_duration_ns=configuration.exposure_duration_ns,
    )
    @test_throws PipeWireHILError PipeWireHILConfiguration(
        ; valid..., remote="",
    )
    @test_throws PipeWireHILError PipeWireHILConfiguration(
        ; valid..., command_node_name=configuration.frame_node_name,
    )
    @test_throws PipeWireHILError PipeWireHILConfiguration(
        ; valid..., frame_schema="bad\0schema",
    )
    @test_throws PipeWireHILError PipeWireHILConfiguration(
        ; valid..., timeout_ns=0,
    )
    @test_throws PipeWireHILError PipeWireHILConfiguration(
        ; valid..., exposure_duration_ns=0,
    )
    encoded = PipeWireHILConfiguration(; valid..., frame_encoding=:uint16, command_scale=1.0f-6)
    @test encoded.frame_encoding === :uint16
    @test encoded.command_scale === 1.0f-6
    @test_throws PipeWireHILError PipeWireHILConfiguration(; valid..., frame_encoding=:uint8)
    for scale in (0, -1, NaN, Inf, 1.0e100, 1.0e-100)
        @test_throws PipeWireHILError PipeWireHILConfiguration(; valid..., command_scale=scale)
    end
end

@testset "HIL streams declare independent required metadata" begin
    native = PipeWireAO.LibPipeWire
    format = NdArrayFormat(NdArray.F32_LE,(2,);layout=NdArray.ROW_MAJOR)
    parameters = AdaptiveOpticsSimPipeWireHIL._stream_parameters(format,"test.frame/1")
    buffers = pod_value(SPA.Parameter,parameters[2])
    metadata_mask = pod_value(Int32,only(property.value for property in buffers.object.properties
        if property.key == native.SPA_PARAM_BUFFERS_metaType))
    @test metadata_mask == Int32(1 << native.SPA_META_Header)
    @test length(parameters) == 6
    types = map(parameters[3:end]) do pod
        metadata = pod_value(SPA.Parameter,pod)
        pod_value(SPA.Id,only(property.value for property in metadata.object.properties
            if property.key == native.SPA_PARAM_META_type)).value
    end
    @test types == UInt32[SPA.META_HEADER,SPA.META_ACQUISITION_IDENTITY,SPA.META_EXPOSURE_TIMING,SPA.META_REFERENCE_TIMESTAMP]
end

@testset "cold Header presentation clock declarations" begin
    adapter=AdaptiveOpticsSimPipeWireHIL
    unknown=adapter._stream_properties("unknown")
    @test unknown["pipewireao.header.pts.clock"]=="unavailable"
    @test !haskey(unknown,"pipewireao.header.pts.absolute-conversion")
    known=adapter._stream_properties("frame";header_pts_clock="monotonic")
    @test known["pipewireao.header.pts.clock"]=="monotonic"
    @test !haskey(known,"pipewireao.header.pts.absolute-conversion")
    format=NdArrayFormat(NdArray.F32_LE,(2,3);layout=NdArray.ROW_MAJOR)
    option=PipeWireArrayStreamOptions(node_name="model",media_name="model",schema="test/1",format=format,
        properties=("pipewireao.header.pts.clock"=>"model",))
    @test Dict(option.properties)["pipewireao.header.pts.clock"]=="model"
    @test_throws ArgumentError PipeWireArrayStreamOptions(node_name="model",media_name="model",schema="test/1",format=format,
        properties=("pipewireao.header.pts.absolute-conversion"=>"available",))
end
