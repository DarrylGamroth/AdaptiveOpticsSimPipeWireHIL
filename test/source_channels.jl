using JSON

@testset "image source channel descriptions" begin
    H = AdaptiveOpticsSimPipeWireHIL
    declaration = (name="first_value", index=0, title="First value", units="nm")
    channels = (declaration, (name="second_value", index=1, title="Second value", units=nothing))
    options(; shape=(1, 2), channels=channels, schema="example.pupil-opd-rms/1") =
        PipeWireImageSourceOptions(node_name="rms", media_name="wavefront-rms";
            shape, channels, schema)
    option = options()
    format = NdArrayFormat(NdArray.F32_LE, option.shape; layout=NdArray.ROW_MAJOR)
    description = H._image_channel_description(option.schema, format, option.channels)
    @test ncodeunits(description) <= 16384
    document = JSON.parse(description)
    @test document["version"] == 1
    output = only(document["outputs"])
    @test output["port"] == "output_1"
    @test output["schema"] == option.schema
    @test output["element_type"] == Int(format.element_type) == 18
    @test output["layout"] == Int(format.layout) == 1
    @test output["shape"] == [1, 2]
    @test getindex.(output["channels"], "index") == [0, 1]
    @test getindex.(output["channels"], "name") == ["first_value", "second_value"]
    @test getindex.(output["channels"], "title") == ["First value", "Second value"]
    @test getindex.(output["channels"], "units") == ["nm", nothing]
    @test H._image_channel_description(option.schema, format, ()) === nothing
    @test_throws ArgumentError PipeWireImageSourceOptions(node_name="rms", media_name="rms",
        schema=option.schema, shape=option.shape,
        properties=Dict("pipewireao.ndarray.channels" => description))
    escaped = options(channels=((; declaration..., title="Quoted \"φ\" \\ value", units="µm"),))
    @test only(only(JSON.parse(H._image_channel_description(escaped.schema, format,
        escaped.channels))["outputs"])["channels"])["title"] == "Quoted \"φ\" \\ value"
    for invalid in (
        (declaration, declaration),
        (declaration, (; declaration..., name="other")),
        (declaration, (; declaration..., index=1)),
        ((; declaration..., name="Uppercase"),),
        ((; declaration..., index=-1),),
        ((; declaration..., index=2),),
        ((; declaration..., index=true),),
        ((; declaration..., index=0.0),),
        ((; declaration..., extra="unknown"),),
        ((name="first", index=0, title="First"),),
        [declaration],
    )
        @test_throws ArgumentError options(channels=invalid)
    end
    for field in (:name, :title, :units), invalid in ("", " \t", "bad\ntext", repeat("µ", 128), String(UInt8[0xff]), 1)
        @test_throws ArgumentError options(channels=(merge(declaration, NamedTuple{(field,)}((invalid,))),))
    end
    many = ntuple(65) do index
        (name="value$index", index=index - 1, title="Value", units=nothing)
    end
    @test_throws ArgumentError options(shape=(1, 65), channels=many)
    large = ntuple(64) do index
        (name="value$index", index=index - 1, title=repeat("φ", 127), units=repeat("µ", 127))
    end
    large_option = options(shape=(1, 64), channels=large)
    large_format = NdArrayFormat(NdArray.F32_LE, large_option.shape; layout=NdArray.ROW_MAJOR)
    @test_throws ArgumentError H._image_channel_description(large_option.schema, large_format, large_option.channels)
end
