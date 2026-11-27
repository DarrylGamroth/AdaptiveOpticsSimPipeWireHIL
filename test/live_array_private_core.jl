# Uses the existing isolated daemon fixture; never connects to the desktop core.
function _test_array_live_delivery(remote)
    _test_array_unlinked_live_delivery(remote)
    for properties_capacity in (1,)
        @testset "standalone arrays properties=$properties_capacity" begin
            format = NdArrayFormat(NdArray.F32_LE, (2, 3, 4); layout=NdArray.ROW_MAJOR,
                rate=SPA.Fraction(1000, 1))
            options = Tuple(PipeWireArrayStreamOptions(node_name=name, media_name=name,
                schema="test.asymmetric-array/1", format=format, metadata=TEST_ARRAY_SENSOR_METADATA,

                properties=Dict("test.axis-order" => "plane,row,column",
                    "test.pts" => "caller model nanoseconds")) for name in ("array-producer", "array-consumer"))
            retained = Ref{Any}(nothing)
            domain = AcquisitionDomain(ntuple(i -> UInt8(i), Val(ACQUISITION_DOMAIN_SIZE)))
            array_identity(sequence, generation=1) = AcquisitionIdentity(domain, UInt64(generation), UInt64(sequence))
            with_array_streams(options; remote=remote, input=(false, true)) do owner
                retained[] = owner
                source, sink = getproperty.(owner.streams, :stream)
                registry = link = nothing
                try
                    registry = with_array_stream(owner, 1) do _
                        Registry(owner.core)
                    end
                    _wait_for_hil_condition() do
                        roundtrip(registry)
                        return length(find_globals(registry; interface="PipeWire:Interface:Port",
                            properties=("node.id" => string(node_id(source)), "port.direction" => "out"))) == 1 &&
                            length(find_globals(registry; interface="PipeWire:Interface:Port",
                            properties=("node.id" => string(node_id(sink)), "port.direction" => "in"))) == 1
                    end
                    output = only(find_globals(registry; interface="PipeWire:Interface:Port",
                        properties=("node.id" => string(node_id(source)), "port.direction" => "out")))
                    input = only(find_globals(registry; interface="PipeWire:Interface:Port",
                        properties=("node.id" => string(node_id(sink)), "port.direction" => "in")))
                    link = with_array_stream(owner, 1) do _
                        create_object(owner.core, "link-factory", Link; properties=Dict(
                            "link.output.node" => string(node_id(source)), "link.output.port" => string(output.id),
                            "link.input.node" => string(node_id(sink)), "link.input.port" => string(input.id),
                            "object.linger" => "false"))
                    end
                    _wait_for_hil_condition() do
                        with_array_stream(owner, 1) do stream
                            is_driving(stream) && stream_state(sink) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING
                        end
                    end
                    array = reshape(Float32.(1:24), 2, 3, 4)
                    expected = copy(array)
                    function publish(sequence; generation=1)
                        trigger_state = ACQUISITION_TRIGGER_HIGH
                        @test stage_pipewire_array!(owner, 1, array;
                            identity=array_identity(sequence, generation), pts=Int64(100 * sequence), duration=UInt64(100),trigger_state,
                            optical_classification=properties_capacity > 0 ? OPTICAL_FRINGED : nothing)
                        _wait_for_hil_condition() do
                            trigger_process!(owner)
                            array_stream_status(owner, 2).pending
                        end
                    end
                    publish(1)
                    destination = similar(array)
                    receipt = drain_pipewire_array!(destination, owner, 2)
                    @test destination == expected
                    @test receipt.identity == array_identity(1)
                    @test receipt.pts == 100
                    @test receipt.duration == 100
                    @test receipt.lost_samples == 0
                    @test receipt.optical_classification === (properties_capacity > 0 ? OPTICAL_FRINGED : nothing)
                    @test receipt.trigger_state == (ACQUISITION_TRIGGER_HIGH)
                    # Do not drain the consumer while publishing more complete arrays.
                    publish(2)
                    fill!(array, 7)
                    @test stage_pipewire_array!(owner, 1, array;
                        identity=array_identity(3), pts=Int64(300), duration=UInt64(100))
                    _wait_for_hil_condition() do
                        with_array_stream(owner, 1) do stream
                            trigger_process!(stream)
                            owner.streams[2].state.identity == array_identity(3)
                        end
                    end
                    receipt = drain_pipewire_array!(destination, owner, 2)
                    @test all(==(7), destination)
                    @test receipt.identity == array_identity(3)
                    @test receipt.lost_samples == 1
                    @test array_stream_status(owner, 2).dropped == 1
                    publish(1; generation=2) # Consumer admits a producer restart without a control signal.
                    receipt = drain_pipewire_array!(destination, owner, 2)
                    @test receipt.identity == array_identity(1, 2)
                    @test receipt.lost_samples == 1
                    @test array_stream_status(owner, 2).gaps == 0
                    # Queue one native packet whose only discontinuity is Header DISCONT.
                    # Public buffer APIs deliberately bypass normal source staging,
                    # which always produces a clean Header.
                    rejected = array_stream_status(owner, 2).rejected
                    with_array_stream(owner, 1) do stream
                        buffer = StreamBuffer()
                        @test dequeue_buffer!(buffer, stream)
                        queued = false
                        try
                            data = buffer_data(buffer)
                            fill!(reinterpret(Float32, buffer_memory(data, 4 * length(array))), 7f0)
                            set_chunk!(data; offset=0, size=4 * length(array), stride=16)
                            set_buffer_header!(buffer, BufferHeader(SPA.META_HEADER_FLAG_DISCONT, 0, 200, 0, 2))
                            initialize_metadata!(buffer)
                            set_buffer_header!(buffer, BufferHeader(SPA.META_HEADER_FLAG_DISCONT, 0, 200, 0, 2))
                            metadata=writable_metadata_records(buffer)
                            set_acquisition_identity!(metadata, array_identity(2, 2))
                            set_exposure_timing!(metadata,ExposureTiming(;duration_nanoseconds=100))
                            set_reference_timestamp!(metadata,reference_timestamp_now())
                            queue_buffer!(buffer, stream)
                            queued = true
                        finally
                            queued || return_buffer!(buffer, stream)
                        end
                    end
                    _wait_for_hil_condition() do
                        with_array_stream(trigger_process!, owner, 1)
                        array_stream_status(owner, 2).rejected > rejected
                    end
                    @test drain_pipewire_array!(destination, owner, 2) === nothing
                    publish(3; generation=2)
                    receipt = drain_pipewire_array!(destination, owner, 2)
                    @test receipt.identity == array_identity(3, 2)
                    @test receipt.lost_samples == 2
                    @test array_stream_status(owner, 2).gaps == 1
                    reset_pipewire_arrays!(owner; identity=array_identity(0, 3))
                    @test drain_pipewire_array!(destination, owner, 2) === nothing
                    @test !stage_pipewire_array!(owner, 1, array;
                        identity=array_identity(4), pts=Int64(400), duration=UInt64(100))
                    @test_throws ArgumentError reset_pipewire_arrays!(owner; identity=array_identity(0, 3))
                    publish(1; generation=3)
                    receipt = drain_pipewire_array!(destination, owner, 2)
                    @test receipt.identity == array_identity(1, 3)
                    @test receipt.lost_samples == 0
                    let
                        for (sequence, trigger_state) in enumerate((ACQUISITION_TRIGGER_LOW, ACQUISITION_TRIGGER_MIXED, ACQUISITION_TRIGGER_ABSENT))
                            @test stage_pipewire_array!(owner,1,array;identity=array_identity(sequence+1,3),
                                pts=Int64(100*(sequence+1)),duration=UInt64(100),trigger_state)
                            _wait_for_hil_condition() do
                                with_array_stream(trigger_process!,owner,1)
                                array_stream_status(owner,2).pending
                            end
                            receipt = drain_pipewire_array!(destination,owner,2)
                            @test receipt.trigger_state == trigger_state
                            @test receipt.identity == array_identity(sequence+1,3)
                            @test all(==(7),destination)
                            @test receipt.lost_samples == 0
                        end
                    end
                    @test !array_stream_status(owner, 1).failed
                    @test !array_stream_status(owner, 2).failed
                finally
                    with_array_stream(owner, 1) do _
                        link === nothing || close(link)
                        registry === nothing || close(registry)
                    end
                end
            end
            owner = retained[]
            @test owner.closed
            @test all(item -> !isopen(item.stream), owner.streams)
            @test all(resource -> !isopen(resource), (owner.core, owner.context, owner.loop))
            @test_throws InvalidStateException array_stream_status(owner, 1)
            @test_throws InvalidStateException trigger_process!(owner)
            @test_throws ErrorException with_array_streams(options; remote=remote, input=(false, true)) do owner
                retained[] = owner
                error("injected owner failure")
            end
            @test retained[].closed
            @test all(item -> !isopen(item.stream), retained[].streams)
            @test all(resource -> !isopen(resource), (retained[].core, retained[].context, retained[].loop))
        end
    end
end

function _test_array_unlinked_live_delivery(remote)
    @testset "unlinked output and late consumer" begin
        format = NdArrayFormat(NdArray.F32_LE, (2, 3, 4); layout=NdArray.ROW_MAJOR)
        options = Tuple(PipeWireArrayStreamOptions(node_name=name, media_name=name,
            schema="test.late-linked-array/1", format=format,metadata=TEST_ARRAY_SENSOR_METADATA)
            for name in ("late-array-producer", "late-array-consumer"))
        domain = AcquisitionDomain(ntuple(i -> UInt8(i), Val(ACQUISITION_DOMAIN_SIZE)))
        identity(sequence) = AcquisitionIdentity(domain, UInt64(1), UInt64(sequence))
        retained = Ref{Any}(nothing)
        with_array_streams(options; remote, input=(false, true)) do owner
            retained[] = owner
            source, sink = getproperty.(owner.streams, :stream)
            registry = link = nothing
            try
                registry = with_array_stream(owner, 1) do _
                    Registry(owner.core)
                end
                _wait_for_hil_condition() do
                    roundtrip(registry)
                    return length(find_globals(registry; interface="PipeWire:Interface:Port",
                        properties=("node.id" => string(node_id(source)), "port.direction" => "out"))) == 1 &&
                        length(find_globals(registry; interface="PipeWire:Interface:Port",
                        properties=("node.id" => string(node_id(sink)), "port.direction" => "in"))) == 1
                end
                array = reshape(Float32.(1:24), 2, 3, 4)
                for sequence in 1:8
                    @test stage_pipewire_array!(owner, 1, array;
                        identity=identity(sequence), pts=Int64(sequence), duration=UInt64(1))
                    with_array_stream(trigger_process!, owner, 1)
                    roundtrip(registry)
                    @test !array_stream_status(owner, 1).failed
                    @test with_array_stream(owner, 1) do stream
                        stream_state(stream) != PipeWireAO.LibPipeWire.PW_STREAM_STATE_ERROR
                    end
                    @test drain_pipewire_array!(similar(array), owner, 2) === nothing
                end
                output = only(find_globals(registry; interface="PipeWire:Interface:Port",
                    properties=("node.id" => string(node_id(source)), "port.direction" => "out")))
                input = only(find_globals(registry; interface="PipeWire:Interface:Port",
                    properties=("node.id" => string(node_id(sink)), "port.direction" => "in")))
                link = with_array_stream(owner, 1) do _
                    create_object(owner.core, "link-factory", Link; properties=Dict(
                        "link.output.node" => string(node_id(source)), "link.output.port" => string(output.id),
                        "link.input.node" => string(node_id(sink)), "link.input.port" => string(input.id),
                        "object.linger" => "false"))
                end
                _wait_for_hil_condition() do
                    with_array_stream(owner, 1) do stream
                        is_driving(stream) && stream_state(sink) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING
                    end
                end
                array .+= 100f0
                @test stage_pipewire_array!(owner, 1, array;
                    identity=identity(9), pts=Int64(900), duration=UInt64(17))
                _wait_for_hil_condition() do
                    with_array_stream(owner, 1) do stream
                        trigger_process!(stream)
                        owner.streams[2].state.identity == identity(9)
                    end
                end
                destination = similar(array)
                receipt = drain_pipewire_array!(destination, owner, 2)
                @test destination == array
                @test receipt.identity == identity(9)
                @test receipt.pts == 900
                @test receipt.duration == 17
                @test !array_stream_status(owner, 1).failed
                @test !array_stream_status(owner, 2).failed
                # Stop both endpoints before resetting the input cohort. Each
                # stream retains its own domain instead of a shared reset domain.
                with_array_stream(stream->set_active!(stream,false),owner,1)
                with_array_stream(stream->set_active!(stream,false),owner,2)
                @test reset_pipewire_array!(owner,1) === owner
                replacement_domain = AcquisitionDomain(ntuple(i->UInt8(i+16),16))
                replacement = AcquisitionIdentity(replacement_domain,UInt64(1),UInt64(1))
                @test_throws ArgumentError reset_pipewire_array!(owner,2;identity=replacement)
                @test reset_pipewire_array!(owner,2) === owner
                @test drain_pipewire_array!(destination,owner,2) === nothing
                @test stage_pipewire_array!(owner,1,array;identity=replacement,pts=Int64(0),duration=UInt64(2))
                with_array_stream(stream->set_active!(stream,true),owner,1)
                with_array_stream(stream->set_active!(stream,true),owner,2)
                received_reset = Ref(false)
                _wait_for_hil_condition() do
                    with_array_stream(trigger_process!,owner,1)
                    new_receipt = drain_pipewire_array!(destination,owner,2)
                    received_reset[] = new_receipt !== nothing && new_receipt.identity == replacement
                end
                @test received_reset[] && destination == array
            finally
                with_array_stream(owner, 1) do _
                    link === nothing || close(link)
                    registry === nothing || close(registry)
                end
            end
        end
        @test retained[].closed
        @test all(item -> !isopen(item.stream), retained[].streams)
        @test all(resource -> !isopen(resource), (retained[].core, retained[].context, retained[].loop))
    end
end
