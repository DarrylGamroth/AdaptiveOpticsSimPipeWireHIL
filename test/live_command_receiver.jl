using Profile: Allocs

# The process-wide byte counter also includes concurrent PipeWire callbacks.
# Sample every allocation and count only the task exercising the public API.
function receiver_allocated(f::F, args::Vararg{Any,N}) where {F,N}
    task = pointer_from_objref(current_task())
    Allocs.clear()
    try
        Allocs.@profile sample_rate=1.0 f(args...)
        return sum(a.size for a in Allocs.fetch().allocs if a.task == task; init=0)
    finally
        Allocs.clear()
    end
end

function command_receiver_trigger_allocations(owner)
    trigger_process!(owner)
    return receiver_allocated(trigger_process!, owner)
end

function poll_empty_mixed_commands!(destination, owner, indices, count)
    for _ in 1:count, index in indices
        drain_pipewire_array!(destination,owner,index) === nothing ||
            error("empty command poll unexpectedly received a sample")
    end
    return nothing
end

function poll_healthy_array_streams(owner, count)
    for _ in 1:count
        AdaptiveOpticsSimPipeWireHIL._array_transport_failed(owner) &&
            error("unexpected failed array stream")
    end
    return nothing
end

@testset "synchronous optical command receiver" begin
    _with_private_core() do remote
        format = NdArrayFormat(NdArray.F32_LE, (3,); layout=NdArray.ROW_MAJOR)
        metadata = (MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP; required=true),
            MetadataRequirement(SPA.META_ACQUISITION_IDENTITY),
            MetadataRequirement(SPA.META_EXPOSURE_TIMING))
        options = map(("command-sink", "command-source")) do name
            PipeWireArrayStreamOptions(; node_name=name, media_name=name,
                schema="test.clocked-command/1", format, metadata)
        end
        with_array_streams((options[1],); remote, input=(true,),
                synchronous_command=true) do owner
            @test_throws InvalidStateException close(owner.core)
        end
        extra_input = PipeWireArrayStreamOptions(; node_name="other-sink", media_name="other-sink",
            schema="test.clocked-command/1", format, metadata)
        with_array_streams((options...,extra_input); remote, input=(true,false,true),
                active=false, synchronous_command=true) do owner
            destination = zeros(Float32,3)
            indices = [1,3]
            roundtrip(owner.core)
            receiver_allocated(poll_empty_mixed_commands!, destination, owner, indices, 512)
            @test receiver_allocated(poll_empty_mixed_commands!, destination, owner, indices, 512) == 0
            receiver_allocated(poll_healthy_array_streams, owner, 512)
            @test receiver_allocated(poll_healthy_array_streams, owner, 512) == 0
            @test_throws ArgumentError drain_pipewire_array!(destination,owner,2)
            @test_throws BoundsError drain_pipewire_array!(destination,owner,4)
        end
        retained = Ref{Any}()
        with_array_streams(options; remote, input=(true,false), active=false,
                synchronous_command=true) do owner
            retained[] = owner.streams[1].stream
            command_receiver_trigger_allocations(owner)
            @test command_receiver_trigger_allocations(owner) == 0
            domain = AcquisitionDomain(ntuple(UInt8, 16))
            identity(generation, sequence) =
                AcquisitionIdentity(domain, UInt64(generation), UInt64(sequence))
            registry = with_array_stream(owner,1) do _
                Registry(owner.core)
            end
            link = nothing
            try
                foreach(1:2) do i
                    with_array_stream(stream -> set_active!(stream,true), owner, i)
                end
                ports(i, direction) = find_globals(registry; interface="PipeWire:Interface:Port",
                    properties=("node.id" => string(with_array_stream(node_id,owner,i)),
                        "port.direction" => direction))
                _wait_for_hil_condition() do
                    roundtrip(registry)
                    length(ports(1,"in")) == length(ports(2,"out")) == 1
                end
                input, output = only(ports(1,"in")), only(ports(2,"out"))
                link = with_array_stream(owner,1) do _
                    create_object(owner.core, "link-factory", Link; properties=Dict(
                        "link.output.node" => output.properties["node.id"],
                        "link.output.port" => string(output.id),
                        "link.input.node" => input.properties["node.id"],
                        "link.input.port" => string(input.id), "object.linger" => "false"))
                end
                _wait_for_hil_condition() do
                    with_array_stream(stream_state, owner,1) ==
                        PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING
                end
                destination = zeros(Float32, 3)
                values = Float32[0.125, -0.25, 0.5]
                stamp = ReferenceTimestamp(42)
                function receive(expected, sequence)
                    @test stage_pipewire_array!(owner, 2, values; identity=expected,
                        pts=Int64(23), duration=UInt64(10), sequence=UInt64(sequence),
                        reference_timestamp=stamp)
                    _wait_for_hil_condition() do
                        trigger_process!(owner)
                        array_stream_status(owner,1).pending
                    end
                    receipt = drain_pipewire_array!(destination, owner,1)
                    @test receipt.identity == expected
                    @test receipt.sequence == sequence && receipt.pts == 23 && receipt.duration == 10
                    @test receipt.reference_timestamp == stamp
                    @test destination == values
                end
                receive(identity(1,1), 1)
                receive(identity(2,1), 2)
                foreach(1:2) do i
                    with_array_stream(stream -> set_active!(stream,false), owner, i)
                end
                @test_throws ArgumentError reset_pipewire_array!(owner,1; identity=identity(2,0))
                @test reset_pipewire_array!(owner,1; identity=identity(3,0)) === owner
                @test reset_pipewire_array!(owner,2) === owner
                @test drain_pipewire_array!(destination, owner,1) === nothing
                foreach(1:2) do i
                    with_array_stream(stream -> set_active!(stream,true), owner, i)
                end
                receive(identity(3,1), 1)
                receive(nothing, 2)
                @test array_stream_status(owner,1).rejected == 0
            finally
                with_array_stream(owner,1) do _
                    link === nothing || close(link)
                    close(registry)
                end
            end
        end
        @test !isopen(retained[])
        @test_throws InvalidStateException node_id(retained[])
        @test_throws InvalidStateException stream_state(retained[])
        @test_throws InvalidStateException set_active!(retained[])
    end
end

function stage_mixed_camera_samples!(owner, values, domain, first, count)
    for sequence in first:first+count-1
        index = isodd(sequence) ? 2 : 3
        identity = AcquisitionIdentity(domain,UInt64(1),UInt64(sequence))
        stage_pipewire_array!(owner,index,values; identity, pts=Int64(sequence),
            duration=UInt64(10), optical_classification=OPTICAL_FRINGED,
            reference_timestamp=ReferenceTimestamp(sequence)) || error("test camera staging failed")
    end
    return nothing
end

@testset "heterogeneous camera staging allocation budget" begin
    _with_private_core() do remote
        format = NdArrayFormat(NdArray.F32_LE, (3,); layout=NdArray.ROW_MAJOR)
        common = (MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP; required=true),
            MetadataRequirement(SPA.META_ACQUISITION_IDENTITY; required=true),
            MetadataRequirement(SPA.META_EXPOSURE_TIMING; required=true))
        chopped = (common..., MetadataRequirement(SPA.META_OPTICAL_CHOPPING; required=true))
        options = map(("command","camera-one","camera-two"),(common,chopped,common)) do name, metadata
            PipeWireArrayStreamOptions(; node_name=name, media_name=name,
                schema="test.clocked-camera/1", format, metadata)
        end
        with_array_streams(options;remote,input=(true,false,false),active=false,
                synchronous_command=true) do owner
            values = Float32[1,2,3]
            domain = AcquisitionDomain(ntuple(UInt8,16))
            roundtrip(owner.core)
            stage_mixed_camera_samples!(owner, values, domain, 1, 20)
            receiver_allocated(stage_mixed_camera_samples!, owner, values, domain, 21, 20)
            @test receiver_allocated(stage_mixed_camera_samples!, owner, values, domain, 41, 20) == 0
        end
    end
end
