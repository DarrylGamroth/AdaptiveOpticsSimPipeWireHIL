mutable struct FakeHILResource
    name::Symbol
    events::Vector{Symbol}
    open::Bool
    running::Bool
    fail_close::Bool
    fail_lock::Bool
end

FakeHILResource(name, events; running=false) =
    FakeHILResource(name, events, true, running, false, false)

Base.isopen(resource::FakeHILResource) = resource.open
PipeWireAO.isrunning(resource::FakeHILResource) = resource.running

function Base.close(resource::FakeHILResource)
    resource.open || return nothing
    push!(resource.events, resource.name)
    resource.open = false
    resource.running = false
    resource.fail_close && throw(PipeWireHILError("cleanup", "$(resource.name) close failed"))
    return nothing
end

function PipeWireAO.stop!(resource::FakeHILResource)
    push!(resource.events, :join)
    resource.running = false
    return resource
end

function PipeWireAO.with_thread_loop_lock(f, resource::FakeHILResource)
    resource.fail_lock && throw(PipeWireHILError("cleanup", "native lock failed"))
    push!(resource.events, :lock)
    try
        return f(resource)
    finally
        push!(resource.events, :unlock)
    end
end

mutable struct FakeHILOwner
    loop::FakeHILResource
    context::FakeHILResource
    core::FakeHILResource
    frame_stream::FakeHILResource
    command_stream::FakeHILResource
    started::Bool
    closed::Bool
    callback_error::Union{Nothing,PipeWireHILError}
end

function fake_hil_owner(callback_error=nothing)
    events = Symbol[]
    return FakeHILOwner(
        FakeHILResource(:loop, events; running=true),
        FakeHILResource(:context, events), FakeHILResource(:core, events),
        FakeHILResource(:frame, events), FakeHILResource(:command, events),
        true, false, callback_error,
    )
end

function PipeWireAO.stop!(prepared::FakeHILOwner)
    push!(prepared.loop.events, :deactivate)
    prepared.callback_error === nothing || throw(prepared.callback_error)
    prepared.started = false
    return prepared
end

@testset "teardown after callback or child cleanup error" begin
    callback_error = PipeWireHILError("callback", "stored process failure")
    prepared = fake_hil_owner(callback_error)
    observed = try
        AdaptiveOpticsSimPipeWireHIL._close_pipewire_hil!(prepared)
    catch exception
        exception
    end
    @test observed === callback_error
    @test prepared.loop.events == [:deactivate, :lock, :command, :frame, :core, :context, :unlock, :join, :loop]
    @test prepared.closed
    @test !prepared.started
    @test all(resource -> !isopen(resource), (
        prepared.loop, prepared.context, prepared.core, prepared.frame_stream, prepared.command_stream,
    ))
    events = copy(prepared.loop.events)
    @test AdaptiveOpticsSimPipeWireHIL._close_pipewire_hil!(prepared) === nothing
    @test prepared.loop.events == events

    prepared = fake_hil_owner(callback_error)
    prepared.command_stream.fail_close = true
    observed = try
        AdaptiveOpticsSimPipeWireHIL._close_pipewire_hil!(prepared)
    catch exception
        exception
    end
    @test observed isa CompositeException
    @test first(observed.exceptions) === callback_error
    @test length(observed.exceptions) == 2
    @test prepared.closed
    @test prepared.loop.events == [:deactivate, :lock, :command, :frame, :core, :context, :unlock, :join, :loop]

    prepared = fake_hil_owner()
    prepared.loop.fail_lock = true
    @test_throws PipeWireHILError AdaptiveOpticsSimPipeWireHIL._close_pipewire_hil!(prepared)
    @test prepared.closed
    @test prepared.loop.events == [:deactivate, :join, :command, :frame, :core, :context, :loop]
end

@testset "partial construction unwind" begin
    for count in 0:5
        events = Symbol[]
        resources = ntuple(index -> index <= count ? FakeHILResource(
            (:loop, :context, :core, :frame, :command)[index], events,
        ) : nothing, 5)
        errors = AdaptiveOpticsSimPipeWireHIL._close_pipewire_resources!(resources...)
        @test isempty(errors)
        expected = count == 0 ? Symbol[] : [reverse([:context, :core, :frame, :command][1:max(count - 1, 0)]); :join; :loop]
        @test events == expected
        @test all(resource -> resource === nothing || !isopen(resource), resources)
    end
end

@testset "optional observation resource unwind" begin
    events = Symbol[]
    resources = map(name -> FakeHILResource(name, events),
        (:loop, :context, :core, :frame, :command, :observation))
    @test isempty(AdaptiveOpticsSimPipeWireHIL._close_pipewire_resources!(resources...))
    @test events == [:observation, :command, :frame, :core, :context, :join, :loop]
    @test all(resource -> !isopen(resource), resources)
end

@testset "optional image resource unwind" begin
    events = Symbol[]
    resources = map(name -> FakeHILResource(name, events),
        (:loop, :context, :core, :frame, :command, :observation))
    images = (FakeHILResource(:image1, events), FakeHILResource(:image2, events))
    @test isempty(AdaptiveOpticsSimPipeWireHIL._close_pipewire_resources!(resources..., images))
    @test events == [:image2, :image1, :observation, :command, :frame, :core, :context, :join, :loop]
    @test all(resource -> !isopen(resource), (resources..., images...))
end

@testset "independent image group closes all resources after stream failure" begin
    events = Symbol[]
    loop = FakeHILResource(:image_loop, events; running=true)
    context = FakeHILResource(:image_context, events)
    core = FakeHILResource(:image_core, events)
    images = (FakeHILResource(:image1, events), FakeHILResource(:image2, events))
    images[1].fail_close = true
    errors = AdaptiveOpticsSimPipeWireHIL._close_pipewire_resources!(
        loop, context, core, nothing, nothing, nothing, images)
    @test length(errors) == 1
    @test only(errors) isa PipeWireHILError
    @test events == [:lock, :image2, :image1, :image_core, :image_context, :unlock, :join, :image_loop]
    @test all(resource -> !isopen(resource), (images..., core, context, loop))
end

mutable struct CalibrationActivationTestStream
    active::Bool
    fail::Bool
    activations::Int
end
function PipeWireAO.set_active!(stream::CalibrationActivationTestStream, active::Bool)
    stream.active = active
    stream.activations += 1
    active && stream.fail && error("injected command activation failure")
    return nothing
end

@testset "named command activation rolls back every port after a partial start" begin
    streams = (woofer=CalibrationActivationTestStream(false, false, 0),
        tweeter=CalibrationActivationTestStream(false, true, 0),
        tiptilt=CalibrationActivationTestStream(false, false, 0))
    @test_throws ErrorException AdaptiveOpticsSimPipeWireHIL._set_command_active!(streams, true)
    @test all(stream -> !stream.active, streams)
    @test streams.woofer.activations == streams.tweeter.activations == 2
    @test streams.tiptilt.activations == 2
    streams.tweeter.fail = false
    AdaptiveOpticsSimPipeWireHIL._set_command_active!(streams, true)
    @test all(stream -> stream.active, streams)
    AdaptiveOpticsSimPipeWireHIL._set_command_active!(streams, false)
    @test all(stream -> !stream.active, streams)
end
