const Diagnostics = Managed.OpticalObservation

function diagnostics_graph()
    resolution = 16
    schema = "test.diagnostics.opd/1"
    pupil(name) = pupil_opd_composition_node(name; resolution,
        uncompensated_opd_schema=schema, surface_opd_schema=schema, pupil_opd_schema=schema)
    sensor = shack_hartmann_rate_node(:sensor; resolution, telescope_diameter_m=8.0,
        n_lenslets=4, n_pix_subap=4, pixel_scale_arcsec=0.1, source_wavelength_m=0.75e-6,
        source_photon_irradiance_m2_s=1.0, opd_schema=schema,
        photon_rate_schema="test.diagnostics.frame/1")
    flat() = zeros(Float32, resolution, resolution)
    return algorithm_graph((pupil(:atmosphere), pupil(:pupil), sensor);
        inputs=(
            graph_input(:uncorrected, :atmosphere=>:uncompensated_opd, flat()),
            graph_input(:flat_atmosphere, :atmosphere=>:surface_opd, flat()),
            graph_input(:surface, :pupil=>:surface_opd, flat()),
        ),
        links=(link(:atmosphere=>:pupil_opd, :pupil=>:uncompensated_opd),
            link(:pupil=>:pupil_opd, :sensor=>:opd)),
        outputs=(graph_output(:pupil_opd, :pupil=>:pupil_opd),
            graph_output(:atmosphere_opd, :atmosphere=>:pupil_opd),
            graph_output(:frame, :sensor=>:photon_rate)))
end

mutable struct DiagnosticImageSink
    buffer::StreamBuffer
    wire::Vector{Float32}
    identity::Union{Nothing,AcquisitionIdentity}
    pts::Int64
end

function (sink::DiagnosticImageSink)(stream)
    dequeue_buffer!(sink.buffer, stream) || return nothing
    try
        copyto!(sink.wire, reinterpret(Float32, bytes(buffer_data(sink.buffer))))
        sink.identity = acquisition_identity(sink.buffer)
        sink.pts = buffer_header(sink.buffer).pts
    finally
        queue_buffer!(sink.buffer, stream)
    end
    return nothing
end

function with_diagnostic_sinks(f, owner, options)
    core = owner.core
    loop = owner.loop
    registry = with_thread_loop_lock(_ -> Registry(core), loop)
    streams, links = Stream[], Link[]
    captures = map(options) do option
        DiagnosticImageSink(StreamBuffer(), zeros(Float32, prod(option.shape)), nothing, 0)
    end
    try
        for (i, (option, capture)) in enumerate(zip(options, captures))
            name = "test.diagnostic.sink.$i"
            with_thread_loop_lock(loop) do _
                stream = Stream(core, name;
                    properties=Dict("node.name"=>name, "node.want-driver"=>"true", "priority.driver"=>"100"),
                    on_process=capture)
                push!(streams, stream)
                precompile(capture, (typeof(stream),))
                format = NdArrayFormat(NdArray.F32_LE, option.shape;
                    layout=NdArray.ROW_MAJOR, rate=option.rate)
                connect!(stream, :input; flags=STREAM_MAP_BUFFERS | STREAM_DRIVER,
                    params=HIL._stream_parameters(format, option.schema; buffers=option.buffers))
            end
            push!(links, connect_ports(core, registry, option.node_name, name))
        end
        function receive(generation, sequence)
            _wait_for_hil_condition() do
                with_thread_loop_lock(loop) do _
                    all(captures) do capture
                        id = capture.identity
                        id !== nothing && id.generation == generation && id.sequence == sequence
                    end && return true
                    for stream in streams
                        is_driving(stream) && trigger_process!(stream)
                    end
                    return false
                end
            end
            return with_thread_loop_lock(loop) do _
                map(capture -> (; wire=copy(capture.wire), capture.identity, capture.pts), captures)
            end
        end
        f(receive)
    finally
        with_thread_loop_lock(loop) do _
            foreach(close, reverse(links))
            foreach(close, reverse(streams))
            close(registry)
        end
    end
end

@testset "optical diagnostics publish one coherent pupil snapshot" begin
    definition = diagnostics_graph()
    graph = prepare_algorithm_graph(definition)
    boundary = prepare_graph_hil_boundary(graph; command_input=:surface, frame_output=:frame)
    observation = Diagnostics.prepare(definition, graph)
    image_options = Diagnostics.source_options(observation)
    @test length(Diagnostics.contracts().port_contracts(definition)) == 3
    @test map(options -> options.shape, image_options) == ((32, 32), (16, 16), (1, 2))
    prepared = nothing
    try
        _with_private_core() do remote
            configuration = PipeWireHILConfiguration(; remote, frame_node_name="diagnostic-frame",
                command_node_name="diagnostic-command", frame_schema="test.diagnostics.frame/1",
                command_schema="test.diagnostics.opd/1", rate=SPA.Fraction(500, 1),
                exposure_duration_ns=2_000_000)
            prepared = prepare_pipewire_hil(boundary, configuration; image_sources=image_options)
            try
                start!(prepared)
                with_diagnostic_sinks(prepared.image_owner, image_options) do receive
                    # CPU 0/1 are reserved by the deployment contract. The synchronous
                    # path still exercises delivery on runners with only those CPUs.
                    allowed = findall(!iszero, ThreadPinning.getaffinity(; cutoff=nothing)) .- 1
                    cpus = worker_cpus()
                    worker_cpu = if length(allowed) >= 2 && !isempty(cpus) &&
                            Threads.nthreads(:default) >= 2 && Threads.nthreads(:interactive) == 0
                        last(cpus)
                    end
                    if worker_cpu !== nothing
                        ThreadPinning.pinthread(first(filter(!=(worker_cpu), allowed)))
                        try
                            Diagnostics.warm!(observation, prepared; cpu=worker_cpu)
                            @test observation.completed[] == 1
                            @test !observation.failed[]
                            Diagnostics.drain!(observation)
                        finally
                            ThreadPinning.setaffinity_cpuids(allowed)
                        end
                    end
                    before = observation.completed[]
                    # Both products use the same aperture; introduce an orthogonal correction.
                    atmosphere = graph_input(graph, :uncorrected)
                    surface = graph_input(graph, :surface)
                    atmosphere .= reshape(Float32.(1:16) .* 1f-9, 16, 1)
                    surface .= reshape(Float32.(1:16) .* 2f-9, 1, 16)
                    step_graph!(graph)
                    observation.generation = frame_acquisition_generation(prepared)
                    pts = Int64(123)
                    period = Diagnostics.PERIOD_NS
                    @test Diagnostics.sample!(observation, PipeWireFrameCommandTiming(UInt64(1), pts, Int64(124)), Int64(0), Int64(0))
                    @test !Diagnostics.sample!(observation, pts, Int64(0), period - 1)
                    @test !Diagnostics.sample!(observation, pts, Int64(0), period)
                    @test observation.dropped[] == 1
                    @test Diagnostics.process_sample!(observation, prepared)
                    focal, residual, rms = receive(1, 1)
                    @test focal.identity == residual.identity == rms.identity
                    @test focal.pts == residual.pts == rms.pts == pts
                    @test all(isfinite, focal.wire) && minimum(focal.wire) >= 0 && sum(focal.wire) > 0

                    mask = observation.pupil
                    uncorrected, corrected = atmosphere[mask], (atmosphere .+ surface)[mask]
                    rms_nm(v) = sqrt(sum(abs2, v .- sum(v) / length(v)) / length(v)) * 1e9
                    @test rms.wire ≈ [rms_nm(uncorrected), rms_nm(corrected)] rtol=1e-6
                    wavefront = permutedims(reshape(residual.wire, reverse(size(mask))))
                    piston = sum(corrected) / length(corrected)
                    # The wire image is oriented with +x right and +y up.
                    physical = permutedims(reverse(wavefront; dims=1))
                    @test maximum(abs, physical[mask] .- (corrected .- piston) .* 1e9) < 2e-5
                    @test all(isnan, physical[.!mask])

                    @test Diagnostics.submit!(observation; generation=UInt64(0))
                    @test !Diagnostics.process_sample!(observation, prepared)
                    @test observation.completed[] == before + 1 && !observation.busy[]
                    @test !Diagnostics.sample!(observation, pts, Int64(0), period - 1)
                    @test observation.failed[]
                    Diagnostics.drain!(observation)
                    reset_pipewire_hil!(prepared)
                    @test frame_acquisition_generation(prepared) == 2
                    @test all(!source.state.pending for source in prepared.image_sources)
                    if worker_cpu !== nothing
                        ThreadPinning.pinthread(first(filter(!=(worker_cpu), allowed)))
                        try
                            Diagnostics.reset!(observation, prepared; cpu=worker_cpu)
                            graph_input(graph, :uncorrected) .= 0
                            graph_input(graph, :surface) .= 0
                            step_graph!(graph)
                            @test Diagnostics.sample!(observation, pts + 1, Int64(0), 2period)
                            image, wavefront, response = receive(2, 1)
                            @test image.identity == wavefront.identity == response.identity
                            @test all(iszero, response.wire)
                            @test !observation.failed[]
                        finally
                            Diagnostics.drain!(observation)
                            ThreadPinning.setaffinity_cpuids(allowed)
                        end
                    end
                end
            finally
                Diagnostics.close!(observation, prepared)
            end
            @test prepared.closed
        end
    finally
        prepared === nothing && Diagnostics.close!(observation)
    end
end
