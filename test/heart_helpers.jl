const DM_SCHEMA = "org.heart.std-dm.actuator-command/1"
const RAW_WFS_SCHEMA = "org.heart.std-wfs.raw-pixels/1"
const WFS_SCHEMA = "org.heart.std-wfs.calibrated-pixels/1"
const STEPS_PER_EXPOSURE = 2
const EXPOSURE_NS = 2_000_000
const ROWS = Cushort(4)
const COLUMNS = Cushort(4)

function heart_bundle(directory, remote; frame_encoding=:float32, actuators=1, exposure_ns=EXPOSURE_NS)
    raw = frame_encoding === :uint16
    schema = raw ? RAW_WFS_SCHEMA : WFS_SCHEMA
    product = clocked_bundle(directory)
    product.root["pipewire"]["remote"] = remote
    product.root["pipewire"]["command"]["schema"] = DM_SCHEMA
    source = product.root["graphs"]["source"]
    source_path = joinpath(directory, source["path"])
    source_graph = YAML.load_file(source_path)
    source_graph["nodes"][1]["config"]["coefficients_schema"] = DM_SCHEMA
    source_graph["nodes"][1]["config"]["mode_count"] = actuators
    source["bindings"]["command"] = binding_file(directory, "heart-command", zeros(Float32, actuators))
    # Test mirror influence functions map actuator displacement to pupil OPD.
    basis = if actuators == 1
        Float32[(row - 2) * (column - 3) / 4 for row in 1:ROWS, column in 1:COLUMNS, _ in 1:1]
    else
        cat(
            Float32[(column - 2) / 2 for row in 1:ROWS, column in 1:COLUMNS],
            Float32[(row - 2) / 2 for row in 1:ROWS, column in 1:COLUMNS]; dims=3,
        )
    end
    source["bindings"]["basis"] = binding_file(directory, "heart-basis", basis)
    if raw
        # The commanded mirror belongs at the DM plane. The entrance OPD
        # stays flat and has its own source output.
        push!(source_graph["nodes"], Dict(
            "name" => "entrance", "type" => "pupil_opd_composition_f32",
            "config" => Dict(
                "resolution" => Int(ROWS), "pupil_opd_schema" => "test.opd/1",
                "uncompensated_opd_schema" => "test.opd/1",
                "surface_opd_schema" => "test.opd/1",
            ),
        ))
        for field in ("uncompensated_opd", "surface_opd")
            source["bindings"][field] = binding_file(
                directory, field, zeros(Float32, ROWS, COLUMNS)
            )
            push!(source_graph["inputs"], Dict(
                "name" => field, "destination" => "entrance." * field, "binding" => field
            ))
        end
        source_graph["outputs"][2]["source"] = "entrance.pupil_opd"
    end
    product.root["graphs"]["source"] = graph_file(
        directory, "source", source_graph, source["bindings"]
    )
    product.root["source"]["period_ns"] = exposure_ns ÷ STEPS_PER_EXPOSURE
    product.root["source"]["command_limit"] = 1e-6

    append!(product.graph["nodes"][1]["config"]["steps"], [
        Dict("op" => "lens", "focal_length_m" => 0.1),
        Dict("op" => "propagate", "distance_m" => 0.1),
    ])
    product.root["graphs"]["optical-main"] = graph_file(
        directory, "optical", product.graph, product.bindings,
    )

    product.path["exposure_duration_ns"] = exposure_ns
    optics = Simulation.prepare(Simulation.load(save(product))).science.plant.graphs.camera
    pitch = optical_sampling_m(prepared_graph_node(optics, :optics))

    # Ideal detector response with optional quantization to raw ADC codes.
    detector = add_response!(product)
    detector_config = detector["nodes"][1]["config"]
    detector_config["photon_noise"] = false
    detector_config["pixel_sampling_m"] = fill(pitch, 2)
    product.path["response"]["pixel_sampling_m"] = detector_config["pixel_sampling_m"]
    detector_config["exposure_duration_s"] = exposure_ns / 1e9
    detector_config["frame_schema"] = schema
    if raw
        detector_config["bits"] = 16
        detector_config["full_well_e"] = 65535
        product.path["sensor"]["frame_encoding"] = "uint16"
    end
    product.path["response"]["exposure_duration_s"] = detector_config["exposure_duration_s"]
    product.path["response"]["normalized_to_photon_rate"] = raw ? 5e4 : 1e6
    product.path["response"]["frame_schema"] = schema
    product.path["sensor"]["schema"] = schema
    bindings = product.root["graphs"]["response"]["bindings"]
    product.root["graphs"]["response"] = graph_file(
        directory, "response", detector, bindings
    )
    return Simulation.load(save(product))
end

function core_configuration(
    remote, dm_port, wfs_port; pixel_type="calibrated", target=7, actuators=1,
    exposure_ns=EXPOSURE_NS, wfs_source=3
)
    rate = 1_000_000_000 // exposure_ns
    config = _private_core_configuration(
        remote;
        spa_libraries="api.heart.* = heart/libspa-heart",
        modules="{ name = libpipewire-module-spa-node-factory }",
    )
    return config * """
    context.objects = [
        { factory = spa-node-factory args = {
            factory.name = api.heart.std-dm.source
            node.name = heart-dm
            node.driver = true
            api.heart.std-dm.bind-address = 127.0.0.1
            api.heart.std-dm.port = $dm_port
            api.heart.std-dm.target-id = $target
            api.heart.std-dm.actuator-count = $actuators
        } }
        { factory = spa-node-factory args = {
            factory.name = api.heart.std-wfs.sink
            node.name = heart-wfs
            api.heart.std-wfs.destination-address = 127.0.0.1
            api.heart.std-wfs.port = $wfs_port
            api.heart.std-wfs.source = $wfs_source
            api.heart.std-wfs.pixel-type = $pixel_type
            api.heart.std-wfs.width = $COLUMNS
            api.heart.std-wfs.height = $ROWS
            api.heart.std-wfs.frame-rate = "$(numerator(rate))/$(denominator(rate))"
            api.heart.std-wfs.pixels-per-datagram = $COLUMNS
            api.heart.std-wfs.network-byte-order = false
        } }
    ]
    """
end

function link_ports(loop, core, registry, output, input)
    ports = nothing
    _wait_for_hil_condition() do
        roundtrip(registry)
        endpoint(node, direction) = find_globals(
            registry; interface = "PipeWire:Interface:Port",
            properties = ("node.id" => string(node), "port.direction" => direction),
        )
        outputs, inputs = endpoint(output, "out"), endpoint(input, "in")
        isempty(outputs) || isempty(inputs) || (ports = (only(outputs), only(inputs)))
        return ports !== nothing
    end
    return with_thread_loop_lock(loop) do _
        create_object(core, "link-factory", Link; properties = Dict(
            "link.output.node" => string(output), "link.output.port" => string(ports[1].id),
            "link.input.node" => string(input), "link.input.port" => string(ports[2].id),
            "object.linger" => "false",
        ))
    end
end

function link_heart!(links, loop, core, registry, streams)
    roundtrip(registry)
    function node(name)
        return only(find_globals(
            registry; interface="PipeWire:Interface:Node", properties=("node.name" => name,)
        )).id
    end
    command = with_array_stream(node_id, streams, 1)
    sensor = with_array_stream(node_id, streams, 2)
    push!(links, link_ports(loop, core, registry, node("heart-dm"), command))
    push!(links, link_ports(loop, core, registry, sensor, node("heart-wfs")))
    _wait_for_hil_condition() do
        all(1:2) do index
            with_array_stream(stream_state, streams, index) ==
                PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING
        end
    end
    return nothing
end

function close_heart_links!(links, loop)
    with_thread_loop_lock(loop) do _
        foreach(close, reverse(links))
        empty!(links)
    end
    return nothing
end
