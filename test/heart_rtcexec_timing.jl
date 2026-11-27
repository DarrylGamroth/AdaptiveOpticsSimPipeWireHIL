# Run this opt-in timing test in a private network and shared-memory namespace.
using Proper
include("simulation_optical.jl")

module HeartExecutorTiming

using Test, Sockets, FITSIO, Statistics, TOML, Clocks
using YAML: YAML
using PipeWireAO, PipeWireAO_jll
using AdaptiveOpticsSim.AlgorithmGraphs: optical_sampling_m, prepared_graph_node
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil, array_stream_status,
    with_array_stream
using ..OpticalSimulationTests: clocked_bundle, save, binding_file, graph_file, add_response!

include("private_core_helpers.jl")
include("heart_helpers.jl")
include("heart_executor.jl")

const CLOCK = SystemNanoClock()
const NS_PER_SECOND = 1_000_000_000
const WFS_DATAGRAMS = Int(ROWS)

# Read the copied receipt under the same single-attempt guard as the
# runner's drain. This observer never removes or applies a command.
function observe_command!(observed, published, state)
    Base.Threads.atomic_cas!(state.busy, false, true) && return nothing
    try
        sequence = state.receipt.sequence
        if 0 < sequence <= length(observed) && observed[sequence] == 0
            stamp = something(state.receipt.reference_timestamp)
            stamp.clock == METADATA_CLOCK_MONOTONIC || error("invalid DM reference clock")
            published[sequence] = Int64(stamp.timestamp_nanoseconds)
            observed[sequence] = Clocks.time_nanos(CLOCK)
        end
    finally
        state.busy[] = false
    end
    return nothing
end

function run_schedule(prepared, remote, period_ns, slots, warmup)
    loop = ThreadLoop("heart-timing-links")
    context = Context(loop)
    core = CoreConnection(context; properties=Dict("remote.name" => remote))
    registry = Registry(core)
    start!(loop)
    links = Link[]
    release = zeros(Int64, slots)
    slot_for_frame = zeros(Int, slots)
    frames = Array{UInt16}(undef, Int(ROWS), Int(COLUMNS), slots)
    observed = zeros(Int64, slots + 2)
    published = similar(observed)
    fill!(published, 0)
    next_slot = 1
    generated = 0
    copied = 0
    gc_before = nothing
    gc = nothing
    origin = Int64(0)
    drain_until = Int64(0)
    statistics = nothing
    opened = nothing
    try
        completed = Simulation.run!(prepared; frames=slots * STEPS_PER_EXPOSURE + 1, clock=CLOCK) do execution, streams, now
            if opened === nothing
                opened = streams
                link_heart!(links, loop, core, registry, streams)
                origin = Clocks.time_nanos(CLOCK) + period_ns
                return ExternalPupil.Hold()
            end
            observe_command!(observed, published, streams.streams[1].state)
            sequence = ExternalPupil.snapshot(execution).sequence
            if sequence % STEPS_PER_EXPOSURE != 0
                return ExternalPupil.Run(0)
            end
            if copied < generated
                copyto!(view(frames, :, :, generated), only(execution.buffers))
                copied = generated
            end
            if next_slot > warmup && gc_before === nothing
                gc_before = Base.gc_num()
            end
            due = origin + (next_slot - 1) * period_ns
            if next_slot <= slots && now >= due
                # Keep the absolute schedule. Count expired slots even if the
                # generator was descheduled, and issue at most one frame now.
                next_slot += (now - due) ÷ period_ns
                if next_slot <= slots
                    generated += 1
                    slot_for_frame[generated] = next_slot
                    release[next_slot] = now
                    next_slot += 1
                    return ExternalPupil.Run(0)
                end
            end
            if next_slot > slots
                if drain_until == 0
                    drain_until = now + 250_000_000
                elseif now >= drain_until
                    statistics = (input=array_stream_status(streams, 1),
                        output=array_stream_status(streams, 2))
                    gc = Base.GC_Diff(Base.gc_num(), gc_before)
                    close_heart_links!(links, loop)
                    roundtrip(registry)
                    return ExternalPupil.Stop()
                end
            end
            return ExternalPupil.Hold()
        end
        @test completed == generated * STEPS_PER_EXPOSURE
        @test opened.closed && !isopen(opened.core)
        return (; origin, release, slot_for_frame=slot_for_frame[1:generated],
            frames=view(frames, :, :, 1:generated), observed, published, statistics, gc)
    finally
        close_heart_links!(links, loop)
        with_thread_loop_lock(loop) do _
            close(registry)
            close(core)
            close(context)
        end
        stop!(loop)
        close(loop)
    end
end

function with_capture(f, directory)
    capture = joinpath(directory, "loopback.pcap")
    log_path = joinpath(directory, "capture.log")
    open(log_path, "w") do log
        filter = "udp and (dst port $WFS_PORT or dst port $DM_PORT)"
        process = run(pipeline(
            `tcpdump -Z root -i lo -n -Q in --time-stamp-precision=nano -B 4096 -s 128 -w $capture $filter`;
            stdout=log, stderr=log,
        ); wait=false)
        try
            _wait_for_hil_condition() do
                process_exited(process) && error(read(log_path, String))
                occursin("listening on lo", read(log_path, String))
            end
            f()
        finally
            process_exited(process) || kill(process, Base.SIGINT)
            wait(process)
        end
    end
    log = read(log_path, String)
    dropped = match(r"(?m)^(\d+) packets dropped by kernel", log)
    dropped === nothing && error("missing packet capture statistics\n$log")
    return capture, parse(Int, only(dropped.captures))
end

function epoch_ns(field)
    seconds, nanos = split(field, '.')
    return parse(Int64, seconds) * NS_PER_SECOND + parse(Int64, rpad(nanos, 9, '0'))
end

struct WfsDatagram
    sequence::Int
    segment::Int
    captured_ns::Int64
    timestamp_ns::UInt64
    pixels::Vector{UInt16}
end

function packets(capture)
    # The exercised HEART profile sends same-endian StdWfs and StdDm.
    wfs = Vector{WfsDatagram}[]
    dm = Vector{Tuple{Int,Int64,Vector{Float32}}}()
    command = `tshark -n -r $capture -T fields -e frame.time_epoch -e udp.dstport -e udp.payload`
    for line in eachline(pipeline(command; stderr=devnull))
        timestamp, port, hex = split(line, '\t')
        packet = hex2bytes(hex)
        instant = epoch_ns(timestamp)
        if parse(Int, port) == WFS_PORT
            segment = Int(reinterpret(UInt16, packet[17:18])[1])
            frame = Int(reinterpret(UInt32, packet[33:36])[1])
            wire_time = reinterpret(UInt64, packet[25:32])[1]
            if segment == 1 || isempty(wfs)
                push!(wfs, WfsDatagram[])
            end
            push!(last(wfs), WfsDatagram(frame, segment, instant, wire_time,
                collect(reinterpret(UInt16, packet[41:end]))))
        else
            frame = Int(reinterpret(UInt32, packet[17:20])[1])
            push!(dm, (frame, instant, collect(reinterpret(Float32, packet[25:end]))))
        end
    end
    return (; wfs, dm)
end

function complete_wfs(group)
    return [row.segment for row in group] == collect(1:WFS_DATAGRAMS) &&
        all(row -> row.timestamp_ns == first(group).timestamp_ns &&
            length(row.pixels) == COLUMNS, group)
end

function record(directory, result, capture, capture_drops, raw, sent, period_ns, warmup)
    wire = packets(capture)
    generated = length(result.slot_for_frame)
    slots = length(result.release)
    complete = filter(complete_wfs, wire.wfs)
    received_frames = size(raw, 3)
    heart_commands = size(sent, 3)
    # The wire's first segment identifies the HIL publication. HEART uses
    # the received-buffer ordinal for DM sync counters. Check the entire
    # ordered raw CB against the datagrams before mapping ordinals.
    lineage = capture_drops == 0 && length(complete) == received_frames &&
        length(wire.dm) == heart_commands && !isempty(wire.dm) && first(wire.dm)[1] == 1 &&
        all(vcat((row.pixels for row in group)...) == vec(raw[:, :, index])
            for (index, group) in pairs(complete)) &&
        all(wire.dm[index][3] == vec(sent[:, :, index]) for index in 1:heart_commands)
    ordinal = Dict(first(group).sequence => index for (index, group) in pairs(complete))
    @test length(ordinal) == length(complete)
    inconsistent = count(group -> any(row -> row.sequence != first(group).sequence, group), complete)
    dm = Dict{Int,Vector{Int64}}()
    for (id, timestamp, _) in wire.dm
        push!(get!(Vector{Int64}, dm, id), timestamp)
    end
    frame_for_slot = Dict(slot => seq for (seq, slot) in pairs(result.slot_for_frame))
    samples = Dict(key => Int64[] for key in ("cadence", "wire", "loop", "scheduled_loop", "release_delay"))
    previous_wfs = Int64(0)
    measured_wfs = measured_generated = measured_dm = measured_ambiguous = measured_observed = 0
    measured_mismatched = 0
    open(joinpath(directory, "frames.csv"), "w") do io
        println(io, "slot,due_mono_ns,release_mono_ns,wfs_id,wfs_first_epoch_ns,wfs_last_epoch_ns,heart_frame,wfs_consistent,dm_packets,dm_epoch_ns,dm_publication_mono_ns,hil_observed_mono_ns")
        for slot in 1:slots
            due = result.origin + (slot - 1) * period_ns
            release_ns = result.release[slot]
            sequence = get(frame_for_slot, slot, 0)
            index = get(ordinal, sequence, 0)
            group = index == 0 ? WfsDatagram[] : complete[index]
            wfs_first, wfs_last = isempty(group) ? (0, 0) :
                (first(group).captured_ns, last(group).captured_ns)
            consistent = index > 0 && all(row -> row.sequence == sequence, group)
            matching = consistent && all(
                row -> row.pixels == result.frames[row.segment, :, sequence], group
            )
            responses = lineage ? get(dm, index, Int64[]) : Int64[]
            dm_ns = length(responses) == 1 && only(responses) >= wfs_last ? only(responses) : 0
            publication = lineage && 0 < index <= length(result.published) ? result.published[index] : 0
            observed = lineage && 0 < index <= length(result.observed) ? result.observed[index] : 0
            println(io, join((slot, due, release_ns, sequence, wfs_first,
                wfs_last, index, matching, length(responses), dm_ns, publication, observed), ','))
            slot > warmup || continue
            if sequence > 0
                measured_generated += 1
                push!(samples["release_delay"], release_ns - due)
            end
            if wfs_first > 0
                measured_wfs += 1
                previous_wfs > 0 && push!(samples["cadence"], wfs_first - previous_wfs)
                previous_wfs = wfs_first
            end
            measured_ambiguous += length(responses) > 1
            measured_observed += observed > 0
            measured_mismatched += index > 0 && !matching
            measured_dm += dm_ns > 0
            if matching && dm_ns > 0
                push!(samples["wire"], dm_ns - wfs_first)
                if publication > 0
                    push!(samples["loop"], publication - release_ns)
                    push!(samples["scheduled_loop"], publication - due)
                    push!(get!(Vector{Int64}, samples, "hil_observed"), observed - release_ns)
                end
            end
        end
    end
    summary = Dict(
        "scheduled" => slots, "warmup" => warmup, "period_ns" => period_ns,
        "generated" => generated, "missed_slots" => slots - generated,
        "wfs_complete" => length(complete), "wfs_datagrams" => sum(length, wire.wfs; init=0),
        "wfs_inconsistent_ids" => inconsistent, "measured_mismatched_wfs" => measured_mismatched,
        "heart_frames" => received_frames, "heart_commands" => heart_commands,
        "dm_datagrams" => length(wire.dm), "hil_receipts" => count(>(0), result.observed),
        "capture_drops" => capture_drops, "lineage" => lineage, "measured_wfs" => measured_wfs,
        "measured_generated" => measured_generated, "measured_unique_dm" => measured_dm,
        "measured_ambiguous_dm" => measured_ambiguous, "measured_hil_receipts" => measured_observed,
        "gc_ns" => result.gc.total_time, "gc_pauses" => result.gc.pause,
        "hil_input" => Dict(pairs(result.statistics.input)), "hil_output" => Dict(pairs(result.statistics.output)),
    )
    for (name, values) in samples
        isempty(values) && continue
        summary[name] = Dict("samples" => length(values), "min_ns" => minimum(values),
            "p50_ns" => quantile(values, 0.5), "p90_ns" => quantile(values, 0.9),
            "p99_ns" => quantile(values, 0.99), "max_ns" => maximum(values))
    end
    open(joinpath(directory, "summary.toml"), "w") do io
        TOML.print(io, summary; sorted=true)
    end
    @test capture_drops == 0
    @test lineage
    @test !isempty(samples["loop"])
    @test all(>=(0), samples["loop"])
    return summary
end

@testset "sustained HEART rtcExec roundtrip" begin
    isempty(getipaddrs()) || error("run in the loopback-only namespace")
    rate = parse(Int, ENV["HEART_TEST_HZ"])
    duration = parse(Int, ENV["HEART_TEST_SECONDS"])
    directory = abspath(ENV["HEART_TEST_TIMING_DIR"])
    mkdir(directory)
    rate > 0 && duration > 0 && NS_PER_SECOND % (2 * rate) == 0 ||
        error("duration must be positive and the rate must divide the model cadence")
    period_ns = NS_PER_SECOND ÷ rate
    warmup = 3 * rate
    slots = warmup + duration * rate
    binary = abspath(ENV["HEART_TEST_EXECUTOR"])
    cli = abspath(ENV["HEART_TEST_CLI"])
    telemetry_cli = abspath(ENV["HEART_TEST_TELEMETRY"])
    plugins = abspath(ENV["HEART_TEST_SPA_DIR"])
    configuration_for(remote) = core_configuration(
        remote, DM_PORT, WFS_PORT; pixel_type="raw", target=0, actuators=2,
        exposure_ns=period_ns, wfs_source=0,
    )
    _with_private_core(; configuration_for, spa_plugin_dir=plugins) do remote
        configuration = heart_bundle(directory, remote; frame_encoding=:uint16,
            actuators=2, exposure_ns=period_ns)
        prepared = Simulation.prepare(configuration)
        result = nothing
        raw = sent = nothing
        capture, drops = with_capture(directory) do
            telemetry = nothing
            try
                with_heart_executor(directory, binary, cli; exposure_ns=period_ns, history=slots + 2) do
                    open(joinpath(directory, "telemetry.log"), "w") do log
                        command = Cmd(
                            `$telemetry_cli -receiver -address 127.0.0.1 -port 6300 -frames $(slots + 2) -debug 0`;
                            dir=directory,
                        )
                        telemetry = run(pipeline(command; stdout=log, stderr=log); wait=false)
                    end
                    result = run_schedule(prepared, remote, period_ns, slots, warmup)
                    raw = dump_buffer(directory, cli, "HOP0.wfs.pixelsRaw")
                    sent = dump_buffer(directory, cli, "WCC.dm0.dmOutput")
                end
            finally
                _stop_hil_process(telemetry)
            end
        end
        record(directory, result, capture, drops, raw, sent, period_ns, warmup)
    end
end

end
