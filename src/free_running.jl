# The command callbacks each own one complete-array slot. Only the serialized
# exposure owner assembles and adopts groups, and only at model boundaries.
mutable struct FreeRunningCommandSet{V}
    sequence::UInt64
    pts::Int64
    values::V
    present::BitVector
end

mutable struct FreeRunningAcquisition{R,S}
    receivers::R
    pending::S
    applied_sequence::UInt64
    applied_updates::UInt64
    dropped_frames::UInt64
end

_prepare_free_running(boundary, states) = nothing
function _prepare_free_running(boundary::AlgorithmGraphs.PreparedGraphCalibrationBoundary, states)
    source = _frame_state(states)
    source.acquisition_domain = _new_calibration_domain()
    identity = AcquisitionIdentity(source.acquisition_domain, source.acquisition_generation, 0)
    ports = _command_states(states)
    receivers = map(ports) do port
        receiver = ArrayStreamState(length(port.command), Float32)
        receiver.identity = identity
        receiver
    end
    pending = [FreeRunningCommandSet(UInt64(0), Int64(0),
        map(port -> zero(port.command), ports), falses(length(ports))) for _ in 1:8]
    # Probe 1 installs the initial graph command. Subsequent probe indices
    # count adoptions separately from the originating camera sequence.
    AlgorithmGraphs.adopt_hil_probe!(boundary, UInt64(1))
    return FreeRunningAcquisition(receivers, pending, UInt64(0), UInt64(0), UInt64(0))
end

_command_receiver(acquisition::FreeRunningAcquisition, index) = acquisition.receivers[index]

function _collect_free_running_commands!(acquisition, states)
    source = _frame_state(states)
    for (i, (receiver, port)) in enumerate(zip(acquisition.receivers, _command_states(states)))
        receiver.failed[] && throw(PipeWireHILError("correction_command", "command stream failed"))
        receiver.pending || continue
        receiver.pending = false
        receipt = receiver.receipt
        identity = receipt.identity
        identity !== nothing && identity.domain == source.acquisition_domain &&
            identity.generation == source.acquisition_generation &&
            acquisition.applied_sequence < identity.sequence <= source.frame_published[] &&
            receipt.duration == source.exposure_duration_ns || continue
        slot = acquisition.pending[mod1(identity.sequence, length(acquisition.pending))]
        slot.sequence <= identity.sequence || continue
        if slot.sequence < identity.sequence
            slot.sequence = identity.sequence
            slot.pts = receipt.pts
            fill!(slot.present, false)
        end
        all(value -> isfinite(value * port.command_scale), receiver.values) ||
            throw(PipeWireHILError("correction_command", "nonfinite command or unit conversion"))
        _unpack_array!(slot.values[i], receiver.values)
        slot.values[i] .*= port.command_scale
        slot.present[i] = true
    end
    latest = nothing
    for slot in acquisition.pending
        if slot.sequence > acquisition.applied_sequence && all(slot.present) &&
                (latest === nothing || latest.sequence < slot.sequence)
            latest = slot
        end
    end
    return latest
end

_stage_free_running_applied!(::Nothing, states, sequence, pts) = nothing
function _stage_free_running_applied!(source::PreparedAppliedCommandSource, state, sequence, pts)
    return _stage_applied_command!(source.state, state.command, source.command_scale, sequence, pts)
end
function _stage_free_running_applied!(sources::NamedTuple, states, sequence, pts)
    for key in keys(sources)
        _stage_free_running_applied!(sources[key], states[key], sequence, pts)
    end
    return nothing
end

function _adopt_free_running!(prepared, acquisition, command)
    command === nothing && return nothing
    for (port, buffer) in zip(_command_states(prepared.state), command.values)
        copyto!(port.command, buffer)
    end
    AlgorithmGraphs.adopt_hil_probe!(prepared.boundary, acquisition.applied_updates + UInt64(2))
    acquisition.applied_sequence = command.sequence
    acquisition.applied_updates += 1
    _stage_free_running_applied!(prepared.applied_command_source, prepared.state, command.sequence, command.pts)
    return nothing
end

"""
    step_free_running!(prepared[, driver]) -> sequence

Adopt the newest complete correction for one acquisition identity, then step
and stage an exposure under the held command. Neither correction nor output
buffer availability gates the model step. A newer exposure replaces a pending
one if the consumer falls behind. The caller owns exposure scheduling.
"""
function step_free_running!(prepared::PreparedPipeWireHIL, driver=nothing)
    prepared.closed && throw(PipeWireHILError("lifecycle", "HIL endpoints are closed"))
    prepared.started || throw(PipeWireHILError("lifecycle", "HIL endpoints are not started"))
    acquisition = prepared.free_running
    acquisition === nothing && throw(ArgumentError("Free-running acquisition requires a held-command boundary"))
    command = _with_loop_access(prepared) do
        _collect_free_running_commands!(acquisition, prepared.state)
    end
    _adopt_free_running!(prepared, acquisition, command)
    sequence = if driver === nothing
        AlgorithmGraphs.step_hil_exposure!(prepared.boundary)
    else
        AlgorithmGraphs.step_hil_exposure_at!(prepared.boundary, driver).sequence
    end
    state = _frame_state(prepared.state)
    _with_loop_access(prepared) do
        state.frame_ready[] != state.frame_published[] && (acquisition.dropped_frames += 1)
        _stage_exposure_record!(state, prepared.acquisition_source, sequence)
        _stage_frame_transport!(state)
        state.frame_ready[] = sequence
        stream = prepared.frame_stream
        if stream_state(stream) == PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING && is_driving(stream)
            _issue_driver_cycle!(() -> trigger_process!(stream), prepared.driver_cycle)
        end
    end
    return sequence
end

"""
    free_running_status(prepared) -> NamedTuple

Read published and dropped frame counts, the pending frame flag, and the most
recent applied command sequence and adoption count under the PipeWire loop lock.
Counts are relative to the current acquisition generation.
"""
function free_running_status(prepared::PreparedPipeWireHIL)
    acquisition = prepared.free_running
    acquisition === nothing && throw(ArgumentError("The HIL transport is in lockstep mode"))
    return _with_loop_access(prepared) do
        state = _frame_state(prepared.state)
        sequence = state.frame_ready[]
        pending = sequence != state.frame_published[]
        (; frames=sequence - acquisition.dropped_frames - UInt64(pending),
            dropped_frames=acquisition.dropped_frames, pending,
            applied_sequence=acquisition.applied_sequence, applied_updates=acquisition.applied_updates)
    end
end

function _reset_free_running!(prepared, driver, idle_strategy)
    acquisition = prepared.free_running
    _await_driver_cycle!(prepared, idle_strategy)
    _with_loop_access(prepared) do
        source = _frame_state(prepared.state)
        source.acquisition_generation < typemax(UInt64) ||
            throw(OverflowError("HIL acquisition generation exhausted"))
        flush!(prepared.frame_stream)
        foreach(flush!, _command_streams(prepared.command_stream))
        if driver === nothing
            AlgorithmGraphs.reset_hil_boundary!(prepared.boundary)
        else
            AlgorithmGraphs.reset_hil_boundary!(prepared.boundary, driver)
        end
        foreach(_clear_exchange_state!, _command_states(prepared.state))
        identity = AcquisitionIdentity(source.acquisition_domain, source.acquisition_generation, 0)
        for receiver in acquisition.receivers
            receiver.pending = false
            receiver.identity = identity
            receiver.publication_sequence = 0
            _clear_array_receipt!(receiver.receipt)
        end
        for slot in acquisition.pending
            slot.sequence = 0
            fill!(slot.present, false)
        end
        acquisition.applied_sequence = acquisition.applied_updates = acquisition.dropped_frames = 0
        AlgorithmGraphs.adopt_hil_probe!(prepared.boundary, UInt64(1))
        _reset_driver_cycle!(prepared.driver_cycle)
        _reset_applied_command!(prepared.applied_command_source)
    end
    _reset_image_owner!(prepared.image_owner)
    return prepared
end
