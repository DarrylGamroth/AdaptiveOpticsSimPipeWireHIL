# AdaptiveOpticsSimPipeWireHIL.jl

[![CI](https://github.com/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL/branch/main/graph/badge.svg)](https://app.codecov.io/gh/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`AdaptiveOpticsSimPipeWireHIL.jl` runs prepared AdaptiveOpticsSim simulations over
ordinary PipeWireAO nodes. AOS owns the models and numerical execution; this
package owns simulation/transport coordination. RTC clients own admission and
session topology. The package supports lockstep and free-running WFS
acquisition, external-pupil execution and clocked optical execution.

## Simulator idle strategy

The managed source and standalone simulation runners accept `yielding`
(default) or `busy-spin` through `PIPEWIREAO_SIM_IDLE_STRATEGY`. The managed
entry point also accepts `--idle-strategy`. `SimRunner.run!` uses Agent.jl
on the task that prepared the plant. A duty returns a positive work count,
zero to apply the idle strategy, or `nothing` to finish.

Busy-spin issues a CPU pause hint while waiting for an active deadline,
publication, or a matching RTC command. Reserve CPU time for the simulator
and its transport and control threads. The host scheduler can still preempt
them. Held sources can sleep until their next control check.

## Free-running WFS acquisition

Pass a boundary from `prepare_graph_calibration_boundary` to
`prepare_pipewire_hil`, start the transport, and call
`step_free_running!(prepared, model_time_driver)` on each model tick.
The initial graph command is held until a complete RTC correction arrives.
The source matches all named ports by the originating frame's acquisition
domain, generation and sequence. It retains up to eight candidate command
sets and adopts the newest complete one at the next model boundary.

A late command leaves the previous complete update applied. A pending camera
frame may be replaced when its consumer falls behind. `free_running_status`
reports published and dropped frames and the last applied command sequence.
Reset discards partial groups and restores the graph's initial command in a
new acquisition generation.

The managed graph-backed WFS launcher accepts `--acquisition free-running`
or `--acquisition lockstep`. The default is lockstep. The configured source
pacing also applies in free-running mode. Matched exchange phase timing
remains available in lockstep.

## Tests

CI runs Aqua and the CPU suites on Linux x86_64 with Julia 1.13 or later, a C
compiler, and the PipeWireAO JLL. The transport tests start isolated private cores.
Run the same suite locally from the package checkout.

```sh
julia --startup-file=no --threads=1,1 --project -e '
    using Pkg
    Pkg.Registry.add("General")
    Pkg.Registry.add(url="https://github.com/DarrylGamroth/PackageRegistry.git")
    Pkg.instantiate()
    Pkg.build("AdaptiveOpticsSimPipeWireHIL")
    Pkg.test(coverage=true)'
```

The managed tests use synthetic AdaptiveOpticsSim graphs and the native
bootstrap and source-control protocols. They check lockstep command adoption,
independent camera stepping, optical diagnostics, and pupil publication across
reset. The pinned diagnostics worker
runs when the host can assign it a distinct CPU above CPU 1.

The coverage report includes Julia package source, extensions and build code.
CUDA and HEART retain their dedicated test selectors.

## Lockstep HIL

The lockstep boundary exposes:

- a complete WFS-frame source; and
- one or more non-actuating correction-command sinks.

The package connects AdaptiveOpticsSim to a PipeWireAO RTC workstation and
owns prepared optical simulation execution. It does not implement RTC algorithms, parse
filter-graph configuration, schedule FGN operations, or control physical
hardware.

```text
AdaptiveOpticsSim WFS frame
    -> PipeWireAO source
    -> FGN graph(s)
    -> PipeWireAO command sink
    -> AdaptiveOpticsSim command for the next frame
```

For frame sequence `n`, the adapter publishes one complete WFS frame and
accepts one complete correction command per declared port carrying the same
sequence. AdaptiveOpticsSim adopts the complete command set for frame `n + 1`. The coupling between
the two nodes stays inside the AOS application, so the PipeWire graph remains
an acyclic source-to-processor-to-sink graph.

The frame source is the selected PipeWire graph driver for this lockstep
development path. An exchange waits until both endpoint streams are streaming
and the source is driving, then explicitly advances the source-publication and
downstream-command cycles. PipeWire and the hosted FGN graph still perform all
data-plane scheduling and frame processing.

If a source buffer is temporarily unavailable, the adapter retries publication
after the current native cycle completes. It retains the staged frame and
sequence; a retry does not advance the simulation or publish a frame twice.

The development profile uses dense host `Array{Float32}` AOS commands and
`Array{Float32}` or `Array{UInt16}` frames,
one frame and one coherent command set in flight, complete-frame transport, and a private local
PipeWire core. AOS owns synchronous GPU-to-host frame staging and host-to-GPU
command adoption when its graph uses a GPU. It makes no physical-device,
deadline, tail-latency, safety, or production qualification claim.

## Transport encoding and command units

`PipeWireHILConfiguration` defaults to `frame_encoding=:float32` and
`command_scale=1.0f0`. The source advertises the frame's exact shape, row-major
layout, configured rate, and `F32_LE` element type. Select
`frame_encoding=:uint16` to advertise `U16_LE` for raw ADC code transport. The
staged boundary packs the AOS frame into a preallocated transport vector
before publication. Native UInt16 codes are copied exactly. Float32 frames sent as
UInt16 are rounded to nearest integer (ties to even). Non-finite
codes and values outside 0:65535 raise `PipeWireHILError`; values are never
clamped. The caller supplies a frame schema appropriate for that encoding.

Commands remain `F32_LE` with the exact command shape, row-major layout, and
configured rate. `command_scale` multiplies incoming values into plant units;
use `command_scale=1.0f-6` when the RTC demands micrometre OPD and the plant
accepts metre OPD. Scale is fixed at preparation and must be finite and positive
as Float32. The sink validates the whole command, its scaled values, and the
outstanding frame sequence before writing the AOS command buffer. Duplicate,
stale, future, incomplete, and non-finite commands are rejected.

For an AOS boundary prepared with `command_input=(:woofer, :tweeter, :tiptilt)`,
provide matching named configuration values:

```julia
command_node_name = (woofer="sim-woofer", tweeter="sim-tweeter", tiptilt="sim-tiptilt")
command_schema = (woofer="instrument.woofer/1", tweeter="instrument.tweeter/1", tiptilt="instrument.tiptilt/1")
command_scale = (woofer=1f0, tweeter=1f0, tiptilt=1f0)
```

The named sets must contain the same 1:16 unique command ports; field order is
irrelevant to transport configuration and validation. Each command retains its
own shape, schema, scale, readiness and receipt timing. No concatenation or
instrument-specific command interpretation occurs. The native callback validates
and copies one port. The exchange owner waits for every port under one command
wait budget, then invokes one AOS adoption barrier. A missing or invalid final
port never authorizes partial adoption or another model step. Every command must match the complete frame acquisition identity,
exposure duration and independent trigger/optical classifications. Reset rejects any pending port.
Scalar configuration and a single-Symbol AOS boundary retain their array API.

`exchange_frame!(prepared, model_time_driver)` uses AOS's next model-time
boundary and returns the exchanged sequence, as does `exchange_frame!(prepared)`.
The caller owns the driver and wall-clock pacing.

`reset_pipewire_hil!(prepared[, model_time_driver])` resets a quiescent boundary,
optionally resets its model-time driver, increments the acquisition generation,
and clears exchange sequences and timing observations. The caller must serialize
reset with exchanges. Endpoints may remain started; reset rejects a pending
frame publication, command receipt, or command adoption.

Preparation unwinds resources acquired before a constructor or connection
failure. Closing attempts all streams, the core, context, and native loop even
when stream deactivation reports a stored callback error. Teardown errors are
reported after those attempts; multiple failures use `CompositeException`.

## Direct CPU camera frames

For the fitted GPI2 PWFS graph, prepare the AOS boundary with
`Optics.prepare_filtergraphs_pwfs_hil(definition; command_input)` and use
`prepare_pipewire_hil(boundary, configuration)` with UInt16 frame encoding.
Call `exchange_frame!(prepared, driver)` with the AOS model-time driver.

The exchange owner dequeues before stepping and waits for a free slot
without advancing model time. Detector layout writes directly into the
mapped buffer in the existing row-major format. The owner keeps the
PipeWire loop locked while the buffer is borrowed so local renegotiation
cannot invalidate the mapping. It queues only after the frame and metadata
are complete and returns the loan on failure. CUDA continues to use the
ordinary staged boundary.

## Exposure acquisition metadata

Header PTS records source publication in host `CLOCK_MONOTONIC`. It is not a
physical exposure start. Simulated exposures publish identity and duration;
physical-start metadata is absent unless a source has a qualified physical clock.

`prepare_pipewire_hil(...; acquisition_source=source)` binds an instrument's
completed-exposure metadata to this adapter. The source implements these advanced
interfaces:

- `validate_acquisition_source(source, boundary, configuration)` validates the
  exact graph and selected frame-array binding during preparation.
- `acquisition_record(source, sequence)::PipeWireExposureRecord` observes the
  completed exposure and returns its **observed** committed sequence, positive
  duration in nanoseconds, `AcquisitionTriggerState` for the whole exposure,
  and optional independent `OpticalClassification`.

Observation must not step or reset the graph, mutate scientific state, advance a
counter, or publish a buffer. The adapter copies and checks the complete immutable
record after the optical step and before frame publication. Callbacks read only
that staged record. A record failure publishes nothing; the existing HIL boundary
blocks another frame until explicit recovery.

Every source writes Header and explicitly negotiated independent metadata records. The default simulated source
reports its configured positive duration; an explicit source supplies observed
whole-exposure electrical and independent optical classifications. Neither source
publishes physical exposure start without an explicit source reading. Publication
sequence and retained physical acquisition sequence are distinct. Product production
time uses REALTIME by default; `reference_clock=METADATA_CLOCK_TAI` selects TAI. This is execution wall time, not simulated exposure time. Clock steps do not
reset acquisition identity or control deadlines.

## Held-probe calibration transport

Held-probe calibration supports one command input or an explicitly named command
set through the AOS graph calibration boundary. Named commands use the same
matching node-name, schema and fixed-scale configuration as ordinary HIL above.
Each port receives one complete command carrying the same armed probe token;
the owner adopts only after every port returns its borrowed buffer. No exposure
can observe a partially received or partially adopted command set.

Use `prepare_pipewire_calibration(boundary, configuration)` with an AOS
`PreparedGraphCalibrationBoundary` when several exposures must use one demanded
probe. It creates the same ordinary frame-source and command-sink port contracts
with separate probe and exposure sequencing. One control owner serializes all
lifecycle, probe, exposure, ACK, reset and status calls outside callbacks:

```julia
prepared = prepare_pipewire_calibration(boundary, configuration)
start!(prepared)
adopt_pipewire_probe!(prepared, UInt64(7); submit=enqueue_probe)
exposure = step_pipewire_exposure!(prepared, model_time_driver;
    before_publish=exposure -> arm_wfs_collector(exposure.identity),
    timeout_ns=remaining_request_budget_ns)
response = receive_complete_wfs_response(exposure.identity)
# Validate the response and its complete acquisition identity before this ACK.
complete_pipewire_exposure!(prepared, exposure.identity)
# Another exposure now runs under probe 7 without receiving another command.
```

`submit()` is called after the expected command Header token is armed. It must
promptly enqueue publication; an asynchronous producer must request native
processing when its data becomes ready. The sink accepts only the armed token,
validates the entire demanded command and fixed unit conversion, and publishes
receipt after returning its borrowed buffer. AOS then synchronously adopts the
probe before any exposure. Probe tokens strictly advance across the endpoint
lifetime, including reset; they are independent of graph exposure sequences.

After successful adoption, `pipewire_probe_values(prepared)` borrows the exact
received demanded `Float32` values before fixed unit conversion. The returned
vector is adapter-owned host storage and retains the original wire bits; dividing
the scaled AOS command cannot recover them reliably. Access rejects a pending
probe receipt/adoption, failed transport or AOS boundary, reset boundary, or
closed endpoints. The associated Header token is
`pipewire_calibration_status(prepared).last_probe_sequence`. The sole serialized
control owner MUST NOT mutate the borrowed vector, and must stop using it before
beginning another probe adoption, reset or close. Copy the values when longer
retention is needed. Held exposures leave the values unchanged.
For named commands, `pipewire_probe_values` returns a NamedTuple of borrowed
vectors in the boundary's declared command order. Every vector belongs to the
same adopted probe token. `pipewire_calibration_status.received_probe_sequence`
likewise reports one receipt per named command; its scalar form is unchanged.
One operation deadline covers the entire command set. Missing, duplicate,
unarmed or unrelated receipts fail the endpoint; close and reconstruct it.

`PipeWireCalibrationExposure` retains the actual AOS model timestamp, exposure
duration, adopted probe token, and full 16-byte-domain/generation/sequence
`AcquisitionIdentity`. Calibration model time is nonnegative, and the exposure
end must fit Int64 nanoseconds. Header PTS remains a monotonic publication-clock observation. The simulator
does not claim a physical exposure start from it; the typed AOS record is the
authoritative model-time sideband, correlated by acquisition identity.
Do not derive model time from Header PTS.

`before_publish(exposure)` runs exactly once on the control owner, outside
PipeWire callbacks and the ThreadLoop lock. The actual AOS model step, pending
exposure record and host frame staging are complete, while the source frame is
still unpublished. An external collector can arm the complete actual identity
before delivery becomes possible. The default callable does nothing. If arming
throws, the adapter retains the already completed model step and pending record,
publishes no source frame, and rejects further exposure, probe adoption or reset
until the endpoints are closed and reconstructed.

Source queueing acknowledges publication. It does not acknowledge a complete
WFS response. The caller validates the actual WFS response and calls
`complete_pipewire_exposure!` with the full identity or returned exposure record.
Another probe, exposure, or reset is forbidden while an exposure is pending.
Sequence-only, duplicate, foreign-domain and stale-generation ACKs reject.
Settling rules, response aggregation, inversion and RTC lifecycle stay with the
caller.

`pipewire_calibration_status(prepared).frame_publication_cycle_sequence` reports
the published frame sequence observed at the native source's `on_trigger_done`.
Queueing can finish before this source cycle ends. A caller needing source-cycle
ordering matches this witness to its pending exposure's sequence in the current
acquisition generation. This is transport evidence only: it does not prove
downstream WFS processing or complete the exposure. Reset clears the witness to
zero; a callback observing failed transport leaves it unchanged. The witness
never clears failure or the pending exposure fence. The callback only records bounded
state and signals the existing completion condition; it requests no processing
and executes no simulation.

Completion waits use a reusable notification condition and actual atomic
receipt/publication evidence. IO notifications refresh native driver readiness.
While readiness or transport progress is pending, a 1 ms control timer requests
bounded retries outside the condition lock. These retries refresh public stream
state or request native processing for the same prepared frame; they never
execute another AOS exposure or stand in for success. One operation deadline
covers readiness and receipt/publication across every retry. Both
`adopt_pipewire_probe!` and `step_pipewire_exposure!` accept `timeout_ns` to clamp
an operation to the owner's remaining request budget. It must be a positive
`UInt64`, fit `Int64` nanoseconds, and not exceed `configuration.timeout_ns`;
validation precedes model or probe side effects. The original operation clock
also covers submission, model execution, frame staging and `before_publish`.
Owner callables must return promptly; synchronous owner work cannot be interrupted
by this completion deadline. This is a software
progress policy, not acquisition pacing or a performance guarantee.

`reset_pipewire_calibration!(prepared[, model_time_driver])` requires quiescent
transport, restores the AOS snapshot, advances acquisition generation, and
requires a new probe adoption. It preserves the last wire probe token. Reset is
not an external device restoration acknowledgment. Unknown submission outcomes,
callback errors, timeouts and adoption/execution errors stop transport admission;
close and reconstruct those endpoints. Closing still attempts all resources.

Focused tests cover the coordinator-facing mailbox and completion fences. The
private-core fixture uses a two-element copy plant and external ordinary streams
for software transport verification, including delayed probe publication and
source-buffer exhaustion/retry. It does not qualify deployed instrument WFS
processing, calibration matrix accuracy, physical devices or real-time deadlines.

## Source control hooks

Version 0.1.2 adds `prepare_pipewire_hil(...; frame_buffers=2)` for a cold,
fixed source-pool selection in `2:64`. A deployment attaching the existing
capacity-one PipeWireAO queue needs three input buffers and must select three
before the first required link negotiates. Command buffers remain two and the
one-frame/one-command exchange is unchanged. Pool capacity alone does not make
an observer non-gating: deployment owns the passive observation link, queue
overflow/storage policy, independent output placement and system evidence.
The optional adopted-command source below is independent of these transport
pool settings.

Version 0.1.1 requires PipeWireAO 0.6.15 and permits native source parameters
through `prepare_pipewire_hil` keywords: `frame_properties`, `frame_params`,
`on_frame_param_changed`, `frame_param_buffer` and `on_frame_param_overflow`.
These use PipeWireAO's public Stream contract. Additional properties cannot
override transport properties.

A callback may validate and stage bounded scalar controls. The application owner
applies them between completed `exchange_frame!` calls, after command adoption.
It must not execute optics, reset the exchange, or write reports on the PipeWire
loop. `with_frame_stream(f, prepared)` provides loop-locked access for bounded
parameter publication; never hold this lock across simulation, reset or waits.
`frame_acquisition_generation(prepared)` returns the loop-protected generation.
Control policy and acknowledgements belong to the application, not this adapter.

## Simulator-adopted command sources

`prepare_pipewire_hil(...; applied_command_node_name="simulator-applied-command")`
prepares an optional source before the first frame. For physical-DM commands,
its `output_1` port has `media.name=dm-applied`, schema `pipewireao/applied-dm-command-opd-um-f32/1`,
the prepared command vector shape, row-major layout, and `F32_LE` elements.
The identifier and element type come from the generated shared scientific
catalog. The default is disabled.
`applied_command_buffers=2` selects its fixed native pool in `2:64`.
`applied_command_source_info(prepared)` returns its declaration for the session
owner; `with_applied_command_stream(f, prepared)` provides bounded access under
the endpoint loop lock. Both return `nothing` when it is disabled.

The source receives a copied snapshot only after `adopt_hil_command!` succeeds
for the matched frame sequence. Its Header carries that sequence and the original
WFS ingress PTS. Receipt alone, failed adoption and unmatched sequences produce
no observation. The payload converts the adopted metre-OPD plant command to
micrometre OPD with a fixed scale of `1.0f-6`. Optional-source preparation accepts
`pipewireao/dm-command-opd-um-f32/1` with input scale `1.0f-6`, or
`pipewireao/actuator-command-opd-m-f32/1` with input scale `1.0f0`. A contradictory
scale or an unknown physical command meaning is rejected for this optional
source; the mandatory exchange still supports caller-declared external schemas.

For `pipewireao/tip-tilt-opd-coefficients-m-f32/1`, the input scale must be
`1.0f0` and the shape must be `(2,)`. The observation retains that schema,
`media.name=tip-tilt-applied`, and the adopted Float32 values in metres OPD.
The two values remain normalized-pupil X and Y ramp coefficients; they are not
physical-DM actuator values. All selected observation contracts are checked
before connecting to a native core.

For named command sets, `applied_command_node_name` can be a matching NamedTuple
of distinct observation node names. Each selected command must independently
satisfy the existing OPD schema and scale contract; these observations do not
assign OPD meaning to arbitrary commands such as angles. The optional snapshot
for each port is staged only after the entire command set is adopted.
`applied_command_source_info` returns matching named declarations, and
`with_applied_command_stream` passes matching named streams. The scalar forms
retain their existing return values. `frame_command_timing` similarly returns
named per-port receipt observations for a named command set.


The exchange owner and callback each attempt one atomic claim of a preallocated
actuator-vector snapshot. Busy consumers drop an observation; pending snapshots are
replaced by the newest adopted command. The callback copies at most one complete
snapshot into a public PipeWireAO buffer. It performs no simulation, wait or
per-frame logging. Staging and successful callback copies allocate zero bytes
after warmup. An observer transport failure disables this optional source until
endpoints are reconstructed, while the mandatory exchange continues.

The source uses an ordinary non-driver Stream. The session owner must supply an
independent observation consumer clock and topology; HIL neither triggers that
clock nor waits for observer readiness or consumption. Reset and stop clear the
pending snapshot and flush source buffers; start reactivates the same node, and
close removes it. The owner must serialize exchange and lifecycle operations.
Private-core tests verify independent consumer delivery, copied values, Header
causality, absence of observer gating, source lifetime and warmed allocations.
This is software transport evidence, not deployed instrument or real-time
qualification.

## Optional complete image sources

The optical owner can declare independent observation sources before admitting
the first frame:

```julia
images = (PipeWireImageSourceOptions(
    node_name="simulator-pupil-opd", media_name="pupil-opd",
    schema=pupil_opd_schema, shape=size(pupil_opd),
    properties=Dict("node.group" => "optical-observation")),)
prepared = prepare_pipewire_hil(boundary, configuration; image_sources=images)
```

Each `output_1` port advertises a complete rank-two `F32_LE` ndarray with its
declared shape, schema and row-major layout. The supplied schema defines the
optical meaning and units. `rate=nothing` uses the HIL configuration rate;
`buffers=2` selects a fixed native pool in `2:64`. Transport properties are
protected. `image_source_info(prepared)` returns a tuple of declarations with
one-based source indexes for the session owner. `with_image_stream(f, prepared,
index)` permits bounded control work under the independent image loop lock.

An owner can describe scalar values within the complete array with
`channels=((name="first_value", index=0, title="First value", units="nm"),)`.
Indices are zero-based logical row-major positions; `units=nothing` declares
unknown units. Channel names and indices must be unique, and strings must be
valid UTF-8 without controls, nonempty and at most 255 bytes. At most 64 channels
and 16384 UTF-8 bytes are admitted. The adapter constructs one immutable native
`pipewireao.ndarray.channels` String before publication, with the actual
`output_1` Port, schema, element type, layout and exact rank-two shape. The String
is source Node information, separate from writable Props. New meanings or units
require a new source instance.
The owner remains responsible for scientific meaning and consistency with its
schema; these declarations add no conversion or frame-processing work.

After preparing the optical image outside callbacks, the serialized simulation
owner calls `stage_pipewire_image!(prepared, index, image, sequence, pts,
generation)`. The image must be a dense host `Matrix{Float32}` of the prepared
shape. Capture the generation with the originating frame, and use that frame's
sequence and original WFS Header PTS. Staging rejects a stale generation rather
than assigning an old image to a new acquisition. The public acquisition
metadata retains the originating domain, generation, sequence and parent WFS
exposure duration. Exposure start is absent and has no acquisition timebase:
Header PTS is a monotonic publication observation, not a physical exposure
timestamp or model time.
The optional source does not step optics or alter command adoption.

Each source owns a preallocated matrix snapshot. Staging and the callback each
attempt one atomic claim and return immediately if it is busy. Staging copies
the completed image; subsequent optical writes cannot alter the snapshot. A
new image replaces a pending one, while a callback copies at most one complete
snapshot into the mapped ndarray buffer, serializing the matrix in row-major
order. Successful warmed staging and callback copies allocate zero bytes.
`false` reports a disabled, busy, stale or invalid staging attempt. Snapshot
loss is counted separately from the optional source's failed state. Observer
failures disable only that source until endpoints are reconstructed.

All optional image streams share one separately prepared native ThreadLoop,
Context and CoreConnection to the configured remote. No image connection or
loop is created when the tuple is empty. Image callbacks copy and queue buffers
on that loop and never take the mandatory WFS loop lock. The native callback
thread is named `AOS-HIL-images`; its 14-byte name remains distinct in Linux
`comm`. Deployment owns thread affinity and scheduling policy. These are ordinary
non-driver streams. The session owner supplies an
independent observer clock and topology. HIL neither waits for viewer readiness
nor triggers observation processing. A missing or slow observer retains only
the newest pending image and never gates mandatory frame/command exchange.
Stop and reset flush the source pools and clear pending snapshots; restart
retains the same node, and close removes all optional streams before closing
their own core, context and loop. Mandatory WFS and adopted-command endpoints
retain their original loop and connection. The simulation owner serializes
staging with reset, stop and close, and joins any optical producer before reset.
`reset_pipewire_image_sources!(prepared)` flushes and clears only the optional
image sources under their own loop lock, preserving the mandatory acquisition
generation and exchange state. Use it to drain synthetic callback-warmup images
before admitting the first model frame.

Image naming follows the established HIL optional-source API (`source`,
`source_info`, `with_*_stream`) and the PipeWireAO ndarray contract (`shape`,
`schema`, `format`, acquisition identity). Optical names and units remain with
the caller. No new optical schema or scientific vocabulary is defined here.

## Standalone complete-array streams

`with_array_streams` connects simulator inputs and products independently of the
lockstep HIL boundary. Each immutable declaration supplies an explicit schema,
`NdArrayFormat`, and caller-owned descriptive properties. The initial transport
supports CPU `Array{Float32}` values of any supported rank in row-major wire order.
It assigns no optical meaning, plane order, units, coordinates or clock domain.

```julia
format = NdArrayFormat(NdArray.F32_LE, (2, 3, 4); layout=NdArray.ROW_MAJOR)
options = (PipeWireArrayStreamOptions(
    node_name="independent-input", media_name="independent input",
    schema="example.complete-array/1", format=format,
    properties=Dict("example.axes" => "plane,row,column",
        "example.pts" => "model nanoseconds")),)
with_array_streams(options; remote="explicit-private-core", input=(true,)) do owner
    # The session owner links the stream through with_array_stream(f, owner, 1).
    destination = zeros(Float32, 2, 3, 4)
    receipt = drain_pipewire_array!(destination, owner, 1)
    # Nothing means no complete new input is available. Otherwise receipt holds
    # identity (domain, generation, sequence), Header pts, duration, lost_samples.
end
```

Set `input=(false,)` for an output. Call
`stage_pipewire_array!(owner, 1, array; identity, pts, duration)` with an
`AcquisitionIdentity`, `Int64` PTS and `UInt64` duration, then invoke public
`PipeWireAO.trigger_process!(owner)` after staging. PipeWire can elect one
connected output as the driver for several camera streams, so triggering only
the arriving camera can leave a non-driving camera pending. The owner helper
triggers each streaming elected output driver; unstaged callbacks emit nothing.
Output streams request driver ownership;
inputs are ordinary sinks. The session owner creates links and coordinates
triggers. The explicit nonempty remote prevents accidental default-core use.

Array declarations use `metadata`, a tuple of `MetadataRequirement` values.
Generic array streams default to Header only. Sensor frame/image profiles require
acquisition identity, positive exposure duration and a reference timestamp.
Optical and electrical classifications require explicit independent declarations.
A command profile can require only ReferenceTimestamp and leave identity/exposure
optional. Input receipts expose publication `sequence` separately from optional
`identity` and `duration`; missing camera facts are never manufactured for a command.

Supply observed `trigger_state` and `optical_classification` when staging a
sensor array. HIL assigns no optical meaning to electrical polarity. Missing
records differ from explicit ABSENT classifications. `reference_timestamp` keeps
one exact UInt64 nanosecond source value through derived outputs. A producer
reads its selected host clock only when that record is negotiated and no source
reference was supplied.

For example, `metadata=(MetadataRequirement(SPA.META_REFERENCE_TIMESTAMP;required=true),)`
declares a command observation profile. For an output without acquisition identity,
supply an explicit Header `sequence`. An empty profile publishes Header only.
Optional records omitted by negotiation are not written, even if the staged
receipt contains those facts; required missing slots fail before buffer writes.
Recycled output records are initialized before publication. Callback input metadata uses a prepared receipt slot; owner
`drain_pipewire_array!` constructs the returned immutable receipt.

The cold property `pipewireao.header.pts.clock` declares presentation-clock semantics.
Frame/applied-command streams identify their monotonic publication clock. Generic
array and image sources default to `unavailable`; owners can explicitly supply a
known clock label such as `model`.
Reference timestamps never imply a physical-event mapping for Header PTS.

Static detector geometry remains outside
these per-buffer records.

Each endpoint owns one complete-array slot. A newer array overwrites a pending
one; callbacks and the sole application owner try the slot once and return
immediately on contention. Callbacks only validate and copy transport data.
`array_stream_status(owner, index)` exposes overwrite/contention drops, missing
sequence gaps, rejected buffers and failure state. Input receipts report the
cumulative number of dropped and missing samples since reset; initial sequences
before the first observed identity cannot be inferred. Headers marked corrupted,
gap or discontinuity are rejected; the next valid sequence exposes the loss.

The application owner serializes staging, draining and lifecycle calls. Sequence
numbers must increase within one generation. A strictly greater generation in
the admitted domain is accepted automatically; the owner uses the receipt to
invalidate any incomplete work from the old generation. Delayed older-generation
buffers are rejected, and gap counts do not cross generations. To flush native
buffers, discard pending receipts or explicitly admit a different domain, call
`reset_pipewire_arrays!(owner; identity)` with sequence zero and a new generation
(or new domain). A failed endpoint requires reconstruction. All streams share one loop and connection, and the `with` scope
closes every native resource even when its body throws. Tests use a private core;
they establish software transport behavior, not device or deadline performance.

## Timing observation API

Version 0.2.2 adds `PipeWireExchangeTiming()` for optional owner-side exchange
phase capture. Pass the reusable record as `exchange_frame!(prepared, driver;
timing=record)`. Successful return assigns its generation and sequence; failed
attempts retain sequence zero. The default `timing=nothing` adds no timing clock
reads. See [the phase boundaries and validation scope](docs/EXCHANGE_TIMING.md).

After a successful `exchange_frame!`,
`frame_command_timing(prepared)` returns the latest
`PipeWireFrameCommandTiming` for that sequence on a scalar boundary. A named
command boundary returns matching per-port observations. It records the monotonic
Header PTS assigned at source publication, the receipt timestamp in the
same-sequence command callback, and their difference in nanoseconds. It
returns `nothing` until a complete command has been received.

This is an end-to-end graph-path observation for the single-frame development
profile. It is intentionally not an RTC benchmark, scheduler, histogram, or
deadline guarantee. Use it to capture raw per-frame observations when running
the actual RTC graph; establish latency distributions and system-level claims
in `pipewireao-rtc`.

## Package boundaries

- AdaptiveOpticsSim owns optical models, WFS products, correction-command
  meaning, model time, and frame/command causality.
- PipeWireAO.jl owns public PipeWire and SPA bindings, stream lifetimes,
  ndarray formats, mapped buffers, and metadata.
- `pipewireao-rtc` owns declared session topology and links, its runner-created
  FGN graphs, lifecycle, and closed-loop system tests. It discovers these
  nodes by their public PipeWire port contracts and does not identify this
  package at runtime.
- This package owns only the bounded mapping between the AOS HIL exchange
  buffers and the two PipeWire nodes.

## Status

Version 0.2.1 implements the lockstep development transport and shared cold
admission checks used by REVOLT Classic, REVOLT Copper, GPI 2.0 and SPIDERS.
`validate_hil_system` checks an existing graph/boundary and its selected buffers;
`validate_wire_contract` checks the instrument's explicit permitted schema,
encoding and unit-scale combinations. These checks do not prepare or advance
the simulation. Instrument meaning remains owned by each plant.

The local suite covers configuration, complete-buffer conversion, sequence and
acquisition identity, model time, reset, lifecycle, held-probe calibration,
timeouts and owner-failure handling. Private-core fixtures verify publication,
command adoption and teardown. Shared instrument checks exercise CPU and CUDA
plants through this transport without connecting to an installed RTC.

These checks cover the lockstep development adapter. Deployed controller
convergence, atmospheric closed-loop system evidence, hardware fidelity and
real-time performance qualification belong to the instrument and PipeWireAO
RTC deployment. SPIDERS external RTC integration is deferred.

Calibration and ordinary HIL preparation accept `reference_clock=METADATA_CLOCK_REALTIME`.
MONOTONIC and TAI can also label host sample readings. Clock labels do not establish
synchronization or accuracy; physical acquisition start remains absent for simulator
production callbacks.

## Prepared optical simulation

`AdaptiveOpticsSimPipeWireHIL.ExternalPupil` owns the execution and publication of
prepared external-pupil or clocked optical science. It takes prepared AOS graphs,
pupil contracts and sensor contracts; it does not read RTC configuration. The RTC
retains session admission, native controls and device interfaces.

Call `prepare(science; ...)` before admission. Managed unpaced CPU clocked
optics, such as the SPIDERS fitted-dual CPU export, validate images on the
first live exposures after Start. Paced, accelerator and external-pupil
launches call `warmup!(execution)` before admission. The install-time CPU
precompile workload also exercises the optics.

Build stream options with `stream_options`, open them with
`with_array_streams`, and call
`prepare_transport!(execution, streams)` before running. `run!(control, execution,
streams; clock)` calls `control(execution, streams, now_ns)` once per duty; return
`Run(deadline)`, `Hold()` or `Stop()`. The callback can inspect `snapshot(execution)`
and invoke `reset!(execution, streams)` at the serialized control boundary.

The caller owns stream lifetime and must close streams after any failure. AOS
retains numerical stepping, physical planes, exposure integration and model-time
semantics. Optical publication copies borrowed products, triggers once after the
completed exposures from a source step, and retries retained samples after a
nonthrowing staging rejection. Failed steps require transport teardown. Science
never executes inside PipeWire processing callbacks.

Clocked optical sources use a synchronous native PipeWire input for their
Float32 DM command vector. The callback returns the borrowed buffer after
copying its payload and metadata to one prepared slot. At the next model
boundary the source adopts the latest complete command available there.
The callback does no optical work and the camera streams still publish
completed exposures independently.

Pkg builds the small command receiver against the installed PipeWireAO JLL.
Building from source requires a C compiler. After `Pkg.develop` of this
checkout, run `Pkg.build("AdaptiveOpticsSimPipeWireHIL")` in that project.

## RTC-managed sources

A managed source is a HIL process with the optional PipeWireAORTC extension
loaded. Its instrument profile selects the existing sealed simulation
configuration and the exact bootstrap and SourceControl node names.

Resolve the launcher environment with Pkg before exporting or testing it.
For a source checkout, run `Pkg.develop(path = "/path/to/AdaptiveOpticsSimPipeWireHIL")`
in the launcher project to refresh the manifest's extension registration.
The managed launcher uses the same environment as the deployed profile.

```julia
include(joinpath(dirname(Base.find_package("AdaptiveOpticsSimPipeWireHIL")), "managed.jl"))
```

The launcher publishes Preparing before loading HIL and AOS. It takes
`--config`, `--remote`, `--control-node`,
`--bootstrap-node`, and `--bootstrap-instance`. The profile supplies process
placement and any required physical-offset input. HIL owns the prepared
simulation, its data nodes and its cursor. RTC admits the process and owns the
Links. Bootstrap runs on the existing RTC monitor. The source task handles
SourceControl between model duties and at cooperative checkpoints during CPU
compiled optical propagation. Status reports the last completed sample. Hold
parks the current sample before exposure publication. A subsequent Run resumes
that sample, while Reset or bootstrap Quit unwinds it on the owning task.
Lockstep Pause waits for the matching command to be adopted. A held Reset
advances generation and clears the cursor before acknowledgement.

The managed deployment requests SIGINT for a process stop. The launcher
unwinds the owner scope on that interrupt and routes cancellation through the
bootstrap monitor, so PipeWire objects close in their owning context.

The extension supports the lockstep, external-pupil, clocked optical and
pupil-source instrument configurations. It shares the same HIL transports
and external-pupil execution used by independent consumers.

## Standalone resolved simulations

`AdaptiveOpticsSimPipeWireHIL.Simulation` loads portable resolved simulation
products without loading `PipeWireAORTC`. Schema 1 preserves the lockstep API;
schema 2 supports external-pupil and clocked optical execution. Instrument authoring
must first be resolved by its owning exporter; this loader does not interpret
instrument settings, templates or optical prescriptions.

```julia
using AdaptiveOpticsSimPipeWireHIL: Simulation
configuration = Simulation.load("simulation/simulation.yaml")
prepared = Simulation.prepare(configuration) # numerical preparation; no core
Simulation.run!(prepared; frames=100) do science, transport
    # The caller admits the inactive endpoints on the first call and owns links.
    # Later calls decide whether the next exchange may proceed.
    true
end
```

`run!` opens ordinary HIL endpoints, invokes the caller before starting them and
before each exchange, and closes transport on completion or failure. Return
`false` to stop. The existing configured timeout bounds readiness and command
waits. This is a finite simulation API; it provides no session supervisor or
link policy. All existing acquisition correlation checks remain required. HEART
UDP peers do not automatically satisfy those metadata contracts.
Create caller-owned links through a separate session connection so their proxy
lifetime cannot block closure of the HIL connection.

The YAML manifest contains `schema_version: 1`, optional `mode: lockstep`, and:

- `graph`: `path`, lowercase hexadecimal `sha256`, and `bindings`. Each named
  binding declares `path`, `sha256`, logical `shape` and exact `eltype`.
- `hil`: `command_input` (one string or a list of named inputs), `frame_output`
  and positive integer `period_ns`.
- `pipewire`: the existing `PipeWireHILConfiguration` fields, with `rate` as
  `[numerator, denominator]` and encoding as `float32` or `uint16`. Named command
  node names, schemas and scales use matching YAML mappings.

Graph files use the existing AOS graph YAML schema. Binding files contain one
FITS image in HDU 1. Logical dimensions are retained exactly, without axis
permutation. Supported element types are `Float32`, `Float64`, `UInt8`, `UInt16`,
`UInt32`, `Int32` and `Bool`; Bool uses lossless UInt8 codes 0 and 1 in FITS.
Other types require exact FITS readback. Graph and binding paths are relative to
the manifest directory and must resolve inside it. The loader validates hashes,
shapes and types before native connection and retains owned snapshots; each
numerical preparation gets independent binding arrays. Files are bounded to
4 MiB for YAML, 256 MiB per binding array and 512 MiB in total, with at most 64
bindings. Loading or preparation does not establish live readiness or real-time
qualification.
Preparation checks response and multilayer-atmosphere sample periods against
the rounded integer model period. The advertised SPA rate remains independent
of this nanosecond rounding.

Focused CPU checks run with `julia --project=/path/to/test-environment
-e 'include("test/simulation.jl")'`. The focused finite transport selector is
`julia --threads=3,1 --project=/path/to/test-environment
test/simulation_private_core.jl`; it owns its private core and checks direct
ndarray loopback, admission rejection and control-failure teardown. Select a
matching native SDK with `PIPEWIREAO_HIL_TEST_PREFIX` when required. These checks
provide software evidence, not HEART, hardware or deadline qualification.

Schema 2 declares `mode: external_pupil` or `mode: clocked_optical` and contains:

- `graphs`: named AOS graph descriptors with `path`, `sha256`, and `bindings`
  as above. Optional `assets` is an array of `path`/`sha256` descriptors for
  compiled model metadata and arrays. Every path is relative to the bundle root.
- `optical.pupil`: the declared `schema`, `(2,p,p)` `shape`, and string
  `properties` of the external pupil contract.
- `optical.paths`: named paths declaring `graph`, `exposure_duration_ns`,
  `output`, `sampling_node`, and `sensor` (`schema`, string `properties`). Optional
  `response` declares its `graph`, `input`, `output`,
  `normalized_to_photon_rate`, `pixel_sampling_m`, `exposure_duration_s`, and
  `frame_schema`. Optional `chopping` declares `graph`, `initial_phase`
  (`fringed` or `unfringed`), and `trigger_high_is_fringed`.
- `pipewire`: `remote`, matching `sensor_node_names`,
  `sensor_buffers`, and `reference_clock` (`realtime`, `monotonic`, or `tai`).
  External-pupil execution requires `pupil_node_name`. Clocked execution requires
  `command` with `node_name`, `schema`, string `properties`, and `buffers`, and
  rejects `pupil_node_name` because it opens a command input instead.
- Clocked execution requires `source`: `graph`, `command_input`, a FITS
  `amplitude` binding descriptor, `pupil_axes: [2,1]`, positive `period_ns` and
  `command_limit`, and nonnegative `pacing_period_ns` (zero means unpaced).

The loader owns graph text, binding arrays, and verified compiled asset bytes.
The aggregate snapshot limit is 512 MiB; shared compiled assets count once.
Compiled nodes must name a declared `model_path` with a matching `model_sha256`.
Preparation restores those bytes under a private temporary directory and keeps
it alive through both AOS graph loading and numerical preparation. AOS owns
model parsing and all numerical behavior. Prepared records expose no reusable
definition containing temporary model paths.

Each optical path, response, chopping graph and source gets an independent
numerical owner even when roles reference the same descriptor. Preparation
validates pupil dimensions and response contracts, refreshes execution metadata,
warms the ordinary HIL execution, and restores initial model and detector state.
CPU execution defaults to `StreamGraphExecution`; accelerator optics require
`CapturedGraphExecution` and fail without capture support. Preparation opens no
core and establishes no transport readiness. Runtime graph bindings are copied
to the selected device; construction calibrations remain on the host. Clocked
source graphs retain their existing host stream execution contract.

```julia
using AdaptiveOpticsSimPipeWireHIL: Simulation, ExternalPupil
prepared = Simulation.prepare(Simulation.load("simulation/simulation.yaml"))
completed = Simulation.run!(prepared; frames=100) do execution, streams, now_ns
    # Caller owns links and admission. Controls run on this single writer.
    # ExternalPupil.reset!(execution, streams) is permitted here.
    ExternalPupil.Run(ExternalPupil.snapshot(execution).deadline_ns)
end
```

Optical controls return `ExternalPupil.Run(deadline::Int64)`, `Hold()`, or
`Stop()`. Holds, pacing waits, rejected inputs, and resets consume no frame
budget; each accepted external-pupil sample or actual clocked source step does.
`run!` closes its streams and native owners on completion, control stop, or
failure. It provides no links, supervisor, or native control authority.

`test/simulation_optical.jl` checks schema-2 loading in the core suite without
Proper. The CPU suites `test/simulation_optical_preparation.jl` and
`test/simulation_compiled_optical.jl` run under `Pkg.test()`. Run either directly
in a test environment with Proper, YAML and FITSIO as direct dependencies.
They check real optical, response, chopping, and source preparation, independent
detector ownership, reset reproducibility, compiled asset relocation, and
tampering without opening a PipeWire core.
For schema 2, `sensor.frame_encoding` defaults to `float32`. Select `uint16`
for a detector response that produces raw ADC codes. The simulator checks the
range 0–65535 and rounds ties to even before staging the complete row-major
U16_LE frame. The sensor schema must agree with the detector output contract.

`test/external_pupil_stream_formats.jl` checks exact exposure rates, array
packing and compact metadata with a CPU-only environment.

The private-core suite `test/simulation_optical_private_core.jl` checks finite
clocked execution, Hold and reset controls, and stream cleanup on completion,
Stop, and failure. The suite uses the matching PipeWireAO JLL by default.
`PIPEWIREAO_HIL_TEST_PREFIX` selects a different matching SDK.
`test/simulation_optical_cuda.jl` adds direct CUDA to the optional environment
and disables scalar indexing. It checks captured optical and response execution,
device-resident runtime bindings, refreshed capture metadata, CPU optical
equivalence, and host source preparation. These checks provide software and
device-execution evidence; they do not qualify physical devices or deadlines.

See [HEART Standard DM and WFS interoperability](docs/HEART.md) for the
clocked optical port contracts and optional private-core verification.

On Julia 1.13.3 with CUDA and CUDACore 6.5.1, the clocked CUDA optical runner
retains the existing CUDA resource reclamation timer's libuv handle for the
duration of execution. This avoids the Julia heap allocation from removing
and reinserting that handle while the timer waits. The CUDA extension uses
private Julia APIs for these versions and leaves reclamation on its original
schedule.
