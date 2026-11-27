# Optional image source transport validation

Validated on 2026-10-11 in the dedicated
`AdaptiveOpticsSimPipeWireHIL-optical-sources` worktree, starting from
`2c15a9f062532e43b69076429465482f4297a0e9`. The worktree had no pre-existing
changes. No normal PipeWire session, service configuration or canonical dirty
checkout was changed.

## Scope and contract

Optional rank-two Float32 ndarray sources own fixed host matrix snapshots.
The serialized optical owner provides completed images and the originating
WFS sequence, Header PTS and acquisition generation. The native callback only
claims one snapshot, copies it in row-major order, sets public Header and
acquisition identity and parent WFS duration, and queues or returns the borrowed
buffer. Physical exposure start is absent, so acquisition timebase is absent;
Header PTS remains the original publication observation. Staging and
the callback try once to claim ownership. Busy staging attempts return false;
new snapshots replace older pending snapshots. Optional failures stop only the
affected image source. The sources have independent observer clocks and never
participate in mandatory exchange completion.

The zero allocation contract covers warmed successful staging with a concrete
`Matrix{Float32}` of the declared shape and successful complete callback copies.
Cold preparation, reset, resource teardown and exceptional paths are outside
that contract.

## Observed evidence

The complete package suite passed 1,549 assertions with Julia 1.12.7, two Julia
threads, and CPU affinity restricted to CPU 15:

```sh
JULIA_NUM_PRECOMPILE_TASKS=1 taskset -c 15 julia \
  --startup-file=no --threads=2 \
  --project=/path/to/hil-env \
  test/runtests.jl
```

| Suite | Passed |
|---|---:|
| Package unit tests and Aqua | 678 |
| Acquisition model-time capture | 17 |
| Private-core node publication and optional source delivery | 106 |
| Private-core calibration transport: throwing submission | 374 |
| Private-core calibration transport: submission deadline | 374 |

The image unit fixture uses an asymmetric 2 by 3 matrix to establish exact
row-major byte order, snapshot ownership after the optical owner mutates its
input, matching Header and acquisition identity, complete extents and strides,
busy drops, replacement, absent source buffers, reset, queue failure isolation
and concurrent snapshot coherence. The warmed staging-plus-callback cycle
allocates zero bytes.

The native fixture starts its own temporary runtime and private PipeWireAO core.
An ordinary independent driver sink receives the exact asymmetric payload and
original Header PTS, acquisition domain, generation, sequence and duration.
The actual `StreamBuffer` callback queues complete mapped buffers with zero
warmed allocation; public `stage_pipewire_image!` also measures zero bytes.
The controlled delivery adds no drops. Three prior observations without a
viewer deliberately retain the latest snapshot and count two replacements.
Removing the viewer leaves subsequent staging safe. Image-only reset preserves
the mandatory generation. Full reset rejects an image carrying the old captured
generation, stop clears pending images, and close removes the optional nodes.

The raw log is retained at
`hil-tests.log`.
An informational Julia signal collected a compiler stack during cold fixture
compilation; the process subsequently exited successfully.

## Dependency selection and limits

The disposable environment selects the clean HIL worktree, the working installed
AdaptiveOpticsSim snapshot under
a local package installation,
and that snapshot's selected PipeWireAO 0.6.17 and PipeWireAO_jll 1.7.0+19 dependencies.
No dependency was changed in the installed environment.

The separate clean AOS optical worktree at baseline `7f2b3aa` lacks
`PreparedGraphCalibrationBoundary`, which this HIL baseline already requires.
Testing transport against the installed compatible AOS preserves the existing
calibration API. No HIL calibration implementation was changed to conceal that
baseline mismatch. Optical computation and complete REVOLT/RTC integration
remain outside this transport validation and belong to their respective owners.

These results establish software transport behavior and warmed allocation for
the tested host image path. They do not establish instrument optical accuracy,
deployed cadence, tail latency, hardware behavior or an RTC stutter guarantee.

## Review follow-up: cold staging and callback placement

Review finding OPT-003 identified that the application warms its observation
worker while the prepared HIL endpoints are inactive. Public staging returns
before `_stage_image!` in that state, so this warmup did not exercise the
successful snapshot-copy path. Image source preparation now calls the exact
production leaf with its owned host matrix and a synthetic identity, then clears
the snapshot before constructing or connecting the native stream. The synthetic
image cannot be published. The existing complete callback precompilation remains
in place. Native regression assertions check zero sequence, PTS, generation and
drop count, no pending/owned/failed snapshot, and all-zero image storage directly
after preparation.

The focused applied-command, image and native private-core rerun passed all
253 assertions, including 114 native assertions, with the same CPU 15 affinity,
two Julia threads and compatible installed dependency environment. The log is
`hil-cold-staging-tests.log`.

At revision `26e1f29`, source inspection established that both optional images
and mandatory WFS streams used the HIL owner's same core, context and native
ThreadLoop. The selected
SDK rejects `PW_STREAM_FLAG_RT_PROCESS` in `src/stream.jl:803`. Its process event
enters the concrete `@cfunction` stream callback and directly invokes
`on_process`; there is no separate image callback dispatcher or application
runtime hook. The SDK documents automatic native-thread adoption by Julia's
`@cfunction` boundary (`src/ndarray_filter.jl:286`).

The local native source at `9cf2006459f6329a2bb42bd2f65266cfed4ba706` sets the
non-RT stream's default `node.loop.class=main` and `node.async=true`
(`src/pipewire/stream.c:2102`). Context loop selection maps that class to the
context's main loop (`src/pipewire/context.c:829`). Its ThreadLoop contract says
callbacks run with the native loop lock held (`src/pipewire/thread-loop.h:62`).
This local source inspection supports the scheduling analysis; it is not an
instrumented observation of the selected JLL binary.

Consequently, independent observation clocks and producer threads prevented
observer backpressure but did not isolate callback residence time. A complete
row-major image copy occupied the shared callback loop and could postpone WFS
callbacks and loop-lock access. A 480 by 480 Float32 image carries 921,600 bytes;
at 20 Hz, its payload rate is 18.432 MB/s and the copy reads and writes at least
36.864 MB/s. These rates do not bound per-callback duration. The next useful
measurement is actual image callback residence alongside WFS frame/command
timings under the selected deployment, including observer connection and
disconnection. No independent-loop redesign follows from source inspection
alone.

## Independent image loop and discriminating regression

The confirmed shared native-loop ownership has now been removed. One private
`ImageSourceOwner` owns a single ThreadLoop, Context and CoreConnection to the
same configured remote for the complete optional image tuple. No image owner or
native image resources are created for an empty tuple. `PreparedPipeWireHIL`
retains its `image_sources` tuple and additionally references `image_owner`;
public options, staging, declarations and stream-access signatures are unchanged.

Cold image preparation and node inspection use that owner's connection and
loop. Image activation, flushing, reset and teardown take only its loop lock.
`with_image_stream` now locks the image loop. Mandatory WFS and adopted-command
endpoints retain their existing connection and loop. Their lifecycle calls
release the WFS lock before handling optional images. Image callbacks never
take the WFS loop lock, and their copied acquisition identity retains the same
caller-provided frame origin. Producer staging still takes no loop lock. The
caller serializes lifecycle calls and joins the optical producer before reset.

The native regression deliberately holds the image loop lock for at least
100 ms in a task fixed to its acquisition thread. While it is held, another
task accesses `with_frame_stream`. The test requires frame access to complete
within the 50 ms observation window and asserts that public `main_loop` results
for image and frame streams are distinct. These time limits discriminate lock
sharing; they are not an RTC latency contract.

The same lock probe ran against an archived `26e1f29` baseline using the same
installed dependencies, private-core fixture and CPU affinity. It observed
`BEFORE_IMAGE_LOOP_DISTINCT=false` and
`BEFORE_FRAME_ACCESS_WHILE_IMAGE_LOOP_HELD=false`. The independence assertion
failed, while all 114 existing native assertions passed. Baseline resources were
closed by the fixture's normal teardown. The log is
`hil-loop-isolation-before.log`.

After isolation, the final focused applied-command, image, lifecycle and native
private-core suite passed 314 assertions: 182 unit/lifecycle and 132 native.
The same successful complete native callback and public staging measurements
remain zero bytes after warmup. Tests retain row-major payload and full
acquisition identity checks, viewer loss, old-generation rejection, reset and
stop drain, node removal and closure of the independent core, context and loop.
A two-image fixture proves both streams share one image core and native loop.
A cleanup fault fixture proves a stream-close error does not prevent attempts
to close the other image, core, context and loop. The final log is
`hil-loop-isolation-final.log`.

This removes serialization on the mandatory native loop. CPU scheduling,
shared Julia GC and deployment resource contention can still affect observed
timings; actual WFS latency evidence belongs to deployment qualification.

## Native image thread name

The image loop's cold native thread name is now `AOS-HIL-images` (14 bytes).
The previous long name and mandatory WFS name both truncated to
`AdaptiveOpticsS` in Linux `comm`, preventing deployment from distinguishing
their thread roles by name. The adapter assigns no CPU affinity or scheduling
policy; deployment owns those settings.

The native private-core suite passed all 134 assertions on CPU 15 with two Julia
threads. On Linux, the fixture reads actual `/proc/self/task/*/comm` entries
during preparation and observes exactly one `AOS-HIL-images` thread alongside
the mandatory `AdaptiveOpticsS` thread. Existing public loop distinction,
independent lock access, successful zero-allocation staging/copy, acquisition,
reset, teardown and shared multi-image group assertions remain passing. The log
is `hil-image-thread-name-tests.log`.

## Publication PTS and physical exposure start

Review on 2026-10-12 found that the optional image callback had copied Header
PTS into acquisition exposure start. Header PTS records source publication; the
ideal derived image does not establish a qualified physical exposure timestamp.
The optional image callback now initializes reusable acquisition metadata and
sets only the original WFS domain, generation and sequence plus parent WFS
duration. It never calls the exposure-start setter. Both public
`acquisition_exposure_start` and `acquisition_timebase` return `nothing`.
Mandatory WFS metadata and source publication behavior are unchanged.

The updated unit regression failed before removing the setter: the observed
exposure start was 20 rather than absent. The log is
`hil-image-exposure-before.log`.
Native delivery assertions also require absent exposure start and timebase,
while retaining exact Header PTS, original identity and parent duration checks.

The focused applied-command, image, lifecycle and private-core suite passed
318 assertions with the documented CPU 15 affinity and `--threads=2` setup.
Successful warmed image staging and the complete native source callback still
allocate zero bytes. The log is
`hil-image-exposure-after.log`.
An initial `--threads=2,0` run passed the corrected metadata and allocation
assertions but reached no queued image during the concurrent fixture's short
unpaced producer burst. That fixture's scheduling-sensitive positive-delivery
assertion failed; its coherence and ownership assertions passed. The production
ownership/drop policy was not changed in response.
