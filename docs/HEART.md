# HEART Standard DM and WFS interoperability

The standalone clocked optical simulator can receive HEART Standard DM commands
and send HEART Standard WFS pixels through the HEART SPA plugins.
The application that owns the PipeWire session creates two explicit links.

| Link | Ndarray contract |
|---|---|
| `api.heart.std-dm.source` → HIL command input | `org.heart.std-dm.actuator-command/1`, row-major F32, one value per actuator in metres |
| HIL sensor output → `api.heart.std-wfs.sink` | Detector rows × columns in row-major order at the inverse of the exposure duration. Use `org.heart.std-wfs.raw-pixels/1` with U16 for the Standard WFS handler in `hrtRtc`. A peer that consumes calibrated frames can use `org.heart.std-wfs.calibrated-pixels/1` with F32. |

Use schema 2 with `mode: clocked_optical`. The AOS graph input, HIL command
stream and DM source must agree on extent and actuator order. The DM source
already converts microns to metres. Set `node.driver = true` on the DM source
so completed UDP commands drive that PipeWire link. The HIL sensor stream drives
its own link when it has a completed exposure. For `hrtRtc`, set the HIL
`sensor.frame_encoding` to `uint16`, use a detector response that produces raw
ADC codes, and set `pixel-type = raw` on the WFS sink. The wire-only test uses
`pixel-type = calibrated` with an ideal calibrated detector response.

The DM source supplies a local monotonic reference timestamp when publishing a
completed command. The simulator applies commands at its own model-step
boundary and holds the last admitted command between arrivals. It rejects
discontinuous buffers, including the first command after source startup, and
accepts subsequent commands in increasing Header sequence order. A sender
restart or 32-bit frame ID wrap that resets the sequence requires the caller to
re-establish stream admission. The UDP wire timestamp carries no acquisition
domain, generation or exposure timing. A HEART controller using this DM source
must use its send-only mode (`dmSendsResponse` disabled).

## Wire routines on a private core

Build the HEART SPA plugin against the PipeWireAO SDK with compact metadata.
Provide the directory containing `heart/libspa-heart.so` via
`HEART_TEST_SPA_DIR`. A staged Meson install suffices. Set `HEART_TEST_SOURCE`
to a checkout or archive of HEART containing `source/device/{include,src}`.
The test compiles the original Standard DM, Standard WFS and checksum routines
in a temporary directory. The test links the two actual SPA factories to
`Simulation.run!` on a private core and uses IPv4 loopback UDP.

```sh
export HEART_TEST_SOURCE=/path/to/heart
export HEART_TEST_SPA_DIR=/path/to/staged/lib/spa-ao-0.2
julia --startup-file=no --threads=3,1 --project=/path/to/science-test-env \
    test/heart_private_core.jl
```

The test environment needs HIL, AdaptiveOpticsSim, Proper, PipeWireAO,
PipeWireAO_jll, FITSIO and YAML as direct dependencies. Julia 1.13 and a C
compiler named `cc` were used for verification. By default the test runs the
daemon shipped by PipeWireAO_jll. Use `PIPEWIREAO_HIL_TEST_PREFIX` to select a
matching SDK explicitly. The test uses the same-endian WFS encoding and the
deployed zero checksum to match the baseline HEART decoder.

The test verifies two commands and two segmented WFS exposures, including the
change in raster caused by the second command, frame IDs, a common timestamp
across each frame's datagrams, and stream cleanup. The optical oracle uses an
independent CPU preparation with an ideal, noiseless photoelectron response.
This selector exercises HEART's original wire routines. The full executor
selector below uses a complete WFS-to-DM controller pipeline.

### Wire verification record, 2026-11-07

The peer used HEART `d5ba5941a6647fd27d2dc034b53f09b3eeb3a4a9`. The staged
plugin was built from `35a75e5` against PipeWireAO_jll 4.0.2+0. The Julia
1.13.3 environment used PipeWireAO 0.9.2, Proper 0.4.1 and AdaptiveOpticsSim
0.3.22 at `c058f32f`. The environment excluded PipeWireAORTC.

- All six plugin suites passed in the normal build and under AddressSanitizer
  and UndefinedBehaviorSanitizer.
- The staged plugin passed all 13 HEART roundtrip checks twice. The existing
  optical private-core test also passed all 12 checks.
- The daemon logged a mixer warning for the ndarray WFS sink. Both single-port
  links streamed and delivered the expected UDP payloads.
- The broader HIL suite passed at the pre-change revision `3bd9742`. The first
  run with the test changes hit a segmentation fault in `do_trigger_driver`
  during lockstep node publication. A later run passed the 1,515 core checks,
  22 model-time checks and 374 node-publication checks, then failed the
  Float32 held-probe completion deadline. Those selectors load no HEART
  plugin. The cause of the broader-suite failures was not established.


## Full executor on a private core

The [two-actuator test configuration](../test/data/heart/rtc.yaml) runs the
original `hrtRtc` executable from `source/rtcExec`. It enables one Standard WFS
receiver, one Shack-Hartmann subaperture, a 2 × 2 reconstructor, and one Standard
DM sender. The HIL test owns an independent CPU optical model and its two
PipeWire links. The executor's API client configures the pipeline, loads the
reconstructor, enters READY and closes the loop. The test advances six
exposures and waits for the returned command after each exposure.

Build the HEART checkout using its normal generators and the DAO and CFITSIO
libraries. For the pinned revision below, the executor and CLI can be built
from the HEART root with these targets. The HEART generators need Python with
PyYAML and pytest on `PATH`.

```sh
export DAO_ROOT=/path/to/daoinsw
export CFITSIO_ROOT=/path/to/cfitsio
export PYTHONPATH="$DAO_ROOT/source/python:$PWD/source/python"
export PATH="$DAO_ROOT/bin:$PWD/source/aoTypes/bin:$PATH"
make -C external all
make -C source/util/src build
make -C source/math/src build
make -C source/aoTypes/gms build
make -C source/aoTypes/apiDef build
for component in aoTypes commands wfsProc config cmdHandler device blocks testServer pipes monitors; do
    make -C "source/$component/src" build || exit
done
mkdir -p bin/hrtApi
export API_CLI_BIN="$PWD/bin"
make -C source/rtcExec/src build
```

Run from the HIL repository root using the same standalone science environment
as the wire selector. Use a staged plugin build and absolute executable paths.
The invocation creates a loopback-only network, a private shared-memory mount,
and a private PipeWire core. It requires Linux user namespaces, `unshare`,
`ip`, and `timeout`. It runs the controller without real-time priority and
uses temporary FITS and GMS state.

```sh
export HEART_TEST_EXECUTOR=/path/to/heart/source/rtcExec/bin/hrtRtc
export HEART_TEST_CLI=/path/to/heart/source/aoTypes/bin/hrtRtcCli
export HEART_TEST_SPA_DIR=/path/to/staged/lib/spa-ao-0.2

unshare -Urnim --fork --kill-child -- sh -ec '
    mount -t tmpfs tmpfs /dev/shm
    ip link set lo up
    ulimit -c 0
    exec "$@"
' sh julia --startup-file=no --threads=3,1 --project=/path/to/science-test-env \
    test/heart_rtcexec_private_core.jl
```

On this host, the private network namespace omitted
`/proc/sys/net/core/busy_read`. The pinned DAO code passed its failed lookup
as a negative `SO_BUSY_POLL` budget and HEART rejected READY. The test preloads
a [small compatibility shim](../test/heart_busy_poll.c) into `hrtRtc` only
when that file is absent. It substitutes zero for the invalid budget and
uses the real socket call. The HEART executable and its UDP codecs were built
from unchanged sources.

### Full executor verification record, 2026-11-07

The test used HEART `d5ba5941a6647fd27d2dc034b53f09b3eeb3a4a9`, DAO
`65f05a53243a37912aa6c9fbd2205b5b83a71b56`, CFITSIO 3.48, and the
staged HEART SPA plugin `35a75e5`. HIL used Julia 1.13.3, Proper 0.4.1,
AdaptiveOpticsSim 0.3.22 at `c058f32f`, PipeWireAO 0.9.2, and
PipeWireAO_jll 4.0.2+0 in an environment without PipeWireAORTC.

All 17 executor checks passed. The full HIL suite passed 3,666 checks and the
wire-only selector passed its 13 roundtrip checks with the UInt16 transport
changes in place.

The test compared all six simulator rasters exactly with HEART's
`HOP0.wfs.pixelsRaw` dump. Four actuator commands applied by the simulator
matched the preceding `WCC.dm0.dmOutput` entries after microns-to-metres
conversion. The two actuator values differed, the raw raster changed, and
HEART acknowledged SHUTDOWN and exited successfully. The test covers this
finite software loop. Instrument calibration, hardware and sustained timing
remain separate validation.

## Sustained executor timing

Use the same two-actuator fixture and private namespace as the finite test.
Build and stage the HEART SPA plugin with Meson `--buildtype=release`. The
timing selector also needs `tcpdump`, `tshark`, and the original HEART
`source/util/bin/hrtTelemetryTester`. This client connects to HEART's
secondary TCP telemetry stream. In the recorded runs it timed out before
shutdown. The selector records the timed data even when its shutdown check
fails.

Use the namespace invocation above with `test/heart_rtcexec_timing.jl` in
place of the finite selector. Set these additional variables for each fresh
process and private core.

```sh
export HEART_TEST_TELEMETRY=/path/to/heart/source/util/bin/hrtTelemetryTester
export HEART_TEST_HZ=100
export HEART_TEST_SECONDS=60
export HEART_TEST_TIMING_DIR=/path/to/new-output-directory
```

The configured exposure and nominal WFS rate follow the selected profile. Each
absolute host-clock slot offers one exposure and advances two model substeps.
The runner keeps the last admitted command and never waits for a command to
schedule the next exposure. An expired slot counts as missed, with no burst of
catch-up exposures. The selector excludes three seconds of warmup and observes
returns for a further 250 ms after the final slot.

- WFS cadence uses consecutive first-datagram capture times on loopback.
- Wire latency uses the first WFS datagram and its corresponding DM datagram.
- Loop latency starts when HIL releases an exposure for simulation and ends
  at the DM source's monotonic publication stamp, recovered from the copied HIL
  input receipt. It includes optical computation, both PipeWire links and HEART
  processing. A second measurement ends when the serialized HIL runner
  observes the receipt, which includes polling delay.
- Scheduled loop latency starts at the absolute due time and includes release
  delay. All latency distributions describe successfully paired returns.
  Missing and ambiguous returns remain visible in the slot and sequence counts.

HEART assigns DM IDs from its receive-side synchronization counter. The
selector reconstructs each WFS raster from the packet capture and checks the
entire receive history against HEART's raw-pixel dump. It checks the outgoing
DM payloads against HEART's actuator output dump. It uses that WFS receive
order to map DM IDs back to the originating HIL exposures. Repeated DM IDs
and inconsistent WFS headers or payloads are excluded from latency pairing.
The capture and host monotonic clock are used for separate intervals. No
wall-clock to monotonic-clock conversion is needed.

The timed path retains one copy of each simulated raster in preallocated test
storage. The observer reads the copied command receipt using the existing
single-attempt guard and never drains or applies the command itself. The
ordinary HIL runner continues to own command admission. Output contains
`frames.csv` with one row per offered slot, `summary.toml`, the packet capture,
HEART dumps, and process logs. A zero timestamp marks an unavailable event.

The [four 60-second runs](evidence/heart-timing-20261107/README.md) record
the achieved cadence, WFS and DM losses, latency distributions, and shutdown
status.
