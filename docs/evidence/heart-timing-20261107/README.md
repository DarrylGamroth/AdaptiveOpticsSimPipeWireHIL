# Sustained HEART rtcExec timing on 2026-11-07

Two independent 60-second measurements at each nominal rate ran the full
HEART executor on the same host as standalone HIL. Each used a fresh
loopback-only network namespace and private PipeWire core, with a 3-second
warmup and a 250 ms return drain. The model used one 4 × 4 raw WFS and a
two-actuator mirror. Releases followed absolute clock slots independently of
command arrivals. See the [selector and reproduction steps](../../HEART.md#sustained-executor-timing).

| Run | Scheduled slots | Missed release | Released without complete WFS | Complete WFS at HEART | WFS cadence, Hz | Paired returns |
|---|---:|---:|---:|---:|---:|---:|
| 10 Hz 1 | 600 | 0 | 0 | 600 | 9.9998 | 596 |
| 10 Hz 2 | 600 | 0 | 0 | 600 | 10.0003 | 600 |
| 100 Hz 1 | 6,000 | 6 | 1 | 5,993 | 99.8837 | 5,974 |
| 100 Hz 2 | 6,000 | 5 | 29 | 5,966 | 99.4333 | 5,949 |

WFS rate is the number of inter-frame intervals divided by the time between
the first and last captured WFS frame starts. All capture drop counters were
zero. HEART's receive dump matched the reconstructed wire rasters in all runs.

Times below are milliseconds. Loop latency begins at HIL release and ends at
DM source publication. HIL observation includes the subsequent polling delay.
Latency percentiles use the paired cohorts above. WFS intervals use all
captured complete frames in the measurement window.

| Run | WFS interval p50 / p99 / max | Loop p50 / p99 / max | HIL-observed p99 / max |
|---|---:|---:|---:|
| 10 Hz 1 | 100.788 / 101.189 / 101.959 | 0.223 / 0.286 / 1.473 | 2.179 / 2.215 |
| 10 Hz 2 | 100.827 / 101.161 / 101.472 | 0.218 / 0.282 / 0.307 | 2.184 / 2.212 |
| 100 Hz 1 | 10.321 / 10.415 / 65.666 | 0.091 / 0.222 / 2.233 | 2.152 / 4.141 |
| 100 Hz 2 | 10.322 / 10.451 / 128.253 | 0.101 / 0.234 / 8.619 | 2.160 / 10.418 |

The 10 Hz runs omitted four latency pairs in total. The 100 Hz runs omitted
77, including 41 slots with no complete WFS transmission. Among complete
frames, the first 100 Hz run had nine missing and five repeated DM IDs.
The second had five missing and three repeated DM IDs and six WFS frames
with inconsistent segment IDs. Those categories can overlap. The remaining
unpaired rows lacked an admissible HIL command or a causal timestamp pair.
HEART logged circular-buffer counter jumps in three runs. The captures and
logs establish the failed deliveries and their layer boundaries. They leave
the causes of the scheduling gaps and inconsistent WFS segment IDs open.

Each controller acknowledged SHUTDOWN after the timed interval, then returned
failure while closing its secondary telemetry stream. The stock telemetry
client timed out while waiting for data and disconnected. Each run passed eight checks for setup, timing, and lineage and failed the
clean-shutdown check.
This is measured software behavior on a shared host under ordinary
scheduling. The results do not establish a loss-free rate or a deadline
qualification.

## Provenance

- HIL runtime `60b60af` and the benchmark selector accompanying this record.
  Julia 1.13.3 with `--threads=3,1` and `OPENBLAS_NUM_THREADS=1`.
  The science environment excluded PipeWireAORTC.
- HEART `d5ba5941a6647fd27d2dc034b53f09b3eeb3a4a9`, DAO
  `65f05a53243a37912aa6c9fbd2205b5b83a71b56`, and CFITSIO 3.48.
  The executor used its normal `-O3` build with `-pg` at link time, `-rt 0`, and
  the existing test-only
  busy-poll shim for the namespace's missing sysctl.
- HEART SPA plugin `35a75e5`, GCC 14.2, Meson release with no LTO.
  PipeWireAO 0.9.2 and PipeWireAO_jll 4.0.2+0. The optimized plugin passed all
  six Meson suites.
- AdaptiveOpticsSim 0.3.22 at `c058f32f` and Proper 0.4.1.

The verification used a Linux x86_64 workstation with an AMD Ryzen 7 6800H.
See the reproduction steps linked above to generate timing rows, packet
captures, and controller logs for a new run.
