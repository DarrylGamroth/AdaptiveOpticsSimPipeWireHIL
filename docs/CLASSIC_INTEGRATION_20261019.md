# Classic image-source integration, 2026-10-19

The dedicated `codex/classic-registered-20261019` worktree starts at clean main
`3c3de5b` and integrates optional image-source commits `245b4b8`, `26e1f29`,
`1403ea4`, `06f7d41` and `984d499`. The two cherry-pick conflicts were limited
to the prepared owner's type parameters. Both acquisition-source identity and
the independent optional image owner are retained.

The acquisition source implementation and the mandatory exposure staging,
command validation and adoption paths retain main's SPIDERS behavior. Image
sources carry originating acquisition identity and duration while Header PTS
remains a publication observation; no physical exposure-start mapping is added.
Images use an independent native loop and never gate mandatory exchange.
Package version is 0.1.3 for a future immutable release. No registration was
performed by this integration.

## Dependency closure and reproduction

The isolated test environment developed only the HIL package. Scientific and transport
dependencies are resolved from the registry:

| Package | Version | Registered tree |
|---|---|---|
| AdaptiveOpticsSim | 0.1.0 | `ee0a2a24f600954045d08d48971f1ce0d0d61b4b` |
| PipeWireAO | 0.6.18 | `14dcef6fff816e5fc89459aca21cd1742b497be3` |
| PipeWireAO_jll | 1.7.1+0 | `17e508fb0310a12153384dca1fc88c860955d2d2` |

The selected native artifact is
`3c63168ffd94740be1e2ea34c1eaf12a9468622a` (x86_64 Linux AVX2).
The full suite used Julia 1.13.2 and two threads. The test harness cleared
inherited native-library overrides and used a short temporary runtime root to
stay within the Unix socket path limit. Each live fixture starts its own private
core. The node-publication fixture honors `PIPEWIREAO_HIL_TEST_PREFIX`, as the
calibration fixture does. Alternate native libraries must match the selected SDK.

## Native compatibility observation

An initial attempt selected a local installation through environment-local JLL
preferences and the fixture prefix. That installation reports native 1.7.0 and
lacks `spa_meta_acquisition_set_trigger_classification`. Existing SPIDERS v3
metadata tests errored at that missing symbol: 704 assertions passed, two tests
errored, and the live tests were not reached. All image unit assertions passed.
This demonstrates an installed-native compatibility limit; the image merge does
not change the v3 implementation. No installed library was modified.

The failed attempt's log and preference file are retained as
`native-1.7.0-tests.log` and `native-1.7.0.LocalPreferences.toml` in the test-state
folder. The supported full-suite run uses the registered 1.7.1+0 native artifact.

## Software results

The final full suite exited successfully with 1,678 assertions:

| Suite | Passed |
|---|---:|
| Package unit tests and Aqua | 777 |
| Acquisition model-time capture | 17 |
| Private-core node publication and optional image delivery | 136 |
| Private-core calibration transport: throwing submission | 374 |
| Private-core calibration transport: submission deadline | 374 |

The image tests cover owned snapshots, exact asymmetric row-major payloads,
originating sequence/domain/generation/duration, absent physical start/timebase,
zero warmed staging and native-callback allocation, no-viewer replacement,
concurrent coherence, independent loop access, stale generation rejection,
reset/stop behavior and deterministic teardown of every optional stream, core,
context and loop. The private core and temporary runtime directories were
removed on successful completion. The failed long-path attempt is retained as
`long-runtime-path-tests.log`; using the short runtime root allowed all live
checks to complete with the same selected registered native artifact.

`git diff --check` passed, and the integrated diff was inspected for unrelated
changes. The SPIDERS acquisition source file is the same git blob as main's
`3c3de5b` (`36ad78416cd30248b10cb405f77e28e51a2a4fb3`).

## Validation scope

These are software tests and private-core transport checks. They do not qualify
physical hardware, optical accuracy, deployed cadence or real-time latency.
The complete log is retained as `tests.log` in the test-state folder.
