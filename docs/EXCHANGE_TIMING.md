# Optional owner-side exchange timing

Version 0.2.2 integrates optional owner-side timing from commit `e57c6f0`
and the suppressed-retry correction from `963bd48` onto the released 0.2.1
adapter. The original changes were reviewed as versions 0.1.4 and 0.1.5 on
`codex/classic-registered-20261019`.

`PipeWireExchangeTiming()` is a reusable record for the serialized scientific
owner. Pass it to either `exchange_frame!(prepared; timing)` or
`exchange_frame!(prepared, model_time_driver; timing)`. The default
`timing=nothing` specializes to timing helpers that neither read a clock nor
write a record. Existing native callbacks, checks, waits and command adoption
retain their order.

All nine fields are `UInt64`. Seven timestamps use the same host-monotonic
`time_ns()` clock as `PipeWireFrameCommandTiming`:

| Field | Boundary |
| --- | --- |
| `started_ns` | On entry, before lifecycle checks |
| `ready_ns` | After checking the frame driver and awaiting the initial native driver cycle |
| `step_started_ns` | Before the AOS graph/HIL frame step, after prior-exchange checks |
| `step_finished_ns` | After the completed AOS step and synchronized frame copy to the host |
| `staged_ns` | After staging exposure metadata and converting the complete transport payload |
| `adoption_started_ns` | After source publication, matching command receipt and the final driver-cycle wait |
| `finished_ns` | After the matching command has been adopted |

The record is cleared on entry. `generation` and `sequence` become positive
only after a successful completed exchange. A thrown exchange leaves both zero;
the reached phase timestamps may help diagnose the failure, and unreached
timestamps remain zero. A prior successful sample cannot survive a new failed
exchange as valid attribution.

Only the scientific exchange owner may mutate, reuse or read the record. It is
not synchronized for readers on other threads. Copy its values into a bounded,
preallocated owner-side collection if a sequence of samples is needed; repeatedly
retaining the same mutable object does not retain historical samples. Export
records after measurement, outside callbacks and exchange waits.

Read `frame_command_timing(prepared)` after successful return for the existing
matching `source_published_nanoseconds` and `command_received_nanoseconds`.
Check that its sequence matches the returned exchange sequence. These separate
observations identify the source-publication-to-command-receipt interval within
the exchange; no new callback timestamp is added.

Opt-in clock reads perturb the measured path. The record supplies phase
observations, not a latency distribution, frame scheduler, production deadline
guarantee or physical device-application timestamp. Exact performance requires
measurement on the selected deployment and comparison with its untimed path.

## Version 0.2.2 integration evidence

The integration starts from released adapter revision `826685f`, retains
AdaptiveOpticsSim 0.2 compatibility, and requires PipeWireAO 0.6.18 for its
`AcquisitionTriggerState` API. Fresh-process validation used Julia 1.13.2,
released AdaptiveOpticsSim 0.2.1 and PipeWireAO 0.6.18, and one Julia and
OpenBLAS thread. The dependencies were pinned in a separate test environment.

The same 368-assertion exchange, wait, driver-cycle, transport and adoption
fixture failed only the suppressed-retry delay assertion before the correction
(367 passed), then passed all 368 assertions after it. The timing helpers also
retain zero warmed Julia heap allocation. Two additional LLVM checks confirm
that the default helper chain contains no calls and returns immediately.
These helper checks do not measure complete live-exchange allocation or latency.

`Pkg.test("AdaptiveOpticsSimPipeWireHIL"; allow_reresolve=false)` passed all
1,753 assertions: 847 package checks including Aqua, 17 model-time checks,
141 private-core publication checks, and 374 checks for each calibration
failure mode. All native tests used temporary isolated runtime directories and
private core names. This includes the new closed-owner timing-invalidation
checks. No system-level RTC cadence or physical-device validation is claimed.

The verification logs are
`setup.log`, `retry-fail-before.log`, `retry-pass-after.log`, and `pkg-test.log`.

## Original focused software evidence

The 2026-10-19 change starts from HIL revision
`48407618405ce8e906f245b59fd49d5a9ba5c00e`. Focused exchange, completion-wait
and driver-cycle tests pass 93 assertions on Julia 1.13.2 in the existing
`hil-tests/env` environment. The new helper checks cover cleared initialization,
monotonic phase order, successful generation/sequence attribution, stale-sample
invalidation after an interrupted attempt, and zero warmed Julia heap allocation
for both the timed and default helper chains.

Optimized LLVM for `_test_complete_exchange_timing!(::Nothing)` is a bare
`ret void`, with no calls or clock reads. This checks every added timing helper
under the default specialization; it is not a whole live-exchange throughput
measurement. The log is
`hil-exchange-timing-focused.log`.
The timed helper chain contains seven clock calls and direct field stores.
Aqua's 11 checks also pass; this LLVM and package-quality evidence is retained in
`hil-exchange-timing-codegen-aqua.log`.

The private-core test also checks that a timed call against a closed prepared
owner invalidates prior successful attribution before throwing, and that the
default call still throws. These live checks were added but not run as part of
the focused change. The existing private-core suite does not execute a complete
frame/command exchange, so a full timed/default exchange allocation comparison
requires the separate owned integration harness. No services were started by
the focused checks.

## Original version 0.1.5: suppressed trigger retries

The completion wait retains a one-millisecond cadence between issued retry
triggers. Previously, a trigger attempt suppressed by an outstanding native
driver cycle also consumed that cooldown. If the outstanding cycle completed
just after the suppressed attempt, a new trigger still waited one millisecond.
The retry now returns whether it issued a trigger; `false` leaves the retry
eligible on the next yielding completion check. Health-only checks and existing
callbacks returning `nothing` retain their one-millisecond cadence.

A deterministic software fixture completes the outstanding cycle immediately
after the first attempt is suppressed. The new regression fails against
registered version 0.1.4: every replacement trigger waits at least one millisecond.
With the correction, 32 fixture trials have a median replacement delay of
0.471 microseconds (range 0.461–9.898 microseconds) on Julia 1.13.2. This proves
the software ordering defect and its correction. It does not establish how often
the native deployment encounters this ordering or its effect on frame cadence.

Native-cycle ownership, same-sequence checks, yielding, timeout failure and
exception propagation are retained. The focused regression also checks the
issued-trigger cooldown, `nothing`-returning health-check cooldown, a perpetually
suppressed wait's timeout and scheduler progress, and zero warmed allocation
for the suppressed-then-issued wait.

The focused transport, adoption, timing, wait and driver-cycle suite passes
368 assertions for version 0.1.5 on Julia 1.13.2. Aqua's 11 checks also pass.

The minimal correction can acquire the native loop lock and check stream
health on each yielding pass while a driver cycle suppresses retries. The lock
cost and contention require a live comparison before any further change.

Verification logs include
`hil-retry-fail-before.log`, `hil-retry-pass-after.log`,
`hil-retry-reconnaissance.log`, and `hil-retry-after-fixture.log`.
