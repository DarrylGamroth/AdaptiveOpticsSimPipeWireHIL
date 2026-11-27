# Package review


| Finding | Severity | Evidence and impact | Disposition |
| --- | --- | --- | --- |
| HIL-001 | High | Julia 1.12 fails to resolve the package. The existing PipeWireAO bound starts at 0.9.3 and all matching registry versions require Julia 1.13. | Declared Julia 1.13 in `Project.toml` and selected stable Julia in CI. |
| HIL-002 | Medium | The default exposure source in `src/acquisition.jl` called `hil_boundary_status` even for a buffered plant that implements just the documented stepping interface. The first step succeeded and exposure staging then threw a `MethodError`. | Resolve status availability during preparation and dispatch on the concrete source type. The regression test covers both the buffered plant and rejection of a mismatching independent status sequence. |
| HIL-003 | Medium | The existing default suite passed on Julia 1.13.4 with 2,614 of 4,493 Julia lines covered. None of the extension lines were exercised. | Added the standalone optical preparation, managed CPU, borrowed camera buffer, and free-running transport suites to the default runner. |
| HIL-004 | Low | Host type to wire element mapping was repeated in exchange and external-pupil preparation. The two paths could diverge when adding an encoding. | Both paths now use methods keyed by the host element type. |
| HIL-005 | Low | Julia help lacks direct docstrings for the two model-time capture functions, `reset_pipewire_calibration!`, three `SimRunner` functions, and `Simulation.AbstractConfiguration`. The calibration reset docstring is attached to an internal helper. | Open documentation follow-up. |

The existing prepared types retain concrete boundary, storage, and callback
types. Named command sets dispatch on `NamedTuple`, and staged and borrowed
camera publication dispatch on the selected buffer representation. These
differences justify separate methods. Transport ownership, command adoption,
and optical stepping still follow the single-writer contracts.

Aqua now checks both the package and the RTC extension. The extension's piracy
check treats three parent-owned simulation hooks as belonging to the extension
that implements them. The check remains active for third-party methods.

## Verification

The baseline used the registered PipeWireAO 0.9.7, PipeWireAO_jll 4.0.10,
and AdaptiveOpticsSim 0.3.31. Coverage was calculated with CoverageTools
over Julia code in `src`, `ext`, and `deps`. The original suite covered
2,614 of 3,037 lines under `src`. Core tests use the JLL daemon and isolated
remote names. These results establish software behavior in the test fixtures.

The CUDA selector passed its 13 device-execution checks with Julia 1.13.4,
CUDA 6.5.1, and an RTX 3080. The hosted CPU coverage report does not incorporate
that local run. The workflow passed `actionlint` and all referenced action
tags resolved. GitHub Actions and the Codecov OIDC upload require a hosted run
on the repository.

The full CI-equivalent `Pkg.test(coverage=true)` suite passed on Julia 1.13.4,
including its managed-worker subprocess. CoverageTools 1.4.1 reported the
following Julia line coverage.

| Scope | Baseline | Initial review | Managed coverage follow-up |
| --- | ---: | ---: | ---: |
| Package source `src/` | 2,614/3,037 (86.07%) | 2,906/3,052 (95.22%) | 2,911/3,052 (95.38%) |
| Julia extensions `ext/` | 0/1,450 (0%) | 511/1,438 (35.54%) | 1,247/1,443 (86.42%) |
| All reported Julia code | 2,614/4,493 (58.18%) | 3,417/4,496 (76.00%) | 4,158/4,501 (92.38%) |

The managed tests exercise sealed lockstep startup, correction adoption,
resume after reset, and the saved phase timings. They also check correction
adoption while the free-running camera continues stepping, clocked-optical
command observation across reset, and private-core delivery of focal image,
wavefront, RMS, and pupil products. The pupil publication and optical
diagnostics tests also passed with CPU affinity restricted to CPUs 0 and 1.
The package-wide result comes from the full run on the local Linux x86_64 host.
