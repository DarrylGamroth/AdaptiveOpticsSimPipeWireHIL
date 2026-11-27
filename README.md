# AdaptiveOpticsSimPipeWireHIL.jl

[![CI](https://github.com/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL/branch/main/graph/badge.svg)](https://app.codecov.io/gh/DarrylGamroth/AdaptiveOpticsSimPipeWireHIL)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Run an AdaptiveOpticsSim instrument simulation against a real-time controller
over PipeWireAO. The instrument supplies the optical graph, wavefront-sensor
(WFS) frames, and correction-command schemas. AdaptiveOpticsSim owns the
numerical model and model time. The controller owns session admission and
PipeWire links.

## Install in an instrument simulator

Use Julia 1.13 or later and a C compiler. Activate the instrument simulator's
Julia project, then add the package from the registry.

```julia
using Pkg
Pkg.Registry.add(url="https://github.com/DarrylGamroth/PackageRegistry.git")
Pkg.add(name="AdaptiveOpticsSimPipeWireHIL", version=v"0.5.12")
Pkg.build("AdaptiveOpticsSimPipeWireHIL")
```

Keep the resolved `Manifest.toml` with the simulator environment so the
launcher uses the tested commit.

## Run a simulation

- **Lockstep.** The simulator publishes one WFS frame, collects all matching
  correction commands, and applies them before the next frame.
- **Free-running.** The simulator steps on its own clock and adopts the newest
  complete correction at a model boundary.
- **Optical sources.** External-pupil and clocked optical execution publish
  completed camera exposures independently. A pupil-source process publishes
  the simulated pupil for an external optical model.

For an RTC-managed simulation, use the launcher from the instrument's Julia
environment with `PipeWireAORTC` and `ThreadPinning` installed. The instrument's
sealed configuration supplies the graph, wire contracts, and process settings.
The RTC invokes the launcher with the configuration path, PipeWire remote,
and bootstrap and control node names.

```julia
include(joinpath(dirname(Base.find_package("AdaptiveOpticsSimPipeWireHIL")), "managed.jl"))
```

For a standalone simulation, load and prepare an exported simulation bundle
with `Simulation.load` and `Simulation.prepare`. Pass the prepared simulation
to `Simulation.run!` and supply a callback to link the PipeWire endpoints and
control execution.

## Tests

From the package checkout on Linux x86-64, add the registry above and run
`Pkg.instantiate(); Pkg.build("AdaptiveOpticsSimPipeWireHIL"); Pkg.test(coverage=true)`
with Julia started using `--project --threads=1,1`. The suite runs Aqua and
exercises synthetic instrument graphs on isolated PipeWire cores.
The [CI workflow](.github/workflows/ci.yml) contains the full setup.
