# Optional real CPU optical preparation; no core, service or device is opened.
using Proper
isdefined(@__MODULE__, :OpticalSimulationTests) || include("simulation_optical.jl")
OpticalSimulationTests.test_preparation()
