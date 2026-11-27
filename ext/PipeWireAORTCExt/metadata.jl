using AdaptiveOpticsSim.AlgorithmGraphs:
    MultiLayerAtmosphereOPDNodeConfig,
    CMOSDetectorAcquisitionNodeConfig,
    EMCCDDetectorAcquisitionNodeConfig
using AdaptiveOpticsSimPipeWireHIL.ExternalPupil: execution_properties

atmosphere_seed(config) = nothing
atmosphere_seed(config::MultiLayerAtmosphereOPDNodeConfig) = config.rng_seed
detector_seed(config) = nothing
detector_seed(config::CMOSDetectorAcquisitionNodeConfig) = config.rng_seed
detector_seed(config::EMCCDDetectorAcquisitionNodeConfig) = config.rng_seed

function single_seed(definition::AlgorithmGraphDefinition, select)
    seeds = filter(!isnothing, map(node -> select(node.config), definition.nodes))
    return length(seeds) == 1 ? only(seeds) : nothing
end

"""
    simulation_properties(options, science)

Return cold, read-only source-node information. These strings do not declare
writable Props and do not describe live calibration adoption or RNG state.

| Key suffix under `aos.simulation.` | Meaning |
| --- | --- |
| `package` | Instrument declaration identifier |
| `profile` | Explicitly declared startup profile, when present |
| `graph` | Selected declarative startup simulation graph (version 3) |
| `backend`, `device` | Validated compute selection and device ordinal |
| `execution`, `captured-node-count` | Actual prepared AOS execution policy and captured node count |
| `model-period-ns`, `exposure-ns` | Validated model period and actual detector exposure in ns |
| `configuration-sha256` | SHA-256 of the startup owner configuration |
| `atmosphere-rng-seed`, `detector-rng-seed` | Initial seed of exactly one recognized owner |
| `calibration-identity` | Declared startup calibration identity; no live adoption claim |

Seeds come from public typed configs in the actual prepared definition. An
unrecognized config or multiple owners provides no unambiguous seed claim.
This function never steps, resets or queries the scientific or source owner.
"""
function simulation_properties(options, science)
    properties = Dict{String,String}(
        "aos.simulation.backend" => string(options.backend),
        "aos.simulation.device" => string(options.device),
        "aos.simulation.model-period-ns" => string(options.period_ns),
    )
    if get(options, :version, nothing) != 5
        properties["aos.simulation.exposure-ns"] = string(options.exposure_ns)
    end
    properties["aos.simulation.package"] = science.declaration.document["id"]
    profile = get(options.plant.settings, :profile, nothing)
    profile === nothing || (properties["aos.simulation.profile"] = string(profile))
    properties["aos.simulation.graph"] = get(options.plant, :graph, "production")
    configuration_sha256 = get(options, :configuration_sha256, nothing)
    configuration_sha256 === nothing ||
        (properties["aos.simulation.configuration-sha256"] = configuration_sha256)
    calibration_identity = get(options, :calibration_identity, nothing)
    calibration_identity === nothing ||
        (properties["aos.simulation.calibration-identity"] = calibration_identity)
    for (name, seed) in (
        ("atmosphere-rng-seed", single_seed(science.definition, atmosphere_seed)),
        ("detector-rng-seed", single_seed(science.definition, detector_seed)),
    )
        seed === nothing || (properties["aos.simulation."*name] = string(seed))
    end
    hasproperty(science, :graph) &&
        merge!(properties, execution_properties(science.graph; prefix = "aos.simulation."))
    return properties
end
