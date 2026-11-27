validate_node_timing(config, options) = nothing
if isdefined(AlgorithmGraphs, :SecondOrderResponseNodeConfig)
    function validate_node_timing(
        config::AlgorithmGraphs.SecondOrderResponseNodeConfig, options
    )
        round(UInt64, config.sample_period_s * 1e9) == options.period_ns ||
            throw(ArgumentError("response sample period differs from configured cadence"))
        return nothing
    end
end
function validate_node_timing(
    config::AlgorithmGraphs.MultiLayerAtmosphereOPDNodeConfig, options
)
    round(UInt64, Float64(config.atmosphere_step) * 1e9) == options.period_ns ||
        throw(ArgumentError("atmosphere step differs from configured cadence"))
end
function validate_node_timing(
    config::AlgorithmGraphs.CMOSDetectorAcquisitionNodeConfig, options
)
    round(UInt64, Float64(config.exposure_duration_s) * 1e9) == options.exposure_ns ||
        throw(ArgumentError("detector exposure differs from configured acquisition"))
end
function validate_node_timing(
    config::AlgorithmGraphs.EMCCDDetectorAcquisitionNodeConfig, options
)
    round(UInt64, Float64(config.exposure_duration_s) * 1e9) == options.exposure_ns ||
        throw(ArgumentError("detector exposure differs from configured acquisition"))
end
