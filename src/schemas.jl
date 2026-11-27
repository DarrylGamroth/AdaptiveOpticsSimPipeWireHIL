# This file is generated from schemas/catalog.toml in JuliaFilterGraph.
# Run `julia scripts/generate_schemas.jl` with the consumer root options to regenerate.

module Schemas

const APPLIED_DM_COMMAND_OPD_UM_WIRE_TYPE = "F32_LE"

"""
F32 commanded physical-DM values in concatenated actuator order, in µm wavefront optical
path difference.
"""
const DEMANDED_PDM_COMMAND_V1 = "pipewireao/dm-command-opd-um-f32/1"

"""
F32 actuator commands in metres of wavefront optical path difference, in physical-actuator
order.
"""
const ACTUATOR_COMMAND_OPD_M_V1 = "pipewireao/actuator-command-opd-m-f32/1"

"""
Two F32 coefficients in metres of wavefront optical path difference, ordered (X ramp, Y
ramp), for the dimensionless normalized-pupil-xy basis. On the supported pupil, the additive
OPD is coefficient[1]*X + coefficient[2]*Y. A positive coefficient adds OPD increasing with
its corresponding pupil coordinate. These are normalized-pupil ramp amplitudes, not angular
pointing values or mirror displacements.
"""
const TIP_TILT_OPD_COEFFICIENTS_M_V1 = "pipewireao/tip-tilt-opd-coefficients-m-f32/1"

"""
F32 applied physical-DM command in µm wavefront optical path difference, in concatenated
actuator order.
"""
const APPLIED_DM_COMMAND_OPD_UM_V1 = "pipewireao/applied-dm-command-opd-um-f32/1"

const _SCHEMA_ELEMENT_TYPES = Dict{String,DataType}(
    DEMANDED_PDM_COMMAND_V1 => Float32,
    ACTUATOR_COMMAND_OPD_M_V1 => Float32,
    TIP_TILT_OPD_COEFFICIENTS_M_V1 => Float32,
    APPLIED_DM_COMMAND_OPD_UM_V1 => Float32,
)

"""
    schema_element_type(schema::AbstractString)

Return the declared Julia element type for a catalogued scientific schema.
Return `nothing` for an external schema; its owner defines the element type.
Identifiers are matched exactly, without interpreting their spelling.
"""
schema_element_type(schema::AbstractString) = get(_SCHEMA_ELEMENT_TYPES, schema, nothing)

const _SCHEMA_CHANNELS = Dict{String,Tuple}(
)

const _SCHEMA_CHANNEL_SHAPES = Dict{String,Tuple}(
)

"Return immutable scalar channel declarations, or () when none are declared."
schema_channels(schema::AbstractString) = get(_SCHEMA_CHANNELS, schema, ())

"Return the exact logical shape for declared channels, or nothing."
schema_channel_shape(schema::AbstractString) = get(_SCHEMA_CHANNEL_SHAPES, schema, nothing)

end
