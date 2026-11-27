# Lightweight deployment entry point. Load this file without importing HIL or AOS.
module StartupImage

using TOML, UUIDs
import PipeWireAORTC: Common, InstrumentConfiguration
import PipeWireAORTC.DeploymentConfiguration as D

const NAME = "PipeWireAOOpticalStartup"
const MANAGED_ENTRY =
    "include(joinpath(dirname(Base.find_package(\"AdaptiveOpticsSimPipeWireHIL\")), \"managed.jl\"))"

directory(package, role) = joinpath(package, "optical-startup", role)

function configuration(package, owner)
    argv = owner["argv"]
    entry = findfirst(==(MANAGED_ENTRY), argv)
    entry === nothing && return nothing
    index = findfirst(==("--config"), argv)
    index !== nothing && index < length(argv) || return nothing
    config = argv[index + 1]
    startswith(config, "@PACKAGE@/") || return nothing
    path = D.relative_asset(package, String(chopprefix(config, "@PACKAGE@/")))
    document = Common.read_json(path)
    get(document, "backend", nothing) == "cpu" || return nothing
    version = get(document, "version", nothing)
    if version in (4, 6)
        return (; path, workload = :prepare_optical_startup)
    elseif version == 3 && isdefined(InstrumentConfiguration, :SIMULATOR_PREPARATION) &&
            isfile(joinpath(dirname(path), InstrumentConfiguration.SIMULATOR_PREPARATION))
        return (; path, workload = :prepare_lockstep_startup)
    end
    return nothing
end

"""
    requested(package, owners)

Return whether an installed owner declares a CPU optical workload.
"""
requested(package, owners) = any(owner -> configuration(package, owner) !== nothing, owners)

"""
    install!(environment, package, owners, prefix)

Build the CPU optical startup images selected by the managed HIL owners.
Use the installed application environment and matching PipeWire prefix.
"""
function install!(environment, package, owners, prefix)
    for owner in owners
        config = configuration(package, owner)
        config === nothing || build!(package, environment, owner, config, prefix)
    end
    return nothing
end

function build!(package, environment, owner, config, prefix)
    path = directory(package, owner["role"])
    manifest = joinpath(environment, "Manifest.toml")
    lockstep = config.workload === :prepare_lockstep_startup
    if !haskey(TOML.parsefile(manifest)["deps"], "PrecompileTools")
        @info "Optical startup image unavailable in the installed environment" owner=owner["role"]
        return nothing
    end
    !(ispath(path) || islink(path)) || throw(ArgumentError("optical startup directory must be new"))
    mkpath(joinpath(path, "src"))
    project = TOML.parsefile(joinpath(environment, "Project.toml"))
    project["name"] = NAME
    project["uuid"] = string(uuid4())
    project["version"] = "0.1.0"
    project["deps"]["PrecompileTools"] = "aea7be01-6a6a-4083-8856-8a6e6704d82a"
    open(io -> TOML.print(io, project; sorted = true), joinpath(path, "Project.toml"), "w")
    cp(manifest, joinpath(path, "Manifest.toml"))
    optics_import = !lockstep && haskey(project["deps"], "Proper") ? "using Proper" : ""
    dependencies = if lockstep
        join((
            "Base.include_dependency(" * repr(file) * "; track_content = true)"
            for file in (joinpath(package, "deployment.conf"),
                         joinpath(environment, "Project.toml"), manifest)
        ), '\n')
    else
        ""
    end
    write(
        joinpath(path, "src", NAME * ".jl"),
        """
        module $NAME
        using PipeWireAORTC, AdaptiveOpticsSimPipeWireHIL
        $optics_import
        $dependencies
        using PrecompileTools: @compile_workload
        const Simulation = AdaptiveOpticsSimPipeWireHIL.Simulation
        @compile_workload Simulation.$(config.workload)($(repr(config.path)))
        end
        """,
    )
    argv = owner["argv"]
    julia = D.selected_julia_executable(first(argv))
    flags = filter(arg -> startswith(arg, "--threads=") ||
                          (lockstep && startswith(arg, "-O")), argv)
    depot = joinpath(path, "depot")
    env = copy(ENV)
    merge!(env, owner["environment"])
    env["JULIA_DEPOT_PATH"] = join([depot; DEPOT_PATH], ':')
    env["JULIA_LOAD_PATH"] = "@:@stdlib"
    env["PIPEWIREAO_PREFIX"] = prefix
    code = "using PipeWireAOOpticalStartup; touch(only(ARGS))"
    build = Cmd([julia, "--startup-file=no", flags..., "--project=" * path,
        "-e", code, joinpath(path, "ready")])
    Common.startup_phase(owner["role"], "build optical startup image") do
        run(setenv(build, env))
    end
    return nothing
end

"""
    configure_owner!(argv, env, package, owner)

Set the managed owner's command and environment to load its installed native
package cache. Leave the command unchanged without a cache.
"""
function configure_owner!(argv, env, package, owner)
    entry = findfirst(==(MANAGED_ENTRY), argv)
    entry === nothing && return nothing
    path = directory(package, owner["role"])
    isfile(joinpath(path, "ready")) || return nothing
    config = configuration(package, owner)
    config === nothing && return nothing
    if config.workload === :prepare_optical_startup
        # Match the dependency import order of the optical workload.
        argv[entry] = "using $NAME; " * argv[entry]
    end
    insert!(argv, 2, "--compiled-modules=existing")
    insert!(argv, 3, "--pkgimages=existing")
    env["PIPEWIREAO_OPTICAL_STARTUP"] = path
    env["JULIA_LOAD_PATH"] = "@:" * path * ":@stdlib"
    env["JULIA_DEPOT_PATH"] = joinpath(path, "depot") * ":" * env["JULIA_DEPOT_PATH"]
    return nothing
end

end
