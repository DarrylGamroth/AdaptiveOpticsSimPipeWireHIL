# Shared process entry point. Start bootstrap before importing HIL and AOS.
Base.exit_on_sigint(false)

module ManagedHIL

import PipeWireAORTC

const Bootstrap = PipeWireAORTC.OwnerBootstrapRuntime
const Configuration = PipeWireAORTC.AOSOwner

include(joinpath(@__DIR__, "..", "ext", "PipeWireAORTCExt", "arguments.jl"))

function interrupted(error)
    while error isa Union{LoadError,InitError}
        error = error.error
    end
    return error isa InterruptException
end

function main(args)
    invocation = arguments(args)
    Bootstrap.with_owner(invocation) do admitted
        try
            admitted.owner_check()
            if !isempty(get(ENV, "PIPEWIREAO_OPTICAL_STARTUP", ""))
                @info "Loading optical startup image" owner=admitted.bootstrap_node
                Base.require(Main, :PipeWireAOOpticalStartup)
            end
            @info "Loading simulation packages" owner=admitted.bootstrap_node
            load = @timed Base.require(Main, :AdaptiveOpticsSimPipeWireHIL)
            @info "Simulation packages loaded" owner=admitted.bootstrap_node wall_seconds=load.time compile_seconds=load.compile_time
            Base.invokelatest(() -> load.value.Simulation.managed_main(admitted))
        catch error
            interrupted(error) || rethrow()
            runtime = admitted.bootstrap_runtime
            Bootstrap.cancel!(runtime)
            Bootstrap.clean_cancel(runtime) || rethrow()
            return nothing
        end
    end
    return 0
end

end

exit(ManagedHIL.main(ARGS))
