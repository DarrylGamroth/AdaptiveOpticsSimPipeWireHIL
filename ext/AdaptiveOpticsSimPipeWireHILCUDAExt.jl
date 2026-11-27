module AdaptiveOpticsSimPipeWireHILCUDAExt

using AdaptiveOpticsSim: Backends
using AdaptiveOpticsSimPipeWireHIL: ExternalPupil
import CUDA

const CUDACore = CUDA.CUDACore

function ExternalPupil._copy_exposure_frame!(
    context::Backends._AbstractPreparedDeviceExecutionContext,
    destination::Array{T,N},
    image::CUDA.CuArray{T,N,CUDA.DeviceMemory},
) where {T<:Union{Float32,Float64},N}
    # The optical worker owns the device context and the response has
    # completed. Enqueue against its retained stream without a closure.
    if !(hasfield(typeof(context), :context) && context.context isa CUDA.CuContext) ||
            CUDA.context() != context.context
        return invoke(ExternalPupil._copy_exposure_frame!,
            Tuple{Any,Array{T,N},AbstractArray{T,N}}, context, destination, image)
    end
    GC.@preserve destination image begin
        try
            Backends._copy_prepared_device_buffer!(context, destination, image)
        finally
            Backends._synchronize_prepared_device_execution_context_blocking!(context)
        end
    end
    return destination
end

function reclamation_timer()
    # The table is shared across Julia tasks. Release its lock before
    # inspecting timer wait queues to avoid nesting the two locks.
    handles = lock(Base.preserve_handle_lock) do
        collect(keys(Base.uvhandles))
    end
    found = nothing
    for handle in handles
        handle isa Timer && handle.interval == 1.0 || continue
        matches = lock(handle.cond) do
            any(handle.cond.waitq) do task
                code = getfield(task, :code)
                hasfield(typeof(code), :cb) || return false
                callback = getfield(code, :cb)
                parentmodule(typeof(callback)) === CUDACore || return false
                method = which(callback, (Timer,))
                return endswith(string(method.file), "/resources/lifecycle.jl")
            end
        end
        matches || continue
        found === nothing || error("multiple CUDA reclamation timers found")
        found = handle
    end
    return found
end

function ExternalPupil._with_reclamation_timer(
    f, ::Backends.AcceleratorComputeDevice{Backends.CUDABackend},
)
    if VERSION != v"1.13.3" || pkgversion(CUDA) != v"6.5.1" ||
        pkgversion(CUDACore) != v"6.5.1" || !CUDACore.retired_drainer[]
        return f()
    end

    # The callback may be running when the optical worker enters. Allow it
    # to return to its wait queue before taking the extra reference.
    deadline = time_ns() + 2_000_000_000
    timer = reclamation_timer()
    while timer === nothing && time_ns() < deadline
        sleep(0.001)
        timer = reclamation_timer()
    end
    timer === nothing && error("could not locate the active CUDA reclamation timer")

    # Keep the existing table entry across ticks. Julia still manages the
    # timer's own reference and CUDA still runs the original callback.
    Base.preserve_handle(timer)
    try
        return f()
    finally
        Base.unpreserve_handle(timer)
    end
end

end
