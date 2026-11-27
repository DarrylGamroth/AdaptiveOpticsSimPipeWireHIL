# Shared isolated PipeWire daemon configuration and bounded waits for tests.
function _wait_for_hil_condition(predicate; timeout=10.0)
    status = timedwait(predicate, timeout; pollint=0.01)
    status == :ok || error("private PipeWireAO fixture timed out")
    return nothing
end

function _stop_hil_process(process)
    process === nothing && return nothing
    Base.process_exited(process) || kill(process)
    wait(process)
    return nothing
end

function _private_core_configuration(core_name; spa_libraries="", modules="")
    return """
    context.properties = {
        core.daemon = true
        core.name = $core_name
        support.dbus = false
        library.use-fallback = false
    }
    context.spa-libs = { support.* = support/libspa-support $spa_libraries }
    context.modules = [
        $modules
        { name = libpipewire-module-scheduler-v1 }
        { name = libpipewire-module-protocol-native }
        { name = libpipewire-module-client-node }
        { name = libpipewire-module-link-factory }
        { name = libpipewire-module-metadata }
        { name = libpipewire-module-access }
    ]
    """
end

const _HIL_CLIENT_CONFIGURATION = """
context.properties = { support.dbus = false }
context.spa-libs = { support.* = support/libspa-support }
context.modules = [
    { name = libpipewire-module-protocol-native }
    { name = libpipewire-module-client-node }
    { name = libpipewire-module-metadata }
]
"""


function _with_private_core(
    f; configuration_for=_private_core_configuration, spa_plugin_dir=nothing
)
    prefix = get(ENV, "PIPEWIREAO_HIL_TEST_PREFIX", String(PipeWireAO_jll.artifact_dir))
    failure_log_path = get(ENV, "PIPEWIREAO_HIL_TEST_FAILURE_LOG", "")
    if !isempty(failure_log_path)
        isabspath(failure_log_path) || error("private core failure log path must be absolute")
        !ispath(failure_log_path) || error("private core failure log path already exists: $failure_log_path")
    end
    native_libdir = isdir(joinpath(prefix, "lib", "x86_64-linux-gnu")) ?
        joinpath(prefix, "lib", "x86_64-linux-gnu") : joinpath(prefix, "lib")
    daemon_path = joinpath(prefix, "bin", "pipewire-ao")
    @test isfile(daemon_path)
    mktempdir(prefix="aos-array-private-core-") do temporary
        runtime, configuration = joinpath(temporary, "runtime"), joinpath(temporary, "configuration")
        mkpath(runtime)
        chmod(runtime, 0o700)
        mkpath(configuration)
        remote = "aos-array-test-$(getpid())-$(time_ns())"
        write(joinpath(configuration, "private-core.conf"), configuration_for(remote))
        write(joinpath(configuration, "client.conf"), _HIL_CLIENT_CONFIGURATION)
        environment = Dict(
            "XDG_RUNTIME_DIR" => runtime, "PIPEWIREAO_RUNTIME_DIR" => runtime,
            "PIPEWIREAO_CONFIG_DIR" => configuration,
            "PIPEWIREAO_MODULE_DIR" => joinpath(native_libdir, "pipewire-ao-0.3"),
            "PIPEWIREAO_SPA_PLUGIN_DIR" => joinpath(native_libdir, "spa-ao-0.2"),
            "PIPEWIREAO_DEBUG" => get(ENV, "PIPEWIREAO_HIL_TEST_DEBUG", "0"),
            "LD_LIBRARY_PATH" => native_libdir * ":" * get(ENV, "LD_LIBRARY_PATH", ""))
        if spa_plugin_dir !== nothing
            environment["PIPEWIREAO_SPA_PLUGIN_DIR"] *= ":" * spa_plugin_dir
        end
        log = open(joinpath(temporary, "core.log"), "w+")
        daemon = nothing
        try
            daemon = run(pipeline(addenv(`$daemon_path -c private-core.conf`, environment);
                stdout=log, stderr=log); wait=false)
            _wait_for_hil_condition(() -> ispath(joinpath(runtime, remote)))
            return withenv(environment...) do
                f(remote)
            end
        catch
            flush(log)
            seekstart(log)
            core_log = read(log, String)
            if !isempty(failure_log_path)
                !ispath(failure_log_path) || error("private core failure log path already exists: $failure_log_path")
                write(failure_log_path, core_log)
            end
            isempty(core_log) || @error "Private core log" core_log
            rethrow()
        finally
            _stop_hil_process(daemon)
            close(log)
        end
    end
end

_retain_test_metadata!(storage::MetadataSnapshot,buffer)=(capture_metadata!(storage,buffer);nothing)
_publish_test_metadata!(buffer,storage::MetadataSnapshot)=(copy_metadata!(buffer,storage);nothing)
