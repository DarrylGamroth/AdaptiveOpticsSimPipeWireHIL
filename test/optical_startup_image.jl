using Test

include(joinpath(@__DIR__, "..", "ext", "PipeWireAORTCExt", "startup_image.jl"))

@testset "load the optical image before the managed entry" begin
    optical = StartupImage
    owner = Dict(
        "role" => "simulator",
        "argv" => ["julia", "-e", optical.MANAGED_ENTRY, "--", "--config", "@PACKAGE@/simulation.conf"],
    )
    command = ["julia", "-e", optical.MANAGED_ENTRY, "--", "--config", "simulation.conf"]
    mktempdir() do package
        argv = copy(command)
        env = Dict("JULIA_DEPOT_PATH" => "/depot")
        optical.configure_owner!(argv, env, package, owner)
        @test argv == command
        @test !haskey(env, "PIPEWIREAO_OPTICAL_STARTUP")

        config = joinpath(package, "simulation.conf")
        write(config, """{"backend":"cpu","version":4}""")
        path = mkpath(optical.directory(package, owner["role"]))
        touch(joinpath(path, "ready"))
        optical.configure_owner!(argv, env, package, owner)
        @test argv[2:3] == ["--compiled-modules=existing", "--pkgimages=existing"]
        @test argv[5] == "using PipeWireAOOpticalStartup; " * optical.MANAGED_ENTRY
        @test argv[6:end] == command[4:end]
        @test env["PIPEWIREAO_OPTICAL_STARTUP"] == path

        receipt = joinpath(package, optical.InstrumentConfiguration.SIMULATOR_PREPARATION)
        mkpath(dirname(receipt))
        touch(receipt)
        write(config, """{"backend":"cpu","version":3}""")
        argv = copy(command)
        env = Dict("JULIA_DEPOT_PATH" => "/depot")
        optical.configure_owner!(argv, env, package, owner)
        @test argv[2:3] == ["--compiled-modules=existing", "--pkgimages=existing"]
        @test argv[4:end] == command[2:end]
        @test env["PIPEWIREAO_OPTICAL_STARTUP"] == path
    end
end

@testset "select the managed CPU optical workloads" begin
    mktempdir() do package
        config = joinpath(package, "simulation.conf")
        argv = ["julia", "-e", StartupImage.MANAGED_ENTRY, "--config", "@PACKAGE@/simulation.conf"]
        owner = Dict("argv" => argv)
        for (backend, version, selected) in (
            ("cpu", 3, false), ("cpu", 4, true), ("cpu", 6, true), ("cuda", 6, false),
        )
            write(config, """{"backend":"$backend","version":$version}""")
            @test StartupImage.requested(package, [owner]) === selected
        end
        receipt = joinpath(package, StartupImage.InstrumentConfiguration.SIMULATOR_PREPARATION)
        mkpath(dirname(receipt))
        touch(receipt)
        write(config, """{"backend":"cpu","version":3}""")
        @test StartupImage.requested(package, [owner])
        write(config, """{"backend":"cuda","version":3}""")
        @test !StartupImage.requested(package, [owner])
        @test !StartupImage.requested(package, [Dict("argv" => ["other"])])
    end
end

@testset "startup image install respects the deployment environment and existing cache" begin
    mktempdir() do package
        environment = mkpath(joinpath(package, "runtime"))
        manifest = joinpath(environment, "Manifest.toml")
        config = joinpath(package, "simulation.conf")
        write(config, """{"backend":"cpu","version":4}""")
        owner = Dict("role" => "simulator",
            "argv" => ["julia", "-e", StartupImage.MANAGED_ENTRY, "--",
                "--config", "@PACKAGE@/simulation.conf"])
        path = StartupImage.directory(package, owner["role"])

        write(manifest, "[deps]\n")
        @test StartupImage.install!(environment, package, [owner], package) === nothing
        @test !ispath(path)

        write(manifest, "[deps.PrecompileTools]\n")
        mkpath(path)
        sentinel = joinpath(path, "ready")
        write(sentinel, "keep installed cache")
        @test_throws ArgumentError StartupImage.install!(environment, package, [owner], package)
        @test read(sentinel, String) == "keep installed cache"
    end
end
