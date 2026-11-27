const EXECUTOR_PORT = 10000
const DM_PORT = 6100
const WFS_PORT = 6000
const RTC_EXPOSURE_NS = 100_000_000

function heart_command(cli, args...)
    output = read(`timeout 10s $cli -address 127.0.0.1 -port $EXECUTOR_PORT $args`, String)
    occursin("ack=0, completion=0", output) || error("HEART command failed\n$output")
    return nothing
end

function executor_listening()
    close(Sockets.connect(ip"127.0.0.1", EXECUTOR_PORT))
    return true
end
connection_retry(::Base.IOError) = false
connection_retry(err) = throw(err)

function with_heart_executor(
    f, directory, binary, cli; exposure_ns=RTC_EXPOSURE_NS, history=nothing
)
    source = joinpath(@__DIR__, "data", "heart")
    for name in ("actuators.csv", "subapertures.csv", "mask.csv")
        cp(joinpath(source, name), joinpath(directory, name))
    end
    config = read(joinpath(source, "rtc.yaml"), String)
    if exposure_ns != RTC_EXPOSURE_NS
        rate = 1e9 / exposure_ns
        exposure_us = exposure_ns / 1000
        config = replace(config, "FPS: 10.0, EXPOSURE_TIME: 100000.0" =>
            "FPS: $rate, EXPOSURE_TIME: $exposure_us")
    end
    if history !== nothing
        config = replace(config, "- GEN:" => """
        - CB:
            CAPACITY: [
                { tagName: "HOP0.wfs.pixelsRaw", capacity: $history },
                { tagName: "WCC.dm0.dmOutput", capacity: $history }
            ]
        - GEN:"""; count=1)
    end
    write(joinpath(directory, "rtc.yaml"), config)
    matrix = joinpath(directory, "reconstructor.fits")
    FITS(matrix, "w") do file
        # One Shack-Hartmann subaperture supplies x and y. The two actuators
        # act on the two pupil tilt axes, with a low gain for the finite run.
        write(file, Float32[0.2 0; 0 0.1])
    end
    command = Cmd(`$binary -config rtc.yaml -nohuge -ip 127.0.0.1 -port $EXECUTOR_PORT -gmsShm -rt 0`;
        dir=directory)
    if !isfile("/proc/sys/net/core/busy_read")
        compatibility = joinpath(directory, "heart-busy-poll.so")
        shim = joinpath(@__DIR__, "heart_busy_poll.c")
        run(`cc -Wall -Wextra -Werror -shared -fPIC -o $compatibility $shim -ldl`)
        command = addenv(command, "LD_PRELOAD" => compatibility)
    end
    log_path = joinpath(directory, "rtc.log")
    open(log_path, "w") do log
        process = run(pipeline(command; stdout=log, stderr=log); wait=false)
        try
            _wait_for_hil_condition() do
                process_exited(process) && error("HEART executor exited during startup")
                try
                    return executor_listening()
                catch err
                    return connection_retry(err)
                end
            end
            heart_command(cli, "CONFIG")
            heart_command(cli, "CTRL_MAT_HOR_LOAD", "-ctrlPath", "HO", "-index", "0",
                "-update", "1", "-filename", matrix)
            heart_command(cli, "READY")
            heart_command(cli, "LOOP", "-state", "1", "-ctrlPath", "ALL")
            result = f()
            heart_command(cli, "SHUTDOWN")
            _wait_for_hil_condition(() -> process_exited(process))
            @test process.exitcode == 0 && process.termsignal == 0
            return result
        catch
            flush(log)
            lines = readlines(log_path)
            println(stderr, join(last(lines, min(80, length(lines))), '\n'))
            rethrow()
        finally
            _stop_hil_process(process)
        end
    end
end

function dump_buffer(directory, cli, buffer)
    path = joinpath(directory, buffer * ".fits")
    heart_command(cli, "CB_DUMP", "-buffer", buffer, "-filename", path)
    return FITS(path) do file
        read(file[1])
    end
end
