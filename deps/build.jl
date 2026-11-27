using PipeWireAO_jll

prefix = PipeWireAO_jll.artifact_dir
library = joinpath(@__DIR__, "libhil_command_receiver.so")
mktempdir(@__DIR__) do directory
    output = joinpath(directory, basename(library))
    compiler = Cmd(Base.shell_split(get(ENV, "CC", "cc")))
    libdir = dirname(PipeWireAO_jll.libpipewire_ao_path)
    includes = joinpath(prefix, "include")
    run(`$compiler -std=gnu11 -O3 -fPIC -shared
        -I$(joinpath(includes, "pipewire-ao-0.3")) -I$(joinpath(includes, "spa-ao-0.2"))
        $(joinpath(@__DIR__, "command_receiver.c")) -L$libdir -lpipewire-ao-0.3 -o $output`)
    mv(output, library; force = true)
end
