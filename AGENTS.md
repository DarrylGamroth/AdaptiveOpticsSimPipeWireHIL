# AdaptiveOpticsSimPipeWireHIL.jl agent guidance

- This package owns AdaptiveOpticsSim simulation execution over public PipeWireAO.jl
  streams, including external-pupil and clocked optical runners. AOS retains
  numerical models and model-time semantics; the RTC retains session admission,
  native controls, device interfaces and deployment supervision.
- AdaptiveOpticsSim owns optical models, detector products, command meaning,
  model time, and frame/command causality.
- PipeWireAO.jl owns PipeWire objects, SPA formats, mapped buffers, metadata,
  and native lifetime wrappers.
- `pipewireao-rtc` owns RTC session topology, FGN graphs, admission, lifecycle
  and system-level closed-loop evidence. Standalone execution takes prepared
  science. Keep RTC protocol integration in the optional PipeWireAORTC extension.
- Do not implement RTC algorithms, a graph parser, a scheduler, a plugin
  registry, physical-device authority, or GUI behavior here.
- Keep simulation work outside PipeWire callbacks. A callback may only validate
  and copy one already-prepared complete buffer, update bounded state, and
  return or queue the borrowed buffer.
- Keep exactly one frame and one coherent command set in flight in the lockstep
  profile. Each named port has one bounded receipt state; only the exchange
  owner may adopt after every same-sequence port arrives.
- Prepared external-pupil and clocked optical execution supports independent
  camera exposure windows and one retained sample per array stream. Keep command
  hold, generation/reset and completed-exposure publication in its single writer;
  do not impose lockstep exchange on this distinct execution mode.
- Preserve canonical terminology from the AdaptiveOpticsSim glossary and the
  PipeWireAO ndarray contract.
- Close streams, core connections, contexts, and loops deterministically.
- Do not connect tests to the user's normal PipeWire session. Live tests must
  use an isolated runtime directory and private core name.
