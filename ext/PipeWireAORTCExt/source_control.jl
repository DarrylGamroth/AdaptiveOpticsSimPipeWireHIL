mutable struct Cursor
    running::Bool
    completed::Bool
    sequence::UInt64
    last_request_id::Int64
    deadline_ns::Int64
    generation::Int64
    host_ns::Union{Nothing,Int64}
end

Cursor() = Cursor(false, false, 0, 0, 0, 1, nothing)

function report!(options, mailbox, state)
    write_report(options, state, state.generation, mailbox.instance)
    SourceControl.report_ready!(mailbox, state.generation, state.sequence)
    return nothing
end

function source_mailbox(options, state)
    mailbox = SourceControl.Mailbox(rand(Int64(1):typemax(Int64)))
    report!(options, mailbox, state)
    return mailbox
end

# The PipeWire callback stages one request. The source task applies it between
# scientific duties and publishes the completed cursor under the same loop lock.
function consume!(
    access::F, options, mailbox, state, period, reset::R, now_ns;
    after::A = () -> nothing,
) where {F,R,A}
    mailbox.ready[] || return nothing
    kind, token, running = access(_ -> SourceControl.pending(mailbox))
    if kind != SourceControl.INITIAL
        result = SourceControl.apply!(state, kind, token, running, now_ns, period, reset)
        if result == 0
            after()
            kind == SourceControl.RESET && report!(options, mailbox, state)
        end
        access() do endpoint
            SourceControl.publish!(mailbox, endpoint, kind, token, result, state, state.generation)
        end
    end
    access() do endpoint
        SourceControl.publish_rejection!(mailbox, endpoint, state, state.generation)
    end
    return nothing
end

struct FilterAccess{L,F}
    loop::L
    filter::F
end

function (access::FilterAccess)(operation::F) where {F}
    return with_thread_loop_lock(access.loop) do _
        operation(access.filter)
    end
end

function source_filter(owner, node, mailbox, properties)
    return with_thread_loop_lock(owner.loop) do _
        filter = Filter(
            owner.core,
            node;
            properties = merge(
                properties,
                SourceControl.source_properties(mailbox),
                Dict("node.name" => node, "media.class" => "Control"),
            ),
            on_param_changed = SourceControl.ParameterChanged(mailbox),
        )
        try
            connect!(
                filter; flags = FILTER_ASYNC | FILTER_INACTIVE, params = mailbox.parameters.params
            )
        catch
            close(filter)
            rethrow()
        end
        return FilterAccess(owner.loop, filter)
    end
end

registered(node) = node_id(node) ∉ (UInt32(0), typemax(UInt32))
registered(stream::Union{Stream,AdaptiveOpticsSimPipeWireHIL._CommandReceiver}) =
    node_id(stream) ∉ (UInt32(0), typemax(UInt32)) && stream_state(stream) in (
        PipeWireAO.LibPipeWire.PW_STREAM_STATE_PAUSED,
        PipeWireAO.LibPipeWire.PW_STREAM_STATE_STREAMING,
    )

function wait_registered!(ready, runtime, ticket)
    while true
        Bootstrap.check_connect!(runtime, ticket)
        ready() && return nothing
        sleep(0.001)
    end
end

function pin_transport!(transport, options)
    options.pipewire_cpu === nothing ||
        Placement.pin_loop!(transport.loop, options.pipewire_cpu; check = options.owner_check)
    return nothing
end
