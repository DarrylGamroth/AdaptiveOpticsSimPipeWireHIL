/* A bounded latest-command slot. PipeWire's data loop never enters Julia. */
#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pipewire/pipewire.h>
#include <spa/buffer/meta.h>

struct command {
    int64_t pts;
    uint64_t sequence;
    uint64_t duration;
    uint64_t generation;
    uint64_t acquisition_sequence;
    uint64_t stamp;
    uint64_t stamp_duration;
    uint64_t lost;
    uint64_t received;
    uint32_t clock;
    uint8_t domain[16];
};

struct receiver {
    struct pw_stream *stream;
    struct spa_hook listener;
    struct command previous;
    struct command command;
    _Atomic uint64_t dropped;
    _Atomic uint64_t gaps;
    _Atomic uint64_t rejected;
    _Atomic bool pending;
    _Atomic bool busy;
    _Atomic bool failed;
    bool exposure_duration_required;
    uint32_t bytes;
    uint32_t n_requirements;
    struct spa_meta_requirement requirements[5];
    uint8_t values[];
};

static bool admit(const struct command *previous, const struct command *next)
{
    if (!next->sequence || (next->pts < 0 && next->pts != INT64_MIN))
        return false;
    if (!next->generation)
        return next->sequence > previous->sequence;
    return next->acquisition_sequence > 0 &&
        (!previous->generation ||
         (!memcmp(next->domain, previous->domain, sizeof(next->domain)) &&
          (next->generation > previous->generation ||
           (next->generation == previous->generation &&
            next->acquisition_sequence > previous->acquisition_sequence))));
}

static bool read_command(const struct receiver *sink, struct spa_buffer *buffer,
        struct command *next)
{
    if (buffer->n_datas != 1 ||
        !spa_meta_records_validate(buffer->metas, buffer->n_metas,
            sink->requirements, sink->n_requirements))
        return false;

    const struct spa_data *payload = &buffer->datas[0];
    const struct spa_chunk *chunk = payload->chunk;
    const struct spa_meta *hm = spa_buffer_find_meta(buffer, SPA_META_Header);
    if (hm == NULL || payload->data == NULL || chunk == NULL || chunk->offset != 0 ||
        chunk->size != sink->bytes || chunk->size > payload->maxsize || chunk->flags != 0 ||
        (chunk->stride != 0 && chunk->stride != (int32_t) sizeof(float) &&
         (uint32_t) chunk->stride != sink->bytes))
        return false;

    const struct spa_meta_header *header = hm->data;
    if (header->flags & (SPA_META_HEADER_FLAG_CORRUPTED | SPA_META_HEADER_FLAG_GAP |
                         SPA_META_HEADER_FLAG_DISCONT))
        return false;
    next->sequence = header->seq;
    next->pts = header->pts;
    const struct spa_meta *im = spa_buffer_find_meta(buffer, SPA_META_AcquisitionIdentity);
    if (im != NULL) {
        const struct spa_meta_acquisition_identity *identity = im->data;
        if (identity->flags) {
            if (!identity->generation)
                return false;
            memcpy(next->domain, identity->domain, sizeof(next->domain));
            next->generation = identity->generation;
            next->acquisition_sequence = identity->sequence;
        }
    }
    const struct spa_meta *em = spa_buffer_find_meta(buffer, SPA_META_ExposureTiming);
    if (em != NULL) {
        const struct spa_meta_exposure_timing *timing = em->data;
        next->duration = timing->duration_nsec;
    }
    if (sink->exposure_duration_required && !next->duration)
        return false;
    const struct spa_meta *tm = spa_buffer_find_meta(buffer, SPA_META_ReferenceTimestamp);
    if (tm != NULL) {
        const struct spa_meta_reference_timestamp *stamp = tm->data;
        next->clock = stamp->clock;
        next->stamp = stamp->timestamp_nsec;
        next->stamp_duration = stamp->duration_nsec;
    }
    return admit(&sink->previous, next);
}

static void receive(void *data)
{
    struct receiver *sink = data;
    struct pw_buffer *loan = pw_stream_dequeue_buffer(sink->stream);
    if (loan == NULL)
        return;

    struct command next = {0};
    if (!read_command(sink, loan->buffer, &next)) {
        atomic_fetch_add_explicit(&sink->rejected, 1, memory_order_relaxed);
        goto done;
    }
    const struct command *previous = &sink->previous;
    uint64_t gap = 0;
    if (next.generation && next.generation == previous->generation &&
        previous->acquisition_sequence)
        gap = next.acquisition_sequence - previous->acquisition_sequence - 1;
    else if (!next.generation && previous->sequence)
        gap = next.sequence - previous->sequence - 1;
    sink->previous = next;
    if (gap)
        atomic_fetch_add_explicit(&sink->gaps, gap, memory_order_relaxed);
    if (atomic_exchange_explicit(&sink->busy, true, memory_order_acquire)) {
        atomic_fetch_add_explicit(&sink->dropped, 1, memory_order_relaxed);
        goto done;
    }
    if (atomic_load_explicit(&sink->pending, memory_order_relaxed))
        atomic_fetch_add_explicit(&sink->dropped, 1, memory_order_relaxed);
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    next.received = (uint64_t) now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
    memcpy(sink->values, loan->buffer->datas[0].data, sink->bytes);
    sink->command = next;
    atomic_store_explicit(&sink->pending, true, memory_order_release);
    atomic_store_explicit(&sink->busy, false, memory_order_release);

done:
    if (pw_stream_queue_buffer(sink->stream, loan) < 0)
        atomic_store_explicit(&sink->failed, true, memory_order_release);
}

static const struct pw_stream_events events = {
    PW_VERSION_STREAM_EVENTS,
    .process = receive,
};

struct receiver *hil_command_receiver_new(struct pw_core *core, const char *name,
        struct pw_properties *properties, const struct spa_pod **params,
        uint32_t n_params, uint32_t bytes, bool active,
        const struct spa_meta_requirement *requirements, uint32_t n_requirements)
{
    struct receiver *sink = calloc(1, sizeof(*sink) + bytes);
    if (sink == NULL) {
        pw_properties_free(properties);
        return NULL;
    }
    atomic_init(&sink->dropped, 0);
    atomic_init(&sink->gaps, 0);
    atomic_init(&sink->rejected, 0);
    atomic_init(&sink->pending, false);
    atomic_init(&sink->busy, false);
    atomic_init(&sink->failed, false);
    sink->bytes = bytes;
    if (n_requirements > SPA_N_ELEMENTS(sink->requirements)) {
        pw_properties_free(properties);
        free(sink);
        errno = EINVAL;
        return NULL;
    }
    sink->n_requirements = n_requirements;
    if (n_requirements)
        memcpy(sink->requirements, requirements, n_requirements * sizeof(*requirements));
    for (uint32_t i = 0; i < n_requirements; i++) {
        if (requirements[i].type == SPA_META_ExposureTiming &&
            (requirements[i].flags & SPA_META_REQUIREMENT_REQUIRED))
            sink->exposure_duration_required = true;
    }
    sink->stream = pw_stream_new(core, name, properties);
    if (sink->stream == NULL) {
        free(sink);
        return NULL;
    }
    pw_stream_add_listener(sink->stream, &sink->listener, &events, sink);
    enum pw_stream_flags flags = PW_STREAM_FLAG_MAP_BUFFERS | PW_STREAM_FLAG_NO_CONVERT |
        PW_STREAM_FLAG_DONT_RECONNECT | PW_STREAM_FLAG_RT_PROCESS;
    if (!active)
        flags |= PW_STREAM_FLAG_INACTIVE;
    int result = pw_stream_connect(sink->stream, PW_DIRECTION_INPUT, PW_ID_ANY,
        flags, params, n_params);
    if (result < 0) {
        pw_stream_destroy(sink->stream);
        free(sink);
        errno = -result;
        return NULL;
    }
    return sink;
}

struct pw_stream *hil_command_receiver_stream(struct receiver *sink)
{
    return sink->stream;
}

bool hil_command_receiver_drain(struct receiver *sink, void *destination, struct command *result)
{
    if (!atomic_load_explicit(&sink->pending, memory_order_acquire) ||
        atomic_load_explicit(&sink->failed, memory_order_acquire) ||
        atomic_exchange_explicit(&sink->busy, true, memory_order_acquire))
        return false;
    memcpy(destination, sink->values, sink->bytes);
    *result = sink->command;
    result->lost = atomic_load_explicit(&sink->dropped, memory_order_relaxed) +
        atomic_load_explicit(&sink->gaps, memory_order_relaxed);
    atomic_store_explicit(&sink->pending, false, memory_order_relaxed);
    atomic_store_explicit(&sink->busy, false, memory_order_release);
    return true;
}

void hil_command_receiver_status(struct receiver *sink, uint64_t *status)
{
    status[0] = atomic_load_explicit(&sink->pending, memory_order_acquire);
    status[1] = atomic_load_explicit(&sink->dropped, memory_order_relaxed);
    status[2] = atomic_load_explicit(&sink->gaps, memory_order_relaxed);
    status[3] = atomic_load_explicit(&sink->rejected, memory_order_relaxed);
    status[4] = atomic_load_explicit(&sink->failed, memory_order_acquire);
}

struct reset_request {
    struct command identity;
    bool check_only;
};

static int reset_receiver(struct spa_loop *loop, bool async, uint32_t seq,
        const void *data, size_t size, void *user_data)
{
    struct receiver *sink = user_data;
    const struct reset_request *request = data;
    const struct command *identity = &request->identity;
    if (identity->generation && sink->previous.generation &&
        !memcmp(identity->domain, sink->previous.domain, sizeof(identity->domain)) &&
        identity->generation <= sink->previous.generation)
        return -EINVAL;
    if (request->check_only)
        return 0;
    if (atomic_load_explicit(&sink->busy, memory_order_acquire))
        return -EBUSY;
    sink->previous = *identity;
    atomic_store_explicit(&sink->pending, false, memory_order_release);
    atomic_store_explicit(&sink->dropped, 0, memory_order_relaxed);
    atomic_store_explicit(&sink->gaps, 0, memory_order_relaxed);
    atomic_store_explicit(&sink->rejected, 0, memory_order_relaxed);
    return 0;
}

int hil_command_receiver_reset(struct receiver *sink, uint64_t generation,
        const uint8_t *domain, bool check_only)
{
    struct reset_request request = {.identity.generation = generation, .check_only = check_only};
    if (generation)
        memcpy(request.identity.domain, domain, sizeof(request.identity.domain));
    struct pw_loop *loop = pw_stream_get_data_loop(sink->stream);
    if (loop == NULL)
        return -EIO;
    return pw_loop_locked(loop, reset_receiver, 0, &request, sizeof(request), sink);
}

void hil_command_receiver_free(struct receiver *sink)
{
    pw_stream_destroy(sink->stream);
    free(sink);
}
