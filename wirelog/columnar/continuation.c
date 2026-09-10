/* Internal continuation/publication contract; no join producer lives here. */

#include "columnar/continuation.h"

#include <stdlib.h>
#include <string.h>

struct wl_columnar_continuation {
    wl_columnar_continuation_producer_t producer;
    wl_columnar_continuation_cursor_t cursor;
    bool done;
    bool cancelled;
};

wl_columnar_continuation_t *
wl_columnar_continuation_create(
    const wl_columnar_continuation_producer_t *producer,
    const wl_columnar_continuation_cursor_t *initial_cursor)
{
    wl_columnar_continuation_t *continuation;

    if (!producer || !producer->produce || !initial_cursor)
        return NULL;
    continuation = calloc(1, sizeof(*continuation));
    if (!continuation)
        return NULL;
    continuation->producer = *producer;
    continuation->cursor = *initial_cursor;
    return continuation;
}

void
wl_columnar_continuation_destroy(wl_columnar_continuation_t *continuation)
{
    if (!continuation)
        return;
    if (continuation->producer.destroy)
        continuation->producer.destroy(continuation->producer.context);
    free(continuation);
}

void
wl_columnar_continuation_cancel(wl_columnar_continuation_t *continuation)
{
    if (!continuation || continuation->cancelled || continuation->done)
        return;
    continuation->cancelled = true;
    if (continuation->producer.cancel)
        continuation->producer.cancel(continuation->producer.context);
}

static bool
valid_sink(const wl_columnar_continuation_sink_t *sink)
{
    return sink && sink->begin && sink->reserve && sink->append
           && sink->commit && sink->abort;
}

wl_columnar_continuation_status_t
wl_columnar_continuation_publish(
    wl_columnar_continuation_t *continuation,
    const wl_columnar_continuation_sink_t *sink)
{
    wl_columnar_continuation_batch_t batch;
    wl_columnar_continuation_status_t status;
    bool begun = false;
    bool committed = false;

    if (!continuation || !valid_sink(sink) || continuation->cancelled)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    if (continuation->done)
        return WL_COLUMNAR_CONTINUATION_DONE;
    if (continuation->producer.validate
        && !continuation->producer.validate(continuation->producer.context,
        &continuation->cursor))
        return WL_COLUMNAR_CONTINUATION_STALE;

    memset(&batch, 0, sizeof(batch));
    status = continuation->producer.produce(continuation->producer.context,
            &continuation->cursor, &batch);
    if (status == WL_COLUMNAR_CONTINUATION_DONE) {
        continuation->done = true;
        return status;
    }
    if (status != WL_COLUMNAR_CONTINUATION_OK)
        return status;
    /* A completion-only result is terminal without publication.  Its cursor
     * is deliberately ignored: completion must not overwrite the last
     * committed cursor or invoke an empty append. */
    if (batch.rows == 0) {
        if (!batch.complete || batch.bytes != 0 || batch.payload != NULL)
            return WL_COLUMNAR_CONTINUATION_INVALID;
        continuation->done = true;
        return WL_COLUMNAR_CONTINUATION_DONE;
    }
    status = sink->begin(sink->context, &batch);
    if (status != WL_COLUMNAR_CONTINUATION_OK)
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    begun = true;
    status = sink->reserve(sink->context, batch.bytes, batch.rows);
    if (status != WL_COLUMNAR_CONTINUATION_OK) {
        sink->abort(sink->context);
        return status == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
            ? status : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    status = sink->append(sink->context, &batch);
    if (status != WL_COLUMNAR_CONTINUATION_OK) {
        sink->abort(sink->context);
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    status = sink->commit(sink->context, &committed);
    if (!committed) {
        /* A sink that has not committed must release its reservation. */
        if (begun)
            sink->abort(sink->context);
        return WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE;
    }

    /* Once the sink reports a durable side effect, abort is no longer safe
     * and replaying this cursor would duplicate the batch.  This remains true
     * when the sink could not return an unambiguous success status. */
    continuation->cursor = batch.next_cursor;
    continuation->done = batch.complete;
    return status == WL_COLUMNAR_CONTINUATION_OK
        ? WL_COLUMNAR_CONTINUATION_OK
        : WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS;
}

const wl_columnar_continuation_cursor_t *
wl_columnar_continuation_cursor(
    const wl_columnar_continuation_t *continuation)
{
    return continuation ? &continuation->cursor : NULL;
}

void *
wl_columnar_continuation_producer_context(
    const wl_columnar_continuation_t *continuation)
{
    return continuation ? continuation->producer.context : NULL;
}

bool
wl_columnar_continuation_is_done(
    const wl_columnar_continuation_t *continuation)
{
    return continuation && continuation->done;
}
