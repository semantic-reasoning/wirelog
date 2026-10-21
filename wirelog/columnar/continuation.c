/* Internal continuation/publication contract; no join producer lives here. */

#include "columnar/continuation.h"

#include <stdlib.h>
#include <string.h>

struct wl_columnar_continuation {
    wl_columnar_continuation_producer_t producer;
    wl_columnar_continuation_cursor_t producer_scan_cursor;
    wl_columnar_continuation_cursor_t published_cursor;
    wl_columnar_continuation_batch_t pending_batch;
    bool done;
    bool cancelled;
    bool terminal;
    bool producer_retired;
    bool has_pending_batch;
    bool publishing;
    bool cancel_requested;
    bool destroy_requested;
    bool destroying;
    wl_columnar_continuation_status_t terminal_status;
};

static void
dispose_pending(wl_columnar_continuation_t *continuation, bool committed)
{
    if (!continuation->has_pending_batch)
        return;
    wl_columnar_continuation_batch_t batch = continuation->pending_batch;
    continuation->has_pending_batch = false;
    memset(&continuation->pending_batch, 0,
        sizeof(continuation->pending_batch));
    continuation->producer.disposition(continuation->producer.context,
        &batch, committed);
}

static void
retire_producer(wl_columnar_continuation_t *continuation)
{
    if (continuation->producer_retired)
        return;
    continuation->producer_retired = true;
    if (continuation->producer.cancel)
        continuation->producer.cancel(continuation->producer.context);
}

static void
terminate_protocol(wl_columnar_continuation_t *continuation,
    wl_columnar_continuation_status_t status)
{
    continuation->terminal = true;
    continuation->terminal_status = status;
    continuation->cancelled = status == WL_COLUMNAR_CONTINUATION_INVALID;
    continuation->cancel_requested = false;
    dispose_pending(continuation, false);
    retire_producer(continuation);
}

static void
destroy_now(wl_columnar_continuation_t *continuation)
{
    if (continuation->destroying)
        return;
    continuation->destroying = true;
    if (continuation->producer.disposition && !continuation->done
        && !continuation->terminal)
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
    if (continuation->producer.destroy)
        continuation->producer.destroy(continuation->producer.context);
    free(continuation);
}

static wl_columnar_continuation_status_t
finish_publish(wl_columnar_continuation_t *continuation,
    wl_columnar_continuation_status_t status)
{
    continuation->publishing = false;
    if (continuation->destroy_requested)
        destroy_now(continuation);
    return status;
}

static bool
cursor_progress_valid(const wl_columnar_continuation_cursor_t *previous,
    const wl_columnar_continuation_cursor_t *next)
{
    return previous->input_identity == next->input_identity
           && previous->arrangement_identity == next->arrangement_identity
           && previous->generation == next->generation
           && next->position >= previous->position
           && next->sequence > previous->sequence;
}

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
    continuation->producer_scan_cursor = *initial_cursor;
    continuation->published_cursor = *initial_cursor;
    return continuation;
}

void
wl_columnar_continuation_destroy(wl_columnar_continuation_t *continuation)
{
    if (!continuation)
        return;
    if (continuation->destroying)
        return;
    continuation->destroy_requested = true;
    if (continuation->publishing) {
        if (continuation->producer.disposition)
            continuation->cancel_requested = true;
        return;
    }
    /* Teardown callbacks can reenter destroy, cancel, or publish. Keep the
     * object busy until all teardown callbacks have returned. */
    continuation->publishing = true;
    destroy_now(continuation);
}

void
wl_columnar_continuation_cancel(wl_columnar_continuation_t *continuation)
{
    if (!continuation || continuation->cancelled || continuation->done
        || continuation->terminal)
        return;
    if (continuation->producer.disposition && continuation->publishing) {
        continuation->cancel_requested = true;
        return;
    }
    if (continuation->producer.disposition) {
        continuation->publishing = true;
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
        if (continuation->destroy_requested)
            destroy_now(continuation);
        else
            continuation->publishing = false;
        return;
    }
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

static wl_columnar_continuation_status_t
publish_legacy(wl_columnar_continuation_t *continuation,
    const wl_columnar_continuation_sink_t *sink)
{
    wl_columnar_continuation_batch_t batch;
    wl_columnar_continuation_status_t status;
    bool begun = false;
    bool committed = false;

    if (continuation->producer.validate
        && !continuation->producer.validate(continuation->producer.context,
        &continuation->published_cursor))
        return WL_COLUMNAR_CONTINUATION_STALE;
    memset(&batch, 0, sizeof(batch));
    status = continuation->producer.produce(continuation->producer.context,
            &continuation->published_cursor, &batch);
    if (status == WL_COLUMNAR_CONTINUATION_DONE) {
        continuation->done = true;
        return status;
    }
    if (status != WL_COLUMNAR_CONTINUATION_OK)
        return status;
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
        if (begun)
            sink->abort(sink->context);
        return WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE;
    }
    continuation->published_cursor = batch.next_cursor;
    continuation->producer_scan_cursor = batch.next_cursor;
    continuation->done = batch.complete;
    return status == WL_COLUMNAR_CONTINUATION_OK
        ? WL_COLUMNAR_CONTINUATION_OK
        : WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS;
}

static wl_columnar_continuation_status_t
publish_protocol(wl_columnar_continuation_t *continuation,
    const wl_columnar_continuation_sink_t *sink)
{
    wl_columnar_continuation_status_t status;
    bool committed = false;
    bool valid = true;

    if (continuation->publishing || continuation->destroying)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    if (continuation->terminal)
        return continuation->terminal_status;
    if (continuation->done)
        return WL_COLUMNAR_CONTINUATION_DONE;
    continuation->publishing = true;
    if (continuation->producer.validate)
        valid = continuation->producer.validate(
            continuation->producer.context,
            &continuation->producer_scan_cursor);
    if (continuation->cancel_requested) {
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_INVALID);
    }
    if (!valid) {
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_STALE);
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_STALE);
    }
    if (!continuation->has_pending_batch) {
        wl_columnar_continuation_batch_t produced;
        memset(&produced, 0, sizeof(produced));
        status = continuation->producer.produce(continuation->producer.context,
                &continuation->producer_scan_cursor, &produced);
        if (continuation->cancel_requested) {
            if (produced.payload || produced.bytes || produced.rows)
                continuation->producer.disposition(
                    continuation->producer.context, &produced, false);
            terminate_protocol(continuation,
                WL_COLUMNAR_CONTINUATION_INVALID);
            return finish_publish(continuation,
                       WL_COLUMNAR_CONTINUATION_INVALID);
        }
        if (status == WL_COLUMNAR_CONTINUATION_DONE) {
            continuation->done = true;
            return finish_publish(continuation, status);
        }
        if (status == WL_COLUMNAR_CONTINUATION_PROGRESS) {
            if (produced.payload || produced.bytes || produced.rows
                || produced.complete
                || !cursor_progress_valid(&continuation->producer_scan_cursor,
                &produced.next_cursor)) {
                if (produced.payload || produced.bytes || produced.rows)
                    continuation->producer.disposition(
                        continuation->producer.context, &produced, false);
                terminate_protocol(continuation,
                    WL_COLUMNAR_CONTINUATION_INVALID);
                return finish_publish(continuation,
                           WL_COLUMNAR_CONTINUATION_INVALID);
            }
            continuation->producer_scan_cursor = produced.next_cursor;
            return finish_publish(continuation, status);
        }
        if (status != WL_COLUMNAR_CONTINUATION_OK) {
            return finish_publish(continuation, status);
        }
        if (produced.rows == 0) {
            if (!produced.complete || produced.bytes != 0
                || produced.payload != NULL) {
                if (produced.payload || produced.bytes || produced.rows)
                    continuation->producer.disposition(
                        continuation->producer.context, &produced, false);
                terminate_protocol(continuation,
                    WL_COLUMNAR_CONTINUATION_INVALID);
                return finish_publish(continuation,
                           WL_COLUMNAR_CONTINUATION_INVALID);
            }
            continuation->done = true;
            return finish_publish(continuation,
                       WL_COLUMNAR_CONTINUATION_DONE);
        }
        continuation->pending_batch = produced;
        continuation->has_pending_batch = true;
    }

    if (continuation->cancel_requested) {
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_INVALID);
    }

    /* The producer's descriptor remains frozen until its infallible
     * disposition callback transfers or drops payload ownership. */
    const wl_columnar_continuation_batch_t *batch =
        &continuation->pending_batch;
    status = sink->begin(sink->context, batch);
    if (continuation->cancel_requested) {
        if (status == WL_COLUMNAR_CONTINUATION_OK)
            sink->abort(sink->context);
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_INVALID);
    }
    if (status != WL_COLUMNAR_CONTINUATION_OK) {
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_SINK_FAILURE);
    }
    status = sink->reserve(sink->context, batch->bytes, batch->rows);
    if (continuation->cancel_requested) {
        sink->abort(sink->context);
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_INVALID);
    }
    if (status != WL_COLUMNAR_CONTINUATION_OK) {
        sink->abort(sink->context);
        wl_columnar_continuation_status_t result =
            status == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
            ? status : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
        if (continuation->cancel_requested) {
            terminate_protocol(continuation,
                WL_COLUMNAR_CONTINUATION_INVALID);
            result = WL_COLUMNAR_CONTINUATION_INVALID;
        }
        return finish_publish(continuation, result);
    }
    status = sink->append(sink->context, batch);
    if (continuation->cancel_requested) {
        sink->abort(sink->context);
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_INVALID);
    }
    if (status != WL_COLUMNAR_CONTINUATION_OK) {
        sink->abort(sink->context);
        if (continuation->cancel_requested) {
            terminate_protocol(continuation,
                WL_COLUMNAR_CONTINUATION_INVALID);
            return finish_publish(continuation,
                       WL_COLUMNAR_CONTINUATION_INVALID);
        }
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_SINK_FAILURE);
    }
    status = sink->commit(sink->context, &committed);
    if (!committed) {
        sink->abort(sink->context);
        if (continuation->cancel_requested) {
            terminate_protocol(continuation,
                WL_COLUMNAR_CONTINUATION_INVALID);
            return finish_publish(continuation,
                       WL_COLUMNAR_CONTINUATION_INVALID);
        }
        return finish_publish(continuation,
                   WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE);
    }

    /* Durable status wins a concurrent cancellation request. Publish and
     * clear the pending state before producer code can reenter. */
    wl_columnar_continuation_batch_t committed_batch = *batch;
    continuation->published_cursor = batch->next_cursor;
    continuation->producer_scan_cursor = batch->next_cursor;
    continuation->done = batch->complete;
    continuation->has_pending_batch = false;
    memset(&continuation->pending_batch, 0,
        sizeof(continuation->pending_batch));
    continuation->producer.disposition(continuation->producer.context,
        &committed_batch, true);
    if (continuation->cancel_requested && !continuation->done)
        terminate_protocol(continuation, WL_COLUMNAR_CONTINUATION_INVALID);
    return finish_publish(continuation, status == WL_COLUMNAR_CONTINUATION_OK
        ? WL_COLUMNAR_CONTINUATION_OK
        : WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS);
}

wl_columnar_continuation_status_t
wl_columnar_continuation_publish(
    wl_columnar_continuation_t *continuation,
    const wl_columnar_continuation_sink_t *sink)
{
    if (!continuation || !valid_sink(sink))
        return WL_COLUMNAR_CONTINUATION_INVALID;
    if (continuation->publishing || continuation->destroying)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    if (continuation->producer.disposition)
        return publish_protocol(continuation, sink);
    if (continuation->cancelled)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    if (continuation->done)
        return WL_COLUMNAR_CONTINUATION_DONE;
    continuation->publishing = true;
    return finish_publish(continuation, publish_legacy(continuation, sink));
}

const wl_columnar_continuation_cursor_t *
wl_columnar_continuation_cursor(
    const wl_columnar_continuation_t *continuation)
{
    return continuation ? &continuation->published_cursor : NULL;
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
