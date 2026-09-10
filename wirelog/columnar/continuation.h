/*
 * columnar/continuation.h - internal resumable publication contract
 *
 * This is deliberately not an implementation of a batched join.  It gives
 * future producers an explicit boundary at which a batch becomes visible.
 */

#ifndef WL_COLUMNAR_CONTINUATION_H
#define WL_COLUMNAR_CONTINUATION_H

#include "columnar/memory_governor.h"

#include <stdbool.h>
#include <stdint.h>

typedef enum {
    WL_COLUMNAR_CONTINUATION_OK = 0,
    WL_COLUMNAR_CONTINUATION_DONE,
    WL_COLUMNAR_CONTINUATION_INVALID,
    WL_COLUMNAR_CONTINUATION_UNSUPPORTED,
    WL_COLUMNAR_CONTINUATION_STALE,
    WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED,
    WL_COLUMNAR_CONTINUATION_SINK_FAILURE,
    WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE,
    /* The sink reports that its side effect is durable, but cannot report an
     * unambiguous successful return to its caller.  The cursor has already
     * advanced; retrying the batch would duplicate the side effect. */
    WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS,
} wl_columnar_continuation_status_t;

/* The identity and generation are opaque to this contract, but are copied
 * into every continuation so a producer can reject stale input explicitly. */
typedef struct {
    uint64_t input_identity;
    uint64_t arrangement_identity;
    uint32_t generation;
    uint64_t position;
    uint64_t sequence;
} wl_columnar_continuation_cursor_t;

typedef struct {
    const void *payload;
    uint64_t bytes;
    uint32_t rows;
    bool complete;
    wl_columnar_continuation_cursor_t next_cursor;
} wl_columnar_continuation_batch_t;

struct wl_columnar_continuation;

typedef wl_columnar_continuation_status_t
(*wl_columnar_continuation_produce_fn)(
    void *context,
    const wl_columnar_continuation_cursor_t *cursor,
    wl_columnar_continuation_batch_t *batch);

typedef bool (*wl_columnar_continuation_validate_fn)(
    void *context, const wl_columnar_continuation_cursor_t *cursor);

typedef void (*wl_columnar_continuation_destroy_fn)(void *context);
typedef void (*wl_columnar_continuation_cancel_fn)(void *context);

typedef struct {
    void *context;
    wl_columnar_continuation_produce_fn produce;
    wl_columnar_continuation_validate_fn validate;
    wl_columnar_continuation_destroy_fn destroy;
    wl_columnar_continuation_cancel_fn cancel;
} wl_columnar_continuation_producer_t;

/* begin/append/abort are synchronous.  abort must release the reservation
 * made by the current begin/reserve transaction.  reserve must account for
 * all bytes that become visible at append time (payload, metadata and sink scratch).
 * commit reports whether its side effects became durable even when it returns
 * COMMIT_FAILURE.  A committed side effect advances the cursor and is
 * reported as COMMIT_AMBIGUOUS when the return status is not OK; callers must
 * not retry that batch. */
typedef wl_columnar_continuation_status_t
(*wl_columnar_continuation_sink_begin_fn)(
    void *context, const wl_columnar_continuation_batch_t *batch);
typedef wl_columnar_continuation_status_t
(*wl_columnar_continuation_sink_reserve_fn)(
    void *context, uint64_t bytes, uint32_t rows);
typedef wl_columnar_continuation_status_t
(*wl_columnar_continuation_sink_append_fn)(
    void *context, const wl_columnar_continuation_batch_t *batch);
typedef wl_columnar_continuation_status_t
(*wl_columnar_continuation_sink_commit_fn)(
    void *context, bool *committed);
typedef void (*wl_columnar_continuation_sink_abort_fn)(void *context);

typedef struct {
    void *context;
    wl_columnar_continuation_sink_begin_fn begin;
    wl_columnar_continuation_sink_reserve_fn reserve;
    wl_columnar_continuation_sink_append_fn append;
    wl_columnar_continuation_sink_commit_fn commit;
    wl_columnar_continuation_sink_abort_fn abort;
} wl_columnar_continuation_sink_t;

typedef struct wl_columnar_continuation wl_columnar_continuation_t;

wl_columnar_continuation_t *
wl_columnar_continuation_create(
    const wl_columnar_continuation_producer_t *producer,
    const wl_columnar_continuation_cursor_t *initial_cursor);

void
wl_columnar_continuation_destroy(wl_columnar_continuation_t *continuation);

void
wl_columnar_continuation_cancel(wl_columnar_continuation_t *continuation);

wl_columnar_continuation_status_t
wl_columnar_continuation_publish(
    wl_columnar_continuation_t *continuation,
    const wl_columnar_continuation_sink_t *sink);

const wl_columnar_continuation_cursor_t *
wl_columnar_continuation_cursor(
    const wl_columnar_continuation_t *continuation);

/* The producer context handed to wl_columnar_continuation_create; owned by
 * the producer, valid until destroy.  Lets a producer module reach its own
 * state through the continuation it created (Issue #1446). */
void *
wl_columnar_continuation_producer_context(
    const wl_columnar_continuation_t *continuation);

bool
wl_columnar_continuation_is_done(
    const wl_columnar_continuation_t *continuation);

#endif /* WL_COLUMNAR_CONTINUATION_H */
