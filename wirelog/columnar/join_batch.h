/*
 * columnar/join_batch.h - bounded keyed-join sub-batch producer (Issue #1446)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * A single-threaded producer that walks a keyed equijoin over a pinned
 * persistent primary arrangement and publishes immutable, pre-admitted
 * batches through the continuation contract in columnar/continuation.h.
 * The cursor advances only after the sink commits a complete batch, so a
 * batch that fails before commit is retried from the same position and a
 * committed batch is never replayed.
 *
 * This is the ordinary keyed-join slice of #1446.  Differential joins,
 * pipeline consumption (a JOIN with a row-local FILTER/MAP tail on the eval
 * stack), LFTJ, cross joins, ephemeral hash fallback, K-Fusion and
 * multiworker transport are outside this unit; see docs/MEMORY.md "Bounded
 * join sub-batches".
 */
#ifndef WL_COLUMNAR_JOIN_BATCH_H
#define WL_COLUMNAR_JOIN_BATCH_H

#include "columnar/continuation.h"
#include "columnar/batch_payload.h"
#include "columnar/internal.h"

#include <stdbool.h>
#include <stdint.h>

/* Why a join shape did or did not enter bounded mode.  The values are
 * stable so a session can record the last reason as a small integer. */
typedef enum {
    COL_JOIN_BATCH_ELIGIBLE = 0,
    COL_JOIN_BATCH_EXCLUDED_OFF = 1,            /* WIRELOG_JOIN_BATCH_BYTES unset */
    COL_JOIN_BATCH_EXCLUDED_CROSS = 2,          /* key_count == 0 */
    COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT = 3,    /* right side is a delta */
    COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT = 4, /* right filter expression */
    COL_JOIN_BATCH_EXCLUDED_WORKER = 5,         /* TDD worker session */
    COL_JOIN_BATCH_EXCLUDED_NO_ARRANGEMENT = 6, /* no persistent arrangement */
    COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE = 7,  /* batch budget < one row */
    COL_JOIN_BATCH_EXCLUDED_PIPELINE = 8,       /* downstream not consumable */
    COL_JOIN_BATCH_EXCLUDED_MATERIALIZED = 9,   /* differential, materialized */
    COL_JOIN_BATCH_EXCLUDED_LFTJ = 10,          /* LFTJ is never bounded */
    COL_JOIN_BATCH_EXCLUDED_INSERT_ROUTE = 11,  /* insertion clone, knob off */
    COL_JOIN_BATCH_REASON_COUNT                 /* not a reason */
} col_join_batch_eligibility_t;

col_join_batch_eligibility_t
col_join_batch_eligibility(const wl_col_session_t *sess, uint32_t key_count,
    bool used_right_delta, bool has_right_filter);

const char *
col_join_batch_eligibility_name(col_join_batch_eligibility_t reason);

/* Record a fallback to the one-shot join: bumps the session counter,
 * stores the reason and logs one JOIN warning per reason per session. */
void
col_join_batch_record_fallback(wl_col_session_t *sess,
    col_join_batch_eligibility_t reason);

/* Fold the fallbacks @worker recorded into @coordinator (#1382): adds the
 * count (saturating), takes the worker's last reason when it recorded any,
 * and merges the warned-reasons mask, so workers created later do not warn
 * again about a reason already warned about (workers of the same round may
 * each warn once).  Called once per worker, on the coordinator's thread,
 * when the worker's teardown is reported. */
void
col_join_batch_fold_worker(wl_col_session_t *coordinator,
    const wl_col_session_t *worker);

/* Producer position: the first UNEXAMINED candidate pair.  rr == UINT32_MAX
 * means "probe left row lr from its bucket head"; it is never stored for an
 * exhausted chain, so resuming can neither skip nor repeat a match. */
typedef struct {
    uint32_t lr;
    uint32_t rr;
} col_join_batch_pos_t;

/* Full producer cursor.  The continuation contract carries the packed
 * position, sequence, identities and arrangement generation; the producer
 * keeps the remaining provenance and validates all of it before producing. */
typedef struct {
    col_join_batch_pos_t next;
    uint64_t sequence;        /* committed batches so far */
    uint64_t left_identity;
    uint64_t left_view_gen;
    uint64_t left_storage_gen;
    uint64_t right_identity;
    uint64_t right_view_gen;
    uint64_t right_storage_gen;
    uint64_t arr_generation;
    uint32_t arr_indexed_rows;
    bool left_is_delta;
    bool timestamps;          /* left carried timestamps at create */
} col_join_batch_cursor_t;

/* Create a producer for the keyed join described by @op over @left and the
 * session relation @op->right_relation, using the resolved key columns
 * @lk/@rk.  Pins the right relation's primary arrangement (one lease,
 * released exactly once by destroy or cancel) and admits one governed
 * scratch batch relation of @batch_bytes payload before returning.
 *
 * Returns 0 and a continuation in *@out; EINVAL on bad arguments, ENOENT
 * when no persistent arrangement can be pinned, ENOTSUP when @batch_bytes
 * cannot hold a single output row, ENOSPC when governed admission is denied,
 * EOVERFLOW on checked size overflow, or ENOMEM on allocation failure.
 * Nothing is left pinned or reserved on failure. */
int
col_join_batch_producer_create(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const uint32_t *lk, const uint32_t *rk, uint32_t kc,
    uint64_t batch_bytes, wl_columnar_continuation_t **out);

/* The MAXIMUM rows per batch the producer settled on; 0 for a NULL
 * continuation.  An emitted batch can be shorter: since #1481 the scratch
 * grows on demand, and a grow the governor refuses is answered with a short
 * batch rather than a failed produce. */
uint32_t
col_join_batch_rows_per_batch(const wl_columnar_continuation_t *cont);

/* Physical rows the scratch batch currently holds; 0 for a NULL
 * continuation.  Test and diagnostic surface for #1481's lazy growth. */
uint32_t
col_join_batch_scratch_capacity(const wl_columnar_continuation_t *cont);

/* Bytes admitted for the producer's descriptor allocation (the struct plus
 * the key row and the key-index arrays); 0 without a governor or for a NULL
 * continuation.  Test surface for the per-producer bound (#1370). */
uint64_t
col_join_batch_descriptor_bytes(const wl_columnar_continuation_t *cont);

/* Copy of the producer's full cursor for tests and diagnostics. */
bool
col_join_batch_cursor_get(const wl_columnar_continuation_t *cont,
    col_join_batch_cursor_t *out);

/* Producer-only controlled stepping status. Stop and execution-error values
 * are sticky until the continuation is discarded. */
typedef enum {
    COL_JOIN_BATCH_CONTROL_PROGRESS = 0,
    COL_JOIN_BATCH_CONTROL_BATCH_READY,
    COL_JOIN_BATCH_CONTROL_DONE,
    COL_JOIN_BATCH_CONTROL_WORK_LIMIT,
    COL_JOIN_BATCH_CONTROL_CANCELLED,
    COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT,
    COL_JOIN_BATCH_CONTROL_STALE_INPUT,
    COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR
} col_join_batch_control_status_t;

typedef struct {
    col_join_batch_control_status_t status;
    int execution_status; /* errno-style code only for EXECUTION_ERROR */
    uint64_t work_ordinal; /* cumulative admitted logical work */
    uint64_t batch_generation; /* nonzero only for BATCH_READY */
    uint64_t required_work; /* current indivisible action cost when stalled */
} col_join_batch_control_result_t;

/* Bind the prepared producer to one active operation attempt and transfer
 * exclusive stepping ownership from generic continuation publication.
 * The bound API is serialized on the thread that calls bind. */
int
col_join_batch_control_bind(wl_columnar_continuation_t *cont,
    wl_evaluation_control_t *control, uint64_t attempt_id);

/* Perform at most max_work units, charging before each state transition or
 * bounded tuple/hash action. Complete private rows remain hidden until peek. */
col_join_batch_control_status_t
col_join_batch_control_step(wl_columnar_continuation_t *cont,
    uint64_t max_work, col_join_batch_control_result_t *result);

/* Repeated peeks return the same immutable private batch and generation.
 * Ack consumes only that exact generation. A bad generation is EINVAL and
 * changes no producer state. */
int
col_join_batch_control_peek(wl_columnar_continuation_t *cont,
    const col_rel_t **batch, uint64_t *generation);
int
col_join_batch_control_ack(wl_columnar_continuation_t *cont,
    uint64_t generation);

/* Validate the exact pending generation and attempt/input state before a
 * caller's private output commit. This does not acknowledge or advance the
 * producer; only the runner's module-private post-commit transition can do so. */
int
col_join_batch_control_precommit(wl_columnar_continuation_t *cont,
    uint64_t generation);

/* Discard and destroy the continuation, releasing its arrangement lease,
 * admitted scratch, and retained control reference; clears *cont. */
int
col_join_batch_control_discard(wl_columnar_continuation_t **cont);

/* Attempt-controlled private JOIN runner. start requires an already-built
 * current arrangement, holds readers for both inputs, then synchronously
 * admits a fixed-capacity result; this setup is outside advance's work bound.
 * advance(0) is a true no-op. An underfunded positive quantum returns
 * resumable PROGRESS with required_work and no state change. Every call
 * consumes at most max_work; DONE transfers output and destroys the runner.
 * Capacity exhaustion is sticky ENOSPC. Ordinary JOIN dispatch is unchanged. */
int
col_join_batch_runner_start(wl_col_session_t *sess, const wl_plan_op_t *op,
    const col_rel_t *left, bool left_is_delta, const uint32_t *lk,
    const uint32_t *rk, uint32_t key_count, uint64_t batch_bytes,
    uint32_t output_row_capacity, wl_evaluation_control_t *control,
    uint64_t attempt_id, wl_columnar_continuation_t **out);
col_join_batch_control_status_t
col_join_batch_runner_advance(wl_columnar_continuation_t **runner,
    uint64_t max_work, col_join_batch_control_result_t *result,
    col_rel_t **completed_output);
int
col_join_batch_runner_discard(wl_columnar_continuation_t **runner);

/* Synchronous all-or-nothing sink that appends each committed batch to a
 * heap-owned relation admitted under the session governor.  The relation's
 * retained token owns every visible output byte; the sink holds no token. */
typedef struct {
    wl_col_session_t *sess;
    col_rel_t *out;
    uint32_t pending_begin; /* out->nrows when the current batch began */
    int error; /* EOVERFLOW: the batch would pass UINT32_MAX rows */
    bool begun;
} col_join_batch_relation_sink_t;

/* Grow @out so it holds at least @need rows, for a bounded join sink
 * (Issue #2173).  When @need exceeds the capacity and double the capacity
 * (clamped to UINT32_MAX) exceeds @need, the doubling is admitted first;
 * only a budget denial of it is retried at exactly @need, with the denial
 * cleared first, so a successful retry leaves no pending denial on @out.
 * Otherwise @need itself is admitted; when it fits, that covers buffers
 * @out owns but has not admitted yet.  Returns OK, RESERVATION_DENIED when
 * the governor refuses the exact attempt, or SINK_FAILURE for any other
 * failure, including a non-denial failure of the doubling. */
wl_columnar_continuation_status_t
col_join_batch_sink_grow(col_rel_t *out, uint64_t need);

/* Initialise @ctx and fill @sink's callbacks.  Refuses pool-owned
 * relations (EINVAL): delta_pool_reset() never frees slot contents, so a
 * governor reference attached to a pooled relation would leak.  Attaches
 * the session governor and admits the capacity @out already owns, so the
 * first batch cannot publish rows into unadmitted buffers.  On failure the
 * caller still owns @out. */
int
col_join_batch_relation_sink_init(col_join_batch_relation_sink_t *ctx,
    wl_columnar_continuation_sink_t *sink, wl_col_session_t *sess,
    col_rel_t *out);

/* Publish every batch of @cont into @out through the relation sink.
 * Returns 0 when the producer reports completion; EOVERFLOW when the legacy
 * row cap (WIRELOG_JOIN_OUTPUT_LIMIT) is reached after a committed batch,
 * in which case the cap may be overshot by at most rows_per_batch - 1;
 * EOVERFLOW when a batch would take @out past UINT32_MAX rows; ENOSPC when
 * the governor denies the growth of @out or, for the differential
 * producer, its scratch; ENOMEM on any other sink, commit or allocation
 * failure; EAGAIN when the inputs went stale; ENOTSUP or EINVAL for
 * contract errors; EIO when a commit was reported ambiguous.  An error
 * from col_join_batch_relation_sink_init is returned unchanged.  The
 * continuation is cancelled on every error. */
int
col_join_batch_run_to_relation(wl_columnar_continuation_t *cont,
    wl_col_session_t *sess, col_rel_t *out);

/* Owned numeric-payload sink for relation-backed continuation batches. This
 * does not change the global batch.payload convention or choose spill policy.
 * Caller-owned context (included in caller floor), new or cleaned before init;
 * governor/store remain caller-owned and live throughout sink use. Lifecycle
 * is synchronous and externally serialized. Configure storage/cancel only
 * between publish calls. Source pointer is borrowed only during publication.
 * Commit fills ready; take transfers it. An untaken ready blocks publication.
 * Cleanup releases both private stages and untaken committed payloads; taken
 * payloads belong solely to callers. Detailed capture errors remain in error. */
typedef struct {
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_spill_store_t *store;
    wl_columnar_batch_payload_storage_t storage;
    wl_columnar_spill_cancel_fn cancel;
    void *cancel_context;
    wl_columnar_spill_status_t error;
    const col_rel_t *source;
    uint32_t rows;
    bool begun;
    wl_columnar_batch_payload_t *staged, *ready;
    wl_intern_t *intern; /* Explicit STRING provenance; borrowed during use. */
} wl_columnar_join_batch_payload_sink_t;

void wl_columnar_join_batch_payload_sink_init(
    wl_columnar_join_batch_payload_sink_t *context,
    wl_columnar_continuation_sink_t *sink,
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store,
    wl_columnar_batch_payload_storage_t storage);
/* STRING-capable sink: caller guarantees this is the producing table for all
 * STRING columns. Keep its owner alive during publication. Taken payloads own
 * their pins independently; cleanup does not free the intern owner. */
void wl_columnar_join_batch_payload_sink_init_with_intern(
    wl_columnar_join_batch_payload_sink_t *context,
    wl_columnar_continuation_sink_t *sink,
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store,
    wl_columnar_batch_payload_storage_t storage, wl_intern_t *intern);
wl_columnar_batch_payload_t *wl_columnar_join_batch_payload_sink_take(
    wl_columnar_join_batch_payload_sink_t *context);
void wl_columnar_join_batch_payload_sink_cleanup(
    wl_columnar_join_batch_payload_sink_t *context);

#endif /* WL_COLUMNAR_JOIN_BATCH_H */
