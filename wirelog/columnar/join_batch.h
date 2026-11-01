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
 * join sub-batches".  The producer runs on calling and worker sessions,
 * one per session at a time.  In a worker run that resolves its operands
 * by input slot (a TDD bound run or insert clone) it runs over an index of
 * its own on the slot's relation, col_join_batch_producer_create_over, as
 * the registry there need not hold that relation under its name (#2201).
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
    COL_JOIN_BATCH_EXCLUDED_WORKER = 5,         /* not returned since #2201 */
    COL_JOIN_BATCH_EXCLUDED_NO_ARRANGEMENT = 6, /* no persistent arrangement */
    COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE = 7,  /* batch budget < one row */
    COL_JOIN_BATCH_EXCLUDED_PIPELINE = 8,       /* downstream not consumable */
    COL_JOIN_BATCH_EXCLUDED_MATERIALIZED = 9,   /* differential, materialized */
    COL_JOIN_BATCH_EXCLUDED_LFTJ = 10,          /* LFTJ is never bounded */
    COL_JOIN_BATCH_EXCLUDED_INSERT_ROUTE = 11,  /* not recorded since #2201 */
    COL_JOIN_BATCH_REASON_COUNT                 /* not a reason */
} col_join_batch_eligibility_t;

/* The routes a bounded producer run is counted under (#2201).  The values
 * index wl_col_session_t's join_batch_runs and join_batch_worker_runs. */
typedef enum {
    COL_JOIN_BATCH_RUN_ORDINARY = 0,     /* ordinary keyed JOIN */
    COL_JOIN_BATCH_RUN_DIFFERENTIAL = 1, /* differential keyed JOIN */
    COL_JOIN_BATCH_RUN_PIPELINE = 2,     /* fused JOIN pipeline */
    COL_JOIN_BATCH_RUN_ROUTES            /* not a route */
} col_join_batch_run_route_t;

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

/* Count one bounded producer run that completed on @sess under @route:
 * a saturating increment of sess->join_batch_runs[route] (#2201). */
void
col_join_batch_record_run(wl_col_session_t *sess,
    col_join_batch_run_route_t route);

/* Fold the fallbacks @worker recorded into @coordinator (#1382): adds the
 * count (saturating), takes the worker's last reason when it recorded any,
 * and merges the warned-reasons mask, so workers created later do not warn
 * again about a reason already warned about (workers of the same round may
 * each warn once).  It also adds, per route, the worker's own runs and the
 * runs it folded from its own workers into the coordinator's
 * join_batch_worker_runs, saturating (#2201).  Called once per worker, on
 * the coordinator's thread, when the worker's teardown is reported. */
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

/* As col_join_batch_producer_create, over the explicit relation @right
 * instead of the one registered under @op->right_relation (#2201): the
 * producer never looks a relation up by name.  It holds a source reader
 * on @right, admits the bytes col_arrangement_owned_bytes gives for
 * @right's rows under the producer's governor, and builds its own index
 * over @right, keyed by @rk; it pins no registry arrangement.  Destroy or
 * cancel frees the index, then returns its admission, then the reader.
 * Returns as col_join_batch_producer_create, with EINVAL for a NULL
 * @right and EBUSY when @right is write-locked; nothing is left reserved
 * or held on failure. */
int
col_join_batch_producer_create_over(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const col_rel_t *right, const uint32_t *lk, const uint32_t *rk,
    uint32_t kc, uint64_t batch_bytes, wl_columnar_continuation_t **out);

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

/* The bytes of the index a producer created over an explicit right
 * relation owns while it lives, as col_arrangement_owned_bytes gives them
 * (#2201); 0 for any other producer, after cancel, or for NULL. */
uint64_t
col_join_batch_index_bytes(const wl_columnar_continuation_t *cont);

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
 * The bound API is serialized on the thread that calls bind. A producer
 * created by col_join_batch_producer_create_over is refused with ENOTSUP
 * (#2201). One INT64 key and 1..64 output lanes retain their historical
 * tariff. Other scalar shapes validate one key/column, hash one byte,
 * compare one key and write one cell per fixed-unit action; a wide row
 * stays private until complete. This does not make the raw constructor's
 * synchronous setup part of the step bound. */
int
col_join_batch_control_bind(wl_columnar_continuation_t *cont,
    wl_evaluation_control_t *control, uint64_t attempt_id);

#ifdef WL_SESSION_TEST_HOOKS
/* Generalized producer: phase1 precedes a cell write, phase2 precedes row
* visibility. The scratch descriptor remains private until BATCH_READY. */
extern void (*wl_columnar_join_batch_test_control_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t, const col_rel_t *);
#endif

/* Perform at most max_work units, charging before each state transition or
 * bounded tuple/hash action. Complete private rows remain hidden until peek.
 * A zero max_work is sticky execution-error/EINVAL, unlike runner advance. */
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

/* Stable caller-owned preparation receipt. Sources, op and all referenced op
 * metadata (including projection indices) remain live and immutable until
 * drained producer teardown. Input resolved-key arrays are borrowed only
 * through READY or abort; preparation copies their entries. After READY
 * the receipt continues owning the producer and source leases: destroy it only
 * through abort_step after consumers have released every batch reader. Never
 * copy/reset a live receipt or destroy its borrowed continuation directly.
 * Local steps spend at most max_work unit actions, except the explicitly
 * nested controlled constructor/governor calls, which retain their own polling
 * contract. This is a polling bound, not a total per-call work bound. */
typedef struct wl_columnar_join_batch_prepare {
    const struct wl_columnar_join_batch_prepare *identity;
    wl_col_session_t *session;
    const wl_plan_op_t *op;
    const col_rel_t *left, *right;
    const uint32_t *input_left_keys, *input_right_keys;
    uint32_t key_count, output_width, rows_per_batch, capacity;
    uint64_t batch_bytes, allocation_bytes;
    bool left_is_delta, ready, aborting, drained;
    wl_evaluation_control_t *control;
    uint64_t attempt_id;
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_source_access_reader_t left_reader, right_reader;
    col_relation_snapshot_t left_snapshot, right_snapshot;
    col_arrangement_t arrangement;
    uint32_t *keys;
    int64_t *types, *key_row;
    void *producer;
    wl_columnar_continuation_t *continuation;
    wl_columnar_memory_reservation_t descriptor_credit, arrays_credit,
        continuation_credit;
    bool descriptor_admitted, arrays_admitted, continuation_admitted;
    bool descriptor_committed, arrays_committed, continuation_committed;
    wl_columnar_relation_scalar_retirement_t retirement;
    bool retirement_started, retirement_committed;
    uint32_t phase, row, key, byte, hash;
    uint64_t cursor;
    uint64_t hash_value;
    int result;
} wl_columnar_join_batch_prepare_t;

int wl_columnar_join_batch_prepare_begin(wl_columnar_join_batch_prepare_t *,
    wl_col_session_t *, const wl_plan_op_t *, const col_rel_t *,
    const col_rel_t *, bool, const uint32_t *, const uint32_t *, uint32_t,
    uint64_t, wl_evaluation_control_t *, uint64_t);
int wl_columnar_join_batch_prepare_step(wl_columnar_join_batch_prepare_t *,
    uint64_t max_work, bool *done);
int wl_columnar_join_batch_prepare_abort_step(
    wl_columnar_join_batch_prepare_t *,
    wl_evaluation_control_t *funding, uint64_t funding_id, bool *done);
#ifdef WL_SESSION_TEST_HOOKS
extern void (*wl_columnar_join_batch_test_prepare_checkpoint)(
    const wl_columnar_join_batch_prepare_t *);
extern uint32_t wl_columnar_join_batch_test_prepare_fail_allocation;
#endif

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
    wl_intern_t *intern; /* Explicit STRING/functor provenance. */
    const wirelog_column_t *inline_columns; /* Borrowed during publication. */
    uint32_t inline_column_count;
    bool inline_schema;
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
/* Explicit flat INLINE declarations; same provenance contract as capture.
 * Keep declarations/intern immutable and live throughout synchronous sink use.
 * No automatic JOIN output declaration inference is performed. */
void wl_columnar_join_batch_payload_sink_init_with_inline(
    wl_columnar_join_batch_payload_sink_t *context,
    wl_columnar_continuation_sink_t *sink,
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store,
    wl_columnar_batch_payload_storage_t storage,
    const wirelog_column_t *columns, uint32_t logical_count,
    wl_intern_t *intern);
wl_columnar_batch_payload_t *wl_columnar_join_batch_payload_sink_take(
    wl_columnar_join_batch_payload_sink_t *context);
void wl_columnar_join_batch_payload_sink_cleanup(
    wl_columnar_join_batch_payload_sink_t *context);

#endif /* WL_COLUMNAR_JOIN_BATCH_H */
