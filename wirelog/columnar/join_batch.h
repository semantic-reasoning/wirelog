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
 * pipeline consumption (JOIN -> FILTER* -> MAP on the eval stack), LFTJ,
 * cross joins, ephemeral hash fallback, K-Fusion and multiworker transport
 * are outside this unit; see docs/MEMORY.md "Bounded join sub-batches".
 */
#ifndef WL_COLUMNAR_JOIN_BATCH_H
#define WL_COLUMNAR_JOIN_BATCH_H

#include "columnar/continuation.h"
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
    uint32_t arr_generation;
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
 * cannot hold a single output row, ENOMEM on admission or allocation
 * failure.  Nothing is left pinned or reserved on failure. */
int
col_join_batch_producer_create(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const uint32_t *lk, const uint32_t *rk, uint32_t kc,
    uint64_t batch_bytes, wl_columnar_continuation_t **out);

/* Rows per batch the producer settled on; 0 for a NULL continuation. */
uint32_t
col_join_batch_rows_per_batch(const wl_columnar_continuation_t *cont);

/* Copy of the producer's full cursor for tests and diagnostics. */
bool
col_join_batch_cursor_get(const wl_columnar_continuation_t *cont,
    col_join_batch_cursor_t *out);

/* Synchronous all-or-nothing sink that appends each committed batch to a
 * heap-owned relation admitted under the session governor.  The relation's
 * retained token owns every visible output byte; the sink holds no token. */
typedef struct {
    wl_col_session_t *sess;
    col_rel_t *out;
    uint32_t pending_begin; /* out->nrows when the current batch began */
    bool begun;
} col_join_batch_relation_sink_t;

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
 * ENOMEM on admission denial or sink failure; EAGAIN when the inputs went
 * stale; ENOTSUP or EINVAL for contract errors; EIO when a commit was
 * reported ambiguous.  The continuation is cancelled on every error. */
int
col_join_batch_run_to_relation(wl_columnar_continuation_t *cont,
    wl_col_session_t *sess, col_rel_t *out);

#endif /* WL_COLUMNAR_JOIN_BATCH_H */
