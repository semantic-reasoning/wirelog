/*
 * columnar/join_batch.c - bounded keyed-join sub-batch producer (Issue #1446)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * See join_batch.h for the contract.  Ownership summary:
 *   - the producer owns one arrangement lease, one governed scratch batch
 *     relation and its key row; destroy releases each exactly once, cancel
 *     releases the lease early and is idempotent;
 *   - the relation sink owns nothing: the output relation's retained token
 *     admits every visible byte, growth first asks for double the current
 *     capacity and falls back to the batch's exact need only on a budget
 *     denial (Issue #2173), and an aborted batch only rewinds nrows (the
 *     admitted capacity stays, as after a failed bulk append in relation.c).
 */
#include "columnar/join_batch.h"

#include "wirelog/util/log.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#if defined(WL_HAVE_C11_THREADS)
#include <threads.h>
#elif defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#else
#include <pthread.h>
#endif

/* ---- eligibility ------------------------------------------------------- */

col_join_batch_eligibility_t
col_join_batch_eligibility(const wl_col_session_t *sess, uint32_t key_count,
    bool used_right_delta, bool has_right_filter)
{
    if (!sess || sess->join_batch_bytes == 0)
        return COL_JOIN_BATCH_EXCLUDED_OFF;
    if (sess->coordinator)
        return COL_JOIN_BATCH_EXCLUDED_WORKER;
    if (key_count == 0)
        return COL_JOIN_BATCH_EXCLUDED_CROSS;
    if (used_right_delta)
        return COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT;
    if (has_right_filter)
        return COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT;
    return COL_JOIN_BATCH_ELIGIBLE;
}

const char *
col_join_batch_eligibility_name(col_join_batch_eligibility_t reason)
{
    switch (reason) {
    case COL_JOIN_BATCH_ELIGIBLE: return "eligible";
    case COL_JOIN_BATCH_EXCLUDED_OFF: return "off";
    case COL_JOIN_BATCH_EXCLUDED_CROSS: return "cross-join";
    case COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT: return "delta-right";
    case COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT: return "filtered-right";
    case COL_JOIN_BATCH_EXCLUDED_WORKER: return "worker-session";
    case COL_JOIN_BATCH_EXCLUDED_NO_ARRANGEMENT: return "no-arrangement";
    case COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE: return "row-too-large";
    case COL_JOIN_BATCH_EXCLUDED_PIPELINE: return "pipeline-not-consumable";
    case COL_JOIN_BATCH_EXCLUDED_MATERIALIZED: return "materialized";
    case COL_JOIN_BATCH_EXCLUDED_LFTJ: return "lftj";
    case COL_JOIN_BATCH_EXCLUDED_INSERT_ROUTE: return "insert-route";
    default: return "unknown";
    }
}

/* Each reason owns one bit of the session's warned-reasons mask.  MSVC in
 * its default C mode has no _Static_assert; use the typedef-array check
 * eval_plan.c uses there. */
#if defined(_MSC_VER)
typedef char col_join_batch_reason_bits_check_
    [(COL_JOIN_BATCH_REASON_COUNT <= 16) ? 1 : -1];
#else
_Static_assert(COL_JOIN_BATCH_REASON_COUNT <= 16,
    "join_batch_warned_reasons has one bit per reason");
#endif

void
col_join_batch_record_fallback(wl_col_session_t *sess,
    col_join_batch_eligibility_t reason)
{
    uint16_t bit;

    if (!sess || reason == COL_JOIN_BATCH_ELIGIBLE
        || reason == COL_JOIN_BATCH_EXCLUDED_OFF)
        return;
    sess->join_batch_fallback_count++;
    sess->join_batch_last_reason = (uint8_t)reason;
    bit = (uint16_t)(1u << ((unsigned)reason & 15u));
    if (sess->join_batch_warned_reasons & bit)
        return;
    sess->join_batch_warned_reasons |= bit;
    /* The pipeline reason is recorded when the fused JOIN pipeline (a JOIN
     * with a row-local FILTER/MAP tail) is not used; the JOIN still runs,
     * and its output is materialized for the next operator. */
    if (reason == COL_JOIN_BATCH_EXCLUDED_PIPELINE)
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
            "join pipeline not used; the JOIN's output is materialized for "
            "the next operator: %s", col_join_batch_eligibility_name(reason));
    else
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
            "bounded join mode fell back to one-shot join: %s",
            col_join_batch_eligibility_name(reason));
}

void
col_join_batch_fold_worker(wl_col_session_t *coordinator,
    const wl_col_session_t *worker)
{
    uint32_t count;

    if (!coordinator || !worker)
        return;
    coordinator->join_batch_warned_reasons |= worker->join_batch_warned_reasons;
    count = worker->join_batch_fallback_count;
    if (count == 0)
        return;
    coordinator->join_batch_fallback_count
        = count > UINT32_MAX - coordinator->join_batch_fallback_count
        ? UINT32_MAX : coordinator->join_batch_fallback_count + count;
    coordinator->join_batch_last_reason = worker->join_batch_last_reason;
}

/* ---- producer ---------------------------------------------------------- */

typedef struct {
    wl_col_session_t *sess;
    const wl_plan_op_t *op;
    const col_rel_t *left;
    const col_rel_t *right;
    uint32_t *lk;
    uint32_t *rk;
    uint32_t kc;
    col_arrangement_pin_t pin;
    wl_columnar_source_access_reader_t runner_left_reader;
    wl_columnar_source_access_reader_t runner_right_reader;
    const col_arrangement_t *arr;
    /* The union keeps the raw allocation tail aligned for key_row on
     * compilers without C11 _Alignof support. */
    union {
        int64_t *key_row;
        int64_t alignment;
    } key_row_storage;
    col_rel_t *batch;        /* governed heap scratch, grown on demand */
    wl_columnar_memory_reservation_t descriptor_reservation;
    wl_columnar_memory_governor_ref_t *descriptor_governor;
    bool descriptor_committed;
    uint32_t rows_per_batch;
    bool grow_denied_logged;  /* one WARN per producer, not per batch */
    col_join_batch_cursor_t cursor;
    bool controlled_bound;
    bool controlled_busy;
    bool controlled_ready;
    bool controlled_complete;
    col_join_batch_control_status_t controlled_status;
    int controlled_execution_status;
    uint64_t controlled_work_ordinal;
    uint64_t controlled_required_work;
    uint64_t controlled_batch_generation;
    uint64_t controlled_attempt_id;
    wl_evaluation_control_t *controlled_control;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t controlled_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD controlled_thread;
#else
    pthread_t controlled_thread;
#endif
    bool controlled_thread_valid;
    uint32_t controlled_left_row;
    uint32_t controlled_right_row;
    uint32_t controlled_chain_row;
    uint32_t controlled_key_column;
    uint32_t controlled_batch_rows;
    col_join_batch_pos_t controlled_next;
    col_join_batch_pos_t controlled_ready_next;
    bool runner_mode;
    bool runner_internal;
    col_rel_t *runner_output;
    uint32_t runner_output_row_capacity;
    uint32_t runner_copy_base;
    uint32_t runner_copy_row;
    uint32_t runner_copy_col;
    uint32_t runner_copy_timestamp_bytes;
    const col_rel_t *runner_pending_batch;
    uint64_t runner_pending_generation;
    uint64_t runner_work_ordinal;
    uint64_t runner_required_work;
    uint64_t runner_observed_producer_ordinal;
    col_join_batch_control_status_t runner_status;
    int runner_execution_status;
    enum {
        COL_JOIN_BATCH_SCAN_LEFT = 0,
        COL_JOIN_BATCH_BUILD_KEY,
        COL_JOIN_BATCH_HASH,
        COL_JOIN_BATCH_MATCH,
        COL_JOIN_BATCH_WRITE,
        COL_JOIN_BATCH_ADVANCE
    } controlled_phase;
} col_join_batch_producer_t;

static uint64_t
pos_pack(uint32_t lr, uint32_t rr)
{
    return ((uint64_t)lr << 32) | (uint64_t)rr;
}

static void
pos_unpack(uint64_t position, col_join_batch_pos_t *pos)
{
    pos->lr = (uint32_t)(position >> 32);
    pos->rr = (uint32_t)(position & 0xffffffffu);
}

static bool
producer_validate(void *context,
    const wl_columnar_continuation_cursor_t *cursor)
{
    const col_join_batch_producer_t *p =
        (const col_join_batch_producer_t *)context;
    const col_join_batch_cursor_t *c;
    col_join_batch_pos_t pos;

    if (!p || !cursor || !p->left || !p->right || !p->arr)
        return false;
    c = &p->cursor;
    pos_unpack(cursor->position, &pos);
    /* Identity and generation of both inputs: any mutation of either side
     * (append, in-place set, compaction, view swap) bumps a generation and
     * makes the stored positions meaningless.  Poisoned generations never
     * match. */
    if (cursor->input_identity != p->left->relation_identity
        || cursor->arrangement_identity != p->right->relation_identity
        || c->left_identity != p->left->relation_identity
        || c->left_view_gen != p->left->view_generation
        || c->left_storage_gen != p->left->storage_generation
        || c->right_identity != p->right->relation_identity
        || c->right_view_gen != p->right->view_generation
        || c->right_storage_gen != p->right->storage_generation
        || c->left_view_gen == UINT64_MAX || c->right_view_gen == UINT64_MAX
        || c->left_storage_gen == UINT64_MAX
        || c->right_storage_gen == UINT64_MAX)
        return false;
    /* The arrangement is pinned and the registry defers any rebuild under
     * the lease, so a changed epoch or indexed extent means the index was
     * replaced outside this lease; both stay load-bearing. */
    if (cursor->generation != p->arr->generation
        || c->arr_generation != p->arr->generation
        || c->arr_indexed_rows != p->arr->indexed_rows)
        return false;
    /* An empty right relation legitimately has nothing indexed; any other
     * unindexed state means the arrangement was torn down under the lease. */
    if (p->right->nrows > 0
        && (p->arr->indexed_rows == 0 || !p->arr->ht_next))
        return false;
    if (pos.rr != UINT32_MAX && pos.rr >= p->arr->ht_cap)
        return false;
    if (pos.lr > p->left->nrows)
        return false;
    /* Provenance: a continuation created for a delta left must not be
     * resumed against a full left of the same identity, and vice versa. */
    if (c->timestamps != (p->left->timestamps != NULL))
        return false;
    return true;
}

static bool
controlled_thread_is_owner(const col_join_batch_producer_t *p)
{
    if (!p->controlled_thread_valid)
        return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(p->controlled_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return p->controlled_thread == GetCurrentThreadId();
#else
    return pthread_equal(p->controlled_thread, pthread_self()) != 0;
#endif
}

static col_join_batch_control_status_t
controlled_stop(col_join_batch_producer_t *p,
    col_join_batch_control_status_t status, int execution_status)
{
    if (p->controlled_status == COL_JOIN_BATCH_CONTROL_PROGRESS) {
        p->controlled_status = status;
        p->controlled_execution_status = execution_status;
    }
    return p->controlled_status;
}

/* Poll attempt identity and immutable source provenance before every saved
 * traversal phase. The byte/row work itself is charged separately, before
 * the corresponding hash, compare, or complete-row write. */
static col_join_batch_control_status_t
controlled_poll(col_join_batch_producer_t *p)
{
    if (p->controlled_status != COL_JOIN_BATCH_CONTROL_PROGRESS)
        return p->controlled_status;
    if (!p->arr || !p->pin.active)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_CANCELLED,
                   ECANCELED);
    if (wl_evaluation_control_active_attempt_id(p->controlled_control)
        != p->controlled_attempt_id)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT,
                   EINVAL);
    int rc = wl_evaluation_control_charge_attempt(p->controlled_control,
            p->controlled_attempt_id, 0);
    if (rc == EINVAL)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT,
                   EINVAL);
    if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_WORK_LIMIT, 0);
    if (rc == WL_EVALUATION_CONTROL_CANCELLED)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_CANCELLED, 0);
    if (rc != 0)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, rc);
    wl_columnar_continuation_cursor_t cursor = { 0 };
    cursor.input_identity = p->left->relation_identity;
    cursor.arrangement_identity = p->right->relation_identity;
    cursor.generation = p->arr->generation;
    cursor.position = pos_pack(p->controlled_left_row,
            p->controlled_right_row);
    cursor.sequence = p->cursor.sequence;
    if (!producer_validate(p, &cursor))
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_STALE_INPUT, EAGAIN);
    return COL_JOIN_BATCH_CONTROL_PROGRESS;
}

static col_join_batch_control_status_t
controlled_charge(col_join_batch_producer_t *p, uint64_t work,
    uint64_t *turn_work, uint64_t max_work)
{
    if (work > max_work - *turn_work)
        return COL_JOIN_BATCH_CONTROL_PROGRESS;
    if (work > UINT64_MAX - p->controlled_work_ordinal)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR,
                   EOVERFLOW);
    if (wl_evaluation_control_active_attempt_id(p->controlled_control)
        != p->controlled_attempt_id)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT,
                   EINVAL);
    int rc = wl_evaluation_control_charge_attempt(p->controlled_control,
            p->controlled_attempt_id, work);
    if (rc == EINVAL)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT,
                   EINVAL);
    if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_WORK_LIMIT, 0);
    if (rc == WL_EVALUATION_CONTROL_CANCELLED)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_CANCELLED, 0);
    if (rc != 0)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, rc);
    if (wl_evaluation_control_active_attempt_id(p->controlled_control)
        != p->controlled_attempt_id)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT,
                   EINVAL);
    p->controlled_work_ordinal += work;
    *turn_work += work;
    return COL_JOIN_BATCH_CONTROL_PROGRESS;
}

static col_join_batch_control_status_t
controlled_make_ready(col_join_batch_producer_t *p, bool complete)
{
    if (p->controlled_batch_generation == UINT64_MAX)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR,
                   EOVERFLOW);
    ++p->controlled_batch_generation;
    p->controlled_ready = true;
    p->controlled_complete = complete;
    p->controlled_ready_next = p->controlled_next;
    p->batch->nrows = p->controlled_batch_rows;
    return COL_JOIN_BATCH_CONTROL_BATCH_READY;
}

static col_join_batch_control_status_t
controlled_step(col_join_batch_producer_t *p, uint64_t max_work)
{
    uint64_t turn_work = 0;
    const uint32_t output_width = p->batch->ncols;
    const uint32_t batch_capacity = p->batch->capacity < p->rows_per_batch
        ? p->batch->capacity : p->rows_per_batch;

    if (p->controlled_status != COL_JOIN_BATCH_CONTROL_PROGRESS)
        return p->controlled_status;
    if (p->controlled_ready)
        return COL_JOIN_BATCH_CONTROL_BATCH_READY;
    if (p->controlled_complete)
        return COL_JOIN_BATCH_CONTROL_DONE;
    if (batch_capacity == 0)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR,
                   ENOTSUP);

    while (turn_work < max_work) {
        uint64_t cost = 1;
        switch (p->controlled_phase) {
        case COL_JOIN_BATCH_SCAN_LEFT:
            if (p->controlled_left_row >= p->left->nrows) {
                col_join_batch_control_status_t status = controlled_poll(p);
                if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
                    return status;
                if (p->controlled_batch_rows)
                    return controlled_make_ready(p, true);
                p->controlled_complete = true;
                return COL_JOIN_BATCH_CONTROL_DONE;
            }
            break;
        case COL_JOIN_BATCH_BUILD_KEY:
            break;
        case COL_JOIN_BATCH_HASH:
            /* One INT64 key: fixed setup plus its eight hash bytes. */
            cost = 9u;
            break;
        case COL_JOIN_BATCH_MATCH:
            cost = 2u; /* candidate and the single key lane */
            break;
        case COL_JOIN_BATCH_WRITE:
            cost = (uint64_t)output_width + 1u;
            break;
        case COL_JOIN_BATCH_ADVANCE:
            break;
        default:
            return controlled_stop(p,
                       COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EINVAL);
        }
        if (cost > max_work - turn_work) {
            p->controlled_required_work = cost;
            return COL_JOIN_BATCH_CONTROL_PROGRESS;
        }
        p->controlled_required_work = 0;
        col_join_batch_control_status_t status = controlled_poll(p);
        if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
            return status;
        status = controlled_charge(p, cost, &turn_work, max_work);
        if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
            return status;

        switch (p->controlled_phase) {
        case COL_JOIN_BATCH_SCAN_LEFT:
            p->controlled_right_row = UINT32_MAX;
            p->controlled_key_column = 0;
            p->controlled_next.lr = p->controlled_left_row;
            p->controlled_next.rr = UINT32_MAX;
            p->controlled_phase = COL_JOIN_BATCH_BUILD_KEY;
            break;
        case COL_JOIN_BATCH_BUILD_KEY:
            p->key_row_storage.key_row[p->rk[p->controlled_key_column]]
                = p->left->columns[p->lk[p->controlled_key_column]]
                [p->controlled_left_row];
            if (++p->controlled_key_column == p->kc)
                p->controlled_phase = COL_JOIN_BATCH_HASH;
            break;
        case COL_JOIN_BATCH_HASH:
            p->controlled_chain_row = col_arrangement_bucket_head_typed(
                p->arr, p->right, p->key_row_storage.key_row);
            p->controlled_right_row = p->controlled_chain_row;
            p->controlled_phase = p->controlled_right_row == UINT32_MAX
                ? COL_JOIN_BATCH_ADVANCE : COL_JOIN_BATCH_MATCH;
            break;
        case COL_JOIN_BATCH_MATCH:
            if (p->controlled_right_row >= p->right->nrows
                || p->controlled_right_row >= p->arr->ht_cap)
                return controlled_stop(p,
                           COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EINVAL);
            if (p->left->columns[p->lk[0]][p->controlled_left_row]
                == p->right->columns[p->rk[0]][p->controlled_right_row])
                p->controlled_phase = COL_JOIN_BATCH_WRITE;
            else
                p->controlled_phase = COL_JOIN_BATCH_ADVANCE;
            break;
        case COL_JOIN_BATCH_WRITE:
            if (col_join_write_pair_at(p->batch, p->controlled_batch_rows,
                p->left, p->controlled_left_row, p->right,
                p->controlled_right_row, p->op->project_indices,
                p->op->project_count) != 0)
                return controlled_stop(p,
                           COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EINVAL);
            if (p->batch->timestamps)
                memset(&p->batch->timestamps[p->controlled_batch_rows], 0,
                    sizeof(col_delta_timestamp_t));
            ++p->controlled_batch_rows;
            p->batch->nrows = p->controlled_batch_rows;
            p->controlled_phase = COL_JOIN_BATCH_ADVANCE;
            break;
        case COL_JOIN_BATCH_ADVANCE:
            if (p->controlled_right_row == UINT32_MAX) {
                ++p->controlled_left_row;
                p->controlled_phase = COL_JOIN_BATCH_SCAN_LEFT;
            } else {
                p->controlled_right_row = col_arrangement_find_next(
                    p->arr, p->controlled_right_row);
                p->controlled_next.lr = p->controlled_left_row;
                p->controlled_next.rr = p->controlled_right_row;
                p->controlled_phase = p->controlled_right_row == UINT32_MAX
                    ? COL_JOIN_BATCH_ADVANCE : COL_JOIN_BATCH_MATCH;
                if (p->controlled_right_row == UINT32_MAX) {
                    ++p->controlled_left_row;
                    p->controlled_phase = COL_JOIN_BATCH_SCAN_LEFT;
                }
            }
            p->controlled_next.lr = p->controlled_left_row;
            p->controlled_next.rr = p->controlled_right_row;
            if (p->controlled_batch_rows == batch_capacity)
                return controlled_make_ready(p,
                           p->controlled_left_row >= p->left->nrows);
            break;
        default:
            return controlled_stop(p,
                       COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EINVAL);
        }
    }
    return COL_JOIN_BATCH_CONTROL_PROGRESS;
}

/* Grow the scratch batch to hold row @n, doubling from whatever a fresh
 * relation pre-allocated and clamping to rows_per_batch.  Returns false when
 * the row cannot be written, which the caller turns into a short batch.
 *
 * col_rel_prepare_resize copies exactly nrows rows (and nrows timestamps),
 * so p->batch->nrows must equal n here.  A stale nrows copies too little,
 * frees the old columns, and leaves rows written so far pointing at
 * uninitialised malloc memory: a silently wrong join that neither ASan nor
 * UBSan reports, only an oracle comparison across a grow (#1481).
 * producer_produce publishes nrows after every written row (#1909), so no
 * reallocation site has a sync point to forget.
 */
static bool
grow_scratch(col_join_batch_producer_t *p, uint32_t n)
{
    uint32_t cap = p->batch->capacity;
    uint32_t want;
    bool denied = false;
    int rc;

    /* The caller only grows at n == capacity, and the n == rows_per_batch
     * park in the fill loop fires first otherwise, so cap < rows_per_batch
     * on entry and both arms exceed cap.  The postcondition below does not
     * rely on that, though: col_rel_reserve_capacity_admitted returns 0
     * without growing whenever want <= capacity, so reporting its status
     * would be reporting the call rather than the outcome. */
    want = cap < p->rows_per_batch / 2u ? cap * 2u : p->rows_per_batch;
    rc = col_rel_reserve_capacity_admitted(p->batch, want, &denied);
    if (rc == 0 && p->batch->capacity > n)
        return true;
    if (!p->grow_denied_logged) {
        p->grow_denied_logged = true;
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
            "join batch scratch stuck at %u rows (wanted %u, ceiling %u): "
            "%s; emitting short batches", cap, want, p->rows_per_batch,
            denied ? "admission denied"
                   : rc != 0 ? "allocation failed"
                             : "the reserve did not grow it");
    }
    return false;
}

static wl_columnar_continuation_status_t
producer_produce(void *context, const wl_columnar_continuation_cursor_t *cursor,
    wl_columnar_continuation_batch_t *batch)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)context;
    col_join_batch_pos_t pos;
    col_join_batch_pos_t next;
    uint32_t n = 0;
    bool complete = false;
    uint64_t bytes = 0;

    if (!p || !cursor || !batch || !p->batch)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    pos_unpack(cursor->position, &pos);
    if (pos.lr > p->left->nrows)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    p->batch->nrows = 0;
    next.lr = pos.lr;
    next.rr = pos.rr;
    while (next.lr < p->left->nrows) {
        uint32_t lr = next.lr;
        uint32_t rr = next.rr;
        if (rr == UINT32_MAX) {
            for (uint32_t k = 0; k < p->kc; k++)
                p->key_row_storage.key_row[p->rk[k]]
                    = p->left->columns[p->lk[k]][lr];
            rr = col_arrangement_find_first_typed(p->arr, p->right,
                    p->key_row_storage.key_row);
        }
        while (rr != UINT32_MAX) {
            /* Chains may hold collision rows; every candidate is checked. */
            if (col_join_keys_match_rel(p->left, lr, p->lk, p->right, rr,
                p->rk, p->kc)) {
                if (n == p->batch->capacity && !grow_scratch(p, n)) {
                    /* Park on THIS candidate, not the next one: it was
                    * never written, and the rr != UINT32_MAX resume path
                    * re-tests it.  Emitting what we have keeps the scan
                    * moving; refusing the batch would leave the cursor
                    * unchanged and livelock under a tight governor. */
                    next.lr = lr;
                    next.rr = rr;
                    goto emit;
                }
                if (col_join_write_pair_at(p->batch, n, p->left, lr,
                    p->right, rr, p->op->project_indices,
                    p->op->project_count) != 0)
                    return WL_COLUMNAR_CONTINUATION_INVALID;
                if (p->batch->timestamps)
                    memset(&p->batch->timestamps[n], 0,
                        sizeof(col_delta_timestamp_t));
                n++;
                /* Keep the live prefix structurally current, so every
                 * relocation preserves the row just written. */
                p->batch->nrows = n;
                if (n == p->rows_per_batch) {
                    /* Batch full: park on the first unexamined candidate.
                     * UINT32_MAX is stored only as "probe the next left
                     * row from its bucket head", never for an exhausted
                     * chain, so resuming cannot repeat this row's matches. */
                    uint32_t nrr = col_arrangement_find_next(p->arr, rr);
                    if (nrr != UINT32_MAX) {
                        next.lr = lr;
                        next.rr = nrr;
                    } else {
                        next.lr = lr + 1u;
                        next.rr = UINT32_MAX;
                    }
                    goto emit;
                }
            }
            rr = col_arrangement_find_next(p->arr, rr);
        }
        next.lr = lr + 1u;
        next.rr = UINT32_MAX;
    }
    /* Every left row examined: the scan is complete.  A batch that filled
     * exactly on the last match reports complete only on the following
     * produce, which arrives here with n == 0. */
    complete = true;
emit:
    p->batch->nrows = n;
    if (n == 0) {
        batch->payload = NULL;
        batch->bytes = 0;
        batch->rows = 0;
        batch->complete = true;
        batch->next_cursor = *cursor;
        return WL_COLUMNAR_CONTINUATION_OK;
    }
    if (!col_rel_retained_bytes_for(p->batch, n, &bytes))
        return WL_COLUMNAR_CONTINUATION_INVALID;
    batch->payload = p->batch;
    batch->bytes = bytes;
    batch->rows = n;
    batch->complete = complete;
    batch->next_cursor = *cursor;
    batch->next_cursor.position = pos_pack(next.lr, next.rr);
    batch->next_cursor.sequence = cursor->sequence + 1u;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static void
producer_release_lease(col_join_batch_producer_t *p)
{
    if (p->pin.active)
        col_arrangement_pin_release(&p->pin);
    if (p->runner_left_reader.owner)
        if (col_rel_source_reader_release(&p->runner_left_reader) != 0)
            abort();
    if (p->runner_right_reader.owner)
        if (col_rel_source_reader_release(&p->runner_right_reader) != 0)
            abort();
    p->arr = NULL;
}

static void
producer_cancel(void *context)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)context;

    if (!p)
        return;
    if (p->controlled_bound)
        controlled_stop(p, COL_JOIN_BATCH_CONTROL_CANCELLED, ECANCELED);
    producer_release_lease(p);
}

static void
producer_destroy(void *context)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)context;
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_evaluation_control_t *control;
    bool committed;

    if (!p)
        return;
    wl_columnar_memory_reservation_init(&reservation);
    if (p->descriptor_reservation.governor
        && !wl_columnar_memory_reservation_move(&reservation,
        &p->descriptor_reservation))
        abort();
    governor = p->descriptor_governor;
    control = p->controlled_control;
    committed = p->descriptor_committed;
    producer_release_lease(p);
    if (p->batch)
        col_rel_destroy(p->batch); /* releases the scratch token and ref */
    if (p->runner_output)
        col_rel_destroy(p->runner_output);
    free(p);
    if (reservation.governor) {
        if (committed){
            if (!wl_columnar_memory_release(&reservation)) abort();
        }else{
            if (!wl_columnar_memory_rollback(&reservation)) abort();
        }
    }
    wl_columnar_memory_governor_ref_release(governor);
    wl_evaluation_control_release(control);
}

static int
col_join_batch_producer_create_internal(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const uint32_t *lk, const uint32_t *rk, uint32_t kc,
    uint64_t batch_bytes, bool runner, wl_columnar_continuation_t **out)
{
    col_join_batch_producer_t *p;
    const col_rel_t *right;
    wl_columnar_continuation_producer_t producer = { 0 };
    wl_columnar_continuation_cursor_t initial;
    uint64_t row_bytes = 0;
    uint64_t descriptor_bytes = 0;
    uint64_t key_bytes, row_storage_bytes;
    uint64_t rows;
    uint32_t ocols;
    wl_columnar_memory_governor_ref_t *descriptor_governor = NULL;
    wl_columnar_memory_reservation_t descriptor_reservation;
    wl_columnar_source_access_reader_t left_reader = { 0 };
    wl_columnar_source_access_reader_t right_reader = { 0 };
    col_arrangement_pin_t existing_pin = { 0 };
    int rc;

    if (out)
        *out = NULL;
    if (!sess || !op || !left || !lk || !rk || kc == 0 || !out
        || !op->right_relation || batch_bytes == 0)
        return EINVAL;
    right = session_find_rel(sess, op->right_relation);
    if (!right)
        return ENOENT;
    if (runner) {
        rc = col_rel_source_reader_acquire(left, &left_reader);
        if (rc)
            return rc;
        rc = col_session_pin_existing_arrangement(&sess->base, right, rk, kc,
                &existing_pin, &right_reader);
        if (rc)
            goto fail_early;
    }
    for (uint32_t k = 0; k < kc; k++)
        if (lk[k] >= left->ncols || rk[k] >= right->ncols) {
            rc = EINVAL;
            goto fail_early;
        }
    if (!wl_columnar_memory_size_mul(kc, 2u * sizeof(uint32_t),
        &key_bytes)
        || !wl_columnar_memory_size_mul(
            right->ncols > 0 ? right->ncols : 1u, sizeof(int64_t),
            &row_storage_bytes)
        || !wl_columnar_memory_size_add(sizeof(*p), key_bytes,
        &descriptor_bytes)
        || !wl_columnar_memory_size_add(descriptor_bytes,
        row_storage_bytes, &descriptor_bytes)
        || descriptor_bytes > SIZE_MAX) {
        rc = EOVERFLOW;
        goto fail_early;
    }
    descriptor_governor = sess->memory_governor
        ? sess->memory_governor
        : left->memory_governor ? left->memory_governor
                                : right->memory_governor;
    wl_columnar_memory_governor_ref_retain(descriptor_governor);
    wl_columnar_memory_reservation_init(&descriptor_reservation);
    if (descriptor_governor) {
        wl_columnar_memory_admission_status_t admission
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(descriptor_governor),
                descriptor_bytes, &descriptor_reservation);
        if (admission > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (admission == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            wl_columnar_memory_governor_ref_release(descriptor_governor);
            rc = admission == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC
                : admission == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
            goto fail_early;
        }
    }
    p = (col_join_batch_producer_t *)calloc(1, (size_t)descriptor_bytes);
    if (!p) {
        if (descriptor_reservation.governor){
            if (!wl_columnar_memory_rollback(&descriptor_reservation)) abort();
        }
        wl_columnar_memory_governor_ref_release(descriptor_governor);
        rc = ENOMEM;
        goto fail_early;
    }
    wl_columnar_memory_reservation_init(&p->descriptor_reservation);
    p->descriptor_governor = descriptor_governor;
    if (descriptor_reservation.governor
        && !wl_columnar_memory_reservation_move(&p->descriptor_reservation,
        &descriptor_reservation)) {
        free(p);
        if (!wl_columnar_memory_rollback(&descriptor_reservation))
            abort();
        wl_columnar_memory_governor_ref_release(descriptor_governor);
        rc = EINVAL;
        goto fail_early;
    }
    p->sess = sess;
    p->op = op;
    p->left = left;
    p->right = right;
    p->kc = kc;
    if (runner) {
        p->pin = existing_pin;
        memset(&existing_pin, 0, sizeof(existing_pin));
        /* Source-reader tokens are address-bound and cannot be copied.  Take
         * leases directly in their final storage before dropping the
         * temporary admission leases. */
        rc = col_rel_source_reader_acquire(left, &p->runner_left_reader);
        if (rc)
            goto fail;
        rc = col_rel_source_reader_acquire(right, &p->runner_right_reader);
        if (rc)
            goto fail;
        int left_release_rc = col_rel_source_reader_release(&left_reader);
        int right_release_rc = col_rel_source_reader_release(&right_reader);
        if (left_release_rc != 0 || right_release_rc != 0)
            abort();
        p->arr = p->pin.arr;
    }
    /* The allocation tail is raw storage.  Its first two regions hold the
     * uint32_t key-column arrays; the final int64_t region is naturally
     * aligned because its offset is 2 * kc * sizeof(uint32_t). */
    unsigned char *scratch = (unsigned char *)(p + 1);
    p->lk = (uint32_t *)scratch;
    p->rk = p->lk + kc;
    p->key_row_storage.key_row = (int64_t *)(scratch
        + 2u * (size_t)kc * sizeof(uint32_t));
    memcpy(p->lk, lk, (size_t)kc * sizeof(uint32_t));
    memcpy(p->rk, rk, (size_t)kc * sizeof(uint32_t));
    if (p->descriptor_reservation.governor) {
        if (!wl_columnar_memory_commit(&p->descriptor_reservation, p)) {
            rc = EINVAL;
            goto fail;
        }
        p->descriptor_committed = true;
    }

    /* One lease for the whole continuation.  The registry refuses to
     * relocate entries while any lease is active, so the arrangement
     * pointer stays valid until destroy or cancel. */
    if (!runner) {
        rc = col_session_pin_arrangement(&sess->base, op->right_relation,
                rk, kc, &p->pin);
        if (rc != 0 || !p->pin.arr) {
            if (rc == 0)
                rc = ENOENT;
            goto fail;
        }
        p->arr = p->pin.arr;
    }
    if (p->arr->indexed_rows == 0 && right->nrows > 0) {
        rc = ENOENT;
        goto fail;
    }

    /* Scratch batch relation: same shape as the output, admitted before
     * allocation under the producer's effective governor. */
    ocols = col_join_output_width(left, right, op);
    bool timestamped = left->timestamps != NULL;
    if (p->descriptor_governor)
        rc = wl_columnar_relation_new_auto_governed("$join_batch", ocols,
                COL_REL_INIT_CAP, timestamped, p->descriptor_governor,
                &p->batch);
    else {
        p->batch = col_rel_new_auto("$join_batch", ocols);
        rc = p->batch ? 0 : ENOMEM;
    }
    if (rc != 0) {
        rc = wl_columnar_join_budget_denial(sess, NULL, rc);
        goto fail;
    }
    rc = wl_columnar_join_budget_denial(sess, p->batch,
            col_join_set_output_types(p->batch, left, right, op));
    if (rc != 0)
        goto fail;
    if (timestamped && !p->batch->timestamps) {
        p->batch->memory_budget_denial_pending = false;
        rc = col_rel_enable_timestamps(p->batch);
        if (rc != 0) {
            if (rc == ENOMEM && p->batch->memory_budget_denial_pending)
                rc = ENOSPC;
            goto fail;
        }
    }
    if (!col_rel_retained_bytes_for(p->batch, 1u, &row_bytes)) {
        rc = EINVAL;
        goto fail;
    }
    /* A zero-width output (both sides nullary) has no bytes to bound; the
     * one-shot join handles it, so report it like a sub-row budget. */
    if (row_bytes == 0) {
        rc = ENOTSUP;
        goto fail;
    }
    rows = batch_bytes / row_bytes;
    if (rows == 0) {
        rc = ENOTSUP;
        goto fail;
    }
    if (rows > UINT32_MAX)
        rows = UINT32_MAX;
    p->rows_per_batch = (uint32_t)rows;
    /* #1481: the scratch starts at COL_REL_INIT_CAP rows and doubles on
     * demand in producer_produce, so a small join reserves what it uses
     * rather than the full WIRELOG_JOIN_BATCH_BYTES allowance. */

    p->cursor.next.lr = 0;
    p->cursor.next.rr = UINT32_MAX;
    p->cursor.sequence = 0;
    p->cursor.left_identity = left->relation_identity;
    p->cursor.left_view_gen = left->view_generation;
    p->cursor.left_storage_gen = left->storage_generation;
    p->cursor.right_identity = right->relation_identity;
    p->cursor.right_view_gen = right->view_generation;
    p->cursor.right_storage_gen = right->storage_generation;
    p->cursor.arr_generation = p->arr->generation;
    p->cursor.arr_indexed_rows = p->arr->indexed_rows;
    p->cursor.left_is_delta = left_is_delta;
    p->cursor.timestamps = left->timestamps != NULL;

    memset(&initial, 0, sizeof(initial));
    initial.input_identity = left->relation_identity;
    initial.arrangement_identity = right->relation_identity;
    initial.generation = p->arr->generation;
    initial.position = pos_pack(0, UINT32_MAX);
    initial.sequence = 0;

    producer.context = p;
    producer.produce = producer_produce;
    producer.validate = producer_validate;
    producer.destroy = producer_destroy;
    producer.cancel = producer_cancel;
    *out = wl_columnar_continuation_create(&producer, &initial);
    if (!*out) {
        rc = ENOMEM;
        goto fail;
    }
    return 0;

fail:
    producer_destroy(p);
    if (existing_pin.active)
        col_arrangement_pin_release(&existing_pin);
    if (left_reader.owner)
        (void)col_rel_source_reader_release(&left_reader);
    if (right_reader.owner)
        (void)col_rel_source_reader_release(&right_reader);
    return rc;

fail_early:
    if (existing_pin.active)
        col_arrangement_pin_release(&existing_pin);
    if (left_reader.owner)
        (void)col_rel_source_reader_release(&left_reader);
    if (right_reader.owner)
        (void)col_rel_source_reader_release(&right_reader);
    return rc;
}

int
col_join_batch_producer_create(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const uint32_t *lk, const uint32_t *rk, uint32_t kc,
    uint64_t batch_bytes, wl_columnar_continuation_t **out)
{
    return col_join_batch_producer_create_internal(sess, op, left,
               left_is_delta, lk, rk, kc, batch_bytes, false, out);
}

static const col_join_batch_producer_t *
producer_of(const wl_columnar_continuation_t *cont)
{
    const col_join_batch_producer_t *p = (const col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(cont);
    /* Only continuations created here carry this producer vtable. */
    return p && p->batch && p->left ? p : NULL;
}

uint32_t
col_join_batch_rows_per_batch(const wl_columnar_continuation_t *cont)
{
    const col_join_batch_producer_t *p = producer_of(cont);
    return p ? p->rows_per_batch : 0;
}

uint32_t
col_join_batch_scratch_capacity(const wl_columnar_continuation_t *cont)
{
    const col_join_batch_producer_t *p = producer_of(cont);
    return p ? p->batch->capacity : 0;
}

uint64_t
col_join_batch_descriptor_bytes(const wl_columnar_continuation_t *cont)
{
    const col_join_batch_producer_t *p = producer_of(cont);
    return p && p->descriptor_reservation.governor
           ? p->descriptor_reservation.bytes : 0;
}

bool
col_join_batch_cursor_get(const wl_columnar_continuation_t *cont,
    col_join_batch_cursor_t *out)
{
    const col_join_batch_producer_t *p = producer_of(cont);
    const wl_columnar_continuation_cursor_t *c;

    if (!p || !out)
        return false;
    *out = p->cursor;
    if (p->controlled_bound)
        /* The acknowledged cursor is producer-local. A ready batch remains
         * speculative until its generation is acknowledged. */
        return true;
    c = wl_columnar_continuation_cursor(cont);
    if (c) {
        pos_unpack(c->position, &out->next);
        out->sequence = c->sequence;
    }
    return true;
}

int
col_join_batch_control_bind(wl_columnar_continuation_t *cont,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(cont);

    if (!p || !p->batch || !control || !attempt_id)
        return EINVAL;
    /* The controlled tariff admits one native INT64 key (eight canonical
     * hash bytes) and no more than 64 output lanes. */
    if (p->kc != 1 || p->batch->ncols == 0 || p->batch->ncols > 64
        || (p->left->column_types
        && p->left->column_types[p->lk[0]] != WIRELOG_TYPE_INT64)
        || (p->right->column_types
        && p->right->column_types[p->rk[0]] != WIRELOG_TYPE_INT64))
        return ENOTSUP;
    if (p->controlled_bound || p->cursor.sequence != 0
        || p->controlled_ready || p->controlled_batch_rows != 0)
        return EBUSY;
    if (wl_evaluation_control_active_attempt_id(control) != attempt_id)
        return EINVAL;
    int rc = wl_evaluation_control_retain(control);
    if (rc)
        return rc;
    if (!wl_columnar_continuation_claim_producer(cont, p)) {
        wl_evaluation_control_release(control);
        return EBUSY;
    }
    p->controlled_control = control;
    p->controlled_attempt_id = attempt_id;
    p->controlled_left_row = p->cursor.next.lr;
    p->controlled_right_row = p->cursor.next.rr;
    p->controlled_right_row = p->cursor.next.rr;
    p->controlled_next = p->cursor.next;
    p->controlled_phase = COL_JOIN_BATCH_SCAN_LEFT;
    p->controlled_status = COL_JOIN_BATCH_CONTROL_PROGRESS;
#if defined(WL_HAVE_C11_THREADS)
    p->controlled_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    p->controlled_thread = GetCurrentThreadId();
#else
    p->controlled_thread = pthread_self();
#endif
    p->controlled_thread_valid = true;
    p->controlled_bound = true;
    return 0;
}

col_join_batch_control_status_t
col_join_batch_control_step(wl_columnar_continuation_t *cont,
    uint64_t max_work, col_join_batch_control_result_t *result)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(cont);

    if (!p || !p->controlled_bound || !result || !max_work) {
        if (p && p->controlled_bound && result && !max_work
            && controlled_thread_is_owner(p) && !p->controlled_busy)
            controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR,
                EINVAL);
        if (result) {
            result->status = p && p->controlled_bound
                ? p->controlled_status
                : COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR;
            result->execution_status = p && p->controlled_bound
                ? p->controlled_execution_status : EINVAL;
            result->work_ordinal = p ? p->controlled_work_ordinal : 0;
            result->batch_generation = 0;
            result->required_work = p ? p->controlled_required_work : 0;
        }
        return COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR;
    }
    if (!controlled_thread_is_owner(p) || p->controlled_busy
        || (p->runner_mode && !p->runner_internal)) {
        result->status = COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR;
        result->execution_status = EBUSY;
        result->work_ordinal = p->controlled_work_ordinal;
        result->batch_generation = p->controlled_ready
            ? p->controlled_batch_generation : 0;
        result->required_work = 0;
        return result->status;
    }
    p->controlled_busy = true;
    col_join_batch_control_status_t status = controlled_step(p, max_work);
    p->controlled_busy = false;
    result->status = status;
    result->execution_status = p->controlled_execution_status;
    result->work_ordinal = p->controlled_work_ordinal;
    result->batch_generation = status == COL_JOIN_BATCH_CONTROL_BATCH_READY
        ? p->controlled_batch_generation : 0;
    result->required_work = p->controlled_required_work;
    return status;
}

int
col_join_batch_control_peek(wl_columnar_continuation_t *cont,
    const col_rel_t **batch, uint64_t *generation)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(cont);

    if (!p || !p->controlled_bound || !batch || !generation)
        return EINVAL;
    if (!controlled_thread_is_owner(p) || p->controlled_busy
        || (p->runner_mode && !p->runner_internal))
        return EBUSY;
    col_join_batch_control_status_t status = controlled_poll(p);
    if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
        return status == COL_JOIN_BATCH_CONTROL_WORK_LIMIT ? EAGAIN
            : status == COL_JOIN_BATCH_CONTROL_CANCELLED ? ECANCELED : EINVAL;
    if (p->controlled_status != COL_JOIN_BATCH_CONTROL_PROGRESS)
        return p->controlled_status == COL_JOIN_BATCH_CONTROL_WORK_LIMIT
            ? EAGAIN : p->controlled_status == COL_JOIN_BATCH_CONTROL_CANCELLED
            ? ECANCELED : EINVAL;
    if (!p->controlled_ready)
        return EAGAIN;
    *batch = p->batch;
    *generation = p->controlled_batch_generation;
    return 0;
}

static void col_join_batch_control_ack_validated(
    wl_columnar_continuation_t *cont, uint64_t generation);

int
col_join_batch_control_ack(wl_columnar_continuation_t *cont,
    uint64_t generation)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(cont);

    if (!p || !p->controlled_bound || !generation)
        return EINVAL;
    if (!controlled_thread_is_owner(p) || p->controlled_busy
        || (p->runner_mode && !p->runner_internal))
        return EBUSY;
    /* A bad token is a caller error and must not latch a concurrent stop. */
    if (!p->controlled_ready || generation != p->controlled_batch_generation)
        return EINVAL;
    col_join_batch_control_status_t status = controlled_poll(p);
    if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
        return status == COL_JOIN_BATCH_CONTROL_WORK_LIMIT ? EAGAIN
            : status == COL_JOIN_BATCH_CONTROL_CANCELLED ? ECANCELED : EINVAL;
    if (p->cursor.sequence == UINT64_MAX)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR,
                   EOVERFLOW), EOVERFLOW;
    col_join_batch_control_ack_validated(cont, generation);
    return 0;
}

int
col_join_batch_control_precommit(wl_columnar_continuation_t *cont,
    uint64_t generation)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(cont);

    if (!p || !p->controlled_bound || !generation)
        return EINVAL;
    if (!controlled_thread_is_owner(p) || p->controlled_busy
        || (p->runner_mode && !p->runner_internal))
        return EBUSY;
    if (!p->controlled_ready || generation != p->controlled_batch_generation)
        return EINVAL;
    col_join_batch_control_status_t status = controlled_poll(p);
    if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
        return status == COL_JOIN_BATCH_CONTROL_WORK_LIMIT ? EAGAIN
            : status == COL_JOIN_BATCH_CONTROL_CANCELLED ? ECANCELED
            : p->controlled_execution_status ? p->controlled_execution_status
            : EINVAL;
    if (p->cursor.sequence == UINT64_MAX)
        return controlled_stop(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR,
                   EOVERFLOW), EOVERFLOW;
    return 0;
}

static void
col_join_batch_control_ack_validated(wl_columnar_continuation_t *cont,
    uint64_t generation)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(cont);

    /* The preceding precommit validation plus same-thread runner ownership
     * makes this a callback-free, no-fail state update. */
    if (!p || !p->controlled_bound || !controlled_thread_is_owner(p)
        || (p->runner_mode && !p->runner_internal)
        || !p->controlled_ready || !generation
        || generation != p->controlled_batch_generation
        || p->cursor.sequence == UINT64_MAX)
        abort();
    bool complete = p->controlled_complete;
    p->cursor.next = p->controlled_ready_next;
    ++p->cursor.sequence;
    p->controlled_batch_rows = 0;
    p->batch->nrows = 0;
    p->controlled_ready = false;
    if (complete) {
        p->controlled_status = COL_JOIN_BATCH_CONTROL_DONE;
    }
}

int
col_join_batch_control_discard(wl_columnar_continuation_t **cont)
{
    if (!cont || !*cont)
        return EINVAL;
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(*cont);
    if (!p || !p->controlled_bound || !controlled_thread_is_owner(p)
        || (p->runner_mode && !p->runner_internal)
        || p->controlled_busy)
        return EBUSY;
    wl_columnar_continuation_destroy(*cont);
    *cont = NULL;
    return 0;
}

static col_join_batch_control_status_t
runner_fail(col_join_batch_producer_t *p,
    col_join_batch_control_status_t status, int execution_status)
{
    if (p->runner_status == COL_JOIN_BATCH_CONTROL_PROGRESS) {
        p->runner_status = status;
        p->runner_execution_status = execution_status;
    }
    return p->runner_status;
}

static void
runner_result(const col_join_batch_producer_t *p,
    col_join_batch_control_result_t *result)
{
    result->status = p->runner_status;
    result->execution_status = p->runner_execution_status;
    result->work_ordinal = p->runner_work_ordinal;
    result->batch_generation = p->runner_pending_batch
        ? p->runner_pending_generation : 0;
    result->required_work = p->runner_required_work;
}

static col_join_batch_control_status_t
runner_validate_transfer(col_join_batch_producer_t *p, int *error)
{
    if (error)
        *error = 0;
    if (!p->pin.active || !p->runner_left_reader.owner
        || !p->runner_right_reader.owner) {
        if (error)
            *error = EAGAIN;
        return COL_JOIN_BATCH_CONTROL_STALE_INPUT;
    }
    if (wl_evaluation_control_active_attempt_id(p->controlled_control)
        != p->controlled_attempt_id) {
        if (error)
            *error = EINVAL;
        return COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT;
    }
    int rc = wl_evaluation_control_charge_attempt(p->controlled_control,
            p->controlled_attempt_id, 0);
    if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT)
        return COL_JOIN_BATCH_CONTROL_WORK_LIMIT;
    if (rc == WL_EVALUATION_CONTROL_CANCELLED)
        return COL_JOIN_BATCH_CONTROL_CANCELLED;
    if (rc != 0) {
        if (error)
            *error = rc;
        return rc == EINVAL ? COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT
            : COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR;
    }

    wl_columnar_continuation_cursor_t cursor = { 0 };
    cursor.input_identity = p->left->relation_identity;
    cursor.arrangement_identity = p->right->relation_identity;
    cursor.generation = p->arr->generation;
    cursor.position = pos_pack(p->controlled_left_row,
            p->controlled_right_row);
    cursor.sequence = p->cursor.sequence;
    if (!producer_validate(p, &cursor)) {
        if (error)
            *error = EAGAIN;
        return COL_JOIN_BATCH_CONTROL_STALE_INPUT;
    }
    return COL_JOIN_BATCH_CONTROL_PROGRESS;
}

int
col_join_batch_runner_start(wl_col_session_t *sess, const wl_plan_op_t *op,
    const col_rel_t *left, bool left_is_delta, const uint32_t *lk,
    const uint32_t *rk, uint32_t key_count, uint64_t batch_bytes,
    uint32_t output_row_capacity, wl_evaluation_control_t *control,
    uint64_t attempt_id, wl_columnar_continuation_t **out)
{
    if (out)
        *out = NULL;
    if (!out || !control || !attempt_id)
        return EINVAL;
    int rc = col_join_batch_producer_create_internal(sess, op, left,
            left_is_delta, lk, rk, key_count, batch_bytes, true, out);
    if (rc)
        return rc;
    rc = col_join_batch_control_bind(*out, control, attempt_id);
    if (rc)
        goto failed;

    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(*out);
    bool denied = false;
    rc = wl_columnar_relation_new_like_governed_checked_mode_attempt_controlled(
        "$join_controlled_output", p->batch, p->descriptor_governor, true,
        &p->runner_output, control, attempt_id);
    if (rc)
        goto failed;
    if (output_row_capacity > p->runner_output->capacity) {
        rc = wl_columnar_relation_reserve_capacity_admitted_controlled(
            p->runner_output, output_row_capacity, &denied, control);
        if (rc) {
            if (rc == ENOMEM && denied)
                rc = ENOSPC;
            goto failed;
        }
    }
    if (p->runner_output->capacity < output_row_capacity
        || ((p->runner_output->timestamps != NULL)
        != (left->timestamps != NULL))) {
        rc = ENOTSUP;
        goto failed;
    }
    p->runner_mode = true;
    p->runner_output_row_capacity = output_row_capacity;
    p->runner_status = COL_JOIN_BATCH_CONTROL_PROGRESS;
    return 0;

failed:
    wl_columnar_continuation_destroy(*out);
    *out = NULL;
    return rc;
}

static col_join_batch_control_status_t
runner_charge(wl_columnar_continuation_t *cont,
    col_join_batch_producer_t *p, uint64_t work)
{
    if (work > UINT64_MAX - p->runner_work_ordinal)
        return runner_fail(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR,
                   EOVERFLOW);
    int rc = wl_evaluation_control_charge_attempt(p->controlled_control,
            p->controlled_attempt_id, work);
    if (rc == EINVAL)
        return runner_fail(p, COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT, EINVAL);
    if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT)
        return runner_fail(p, COL_JOIN_BATCH_CONTROL_WORK_LIMIT, 0);
    if (rc == WL_EVALUATION_CONTROL_CANCELLED)
        return runner_fail(p, COL_JOIN_BATCH_CONTROL_CANCELLED, 0);
    if (rc != 0)
        return runner_fail(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, rc);
    if (wl_evaluation_control_active_attempt_id(p->controlled_control)
        != p->controlled_attempt_id)
        return runner_fail(p, COL_JOIN_BATCH_CONTROL_STALE_ATTEMPT, EINVAL);
    p->runner_work_ordinal += work;
    (void)cont;
    return COL_JOIN_BATCH_CONTROL_PROGRESS;
}

col_join_batch_control_status_t
col_join_batch_runner_advance(wl_columnar_continuation_t **runner,
    uint64_t max_work, col_join_batch_control_result_t *result,
    col_rel_t **completed_output)
{
    if (completed_output)
        *completed_output = NULL;
    if (!runner || !*runner || !result || !completed_output
        || max_work > WL_EVALUATION_CONTROL_QUANTUM) {
        if (result) {
            result->status = COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR;
            result->execution_status = EINVAL;
            result->work_ordinal = 0;
            result->batch_generation = 0;
            result->required_work = 0;
        }
        return COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR;
    }
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(*runner);
    if (!p || !p->runner_mode || !p->controlled_bound
        || !controlled_thread_is_owner(p)) {
        result->status = COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR;
        result->execution_status = EINVAL;
        result->work_ordinal = 0;
        result->batch_generation = 0;
        result->required_work = 0;
        return result->status;
    }
    if (max_work == 0) {
        runner_result(p, result);
        return p->runner_status;
    }
    if (p->runner_status != COL_JOIN_BATCH_CONTROL_PROGRESS) {
        runner_result(p, result);
        return p->runner_status;
    }
    if (p->controlled_busy) {
        runner_fail(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EBUSY);
        runner_result(p, result);
        return p->runner_status;
    }

    p->runner_required_work = 0;

    uint64_t remaining = max_work;
    if (!p->runner_pending_batch) {
        uint64_t before = p->controlled_work_ordinal;
        col_join_batch_control_result_t producer_result;
        p->runner_internal = true;
        col_join_batch_control_status_t status = col_join_batch_control_step(
            *runner, remaining, &producer_result);
        p->runner_internal = false;
        uint64_t used = producer_result.work_ordinal - before;
        if (used > remaining) {
            runner_fail(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EOVERFLOW);
            runner_result(p, result);
            return p->runner_status;
        }
        if (used > UINT64_MAX - p->runner_work_ordinal) {
            runner_fail(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EOVERFLOW);
            runner_result(p, result);
            return p->runner_status;
        }
        p->runner_work_ordinal += used;
        p->runner_observed_producer_ordinal = producer_result.work_ordinal;
        remaining -= used;
        if (status == COL_JOIN_BATCH_CONTROL_DONE) {
            int final_error = 0;
            col_join_batch_control_status_t final_status
                = runner_validate_transfer(p, &final_error);
            if (final_status != COL_JOIN_BATCH_CONTROL_PROGRESS) {
                runner_fail(p, final_status, final_error);
                runner_result(p, result);
                return p->runner_status;
            }
            uint64_t completed_work = p->runner_work_ordinal;
            *completed_output = p->runner_output;
            p->runner_output = NULL;
            wl_columnar_continuation_destroy(*runner);
            *runner = NULL;
            result->status = COL_JOIN_BATCH_CONTROL_DONE;
            result->execution_status = 0;
            result->work_ordinal = completed_work;
            result->batch_generation = 0;
            result->required_work = 0;
            return result->status;
        }
        if (status != COL_JOIN_BATCH_CONTROL_PROGRESS
            && status != COL_JOIN_BATCH_CONTROL_BATCH_READY) {
            runner_fail(p, status, producer_result.execution_status);
            runner_result(p, result);
            return p->runner_status;
        }
        if (status == COL_JOIN_BATCH_CONTROL_PROGRESS) {
            p->runner_required_work = producer_result.required_work;
            runner_result(p, result);
            return COL_JOIN_BATCH_CONTROL_PROGRESS;
        }
        /* STEP returned BATCH_READY on this owner thread. Capture its stable
         * descriptor directly; calling peek here would add a second poll and
         * could strand a ready batch at the exact work limit. */
        p->runner_pending_batch = p->batch;
        p->runner_pending_generation = p->controlled_batch_generation;
        if (!p->runner_pending_generation || !p->controlled_ready) {
            runner_fail(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, EINVAL);
            runner_result(p, result);
            return p->runner_status;
        }
        p->runner_copy_base = p->runner_output->nrows;
        p->runner_copy_row = 0;
        p->runner_copy_col = 0;
        p->runner_copy_timestamp_bytes = 0;
        if (p->runner_pending_batch->nrows
            > p->runner_output_row_capacity - p->runner_copy_base) {
            runner_fail(p, COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR, ENOSPC);
            runner_result(p, result);
            return p->runner_status;
        }
    }

    const col_rel_t *batch = p->runner_pending_batch;
    col_rel_t *output = p->runner_output;
    while (p->runner_copy_row < batch->nrows) {
        while (p->runner_copy_col < batch->ncols) {
            if (!remaining) {
                p->runner_required_work = 1u;
                goto progress;
            }
            col_join_batch_control_status_t status = runner_charge(*runner,
                    p, 1u);
            if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
                goto stopped;
            output->columns[p->runner_copy_col]
            [p->runner_copy_base + p->runner_copy_row]
                = batch->columns[p->runner_copy_col][p->runner_copy_row];
            --remaining;
            ++p->runner_copy_col;
        }
        if (batch->timestamps) {
            const uint32_t bytes = (uint32_t)sizeof(*batch->timestamps);
            if (p->runner_copy_timestamp_bytes < bytes) {
                uint32_t left = bytes - p->runner_copy_timestamp_bytes;
                if (left > remaining) {
                    p->runner_required_work = left;
                    goto progress;
                }
                col_join_batch_control_status_t status = runner_charge(
                    *runner, p, left);
                if (status != COL_JOIN_BATCH_CONTROL_PROGRESS)
                    goto stopped;
                memcpy(&output->timestamps[p->runner_copy_base
                    + p->runner_copy_row],
                    &batch->timestamps[p->runner_copy_row],
                    bytes);
                remaining -= left;
                p->runner_copy_timestamp_bytes = bytes;
            }
        }
        ++p->runner_copy_row;
        p->runner_copy_col = 0;
        p->runner_copy_timestamp_bytes = 0;
    }

    {
        if (!remaining) {
            p->runner_required_work = 1u;
            goto progress;
        }
        col_join_batch_control_status_t charged = runner_charge(*runner, p,
                1u);
        if (charged != COL_JOIN_BATCH_CONTROL_PROGRESS)
            goto stopped;
        --remaining;
        p->runner_internal = true;
        int rc = col_join_batch_control_precommit(*runner,
                p->runner_pending_generation);
        p->runner_internal = false;
        if (rc) {
            runner_fail(p, p->controlled_status,
                p->controlled_execution_status ? p->controlled_execution_status
                : rc);
            goto stopped;
        }
        /* Both operations are prevalidated, same-thread, and non-failing.
         * The output relation is still private until DONE transfers ownership. */
        output->nrows = p->runner_copy_base + batch->nrows;
        wl_columnar_relation_touch_view(output);
        p->runner_internal = true;
        col_join_batch_control_ack_validated(*runner,
            p->runner_pending_generation);
        p->runner_internal = false;
        p->runner_pending_batch = NULL;
        p->runner_pending_generation = 0;
    }
progress:
    runner_result(p, result);
    return COL_JOIN_BATCH_CONTROL_PROGRESS;
stopped:
    runner_result(p, result);
    return p->runner_status;
}

int
col_join_batch_runner_discard(wl_columnar_continuation_t **runner)
{
    if (!runner || !*runner)
        return EINVAL;
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)
        wl_columnar_continuation_producer_context(*runner);
    if (!p || !p->runner_mode || !controlled_thread_is_owner(p)
        || p->controlled_busy)
        return EBUSY;
    wl_columnar_continuation_destroy(*runner);
    *runner = NULL;
    return 0;
}

/* ---- relation sink ----------------------------------------------------- */

wl_columnar_continuation_status_t
col_join_batch_sink_grow(col_rel_t *out, uint64_t need)
{
    bool denied = false;
    int rc;

    if (!out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    if (need > UINT32_MAX)
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    if (need > out->capacity) {
        uint64_t doubled = 2u * (uint64_t)out->capacity;

        if (doubled > UINT32_MAX)
            doubled = UINT32_MAX;
        /* A doubling that does not cover @need is not attempted: the exact
         * attempt below is the larger one. */
        if (doubled > need) {
            rc = col_rel_reserve_capacity_admitted(out, (uint32_t)doubled,
                    &denied);
            if (rc == 0)
                return WL_COLUMNAR_CONTINUATION_OK;
            /* Only a budget denial of the doubling is retried.  Any other
             * failure is reported as SINK_FAILURE without a second attempt,
             * the classification the exact path gives it. */
            if (rc != ENOMEM || !denied)
                return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
            /* A denied doubling retried exactly is not a failure: clear its
             * verdict so only the exact attempt's outcome remains visible
             * (col_rel_reserve_capacity_admitted also clears the flag on
             * entry). */
            denied = false;
            out->memory_budget_denial_pending = false;
        }
    }
    rc = col_rel_reserve_capacity_admitted(out, (uint32_t)need, &denied);
    if (rc == 0)
        return WL_COLUMNAR_CONTINUATION_OK;
    return (rc == ENOMEM && denied)
           ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
           : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
}

static wl_columnar_continuation_status_t
sink_begin(void *context, const wl_columnar_continuation_batch_t *batch)
{
    col_join_batch_relation_sink_t *s =
        (col_join_batch_relation_sink_t *)context;

    if (s)
        s->error = 0;
    if (!s || !s->out || !batch || batch->rows == 0 || !batch->payload)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    s->pending_begin = s->out->nrows;
    s->begun = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
sink_reserve(void *context, uint64_t bytes, uint32_t rows)
{
    col_join_batch_relation_sink_t *s =
        (col_join_batch_relation_sink_t *)context;
    uint64_t need;

    (void)bytes; /* the relation's own footprint formula is authoritative */
    if (!s || !s->begun || !s->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    need = (uint64_t)s->out->nrows + rows;
    if (need > UINT32_MAX) {
        /* A representation limit, not a memory failure (#1382). */
        s->error = EOVERFLOW;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    /* Capacity the relation already owns is admitted by the sink_init
    * invariant, so a batch that fits needs no governor round trip; this
    * is what makes a retry after a failed append reservation-free. */
    if (need <= s->out->capacity)
        return WL_COLUMNAR_CONTINUATION_OK;
    /* Growing exactly to each batch reallocated the output once per batch
    * (Issue #2173), so the growth is geometric with an exact fallback. */
    return col_join_batch_sink_grow(s->out, need);
}

static wl_columnar_continuation_status_t
sink_append(void *context, const wl_columnar_continuation_batch_t *batch)
{
    col_join_batch_relation_sink_t *s =
        (col_join_batch_relation_sink_t *)context;
    const col_rel_t *payload;

    if (!s || !s->begun || !s->out || !batch || !batch->payload)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    payload = (const col_rel_t *)batch->payload;
    if (payload->ncols != s->out->ncols || batch->rows > payload->nrows
        || (uint64_t)s->pending_begin + batch->rows > s->out->capacity)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    for (uint32_t i = 0; i < batch->rows; i++) {
        uint32_t row = s->pending_begin + i;
        for (uint32_t c = 0; c < s->out->ncols; c++) {
            if (col_rel_set_raw(s->out, row, c, payload->columns[c][i]) != 0)
                return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
        }
        if (s->out->timestamps) {
            if (payload->timestamps)
                s->out->timestamps[row] = payload->timestamps[i];
            else
                memset(&s->out->timestamps[row], 0,
                    sizeof(col_delta_timestamp_t));
        }
    }
    s->out->nrows = s->pending_begin + batch->rows;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
sink_commit(void *context, bool *committed)
{
    col_join_batch_relation_sink_t *s =
        (col_join_batch_relation_sink_t *)context;

    if (committed)
        *committed = false;
    if (!s || !s->begun || !s->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    /* The rows are already in place; publishing the view generation once
     * per batch is the durable side effect. */
    wl_columnar_relation_touch_view(s->out);
    s->begun = false;
    if (committed)
        *committed = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static void
sink_abort(void *context)
{
    col_join_batch_relation_sink_t *s =
        (col_join_batch_relation_sink_t *)context;

    if (!s || !s->out)
        return;
    /* Rewind the visible rows only.  Capacity admitted for this batch is
     * retained by the relation's token (conservative, like relation.c's
     * failed bulk append) and is reused by the retry without a new
     * reservation. */
    if (s->begun)
        s->out->nrows = s->pending_begin;
    s->begun = false;
}

int
col_join_batch_relation_sink_init(col_join_batch_relation_sink_t *ctx,
    wl_columnar_continuation_sink_t *sink, wl_col_session_t *sess,
    col_rel_t *out)
{
    int rc;

    if (!ctx || !sink || !sess || !out)
        return EINVAL;
    if (out->pool_owned || out->arena_owned || out->col_shared)
        return EINVAL;
    memset(ctx, 0, sizeof(*ctx));
    ctx->sess = sess;
    ctx->out = out;
    if (sess->memory_governor) {
        bool denied = false;
        rc = col_rel_attach_memory_governor(out, sess->memory_governor);
        if (rc != 0)
            return rc;
        rc = col_rel_reserve_capacity_admitted(out, out->capacity, &denied);
        if (rc != 0)
            return rc;
    }
    sink->context = ctx;
    sink->begin = sink_begin;
    sink->reserve = sink_reserve;
    sink->append = sink_append;
    sink->commit = sink_commit;
    sink->abort = sink_abort;
    return 0;
}

int
col_join_batch_run_to_relation(wl_columnar_continuation_t *cont,
    wl_col_session_t *sess, col_rel_t *out)
{
    col_join_batch_relation_sink_t ctx;
    wl_columnar_continuation_sink_t sink;
    int rc;

    if (!cont || !sess || !out)
        return EINVAL;
    rc = col_join_batch_relation_sink_init(&ctx, &sink, sess, out);
    if (rc != 0) {
        wl_columnar_continuation_cancel(cont);
        return rc;
    }
    for (;;) {
        uint64_t before = out->nrows;
        wl_columnar_continuation_status_t status
            = wl_columnar_continuation_publish(cont, &sink);
        switch (status) {
        case WL_COLUMNAR_CONTINUATION_OK:
            /* Charge only this committed batch. Each concurrent producer
             * can overshoot the shared cap by its in-flight batch. */
            if (wl_columnar_join_output_limit_reached(sess, out,
                out->nrows - before)) {
                wl_columnar_continuation_cancel(cont);
                return EOVERFLOW;
            }
            continue;
        case WL_COLUMNAR_CONTINUATION_DONE:
            return 0;
        case WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED:
            rc = ENOSPC;
            break;
        case WL_COLUMNAR_CONTINUATION_SINK_FAILURE:
            rc = ctx.error != 0 ? ctx.error : ENOMEM;
            break;
        case WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE:
        case WL_COLUMNAR_CONTINUATION_ALLOCATION_FAILURE:
            rc = ENOMEM;
            break;
        case WL_COLUMNAR_CONTINUATION_STALE:
            rc = EAGAIN;
            break;
        case WL_COLUMNAR_CONTINUATION_UNSUPPORTED:
            rc = ENOTSUP;
            break;
        case WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS:
            rc = EIO;
            break;
        case WL_COLUMNAR_CONTINUATION_INVALID:
        default:
            rc = EINVAL;
            break;
        }
        wl_columnar_continuation_cancel(cont);
        return rc;
    }
}

static wl_columnar_continuation_status_t
wl_columnar_join_batch_payload_begin(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_columnar_join_batch_payload_sink_t *s = context;
    s->error = WL_COLUMNAR_SPILL_OK;
    if (s->begun || s->ready || s->staged) {
        s->error = WL_COLUMNAR_SPILL_BUSY;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    if (!batch || !batch->payload || !batch->rows) {
        s->error = WL_COLUMNAR_SPILL_INVALID;
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    s->source = batch->payload; s->rows = batch->rows; s->begun = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
wl_columnar_join_batch_payload_reserve(void *context, uint64_t bytes,
    uint32_t rows)
{
    (void)bytes; /* Capture admits its actual representation, including metadata. */
    wl_columnar_join_batch_payload_sink_t *s = context;
    if (!s->begun || s->staged || rows != s->rows) {
        s->error = WL_COLUMNAR_SPILL_INVALID;
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    s->error = wl_columnar_batch_payload_capture(s->governor, s->store,
            s->source, rows, s->storage, s->cancel, s->cancel_context,
            &s->staged);
    return s->error == WL_COLUMNAR_SPILL_OK ? WL_COLUMNAR_CONTINUATION_OK
        : s->error ==
           WL_COLUMNAR_SPILL_MEMORY ?
           WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
        : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
}

static wl_columnar_continuation_status_t
wl_columnar_join_batch_payload_append(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_columnar_join_batch_payload_sink_t *s = context;
    if (!s->begun || !s->staged || !batch || batch->payload != s->source
        || batch->rows != s->rows) {
        s->error = WL_COLUMNAR_SPILL_INVALID;
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
wl_columnar_join_batch_payload_commit(void *context, bool *committed)
{
    wl_columnar_join_batch_payload_sink_t *s = context;
    *committed = false;
    if (!s->begun || !s->staged ||
        s->ready) return WL_COLUMNAR_CONTINUATION_INVALID;
    s->ready = s->staged; s->staged = NULL;
    s->begun = false; s->source = NULL; s->rows = 0;
    *committed = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static void
wl_columnar_join_batch_payload_abort(void *context)
{
    wl_columnar_join_batch_payload_sink_t *s = context;
    if (s->staged &&
        wl_columnar_batch_payload_release(s->staged) != WL_COLUMNAR_SPILL_OK)
        abort();
    s->staged = NULL; s->source = NULL; s->rows = 0; s->begun = false;
}

void
wl_columnar_join_batch_payload_sink_init(
    wl_columnar_join_batch_payload_sink_t *s,
    wl_columnar_continuation_sink_t *sink,
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store,
    wl_columnar_batch_payload_storage_t storage)
{
    memset(s, 0, sizeof(*s));
    s->governor = governor; s->store = store; s->storage = storage;
    *sink = (wl_columnar_continuation_sink_t){
        .context = s, .begin = wl_columnar_join_batch_payload_begin,
        .reserve = wl_columnar_join_batch_payload_reserve,
        .append = wl_columnar_join_batch_payload_append,
        .commit = wl_columnar_join_batch_payload_commit,
        .abort = wl_columnar_join_batch_payload_abort,
    };
}

wl_columnar_batch_payload_t *
wl_columnar_join_batch_payload_sink_take(
    wl_columnar_join_batch_payload_sink_t *s)
{
    if (!s) return NULL;
    wl_columnar_batch_payload_t *p = s->ready;
    s->ready = NULL;
    return p;
}

void
wl_columnar_join_batch_payload_sink_cleanup(
    wl_columnar_join_batch_payload_sink_t *s)
{
    if (!s) return;
    wl_columnar_join_batch_payload_abort(s);
    if (s->ready &&
        wl_columnar_batch_payload_release(s->ready) != WL_COLUMNAR_SPILL_OK)
        abort();
    s->ready = NULL;
}
