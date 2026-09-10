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
 *     admits every visible byte, growth is admitted exactly to the batch
 *     that needs it, and an aborted batch only rewinds nrows (the admitted
 *     capacity stays, as after a failed bulk append in relation.c).
 */
#include "columnar/join_batch.h"

#include "wirelog/util/log.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

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
    default: return "unknown";
    }
}

void
col_join_batch_record_fallback(wl_col_session_t *sess,
    col_join_batch_eligibility_t reason)
{
    uint8_t bit;

    if (!sess || reason == COL_JOIN_BATCH_ELIGIBLE
        || reason == COL_JOIN_BATCH_EXCLUDED_OFF)
        return;
    sess->join_batch_fallback_count++;
    sess->join_batch_last_reason = (uint8_t)reason;
    bit = (uint8_t)(1u << ((unsigned)reason & 7u));
    if (sess->join_batch_warned_reasons & bit)
        return;
    sess->join_batch_warned_reasons |= bit;
    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
        "bounded join mode fell back to one-shot join: %s",
        col_join_batch_eligibility_name(reason));
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
    const col_arrangement_t *arr;
    int64_t *key_row;
    col_rel_t *batch;        /* governed heap scratch, rows_per_batch rows */
    uint32_t rows_per_batch;
    col_join_batch_cursor_t cursor;
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
    /* The arrangement is pinned, but a rebuild under the lease is not
     * deferred, so its epoch and indexed extent are load-bearing too. */
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
                p->key_row[p->rk[k]] = p->left->columns[p->lk[k]][lr];
            rr = col_arrangement_find_first_typed(p->arr, p->right,
                    p->key_row);
        }
        while (rr != UINT32_MAX) {
            /* Chains may hold collision rows; every candidate is checked. */
            if (col_join_keys_match_rel(p->left, lr, p->lk, p->right, rr,
                p->rk, p->kc)) {
                if (col_join_write_pair_at(p->batch, n, p->left, lr,
                    p->right, rr, p->op->project_indices,
                    p->op->project_count) != 0)
                    return WL_COLUMNAR_CONTINUATION_INVALID;
                if (p->batch->timestamps)
                    memset(&p->batch->timestamps[n], 0,
                        sizeof(col_delta_timestamp_t));
                n++;
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
    p->arr = NULL;
}

static void
producer_cancel(void *context)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)context;

    if (!p)
        return;
    producer_release_lease(p);
}

static void
producer_destroy(void *context)
{
    col_join_batch_producer_t *p = (col_join_batch_producer_t *)context;

    if (!p)
        return;
    producer_release_lease(p);
    if (p->batch)
        col_rel_destroy(p->batch); /* releases the scratch token and ref */
    free(p->key_row);
    free(p->lk);
    free(p->rk);
    free(p);
}

int
col_join_batch_producer_create(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const uint32_t *lk, const uint32_t *rk, uint32_t kc,
    uint64_t batch_bytes, wl_columnar_continuation_t **out)
{
    col_join_batch_producer_t *p;
    const col_rel_t *right;
    wl_columnar_continuation_producer_t producer;
    wl_columnar_continuation_cursor_t initial;
    uint64_t row_bytes = 0;
    uint64_t rows;
    uint32_t ocols;
    int rc;

    if (out)
        *out = NULL;
    if (!sess || !op || !left || !lk || !rk || kc == 0 || !out
        || !op->right_relation || batch_bytes == 0)
        return EINVAL;
    right = session_find_rel(sess, op->right_relation);
    if (!right)
        return ENOENT;
    for (uint32_t k = 0; k < kc; k++)
        if (lk[k] >= left->ncols || rk[k] >= right->ncols)
            return EINVAL;
    p = (col_join_batch_producer_t *)calloc(1, sizeof(*p));
    if (!p)
        return ENOMEM;
    p->sess = sess;
    p->op = op;
    p->left = left;
    p->right = right;
    p->kc = kc;
    p->lk = (uint32_t *)malloc((size_t)kc * sizeof(uint32_t));
    p->rk = (uint32_t *)malloc((size_t)kc * sizeof(uint32_t));
    p->key_row = (int64_t *)calloc(right->ncols > 0 ? right->ncols : 1u,
            sizeof(int64_t));
    if (!p->lk || !p->rk || !p->key_row) {
        rc = ENOMEM;
        goto fail;
    }
    memcpy(p->lk, lk, (size_t)kc * sizeof(uint32_t));
    memcpy(p->rk, rk, (size_t)kc * sizeof(uint32_t));

    /* One lease for the whole continuation.  The registry refuses to
     * relocate entries while any lease is active, so the arrangement
     * pointer stays valid until destroy or cancel. */
    if (col_session_pin_arrangement(&sess->base, op->right_relation, rk, kc,
        &p->pin) != 0 || !p->pin.arr) {
        rc = ENOENT;
        goto fail;
    }
    p->arr = p->pin.arr;
    if (p->arr->indexed_rows == 0 && right->nrows > 0) {
        rc = ENOENT;
        goto fail;
    }

    /* Scratch batch relation: same shape as the output, admitted once
     * under the session governor, sized so one batch fits batch_bytes. */
    ocols = col_join_output_width(left, right, op);
    p->batch = col_rel_new_auto("$join_batch", ocols);
    if (!p->batch) {
        rc = ENOMEM;
        goto fail;
    }
    if (col_join_set_output_types(p->batch, left, right, op) != 0) {
        rc = ENOMEM;
        goto fail;
    }
    if (left->timestamps && col_rel_enable_timestamps(p->batch) != 0) {
        rc = ENOMEM;
        goto fail;
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
    if (sess->memory_governor) {
        bool denied = false;
        rc = col_rel_attach_memory_governor(p->batch, sess->memory_governor);
        if (rc != 0)
            goto fail;
        rc = col_rel_reserve_capacity_admitted(p->batch, p->batch->capacity,
                &denied);
        if (rc != 0)
            goto fail;
    }
    {
        bool denied = false;
        rc = col_rel_reserve_capacity_admitted(p->batch, p->rows_per_batch,
                &denied);
        if (rc != 0)
            goto fail;
    }

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
    return rc;
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

bool
col_join_batch_cursor_get(const wl_columnar_continuation_t *cont,
    col_join_batch_cursor_t *out)
{
    const col_join_batch_producer_t *p = producer_of(cont);
    const wl_columnar_continuation_cursor_t *c;

    if (!p || !out)
        return false;
    c = wl_columnar_continuation_cursor(cont);
    *out = p->cursor;
    if (c) {
        pos_unpack(c->position, &out->next);
        out->sequence = c->sequence;
    }
    return true;
}

/* ---- relation sink ----------------------------------------------------- */

static wl_columnar_continuation_status_t
sink_begin(void *context, const wl_columnar_continuation_batch_t *batch)
{
    col_join_batch_relation_sink_t *s =
        (col_join_batch_relation_sink_t *)context;

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
    bool denied = false;
    int rc;

    (void)bytes; /* the relation's own footprint formula is authoritative */
    if (!s || !s->begun || !s->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    need = (uint64_t)s->out->nrows + rows;
    if (need > UINT32_MAX)
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    /* Capacity the relation already owns is admitted by the sink_init
    * invariant, so a batch that fits needs no governor round trip; this
    * is what makes a retry after a failed append reservation-free. */
    if (need <= s->out->capacity)
        return WL_COLUMNAR_CONTINUATION_OK;
    rc = col_rel_reserve_capacity_admitted(s->out, (uint32_t)need, &denied);
    if (rc == 0)
        return WL_COLUMNAR_CONTINUATION_OK;
    return (rc == ENOMEM && denied)
           ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
           : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
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
        if (s->out->timestamps)
            memset(&s->out->timestamps[row], 0,
                sizeof(col_delta_timestamp_t));
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
        wl_columnar_continuation_status_t status
            = wl_columnar_continuation_publish(cont, &sink);
        switch (status) {
        case WL_COLUMNAR_CONTINUATION_OK:
            /* Legacy row cap, evaluated once per committed batch: it may
             * be overshot by at most rows_per_batch - 1 rows. */
            if (col_join_output_limit_reached(sess, out)) {
                wl_columnar_continuation_cancel(cont);
                return EOVERFLOW;
            }
            continue;
        case WL_COLUMNAR_CONTINUATION_DONE:
            return 0;
        case WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED:
        case WL_COLUMNAR_CONTINUATION_SINK_FAILURE:
        case WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE:
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
