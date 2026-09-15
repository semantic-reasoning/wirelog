/*
 * columnar/diff_join_batch.c - bounded differential keyed-join producer
 *
 * The differential arrangement is indexed in a short transaction before the
 * continuation is created.  The continuation itself is read-only with
 * respect to the arrangement and holds a generation/source lease until it is
 * cancelled or destroyed.
 */
#include "columnar/diff_join_batch.h"

#include "wirelog/util/log.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const col_rel_t *left;
    const col_rel_t *right;
    uint32_t *project_indices;
    uint32_t project_count;
    uint32_t *lk;
    uint32_t *rk;
    uint32_t kc;
    col_diff_arrangement_pin_t pin;
    wl_columnar_source_access_reader_t left_source_reader;
    wl_columnar_source_access_reader_t source_reader;
    bool left_source_reader_active;
    bool source_reader_active;
    col_diff_arrangement_t *arr;
    col_rel_t *batch;
    int64_t *key_row;
    uint32_t rows_per_batch;
    col_join_batch_cursor_t cursor;
} col_diff_join_batch_producer_t;

static uint64_t
pos_pack(uint32_t lr, uint32_t rr)
{
    return ((uint64_t)lr << 32) | (uint64_t)rr;
}

static void
pos_unpack(uint64_t packed, col_join_batch_pos_t *pos)
{
    pos->lr = (uint32_t)(packed >> 32);
    pos->rr = (uint32_t)packed;
}

static uint32_t
diff_next(const col_diff_arrangement_t *arr, uint32_t row)
{
    uint32_t link;

    if (!arr || !arr->ht_next || row >= arr->ht_cap)
        return UINT32_MAX;
    link = arr->ht_next[row];
    return link == 0 ? UINT32_MAX : link - 1u;
}

static uint32_t
diff_first(const col_diff_arrangement_t *arr, const col_rel_t *right,
    const uint32_t *rk, uint32_t kc, const int64_t *key_row)
{
    uint32_t hash = 2166136261u;

    if (!arr || !right || !arr->ht_head || arr->nbuckets == 0)
        return UINT32_MAX;
    for (uint32_t k = 0; k < kc; k++)
        hash = wl_columnar_hash_value(hash, right, rk[k], key_row[rk[k]]);
    hash &= arr->nbuckets - 1u;
    return arr->ht_head[hash] == 0 ? UINT32_MAX : arr->ht_head[hash] - 1u;
}

static bool
producer_validate(void *context,
    const wl_columnar_continuation_cursor_t *cursor)
{
    const col_diff_join_batch_producer_t *p = context;
    const col_join_batch_cursor_t *c;
    col_join_batch_pos_t pos;

    if (!p || !cursor || !p->left || !p->right || !p->arr
        || !p->pin.active)
        return false;
    c = &p->cursor;
    pos_unpack(cursor->position, &pos);
    if (cursor->input_identity != p->left->relation_identity
        || cursor->arrangement_identity != p->right->relation_identity
        || c->left_identity != p->left->relation_identity
        || c->left_view_gen != p->left->view_generation
        || c->left_storage_gen != p->left->storage_generation
        || c->right_identity != p->right->relation_identity
        || c->right_view_gen != p->right->view_generation
        || c->right_storage_gen != p->right->storage_generation
        || c->left_view_gen == UINT64_MAX
        || c->left_storage_gen == UINT64_MAX
        || c->right_view_gen == UINT64_MAX
        || c->right_storage_gen == UINT64_MAX)
        return false;
    if (p->pin.entry->generation != p->pin.generation
        || !wl_columnar_relation_snapshot_equal(p->arr->source_snapshot,
        p->pin.source_snapshot)
        || p->arr->indexed_rows != p->cursor.arr_indexed_rows
        || p->arr->indexed_rows != p->right->nrows)
        return false;
    if (cursor->generation != p->pin.generation
        || c->arr_generation != p->pin.generation)
        return false;
    if (pos.rr != UINT32_MAX && pos.rr >= p->arr->ht_cap)
        return false;
    if (c->timestamps != (p->left->timestamps != NULL
        || p->right->timestamps != NULL))
        return false;
    return true;
}

static wl_columnar_continuation_status_t
producer_produce(void *context,
    const wl_columnar_continuation_cursor_t *cursor,
    wl_columnar_continuation_batch_t *batch)
{
    col_diff_join_batch_producer_t *p = context;
    col_join_batch_pos_t pos;
    col_join_batch_pos_t next;
    uint32_t n = 0;
    uint64_t bytes;

    if (!p || !cursor || !batch || !p->batch)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    pos_unpack(cursor->position, &pos);
    if (pos.lr > p->left->nrows)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    p->batch->nrows = 0;
    next = pos;
    while (next.lr < p->left->nrows) {
        uint32_t lr = next.lr;
        uint32_t rr = next.rr;
        if (rr == UINT32_MAX) {
            for (uint32_t k = 0; k < p->kc; k++)
                p->key_row[p->rk[k]] = p->left->columns[p->lk[k]][lr];
            rr = diff_first(p->arr, p->right, p->rk, p->kc, p->key_row);
        }
        while (rr != UINT32_MAX) {
            if (rr >= p->right->nrows)
                return WL_COLUMNAR_CONTINUATION_STALE;
            if (col_join_keys_match_rel(p->left, lr, p->lk, p->right, rr,
                p->rk, p->kc)) {
                if (col_join_write_pair_at(p->batch, n, p->left, lr,
                    p->right, rr, p->project_indices,
                    p->project_count) != 0)
                    return WL_COLUMNAR_CONTINUATION_INVALID;
                if (p->batch->timestamps) {
                    int64_t lm = p->left->timestamps
                        ? p->left->timestamps[lr].multiplicity : 1;
                    int64_t rm = p->right->timestamps
                        ? p->right->timestamps[rr].multiplicity : 1;
                    /* The left input is the delta-driving side.  Preserve
                     * its provenance; the right arrangement contributes
                     * multiplicity but does not replace that provenance. */
                    if (p->left->timestamps)
                        p->batch->timestamps[n]
                            = p->left->timestamps[lr];
                    else if (p->right->timestamps)
                        p->batch->timestamps[n]
                            = p->right->timestamps[rr];
                    if (wl_columnar_arithmetic_checked_mul_int64(lm, rm,
                        &p->batch->timestamps[n].multiplicity) != 0)
                        return WL_COLUMNAR_CONTINUATION_INVALID;
                }
                n++;
                if (n == p->rows_per_batch) {
                    uint32_t nrr = diff_next(p->arr, rr);
                    next.lr = nrr == UINT32_MAX ? lr + 1u : lr;
                    next.rr = nrr;
                    goto emit;
                }
            }
            rr = diff_next(p->arr, rr);
        }
        next.lr = lr + 1u;
        next.rr = UINT32_MAX;
    }

emit:
    if (n == 0) {
        batch->payload = NULL;
        batch->bytes = 0;
        batch->rows = 0;
        batch->complete = true;
        batch->next_cursor = *cursor;
        return WL_COLUMNAR_CONTINUATION_OK;
    }
    if (!col_rel_retained_bytes_for(p->batch, n, &bytes))
        return WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED;
    p->batch->nrows = n;
    batch->payload = p->batch;
    batch->bytes = bytes;
    batch->rows = n;
    batch->complete = next.lr >= p->left->nrows;
    batch->next_cursor = *cursor;
    batch->next_cursor.position = pos_pack(next.lr, next.rr);
    batch->next_cursor.sequence = cursor->sequence + 1u;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static void
producer_release(void *context)
{
    col_diff_join_batch_producer_t *p = context;

    if (!p)
        return;
    col_diff_arrangement_pin_release(&p->pin);
    if (p->left_source_reader_active) {
        (void)col_rel_source_reader_release(&p->left_source_reader);
        p->left_source_reader_active = false;
    }
    if (p->source_reader_active) {
        (void)col_rel_source_reader_release(&p->source_reader);
        p->source_reader_active = false;
    }
    p->arr = NULL;
}

static void
producer_cancel(void *context)
{
    producer_release(context);
}

static void
producer_destroy(void *context)
{
    col_diff_join_batch_producer_t *p = context;

    if (!p)
        return;
    producer_release(p);
    col_rel_destroy(p->batch);
    free(p->key_row);
    free(p->lk);
    free(p->rk);
    free(p->project_indices);
    free(p);
}

static int
index_right(wl_col_session_t *sess, const char *name, const col_rel_t *right,
    const uint32_t *rk, uint32_t kc)
{
    wl_columnar_arrangement_diff_txn_t txn = { 0 };
    col_diff_arrangement_t *arr;
    int rc = wl_columnar_arrangement_diff_txn_begin(sess, name, right, rk,
            kc, &txn);

    if (rc != 0)
        return rc;
    arr = txn.working;
    rc = col_diff_arrangement_ensure_ht_capacity(arr, right->nrows);
    if (rc == 0) {
        for (uint32_t row = arr->indexed_rows; row < right->nrows; row++) {
            uint32_t hash = 2166136261u;
            for (uint32_t k = 0; k < kc; k++)
                hash = wl_columnar_hash_value(hash, right, rk[k],
                        right->columns[rk[k]][row]);
            hash &= arr->nbuckets - 1u;
            arr->ht_next[row] = arr->ht_head[hash];
            arr->ht_head[hash] = row + 1u;
        }
        arr->indexed_rows = right->nrows;
        arr->current_nrows = right->nrows;
        arr->source_snapshot = wl_columnar_relation_snapshot(right);
    }
    if (rc == 0)
        rc = wl_columnar_arrangement_diff_txn_commit(&txn);
    if (rc != 0)
        wl_columnar_arrangement_diff_txn_abort(&txn);
    return rc;
}

int
col_diff_join_batch_producer_create(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const uint32_t *lk, const uint32_t *rk, uint32_t kc,
    uint64_t batch_bytes, wl_columnar_continuation_t **out)
{
    col_diff_join_batch_producer_t *p = NULL;
    const col_rel_t *right;
    wl_columnar_continuation_producer_t producer = { 0 };
    wl_columnar_continuation_cursor_t initial = { 0 };
    uint64_t row_bytes, rows;
    uint32_t ocols;
    int rc;

    if (out)
        *out = NULL;
    if (!sess || !op || !left || !lk || !rk || kc == 0 || !out
        || !op->right_relation || batch_bytes == 0
        || (op->project_count > 0 && !op->project_indices))
        return EINVAL;
    right = session_find_rel(sess, op->right_relation);
    if (!right)
        return ENOENT;
    p = calloc(1, sizeof(*p));
    if (!p)
        return ENOMEM;
    p->left = left;
    p->right = right;
    p->kc = kc;
    p->project_count = op->project_count;
    if (p->project_count > 0) {
        p->project_indices = malloc((size_t)p->project_count
                * sizeof(*p->project_indices));
        if (!p->project_indices) {
            rc = ENOMEM;
            goto fail;
        }
        memcpy(p->project_indices, op->project_indices,
            (size_t)p->project_count * sizeof(*p->project_indices));
    }
    rc = col_rel_source_reader_acquire(right, &p->source_reader);
    if (rc != 0)
        goto fail;
    p->source_reader_active = true;
    rc = col_rel_source_reader_acquire(left, &p->left_source_reader);
    if (rc != 0)
        goto fail;
    p->left_source_reader_active = true;
    p->lk = malloc((size_t)kc * sizeof(*p->lk));
    p->rk = malloc((size_t)kc * sizeof(*p->rk));
    p->key_row = calloc(right->ncols ? right->ncols : 1u,
            sizeof(*p->key_row));
    if (!p->lk || !p->rk || !p->key_row) {
        rc = ENOMEM;
        goto fail;
    }
    memcpy(p->lk, lk, (size_t)kc * sizeof(*p->lk));
    memcpy(p->rk, rk, (size_t)kc * sizeof(*p->rk));
    rc = index_right(sess, op->right_relation, right, rk, kc);
    if (rc != 0)
        goto fail;
    rc = col_session_pin_diff_arrangement(sess, op->right_relation, right,
            rk, kc, &p->pin);
    if (rc != 0)
        goto fail;
    p->arr = p->pin.arr;
    ocols = col_join_output_width(left, right, op);
    p->batch = col_rel_new_auto("$diff_join_batch", ocols);
    if (!p->batch || col_join_set_output_types(p->batch, left, right, op)
        != 0) {
        rc = ENOMEM;
        goto fail;
    }
    if ((left->timestamps || right->timestamps)
        && col_rel_enable_timestamps(p->batch) != 0) {
        rc = ENOMEM;
        goto fail;
    }
    if (!col_rel_retained_bytes_for(p->batch, 1u, &row_bytes)
        || row_bytes == 0) {
        rc = ENOTSUP;
        goto fail;
    }
    rows = batch_bytes / row_bytes;
    if (rows == 0) {
        rc = ENOTSUP;
        goto fail;
    }
    p->rows_per_batch = rows > UINT32_MAX ? UINT32_MAX : (uint32_t)rows;
    if (sess->memory_governor) {
        rc = col_rel_attach_memory_governor(p->batch, sess->memory_governor);
        if (rc != 0)
            goto fail;
    }
    rc = col_rel_reserve_capacity_admitted(p->batch, p->rows_per_batch,
            NULL);
    if (rc != 0)
        goto fail;
    p->cursor.left_identity = left->relation_identity;
    p->cursor.left_view_gen = left->view_generation;
    p->cursor.left_storage_gen = left->storage_generation;
    p->cursor.right_identity = right->relation_identity;
    p->cursor.right_view_gen = right->view_generation;
    p->cursor.right_storage_gen = right->storage_generation;
    p->cursor.arr_generation = p->pin.generation;
    p->cursor.arr_indexed_rows = p->arr->indexed_rows;
    p->cursor.left_is_delta = left_is_delta;
    p->cursor.timestamps = left->timestamps != NULL ||
        right->timestamps != NULL;
    initial.input_identity = left->relation_identity;
    initial.arrangement_identity = right->relation_identity;
    initial.generation = p->cursor.arr_generation;
    initial.position = pos_pack(0, UINT32_MAX);
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
