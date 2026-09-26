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

#ifdef WL_TEST_JOIN_BATCH_DESCRIPTOR_HOOKS
#if defined(_MSC_VER)
static __declspec(thread) uint32_t test_fail_alloc_at;
static __declspec(thread) uint32_t test_alloc_count;
static __declspec(thread) bool test_fail_commit;
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
static _Thread_local uint32_t test_fail_alloc_at;
static _Thread_local uint32_t test_alloc_count;
static _Thread_local bool test_fail_commit;
#else
static __thread uint32_t test_fail_alloc_at;
static __thread uint32_t test_alloc_count;
static __thread bool test_fail_commit;
#endif

void
wl_columnar_diff_join_batch_test_fail_allocation_at(uint32_t n)
{
    test_fail_alloc_at = n;
    test_alloc_count = 0;
}

void
wl_columnar_diff_join_batch_test_fail_next_commit(void)
{
    test_fail_commit = true;
}

static bool
test_fail_descriptor_alloc(void)
{
    if (test_fail_alloc_at == 0)
        return false;
    uint32_t n = ++test_alloc_count;
    if (n != test_fail_alloc_at)
        return false;
    test_fail_alloc_at = 0;
    return true;
}
#endif

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
    wl_columnar_memory_reservation_t descriptor_reservation;
    wl_columnar_memory_governor_ref_t *descriptor_governor;
    bool descriptor_committed;
    uint32_t rows_per_batch;
    col_join_batch_cursor_t cursor;
} col_diff_join_batch_producer_t;

/* Preserve the rows already written while the relation grows.  The relation
 * resize path copies exactly nrows (including timestamps), so publishing the
 * live prefix before reserve is required for correctness. */
static wl_columnar_continuation_status_t
grow_scratch(col_diff_join_batch_producer_t *p, uint32_t n)
{
    uint32_t cap = p->batch->capacity;
    uint32_t want;
    bool denied = false;
    int rc;

    if (cap >= p->rows_per_batch)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    want = cap < p->rows_per_batch / 2u ? cap * 2u : p->rows_per_batch;
    if (want <= cap)
        want = p->rows_per_batch;
    p->batch->nrows = n;
    rc = col_rel_reserve_capacity_admitted(p->batch, want, &denied);
    if (rc == 0 && p->batch->capacity > n)
        return WL_COLUMNAR_CONTINUATION_OK;
    if (rc == ENOMEM)
        return denied ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
                      : WL_COLUMNAR_CONTINUATION_ALLOCATION_FAILURE;
    return WL_COLUMNAR_CONTINUATION_INVALID;
}

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
    if (pos.lr > p->left->nrows)
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
    bool complete = false;
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
                if (n == p->batch->capacity) {
                    wl_columnar_continuation_status_t grow_status
                        = grow_scratch(p, n);
                    if (grow_status != WL_COLUMNAR_CONTINUATION_OK)
                        return grow_status;
                }
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
                /* Keep the live prefix structurally current, so every future
                 * relocation preserves the row just written. */
                p->batch->nrows = n;
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
    complete = true;

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
    batch->complete = complete;
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
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
    bool committed;

    if (!p)
        return;
    wl_columnar_memory_reservation_init(&reservation);
    if (p->descriptor_reservation.governor
        && !wl_columnar_memory_reservation_move(&reservation,
        &p->descriptor_reservation))
        abort();
    governor = p->descriptor_governor;
    committed = p->descriptor_committed;
    producer_release(p);
    col_rel_destroy(p->batch);
    free(p->key_row);
    free(p->lk);
    free(p->rk);
    free(p->project_indices);
    free(p);
    if (reservation.governor) {
        if (committed){
            if (!wl_columnar_memory_release(&reservation)) abort();
        }else{
            if (!wl_columnar_memory_rollback(&reservation)) abort();
        }
    }
    wl_columnar_memory_governor_ref_release(governor);
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
        /* A source snapshot change resets this private copy in txn_begin,
         * so public appends rebuild from row zero.  Retain the tail walk for
         * defensive same-snapshot row-count changes from internal callers. */
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
    uint64_t project_bytes, key_bytes, key_row_bytes, descriptor_bytes;
    uint32_t ocols;
    wl_columnar_memory_governor_ref_t *descriptor_governor = NULL;
    wl_columnar_memory_reservation_t descriptor_reservation;
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
    if (!wl_columnar_memory_size_mul(op->project_count,
        sizeof(uint32_t), &project_bytes)
        || !wl_columnar_memory_size_mul(kc,
        2u * sizeof(uint32_t), &key_bytes)
        || !wl_columnar_memory_size_mul(
            right->ncols > 0 ? right->ncols : 1u, sizeof(int64_t),
            &key_row_bytes)
        || !wl_columnar_memory_size_add(sizeof(*p), project_bytes,
        &descriptor_bytes)
        || !wl_columnar_memory_size_add(descriptor_bytes, key_bytes,
        &descriptor_bytes)
        || !wl_columnar_memory_size_add(descriptor_bytes, key_row_bytes,
        &descriptor_bytes)
        || descriptor_bytes > SIZE_MAX)
        return EOVERFLOW;
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
            return admission == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC
                : admission == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
        }
    }
#ifdef WL_TEST_JOIN_BATCH_DESCRIPTOR_HOOKS
    p = test_fail_descriptor_alloc() ? NULL : calloc(1, sizeof(*p));
#else
    p = calloc(1, sizeof(*p));
#endif
    if (!p) {
        if (descriptor_reservation.governor){
            if (!wl_columnar_memory_rollback(&descriptor_reservation)) abort();
        }
        wl_columnar_memory_governor_ref_release(descriptor_governor);
        return ENOMEM;
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
        return EINVAL;
    }
    p->left = left;
    p->right = right;
    p->kc = kc;
    p->project_count = op->project_count;
    if (p->project_count > 0) {
#ifdef WL_TEST_JOIN_BATCH_DESCRIPTOR_HOOKS
        p->project_indices = test_fail_descriptor_alloc() ? NULL
            : malloc((size_t)p->project_count
                * sizeof(*p->project_indices));
#else
        p->project_indices = malloc((size_t)p->project_count
                * sizeof(*p->project_indices));
#endif
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
#ifdef WL_TEST_JOIN_BATCH_DESCRIPTOR_HOOKS
    p->lk = test_fail_descriptor_alloc() ? NULL
        : malloc((size_t)kc * sizeof(*p->lk));
    p->rk = test_fail_descriptor_alloc() ? NULL
        : malloc((size_t)kc * sizeof(*p->rk));
    p->key_row = test_fail_descriptor_alloc() ? NULL
        : calloc(right->ncols ? right->ncols : 1u, sizeof(*p->key_row));
#else
    p->lk = malloc((size_t)kc * sizeof(*p->lk));
    p->rk = malloc((size_t)kc * sizeof(*p->rk));
    p->key_row = calloc(right->ncols ? right->ncols : 1u,
            sizeof(*p->key_row));
#endif
    if (!p->lk || !p->rk || !p->key_row) {
        rc = ENOMEM;
        goto fail;
    }
    memcpy(p->lk, lk, (size_t)kc * sizeof(*p->lk));
    memcpy(p->rk, rk, (size_t)kc * sizeof(*p->rk));
    if (p->descriptor_reservation.governor) {
        bool commit_ok = true;
#ifdef WL_TEST_JOIN_BATCH_DESCRIPTOR_HOOKS
        if (test_fail_commit) {
            test_fail_commit = false;
            commit_ok = false;
        }
#endif
        if (!commit_ok
            || !wl_columnar_memory_commit(&p->descriptor_reservation, p)) {
            rc = EINVAL;
            goto fail;
        }
        p->descriptor_committed = true;
    }
    rc = index_right(sess, op->right_relation, right, rk, kc);
    if (rc != 0)
        goto fail;
    rc = col_session_pin_diff_arrangement(sess, op->right_relation, right,
            rk, kc, &p->pin);
    if (rc != 0)
        goto fail;
    p->arr = p->pin.arr;
    ocols = col_join_output_width(left, right, op);
    /* Session ownership is authoritative.  Without a session governor, the
     * delta-driving left source owns this transient scratch; the right source
     * is the fallback for legacy inputs that carry governance only there. */
    bool timestamped = left->timestamps || right->timestamps;
    if (descriptor_governor)
        rc = wl_columnar_relation_new_auto_governed("$diff_join_batch",
                ocols, COL_REL_INIT_CAP, timestamped, descriptor_governor,
                &p->batch);
    else {
        p->batch = col_rel_new_auto("$diff_join_batch", ocols);
        rc = p->batch ? 0 : ENOMEM;
    }
    if (rc != 0)
        goto fail;
    if (!p->batch) {
        rc = ENOMEM;
        goto fail;
    }
    rc = col_join_set_output_types(p->batch, left, right, op);
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
