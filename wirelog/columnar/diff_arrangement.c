/*
 * columnar/diff_arrangement.c - Differential Arrangement Implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Delta-aware hash table for incremental indexing.
 * Supports base/current row tracking and deep-copy isolation.
 */

#include "diff_arrangement.h"

#include "columnar/mem_ledger.h"

#include <errno.h>
#include <string.h>

#define DIFF_ARRANGEMENT_INITIAL_BUCKETS 1024

uint64_t
col_diff_arrangement_bytes(const col_diff_arrangement_t *arr)
{
    uint64_t key_bytes;
    uint64_t bucket_bytes;
    uint64_t next_bytes;
    uint64_t bytes;

    if (!arr)
        return 0;
    if (!wl_columnar_memory_size_mul(arr->key_count, sizeof(uint32_t),
        &key_bytes)
        || !wl_columnar_memory_size_mul(arr->nbuckets, sizeof(uint32_t),
        &bucket_bytes)
        || !wl_columnar_memory_size_mul(arr->ht_cap, sizeof(uint32_t),
        &next_bytes)
        || !wl_columnar_memory_size_add(sizeof(*arr), key_bytes, &bytes)
        || !wl_columnar_memory_size_add(bytes, bucket_bytes, &bytes)
        || !wl_columnar_memory_size_add(bytes, next_bytes, &bytes))
        return 0;
    return bytes;
}

static bool
diff_shape_bytes(uint32_t key_count, uint32_t nbuckets, uint32_t ht_cap,
    uint64_t *out)
{
    uint64_t key_bytes;
    uint64_t bucket_bytes;
    uint64_t next_bytes;
    uint64_t bytes;

    if (!out
        || !wl_columnar_memory_size_mul(key_count, sizeof(uint32_t),
        &key_bytes)
        || !wl_columnar_memory_size_mul(nbuckets, sizeof(uint32_t),
        &bucket_bytes)
        || !wl_columnar_memory_size_mul(ht_cap, sizeof(uint32_t),
        &next_bytes)
        || !wl_columnar_memory_size_add(sizeof(col_diff_arrangement_t),
        key_bytes, &bytes)
        || !wl_columnar_memory_size_add(bytes, bucket_bytes, &bytes)
        || !wl_columnar_memory_size_add(bytes, next_bytes, &bytes))
        return false;
    *out = bytes;
    return true;
}

static bool
diff_size_arg(uint64_t bytes, size_t *out)
{
    if (!out || bytes > SIZE_MAX)
        return false;
    *out = (size_t)bytes;
    return true;
}

static bool
diff_capacity_at_least(uint32_t current, uint32_t minimum,
    uint32_t *out)
{
    uint32_t capacity = current ? current : DIFF_ARRANGEMENT_INITIAL_BUCKETS;

    while (capacity < minimum) {
        if (capacity > UINT32_MAX / 2u) {
            capacity = minimum;
            break;
        }
        capacity *= 2u;
    }
    *out = capacity;
    return capacity >= minimum;
}

static bool
diff_bucket_capacity(uint32_t nrows, uint32_t current, uint32_t *out)
{
    uint32_t buckets = current ? current : DIFF_ARRANGEMENT_INITIAL_BUCKETS;

    while (nrows > buckets - buckets / 4u) {
        if (buckets > UINT32_MAX / 2u)
            return false;
        buckets *= 2u;
    }
    *out = buckets;
    return true;
}

static bool
diff_publish_reservation(col_diff_arrangement_t *arr,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes)
{
    wl_columnar_memory_reservation_t previous;
    wl_columnar_memory_reservation_t rollback;

    if (!arr || !pending || !arr->memory_governor)
        return false;
    wl_columnar_memory_reservation_init(&previous);
    wl_columnar_memory_reservation_init(&rollback);
    if (!wl_columnar_memory_commit(pending, arr))
        return false;
    if (!wl_columnar_memory_reservation_move(&previous,
        &arr->reservation))
        return false;
    if (!wl_columnar_memory_reservation_move(&arr->reservation, pending)) {
        (void)wl_columnar_memory_reservation_move(&arr->reservation,
            &previous);
        return false;
    }
    if (!wl_columnar_memory_release(&previous)) {
        if (wl_columnar_memory_reservation_move(&rollback,
            &arr->reservation))
            (void)wl_columnar_memory_reservation_move(&arr->reservation,
                &previous);
        (void)wl_columnar_memory_release(&rollback);
        return false;
    }
    arr->reserved_bytes = bytes;
    return true;
}

/* Charge or credit the difference between @before and the current
 * footprint under ARRANGEMENT (Issue #1380). */
static void
diff_arr_ledger_sync(col_diff_arrangement_t *arr, uint64_t before)
{
    if (!arr || !arr->ledger)
        return;
    uint64_t after;
    if (!diff_shape_bytes(arr->key_count, arr->nbuckets, arr->ht_cap,
        &after))
        return;
    if (after > before)
        wl_mem_ledger_alloc(arr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
            after - before);
    else if (before > after)
        wl_mem_ledger_free(arr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
            before - after);
}

void
col_diff_arrangement_attach_ledger(col_diff_arrangement_t *arr,
    struct wl_mem_ledger *ledger)
{
    if (!arr || arr->ledger || !ledger)
        return;
    arr->ledger = ledger;
    diff_arr_ledger_sync(arr, 0);
}

int
col_diff_arrangement_attach_memory_governor(
    col_diff_arrangement_t *arr,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    wl_columnar_memory_governor_t *governor;
    wl_columnar_memory_admission_status_t status;

    if (!arr || !memory_governor || arr->memory_governor)
        return EINVAL;
    governor = wl_columnar_memory_governor_ref_get(memory_governor);
    if (!governor)
        return EINVAL;
    wl_columnar_memory_reservation_init(&arr->reservation);
    uint64_t bytes;
    if (!diff_shape_bytes(arr->key_count, arr->nbuckets, arr->ht_cap,
        &bytes))
        return EOVERFLOW;
    status = wl_columnar_memory_reserve_checked(governor, bytes,
            &arr->reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
        arr->memory_budget_denial_pending = true;
        return ENOMEM;
    }
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return EINVAL;
    if (!wl_columnar_memory_commit(&arr->reservation, arr)) {
        (void)wl_columnar_memory_release(&arr->reservation);
        return ENOMEM;
    }
    arr->memory_governor = memory_governor;
    wl_columnar_memory_governor_ref_retain(memory_governor);
    arr->reserved_bytes = bytes;
    return 0;
}

static void
diff_arr_release_governor(col_diff_arrangement_t *arr)
{
    if (!arr || !arr->memory_governor)
        return;
    (void)wl_columnar_memory_release(&arr->reservation);
    arr->reserved_bytes = 0;
    wl_columnar_memory_governor_ref_release(arr->memory_governor);
    arr->memory_governor = NULL;
}

col_diff_arrangement_t *
col_diff_arrangement_create(const uint32_t *key_cols, uint32_t key_count,
    uint32_t worker_id)
{
    return col_diff_arrangement_create_with_memory_governor(
        key_cols, key_count, worker_id, NULL);
}

col_diff_arrangement_t *
col_diff_arrangement_create_with_memory_governor(
    const uint32_t *key_cols, uint32_t key_count, uint32_t worker_id,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    return col_diff_arrangement_create_with_memory_governor_status(
        key_cols, key_count, worker_id, memory_governor, NULL);
}

col_diff_arrangement_t *
col_diff_arrangement_create_with_memory_governor_status(
    const uint32_t *key_cols, uint32_t key_count, uint32_t worker_id,
    wl_columnar_memory_governor_ref_t *memory_governor,
    wl_columnar_memory_admission_status_t *status_out)
{
    wl_columnar_memory_reservation_t pending;
    uint64_t key_bytes;
    uint64_t next_bytes;
    uint64_t initial_bytes;
    size_t key_size;
    size_t next_size;
    bool pending_valid = false;
    col_diff_arrangement_t *arr = NULL;

    if (status_out)
        *status_out = WL_COLUMNAR_MEMORY_ADMISSION_OK;
    if (key_count > 0 && !key_cols)
        return NULL;
    if (!wl_columnar_memory_size_mul(key_count, sizeof(uint32_t),
        &key_bytes)
        || !wl_columnar_memory_size_mul(DIFF_ARRANGEMENT_INITIAL_BUCKETS,
        sizeof(uint32_t), &next_bytes)
        || !diff_shape_bytes(key_count, DIFF_ARRANGEMENT_INITIAL_BUCKETS,
        DIFF_ARRANGEMENT_INITIAL_BUCKETS, &initial_bytes)
        || !diff_size_arg(key_bytes, &key_size)
        || !diff_size_arg(next_bytes, &next_size))
        return NULL;

    wl_columnar_memory_reservation_init(&pending);
    if (memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(memory_governor),
                initial_bytes, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status_out)
                *status_out = status;
            return NULL;
        }
        pending_valid = true;
    }

    arr = calloc(1, sizeof(*arr));
    if (!arr)
        goto fail;
    arr->key_count = key_count;
    arr->worker_id = worker_id;
    arr->nbuckets = DIFF_ARRANGEMENT_INITIAL_BUCKETS;
    arr->ht_cap = DIFF_ARRANGEMENT_INITIAL_BUCKETS;
    arr->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
    arr->ledger = NULL;
    arr->memory_governor = NULL;
    wl_columnar_memory_reservation_init(&arr->reservation);
    arr->reserved_bytes = 0;
    arr->key_cols = key_bytes ? malloc(key_size) : NULL;
    if (key_bytes && !arr->key_cols)
        goto fail;
    arr->ht_head = calloc(DIFF_ARRANGEMENT_INITIAL_BUCKETS,
            sizeof(*arr->ht_head));
    arr->ht_next = malloc(next_size);
    if (!arr->ht_head || !arr->ht_next)
        goto fail;

    arr->base_nrows = 0;
    arr->current_nrows = 0;
    arr->indexed_rows = 0;
    if (key_bytes)
        memcpy(arr->key_cols, key_cols, key_size);

    if (pending_valid) {
        if (!wl_columnar_memory_commit(&pending, arr)
            || !wl_columnar_memory_reservation_move(
                &arr->reservation, &pending))
            goto fail;
        arr->memory_governor = memory_governor;
        wl_columnar_memory_governor_ref_retain(memory_governor);
        arr->reserved_bytes = initial_bytes;
    }
    return arr;

fail:
    if (pending_valid)
        (void)wl_columnar_memory_release(&pending);
    if (arr) {
        free(arr->key_cols);
        free(arr->ht_head);
        free(arr->ht_next);
        free(arr);
    }
    return NULL;
}

void
col_diff_arrangement_destroy(col_diff_arrangement_t *arr)
{
    if (!arr)
        return;

    if (arr->ledger) {
        uint64_t bytes = col_diff_arrangement_bytes(arr);
        wl_mem_ledger_free(arr->ledger, WL_MEM_SUBSYS_ARRANGEMENT, bytes);
    }
    diff_arr_release_governor(arr);
    free(arr->key_cols);
    free(arr->ht_head);
    free(arr->ht_next);
    free(arr);
}

bool
col_diff_arrangement_has_delta(const col_diff_arrangement_t *arr)
{
    return arr->current_nrows > arr->base_nrows;
}

void
col_diff_arrangement_get_delta_range(const col_diff_arrangement_t *arr,
    uint32_t *out_base_nrows, uint32_t *out_current_nrows)
{
    *out_base_nrows = arr->base_nrows;
    *out_current_nrows = arr->current_nrows;
}

/* Deep-copy the arrangement so a K-fusion worker can evaluate without
 * touching sibling worker state. See docs/ARCHITECTURE.md §4 "K-Fusion
 * Parallelism": "Each worker has isolated copies of differential
 * arrangements" + "Deep-copy isolation simplifies correctness". The
 * worker still reads the shared col_rel_t column buffers but, under the
 * Option (iii) immutable-during-epoch invariant (Issue #532 Task 4),
 * those buffers are not mutated during the read phase, so no locking
 * is required. Isolation is validated by tests/test_diff_arrangement_inline_shadow.c. */
col_diff_arrangement_t *
col_diff_arrangement_deep_copy(const col_diff_arrangement_t *arr)
{
    return col_diff_arrangement_deep_copy_with_memory_governor(arr, NULL);
}

col_diff_arrangement_t *
col_diff_arrangement_deep_copy_with_memory_governor(
    const col_diff_arrangement_t *arr,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    return col_diff_arrangement_deep_copy_with_memory_governor_status(
        arr, memory_governor, NULL);
}

col_diff_arrangement_t *
col_diff_arrangement_deep_copy_with_memory_governor_status(
    const col_diff_arrangement_t *arr,
    wl_columnar_memory_governor_ref_t *memory_governor,
    wl_columnar_memory_admission_status_t *status_out)
{
    wl_columnar_memory_reservation_t pending;
    uint64_t copy_bytes;
    uint64_t key_bytes;
    uint64_t bucket_bytes;
    uint64_t next_bytes;
    size_t key_size;
    size_t bucket_size;
    size_t next_size;
    bool pending_valid = false;
    col_diff_arrangement_t *copy = NULL;

    if (status_out)
        *status_out = WL_COLUMNAR_MEMORY_ADMISSION_OK;
    if (!arr
        || (arr->key_count > 0 && !arr->key_cols)
        || (arr->nbuckets > 0 && !arr->ht_head)
        || (arr->ht_cap > 0 && !arr->ht_next)
        || !wl_columnar_memory_size_mul(arr->key_count, sizeof(uint32_t),
        &key_bytes)
        || !wl_columnar_memory_size_mul(arr->nbuckets, sizeof(uint32_t),
        &bucket_bytes)
        || !wl_columnar_memory_size_mul(arr->ht_cap, sizeof(uint32_t),
        &next_bytes)
        || !diff_shape_bytes(arr->key_count, arr->nbuckets, arr->ht_cap,
        &copy_bytes)
        || !diff_size_arg(key_bytes, &key_size)
        || !diff_size_arg(bucket_bytes, &bucket_size)
        || !diff_size_arg(next_bytes, &next_size))
        return NULL;

    wl_columnar_memory_reservation_init(&pending);
    if (memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(memory_governor),
                copy_bytes, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status_out)
                *status_out = status;
            return NULL;
        }
        pending_valid = true;
    }

    copy = malloc(sizeof(*copy));
    if (!copy)
        goto fail;
    copy->key_cols = key_bytes ? malloc(key_size) : NULL;
    copy->ht_head = bucket_bytes ? malloc(bucket_size) : NULL;
    copy->ht_next = next_bytes ? malloc(next_size) : NULL;
    if ((key_bytes && !copy->key_cols)
        || (bucket_bytes && !copy->ht_head)
        || (next_bytes && !copy->ht_next))
        goto fail;

    copy->key_count = arr->key_count;
    copy->base_nrows = arr->base_nrows;
    copy->current_nrows = arr->current_nrows;
    copy->indexed_rows = arr->indexed_rows;
    copy->worker_id = arr->worker_id;
    copy->nbuckets = arr->nbuckets;
    copy->ht_cap = arr->ht_cap;
    copy->source_snapshot = arr->source_snapshot;
    /* The copy belongs to whoever asked for it (a worker session); the
    * caller attaches the right ledger.  Never inherit the source's. */
    copy->ledger = NULL;
    copy->memory_governor = NULL;
    wl_columnar_memory_reservation_init(&copy->reservation);
    copy->reserved_bytes = 0;
    if (key_bytes)
        memcpy(copy->key_cols, arr->key_cols, key_size);
    if (bucket_bytes)
        memcpy(copy->ht_head, arr->ht_head, bucket_size);
    if (next_bytes)
        memcpy(copy->ht_next, arr->ht_next, next_size);

    if (pending_valid) {
        if (!wl_columnar_memory_commit(&pending, copy)
            || !wl_columnar_memory_reservation_move(
                &copy->reservation, &pending))
            goto fail;
        copy->memory_governor = memory_governor;
        wl_columnar_memory_governor_ref_retain(memory_governor);
        copy->reserved_bytes = copy_bytes;
    }
    return copy;

fail:
    if (pending_valid)
        (void)wl_columnar_memory_release(&pending);
    if (copy) {
        free(copy->key_cols);
        free(copy->ht_head);
        free(copy->ht_next);
        free(copy);
    }
    return NULL;
}

void
col_diff_arrangement_reset_delta(col_diff_arrangement_t *arr)
{
    arr->base_nrows = arr->current_nrows;
}

int
col_diff_arrangement_ensure_ht_capacity(col_diff_arrangement_t *arr,
    uint32_t nrows)
{
    wl_columnar_memory_reservation_t pending;
    uint64_t prospective;
    uint32_t target_cap;
    uint32_t target_buckets;
    uint32_t *new_head = NULL;
    uint32_t *new_next = NULL;
    bool replace_head;
    bool replace_next;
    bool pending_valid = false;
    uint64_t before;
    int failure_rc = ENOMEM;

    if (!arr)
        return EINVAL;
    if (arr->ht_head && arr->ht_next
        && nrows <= arr->ht_cap
        && nrows <= arr->nbuckets - arr->nbuckets / 4u)
        return 0;

    if (!diff_capacity_at_least(arr->ht_cap, nrows, &target_cap)
        || !diff_bucket_capacity(nrows, arr->nbuckets, &target_buckets)
        || !diff_shape_bytes(arr->key_count, target_buckets, target_cap,
        &prospective))
        return EOVERFLOW;
    replace_head = !arr->ht_head || target_buckets != arr->nbuckets;
    replace_next = !arr->ht_next || target_cap != arr->ht_cap;
    before = col_diff_arrangement_bytes(arr);
    if (before == 0)
        return ENOMEM;

    wl_columnar_memory_reservation_init(&pending);
    if (arr->memory_governor && prospective > arr->reserved_bytes) {
        wl_columnar_memory_admission_status_t status;
        status = wl_columnar_memory_reserve_growth(
            wl_columnar_memory_governor_ref_get(arr->memory_governor),
            arr->reserved_bytes, prospective, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                arr->memory_budget_denial_pending = true;
            return ENOMEM;
        }
        pending_valid = true;
    }

    if (replace_next) {
        uint64_t next_bytes;
        if (!wl_columnar_memory_size_mul(target_cap, sizeof(uint32_t),
            &next_bytes)) {
            failure_rc = EOVERFLOW;
            goto fail;
        }
        size_t next_size;
        if (!diff_size_arg(next_bytes, &next_size)) {
            failure_rc = EOVERFLOW;
            goto fail;
        }
        new_next = malloc(next_size);
        if (!new_next)
            goto fail;
        if (!replace_head && arr->ht_next && arr->ht_cap > 0) {
            uint64_t old_next_bytes;
            size_t old_next_size;
            if (!wl_columnar_memory_size_mul(arr->ht_cap,
                sizeof(*new_next), &old_next_bytes)
                || !diff_size_arg(old_next_bytes, &old_next_size)) {
                failure_rc = EOVERFLOW;
                goto fail;
            }
            memcpy(new_next, arr->ht_next, old_next_size);
        }
    }

    if (replace_head) {
        uint64_t head_bytes;
        if (!wl_columnar_memory_size_mul(target_buckets, sizeof(uint32_t),
            &head_bytes)) {
            failure_rc = EOVERFLOW;
            goto fail;
        }
        size_t head_size;
        if (!diff_size_arg(head_bytes, &head_size)) {
            failure_rc = EOVERFLOW;
            goto fail;
        }
        new_head = calloc(1, head_size);
        if (!new_head)
            goto fail;
    }

    if (pending_valid) {
        if (!diff_publish_reservation(arr, &pending, prospective))
            goto fail;
    }

    if (new_head) {
        free(arr->ht_head);
        arr->ht_head = new_head;
        arr->nbuckets = target_buckets;
        arr->indexed_rows = 0; /* Force full re-index by caller */
        new_head = NULL;
    }
    if (new_next) {
        free(arr->ht_next);
        arr->ht_next = new_next;
        arr->ht_cap = target_cap;
        new_next = NULL;
    }
    diff_arr_ledger_sync(arr, before);
    return 0;

fail:
    free(new_head);
    free(new_next);
    if (pending_valid)
        (void)wl_columnar_memory_release(&pending);
    return failure_rc;
}
