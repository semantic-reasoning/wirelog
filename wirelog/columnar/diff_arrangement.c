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
    if (!arr)
        return 0;
    uint64_t bytes = sizeof(*arr);
    bytes += (uint64_t)arr->key_count * sizeof(uint32_t);
    bytes += (uint64_t)arr->nbuckets * sizeof(uint32_t);
    bytes += (uint64_t)arr->ht_cap * sizeof(uint32_t);
    return bytes;
}

/* Charge or credit the difference between @before and the current
 * footprint under ARRANGEMENT (Issue #1380). */
static void
diff_arr_ledger_sync(col_diff_arrangement_t *arr, uint64_t before)
{
    if (!arr || !arr->ledger)
        return;
    uint64_t after = col_diff_arrangement_bytes(arr);
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
    status = wl_columnar_memory_reserve_checked(governor,
            col_diff_arrangement_bytes(arr), &arr->reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
        return ENOMEM;
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return EINVAL;
    if (!wl_columnar_memory_commit(&arr->reservation, arr)) {
        (void)wl_columnar_memory_release(&arr->reservation);
        return ENOMEM;
    }
    arr->memory_governor = memory_governor;
    wl_columnar_memory_governor_ref_retain(memory_governor);
    arr->reserved_bytes = col_diff_arrangement_bytes(arr);
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
    col_diff_arrangement_t *arr = malloc(sizeof(*arr));
    if (!arr)
        return NULL;

    arr->key_cols = malloc(key_count * sizeof(uint32_t));
    if (!arr->key_cols) {
        free(arr);
        return NULL;
    }
    memcpy(arr->key_cols, key_cols, key_count * sizeof(uint32_t));

    arr->key_count = key_count;
    arr->base_nrows = 0;
    arr->current_nrows = 0;
    arr->indexed_rows = 0;
    arr->worker_id = worker_id;
    arr->nbuckets = DIFF_ARRANGEMENT_INITIAL_BUCKETS;
    arr->ht_cap = DIFF_ARRANGEMENT_INITIAL_BUCKETS;
    arr->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
    arr->ledger = NULL;
    arr->memory_governor = NULL;
    wl_columnar_memory_reservation_init(&arr->reservation);
    arr->reserved_bytes = 0;

    arr->ht_head = calloc(arr->ht_cap, sizeof(uint32_t));
    arr->ht_next = malloc(arr->ht_cap * sizeof(uint32_t));

    if (!arr->ht_head || !arr->ht_next) {
        free(arr->key_cols);
        free(arr->ht_head);
        free(arr->ht_next);
        free(arr);
        return NULL;
    }

    return arr;
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
 * is required. Isolation is validated by tests/test_k_fusion_inline_shadow.c. */
col_diff_arrangement_t *
col_diff_arrangement_deep_copy(const col_diff_arrangement_t *arr)
{
    col_diff_arrangement_t *copy = malloc(sizeof(*copy));
    if (!copy)
        return NULL;

    copy->key_cols = malloc(arr->key_count * sizeof(uint32_t));
    copy->ht_head = arr->nbuckets
        ? malloc(arr->nbuckets * sizeof(uint32_t)) : NULL;
    copy->ht_next = arr->ht_cap
        ? malloc(arr->ht_cap * sizeof(uint32_t)) : NULL;

    if (!copy->key_cols || (arr->nbuckets && !copy->ht_head)
        || (arr->ht_cap && !copy->ht_next)) {
        free(copy->key_cols);
        free(copy->ht_head);
        free(copy->ht_next);
        free(copy);
        return NULL;
    }

    memcpy(copy->key_cols, arr->key_cols, arr->key_count * sizeof(uint32_t));
    if (arr->nbuckets)
        memcpy(copy->ht_head, arr->ht_head, arr->nbuckets * sizeof(uint32_t));
    if (arr->ht_cap)
        memcpy(copy->ht_next, arr->ht_next, arr->ht_cap * sizeof(uint32_t));

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

    return copy;
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
    wl_columnar_memory_governor_t *governor = NULL;
    uint64_t prospective;
    uint32_t target_cap;
    uint32_t target_buckets;
    bool pending_valid = false;

    if (arr->ht_head && arr->ht_next
        && nrows <= arr->ht_cap && nrows <= arr->nbuckets * 3 / 4)
        return 0;

    target_cap = arr->ht_cap ? arr->ht_cap : DIFF_ARRANGEMENT_INITIAL_BUCKETS;
    while (target_cap < nrows)
        target_cap *= 2u;
    target_buckets = arr->nbuckets ? arr->nbuckets
        : DIFF_ARRANGEMENT_INITIAL_BUCKETS;
    while (nrows > target_buckets * 3 / 4)
        target_buckets *= 2u;
    prospective = sizeof(*arr)
        + (uint64_t)arr->key_count * sizeof(uint32_t)
        + (uint64_t)target_buckets * sizeof(uint32_t)
        + (uint64_t)target_cap * sizeof(uint32_t);
    if (arr->memory_governor && prospective > arr->reserved_bytes) {
        wl_columnar_memory_admission_status_t status;
        governor = wl_columnar_memory_governor_ref_get(arr->memory_governor);
        wl_columnar_memory_reservation_init(&pending);
        status = governor ? wl_columnar_memory_reserve_growth(governor,
                arr->reserved_bytes, prospective, &pending)
            : WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return ENOMEM;
        pending_valid = true;
    }

    uint64_t before = arr->ledger ? col_diff_arrangement_bytes(arr) : 0;

    /* Grow ht_next capacity if needed */
    if (!arr->ht_next || nrows > arr->ht_cap) {
        uint32_t new_cap = arr->ht_cap ? arr->ht_cap
            : DIFF_ARRANGEMENT_INITIAL_BUCKETS;
        while (new_cap < nrows)
            new_cap *= 2;
        uint32_t *new_next = realloc(arr->ht_next,
                new_cap * sizeof(uint32_t));
        if (!new_next) {
            if (pending_valid)
                (void)wl_columnar_memory_release(&pending);
            diff_arr_ledger_sync(arr, before);
            return ENOMEM;
        }
        arr->ht_next = new_next;
        arr->ht_cap = new_cap;
    }

    /* Grow bucket array if load factor > 75% */
    if (!arr->ht_head || nrows > arr->nbuckets * 3 / 4) {
        uint32_t new_nbuckets = arr->nbuckets ? arr->nbuckets
            : DIFF_ARRANGEMENT_INITIAL_BUCKETS;
        while (nrows > new_nbuckets * 3 / 4)
            new_nbuckets *= 2;
        uint32_t *new_head = calloc(new_nbuckets, sizeof(uint32_t));
        if (!new_head) {
            if (pending_valid)
                (void)wl_columnar_memory_release(&pending);
            diff_arr_ledger_sync(arr, before);
            return ENOMEM;
        }
        free(arr->ht_head);
        arr->ht_head = new_head;
        arr->nbuckets = new_nbuckets;
        arr->indexed_rows = 0; /* Force full re-index by caller */
    }
    if (pending_valid) {
        wl_columnar_memory_reservation_t previous;
        wl_columnar_memory_reservation_init(&previous);
        if (!wl_columnar_memory_reservation_move(&previous,
            &arr->reservation)
            || !wl_columnar_memory_reservation_move(&arr->reservation,
            &pending)
            || !wl_columnar_memory_release(&previous)) {
            (void)wl_columnar_memory_release(&pending);
            return ENOMEM;
        }
        arr->reserved_bytes = prospective;
    }

    diff_arr_ledger_sync(arr, before);
    return 0;
}
