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

#include <errno.h>
#include <string.h>

#define DIFF_ARRANGEMENT_INITIAL_BUCKETS 1024

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
    copy->ht_head = malloc(arr->ht_cap * sizeof(uint32_t));
    copy->ht_next = malloc(arr->ht_cap * sizeof(uint32_t));

    if (!copy->key_cols || !copy->ht_head || !copy->ht_next) {
        free(copy->key_cols);
        free(copy->ht_head);
        free(copy->ht_next);
        free(copy);
        return NULL;
    }

    memcpy(copy->key_cols, arr->key_cols, arr->key_count * sizeof(uint32_t));
    memcpy(copy->ht_head, arr->ht_head, arr->ht_cap * sizeof(uint32_t));
    memcpy(copy->ht_next, arr->ht_next, arr->ht_cap * sizeof(uint32_t));

    copy->key_count = arr->key_count;
    copy->base_nrows = arr->base_nrows;
    copy->current_nrows = arr->current_nrows;
    copy->indexed_rows = arr->indexed_rows;
    copy->worker_id = arr->worker_id;
    copy->nbuckets = arr->nbuckets;
    copy->ht_cap = arr->ht_cap;

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
    if (nrows <= arr->ht_cap && nrows <= arr->nbuckets * 3 / 4)
        return 0;

    /* Grow ht_next capacity if needed */
    if (nrows > arr->ht_cap) {
        uint32_t new_cap = arr->ht_cap;
        while (new_cap < nrows)
            new_cap *= 2;
        uint32_t *new_next = realloc(arr->ht_next,
                new_cap * sizeof(uint32_t));
        if (!new_next)
            return ENOMEM;
        arr->ht_next = new_next;
        arr->ht_cap = new_cap;
    }

    /* Grow bucket array if load factor > 75% */
    if (nrows > arr->nbuckets * 3 / 4) {
        uint32_t new_nbuckets = arr->nbuckets;
        while (nrows > new_nbuckets * 3 / 4)
            new_nbuckets *= 2;
        uint32_t *new_head = calloc(new_nbuckets, sizeof(uint32_t));
        if (!new_head)
            return ENOMEM;
        free(arr->ht_head);
        arr->ht_head = new_head;
        arr->nbuckets = new_nbuckets;
        arr->indexed_rows = 0; /* Force full re-index by caller */
    }

    return 0;
}
