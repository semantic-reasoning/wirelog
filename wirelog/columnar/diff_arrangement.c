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

#include <string.h>

#define DIFF_ARRANGEMENT_INITIAL_BUCKETS 1024

/**
 * col_diff_arrangement - Internal structure for delta-aware hash indexing.
 *
 * Fields:
 *   key_cols[]: Array of column indices forming the key
 *   key_count: Number of key columns
 *   base_nrows: Row count at last reset_delta call (baseline for delta detection)
 *   current_nrows: Current total row count
 *   indexed_rows: Number of rows indexed in the hash table
 *   worker_id: K-fusion worker ID (0 = sequential path, >0 = parallel worker)
 *   ht_head[]: Hash bucket heads (indices into ht_next)
 *   ht_next[]: Hash chain links
 *   nbuckets: Current number of hash buckets
 *   ht_cap: Capacity of ht_head and ht_next arrays
 */
struct col_diff_arrangement
{
    uint32_t *key_cols;
    uint32_t key_count;
    uint32_t base_nrows;
    uint32_t current_nrows;
    uint32_t indexed_rows;
    uint32_t worker_id;
    uint32_t *ht_head;
    uint32_t *ht_next;
    uint32_t nbuckets;
    uint32_t ht_cap;
};

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
