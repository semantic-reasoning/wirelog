/*
 * columnar/diff_arrangement.h - Differential Arrangement Structures
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Delta-aware hash table for incremental indexing with base/current row tracking.
 * Supports deep-copy isolation pattern for K-fusion worker parallelism.
 */

#ifndef WL_COLUMNAR_DIFF_ARRANGEMENT_H
#define WL_COLUMNAR_DIFF_ARRANGEMENT_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * col_diff_arrangement - Delta-aware hash table for incremental indexing.
 *
 * Fields:
 *   key_cols[]: Array of column indices forming the key
 *   key_count: Number of key columns
 *   base_nrows: Row count at last reset_delta call (baseline for delta detection)
 *   current_nrows: Current total row count
 *   indexed_rows: Number of rows indexed in the hash table
 *   worker_id: K-fusion worker ID (0 = sequential path, >0 = parallel worker)
 *   ht_head[]: Hash bucket heads (1-based indices into ht_next; 0 = empty)
 *   ht_next[]: Hash chain links (1-based; 0 = end of chain)
 *   nbuckets: Current number of hash buckets (power of 2)
 *   ht_cap: Capacity of ht_next array
 */
typedef struct col_diff_arrangement {
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
} col_diff_arrangement_t;

/**
 * col_diff_arrangement_create - Create a new differential arrangement.
 *
 * @param key_cols: Array of column indices to use as keys
 * @param key_count: Number of key columns
 * @param worker_id: K-fusion worker ID (0 for sequential path)
 *
 * Returns: Pointer to newly allocated arrangement (caller must free with col_diff_arrangement_destroy)
 */
col_diff_arrangement_t *col_diff_arrangement_create(
    const uint32_t *key_cols, uint32_t key_count, uint32_t worker_id);

/**
 * col_diff_arrangement_destroy - Deallocate a differential arrangement.
 *
 * Safe to call multiple times (idempotent).
 */
void col_diff_arrangement_destroy(col_diff_arrangement_t *arr);

/**
 * col_diff_arrangement_has_delta - Check if arrangement has new rows since last reset.
 *
 * Returns: true if current_nrows > base_nrows
 */
bool col_diff_arrangement_has_delta(const col_diff_arrangement_t *arr);

/**
 * col_diff_arrangement_get_delta_range - Get the range of new rows.
 *
 * @param arr: Arrangement to query
 * @param out_base_nrows: (out) Starting row index (= base_nrows)
 * @param out_current_nrows: (out) Ending row index (= current_nrows)
 */
void col_diff_arrangement_get_delta_range(
    const col_diff_arrangement_t *arr,
    uint32_t *out_base_nrows,
    uint32_t *out_current_nrows);

/**
 * col_diff_arrangement_deep_copy - Create an independent copy for K-fusion worker.
 *
 * Allocates new memory for the arrangement and all hash table buckets.
 * Each K-fusion worker gets an independent copy to avoid synchronization.
 *
 * Contract (docs/ARCHITECTURE.md §4 "K-Fusion Parallelism"):
 *   - "Each worker has isolated copies of differential arrangements
 *      (no cross-worker synchronization)"
 *   - "Deep-copy isolation simplifies correctness (no locks needed
 *      for K-fusion workers)"
 *
 * Interaction with inline compound storage (Issue #532 Task 4):
 *   Workers index the same underlying col_rel_t column buffers but
 *   through private ht_head / ht_next arrays produced by this function.
 *   The Option (iii) immutable-during-epoch strategy guarantees those
 *   column buffers are not written during the worker read phase, so
 *   concurrent retrieves of inline compounds are race-free without any
 *   locking. See tests/test_k_fusion_inline_shadow.c.
 *
 * @param arr: Source arrangement
 *
 * Returns: Pointer to new independent copy (caller must free with col_diff_arrangement_destroy)
 */
col_diff_arrangement_t *col_diff_arrangement_deep_copy(
    const col_diff_arrangement_t *arr);

/**
 * col_diff_arrangement_reset_delta - Mark current rows as base for next epoch.
 *
 * Sets base_nrows = current_nrows so next iteration detects only new rows.
 */
void col_diff_arrangement_reset_delta(col_diff_arrangement_t *arr);

/**
 * col_diff_arrangement_ensure_ht_capacity - Grow hash table for nrows.
 *
 * Ensures ht_next can hold at least nrows entries. If load factor exceeds
 * 75%, rebuilds bucket array (sets indexed_rows = 0 to force full re-index).
 *
 * Returns: 0 on success, ENOMEM on allocation failure
 */
int col_diff_arrangement_ensure_ht_capacity(col_diff_arrangement_t *arr,
    uint32_t nrows);

#endif /* WL_COLUMNAR_DIFF_ARRANGEMENT_H */
