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

/* Forward declaration */
typedef struct col_diff_arrangement col_diff_arrangement_t;

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

#endif /* WL_COLUMNAR_DIFF_ARRANGEMENT_H */
