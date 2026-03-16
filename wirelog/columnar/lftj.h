/*
 * columnar/lftj.h - Leapfrog Triejoin Public API (Issue #194)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Leapfrog Triejoin (LFTJ): worst-case optimal multi-way join algorithm.
 * Targets memory-efficient DOOP 8-9 way joins that produce large Cartesian
 * intermediates with the FNV-1a binary hash-join cascade.
 *
 * Reference: T. L. Veldhuizen, "Leapfrog Triejoin: A Simple, Worst-Case
 * Optimal Join Algorithm", ICDT 2014.
 *
 * This implementation handles multi-way joins on a SINGLE shared key column
 * across k >= 2 input relations. All relations are sorted on the key column;
 * the leapfrog seek skips non-matching key values without materialising any
 * intermediate relation.
 *
 * Output row format: [key, non_key_cols_rel0..., non_key_cols_rel1..., ...]
 * Peak memory: O(sum(N_i * ncols_i)) for sorted copies only.
 */

#ifndef WL_COLUMNAR_LFTJ_H
#define WL_COLUMNAR_LFTJ_H

#include <stdint.h>

/* Maximum number of input relations supported by wl_lftj_join. */
#define WL_LFTJ_MAX_K 64

/* ======================================================================== */
/* Input Descriptor                                                         */
/* ======================================================================== */

/**
 * wl_lftj_input_t - One relation in a multi-way leapfrog join.
 *
 * @data:    Row-major int64_t buffer (borrowed; caller owns lifetime).
 *           Must contain nrows * ncols elements.
 * @nrows:   Number of rows in the relation.
 * @ncols:   Number of columns per row (>= 1).
 * @key_col: Index of the join key column (must be < ncols).
 */
typedef struct {
    const int64_t *data;
    uint32_t nrows;
    uint32_t ncols;
    uint32_t key_col;
} wl_lftj_input_t;

/* ======================================================================== */
/* Output Callback                                                          */
/* ======================================================================== */

/**
 * wl_lftj_result_fn - Callback invoked for each output tuple.
 *
 * @row:   Output row: [key, non_key_rel0..., non_key_rel1..., ...].
 *         Pointer is valid only for the duration of the callback.
 * @ncols: Total columns in the output row.
 * @user:  Opaque user data passed to wl_lftj_join.
 *
 * Note: Callback cannot cancel enumeration. Pathological key groups
 * with many duplicate rows may produce O(m^k) output tuples for a
 * single key value. Phase 2 will address this via a materialization API.
 */
typedef void (*wl_lftj_result_fn)(const int64_t *row, uint32_t ncols,
                                  void *user);

/* ======================================================================== */
/* Multi-way Join                                                           */
/* ======================================================================== */

/**
 * wl_lftj_join - Multi-way leapfrog triejoin on a single shared key column.
 *
 * For each key value present in ALL k input relations, emits the Cartesian
 * product of matching non-key columns across all relations for that key.
 *
 * Algorithm:
 *   1. Sort each relation by key_col (O(N_i log N_i) per relation).
 *   2. Leapfrog seek: advance iterators toward a common key value.
 *      Each seek uses binary search: O(log N_i).
 *   3. On key agreement: enumerate cross-product of matching rows.
 *   4. Advance all iterators past the matched key and repeat.
 *
 * Output column count: 1 + sum(inputs[i].ncols - 1 for i in 0..k-1).
 *
 * @inputs:  Array of k input relation descriptors (not modified).
 * @k:       Number of input relations. Must be >= 2.
 * @cb:      Output callback; invoked once per output tuple.
 * @user:    Opaque user data forwarded to cb.
 * @return:  0 on success, EINVAL on bad arguments, ENOMEM on OOM.
 */
int
wl_lftj_join(const wl_lftj_input_t *inputs, uint32_t k, wl_lftj_result_fn cb,
             void *user);

#endif /* WL_COLUMNAR_LFTJ_H */
