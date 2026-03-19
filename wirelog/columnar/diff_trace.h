/*
 * columnar/diff_trace.h - Differential Trace Structures
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Lattice timestamp representation for differential dataflow evaluation.
 * Supports multi-worker deep-copy isolation and efficient memory layout.
 */

#ifndef WL_COLUMNAR_DIFF_TRACE_H
#define WL_COLUMNAR_DIFF_TRACE_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

/* ======================================================================== */
/* Differential Trace (Issue #259)                                          */
/* ======================================================================== */

/**
 * col_diff_trace_t - Lattice timestamp for differential dataflow.
 *
 * Represents a point in the (epoch × iteration) lattice used for tracking
 * provenance and ordering in differential dataflow evaluation.
 *
 * Layout (16 bytes, cache-aligned):
 *   uint32_t outer_epoch     Insertion epoch (incremented by session_insert_incremental)
 *   uint32_t iteration       Fixed-point iteration within epoch (0-based)
 *   uint32_t worker          K-fusion worker index (0 = sequential path)
 *   uint32_t _reserved       Reserved for future use (must be zero)
 *
 * Ordering (lattice semilattice with join):
 *   - Epoch-first: (e1, i1) < (e2, i2) iff e1 < e2 OR (e1 == e2 AND i1 < i2)
 *   - Join operation: max((e1, i1), (e2, i2)) = lexicographic maximum
 *   - Convergence: finite iteration (not UINT32_MAX) indicates convergence at epoch/iter
 */
typedef struct
{
    uint32_t outer_epoch;
    uint32_t iteration;
    uint32_t worker;
    uint32_t _reserved;
} col_diff_trace_t;

/**
 * col_diff_trace_init - Initialize a trace to a specific epoch and iteration.
 */
static inline col_diff_trace_t
col_diff_trace_init(uint32_t epoch, uint32_t iter, uint32_t worker_id)
{
    return (col_diff_trace_t){
               .outer_epoch = epoch,
               .iteration = iter,
               .worker = worker_id,
               ._reserved = 0,
    };
}

/**
 * col_diff_trace_compare - Compare two traces in lattice order.
 *
 * Returns: < 0 if t1 < t2, 0 if t1 == t2, > 0 if t1 > t2
 */
static inline int
col_diff_trace_compare(const col_diff_trace_t *t1, const col_diff_trace_t *t2)
{
    if (t1->outer_epoch != t2->outer_epoch)
        return (t1->outer_epoch < t2->outer_epoch) ? -1 : 1;
    if (t1->iteration != t2->iteration)
        return (t1->iteration < t2->iteration) ? -1 : 1;
    return 0;
}

/**
 * col_diff_trace_join - Compute lattice join (max) of two traces.
 *
 * Returns the lexicographic maximum in epoch × iteration order.
 */
static inline col_diff_trace_t
col_diff_trace_join(const col_diff_trace_t *t1, const col_diff_trace_t *t2)
{
    if (col_diff_trace_compare(t1, t2) >= 0)
        return *t1;
    return *t2;
}

/**
 * col_diff_trace_has_converged - Check if trace represents convergence.
 *
 * A trace has converged if iteration is not UINT32_MAX (sentinel for "not set").
 */
static inline bool
col_diff_trace_has_converged(const col_diff_trace_t *t)
{
    return t->iteration != UINT32_MAX;
}

/**
 * col_diff_trace_reset_for_epoch - Reset trace to epoch with max iteration sentinel.
 */
static inline void
col_diff_trace_reset_for_epoch(col_diff_trace_t *t, uint32_t new_epoch)
{
    t->outer_epoch = new_epoch;
    t->iteration = UINT32_MAX;
}

#endif /* WL_COLUMNAR_DIFF_TRACE_H */
