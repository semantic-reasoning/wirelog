/*
 * columnar/diff_trace.c - Differential Trace Implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Lattice timestamp implementation for differential dataflow evaluation.
 * Provides multi-worker tracing and convergence detection.
 */

#include "diff_trace.h"

#include <stddef.h>

/**
 * col_diff_trace_t is a cache-aligned 16-byte structure:
 *   - outer_epoch (uint32_t): Insertion epoch
 *   - iteration (uint32_t): Fixed-point iteration within epoch
 *   - worker (uint32_t): K-fusion worker ID
 *   - _reserved (uint32_t): Padding for future use
 *
 * The implementation uses static inline functions defined in the header for
 * performance-critical operations (compare, join, convergence checks).
 *
 * This file provides compile-time assertions and any future out-of-line
 * implementations as needed.
 */

#ifdef __STDC_VERSION__
#if __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(col_diff_trace_t) == 16,
    "col_diff_trace_t must be exactly 16 bytes for cache alignment");

_Static_assert(offsetof(col_diff_trace_t, outer_epoch) == 0,
    "outer_epoch must be at offset 0");

_Static_assert(offsetof(col_diff_trace_t, iteration) == 4,
    "iteration must be at offset 4");

_Static_assert(offsetof(col_diff_trace_t, worker) == 8,
    "worker must be at offset 8");

_Static_assert(offsetof(col_diff_trace_t, _reserved) == 12,
    "_reserved must be at offset 12");
#endif
#endif
