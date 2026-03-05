/*
 * backend/columnar_nanoarrow.h - wirelog Nanoarrow Columnar Backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * ========================================================================
 * Overview
 * ========================================================================
 *
 * The nanoarrow columnar backend stores relations in row-major int64_t
 * buffers and uses Apache Arrow schemas (via nanoarrow) for type metadata.
 * Evaluation uses a stack-based relational algebra interpreter executing
 * the wl_ffi_plan_t operator sequence with semi-naive fixed-point
 * iteration for recursive strata.
 *
 * ========================================================================
 * Evaluation Model
 * ========================================================================
 *
 * Plan operators are emitted in post-order (left child first), forming a
 * stack machine:
 *
 *   VARIABLE(rel)   -> push named relation onto eval stack
 *   MAP(indices)    -> pop, project columns, push result
 *   FILTER(expr)    -> pop, apply predicate, push filtered result
 *   JOIN(right,keys)-> pop left, join with named right, push output
 *   ANTIJOIN(...)   -> pop left, remove rows matching right, push
 *   CONCAT          -> pop top two, concatenate, push union
 *   CONSOLIDATE     -> pop, sort+deduplicate, push
 *   REDUCE(agg)     -> pop, group-by + aggregate, push
 *   SEMIJOIN(...)   -> pop left, semijoin with right, push left cols only
 *
 * Column name tracking: each stack entry carries column names for
 * variable→position resolution in JOIN conditions.
 */

#ifndef WL_BACKEND_COLUMNAR_NANOARROW_H
#define WL_BACKEND_COLUMNAR_NANOARROW_H

#include "../backend.h"

/**
 * wl_backend_columnar:
 *
 * Return the singleton vtable for the nanoarrow columnar backend.
 * The backend is thread-compatible: each session is independent.
 * Sessions are NOT thread-safe; external locking required for sharing.
 */
const wl_compute_backend_t *
wl_backend_columnar(void);

#endif /* WL_BACKEND_COLUMNAR_NANOARROW_H */
