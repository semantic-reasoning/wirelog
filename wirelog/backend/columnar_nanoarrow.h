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

/*
 * NOTE: wl_col_session_t and COL_SESSION() are defined in columnar_nanoarrow.c
 * because col_rel_t (a private implementation type) cannot be declared in this
 * header. See columnar_nanoarrow.c for the full memory layout documentation.
 *
 * Summary of the embedding contract:
 *   - wl_col_session_t embeds wl_session_t as its first field (base)
 *   - (wl_col_session_t *)session is safe per C11 §6.7.2.1 ¶15
 *   - session.c:38 sets (*out)->backend after col_session_create returns
 *   - All col_session_* vtable functions cast via COL_SESSION() internally
 *
 * @see backend_dd.c:35-44 for the embedding pattern reference
 * @see session.h:38-40 for canonical wl_session_t definition
 */

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
