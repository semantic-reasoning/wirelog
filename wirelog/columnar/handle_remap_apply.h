/*
 * columnar/handle_remap_apply.h - EDB handle-remap apply pass (Issue #589)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Row-scan helpers that rewrite compound handles in a col_rel_t's
 * column buffers using a build-then-query wl_handle_remap_t.
 *
 * Used by the rotation helper (#550 Option C) to repoint EDB rows
 * from the old session's compound arena to the new session's arena
 * after a handle-renumbering pass.  The pass is single-threaded and
 * runs while the compound arena is frozen (#561 / #584 freeze
 * contract), so the table itself does not need internal
 * synchronisation.
 *
 * Algorithm
 * ---------
 *
 * For each row r of the relation, for each handle-bearing column c
 * in @handle_col_idx:
 *   - read the current handle value h = rel->columns[c][r];
 *   - if h == 0 (WL_COMPOUND_HANDLE_NULL) leave the cell unchanged
 *     (no compound there);
 *   - look h up in the remap table -- on hit, write the new handle
 *     back to the cell; on miss, treat as a corruption signal and
 *     return EIO so the rotation helper can abort cleanly.
 *
 * Total complexity: O(nrows * handle_col_count).  No allocations.
 */

#ifndef WL_COLUMNAR_HANDLE_REMAP_APPLY_H
#define WL_COLUMNAR_HANDLE_REMAP_APPLY_H

#include "handle_remap.h"
#include "internal.h"

#include <stdint.h>

/**
 * wl_handle_remap_apply_columns:
 * @rel:               relation whose handle columns should be rewritten.
 *                     Must not be NULL.
 * @handle_col_idx:    array of physical column indices that hold
 *                     compound handles.  Must not be NULL when
 *                     @handle_col_count > 0.  Each entry must be
 *                     strictly less than @rel->ncols.
 * @handle_col_count:  number of entries in @handle_col_idx.  Zero is
 *                     a no-op.
 * @remap:             populated remap table from a prior arena
 *                     compaction pass.  Must not be NULL.
 * @out_rewrites:      (out, optional): on success, receives the total
 *                     number of cells rewritten across all columns.
 *                     May be NULL.
 *
 * Walk every (row, column) cell in @handle_col_idx and rewrite the
 * stored handle through @remap.  See file header for the full
 * algorithm.
 *
 * Returns:
 *   0 on success.
 *   EINVAL if @rel, @remap, or (handle_col_count > 0 && handle_col_idx
 *          == NULL) is invalid, or if any entry in @handle_col_idx
 *          exceeds @rel->ncols.
 *   EIO   if any non-zero handle in a target cell does not appear in
 *         the remap table.  The rewrite is partial in this case:
 *         columns [0..k-1] of @handle_col_idx are fully rewritten
 *         and column k is rewritten through row r-1, where (r, k) is
 *         the failing cell; (r, k) and beyond are unchanged.
 *         @out_rewrites reflects exactly that prefix.  The relation
 *         is in a half-rotated state — the caller (rotation helper,
 *         #550 Option C) MUST treat it as poisoned: discard, restore
 *         from a snapshot, or abort the rotation.  There is no
 *         in-band roll-back primitive.
 */
int
wl_handle_remap_apply_columns(col_rel_t *rel,
    const uint32_t *handle_col_idx,
    uint32_t handle_col_count,
    const wl_handle_remap_t *remap,
    uint64_t *out_rewrites);

#endif /* WL_COLUMNAR_HANDLE_REMAP_APPLY_H */
