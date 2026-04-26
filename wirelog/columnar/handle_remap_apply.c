/*
 * columnar/handle_remap_apply.c - EDB handle-remap apply pass (Issue #589)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "handle_remap_apply.h"

#include "wirelog/util/log.h"

#include <errno.h>
#include <inttypes.h>

int
wl_handle_remap_apply_columns(col_rel_t *rel,
    const uint32_t *handle_col_idx,
    uint32_t handle_col_count,
    const wl_handle_remap_t *remap,
    uint64_t *out_rewrites)
{
    if (out_rewrites)
        *out_rewrites = 0;
    if (!rel || !remap)
        return EINVAL;
    if (handle_col_count > 0 && !handle_col_idx)
        return EINVAL;
    /* Validate each requested column index up front so we never write
     * past the relation's column array on the row-scan loop below. */
    for (uint32_t k = 0; k < handle_col_count; k++) {
        if (handle_col_idx[k] >= rel->ncols)
            return EINVAL;
    }
    if (handle_col_count == 0 || rel->nrows == 0)
        return 0;

    uint64_t rewrites = 0;
    /* Outer loop is by column so the linear-probe cache footprint
     * (keys[]/values[]) stays warm across rows for the same column.
     * compound_arena_design.md §2 sized the table for the working set;
     * each lookup is O(1) amortised. */
    for (uint32_t k = 0; k < handle_col_count; k++) {
        uint32_t c = handle_col_idx[k];
        int64_t *col = rel->columns[c];
        for (uint32_t r = 0; r < rel->nrows; r++) {
            int64_t old_h = col[r];
            if (old_h == 0)
                continue; /* no compound in this row -- by design */
            int64_t new_h
                = wl_handle_remap_lookup(remap, old_h);
            if (new_h == 0) {
                /* Miss: any non-zero EDB handle must be present in the
                 * remap.  This is a corruption signal; surface it so
                 * the rotation helper can abort. */
                WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_ERROR,
                    "event=remap_apply_miss rel=%s col=%u row=%u "
                    "handle=0x%" PRIx64,
                    rel->name ? rel->name : "(anon)", c, r,
                    (uint64_t)old_h);
                if (out_rewrites)
                    *out_rewrites = rewrites;
                return EIO;
            }
            col[r] = new_h;
            rewrites++;
        }
    }
    if (out_rewrites)
        *out_rewrites = rewrites;
    /* Issue #583 lifecycle audit (COMPOUND section): one summary line
     * per relation makes the pass observable without flooding when the
     * caller iterates many EDB relations. */
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=remap_apply rel=%s cols=%u rows=%u rewrites=%"
        PRIu64,
        rel->name ? rel->name : "(anon)", handle_col_count, rel->nrows,
        rewrites);
    return 0;
}
