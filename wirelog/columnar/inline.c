/*
 * columnar/inline.c - wirelog Inline Compound Storage Helpers
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include <errno.h>
#include <stdint.h>

/* Inline Compound Storage (Issue #532 Task 3)                              */
/*                                                                          */
/* Per-row accessors for the inline compound tier. The Task #2 physical     */
/* layout gives each INLINE logical column `compound_arity_map[i]`          */
/* contiguous physical slots; these ops resolve that offset by prefix       */
/* sum and touch only the addressed range. Functor identity is schema-     */
/* level, so no handle is stored per row.                                   */
/* ======================================================================== */

/* Resolve the physical offset and slot width of a logical column in an
 * INLINE-kind relation. Returns 0 and writes *out_offset / *out_width on
 * success; EINVAL if the relation is not INLINE, compound_arity_map is
 * absent, a zero-width entry is seen (corruption), or the prefix sum
 * walks past the physical schema (logical_col out of range).
 *
 * K-Fusion C2 (§5): locate is pure/read-only; failures surface as
 * structured error tags so the fused driver can route the row to the
 * SIDE tier without aborting the worker.
 *
 * Public API (Issue #534 Task #1): FILTER/PROJECT/LFTJ wiring callers
 * use this resolver to translate `logical` column indices emitted by IR
 * lowering (#531) into the `physical` offsets the ops hot paths operate
 * on. Callers must also pass NULL-guarded pointers (rel, out_offset,
 * out_width) — we keep the helper strict to avoid silent misuse in the
 * fused driver. */
int
wl_col_rel_inline_locate(const col_rel_t *rel, uint32_t logical_col,
    uint32_t *out_offset, uint32_t *out_width)
{
    if (!rel || !out_offset || !out_width) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=locate error=null_arg rel=%p out_offset=%p out_width=%p",
            (const void *)rel, (const void *)out_offset,
            (const void *)out_width);
        return EINVAL;
    }
    if (rel->compound_kind != WIRELOG_COMPOUND_KIND_INLINE) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=locate error=not_inline rel=%s kind=%d logical_col=%u",
            rel->name ? rel->name : "(anon)", (int)rel->compound_kind,
            logical_col);
        return EINVAL;
    }
    if (!rel->compound_arity_map) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_ERROR,
            "event=locate error=missing_arity_map rel=%s",
            rel->name ? rel->name : "(anon)");
        return EINVAL;
    }

    uint32_t offset = 0u;
    for (uint32_t i = 0; ; i++) {
        /* Guard: all logical columns have been consumed without finding
         * logical_col.  Checking offset >= ncols BEFORE indexing
         * compound_arity_map prevents an out-of-bounds read when
         * logical_col is >= logical_ncols (e.g. the caller passes 99
         * for a 4-column schema).  The array has exactly logical_ncols
         * entries, and after the last entry offset == ncols. */
        if (offset >= rel->ncols) {
            WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
                "event=locate error=logical_col_oor rel=%s "
                "logical_col=%u physical_ncols=%u",
                rel->name ? rel->name : "(anon)", logical_col, rel->ncols);
            return EINVAL;
        }
        uint32_t width = rel->compound_arity_map[i];
        if (width == 0u) {
            WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_ERROR,
                "event=locate error=corrupt_width rel=%s logical_col=%u "
                "scan_idx=%u",
                rel->name ? rel->name : "(anon)", logical_col, i);
            return EINVAL; /* Task #2 never emits zero-width entries */
        }
        if (offset + width > rel->ncols) {
            WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
                "event=locate error=logical_col_oor rel=%s "
                "logical_col=%u physical_ncols=%u",
                rel->name ? rel->name : "(anon)", logical_col, rel->ncols);
            return EINVAL; /* logical_col beyond end of schema */
        }
        if (i == logical_col) {
            *out_offset = offset;
            *out_width = width;
            return 0;
        }
        offset += width;
    }
}

int
wl_col_rel_store_inline_compound(col_rel_t *rel, uint32_t row_idx,
    uint32_t logical_col, const int64_t *args, uint32_t arity)
{
    if (!rel || !args || arity == 0u) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=store path=inline error=bad_input rel=%p args=%p arity=%u",
            (const void *)rel, (const void *)args, arity);
        return EINVAL;
    }
    if (row_idx >= rel->nrows) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=store path=inline error=row_oor rel=%s row=%u nrows=%u",
            rel->name ? rel->name : "(anon)", row_idx, rel->nrows);
        return EINVAL;
    }

    uint32_t offset = 0u;
    uint32_t width = 0u;
    int rc = wl_col_rel_inline_locate(rel, logical_col, &offset, &width);
    if (rc != 0)
        return rc;
    if (width != arity) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=store path=inline error=arity_mismatch rel=%s "
            "logical_col=%u expected=%u got=%u",
            rel->name ? rel->name : "(anon)", logical_col, width, arity);
        return EINVAL;
    }

    for (uint32_t k = 0; k < arity; k++)
        col_rel_set(rel, row_idx, offset + k, args[k]);
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "event=store path=inline rel=%s row=%u logical_col=%u offset=%u "
        "arity=%u",
        rel->name ? rel->name : "(anon)", row_idx, logical_col, offset, arity);
    return 0;
}
int
wl_col_rel_retrieve_inline_compound(const col_rel_t *rel, uint32_t row_idx,
    uint32_t logical_col, int64_t *out_args, uint32_t arity)
{
    if (!rel || !out_args || arity == 0u) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=retrieve path=inline error=bad_input rel=%p out=%p "
            "arity=%u",
            (const void *)rel, (const void *)out_args, arity);
        return EINVAL;
    }
    if (row_idx >= rel->nrows) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=retrieve path=inline error=row_oor rel=%s row=%u nrows=%u",
            rel->name ? rel->name : "(anon)", row_idx, rel->nrows);
        return EINVAL;
    }

    uint32_t offset = 0u;
    uint32_t width = 0u;
    int rc = wl_col_rel_inline_locate(rel, logical_col, &offset, &width);
    if (rc != 0)
        return rc;
    if (width != arity) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=retrieve path=inline error=arity_mismatch rel=%s "
            "logical_col=%u expected=%u got=%u",
            rel->name ? rel->name : "(anon)", logical_col, width, arity);
        return EINVAL;
    }

    for (uint32_t k = 0; k < arity; k++)
        out_args[k] = col_rel_get(rel, row_idx, offset + k);
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "event=retrieve path=inline rel=%s row=%u logical_col=%u offset=%u "
        "arity=%u",
        rel->name ? rel->name : "(anon)", row_idx, logical_col, offset, arity);
    return 0;
}

int
wl_col_rel_retract_inline_compound(col_rel_t *rel, uint32_t row_idx,
    uint32_t logical_col, int64_t multiplicity)
{
    if (!rel || row_idx >= rel->nrows) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=retract path=inline error=bad_input rel=%p row=%u",
            (const void *)rel, row_idx);
        return EINVAL;
    }
    if (multiplicity == 0) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=retract path=inline error=zero_multiplicity rel=%s "
            "row=%u logical_col=%u",
            rel->name ? rel->name : "(anon)", row_idx, logical_col);
        return EINVAL; /* retraction with zero multiplicity is nonsense */
    }

    uint32_t offset = 0u;
    uint32_t width = 0u;
    int rc = wl_col_rel_inline_locate(rel, logical_col, &offset, &width);
    if (rc != 0)
        return rc;
    /* K-Fusion C3 (§5): physical slots preserved across retraction so the
     * delta/timestamp layer can re-match the tuple during semi-naive. */

    /* Physical slots are deliberately NOT mutated: Z-set multiplicity is
     * tracked at the delta/timestamps layer (col_rel_t::timestamps), and
     * retraction-seeded semi-naive evaluation still needs the original
     * tuple visible for join matching during the retraction pass. The
     * caller is responsible for negating multiplicity in the session
     * delta path; this op validates addressability of the compound. */
    (void)offset;
    (void)width;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_DEBUG,
        "event=retract path=inline rel=%s row=%u logical_col=%u offset=%u "
        "width=%u multiplicity=%lld",
        rel->name ? rel->name : "(anon)", row_idx, logical_col, offset, width,
        (long long)multiplicity);
    return 0;
}

/* ======================================================================== */
/* Inline Compound FILTER/PROJECT/LFTJ Wiring Helpers (Issue #534 Task #1)  */
/*                                                                          */
/* All three helpers are pure, read-only (or payload-copy only) and do not  */
/* touch the Z-set multiplicity layer (Invariant #1). K-Fusion isolation    */
/* is preserved because each helper operates on its passed relation(s) and  */
/* never reaches into sibling-worker state; the `dst` buffer is assumed to  */
/* be per-worker owned (Task #2 deep-copy contract).                        */
/* ======================================================================== */

int
wl_col_rel_inline_arg_physical_col(const col_rel_t *rel, uint32_t logical_col,
    uint32_t arg_idx, uint32_t *out_physical_col)
{
    if (!out_physical_col) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=arg_physical_col error=null_out rel=%p logical_col=%u "
            "arg_idx=%u",
            (const void *)rel, logical_col, arg_idx);
        return EINVAL;
    }
    uint32_t offset = 0u;
    uint32_t width = 0u;
    int rc = wl_col_rel_inline_locate(rel, logical_col, &offset, &width);
    if (rc != 0)
        return rc;
    if (arg_idx >= width) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=arg_physical_col error=arg_oor rel=%s logical_col=%u "
            "arg_idx=%u width=%u",
            rel->name ? rel->name : "(anon)", logical_col, arg_idx, width);
        return EINVAL;
    }
    *out_physical_col = offset + arg_idx;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "event=arg_physical_col rel=%s logical_col=%u arg_idx=%u "
        "physical_col=%u",
        rel->name ? rel->name : "(anon)", logical_col, arg_idx,
        offset + arg_idx);
    return 0;
}

int
wl_col_rel_inline_compound_equals(const col_rel_t *rel, uint32_t row_idx,
    uint32_t logical_col, const int64_t *expect, uint32_t expect_arity)
{
    if (!rel || !expect || expect_arity == 0u) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=equals path=inline error=bad_input rel=%p expect=%p "
            "arity=%u",
            (const void *)rel, (const void *)expect, expect_arity);
        return 0;
    }
    if (row_idx >= rel->nrows) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=equals path=inline error=row_oor rel=%s row=%u nrows=%u",
            rel->name ? rel->name : "(anon)", row_idx, rel->nrows);
        return 0;
    }
    uint32_t offset = 0u;
    uint32_t width = 0u;
    int rc = wl_col_rel_inline_locate(rel, logical_col, &offset, &width);
    if (rc != 0)
        return 0; /* locate failure is treated as a non-match (fallback) */
    if (width != expect_arity) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_DEBUG,
            "event=equals path=inline outcome=arity_mismatch rel=%s "
            "logical_col=%u expected=%u got=%u",
            rel->name ? rel->name : "(anon)", logical_col, expect_arity, width);
        return 0;
    }
    for (uint32_t k = 0; k < expect_arity; k++) {
        if (col_rel_get(rel, row_idx, offset + k) != expect[k]) {
            WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
                "event=equals path=inline outcome=mismatch rel=%s row=%u "
                "logical_col=%u arg=%u",
                rel->name ? rel->name : "(anon)", row_idx, logical_col, k);
            return 0;
        }
    }
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "event=equals path=inline outcome=match rel=%s row=%u logical_col=%u "
        "arity=%u",
        rel->name ? rel->name : "(anon)", row_idx, logical_col, expect_arity);
    return 1;
}

int
wl_col_rel_inline_project_column(col_rel_t *dst, uint32_t dst_row,
    const col_rel_t *src, uint32_t src_row, uint32_t logical_col)
{
    if (!dst || !src) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=project path=inline error=null_arg dst=%p src=%p",
            (const void *)dst, (const void *)src);
        return EINVAL;
    }
    if (dst_row >= dst->nrows || src_row >= src->nrows) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=project path=inline error=row_oor dst=%s dst_row=%u "
            "dst_nrows=%u src=%s src_row=%u src_nrows=%u",
            dst->name ? dst->name : "(anon)", dst_row, dst->nrows,
            src->name ? src->name : "(anon)", src_row, src->nrows);
        return EINVAL;
    }

    uint32_t src_offset = 0u;
    uint32_t src_width = 0u;
    int rc = wl_col_rel_inline_locate(src, logical_col, &src_offset,
            &src_width);
    if (rc != 0)
        return rc;

    uint32_t dst_offset = 0u;
    uint32_t dst_width = 0u;
    rc = wl_col_rel_inline_locate(dst, logical_col, &dst_offset, &dst_width);
    if (rc != 0)
        return rc;

    if (src_width != dst_width) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=project path=inline error=width_mismatch dst=%s src=%s "
            "logical_col=%u src_width=%u dst_width=%u",
            dst->name ? dst->name : "(anon)",
            src->name ? src->name : "(anon)",
            logical_col, src_width, dst_width);
        return EINVAL;
    }

    /* Copy the arity contiguous physical slots. Per-slot col_rel_set keeps
     * the columnar layout consistent (columns[col][row]). K-Fusion C4 (§5):
     * dst is per-worker owned; no cross-worker writes occur here. */
    for (uint32_t k = 0; k < src_width; k++) {
        int64_t v = col_rel_get(src, src_row, src_offset + k);
        col_rel_set(dst, dst_row, dst_offset + k, v);
    }
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "event=project path=inline dst=%s src=%s logical_col=%u width=%u "
        "dst_offset=%u src_offset=%u",
        dst->name ? dst->name : "(anon)", src->name ? src->name : "(anon)",
        logical_col, src_width, dst_offset, src_offset);
    return 0;
}
