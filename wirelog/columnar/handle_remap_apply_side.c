/*
 * columnar/handle_remap_apply_side.c - Side-relation handle remap pass
 *                                       (Issue #590)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "handle_remap_apply_side.h"

#include "columnar_nanoarrow.h"
#include "compound_side.h"
#include "handle_remap_apply.h"
#include "wirelog/util/log.h"

#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

/* Side-relations are named __compound_<functor>_<arity> (#580 /
 * compound_side.c); the prefix is the only invariant the apply path
 * needs.
 *
 * RESERVED PREFIX: the apply path treats every relation whose name
 * begins with "__compound_" as a side-relation managed by the
 * compound subsystem.  User code MUST NOT register relations under
 * this prefix; a collision would be silently sweep-rewritten by
 * wl_handle_remap_apply_session_side_relations and EIO-poisoned if
 * the user's row handles aren't present in the remap.  The prefix
 * convention is owned by #580 / compound_side.c and predates this
 * file; #580 follow-up tracks the col_rel_alloc-time validator. */
#define WL_COMPOUND_SIDE_NAME_PREFIX     "__compound_"
#define WL_COMPOUND_SIDE_NAME_PREFIX_LEN 11u

static int
is_side_relation_name_(const char *name)
{
    if (!name)
        return 0;
    return strncmp(name, WL_COMPOUND_SIDE_NAME_PREFIX,
               WL_COMPOUND_SIDE_NAME_PREFIX_LEN) == 0;
}

int
wl_handle_remap_apply_side_relation(col_rel_t *rel,
    const uint32_t *nested_arg_idx,
    uint32_t nested_arg_count,
    const wl_handle_remap_t *remap,
    uint64_t *out_rewrites)
{
    if (out_rewrites)
        *out_rewrites = 0;
    if (!rel || !remap)
        return EINVAL;
    if (!is_side_relation_name_(rel->name))
        return EINVAL;
    if (nested_arg_count > 0 && !nested_arg_idx)
        return EINVAL;

    /* Build the merged column-index list: column 0 (mandatory, the
     * row's own handle) plus the caller-supplied nested-arg columns.
     * Reject any nested entry that aliases column 0 -- doubling the
     * write would still be safe (idempotent under the same remap)
     * but it would mis-count rewrites and confuses the caller's
     * post-condition reasoning.  Cap by rel->ncols on the upper
     * bound. */
    if (nested_arg_count + 1u < nested_arg_count) /* overflow */
        return EINVAL;
    uint32_t total = nested_arg_count + 1u;
    /* Stack-allocate up to 16 slots; the side-relation arity is small
     * by design (compound_side.c's name buffer assumes <= 5-digit
     * decimal arity, but in practice arities are <= 8).  Heap-fall
     * back if the caller insists on more.  Either way, no allocation
     * on the hot path of typical workloads. */
    enum { STACK_SLOTS = 16 };
    uint32_t stack_idx[STACK_SLOTS];
    uint32_t *idx = NULL;
    int heap = 0;
    if (total <= STACK_SLOTS) {
        idx = stack_idx;
    } else {
        idx = (uint32_t *)malloc((size_t)total * sizeof(uint32_t));
        if (!idx)
            return ENOMEM;
        heap = 1;
    }
    idx[0] = 0u;
    for (uint32_t k = 0; k < nested_arg_count; k++) {
        uint32_t c = nested_arg_idx[k];
        if (c == 0u || c >= rel->ncols) {
            if (heap)
                free(idx);
            return EINVAL;
        }
        idx[k + 1u] = c;
    }

    int rc = wl_handle_remap_apply_columns(rel, idx, total, remap,
            out_rewrites);
    if (heap)
        free(idx);
    return rc;
}

int
wl_handle_remap_apply_session_side_relations(struct wl_col_session_t *sess,
    const wl_handle_remap_t *remap,
    uint64_t *out_rels_rewritten,
    uint64_t *out_total_cells)
{
    if (out_rels_rewritten)
        *out_rels_rewritten = 0;
    if (out_total_cells)
        *out_total_cells = 0;
    if (!sess || !remap)
        return EINVAL;

    uint64_t rels = 0;
    uint64_t cells = 0;
    /* Iterate the session's relation array.  NULL slots can occur
     * after session_remove_rel; skip them.  We only sweep column 0
     * here -- nested args stay caller-driven (see header). */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *rel = sess->rels[i];
        if (!rel)
            continue;
        if (!is_side_relation_name_(rel->name))
            continue;
        uint64_t r = 0;
        int rc = wl_handle_remap_apply_side_relation(rel, NULL, 0u,
                remap, &r);
        cells += r;
        if (rc != 0) {
            /* Propagate EIO with the prefix that succeeded; the
             * relation we failed on is now partially rewritten
             * (poisoned per #589 contract).  The session is also
             * poisoned -- the caller (rotation helper) treats the
             * whole session as a failed rotation. */
            if (out_rels_rewritten)
                *out_rels_rewritten = rels;
            if (out_total_cells)
                *out_total_cells = cells;
            WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_ERROR,
                "event=remap_apply_side_session_eio rel=%s "
                "rels_rewritten=%" PRIu64 " cells=%" PRIu64,
                rel->name ? rel->name : "(anon)", rels, cells);
            return rc;
        }
        rels++;
    }

    if (out_rels_rewritten)
        *out_rels_rewritten = rels;
    if (out_total_cells)
        *out_total_cells = cells;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=remap_apply_side_session "
        "rels=%" PRIu64 " cells=%" PRIu64,
        rels, cells);
    return 0;
}

int
wl_handle_remap_invalidate_side_relation_caches(struct wl_col_session_t *sess,
    uint64_t *out_rels_invalidated)
{
    if (out_rels_invalidated)
        *out_rels_invalidated = 0;
    if (!sess)
        return EINVAL;

    uint64_t touched = 0;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *rel = sess->rels[i];
        if (!rel)
            continue;
        if (!is_side_relation_name_(rel->name))
            continue;

        /* (1) Per-relation arrangement caches (full, filtered,
         * differential).  Existing helper already covers all three
         * keyed-by-row-value caches and is no-op safe when the
         * relation has no entries. */
        col_session_invalidate_arrangements(&sess->base, rel->name);

        /* (2) Per-relation row-dedup hash.  IMPORTANT: dedup_slots is
         * heap-allocated (eval.c lazy-builds it via calloc); free()
         * before clearing the pointer or we leak dedup_cap *
         * sizeof(*dedup_slots) bytes per remapped relation per
         * rotation.  The next consolidation rebuilds the table from
         * the (rewritten) row data. */
        free(rel->dedup_slots);
        rel->dedup_slots = NULL;
        rel->dedup_cap = 0;
        rel->dedup_count = 0;

        touched++;
    }

    if (out_rels_invalidated)
        *out_rels_invalidated = touched;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=remap_invalidate_side_caches rels=%" PRIu64,
        touched);
    return 0;
}
