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

    typedef struct {
        col_rel_t *rel;
        col_rel_t *owner;
    } remap_target_t;

    remap_target_t *targets = NULL;
    col_rel_t **owners = NULL;
    wl_columnar_source_access_writer_t *writers = NULL;
    if (sess->nrels > 0) {
        targets = (remap_target_t *)calloc(sess->nrels, sizeof(*targets));
        owners = (col_rel_t **)calloc(sess->nrels, sizeof(*owners));
        writers = (wl_columnar_source_access_writer_t *)calloc(
            sess->nrels, sizeof(*writers));
        if (!targets || !owners || !writers) {
            free(targets);
            free(owners);
            free(writers);
            return ENOMEM;
        }
    }

    uint32_t target_count = 0;
    uint32_t owner_count = 0;
    int rc = 0;
    /* Collect all targets and canonical owners before acquiring anything. */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *rel = sess->rels[i];
        if (!rel || !is_side_relation_name_(rel->name))
            continue;
        col_rel_t *owner = NULL;
        rc = col_rel_storage_owner_resolve(rel, &owner);
        if (rc != 0)
            goto fail;
        if (owner->storage_alias_borrows > 0) {
            rc = EBUSY;
            goto fail;
        }
        targets[target_count++] = (remap_target_t){ rel, owner };
        bool known = false;
        for (uint32_t j = 0; j < owner_count; j++) {
            if (owners[j] == owner) {
                known = true;
                break;
            }
        }
        if (!known)
            owners[owner_count++] = owner;
    }

    /* Stable address ordering is the global lock order for multi-owner ops. */
    for (uint32_t i = 1; i < owner_count; i++) {
        col_rel_t *key = owners[i];
        uint32_t j = i;
        while (j > 0 && (uintptr_t)owners[j - 1] > (uintptr_t)key) {
            owners[j] = owners[j - 1];
            j--;
        }
        owners[j] = key;
    }
    for (uint32_t i = 0; i < owner_count; i++) {
        rc = wl_columnar_source_access_writer_acquire(
            &owners[i]->source_access, &writers[i]);
        if (rc != 0)
            goto fail;
    }

    /* All lookups happen while every owner is excluded, before any write. */
    for (uint32_t i = 0; i < target_count; i++) {
        uint64_t ignored = 0;
        rc = wl_handle_remap_preflight_columns(targets[i].rel,
                (uint32_t[]){ 0u }, 1u, remap, &ignored);
        if (rc != 0)
            goto fail;
    }

    uint64_t cells = 0;
    uint32_t applied_owners = 0;
    for (uint32_t i = 0; i < target_count; i++) {
        bool already_applied = false;
        for (uint32_t j = 0; j < i; j++)
            if (targets[j].owner == targets[i].owner)
                already_applied = true;
        if (already_applied)
            continue;
        uint64_t rewrites = 0;
        rc = wl_handle_remap_apply_columns_held(targets[i].rel,
                (uint32_t[]){ 0u }, 1u, remap, &rewrites);
        if (rc != 0)
            goto fail;
        cells += rewrites;
        applied_owners++;
    }

    /* Cache invalidation is part of the commit and remains under the leases. */
    rc = wl_handle_remap_invalidate_side_relation_caches(sess, NULL);
    if (rc != 0)
        goto fail;
    if (out_rels_rewritten)
        *out_rels_rewritten = target_count;
    if (out_total_cells)
        *out_total_cells = cells;
    (void)applied_owners;

fail:
    for (uint32_t i = owner_count; i > 0; i--) {
        if (writers[i - 1].owner)
            (void)wl_columnar_source_access_writer_release(&writers[i - 1]);
    }
    free(targets);
    free(owners);
    free(writers);
    if (rc != 0) {
        if (out_rels_rewritten)
            *out_rels_rewritten = 0;
        if (out_total_cells)
            *out_total_cells = 0;
        return rc;
    }
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=remap_apply_side_session "
        "rels=%" PRIu32 " cells=%" PRIu64,
        target_count, cells);
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
