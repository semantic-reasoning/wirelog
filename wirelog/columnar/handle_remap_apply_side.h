/*
 * columnar/handle_remap_apply_side.h - Side-relation handle remap pass
 *                                       (Issue #590)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Layered on top of #589's wl_handle_remap_apply_columns primitive.
 * #590 adds two thin wrappers tailored to the side-relation tier
 * (#580 / compound_side.{c,h}):
 *
 *   1. wl_handle_remap_apply_side_relation: per-relation primitive.
 *      Always rewrites column 0 (the row's own handle, by side-
 *      relation contract).  Optionally rewrites caller-supplied
 *      "nested handle" arg-column indices (e.g. arg0 of a side-
 *      relation row that stores a handle into another side-
 *      relation, the f(g(...)) case).  Arg columns that hold scalars
 *      (intern IDs, numeric literals) MUST NOT appear in the nested
 *      list -- otherwise a literal that collides with an old handle
 *      gets silently rewritten.
 *
 *   2. wl_handle_remap_apply_session_side_relations: session-level
 *      driver.  Iterates sess->rels[], filters by the
 *      "__compound_<functor>_<arity>" name prefix (#580 convention),
 *      and rewrites column 0 of each match.  Nested-arg rewrites
 *      stay caller-driven because there is no schema-level "this
 *      arg holds a handle" tag today; callers that know the nested
 *      structure (rotation helper #550 Option C) iterate side-
 *      relations themselves and call the per-relation primitive.
 *
 * Multiplicity (Z-set) is preserved by construction: the apply pass
 * touches col_rel_t::columns[][] only; col_rel_t::timestamps[] (which
 * carries the per-row multiplicity) is untouched.
 */

#ifndef WL_COLUMNAR_HANDLE_REMAP_APPLY_SIDE_H
#define WL_COLUMNAR_HANDLE_REMAP_APPLY_SIDE_H

#include "handle_remap.h"
#include "internal.h"

#include <stdint.h>

/**
 * wl_handle_remap_apply_side_relation:
 * @rel:                a side-relation (name must start with
 *                      "__compound_").  Must not be NULL.
 * @nested_arg_idx:     (optional): physical column indices of arg
 *                      columns that hold nested compound handles.
 *                      May be NULL when @nested_arg_count == 0.
 *                      Each entry must be in [1, rel->ncols).  Index
 *                      0 is implicit (always rewritten as the row's
 *                      own handle column) and must NOT appear here.
 * @nested_arg_count:   number of entries in @nested_arg_idx.
 * @remap:              populated remap table.  Must not be NULL.
 * @out_rewrites:       (out, optional): on success, total cells
 *                      rewritten across all touched columns
 *                      (column 0 + nested args).  May be NULL.
 *
 * Rewrite the row's own handle (column 0) plus every cell in
 * @nested_arg_idx.  Delegates the row-scan + lookup + EIO-on-miss
 * mechanics to wl_handle_remap_apply_columns; the half-rotation
 * contract (relation poisoned on EIO, no in-band rollback) carries
 * over verbatim.
 *
 * Returns:
 *   0 on success.
 *   EINVAL if @rel or @remap is NULL, if @rel is not a side-relation
 *          (name does not match the __compound_ prefix), if
 *          @nested_arg_count > 0 with NULL @nested_arg_idx, if any
 *          @nested_arg_idx entry is 0 (would alias the implicit
 *          handle column) or >= @rel->ncols.
 *   EIO    if any non-zero handle in column 0 or @nested_arg_idx
 *          is missing from @remap.  See wl_handle_remap_apply_columns
 *          for the partial-rewrite/poisoned-relation contract.
 */
int
wl_handle_remap_apply_side_relation(col_rel_t *rel,
    const uint32_t *nested_arg_idx,
    uint32_t nested_arg_count,
    const wl_handle_remap_t *remap,
    uint64_t *out_rewrites);

/**
 * wl_handle_remap_apply_session_side_relations:
 * @sess:                session whose side-relations should be
 *                       rewritten.  Must not be NULL.
 * @remap:               populated remap table.  Must not be NULL.
 * @out_rels_rewritten:  (out, optional): number of side-relations
 *                       whose column 0 was scanned (each match
 *                       contributes one).
 * @out_total_cells:     (out, optional): cumulative cell rewrite
 *                       count across every matched side-relation's
 *                       column 0.
 *
 * Walk @sess->rels[0..nrels), filter by the __compound_ name prefix,
 * and rewrite column 0 of each matched relation through @remap.
 *
 * This driver covers ONLY column 0 (the row's own handle).  Nested
 * arg-column rewrites stay caller-driven via
 * wl_handle_remap_apply_side_relation, because the side-relation
 * schema does not currently tag which arg columns hold nested
 * handles vs scalar payload.  A blind sweep over every arg column
 * would silently corrupt intern IDs / numeric literals that happen
 * to collide with an old handle -- exactly the failure mode the
 * critic flagged in #590's pre-review.
 *
 * Returns:
 *   0 on success.
 *   EINVAL if @sess or @remap is NULL.
 *   EIO    on the first relation whose column 0 fails the lookup;
 *          the iteration aborts at that point.  @out_rels_rewritten
 *          and @out_total_cells reflect the prefix that succeeded.
 *          Per #589's contract the failing relation is partially
 *          rewritten through the row before the missing handle and
 *          must be treated as poisoned by the caller.
 */
int
wl_handle_remap_apply_session_side_relations(struct wl_col_session_t *sess,
    const wl_handle_remap_t *remap,
    uint64_t *out_rels_rewritten,
    uint64_t *out_total_cells);

/**
 * wl_handle_remap_invalidate_side_relation_caches:
 * @sess:                 session whose side-relation caches should be
 *                        invalidated.  Must not be NULL.
 * @out_rels_invalidated: (out, optional): number of side-relations
 *                        touched by the invalidation pass.
 *
 * Post-remap cache cleanup (#591).  Walks @sess->rels[] looking for
 * the __compound_ name prefix and, for each matched relation:
 *
 *   1. Calls col_session_invalidate_arrangements(@sess, rel->name).
 *      That helper (already shipped, arrangement.c) clears the full,
 *      filtered, and differential arrangement caches whose hash keys
 *      include the (now stale) handle column values.
 *
 *   2. Frees and zeroes the per-relation row-dedup hash:
 *           free(rel->dedup_slots);
 *           rel->dedup_slots = NULL;
 *           rel->dedup_cap = 0; rel->dedup_count = 0;
 *      The next consolidation rebuilds the dedup table from the
 *      rewritten row data (eval.c).  Setting only the pointer to
 *      NULL would leak the prior allocation.
 *
 * Out of scope for #591 (call out for #598 / follow-up):
 *   - mat_cache is content-keyed and may carry stale join results
 *     over remapped sources.  Whether to clear or accept the
 *     content-hash miss is left to the rotation helper (#550-C);
 *     this pass does NOT touch mat_cache.
 *   - sarr_entries (sorted-by-key permutations) are not currently
 *     touched by col_session_invalidate_arrangements.  Side-
 *     relations are not expected to have sarr entries today, so
 *     the gap is harmless until a side-relation gets sorted; that
 *     extension belongs to a follow-up that grows
 *     col_session_invalidate_arrangements rather than this pass.
 *
 * Multiplicity (Z-set): the invalidation pass touches per-relation
 * cache fields only.  col_rel_t::timestamps[] (which carries per-
 * row multiplicity) is never read or written.
 *
 * Returns:
 *   0 on success.
 *   EINVAL if @sess is NULL.
 *
 * NULL-safe on a session with zero side-relations.  No EIO path:
 * the underlying invalidation helpers are infallible.
 */
int
wl_handle_remap_invalidate_side_relation_caches(struct wl_col_session_t *sess,
    uint64_t *out_rels_invalidated);

#endif /* WL_COLUMNAR_HANDLE_REMAP_APPLY_SIDE_H */
