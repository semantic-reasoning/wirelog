/*
 * columnar/compound_side.h - Side-Relation Auto-Creation API (Issue #533)
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
 * Declared or undeclared compounds that fall outside the inline tier's
 * arity/nesting thresholds are stored in a side-relation named
 *
 *      __compound_<functor>_<arity>
 *
 * whose schema is
 *
 *      (handle, arg0, arg1, ..., arg{arity-1})
 *
 * Each column is int64.  The `handle` column is the 64-bit handle
 * returned by wl_compound_arena_alloc; arg0..arg{N-1} hold the functor
 * arguments (intern IDs, numeric literals, or nested compound handles).
 *
 * wl_compound_side_ensure() is idempotent: re-calling it for the same
 * (functor, arity) pair returns the existing col_rel_t without
 * allocating a new one.
 *
 * No runtime dispatch: the relation name is computed at compile time
 * (via snprintf at call time, not per row) and registered in the
 * session relation map once.  All subsequent accesses go through the
 * existing session_find_rel fast path (hash lookup, O(1)).
 */

#ifndef WL_COLUMNAR_COMPOUND_SIDE_H
#define WL_COLUMNAR_COMPOUND_SIDE_H

#include "internal.h"

#include <stdbool.h>
#include <stdint.h>

/**
 * Maximum length of a side-relation name buffer, including terminator.
 * "__compound_" (11) + max functor identifier (~64) + "_" + arity (~3) + NUL.
 */
#define WL_COMPOUND_SIDE_NAME_MAX 96u

/**
 * wl_compound_side_name:
 * @functor: null-terminated functor identifier (e.g. "metadata").
 * @arity:   compound arity (number of arguments).
 * @buf:     output buffer (must be at least WL_COMPOUND_SIDE_NAME_MAX bytes).
 * @bufsz:   size of @buf.
 *
 * Format the canonical side-relation name "__compound_<functor>_<arity>"
 * into @buf.  Returns 0 on success, -1 if the buffer is too small or any
 * argument is invalid.
 */
int
wl_compound_side_name(const char *functor, uint32_t arity,
    char *buf, size_t bufsz);

/**
 * wl_compound_side_ensure:
 * @sess:     columnar session.
 * @functor:  null-terminated functor identifier.
 * @arity:    compound arity (must be > 0).
 * @out_rel:  (out, optional) relation pointer on success.
 *
 * Idempotently create and register the __compound_<functor>_<arity>
 * side-relation on @sess with schema (handle, arg0, ..., arg{arity-1}).
 * If the relation already exists, returns it without side effects.
 *
 * Returns 0 on success, EINVAL for bad arguments, ENOMEM on allocation
 * failure.  On failure, *out_rel is set to NULL.
 */
int
wl_compound_side_ensure(wl_col_session_t *sess, const char *functor,
    uint32_t arity, col_rel_t **out_rel);

#endif /* WL_COLUMNAR_COMPOUND_SIDE_H */
