/*
 * columnar/compound_side.c - Side-Relation Auto-Creation (Issue #533)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements wl_compound_side_ensure: idempotent registration of
 * __compound_<functor>_<arity> side-relations on the columnar session.
 */

#include "compound_side.h"

#include "internal.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Column-name buffer size: "arg" + up to 10-digit decimal + NUL. */
#define WL_COMPOUND_SIDE_COLNAME_MAX 16u

int
wl_compound_side_name(const char *functor, uint32_t arity,
    char *buf, size_t bufsz)
{
    if (!functor || !buf || bufsz == 0)
        return -1;
    int n = snprintf(buf, bufsz, "__compound_%s_%u", functor, arity);
    if (n < 0 || (size_t)n >= bufsz)
        return -1;
    return 0;
}

int
wl_compound_side_ensure(wl_col_session_t *sess, const char *functor,
    uint32_t arity, col_rel_t **out_rel)
{
    if (out_rel)
        *out_rel = NULL;
    /* Issue #580: side-relation rows carry a compound handle in column 0,
     * so the session must own a compound arena before a side-relation can
     * be registered.  Refusing here surfaces the misconfiguration at the
     * registration site instead of letting downstream allocations silently
     * skip into the arena's frozen / NULL fallback. */
    if (!sess || !sess->compound_arena || !functor || arity == 0)
        return EINVAL;

    char name[WL_COMPOUND_SIDE_NAME_MAX];
    if (wl_compound_side_name(functor, arity, name, sizeof(name)) != 0)
        return EINVAL;

    /* Idempotent: return existing relation if present. */
    col_rel_t *existing = session_find_rel(sess, name);
    if (existing) {
        if (out_rel)
            *out_rel = existing;
        return 0;
    }

    /* Allocate new relation with schema (handle, arg0, ..., arg{arity-1}). */
    col_rel_t *rel = NULL;
    int rc = col_rel_alloc(&rel, name);
    if (rc != 0)
        return rc;

    uint32_t ncols = arity + 1; /* +1 for the handle column */
    /* Build the column-name array as a heap-allocated pointer array
     * backed by a single contiguous buffer.  col_rel_set_schema takes
     * its own copies; we free both the pointer array and the backing
     * buffer unconditionally after the call. */
    char **names_owned = (char **)calloc(ncols, sizeof(char *));
    if (!names_owned) {
        col_rel_destroy(rel);
        return ENOMEM;
    }
    char *names_buf
        = (char *)malloc((size_t)ncols * WL_COMPOUND_SIDE_COLNAME_MAX);
    if (!names_buf) {
        free(names_owned);
        col_rel_destroy(rel);
        return ENOMEM;
    }

    /* Column 0: "handle"; columns 1..N: "arg{i-1}" */
    {
        char *slot0 = names_buf + 0 * WL_COMPOUND_SIDE_COLNAME_MAX;
        snprintf(slot0, WL_COMPOUND_SIDE_COLNAME_MAX, "handle");
        names_owned[0] = slot0;
        for (uint32_t i = 1; i < ncols; i++) {
            char *slot = names_buf + (size_t)i * WL_COMPOUND_SIDE_COLNAME_MAX;
            snprintf(slot, WL_COMPOUND_SIDE_COLNAME_MAX, "arg%u", i - 1);
            names_owned[i] = slot;
        }
    }

    rc = col_rel_set_schema(rel, ncols, (const char *const *)names_owned);
    free(names_owned);
    free(names_buf);
    if (rc != 0) {
        col_rel_destroy(rel);
        return rc;
    }

    rc = session_add_rel(sess, rel);
    if (rc != 0) {
        col_rel_destroy(rel);
        return rc;
    }

    if (out_rel)
        *out_rel = rel;
    return 0;
}
