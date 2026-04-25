/*
 * columnar/rotation_mvcc.c - MVCC placeholder rotation strategy (Issue #600)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * MVCC rotation strategy: PLACEHOLDER. Behavior is currently identical to
 * the STANDARD strategy (direct wl_arena_reset / compound-arena GC). When
 * an MVCC-aware rotation model lands (per-version retention, snapshot
 * isolation), this file becomes the implementation site; for now it
 * exists to make the vtable selection path real and testable.
 *
 * The init hook emits one WL_LOG INFO line on the SESSION section when a
 * session first selects MVCC, so callers see that the placeholder is in
 * effect rather than the future MVCC implementation.
 */

#include "columnar/internal.h"

#include "arena/arena.h"
#include "arena/compound_arena.h"
#include "wirelog/util/log.h"

static void
mvcc_rotate_eval_arena(struct wl_col_session_t *sess)
{
    /* Placeholder: behave like STANDARD. */
    if (sess && sess->eval_arena)
        wl_arena_reset(sess->eval_arena);
}

static void
mvcc_gc_epoch_boundary(struct wl_col_session_t *sess)
{
    /* Placeholder: behave like STANDARD. */
    if (sess && sess->compound_arena)
        wl_compound_arena_gc_epoch_boundary(sess->compound_arena);
}

static int
mvcc_init(struct wl_col_session_t *sess)
{
    (void)sess;
    /* Log once per process so the placeholder status is visible without
     * spamming every session_create call. Not thread-safe in the strict
     * sense, but a duplicate log line is benign. */
    static bool warned = false;
    if (!warned) {
        warned = true;
        WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
            "event=rotation_mvcc_placeholder note=\"MVCC strategy is a "
            "placeholder; behavior matches STANDARD\"");
    }
    return 0;
}

static void
mvcc_destroy(struct wl_col_session_t *sess)
{
    (void)sess;
}

const col_rotation_ops_t col_rotation_mvcc_ops = {
    .rotate_eval_arena = mvcc_rotate_eval_arena,
    .gc_epoch_boundary = mvcc_gc_epoch_boundary,
    .init = mvcc_init,
    .destroy = mvcc_destroy,
};
