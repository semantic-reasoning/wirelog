/*
 * columnar/rotation_pinned.c - pin-aware rotation strategy (Issue #600)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * "Pinned" rotation strategy: PLACEHOLDER. Behavior is currently identical
 * to the STANDARD strategy (direct wl_arena_reset / compound-arena GC).
 * The strategy is named for its destined *mechanism* -- pin-aware
 * reclamation driven by per-generation pin counters from K-Fusion
 * freeze/unfreeze fences (RCU/EBR territory) -- not for any
 * MVCC/transactional concept; wirelog is single-mutator and does not
 * implement multi-version concurrency control. When the real pin-aware
 * implementation lands (per-generation pin counts, deferred reclamation
 * across freeze/unfreeze boundaries), this file becomes the
 * implementation site; for now it exists to make the vtable selection
 * path real and testable.
 *
 * The init hook emits one WL_LOG INFO line on the SESSION section when a
 * session first selects the pinned strategy, so callers see that the
 * placeholder is in effect rather than the future implementation.
 */

#include "columnar/internal.h"

#include "arena/arena.h"
#include "arena/compound_arena.h"
#include "wirelog/util/log.h"

static void
pinned_rotate_eval_arena(struct wl_col_session_t *sess)
{
    /* Placeholder: behave like STANDARD. */
    if (sess && sess->eval_arena)
        wl_arena_reset(sess->eval_arena);
}

static void
pinned_gc_epoch_boundary(struct wl_col_session_t *sess)
{
    /* Placeholder: behave like STANDARD. */
    if (sess && sess->compound_arena)
        wl_compound_arena_gc_epoch_boundary(sess->compound_arena);
}

static int
pinned_init(struct wl_col_session_t *sess)
{
    (void)sess;
    /* Log once per process so the placeholder status is visible without
     * spamming every session_create call. Not thread-safe in the strict
     * sense, but a duplicate log line is benign. */
    static bool warned = false;
    if (!warned) {
        warned = true;
        WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
            "event=rotation_pinned_placeholder note=\"pinned strategy is a "
            "placeholder; behavior matches STANDARD\"");
    }
    return 0;
}

static void
pinned_destroy(struct wl_col_session_t *sess)
{
    (void)sess;
}

const col_rotation_ops_t col_rotation_pinned_ops = {
    .rotate_eval_arena = pinned_rotate_eval_arena,
    .gc_epoch_boundary = pinned_gc_epoch_boundary,
    .init = pinned_init,
    .destroy = pinned_destroy,
};
