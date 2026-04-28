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
 * session first selects the pinned strategy, plus a one-shot stderr
 * fprintf footgun guard (#630) so the placeholder status is visible
 * regardless of compile-time WL_LOG ceiling. Operators who select
 * WIRELOG_ROTATION=pinned in production env would otherwise get no
 * visible signal: WL_LOG INFO on the SESSION channel is stripped at
 * release log levels (-Dwirelog_log_max_level=error). The stderr line
 * bypasses the structured logger so it prints regardless. Set
 * WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER=1 to silence it after an
 * informed opt-in.
 */

#include "columnar/internal.h"

#include "arena/arena.h"
#include "arena/compound_arena.h"
#include "wirelog/util/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

        /* Footgun guard (#630): the WL_LOG INFO line above is stripped
         * at release log levels (-Dwirelog_log_max_level=error), so
         * operators who select WIRELOG_ROTATION=pinned in production
         * env get no visible signal that they have opted into a
         * placeholder strategy. Emit a one-shot stderr line that
         * bypasses the structured logger so it prints regardless of
         * compile-time level. The acknowledge env var silences the
         * warning for operators who have explicitly opted in to
         * running the placeholder. */
        const char *ack
            = getenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        if (!(ack && strcmp(ack, "1") == 0)) {
            fprintf(stderr,
                "[wirelog] WARNING: WIRELOG_ROTATION=pinned selected, but "
                "the pinned strategy is currently a PLACEHOLDER -- its "
                "behavior is identical to WIRELOG_ROTATION=standard. Real "
                "pin-aware reclamation will land via issue #630. Set "
                "WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER=1 to silence "
                "this warning.\n");
        }
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
