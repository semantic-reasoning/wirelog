/*
 * columnar/rotation_standard.c - STANDARD rotation strategy (Issue #600)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Default rotation strategy. Pure passthrough to the existing arena APIs:
 *   rotate_eval_arena -> wl_arena_reset(sess->eval_arena)
 *   gc_epoch_boundary -> wl_compound_arena_gc_epoch_boundary(sess->compound_arena)
 *
 * init/destroy are no-ops; the strategy holds no per-session state.
 */

#include "columnar/internal.h"

#include "arena/arena.h"
#include "arena/compound_arena.h"

static void
standard_rotate_eval_arena(struct wl_col_session_t *sess)
{
    if (sess && sess->eval_arena)
        wl_arena_reset(sess->eval_arena);
}

static void
standard_gc_epoch_boundary(struct wl_col_session_t *sess)
{
    if (sess && sess->compound_arena)
        wl_compound_arena_gc_epoch_boundary(sess->compound_arena);
}

static int
standard_init(struct wl_col_session_t *sess)
{
    (void)sess;
    return 0;
}

static void
standard_destroy(struct wl_col_session_t *sess)
{
    (void)sess;
}

const col_rotation_ops_t col_rotation_standard_ops = {
    .rotate_eval_arena = standard_rotate_eval_arena,
    .gc_epoch_boundary = standard_gc_epoch_boundary,
    .init = standard_init,
    .destroy = standard_destroy,
};
