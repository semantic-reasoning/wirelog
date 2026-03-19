/*
 * columnar/frontier_epoch.c - Epoch-based frontier vtable implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Issue #261: Implements the default epoch-based frontier model via the
 * col_frontier_ops_t vtable interface. This implementation reproduces
 * the original direct-access behavior exactly.
 */

#include "columnar/internal.h"

#include <stdint.h>

static bool
epoch_should_skip_iteration(void *arg, uint32_t stratum_idx,
    uint32_t eff_iter)
{
    wl_col_session_t *sess = (wl_col_session_t *)arg;
    if (stratum_idx >= MAX_STRATA)
        return false;
    bool same_epoch =
        (sess->outer_epoch == sess->frontiers[stratum_idx].outer_epoch);
    bool beyond = (eff_iter > sess->frontiers[stratum_idx].iteration);
    return same_epoch && beyond;
}

static bool
epoch_should_skip_rule(void *arg, uint32_t rule_id, uint32_t eff_iter)
{
    wl_col_session_t *sess = (wl_col_session_t *)arg;
    if (rule_id >= MAX_RULES)
        return false;
    bool same_epoch =
        (sess->rule_frontiers[rule_id].outer_epoch == sess->outer_epoch);
    bool beyond = (eff_iter > sess->rule_frontiers[rule_id].iteration);
    return same_epoch && beyond;
}

static void
epoch_record_stratum_convergence(void *arg, uint32_t stratum_idx,
    uint32_t outer_epoch, uint32_t iteration)
{
    wl_col_session_t *sess = (wl_col_session_t *)arg;
    if (stratum_idx >= MAX_STRATA)
        return;
    sess->frontiers[stratum_idx].outer_epoch = outer_epoch;
    sess->frontiers[stratum_idx].iteration = iteration;
}

static void
epoch_record_rule_convergence(void *arg, uint32_t rule_id,
    uint32_t outer_epoch, uint32_t iteration)
{
    wl_col_session_t *sess = (wl_col_session_t *)arg;
    if (rule_id >= MAX_RULES)
        return;
    sess->rule_frontiers[rule_id].outer_epoch = outer_epoch;
    sess->rule_frontiers[rule_id].iteration = iteration;
}

static void
epoch_reset_stratum_frontier(void *arg, uint32_t stratum_idx,
    uint32_t outer_epoch)
{
    wl_col_session_t *sess = (wl_col_session_t *)arg;
    if (stratum_idx >= MAX_STRATA)
        return;
    sess->frontiers[stratum_idx].outer_epoch = outer_epoch;
    sess->frontiers[stratum_idx].iteration = UINT32_MAX;
}

static void
epoch_reset_rule_frontier(void *arg, uint32_t rule_id,
    uint32_t outer_epoch)
{
    wl_col_session_t *sess = (wl_col_session_t *)arg;
    if (rule_id >= MAX_RULES)
        return;
    sess->rule_frontiers[rule_id].outer_epoch = outer_epoch;
    sess->rule_frontiers[rule_id].iteration = UINT32_MAX;
}

static void
epoch_init_stratum(void *arg, uint32_t stratum_idx)
{
    wl_col_session_t *sess = (wl_col_session_t *)arg;
    if (stratum_idx >= MAX_STRATA)
        return;
    if (sess->frontiers[stratum_idx].iteration == 0)
        sess->frontiers[stratum_idx].iteration = UINT32_MAX;
}

const col_frontier_ops_t col_frontier_epoch_ops = {
    .should_skip_iteration = epoch_should_skip_iteration,
    .should_skip_rule = epoch_should_skip_rule,
    .record_stratum_convergence = epoch_record_stratum_convergence,
    .record_rule_convergence = epoch_record_rule_convergence,
    .reset_stratum_frontier = epoch_reset_stratum_frontier,
    .reset_rule_frontier = epoch_reset_rule_frontier,
    .init_stratum = epoch_init_stratum,
};
