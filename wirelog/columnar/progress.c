/*
 * columnar/progress.c - Per-Worker Frontier Progress Protocol
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Per-worker frontier progress tracking for parallel evaluation.
 * Workers report convergence points; the coordinator merges them
 * to compute the global minimum frontier and detect convergence.
 *
 * Issue #317: Per-Worker Frontier Progress Protocol
 */

#include "columnar/progress.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Internal Helpers                                                         */
/* ======================================================================== */

/*
 * entry_slot: flat index for (stratum_idx, worker_id).
 * Layout: entries[stratum_idx * num_workers + worker_id]
 */
static inline uint32_t
entry_slot(const wl_frontier_progress_t *p, uint32_t stratum_idx,
    uint32_t worker_id)
{
    return stratum_idx * p->num_workers + worker_id;
}

/* ======================================================================== */
/* Lifecycle                                                                */
/* ======================================================================== */

int
wl_frontier_progress_init(wl_frontier_progress_t *p,
    uint32_t num_workers, uint32_t num_strata)
{
    if (!p || num_workers == 0 || num_strata == 0)
        return EINVAL;

    uint32_t count = num_strata * num_workers;
    p->entries = (wl_worker_frontier_entry_t *)calloc(
        count, sizeof(wl_worker_frontier_entry_t));
    if (!p->entries)
        return ENOMEM;

    /* Initialize all entries to the "not yet recorded" sentinel.
     * outer_epoch=0 matches coordinator's initial epoch; iteration=UINT32_MAX
     * indicates no report has arrived for this epoch yet. */
    for (uint32_t i = 0; i < count; i++) {
        p->entries[i].outer_epoch = 0;
        p->entries[i].iteration = UINT32_MAX;
    }

    p->num_workers = num_workers;
    p->num_strata = num_strata;
    return 0;
}

void
wl_frontier_progress_destroy(wl_frontier_progress_t *p)
{
    if (!p)
        return;
    free(p->entries);
    p->entries = NULL;
    p->num_workers = 0;
    p->num_strata = 0;
}

/* ======================================================================== */
/* Worker Reporting                                                         */
/* ======================================================================== */

void
wl_frontier_progress_record(wl_frontier_progress_t *p,
    uint32_t worker_id, uint32_t stratum_idx,
    uint32_t outer_epoch, uint32_t iteration)
{
    if (!p || !p->entries)
        return;
    if (worker_id >= p->num_workers || stratum_idx >= p->num_strata)
        return;

    wl_worker_frontier_entry_t *e
        = &p->entries[entry_slot(p, stratum_idx, worker_id)];
    e->outer_epoch = outer_epoch;
    e->iteration = iteration;
}

/* ======================================================================== */
/* Coordinator Queries                                                      */
/* ======================================================================== */

/*
 * wl_frontier_progress_min_iteration:
 * Compute the minimum iteration across all workers for this stratum/epoch.
 * Workers that have not reported for this epoch (different outer_epoch) are
 * excluded: they have not yet advanced and cannot contribute to the minimum.
 *
 * Returns UINT32_MAX when no worker has reported for (stratum_idx, outer_epoch).
 */
uint32_t
wl_frontier_progress_min_iteration(const wl_frontier_progress_t *p,
    uint32_t stratum_idx, uint32_t outer_epoch)
{
    if (!p || !p->entries || stratum_idx >= p->num_strata)
        return UINT32_MAX;

    uint32_t min_iter = UINT32_MAX;
    for (uint32_t wi = 0; wi < p->num_workers; wi++) {
        const wl_worker_frontier_entry_t *e
            = &p->entries[entry_slot(p, stratum_idx, wi)];
        /* Skip workers that have not reported for this epoch */
        if (e->outer_epoch != outer_epoch)
            continue;
        if (e->iteration < min_iter)
            min_iter = e->iteration;
    }
    return min_iter;
}

/*
 * wl_frontier_progress_all_converged:
 * Returns true iff every worker has reported a non-UINT32_MAX iteration
 * for this (stratum_idx, outer_epoch).
 *
 * A worker is considered "not yet converged" if:
 *   - Its outer_epoch does not match (no report for this epoch), OR
 *   - Its iteration is UINT32_MAX (report arrived but sentinel not cleared)
 */
bool
wl_frontier_progress_all_converged(const wl_frontier_progress_t *p,
    uint32_t stratum_idx, uint32_t outer_epoch)
{
    if (!p || !p->entries || stratum_idx >= p->num_strata ||
        p->num_workers == 0)
        return false;

    for (uint32_t wi = 0; wi < p->num_workers; wi++) {
        const wl_worker_frontier_entry_t *e
            = &p->entries[entry_slot(p, stratum_idx, wi)];
        if (e->outer_epoch != outer_epoch || e->iteration == UINT32_MAX)
            return false;
    }
    return true;
}

/* ======================================================================== */
/* Coordinator Reset                                                        */
/* ======================================================================== */

/*
 * wl_frontier_progress_reset_stratum:
 * Reset all worker slots for stratum_idx to (new_epoch, UINT32_MAX).
 * Called by the coordinator before issuing parallel worker tasks for a
 * new evaluation epoch so stale reports from the previous epoch are
 * not mistaken for fresh ones.
 */
void
wl_frontier_progress_reset_stratum(wl_frontier_progress_t *p,
    uint32_t stratum_idx, uint32_t new_epoch)
{
    if (!p || !p->entries || stratum_idx >= p->num_strata)
        return;

    for (uint32_t wi = 0; wi < p->num_workers; wi++) {
        wl_worker_frontier_entry_t *e
            = &p->entries[entry_slot(p, stratum_idx, wi)];
        e->outer_epoch = new_epoch;
        e->iteration = UINT32_MAX;
    }
}
