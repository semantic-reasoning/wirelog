/*
 * columnar/progress.h - Per-Worker Frontier Progress Protocol
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Tracks per-worker, per-stratum frontier advancement so the coordinator
 * can compute the global (minimum) frontier and detect convergence across
 * all parallel workers after wl_workqueue_wait_all().
 *
 * Issue #317: Per-Worker Frontier Progress Protocol
 */

#ifndef WL_COLUMNAR_PROGRESS_H
#define WL_COLUMNAR_PROGRESS_H

#include <stdbool.h>
#include <stdint.h>

/*
 * wl_worker_frontier_entry_t:
 * One worker's recorded convergence point for one stratum.
 *
 * @outer_epoch: Insertion epoch at which convergence was recorded.
 *               Matches wl_col_session_t.outer_epoch semantics.
 * @iteration:   Effective iteration at convergence.
 *               UINT32_MAX = not yet recorded for this epoch.
 */
typedef struct {
    uint32_t outer_epoch;
    uint32_t iteration;
} wl_worker_frontier_entry_t;

/*
 * wl_frontier_progress_t:
 * Coordinator-owned tracker for per-worker, per-stratum frontier progress.
 *
 * Memory layout: entries[stratum_idx * num_workers + worker_id]
 * Flat 2D array indexed by (stratum_idx, worker_id).
 *
 * Workers call wl_frontier_progress_record() after col_eval_stratum
 * converges for their partition.  The coordinator calls
 * wl_frontier_progress_min_iteration() after wl_workqueue_wait_all()
 * to compute the conservative global frontier for each stratum.
 *
 * Thread safety: safe during parallel scatter (each worker writes to a
 * distinct slot).  min/all_converged/reset must be called only after
 * the workqueue barrier.
 */
typedef struct {
    wl_worker_frontier_entry_t *entries; /* owned flat array [num_strata * num_workers] */
    uint32_t num_workers;
    uint32_t num_strata;
} wl_frontier_progress_t;

/*
 * wl_frontier_progress_init:
 * Initialize a frontier progress tracker for num_workers workers and
 * num_strata strata.  All entries are initialized to the "not yet
 * recorded" sentinel: outer_epoch=0, iteration=UINT32_MAX.
 *
 * Note: callers should pass num_strata <= MAX_STRATA (32, defined in
 * internal.h) to stay consistent with the session's frontiers[] arrays.
 * This module does not enforce the upper bound to remain independent of
 * internal.h.
 *
 * Returns 0 on success, EINVAL if arguments are invalid, ENOMEM on
 * allocation failure.
 */
int wl_frontier_progress_init(wl_frontier_progress_t *p,
    uint32_t num_workers, uint32_t num_strata);

/*
 * wl_frontier_progress_destroy:
 * Release memory owned by p.  p itself is not freed (caller manages it).
 * Safe to call on a zero-initialized struct.
 */
void wl_frontier_progress_destroy(wl_frontier_progress_t *p);

/*
 * wl_frontier_progress_record:
 * Called by a worker after stratum stratum_idx converges at (outer_epoch,
 * iteration).  Thread-safe during scatter phase: each worker writes only
 * to its own slot at entries[stratum_idx * num_workers + worker_id].
 * Slots for the same worker_id are strided by num_workers (not contiguous),
 * so there are no data races between workers writing different stratum slots.
 *
 * No-op if p is NULL, entries is NULL, or indices are out of range.
 */
void wl_frontier_progress_record(wl_frontier_progress_t *p,
    uint32_t worker_id, uint32_t stratum_idx,
    uint32_t outer_epoch, uint32_t iteration);

/*
 * wl_frontier_progress_min_iteration:
 * Returns the minimum iteration across all workers for the given
 * (stratum_idx, outer_epoch) pair.  Workers that have not reported
 * for this epoch are excluded (their entries have a different outer_epoch).
 *
 * Returns UINT32_MAX if no worker has recorded progress for this
 * stratum/epoch combination.
 *
 * Called by the coordinator after the workqueue barrier to compute the
 * conservative global frontier (can only skip iterations ALL workers
 * have completed).
 */
uint32_t wl_frontier_progress_min_iteration(const wl_frontier_progress_t *p,
    uint32_t stratum_idx, uint32_t outer_epoch);

/*
 * wl_frontier_progress_all_converged:
 * Returns true iff every worker has recorded a non-UINT32_MAX iteration
 * for (stratum_idx, outer_epoch).  Used by the coordinator to verify
 * that all workers completed evaluation before merging frontiers.
 *
 * Returns false if p is NULL, entries is NULL, or stratum_idx is out
 * of range.
 */
bool wl_frontier_progress_all_converged(const wl_frontier_progress_t *p,
    uint32_t stratum_idx, uint32_t outer_epoch);

/*
 * wl_frontier_progress_reset_stratum:
 * Reset all worker entries for stratum_idx to (new_epoch, UINT32_MAX).
 * Called by the coordinator before re-evaluation begins for a new
 * insertion epoch to clear stale reports from the previous epoch.
 *
 * No-op if p is NULL, entries is NULL, or stratum_idx is out of range.
 */
void wl_frontier_progress_reset_stratum(wl_frontier_progress_t *p,
    uint32_t stratum_idx, uint32_t new_epoch);

#endif /* WL_COLUMNAR_PROGRESS_H */
