/*
 * backend/columnar_nanoarrow.c - wirelog Nanoarrow Columnar Backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#define _GNU_SOURCE

#include "columnar/internal.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Platform-specific memory detection headers (Issue #221) */
#ifdef __linux__
#include <sys/sysinfo.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

/* Relation storage and cache functions moved to columnar/relation.c and
 * columnar/cache.c; declarations in columnar/internal.h. */

/* Arrangement functions moved to columnar/arrangement.c;
 * frontier/affected-strata functions moved to columnar/frontier.c;
 * Mobius/Z-set functions moved to columnar/mobius.c;
 * declarations in columnar/internal.h. */

col_rel_t *
session_find_rel(wl_col_session_t *sess, const char *name)
{
    if (!name)
        return NULL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0)
            return sess->rels[i];
    }
    return NULL;
}

int
session_add_rel(wl_col_session_t *sess, col_rel_t *r)
{
    /* Pool-owned structs must be promoted to heap before storing in the
     * session, because col_session_destroy calls free() on each entry. */
    if (r->pool_owned) {
        col_rel_t *heap = (col_rel_t *)calloc(1, sizeof(col_rel_t));
        if (!heap)
            return ENOMEM;
        *heap = *r;
        heap->pool_owned = false;
        /* Zero out source slot so pool_reset doesn't double-free contents */
        memset(r, 0, sizeof(*r));
        r = heap;
    }
    if (sess->nrels >= sess->rel_cap) {
        uint32_t nc = sess->rel_cap ? sess->rel_cap * 2 : 16;
        col_rel_t **nr
            = (col_rel_t **)realloc(sess->rels, sizeof(col_rel_t *) * nc);
        if (!nr)
            return ENOMEM;
        sess->rels = nr;
        sess->rel_cap = nc;
    }
    sess->rels[sess->nrels++] = r;
    return 0;
}

void
session_remove_rel(wl_col_session_t *sess, const char *name)
{
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0) {
            col_rel_destroy(sess->rels[i]);
            sess->rels[i] = NULL;
            return;
        }
    }
}

/* Operator implementations moved to columnar/ops.c;
 * evaluator functions moved to columnar/eval.c;
 * declarations in columnar/internal.h. */

/* ======================================================================== */
/* Public Accessors                                                          */
/* ======================================================================== */

/*
 * col_session_get_iteration_count:
 *
 * Return the number of fixed-point iterations performed during the last
 * call to col_eval_stratum.  Returns 0 if no evaluation has occurred yet.
 *
 * @param sess  A wl_session_t* backed by the columnar backend.
 */
uint32_t
col_session_get_iteration_count(wl_session_t *sess)
{
    return COL_SESSION(sess)->total_iterations;
}

/*
 * col_session_get_cache_stats:
 *
 * Return CSE materialization cache hit and miss counts accumulated during
 * the last evaluation.  Both out-parameters are optional (NULL-safe).
 *
 * @param sess    A wl_session_t* backed by the columnar backend.
 * @param out_hits    Set to the number of cache hits (may be NULL).
 * @param out_misses  Set to the number of cache misses (may be NULL).
 */
void
col_session_get_cache_stats(wl_session_t *sess, uint64_t *out_hits,
    uint64_t *out_misses)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_hits)
        *out_hits = cs->mat_cache.hits;
    if (out_misses)
        *out_misses = cs->mat_cache.misses;
}

/*
 * col_session_get_perf_stats:
 *
 * Return accumulated profiling counters (in nanoseconds) from the last
 * wl_session_snapshot() call.  Counters are reset at the start of each
 * evaluation pass.  Both out-parameters are optional (NULL-safe).
 *
 * @param sess             A wl_session_t* backed by the columnar backend.
 * @param out_consolidation_ns  Time spent in incremental consolidation.
 * @param out_kfusion_ns        Time spent in K-fusion dispatch.
 */
void
col_session_get_perf_stats(wl_session_t *sess, uint64_t *out_consolidation_ns,
    uint64_t *out_kfusion_ns,
    uint64_t *out_kfusion_alloc_ns,
    uint64_t *out_kfusion_dispatch_ns,
    uint64_t *out_kfusion_merge_ns,
    uint64_t *out_kfusion_cleanup_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_consolidation_ns)
        *out_consolidation_ns = cs->consolidation_ns;
    if (out_kfusion_ns)
        *out_kfusion_ns = cs->kfusion_ns;
    if (out_kfusion_alloc_ns)
        *out_kfusion_alloc_ns = cs->kfusion_alloc_ns;
    if (out_kfusion_dispatch_ns)
        *out_kfusion_dispatch_ns = cs->kfusion_dispatch_ns;
    if (out_kfusion_merge_ns)
        *out_kfusion_merge_ns = cs->kfusion_merge_ns;
    if (out_kfusion_cleanup_ns)
        *out_kfusion_cleanup_ns = cs->kfusion_cleanup_ns;
}

/*
 * col_session_get_consolidation_stats:
 *
 * Return fast-path and slow-path hit counts accumulated across all
 * consolidation calls in the last wl_session_snapshot().
 * Both out-parameters are NULL-safe.
 *
 * @param sess            A wl_session_t* backed by the columnar backend.
 * @param out_fast_hits   Count of calls that took the O(D) fast path.
 * @param out_slow_hits   Count of calls that took the O(N+D) merge walk.
 */
void
col_session_get_consolidation_stats(wl_session_t *sess,
    uint64_t *out_fast_hits, uint64_t *out_slow_hits)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_fast_hits)
        *out_fast_hits = cs->consolidate_fast_hits;
    if (out_slow_hits)
        *out_slow_hits = cs->consolidate_slow_hits;
}

/*
 * col_session_cleanup_old_data:
 *
 * Remove data that is entirely before the frontier (iteration, stratum).
 * Only rows with timestamps <= frontier are removed.
 *
 * This function performs selective cleanup of old delta rows to reduce
 * memory usage. Timestamps before the frontier have already been processed
 * and will not be needed again during evaluation.
 *
 * @param sess     wl_session_t* backed by columnar backend
 * @param frontier Minimum (iteration, stratum) to preserve; data before this
 * can be freed
 */
static void
col_session_cleanup_old_data(wl_session_t *sess, col_frontier_t frontier)
{
    if (!sess)
        return;

    wl_col_session_t *cs = COL_SESSION(sess);

    /* Scan all relations, remove rows with timestamps <= frontier */
    for (uint32_t ri = 0; ri < cs->nrels; ri++) {
        col_rel_t *rel = cs->rels[ri];
        if (!rel || !rel->timestamps || rel->nrows == 0)
            continue;

        /* Find first row that is after frontier */
        uint32_t keep_from = 0;
        for (uint32_t row = 0; row < rel->nrows; row++) {
            const col_delta_timestamp_t *ts = &rel->timestamps[row];
            if (ts->iteration > frontier.iteration
                || (ts->iteration == frontier.iteration
                && ts->stratum > frontier.stratum)) {
                keep_from = row;
                break;
            }
        }

        /* If all rows are at or before frontier, clear entire relation */
        if (keep_from == rel->nrows) {
            free(rel->timestamps);
            rel->timestamps = NULL;
            rel->nrows = 0;
            rel->capacity = 0;
            free(rel->data);
            rel->data = NULL;
            /* Invalidate arrangements (data changed) */
            col_session_invalidate_arrangements(sess, rel->name);
        } else if (keep_from > 0) {
            /* Shift rows forward and update timestamps */
            uint32_t new_nrows = rel->nrows - keep_from;
            size_t row_bytes = (size_t)rel->ncols * sizeof(int64_t);

            memmove(rel->data, rel->data + (size_t)keep_from * rel->ncols,
                new_nrows * row_bytes);
            memmove(rel->timestamps, rel->timestamps + keep_from,
                new_nrows * sizeof(col_delta_timestamp_t));

            rel->nrows = new_nrows;
            /* Invalidate arrangements (data changed) */
            col_session_invalidate_arrangements(sess, rel->name);
        }
    }
}

/* ======================================================================== */
/* Vtable Functions                                                          */
/* ======================================================================== */

/*
 * col_should_activate_diff: Determine whether differential operators should
 * be used for this evaluation pass (Issue #264).
 *
 * Differential operators are activated only when ALL conditions hold:
 *   1. diff_enabled is true (session-level master switch)
 *   2. affected_mask is not UINT64_MAX (not a non-incremental full eval)
 *   3. affected_mask is not 0 (at least one stratum affected)
 *
 * When any condition fails, epoch-based operators are used (safe fallback).
 */
static bool
col_should_activate_diff(const wl_col_session_t *sess, uint64_t affected_mask)
{
    return sess->diff_enabled
           && affected_mask != UINT64_MAX
           && affected_mask != 0;
}

/*
 * col_detect_physical_memory: Detect total physical RAM in bytes (Issue #221).
 *
 * Uses platform-specific syscalls to query total installed RAM. Returns 0 if
 * the platform is not supported or if detection fails. No /proc/meminfo used.
 */
static uint64_t
col_detect_physical_memory(void)
{
#ifdef __linux__
    struct sysinfo info;
    if (sysinfo(&info) == 0)
        return (uint64_t)info.totalram * (uint64_t)info.mem_unit;
    return 0;
#elif defined(__APPLE__)
    int mib[2] = { CTL_HW, HW_MEMSIZE };
    uint64_t memsize = 0;
    size_t len = sizeof(memsize);
    if (sysctl(mib, 2, &memsize, &len, NULL, 0) == 0)
        return memsize;
    return 0;
#elif defined(_WIN32)
    MEMORYSTATUSEX memStatus;
    memStatus.dwLength = sizeof(memStatus);
    if (GlobalMemoryStatusEx(&memStatus))
        return (uint64_t)memStatus.ullTotalPhys;
    return 0;
#else
    return 0;
#endif
}

/*
 * col_session_create: Initialize a columnar backend session
 *
 * Implements wl_compute_backend_t.session_create vtable slot.
 *
 * @param plan:        Execution plan (borrowed, must outlive session)
 * @param num_workers: Thread pool size for parallel K-fusion. When > 1,
 *                     creates a workqueue at session init. When 1, K-fusion
 *                     evaluates copies sequentially (no thread overhead).
 * @param out:         (out) Receives &sess->base on success
 *
 * Memory initialization order:
 *   1. Allocate wl_col_session_t (zero-initialized via calloc)
 *   2. Set sess->plan = plan (borrowed reference)
 *   3. Allocate rels[] with initial capacity 16
 *   4. Pre-register EDB relations from plan->edb_relations (ncols lazy-inited)
 *   5. Set *out = &sess->base  (session.c:38 then sets base.backend)
 *
 * @return 0 on success, EINVAL if plan/out is NULL, ENOMEM on alloc failure
 *
 * @see wl_session_create in session.c for vtable dispatch context
 * @see wl_col_session_t memory layout documentation above
 */
static int
col_session_create(const wl_plan_t *plan, uint32_t num_workers,
    wl_session_t **out)
{
    if (!plan || !out)
        return EINVAL;

    wl_col_session_t *sess
        = (wl_col_session_t *)calloc(1, sizeof(wl_col_session_t));
    if (!sess)
        return ENOMEM;

    sess->frontier_ops = &col_frontier_epoch_ops;
    sess->plan = plan;
    sess->num_workers = num_workers > 0 ? num_workers : 1;

    /* Dynamic join output limit (Issue #221) */
    {
        const char *join_limit_env = getenv("WIRELOG_JOIN_OUTPUT_LIMIT");
        bool env_valid = false;
        if (join_limit_env && join_limit_env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            uint64_t val = strtoull(join_limit_env, &endp, 10);
            if (endp != join_limit_env && *endp == '\0' && errno != ERANGE) {
                sess->join_output_limit = val;
                env_valid = true;
            }
        }
        if (!env_valid) {
            uint64_t phys = col_detect_physical_memory();
            if (phys > 0) {
                /* 25% of RAM / (8 bytes * 5 avg cols * num_workers) */
                sess->join_output_limit
                    = (phys / 4)
                    / (40ULL
                    * (sess->num_workers > 0 ? sess->num_workers : 1));
            } else {
                sess->join_output_limit = COL_JOIN_OUTPUT_LIMIT_DEFAULT;
            }
        }
        /* Clamp to UINT32_MAX since nrows is uint32_t */
        if (sess->join_output_limit > UINT32_MAX)
            sess->join_output_limit = UINT32_MAX;
    }

    sess->rel_cap = 16;
    sess->rels = (col_rel_t **)calloc(sess->rel_cap, sizeof(col_rel_t *));
    if (!sess->rels) {
        free(sess);
        return ENOMEM;
    }

    /* Create workqueue for parallel K-fusion when num_workers > 1.
     * Single-threaded mode (num_workers=1) leaves wq=NULL; K-fusion
     * evaluates copies sequentially with no thread overhead. (Issue #99) */
    if (sess->num_workers > 1) {
        sess->wq = wl_workqueue_create(sess->num_workers);
        if (!sess->wq) {
            free(sess->rels);
            free(sess);
            return ENOMEM;
        }
    }

    /* Create delta pool for per-iteration temporaries.
     * Slab: 256 relations (cover ~20 rules x 5 ops + headroom)
     * Arena: 64MB initial (for row data buffers) */
    sess->delta_pool
        = delta_pool_create(256, sizeof(col_rel_t), 64 * 1024 * 1024);
    if (!sess->delta_pool) {
        /* Non-fatal: pool allocation failed, fall back to malloc */
    }

    /* Issue #224: Initialize memory accounting ledger.
     * Budget: 75% of physical RAM, or from WIRELOG_MEMORY_BUDGET env var.
     * 0 = unlimited (when physical memory detection fails). */
    {
        uint64_t budget = 0;
        const char *budget_env = getenv("WIRELOG_MEMORY_BUDGET");
        if (budget_env && budget_env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            uint64_t val = strtoull(budget_env, &endp, 10);
            if (endp != budget_env && *endp == '\0' && errno != ERANGE)
                budget = val;
        }
        if (budget == 0) {
            uint64_t phys = col_detect_physical_memory();
            if (phys > 0)
                budget = (phys / 4ULL) * 3ULL; /* 75% of RAM */
        }
        wl_mem_ledger_init(&sess->mem_ledger, budget);
    }

    /* Issue #264: Initialize differential path master switch.
     * Default: enabled (true). Users can disable via WIRELOG_DIFF_ENABLED=0
     * to force epoch-based evaluation regardless of affected_strata. */
    {
        const char *diff_env = getenv("WIRELOG_DIFF_ENABLED");
        if (diff_env && diff_env[0] == '0' && diff_env[1] == '\0')
            sess->diff_enabled = false;
        else
            sess->diff_enabled = true;
    }

    /* Issue #277: Cache debug/log env vars at session init to avoid repeated
     * getenv() calls in hot paths. */
    sess->debug_join = (getenv("WL_DEBUG_JOIN") != NULL);
    sess->consolidation_log = (getenv("WL_CONSOLIDATION_LOG") != NULL);

    /* Issue #176: Configure per-iteration cache eviction threshold.
     * Default: 80% of COL_MAT_CACHE_LIMIT_BYTES (cache evicts when exceeding
     * this threshold). Users can override via environment variable or API.
     * 0 = disabled (cache cleared each iteration, backward compatible). */
    sess->cache_evict_threshold
        = (COL_MAT_CACHE_LIMIT_BYTES * 80) / 100; /* default: 80% of limit */

    /* Pre-register EDB relations (ncols determined at first insert) */
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        col_rel_t *r = NULL;
        int rc = col_rel_alloc(&r, plan->edb_relations[i]);
        if (rc != 0)
            goto oom;
        rc = session_add_rel(sess, r);
        if (rc != 0) {
            col_rel_destroy(r);
            goto oom;
        }
    }

    /* Issue #105: Populate stratum_is_monotone from plan.
     * Copy monotone property from each stratum in the plan.
     * Conservative default (all false from calloc) is already set,
     * so only copy if strata exist. */
    for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA; si++) {
        sess->stratum_is_monotone[si] = plan->strata[si].is_monotone;
    }

    /* Issue #103: Initialize 2D frontier epoch tracking.
     * outer_epoch is initialized to 0 by calloc (line 4673) and incremented
     * before each EDB insertion via col_session_insert_incremental. This
     * distinguishes different insertion epochs for 2D frontier (epoch, iteration)
     * pairs to prevent incorrect skip-condition evaluation across epochs. */
    /* outer_epoch = 0; */ /* Already zeroed by calloc */

    *out = &sess->base;
    return 0;

oom:
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free(sess->rels);
    wl_workqueue_destroy(sess->wq);       /* NULL-safe */
    delta_pool_destroy(sess->delta_pool); /* NULL-safe */
    free(sess);
    return ENOMEM;
}

/*
 * col_session_destroy: Free all resources owned by a columnar session
 *
 * Implements wl_compute_backend_t.session_destroy vtable slot.
 * NULL-safe. Frees rels[], each rels[i], and the session struct itself.
 * The plan is borrowed and NOT freed here.
 *
 * @param session: wl_session_t* (cast to wl_col_session_t* internally)
 */
static void
col_session_destroy(wl_session_t *session)
{
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free(sess->rels);
    col_mat_cache_clear(&sess->mat_cache);
    wl_workqueue_destroy(sess->wq);
    /* Free arrangement registry (Phase 3C) */
    for (uint32_t i = 0; i < sess->arr_count; i++) {
        free(sess->arr_entries[i].rel_name);
        free(sess->arr_entries[i].key_cols);
        arr_free_contents(&sess->arr_entries[i].arr);
    }
    free(sess->arr_entries);
    col_session_free_delta_arrangements(sess);
    col_session_free_sorted_arrangements(sess);
    col_session_free_diff_arrangements(sess);
    delta_pool_destroy(sess->delta_pool);
    free(sess);
}

int
col_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    col_rel_t *r = session_find_rel(COL_SESSION(session), relation);
    if (!r)
        return ENOENT;

    /* Lazy schema initialisation on first insert */
    if (r->ncols == 0) {
        int rc = col_rel_set_schema(r, num_cols, NULL);
        if (rc != 0)
            return rc;
    } else if (r->ncols != num_cols) {
        return EINVAL; /* column count mismatch */
    }

    for (uint32_t i = 0; i < num_rows; i++) {
        int rc = col_rel_append_row(r, data + (size_t)i * num_cols);
        if (rc != 0)
            return rc;
    }

    /* Enable incremental re-evaluation mode: mark this relation as the
     * insertion point for affected stratum detection. This activates
     * frontier persistence in col_session_snapshot, enabling the per-iteration
     * skip condition to reduce iterations on subsequent snapshots. */
    COL_SESSION(session)->last_inserted_relation = relation;

    return 0;
}

/*
 * col_session_insert_incremental: Append facts to a session WITHOUT resetting
 * the per-stratum frontier.
 *
 * Unlike col_session_insert(), this function preserves frontier[] state so
 * that a subsequent col_session_step() call can perform incremental
 * re-evaluation: only strata whose frontier has not yet converged past the
 * current iteration are evaluated.
 *
 * Facts are appended to the existing relation; existing rows are kept.
 * Schema is lazily initialised on the first call (same as col_session_insert).
 *
 * @param session:  Active wl_session_t created by col_session_create
 * @param relation: Name of the EDB relation to append to
 * @param data:     Row-major int64_t array, num_rows * num_cols elements
 * @param num_rows: Number of rows to append (0 is a no-op and returns 0)
 * @param num_cols: Number of columns per row
 * @return 0 on success, EINVAL on bad args, ENOENT if relation unknown,
 *         ENOMEM on allocation failure
 */
int
col_session_insert_incremental(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    if (num_rows == 0)
        return 0; /* true no-op */

    col_rel_t *r = session_find_rel(COL_SESSION(session), relation);
    if (!r)
        return ENOENT;

    /* Lazy schema initialisation on first insert */
    if (r->ncols == 0) {
        int rc = col_rel_set_schema(r, num_cols, NULL);
        if (rc != 0)
            return rc;
    } else if (r->ncols != num_cols) {
        return EINVAL; /* column count mismatch */
    }

    /* Append rows; frontier[] is intentionally NOT modified */
    for (uint32_t i = 0; i < num_rows; i++) {
        int rc = col_rel_append_row(r, data + (size_t)i * num_cols);
        if (rc != 0)
            return rc;
    }

    /* Invalidate arrangement caches for the modified relation so subsequent
     * re-evaluation rebuilds hash indices with the new rows (issue #92). */
    col_session_invalidate_arrangements(session, relation);

    /* Issue #103: Increment outer_epoch to mark a new insertion epoch.
     * This epoch counter distinguishes different insertion phases for 2D frontier
     * tracking: (outer_epoch, iteration) pairs ensure iterations are skipped only
     * within the same epoch. Wrapping at UINT32_MAX is acceptable (continues
     * distinguishing epochs across multiple insertions). */
    wl_col_session_t *sess = COL_SESSION(session);
    sess->outer_epoch++;

    /* Record the inserted relation so col_session_step can skip unaffected
     * strata (Phase 4 affected-stratum skip optimization). */
    sess->last_inserted_relation = relation;
    return 0;
}

/* Forward declaration for col_session_remove_incremental */
static int
col_session_remove_incremental(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols);

static int
col_session_remove(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);
    if (sess->delta_cb != NULL)
        return col_session_remove_incremental(session, relation, data, num_rows,
                   num_cols);

    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;
    if (r->ncols == 0)
        return 0; /* uninitialized schema = nothing to remove */
    if (r->ncols != num_cols)
        return EINVAL;

    /* Compact: remove matching rows */
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        uint32_t out_r = 0;
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            const int64_t *row = r->data + (size_t)ri * num_cols;
            if (memcmp(row, del, sizeof(int64_t) * num_cols) != 0) {
                if (out_r != ri)
                    memcpy(r->data + (size_t)out_r * num_cols, row,
                        sizeof(int64_t) * num_cols);
                out_r++;
            } else {
                /* Remove first matching row only */
                di = num_rows; /* break outer loop after this one */
                for (uint32_t rest = ri + 1; rest < r->nrows; rest++, out_r++)
                    memcpy(r->data + (size_t)out_r * num_cols,
                        r->data + (size_t)rest * num_cols,
                        sizeof(int64_t) * num_cols);
                r->nrows = out_r;
                goto next_del;
            }
        }
        r->nrows = out_r;
next_del:;
    }
    return 0;
}

/*
 * col_session_remove_incremental: Remove rows and pre-seed retraction deltas
 *
 * (Issue #158) Semi-naive delta retraction for non-recursive strata.
 * When a delta callback is registered, this function:
 *   1. Creates $r$<name> relation from removed rows
 *   2. Registers it as a session relation (for VARIABLE ops to consume)
 *   3. Removes rows from the EDB using existing compact logic
 *   4. Records the removal for affected-stratum calculation
 *
 * The $r$<name> relation is used during the next session_step to seed
 * the retraction evaluation, enabling delta-only propagation.
 */
static int
col_session_remove_incremental(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);

    /* Find EDB relation */
    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;
    if (r->ncols == 0)
        return 0; /* uninitialized schema = nothing to remove */
    if (r->ncols != num_cols)
        return EINVAL;

    /* Allocate $r$<name> delta relation to collect removed rows */
    char rname[256];
    snprintf(rname, sizeof(rname), "$r$%s", relation);

    col_rel_t *rdelta = col_rel_new_auto(rname, num_cols);
    if (!rdelta)
        return ENOMEM;

    /* Append each removed row to the delta relation.
     * We need to track which rows are actually being removed from the EDB,
     * then add them to rdelta. */
    int rc = 0;
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        /* Check if this row exists in EDB; if so, append to rdelta */
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            const int64_t *row = r->data + (size_t)ri * num_cols;
            if (memcmp(row, del, sizeof(int64_t) * num_cols) == 0) {
                /* Found matching row; add to retraction delta */
                rc = col_rel_append_row(rdelta, del);
                if (rc != 0) {
                    col_rel_destroy(rdelta);
                    return rc;
                }
                break; /* Only one copy per removal request */
            }
        }
    }

    /* Register $r$<name> in session (replacing any prior) */
    session_remove_rel(sess, rname);
    rc = session_add_rel(sess, rdelta);
    if (rc != 0) {
        col_rel_destroy(rdelta);
        return rc;
    }

    /* Remove rows from the EDB using existing compact logic */
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        uint32_t out_r = 0;
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            const int64_t *row = r->data + (size_t)ri * num_cols;
            if (memcmp(row, del, sizeof(int64_t) * num_cols) != 0) {
                if (out_r != ri)
                    memcpy(r->data + (size_t)out_r * num_cols, row,
                        sizeof(int64_t) * num_cols);
                out_r++;
            } else {
                /* Remove first matching row only */
                di = num_rows; /* break outer loop after this one */
                for (uint32_t rest = ri + 1; rest < r->nrows; rest++, out_r++)
                    memcpy(r->data + (size_t)out_r * num_cols,
                        r->data + (size_t)rest * num_cols,
                        sizeof(int64_t) * num_cols);
                r->nrows = out_r;
                goto next_del_incr;
            }
        }
        r->nrows = out_r;
next_del_incr:;
    }

    /* Clamp base_nrows to current row count */
    if (r->base_nrows > r->nrows)
        r->base_nrows = r->nrows;

    /* Mark removal for affected-stratum calculation */
    sess->last_removed_relation = relation;
    sess->outer_epoch++;

    return 0;
}

/*
 * col_session_step: Advance the session by one evaluation epoch
 *
 * Implements wl_compute_backend_t.session_step vtable slot.
 *
 * Iterates all strata in plan order. For each stratum:
 *   - Fast path (no delta_cb): col_eval_stratum directly
 *   - Delta path: col_stratum_step_with_delta (snapshot + eval + set diff)
 * Arena is reset after each stratum to reclaim temporary evaluation data.
 *
 * TODO(Phase 2B): Replace set-diff delta with semi-naive ΔR propagation.
 *
 * @param session: wl_session_t* (cast to wl_col_session_t* internally)
 * @return 0 on success, non-zero on evaluation error
 */
static int
col_session_step(wl_session_t *session)
{
    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;

    /* Compute affected strata bitmask (Phase 4 incremental skip).
     * When last_inserted_relation is set (incremental path), only evaluate
     * strata that transitively depend on the inserted relation.  When NULL
     * (regular step), UINT64_MAX means all strata are evaluated. */
    uint64_t affected_mask = UINT64_MAX;
    if (sess->last_inserted_relation != NULL) {
        affected_mask = col_compute_affected_strata(
            session, sess->last_inserted_relation);
    }

    /* Issue #158: Pre-seed retraction deltas for affected strata.
     * If last_removed_relation is set (removal via col_session_remove_incremental),
     * check if $r$<name> exists for retractions, and set retraction_seeded. */
    if (sess->last_removed_relation != NULL) {
        affected_mask &= col_compute_affected_strata(
            session, sess->last_removed_relation);
        char rname[256];
        if (retraction_rel_name(sess->last_removed_relation, rname,
            sizeof(rname))
            == 0) {
            col_rel_t *rdelta = session_find_rel(sess, rname);
            if (rdelta && rdelta->nrows > 0)
                sess->retraction_seeded = true;
        }
    }

    /* Issue #264: Activate differential operators when session toggle is on
     * and only partial strata are affected (see col_should_activate_diff). */
    sess->diff_operators_active = col_should_activate_diff(sess, affected_mask);

    /* Issue #106 (US-106-004): Reset rule frontiers with stratum context awareness.
     * col_session_step is for delta callback mode (no pre-seeded deltas).
     * Always reset affected rules to force re-evaluation.
     * Selective reset based on pre-seeded delta is only in col_session_snapshot.
     *
     * @see col_session_snapshot for selective rule frontier reset (Issue #107) */
    if (affected_mask == UINT64_MAX) {
        /* Full evaluation (non-incremental): reset all rules to (current_epoch, UINT32_MAX)
         * sentinel. Prevents premature skip across different evaluation contexts. */
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            sess->frontier_ops->reset_rule_frontier(sess, ri,
                sess->outer_epoch);
        }
    } else {
        /* Incremental (delta callback mode): reset affected rules to (current_epoch, UINT32_MAX).
         * No pre-seeded deltas in this path, so reset unconditionally. */
        for (uint32_t si = 0; si < plan->stratum_count; si++) {
            if ((affected_mask & ((uint64_t)1 << si)) != 0) {
                uint32_t rule_base = 0;
                for (uint32_t j = 0; j < si; j++)
                    rule_base += plan->strata[j].relation_count;
                for (uint32_t ri = 0; ri < plan->strata[si].relation_count;
                    ri++) {
                    uint32_t rule_id = rule_base + ri;
                    if (rule_id < MAX_RULES) {
                        sess->frontier_ops->reset_rule_frontier(sess, rule_id,
                            sess->outer_epoch);
                    }
                }
            }
        }
    }

    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        /* Skip strata not affected by the last incremental insertion */
        if ((affected_mask & ((uint64_t)1 << si)) == 0)
            continue;

        const wl_plan_stratum_t *sp = &plan->strata[si];
        int rc = sess->delta_cb ? col_stratum_step_with_delta(sp, sess, si)
                                : col_eval_stratum(sp, sess, si);
        if (rc != 0)
            return rc;
    }

    /* Issue #158: Cleanup retraction state and delta relations after step */
    sess->last_removed_relation = NULL;
    sess->retraction_seeded = false;
    /* Remove all $r$<name> relations from session */
    for (uint32_t i = 0; i < sess->nrels;) {
        col_rel_t *r = sess->rels[i];
        if (r && strncmp(r->name, "$r$", 3) == 0) {
            session_remove_rel(sess, r->name);
            /* session_remove_rel shifts array, so don't increment i */
        } else {
            i++;
        }
    }

    /* Issue #217: Compact relation buffers after retraction cleanup.
     * Releases oversized data/timestamps buffers and merge_buf when
     * bulk retractions have left capacity >> nrows. */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r)
            col_rel_compact(r);
    }

    /* Reset after successful eval so next plain session_step runs all strata */
    sess->last_inserted_relation = NULL;
    return 0;
}

/*
 * col_session_set_delta_cb: Register a delta callback on this session
 *
 * Implements wl_compute_backend_t.session_set_delta_cb vtable slot.
 * The callback is invoked with diff=+1 for new tuples during col_session_step.
 *
 * TODO(Phase 2B): Also fire diff=-1 for retracted tuples when semi-naive
 * delta propagation tracks removed tuples explicitly.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Function invoked per output delta tuple (NULL to disable)
 * @param user_data: Opaque pointer passed through to callback
 */
static void
col_session_set_delta_cb(wl_session_t *session, wl_on_delta_fn callback,
    void *user_data)
{
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    sess->delta_cb = callback;
    sess->delta_data = user_data;
}

/*
 * col_session_snapshot: Evaluate all strata and emit current IDB tuples
 *
 * Implements wl_compute_backend_t.session_snapshot vtable slot.
 *
 * Evaluation order:
 *   1. Execute all strata in plan order (col_eval_stratum per stratum)
 *   2. For each IDB relation in each stratum, invoke callback once per row
 *
 * Complexity: O(S * R * N) where S=strata, R=relations per stratum, N=rows
 *
 * TODO(Phase 2B): Snapshot should read from stable R (not recompute);
 * currently re-evaluates on every call which is O(input) per snapshot.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Invoked once per output tuple (relation, row, ncols)
 * @param user_data: Opaque pointer passed through to callback
 * @return 0 on success, EINVAL if session/callback NULL, non-zero on eval error
 */
static int
col_session_snapshot(wl_session_t *session, wl_on_tuple_fn callback,
    void *user_data)
{
    if (!session || !callback)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;

    /* Reset profiling counters for this evaluation pass */
    sess->consolidation_ns = 0;
    sess->kfusion_ns = 0;
    sess->kfusion_alloc_ns = 0;
    sess->kfusion_dispatch_ns = 0;
    sess->kfusion_merge_ns = 0;
    sess->kfusion_cleanup_ns = 0;
    sess->consolidate_fast_hits = 0;
    sess->consolidate_slow_hits = 0;

    /* Phase 4 incremental skip: when last_inserted_relation is set, only
     * re-evaluate strata that transitively depend on the inserted relation.
     * On the first snapshot (total_iterations == 0), always evaluate all strata
     * to establish the baseline. */
    uint64_t affected_mask = UINT64_MAX;
    if (sess->last_inserted_relation != NULL && sess->total_iterations > 0) {
        affected_mask = col_compute_affected_strata(
            session, sess->last_inserted_relation);

        /* Issue #83: Pre-seed EDB delta relations for delta-only propagation.
         * For each relation with nrows > base_nrows, create a $d$<name> delta
         * containing only the new rows. This allows FORCE_DELTA at iteration 0
         * to use the delta instead of the full relation, avoiding full
         * re-derivation of existing IDB tuples. */
        for (uint32_t i = 0; i < sess->nrels; i++) {
            col_rel_t *r = sess->rels[i];
            if (!r || r->base_nrows == 0 || r->nrows <= r->base_nrows)
                continue;
            /* Create delta relation with rows[base_nrows..nrows) */
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", r->name);
            uint32_t delta_nrows = r->nrows - r->base_nrows;
            col_rel_t *delta = col_rel_new_auto(dname, r->ncols);
            if (!delta)
                continue; /* best-effort; falls back to full eval */
            for (uint32_t row = 0; row < delta_nrows; row++) {
                col_rel_append_row(
                    delta, r->data + (size_t)(r->base_nrows + row) * r->ncols);
            }
            session_remove_rel(sess, dname);
            session_add_rel(sess, delta);
        }
        sess->delta_seeded = true;
    }

    /* Issue #264: Activate differential operators when session toggle is on
     * and only partial strata are affected (see col_should_activate_diff). */
    sess->diff_operators_active = col_should_activate_diff(sess, affected_mask);

    /* For affected strata, selectively reset the per-stratum frontier to UINT32_MAX
     * (not-set sentinel) based on pre-seeded EDB delta presence.
     * UINT32_MAX ensures `iter > UINT32_MAX` is always false, forcing full
     * re-evaluation of strata with pre-seeded deltas.
     *
     * Issue #107: Selective frontier reset based on pre-seeded delta check.
     * Reset frontiers ONLY for strata that have pre-seeded EDB deltas.
     * Preserve frontiers for transitively-affected strata (no direct EDB delta).
     *
     * Safety: Transitively-affected strata receive new facts from upstream via
     * delta propagation, but convergence still occurs within previous frontier
     * bounds. Semi-naive evaluation processes deltas incrementally: iteration i
     * only derives from deltas at iteration i-1. If a stratum converged at
     * iteration F (no new facts at F+1+), subsequent upstream facts flow through
     * iterations 0..F, unlikely to require F+1+ unless graph topology changes.
     * Test coverage (test_delta_propagation test 3) validates correctness for
     * cyclic multi-iteration patterns. CSPA benchmark confirms safety. */
    if (affected_mask != UINT64_MAX) {
        for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA;
            si++) {
            if ((affected_mask & ((uint64_t)1 << si)) != 0) {
                /* Issue #107: Selective rule frontier reset based on pre-seeded delta presence.
                 * Reset frontier for strata that have pre-seeded EDB deltas.
                 * Preserve frontier for transitively-affected strata (no direct EDB delta).
                 *
                 * Safety: Transitively-affected strata receive new facts from upstream
                 * strata, but in the presence of pre-seeded deltas, fact propagation
                 * still converges within previous frontier bounds. The pre-seeded delta
                 * check already limits EDB propagation (delta from [base_nrows, nrows)).
                 *
                 * Test coverage: test_delta_propagation validates cyclic correctness. */
                if (stratum_has_preseeded_delta(&plan->strata[si], sess)) {
                    sess->frontier_ops->reset_stratum_frontier(sess, si,
                        sess->outer_epoch);
                }
                /* Else: stratum affected but no pre-seeded delta → KEEP frontier */
            }
        }
        /* Phase 4 (US-4-004) + Issue #107: Selective rule frontier reset.
         * Use col_compute_affected_rules bitmask to identify rules needing
         * re-evaluation. For each affected rule, check if its stratum has
         * pre-seeded EDB delta before resetting the frontier.
         *
         * Reset when:
         *   1. Rule is affected (bit set in affected_rules)
         *   2. Rule's stratum is affected (bit set in affected_mask)
         *   3. Stratum HAS pre-seeded EDB delta
         *
         * Preserve when:
         *   1. Rule's stratum affected but NO pre-seeded delta (transitively affected only)
         *   2. Frontier skip can still fire for iterations beyond previous convergence point
         *
         * Performance: Frontier skip on transitively-affected strata reduces iterations
         * for IDB-only derivations, improving speedup from frontier skip optimization. */
        uint64_t affected_rules
            = col_compute_affected_rules(session, sess->last_inserted_relation);
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            if ((affected_rules & ((uint64_t)1 << ri)) == 0)
                continue;
            uint32_t si = rule_index_to_stratum_index(plan, ri);
            if (si == UINT32_MAX)
                continue;
            if ((affected_mask & ((uint64_t)1 << si)) == 0)
                continue;
            if (stratum_has_preseeded_delta(&plan->strata[si], sess)) {
                sess->frontier_ops->reset_rule_frontier(sess, ri,
                    sess->outer_epoch);
            }
            /* Else: rule's stratum affected but no pre-seeded delta → KEEP frontier */
        }
    } else {
        /* Full re-evaluation (non-incremental call): reset ALL rule frontiers
        * to (current_epoch, UINT32_MAX) sentinel. Prevents premature skip. */
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            sess->frontier_ops->reset_rule_frontier(sess, ri,
                sess->outer_epoch);
        }
    }

    /* Execute strata in order, skipping unaffected ones */
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if ((affected_mask & ((uint64_t)1 << si)) == 0)
            continue;
        int rc = col_eval_stratum(&plan->strata[si], sess, si);
        if (rc != 0) {
            /* Issue #177: Cleanup pre-seeded $d$ deltas on error.
             * If evaluation fails, remove temporary delta relations created
             * during delta-seeded incremental eval. Benign to leave them
             * (replaced on next snapshot), but cleaner to remove. */
            for (uint32_t i = 0; i < sess->nrels; i++) {
                col_rel_t *r = sess->rels[i];
                if (r && strncmp(r->name, "$d$", 3) == 0) {
                    col_rel_destroy(r);
                    sess->rels[i] = NULL;
                }
            }
            /* Compact rels[] to close holes */
            uint32_t out = 0;
            for (uint32_t in = 0; in < sess->nrels; in++) {
                if (sess->rels[in] != NULL)
                    sess->rels[out++] = sess->rels[in];
            }
            sess->nrels = out;
            return rc;
        }
        if (sess->eval_arena)
            wl_arena_reset(sess->eval_arena);
    }

    /* Reset after successful eval so next plain snapshot runs all strata */
    sess->last_inserted_relation = NULL;
    sess->delta_seeded = false;

    /* Issue #83: Update base_nrows for all relations after convergence.
     * This marks the current state as "stable" so the next incremental
     * insert can compute the delta as rows[base_nrows..nrows). */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r)
            r->base_nrows = r->nrows;
    }

    /* Issue #217: Compact relation buffers after convergence.
     * Releases oversized data/timestamps buffers and merge_buf when
     * bulk retractions have left capacity >> nrows. */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r)
            col_rel_compact(r);
    }

    /* Invoke callback for every tuple in every IDB relation */
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const char *rname = sp->relations[ri].name;
            col_rel_t *r = session_find_rel(sess, rname);
            if (!r || r->nrows == 0)
                continue;
            for (uint32_t row = 0; row < r->nrows; row++) {
                callback(rname, r->data + (size_t)row * r->ncols, r->ncols,
                    user_data);
            }
        }
    }
    return 0;
}

/* Affected strata/rules detection moved to columnar/frontier.c;
 * Mobius/Z-set functions moved to columnar/mobius.c;
 * declarations in columnar/internal.h. */
/* ======================================================================== */
/* Vtable Singleton                                                          */
/* ======================================================================== */

static const wl_compute_backend_t col_backend = {
    .name = "columnar",
    .session_create = col_session_create,
    .session_destroy = col_session_destroy,
    .session_insert = col_session_insert,
    .session_remove = col_session_remove,
    .session_step = col_session_step,
    .session_set_delta_cb = col_session_set_delta_cb,
    .session_snapshot = col_session_snapshot,
};

const wl_compute_backend_t *
wl_backend_columnar(void)
{
    return &col_backend;
}
