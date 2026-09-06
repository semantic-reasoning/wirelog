/*
 * backend/columnar_nanoarrow.c - wirelog Nanoarrow Columnar Backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#define _GNU_SOURCE

#include "columnar/compound_side.h"
#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <inttypes.h>
#include <math.h>
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

/* A uint64_t dependency mask cannot represent indices above 63.  Treat an
 * unrepresentable index as affected so a wide plan is evaluated conservatively
 * instead of invoking undefined behaviour or silently skipping work. */
static bool
col_affected_mask_contains(uint64_t mask, uint32_t index)
{
    return index >= 64 || (mask & (UINT64_C(1) << index)) != 0;
}

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

    /* Fast path: try hash lookup first (O(1)) */
    if (sess->rel_hash_nbuckets > 0) {
        col_rel_t *rel = session_rel_hash_lookup(sess, name);
        if (rel)
            return rel;
    }

    /* Lazy initialization: build hash on first use */
    if (sess->rel_hash_nbuckets == 0 && sess->nrels > 0) {
        if (session_rel_build_hash(sess) == 0) {
            /* Retry lookup after rebuild */
            return session_rel_hash_lookup(sess, name);
        }
        /* On malloc failure, fall back to linear search below */
    }

    /* Fallback: linear search (used when hash not available or error) */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0)
            return sess->rels[i];
    }
    return NULL;
}

static uint32_t
session_compound_max_epochs_from_env(void)
{
    const char *env = getenv("WIRELOG_COMPOUND_MAX_EPOCHS");
    if (!env || env[0] == '\0')
        return 0u;
    char *endp = NULL;
    errno = 0;
    unsigned long v = strtoul(env, &endp, 10);
    if (endp == env || *endp != '\0' || errno == ERANGE || v == 0)
        return 0u;
    if (v > (unsigned long)(WL_COMPOUND_EPOCH_MAX + 1u))
        return 0u;
    return (uint32_t)v;
}

static int
session_hash_result_or_fallback(wl_col_session_t *sess, int rc)
{
    if (rc == ENOMEM) {
        session_rel_free_hash(sess);
        return 0;
    }
    return rc;
}

static void
session_invalidate_relation_caches(wl_col_session_t *sess, const char *name)
{
    col_session_invalidate_arrangements(&sess->base, name);
    col_mat_cache_clear(&sess->mat_cache);

    uint32_t out = 0;
    for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
        col_filt_cache_entry_t *e = &sess->filt_cache[i];
        if (strcmp(e->rel_name, name) == 0) {
            free(e->rel_name);
            free(e->filter_data);
            if (e->filtered)
                col_rel_destroy(e->filtered);
            memset(e, 0, sizeof(*e));
            continue;
        }
        if (out != i) {
            sess->filt_cache[out] = sess->filt_cache[i];
            memset(&sess->filt_cache[i], 0, sizeof(sess->filt_cache[i]));
        }
        out++;
    }
    sess->filt_cache_count = out;
}

static void
session_note_inserted_input(wl_col_session_t *sess, const col_rel_t *relation,
    bool advance_epoch)
{
    const char *relation_name = relation->name;

    session_invalidate_relation_caches(sess, relation_name);
    if (advance_epoch)
        sess->outer_epoch++;
    if (sess->last_inserted_relation
        && strcmp(sess->last_inserted_relation, relation_name) != 0)
        sess->pending_full_input_eval = true;
    sess->last_inserted_relation = relation_name;
    sess->pending_input_change = true;
    sess->snapshot_stable_valid = false;
}

int
session_add_rel(wl_col_session_t *sess, col_rel_t *r)
{
    /* Pool-owned structs must be promoted to heap before storing in the
     * session, because col_session_destroy calls free() on each entry. */
    col_rel_t *pool_src = NULL;
    if (r->pool_owned) {
        col_rel_t *heap = (col_rel_t *)calloc(1, sizeof(col_rel_t));
        if (!heap)
            return ENOMEM;
        *heap = *r;
        heap->pool_owned = false;
        /* Zero out source slot so pool_reset doesn't double-free contents */
        memset(r, 0, sizeof(*r));
        pool_src = r;
        r = heap;
    }
    /* Arena-owned data must be promoted to heap before storing in the
     * session, because arena_reset invalidates all arena pointers. */
    if (r->arena_owned && r->columns && r->ncols > 0) {
        /* Promote arena columns to heap: per-column malloc + memcpy */
        int64_t **heap_cols = col_columns_alloc(r->ncols, r->capacity);
        if (!heap_cols)
            goto oom;
        for (uint32_t c = 0; c < r->ncols; c++)
            memcpy(heap_cols[c], r->columns[c],
                sizeof(int64_t) * r->capacity);
        /* free old columns array (arena owns buffers) */
        free((void *)r->columns);
        r->columns = heap_cols;
        r->arena_owned = false;
    }

    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, r->name) == 0) {
            if (sess->rels[i] == r)
                return 0;
            session_invalidate_relation_caches(sess, r->name);
            col_rel_destroy(sess->rels[i]);
            sess->rels[i] = r;
            return 0;
        }
    }

    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (!sess->rels[i]) {
            sess->rels[i] = r;
            int hash_ret = session_hash_result_or_fallback(sess,
                    session_rel_build_hash(sess));
            if (hash_ret != 0)
                return hash_ret;
            return 0;
        }
    }

    if (sess->nrels >= sess->rel_cap) {
        uint32_t nc = sess->rel_cap ? sess->rel_cap * 2 : 16;
        col_rel_t **nr = (col_rel_t **)realloc(
            (void *)sess->rels, sizeof(col_rel_t *) * nc);
        if (!nr)
            goto oom;
        sess->rels = nr;
        sess->rel_cap = nc;
    }
    uint32_t idx = sess->nrels;
    sess->rels[sess->nrels++] = r;

    /* Update hash table for O(1) lookup (Issue #281).
     * May rebuild if load factor exceeded or rebuild on first insert. */
    int hash_ret = session_rel_hash_insert(sess, idx);
    hash_ret = session_hash_result_or_fallback(sess, hash_ret);
    if (hash_ret != 0)
        return hash_ret;
    /* ENOMEM in hash insert is non-fatal; fallback to linear search in
     * session_find_rel. Continue adding relation. */

    return 0;

oom:
    /* Undo the pool-to-heap promotion.  A failed session_add_rel leaves the
     * relation with the caller, which destroys it via col_rel_destroy(), so
     * the promoted copy has to go back into the pool slot before returning.
     * Without this the copy -- and the name, col_names[] and compound
     * metadata it is now the only pointer to -- leaks, and the caller's
     * col_rel_destroy() sees the zeroed slot with pool_owned cleared by the
     * memset above and calls free() on an interior pointer into the
     * delta_pool slab.  Both promotions run on relations from
     * col_rel_pool_new_auto(), which sets pool_owned and arena_owned
     * together. */
    if (pool_src) {
        *pool_src = *r;
        pool_src->pool_owned = true;
        free(r);
    }
    return ENOMEM;
}

void
session_remove_rel(wl_col_session_t *sess, const char *name)
{
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0) {
            session_invalidate_relation_caches(sess, name);
            col_rel_destroy(sess->rels[i]);
            sess->rels[i] = NULL;
            session_rel_free_hash(sess);
            return;
        }
    }
}

typedef struct wl_columnar_session_tdd_decision {
    bool use_tdd;
    wl_columnar_internal_tdd_fallback_reason_t fallback_reason;
} wl_columnar_session_tdd_decision_t;

static const char *
wl_columnar_session_tdd_fallback_reason_name(
    wl_columnar_internal_tdd_fallback_reason_t reason)
{
    switch (reason) {
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE:
        return "none";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NON_RECURSIVE:
        return "non_recursive";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_SNAPSHOT_INELIGIBLE:
        return "snapshot_ineligible";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NO_EXCHANGE:
        return "no_exchange";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN:
        return "unsafe_plan";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS:
        return "adaptive_workers";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_REASON_COUNT:
        break;
    }
    return "unknown";
}

static bool
wl_columnar_session_tdd_stratum_has_exchange(const wl_plan_stratum_t *sp)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];
        for (uint32_t oi = 0; oi < rp->op_count; oi++) {
            if (rp->ops[oi].op == WL_PLAN_OP_EXCHANGE)
                return true;
        }
    }
    return false;
}

static bool
wl_columnar_session_tdd_stratum_is_safe(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    if (tdd_stratum_has_unsupported_lftj(sp))
        return false;

    /* TDD is only beneficial for recursive strata when there is no
     * IDB self-join OR the IDB self-join is exchange-aligned.  A
     * non-aligned self-join forces replicate_mode (all W workers do
     * the full join), which is worse than single-threaded. */
    if (!tdd_stratum_has_idb_self_join(sp)) {
        /* Category A: no IDB-IDB joins.  Owner-partitioned TDD is
         * correct only when every recursive IDB probe uses the
         * relation's EXCHANGE key.  Rules that consume recursive IDB
         * rows through non-exchange keys or multiple IDB body atoms
         * require cross-owner access and must stay single-threaded for
         * now. */
        return stratum_max_idb_body_atoms(sp) <= 1
               && tdd_stratum_single_idb_join_keys_exchange_aligned(sp);
    }
    if (tdd_stratum_idb_self_join_exchange_aligned(sp, sess))
        return true;

    /* Category C: non-aligned IDB-IDB joins. BDX mode is only correct for
     * rules with at most 2 IDB body atoms. */
    return stratum_max_idb_body_atoms(sp) <= 2;
}

static void
wl_columnar_session_tdd_debug_decision(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, bool snapshot_tdd_eligible,
    wl_columnar_session_tdd_decision_t decision)
{
    const char *env = getenv("WIRELOG_TDD_DECISION_DEBUG");
    if (!env || env[0] == '\0' || env[0] == '0')
        return;

    const char *first_rel = (sp && sp->relation_count > 0)
        ? sp->relations[0].name : "(none)";
    wl_tdd_segment_stats_t segment_stats;
    tdd_stratum_segment_stats(sp, &segment_stats);
    fprintf(stderr,
        "TDD decision rel=%s recursive=%d snapshot=%d exchange=%d "
        "safe=%d global_read_candidate=%d self_join=%d idb_atoms=%u "
        "segments=%u segment_seed=%u segment_global_read=%u "
        "segment_unsafe=%u segment_max_idb=%u segment_max_joins=%u "
        "single_key_aligned=%d use_tdd=%d fallback=%s\n",
        first_rel ? first_rel : "(null)",
        sp ? (int)sp->is_recursive : 0,
        (int)snapshot_tdd_eligible,
        sp ? (int)wl_columnar_session_tdd_stratum_has_exchange(sp) : 0,
        sp ? (int)wl_columnar_session_tdd_stratum_is_safe(sp, sess) : 0,
        sp ? (int)tdd_stratum_global_read_candidate(sp) : 0,
        sp ? (int)tdd_stratum_has_idb_self_join(sp) : 0,
        sp ? stratum_max_idb_body_atoms(sp) : 0,
        segment_stats.total_segments,
        segment_stats.seed_only_segments,
        segment_stats.global_read_segments,
        segment_stats.unsafe_segments,
        segment_stats.max_segment_idb_atoms,
        segment_stats.max_segment_join_like,
        sp ? (int)tdd_stratum_single_idb_join_keys_exchange_aligned(sp) : 0,
        (int)decision.use_tdd,
        wl_columnar_session_tdd_fallback_reason_name(
            decision.fallback_reason));
}

static wl_columnar_session_tdd_decision_t
wl_columnar_session_tdd_plan_stratum(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess,
    bool snapshot_tdd_eligible)
{
    wl_columnar_session_tdd_decision_t decision = {
        .use_tdd = false,
        .fallback_reason = WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE,
    };

    if (!sp->is_recursive) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NON_RECURSIVE;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (tdd_stratum_has_unsupported_lftj(sp)) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (!snapshot_tdd_eligible) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_SNAPSHOT_INELIGIBLE;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (!wl_columnar_session_tdd_stratum_has_exchange(sp)) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NO_EXCHANGE;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (!wl_columnar_session_tdd_stratum_is_safe(sp, sess)) {
        const char *env = getenv("WIRELOG_TDD_GLOBAL_READ");
        if (!(env && env[0] == '0' && env[1] == '\0')
            && tdd_stratum_global_read_candidate(sp)) {
            decision.use_tdd = true;
            wl_columnar_session_tdd_debug_decision(sp, sess,
                snapshot_tdd_eligible, decision);
            return decision;
        }
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }

    decision.use_tdd = true;
    wl_columnar_session_tdd_debug_decision(sp, sess, snapshot_tdd_eligible,
        decision);
    return decision;
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
 * col_session_get_exchange_time_ns:
 *
 * Return legacy coordinator serial time across the last wl_session_snapshot()
 * call.  This includes recursive TDD exchange scatter/gather and the
 * non-recursive TDD coordinator merge path.  out_exchange_ns is NULL-safe.
 */
void
col_session_get_exchange_time_ns(wl_session_t *sess,
    uint64_t *out_exchange_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_exchange_ns)
        *out_exchange_ns = cs->exchange_time_ns;
}

/*
 * col_session_get_tdd_perf_stats:
 *
 * Return recursive TDD evaluator profiling counters from the last
 * wl_session_snapshot() call.  The counters expose the current
 * queue-assisted after-barrier implementation without changing execution.
 * All out-parameters are NULL-safe.
 */
void
col_session_get_tdd_perf_stats(wl_session_t *sess, uint64_t *out_total_ns,
    uint64_t *out_dispatch_wait_ns, uint64_t *out_submit_loop_ns,
    uint64_t *out_wait_barrier_ns, uint64_t *out_worker_sum_ns,
    uint64_t *out_worker_max_ns, uint64_t *out_idle_estimate_ns,
    uint64_t *out_queue_drain_ns,
    uint64_t *out_convergence_ns, uint64_t *out_exchange_ns,
    uint64_t *out_final_merge_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_total_ns)
        *out_total_ns = cs->tdd_total_ns;
    if (out_dispatch_wait_ns)
        *out_dispatch_wait_ns = cs->tdd_dispatch_wait_ns;
    if (out_submit_loop_ns)
        *out_submit_loop_ns = cs->tdd_submit_loop_ns;
    if (out_wait_barrier_ns)
        *out_wait_barrier_ns = cs->tdd_wait_barrier_ns;
    if (out_worker_sum_ns)
        *out_worker_sum_ns = cs->tdd_worker_sum_ns;
    if (out_worker_max_ns)
        *out_worker_max_ns = cs->tdd_worker_max_ns;
    if (out_idle_estimate_ns)
        *out_idle_estimate_ns = cs->tdd_idle_estimate_ns;
    if (out_queue_drain_ns)
        *out_queue_drain_ns = cs->tdd_queue_drain_ns;
    if (out_convergence_ns)
        *out_convergence_ns = cs->tdd_convergence_ns;
    if (out_exchange_ns)
        *out_exchange_ns = cs->tdd_exchange_ns;
    if (out_final_merge_ns)
        *out_final_merge_ns = cs->tdd_final_merge_ns;
}

/*
 * wl_columnar_session_get_tdd_exchange_breakdown_stats:
 *
 * Return private recursive TDD exchange sub-counters from the last
 * wl_session_snapshot() call.  Kept out of the installed columnar header so
 * benchmark instrumentation can advance without changing public API shape.
 * All out-parameters are NULL-safe.
 */
#if defined(__GNUC__) && __GNUC__ >= 4
__attribute__((visibility("hidden")))
#endif
void
wl_columnar_session_get_tdd_exchange_breakdown_stats(wl_session_t *sess,
    uint64_t *out_matrix_ns, uint64_t *out_coordinator_ns,
    uint64_t *out_scatter_ns, uint64_t *out_gather_ns,
    uint64_t *out_broadcast_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_matrix_ns)
        *out_matrix_ns = cs->tdd_exchange_matrix_ns;
    if (out_coordinator_ns)
        *out_coordinator_ns = cs->tdd_exchange_coordinator_ns;
    if (out_scatter_ns)
        *out_scatter_ns = cs->tdd_exchange_scatter_ns;
    if (out_gather_ns)
        *out_gather_ns = cs->tdd_exchange_gather_ns;
    if (out_broadcast_ns)
        *out_broadcast_ns = cs->tdd_exchange_broadcast_ns;
}

/*
 * wl_columnar_session_get_tdd_worker_width_stats:
 *
 * Return adaptive TDD worker-width selections from the last snapshot.
 * Hidden benchmark instrumentation hook; not part of the installed API.
 */
#if defined(__GNUC__) && __GNUC__ >= 4
__attribute__((visibility("hidden")))
#endif
void
wl_columnar_session_get_tdd_worker_width_stats(wl_session_t *sess,
    uint32_t *out_last_active_workers, uint32_t *out_max_active_workers)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_last_active_workers)
        *out_last_active_workers = cs->tdd_last_active_workers;
    if (out_max_active_workers)
        *out_max_active_workers = cs->tdd_max_active_workers;
}

/*
 * wl_columnar_session_get_tdd_decision_stats:
 *
 * Return TDD planner decisions from the last snapshot.  Recursive strata are
 * planned TDD-first; fallback counters explain why K-fusion/serial evaluation
 * was used instead.
 */
#if defined(__GNUC__) && __GNUC__ >= 4
__attribute__((visibility("hidden")))
#endif
void
wl_columnar_session_get_tdd_decision_stats(wl_session_t *sess,
    uint32_t *out_recursive_strata, uint32_t *out_executed_strata,
    uint32_t *out_fallback_strata, uint32_t *out_snapshot_ineligible,
    uint32_t *out_no_exchange, uint32_t *out_unsafe_plan,
    uint32_t *out_adaptive_workers, const char **out_last_fallback_reason)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_recursive_strata)
        *out_recursive_strata = cs->tdd_recursive_strata;
    if (out_executed_strata)
        *out_executed_strata = cs->tdd_executed_strata;
    if (out_fallback_strata)
        *out_fallback_strata = cs->tdd_fallback_strata;
    if (out_snapshot_ineligible) {
        *out_snapshot_ineligible =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_SNAPSHOT_INELIGIBLE];
    }
    if (out_no_exchange) {
        *out_no_exchange =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NO_EXCHANGE];
    }
    if (out_unsafe_plan) {
        *out_unsafe_plan =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN];
    }
    if (out_adaptive_workers) {
        *out_adaptive_workers =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS];
    }
    if (out_last_fallback_reason) {
        *out_last_fallback_reason =
            wl_columnar_session_tdd_fallback_reason_name(
            cs->tdd_last_fallback_reason);
    }
}

int
wl_columnar_session_ensure_workqueue(wl_col_session_t *sess,
    uint32_t active_workers)
{
    if (!sess)
        return EINVAL;
    if (active_workers <= 1)
        return 0;
    if (active_workers > sess->num_workers)
        return EINVAL;
    if (sess->wq && sess->wq_workers >= active_workers)
        return 0;

    wl_work_queue_t *new_wq = wl_workqueue_create(active_workers);
    if (!new_wq)
        return ENOMEM;

    wl_workqueue_destroy(sess->wq);
    sess->wq = new_wq;
    sess->wq_workers = active_workers;
    return 0;
}

int
wl_columnar_session_ensure_tdd_worker_slots(wl_col_session_t *sess,
    uint32_t active_workers)
{
    if (!sess)
        return EINVAL;
    if (active_workers == 0 || active_workers > sess->num_workers)
        return EINVAL;
    if (sess->tdd_workers_cap >= active_workers)
        return 0;

    wl_col_session_t *new_workers = (wl_col_session_t *)realloc(
        sess->tdd_workers, active_workers * sizeof(wl_col_session_t));
    if (!new_workers)
        return ENOMEM;

    memset(&new_workers[sess->tdd_workers_cap], 0,
        (active_workers - sess->tdd_workers_cap) * sizeof(wl_col_session_t));
    sess->tdd_workers = new_workers;
    sess->tdd_workers_cap = active_workers;
    return 0;
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
uint64_t
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
 * col_compute_worker_cap: RAM-aware worker cap formula (Issue #409).
 * See declaration in internal.h for full rationale and examples.
 */
uint32_t
col_compute_worker_cap(uint64_t ram_bytes)
{
    if (ram_bytes == 0)
        return 16u;
    uint64_t ram_linear = ram_bytes / (40ULL * 1024 * 1024);
    uint64_t ram_sqrt = (uint64_t)sqrt((double)ram_bytes / 8.0);
    uint64_t dyn = ram_linear < ram_sqrt ? ram_linear : ram_sqrt;
    if (dyn < 16u)
        dyn = 16u;
    if (dyn > 4096u)
        dyn = 4096u;
    return (uint32_t)dyn;
}

/*
 * col_session_create: Initialize a columnar backend session
 *
 * Implements wl_compute_backend_t.session_create vtable slot.
 *
 * @param plan:        Execution plan (borrowed, must outlive session)
 * @param num_workers: Thread pool size cap for parallel K-fusion/TDD.
 *                    Workers are allocated lazily for the active width of a
 *                    given stratum/operator.
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

    /* Issue #600: select rotation strategy via WIRELOG_ROTATION env var.
     * Default is STANDARD; "pinned" selects the pin-aware placeholder.
     * Run init() if the chosen vtable provides one; on failure, treat as
     * ENOMEM and fall through the oom path (init must not have allocated
     * anything else, otherwise it must clean up before returning).
     *
     * #630: the legacy "mvcc" value is no longer accepted -- the
     * strategy was renamed to "pinned" because wirelog is single-mutator
     * and the pin-aware reclamation mechanism (RCU/EBR territory) is
     * the correct mental model.  An "mvcc" env value emits a one-shot
     * stderr migration message and falls through to STANDARD so a
     * stale operator config surfaces loudly rather than silently
     * selecting the placeholder. */
    {
        const col_rotation_ops_t *chosen = &col_rotation_standard_ops;
        const char *rot_env = getenv("WIRELOG_ROTATION");
        const char *strategy_name = "standard";
        if (rot_env && strcmp(rot_env, "pinned") == 0) {
            chosen = &col_rotation_pinned_ops;
            strategy_name = "pinned";
        } else if (rot_env && strcmp(rot_env, "mvcc") == 0) {
            static bool mvcc_migrated = false;
            if (!mvcc_migrated) {
                mvcc_migrated = true;
                fprintf(stderr,
                    "[wirelog] WIRELOG_ROTATION=mvcc was renamed to "
                    "pinned in #630; treating as standard. Set "
                    "WIRELOG_ROTATION=pinned to select the pin-aware "
                    "placeholder.\n");
            }
        }
        sess->rotation_ops = chosen;
        WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
            "event=rotation_ops_select strategy=%s", strategy_name);
        if (chosen->init) {
            int rc = chosen->init(sess);
            if (rc != 0) {
                free(sess);
                return ENOMEM;
            }
        }
    }

    sess->plan = plan;
    sess->intern = plan->intern;
    sess->num_workers = num_workers > 0 ? num_workers : 1;
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO, "session created num_workers=%u",
        sess->num_workers);

    /*
     * WL_MAX_WORKERS: Per-worker and W² exchange buffer cost cap
     *
     * Rationale:
     * - K-fusion k is bounded by max IDB body atoms per recursive rule.
     *   For all known workloads (DOOP k=8-9, CSPA k=5), max k < 16.
     * - TDD distributed eval uses W×W exchange buffer matrix: W² × 8B
     *   allocated by coordinator (Issue #318). Total infra cost per W:
     *   Per-worker linear: 20MB (8MB arena + 4MB pool + 8MB thread stack
     *   + arrangement clones).
     *   Exchange matrix overhead: 8B × W × W (partition buffers sent from
     *   each worker to each other worker).
     *
     * Total memory for W workers: W × 20MB + W² × 8B
     * Examples:
     *   W=16:  320MB + 2KB ≈ 320MB
     *   W=512: 10.2GB + 2MB ≈ 10.2GB
     *   W=1024: 20.4GB + 8MB ≈ 20.4GB
     *
     * Environment variable WIRELOG_MAX_WORKERS allows override [1, 512].
     * Default 16 covers all known workloads with safety margin.
     * Set WL_MEM_REPORT=1 for clamping diagnostics.
     */
#define WL_MAX_WORKERS_HARD_LIMIT 4096u
    {
        /* Dynamic cap: reject num_workers exceeding the RAM-based safe limit
         * (Issue #409). WIRELOG_MAX_WORKERS env override is accepted up to the
         * hard limit with a warning; without override, requests above the
         * RAM-based cap are rejected so callers get a clear error (not silent
         * truncation). See col_compute_worker_cap() for the formula. */
        uint32_t ram_cap = col_compute_worker_cap(col_detect_physical_memory());

        bool has_env_override = false;
        uint32_t env_cap = 0;
        const char *env = getenv("WIRELOG_MAX_WORKERS");
        if (env && env[0] != '\0') {
            char *endp = NULL;
            unsigned long val = strtoul(env, &endp, 10);
            if (endp != env && *endp == '\0' && val > 0) {
                has_env_override = true;
                env_cap = (val <= WL_MAX_WORKERS_HARD_LIMIT)
                    ? (uint32_t)val
                    : WL_MAX_WORKERS_HARD_LIMIT;
                if (env_cap > ram_cap)
                    fprintf(stderr,
                        "[wirelog] WIRELOG_MAX_WORKERS=%u overrides "
                        "RAM-based cap %u; ensure sufficient memory\n",
                        env_cap, ram_cap);
            }
        }

        uint32_t effective_cap = has_env_override ? env_cap : ram_cap;

        if (sess->num_workers > effective_cap) {
            if (!has_env_override) {
                fprintf(stderr,
                    "[wirelog] num_workers %u exceeds RAM-based cap %u "
                    "(set WIRELOG_MAX_WORKERS to override, max %u)\n",
                    sess->num_workers, effective_cap,
                    WL_MAX_WORKERS_HARD_LIMIT);
                free(sess);
                return EINVAL;
            }
            /* Env override active: clamp to the explicit cap */
            if (getenv("WL_MEM_REPORT"))
                fprintf(stderr,
                    "[wirelog] clamping num_workers %u -> %u\n",
                    sess->num_workers, effective_cap);
            sess->num_workers = effective_cap;
        }
    }
#undef WL_MAX_WORKERS_HARD_LIMIT

    /* The callback policy uses the effective cap after RAM/env clamping. */
    sess->callback_configured_workers = sess->num_workers;
    sess->callback_active_workers = 1;
    sess->callback_parallel_execution = false;
    sess->callback_session_key = sess;

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
                /* Global per-join cap: 25% of RAM / (8 bytes * 3 avg cols).
                 * Each K-fusion worker processes a 1/K data partition, so
                 * per-partition join output does NOT scale with K.  Dividing
                 * by num_workers here was a regression (commit 6929689) that
                 * caused silent data loss in multi-worker mode (Issue #404).
                 * Dynamic mem_ledger backpressure handles runtime coordination.
                 *
                 * Avg-col assumption was 5 (Issue #221, /40), but DOOP-class
                 * points-to analyses dominate the recursive recursive-join
                 * cost on 2-3 column intermediates (SubtypeOf, MethodLookup,
                 * VarPointsTo, CallGraphEdge); /40 was conservatively low and
                 * caused DOOP to hit the cap at ~105M intermediate rows on
                 * 16 GB hosts (Issue #791).  /24 is a 67 % headroom bump that
                 * preserves the 25 % RAM safety margin and matches the
                 * narrow-join workloads that dominate the v0.43 portfolio. */
                sess->join_output_limit = (phys / 4) / 24ULL;
            } else {
                sess->join_output_limit
                    = (uint64_t)COL_JOIN_OUTPUT_LIMIT_DEFAULT;
            }
        }
        /* Clamp to UINT32_MAX since nrows is uint32_t */
        if (sess->join_output_limit > UINT32_MAX)
            sess->join_output_limit = UINT32_MAX;
    }

    sess->rel_cap = 16;
    sess->pending_input_change = true;
    sess->rels = (col_rel_t **)calloc(sess->rel_cap, sizeof(col_rel_t *));
    if (!sess->rels) {
        free(sess);
        return ENOMEM;
    }

    /* Worker resources are allocated lazily after an operator/stratum chooses
     * its active width. This preserves workers=N as a cap rather than forcing
     * N threads and N TDD worker slots at session creation. */

    /* Create delta pool for per-iteration temporaries.
     * Slab: 256 relations (cover ~20 rules x 5 ops + headroom)
     * Arena: 64MB initial (for row data buffers) */
    sess->delta_pool
        = delta_pool_create(256, sizeof(col_rel_t), 64UL * 1024 * 1024);
    if (!sess->delta_pool) {
        /* Non-fatal: pool allocation failed, fall back to malloc */
    }

    /* Create per-iteration arena for operator data buffers.
     * 64MB capacity covers typical operator output allocations
     * (COL_REL_INIT_CAP * ncols * sizeof(int64_t) per operator).
     * Reset at each iteration boundary alongside delta_pool_reset.
     * NULL arena is handled gracefully: operators fall back to malloc. */
    sess->eval_arena = wl_arena_create(64UL * 1024 * 1024);
    /* Non-fatal if NULL: col_rel_pool_new_auto falls back to malloc */

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
            if (phys > 0) {
                uint64_t total = (phys / 4ULL) * 3ULL; /* 75% of RAM */
                uint32_t nw = sess->num_workers;
                if (nw > 1) {
                    /* Issue #416 (lazy budget partitioning): coordinator
                     * keeps the full budget so single-threaded strata
                     * (use_tdd=false) are not penalised by an artificially
                     * low backpressure threshold. Worker sessions receive a
                     * per-party budget when they are actually used. */
                    sess->tdd_budget_per_party
                        = total / (uint64_t)(nw + 1);
                    budget = total;
                } else {
                    budget = total;
                }
            }
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
     * getenv() calls in hot paths. Retained as dead-until-retired session
     * fields until the legacy flags are formally removed in a follow-up
     * issue; the active flag surface is now wl_log_thresholds[], seeded by
     * wl_log_init() (see Issue #287 presence-check shim). */
    sess->debug_join = (getenv("WL_DEBUG_JOIN") != NULL);
    sess->consolidation_log = (getenv("WL_CONSOLIDATION_LOG") != NULL);

    /* Issue #287: initialize the structured logger so WL_LOG() in migrated
     * call sites honors WL_LOG / WL_LOG_FILE / legacy presence flags.
     * Idempotent; cheap on re-entry. */
    wl_log_init();

    /* Issue #286: Configurable cache eviction threshold via environment variable.
     * Default: 90% of COL_MAT_CACHE_LIMIT_BYTES (cache evicts when exceeding
     * this threshold). Users can override via WL_MAT_CACHE_EVICT_THRESHOLD_PERCENT
     * environment variable. */
    const char *threshold_env = getenv("WL_MAT_CACHE_EVICT_THRESHOLD_PERCENT");
    int threshold_percent = 90; /* default */
    if (threshold_env) {
        char *endptr;
        long val = strtol(threshold_env, &endptr, 10);
        if (*endptr == '\0' && val >= 1 && val <= 100)
            threshold_percent = (int)val;
        /* Non-numeric or out-of-range: keep default 90% */
    }
    sess->cache_evict_threshold
        = (COL_MAT_CACHE_LIMIT_BYTES * (uint64_t)threshold_percent) / 100;

    /* Issue #216: Initialize arrangement cache LRU tracking.
     * arr_clock starts at 1 so unaccessed entries (lru_clock=0) sort last.
     * arr_cache_limit_bytes defaults to COL_ARR_CACHE_LIMIT_BYTES (256 MB)
     * and can be overridden via WL_ARR_CACHE_LIMIT_BYTES env var. */
    sess->arr_clock = 1;
    sess->arr_total_bytes = 0;
    {
        size_t arr_limit = COL_ARR_CACHE_LIMIT_BYTES;
        const char *arr_limit_env = getenv("WL_ARR_CACHE_LIMIT_BYTES");
        if (arr_limit_env && arr_limit_env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            unsigned long long val = strtoull(arr_limit_env, &endp, 10);
            if (endp != arr_limit_env && *endp == '\0' && errno != ERANGE
                && val > 0)
                arr_limit = (size_t)val;
        }
        sess->arr_cache_limit_bytes = arr_limit;
    }

    /* Issue #559: Side-relation compound arena.  Phase 0 gateway field.
     * Allocated after the simpler malloc/calloc paths so the dedicated
     * `oom:` label is the only cleanup site that has to free it.
     * session_seed=0x53455353u ('SESS') is a placeholder; the remap-aware
     * seed will be plumbed in when handle rotation lands (#586+).
     * default_gen_cap=4096 mirrors the smoke-test usage in
     * tests/test_compound_arena.c; max_epochs=0 selects the library
     * default (WL_COMPOUND_EPOCH_MAX + 1). */
    uint32_t compound_max_epochs = session_compound_max_epochs_from_env();
    sess->compound_arena = wl_compound_arena_create(0x53455353u, 4096u,
            compound_max_epochs);
    if (!sess->compound_arena)
        goto oom;
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
        "event=compound_arena_init seed=0x%08x default_gen_cap=%u "
        "max_epochs=%u",
        0x53455353u, 4096u, sess->compound_arena->max_epochs);
    /* Issue #583: lifecycle audit trail for the compound side-relation
     * subsystem.  WL_LOG=COMPOUND:5 captures create/destroy/alloc/
     * freeze/unfreeze in one section without enabling the noisier
     * SESSION INFO surface. */
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=session_create seed=0x%08x default_gen_cap=%u "
        "max_epochs=%u",
        0x53455353u, 4096u, sess->compound_arena->max_epochs);

    /* Pre-register EDB relations (ncols determined at first insert) */
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        col_rel_t *r = NULL;
        int rc = col_rel_alloc(&r, plan->edb_relations[i]);
        if (rc != 0)
            goto oom;
        /* Issue #535: propagate graph-column metadata from plan to col_rel_t.
         * Guard on non-NULL array to stay compatible with callers that have
         * not populated edb_has_graph_column (defensive; wl_plan_from_program
         * always populates it). */
        if (plan->edb_has_graph_column != NULL
            && plan->edb_has_graph_column[i]) {
            r->has_graph_column = true;
            r->graph_col_idx = plan->edb_graph_col_index[i];
        }
        /* Issue #1038: carry the declared physical width so the first insert
         * is checked against the `.decl` rather than defining it.  Guarded on
         * non-NULL for callers that build a plan by hand and never populate
         * the array; they keep the old first-producer-wins behaviour. */
        if (plan->edb_declared_width != NULL
            && plan->edb_declared_width[i] != WL_PLAN_WIDTH_UNDECLARED)
            r->declared_ncols = plan->edb_declared_width[i];
        /* Preserve declared physical lane types before the first fact is
         * inserted.  In particular, float facts are binary64 bits in the
         * existing 64-bit storage lane and must not be accepted as legacy
         * integers merely because the relation starts empty. */
        bool has_float_type = false;
        if (plan->edb_column_types && plan->edb_column_type_counts
            && plan->edb_column_types[i]
            && plan->edb_column_type_counts[i] > 0) {
            uint32_t type_count = plan->edb_column_type_counts[i];
            for (uint32_t c = 0; c < type_count; c++)
                has_float_type |= plan->edb_column_types[i][c]
                    == WIRELOG_TYPE_FLOAT;
        }
        if (has_float_type) {
            uint32_t type_count = plan->edb_column_type_counts[i];
            rc = col_rel_set_schema(r, type_count, NULL);
            if (rc == 0)
                rc = col_rel_set_column_types(r,
                        plan->edb_column_types[i], type_count);
            if (rc != 0) {
                col_rel_destroy(r);
                goto oom;
            }
        }
        rc = session_add_rel(sess, r);
        if (rc != 0) {
            col_rel_destroy(r);
            goto oom;
        }
    }

    /* Issue #535: Auto-create __graph_metadata when any EDB has __graph_id.
     * The relation is empty at creation; user code populates it.
     * Duplicate guard: skip if already declared explicitly. */
    {
        bool any_graph_enabled = false;
        if (plan->edb_has_graph_column != NULL) {
            for (uint32_t i = 0; i < plan->edb_count; i++) {
                if (plan->edb_has_graph_column[i]) {
                    any_graph_enabled = true;
                    break;
                }
            }
        }
        if (any_graph_enabled
            && session_find_rel(sess, "__graph_metadata") == NULL) {
            col_rel_t *meta = NULL;
            int rc = col_rel_alloc(&meta, "__graph_metadata");
            if (rc != 0)
                goto oom;
            static const char *const meta_cols[6] = {
                "graph_id", "tenant", "timestamp", "location", "risk",
                "description"
            };
            rc = col_rel_set_schema(meta, 6, meta_cols);
            if (rc != 0) {
                col_rel_destroy(meta);
                goto oom;
            }
            rc = session_add_rel(sess, meta);
            if (rc != 0) {
                col_rel_destroy(meta);
                goto oom;
            }
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

    /* Issue #317: Initialize per-worker frontier progress tracker.
     * Uses MAX_STRATA so the tracker is valid without knowing the plan's
     * stratum count at init time.  Failure is non-fatal: progress tracking
     * degrades gracefully (all_converged returns false, min returns UINT32_MAX).
     * The oom label does NOT free progress.entries because wl_frontier_progress_init
     * only sets entries on success; on ENOMEM entries remains NULL so destroy is safe. */
    wl_frontier_progress_init(&sess->progress, sess->num_workers, MAX_STRATA);

    *out = &sess->base;
    return 0;

oom:
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free((void *)sess->rels);
    wl_workqueue_destroy(sess->wq);       /* NULL-safe */
    delta_pool_destroy(sess->delta_pool); /* NULL-safe */
    wl_compound_arena_free(sess->compound_arena); /* NULL-safe (Issue #559) */
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
    wl_columnar_delta_events_clear(sess);

    /* Issue #1380: emit the memory baseline before teardown when explicitly
     * requested.  The ledger is session-owned, so this is the last point at
     * which current and peak values for relations, arenas, caches,
     * arrangements, and timestamps are all available.  Keep the report
     * opt-in so normal runs and their stderr remain unchanged. */
    if (getenv("WL_MEM_REPORT")) {
        fprintf(stderr, "[wirelog mem] scope=coordinator workers=%u\n",
            sess->num_workers);
        wl_mem_ledger_report(&sess->mem_ledger);
    }

    /* Issue #959: report the join-output high-water mark before teardown.
     *
     * The open question is whether dividing the row cap by W is right --
     * col_session_worker_init() gives each worker join_output_limit / W,
     * which assumes a worker's largest intermediate is 1/W of the
     * single-threaded one.  Workers are separate sessions, so each prints
     * its own line and the ratio is readable directly:
     *
     *     WL_LOG=SESSION:4 ... 2>&1 | grep join_output_peak
     *
     * A worker peak near single_threaded_peak / W means the division is
     * sound for that workload; a much larger one is the reproduction #959
     * has been missing, and also sizes the fix.  DEBUG rather than INFO so
     * it does not appear in ordinary runs.
     *
     * Known gap, stated rather than left to be discovered: only the
     * coordinator reaches here.  Worker sessions are bitwise copies torn
     * down on their own path and never call col_session_destroy, so at W=8
     * this still prints exactly one line.  What it gives today is the
     * single-threaded reference number -- the denominator of the ratio --
     * and confirmation that it does not change with W.  Capturing the
     * numerator needs the same emit on the worker teardown path, which is
     * the next step for #959 and is deliberately not guessed at here. */
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_DEBUG,
        "join_output_peak rows=%llu limit=%llu workers=%u",
        (unsigned long long)sess->join_output_peak,
        (unsigned long long)sess->join_output_limit,
        sess->num_workers);

    /* Issue #600: tear down rotation strategy first so the destroy hook
     * still sees a fully-populated session (eval_arena, compound_arena,
     * etc.) before any of the other teardown frees them. NULL-safe. */
    if (sess->rotation_ops && sess->rotation_ops->destroy)
        sess->rotation_ops->destroy(sess);
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free((void *)sess->rels);
    /* Free relation name hash table (Issue #281) */
    session_rel_free_hash(sess);
    col_mat_cache_clear(&sess->mat_cache);
    wl_workqueue_destroy(sess->wq);
    wl_kfusion_adaptive_destroy(sess->kfusion_adaptive);
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
    col_session_free_filt_arrangements(sess);
    /* Free contents of pool-allocated relations before bulk destroy.
     * delta_pool_destroy frees the slab/arena but skips individually
     * malloc'd members (name, columns, col_names, row_scratch). */
    {
        delta_pool_t *dp = sess->delta_pool;
        if (dp) {
            for (uint32_t s = 0; s < dp->slot_used; s++) {
                col_rel_t *pr = (col_rel_t *)(dp->slab
                    + (size_t)s * dp->slot_size);
                col_rel_free_contents(pr);
            }
        }
    }
    delta_pool_destroy(sess->delta_pool);
    wl_arena_free(sess->eval_arena);
    /* Free exchange buffer matrix (Issue #316): coordinator-owned W x W grid */
    if (sess->exchange_bufs) {
        for (uint32_t src = 0; src < sess->exchange_num_workers; src++) {
            if (sess->exchange_bufs[src]) {
                for (uint32_t dst = 0; dst < sess->exchange_num_workers; dst++)
                    col_rel_destroy(sess->exchange_bufs[src][dst]);
                free((void *)sess->exchange_bufs[src]);
            }
        }
        free((void *)sess->exchange_bufs);
    }
    /* Issue #317: Free per-worker frontier progress tracker.
     * Worker sessions have progress.entries == NULL (set in col_worker_session_create)
     * so this is safe to call on both coordinator and worker sessions. */
    wl_frontier_progress_destroy(&sess->progress);
    /* Issue #318: Free TDD worker sessions array.
     * Workers that have been initialized (w < tdd_workers_count) are destroyed
     * first, then the array itself is freed.  Worker sessions that were
     * initialized via col_worker_session_create own their own arena/pool/rels. */
    if (sess->tdd_workers) {
        for (uint32_t w = 0; w < sess->tdd_workers_count; w++)
            col_worker_session_destroy(&sess->tdd_workers[w]);
        free(sess->tdd_workers);
    }
    /* Issue #386: Free filtered relation cache */
    for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
        free(sess->filt_cache[i].rel_name);
        free(sess->filt_cache[i].filter_data);
        if (sess->filt_cache[i].filtered)
            col_rel_destroy(sess->filt_cache[i].filtered);
    }
    free(sess->filt_cache);
    /* Issue #559: free side-relation compound arena (NULL-safe). */
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
        "event=compound_arena_destroy live_handles=%llu",
        sess->compound_arena
            ? (unsigned long long)sess->compound_arena->live_handles
            : 0ULL);
    /* Issue #583: lifecycle audit trail (mirrors session_create). */
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=session_destroy live_handles=%llu",
        sess->compound_arena
            ? (unsigned long long)sess->compound_arena->live_handles
            : 0ULL);
    wl_compound_arena_free(sess->compound_arena);
    free(sess);
}

/* ======================================================================== */
/* Per-Worker Session State (Issue #315)                                    */
/* ======================================================================== */

/*
 * col_worker_session_create:
 * Create an isolated worker session from a coordinator session.
 * See internal.h for full documentation.
 */
int
col_worker_session_create(wl_col_session_t *coordinator,
    uint32_t worker_id, col_rel_t **partitions,
    uint32_t num_partitions, wl_col_session_t *out_worker)
{
    if (!coordinator || !out_worker || (!partitions && num_partitions > 0))
        return EINVAL;

    /* Step 1: Bitwise copy — copies all value fields (frontiers,
     * counters, booleans, plan pointer, frontier_ops). */
    *out_worker = *coordinator;

    /* Step 2: Set identity fields */
    out_worker->worker_id = worker_id;
    out_worker->coordinator = coordinator;
    out_worker->extension_expr_status = 0;
    out_worker->callback_session_key = coordinator->callback_session_key;
    out_worker->delta_events = NULL;
    out_worker->delta_event_count = 0;
    out_worker->delta_event_capacity = 0;
    out_worker->delta_event_transaction = false;

    /* Prevent accidental wl_session_destroy on stack-allocated worker */
    out_worker->base.backend = NULL;
    out_worker->base.owns_extension_snapshot = false;

    /* Step 3: NULL all owned pointers (safe for cleanup on early abort) */
    out_worker->wq = NULL;
    out_worker->wq_workers = 0;
    out_worker->kfusion_adaptive = NULL;
    out_worker->eval_arena = NULL;
    out_worker->delta_pool = NULL;
    out_worker->rels = NULL;
    out_worker->nrels = 0;
    out_worker->rel_cap = 0;
    out_worker->rel_hash_head = NULL;
    out_worker->rel_hash_next = NULL;
    out_worker->rel_hash_nbuckets = 0;
    out_worker->rel_hash_chain_cap = 0;
    out_worker->arr_entries = NULL;
    out_worker->arr_count = 0;
    out_worker->arr_cap = 0;
    out_worker->diff_arr_entries = NULL;
    out_worker->diff_arr_count = 0;
    out_worker->diff_arr_cap = 0;
    out_worker->darr_entries = NULL;
    out_worker->darr_count = 0;
    out_worker->darr_cap = 0;
    out_worker->sarr_entries = NULL;
    out_worker->sarr_count = 0;
    out_worker->sarr_cap = 0;
    out_worker->filt_arr_entries = NULL;
    out_worker->filt_arr_count = 0;
    out_worker->filt_arr_cap = 0;
    out_worker->filt_cache = NULL;
    out_worker->filt_cache_count = 0;
    out_worker->filt_cache_cap = 0;
    /* Issue #579 / R-5: workers BORROW the coordinator's frozen arena.
     * Worker destroy must NOT free this pointer (see
     * col_worker_session_destroy).  The K-Fusion freeze contract
     * (Issue #561) guarantees the arena is frozen for the duration
     * of worker access. */
    out_worker->compound_arena = coordinator->compound_arena;
    memset(&out_worker->mat_cache, 0, sizeof(col_mat_cache_t));
    /* Exchange buffers are owned by coordinator; worker inherits borrowed ptr */
    out_worker->exchange_bufs = NULL;
    out_worker->exchange_num_workers = 0;
    /* Issue #318: TDD workers array is owned by coordinator; workers have none */
    out_worker->tdd_workers = NULL;
    out_worker->tdd_workers_cap = 0;
    out_worker->tdd_workers_count = 0;
    /* Issue #317: Worker does not own the progress tracker.
     * NULL entries so col_session_destroy (if called on worker) does not
     * double-free the coordinator's entries array. */
    out_worker->progress.entries = NULL;
    out_worker->progress.num_workers = 0;
    out_worker->progress.num_strata = 0;

    /* Step 4: NULL borrowed fields that workers must not use */
    out_worker->delta_cb = NULL;
    out_worker->delta_data = NULL;
    out_worker->last_inserted_relation = NULL;
    out_worker->last_removed_relation = NULL;

    /* Step 5: Initialize independent mem_ledger (avoid copying atomics).
     * Workers receive a per-party budget, but the coordinator keeps its
     * original budget. W is an upper bound on active workers, and shrinking the
     * coordinator permanently causes later sequential strata to trip
     * backpressure under a stale worker-sized budget. */
    uint32_t active_workers = coordinator->tdd_active_workers > 0
        ? coordinator->tdd_active_workers : coordinator->num_workers;
    if (active_workers == 0)
        active_workers = 1;
    uint64_t worker_budget;
    if (coordinator->tdd_budget_per_party > 0) {
        uint64_t max_workers = coordinator->num_workers > 0
            ? coordinator->num_workers : active_workers;
        uint64_t active_party_budget = (coordinator->tdd_budget_per_party
            * (max_workers + 1)) / (uint64_t)(active_workers + 1);
        if (active_party_budget == 0)
            active_party_budget = coordinator->tdd_budget_per_party;
        worker_budget = active_party_budget;
    } else {
        worker_budget = atomic_load_explicit(
            &coordinator->mem_ledger.total_budget, memory_order_relaxed)
            / active_workers;
    }
    wl_mem_ledger_init(&out_worker->mem_ledger, worker_budget);

    /* Issue #426: Scale join_output_limit per worker.
     * The bitwise copy above gave the worker the coordinator's full limit;
     * shrink to a fair per-worker share (0 = disabled, preserved as-is).
     * Clamp to minimum 1 so that limit < W does not accidentally disable
     * the cap (0 means unlimited). */
    if (active_workers > 0 && coordinator->join_output_limit > 0) {
        uint64_t per_worker =
            coordinator->join_output_limit / active_workers;
        out_worker->join_output_limit = per_worker > 0 ? per_worker : 1;
    }

    /* Step 6: Populate rels[] with partition relations (ownership transfer) */
    if (num_partitions > 0) {
        out_worker->rels
            = (col_rel_t **)calloc(num_partitions, sizeof(col_rel_t *));
        if (!out_worker->rels)
            goto cleanup;
        for (uint32_t i = 0; i < num_partitions; i++) {
            out_worker->rels[i] = partitions[i];
            partitions[i] = NULL; /* Mark as transferred */
        }
    }
    out_worker->nrels = num_partitions;
    out_worker->rel_cap = num_partitions;

    /* Step 7: Allocate per-worker arena (scaled by num_workers) */
    {
        uint32_t k = active_workers > 0 ? active_workers : 1;
        size_t arena_cap = coordinator->eval_arena
            ? coordinator->eval_arena->capacity / k
            : 8UL * 1024 * 1024;
        if (arena_cap < 8UL * 1024 * 1024)
            arena_cap = 8UL * 1024 * 1024;
        out_worker->eval_arena = wl_arena_create(arena_cap);
        /* Non-fatal if NULL: operators fall back to malloc */
    }

    /* Step 8: Allocate per-worker delta_pool (scaled by num_workers) */
    {
        uint32_t k = active_workers > 0 ? active_workers : 1;
        size_t pool_arena = 32UL * 1024 * 1024 / k;
        if (pool_arena < 4UL * 1024 * 1024)
            pool_arena = 4UL * 1024 * 1024;
        uint32_t pool_slots = 128 / k;
        if (pool_slots < 16)
            pool_slots = 16;
        out_worker->delta_pool
            = delta_pool_create(pool_slots, sizeof(col_rel_t), pool_arena);
        /* Non-fatal if NULL: operators fall back to malloc */
    }

    /* Step 9: Deep-clone arrangement registries */
    if (coordinator->arr_count > 0) {
        int rc = col_arr_entries_clone(coordinator->arr_entries,
                coordinator->arr_count, &out_worker->arr_entries,
                &out_worker->arr_cap);
        if (rc != 0)
            goto cleanup;
        out_worker->arr_count = coordinator->arr_count;
    }
    if (coordinator->diff_arr_count > 0) {
        int rc = col_diff_arr_entries_clone(coordinator->diff_arr_entries,
                coordinator->diff_arr_count, &out_worker->diff_arr_entries,
                &out_worker->diff_arr_cap);
        if (rc != 0)
            goto cleanup;
        out_worker->diff_arr_count = coordinator->diff_arr_count;
    }

    return 0;

cleanup:
    col_worker_session_destroy(out_worker);
    return ENOMEM;
}

/*
 * col_worker_session_destroy:
 * Free all resources owned by a worker session.
 * See internal.h for full documentation.
 */
void
col_worker_session_destroy(wl_col_session_t *worker)
{
    if (!worker)
        return;
    wl_columnar_delta_events_clear(worker);

    /* Issue #1380: worker ledgers are independent copies with their own
     * allocations and peaks.  Report them before freeing worker-owned
     * relations, pools, and arenas so bounded-memory runs can compare worker
     * peaks instead of seeing only the coordinator baseline. */
    if (getenv("WL_MEM_REPORT")) {
        fprintf(stderr, "[wirelog mem] scope=worker id=%u workers=%u\n",
            worker->worker_id, worker->num_workers);
        wl_mem_ledger_report(&worker->mem_ledger);
    }

    /* Free mat_cache entries (all worker-owned since zeroed at create) */
    col_mat_cache_clear(&worker->mat_cache);

    /* Free arrangement registries */
    for (uint32_t i = 0; i < worker->arr_count; i++) {
        free(worker->arr_entries[i].rel_name);
        free(worker->arr_entries[i].key_cols);
        arr_free_contents(&worker->arr_entries[i].arr);
    }
    free(worker->arr_entries);
    col_session_free_delta_arrangements(worker);
    col_session_free_sorted_arrangements(worker);
    col_session_free_diff_arrangements(worker);
    col_session_free_filt_arrangements(worker);

    /* Free owned relations (partition data) */
    for (uint32_t i = 0; i < worker->nrels; i++) {
        if (worker->rels[i]) {
            /* A failed TDD setup can leave the same relation pointer in more
             * than one slot while ownership is being transferred.  Only the
             * first slot owns the relation; later aliases must not free it a
             * second time. */
            bool duplicate = false;
            for (uint32_t j = 0; j < i; j++) {
                if (worker->rels[j] == worker->rels[i]) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) {
                worker->rels[i] = NULL;
                continue;
            }
            col_rel_free_contents(worker->rels[i]);
            free(worker->rels[i]);
        }
    }
    free((void *)worker->rels);

    /* Free hash table (may have been lazily built) */
    session_rel_free_hash(worker);

    /* Free allocators (all NULL-safe) */
    wl_workqueue_destroy(worker->wq);
    /* Free contents of pool-allocated relations before bulk destroy. */
    {
        delta_pool_t *dp = worker->delta_pool;
        if (dp) {
            for (uint32_t s = 0; s < dp->slot_used; s++) {
                col_rel_t *pr = (col_rel_t *)(dp->slab
                    + (size_t)s * dp->slot_size);
                col_rel_free_contents(pr);
            }
        }
    }
    delta_pool_destroy(worker->delta_pool);
    wl_arena_free(worker->eval_arena);

    /* compound_arena is BORROWED from coordinator (Issue #579 / R-5).
     * DO NOT call wl_compound_arena_free here — the coordinator owns it
     * and frees it in its own col_session_destroy path. */

    /* Issue #386: Free filtered relation cache (workers own their own copy) */
    for (uint32_t i = 0; i < worker->filt_cache_count; i++) {
        free(worker->filt_cache[i].rel_name);
        free(worker->filt_cache[i].filter_data);
        if (worker->filt_cache[i].filtered)
            col_rel_destroy(worker->filt_cache[i].filtered);
    }
    free(worker->filt_cache);

    /* Zero the struct to prevent dangling pointer use */
    memset(worker, 0, sizeof(*worker));
}

int
col_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    if (num_rows == 0)
        return 0;

    /* Issue #662: When a delta callback is installed, EDB inserts must take
     * the incremental path so arrangement caches are invalidated and the
     * outer-epoch counter advances; otherwise a subsequent col_session_step
     * with delta_cb may run differential operators against stale arrangement
     * indices and surface as -1/EINVAL.  This mirrors col_session_remove,
     * which already reroutes to col_session_remove_incremental in the same
     * condition. */
    wl_col_session_t *sess = COL_SESSION(session);
    if (sess->delta_cb != NULL)
        return col_session_insert_incremental(session, relation, data,
                   num_rows, num_cols);

    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;

    /* Lazy schema initialisation on first insert.  Issue #1038: the first
     * insert used to *define* the width; it is now checked against the
     * declared physical width when the program declared one, so a host
     * cannot establish a relation at a shape its own `.decl` contradicts. */
    if (r->ncols == 0) {
        if (r->declared_ncols != 0 && num_cols != r->declared_ncols)
            return EINVAL;
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

    session_note_inserted_input(sess, r, false);
    /* The non-incremental API must force a full epoch evaluation even when
     * the previous update targeted the same relation. */
    sess->pending_full_input_eval = true;

    return 0;
}

static bool
compound_arg_type_valid(wirelog_column_type_t type)
{
    switch (type) {
    case WIRELOG_TYPE_INT32:
    case WIRELOG_TYPE_INT64:
    case WIRELOG_TYPE_UINT32:
    case WIRELOG_TYPE_UINT64:
    case WIRELOG_TYPE_FLOAT:
    case WIRELOG_TYPE_STRING:
    case WIRELOG_TYPE_BOOL:
        return true;
    }
    return false;
}

static int
col_session_make_compound(wl_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out)
{
    if (handle_out)
        *handle_out = WIRELOG_COMPOUND_HANDLE_NULL;
    if (!session || !functor || !args || arity == 0 || !handle_out)
        return EINVAL;
    if (arity > UINT32_MAX - 1u)
        return EOVERFLOW;
    if (arity > UINT32_MAX / (uint32_t)sizeof(int64_t))
        return EOVERFLOW;
    for (uint32_t i = 0; i < arity; i++) {
        if (!compound_arg_type_valid(args[i].type))
            return EINVAL;
    }

    wl_col_session_t *sess = COL_SESSION(session);
    if (!sess || !sess->compound_arena)
        return EINVAL;
    if (sess->compound_arena->frozen)
        return EBUSY;
    if (sess->compound_arena->current_epoch >= sess->compound_arena->max_epochs)
        return ENOSPC;

    col_rel_t *side_rel = NULL;
    int rc = wl_compound_side_ensure(sess, functor, arity, &side_rel);
    if (rc != 0)
        return rc;
    if (!side_rel)
        return EINVAL;

    uint32_t payload_size = arity * (uint32_t)sizeof(int64_t);
    uint64_t handle = wl_compound_arena_alloc(sess->compound_arena,
            payload_size);
    if (handle == WIRELOG_COMPOUND_HANDLE_NULL) {
        if (sess->compound_arena->frozen)
            return EBUSY;
        if (sess->compound_arena->current_epoch
            >= sess->compound_arena->max_epochs)
            return ENOSPC;
        return ENOMEM;
    }

    uint32_t ncols = arity + 1u;
    int64_t stack_row[16];
    int64_t *row = stack_row;
    if (ncols > (uint32_t)(sizeof(stack_row) / sizeof(stack_row[0]))) {
        row = (int64_t *)malloc((size_t)ncols * sizeof(*row));
        if (!row) {
            wl_compound_arena_retain(sess->compound_arena, handle, -1);
            return ENOMEM;
        }
    }

    row[0] = (int64_t)handle;
    for (uint32_t i = 0; i < arity; i++)
        row[i + 1u] = args[i].value;

    rc = col_rel_append_row(side_rel, row);
    if (row != stack_row)
        free(row);
    if (rc != 0) {
        wl_compound_arena_retain(sess->compound_arena, handle, -1);
        return rc;
    }

    session_note_inserted_input(sess, side_rel, true);
    *handle_out = handle;
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

    /* Lazy schema initialisation on first insert.  Issue #1038: the first
     * insert used to *define* the width; it is now checked against the
     * declared physical width when the program declared one, so a host
     * cannot establish a relation at a shape its own `.decl` contradicts. */
    if (r->ncols == 0) {
        if (r->declared_ncols != 0 && num_cols != r->declared_ncols)
            return EINVAL;
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

    wl_col_session_t *sess = COL_SESSION(session);
    session_note_inserted_input(sess, r, true);
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

    int64_t row_stack[COL_STACK_MAX];
    int64_t *row_buf = row_stack;
    if (num_cols > COL_STACK_MAX) {
        row_buf = (int64_t *)malloc(num_cols * sizeof(int64_t));
        if (!row_buf)
            return ENOMEM;
    }

    /* Compact: remove matching rows */
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        uint32_t out_r = 0;
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            col_rel_row_copy_out(r, ri, row_buf);
            if (memcmp(row_buf, del, sizeof(int64_t) * num_cols) != 0) {
                if (out_r != ri)
                    col_rel_row_copy_in(r, out_r, row_buf);
                out_r++;
            } else {
                /* Remove first matching row only */
                di = num_rows; /* break outer loop after this one */
                for (uint32_t rest = ri + 1; rest < r->nrows; rest++, out_r++)
                    col_rel_row_move(r, out_r, rest);
                r->nrows = out_r;
                goto next_del;
            }
        }
        r->nrows = out_r;
next_del:;
    }
    if (row_buf != row_stack)
        free(row_buf);
    session_invalidate_relation_caches(sess, r->name);
    sess->pending_input_change = true;
    sess->snapshot_stable_valid = false;
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
    snprintf(rname, sizeof(rname), "$r$%s", r->name);

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
            const int64_t *row = col_rel_row(r, ri);
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

    int64_t row_stack[COL_STACK_MAX];
    int64_t *row_buf = row_stack;
    if (num_cols > COL_STACK_MAX) {
        row_buf = (int64_t *)malloc(num_cols * sizeof(int64_t));
        if (!row_buf) {
            col_rel_destroy(rdelta);
            return ENOMEM;
        }
    }

    /* Register $r$<name> in session (replacing any prior) */
    session_remove_rel(sess, rname);
    rc = session_add_rel(sess, rdelta);
    if (rc != 0) {
        col_rel_destroy(rdelta);
        if (row_buf != row_stack)
            free(row_buf);
        return rc;
    }

    /* Remove rows from the EDB using existing compact logic */
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        uint32_t out_r = 0;
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            col_rel_row_copy_out(r, ri, row_buf);
            if (memcmp(row_buf, del, sizeof(int64_t) * num_cols) != 0) {
                if (out_r != ri)
                    col_rel_row_copy_in(r, out_r, row_buf);
                out_r++;
            } else {
                /* Remove first matching row only */
                di = num_rows; /* break outer loop after this one */
                for (uint32_t rest = ri + 1; rest < r->nrows; rest++, out_r++)
                    col_rel_row_move(r, out_r, rest);
                r->nrows = out_r;
                goto next_del_incr;
            }
        }
        r->nrows = out_r;
next_del_incr:;
    }
    if (row_buf != row_stack)
        free(row_buf);

    /* Clamp base_nrows to current row count */
    if (r->base_nrows > r->nrows)
        r->base_nrows = r->nrows;

    /* Issue #472: Invalidate arrangement caches for the modified relation
     * so subsequent re-evaluation rebuilds hash indices without the removed
     * rows.  Without this, cached arrangements contain stale entries that
     * produce phantom join matches during full re-eval retraction. */
    session_invalidate_relation_caches(sess, r->name);

    /* Mark removal for affected-stratum calculation */
    sess->last_removed_relation = r->name;
    sess->outer_epoch++;
    sess->pending_input_change = true;

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
 * TODO(#810): Replace set-diff delta with semi-naive ΔR propagation.
 *
 * @param session: wl_session_t* (cast to wl_col_session_t* internally)
 * @return 0 on success, non-zero on evaluation error
 */
static int
col_session_step(wl_session_t *session)
{
    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;
    sess->extension_expr_status = 0;

    if (sess->delta_cb && !sess->pending_input_change
        && sess->last_inserted_relation == NULL
        && sess->last_removed_relation == NULL)
        return 0;

    /* Compute affected strata bitmask (Phase 4 incremental skip).
     * A step may carry both an insertion and a removal, and the two
     * relations need not share a stratum, so the mask is the UNION of the
     * strata reachable from each -- seeded at 0 so a single-source step
     * still narrows to just that source's strata (Issue #1031).  When
     * neither pointer is set (regular step) UINT64_MAX means all strata are
     * evaluated, and pending_full_input_eval ("this step cannot be
     * incremental") keeps that default no matter what is pending. */
    uint64_t affected_mask = UINT64_MAX;
    if (!sess->pending_full_input_eval
        && (sess->last_inserted_relation != NULL
        || sess->last_removed_relation != NULL)) {
        affected_mask = 0;
        if (sess->last_inserted_relation != NULL) {
            affected_mask |= col_compute_affected_strata(
                session, sess->last_inserted_relation);
        }
        if (sess->last_removed_relation != NULL) {
            affected_mask |= col_compute_affected_strata(
                session, sess->last_removed_relation);
        }
    }

    /* Issue #158: Pre-seed retraction deltas.  If last_removed_relation is
     * set (removal via col_session_remove_incremental), check if $r$<name>
     * exists for retractions, and set retraction_seeded.  This block does
     * not narrow affected_mask; the removal's contribution to it is unioned
     * in above (Issue #1031). */
    if (sess->last_removed_relation != NULL) {
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
            if (col_affected_mask_contains(affected_mask, si)) {
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

    if (sess->delta_cb) {
        wl_columnar_delta_events_clear(sess);
        sess->delta_event_transaction = true;
    }
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        /* Skip strata not affected by the last incremental insertion */
        if (!col_affected_mask_contains(affected_mask, si))
            continue;

        const wl_plan_stratum_t *sp = &plan->strata[si];
        int rc = sess->delta_cb ? col_stratum_step_with_delta(sp, sess, si)
                                : col_eval_stratum_tdd(sp, sess, si);
        if (rc != 0) {
            if (sess->delta_cb) {
                sess->delta_event_transaction = false;
                wl_columnar_delta_events_clear(sess);
            }
            return rc;
        }
    }
    if (sess->delta_cb) {
        sess->delta_event_transaction = false;
        wl_columnar_delta_events_publish(sess);
        wl_columnar_delta_events_clear(sess);
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
    sess->pending_input_change = false;
    sess->pending_full_input_eval = false;
    sess->has_evaluated = true;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r)
            r->base_nrows = r->nrows;
    }
    sess->snapshot_stable_valid = true;
    return 0;
}

/*
 * col_session_set_delta_cb: Register a delta callback on this session
 *
 * Implements wl_compute_backend_t.session_set_delta_cb vtable slot.
 * The callback is invoked with diff=+1 for new tuples during col_session_step.
 *
 * TODO(#810): Also fire diff=-1 for retracted tuples when semi-naive
 * delta propagation tracks removed tuples explicitly.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Function invoked per output delta tuple (NULL to disable)
 * @param user_data: Opaque pointer passed through to callback
 */
static void
col_session_set_delta_cb(wl_session_t *session, wirelog_on_delta_fn callback,
    void *user_data)
{
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    sess->delta_cb = callback;
    sess->delta_data = user_data;
}

static void
col_session_reset_snapshot_profile(wl_col_session_t *sess)
{
    memset(&sess->tdd_audit, 0, sizeof(sess->tdd_audit));
    sess->consolidation_ns = 0;
    sess->kfusion_ns = 0;
    sess->kfusion_alloc_ns = 0;
    sess->kfusion_dispatch_ns = 0;
    sess->kfusion_merge_ns = 0;
    sess->kfusion_cleanup_ns = 0;
    sess->consolidate_fast_hits = 0;
    sess->consolidate_slow_hits = 0;
    sess->exchange_time_ns = 0;
    sess->tdd_total_ns = 0;
    sess->tdd_dispatch_wait_ns = 0;
    sess->tdd_submit_loop_ns = 0;
    sess->tdd_wait_barrier_ns = 0;
    sess->tdd_worker_sum_ns = 0;
    sess->tdd_worker_max_ns = 0;
    sess->tdd_idle_estimate_ns = 0;
    sess->tdd_queue_drain_ns = 0;
    sess->tdd_convergence_ns = 0;
    sess->tdd_exchange_ns = 0;
    sess->tdd_exchange_matrix_ns = 0;
    sess->tdd_exchange_coordinator_ns = 0;
    sess->tdd_exchange_scatter_ns = 0;
    sess->tdd_exchange_gather_ns = 0;
    sess->tdd_exchange_broadcast_ns = 0;
    sess->tdd_final_merge_ns = 0;
    sess->tdd_last_active_workers = 0;
    sess->tdd_max_active_workers = 0;
    sess->tdd_recursive_strata = 0;
    sess->tdd_executed_strata = 0;
    sess->tdd_fallback_strata = 0;
    memset(sess->tdd_fallback_reason_counts, 0,
        sizeof(sess->tdd_fallback_reason_counts));
    sess->tdd_last_fallback_reason = WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE;
    sess->tdd_decision_tracking_active = false;
}

static int
col_session_emit_snapshot(const wl_plan_t *plan, wl_col_session_t *sess,
    wirelog_on_tuple_fn callback, void *user_data)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const char *rname = sp->relations[ri].name;
            col_rel_t *r = session_find_rel(sess, rname);
            if (!r || r->nrows == 0)
                continue;
            for (uint32_t row = 0; row < r->nrows; row++) {
                callback(rname, col_rel_row(r, row), r->ncols,
                    user_data);
            }
        }
    }
    return 0;
}

static void
col_session_clear_idb_rows(const wl_plan_t *plan, wl_col_session_t *sess)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            if (!r)
                continue;
            r->nrows = 0;
            r->sorted_nrows = 0;
            r->run_count = 0;
            r->base_nrows = 0;
            col_session_invalidate_arrangements(&sess->base, r->name);
        }
    }
    col_mat_cache_clear(&sess->mat_cache);
}

static void
wl_columnar_session_profile_begin(const wl_plan_t *plan, uint64_t affected_mask,
    uint32_t workers, bool stable)
{
    uint32_t expected = 0;
    if (!stable) {
        for (uint32_t si = 0; si < plan->stratum_count; si++) {
            if (col_affected_mask_contains(affected_mask, si))
                expected++;
        }
    }
    fprintf(stderr,
        "TDD snapshot begin plan_count=%u expected_count=%u scope=%s "
        "affected_mask=%016" PRIx64 " requested_workers=%u\n",
        plan->stratum_count, expected,
        stable ? "stable" : affected_mask == UINT64_MAX ? "full" : "affected",
        affected_mask, workers);
}

/*
 * col_session_snapshot: Evaluate all strata and emit current IDB tuples
 *
 * Implements wl_compute_backend_t.session_snapshot vtable slot.
 *
 * Evaluation order:
 *   1. If the session is already clean and stable, read cached IDB rows.
 *   2. Otherwise execute all strata in plan order.
 *   3. For each IDB relation in each stratum, invoke callback once per row
 *
 * Complexity: O(S * R * N) where S=strata, R=relations per stratum, N=rows
 *
 * Issue #811: clean snapshots read stable R without recomputing.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Invoked once per output tuple (relation, row, ncols)
 * @param user_data: Opaque pointer passed through to callback
 * @return 0 on success, EINVAL if session/callback NULL, non-zero on eval error
 */
static int
col_session_snapshot(wl_session_t *session, wirelog_on_tuple_fn callback,
    void *user_data)
{
    if (!session || !callback)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;
    sess->extension_expr_status = 0;
    const char *tdd_profile = getenv("WIRELOG_TDD_STRATUM_PROFILE");
    bool tdd_profile_active = tdd_profile && tdd_profile[0] != '\0'
        && tdd_profile[0] != '0';
    uint32_t tdd_profile_evaluated = 0;

    /* K-fusion's pre-seeded delta expansion is designed for step-style
     * outbound evaluation.  On the snapshot route it can suppress the EDB
     * base segment before the newly inserted delta reaches a fused recursive
     * relation (#1030).  A delta callback makes inserts incremental, but a
     * snapshot still promises the complete current model; fall back to the
     * already-correct full evaluation path for that combination. */
    if (sess->delta_cb != NULL && sess->last_inserted_relation != NULL
        && sess->has_evaluated)
        sess->pending_full_input_eval = true;

    /* Reset profiling counters for this evaluation pass */
    col_session_reset_snapshot_profile(sess);

    if (sess->snapshot_stable_valid
        && !sess->pending_input_change
        && !sess->pending_full_input_eval
        && sess->last_inserted_relation == NULL
        && sess->last_removed_relation == NULL
        && !sess->delta_seeded
        && !sess->retraction_seeded) {
        if (tdd_profile_active)
            wl_columnar_session_profile_begin(plan, 0, sess->num_workers, true);
        int rc = col_session_emit_snapshot(plan, sess, callback, user_data);
        if (tdd_profile_active && rc == 0)
            fprintf(stderr, "TDD snapshot complete evaluated_count=0 rc=0\n");
        return rc;
    }

    if (sess->has_evaluated
        && (sess->pending_full_input_eval
        || (sess->pending_input_change
        && sess->last_inserted_relation == NULL))) {
        col_session_clear_idb_rows(plan, sess);
    }

    /* Phase 4 incremental skip: when last_inserted_relation is set, only
     * re-evaluate strata that transitively depend on the inserted relation.
     * On the first snapshot (has_evaluated == false), always evaluate all strata
     * to establish the baseline. */
    uint64_t affected_mask = UINT64_MAX;
    if (!sess->pending_full_input_eval && sess->last_inserted_relation != NULL
        && sess->has_evaluated) {
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
                    delta, col_rel_row(r, r->base_nrows + row));
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
            if (col_affected_mask_contains(affected_mask, si)) {
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
                    /* Issue #317: Reset per-worker progress for this stratum
                     * so stale reports from the previous epoch do not block
                     * convergence detection in col_eval_stratum_multiworker. */
                    wl_frontier_progress_reset_stratum(&sess->progress, si,
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
            if (!col_affected_mask_contains(affected_rules, ri))
                continue;
            uint32_t si = rule_index_to_stratum_index(plan, ri);
            if (si == UINT32_MAX)
                continue;
            if (!col_affected_mask_contains(affected_mask, si))
                continue;
            if (stratum_has_preseeded_delta(&plan->strata[si], sess)) {
                sess->frontier_ops->reset_rule_frontier(sess, ri,
                    sess->outer_epoch);
            }
            /* Else: rule's stratum affected but no pre-seeded delta → KEEP frontier */
        }
    } else {
        /* Full re-evaluation (non-incremental call): reset all stratum and
         * rule frontiers to (current_epoch, UINT32_MAX) so no stale frontier
         * can skip required iterations after clearing IDB state. */
        for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA;
            si++) {
            sess->frontier_ops->reset_stratum_frontier(sess, si,
                sess->outer_epoch);
        }
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            sess->frontier_ops->reset_rule_frontier(sess, ri,
                sess->outer_epoch);
        }
    }

    /* Issue #361: Use TDD parallel evaluation in snapshot when workers are
     * available and facts have been loaded (initial non-incremental eval).
     * col_eval_stratum_tdd falls back to single-threaded when W<=1.
     * Issue #413: Enable TDD for initial snapshot (has_evaluated == false)
     * OR incremental evaluation (last_inserted_relation != NULL). */
    bool snapshot_tdd_eligible = (affected_mask == UINT64_MAX
        && sess->num_workers > 1
        && ((!sess->pending_full_input_eval
        && sess->last_inserted_relation != NULL)
        || !sess->has_evaluated));
    sess->tdd_decision_tracking_active = true;
    if (tdd_profile_active)
        wl_columnar_session_profile_begin(plan, affected_mask,
            sess->num_workers, false);
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if (!col_affected_mask_contains(affected_mask, si))
            continue;
        wl_columnar_session_tdd_decision_t tdd_decision =
            wl_columnar_session_tdd_plan_stratum(&plan->strata[si], sess,
                snapshot_tdd_eligible);
        bool use_tdd = tdd_decision.use_tdd;
        if (plan->strata[si].is_recursive) {
            sess->tdd_recursive_strata++;
            if (use_tdd) {
                sess->tdd_executed_strata++;
            } else {
                wl_columnar_internal_tdd_fallback_reason_t reason =
                    tdd_decision.fallback_reason;
                sess->tdd_fallback_strata++;
                sess->tdd_last_fallback_reason = reason;
                if (reason > WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE
                    && reason <
                    WL_COLUMNAR_INTERNAL_TDD_FALLBACK_REASON_COUNT) {
                    sess->tdd_fallback_reason_counts[reason]++;
                }
            }
        }
        /* Issue #390: Correctness oracle — compare TDD vs single-threaded
         * tuple counts for recursive strata.  Enabled via env var.
         * Debug-only; never runs in production.
         *
         * Save pre-stratum IDB state before TDD so the single-threaded
         * re-evaluation starts from the same base facts. */
        static int tdd_cc = -1;
        if (tdd_cc < 0) {
            const char *env = getenv("WIRELOG_TDD_CORRECTNESS_CHECK");
            tdd_cc = (env && env[0] == '1') ? 1 : 0;
        }
        bool tdd_cc_active = tdd_cc
            && use_tdd && plan->strata[si].is_recursive;
        const wl_plan_stratum_t *csp = &plan->strata[si];
        uint32_t cnrels = csp->relation_count;
        col_rel_t **pre_saved = NULL;
        if (tdd_cc_active) {
            pre_saved
                = (col_rel_t **)calloc(cnrels, sizeof(col_rel_t *));
            if (pre_saved) {
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    if (r && r->nrows > 0 && r->ncols > 0) {
                        pre_saved[ri]
                            = col_rel_new_auto(r->name, r->ncols);
                        if (pre_saved[ri])
                            col_rel_append_all(
                                pre_saved[ri], r, NULL);
                    }
                }
            }
        }

        uint64_t stratum_t0 = tdd_profile_active ? now_ns() : 0;
        uint64_t kfusion_base = sess->kfusion_ns;
        uint64_t exchange_base = sess->exchange_time_ns;
        uint64_t tdd_base = sess->tdd_total_ns;
        uint64_t wait_base = sess->tdd_wait_barrier_ns;
        uint64_t merge_base = sess->tdd_final_merge_ns;
        memset(&sess->tdd_audit, 0, sizeof(sess->tdd_audit));
        sess->tdd_audit.enabled = tdd_profile_active;

        int rc = use_tdd
            ? col_eval_stratum_tdd(&plan->strata[si], sess, si)
            : col_eval_stratum(&plan->strata[si], sess, si);
        sess->tdd_audit.enabled = false;

        if (tdd_profile_active) {
            tdd_profile_evaluated++;
            const char *first_rel = plan->strata[si].relation_count > 0
                ? plan->strata[si].relations[0].name : "(none)";
            fprintf(stderr,
                "TDD stratum idx=%u rel=%s recursive=%d use_tdd=%d "
                "fallback=%s "
                "total_ms=%.3f kfusion_ms=%.3f exchange_ms=%.3f "
                "tdd_ms=%.3f requested_workers=%u strategy=%s "
                "selected_workers=%u submitted_tasks=%" PRIu64 " "
                "completed_rounds=%" PRIu64 " replay=%s replay_rc=%d rc=%d "
                "worker_min_ms=%.3f worker_max_ms=%.3f worker_sum_ms=%.3f "
                "worker_delta_rows=%" PRIu64 " wait_ms=%.3f merge_ms=%.3f\n",
                si, first_rel ? first_rel : "(null)",
                (int)plan->strata[si].is_recursive, (int)use_tdd,
                wl_columnar_session_tdd_fallback_reason_name(
                    tdd_decision.fallback_reason),
                (double)(now_ns() - stratum_t0) / 1e6,
                (double)(sess->kfusion_ns - kfusion_base) / 1e6,
                (double)(sess->exchange_time_ns - exchange_base) / 1e6,
                (double)(sess->tdd_total_ns - tdd_base) / 1e6,
                sess->num_workers,
                sess->tdd_audit.strategy ? sess->tdd_audit.strategy : "none",
                sess->tdd_audit.selected_workers,
                sess->tdd_audit.submitted_tasks,
                sess->tdd_audit.completed_rounds,
                sess->tdd_audit.replay ? sess->tdd_audit.replay : "none",
                sess->tdd_audit.replay_rc, rc,
                (double)sess->tdd_audit.worker_min_ns / 1e6,
                (double)sess->tdd_audit.worker_max_ns / 1e6,
                (double)sess->tdd_audit.worker_sum_ns / 1e6,
                sess->tdd_audit.worker_delta_rows,
                (double)(sess->tdd_wait_barrier_ns - wait_base) / 1e6,
                (double)(sess->tdd_final_merge_ns - merge_base) / 1e6);
        }

        if (tdd_cc_active && rc == 0 && pre_saved) {
            uint32_t *tdd_counts
                = (uint32_t *)calloc(cnrels, sizeof(uint32_t));
            col_rel_t **tdd_saved
                = (col_rel_t **)calloc(cnrels, sizeof(col_rel_t *));
            if (tdd_counts && tdd_saved) {
                /* Save TDD results */
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    tdd_counts[ri] = r ? r->nrows : 0;
                    if (r && r->nrows > 0 && r->ncols > 0) {
                        tdd_saved[ri]
                            = col_rel_new_auto(r->name, r->ncols);
                        if (tdd_saved[ri])
                            col_rel_append_all(
                                tdd_saved[ri], r, NULL);
                    }
                }

                /* Restore pre-stratum IDB (base facts preserved) */
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    if (r)
                        r->nrows = 0;
                    if (pre_saved[ri] && r) {
                        if (r->ncols == 0
                            && pre_saved[ri]->ncols > 0)
                            col_rel_set_schema(r,
                                pre_saved[ri]->ncols,
                                (const char *const *)
                                pre_saved[ri]->col_names);
                        col_rel_append_all(
                            r, pre_saved[ri], NULL);
                    }
                }

                /* Re-evaluate single-threaded */
                int st_rc = col_eval_stratum(csp, sess, si);

                if (st_rc == 0) {
                    for (uint32_t ri = 0; ri < cnrels; ri++) {
                        col_rel_t *r = session_find_rel(
                            sess, csp->relations[ri].name);
                        uint32_t st = r ? r->nrows : 0;
                        if (st != tdd_counts[ri])
                            fprintf(stderr,
                                "[TDD-CHECK] stratum %u "
                                "relation '%s': TDD=%u "
                                "single=%u\n",
                                si, csp->relations[ri].name,
                                tdd_counts[ri], st);
                    }
                }

                /* Restore TDD results */
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    if (r)
                        r->nrows = 0;
                    if (tdd_saved[ri] && r) {
                        if (r->ncols == 0
                            && tdd_saved[ri]->ncols > 0)
                            col_rel_set_schema(r,
                                tdd_saved[ri]->ncols,
                                (const char *const *)
                                tdd_saved[ri]->col_names);
                        col_rel_append_all(
                            r, tdd_saved[ri], NULL);
                    }
                    col_rel_destroy(tdd_saved[ri]);
                }
            }
            free(tdd_counts);
            free((void *)tdd_saved);
        }
        if (pre_saved) {
            for (uint32_t ri = 0; ri < cnrels; ri++)
                col_rel_destroy(pre_saved[ri]);
            free((void *)pre_saved);
        }

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
            sess->tdd_decision_tracking_active = false;
            return rc;
        }
        if (sess->eval_arena)
            wl_arena_reset(sess->eval_arena);
    }
    sess->tdd_decision_tracking_active = false;

    /* Reset after successful eval so next plain snapshot runs all strata */
    sess->last_inserted_relation = NULL;
    sess->delta_seeded = false;
    sess->pending_input_change = false;
    sess->pending_full_input_eval = false;
    sess->has_evaluated = true;
    sess->snapshot_stable_valid = true;

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

    int snapshot_rc = col_session_emit_snapshot(plan, sess, callback,
            user_data);
    if (tdd_profile_active && snapshot_rc == 0)
        fprintf(stderr, "TDD snapshot complete evaluated_count=%u rc=0\n",
            tdd_profile_evaluated);
    return snapshot_rc;
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
    .session_make_compound = col_session_make_compound,
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
