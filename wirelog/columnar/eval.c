/*
 * columnar/eval.c - wirelog Columnar Backend Evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Stratum evaluation, relation plan dispatch, and delta/frontier tracking
 * extracted from backend/columnar_nanoarrow.c for modular compilation.
 */

#define _GNU_SOURCE

#include "columnar/internal.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

/* ======================================================================== */
/* Hash-Set Dedup for TDD Workers (Issue #361)                               */
/* ======================================================================== */

/*
 * O(1) dedup using an open-addressing hash set with linear probing.
 * Replaces the O(N) merge walk in col_op_consolidate_incremental_delta
 * for TDD worker IDB relations, giving true parallel scaling.
 *
 * Hash set lives in col_rel_t::dedup_slots (NULL for non-TDD relations).
 * Slot value 0 = empty; real hashes are forced non-zero (h ? h : 1).
 */

/* Hash all columns of a single row using XXH3. */
static inline uint64_t
dedup_row_hash(const col_rel_t *r, uint32_t row)
{
    int64_t buf[8];
    int64_t *p = r->ncols <= 8 ? buf
        : (int64_t *)malloc((size_t)r->ncols * sizeof(int64_t));
    if (!p)
        return 1; /* fallback: treat as unique */
    for (uint32_t c = 0; c < r->ncols; c++)
        p[c] = r->columns[c][row];
    uint64_t h = XXH3_64bits(p, (size_t)r->ncols * sizeof(int64_t));
    if (p != buf)
        free(p);
    return h ? h : 1; /* avoid 0 sentinel */
}

/* Grow the hash set to double capacity. */
static int
dedup_set_grow(col_rel_t *r)
{
    uint32_t old_cap = r->dedup_cap;
    uint32_t new_cap = old_cap ? old_cap * 2 : 1024;
    uint64_t *new_slots = (uint64_t *)calloc(new_cap, sizeof(uint64_t));
    if (!new_slots)
        return ENOMEM;
    uint32_t mask = new_cap - 1;
    /* Rehash existing entries. */
    for (uint32_t i = 0; i < old_cap; i++) {
        uint64_t h = r->dedup_slots[i];
        if (h == 0)
            continue;
        uint32_t pos = (uint32_t)(h & mask);
        while (new_slots[pos] != 0)
            pos = (pos + 1) & mask;
        new_slots[pos] = h;
    }
    free(r->dedup_slots);
    r->dedup_slots = new_slots;
    r->dedup_cap = new_cap;
    return 0;
}

/* Probabilistic dedup using 64-bit XXH3 hashes only (no full row comparison).
 * Birthday-bound collision probability: ~N^2 / 2^65.
 * For N=1M rows: P < 10^-7 (negligible).
 * Exact dedup is performed by tdd_dedup_rel after merge. */
static inline bool
dedup_set_insert(col_rel_t *r, uint64_t h)
{
    if (r->dedup_count * 10 >= r->dedup_cap * 7) { /* >70% load */
        if (dedup_set_grow(r) != 0)
            return true; /* OOM: treat as unique (safe fallback) */
    }
    uint32_t mask = r->dedup_cap - 1;
    uint32_t pos = (uint32_t)(h & mask);
    while (r->dedup_slots[pos] != 0) {
        if (r->dedup_slots[pos] == h)
            return false; /* duplicate */
        pos = (pos + 1) & mask;
    }
    r->dedup_slots[pos] = h;
    r->dedup_count++;
    return true;
}

/* Initialize dedup set from existing rows [0..nrows). */
static int
dedup_set_init_from_rel(col_rel_t *r)
{
    /* Size: next power-of-2 >= 2 * nrows, min 1024. */
    uint32_t target = r->nrows > 512 ? r->nrows * 2 : 1024;
    uint32_t cap = 1024;
    while (cap < target)
        cap *= 2;
    r->dedup_slots = (uint64_t *)calloc(cap, sizeof(uint64_t));
    if (!r->dedup_slots)
        return ENOMEM;
    r->dedup_cap = cap;
    r->dedup_count = 0;
    for (uint32_t i = 0; i < r->nrows; i++) {
        uint64_t h = dedup_row_hash(r, i);
        dedup_set_insert(r, h);
    }
    return 0;
}

/* ======================================================================== */
/* Stratum Evaluator                                                         */
/* ======================================================================== */

/*
 * col_eval_relation_plan:
 * Evaluate all operators for one relation plan using the eval stack.
 * On success, the top of stack holds the result relation (owned).
 */
int
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    for (uint32_t i = 0; i < rplan->op_count; i++) {
        const wl_plan_op_t *op = &rplan->ops[i];
        int rc = 0;

        /* Phase 3C NOTE: Weighted operation cases (WL_PLAN_OP_JOIN_WEIGHTED,
         * WL_PLAN_OP_REDUCE_WEIGHTED) are not yet present. These functions
         * exist and are tested independently (col_op_join_weighted,
         * col_op_reduce_weighted in columnar_nanoarrow.c). Integration into
         * this switch will occur when the plan generator emits weighted opcodes
         * for Z-set multiplicity evaluation. For now, col_op_join and
         * col_op_reduce dispatch to their base (non-weighted) versions. */
        /* Issue #361: Skip base-case EDB VARIABLEs in TDD workers at iter > 0.
         * A base-case VARIABLE references a static EDB (no delta) and is NOT
         * followed by JOIN (it feeds CONCAT via MAP). The base-case tuples are
         * already in the IDB from iter 0, so re-loading them every sub-pass
         * wastes O(N_base) work per sub-pass. Push empty instead. */
        if (op->op == WL_PLAN_OP_VARIABLE
            && sess->tdd_subpass_active && sess->current_iteration > 0
            && op->delta_mode == WL_DELTA_AUTO && op->relation_name) {
            /* Look ahead: base-case VARIABLE is followed by MAP (not JOIN). */
            bool next_is_join = (i + 1 < rplan->op_count
                && (rplan->ops[i + 1].op == WL_PLAN_OP_JOIN
                || rplan->ops[i + 1].op == WL_PLAN_OP_SEMIJOIN
                || rplan->ops[i + 1].op == WL_PLAN_OP_ANTIJOIN));
            if (!next_is_join) {
                char dname[256];
                snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
                col_rel_t *d = session_find_rel(sess, dname);
                if (!d || d->nrows == 0) {
                    /* Check: is this relation also used as a JOIN right_relation
                     * anywhere in this plan? If so, it's a shared EDB (e.g.
                     * 'edge' in TC) and must not be skipped. */
                    bool used_in_join = false;
                    for (uint32_t j = 0; j < rplan->op_count; j++) {
                        const wl_plan_op_t *oj = &rplan->ops[j];
                        if ((oj->op == WL_PLAN_OP_JOIN
                            || oj->op == WL_PLAN_OP_SEMIJOIN
                            || oj->op == WL_PLAN_OP_ANTIJOIN)
                            && oj->right_relation
                            && strcmp(oj->right_relation,
                            op->relation_name) == 0) {
                            used_in_join = true;
                            break;
                        }
                    }
                    if (used_in_join)
                        goto normal_eval;
                    /* Static EDB with no delta, not used in any JOIN →
                     * pure base case. Push empty and let MAP run on it.
                     * MAP on 0 rows produces the correct output schema
                     * (project_count columns) at O(1) cost, which CONCAT
                     * requires. Skipping MAP would leave the wrong ncols
                     * on the stack. */
                    col_rel_t *full = session_find_rel(sess, op->relation_name);
                    col_rel_t *empty = col_rel_pool_new_like(
                        sess->delta_pool, "$base_skip", full ? full : NULL);
                    if (!empty) {
                        rc = ENOMEM; break;
                    }
                    rc = eval_stack_push(stack, empty, true);
                    if (rc != 0) {
                        col_rel_destroy(empty); break;
                    }
                    continue;
                }
            }
        }
normal_eval:

        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            rc = col_op_variable(op, stack, sess);
            break;
        case WL_PLAN_OP_MAP:
            rc = col_op_map(op, stack, sess);
            break;
        case WL_PLAN_OP_FILTER:
            rc = col_op_filter(op, stack, sess);
            break;
        case WL_PLAN_OP_JOIN:
            rc = sess->diff_operators_active
                ? col_op_join_diff(op, stack, sess)
                : col_op_join(op, stack, sess);
            break;
        case WL_PLAN_OP_ANTIJOIN:
            rc = col_op_antijoin(op, stack, sess);
            break;
        case WL_PLAN_OP_CONCAT:
            rc = col_op_concat(stack, sess);
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            rc = sess->diff_operators_active
                ? col_op_consolidate_diff(stack, sess)
                : col_op_consolidate(stack, sess);
            break;
        case WL_PLAN_OP_REDUCE:
            rc = col_op_reduce(op, stack, sess);
            break;
        case WL_PLAN_OP_SEMIJOIN:
            rc = col_op_semijoin(op, stack, sess);
            break;
        case WL_PLAN_OP_K_FUSION: {
            uint64_t t0 = now_ns();
            rc = col_op_k_fusion(op, stack, sess);
            COL_SESSION(sess)->kfusion_ns += now_ns() - t0;
            break;
        }
        case WL_PLAN_OP_LFTJ:
            rc = col_op_lftj(op, stack, sess);
            break;
        case WL_PLAN_OP_EXCHANGE:
            rc = col_op_exchange(op, stack, sess);
            break;
        default:
            break;
        }
        if (rc != 0)
            return rc;
    }
    return 0;
}

/*
 * Helper: Format retraction delta relation name ($r$<rel>)
 * Returns 0 on success, ENOMEM if buffer too small.
 */
int
retraction_rel_name(const char *rel, char *buf, size_t sz)
{
    int n = snprintf(buf, sz, "$r$%s", rel);
    return (n < 0 || (size_t)n >= sz) ? ENOMEM : 0;
}

/*
 * has_empty_forced_delta:
 * Check if a relation plan would produce empty output because it contains
 * a FORCE_DELTA op whose delta relation is empty or absent.
 *
 * On iteration 0, no deltas exist yet; FORCE_DELTA ops fall back to the
 * full relation (base-case seeding), so we always return false.
 *
 * On iteration > 0, if any FORCE_DELTA VARIABLE or JOIN op references a
 * delta that is empty/absent, the entire plan would produce 0 rows, so
 * we can safely skip evaluation.
 *
 * Returns true if the plan can be skipped (empty forced-delta found).
 *
 * Issue #158 extension: When retraction_seeded and iteration == 0, check
 * $r$<name> relations instead of $d$<name>.
 */
bool
has_empty_forced_delta(const wl_plan_relation_t *rp, wl_col_session_t *sess,
    uint32_t iteration)
{
    if (iteration == 0 && !sess->delta_seeded && !sess->retraction_seeded)
        return false; /* Base case: no deltas exist yet (non-incremental) */

    for (uint32_t oi = 0; oi < rp->op_count; oi++) {
        const wl_plan_op_t *op = &rp->ops[oi];
        if (op->delta_mode != WL_DELTA_FORCE_DELTA)
            continue;

        const char *rel_name = NULL;
        if (op->op == WL_PLAN_OP_VARIABLE)
            rel_name = op->relation_name;
        else if (op->op == WL_PLAN_OP_JOIN || op->op == WL_PLAN_OP_SEMIJOIN)
            rel_name = op->right_relation;

        if (rel_name) {
            char dname[256];
            if (sess->retraction_seeded && iteration == 0) {
                /* Retraction mode: look for $r$<name> */
                if (retraction_rel_name(rel_name, dname, sizeof(dname)) != 0)
                    return false; /* Buffer overflow, skip check */
            } else {
                /* Normal mode: look for $d$<name> */
                snprintf(dname, sizeof(dname), "$d$%s", rel_name);
            }
            col_rel_t *d = session_find_rel(sess, dname);
            if (!d || d->nrows == 0)
                return true; /* Found empty forced-delta */
        }
    }
    return false;
}

/* Forward declaration of col_frontier_compute (defined later) */
static col_frontier_t
col_frontier_compute(const col_rel_t *rel);

/*
 * col_eval_stratum:
 * Evaluate one stratum, writing results into session relations.
 * Non-recursive strata are evaluated once.
 * Recursive strata use semi-naive fixed-point iteration.
 *
 * Returns 0 on success, non-zero on error.
 */
int
col_eval_stratum(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    if (!sp->is_recursive) {
        /* Non-recursive: evaluate each relation plan once */
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];

            eval_stack_t stack;
            eval_stack_init(&stack);

            int rc = col_eval_relation_plan(rp, &stack, sess);
            if (rc != 0) {
                eval_stack_drain(&stack);
                return rc;
            }

            if (stack.top == 0)
                continue;

            eval_entry_t result = eval_stack_pop(&stack);
            eval_stack_drain(&stack); /* drain any leftover entries */

            col_rel_t *target = session_find_rel(sess, rp->name);
            if (!target) {
                /* First time: create and register the relation */
                if (result.owned) {
                    /* Rename the result relation */
                    free(result.rel->name);
                    result.rel->name = wl_strdup(rp->name);
                    if (!result.rel->name) {
                        col_rel_destroy(result.rel);
                        return ENOMEM;
                    }
                    rc = session_add_rel(sess, result.rel);
                    if (rc != 0) {
                        col_rel_destroy(result.rel);
                        return rc;
                    }
                    result.owned = false;
                } else {
                    col_rel_t *copy = col_rel_pool_new_like(
                        sess->delta_pool, rp->name, result.rel);
                    if (!copy)
                        return ENOMEM;
                    if ((rc = col_rel_append_all(copy, result.rel,
                        sess->eval_arena)) != 0) {
                        col_rel_destroy(copy);
                        return rc;
                    }
                    rc = session_add_rel(sess, copy);
                    if (rc != 0) {
                        col_rel_destroy(copy);
                        return rc;
                    }
                }
            } else {
                /* Append new results to existing relation */
                rc = col_rel_append_all(target, result.rel, sess->eval_arena);
                if (result.owned)
                    col_rel_destroy(result.rel);
                if (rc != 0)
                    return rc;
            }
        }
        col_mat_cache_clear(&sess->mat_cache);
        delta_pool_reset(sess->delta_pool);
        wl_arena_reset(sess->eval_arena);

        /* Non-recursive stratum frontier: record convergence epoch and iteration.
        * Non-recursive strata always converge at iteration UINT32_MAX (no loop),
        * so store (outer_epoch, UINT32_MAX) to enable epoch-aware skip on next
        * incremental call. Always update both fields so same-epoch skip logic
        * fires correctly when the frontier persists across session_step calls. */
        sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
            sess->outer_epoch, UINT32_MAX);

        /* Issue #317: Report non-recursive convergence to coordinator's
         * progress tracker.  Non-recursive strata always converge at
         * UINT32_MAX (no iteration loop), matching the sentinel convention. */
        if (sess->coordinator) {
            wl_frontier_progress_record(&sess->coordinator->progress,
                sess->worker_id, stratum_idx, sess->outer_epoch, UINT32_MAX);
        }

        /* Non-recursive rule frontiers: mark each rule fully evaluated.
         * UINT32_MAX sentinel matches stratum frontier convention. */
        if (sess->plan) {
            uint32_t rule_base = 0;
            for (uint32_t si = 0; si < stratum_idx; si++)
                rule_base += sess->plan->strata[si].relation_count;
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                uint32_t rule_idx = rule_base + ri;
                if (rule_idx < MAX_RULES) {
                    /* Issue #106: Conservative approach - always reset rule frontiers
                     * for affected strata to (current_epoch, UINT32_MAX) sentinel.
                     * UINT32_MAX prevents premature skip during re-evaluation. */
                    sess->frontier_ops->reset_rule_frontier(sess, rule_idx,
                        sess->outer_epoch);
                }
            }
        }

        return 0;
    }

    /*
     * Recursive stratum: semi-naive fixed-point iteration.
     * Iterate until no new tuples are produced.
     *
     * Pre-register empty IDB relations so that VARIABLE ops can find
     * them on the first iteration (before any tuples are produced).
     */
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        col_rel_t *existing = session_find_rel(sess, sp->relations[ri].name);
        if (!existing) {
            col_rel_t *empty = NULL;
            int rc = col_rel_alloc(&empty, sp->relations[ri].name);
            if (rc != 0)
                return ENOMEM;
            rc = session_add_rel(sess, empty);
            if (rc != 0) {
                col_rel_destroy(empty);
                return rc;
            }
        }
    }

    /*
     * Semi-naive fixed-point iteration with delta tracking.
     * VARIABLE ops prefer "$d$relname" delta relations (rows added in the
     * previous iteration). JOIN right-side lookups always use the full
     * relation by name, giving delta (left) x full (right) join semantics.
     */
    uint32_t nrels = sp->relation_count;
    col_rel_t **delta_rels = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
    if (!delta_rels)
        return ENOMEM;

    /* Sort pre-existing data in each IDB relation before iterating.
     * Handles the EDB+IDB case: when base facts are pre-loaded into a
     * relation that also appears as an IDB in a recursive rule, the loaded
     * facts may be in insertion order (unsorted).
     * col_op_consolidate_incremental_delta requires rel->data[0..snap) to be
     * sorted; an unsorted prefix causes the 2-pointer merge to miss duplicates,
     * producing spurious output rows. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (r && r->nrows > 1) {
            col_rel_radix_sort_int64(r);
        }
    }

    /* Initialize recursive stratum frontier to UINT32_MAX (not set sentinel)
     * only on the first evaluation (frontier==0 from calloc). If a prior
     * col_session_insert_incremental call preserved a real convergence frontier,
     * keep it so the per-iteration skip condition fires for iterations beyond
     * the prior convergence point. */
    sess->frontier_ops->init_stratum(sess, stratum_idx);

    /* Phase 4 (US-4-004): Compute the base global rule index for this stratum.
     * Rule indices are assigned by enumerating strata in order and relations
     * within each stratum, matching col_compute_affected_rules convention. */
    uint32_t rule_id_base = 0;
    if (sess->plan) {
        for (uint32_t si = 0;
            si < stratum_idx && si < sess->plan->stratum_count; si++) {
            rule_id_base += sess->plan->strata[si].relation_count;
        }
    }
    /* Clamp so rule_id_base + ri never exceeds MAX_RULES - 1 */
    if (rule_id_base >= MAX_RULES)
        rule_id_base = MAX_RULES;

    uint32_t iter;
    col_frontier_t strat_frontier = { 0, 0 };
    uint32_t final_eff_iter = 0; /* effective sub-pass index at convergence */

    /* Issue #282: Save the session-level diff_operators_active so we can
     * restore it after the recursive stratum evaluation completes.
     * Within the iteration loop, we override it per sub-pass. */
    bool saved_diff_operators_active = sess->diff_operators_active;

    /*
     * Stride-based semi-naive iteration (Issue #237).
     * Each outer iteration runs EVAL_STRIDE sub-passes, chaining the delta
     * from each sub-pass into the next.  Effective recursion depth covered:
     *   MAX_ITERATIONS * EVAL_STRIDE = 4096 * 8 = 32768
     * This allows deep-chain workloads (e.g. CRDT) to converge where they
     * previously hit the MAX_ITERATIONS cap and produced incomplete results.
     *
     * When EVAL_STRIDE == 1 the behaviour is identical to the prior single-
     * pass loop.
     */
    for (iter = 0; iter < MAX_ITERATIONS; iter++) {
        bool outer_any_new = false; /* any sub-pass produced new tuples  */
        int outer_rc = 0;           /* error propagated from inner loop  */
        bool stride_all_skipped
            = true; /* true until a sub-pass actually runs */
        bool outer_continue_next
            = false;            /* net-zero/all-empty: retry next outer */
        bool converged = false; /* fixed point reached                */

        /* Allocate snap once per outer stride; re-filled at the start of each
         * sub-pass since nrows grows as new tuples are appended. */
        uint32_t *snap = (uint32_t *)malloc(nrels * sizeof(uint32_t));
        if (!snap) {
            free(delta_rels);
            return ENOMEM;
        }

        /* ------------------------------------------------------------------ */
        /* Inner sub-pass loop: EVAL_STRIDE semi-naive passes per outer iter.  */
        /* ------------------------------------------------------------------ */
        for (uint32_t sub = 0; sub < EVAL_STRIDE; sub++) {
            uint32_t eff_iter = iter * EVAL_STRIDE + sub;

            /* Publish effective iteration so operators can distinguish base case
             * (eff_iter 0: FORCE_DELTA falls back to full) from delta case
             * (eff_iter > 0: FORCE_DELTA with absent delta → empty result). */
            sess->current_iteration = eff_iter;

            /* Issue #282: Enable differential operators for recursive iterations
             * > 0.  At eff_iter 0, we seed from full EDB relations (base case)
             * so arrangement reuse is not applicable.  From eff_iter 1 onward,
             * only the delta is processed (semi-naive), so col_op_join_diff can
             * build the hash table incrementally: O(D) instead of O(N).
             * diff_arr_entries persist across sub-passes and are invalidated by
             * col_session_invalidate_arrangements when the underlying relation
             * changes (called after consolidation each sub-pass).
             * The saved value is restored after the recursive stratum completes
             * so non-recursive strata and the outer session logic are unaffected. */
            sess->diff_operators_active
                = sess->diff_enabled && eff_iter > 0;

            /* Iteration skip based on frontier (convergence point of prior
             * evaluation).  Skip sub-pass when both hold:
             * 1. Same outer_epoch — frontier is valid for this insertion epoch
             * 2. eff_iter > frontier.iteration — already processed in this epoch
             * Once eff_iter exceeds the frontier for sub=0, it also exceeds for
             * all higher sub values, so the entire outer iteration effectively
             * continues to the next one. */
            if (sess->frontier_ops->should_skip_iteration(sess, stratum_idx,
                eff_iter)) {
                continue; /* skip this sub-pass */
            }

            stride_all_skipped = false; /* at least one sub-pass runs */

            /* Clear per-sub-pass delta arrangement cache (sequential eval path).
             * K-fusion workers manage their own darr caches independently. */
            col_session_free_delta_arrangements(sess);

            /* Register delta relations from previous sub-pass into session */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                if (!delta_rels[ri])
                    continue;
                char dname[256];
                snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
                session_remove_rel(sess, dname);
                int rc = session_add_rel(sess, delta_rels[ri]);
                if (rc != 0)
                    col_rel_destroy(delta_rels[ri]);
                delta_rels[ri] = NULL; /* session now owns it */
            }

            /* Record per-relation row counts (O(1) snapshot — no data copy).
             * Re-filled each sub-pass because nrows changes after each pass. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
                snap[ri] = r ? r->nrows : 0; /* O(1) count only, no copy */
            }

            /* Phase 3D: Frontier skip with multiplicities (US-3D-002).
             * Skip sub-pass if all delta relations have zero net multiplicity.
             * When detected, retry with the next outer iteration (same as the
             * prior single-pass behaviour). */
            if (eff_iter
                > 0) { /* only skip from effective iteration 1 onward */
                bool all_deltas_net_zero = true;
                for (uint32_t ri = 0; ri < nrels; ri++) {
                    char dname[256];
                    snprintf(dname, sizeof(dname), "$d$%s",
                        sp->relations[ri].name);
                    col_rel_t *delta = session_find_rel(sess, dname);
                    if (!delta || delta->nrows == 0) {
                        /* Empty delta: net multiplicity is zero */
                        continue;
                    }
                    /* Compute net multiplicity for this delta relation */
                    int64_t net_mult = 0;
                    for (uint32_t row = 0; row < delta->nrows; row++) {
                        net_mult += delta->timestamps[row].multiplicity;
                    }
                    if (net_mult != 0) {
                        all_deltas_net_zero = false;
                        break;
                    }
                }
                if (all_deltas_net_zero) {
                    /* All deltas zero net-multiplicity: skip to next outer iter */
                    outer_continue_next = true;
                    break;
                }
            }

            /* Stratum-level early exit: if all rules have empty forced deltas,
             * the sub-pass will produce no new facts. (Issue #81) */
            if (eff_iter > 0) {
                bool all_rules_empty = true;
                for (uint32_t ri = 0; ri < nrels; ri++) {
                    if (!has_empty_forced_delta(&sp->relations[ri], sess,
                        eff_iter)) {
                        all_rules_empty = false;
                        break;
                    }
                }
                if (all_rules_empty) {
                    outer_continue_next = true;
                    break;
                }
            }

            /* Single-pass semi-naive evaluation. VARIABLE prefers delta when it
             * is a strict subset of full (genuine new facts). JOIN propagates
             * the is_delta flag through results and applies right-delta when
             * left is full and a strictly-smaller right delta exists. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                const wl_plan_relation_t *rp = &sp->relations[ri];

                /* Issue #106 (US-106-003): Rule-level frontier skip with epoch
                 * gating.  Skip rule evaluation only when BOTH hold:
                 * 1. Same outer_epoch (prevents cross-epoch incorrect skips)
                 * 2. eff_iter > convergence point (already processed in epoch)
                 * Across epoch boundaries mismatch => always re-eval. */
                uint32_t rule_id = rule_id_base + ri;
                if (sess->frontier_ops->should_skip_rule(sess, rule_id,
                    eff_iter)) {
                    continue;
                }

                /* Pre-scan skip: if a FORCE_DELTA op references an empty or
                 * absent delta (eff_iter > 0), the plan produces 0 rows. */
                if (has_empty_forced_delta(rp, sess, eff_iter)) {
                    continue;
                }

                eval_stack_t stack;
                eval_stack_init(&stack);

                int rc = col_eval_relation_plan(rp, &stack, sess);
                if (rc != 0) {
                    eval_stack_drain(&stack);
                    outer_rc = rc;
                    goto stride_error;
                }

                if (stack.top == 0)
                    continue;

                eval_entry_t result = eval_stack_pop(&stack);
                eval_stack_drain(&stack);

                /* Post-eval skip: evaluation produced 0 rows — safety net for
                 * cases not caught by pre-scan (e.g. filters eliminating all
                 * rows). */
                if (result.rel && result.rel->nrows == 0) {
                    if (result.owned)
                        col_rel_destroy(result.rel);
                    continue;
                }

                col_rel_t *target = session_find_rel(sess, rp->name);
                if (!target) {
                    col_rel_t *copy;
                    if (result.owned) {
                        copy = result.rel;
                        free(copy->name);
                        copy->name = wl_strdup(rp->name);
                        if (!copy->name) {
                            col_rel_destroy(copy);
                            outer_rc = ENOMEM;
                            goto stride_error;
                        }
                        result.owned = false;
                    } else {
                        copy = col_rel_pool_new_like(
                            sess->delta_pool, rp->name, result.rel);
                        if (!copy) {
                            outer_rc = ENOMEM;
                            goto stride_error;
                        }
                        if ((rc = col_rel_append_all(copy, result.rel,
                            sess->eval_arena)) != 0) {
                            col_rel_destroy(copy);
                            outer_rc = rc;
                            goto stride_error;
                        }
                    }
                    rc = session_add_rel(sess, copy);
                    if (rc != 0) {
                        col_rel_destroy(copy);
                        outer_rc = rc;
                        goto stride_error;
                    }
                } else {
                    /* Adopt schema from result if target is still uninitialised */
                    if (target->ncols == 0 && result.rel->ncols > 0) {
                        rc = col_rel_set_schema(
                            target, result.rel->ncols,
                            (const char *const *)result.rel->col_names);
                        if (rc != 0) {
                            if (result.owned)
                                col_rel_destroy(result.rel);
                            outer_rc = rc;
                            goto stride_error;
                        }
                    }
                    rc = col_rel_append_all(target, result.rel,
                            sess->eval_arena);
                    if (result.owned)
                        col_rel_destroy(result.rel);
                    if (rc != 0) {
                        outer_rc = rc;
                        goto stride_error;
                    }
                }
            }

            /* Remove delta relations from session (evaluation is complete) */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                char dname[256];
                snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
                session_remove_rel(sess, dname);
            }

            /* Phase 4: Frontier is computed incrementally as deltas are created
             * because delta_rels[ri] may be set to NULL in the next sub-pass's
             * registration loop.  strat_frontier declared before outer loop. */

            /* Consolidate all IDB relations to remove duplicates and compute
             * delta as a byproduct.  snap[ri] marks the boundary between the
             * already-sorted prefix and unsorted new rows appended this pass. */
            bool any_new = false;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
                if (!r || snap[ri] >= r->nrows) {
                    continue; /* no new rows for this relation */
                }

                char dname[256];
                snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
                col_rel_t *delta = col_rel_pool_new_like(
                    sess->delta_pool, dname, r);
                if (!delta) {
                    outer_rc = ENOMEM;
                    goto stride_error;
                }

                /* Consolidate WITH delta output (no separate merge walk) */
                uint32_t cons_old = snap[ri];
                uint32_t cons_new = r->nrows - cons_old; /* delta count D */
                int fast_flag = 0;
                uint64_t cons_t0 = now_ns();
                int rc2 = col_op_consolidate_incremental_delta(r, snap[ri],
                        delta, &fast_flag);
                uint64_t cons_elapsed = now_ns() - cons_t0;
                sess->consolidation_ns += cons_elapsed;
                sess->consolidate_fast_hits += (uint64_t)fast_flag;
                sess->consolidate_slow_hits += (uint64_t)(1 - fast_flag);
                /* Invalidate arrangements for this relation (data changed). */
                col_session_invalidate_arrangements(&sess->base,
                    sp->relations[ri].name);
                /* Per-call trace: WL_CONSOLIDATION_LOG=1 prints per-call info */
                if (sess->consolidation_log) {
                    fprintf(stderr,
                        "CONS eff_iter=%u stratum=%u rel=%s N=%u D=%u "
                        "time_us=%.1f ratio=%.4f\n",
                        eff_iter, stratum_idx, sp->relations[ri].name,
                        cons_old, cons_new, (double)cons_elapsed / 1000.0,
                        cons_old > 0 ? (double)cons_new / (double)cons_old
                                         : 0.0);
                }
                if (rc2 != 0) {
                    col_rel_destroy(delta);
                    outer_rc = rc2;
                    goto stride_error;
                }

                if (delta->nrows > 0) {
                    /* Stamp each new row with its provenance (eff_iter, stratum).
                     * worker=0 indicates the sequential (non-K-fusion) path. */
                    delta->timestamps = (col_delta_timestamp_t *)calloc(
                        delta->nrows, sizeof(col_delta_timestamp_t));
                    if (!delta->timestamps) {
                        col_rel_destroy(delta);
                        outer_rc = ENOMEM;
                        goto stride_error;
                    }
                    for (uint32_t ti = 0; ti < delta->nrows; ti++) {
                        delta->timestamps[ti].iteration = eff_iter;
                        delta->timestamps[ti].stratum = stratum_idx;
                        /* worker left zero: sequential evaluation path */
                        delta->timestamps[ti].multiplicity = 1;
                    }

                    /* Phase 4: Enable timestamp tracking on target relation to
                     * preserve provenance through consolidation.  This enables
                     * frontier computation to determine convergence. */
                    if (!r->timestamps && r->capacity > 0) {
                        r->timestamps = (col_delta_timestamp_t *)calloc(
                            r->capacity, sizeof(col_delta_timestamp_t));
                        if (!r->timestamps) {
                            free(delta->timestamps);
                            col_rel_destroy(delta);
                            outer_rc = ENOMEM;
                            goto stride_error;
                        }
                    }

                    delta_rels[ri] = delta;
                    any_new = true;

                    /* Phase 4: Compute frontier from this delta immediately.
                     * Must happen before next sub-pass registration sets
                     * delta_rels[ri]=NULL.
                     * frontier = MAX eff_iter that produced facts. */
                    col_frontier_t rel_frontier = col_frontier_compute(delta);
                    if (rel_frontier.iteration > strat_frontier.iteration
                        || (rel_frontier.iteration == strat_frontier.iteration
                        && rel_frontier.stratum > strat_frontier.stratum)) {
                        strat_frontier = rel_frontier;
                    }
                } else {
                    col_rel_destroy(delta);
                }
            }

            delta_pool_reset(sess->delta_pool);
            wl_arena_reset(sess->eval_arena);
            /* Issue #176: Per-sub-pass cache eviction for recursive strata.
             * Use configurable eviction threshold (cache_evict_threshold):
             * - If 0: clear entire cache each sub-pass (backward compatible)
             * - If > 0: evict LRU entries when cache exceeds threshold */
            if (sess->cache_evict_threshold == 0) {
                col_mat_cache_clear(&sess->mat_cache);
            } else {
                col_mat_cache_evict_until(&sess->mat_cache,
                    sess->cache_evict_threshold);
            }

            if (!any_new) {
                /* Fixed point reached: no new tuples in this sub-pass. */
                converged = true;
                break;
            }
            outer_any_new = true;
            final_eff_iter = eff_iter;
            continue; /* next sub-pass */

stride_error:
            break; /* exit inner loop; outer_rc carries the error */
        } /* end inner sub-pass loop */

        free(snap);

        if (outer_rc != 0) {
            /* Issue #282: Restore diff_operators_active on error path */
            sess->diff_operators_active = saved_diff_operators_active;
            free(delta_rels);
            return outer_rc;
        }

        /* Dispatch on why the inner loop exited */
        if (stride_all_skipped)
            continue; /* all sub-passes were frontier-skipped: next outer iter */
        if (outer_continue_next)
            continue; /* net-zero or all-empty delta: retry next outer iter */
        if (converged || !outer_any_new)
            break; /* true convergence */
    } /* end outer stride loop */
    /* Issue #282: Restore session-level diff_operators_active.
     * The per-iteration override above applies only within the recursive
     * fixed-point loop.  Restoring the saved value ensures subsequent
     * non-recursive strata and outer session logic see the correct state. */
    sess->diff_operators_active = saved_diff_operators_active;

    sess->total_iterations = final_eff_iter;

    /* Issue #106 (US-106-005): Record per-rule frontier at convergence with epoch.
     * When stratum converges at effective iteration I, record (outer_epoch, I).
     * On next incremental snapshot, skip fires when eff_iter > I. */
    for (uint32_t ri = 0; ri < nrels && rule_id_base + ri < MAX_RULES; ri++) {
        uint32_t rule_id = rule_id_base + ri;
        sess->frontier_ops->record_rule_convergence(sess, rule_id,
            sess->outer_epoch, final_eff_iter);
    }

    /* Phase 4: Update per-stratum frontier after recursive stratum evaluation.
     * strat_frontier was computed incrementally via delta timestamps (eff_iter),
     * so it already holds the effective iteration index at convergence. */
    if (strat_frontier.iteration != UINT32_MAX) {
        sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
            sess->outer_epoch, strat_frontier.iteration);

        /* Issue #317: Report recursive convergence to coordinator's progress
         * tracker so col_eval_stratum_multiworker can compute the global
         * minimum frontier after the workqueue barrier. */
        if (sess->coordinator) {
            wl_frontier_progress_record(&sess->coordinator->progress,
                sess->worker_id, stratum_idx, sess->outer_epoch,
                strat_frontier.iteration);
        }
    }

    /* Cleanup all delta relations after frontier has been computed */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (delta_rels[ri])
            col_rel_destroy(delta_rels[ri]);
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        session_remove_rel(sess, dname);
    }
    free(delta_rels);
    delta_pool_reset(sess->delta_pool);
    wl_arena_reset(sess->eval_arena);
    col_mat_cache_clear(&sess->mat_cache);

    return 0;
}
static col_frontier_t
col_frontier_compute(const col_rel_t *rel)
{
    col_frontier_t f = { 0, 0 };

    /* Handle NULL or empty relation */
    if (!rel || rel->nrows == 0 || !rel->timestamps)
        return f;

    /* Initialize frontier to first row's timestamp */
    f.iteration = rel->timestamps[0].iteration;
    f.stratum = rel->timestamps[0].stratum;

    /* Find minimum (iteration, stratum) */
    for (uint32_t i = 1; i < rel->nrows; i++) {
        const col_delta_timestamp_t *ts = &rel->timestamps[i];
        if (ts->iteration < f.iteration
            || (ts->iteration == f.iteration && ts->stratum < f.stratum)) {
            f.iteration = ts->iteration;
            f.stratum = ts->stratum;
        }
    }

    return f;
}
static bool
col_row_in_sorted(const int64_t *sorted_data, uint32_t nrows, uint32_t ncols,
    const int64_t *row)
{
    if (!sorted_data || nrows == 0 || ncols == 0)
        return false;
    uint32_t lo = 0, hi = nrows;
    size_t row_bytes = sizeof(int64_t) * ncols;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        int cmp = memcmp(sorted_data + (size_t)mid * ncols, row, row_bytes);
        if (cmp == 0)
            return true;
        if (cmp < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return false;
}

/*
 * col_idb_consolidate: Sort + dedup one IDB relation in-place.
 *
 * Reuses the eval stack + col_op_consolidate operator so sort order
 * is consistent with the rest of the evaluation pipeline.
 */
static int
col_idb_consolidate(col_rel_t *r, wl_col_session_t *sess)
{
    eval_stack_t stk;
    eval_stack_init(&stk);
    int rc = eval_stack_push(&stk, r, false); /* borrowed */
    if (rc != 0)
        return rc;
    col_op_consolidate(&stk, sess);
    if (stk.top > 0) {
        eval_entry_t ce = eval_stack_pop(&stk);
        if (ce.owned && ce.rel != r) {
            col_columns_free(r->columns, r->ncols);
            r->columns = ce.rel->columns;
            r->nrows = ce.rel->nrows;
            r->capacity = ce.rel->capacity;
            ce.rel->columns = NULL;
            col_rel_destroy(ce.rel);
        }
    }
    return 0;
}

/*
 * col_stratum_step_with_delta: Evaluate one stratum and fire delta callbacks.
 *
 * Phase 2A algorithm (full re-eval + set diff):
 *   1. Snapshot each IDB relation's current sorted rows (prev state)
 *   2. Run col_eval_stratum (appends newly derived rows)
 *   3. Consolidate each IDB relation (sort + dedup)
 *   4. Fire delta_cb(+1) for each row in new state not found in prev state
 *   5. Free snapshots
 *
 * TODO(Phase 2B): Replace step 2 with semi-naive ΔR propagation.
 */

/* Forward declaration for col_stratum_step_with_delta (defined below) */
int
col_stratum_step_with_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
    uint32_t stratum_idx);

/*
 * col_stratum_step_retraction_nonrecursive: Retraction delta propagation
 *
 * (Issue #158) Semi-naive delta retraction for non-recursive strata.
 * Evaluates the stratum in retraction mode, using $r$<name> delta relations
 * to propagate only retractions (O(|Δ|)) instead of full re-evaluation.
 *
 * Algorithm:
 *   1. Set retraction_seeded = true
 *   2. Evaluate stratum (produces rows to retract in result buffer)
 *   3. For each IDB relation:
 *      - Find retraction candidates (rows produced by eval)
 *      - Remove those rows in-place (compact)
 *      - Fire delta_cb with diff=-1 for each removed row
 *   4. Reset retraction_seeded = false
 *
 * Falls back to full re-eval (col_stratum_step_with_delta) for recursive strata.
 */
static int
col_stratum_step_retraction_nonrecursive(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    if (sp->is_recursive) {
        /* Recursive strata fall back to full re-eval */
        return col_stratum_step_with_delta(sp, sess, stratum_idx);
    }

    uint32_t rc_cnt = sp->relation_count;

    /* retract_data[ri] will hold the stolen post-consolidation pointer;
     * no malloc/memcpy needed — ownership is transferred from r->data. */
    int64_t **retract_data = (int64_t **)calloc(rc_cnt, sizeof(int64_t *));
    uint32_t *retract_nrows = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    if (!retract_data || !retract_nrows) {
        free(retract_data);
        free(retract_nrows);
        return ENOMEM;
    }

    /* Step 0: Pointer-swap original data into retract_backup fields (O(1))
     * and clear relation for retraction evaluation. */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        r->retract_backup_columns = r->columns;
        r->retract_backup_nrows = r->nrows;
        r->retract_backup_capacity = r->capacity;
        r->retract_backup_sorted_nrows = r->sorted_nrows;
        r->retract_backup_run_count = r->run_count;
        memcpy(r->retract_backup_run_ends, r->run_ends,
            sizeof(r->run_ends));
        r->columns = NULL;
        r->capacity = 0;
        r->nrows = 0;
        r->sorted_nrows = 0;
        r->run_count = 0;
    }

    /* Step 1: Enable retraction-seeded mode and evaluate stratum */
    sess->retraction_seeded = true;
    int rc = col_eval_stratum(sp, sess, stratum_idx);
    sess->retraction_seeded = false;
    if (rc != 0) {
        /* Restore all backup pointers; free any eval-allocated buffers */
        for (uint32_t i = 0; i < rc_cnt; i++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[i].name);
            if (!r || r->ncols == 0)
                continue;
            col_columns_free(r->columns, r->ncols);
            r->columns = r->retract_backup_columns;
            r->nrows = r->retract_backup_nrows;
            r->capacity = r->retract_backup_capacity;
            r->sorted_nrows = r->retract_backup_sorted_nrows;
            r->run_count = r->retract_backup_run_count;
            memcpy(r->run_ends, r->retract_backup_run_ends,
                sizeof(r->run_ends));
            r->retract_backup_columns = NULL;
            r->retract_backup_nrows = 0;
            r->retract_backup_capacity = 0;
            r->retract_backup_sorted_nrows = 0;
            r->retract_backup_run_count = 0;
        }
        free(retract_data);
        free(retract_nrows);
        return rc;
    }

    /* Steps 2+3: Per-relation: consolidate retraction candidates, steal the
     * buffer pointer (no malloc/memcpy), then swap back the original (O(1)). */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;

        if (r->nrows > 0) {
            rc = col_idb_consolidate(r, sess);
            if (rc != 0) {
                /* Restore any relations still holding backup state */
                for (uint32_t i = 0; i < rc_cnt; i++) {
                    col_rel_t *r2
                        = session_find_rel(sess, sp->relations[i].name);
                    if (!r2 || r2->ncols == 0)
                        continue;
                    if (r2->retract_backup_columns != NULL) {
                        col_columns_free(r2->columns, r2->ncols);
                        r2->columns = r2->retract_backup_columns;
                        r2->nrows = r2->retract_backup_nrows;
                        r2->capacity = r2->retract_backup_capacity;
                        r2->sorted_nrows = r2->retract_backup_sorted_nrows;
                        r2->run_count = r2->retract_backup_run_count;
                        memcpy(r2->run_ends, r2->retract_backup_run_ends,
                            sizeof(r2->run_ends));
                        r2->retract_backup_columns = NULL;
                        r2->retract_backup_nrows = 0;
                        r2->retract_backup_capacity = 0;
                        r2->retract_backup_sorted_nrows = 0;
                        r2->retract_backup_run_count = 0;
                    }
                    free(retract_data[i]);
                }
                free(retract_data);
                free(retract_nrows);
                return rc;
            }
            /* Steal: gather into flat buffer for col_row_in_sorted */
            uint32_t nc = r->ncols;
            int64_t *flat = (int64_t *)malloc(
                (size_t)r->nrows * nc * sizeof(int64_t));
            if (flat) {
                for (uint32_t row = 0; row < r->nrows; row++)
                    col_rel_row_copy_out(r, row, flat + (size_t)row * nc);
            }
            retract_data[ri] = flat;
            retract_nrows[ri] = flat ? r->nrows : 0;
            col_columns_free(r->columns, r->ncols);
            r->columns = NULL;
            r->capacity = 0;
            r->nrows = 0;
        }

        /* Free any eval-allocated buffer not stolen above (nrows==0 case) */
        col_columns_free(r->columns, r->ncols);
        r->columns = NULL;

        /* Swap back original data (O(1)) */
        r->columns = r->retract_backup_columns;
        r->nrows = r->retract_backup_nrows;
        r->capacity = r->retract_backup_capacity;
        r->sorted_nrows = r->retract_backup_sorted_nrows;
        r->run_count = r->retract_backup_run_count;
        memcpy(r->run_ends, r->retract_backup_run_ends,
            sizeof(r->run_ends));
        r->retract_backup_columns = NULL;
        r->retract_backup_nrows = 0;
        r->retract_backup_capacity = 0;
        r->retract_backup_sorted_nrows = 0;
        r->retract_backup_run_count = 0;
    }

    /* Step 4: Remove retracted rows and fire delta callbacks */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0 || retract_nrows[ri] == 0)
            continue;

        uint32_t ncols = r->ncols;

        for (uint32_t del_idx = 0; del_idx < retract_nrows[ri]; del_idx++) {
            const int64_t *to_remove
                = retract_data[ri] + (size_t)del_idx * ncols;

            /* Find and remove this row in-place */
            uint32_t out_r = 0;
            bool found = false;
            for (uint32_t src_idx = 0; src_idx < r->nrows; src_idx++) {
                int64_t sbuf[COL_STACK_MAX];
                int64_t *src_buf = sbuf;
                if (ncols > COL_STACK_MAX)
                    src_buf = (int64_t *)malloc(ncols * sizeof(int64_t));
                col_rel_row_copy_out(r, src_idx, src_buf);
                if (memcmp(src_buf, to_remove, sizeof(int64_t) * ncols)
                    == 0) {
                    if (src_buf != sbuf)
                        free(src_buf);
                    /* Found matching row; skip it (removal) */
                    found = true;
                    /* Copy remaining rows forward */
                    for (uint32_t rest = src_idx + 1; rest < r->nrows;
                        rest++) {
                        col_rel_row_move(r, out_r, rest);
                        out_r++;
                    }
                    r->nrows = out_r;
                    break;
                } else {
                    /* Keep this row */
                    if (out_r != src_idx)
                        col_rel_row_copy_in(r, out_r, src_buf);
                    out_r++;
                    if (src_buf != sbuf)
                        free(src_buf);
                }
            }

            /* Fire delta callback if row was actually removed */
            if (found && sess->delta_cb) {
                sess->delta_cb(r->name, to_remove, ncols, -1,
                    sess->delta_data);
            }
        }
    }

    /* Cleanup: free stolen retraction buffers */
    for (uint32_t i = 0; i < rc_cnt; i++)
        free(retract_data[i]);
    free(retract_data);
    free(retract_nrows);
    return 0;
}

int
col_stratum_step_with_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    /* Issue #158: For now, use full re-evaluation for retraction.
     * When retraction_seeded is set, the standard delta callback logic
     * compares prev state with new state (recomputed from affected input),
     * and diff=-1 callbacks are fired for removed tuples.
     * Future optimization: implement col_stratum_step_retraction_nonrecursive
     * for direct delta-only propagation of retractions. */

    uint32_t rc_cnt = sp->relation_count;

    /* Allocate snapshot arrays */
    int64_t **prev_data = (int64_t **)calloc(rc_cnt, sizeof(int64_t *));
    uint32_t *prev_nrows = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    uint32_t *prev_ncols = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    if (!prev_data || !prev_nrows || !prev_ncols) {
        free(prev_data);
        free(prev_nrows);
        free(prev_ncols);
        return ENOMEM;
    }

    /* Step 1: pointer-swap snapshot for each IDB relation (issue #300).
     * Instead of malloc+memcpy, save the live data pointer and give the
     * relation a NULL buffer.  col_eval_stratum will allocate a fresh
     * buffer via append, so the old pointer stays valid for comparison. */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        prev_ncols[ri] = r->ncols;
        if (r->nrows > 0) {
            /* Gather into flat buffer for col_row_in_sorted */
            uint32_t nc = r->ncols;
            int64_t *flat = (int64_t *)malloc(
                (size_t)r->nrows * nc * sizeof(int64_t));
            if (flat) {
                for (uint32_t row = 0; row < r->nrows; row++)
                    col_rel_row_copy_out(r, row, flat + (size_t)row * nc);
            }
            prev_data[ri] = flat;
            prev_nrows[ri] = flat ? r->nrows : 0;
            /* Free and detach columns; eval will allocate fresh */
            col_columns_free(r->columns, r->ncols);
            r->columns = NULL;
            r->nrows = 0;
            r->capacity = 0;
            r->sorted_nrows = 0;
        } else {
            prev_nrows[ri] = 0;
        }
    }

    /* Step 2: evaluate stratum (appends new rows to IDB relations) */
    int rc = col_eval_stratum(sp, sess, stratum_idx);
    if (rc != 0)
        goto cleanup;

    /* Steps 3-4: consolidate each IDB relation, fire callbacks for new rows */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r)
            continue;

        /* Consolidate: sort + dedup so binary search is valid */
        rc = col_idb_consolidate(r, sess);
        if (rc != 0)
            goto cleanup;

        uint32_t ncols = r->ncols;

        /* Gather current state into flat buffer for col_row_in_sorted */
        int64_t *cur_flat = NULL;
        if (r->nrows > 0 && ncols > 0) {
            cur_flat = (int64_t *)malloc(
                (size_t)r->nrows * ncols * sizeof(int64_t));
            if (cur_flat) {
                for (uint32_t row = 0; row < r->nrows; row++)
                    col_rel_row_copy_out(r, row,
                        cur_flat + (size_t)row * ncols);
            }
        }

        /* Fire delta_cb(+1) for rows not present in prev sorted state */
        if (r->nrows > 0 && cur_flat) {
            for (uint32_t row = 0; row < r->nrows; row++) {
                const int64_t *rowp = cur_flat + (size_t)row * ncols;
                if (!col_row_in_sorted(prev_data[ri], prev_nrows[ri], ncols,
                    rowp)) {
                    sess->delta_cb(r->name, rowp, ncols, +1, sess->delta_data);
                }
            }
        }

        /* Fire delta_cb(-1) for rows present in prev sorted state but not in new
         */
        if (prev_nrows[ri] > 0) {
            for (uint32_t row = 0; row < prev_nrows[ri]; row++) {
                const int64_t *rowp
                    = prev_data[ri] + (size_t)row * prev_ncols[ri];
                if (!col_row_in_sorted(cur_flat, r->nrows, prev_ncols[ri],
                    rowp)) {
                    sess->delta_cb(r->name, rowp, prev_ncols[ri], -1,
                        sess->delta_data);
                }
            }
        }
        free(cur_flat);
    }

cleanup:
    for (uint32_t i = 0; i < rc_cnt; i++) {
        if (rc != 0 && prev_data[i]) {
            /* Error path: restore from flat snapshot into column-major */
            col_rel_t *r = session_find_rel(sess, sp->relations[i].name);
            if (r && !r->columns) {
                uint32_t nc = prev_ncols[i];
                uint32_t nr = prev_nrows[i];
                r->columns = col_columns_alloc(nc, nr > 0 ? nr : 1);
                if (r->columns) {
                    for (uint32_t row = 0; row < nr; row++) {
                        const int64_t *rowp
                            = prev_data[i] + (size_t)row * nc;
                        col_rel_row_copy_in(r, row, rowp);
                    }
                    r->nrows = nr;
                    r->capacity = nr > 0 ? nr : 1;
                    r->sorted_nrows = nr;
                    r->run_count = 1;
                    r->run_ends[0] = nr;
                }
                free(prev_data[i]);
                prev_data[i] = NULL;
            }
        }
        free(prev_data[i]);
    }
    free(prev_data);
    free(prev_nrows);
    free(prev_ncols);
    return rc;
}
bool
stratum_has_preseeded_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        col_rel_t *delta = session_find_rel(sess, dname);
        if (delta && delta->nrows > 0)
            return true;
    }
    return false;
}

/*
 * rule_index_to_stratum_index: Map a flat rule index to its stratum index
 *
 * Rules are laid out contiguously across strata in plan order. Stratum 0
 * owns rule indices [0, relation_count_0), stratum 1 owns
 * [relation_count_0, relation_count_0 + relation_count_1), and so on.
 * This function walks the strata, accumulating a running rule offset, and
 * returns the stratum whose window contains rule_id.
 *
 * Issue #107: Selective rule frontier reset uses this mapping to check
 * if a rule's stratum has pre-seeded delta before resetting the rule's frontier.
 *
 * @param plan:    Execution plan containing the strata array.
 * @param rule_id: Flat (zero-based) rule index to look up.
 * @return Stratum index that owns rule_id, or UINT32_MAX if rule_id is
 *         out of range (>= total rule count across all strata).
 */
uint32_t
rule_index_to_stratum_index(const wl_plan_t *plan, uint32_t rule_id)
{
    uint32_t offset = 0;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        uint32_t next_offset = offset + plan->strata[si].relation_count;
        if (rule_id < next_offset)
            return si;
        offset = next_offset;
    }
    return UINT32_MAX;
}

/* ======================================================================== */
/* Distributed Stratum Evaluator (Issue #318)                               */
/* ======================================================================== */

/*
 * col_eval_tdd_worker_ctx_t:
 * Per-worker context for one sub-pass of distributed stratum evaluation.
 *
 * For non-recursive workers (tdd_worker_nonrecursive_fn): only sp,
 * worker_sess, stratum_idx, and rc are used.
 *
 * For recursive sub-pass workers (tdd_worker_subpass_fn): all fields.
 * The coordinator allocates delta_rels[nrels] before dispatch; the worker
 * fills in entries for each relation that produced new tuples.  Ownership
 * of each delta_rels[ri] (heap-allocated via col_rel_new_like) transfers
 * to the coordinator after the workqueue barrier.
 */
typedef struct {
    const wl_plan_stratum_t *sp;   /* borrowed: stratum plan          */
    wl_col_session_t *worker_sess; /* borrowed: isolated worker       */
    uint32_t stratum_idx;
    uint32_t eff_iter;             /* effective sub-pass index (set by coord) */
    bool any_new;                  /* OUT: produced ≥1 new tuple      */
    bool all_empty_delta;          /* OUT: all FORCE_DELTA empty, skipped */
    bool force_diff;               /* IN: enable diff from eff_iter 0 (BDX) */
    col_rel_t **delta_rels;        /* OUT: produced deltas [nrels], coord frees */
    int rc;                        /* OUT: return code                */
} col_eval_tdd_worker_ctx_t;

/*
 * tdd_cleanup_workers:
 * Destroy and zero all initialized TDD worker sessions.
 * Safe to call on a coordinator with no workers (tdd_workers_count == 0).
 */
static void
tdd_cleanup_workers(wl_col_session_t *coord)
{
    for (uint32_t w = 0; w < coord->tdd_workers_count; w++) {
        if (coord->tdd_workers[w].coordinator != NULL)
            col_worker_session_destroy(&coord->tdd_workers[w]);
        memset(&coord->tdd_workers[w], 0, sizeof(wl_col_session_t));
    }
    coord->tdd_workers_count = 0;
}

/*
 * tdd_init_workers:
 * Partition all coordinator relations across W worker sessions.
 * Each worker gets 1/W rows of each relation, partitioned by column 0.
 * Empty/zero-column relations are replicated as empty copies.
 *
 * Any previously initialized workers are destroyed first.
 * On failure, any partially-created workers are destroyed.
 */
static int
tdd_init_workers(wl_col_session_t *coord)
{
    tdd_cleanup_workers(coord);

    uint32_t W = coord->num_workers;
    uint32_t nrels = coord->nrels;

    /* No relations: create empty worker sessions */
    if (nrels == 0) {
        for (uint32_t w = 0; w < W; w++) {
            int rc = col_worker_session_create(coord, w, NULL, 0,
                    &coord->tdd_workers[w]);
            if (rc != 0) {
                tdd_cleanup_workers(coord);
                return rc;
            }
            coord->tdd_workers_count = w + 1;
        }
        return 0;
    }

    /* Allocate W x nrels partition matrix */
    col_rel_t ***worker_parts = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_parts)
        return ENOMEM;

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        worker_parts[w] = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
        if (!worker_parts[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)worker_parts[j]);
            free((void *)worker_parts);
            return ENOMEM;
        }
    }

    /* Partition each coordinator relation by column 0 */
    uint32_t key_cols[] = { 0 };
    uint32_t parts_built = 0;

    for (uint32_t r = 0; r < nrels && rc == 0; r++) {
        col_rel_t *rel = coord->rels[r];
        if (!rel)
            continue;

        const char *name = rel->name;

        if (rel->ncols == 0 || rel->nrows == 0) {
            /* Empty: give each worker an empty relation */
            for (uint32_t w = 0; w < W && rc == 0; w++) {
                worker_parts[w][parts_built]
                    = col_rel_new_auto(name, rel->ncols);
                if (!worker_parts[w][parts_built])
                    rc = ENOMEM;
            }
        } else {
            col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
            if (!parts) {
                rc = ENOMEM;
            } else {
                rc = col_rel_partition_by_key(rel, key_cols, 1, W, parts);
                if (rc == 0) {
                    for (uint32_t w = 0; w < W && rc == 0; w++) {
                        free(parts[w]->name);
                        parts[w]->name = wl_strdup(name);
                        if (!parts[w]->name) {
                            rc = ENOMEM;
                        } else {
                            worker_parts[w][parts_built] = parts[w];
                            parts[w] = NULL; /* ownership transferred */
                        }
                    }
                }
                /* Free any unowned partition slots on error */
                for (uint32_t w = 0; w < W; w++)
                    col_rel_destroy(parts[w]); /* NULL-safe */
                free(parts);
            }
        }

        if (rc == 0)
            parts_built++;
    }

    /* Create worker sessions */
    uint32_t created = 0;
    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            rc = col_worker_session_create(coord, w,
                    worker_parts[w], parts_built, &coord->tdd_workers[w]);
            if (rc != 0)
                break;
            created++;
        }
    }

    /* On failure: destroy successfully-created workers; free unclaimed partitions */
    if (rc != 0) {
        for (uint32_t w = created; w < W; w++) {
            for (uint32_t p = 0; p < parts_built; p++)
                col_rel_destroy(worker_parts[w][p]);
        }
        coord->tdd_workers_count = created;
        tdd_cleanup_workers(coord);
    } else {
        coord->tdd_workers_count = W;
    }

    for (uint32_t w = 0; w < W; w++)
        free((void *)worker_parts[w]);
    free((void *)worker_parts);

    return rc;
}

/*
 * tdd_replicate_workers:
 * Give every worker a FULL COPY of all coordinator relations.
 *
 * Unlike tdd_init_workers (which partitions by column 0), this function
 * replicates each relation to every worker.  Required for recursive strata
 * where multi-way joins may reference columns other than the partition key
 * (e.g. the same-generation self-join on parent.col1).
 *
 * Issue #352: partitioning by col0 breaks self-joins on non-col0 columns
 * and 3-body recursive rules where IDB appears in the middle of the join.
 */
static int
tdd_replicate_workers(wl_col_session_t *coord)
{
    tdd_cleanup_workers(coord);

    uint32_t W = coord->num_workers;
    uint32_t nrels = coord->nrels;

    /* No relations: create empty worker sessions */
    if (nrels == 0) {
        for (uint32_t w = 0; w < W; w++) {
            int rc = col_worker_session_create(coord, w, NULL, 0,
                    &coord->tdd_workers[w]);
            if (rc != 0) {
                tdd_cleanup_workers(coord);
                return rc;
            }
            coord->tdd_workers_count = w + 1;
        }
        return 0;
    }

    /* Allocate W x nrels relation matrix */
    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_rels)
        return ENOMEM;

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        worker_rels[w] = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
        if (!worker_rels[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)worker_rels[j]);
            free((void *)worker_rels);
            return ENOMEM;
        }
    }

    /* Replicate each coordinator relation to every worker */
    uint32_t rels_built = 0;

    for (uint32_t r = 0; r < nrels && rc == 0; r++) {
        col_rel_t *rel = coord->rels[r];
        if (!rel)
            continue;

        const char *name = rel->name;

        for (uint32_t w = 0; w < W && rc == 0; w++) {
            col_rel_t *copy = col_rel_new_auto(name, rel->ncols);
            if (!copy) {
                rc = ENOMEM;
                break;
            }
            if (rel->nrows > 0) {
                rc = col_rel_append_all(copy, rel, NULL);
                if (rc != 0) {
                    col_rel_destroy(copy);
                    break;
                }
            }
            worker_rels[w][rels_built] = copy;
        }

        if (rc == 0)
            rels_built++;
    }

    /* Create worker sessions */
    uint32_t created = 0;
    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            rc = col_worker_session_create(coord, w,
                    worker_rels[w], rels_built, &coord->tdd_workers[w]);
            if (rc != 0)
                break;
            created++;
        }
    }

    /* On failure: destroy successfully-created workers; free unclaimed rels */
    if (rc != 0) {
        for (uint32_t w = created; w < W; w++) {
            for (uint32_t p = 0; p < rels_built; p++)
                col_rel_destroy(worker_rels[w][p]);
        }
        coord->tdd_workers_count = created;
        tdd_cleanup_workers(coord);
    } else {
        coord->tdd_workers_count = W;
    }

    for (uint32_t w = 0; w < W; w++)
        free((void *)worker_rels[w]);
    free((void *)worker_rels);

    return rc;
}

/*
 * tdd_worker_subpass_fn:
 * Execute one sub-pass of the semi-naive iteration on a worker's partition.
 *
 * Mirrors eval.c:392-736 for a single (iter, sub) effective iteration,
 * operating entirely on the worker's local session.  The coordinator
 * controls the outer iteration loop and convergence detection.
 *
 * Three differences from the single-worker path (per IMPLEMENTATION_PLAN
 * Clarification 1):
 *   1. eff_iter comes from ctx->eff_iter (set by coordinator).
 *   2. diff_operators_active set explicitly (Clarification 3).
 *   3. Produced delta_rels stored in ctx->delta_rels[] for broadcast
 *      exchange; deltas are heap-allocated (col_rel_new_like) so they
 *      remain valid across delta_pool_reset.
 */
static void
tdd_worker_subpass_fn(void *arg)
{
    col_eval_tdd_worker_ctx_t *ctx = (col_eval_tdd_worker_ctx_t *)arg;
    wl_col_session_t *sess = ctx->worker_sess;
    const wl_plan_stratum_t *sp = ctx->sp;
    uint32_t eff_iter = ctx->eff_iter;
    uint32_t nrels = sp->relation_count;

    /* Issue #282: Enable differential operators from eff_iter 1 onward.
     * Mirrors eval.c:410-411 / Clarification 3.
     * Issue #390: BDX mode forces diff from eff_iter 0 so that K_FUSION
     * evaluates Δr ⋈ r_w (broadcast delta × local partition) instead of
     * r_w ⋈ r_w (incomplete local self-join). */
    bool saved_diff = sess->diff_operators_active;
    sess->diff_operators_active = sess->diff_enabled
        && (eff_iter > 0 || ctx->force_diff);
    /* Issue #318: Signal to col_op_variable AUTO heuristic that we are inside
     * a TDD worker sub-pass.  Broadcast $d$<rel> may be >= local partition
     * size, so the normal "delta < full" guard must be bypassed. */
    bool saved_tdd_subpass = sess->tdd_subpass_active;
    sess->tdd_subpass_active = true;
    sess->current_iteration = eff_iter;

    /* Free per-sub-pass delta arrangements (eval.c:429) */
    col_session_free_delta_arrangements(sess);

    /* Early-exit: all relation plans have empty FORCE_DELTA (eval.c:486-498).
    * Worker reports all_empty_delta; coordinator skips to next outer iter. */
    if (eff_iter > 0) {
        bool all_empty = true;
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (!has_empty_forced_delta(&sp->relations[ri], sess, eff_iter)) {
                all_empty = false;
                break;
            }
        }
        if (all_empty) {
            ctx->all_empty_delta = true;
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->diff_operators_active = saved_diff;
            return;
        }
    }

    /* Snapshot nrows before evaluation (eval.c:446-449) */
    uint32_t *snap = (uint32_t *)calloc(nrels, sizeof(uint32_t));
    if (!snap) {
        ctx->rc = ENOMEM;
        sess->tdd_subpass_active = saved_tdd_subpass;
        sess->diff_operators_active = saved_diff;
        return;
    }
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        snap[ri] = r ? r->nrows : 0;
    }

    /* Evaluate all relation plans (eval.c:505-604) */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];

        if (has_empty_forced_delta(rp, sess, eff_iter))
            continue;

        eval_stack_t stack;
        eval_stack_init(&stack);

        int rc = col_eval_relation_plan(rp, &stack, sess);
        if (rc != 0) {
            eval_stack_drain(&stack);
            ctx->rc = rc;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->diff_operators_active = saved_diff;
            return;
        }

        if (stack.top == 0)
            continue;

        eval_entry_t result = eval_stack_pop(&stack);
        eval_stack_drain(&stack);

        if (result.rel && result.rel->nrows == 0) {
            if (result.owned)
                col_rel_destroy(result.rel);
            continue;
        }

        col_rel_t *target = session_find_rel(sess, rp->name);
        if (!target) {
            col_rel_t *copy;
            if (result.owned) {
                copy = result.rel;
                free(copy->name);
                copy->name = wl_strdup(rp->name);
                if (!copy->name) {
                    col_rel_destroy(copy);
                    ctx->rc = ENOMEM;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->diff_operators_active = saved_diff;
                    return;
                }
                result.owned = false;
            } else {
                copy = col_rel_pool_new_like(sess->delta_pool, rp->name,
                        result.rel);
                if (!copy) {
                    ctx->rc = ENOMEM;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->diff_operators_active = saved_diff;
                    return;
                }
                rc = col_rel_append_all(copy, result.rel, sess->eval_arena);
                if (rc != 0) {
                    col_rel_destroy(copy);
                    ctx->rc = rc;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->diff_operators_active = saved_diff;
                    return;
                }
            }
            rc = session_add_rel(sess, copy);
            if (rc != 0) {
                col_rel_destroy(copy);
                ctx->rc = rc;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->diff_operators_active = saved_diff;
                return;
            }
        } else {
            if (target->ncols == 0 && result.rel->ncols > 0) {
                rc = col_rel_set_schema(target, result.rel->ncols,
                        (const char *const *)result.rel->col_names);
                if (rc != 0) {
                    if (result.owned)
                        col_rel_destroy(result.rel);
                    ctx->rc = rc;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->diff_operators_active = saved_diff;
                    return;
                }
            }
            rc = col_rel_append_all(target, result.rel, sess->eval_arena);
            if (result.owned)
                col_rel_destroy(result.rel);
            if (rc != 0) {
                ctx->rc = rc;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->diff_operators_active = saved_diff;
                return;
            }
        }
    }

    /* Issue #361: Clear delta relations instead of removing them.
     * Pre-installed $d$ persists across iterations; nrows=0 triggers
     * has_empty_forced_delta skip, same as absent $d$. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        col_rel_t *d = session_find_rel(sess, dname);
        if (d) {
            d->nrows = 0;
            free(d->timestamps);
            d->timestamps = NULL;
        }
    }

    /* Consolidate + produce deltas (eval.c:621-713).
     * Use col_rel_new_like (heap) so deltas survive delta_pool_reset. */
    bool any_new = false;
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || snap[ri] >= r->nrows)
            continue;

        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);

        /* Heap-allocate delta so it survives delta_pool_reset below. */
        col_rel_t *delta = col_rel_new_like(dname, r);
        if (!delta) {
            ctx->rc = ENOMEM;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->diff_operators_active = saved_diff;
            return;
        }

        int rc2 = 0;
        if (r->dedup_slots) {
            /* Hash-set dedup: O(D) per subpass instead of O(N) merge.
             * For each new row, check the hash set. Keep only truly
             * new rows in the relation and emit them to delta. */
            int64_t row_buf[8];
            int64_t *rbuf = r->ncols <= 8 ? row_buf
                : (int64_t *)malloc((size_t)r->ncols * sizeof(int64_t));
            if (!rbuf) {
                col_rel_destroy(delta);
                ctx->rc = ENOMEM;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->diff_operators_active = saved_diff;
                return;
            }
            uint32_t keep = snap[ri];
            for (uint32_t i = snap[ri]; i < r->nrows; i++) {
                uint64_t h = dedup_row_hash(r, i);
                if (dedup_set_insert(r, h)) {
                    /* New row: compact into [keep] and emit to delta. */
                    if (keep != i)
                        col_rel_row_move(r, keep, i);
                    for (uint32_t c = 0; c < r->ncols; c++)
                        rbuf[c] = r->columns[c][keep];
                    col_rel_append_row(delta, rbuf);
                    keep++;
                }
            }
            r->nrows = keep;
            r->sorted_nrows = keep; /* not truly sorted but OK for hash joins */
            if (rbuf != row_buf)
                free(rbuf);
        } else {
            int fast_flag = 0;
            rc2 = col_op_consolidate_incremental_delta(r, snap[ri], delta,
                    &fast_flag);
        }
        col_session_invalidate_arrangements(&sess->base,
            sp->relations[ri].name);

        /* Issue #388: rc2 != 0 (including EOVERFLOW from join truncation)
         * propagates as a worker error, so any_new is not set on truncation.
         * This is correct: EOVERFLOW is a hard error requiring coordinator
         * intervention, not a soft truncation that should continue. */
        if (rc2 != 0) {
            col_rel_destroy(delta);
            ctx->rc = rc2;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->diff_operators_active = saved_diff;
            return;
        }

        if (delta->nrows > 0) {
            /* Stamp timestamps (eval.c:668-696) */
            delta->timestamps = (col_delta_timestamp_t *)calloc(
                delta->nrows, sizeof(col_delta_timestamp_t));
            if (!delta->timestamps) {
                col_rel_destroy(delta);
                ctx->rc = ENOMEM;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->diff_operators_active = saved_diff;
                return;
            }
            for (uint32_t ti = 0; ti < delta->nrows; ti++) {
                delta->timestamps[ti].iteration = eff_iter;
                delta->timestamps[ti].stratum = ctx->stratum_idx;
                delta->timestamps[ti].worker = (uint16_t)sess->worker_id;
                delta->timestamps[ti].multiplicity = 1;
            }

            /* Enable target timestamps */
            if (!r->timestamps && r->capacity > 0) {
                r->timestamps = (col_delta_timestamp_t *)calloc(
                    r->capacity, sizeof(col_delta_timestamp_t));
                if (!r->timestamps) {
                    free(delta->timestamps);
                    col_rel_destroy(delta);
                    ctx->rc = ENOMEM;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->diff_operators_active = saved_diff;
                    return;
                }
            }

            ctx->delta_rels[ri] = delta; /* coordinator takes ownership */
            any_new = true;
        } else {
            col_rel_destroy(delta);
        }
    }

    free(snap);

    /* Reset per-sub-pass allocators and cache (eval.c:716-727) */
    delta_pool_reset(sess->delta_pool);
    wl_arena_reset(sess->eval_arena);
    if (sess->cache_evict_threshold == 0) {
        col_mat_cache_clear(&sess->mat_cache);
    } else {
        col_mat_cache_evict_until(&sess->mat_cache,
            sess->cache_evict_threshold);
    }

    ctx->any_new = any_new;
    sess->tdd_subpass_active = saved_tdd_subpass;
    sess->diff_operators_active = saved_diff;
}

/*
 * tdd_worker_nonrecursive_fn:
 * Work function for non-recursive distributed stratum evaluation.
 * Each worker evaluates the stratum on its local data partition.
 */
static void
tdd_worker_nonrecursive_fn(void *arg)
{
    col_eval_tdd_worker_ctx_t *ctx = (col_eval_tdd_worker_ctx_t *)arg;

    ctx->rc = col_eval_stratum(ctx->sp, ctx->worker_sess, ctx->stratum_idx);
}

/*
 * tdd_merge_worker_results:
 * After workers complete evaluation, merge their derived IDB relations
 * back into the coordinator session.
 *
 * For each relation in the stratum plan, collects all worker outputs
 * and appends them into a single coordinator-owned relation.
 */
static int
tdd_merge_worker_results(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    uint32_t W = coord->tdd_workers_count;
    int rc = 0;

    for (uint32_t ri = 0; ri < sp->relation_count && rc == 0; ri++) {
        const char *rel_name = sp->relations[ri].name;

        /* Find or create target relation in coordinator */
        col_rel_t *target = session_find_rel(coord, rel_name);

        for (uint32_t w = 0; w < W && rc == 0; w++) {
            col_rel_t *wrel
                = session_find_rel(&coord->tdd_workers[w], rel_name);
            if (!wrel || wrel->nrows == 0)
                continue;

            if (!target) {
                /* First non-empty result: move to coordinator */
                target = col_rel_new_auto(rel_name, wrel->ncols);
                if (!target) {
                    rc = ENOMEM;
                    break;
                }
                rc = session_add_rel(coord, target);
                if (rc != 0) {
                    col_rel_destroy(target);
                    target = NULL;
                    break;
                }
            }

            /* Set schema if coordinator relation was pre-registered with 0 cols */
            if (target->ncols == 0 && wrel->ncols > 0) {
                rc = col_rel_set_schema(target, wrel->ncols,
                        (const char *const *)wrel->col_names);
                if (rc != 0)
                    break;
            }

            rc = col_rel_append_all(target, wrel, NULL);
        }

        /* Issue #353: When all workers produced empty results, the relation
         * was never registered on the coordinator.  Subsequent strata that
         * reference it (e.g. recursive stratum reading non-recursive output)
         * would fail with ENOENT.  Register an empty relation so downstream
         * strata can find it. */
        if (!target && rc == 0) {
            target = col_rel_new_auto(rel_name, 0);
            if (!target) {
                rc = ENOMEM;
            } else {
                rc = session_add_rel(coord, target);
                if (rc != 0) {
                    col_rel_destroy(target);
                    target = NULL;
                }
            }
        }
    }

    return rc;
}

/*
 * tdd_dedup_rel:
 * Sort a relation in-place and remove consecutive duplicate rows.
 * Called after merging worker results for recursive strata to eliminate
 * duplicates introduced by broadcast exchange (multiple partitions may
 * independently derive the same tuple via different equal-length paths).
 *
 * Storage is column-major: r->columns[col][row].
 */
static void
tdd_dedup_rel(col_rel_t *r)
{
    if (!r || r->nrows <= 1 || r->ncols == 0)
        return;

    uint32_t ncols = r->ncols;
    uint32_t nrows = r->nrows;

    /* Presort check: O(n) scan avoids sort for already-ordered input
     * (e.g. single-worker runs or pre-sorted merge results). */
    bool sorted = true;
    for (uint32_t i = 1; i < nrows && sorted; i++) {
        for (uint32_t c = 0; c < ncols; c++) {
            int64_t a = r->columns[c][i - 1];
            int64_t b = r->columns[c][i];
            if (a < b)
                break;         /* this pair is in order */
            if (a > b) {
                sorted = false;
                break;
            }
        }
    }

    if (!sorted) {
        /* Hash-based dedup: O(n), avoids O(n log n) sort.
         * Open-addressing table with FNV-1a row hashing, load <= 0.5.
         * Two-pass: first pass marks unique rows (read-only on columns),
         * second pass compacts in-place. */
        uint32_t cap = 4;
        while (cap < nrows * 2)
            cap <<= 1;
        uint32_t mask = cap - 1;
        uint32_t *ht = (uint32_t *)malloc(cap * sizeof(uint32_t));
        uint8_t  *keep = (uint8_t *)malloc(nrows);

        if (!ht || !keep) {
            /* Allocation failure: fall back to sort-based path */
            free(ht);
            free(keep);
            col_rel_radix_sort_int64(r);
            sorted = true; /* fall through to sorted dedup below */
        } else {
            memset(ht, 0xFF, cap * sizeof(uint32_t)); /* 0xFF = UINT32_MAX */
            memset(keep, 0, nrows);

            /* First pass: build hash table, mark unique rows */
            for (uint32_t i = 0; i < nrows; i++) {
                uint64_t h = 14695981039346656037ULL; /* FNV-1a offset basis */
                for (uint32_t c = 0; c < ncols; c++) {
                    h ^= (uint64_t)r->columns[c][i];
                    h *= 1099511628211ULL; /* FNV prime */
                }
                uint32_t slot = (uint32_t)(h & mask);
                for (;;) {
                    uint32_t ex = ht[slot];
                    if (ex == UINT32_MAX) {
                        ht[slot] = i;
                        keep[i] = 1;
                        break;
                    }
                    bool eq = true;
                    for (uint32_t c = 0; c < ncols; c++) {
                        if (r->columns[c][ex] != r->columns[c][i]) {
                            eq = false;
                            break;
                        }
                    }
                    if (eq)
                        break; /* duplicate */
                    slot = (slot + 1) & mask;
                }
            }
            free(ht);

            /* Second pass: compact columns in-place */
            uint32_t out = 0;
            for (uint32_t i = 0; i < nrows; i++) {
                if (!keep[i])
                    continue;
                if (out != i) {
                    col_columns_copy_row(r->columns, out,
                        (int64_t *const *)r->columns, i, ncols);
                    if (r->timestamps)
                        r->timestamps[out] = r->timestamps[i];
                }
                out++;
            }
            free(keep);
            r->nrows = out;
            return;
        }
    }

    /* Sorted path: linear scan dedup (input sorted, no sort needed) */
    uint32_t out = 1;
    for (uint32_t i = 1; i < r->nrows; i++) {
        bool dup = true;
        for (uint32_t c = 0; c < ncols; c++) {
            if (r->columns[c][i - 1] != r->columns[c][i]) {
                dup = false;
                break;
            }
        }
        if (!dup) {
            if (out != i)
                col_columns_copy_row(r->columns, out,
                    (int64_t *const *)r->columns, i, ncols);
            if (r->timestamps)
                r->timestamps[out] = r->timestamps[i];
            out++;
        }
    }
    r->nrows = out;
}

/*
 * tdd_sorted_merge_append:
 * Merge src (sorted, no overlap with dst) into dst (sorted), maintaining
 * lexicographic sorted order.  O(N + D) where N = dst->nrows, D = src->nrows.
 *
 * Preconditions (caller guarantees):
 *   - dst->columns[] rows are in sorted (lexicographic) order
 *   - src->columns[] rows are in sorted order
 *   - No row appears in both dst and src (guaranteed by bdx_merge_diff)
 *
 * Replaces col_rel_append_all + tdd_dedup_rel(dst) in tdd_bdx_exchange_deltas
 * to keep the coordinator IDB sorted for bdx_merge_diff on the next iteration.
 * Issue #390.
 */
int
tdd_sorted_merge_append(col_rel_t *dst, col_rel_t *src)
{
    if (!src || src->nrows == 0)
        return 0;
    if (!dst || dst->ncols == 0)
        return 0;

    uint32_t N = dst->nrows;
    uint32_t D = src->nrows;
    uint32_t ncols = dst->ncols;
    uint32_t total = N + D;

    if (N == 0)
        return col_rel_append_all(dst, src, NULL);

    /* Copy current dst rows into the persistent merge buffer */
    if (dst->merge_buf_cap < N) {
        int64_t **mc = col_columns_alloc(ncols, N);
        if (!mc)
            return ENOMEM;
        col_columns_free(dst->merge_columns, ncols);
        dst->merge_columns = mc;
        dst->merge_buf_cap = N;
    }
    for (uint32_t c = 0; c < ncols; c++)
        memcpy(dst->merge_columns[c], dst->columns[c], N * sizeof(int64_t));

    /* Grow dst columns to hold the merged result */
    if (dst->capacity < total) {
        if (col_columns_realloc(dst->columns, ncols, total) != 0)
            return ENOMEM;
        dst->capacity = total;
    }

    /* Two-pointer merge: both sequences are sorted, no overlap */
    uint32_t i = 0, j = 0, wr = 0;
    while (i < N && j < D) {
        int cmp = 0;
        for (uint32_t c = 0; c < ncols; c++) {
            int64_t a = dst->merge_columns[c][i];
            int64_t b = src->columns[c][j];
            if (a < b) {
                cmp = -1; break;
            }
            if (a > b) {
                cmp = 1; break;
            }
        }
        if (cmp <= 0) {
            col_columns_copy_row(dst->columns, wr,
                (int64_t *const *)dst->merge_columns, i, ncols);
            i++; wr++;
        } else {
            col_columns_copy_row(dst->columns, wr,
                (int64_t *const *)src->columns, j, ncols);
            j++; wr++;
        }
    }
    while (i < N) {
        col_columns_copy_row(dst->columns, wr,
            (int64_t *const *)dst->merge_columns, i, ncols);
        i++; wr++;
    }
    while (j < D) {
        col_columns_copy_row(dst->columns, wr,
            (int64_t *const *)src->columns, j, ncols);
        j++; wr++;
    }
    dst->nrows = total;
    return 0;
}

/*
 * tdd_preregister_idb_on_workers:
 * Pre-register empty IDB relations on each worker session so that
 * VARIABLE ops can find them on the first sub-pass (eff_iter == 0).
 * Mirrors eval.c:291-304.
 */
static int
tdd_preregister_idb_on_workers(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    uint32_t W = coord->tdd_workers_count;

    for (uint32_t w = 0; w < W; w++) {
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const char *rname = sp->relations[ri].name;
            if (session_find_rel(&coord->tdd_workers[w], rname))
                continue;
            col_rel_t *empty = NULL;
            int rc = col_rel_alloc(&empty, rname);
            if (rc != 0)
                return ENOMEM;
            rc = session_add_rel(&coord->tdd_workers[w], empty);
            if (rc != 0) {
                col_rel_destroy(empty);
                return rc;
            }
        }
    }
    return 0;
}

/* Forward declaration — defined below tdd_exchange_deltas. */
static int tdd_broadcast_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W);

/*
 * tdd_broadcast_relation_delta:
 * Union all worker deltas for relation index ri and install the union as
 * $d$<relname> on every worker.  Used by tdd_exchange_deltas for relations
 * that require broadcast (e.g. IDB self-join strata in asymmetric mode).
 *
 * Ownership of ctxs[w].delta_rels[ri] transfers here; all entries are
 * consumed (freed or moved) before return.
 */
static int
tdd_broadcast_relation_delta(const wl_plan_stratum_t *sp, uint32_t ri,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W)
{
    char dname[256];
    snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);

    for (uint32_t w = 0; w < W; w++)
        session_remove_rel(&coord->tdd_workers[w], dname);

    uint32_t total = 0, ncols = 0;
    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *d = ctxs[w].delta_rels[ri];
        if (d && d->nrows > 0) {
            total += d->nrows;
            if (ncols == 0)
                ncols = d->ncols;
        }
    }
    if (total == 0) {
        for (uint32_t w = 0; w < W; w++) {
            col_rel_destroy(ctxs[w].delta_rels[ri]);
            ctxs[w].delta_rels[ri] = NULL;
        }
        return 0;
    }

    col_rel_t *union_d = col_rel_new_auto(dname, ncols);
    if (!union_d)
        return ENOMEM;

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *d = ctxs[w].delta_rels[ri];
        ctxs[w].delta_rels[ri] = NULL;
        if (d && d->nrows > 0)
            rc = col_rel_append_all(union_d, d, NULL);
        col_rel_destroy(d);
        if (rc != 0) {
            col_rel_destroy(union_d);
            return rc;
        }
    }

    /* Issue #390: Dedup broadcast union to prevent duplicate delta
     * amplification.  In self_join_mode, workers hold disjoint 1/W IDB
     * partitions but can independently derive the same tuple via
     * different join paths.  Matches tdd_broadcast_deltas (line 3299). */
    if (union_d->nrows > 1)
        tdd_dedup_rel(union_d);

    /* Issue #390: zero-copy broadcast via col_shared.
     * Anchor union_d in worker 0's session; workers 1..W-1 borrow
     * column pointers via col_rel_install_shared_view (O(ncols) pointer
     * setup) instead of O(|delta|) deep copies.  Mirrors the pattern
     * in tdd_broadcast_deltas (lines 3307-3354). */
    rc = session_add_rel(&coord->tdd_workers[0], union_d);
    if (rc != 0) {
        col_rel_destroy(union_d);
        return rc;
    }
    /* union_d is now owned by worker 0's session */
    for (uint32_t dst = 1; dst < W; dst++) {
        col_rel_t *view = col_rel_new_auto(dname, ncols);
        if (!view)
            return ENOMEM;
        rc = col_rel_install_shared_view(view, union_d);
        if (rc != 0) {
            /* Fallback: deep copy on shared-view alloc failure */
            rc = col_rel_append_all(view, union_d, NULL);
            if (rc != 0) {
                col_rel_destroy(view);
                return rc;
            }
        }
        rc = session_add_rel(&coord->tdd_workers[dst], view);
        if (rc != 0) {
            col_rel_destroy(view);
            return rc;
        }
    }
    return 0;
}

/*
 * tdd_alloc_exchange_bufs:
 * Allocate the W x W mailbox matrix on the coordinator session.
 * exchange_bufs[src][dst] will hold rows that worker src sends to dst.
 * On failure, exchange_bufs remains NULL.
 */
static int
tdd_alloc_exchange_bufs(wl_col_session_t *coord, uint32_t W)
{
    coord->exchange_bufs
        = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!coord->exchange_bufs)
        return ENOMEM;

    for (uint32_t w = 0; w < W; w++) {
        coord->exchange_bufs[w]
            = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!coord->exchange_bufs[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)coord->exchange_bufs[j]);
            free((void *)coord->exchange_bufs);
            coord->exchange_bufs = NULL;
            return ENOMEM;
        }
    }

    coord->exchange_num_workers = W;
    return 0;
}

/*
 * tdd_free_exchange_bufs:
 * Destroy all relations held in the W x W mailbox matrix, free the
 * matrix, and clear the coordinator fields.
 */
static void
tdd_free_exchange_bufs(wl_col_session_t *coord)
{
    if (!coord->exchange_bufs)
        return;

    uint32_t W = coord->exchange_num_workers;

    for (uint32_t src = 0; src < W; src++) {
        if (!coord->exchange_bufs[src])
            continue;
        for (uint32_t dst = 0; dst < W; dst++)
            col_rel_destroy(coord->exchange_bufs[src][dst]);
        free((void *)coord->exchange_bufs[src]);
    }

    free((void *)coord->exchange_bufs);
    coord->exchange_bufs = NULL;
    coord->exchange_num_workers = 0;
}

/*
 * tdd_gather_for_worker:
 * Merge exchange_bufs[0..W-1][dst] into a single heap-allocated relation
 * named dname.  Returns NULL in *out_gathered when all source partitions
 * are empty (no new tuples for this worker).
 */
static int
tdd_gather_for_worker(wl_col_session_t *coord, uint32_t dst, uint32_t W,
    const char *dname, col_rel_t **out_gathered)
{
    uint32_t total_rows = 0;
    uint32_t ncols = 0;

    for (uint32_t src = 0; src < W; src++) {
        col_rel_t *part = coord->exchange_bufs[src][dst];
        if (part && part->nrows > 0) {
            total_rows += part->nrows;
            if (ncols == 0)
                ncols = part->ncols;
        }
    }

    *out_gathered = NULL;

    if (total_rows == 0 || ncols == 0)
        return 0;

    col_rel_t *gathered = col_rel_new_auto(dname, ncols);
    if (!gathered)
        return ENOMEM;

    for (uint32_t src = 0; src < W; src++) {
        col_rel_t *part = coord->exchange_bufs[src][dst];
        if (!part || part->nrows == 0)
            continue;
        int rc = col_rel_append_all(gathered, part, NULL);
        if (rc != 0) {
            col_rel_destroy(gathered);
            return rc;
        }
    }

    *out_gathered = gathered;
    return 0;
}

/*
 * tdd_exchange_deltas:
 * After a sub-pass barrier, redistribute worker deltas using
 * hash-partitioned scatter/gather.
 *
 * If the stratum plan contains WL_PLAN_OP_EXCHANGE ops, uses the
 * key column metadata from those ops to hash-partition each worker's
 * delta into exchange_bufs[w][*], then gathers exchange_bufs[*][dst]
 * for each destination worker and installs the result as $d$<relname>.
 * This eliminates broadcast duplicates for plans that carry EXCHANGE ops.
 *
 * If no EXCHANGE ops are present (e.g. transitive closure with a join
 * key that differs from the partition key), falls back to
 * tdd_broadcast_deltas to preserve correctness.
 *
 * Issue #372: When self_join_mode is true (IDB self-join stratum using
 * asymmetric partition-replicate), all relation deltas are broadcast to
 * every worker.  Each worker holds 1/W of the IDB (partitioned by hash)
 * and needs the full delta to probe against its local partition.
 * Hash-partitioning the delta would send each worker only 1/W of the new
 * tuples, causing missed joins with the complementary IDB partition.
 *
 * Ownership of ctxs[w].delta_rels[ri] entries transfers here;
 * all entries are consumed (freed or moved) before return.
 */
static int
tdd_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    bool default_hash, bool self_join_mode)
{
    uint32_t nrels = sp->relation_count;

    /* Issue #372: Self-join strata use asymmetric partition-replicate.
     * IDB is partitioned (via hybrid init); deltas are broadcast so each
     * worker can probe its 1/W IDB partition with the full delta. */
    if (self_join_mode && default_hash) {
        int rc = 0;
        for (uint32_t ri = 0; rc == 0 && ri < nrels; ri++)
            rc = tdd_broadcast_relation_delta(sp, ri, coord, ctxs, W);
        return rc;
    }

    /* Check whether any relation plan carries EXCHANGE op metadata. */
    bool has_exchange = false;
    for (uint32_t ri = 0; ri < nrels && !has_exchange; ri++) {
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                has_exchange = true;
                break;
            }
        }
    }

    /* No EXCHANGE metadata and no default hash: broadcast (replicate mode). */
    if (!has_exchange && !default_hash)
        return tdd_broadcast_deltas(sp, coord, ctxs, W);

    /* Replicate mode: all workers hold identical data, so hash scatter/gather
     * would produce W× duplicate deltas.  Force broadcast to avoid bloat. */
    if (!default_hash)
        return tdd_broadcast_deltas(sp, coord, ctxs, W);

    /* Hash-partitioned scatter/gather exchange. */
    int rc = tdd_alloc_exchange_bufs(coord, W);
    if (rc != 0) {
        for (uint32_t w = 0; w < W; w++)
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
            }
        return rc;
    }

    for (uint32_t ri = 0; ri < nrels; ri++) {
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);

        /* Locate EXCHANGE key columns for this relation (if any). */
        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta
                    = (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->key_col_count > 0) {
                    key_cols = meta->key_col_idxs;
                    key_count = meta->key_col_count;
                }
                break;
            }
        }

        /* Remove stale $d$ from every worker before scatter. */
        for (uint32_t w = 0; w < W; w++)
            session_remove_rel(&coord->tdd_workers[w], dname);

        /* Issue #361: Relations without EXCHANGE ops use default col0
         * hash-exchange when default_hash is set (hybrid init partitions
         * IDB by col0).  EDB is replicated, so joins against partitioned
         * $d$ are complete within each worker. */
        uint32_t default_key[] = { 0 };
        if ((!key_cols || key_count == 0) && default_hash) {
            key_cols = default_key;
            key_count = 1;
        }

        if (!key_cols || key_count == 0) {
            /* Broadcast: union worker deltas, install on every worker.
             * Used for replicate-mode strata where IDB is not partitioned. */
            uint32_t total = 0, ncols = 0;
            for (uint32_t w = 0; w < W; w++) {
                col_rel_t *d = ctxs[w].delta_rels[ri];
                if (d && d->nrows > 0) {
                    total += d->nrows;
                    if (ncols == 0) ncols = d->ncols;
                }
            }
            if (total == 0) {
                for (uint32_t w = 0; w < W; w++) {
                    col_rel_destroy(ctxs[w].delta_rels[ri]);
                    ctxs[w].delta_rels[ri] = NULL;
                }
            } else {
                col_rel_t *union_d = col_rel_new_auto(dname, ncols);
                if (!union_d) {
                    rc = ENOMEM; goto exchange_done;
                }
                for (uint32_t w = 0; w < W; w++) {
                    col_rel_t *d = ctxs[w].delta_rels[ri];
                    ctxs[w].delta_rels[ri] = NULL;
                    if (d && d->nrows > 0)
                        rc = col_rel_append_all(union_d, d, NULL);
                    col_rel_destroy(d);
                    if (rc != 0) {
                        col_rel_destroy(union_d); goto exchange_done;
                    }
                }
                for (uint32_t dst = 0; dst < W; dst++) {
                    col_rel_t *copy;
                    if (dst < W - 1) {
                        copy = col_rel_new_auto(dname, ncols);
                        if (!copy) {
                            col_rel_destroy(union_d); rc = ENOMEM;
                            goto exchange_done;
                        }
                        rc = col_rel_append_all(copy, union_d, NULL);
                        if (rc != 0) {
                            col_rel_destroy(copy); col_rel_destroy(union_d);
                            goto exchange_done;
                        }
                    } else {
                        copy = union_d;
                        union_d = NULL;
                    }
                    rc = session_add_rel(&coord->tdd_workers[dst], copy);
                    if (rc != 0) {
                        col_rel_destroy(copy);
                        if (union_d) col_rel_destroy(union_d);
                        goto exchange_done;
                    }
                }
            }
        } else {
            /* Hash-partitioned scatter/gather for EXCHANGE-keyed relations */
            for (uint32_t w = 0; w < W; w++) {
                col_rel_t *d = ctxs[w].delta_rels[ri];
                ctxs[w].delta_rels[ri] = NULL;

                if (!d || d->nrows == 0 || d->ncols == 0) {
                    col_rel_destroy(d);
                    continue;
                }

                rc = col_rel_partition_by_key(d, key_cols, key_count,
                        W, coord->exchange_bufs[w]);
                col_rel_destroy(d);

                if (rc != 0)
                    goto exchange_done;
            }

            /* Gather: worker dst receives exchange_bufs[*][dst]. */
            for (uint32_t dst = 0; dst < W; dst++) {
                col_rel_t *gathered = NULL;
                rc = tdd_gather_for_worker(coord, dst, W, dname, &gathered);
                if (rc != 0)
                    goto exchange_done;

                if (gathered) {
                    rc = session_add_rel(&coord->tdd_workers[dst], gathered);
                    if (rc != 0) {
                        col_rel_destroy(gathered);
                        goto exchange_done;
                    }
                }
            }
        }

        /* Release exchange_bufs[*][*] for this relation — data was copied
         * into the gathered relations above. */
        for (uint32_t src = 0; src < W; src++) {
            for (uint32_t dst = 0; dst < W; dst++) {
                col_rel_destroy(coord->exchange_bufs[src][dst]);
                coord->exchange_bufs[src][dst] = NULL;
            }
        }
    }

exchange_done:
    tdd_free_exchange_bufs(coord);
    return rc;
}

/*
 * tdd_broadcast_deltas:
 * After a sub-pass barrier, union all worker deltas for each IDB
 * relation and install the union as $d$<relname> on EVERY worker.
 *
 * Ownership of entries in ctxs[w].delta_rels[ri] transfers here;
 * all entries are consumed (freed or moved to a worker session).
 * After return, ctxs[w].delta_rels[ri] == NULL for all w, ri.
 *
 * Workers with no delta for a relation receive no $d$ entry, so
 * has_empty_forced_delta fires and skips that rule next sub-pass.
 */

/*
 * is_stratum_idb:
 * Returns true if the given relation name is an IDB in this stratum.
 */
static bool
is_stratum_idb(const wl_plan_stratum_t *sp, const char *name)
{
    if (!name)
        return false;
    for (uint32_t rj = 0; rj < sp->relation_count; rj++) {
        if (strcmp(name, sp->relations[rj].name) == 0)
            return true;
    }
    return false;
}

/*
 * ops_have_idb_idb_join:
 * Walk an op sequence tracking whether the eval stack top derives from IDB.
 * Returns true if any JOIN has BOTH IDB-derived left input AND IDB right.
 * VARIABLE resets the tracker; JOIN with IDB right propagates it.
 */
static bool
ops_have_idb_idb_join(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp)
{
    bool stack_has_idb = false;
    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE) {
            stack_has_idb = is_stratum_idb(sp, op->relation_name);
        } else if (op->op == WL_PLAN_OP_JOIN && op->right_relation) {
            bool right_idb = is_stratum_idb(sp, op->right_relation);
            if (right_idb && stack_has_idb)
                return true;
            if (right_idb)
                stack_has_idb = true;
        }
    }
    return false;
}

/*
 * ops_count_idb_body_atoms:
 * Walk an op sequence and count the number of distinct IDB relations
 * referenced as body atoms (VARIABLE loads + JOIN right_relations).
 * Extends the pattern from ops_have_idb_idb_join but returns a count.
 */
static uint32_t
ops_count_idb_body_atoms(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp)
{
    uint32_t count = 0;
    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE) {
            if (is_stratum_idb(sp, op->relation_name))
                count++;
        } else if (op->op == WL_PLAN_OP_JOIN && op->right_relation) {
            if (is_stratum_idb(sp, op->right_relation))
                count++;
        }
    }
    return count;
}

/*
 * stratum_max_idb_body_atoms:
 * Walk all relations in a stratum (including K_FUSION children) and
 * return the maximum number of IDB body atoms across all rules.
 * Used as a static guard: BDX mode is only correct for rules with
 * at most 2 IDB body atoms.
 */
uint32_t
stratum_max_idb_body_atoms(const wl_plan_stratum_t *sp)
{
    uint32_t max_count = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];

        /* Check top-level ops */
        uint32_t c = ops_count_idb_body_atoms(rel->ops, rel->op_count, sp);
        if (c > max_count)
            max_count = c;

        /* Check ops inside K_FUSION */
        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION
                && rel->ops[oi].opaque_data) {
                const wl_plan_op_k_fusion_t *kf =
                    (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
                for (uint32_t ki = 0; ki < kf->k; ki++) {
                    c = ops_count_idb_body_atoms(kf->k_ops[ki],
                            kf->k_op_counts[ki], sp);
                    if (c > max_count)
                        max_count = c;
                }
            }
        }
    }
    return max_count;
}

/*
 * tdd_stratum_has_idb_self_join:
 * Returns true if any rule in the stratum has a JOIN where BOTH the left
 * input (from VARIABLE or previous JOIN) AND the right_relation are IDB.
 * Only these true IDB-IDB joins (e.g. CSPA's valueFlow join valueFlow)
 * require full replication.  EDB-IDB joins (e.g. CRDT's insert join
 * nextSiblingAnc) work correctly with data partitioning (partition IDB,
 * replicate EDB).
 */
bool
tdd_stratum_has_idb_self_join(const wl_plan_stratum_t *sp)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];

        /* Check top-level ops */
        if (ops_have_idb_idb_join(rel->ops, rel->op_count, sp))
            return true;

        /* Check ops inside K_FUSION */
        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION
                && rel->ops[oi].opaque_data) {
                const wl_plan_op_k_fusion_t *kf =
                    (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
                for (uint32_t ki = 0; ki < kf->k; ki++) {
                    if (ops_have_idb_idb_join(kf->k_ops[ki],
                        kf->k_op_counts[ki], sp))
                        return true;
                }
            }
        }
    }
    return false;
}

/*
 * idb_idb_join_right_keys_match_exchange:
 * Walk an op sequence.  For each IDB-IDB JOIN found, verify that every
 * right_key column name resolves to a column index that is listed in the
 * EXCHANGE key_col_idxs of the right relation.
 *
 * Returns false as soon as any IDB-IDB JOIN is found whose right_keys do
 * NOT match the EXCHANGE partition key — meaning cross-partition joins would
 * occur and asymmetric init is unsafe.
 *
 * Returns true if every IDB-IDB JOIN in this op sequence is exchange-aligned
 * (or if there are no IDB-IDB JOINs at all).
 */
static bool
idb_idb_join_right_keys_match_exchange(const wl_plan_op_t *ops,
    uint32_t op_count, const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    bool stack_has_idb = false;
    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE) {
            stack_has_idb = is_stratum_idb(sp, op->relation_name);
        } else if (op->op == WL_PLAN_OP_JOIN && op->right_relation) {
            bool right_idb = is_stratum_idb(sp, op->right_relation);
            if (right_idb && stack_has_idb) {
                /* Found an IDB-IDB join.  For asymmetric partition-replicate to
                 * be correct, BOTH the left join key AND the right join key
                 * must equal the EXCHANGE partition key.  If left_keys and
                 * right_keys are both "col0" (e.g. vA:-vF(z,x),vF(z,y)),
                 * each worker's partition is self-contained.  If left_key
                 * is "col1" and right_key is "col0" (e.g. TC r:-r(x,y),r(y,z)),
                 * cross-partition joins are needed and replication is required.
                 */
                if (!op->right_keys || !op->left_keys || op->key_count == 0)
                    return false;

                /* Find the EXCHANGE key for the right relation. */
                const uint32_t *xkey = NULL;
                uint32_t xkey_count = 0;
                for (uint32_t rj = 0; rj < sp->relation_count; rj++) {
                    if (strcmp(sp->relations[rj].name, op->right_relation) != 0)
                        continue;
                    for (uint32_t oj = 0; oj < sp->relations[rj].op_count;
                        oj++) {
                        if (sp->relations[rj].ops[oj].op ==
                            WL_PLAN_OP_EXCHANGE) {
                            const wl_plan_op_exchange_t *meta =
                                (const wl_plan_op_exchange_t *)
                                sp->relations[rj].ops[oj].opaque_data;
                            if (meta && meta->key_col_count > 0) {
                                xkey = meta->key_col_idxs;
                                xkey_count = meta->key_col_count;
                            }
                            break;
                        }
                    }
                    break;
                }

                if (!xkey || xkey_count == 0)
                    return false; /* No EXCHANGE key: cannot verify alignment */
                if (xkey_count != op->key_count)
                    return false; /* Key count mismatch */

                /* Resolve right_keys (column names) to indices via the
                 * coordinator's relation schema, then compare to xkey.
                 * Also verify left_keys against the same xkey: for the join
                 * to be fully local, BOTH left and right join keys must match
                 * the EXCHANGE partition key.  Example: for TC r:-r(x,y),r(y,z)
                 * right_key="col0" matches but left_key="col1" does not, so
                 * workers would need cross-partition data. */
                col_rel_t *rrel = session_find_rel(coord, op->right_relation);
                if (!rrel || !rrel->col_names || rrel->ncols == 0)
                    return false; /* No schema: cannot verify */

                for (uint32_t k = 0; k < op->key_count; k++) {
                    /* Check right_key against EXCHANGE key */
                    const char *rkname = op->right_keys[k];
                    uint32_t rcidx = UINT32_MAX;
                    for (uint32_t c = 0; c < rrel->ncols; c++) {
                        if (rrel->col_names[c]
                            && strcmp(rrel->col_names[c], rkname) == 0) {
                            rcidx = c;
                            break;
                        }
                    }
                    if (rcidx == UINT32_MAX)
                        return false; /* Right column not found */
                    bool rfound = false;
                    for (uint32_t xk = 0; xk < xkey_count; xk++) {
                        if (xkey[xk] == rcidx) {
                            rfound = true;
                            break;
                        }
                    }
                    if (!rfound)
                        return false; /* Right join key not in EXCHANGE key */

                    /* Check left_key against xkey (using same column index
                     * space — left relation is the same IDB, same schema). */
                    const char *lkname = op->left_keys[k];
                    uint32_t lcidx = UINT32_MAX;
                    for (uint32_t c = 0; c < rrel->ncols; c++) {
                        if (rrel->col_names[c]
                            && strcmp(rrel->col_names[c], lkname) == 0) {
                            lcidx = c;
                            break;
                        }
                    }
                    if (lcidx == UINT32_MAX)
                        return false; /* Left column not found */
                    bool lfound = false;
                    for (uint32_t xk = 0; xk < xkey_count; xk++) {
                        if (xkey[xk] == lcidx) {
                            lfound = true;
                            break;
                        }
                    }
                    if (!lfound)
                        return false; /* Left join key not in EXCHANGE key */
                }
            }
            if (right_idb)
                stack_has_idb = true;
        }
    }
    return true;
}

/*
 * tdd_stratum_idb_self_join_exchange_aligned:
 * Returns true if the stratum has IDB self-joins AND all of them are
 * exchange-aligned (right_keys match the EXCHANGE partition key).
 *
 * When true, the stratum can use asymmetric partition-replicate:
 * each worker holds 1/W of the IDB (partitioned by EXCHANGE key),
 * and the delta is broadcast.  Joins are fully local because the join
 * key == partition key on both sides.
 *
 * When false (join key differs from partition key, e.g. transitive closure
 * r(x,z):-r(x,y),r(y,z) where join is on col1=col0), cross-partition joins
 * would occur with partitioned IDB, so replicate_mode must be used instead.
 */
bool
tdd_stratum_idb_self_join_exchange_aligned(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    if (!tdd_stratum_has_idb_self_join(sp))
        return false;

    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];

        if (!idb_idb_join_right_keys_match_exchange(
                rel->ops, rel->op_count, sp, coord))
            return false;

        /* Check inside K_FUSION */
        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION
                && rel->ops[oi].opaque_data) {
                const wl_plan_op_k_fusion_t *kf =
                    (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
                for (uint32_t ki = 0; ki < kf->k; ki++) {
                    if (!idb_idb_join_right_keys_match_exchange(
                            kf->k_ops[ki], kf->k_op_counts[ki], sp, coord))
                        return false;
                }
            }
        }
    }
    return true;
}

/*
 * tdd_init_workers_hybrid:
 * Hybrid initialization for data-partitioned strata.
 *
 * IDB relations (those in sp->relations[]) are partitioned across workers
 * by their EXCHANGE key columns.  Non-IDB relations (EDB, earlier-stratum
 * derived) are replicated to every worker.
 *
 * This gives each worker ~1/W of the IDB while ensuring complete join
 * coverage: IDB-EDB joins are always complete because EDB is replicated.
 * The hash-partitioned delta exchange maintains the partition invariant.
 */
static int
tdd_init_workers_hybrid(const wl_plan_stratum_t *sp, wl_col_session_t *coord)
{
    tdd_cleanup_workers(coord);

    uint32_t W = coord->num_workers;
    uint32_t nrels = coord->nrels;

    if (nrels == 0) {
        for (uint32_t w = 0; w < W; w++) {
            int rc = col_worker_session_create(coord, w, NULL, 0,
                    &coord->tdd_workers[w]);
            if (rc != 0) {
                tdd_cleanup_workers(coord);
                return rc;
            }
            coord->tdd_workers_count = w + 1;
        }
        return 0;
    }

    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_rels)
        return ENOMEM;

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        worker_rels[w] = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
        if (!worker_rels[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)worker_rels[j]);
            free((void *)worker_rels);
            return ENOMEM;
        }
    }

    /* Pre-scan stratum EXCHANGE ops for EDB partition info.
     * If an EXCHANGE specifies edb_rel_name + edb_key_col_idxs, we can
     * hash-partition that EDB instead of replicating it, reducing each
     * worker's scan from O(|EDB|) to O(|EDB|/W). */
    const char *edb_part_name = NULL;
    const uint32_t *edb_part_keys = NULL;
    uint32_t edb_part_key_count = 0;

    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta =
                    (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->edb_rel_name
                    && meta->edb_key_col_idxs
                    && meta->edb_key_col_count > 0) {
                    edb_part_name = meta->edb_rel_name;
                    edb_part_keys = meta->edb_key_col_idxs;
                    edb_part_key_count = meta->edb_key_col_count;
                }
                break;
            }
        }
        if (edb_part_name)
            break;
    }

    uint32_t rels_built = 0;

    for (uint32_t r = 0; r < nrels && rc == 0; r++) {
        col_rel_t *rel = coord->rels[r];
        if (!rel)
            continue;

        const char *name = rel->name;

        bool is_idb = false;
        const uint32_t *exchange_key = NULL;
        uint32_t exchange_key_count = 0;

        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            if (strcmp(name, sp->relations[ri].name) != 0)
                continue;
            is_idb = true;
            for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
                if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                    const wl_plan_op_exchange_t *meta =
                        (const wl_plan_op_exchange_t *)
                        sp->relations[ri].ops[oi].opaque_data;
                    if (meta && meta->key_col_count > 0) {
                        exchange_key = meta->key_col_idxs;
                        exchange_key_count = meta->key_col_count;
                    }
                    break;
                }
            }
            break;
        }

        if (is_idb && rel->nrows > 0 && rel->ncols > 0) {
            uint32_t default_key[] = { 0 };
            const uint32_t *key = (exchange_key && exchange_key_count > 0)
                ? exchange_key : default_key;
            uint32_t key_count = (exchange_key && exchange_key_count > 0)
                ? exchange_key_count : 1u;

            col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
            if (!parts) {
                rc = ENOMEM;
            } else {
                rc = col_rel_partition_by_key(rel, key, key_count, W, parts);
                if (rc == 0) {
                    for (uint32_t w = 0; w < W && rc == 0; w++) {
                        free(parts[w]->name);
                        parts[w]->name = wl_strdup(name);
                        if (!parts[w]->name) {
                            rc = ENOMEM;
                        } else {
                            /* Init hash-set dedup for O(1) consolidation. */
                            dedup_set_init_from_rel(parts[w]);
                            worker_rels[w][rels_built] = parts[w];
                            parts[w] = NULL;
                        }
                    }
                }
                for (uint32_t w = 0; w < W; w++)
                    col_rel_destroy(parts[w]);
                free(parts);
            }
        } else if (edb_part_name && rel->nrows > 0
            && strcmp(name, edb_part_name) == 0
            && edb_part_key_count > 0
            && rel->ncols > 0) {
            /* EDB partitioning: hash-partition this EDB by the join key
             * so each worker scans only ~1/W of the rows.  The partition
             * key (edb_part_keys) matches the IDB exchange key through
             * the join condition, ensuring local join completeness. */
            col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
            if (!parts) {
                rc = ENOMEM;
            } else {
                rc = col_rel_partition_by_key(rel, edb_part_keys,
                        edb_part_key_count, W, parts);
                if (rc == 0) {
                    for (uint32_t w = 0; w < W && rc == 0; w++) {
                        free(parts[w]->name);
                        parts[w]->name = wl_strdup(name);
                        if (!parts[w]->name) {
                            rc = ENOMEM;
                        } else {
                            worker_rels[w][rels_built] = parts[w];
                            parts[w] = NULL;
                        }
                    }
                }
                for (uint32_t w = 0; w < W; w++)
                    col_rel_destroy(parts[w]);
                free(parts);
            }
        } else {
            /* Zero-copy EDB sharing: workers borrow the coordinator's
             * column buffers instead of deep-copying.  The coordinator
             * relation outlives the workers, so borrowing is safe.
             * This eliminates W× memory duplication and cache thrashing. */
            for (uint32_t w = 0; w < W && rc == 0; w++) {
                col_rel_t *view = col_rel_new_auto(name, rel->ncols);
                if (!view) {
                    rc = ENOMEM;
                    break;
                }
                if (rel->nrows > 0) {
                    view->col_shared = (bool *)calloc(rel->ncols, sizeof(bool));
                    if (!view->col_shared) {
                        /* Fallback: deep copy on alloc failure */
                        rc = col_rel_append_all(view, rel, NULL);
                        if (rc != 0) {
                            col_rel_destroy(view);
                            break;
                        }
                    } else {
                        for (uint32_t c = 0; c < rel->ncols; c++) {
                            free(view->columns[c]);
                            view->columns[c] = rel->columns[c];
                            view->col_shared[c] = true;
                        }
                        view->nrows = rel->nrows;
                        view->capacity = rel->capacity;
                        view->sorted_nrows = rel->sorted_nrows;
                    }
                }
                /* Init hash-set dedup for empty IDB workers so
                 * consolidation uses O(D) instead of O(N) merge. */
                if (is_idb && rel->nrows == 0)
                    dedup_set_init_from_rel(view);
                worker_rels[w][rels_built] = view;
            }
        }

        if (rc == 0)
            rels_built++;
    }

    uint32_t created = 0;
    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            rc = col_worker_session_create(coord, w,
                    worker_rels[w], rels_built, &coord->tdd_workers[w]);
            if (rc != 0)
                break;
            created++;
        }
    }

    if (rc != 0) {
        for (uint32_t w = created; w < W; w++) {
            for (uint32_t p = 0; p < rels_built; p++)
                col_rel_destroy(worker_rels[w][p]);
        }
        coord->tdd_workers_count = created;
        tdd_cleanup_workers(coord);
    } else {
        coord->tdd_workers_count = W;
    }

    for (uint32_t w = 0; w < W; w++)
        free((void *)worker_rels[w]);
    free((void *)worker_rels);

    return rc;
}

static int
tdd_broadcast_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W)
{
    uint32_t nrels = sp->relation_count;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);

        /* Count total rows and find ncols */
        uint32_t total_rows = 0;
        uint32_t ncols = 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            if (d && d->nrows > 0) {
                total_rows += d->nrows;
                if (ncols == 0)
                    ncols = d->ncols;
            }
        }

        /* Issue #361: Reuse pre-installed $d$ on workers when available.
         * Avoids per-iteration alloc/free/session_remove/session_add. */
        if (total_rows == 0) {
            for (uint32_t w = 0; w < W; w++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
                /* Existing $d$ already cleared by worker (nrows=0) */
            }
            continue;
        }

        /* Build union delta: try reusing worker 0's existing $d$ as union buf */
        col_rel_t *union_d = NULL;
        bool union_from_session = false;
        col_rel_t *slot0 = session_find_rel(&coord->tdd_workers[0], dname);
        if (slot0 && slot0->ncols == ncols) {
            /* Reuse pre-installed $d$ on worker 0 as union buffer */
            slot0->nrows = 0;
            union_d = slot0;
            union_from_session = true;
        } else {
            union_d = col_rel_new_auto(dname, ncols);
            if (!union_d) {
                for (uint32_t w = 0; w < W; w++) {
                    col_rel_destroy(ctxs[w].delta_rels[ri]);
                    ctxs[w].delta_rels[ri] = NULL;
                }
                return ENOMEM;
            }
        }

        int append_rc = 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            ctxs[w].delta_rels[ri] = NULL;
            if (!d) continue;
            if (append_rc == 0 && d->nrows > 0)
                append_rc = col_rel_append_all(union_d, d, NULL);
            col_rel_destroy(d);
        }

        if (append_rc != 0) {
            if (!union_from_session)
                col_rel_destroy(union_d);
            return append_rc;
        }

        /* Issue #388: Dedup broadcast union to prevent W-fold amplification.
         * In replicate mode, all W workers derive the same tuples from
         * identical data. Without dedup, the union contains W copies of
         * each new tuple, compounding exponentially across iterations. */
        if (union_d->nrows > 1)
            tdd_dedup_rel(union_d);

        /* Issue #396: zero-copy broadcast via col_shared.
         * Anchor union_d in worker 0's session so its lifetime covers all
         * workers' reads during the next sub-pass.  Workers 1..W-1 borrow
         * union_d's column pointers via col_rel_install_shared_view (O(ncols)
         * pointer setup) instead of O(|delta|) deep copies. */
        if (!union_from_session) {
            /* Install union_d into worker 0's session as the authoritative $d$.
            * session_add_rel replaces any existing entry with the same name. */
            int rc = session_add_rel(&coord->tdd_workers[0], union_d);
            if (rc != 0) {
                col_rel_destroy(union_d);
                return rc;
            }
            union_from_session = true;
            /* slot0 now points to union_d (owned by worker 0's session) */
        }
        /* worker 0 already holds union_d; install shared views on workers 1..W-1 */
        for (uint32_t w = 1; w < W; w++) {
            col_rel_t *worker_d = session_find_rel(
                &coord->tdd_workers[w], dname);
            if (worker_d && worker_d->ncols == ncols) {
                /* Reuse: install shared view (O(ncols) pointer setup) */
                int rc = col_rel_install_shared_view(worker_d, union_d);
                if (rc != 0) {
                    /* Fallback: deep copy on shared-view alloc failure */
                    worker_d->nrows = 0;
                    rc = col_rel_append_all(worker_d, union_d, NULL);
                    if (rc != 0)
                        return rc;
                }
            } else {
                /* First iteration or schema mismatch: create new relation */
                col_rel_t *new_d = col_rel_new_auto(dname, ncols);
                if (!new_d)
                    return ENOMEM;
                int rc = col_rel_install_shared_view(new_d, union_d);
                if (rc != 0) {
                    /* Fallback: deep copy on shared-view alloc failure */
                    rc = col_rel_append_all(new_d, union_d, NULL);
                    if (rc != 0) {
                        col_rel_destroy(new_d);
                        return rc;
                    }
                }
                rc = session_add_rel(&coord->tdd_workers[w], new_d);
                if (rc != 0) {
                    col_rel_destroy(new_d);
                    return rc;
                }
            }
        }
        /* union_d is owned by worker 0's session; no explicit free needed */
        (void)union_from_session;
    }

    return 0;
}

/*
 * tdd_record_recursive_convergence:
 * After recursive fixed-point convergence, record stratum and per-rule
 * frontiers on the coordinator.  Mirrors eval.c:768-791 for the TDD path.
 */
static void
tdd_record_recursive_convergence(wl_col_session_t *coord,
    const wl_plan_stratum_t *sp, uint32_t stratum_idx,
    uint32_t rule_id_base, uint32_t final_eff_iter)
{
    uint32_t nrels = sp->relation_count;

    /* Per-rule frontier (eval.c:771-775) */
    for (uint32_t ri = 0; ri < nrels && rule_id_base + ri < MAX_RULES; ri++) {
        coord->frontier_ops->record_rule_convergence(coord,
            rule_id_base + ri, coord->outer_epoch, final_eff_iter);
    }

    /* Stratum frontier (eval.c:780-782) */
    coord->frontier_ops->record_stratum_convergence(coord,
        stratum_idx, coord->outer_epoch, final_eff_iter);
}

/*
 * tdd_record_nonrecursive_convergence:
 * After non-recursive stratum dispatch, record stratum and per-rule frontiers.
 * Non-recursive strata always converge in one step; uses UINT32_MAX sentinel.
 * Mirrors eval.c:247-279 for the TDD coordinator path.
 */
static void
tdd_record_nonrecursive_convergence(wl_col_session_t *coord,
    const wl_plan_stratum_t *sp, uint32_t stratum_idx)
{
    /* Stratum frontier: UINT32_MAX sentinel (eval.c:252-253) */
    coord->frontier_ops->record_stratum_convergence(coord,
        stratum_idx, coord->outer_epoch, UINT32_MAX);

    /* Per-rule frontiers (eval.c:265-279) */
    if (coord->plan) {
        uint32_t rule_base = 0;
        for (uint32_t si = 0; si < stratum_idx; si++)
            rule_base += coord->plan->strata[si].relation_count;
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            uint32_t rule_idx = rule_base + ri;
            if (rule_idx < MAX_RULES)
                coord->frontier_ops->reset_rule_frontier(coord, rule_idx,
                    coord->outer_epoch);
        }
    }
}

/*
 * tdd_check_convergence:
 * Returns true if global fixed-point reached: no worker produced new tuples.
 * Called after each sub-pass barrier, before the exchange step.
 */
static bool
tdd_check_convergence(const col_eval_tdd_worker_ctx_t *ctxs, uint32_t W)
{
    for (uint32_t w = 0; w < W; w++) {
        if (ctxs[w].any_new)
            return false;
    }
    return true;
}

/*
 * bdx_row_cmp:
 * Compare row i from relation a with row j from relation b.
 * Both must have the same ncols.  Returns <0, 0, or >0.
 */
static inline int
bdx_row_cmp(const col_rel_t *a, uint32_t i,
    const col_rel_t *b, uint32_t j, uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        int64_t va = a->columns[c][i];
        int64_t vb = b->columns[c][j];
        if (va < vb) return -1;
        if (va > vb) return 1;
    }
    return 0;
}

/*
 * bdx_merge_diff:
 * Given sorted+deduped 'delta' and sorted+deduped 'base', remove from
 * delta any row that also appears in base.  Operates in-place on delta.
 * Both delta and base must be sorted by the same column order (radix sort).
 */
static void
bdx_merge_diff(col_rel_t *delta, const col_rel_t *base)
{
    if (!delta || delta->nrows == 0 || !base || base->nrows == 0)
        return;
    uint32_t ncols = delta->ncols;
    uint32_t di = 0, bi = 0, wr = 0;
    while (di < delta->nrows && bi < base->nrows) {
        int cmp = bdx_row_cmp(delta, di, base, bi, ncols);
        if (cmp < 0) {
            /* delta[di] < base[bi]: new row, keep it */
            if (wr != di) {
                for (uint32_t c = 0; c < ncols; c++)
                    delta->columns[c][wr] = delta->columns[c][di];
            }
            wr++;
            di++;
        } else if (cmp > 0) {
            bi++;
        } else {
            /* duplicate: skip */
            di++;
            bi++;
        }
    }
    /* Remaining delta rows are all > any base row: keep them */
    while (di < delta->nrows) {
        if (wr != di) {
            for (uint32_t c = 0; c < ncols; c++)
                delta->columns[c][wr] = delta->columns[c][di];
        }
        wr++;
        di++;
    }
    delta->nrows = wr;
}

/*
 * tdd_bdx_exchange_deltas:
 * Broadcast-Delta with Hash-Exchange Output (BDX) for Category C strata
 * (non-exchange-aligned IDB-IDB joins with <= 2 IDB body atoms).
 *
 * Algorithm per iteration:
 *   1. Union all worker deltas per relation into combined_delta
 *   2. Dedup combined_delta against coordinator's accumulated IDB
 *   3. Append combined_delta to coordinator IDB (monotonic growth)
 *   4. Truncate worker IDB to pre-subpass snapshot (remove pollution)
 *   5. Hash-partition combined_delta by EXCHANGE key -> append to workers
 *   6. Update snap after hash-exchange
 *   7. Insert hashes into worker dedup_sets
 *   8. Broadcast combined_delta as $d$ via col_shared zero-copy views
 *
 * snap[w * nrels + ri] holds the pre-subpass IDB nrows for truncation.
 * Ownership of ctxs[w].delta_rels[ri] entries transfers here.
 */
static int
tdd_bdx_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    uint32_t *snap)
{
    uint32_t nrels = sp->relation_count;
    int rc = 0;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        const char *rel_name = sp->relations[ri].name;

        /* Remove stale $d$ from workers */
        for (uint32_t w = 0; w < W; w++)
            session_remove_rel(&coord->tdd_workers[w], dname);

        /* Step 1: Union all worker deltas */
        uint32_t total = 0, ncols = 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            if (d && d->nrows > 0) {
                total += d->nrows;
                if (ncols == 0)
                    ncols = d->ncols;
            }
        }

        if (total == 0) {
            for (uint32_t w = 0; w < W; w++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
            }
            continue;
        }

        col_rel_t *combined = col_rel_new_auto(dname, ncols);
        if (!combined)
            return ENOMEM;

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            ctxs[w].delta_rels[ri] = NULL;
            if (d && d->nrows > 0)
                rc = col_rel_append_all(combined, d, NULL);
            col_rel_destroy(d);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }

        /* Step 2: Dedup combined within itself, then merge-diff against
         * coordinator's accumulated IDB to keep only truly new rows. */
        if (combined->nrows > 1)
            tdd_dedup_rel(combined);

        col_rel_t *coord_idb = session_find_rel(coord, rel_name);
        if (coord_idb && coord_idb->nrows > 0 && combined->nrows > 0)
            bdx_merge_diff(combined, coord_idb);

        if (combined->nrows == 0) {
            col_rel_destroy(combined);
            continue;
        }

        /* Step 3: Append combined_delta to coordinator IDB */
        if (coord_idb) {
            if (coord_idb->ncols == 0 && combined->ncols > 0) {
                rc = col_rel_set_schema(coord_idb, combined->ncols,
                        (const char *const *)combined->col_names);
                if (rc != 0) {
                    col_rel_destroy(combined);
                    return rc;
                }
            }
            /* Phase 0 optimization (#406): Use sorted merge-append instead of
             * append-then-sort. Since combined_delta is already deduped
             * against coord_idb, merge-append maintains sort order in O(N+D)
             * time instead of O(N log N) full re-sort. */
            rc = tdd_sorted_merge_append(coord_idb, combined);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            /* Dedup any duplicate rows from merge (safety check).
             * With presort check, this becomes O(N) if already sorted. */
            if (coord_idb->nrows > 1)
                tdd_dedup_rel(coord_idb);
        }

        /* Step 4: Truncate worker IDB to pre-subpass snapshot */
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *widb = session_find_rel(
                &coord->tdd_workers[w], rel_name);
            if (widb)
                widb->nrows = snap[w * nrels + ri];
        }

        /* Step 5: Hash-partition combined_delta by EXCHANGE key,
         * append to correct worker's IDB */
        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta
                    = (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->key_col_count > 0) {
                    key_cols = meta->key_col_idxs;
                    key_count = meta->key_col_count;
                }
                break;
            }
        }
        uint32_t default_key[] = { 0 };
        if (!key_cols || key_count == 0) {
            key_cols = default_key;
            key_count = 1;
        }

        col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!parts) {
            col_rel_destroy(combined);
            return ENOMEM;
        }
        rc = col_rel_partition_by_key(combined, key_cols, key_count, W, parts);
        if (rc != 0) {
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free(parts);
            col_rel_destroy(combined);
            return rc;
        }

        for (uint32_t w = 0; w < W; w++) {
            if (parts[w] && parts[w]->nrows > 0) {
                col_rel_t *widb = session_find_rel(
                    &coord->tdd_workers[w], rel_name);
                if (widb) {
                    rc = col_rel_append_all(widb, parts[w], NULL);
                    /* Step 7: Insert hashes of new rows into dedup_set */
                    if (rc == 0) {
                        for (uint32_t row = 0; row < parts[w]->nrows;
                            row++) {
                            uint64_t h = dedup_row_hash(parts[w], row);
                            dedup_set_insert(widb, h);
                        }
                    }
                }
            }
            /* Step 6: Update snap after hash-exchange */
            col_rel_t *widb = session_find_rel(
                &coord->tdd_workers[w], rel_name);
            snap[w * nrels + ri] = widb ? widb->nrows : 0;
            col_rel_destroy(parts[w]);
        }
        free(parts);

        /* Step 8: Broadcast combined_delta as $d$ via col_shared zero-copy */
        rc = session_add_rel(&coord->tdd_workers[0], combined);
        if (rc != 0) {
            col_rel_destroy(combined);
            return rc;
        }
        /* combined is now owned by worker 0's session */
        for (uint32_t dst = 1; dst < W; dst++) {
            col_rel_t *view = col_rel_new_auto(dname, ncols);
            if (!view)
                return ENOMEM;
            rc = col_rel_install_shared_view(view, combined);
            if (rc != 0) {
                rc = col_rel_append_all(view, combined, NULL);
                if (rc != 0) {
                    col_rel_destroy(view);
                    return rc;
                }
            }
            rc = session_add_rel(&coord->tdd_workers[dst], view);
            if (rc != 0) {
                col_rel_destroy(view);
                return rc;
            }
        }
    }

    return 0;
}

/*
 * col_eval_stratum_tdd_recursive:
 * Coordinator-driven semi-naive fixed-point for recursive strata.
 *
 * Pipeline per sub-pass:
 *   DISPATCH (W workers via workqueue) → BARRIER → CONVERGENCE CHECK
 *   → EXCHANGE (hash/broadcast/BDX depending on mode) → next sub-pass
 *
 * After convergence, merges worker IDB into coordinator and deduplicates.
 * Broadcast exchange may produce the same derived tuple on multiple workers
 * (when multiple equal-length paths lead to the same conclusion); the final
 * sort+dedup step removes these.
 */
static int
col_eval_stratum_tdd_recursive(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx)
{
    uint32_t W = coord->num_workers;
    uint32_t nrels = sp->relation_count;
    int rc = 0;

    /* Pre-register empty IDB relations on coordinator (eval.c:291-304) */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (session_find_rel(coord, sp->relations[ri].name))
            continue;
        col_rel_t *empty = NULL;
        int alloc_rc = col_rel_alloc(&empty, sp->relations[ri].name);
        if (alloc_rc != 0)
            return ENOMEM;
        alloc_rc = session_add_rel(coord, empty);
        if (alloc_rc != 0) {
            col_rel_destroy(empty);
            return alloc_rc;
        }
    }

    /* Issue #350: On incremental steps (new EDB inserted), clear pre-existing
     * IDB rows so workers recompute from scratch.  Without this, stale IDB
     * partitioned by col0 prevents cross-partition recursive joins
     * (e.g. tc(1,3) on worker 1 cannot join edge(3,4) on worker 3).
     * Also reset stratum frontier so should_skip_iteration does not skip
     * iterations beyond the previous step's convergence point.
     * When no new EDB was inserted, skip clearing to preserve frontier skip.
     * Issue #372: Skip clearing on first snapshot (total_iterations == 0):
     * IDB relations may hold EDB seeds (e.g. r(1,2) for r(x,z):-r(x,y),r(y,z))
     * that must survive into the first evaluation pass. */
    if (coord->last_inserted_relation != NULL
        && coord->total_iterations > 0) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(coord, sp->relations[ri].name);
            if (r && r->nrows > 0) {
                r->nrows = 0;
                col_session_invalidate_arrangements(&coord->base,
                    sp->relations[ri].name);
            }
        }
        coord->frontier_ops->reset_stratum_frontier(coord, stratum_idx,
            coord->outer_epoch);
    }

    /* Issue #361, #372, #390: Determine init strategy:
     *
     *   self_join_mode (asymmetric partition-replicate): IDB self-join strata
     *   where the join key == EXCHANGE partition key on both sides (e.g. CSPA
     *   valueAlias: vA(x,y):-vF(z,x),vF(z,y) joins on col0 which is the
     *   EXCHANGE key).  Workers hold 1/W of the IDB; delta is broadcast.
     *   Each join is fully local because the join key equals the partition key.
     *
     *   bdx_mode (broadcast-delta with hash-exchange): IDB self-join strata
     *   where the join key != EXCHANGE key but rules have at most 2 IDB body
     *   atoms.  Workers hold 1/W of IDB (hybrid init), combined delta is
     *   deduped against coordinator IDB, hash-exchanged to workers, and
     *   broadcast as $d$.
     *
     *   replicate_mode (full replication): used when:
     *     - No new EDB inserted (frontier-skip path), OR
     *     - Stratum has IDB self-join with >2 IDB body atoms (BDX unsafe).
     *
     * Issue #388: Optional W=1 fallback for replicate mode only. */
    bool self_join_mode = tdd_stratum_idb_self_join_exchange_aligned(sp, coord);
    bool bdx_mode = tdd_stratum_has_idb_self_join(sp) && !self_join_mode;
    bool replicate_mode = (coord->last_inserted_relation == NULL)
        || (bdx_mode && stratum_max_idb_body_atoms(sp) > 2);
    bdx_mode = bdx_mode && !replicate_mode;
    if (replicate_mode) {
        const char *env = getenv("WIRELOG_TDD_REPLICATE_W1");
        if (env && env[0] == '1')
            W = 1;
    }
    if (replicate_mode)
        rc = tdd_replicate_workers(coord);
    else
        rc = tdd_init_workers_hybrid(sp, coord);
    if (rc != 0)
        return rc;

    /* Pre-register empty IDB on each worker */
    rc = tdd_preregister_idb_on_workers(sp, coord);
    if (rc != 0) {
        tdd_cleanup_workers(coord);
        return rc;
    }

    /* Sort pre-existing IDB data on workers (eval.c:324-329) */
    for (uint32_t w = 0; w < W; w++) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(&coord->tdd_workers[w],
                    sp->relations[ri].name);
            if (r && r->nrows > 1)
                col_rel_radix_sort_int64(r);
        }
    }

    /* Issue #361: Pre-install empty $d$ delta relations on each worker.
     * Persistent across iterations — worker clears (nrows=0) instead of
     * session_remove_rel, and broadcast refills instead of create+add.
     * Eliminates per-iteration alloc/free/session-ops (14k iters for CRDT). */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        col_rel_t *coord_rel = session_find_rel(coord,
                sp->relations[ri].name);
        uint32_t ncols_ri = coord_rel ? coord_rel->ncols : 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *slot = col_rel_new_auto(dname, ncols_ri);
            if (!slot) {
                tdd_cleanup_workers(coord);
                return ENOMEM;
            }
            rc = session_add_rel(&coord->tdd_workers[w], slot);
            if (rc != 0) {
                col_rel_destroy(slot);
                tdd_cleanup_workers(coord);
                return rc;
            }
        }
    }

    /* Phase 4: Frontier Initialization (eval.c:336)
     * Initialize per-stratum frontier tracking for convergence detection.
     * Frontier records the iteration at which each stratum converged
     * (fixed-point reached with no new tuples).
     */
    coord->frontier_ops->init_stratum(coord, stratum_idx);

    /* Compute rule_id_base for per-rule frontier recording
     * Each rule (IDB relation) gets a unique frontier slot indexed by
     * rule_id_base + relation_index within stratum.
     */
    uint32_t rule_id_base = 0;
    if (coord->plan) {
        for (uint32_t si = 0;
            si < stratum_idx && si < coord->plan->stratum_count; si++) {
            rule_id_base += coord->plan->strata[si].relation_count;
        }
    }
    if (rule_id_base >= MAX_RULES)
        rule_id_base = MAX_RULES;

    uint32_t final_eff_iter = 0;
    bool saved_diff = coord->diff_operators_active;

    /* Phase 4: Iteration Loop Control
     * Semi-naive fixed-point computation with two nested loops:
     *  - outer loop: tracks global convergence across all sub-passes
     *  - inner sub loop: one EVAL_STRIDE sub-iteration per outer iteration
     *
     * Terminates when:
     *  1. Fixed-point reached: no worker produced new tuples in iteration
     *  2. Coordinator-level frontier skip: iteration > stratum frontier
     *
     * Each iteration:
     *  - DISPATCH W workers to evaluate sub-pass
     *  - BARRIER to wait for all workers
     *  - CONVERGENCE CHECK: if all workers have empty delta → fixed point
     *  - EXCHANGE: broadcast/hash-partition deltas to next iteration
     */

    /* Issue #361: Pre-allocate worker contexts and delta_rels arrays once.
     * Reuse across iterations to avoid calloc/free overhead per sub-pass
     * (~14k iterations for CRDT). */
    col_eval_tdd_worker_ctx_t *ctxs
        = (col_eval_tdd_worker_ctx_t *)calloc(
            W, sizeof(col_eval_tdd_worker_ctx_t));
    if (!ctxs) {
        rc = ENOMEM;
        goto done;
    }
    for (uint32_t w = 0; w < W; w++) {
        ctxs[w].delta_rels = (col_rel_t **)calloc(
            nrels, sizeof(col_rel_t *));
        if (!ctxs[w].delta_rels) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)ctxs[j].delta_rels);
            free(ctxs);
            ctxs = NULL;
            rc = ENOMEM;
            goto done;
        }
    }

    /* Issue #390: BDX snap array — pre-subpass IDB sizes per worker/relation.
     * Used to truncate worker IDB back to clean partition state after each
     * sub-pass (removes cross-partition pollution from join output). */
    uint32_t *bdx_snap = NULL;
    if (bdx_mode) {
        bdx_snap = (uint32_t *)calloc(
            (size_t)W * nrels, sizeof(uint32_t));
        if (!bdx_snap) {
            rc = ENOMEM;
            goto done;
        }
        /* Initialize snap from current worker IDB sizes */
        for (uint32_t w = 0; w < W; w++) {
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *widb = session_find_rel(
                    &coord->tdd_workers[w], sp->relations[ri].name);
                bdx_snap[w * nrels + ri] = widb ? widb->nrows : 0;
            }
        }
        /* Initialize coordinator IDB from worker partitions for dedup.
        * Coordinator needs a sorted copy of all IDB for merge-diff. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *cidb = session_find_rel(coord,
                    sp->relations[ri].name);
            if (cidb)
                cidb->nrows = 0;
            for (uint32_t w = 0; w < W; w++) {
                col_rel_t *widb = session_find_rel(
                    &coord->tdd_workers[w], sp->relations[ri].name);
                if (widb && widb->nrows > 0 && cidb) {
                    if (cidb->ncols == 0 && widb->ncols > 0) {
                        rc = col_rel_set_schema(cidb, widb->ncols,
                                (const char *const *)widb->col_names);
                        if (rc != 0) {
                            free(bdx_snap);
                            bdx_snap = NULL;
                            goto done;
                        }
                    }
                    rc = col_rel_append_all(cidb, widb, NULL);
                    if (rc != 0) {
                        free(bdx_snap);
                        bdx_snap = NULL;
                        goto done;
                    }
                }
            }
            if (cidb && cidb->nrows > 1)
                tdd_dedup_rel(cidb);
        }
        /* Pre-seed $d$ with full initial IDB on all workers.
         * BDX forces diff from eff_iter 0, so K_FUSION uses the broadcast
         * delta (complete IDB) rather than the local partition self-join
         * which would be incomplete for non-aligned join keys. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *cidb = session_find_rel(coord,
                    sp->relations[ri].name);
            if (!cidb || cidb->nrows == 0)
                continue;
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s",
                sp->relations[ri].name);
            uint32_t ncols = cidb->ncols;
            /* Install full IDB as $d$ on worker 0 */
            col_rel_t *d0 = session_find_rel(&coord->tdd_workers[0], dname);
            if (d0 && d0->ncols == ncols) {
                d0->nrows = 0;
                rc = col_rel_append_all(d0, cidb, NULL);
            } else {
                d0 = col_rel_new_auto(dname, ncols);
                if (!d0) {
                    rc = ENOMEM; break;
                }
                rc = col_rel_append_all(d0, cidb, NULL);
                if (rc == 0)
                    rc = session_add_rel(&coord->tdd_workers[0], d0);
                else
                    col_rel_destroy(d0);
            }
            if (rc != 0)
                break;
            /* Shared views on workers 1..W-1 */
            d0 = session_find_rel(&coord->tdd_workers[0], dname);
            for (uint32_t w = 1; w < W && rc == 0; w++) {
                col_rel_t *dw = session_find_rel(
                    &coord->tdd_workers[w], dname);
                if (dw && dw->ncols == ncols) {
                    rc = col_rel_install_shared_view(dw, d0);
                    if (rc != 0) {
                        dw->nrows = 0;
                        rc = col_rel_append_all(dw, d0, NULL);
                    }
                } else {
                    dw = col_rel_new_auto(dname, ncols);
                    if (!dw) {
                        rc = ENOMEM; break;
                    }
                    int vrc = col_rel_install_shared_view(dw, d0);
                    if (vrc != 0)
                        rc = col_rel_append_all(dw, d0, NULL);
                    if (rc == 0)
                        rc = session_add_rel(&coord->tdd_workers[w], dw);
                    else
                        col_rel_destroy(dw);
                }
            }
            if (rc != 0)
                break;
        }
        if (rc != 0) {
            free(bdx_snap);
            bdx_snap = NULL;
            goto done;
        }
    }

    for (uint32_t iter = 0; iter < MAX_ITERATIONS; iter++) {
        bool outer_any_new = false;
        bool converged = false;
        bool stride_all_skipped = true;
        bool outer_continue_next = false;

        for (uint32_t sub = 0; sub < EVAL_STRIDE; sub++) {
            uint32_t eff_iter = iter * EVAL_STRIDE + sub;

            /* Phase 4: Frontier Skip Optimization (eval.c:420-423) */
            if (coord->frontier_ops->should_skip_iteration(coord,
                stratum_idx, eff_iter)) {
                continue;
            }
            stride_all_skipped = false;

            /* Reset worker contexts for this sub-pass (reuse allocation) */
            for (uint32_t w = 0; w < W; w++) {
                memset(ctxs[w].delta_rels, 0, nrels * sizeof(col_rel_t *));
                ctxs[w].sp = sp;
                ctxs[w].worker_sess = &coord->tdd_workers[w];
                ctxs[w].stratum_idx = stratum_idx;
                ctxs[w].eff_iter = eff_iter;
                ctxs[w].any_new = false;
                ctxs[w].all_empty_delta = false;
                ctxs[w].force_diff = bdx_mode;
                ctxs[w].rc = 0;
            }

            /* DISPATCH */
            bool submit_ok = true;
            for (uint32_t w = 0; w < W; w++) {
                if (wl_workqueue_submit(coord->wq, tdd_worker_subpass_fn,
                    &ctxs[w]) != 0) {
                    wl_workqueue_drain(coord->wq);
                    submit_ok = false;
                    break;
                }
            }

            if (!submit_ok) {
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                rc = ENOMEM;
                goto done;
            }

            /* BARRIER */
            wl_workqueue_wait_all(coord->wq);

            /* Collect first worker error */
            for (uint32_t w = 0; w < W; w++) {
                if (ctxs[w].rc != 0 && rc == 0)
                    rc = ctxs[w].rc;
            }

            if (rc != 0) {
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                goto done;
            }

            /* Stratum-level early exit: all workers have all_empty_delta */
            bool all_workers_empty = true;
            for (uint32_t w = 0; w < W; w++) {
                if (!ctxs[w].all_empty_delta) {
                    all_workers_empty = false;
                    break;
                }
            }
            if (all_workers_empty) {
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                outer_continue_next = true;
                break;
            }

            /* Phase 4: Global Convergence Detection
             * CONVERGENCE: fixed point if no worker produced new tuples.
             *
             * Each worker tracks any_new = true if its partition produced
             * at least one new tuple during this sub-pass. Global convergence
             * occurs when ALL workers have any_new = false.
             *
             * Correctness: Under distributed execution with hash-partitioned
             * exchange, each worker independently computes new tuples from
             * its partition. No tuple can be created without appearing in
             * at least one worker's delta. Therefore, checking all workers'
             * any_new flags is both necessary and sufficient for fixed-point
             * detection.
             */
            if (tdd_check_convergence(ctxs, W)) {
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                converged = true;
                break;
            }

            /* EXCHANGE: BDX for Category C, hash scatter/gather for
             * standard hybrid, broadcast for replicate/self_join_mode.
             * Issue #372: pass self_join_mode so asymmetric strata broadcast
             * deltas to all workers (each holds 1/W IDB, needs full delta). */
            int brc;
            if (bdx_mode)
                brc = tdd_bdx_exchange_deltas(sp, coord, ctxs, W,
                        bdx_snap);
            else
                brc = tdd_exchange_deltas(sp, coord, ctxs, W,
                        !replicate_mode, self_join_mode);

            if (brc != 0) {
                rc = brc;
                goto done;
            }

            outer_any_new = true;
            final_eff_iter = eff_iter;
        } /* end sub loop */

        if (stride_all_skipped)
            continue;
        if (outer_continue_next)
            continue;
        if (converged || !outer_any_new)
            break;
    } /* end outer loop */

done:
    /* Free pre-allocated worker contexts */
    if (ctxs) {
        for (uint32_t w = 0; w < W; w++)
            free((void *)ctxs[w].delta_rels);
        free(ctxs);
    }
    free(bdx_snap);
    coord->diff_operators_active = saved_diff;
    coord->total_iterations = final_eff_iter;

    if (rc == 0) {
        /* Record stratum and per-rule frontiers (mirrors eval.c:768-791) */
        tdd_record_recursive_convergence(coord, sp, stratum_idx,
            rule_id_base, final_eff_iter);

        if (bdx_mode) {
            /* BDX mode: coordinator IDB is already maintained (monotonic
             * growth via tdd_bdx_exchange_deltas).  Just dedup final. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(coord,
                        sp->relations[ri].name);
                if (r && r->nrows > 1)
                    tdd_dedup_rel(r);
            }
        } else {
            /* Non-BDX: Merge worker IDB into coordinator */
            rc = tdd_merge_worker_results(sp, coord);

            /* Dedup coordinator IDB (broadcast exchange may introduce
             * duplicates when equal-length paths exist across partitions). */
            if (rc == 0) {
                for (uint32_t ri = 0; ri < nrels; ri++) {
                    col_rel_t *r = session_find_rel(coord,
                            sp->relations[ri].name);
                    if (r && r->nrows > 1)
                        tdd_dedup_rel(r);
                }
            }
        }
    }

    tdd_cleanup_workers(coord);
    return rc;
}

/*
 * col_eval_stratum_tdd_nonrecursive:
 * Non-recursive distributed path: PARTITION → DISPATCH → BARRIER →
 * CONSOLIDATE (no exchange needed for non-recursive rules).
 */
static int
col_eval_stratum_tdd_nonrecursive(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx)
{
    uint32_t W = coord->num_workers;
    int rc;

    /* Phase 1: PARTITION — partition all coordinator relations to workers */
    rc = tdd_init_workers(coord);
    if (rc != 0)
        return rc;

    /* Phase 2: DISPATCH — submit W workers */
    col_eval_tdd_worker_ctx_t *ctxs
        = (col_eval_tdd_worker_ctx_t *)calloc(
            W, sizeof(col_eval_tdd_worker_ctx_t));
    if (!ctxs) {
        tdd_cleanup_workers(coord);
        return ENOMEM;
    }

    for (uint32_t w = 0; w < W; w++) {
        ctxs[w].sp = sp;
        ctxs[w].worker_sess = &coord->tdd_workers[w];
        ctxs[w].stratum_idx = stratum_idx;
        ctxs[w].rc = 0;
        if (wl_workqueue_submit(coord->wq, tdd_worker_nonrecursive_fn,
            &ctxs[w])
            != 0) {
            rc = ENOMEM;
            wl_workqueue_drain(coord->wq);
            free(ctxs);
            tdd_cleanup_workers(coord);
            return rc;
        }
    }

    /* Phase 3: BARRIER */
    wl_workqueue_wait_all(coord->wq);

    /* Collect first worker error */
    for (uint32_t w = 0; w < W; w++) {
        if (ctxs[w].rc != 0 && rc == 0)
            rc = ctxs[w].rc;
    }
    free(ctxs);

    /* Phase 6: CONSOLIDATE — merge worker IDB results to coordinator */
    if (rc == 0)
        rc = tdd_merge_worker_results(sp, coord);

    /* Dedup coordinator IDB (multiple workers evaluating the same rules on
     * different partitions may produce overlapping tuples). */
    if (rc == 0) {
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            col_rel_t *r = session_find_rel(coord,
                    sp->relations[ri].name);
            if (r && r->nrows > 1)
                tdd_dedup_rel(r);
        }
    }

    /* Phase 7: Record stratum and per-rule frontiers (mirrors eval.c:247-279) */
    if (rc == 0)
        tdd_record_nonrecursive_convergence(coord, sp, stratum_idx);

    /* Phase 8: Cleanup worker state */
    tdd_cleanup_workers(coord);

    return rc;
}

/*
 * col_eval_stratum_tdd:
 * Distributed stratum evaluator with 7-phase pipeline.
 *
 * For W=1: delegates to col_eval_stratum() (zero overhead).
 * For W>1: orchestrates PARTITION → DISPATCH → BARRIER →
 *          EXCHANGE → BARRIER → CONSOLIDATE → CONVERGENCE
 *          per iteration of the semi-naive fixed-point loop.
 *
 * Called from col_session_step() in place of col_eval_stratum()
 * when distributed evaluation is possible.
 */
int
col_eval_stratum_tdd(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx)
{
    if (!sp || !coord)
        return EINVAL;

    /* Single-worker fast path: zero overhead delegation */
    if (coord->num_workers <= 1 || !coord->tdd_workers)
        return col_eval_stratum(sp, coord, stratum_idx);

    if (!sp->is_recursive)
        return col_eval_stratum_tdd_nonrecursive(sp, coord, stratum_idx);

    return col_eval_stratum_tdd_recursive(sp, coord, stratum_idx);
}

/* ======================================================================== */
/* Multi-Worker Stratum Evaluation (Issue #317)                             */
/* ======================================================================== */

/*
 * col_eval_stratum_worker_ctx_t:
 * Per-worker context for col_eval_stratum_multiworker dispatch.
 */
typedef struct {
    const wl_plan_stratum_t *sp;   /* borrowed: stratum plan */
    wl_col_session_t *worker_sess; /* borrowed: isolated worker session */
    uint32_t stratum_idx;
    int rc; /* return code from col_eval_stratum */
} col_eval_stratum_worker_ctx_t;

/*
 * col_eval_stratum_worker_fn:
 * Work function executed by each worker thread.  Runs col_eval_stratum on
 * the worker's partition, then reports the resulting frontier to the
 * coordinator's progress tracker (Issue #317).
 *
 * Thread safety: writes only to its own progress slot (worker_id dimension),
 * so no synchronization is needed during the scatter phase.
 */
static void
col_eval_stratum_worker_fn(void *arg)
{
    col_eval_stratum_worker_ctx_t *ctx = (col_eval_stratum_worker_ctx_t *)arg;

    ctx->rc = col_eval_stratum(ctx->sp, ctx->worker_sess, ctx->stratum_idx);
}

/*
 * col_eval_stratum_multiworker:
 * Evaluate one stratum in parallel across num_workers pre-created worker
 * sessions.  After wl_workqueue_wait_all(), merges per-worker frontier
 * progress reports into the coordinator's global frontier.
 *
 * Protocol (Issue #317):
 *   1. Reset progress for this stratum (stale epoch entries cleared).
 *   2. Submit num_workers tasks; each runs col_eval_stratum + progress_record.
 *   3. wl_workqueue_wait_all() barrier: all workers complete.
 *   4. If all workers converged, update coordinator's frontier with the
 *      global minimum iteration (conservative lower bound for skip logic).
 *
 * Preconditions:
 *   - coord->wq is non-NULL (thread pool created at col_session_create)
 *   - workers[0..num_workers-1] are valid worker sessions with coordinator
 *     pointer set to coord
 *   - coord->progress is initialized (done in col_session_create)
 *
 * Returns 0 on success, EINVAL on bad arguments, or the first non-zero
 * error code returned by a worker.
 */
int
col_eval_stratum_multiworker(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx,
    wl_col_session_t *workers, uint32_t num_workers)
{
    if (!sp || !coord || !workers || num_workers == 0)
        return EINVAL;

    /* Step 1: Reset this stratum's progress slots for the current epoch.
     * Prevents stale convergence reports from a previous epoch blocking
     * the all_converged check after the barrier. */
    wl_frontier_progress_reset_stratum(&coord->progress, stratum_idx,
        coord->outer_epoch);

    /* Step 2: Allocate per-worker contexts and submit to workqueue */
    col_eval_stratum_worker_ctx_t *ctxs
        = (col_eval_stratum_worker_ctx_t *)calloc(
            num_workers, sizeof(col_eval_stratum_worker_ctx_t));
    if (!ctxs)
        return ENOMEM;

    int rc = 0;
    for (uint32_t w = 0; w < num_workers; w++) {
        ctxs[w].sp = sp;
        ctxs[w].worker_sess = &workers[w];
        ctxs[w].stratum_idx = stratum_idx;
        ctxs[w].rc = 0;
        if (wl_workqueue_submit(coord->wq, col_eval_stratum_worker_fn,
            &ctxs[w])
            != 0) {
            rc = ENOMEM;
            wl_workqueue_drain(coord->wq);
            free(ctxs);
            return rc;
        }
    }

    /* Step 3: Barrier — wait for all workers to complete and report */
    wl_workqueue_wait_all(coord->wq);

    /* Collect first worker error (if any) */
    for (uint32_t w = 0; w < num_workers; w++) {
        if (ctxs[w].rc != 0 && rc == 0)
            rc = ctxs[w].rc;
    }
    free(ctxs);
    if (rc != 0)
        return rc;

    /* Step 4: Merge per-worker frontiers into coordinator's global frontier.
     * The global minimum iteration is the conservative bound: the coordinator
     * can safely claim "all workers have processed up to iteration min_iter",
     * enabling the frontier skip optimization for subsequent incremental eval. */
    if (wl_frontier_progress_all_converged(&coord->progress, stratum_idx,
        coord->outer_epoch)) {
        uint32_t min_iter = wl_frontier_progress_min_iteration(
            &coord->progress, stratum_idx, coord->outer_epoch);
        if (min_iter != UINT32_MAX) {
            coord->frontier_ops->record_stratum_convergence(coord,
                stratum_idx, coord->outer_epoch, min_iter);
        }
    }

    return 0;
}
