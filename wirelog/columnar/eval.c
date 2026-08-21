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
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TDD_OWNER_FALLBACK_MIN_ITER 31u
#define TDD_OWNER_FALLBACK_DELTA_ROWS 512u

#define WL_COLUMNAR_EVAL_DEDUP_ROW_HASH wl_columnar_eval_dedup_row_hash
#define WL_COLUMNAR_EVAL_DEDUP_SET_INSERT wl_columnar_eval_dedup_set_insert
#define WL_COLUMNAR_EVAL_DEDUP_SET_CONTAINS wl_columnar_eval_dedup_set_contains
#define WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL \
        wl_columnar_eval_dedup_set_init_from_rel

/* Relation-plan dispatch is implemented in columnar/eval_plan.c. */
static col_frontier_t
col_frontier_compute(const col_rel_t *rel);
static int
col_eval_nonrecursive_relation_parallel(const wl_plan_relation_t *rp,
    wl_col_session_t *coord);
static int
col_canonicalize_recursive_aggregates(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess);

static bool
col_reduce_output_group_equal(const col_rel_t *rel, uint32_t a, uint32_t b,
    uint32_t group_by_count, uint32_t aggregate_index)
{
    for (uint32_t c = 0; c < group_by_count; c++) {
        uint32_t out_col = c >= aggregate_index ? c + 1 : c;
        if (col_rel_get(rel, a, out_col)
            != col_rel_get(rel, b, out_col))
            return false;
    }
    return true;
}

typedef struct {
    uint64_t hash;
    uint32_t row;
} col_group_slot_t;

static uint64_t
col_group_hash(const col_rel_t *rel, uint32_t row, uint32_t group_by_count,
    uint32_t aggregate_index)
{
    uint64_t hash = UINT64_C(1469598103934665603);
    for (uint32_t c = 0; c < group_by_count; c++) {
        uint32_t out_col = c >= aggregate_index ? c + 1 : c;
        hash ^= (uint64_t)col_rel_get(rel, row, out_col);
        hash *= UINT64_C(1099511628211);
    }
    return hash ? hash : 1;
}

static int
col_canonicalize_recursive_aggregate_relation(col_rel_t *rel,
    const wl_plan_agg_spec_t *spec, const wl_intern_t *intern)
{
    if (!rel || !spec || !spec->has_spec || rel->nrows < 2)
        return 0;

    uint32_t gc = spec->group_by_count;
    uint32_t agg_index = spec->aggregate_index < rel->ncols
        ? spec->aggregate_index : gc;
    if (rel->ncols <= gc)
        return EINVAL;

    uint32_t out = 0;
    uint32_t map_cap = 1;
    uint64_t desired = (uint64_t)rel->nrows * 2U;
    while ((uint64_t)map_cap < desired && map_cap <= UINT32_MAX / 2U)
        map_cap <<= 1;
    if ((uint64_t)map_cap < desired)
        return ENOMEM;
    col_group_slot_t *groups = (col_group_slot_t *)calloc(map_cap,
            sizeof(*groups));
    if (!groups)
        return ENOMEM;
    uint32_t map_mask = map_cap - 1;
    for (uint32_t row = 0; row < rel->nrows; row++) {
        uint32_t found = UINT32_MAX;
        uint64_t hash = col_group_hash(rel, row, gc, agg_index);
        uint32_t slot = (uint32_t)hash & map_mask;
        while (groups[slot].hash != 0) {
            uint32_t keep = groups[slot].row;
            if (groups[slot].hash == hash
                && col_reduce_output_group_equal(rel, keep, row, gc,
                agg_index)) {
                found = keep;
                break;
            }
            slot = (slot + 1) & map_mask;
        }

        if (found != UINT32_MAX) {
            int64_t cur = col_rel_get(rel, found, agg_index);
            int64_t val = col_rel_get(rel, row, agg_index);
            /* Same comparator as col_op_reduce(): a fixpoint that ordered
             * lexicographically within an iteration and by intern id across
             * iterations would be worse than the original bug (#965). */
            bool better = col_agg_better(spec->fn, spec->operand_type,
                    intern, val, cur);
            if (better) {
                col_rel_set(rel, found, agg_index, val);
                if (rel->timestamps)
                    rel->timestamps[found] = rel->timestamps[row];
            }
            continue;
        }

        if (out != row) {
            for (uint32_t c = 0; c < rel->ncols; c++)
                col_rel_set(rel, out, c, col_rel_get(rel, row, c));
            if (rel->timestamps)
                rel->timestamps[out] = rel->timestamps[row];
        }
        groups[slot].hash = hash;
        groups[slot].row = out;
        out++;
    }

    if (out != rel->nrows) {
        rel->nrows = out;
        rel->sorted_nrows = out;
        rel->base_nrows = out;
        rel->run_count = 0;
        rel->dedup_count = 0;
        memset(rel->run_ends, 0, sizeof(rel->run_ends));
    }

    free(groups);

    return 0;
}

static int
col_canonicalize_recursive_aggregates(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    /*
     * Reading the recorded specification rather than looking for a REDUCE in
     * rp->ops is the whole of #975: rewrite_multiway_delta() replaces a fused
     * relation's operators with a single K_FUSION and moves the originals into
     * its opaque_data, so the operator was not there to be found and this ran
     * on every recursive aggregate relation *except* the ones fusion applies
     * to.  wl_plan_relation_t.recursive_agg is filled at IR lowering, before
     * any rewrite runs.
     *
     * Timing is unchanged: this is the fixpoint, not per-iteration
     * consolidation.  A same-stratum consumer would therefore hold rows
     * derived from labels this reduction then dominates away, output that
     * contradicts itself.  #1021 settled that by refusing the shape at plan
     * time -- a recursive min/max aggregate may not share an SCC with any
     * other relation -- rather than by changing when this runs, so nothing
     * here had to move.  Moving the reduction into consolidation is still
     * open and would readmit the intra-round half of the shape; that is
     * deferred to milestone 0.70.0, see docs/SEMANTICS.md.
     *
     * The correction that matters to a reader of the old comment here:
     * wl_plan_stratum_t.is_monotone is no longer "hardcoded false and never
     * computed".  wl_plan_from_program() computes it (negation only), and it
     * still has no consumer, so a true value is not yet a signal anything may
     * act on.
     */
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];
        if (!rp->recursive_agg.has_spec)
            continue;
        col_rel_t *rel = session_find_rel(sess, rp->name);
        int rc = col_canonicalize_recursive_aggregate_relation(rel,
                &rp->recursive_agg, sess->intern);
        if (rc != 0)
            return rc;
        col_session_invalidate_arrangements(&sess->base, rp->name);
    }
    return 0;
}

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
        /* Issue #914: a non-recursive stratum is always the base case. The
         * recursive-stratum loop leaves sess->current_iteration > 0 and never
         * resets it, which would wrongly trigger the base-case EDB VARIABLE
         * skip (eval.c base-case skip) and drop this stratum's head tuples.
         * Reset here so it covers both the parallel and sequential paths. */
        sess->current_iteration = 0;

        /* Non-recursive: evaluate each relation plan once */
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];
            int par_rc = col_eval_nonrecursive_relation_parallel(rp, sess);
            if (par_rc == 0)
                continue;
            if (par_rc != EAGAIN) {
                if (getenv("WIRELOG_DEBUG_NONREC_TDD"))
                    fprintf(stderr, "nonrec_tdd failed rel=%s rc=%d\n",
                        rp->name ? rp->name : "(null)", par_rc);
                return par_rc;
            }

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
                    rc = col_rel_append_all(copy, result.rel,
                            sess->eval_arena);
                    if (rc != 0) {
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
        sess->rotation_ops->rotate_eval_arena(sess);

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

    /* Allocate snap once before the iteration loop (Issue #367): hoisting
     * the allocation out of the outer loop eliminates 2 malloc/free calls
     * per iteration (14K+ iterations for CRDT). */
    uint32_t *snap = (uint32_t *)malloc(nrels * sizeof(uint32_t));
    if (!snap) {
        free((void *)delta_rels);
        return ENOMEM;
    }

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

        /* snap[] is filled at the start of each sub-pass (nrows grows as new
         * tuples are appended).  The array is allocated once before this loop
         * (Issue #367) to avoid per-iteration malloc/free overhead. */

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
                const char *dname = sp->relations[ri].delta_name;
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
                    const char *dname = sp->relations[ri].delta_name;
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
                 * rows).
                 *
                 * The !result.rel arm is not analyzer appeasement.
                 * eval_stack_pop() yields {NULL, ...} only for an empty
                 * stack, and the stack.top == 0 check three lines above has
                 * already excluded that, so a NULL here would mean a NULL rel
                 * was pushed.  It is dispositioned as "no result" to match
                 * that same branch rather than as an error, so one observable
                 * state does not get two dispositions depending on which
                 * check saw it first.  (diff.c and join.c return EINVAL on a
                 * NULL pop because they pop without a prior emptiness check.)
                 * Otherwise the statements below dereference result.rel:
                 * ->name on the rename arm, ->ncols on the schema-adoption
                 * arm.  col_rel_destroy(NULL) is a no-op, so the continue
                 * leaks nothing. */
                if (!result.rel || result.rel->nrows == 0) {
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
                        rc = col_rel_append_all(copy, result.rel,
                                sess->eval_arena);
                        if (rc != 0) {
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
                const char *dname = sp->relations[ri].delta_name;
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

                const char *dname = sp->relations[ri].delta_name;
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
                /* Per-call trace: WL_LOG=CONSOLIDATION:5 (or legacy
                 * WL_CONSOLIDATION_LOG presence shim). */
                WL_LOG(WL_LOG_SEC_CONSOLIDATION, WL_LOG_DEBUG,
                    "eff_iter=%u stratum=%u rel=%s N=%u D=%u "
                    "time_us=%.1f ratio=%.4f",
                    eff_iter, stratum_idx, sp->relations[ri].name,
                    cons_old, cons_new, (double)cons_elapsed / 1000.0,
                    cons_old > 0 ? (double)cons_new / (double)cons_old
                                     : 0.0);
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

            /* Issue #176: Per-sub-pass cache eviction for recursive strata.
             * Materialized join results may live in delta_pool, so release
             * them before resetting the pool.  Resetting first clears the
             * pool-owned marker and leaves the cache holding stale pointers.
             * Use configurable eviction threshold (cache_evict_threshold):
             * - If 0: clear entire cache each sub-pass (backward compatible)
             * - If > 0: evict LRU entries when cache exceeds threshold */
            if (sess->cache_evict_threshold == 0) {
                col_mat_cache_clear(&sess->mat_cache);
            } else {
                col_mat_cache_evict_until(&sess->mat_cache,
                    sess->cache_evict_threshold);
            }

            delta_pool_reset(sess->delta_pool);
            sess->rotation_ops->rotate_eval_arena(sess);
            /* Issue #560: Advance the compound-arena epoch frontier at the
             * end of each recursive sub-pass iteration so the
             * epoch-frontier GC can reclaim handles whose multiplicity
             * dropped to zero in this iteration.  NULL-guard each link in
             * the dispatch chain because tests and early lifecycle paths
             * may operate without a compound arena or rotation strategy.
             * The compound_arena is borrowed from the coordinator (Issue
             * #579 / R-5); only the coordinator may mutate it, so
             * worker-context invocations skip the dispatch. */
            if (sess->coordinator == NULL
                && sess->compound_arena && sess->rotation_ops
                && sess->rotation_ops->gc_epoch_boundary) {
                sess->rotation_ops->gc_epoch_boundary(sess);
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

        if (outer_rc != 0) {
            /* Issue #282: Restore diff_operators_active on error path */
            sess->diff_operators_active = saved_diff_operators_active;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                session_remove_rel(sess, sp->relations[ri].delta_name);
                if (delta_rels[ri])
                    col_rel_destroy(delta_rels[ri]);
            }
            free(snap);
            free((void *)delta_rels);
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

    /* Issue #914: current_iteration is deliberately left at final_eff_iter (> 0)
     * here. The non-recursive branch of col_eval_stratum is responsible for
     * resetting it to 0 before its base-case evaluation; do not add a second
     * reset here (consensus declined that). See the non-recursive branch above. */

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

    int agg_rc = col_canonicalize_recursive_aggregates(sp, sess);
    if (agg_rc != 0) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (delta_rels[ri])
                col_rel_destroy(delta_rels[ri]);
        }
        free(snap);
        free((void *)delta_rels);
        return agg_rc;
    }

    /* Cleanup all delta relations after frontier has been computed */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (delta_rels[ri])
            col_rel_destroy(delta_rels[ri]);
        const char *dname = sp->relations[ri].delta_name;
        session_remove_rel(sess, dname);
    }
    free(snap);
    free((void *)delta_rels);
    delta_pool_reset(sess->delta_pool);
    sess->rotation_ops->rotate_eval_arena(sess);
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
bool
stratum_has_preseeded_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const char *dname = sp->relations[ri].delta_name;
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
    coord->tdd_active_workers = 0;
}

static void
tdd_record_active_workers(wl_col_session_t *coord, uint32_t W)
{
    coord->tdd_last_active_workers = W;
    if (W > coord->tdd_max_active_workers)
        coord->tdd_max_active_workers = W;
}

static void
tdd_dedup_rel(col_rel_t *r);

typedef struct {
    const wl_plan_relation_t *rp;
    wl_col_session_t *worker_sess;
    eval_entry_t result;
    int rc;
} nonrec_rule_worker_ctx_t;

static void
nonrec_rule_worker_fn(void *arg)
{
    nonrec_rule_worker_ctx_t *ctx = (nonrec_rule_worker_ctx_t *)arg;
    eval_stack_t stack;
    eval_stack_init(&stack);

    ctx->result.rel = NULL;
    ctx->result.owned = false;
    ctx->rc = col_eval_relation_plan(ctx->rp, &stack, ctx->worker_sess);
    if (ctx->rc != 0) {
        eval_stack_drain(&stack);
        return;
    }
    if (stack.top > 0)
        ctx->result = eval_stack_pop(&stack);
    eval_stack_drain(&stack);
}

static bool
nonrec_plan_has_consolidate(const wl_plan_relation_t *rp)
{
    for (uint32_t oi = 0; oi < rp->op_count; oi++)
        if (rp->ops[oi].op == WL_PLAN_OP_CONSOLIDATE)
            return true;
    return false;
}

static bool
nonrec_plan_parallel_safe(const wl_plan_relation_t *rp,
    const char **out_driver)
{
    const char *driver = NULL;
    uint32_t variables = 0;

    for (uint32_t oi = 0; oi < rp->op_count; oi++) {
        const wl_plan_op_t *op = &rp->ops[oi];
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            if (!op->relation_name || op->delta_mode != WL_DELTA_AUTO)
                return false;
            if (variables++ == 0)
                driver = op->relation_name;
            break;
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_ANTIJOIN:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_CONSOLIDATE:
            break;
        case WL_PLAN_OP_JOIN:
            if (op->key_count == 0)
                return false;
            break;
        default:
            return false;
        }
    }
    if (variables != 1 || !driver || strcmp(driver, rp->name) == 0)
        return false;
    if (out_driver)
        *out_driver = driver;
    return true;
}

static uint32_t
nonrec_parallel_min_rows_per_worker(void)
{
    const char *env = getenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");
    uint32_t min_rows = 32768u;
    if (env && env[0] != '\0') {
        char *endp = NULL;
        errno = 0;
        unsigned long v = strtoul(env, &endp, 10);
        if (endp != env && *endp == '\0' && errno != ERANGE && v > 0
            && v <= UINT32_MAX)
            min_rows = (uint32_t)v;
    }
    return min_rows;
}

static int
nonrec_copy_relation_slice(const col_rel_t *src, const char *name,
    uint32_t begin, uint32_t end, col_rel_t **out)
{
    if (!src || !out || begin > end || end > src->nrows)
        return EINVAL;
    col_rel_t *dst = col_rel_new_like(name, src);
    if (!dst)
        return ENOMEM;

    int64_t *row = (int64_t *)malloc(
        sizeof(int64_t) * (src->ncols ? src->ncols : 1));
    if (!row) {
        col_rel_destroy(dst);
        return ENOMEM;
    }
    int rc = 0;
    for (uint32_t r = begin; r < end && rc == 0; r++) {
        for (uint32_t c = 0; c < src->ncols; c++)
            row[c] = src->columns[c][r];
        rc = col_rel_append_row(dst, row);
    }
    free(row);
    if (rc != 0) {
        col_rel_destroy(dst);
        return rc;
    }
    *out = dst;
    return 0;
}

static int
nonrec_make_shared_relation_view(const col_rel_t *src, col_rel_t **out)
{
    if (!src || !out)
        return EINVAL;
    col_rel_t *view = col_rel_new_like(src->name, src);
    if (!view)
        return ENOMEM;
    int rc = col_rel_install_shared_view(view, src);
    if (rc != 0) {
        rc = col_rel_append_all(view, src, NULL);
        if (rc != 0) {
            col_rel_destroy(view);
            return rc;
        }
    }
    *out = view;
    return 0;
}

static int
col_eval_nonrecursive_relation_parallel(const wl_plan_relation_t *rp,
    wl_col_session_t *coord)
{
    const char *driver_name = NULL;
    if (!nonrec_plan_parallel_safe(rp, &driver_name))
        return EAGAIN;
    if (coord->coordinator != NULL)
        return EAGAIN;
    if (coord->delta_seeded || coord->retraction_seeded
        || coord->retraction_right_pass || coord->diff_operators_active)
        return EAGAIN;
    if (coord->num_workers <= 1 || coord->nrels == 0)
        return EAGAIN;

    col_rel_t *driver = session_find_rel(coord, driver_name);
    if (!driver || driver->nrows == 0)
        return EAGAIN;

    uint32_t min_rows = nonrec_parallel_min_rows_per_worker();
    uint32_t W = driver->nrows / min_rows;
    if (W > coord->num_workers)
        W = coord->num_workers;
    if (W > 32)
        W = 32;
    if (W <= 1)
        return EAGAIN;

    char slice_name[256];
    int sn = snprintf(slice_name, sizeof(slice_name), "$nonrec$%s",
            driver_name);
    if (sn < 0 || (size_t)sn >= sizeof(slice_name))
        return EAGAIN;

    wl_plan_relation_t rp_copy = *rp;
    wl_plan_op_t *ops_copy = (wl_plan_op_t *)malloc(
        sizeof(wl_plan_op_t) * (rp->op_count ? rp->op_count : 1));
    if (!ops_copy)
        return ENOMEM;
    memcpy(ops_copy, rp->ops, sizeof(wl_plan_op_t) * rp->op_count);
    bool replaced_driver = false;
    for (uint32_t oi = 0; oi < rp->op_count; oi++) {
        if (ops_copy[oi].op == WL_PLAN_OP_VARIABLE
            && ops_copy[oi].relation_name
            && strcmp(ops_copy[oi].relation_name, driver_name) == 0) {
            ops_copy[oi].relation_name = slice_name;
            replaced_driver = true;
            break;
        }
    }
    if (!replaced_driver) {
        free(ops_copy);
        return EAGAIN;
    }
    rp_copy.ops = ops_copy;

    int rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (rc != 0) {
        free(ops_copy);
        return rc;
    }
    rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (rc != 0) {
        free(ops_copy);
        return rc;
    }
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);

    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    nonrec_rule_worker_ctx_t *ctxs = (nonrec_rule_worker_ctx_t *)calloc(
        W, sizeof(nonrec_rule_worker_ctx_t));
    if (!worker_rels || !ctxs) {
        free((void *)worker_rels);
        free(ctxs);
        free(ops_copy);
        tdd_cleanup_workers(coord);
        return ENOMEM;
    }

    uint32_t built_workers = 0;
    uint32_t chunk = (driver->nrows + W - 1u) / W;
    atomic_uint_fast64_t shared_join_count;
    atomic_store_explicit(&shared_join_count, 0, memory_order_relaxed);
    for (uint32_t w = 0; w < W && rc == 0; w++) {
        worker_rels[w] = (col_rel_t **)calloc(coord->nrels + 1,
                sizeof(col_rel_t *));
        if (!worker_rels[w]) {
            rc = ENOMEM;
            break;
        }
        uint32_t rels_built = 0;
        for (uint32_t ri = 0; ri < coord->nrels && rc == 0; ri++) {
            col_rel_t *src = coord->rels[ri];
            if (!src)
                continue;
            col_rel_t *rel = NULL;
            rc = nonrec_make_shared_relation_view(src, &rel);
            if (rc == 0)
                worker_rels[w][rels_built++] = rel;
        }
        if (rc == 0) {
            uint32_t begin = w * chunk;
            uint32_t end = begin + chunk;
            if (begin > driver->nrows)
                begin = driver->nrows;
            if (end > driver->nrows)
                end = driver->nrows;
            col_rel_t *slice = NULL;
            rc = nonrec_copy_relation_slice(driver, slice_name, begin, end,
                    &slice);
            if (rc == 0)
                worker_rels[w][rels_built++] = slice;
        }
        if (rc != 0)
            break;
        rc = col_worker_session_create(coord, w, worker_rels[w], rels_built,
                &coord->tdd_workers[w]);
        if (rc == 0) {
            coord->tdd_workers[w].join_output_limit =
                coord->join_output_limit;
            if (coord->join_output_limit > 0) {
                coord->tdd_workers[w].join_output_shared_count =
                    &shared_join_count;
                coord->tdd_workers[w].join_output_shared_limit =
                    coord->join_output_limit;
            }
            built_workers++;
        }
    }
    coord->tdd_workers_count = built_workers;

    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            ctxs[w].rp = &rp_copy;
            ctxs[w].worker_sess = &coord->tdd_workers[w];
            if (wl_workqueue_submit(coord->wq, nonrec_rule_worker_fn,
                &ctxs[w]) != 0) {
                rc = ENOMEM;
                break;
            }
        }
        wl_workqueue_wait_all(coord->wq);
        for (uint32_t w = 0; w < W; w++)
            if (ctxs[w].rc != 0 && rc == 0)
                rc = ctxs[w].rc;
    }

    bool merge_started = false;
    if (rc == 0) {
        merge_started = true;
        col_rel_t *target = session_find_rel(coord, rp->name);
        const col_rel_t *schema_rel = NULL;
        for (uint32_t w = 0; w < W && rc == 0; w++) {
            col_rel_t *wr = ctxs[w].result.rel;
            if (!wr)
                continue;
            if (!schema_rel && wr->ncols > 0)
                schema_rel = wr;
            if (wr->nrows == 0)
                continue;
            if (!target) {
                target = col_rel_new_auto(rp->name, wr->ncols);
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
            if (target->ncols == 0 && wr->ncols > 0) {
                rc = col_rel_set_schema(target, wr->ncols,
                        (const char *const *)wr->col_names);
                if (rc != 0)
                    break;
            }
            rc = col_rel_append_all(target, wr, coord->eval_arena);
        }
        if (rc == 0 && !target) {
            uint32_t ncols = schema_rel ? schema_rel->ncols : 0;
            target = col_rel_new_auto(rp->name, ncols);
            if (!target) {
                rc = ENOMEM;
            } else if (schema_rel && ncols > 0) {
                rc = col_rel_set_schema(target, ncols,
                        (const char *const *)schema_rel->col_names);
            }
            if (rc == 0) {
                rc = session_add_rel(coord, target);
                if (rc != 0) {
                    col_rel_destroy(target);
                    target = NULL;
                }
            } else if (target) {
                col_rel_destroy(target);
                target = NULL;
            }
        }
        if (rc == 0 && target && nonrec_plan_has_consolidate(rp))
            tdd_dedup_rel(target);
    }

    for (uint32_t w = 0; w < W; w++) {
        if (ctxs[w].result.rel && ctxs[w].result.owned
            && !ctxs[w].result.rel->pool_owned)
            col_rel_destroy(ctxs[w].result.rel);
    }
    free(ctxs);
    free(ops_copy);
    if (worker_rels) {
        for (uint32_t w = 0; w < W; w++) {
            if (worker_rels[w]) {
                if (w >= built_workers) {
                    for (uint32_t ri = 0; ri < coord->nrels + 1; ri++)
                        col_rel_destroy(worker_rels[w][ri]);
                }
                free((void *)worker_rels[w]);
            }
        }
        free((void *)worker_rels);
    }
    tdd_cleanup_workers(coord);
    if (!merge_started && rc == EOVERFLOW)
        return EAGAIN;
    return rc;
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
tdd_init_workers(wl_col_session_t *coord, uint32_t W)
{
    tdd_cleanup_workers(coord);

    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    ensure_rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);
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
                rc = col_rel_exchange_partition(rel, key_cols, 1, W, parts);
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
                free((void *)parts);
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
tdd_replicate_workers(wl_col_session_t *coord, uint32_t W)
{
    tdd_cleanup_workers(coord);

    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    ensure_rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);
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

static int
tdd_init_workers_global_read(wl_col_session_t *coord, uint32_t W)
{
    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (rc != 0)
        return rc;
    rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (rc != 0)
        return rc;

    tdd_cleanup_workers(coord);

    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);

    uint32_t nrels = coord->nrels;
    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_rels)
        return ENOMEM;

    for (uint32_t w = 0; w < W; w++) {
        worker_rels[w] = (col_rel_t **)calloc(nrels ? nrels : 1,
                sizeof(col_rel_t *));
        if (!worker_rels[w]) {
            rc = ENOMEM;
            goto cleanup;
        }
        for (uint32_t r = 0; r < nrels; r++) {
            col_rel_t *src = coord->rels[r];
            if (!src)
                continue;
            rc = nonrec_make_shared_relation_view(src, &worker_rels[w][r]);
            if (rc != 0)
                goto cleanup;
        }
        rc = col_worker_session_create(coord, w, worker_rels[w], nrels,
                &coord->tdd_workers[w]);
        if (rc != 0)
            goto cleanup;
        coord->tdd_workers_count = w + 1;
    }

cleanup:
    if (worker_rels) {
        for (uint32_t w = coord->tdd_workers_count; w < W; w++) {
            if (!worker_rels[w])
                continue;
            for (uint32_t r = 0; r < nrels; r++)
                col_rel_destroy(worker_rels[w][r]);
        }
        for (uint32_t w = 0; w < W; w++)
            free((void *)worker_rels[w]);
        free((void *)worker_rels);
    }
    if (rc != 0)
        tdd_cleanup_workers(coord);
    return rc;
}

static int
tdd_refresh_global_read_relation(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t ri, uint32_t W)
{
    if (!sp || !coord || ri >= sp->relation_count)
        return EINVAL;

    const char *name = sp->relations[ri].name;
    col_rel_t *src = session_find_rel(coord, name);
    if (!src)
        return 0;

    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *dst = session_find_rel(&coord->tdd_workers[w], name);
        if (!dst)
            continue;
        if (src->ncols > 0) {
            if (dst->ncols != src->ncols) {
                col_rel_t *view = NULL;
                int rc = nonrec_make_shared_relation_view(src, &view);
                if (rc != 0)
                    return rc;
                rc = session_add_rel(&coord->tdd_workers[w], view);
                if (rc != 0) {
                    col_rel_destroy(view);
                    return rc;
                }
            } else {
                int rc = col_rel_install_shared_view(dst, src);
                if (rc != 0) {
                    dst->nrows = 0;
                    rc = col_rel_append_all(dst, src, NULL);
                }
                if (rc != 0)
                    return rc;
            }
        } else {
            dst->nrows = 0;
        }
        col_session_invalidate_arrangements(&coord->tdd_workers[w].base,
            name);
    }
    return 0;
}

static int
tdd_seed_global_read_initial_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t W)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;
        col_rel_t *src = session_find_rel(coord, rel_name);

        for (uint32_t w = 0; w < W; w++)
            session_remove_rel(&coord->tdd_workers[w], dname);

        if (!src || src->nrows == 0)
            continue;

        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta =
                    (const wl_plan_op_exchange_t *)
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
        bool key_valid = src->ncols > 0;
        for (uint32_t ki = 0; key_valid && ki < key_count; ki++) {
            if (key_cols[ki] >= src->ncols)
                key_valid = false;
        }
        if (!key_valid) {
            key_cols = default_key;
            key_count = 1;
        }

        col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!parts)
            return ENOMEM;
        int rc = col_rel_exchange_partition(src, key_cols, key_count, W,
                parts);
        if (rc != 0) {
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free((void *)parts);
            return rc;
        }

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *part = parts[w];
            parts[w] = NULL;
            if (!part || part->nrows == 0) {
                col_rel_destroy(part);
                continue;
            }
            free(part->name);
            part->name = wl_strdup(dname);
            if (!part->name) {
                col_rel_destroy(part);
                rc = ENOMEM;
                break;
            }
            rc = session_add_rel(&coord->tdd_workers[w], part);
            if (rc != 0) {
                col_rel_destroy(part);
                break;
            }
        }
        for (uint32_t w = 0; w < W; w++)
            col_rel_destroy(parts[w]);
        free((void *)parts);
        if (rc != 0)
            return rc;
    }
    return 0;
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
static int bdx_hash_diff(col_rel_t *delta, const col_rel_t *base);
static int tdd_hashset_diff(col_rel_t *delta, const col_rel_t *base);
static int
tdd_install_empty_delta_on_workers(wl_col_session_t *coord,
    const char *dname, uint32_t ncols, uint32_t W)
{
    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *empty = col_rel_new_auto(dname, ncols);
        if (!empty)
            return ENOMEM;
        int rc = session_add_rel(&coord->tdd_workers[w], empty);
        if (rc != 0) {
            if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG")) {
                fprintf(stderr,
                    "TDD install empty delta error rel=%s worker=%u cols=%u rc=%d\n",
                    dname, w, ncols, rc);
            }
            col_rel_destroy(empty);
            return rc;
        }
    }
    return 0;
}

static void
tdd_worker_subpass_fn(void *arg)
{
    col_eval_tdd_worker_ctx_t *ctx = (col_eval_tdd_worker_ctx_t *)arg;
    wl_col_session_t *sess = ctx->worker_sess;
    const wl_plan_stratum_t *sp = ctx->sp;
    uint32_t eff_iter = ctx->eff_iter;
    uint32_t nrels = sp->relation_count;
    uint64_t worker_t0 = now_ns();

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
    bool saved_outbound_only = sess->tdd_outbound_only_active;
    bool saved_delta_seeded = sess->delta_seeded;
    sess->tdd_subpass_active = true;
    sess->tdd_outbound_only_active = ctx->outbound_only;
    if (ctx->force_diff && ctx->outbound_only && eff_iter > 0)
        sess->delta_seeded = true;
    sess->current_iteration = eff_iter;
#define TDD_WORKER_RETURN() \
        do { \
            sess->tdd_subpass_active = saved_tdd_subpass; \
            sess->tdd_outbound_only_active = saved_outbound_only; \
            sess->delta_seeded = saved_delta_seeded; \
            sess->diff_operators_active = saved_diff; \
            ctx->runtime_ns = now_ns() - worker_t0; \
            return; \
        } while (0)

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
            sess->tdd_outbound_only_active = saved_outbound_only;
            sess->diff_operators_active = saved_diff;
            TDD_WORKER_RETURN();
        }
    }

    /* Snapshot nrows before evaluation (eval.c:446-449) */
    uint32_t *snap = (uint32_t *)calloc(nrels, sizeof(uint32_t));
    if (!snap) {
        ctx->rc = ENOMEM;
        sess->tdd_subpass_active = saved_tdd_subpass;
        sess->tdd_outbound_only_active = saved_outbound_only;
        sess->diff_operators_active = saved_diff;
        TDD_WORKER_RETURN();
    }
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        snap[ri] = r ? r->nrows : 0;
    }

    bool any_new = false;

    /* Evaluate all relation plans (eval.c:505-604) */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];

        if (has_empty_forced_delta(rp, sess, eff_iter))
            continue;

        eval_stack_t stack;
        eval_stack_init(&stack);

        int rc = col_eval_relation_plan(rp, &stack, sess);
        if (rc != 0) {
            if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG"))
                fprintf(stderr,
                    "TDD relation error worker=%u rel=%s iter=%u rc=%d\n",
                    sess->worker_id, rp->name ? rp->name : "(null)",
                    eff_iter, rc);
            eval_stack_drain(&stack);
            ctx->rc = rc;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->tdd_outbound_only_active = saved_outbound_only;
            sess->diff_operators_active = saved_diff;
            TDD_WORKER_RETURN();
        }

        if (stack.top == 0)
            continue;

        eval_entry_t result = eval_stack_pop(&stack);
        eval_stack_drain(&stack);

        /* Post-eval skip: evaluation produced 0 rows.
         *
         * The !result.rel arm is not analyzer appeasement.  eval_stack_pop()
         * yields {NULL, ...} only for an empty stack, and the stack.top == 0
         * check three lines above has already excluded that, so a NULL here
         * would mean a NULL rel was pushed.  It is dispositioned as "no
         * result" to match that same branch rather than as an error, so one
         * observable state does not get two dispositions depending on which
         * check saw it first.  (diff.c and join.c return EINVAL on a NULL
         * pop because they pop without a prior emptiness check.)
         *
         * Two dereferences follow otherwise: ->name / ->ncols on the
         * non-outbound path.  col_rel_new_like() below no longer needs one:
         * since Issue #1140 it rejects a NULL src and returns NULL, which
         * its caller already checks.  col_rel_destroy(NULL) is a no-op, so
         * the continue leaks nothing. */
        if (!result.rel || result.rel->nrows == 0) {
            if (result.owned)
                col_rel_destroy(result.rel);
            continue;
        }

        col_rel_t *target = session_find_rel(sess, rp->name);
        if (ctx->outbound_only) {
            const char *dname = sp->relations[ri].delta_name;
            col_rel_t *delta = col_rel_new_like(dname, result.rel);
            if (!delta) {
                if (result.owned)
                    col_rel_destroy(result.rel);
                ctx->rc = ENOMEM;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }
            int rc = col_rel_append_all(delta, result.rel, sess->eval_arena);
            if (result.owned)
                col_rel_destroy(result.rel);
            if (rc != 0) {
                col_rel_destroy(delta);
                ctx->rc = rc;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }
            if (target && target->nrows > 0
                && !(ctx->force_diff && ctx->outbound_only)) {
                rc = bdx_hash_diff(delta, target);
                if (rc != 0) {
                    if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG"))
                        fprintf(stderr,
                            "TDD local diff error worker=%u rel=%s iter=%u "
                            "delta_cols=%u target_cols=%u rc=%d\n",
                            sess->worker_id, rp->name ? rp->name : "(null)",
                            eff_iter, delta->ncols, target->ncols, rc);
                    col_rel_destroy(delta);
                    ctx->rc = rc;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
            }
            if (sess->coordinator && delta->nrows > 0) {
                col_rel_t *coord_target = session_find_rel(
                    sess->coordinator, rp->name);
                if (coord_target && coord_target->nrows > 0) {
                    rc = tdd_hashset_diff(delta, coord_target);
                    if (rc != 0) {
                        if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG"))
                            fprintf(stderr,
                                "TDD coord diff error worker=%u rel=%s "
                                "iter=%u delta_cols=%u target_cols=%u rc=%d\n",
                                sess->worker_id,
                                rp->name ? rp->name : "(null)", eff_iter,
                                delta->ncols, coord_target->ncols, rc);
                        col_rel_destroy(delta);
                        ctx->rc = rc;
                        free(snap);
                        sess->tdd_subpass_active = saved_tdd_subpass;
                        sess->tdd_outbound_only_active = saved_outbound_only;
                        sess->diff_operators_active = saved_diff;
                        TDD_WORKER_RETURN();
                    }
                }
            }
            if (delta->nrows > 1)
                tdd_dedup_rel(delta);
            bool produced = delta->nrows > 0;
            rc = wl_columnar_eval_tdd_queue_publish_delta(ctx, sess, delta,
                    ri, eff_iter);
            if (rc != 0) {
                ctx->rc = rc;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }
            if (produced)
                any_new = true;
            continue;
        }

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
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
                result.owned = false;
            } else {
                copy = col_rel_pool_new_like(sess->delta_pool, rp->name,
                        result.rel);
                if (!copy) {
                    ctx->rc = ENOMEM;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
                rc = col_rel_append_all(copy, result.rel, sess->eval_arena);
                if (rc != 0) {
                    col_rel_destroy(copy);
                    ctx->rc = rc;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
            }
            rc = session_add_rel(sess, copy);
            if (rc != 0) {
                col_rel_destroy(copy);
                ctx->rc = rc;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
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
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
            }
            rc = col_rel_append_all(target, result.rel, sess->eval_arena);
            if (result.owned)
                col_rel_destroy(result.rel);
            if (rc != 0) {
                ctx->rc = rc;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }
        }
    }

    /* Issue #361: Clear delta relations instead of removing them.
     * Pre-installed $d$ persists across iterations; nrows=0 triggers
     * has_empty_forced_delta skip, same as absent $d$. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        col_rel_t *d = session_find_rel(sess, dname);
        if (d) {
            d->nrows = 0;
            free(d->timestamps);
            d->timestamps = NULL;
        }
    }

    /* Consolidate + produce deltas (eval.c:621-713).
     * Use col_rel_new_like (heap) so deltas survive delta_pool_reset. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || snap[ri] >= r->nrows)
            continue;

        const char *dname = sp->relations[ri].delta_name;

        /* Heap-allocate delta so it survives delta_pool_reset below. */
        col_rel_t *delta = col_rel_new_like(dname, r);
        if (!delta) {
            ctx->rc = ENOMEM;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->tdd_outbound_only_active = saved_outbound_only;
            sess->diff_operators_active = saved_diff;
            TDD_WORKER_RETURN();
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
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }
            uint32_t keep = snap[ri];
            for (uint32_t i = snap[ri]; i < r->nrows; i++) {
                uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(r, i);
                if (WL_COLUMNAR_EVAL_DEDUP_SET_INSERT(r, h)) {
                    /* New row: compact into [keep] and emit to delta. */
                    if (keep != i)
                        col_rel_row_move(r, keep, i);
                    for (uint32_t c = 0; c < r->ncols; c++)
                        rbuf[c] = r->columns[c][keep];
                    rc2 = col_rel_append_row(delta, rbuf);
                    if (rc2 != 0)
                        break;
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

        /* rc2 != 0 propagates as a worker error so any_new is not set.
         * Sources: col_op_consolidate_incremental_delta (EOVERFLOW/ENOMEM)
         * or col_rel_append_row (ENOMEM) from the hash-set dedup path.
         * Both are hard errors requiring coordinator intervention. */
        if (rc2 != 0) {
            col_rel_destroy(delta);
            ctx->rc = rc2;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->tdd_outbound_only_active = saved_outbound_only;
            sess->diff_operators_active = saved_diff;
            TDD_WORKER_RETURN();
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
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
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
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
            }

            /* Issue #410, Commit 5: Queue-only transport.
             * delta ownership transfers to queue; coordinator reconstructs
             * ctxs via wl_columnar_eval_tdd_queue_reconstruct_delta_matrix
             * after barrier.
             * Fallback to ctx write when queue unavailable (alloc failure). */
            if (sess->coordinator && sess->coordinator->delta_queue) {
                int enq_rc = wl_mpsc_enqueue(
                    sess->coordinator->delta_queue,
                    sess->worker_id, delta, ctx->stratum_idx, ri);
                if (enq_rc != 0) {
                    /* Queue full — signal error; destroy orphaned delta. */
                    col_rel_destroy(delta);
                    ctx->rc = ENOMEM;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
            } else {
                /* No queue (creation failed): fall back to direct ctx write. */
                ctx->delta_rels[ri] = delta;
            }
            any_new = true;
        } else {
            col_rel_destroy(delta);
        }
    }

    free(snap);

    /* Reset per-sub-pass allocators and cache (eval.c:716-727) */
    delta_pool_reset(sess->delta_pool);
    sess->rotation_ops->rotate_eval_arena(sess);
    if (sess->cache_evict_threshold == 0) {
        col_mat_cache_clear(&sess->mat_cache);
    } else {
        col_mat_cache_evict_until(&sess->mat_cache,
            sess->cache_evict_threshold);
    }

    ctx->any_new = any_new;
    sess->tdd_subpass_active = saved_tdd_subpass;
    sess->tdd_outbound_only_active = saved_outbound_only;
    sess->diff_operators_active = saved_diff;
    ctx->runtime_ns = now_ns() - worker_t0;
#undef TDD_WORKER_RETURN
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
            /* Fall through to the sorted dedup below. */
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
 * Kept with external linkage for compatibility with the existing non-header
 * symbol.  The BDX coordinator path no longer calls it because BDX deltas are
 * not guaranteed to arrive sorted.
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
                cmp = -1;
                break;
            }
            if (a > b) {
                cmp = 1;
                break;
            }
        }
        if (cmp <= 0) {
            col_columns_copy_row(dst->columns, wr,
                (int64_t *const *)dst->merge_columns, i, ncols);
            i++;
            wr++;
        } else {
            col_columns_copy_row(dst->columns, wr,
                (int64_t *const *)src->columns, j, ncols);
            j++;
            wr++;
        }
    }
    while (i < N) {
        col_columns_copy_row(dst->columns, wr,
            (int64_t *const *)dst->merge_columns, i, ncols);
        i++;
        wr++;
    }
    while (j < D) {
        col_columns_copy_row(dst->columns, wr,
            (int64_t *const *)src->columns, j, ncols);
        j++;
        wr++;
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
    const char *dname = sp->relations[ri].delta_name;

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
        uint64_t t0 = now_ns();
        for (uint32_t ri = 0; rc == 0 && ri < nrels; ri++)
            rc = tdd_broadcast_relation_delta(sp, ri, coord, ctxs, W);
        coord->tdd_exchange_broadcast_ns += now_ns() - t0;
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
    if (!has_exchange && !default_hash) {
        uint64_t t0 = now_ns();
        int rc = tdd_broadcast_deltas(sp, coord, ctxs, W);
        coord->tdd_exchange_broadcast_ns += now_ns() - t0;
        return rc;
    }

    /* Replicate mode: all workers hold identical data, so hash scatter/gather
     * would produce W× duplicate deltas.  Force broadcast to avoid bloat. */
    if (!default_hash) {
        uint64_t t0 = now_ns();
        int rc = tdd_broadcast_deltas(sp, coord, ctxs, W);
        coord->tdd_exchange_broadcast_ns += now_ns() - t0;
        return rc;
    }

    /* Hash-partitioned scatter/gather exchange. */
    uint64_t matrix_t0 = now_ns();
    int rc = tdd_alloc_exchange_bufs(coord, W);
    coord->tdd_exchange_matrix_ns += now_ns() - matrix_t0;
    if (rc != 0) {
        for (uint32_t w = 0; w < W; w++)
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
            }
        return rc;
    }

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;

        /* Locate EXCHANGE key columns for this relation (if any). */
        uint64_t prepare_t0 = now_ns();
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
        coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;

        if (!key_cols || key_count == 0) {
            /* Broadcast: union worker deltas, install on every worker.
             * Used for replicate-mode strata where IDB is not partitioned. */
            uint64_t broadcast_t0 = now_ns();
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
            coord->tdd_exchange_broadcast_ns += now_ns() - broadcast_t0;
        } else {
            /* Hash-partitioned scatter/gather for EXCHANGE-keyed relations */
            uint64_t scatter_t0 = now_ns();
            for (uint32_t w = 0; w < W; w++) {
                col_rel_t *d = ctxs[w].delta_rels[ri];
                ctxs[w].delta_rels[ri] = NULL;

                if (!d || d->nrows == 0 || d->ncols == 0) {
                    col_rel_destroy(d);
                    continue;
                }

                rc = col_rel_exchange_partition(d, key_cols, key_count,
                        W, coord->exchange_bufs[w]);
                col_rel_destroy(d);

                if (rc != 0)
                    goto exchange_done;
            }
            coord->tdd_exchange_scatter_ns += now_ns() - scatter_t0;

            /* Gather: worker dst receives exchange_bufs[*][dst]. */
            uint64_t gather_t0 = now_ns();
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
            coord->tdd_exchange_gather_ns += now_ns() - gather_t0;
        }

        /* Release exchange_bufs[*][*] for this relation — data was copied
         * into the gathered relations above. */
        uint64_t matrix_release_t0 = now_ns();
        for (uint32_t src = 0; src < W; src++) {
            for (uint32_t dst = 0; dst < W; dst++) {
                col_rel_destroy(coord->exchange_bufs[src][dst]);
                coord->exchange_bufs[src][dst] = NULL;
            }
        }
        coord->tdd_exchange_matrix_ns += now_ns() - matrix_release_t0;
    }

exchange_done:
    matrix_t0 = now_ns();
    tdd_free_exchange_bufs(coord);
    coord->tdd_exchange_matrix_ns += now_ns() - matrix_t0;
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
tdd_init_workers_hybrid(const wl_plan_stratum_t *sp, wl_col_session_t *coord,
    bool partition_edb, uint32_t W)
{
    tdd_cleanup_workers(coord);

    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    ensure_rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);
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

    if (partition_edb) {
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
    }

    /* Issue #535 hardening: if only one of the two co-partitioned sides carries
     * has_graph_column, col_rel_exchange_partition would pick different hash
     * keys for IDB and EDB → matching tuples land on different workers → silent
     * wrong results.  Detect the asymmetry here and force both sides onto the
     * natural join key (Option A: warn + fallback). */
    bool force_natural_key = false;
    if (edb_part_name) {
        bool idb_graph = false;
        bool edb_graph = false;
        const char *idb_name_found = NULL;
        const char *edb_name_found = NULL;
        for (uint32_t r = 0; r < nrels; r++) {
            col_rel_t *rel = coord->rels[r];
            if (!rel)
                continue;
            /* IDB: appears in sp->relations */
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                if (strcmp(rel->name, sp->relations[ri].name) == 0) {
                    idb_graph = rel->has_graph_column;
                    idb_name_found = rel->name;
                    break;
                }
            }
            /* EDB co-partition target */
            if (strcmp(rel->name, edb_part_name) == 0) {
                edb_graph = rel->has_graph_column;
                edb_name_found = rel->name;
            }
        }
        if (idb_name_found && edb_name_found
            && idb_graph != edb_graph) {
            WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_WARN,
                "EDB/IDB graph-column flag mismatch for rel '%s' vs '%s';"
                " falling back to natural key for both sides to preserve"
                " co-partitioning correctness",
                idb_name_found, edb_name_found);
            force_natural_key = true;
        }
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
                /* When graph-flag mismatch detected, bypass the graph-key
                 * override in col_rel_exchange_partition and use the natural
                 * join key directly so both sides hash identically. */
                if (force_natural_key)
                    rc = col_rel_partition_by_key(rel, key, key_count,
                            W, parts);
                else
                    rc = col_rel_exchange_partition(rel, key, key_count,
                            W, parts);
                if (rc == 0) {
                    for (uint32_t w = 0; w < W && rc == 0; w++) {
                        free(parts[w]->name);
                        parts[w]->name = wl_strdup(name);
                        if (!parts[w]->name) {
                            rc = ENOMEM;
                        } else {
                            /* Init hash-set dedup for O(1) consolidation. */
                            WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL(parts[w]);
                            worker_rels[w][rels_built] = parts[w];
                            parts[w] = NULL;
                        }
                    }
                }
                for (uint32_t w = 0; w < W; w++)
                    col_rel_destroy(parts[w]);
                free((void *)parts);
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
                /* When graph-flag mismatch detected, bypass the graph-key
                 * override and use the supplied EDB key directly. */
                if (force_natural_key)
                    rc = col_rel_partition_by_key(rel, edb_part_keys,
                            edb_part_key_count, W, parts);
                else
                    rc = col_rel_exchange_partition(rel, edb_part_keys,
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
                free((void *)parts);
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
                    WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL(view);
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
        const char *dname = sp->relations[ri].delta_name;

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

static bool
tdd_estimate_add_rel_name(const char **names, uint32_t *count, uint32_t cap,
    const char *name)
{
    if (!name || !names || !count)
        return true;
    for (uint32_t i = 0; i < *count; i++) {
        if (names[i] && strcmp(names[i], name) == 0)
            return true;
    }
    if (*count >= cap)
        return false;
    names[(*count)++] = name;
    return true;
}

static bool
tdd_estimate_collect_ops(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp, const char **names, uint32_t *count,
    uint32_t cap)
{
    (void)sp;
    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        const char *rel_name = NULL;
        if (op->op == WL_PLAN_OP_VARIABLE) {
            rel_name = op->relation_name;
        } else if (op->op == WL_PLAN_OP_JOIN
            || op->op == WL_PLAN_OP_SEMIJOIN
            || op->op == WL_PLAN_OP_ANTIJOIN) {
            rel_name = op->right_relation;
        } else if (op->op == WL_PLAN_OP_K_FUSION && op->opaque_data) {
            const wl_plan_op_k_fusion_t *kf =
                (const wl_plan_op_k_fusion_t *)op->opaque_data;
            for (uint32_t ki = 0; ki < kf->k; ki++) {
                if (!tdd_estimate_collect_ops(kf->k_ops[ki],
                    kf->k_op_counts[ki], sp, names, count, cap))
                    return false;
            }
        } else if (op->op == WL_PLAN_OP_LFTJ && op->opaque_data) {
            const wl_plan_op_lftj_t *lftj =
                (const wl_plan_op_lftj_t *)op->opaque_data;
            for (uint32_t i = 0; i < lftj->k; i++) {
                if (!tdd_estimate_add_rel_name(names, count, cap,
                    lftj->rel_names ? lftj->rel_names[i] : NULL))
                    return false;
            }
        }
        if (!tdd_estimate_add_rel_name(names, count, cap, rel_name))
            return false;
    }
    return true;
}

static uint64_t
tdd_estimate_stratum_work_rows(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    const char *names[256];
    uint32_t name_count = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        if (!tdd_estimate_add_rel_name(names, &name_count, 256,
            sp->relations[ri].name))
            goto fallback;
        if (!tdd_estimate_collect_ops(sp->relations[ri].ops,
            sp->relations[ri].op_count, sp, names, &name_count, 256))
            goto fallback;
    }

    uint64_t rows = 0;
    for (uint32_t i = 0; i < name_count; i++) {
        col_rel_t *r = session_find_rel(coord, names[i]);
        if (r)
            rows += r->nrows;
    }
    return rows;

fallback:
    rows = 0;
    for (uint32_t ri = 0; ri < coord->nrels; ri++) {
        col_rel_t *r = coord->rels[ri];
        if (r)
            rows += r->nrows;
    }
    return rows;
}

static uint32_t
tdd_choose_active_workers(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t max_workers, bool replicate_mode)
{
    if (max_workers <= 1)
        return 1;

    const char *env = getenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    uint64_t rows_per_worker = 4096;
    if (env && env[0] != '\0') {
        char *endp = NULL;
        errno = 0;
        unsigned long long v = strtoull(env, &endp, 10);
        if (endp != env && *endp == '\0' && errno != ERANGE && v > 0)
            rows_per_worker = (uint64_t)v;
    }

    uint64_t rows = tdd_estimate_stratum_work_rows(sp, coord);
    uint32_t active = 1;
    if (rows > 0) {
        uint64_t wanted = (rows + rows_per_worker - 1) / rows_per_worker;
        if (wanted > UINT32_MAX)
            wanted = UINT32_MAX;
        active = (uint32_t)wanted;
    }

    if (replicate_mode && active > 8)
        active = 8;
    if (!replicate_mode) {
        uint32_t max_active = 32;
        env = getenv("WIRELOG_TDD_MAX_ACTIVE_WORKERS");
        if (env && env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            unsigned long v = strtoul(env, &endp, 10);
            if (endp != env && *endp == '\0' && errno != ERANGE && v > 0
                && v <= UINT32_MAX)
                max_active = (uint32_t)v;
        }
        if (active > max_active)
            active = max_active;
    }
    if (active < 1)
        active = 1;
    if (active > max_workers)
        active = max_workers;
    return active;
}

static uint32_t
tdd_global_read_worker_cap(void)
{
    uint32_t cap = 16;
    const char *env = getenv("WIRELOG_TDD_GLOBAL_READ_MAX_ACTIVE_WORKERS");
    if (env && env[0] != '\0') {
        char *endp = NULL;
        errno = 0;
        unsigned long v = strtoul(env, &endp, 10);
        if (endp != env && *endp == '\0' && errno != ERANGE && v > 0
            && v <= UINT32_MAX)
            cap = (uint32_t)v;
    }
    return cap;
}

/*
 * bdx_hash_diff:
 * Remove from delta any row that already exists in base.  This exact
 * hash-table diff does not require sorted inputs, which matches the current
 * BDX data flow where worker deltas are appended in partition order and
 * tdd_dedup_rel() may preserve unsorted encounter order.
 */
static int
bdx_hash_diff(col_rel_t *delta, const col_rel_t *base)
{
    if (!delta || delta->nrows == 0 || !base || base->nrows == 0)
        return 0;
    if (delta->ncols != base->ncols)
        return EINVAL;
    if (base->nrows > UINT32_MAX - 1)
        return ENOMEM;

    size_t target = (size_t)base->nrows * 2;
    size_t cap_sz = 4;
    while (cap_sz < target) {
        if (cap_sz > SIZE_MAX / 2)
            return ENOMEM;
        cap_sz <<= 1;
    }
    if (cap_sz > UINT32_MAX)
        return ENOMEM;

    uint32_t cap = (uint32_t)cap_sz;
    uint32_t mask = cap - 1;
    uint32_t *slots = (uint32_t *)malloc(cap * sizeof(uint32_t));
    if (!slots)
        return ENOMEM;
    memset(slots, 0, cap * sizeof(uint32_t));

    uint32_t ncols = base->ncols;
    for (uint32_t i = 0; i < base->nrows; i++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(base, i);
        uint32_t slot = (uint32_t)(h & mask);
        while (slots[slot] != 0)
            slot = (slot + 1) & mask;
        slots[slot] = i + 1;
    }

    uint32_t wr = 0;
    for (uint32_t di = 0; di < delta->nrows; di++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(delta, di);
        uint32_t slot = (uint32_t)(h & mask);
        bool found = false;
        while (slots[slot] != 0) {
            uint32_t bi = slots[slot] - 1;
            bool eq = true;
            for (uint32_t c = 0; c < ncols; c++) {
                if (base->columns[c][bi] != delta->columns[c][di]) {
                    eq = false;
                    break;
                }
            }
            if (eq) {
                found = true;
                break;
            }
            slot = (slot + 1) & mask;
        }
        if (found)
            continue;
        if (wr != di) {
            col_columns_copy_row(delta->columns, wr,
                (int64_t *const *)delta->columns, di, ncols);
            if (delta->timestamps)
                delta->timestamps[wr] = delta->timestamps[di];
        }
        wr++;
    }

    free(slots);
    delta->nrows = wr;
    return 0;
}

static int
tdd_hashset_diff(col_rel_t *delta, const col_rel_t *base)
{
    if (!delta || delta->nrows == 0 || !base || base->nrows == 0)
        return 0;
    if (delta->ncols != base->ncols)
        return EINVAL;
    if (!base->dedup_slots)
        return bdx_hash_diff(delta, base);

    uint32_t wr = 0;
    for (uint32_t di = 0; di < delta->nrows; di++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(delta, di);
        if (WL_COLUMNAR_EVAL_DEDUP_SET_CONTAINS(base, h))
            continue;
        if (wr != di) {
            col_columns_copy_row(delta->columns, wr,
                (int64_t *const *)delta->columns, di, delta->ncols);
            if (delta->timestamps)
                delta->timestamps[wr] = delta->timestamps[di];
        }
        wr++;
    }
    delta->nrows = wr;
    return 0;
}

static void
tdd_dedup_set_insert_rel(col_rel_t *target, const col_rel_t *rows)
{
    if (!target || !target->dedup_slots || !rows)
        return;
    for (uint32_t row = 0; row < rows->nrows; row++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(rows, row);
        WL_COLUMNAR_EVAL_DEDUP_SET_INSERT(target, h);
    }
}

static void
tdd_clear_relation_dedup_set(col_rel_t *r)
{
    if (!r)
        return;
    free(r->dedup_slots);
    r->dedup_slots = NULL;
    r->dedup_cap = 0;
    r->dedup_count = 0;
}

static col_rel_t **
tdd_save_coord_idb(const wl_plan_stratum_t *sp, wl_col_session_t *coord)
{
    col_rel_t **saved = (col_rel_t **)calloc(
        sp->relation_count, sizeof(col_rel_t *));
    if (!saved)
        return NULL;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        col_rel_t *r = session_find_rel(coord, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        saved[ri] = col_rel_new_auto(r->name, r->ncols);
        if (!saved[ri])
            goto fail;
        if (r->nrows > 0 && col_rel_append_all(saved[ri], r, NULL) != 0)
            goto fail;
    }
    return saved;

fail:
    for (uint32_t ri = 0; ri < sp->relation_count; ri++)
        col_rel_destroy(saved[ri]);
    free((void *)saved);
    return NULL;
}

static int
tdd_restore_coord_idb(const wl_plan_stratum_t *sp, wl_col_session_t *coord,
    col_rel_t **saved)
{
    if (!saved)
        return 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const char *name = sp->relations[ri].name;
        col_rel_t *r = session_find_rel(coord, name);
        if (!r) {
            int alloc_rc = col_rel_alloc(&r, name);
            if (alloc_rc != 0)
                return alloc_rc;
            alloc_rc = session_add_rel(coord, r);
            if (alloc_rc != 0) {
                col_rel_destroy(r);
                return alloc_rc;
            }
        }
        r->nrows = 0;
        r->sorted_nrows = 0;
        r->run_count = 0;
        memset(r->run_ends, 0, sizeof(r->run_ends));
        free(r->timestamps);
        r->timestamps = NULL;
        tdd_clear_relation_dedup_set(r);
        if (saved[ri] && saved[ri]->ncols > 0) {
            if (r->ncols == 0) {
                int rc = col_rel_set_schema(r, saved[ri]->ncols,
                        (const char *const *)saved[ri]->col_names);
                if (rc != 0)
                    return rc;
            }
            int rc = col_rel_append_all(r, saved[ri], NULL);
            if (rc != 0)
                return rc;
        }
        col_session_invalidate_arrangements(&coord->base, name);
    }
    return 0;
}

static void
tdd_free_saved_coord_idb(const wl_plan_stratum_t *sp, col_rel_t **saved)
{
    if (!saved)
        return;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++)
        col_rel_destroy(saved[ri]);
    free((void *)saved);
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
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;

        uint64_t prepare_t0 = now_ns();

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
            coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;
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
        if (coord_idb && coord_idb->nrows > 0 && combined->nrows > 0) {
            rc = tdd_hashset_diff(combined, coord_idb);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }

        if (combined->nrows == 0) {
            col_rel_destroy(combined);
            coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;
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
            rc = col_rel_append_all(coord_idb, combined, NULL);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            tdd_dedup_set_insert_rel(coord_idb, combined);
            col_session_invalidate_arrangements(&coord->base, rel_name);
        }

        /* Step 4: Truncate worker IDB to pre-subpass snapshot */
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *widb = session_find_rel(
                &coord->tdd_workers[w], rel_name);
            if (widb)
                widb->nrows = snap[w * nrels + ri];
        }
        coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;

        /* Step 5: Hash-partition combined_delta by EXCHANGE key,
         * append to correct worker's IDB */
        uint64_t scatter_t0 = now_ns();
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
        rc = col_rel_exchange_partition(combined, key_cols, key_count, W,
                parts);
        if (rc != 0) {
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free((void *)parts);
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
                            uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(
                                parts[w], row);
                            WL_COLUMNAR_EVAL_DEDUP_SET_INSERT(widb, h);
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
        free((void *)parts);
        coord->tdd_exchange_scatter_ns += now_ns() - scatter_t0;

        /* Step 8: Broadcast combined_delta as $d$ via col_shared zero-copy */
        uint64_t broadcast_t0 = now_ns();
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
        coord->tdd_exchange_broadcast_ns += now_ns() - broadcast_t0;
    }

    return 0;
}

static int
tdd_owner_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    bool *out_any_accepted, uint32_t *out_accepted_rows)
{
    uint32_t nrels = sp->relation_count;
    int rc = 0;
    if (out_any_accepted)
        *out_any_accepted = false;
    if (out_accepted_rows)
        *out_accepted_rows = 0;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;
        uint64_t prepare_t0 = now_ns();

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
            coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;
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

        if (combined->nrows > 1)
            tdd_dedup_rel(combined);

        col_rel_t *coord_idb = session_find_rel(coord, rel_name);
        if (coord_idb && coord_idb->nrows > 0 && combined->nrows > 0) {
            rc = tdd_hashset_diff(combined, coord_idb);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }
        if (combined->nrows == 0) {
            rc = tdd_install_empty_delta_on_workers(coord, dname, ncols, W);
            col_rel_destroy(combined);
            coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;
            if (rc != 0)
                return rc;
            continue;
        }
        if (out_any_accepted)
            *out_any_accepted = true;
        if (out_accepted_rows)
            *out_accepted_rows += combined->nrows;

        if (coord_idb) {
            if (coord_idb->ncols == 0 && combined->ncols > 0) {
                rc = col_rel_set_schema(coord_idb, combined->ncols,
                        (const char *const *)combined->col_names);
                if (rc != 0) {
                    col_rel_destroy(combined);
                    return rc;
                }
            }
            rc = col_rel_append_all(coord_idb, combined, NULL);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            tdd_dedup_set_insert_rel(coord_idb, combined);
            col_session_invalidate_arrangements(&coord->base, rel_name);
        }
        coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;

        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta =
                    (const wl_plan_op_exchange_t *)
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

        uint64_t scatter_t0 = now_ns();
        col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!parts) {
            col_rel_destroy(combined);
            return ENOMEM;
        }
        rc = col_rel_exchange_partition(combined, key_cols, key_count, W,
                parts);
        col_rel_destroy(combined);
        if (rc != 0) {
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free((void *)parts);
            return rc;
        }

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *part = parts[w];
            parts[w] = NULL;
            if (!part || part->nrows == 0) {
                col_rel_destroy(part);
                continue;
            }

            col_rel_t *widb = session_find_rel(
                &coord->tdd_workers[w], rel_name);
            if (widb) {
                if (widb->ncols == 0 && part->ncols > 0) {
                    rc = col_rel_set_schema(widb, part->ncols,
                            (const char *const *)part->col_names);
                    if (rc != 0) {
                        col_rel_destroy(part);
                        break;
                    }
                }
                rc = col_rel_append_all(widb, part, NULL);
                if (rc != 0) {
                    col_rel_destroy(part);
                    break;
                }
                if (widb->dedup_slots) {
                    for (uint32_t row = 0; row < part->nrows; row++) {
                        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(
                            part, row);
                        WL_COLUMNAR_EVAL_DEDUP_SET_INSERT(widb, h);
                    }
                }
                col_session_invalidate_arrangements(
                    &coord->tdd_workers[w].base, rel_name);
            }

            free(part->name);
            part->name = wl_strdup(dname);
            if (!part->name) {
                col_rel_destroy(part);
                rc = ENOMEM;
                break;
            }
            rc = session_add_rel(&coord->tdd_workers[w], part);
            if (rc != 0) {
                col_rel_destroy(part);
                break;
            }
        }

        for (uint32_t w = 0; w < W; w++)
            col_rel_destroy(parts[w]);
        free((void *)parts);
        coord->tdd_exchange_scatter_ns += now_ns() - scatter_t0;
        if (rc != 0)
            return rc;
    }

    return 0;
}

static int
tdd_global_read_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    bool *out_any_accepted, uint32_t *out_accepted_rows)
{
    uint32_t nrels = sp->relation_count;
    int rc = 0;
    if (out_any_accepted)
        *out_any_accepted = false;
    if (out_accepted_rows)
        *out_accepted_rows = 0;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;

        for (uint32_t w = 0; w < W; w++)
            session_remove_rel(&coord->tdd_workers[w], dname);

        uint32_t total = 0;
        uint32_t ncols = 0;
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

        if (combined->nrows > 1)
            tdd_dedup_rel(combined);

        col_rel_t *coord_idb = session_find_rel(coord, rel_name);
        if (coord_idb && coord_idb->nrows > 0 && combined->nrows > 0) {
            rc = tdd_hashset_diff(combined, coord_idb);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }
        if (combined->nrows == 0) {
            rc = tdd_install_empty_delta_on_workers(coord, dname, ncols, W);
            col_rel_destroy(combined);
            if (rc != 0)
                return rc;
            continue;
        }

        if (out_any_accepted)
            *out_any_accepted = true;
        if (out_accepted_rows)
            *out_accepted_rows += combined->nrows;

        if (coord_idb) {
            if (coord_idb->ncols == 0 && combined->ncols > 0) {
                rc = col_rel_set_schema(coord_idb, combined->ncols,
                        (const char *const *)combined->col_names);
                if (rc != 0) {
                    col_rel_destroy(combined);
                    return rc;
                }
            }
            rc = col_rel_append_all(coord_idb, combined, NULL);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            tdd_dedup_set_insert_rel(coord_idb, combined);
            col_session_invalidate_arrangements(&coord->base, rel_name);
            rc = tdd_refresh_global_read_relation(sp, coord, ri, W);
            if (rc != 0) {
                if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG")) {
                    fprintf(stderr,
                        "TDD global-read refresh error rel=%s cols=%u rows=%u rc=%d\n",
                        rel_name, coord_idb->ncols, coord_idb->nrows, rc);
                }
                col_rel_destroy(combined);
                return rc;
            }
        }

        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta =
                    (const wl_plan_op_exchange_t *)
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
        bool key_valid = combined->ncols > 0;
        for (uint32_t ki = 0; key_valid && ki < key_count; ki++) {
            if (key_cols[ki] >= combined->ncols)
                key_valid = false;
        }
        if (!key_valid) {
            key_cols = default_key;
            key_count = 1;
        }

        col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!parts) {
            col_rel_destroy(combined);
            return ENOMEM;
        }
        uint32_t combined_cols = combined->ncols;
        uint32_t combined_rows = combined->nrows;
        uint32_t debug_key0 = key_count > 0 ? key_cols[0] : 0;
        rc = col_rel_exchange_partition(combined, key_cols, key_count, W,
                parts);
        col_rel_destroy(combined);
        if (rc != 0) {
            if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG")) {
                fprintf(stderr,
                    "TDD global-read partition error rel=%s cols=%u rows=%u "
                    "key_count=%u key0=%u rc=%d\n",
                    rel_name, combined_cols, combined_rows, key_count,
                    debug_key0, rc);
            }
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free((void *)parts);
            return rc;
        }

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *part = parts[w];
            parts[w] = NULL;
            if (!part || part->nrows == 0) {
                col_rel_destroy(part);
                continue;
            }
            free(part->name);
            part->name = wl_strdup(dname);
            if (!part->name) {
                col_rel_destroy(part);
                rc = ENOMEM;
                break;
            }
            rc = session_add_rel(&coord->tdd_workers[w], part);
            if (rc != 0) {
                col_rel_destroy(part);
                break;
            }
        }
        for (uint32_t w = 0; w < W; w++)
            col_rel_destroy(parts[w]);
        free((void *)parts);
        if (rc != 0)
            return rc;
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
    uint64_t tdd_total_t0 = now_ns();

    /* Pre-register empty IDB relations on coordinator (eval.c:291-304) */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (session_find_rel(coord, sp->relations[ri].name))
            continue;
        col_rel_t *empty = NULL;
        int alloc_rc = col_rel_alloc(&empty, sp->relations[ri].name);
        if (alloc_rc != 0) {
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return ENOMEM;
        }
        alloc_rc = session_add_rel(coord, empty);
        if (alloc_rc != 0) {
            col_rel_destroy(empty);
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
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
     * Issue #372: Skip clearing on first snapshot (has_evaluated == false):
     * IDB relations may hold EDB seeds (e.g. r(1,2) for r(x,z):-r(x,y),r(y,z))
     * that must survive into the first evaluation pass. */
    if (coord->last_inserted_relation != NULL
        && coord->has_evaluated) {
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
     *     - Stratum is not exchange-aligned AND has >2 IDB body atoms (BDX unsafe).
     *
     * Issue #388: Optional W=1 fallback for replicate mode only. */
    bool has_idb_self_join = tdd_stratum_has_idb_self_join(sp);
    bool self_join_mode = tdd_stratum_idb_self_join_exchange_aligned(sp, coord);
    bool owner_exchange_mode = !has_idb_self_join
        && stratum_max_idb_body_atoms(sp) <= 1
        && tdd_stratum_single_idb_join_keys_exchange_aligned(sp);
    bool bdx_mode = has_idb_self_join && !self_join_mode;
    const char *global_read_env = getenv("WIRELOG_TDD_GLOBAL_READ");
    bool global_read_mode = !(global_read_env && global_read_env[0] == '0'
        && global_read_env[1] == '\0')
        && !owner_exchange_mode && !self_join_mode && !bdx_mode
        && tdd_stratum_global_read_candidate(sp);
    bool replicate_mode = !owner_exchange_mode
        && !global_read_mode
        && ((coord->last_inserted_relation == NULL)
        || (!self_join_mode && stratum_max_idb_body_atoms(sp) > 2));
    bdx_mode = bdx_mode && !replicate_mode;
    if (replicate_mode) {
        const char *env = getenv("WIRELOG_TDD_REPLICATE_W1");
        if (env && env[0] == '1')
            W = 1;
    }
    W = tdd_choose_active_workers(sp, coord, W, replicate_mode);
    if (global_read_mode) {
        uint32_t cap = tdd_global_read_worker_cap();
        if (W > cap)
            W = cap;
    }
    if (W <= 1) {
        tdd_record_active_workers(coord, 1);
        if (coord->tdd_decision_tracking_active) {
            if (coord->tdd_executed_strata > 0)
                coord->tdd_executed_strata--;
            coord->tdd_fallback_strata++;
            coord->tdd_last_fallback_reason =
                WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS;
            coord->tdd_fallback_reason_counts[
                WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS]++;
        }
        uint32_t saved_total_iterations = coord->total_iterations;
        int seq_rc = col_eval_stratum(sp, coord, stratum_idx);
        if (seq_rc == 0 && saved_total_iterations > 0)
            coord->total_iterations += saved_total_iterations;
        return seq_rc;
    }

    col_rel_t **owner_fallback_saved = NULL;
    col_rel_t **global_read_saved = NULL;
    bool owner_adaptive_fallback = false;
    if (owner_exchange_mode) {
        owner_fallback_saved = tdd_save_coord_idb(sp, coord);
        if (!owner_fallback_saved) {
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return ENOMEM;
        }
    } else if (global_read_mode) {
        global_read_saved = tdd_save_coord_idb(sp, coord);
        if (!global_read_saved) {
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return ENOMEM;
        }
    }

    if (global_read_mode)
        rc = tdd_init_workers_global_read(coord, W);
    else if (replicate_mode)
        rc = tdd_replicate_workers(coord, W);
    else
        rc = tdd_init_workers_hybrid(sp, coord, true, W);
    if (rc != 0) {
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        return rc;
    }

    if (owner_exchange_mode) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(coord, sp->relations[ri].name);
            if (r && !r->dedup_slots) {
                rc = WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL(r);
                if (rc != 0) {
                    tdd_cleanup_workers(coord);
                    tdd_free_saved_coord_idb(sp, owner_fallback_saved);
                    tdd_free_saved_coord_idb(sp, global_read_saved);
                    coord->tdd_total_ns += now_ns() - tdd_total_t0;
                    return rc;
                }
            }
        }
    }

    /* Pre-register empty IDB on each worker */
    rc = tdd_preregister_idb_on_workers(sp, coord);
    if (rc != 0) {
        tdd_cleanup_workers(coord);
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
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
        const char *dname = sp->relations[ri].delta_name;
        col_rel_t *coord_rel = session_find_rel(coord,
                sp->relations[ri].name);
        uint32_t ncols_ri = coord_rel ? coord_rel->ncols : 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *slot = col_rel_new_auto(dname, ncols_ri);
            if (!slot) {
                tdd_cleanup_workers(coord);
                tdd_free_saved_coord_idb(sp, owner_fallback_saved);
                tdd_free_saved_coord_idb(sp, global_read_saved);
                coord->tdd_total_ns += now_ns() - tdd_total_t0;
                return ENOMEM;
            }
            rc = session_add_rel(&coord->tdd_workers[w], slot);
            if (rc != 0) {
                col_rel_destroy(slot);
                tdd_cleanup_workers(coord);
                tdd_free_saved_coord_idb(sp, owner_fallback_saved);
                tdd_free_saved_coord_idb(sp, global_read_saved);
                coord->tdd_total_ns += now_ns() - tdd_total_t0;
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
    /* Keep this initialized before any goto done path. */
    uint32_t *bdx_snap = NULL;
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

    /* Issue #410: Create MPSC delta queue for dual-write transport.
     * Capacity = W × nrels × 2 (2x headroom; at most W×nrels per sub-pass).
     * Failure is non-fatal: enqueue is skipped when delta_queue is NULL. */
    coord->delta_queue = wl_mpsc_queue_create(W, nrels * 2 < 2 ? 2 : nrels * 2);

    /* Issue #390: BDX snap array — pre-subpass IDB sizes per worker/relation.
     * Used to truncate worker IDB back to clean partition state after each
     * sub-pass (removes cross-partition pollution from join output). */
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
            const char *dname = sp->relations[ri].delta_name;
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

    if (global_read_mode) {
        rc = tdd_seed_global_read_initial_deltas(sp, coord, W);
        if (rc != 0)
            goto done;
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
                memset((void *)ctxs[w].delta_rels, 0,
                    nrels * sizeof(col_rel_t *));
                ctxs[w].sp = sp;
                ctxs[w].worker_sess = &coord->tdd_workers[w];
                ctxs[w].stratum_idx = stratum_idx;
                ctxs[w].eff_iter = eff_iter;
                ctxs[w].any_new = false;
                ctxs[w].all_empty_delta = false;
                ctxs[w].force_diff = bdx_mode || global_read_mode;
                ctxs[w].outbound_only = owner_exchange_mode
                    || global_read_mode;
                ctxs[w].runtime_ns = 0;
                ctxs[w].rc = 0;
            }
            /* DISPATCH */
            bool submit_ok = true;
            uint64_t dispatch_t0 = now_ns();
            for (uint32_t w = 0; w < W; w++) {
                if (wl_workqueue_submit(coord->wq, tdd_worker_subpass_fn,
                    &ctxs[w]) != 0) {
                    wl_workqueue_drain(coord->wq);
                    submit_ok = false;
                    break;
                }
            }
            uint64_t submit_ns = now_ns() - dispatch_t0;
            coord->tdd_submit_loop_ns += submit_ns;

            if (!submit_ok) {
                wl_columnar_eval_tdd_queue_discard_delta_queue(
                    coord->delta_queue, W, nrels);
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                rc = ENOMEM;
                goto done;
            }

            /* BARRIER */
            uint64_t wait_t0 = now_ns();
            wl_workqueue_wait_all(coord->wq);
            uint64_t wait_ns = now_ns() - wait_t0;
            coord->tdd_wait_barrier_ns += wait_ns;
            coord->tdd_dispatch_wait_ns += submit_ns + wait_ns;
            uint64_t worker_sum_ns = 0;
            uint64_t worker_max_ns = 0;
            for (uint32_t w = 0; w < W; w++) {
                worker_sum_ns += ctxs[w].runtime_ns;
                if (ctxs[w].runtime_ns > worker_max_ns)
                    worker_max_ns = ctxs[w].runtime_ns;
            }
            coord->tdd_worker_sum_ns += worker_sum_ns;
            coord->tdd_worker_max_ns += worker_max_ns;
            if (wait_ns > worker_max_ns)
                coord->tdd_idle_estimate_ns += wait_ns - worker_max_ns;

            /* Collect first worker error */
            for (uint32_t w = 0; w < W; w++) {
                if (ctxs[w].rc != 0 && rc == 0)
                    rc = ctxs[w].rc;
            }

            if (rc != 0) {
                if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG"))
                    fprintf(stderr,
                        "TDD worker error stratum=%u iter=%u rc=%d\n",
                        stratum_idx, eff_iter, rc);
                wl_columnar_eval_tdd_queue_discard_delta_queue(
                    coord->delta_queue, W, nrels);
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                goto done;
            }

            /* Issue #410, Commit 4: Drain MPSC queue and reconstruct
             * ctxs[w].delta_rels[ri] via adapter.  Workers dual-write to both
             * ctx and queue; shadow assert verifies agreement (debug only). */
            if (coord->delta_queue) {
                uint64_t queue_t0 = now_ns();
                uint32_t max_msgs = W * nrels;
                wl_delta_msg_t *msgs = (wl_delta_msg_t *)calloc(
                    max_msgs > 0 ? max_msgs : 1u, sizeof(wl_delta_msg_t));
                if (msgs) {
                    uint32_t msg_count = wl_mpsc_dequeue_all(
                        coord->delta_queue, msgs, max_msgs);

                    /* Clear and reconstruct from queue messages. */
                    for (uint32_t w = 0; w < W; w++)
                        memset((void *)ctxs[w].delta_rels, 0,
                            nrels * sizeof(col_rel_t *));
                    wl_columnar_eval_tdd_queue_reconstruct_delta_matrix(
                        ctxs, msgs, msg_count, W, nrels);
                    free(msgs);
                }
                coord->tdd_queue_drain_ns += now_ns() - queue_t0;
            }

            uint64_t convergence_t0 = now_ns();

            /* Stratum-level early exit: all workers have all_empty_delta */
            bool all_workers_empty = true;
            for (uint32_t w = 0; w < W; w++) {
                if (!ctxs[w].all_empty_delta) {
                    all_workers_empty = false;
                    break;
                }
            }
            if (all_workers_empty) {
                coord->tdd_convergence_ns += now_ns() - convergence_t0;
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
            if (!owner_exchange_mode && !global_read_mode
                && tdd_check_convergence(ctxs, W)) {
                coord->tdd_convergence_ns += now_ns() - convergence_t0;
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                converged = true;
                break;
            }
            coord->tdd_convergence_ns += now_ns() - convergence_t0;

            /* EXCHANGE: BDX for Category C, hash scatter/gather for
             * standard hybrid, broadcast for replicate/self_join_mode.
             * Issue #372: pass self_join_mode so asymmetric strata broadcast
             * deltas to all workers (each holds 1/W IDB, needs full delta). */
            int brc;
            bool owner_any_accepted = false;
            uint32_t owner_accepted_rows = 0;
            {
                uint64_t t0 = now_ns();
                coord->current_iteration = eff_iter;
                if (owner_exchange_mode)
                    brc = tdd_owner_exchange_deltas(sp, coord, ctxs, W,
                            &owner_any_accepted, &owner_accepted_rows);
                else if (global_read_mode)
                    brc = tdd_global_read_exchange_deltas(sp, coord, ctxs, W,
                            &owner_any_accepted, &owner_accepted_rows);
                else if (bdx_mode)
                    brc = tdd_bdx_exchange_deltas(sp, coord, ctxs, W,
                            bdx_snap);
                else
                    brc = tdd_exchange_deltas(sp, coord, ctxs, W,
                            !replicate_mode, self_join_mode);
                uint64_t exchange_ns = now_ns() - t0;
                coord->exchange_time_ns += exchange_ns;
                coord->tdd_exchange_ns += exchange_ns;
            }

            if (brc != 0) {
                if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG"))
                    fprintf(stderr,
                        "TDD exchange error stratum=%u iter=%u rc=%d "
                        "owner=%d global=%d bdx=%d replicate=%d\n",
                        stratum_idx, eff_iter, brc, owner_exchange_mode,
                        global_read_mode, bdx_mode, replicate_mode);
                rc = brc;
                goto done;
            }

            /* Linear owner-mode can be slower than the sequential recursive
             * evaluator on high-diameter, tiny-frontier workloads: every
             * sub-pass pays a W-worker barrier and an exchange for only a few
             * accepted rows.  Bail out early and replay the stratum through
             * the existing single-threaded evaluator when that shape is
             * detected. */
            if (owner_exchange_mode
                && eff_iter >= TDD_OWNER_FALLBACK_MIN_ITER
                && owner_accepted_rows > 0
                && owner_accepted_rows < TDD_OWNER_FALLBACK_DELTA_ROWS) {
                owner_adaptive_fallback = true;
                converged = true;
                break;
            }

            if ((owner_exchange_mode || global_read_mode)
                && !owner_any_accepted) {
                converged = true;
                break;
            }

            outer_any_new = (owner_exchange_mode || global_read_mode)
                ? owner_any_accepted : true;
            if (outer_any_new)
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
    /* Issue #410: Destroy MPSC delta queue created for this stratum eval. */
    wl_mpsc_queue_destroy(coord->delta_queue);
    coord->delta_queue = NULL;

    /* Free pre-allocated worker contexts */
    if (ctxs) {
        for (uint32_t w = 0; w < W; w++)
            free((void *)ctxs[w].delta_rels);
        free(ctxs);
    }
    free(bdx_snap);
    coord->diff_operators_active = saved_diff;

    if (owner_adaptive_fallback && rc == 0) {
        rc = tdd_restore_coord_idb(sp, coord, owner_fallback_saved);
        tdd_cleanup_workers(coord);
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        if (rc != 0)
            return rc;
        coord->frontier_ops->reset_stratum_frontier(coord, stratum_idx,
            coord->outer_epoch);
        return col_eval_stratum(sp, coord, stratum_idx);
    }
    if (global_read_mode && rc == EOVERFLOW) {
        int restore_rc = tdd_restore_coord_idb(sp, coord, global_read_saved);
        tdd_cleanup_workers(coord);
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        if (restore_rc != 0)
            return restore_rc;
        coord->frontier_ops->reset_stratum_frontier(coord, stratum_idx,
            coord->outer_epoch);
        return col_eval_stratum(sp, coord, stratum_idx);
    }
    tdd_free_saved_coord_idb(sp, owner_fallback_saved);
    tdd_free_saved_coord_idb(sp, global_read_saved);

    /* Issue #416: Accumulate instead of assign so total_iterations stays > 0
     * after any completed evaluation.  Assigning final_eff_iter (which is 0
     * when a replicate-mode stratum converges with no new tuples) would
     * incorrectly reset the first-snapshot guard used by col_snapshot and
     * col_eval_stratum_tdd_recursive (total_iterations > 0). */
    coord->total_iterations += final_eff_iter;

    if (rc == 0) {
        /* Record stratum and per-rule frontiers (mirrors eval.c:768-791) */
        tdd_record_recursive_convergence(coord, sp, stratum_idx,
            rule_id_base, final_eff_iter);

        if (bdx_mode || owner_exchange_mode || global_read_mode) {
            /* Coordinator-owned modes maintain IDB monotonically during
             * exchange.  Just dedup final. */
            uint64_t merge_t0 = now_ns();
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(coord,
                        sp->relations[ri].name);
                if (r && r->nrows > 1)
                    tdd_dedup_rel(r);
            }
            rc = col_canonicalize_recursive_aggregates(sp, coord);
            coord->tdd_final_merge_ns += now_ns() - merge_t0;
        } else {
            /* Non-BDX: Merge worker IDB into coordinator */
            uint64_t merge_t0 = now_ns();
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
                rc = col_canonicalize_recursive_aggregates(sp, coord);
            }
            coord->tdd_final_merge_ns += now_ns() - merge_t0;
        }
    }

    tdd_cleanup_workers(coord);
    coord->tdd_total_ns += now_ns() - tdd_total_t0;
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

    W = tdd_choose_active_workers(sp, coord, W, false);
    if (W <= 1) {
        tdd_record_active_workers(coord, 1);
        return col_eval_stratum(sp, coord, stratum_idx);
    }

    /* Phase 1: PARTITION — partition all coordinator relations to workers */
    rc = tdd_init_workers(coord, W);
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

    /* Phase 6: CONSOLIDATE — merge worker IDB results to coordinator.
     * This is a serial coordinator phase: accumulate into exchange_time_ns
     * so serial_fraction / exchange_fraction accounts for non-recursive strata
     * as well as recursive exchange barriers. */
    if (rc == 0) {
        uint64_t t0 = now_ns();
        rc = tdd_merge_worker_results(sp, coord);
        coord->exchange_time_ns += now_ns() - t0;
    }

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

    if (tdd_stratum_has_unsupported_lftj(sp))
        return col_eval_stratum(sp, coord, stratum_idx);

    /* Single-worker fast path: zero overhead delegation */
    if (coord->num_workers <= 1)
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

/* ======================================================================== */
/* Inline compound helpers are implemented in columnar/inline.c. */
