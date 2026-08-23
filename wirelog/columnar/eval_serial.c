/*
 * columnar/eval_serial.c - serial stratum evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Serial (single-threaded) stratum evaluation extracted from
 * columnar/eval.c.
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

/* Distributed (TDD) stratum evaluation is implemented in columnar/eval.c. */
static col_frontier_t
col_frontier_compute(const col_rel_t *rel);

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

int
wl_columnar_eval_serial_canonicalize_aggregates(const wl_plan_stratum_t *sp,
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
            int par_rc = wl_columnar_eval_nonrec_relation_parallel(rp, sess);
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

    int agg_rc = wl_columnar_eval_serial_canonicalize_aggregates(sp, sess);
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
