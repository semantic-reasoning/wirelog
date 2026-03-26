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
        r->columns = NULL;
        r->capacity = 0;
        r->nrows = 0;
        r->sorted_nrows = 0;
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
            r->retract_backup_columns = NULL;
            r->retract_backup_nrows = 0;
            r->retract_backup_capacity = 0;
            r->retract_backup_sorted_nrows = 0;
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
                        r2->retract_backup_columns = NULL;
                        r2->retract_backup_nrows = 0;
                        r2->retract_backup_capacity = 0;
                        r2->retract_backup_sorted_nrows = 0;
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
        r->retract_backup_columns = NULL;
        r->retract_backup_nrows = 0;
        r->retract_backup_capacity = 0;
        r->retract_backup_sorted_nrows = 0;
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
