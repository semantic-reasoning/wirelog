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
            rc = col_op_join(op, stack, sess);
            break;
        case WL_PLAN_OP_ANTIJOIN:
            rc = col_op_antijoin(op, stack, sess);
            break;
        case WL_PLAN_OP_CONCAT:
            rc = col_op_concat(stack, sess);
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            rc = col_op_consolidate(stack, sess);
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
                    col_rel_t *copy = col_rel_new_like(rp->name, result.rel);
                    if (!copy)
                        return ENOMEM;
                    if ((rc = col_rel_append_all(copy, result.rel)) != 0) {
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
                rc = col_rel_append_all(target, result.rel);
                if (result.owned)
                    col_rel_destroy(result.rel);
                if (rc != 0)
                    return rc;
            }
        }
        col_mat_cache_clear(&sess->mat_cache);
        delta_pool_reset(sess->delta_pool);

        /* Non-recursive stratum frontier: record convergence epoch and iteration.
         * Non-recursive strata always converge at iteration UINT32_MAX (no loop),
         * so store (outer_epoch, UINT32_MAX) to enable epoch-aware skip on next
         * incremental call. Always update both fields so same-epoch skip logic
         * fires correctly when the frontier persists across session_step calls. */
        if (stratum_idx < MAX_STRATA) {
            col_frontier_2d_t f2d = { sess->outer_epoch, UINT32_MAX };
            sess->frontiers[stratum_idx] = f2d;
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
                    sess->rule_frontiers[rule_idx].outer_epoch
                        = sess->outer_epoch;
                    sess->rule_frontiers[rule_idx].iteration = UINT32_MAX;
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
            uint32_t nc = r->ncols;
            size_t row_bytes = (size_t)nc * sizeof(int64_t);
            QSORT_R_CALL(r->data, r->nrows, row_bytes, &nc, row_cmp_fn);
            r->sorted_nrows = r->nrows;
        }
    }

    /* Initialize recursive stratum frontier to UINT32_MAX (not set sentinel)
     * only on the first evaluation (frontier==0 from calloc). If a prior
     * col_session_insert_incremental call preserved a real convergence frontier,
     * keep it so the per-iteration skip condition fires for iterations beyond
     * the prior convergence point. */
    if (stratum_idx < MAX_STRATA
        && sess->frontiers[stratum_idx].iteration == 0) {
        sess->frontiers[stratum_idx].iteration = UINT32_MAX;
    }

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
    for (iter = 0; iter < MAX_ITERATIONS; iter++) {
        /* Publish current iteration so operators can distinguish base case
         * (iter 0: FORCE_DELTA falls back to full) from delta case
         * (iter > 0: FORCE_DELTA with absent delta → empty result). */
        sess->current_iteration = iter;

        /* Phase 3D-Ext-002 (DORMANT): Fine-grained frontier skip infrastructure.
         *
         * STATUS: This optimization is currently unreachable in the single-call evaluation
         * model (session_step, session_snapshot). Designed for future incremental re-evaluation
         * where the frontier persists across multiple calls without being reset.
         *
         * WHY UNREACHABLE (Phase 4): Non-recursive strata reset their frontier to (0, stratum_idx)
         * before recursive strata evaluate. Since each stratum initializes its frontier to iteration=0,
         * the per-stratum skip condition `iter > frontiers[stratum_idx].iteration` is always false
         * when evaluating the first session_step call. Becomes active only when frontier persists
         * across multiple session_step calls (incremental re-evaluation, Phase 4+).
         *
         * INTENDED SEMANTICS (when frontier persists across calls):
         * Skip iteration only if: iter > frontier.iteration AND frontier.stratum < current_stratum.
         * Both conditions must be true to skip safely. This prevents:
         * - Skipping iterations before frontier (data loss)
         * - Skipping stratum 0 (recursion entry point)
         * - Premature termination of recursive stratum evaluation
         *
         * LATENT CORRECTNESS BUG (if activated without semantic fix):
         * Cross-stratum iteration counter mismatch. Variable 'iter' is this stratum's local
         * fixed-point counter. Variable 'frontier.iteration' is from previous stratum's
         * convergence point. These are semantically unrelated values that happen to share
         * the name "iteration". Comparing them across strata is incorrect and could skip
         * needed work in multi-recursive-stratum programs, producing incomplete results.
         *
         * PHASE 4+ WORK (Incremental Evaluation):
         * Before activating this skip, implement one of:
         * 1. Per-stratum frontier tracking (store in array, indexed by stratum_idx)
         * 2. Same-stratum comparison only (only skip if frontier.stratum == stratum_idx)
         * 3. Change continue to break if stratum has converged beyond this point
         * See docs/3d-ext-incremental-eval-roadmap.md for design options.
         *
         * ARCHITECT REVIEW: Conditional approval (a2a42d1fa88a8d650).
         * Dormant status verified. Recommendation: document before activation.
         * See progress.txt Phase 3D-Ext section for full architect findings.
         */
        /* Phase 4: Per-stratum frontier skip condition (DORMANT in this context).
         * When frontier is reset to UINT32_MAX for affected strata, this skip
         * condition is ineffective (iter > UINT32_MAX is always false).
         * Skip logic only activates when frontier persists across multiple
         * incremental snapshots with small delta facts.
         *
         * ENABLED: for unaffected strata, skip iterations beyond frontier.
         * Affected strata (frontier=UINT32_MAX) naturally re-evaluate all iterations. */
        /* US-104-002: 2D frontier skip condition (epoch-aware).
         * Skip only when BOTH conditions hold:
         *   1. Same insertion epoch: frontier was set in this outer_epoch, so
         *      the convergence point is still valid for the current data set.
         *   2. Iteration beyond convergence: iter > frontier.iteration means
         *      this stratum already converged at a lower iteration count.
         * When epochs differ (new insertion cycle), outer_epoch mismatch means
         * the frontier is stale — do NOT skip, always re-evaluate from iter 0. */
        if (stratum_idx < MAX_STRATA) {
            bool same_epoch = (sess->outer_epoch
                               == sess->frontiers[stratum_idx].outer_epoch);
            bool beyond_convergence
                = (iter > sess->frontiers[stratum_idx].iteration);
            if (same_epoch && beyond_convergence) {
                continue; /* Skip: already processed at this iter in same epoch */
            }
        }

        /* Clear per-iteration delta arrangement cache (sequential eval path).
         * K-fusion workers manage their own darr caches independently. */
        col_session_free_delta_arrangements(sess);

        /* Register delta relations from previous iteration into session */
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

        /* Record per-relation row counts (O(1) snapshot — no data copy). */
        uint32_t *snap = (uint32_t *)malloc(nrels * sizeof(uint32_t));
        if (!snap) {
            free(delta_rels);
            return ENOMEM;
        }
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            snap[ri] = r ? r->nrows : 0; /* O(1) count only, no copy */
        }

        /* Phase 3D: Frontier skip with multiplicities (US-3D-002)
         * Skip iteration if all delta relations have zero net multiplicity.
         * This optimizes away iterations where no new facts can be derived.
         * Condition: sum of all multiplicities in all delta relations == 0. */
        if (iter > 0) { /* Only skip from iteration 1 onward */
            bool all_deltas_net_zero = true;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                char dname[256];
                snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
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
                /* All deltas have zero net multiplicity: skip evaluation */
                free(snap);
                continue;
            }
        }

        /* Stratum-level early exit: if all rules have empty forced deltas,
         * the iteration will produce no new facts. Skip it. (Issue #81) */
        if (iter > 0) {
            bool all_rules_empty = true;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                if (!has_empty_forced_delta(&sp->relations[ri], sess, iter)) {
                    all_rules_empty = false;
                    break;
                }
            }
            if (all_rules_empty) {
                free(snap);
                continue;
            }
        }

        /* Single-pass semi-naive evaluation. VARIABLE prefers delta when it
         * is a strict subset of full (genuine new facts). JOIN propagates
         * the is_delta flag through results and applies right-delta when
         * left is full and a strictly-smaller right delta exists. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];

            /* Issue #106 (US-106-003): Rule-level frontier skip with epoch gating.
             * Skip rule evaluation only when BOTH conditions hold:
             * 1. Same outer_epoch (prevents cross-epoch incorrect skips)
             * 2. Iteration > convergence point (rule already processed in this epoch)
             * Across epoch boundaries, outer_epoch mismatch => skip condition false => re-eval. */
            uint32_t rule_id = rule_id_base + ri;
            if (rule_id < MAX_RULES
                && sess->rule_frontiers[rule_id].outer_epoch
                       == sess->outer_epoch
                && iter > sess->rule_frontiers[rule_id].iteration) {
                continue;
            }

            /* Pre-scan skip: if a FORCE_DELTA op references an empty or
             * absent delta (iteration > 0), the plan would produce 0 rows.
             * Skip evaluation entirely to avoid unnecessary work. */
            if (has_empty_forced_delta(rp, sess, iter)) {
                continue;
            }

            eval_stack_t stack;
            eval_stack_init(&stack);

            int rc = col_eval_relation_plan(rp, &stack, sess);
            if (rc != 0) {
                eval_stack_drain(&stack);
                free(snap);
                free(delta_rels);
                return rc;
            }

            if (stack.top == 0)
                continue;

            eval_entry_t result = eval_stack_pop(&stack);
            eval_stack_drain(&stack);

            /* Post-eval skip: if evaluation produced 0 rows, skip the
             * append + consolidate path.  This is a safety net for cases
             * not caught by the pre-scan (e.g. filters that eliminate
             * all rows). */
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
                        free(snap);
                        free(delta_rels);
                        return ENOMEM;
                    }
                    result.owned = false;
                } else {
                    copy = col_rel_new_like(rp->name, result.rel);
                    if (!copy) {
                        free(snap);
                        free(delta_rels);
                        return ENOMEM;
                    }
                    if ((rc = col_rel_append_all(copy, result.rel)) != 0) {
                        col_rel_destroy(copy);
                        free(snap);
                        free(delta_rels);
                        return rc;
                    }
                }
                rc = session_add_rel(sess, copy);
                if (rc != 0) {
                    col_rel_destroy(copy);
                    free(snap);
                    free(delta_rels);
                    return rc;
                }
            } else {
                /* Adopt schema from result if target is still uninitialized */
                if (target->ncols == 0 && result.rel->ncols > 0) {
                    rc = col_rel_set_schema(
                        target, result.rel->ncols,
                        (const char *const *)result.rel->col_names);
                    if (rc != 0) {
                        if (result.owned)
                            col_rel_destroy(result.rel);
                        free(snap);
                        free(delta_rels);
                        return rc;
                    }
                }
                rc = col_rel_append_all(target, result.rel);
                if (result.owned)
                    col_rel_destroy(result.rel);
                if (rc != 0) {
                    free(snap);
                    free(delta_rels);
                    return rc;
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
         * because delta_rels[ri] may be set to NULL in the next iteration's
         * registration loop. strat_frontier is declared before the iteration loop. */

        /* Consolidate all IDB relations to remove duplicates and compute delta
         * as a byproduct.  snap[ri] marks the boundary between the already-
         * sorted prefix and unsorted new rows appended this iteration. */
        bool any_new = false;
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            if (!r || snap[ri] >= r->nrows) {
                continue; /* no new rows for this relation */
            }

            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
            col_rel_t *delta = col_rel_new_like(dname, r);
            if (!delta) {
                free(snap);
                free(delta_rels);
                return ENOMEM;
            }

            /* Consolidate WITH delta output (no separate merge walk) */
            uint32_t cons_old = snap[ri];
            uint32_t cons_new = r->nrows - cons_old; /* delta count D */
            uint64_t cons_t0 = now_ns();
            int rc2 = col_op_consolidate_incremental_delta(r, snap[ri], delta);
            uint64_t cons_elapsed = now_ns() - cons_t0;
            sess->consolidation_ns += cons_elapsed;
            /* Invalidate arrangements for this relation (data changed). */
            col_session_invalidate_arrangements(&sess->base,
                                                sp->relations[ri].name);
            /* Per-call trace: WL_CONSOLIDATION_LOG=1 prints N/D/time per call */
            if (getenv("WL_CONSOLIDATION_LOG")) {
                fprintf(stderr,
                        "CONS iter=%u stratum=%u rel=%s N=%u D=%u "
                        "time_us=%.1f ratio=%.4f\n",
                        iter, stratum_idx, sp->relations[ri].name, cons_old,
                        cons_new, (double)cons_elapsed / 1000.0,
                        cons_old > 0 ? (double)cons_new / (double)cons_old
                                     : 0.0);
            }
            if (rc2 != 0) {
                col_rel_destroy(delta);
                free(snap);
                free(delta_rels);
                return rc2;
            }

            if (delta->nrows > 0) {
                /* Stamp each new row with its provenance (iteration, stratum).
                 * worker=0 indicates the sequential (non-K-fusion) path. */
                delta->timestamps = (col_delta_timestamp_t *)calloc(
                    delta->nrows, sizeof(col_delta_timestamp_t));
                if (!delta->timestamps) {
                    col_rel_destroy(delta);
                    free(snap);
                    free(delta_rels);
                    return ENOMEM;
                }
                for (uint32_t ti = 0; ti < delta->nrows; ti++) {
                    delta->timestamps[ti].iteration = iter;
                    delta->timestamps[ti].stratum = stratum_idx;
                    /* worker left zero: sequential evaluation path */
                    delta->timestamps[ti].multiplicity = 1;
                }

                /* Phase 4: Enable timestamp tracking on target relation to preserve
                 * provenance through consolidation. This enables frontier computation
                 * to determine which iterations have converged.
                 * Backpressure (Issue #224, Step 4): skip when disable_timestamps
                 * is set (RELATION subsystem > 90% cap). Frontier uses conservative
                 * defaults when timestamps are absent. */
                /* Auto-disable timestamps when RELATION > 90% cap
                 * (Issue #224, Step 4). Latches true and stays disabled. */
                if (!sess->disable_timestamps
                    && wl_mem_ledger_should_backpressure(
                        &sess->ledger, WL_MEM_SUBSYS_RELATION, 90)) {
                    sess->disable_timestamps = true;
                    const char *mem_report_env = getenv("WL_MEM_REPORT");
                    if (mem_report_env && mem_report_env[0] == '1')
                        fprintf(stderr, "[wirelog mem] RELATION backpressure: "
                                        "disabling timestamp tracking\n");
                }
                if (!r->timestamps && r->capacity > 0
                    && !sess->disable_timestamps) {
                    r->timestamps = (col_delta_timestamp_t *)calloc(
                        r->capacity, sizeof(col_delta_timestamp_t));
                    if (!r->timestamps) {
                        free(delta->timestamps);
                        col_rel_destroy(delta);
                        free(snap);
                        free(delta_rels);
                        return ENOMEM;
                    }
                }

                delta_rels[ri] = delta;
                any_new = true;

                /* Phase 4: Compute frontier from this delta immediately.
                 * This must happen before the next iteration's registration loop
                 * sets delta_rels[ri] = NULL.
                 * frontier = MAXIMUM iteration that produced facts. Enables skip
                 * optimization in next session_step: skip iterations <= frontier. */
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

        free(snap);

        delta_pool_reset(sess->delta_pool);
        /* Issue #176: Per-iteration cache eviction for recursive strata.
         * Use configurable eviction threshold (cache_evict_threshold):
         * - If 0: disabled, cache cleared each iteration (backward compatible)
         * - If > 0: evict LRU entries when cache size exceeds threshold
         * This preserves hit rate for frequently accessed cached joins
         * while bounding memory growth across deep recursion (100+ iterations). */
        if (sess->cache_evict_threshold == 0) {
            /* Legacy behavior: clear entire cache */
            col_mat_cache_clear(&sess->mat_cache);
        } else {
            /* Smart eviction: remove only least-used entries */
            col_mat_cache_evict_until(&sess->mat_cache,
                                      sess->cache_evict_threshold);
        }

        /* Per-iteration memory report (Issue #224, Step 5).
         * WL_MEM_REPORT=1 enables parseable per-iteration output:
         * "MEM iter=X stratum=Y total=Z.1fGB rel=R.1fGB arena=A.1fGB cache=C.1fGB" */
        {
            const char *mem_report_env = getenv("WL_MEM_REPORT");
            if (mem_report_env && mem_report_env[0] == '1') {
                double gb = 1024.0 * 1024.0 * 1024.0;
                double total_gb
                    = (double)atomic_load_explicit(&sess->ledger.current_bytes,
                                                   memory_order_relaxed)
                      / gb;
                double rel_gb
                    = (double)atomic_load_explicit(
                          &sess->ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
                          memory_order_relaxed)
                      / gb;
                double arena_gb
                    = (double)atomic_load_explicit(
                          &sess->ledger.subsys_bytes[WL_MEM_SUBSYS_ARENA],
                          memory_order_relaxed)
                      / gb;
                double cache_gb
                    = (double)atomic_load_explicit(
                          &sess->ledger.subsys_bytes[WL_MEM_SUBSYS_CACHE],
                          memory_order_relaxed)
                      / gb;
                fprintf(stderr,
                        "MEM iter=%u stratum=%u total=%.1fGB "
                        "rel=%.1fGB arena=%.1fGB cache=%.1fGB\n",
                        iter, stratum_idx, total_gb, rel_gb, arena_gb,
                        cache_gb);
            }
        }

        if (!any_new) {
            sess->total_iterations = iter;
            break;
        }
    }
    sess->total_iterations = iter;

    /* Issue #106 (US-106-005): Record per-rule frontier at convergence with epoch.
     * When stratum converges at iteration I, record (outer_epoch, I) for each rule.
     * On next incremental snapshot, skip condition checks epoch match AND iter > I.
     * This preserves fine-grained rule convergence across insertions. */
    for (uint32_t ri = 0; ri < nrels && rule_id_base + ri < MAX_RULES; ri++) {
        uint32_t rule_id = rule_id_base + ri;
        sess->rule_frontiers[rule_id].outer_epoch = sess->outer_epoch;
        sess->rule_frontiers[rule_id].iteration = iter;
    }

    /* Phase 4: Update per-stratum frontier after recursive stratum evaluation.
     * frontier was computed incrementally during consolidation, so just
     * store it in the session. Each stratum independently tracks its
     * convergence frontier for the skip optimization in the next session_step. */
    if (strat_frontier.iteration != UINT32_MAX && stratum_idx < MAX_STRATA) {
        /* Set this stratum's 2D frontier to the convergence point.
         * outer_epoch tracks the insertion epoch; iteration tracks convergence.
         * This enables skipping iterations if frontier persists across
         * session_step calls (incremental evaluation). */
        col_frontier_2d_t f2d = { sess->outer_epoch, strat_frontier.iteration };
        sess->frontiers[stratum_idx] = f2d;
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
            free(r->data);
            r->data = ce.rel->data;
            r->nrows = ce.rel->nrows;
            r->capacity = ce.rel->capacity;
            ce.rel->data = NULL;
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

    /* Step 0: Save current state of each IDB relation and rows to retract */
    int64_t **saved_data = (int64_t **)calloc(rc_cnt, sizeof(int64_t *));
    uint32_t *saved_nrows = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    uint32_t *saved_ncols = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    int64_t **retract_data = (int64_t **)calloc(rc_cnt, sizeof(int64_t *));
    uint32_t *retract_nrows = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    if (!saved_data || !saved_nrows || !saved_ncols || !retract_data
        || !retract_nrows) {
        free(saved_data);
        free(saved_nrows);
        free(saved_ncols);
        free(retract_data);
        free(retract_nrows);
        return ENOMEM;
    }

    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        if (r->nrows > 0) {
            size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
            saved_data[ri] = (int64_t *)malloc(sz);
            if (!saved_data[ri]) {
                for (uint32_t i = 0; i < ri; i++)
                    free(saved_data[i]);
                free(saved_data);
                free(saved_nrows);
                free(saved_ncols);
                free(retract_data);
                free(retract_nrows);
                return ENOMEM;
            }
            memcpy(saved_data[ri], r->data, sz);
            saved_nrows[ri] = r->nrows;
            saved_ncols[ri] = r->ncols;
        }
    }

    /* Step 1: Clear IDB relations before evaluation */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r)
            continue;
        r->nrows = 0;
    }

    /* Step 2: Enable retraction-seeded mode and evaluate stratum */
    sess->retraction_seeded = true;
    int rc = col_eval_stratum(sp, sess, stratum_idx);
    sess->retraction_seeded = false;
    if (rc != 0) {
        for (uint32_t i = 0; i < rc_cnt; i++)
            free(saved_data[i]);
        free(saved_data);
        free(saved_nrows);
        free(saved_ncols);
        free(retract_data);
        free(retract_nrows);
        return rc;
    }

    /* Step 3: Save retraction candidates before consolidation overwrites them */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0 || r->nrows == 0)
            continue;
        size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
        retract_data[ri] = (int64_t *)malloc(sz);
        if (!retract_data[ri]) {
            for (uint32_t i = 0; i < rc_cnt; i++) {
                free(saved_data[i]);
                free(retract_data[i]);
            }
            free(saved_data);
            free(saved_nrows);
            free(saved_ncols);
            free(retract_data);
            free(retract_nrows);
            return ENOMEM;
        }
        memcpy(retract_data[ri], r->data, sz);
        retract_nrows[ri] = r->nrows;
    }

    /* Step 4: Consolidate retraction rows (for proper dedup) */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        rc = col_idb_consolidate(r, sess);
        if (rc != 0) {
            for (uint32_t i = 0; i < rc_cnt; i++) {
                free(saved_data[i]);
                free(retract_data[i]);
            }
            free(saved_data);
            free(saved_nrows);
            free(saved_ncols);
            free(retract_data);
            free(retract_nrows);
            return rc;
        }
    }

    /* Step 5: Restore saved state and remove retracted rows */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;

        uint32_t ncols = r->ncols;

        /* Restore saved state */
        if (saved_nrows[ri] > 0) {
            if (r->capacity < saved_nrows[ri]) {
                int64_t *new_data = (int64_t *)realloc(
                    r->data, saved_nrows[ri] * ncols * sizeof(int64_t));
                if (!new_data) {
                    for (uint32_t i = 0; i < rc_cnt; i++) {
                        free(saved_data[i]);
                        free(retract_data[i]);
                    }
                    free(saved_data);
                    free(saved_nrows);
                    free(saved_ncols);
                    free(retract_data);
                    free(retract_nrows);
                    return ENOMEM;
                }
                r->data = new_data;
                r->capacity = saved_nrows[ri];
            }
            memcpy(r->data, saved_data[ri],
                   saved_nrows[ri] * ncols * sizeof(int64_t));
            r->nrows = saved_nrows[ri];
        } else {
            r->nrows = 0;
        }

        /* Remove each retracted row from the current state */
        if (retract_nrows[ri] > 0) {
            /* Use consolidated retraction data */
            for (uint32_t del_idx = 0; del_idx < retract_nrows[ri]; del_idx++) {
                const int64_t *to_remove = r->data + (size_t)del_idx * ncols;
                if (retract_data[ri]) {
                    to_remove = retract_data[ri] + (size_t)del_idx * ncols;
                }

                /* Find and remove this row in-place */
                uint32_t out_r = 0;
                bool found = false;
                for (uint32_t src_idx = 0; src_idx < r->nrows; src_idx++) {
                    const int64_t *src = r->data + (size_t)src_idx * ncols;
                    if (memcmp(src, to_remove, sizeof(int64_t) * ncols) == 0) {
                        /* Found matching row; skip it (removal) */
                        found = true;
                        /* Copy remaining rows */
                        for (uint32_t rest = src_idx + 1; rest < r->nrows;
                             rest++) {
                            memcpy(r->data + (size_t)out_r * ncols,
                                   r->data + (size_t)rest * ncols,
                                   sizeof(int64_t) * ncols);
                            out_r++;
                        }
                        r->nrows = out_r;
                        break;
                    } else {
                        /* Keep this row */
                        if (out_r != src_idx)
                            memcpy(r->data + (size_t)out_r * ncols, src,
                                   sizeof(int64_t) * ncols);
                        out_r++;
                    }
                }

                /* Fire delta callback if row was actually removed */
                if (found && sess->delta_cb) {
                    sess->delta_cb(r->name, to_remove, ncols, -1,
                                   sess->delta_data);
                }
            }
        }
    }

    /* Cleanup */
    for (uint32_t i = 0; i < rc_cnt; i++) {
        free(saved_data[i]);
        free(retract_data[i]);
    }
    free(saved_data);
    free(saved_nrows);
    free(saved_ncols);
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

    /* Step 1: snapshot sorted prev state for each IDB relation */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        /* Snapshot even if empty: needed to detect retractions via set-diff */
        if (r->nrows > 0) {
            size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
            prev_data[ri] = (int64_t *)malloc(sz);
            if (!prev_data[ri]) {
                for (uint32_t i = 0; i < ri; i++)
                    free(prev_data[i]);
                free(prev_data);
                free(prev_nrows);
                free(prev_ncols);
                return ENOMEM;
            }
            memcpy(prev_data[ri], r->data, sz);
            prev_nrows[ri] = r->nrows;
            prev_ncols[ri] = r->ncols;
        } else {
            /* Relation is empty, but mark it so we remember it existed */
            prev_nrows[ri] = 0;
            prev_ncols[ri] = r->ncols;
        }
    }

    /* Step 1b: Clear IDB relations before re-evaluation to enable retraction
     * detection */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r)
            continue;
        r->nrows = 0; /* Clear the relation for fresh derivation */
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

        /* Fire delta_cb(+1) for rows not present in prev sorted state */
        if (r->nrows > 0) {
            for (uint32_t row = 0; row < r->nrows; row++) {
                const int64_t *rowp = r->data + (size_t)row * ncols;
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
                if (!col_row_in_sorted(r->data, r->nrows, prev_ncols[ri],
                                       rowp)) {
                    sess->delta_cb(r->name, rowp, prev_ncols[ri], -1,
                                   sess->delta_data);
                }
            }
        }
    }

cleanup:
    for (uint32_t i = 0; i < rc_cnt; i++)
        free(prev_data[i]);
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
