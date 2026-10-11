/*
 * columnar/eval_plan.c - wirelog Relation-Plan Dispatch
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */
#define _GNU_SOURCE

#include "columnar/internal.h"
#include "columnar/join_batch.h"
#include "columnar/join_pipeline.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Stratum Evaluator                                                         */
/* ======================================================================== */

/* Compile-time check: WL_PLAN_OP__BACKEND_START must match the first
 * backend-specific operator (Issue #495). */
#if defined(_MSC_VER)
/* MSVC C mode: use typedef array trick for compile-time check */
typedef char static_check_backend_start_
    [(WL_PLAN_OP_K_FUSION == WL_PLAN_OP__BACKEND_START) ? 1 : -1];
#else
_Static_assert(WL_PLAN_OP_K_FUSION == WL_PLAN_OP__BACKEND_START,
    "WL_PLAN_OP__BACKEND_START must equal WL_PLAN_OP_K_FUSION");
#endif

/*
 * col_eval_relation_plan:
 * Evaluate all operators for one relation plan using the eval stack.
 * On success, the top of stack holds the result relation (owned).
 */
int
wl_columnar_eval_plan_aggregate_validate(const wl_plan_relation_t *plan,
    wl_evaluation_control_t *control, bool *supported)
{
    if (!plan || !supported || !control) return EINVAL;
    *supported = false;
    if (!plan->ops || (plan->op_count != 3u && plan->op_count != 4u)) return 0;
    for (uint32_t i = 0; i < plan->op_count; i++) {
        int rc = wl_evaluation_control_charge(control, 48);
        if (rc) return rc;
        const wl_plan_op_t *op = &plan->ops[i];
        if (i == 0) {
            if (op->op != WL_PLAN_OP_VARIABLE || !op->relation_name
                || (op->delta_mode != WL_DELTA_AUTO
                && op->delta_mode != WL_DELTA_FORCE_FULL)) return ENOTSUP;
        } else if (i + 1u == plan->op_count) {
            if (op->op != WL_PLAN_OP_CONSOLIDATE) return ENOTSUP;
        } else if (i + 2u == plan->op_count) {
            if (op->op != WL_PLAN_OP_REDUCE) return ENOTSUP;
        } else if (op->op != WL_PLAN_OP_FILTER) return ENOTSUP;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    *supported = true;
    return 0;
}

int
wl_columnar_eval_plan_union_validate(const wl_plan_relation_t *plan,
    wl_evaluation_control_t *control, bool *supported)
{
    if (!plan || !supported || !control) return EINVAL;
    *supported = false;
    if (!plan->ops || plan->op_count < 3u) return 0;
    uint32_t depth = 0;
    bool branch = false;
    for (uint32_t i = 0; i < plan->op_count; i++) {
        int rc = wl_evaluation_control_charge(control, 48);
        if (rc) return rc;
        const wl_plan_op_t *op = &plan->ops[i];
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            if (branch || depth == COL_STACK_MAX || !op->relation_name
                || (op->delta_mode != WL_DELTA_AUTO
                && op->delta_mode != WL_DELTA_FORCE_FULL)) return ENOTSUP;
            depth++; branch = true;
            break;
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_REDUCE:
            if (!branch || !depth) return ENOTSUP;
            break;
        case WL_PLAN_OP_JOIN:
            if (!branch || !depth || op->materialized
                || op->right_filter_expr.size
                || (op->delta_mode != WL_DELTA_AUTO
                && op->delta_mode != WL_DELTA_FORCE_FULL)) return ENOTSUP;
            break;
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            if (!branch || !depth
                || (op->delta_mode != WL_DELTA_AUTO
                && op->delta_mode != WL_DELTA_FORCE_FULL)) return ENOTSUP;
            break;
        case WL_PLAN_OP_CONCAT:
            if (!branch || depth < 2u) return ENOTSUP;
            depth--; branch = false;
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            if (!depth) return ENOTSUP;
            break;
        default: return ENOTSUP;
        }
        /* A completed first child remains below the next VARIABLE. */
        if (i + 1u < plan->op_count
            && plan->ops[i + 1u].op == WL_PLAN_OP_VARIABLE) branch = false;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    if (depth != 1u
        || plan->ops[plan->op_count - 1u].op != WL_PLAN_OP_CONSOLIDATE)
        return ENOTSUP;
    *supported = true;
    return 0;
}

static int
wl_columnar_eval_plan_body(const wl_plan_relation_t *rplan, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    for (uint32_t i = 0; i < rplan->op_count; i++) {
        const wl_plan_op_t *op = &rplan->ops[i];
        if (sess->tdd_input_run)
            sess->tdd_input_run->current_op = op;
        int rc = 0;

        /* NOTE: Weighted operation cases (WL_PLAN_OP_JOIN_WEIGHTED,
         * WL_PLAN_OP_REDUCE_WEIGHTED) are not yet present. These functions
         * exist and are tested independently (col_op_join_weighted,
         * col_op_reduce_weighted in columnar_nanoarrow.c). Integration into
         * this switch will occur when the plan generator emits weighted opcodes
         * for Z-set multiplicity evaluation. For now, col_op_join and
         * col_op_reduce dispatch to their base (non-weighted) versions. */
        /* Issue #361/#367: Skip base-case EDB VARIABLEs at iter > 0.
         * A base-case VARIABLE references a static EDB (no delta) and is NOT
         * followed by JOIN (it feeds CONCAT via MAP). The base-case tuples are
         * already in the IDB from iter 0, so re-loading them every sub-pass
         * wastes O(N_base) work per sub-pass. Push empty instead.
         * Applies to both W=1 sequential evaluation and W>1 TDD workers.
         * For CRDT (k=1, no FORCE_DELTA expansion), this eliminates redundant
         * 104K-row consolidation sorting on every sub-pass (Issue #367). */
        if (!sess->tdd_input_run && op->op == WL_PLAN_OP_VARIABLE
            && sess->current_iteration > 0
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
                    /* Issue #1140: the lookup can miss -- session_find_rel()
                     * returns NULL for a relation that was never inserted.
                     * Abandon the optimization rather than build $base_skip
                     * from a template that is not there: falling through to
                     * col_op_variable() gives the missing relation its normal
                     * ENOENT disposition, so one state does not get two
                     * answers depending on which check saw it first.  The
                     * previous "full ? full : NULL" argument guarded nothing
                     * -- both arms yield full. */
                    if (!full)
                        goto normal_eval;
                    col_rel_t *empty = col_rel_pool_new_like(
                        sess->delta_pool, "$base_skip", full);
                    /* Issue #1140: these exits return rather than break.  A
                     * break here leaves the for loop, not a switch, and the
                     * function then falls through to "return 0" -- reporting
                     * success with nothing pushed on the stack, which the
                     * caller reads as a rule that produced no rows. */
                    if (!empty)
                        return ENOMEM;
                    rc = eval_stack_push(stack, empty, true);
                    if (rc != 0) {
                        col_rel_destroy(empty);
                        return rc;
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
            rc = wl_columnar_filter_op(op, stack, sess);
            break;
        case WL_PLAN_OP_JOIN:
            if (sess->base.evaluation_control
                && sess->diff_operators_active)
                return ENOTSUP;
            if (sess->base.evaluation_control && sess->join_batch_bytes) {
                rc = wl_evaluation_control_charge(sess->base.evaluation_control,
                        96);
                if (!rc) rc = wl_evaluation_control_charge(
                        sess->base.evaluation_control, 0);
                if (rc) return rc;
                if (i + 1u < rplan->op_count
                    && rplan->ops[i + 1u].op != WL_PLAN_OP_CONSOLIDATE) {
                    uint32_t next_index = i;
                    int pipeline_rc =
                        wl_columnar_join_pipeline_try_eval_controlled(rplan,
                            i, stack, sess, &next_index,
                            sess->base.evaluation_control);
                    if (pipeline_rc == 1) {
                        i = next_index;
                        continue;
                    }
                    if (pipeline_rc) return pipeline_rc;
                }
                if (i + 1u < rplan->op_count) {
                    col_join_batch_record_fallback(sess,
                        COL_JOIN_BATCH_EXCLUDED_PIPELINE);
                    if (sess->join_batch_strict) return ENOTSUP;
                }
                goto ordinary_join;
            }
            if (!sess->diff_operators_active && sess->join_batch_bytes > 0) {
                uint32_t next_index = i;
                int pipeline_rc = wl_columnar_join_pipeline_try_eval(rplan, i,
                        stack, sess, &next_index);
                if (pipeline_rc == 1) {
                    i = next_index;
                    continue;
                }
                if (pipeline_rc != 0 && pipeline_rc != ENOENT
                    && pipeline_rc != ENOTSUP && pipeline_rc != EAGAIN)
                    return pipeline_rc;
                /* A final JOIN already has a supported bounded relation sink
                 * in wl_columnar_join_op().  Only a JOIN with downstream
                 * operators is a pipeline request that strict mode must
                 * reject when this fused consumer cannot handle it. */
                bool has_downstream = i + 1u < rplan->op_count;
                if (pipeline_rc == 0 && !has_downstream)
                    goto ordinary_join;
                col_join_batch_record_fallback(sess,
                    COL_JOIN_BATCH_EXCLUDED_PIPELINE);
                if (sess->join_batch_strict) {
                    return ENOTSUP;
                }
            }
ordinary_join:
            rc = sess->diff_operators_active
                ? wl_columnar_join_diff_op(op, stack, sess)
                : wl_columnar_join_op(op, stack, sess);
            break;
        case WL_PLAN_OP_ANTIJOIN:
            rc = wl_columnar_antijoin_op(op, stack, sess);
            break;
        case WL_PLAN_OP_CONCAT:
            rc = sess->base.evaluation_control
                ? wl_columnar_merge_concat_controlled(stack, sess,
                    sess->base.evaluation_control) : col_op_concat(stack, sess);
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
            rc = wl_columnar_semijoin_op(op, stack, sess);
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
            assert(0 && "unknown plan op type in columnar eval dispatch");
            break;
        }
        if (rc != 0)
            return rc;
    }
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_plan_test_entry_checkpoint)(wl_evaluation_control_t *);
#endif

int
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!rplan || !stack || !sess || (rplan->op_count && !rplan->ops))
        return EINVAL;
    if (sess->base.evaluation_control) {
        wl_evaluation_control_t *control = sess->base.evaluation_control;
        int rc = wl_evaluation_control_charge(control, 40);
        if (rc)
            return rc;
        /* Existing exact routes retain their 40-unit header. The generated
         * JOIN route additionally admits32 before its new fixed predicates.
         * No plan traversal occurs here. */
        bool join = rplan->op_count == 1
            && rplan->ops[0].op == WL_PLAN_OP_JOIN;
        bool variable_pair = rplan->op_count == 2
            && rplan->ops[0].op == WL_PLAN_OP_VARIABLE;
        bool filter = variable_pair && rplan->ops[1].op == WL_PLAN_OP_FILTER;
        bool map = variable_pair && rplan->ops[1].op == WL_PLAN_OP_MAP;
        bool reduce = variable_pair && rplan->ops[1].op == WL_PLAN_OP_REDUCE;
        bool consolidate = variable_pair &&
            rplan->ops[1].op == WL_PLAN_OP_CONSOLIDATE;
        bool variable_triple = rplan->op_count == 3
            && rplan->ops[0].op == WL_PLAN_OP_VARIABLE;
        bool pipeline = variable_triple
            && rplan->ops[1].op == WL_PLAN_OP_FILTER
            && rplan->ops[2].op == WL_PLAN_OP_MAP;
        bool owned_consolidate = variable_triple
            && rplan->ops[1].op == WL_PLAN_OP_MAP
            && rplan->ops[2].op == WL_PLAN_OP_CONSOLIDATE;
        bool filtered_consolidate = rplan->op_count == 4
            && rplan->ops[0].op == WL_PLAN_OP_VARIABLE
            && rplan->ops[1].op == WL_PLAN_OP_FILTER
            && rplan->ops[2].op == WL_PLAN_OP_MAP
            && rplan->ops[3].op == WL_PLAN_OP_CONSOLIDATE;
        if ((rplan->op_count == 3 || rplan->op_count == 4)
            && rplan->ops[1].op == WL_PLAN_OP_JOIN) {
            rc = wl_evaluation_control_charge(control, 32);
            if (rc) return rc;
        }
        bool joined_consolidate = (rplan->op_count == 3 || rplan->op_count == 4)
            && rplan->ops[0].op == WL_PLAN_OP_VARIABLE
            && rplan->ops[1].op == WL_PLAN_OP_JOIN
            && (rplan->ops[1].delta_mode == WL_DELTA_AUTO
            || rplan->ops[1].delta_mode == WL_DELTA_FORCE_FULL)
            && !rplan->ops[1].materialized &&
            !rplan->ops[1].right_filter_expr.size
            && (rplan->op_count == 3 ? rplan->ops[2].op ==
            WL_PLAN_OP_CONSOLIDATE
                : rplan->ops[2].op == WL_PLAN_OP_MAP
            && rplan->ops[3].op == WL_PLAN_OP_CONSOLIDATE);
        bool batched_pipeline = false;
        if (sess->join_batch_bytes && rplan->op_count >= 4u) {
            rc = wl_columnar_join_pipeline_plan_validate_controlled(rplan,
                    sess, control, &batched_pipeline);
            if (rc) return rc;
        }
        bool union_plan = false;
        if (rplan->op_count >= 5u) {
            rc = wl_columnar_eval_plan_union_validate(rplan, control,
                    &union_plan);
            if (rc) return rc;
        }
        bool aggregate_plan = false;
        if ((rplan->op_count == 3u && rplan->ops[1].op == WL_PLAN_OP_REDUCE)
            || (rplan->op_count == 4u
            && rplan->ops[1].op == WL_PLAN_OP_FILTER
            && rplan->ops[2].op == WL_PLAN_OP_REDUCE)) {
            rc = wl_columnar_eval_plan_aggregate_validate(rplan, control,
                    &aggregate_plan);
            if (rc) return rc;
        }
        /* Preserve existing exact-route admission. Only a newly composed
         * short stream needs the shared ordinary postfix traversal. */
        if (!union_plan && !join && !filter && !map && !pipeline && !reduce
            && !consolidate && !owned_consolidate && !filtered_consolidate
            && !joined_consolidate && !aggregate_plan) {
            rc = wl_columnar_eval_plan_union_validate(rplan, control,
                    &union_plan);
            if (rc) return rc;
        }
        if (!union_plan && !join && !filter && !map && !pipeline && !reduce &&
            !consolidate
            && !owned_consolidate && !filtered_consolidate &&
            !joined_consolidate && !aggregate_plan)
            return ENOTSUP;
        const wl_plan_op_t *op = &rplan->ops[0];
        if (sess->num_workers > 1 || sess->diff_operators_active
            || sess->tdd_input_run || sess->tdd_subpass_active
            || sess->tdd_outbound_only_active || sess->delta_seeded
            || sess->retraction_seeded || sess->retraction_right_pass
            || (sess->join_batch_bytes && !batched_pipeline
            && (!joined_consolidate || rplan->op_count != 3u
            || !rplan->ops[1].key_count))
            || sess->join_output_shared_count
            || !sess->memory_governor
            || (op->delta_mode != WL_DELTA_FORCE_FULL
            && !((owned_consolidate || filtered_consolidate ||
            joined_consolidate || union_plan || aggregate_plan)
            && op->delta_mode == WL_DELTA_AUTO)))
            return ENOTSUP;
        if (filter || map || pipeline || reduce || consolidate ||
            owned_consolidate || filtered_consolidate || joined_consolidate ||
            union_plan || aggregate_plan) {
            rc = wl_evaluation_control_charge(control,
                    filtered_consolidate ||
                    joined_consolidate ? 40u : pipeline ||
                    owned_consolidate ? 32u : map || reduce ||
                    consolidate ? 24u : 16u);
            if (rc)
                return rc;
            if (sess->current_iteration || sess->delta_observer)
                return ENOTSUP;
            if (stack->top >= COL_STACK_MAX)
                return ENOMEM;
        } else {
            if (op->right_filter_expr.size || op->materialized)
                return ENOTSUP;
            if (!stack->top || stack->top > COL_STACK_MAX
                || stack->items[stack->top - 1].kind !=
                WL_COLUMNAR_EVAL_ENTRY_RELATION
                || !stack->items[stack->top - 1].rel)
                return EINVAL;
            if (stack->items[stack->top - 1].is_delta)
                return ENOTSUP;
        }
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_plan_test_entry_checkpoint)
            wl_columnar_eval_plan_test_entry_checkpoint(control);
#endif
        rc = wl_evaluation_control_charge(control, 0);
        if (rc)
            return rc;
    }
    if (!sess->tdd_input_run)
        return wl_columnar_eval_plan_body(rplan, stack, sess);
    wl_columnar_eval_tdd_run_t *run = sess->tdd_input_run;
    if (!run->evaluating || run->current_op
        || rplan->ops != run->manifest->owner_ops + run->slice->start
        || rplan->op_count != run->slice->count)
        return EBUSY;
    int rc = wl_columnar_eval_plan_body(rplan, stack, sess);
    run->current_op = NULL;
    return rc;
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
 * For a conjunctive plan, an empty/absent forced-delta input proves the
 * output empty. CONCAT alternatives require normal evaluation instead:
 * an empty input in one branch says nothing about the other branches.
 *
 * Returns true if the plan can be skipped (empty forced-delta found).
 *
 * Issue #158 extension: When retraction_seeded and iteration == 0, check
 * $r$<name> relations instead of $d$<name>.
 *
 * Issue #1019: this scan is blinded by the same plan rewrites that blinded
 * col_compute_affected_strata(). On a fused relation rp->ops is
 * [K_FUSION, EXCHANGE], both zero-initialised, so delta_mode is never
 * WL_DELTA_FORCE_DELTA and this returns false unconditionally.
 *
 * FOUR of the eight call sites are blinded, not two.  All four pass a
 * top-level wl_plan_stratum_t relation plan (&sp->relations[ri]), which is
 * exactly what the rewrite replaced:
 *   - col_eval_stratum(): the whole-stratum early exit (break the sub-pass
 *     when EVERY rule answers true) and the per-rule pre-scan below it;
 *   - tdd_worker_subpass_fn(): the same pair again on the TDD worker path
 *     -- an all-rules early exit that sets ctx->all_empty_delta, and a
 *     per-rule pre-scan in its relation loop.
 * The other two are not blinded, because they pass an inner operator list
 * that still carries the FORCE_DELTA ops the rewrite hid:
 *   - col_op_k_fusion_serial() and col_op_k_fusion() (columnar/kfusion.c)
 *     evaluate meta->k_ops[d] directly.
 * Call sites are named by function on purpose: line numbers drift.
 *
 * The blinding costs iterations, never correctness, so it is deliberately
 * NOT fixed the way #1019 fixed the frontier: the answer depends on live
 * per-iteration session state ($d$/$r$ relation contents, delta_seeded,
 * retraction_seeded), which does not exist at plan-lowering time and so
 * cannot be recorded on the plan. The two unblinded in-kernel checks are
 * not a substitute either, and they differ from each other:
 * col_op_k_fusion_serial() skips one fused branch with a continue, while
 * col_op_k_fusion() builds a live_indices[] list and short-circuits the
 * WHOLE K_FUSION op to an empty relation when live_count == 0. Neither can
 * reproduce the whole-sub-pass break this function enables.
 */
bool
has_empty_forced_delta(const wl_plan_relation_t *rp, wl_col_session_t *sess,
    uint32_t iteration)
{
    if (iteration == 0 && !sess->delta_seeded && !sess->retraction_seeded)
        return false; /* Base case: no deltas exist yet (non-incremental) */

    /* CONCAT joins independent rule/delta alternatives, not conjunctive
     * inputs. One empty input cannot prove that their union is empty (#1376).
     * Scan the WHOLE plan first: an empty input may precede the CONCAT.
     * Let normal evaluation handle these plans, including fused inner lists.
     */
    for (uint32_t oi = 0; oi < rp->op_count; oi++) {
        if (rp->ops[oi].op == WL_PLAN_OP_CONCAT)
            return false;
    }

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
