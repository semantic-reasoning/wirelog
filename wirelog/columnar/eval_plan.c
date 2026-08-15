/*
 * columnar/eval_plan.c - wirelog Relation-Plan Dispatch
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
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
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    for (uint32_t i = 0; i < rplan->op_count; i++) {
        const wl_plan_op_t *op = &rplan->ops[i];
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
        if (op->op == WL_PLAN_OP_VARIABLE
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
            rc = wl_columnar_filter_op(op, stack, sess);
            break;
        case WL_PLAN_OP_JOIN:
            rc = sess->diff_operators_active
                ? wl_columnar_join_diff_op(op, stack, sess)
                : wl_columnar_join_op(op, stack, sess);
            break;
        case WL_PLAN_OP_ANTIJOIN:
            rc = wl_columnar_antijoin_op(op, stack, sess);
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
 *   - col_op_k_fusion_serial() and col_op_k_fusion() (columnar/ops.c)
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
