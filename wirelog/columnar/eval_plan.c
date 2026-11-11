/*
 * columnar/eval_plan.c - wirelog Relation-Plan Dispatch
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */
#define _GNU_SOURCE

#include "columnar/internal.h"
#include "columnar/batch_payload.h"
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

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_plan_test_after_terminal_exchange)(
    wl_evaluation_control_t *, uint64_t);
#endif

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

int
wl_columnar_eval_plan_numeric_composed_abort_supported(
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    bool *supported)
{
    if (!supported) return EINVAL;
    *supported = false;
    if (!plan || !plan->ops || begin >= end || end > plan->op_count)
        return EINVAL;
    uint32_t depth = 0;
    for (uint32_t i = begin; i < end; ++i) {
        switch (plan->ops[i].op) {
        case WL_PLAN_OP_VARIABLE:
            if (depth == COL_STACK_MAX) return ENOBUFS;
            ++depth;
            break;
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_MAP:
            if (!depth) return EINVAL;
            break;
        case WL_PLAN_OP_LFTJ:
            /* A generated LFTJ owns its k input relations in opaque_data and
             * contributes one result to the composed runtime stack. */
            if (depth == COL_STACK_MAX) return ENOBUFS;
            ++depth;
            break;
        case WL_PLAN_OP_REDUCE:
        case WL_PLAN_OP_ANTIJOIN:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_JOIN:
            if (!depth) return EINVAL;
            /* The owned ordinary JOIN attempt retains cancellation state and
             * cleanup after its synchronous run returns. Host ABORT cannot
             * interrupt that run. Materialized JOIN has no equivalent framed
             * owner and remains usable only through ordinary advance. */
            if (plan->ops[i].materialized) return 0;
            break;
        case WL_PLAN_OP_CONCAT:
            if (depth < 2) return EINVAL;
            --depth;
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            if (!depth || i + 1 != end) return ENOTSUP;
            break;
        default:
            return 0;
        }
    }
    *supported = depth == 1
        && plan->ops[end - 1].op == WL_PLAN_OP_CONSOLIDATE;
    return 0;
}

int
wl_columnar_eval_plan_numeric_composed_abort_supported_attempt_controlled(
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    bool *supported, wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if (!supported) return EINVAL;
    *supported = false;
    int rc = wl_evaluation_control_charge_attempt(control, attempt_id,
            WL_COLUMNAR_EVAL_PLAN_ABORT_SCAN_ENTRY_WORK);
    if (rc) return rc;
    if (!plan || !plan->ops || begin >= end || end > plan->op_count)
        return EINVAL;
    uint32_t depth = 0;
    uint32_t i = begin;
    while (i < end) {
        rc = wl_evaluation_control_charge_attempt(control, attempt_id,
                WL_COLUMNAR_EVAL_PLAN_ABORT_SCAN_OP_WORK);
        if (rc) return rc;
        switch (plan->ops[i].op) {
        case WL_PLAN_OP_VARIABLE:
            if (depth == COL_STACK_MAX) return ENOBUFS;
            ++depth;
            break;
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_MAP:
            if (!depth) return EINVAL;
            break;
        case WL_PLAN_OP_LFTJ:
            /* A generated LFTJ owns its k input relations in opaque_data and
             * contributes one result to the composed runtime stack. */
            if (depth == COL_STACK_MAX) return ENOBUFS;
            ++depth;
            break;
        case WL_PLAN_OP_REDUCE:
        case WL_PLAN_OP_ANTIJOIN:
        case WL_PLAN_OP_SEMIJOIN:
            /* Keep ordinary OP64 unchanged. From its successful debit:
             * suffix8 + rc2 + opcode5 + dispatch1 + call fronts4 +
             * complete next controller42 + refusal rc/return2 =64.
             * No predicate or helper precedes this direct extra payment.
             * The funded probe body including next OP fronts is94<=128. */
            rc = wl_evaluation_control_charge_attempt(control, attempt_id,
                    WL_COLUMNAR_EVAL_PLAN_ABORT_SCAN_PROBE_WORK);
            if (rc) return rc;
            if (!depth) return EINVAL;
            if (plan->ops[i].materialized) return 0;
            break;
        case WL_PLAN_OP_JOIN:
            if (!depth) return EINVAL;
            /* The owned ordinary JOIN attempt retains cancellation state and
             * cleanup after its synchronous run returns. Host ABORT cannot
             * interrupt that run. Materialized JOIN has no equivalent framed
             * owner and remains usable only through ordinary advance. */
            if (plan->ops[i].materialized) return 0;
            break;
        case WL_PLAN_OP_CONCAT:
            if (depth < 2) return EINVAL;
            --depth;
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            if (!depth || i + 1 != end) return ENOTSUP;
            break;
        default:
            return 0;
        }
        ++i;
    }
    rc = wl_evaluation_control_charge_attempt(control, attempt_id,
            WL_COLUMNAR_EVAL_PLAN_ABORT_SCAN_FINAL_WORK);
    if (rc) return rc;
    *supported = depth == 1
        && plan->ops[end - 1].op == WL_PLAN_OP_CONSOLIDATE;
    return 0;
}

/*
 * wl_columnar_eval_plan_var_skip_scan:
 * Decide whether the rows of the VARIABLE at @var_idx can be left out of
 * this sub-pass (Issue #2127).  The scan follows the stack item that
 * VARIABLE pushes to the end of the plan:
 *   - MAP, FILTER, CONSOLIDATE and EXCHANGE change only that item;
 *   - CONCAT merges it with the item above, after which the merged item is
 *     followed;
 *   - JOIN, SEMIJOIN and ANTIJOIN read a second relation by name, so the
 *     scan continues past one only when it is not FORCE_DELTA and @has_delta
 *     reports no new rows for its right relation;
 *   - an op acting on an item pushed above the tracked one is passed over.
 * REDUCE, an unnamed or FORCE_DELTA join, a join whose right relation has
 * new rows, and K_FUSION or LFTJ at any depth (their stack effect is not
 * modelled here) end the scan with false.  True means the item depends only
 * on relations that @has_delta reports unchanged.  @has_delta recognises
 * changes published as "$d$<name>" rows and no other form.
 */
bool
wl_columnar_eval_plan_var_skip_scan(const wl_plan_relation_t *rplan,
    uint32_t var_idx, wl_columnar_eval_plan_delta_probe_fn has_delta,
    void *ctx)
{
    uint32_t above = 0;

    for (uint32_t j = var_idx + 1; j < rplan->op_count; j++) {
        const wl_plan_op_t *o = &rplan->ops[j];

        switch (o->op) {
        case WL_PLAN_OP_VARIABLE:
            above++;
            continue;
        case WL_PLAN_OP_CONCAT:
            if (above > 0)
                above--;
            continue;
        case WL_PLAN_OP_K_FUSION:
        case WL_PLAN_OP_LFTJ:
            return false;
        default:
            break;
        }
        if (above > 0)
            continue;
        switch (o->op) {
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_CONSOLIDATE:
        case WL_PLAN_OP_EXCHANGE:
            continue;
        case WL_PLAN_OP_JOIN:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            if (o->delta_mode != WL_DELTA_FORCE_DELTA && o->right_relation
                && !has_delta(ctx, o->right_relation))
                continue;
            return false;
        default:
            return false;
        }
    }
    return true;
}

/* Delta probe for the skip scan: true when the session holds a nonempty
 * "$d$<name>". */
static bool
session_has_delta_rows(void *ctx, const char *relation_name)
{
    char dname[256];
    snprintf(dname, sizeof(dname), "$d$%s", relation_name);
    col_rel_t *d = session_find_rel((wl_col_session_t *)ctx, dname);
    return d && d->nrows > 0;
}

/*
 * col_eval_relation_plan:
 * Evaluate all operators for one relation plan using the eval stack.
 * On success, the top of stack holds the result relation (owned).
 */
static int
wl_columnar_eval_plan_body(const wl_plan_relation_t *rplan, eval_stack_t *stack,
    wl_col_session_t *sess, uint32_t start)
{
    for (uint32_t i = start; i < rplan->op_count; i++) {
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
         * 104K-row consolidation sorting on every sub-pass (Issue #367).
         * Issue #2127: a VARIABLE followed by FILTER can still reach a JOIN
         * with a recursive relation, so the skip also requires
         * wl_columnar_eval_plan_var_skip_scan() to find no relation with new
         * rows on the way to the head. */
        if (!sess->tdd_input_run && op->op == WL_PLAN_OP_VARIABLE
            && sess->current_iteration > 0
            && op->delta_mode == WL_DELTA_AUTO && op->relation_name) {
            /* Look ahead: base-case VARIABLE is followed by MAP (not JOIN). */
            bool next_is_join = (i + 1 < rplan->op_count
                && (rplan->ops[i + 1].op == WL_PLAN_OP_JOIN
                || rplan->ops[i + 1].op == WL_PLAN_OP_SEMIJOIN
                || rplan->ops[i + 1].op == WL_PLAN_OP_ANTIJOIN));
            if (!next_is_join
                && wl_columnar_eval_plan_var_skip_scan(rplan, i,
                session_has_delta_rows, sess)) {
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
                    sess->base_case_skips++;
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
            if (!sess->diff_operators_active && sess->join_batch_bytes > 0) {
                uint32_t next_index = i;
                wl_columnar_join_pipeline_decline_t decline
                    = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NONE;
                int pipeline_rc = wl_columnar_join_pipeline_try_eval(rplan, i,
                        stack, sess, &next_index, &decline);
                if (pipeline_rc == 1) {
                    i = next_index;
                    continue;
                }
                /* An ORDINARY decline leaves the outcome to
                 * wl_columnar_join_op.  A DELTA_RIGHT decline under strict
                 * mode is refused here, so the left input stays on the
                 * stack.  Otherwise a JOIN whose output reaches a relation
                 * sink (end of plan, CONCAT, CONSOLIDATE, or an EXCHANGE
                 * across more than one worker) has the outcome a final
                 * JOIN would have, and one read by another operator is
                 * recorded as pipeline-not-consumable.  A TDD bound run
                 * evaluates a slice of a rule's plan whose ops end where
                 * the slice ends, so a JOIN ending the slice reaches a
                 * relation sink here too (#2201). */
                if (pipeline_rc == 0) {
                    if (decline == WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY)
                        goto ordinary_join;
                    if (decline
                        == WL_COLUMNAR_JOIN_PIPELINE_DECLINE_DELTA_RIGHT) {
                        if (!sess->join_batch_strict)
                            goto ordinary_join;
                        col_join_batch_record_fallback(sess,
                            COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT);
                        return ENOTSUP;
                    }
                    if (wl_columnar_join_pipeline_successor(rplan, i + 1u)
                        == WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK)
                        goto ordinary_join;
                } else if (pipeline_rc != ENOENT && pipeline_rc != ENOTSUP
                    && pipeline_rc != EAGAIN) {
                    return pipeline_rc;
                }
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

/* Shared exact literal predicate grammar. Audit is allocation-free; execution
 * decodes once into admitted attempt scratch, never once per source row. */
bool
wl_columnar_eval_plan_cold_predicate_decode(const wl_plan_op_t *f,
    uint32_t ncols, const wirelog_column_type_t *types,
    wl_columnar_eval_plan_cold_predicate_t *out)
{
    if (!f) return false;
    const wl_plan_expr_buffer_t *e = &f->filter_expr;
    if (f->op != WL_PLAN_OP_FILTER || !e->data || e->size < 17 ||
        e->data[0] != WL_PLAN_EXPR_VAR) return false;
    uint32_t len = (uint32_t)e->data[1] | ((uint32_t)e->data[2] << 8);
    if (len < 4 || len + 13 != e->size ||
        memcmp(e->data + 3, "col", 3)) return false;
    uint32_t col = 0;
    for (uint32_t j = 6; j < len + 3; j++) {
        unsigned digit = (unsigned)e->data[j] - '0';
        if (digit > 9 || col > (UINT32_MAX - digit) / 10) return false;
        col = col * 10 + digit;
    }
    uint32_t pos = len + 3;
    if (col >= ncols || (types && types[col] != WIRELOG_TYPE_INT64) ||
        e->data[pos] != WL_PLAN_EXPR_CONST_INT ||
        e->data[pos + 9] < WL_PLAN_EXPR_CMP_EQ ||
        e->data[pos + 9] > WL_PLAN_EXPR_CMP_GTE) return false;
    if (out) {
        uint64_t literal = 0;
        for (unsigned i = 0; i < 8; i++)
            literal |= (uint64_t)e->data[pos + 1 + i] << (8 * i);
        memcpy(&out->literal, &literal, sizeof(literal));
        out->column = col;
        out->comparison = e->data[pos + 9];
    }
    return true;
}

/* Resolve a stage-local lane through preceding direct MAPs without an
 * allocation, recursion or a cardinality-sized schema. Earlier stages have
 * already been audited when this helper is used by the decoder. */
bool
wl_columnar_eval_plan_cold_column(const wl_plan_relation_t *rp,
    uint32_t end, uint32_t column, uint32_t ncols, uint32_t *source)
{
    if (!rp || !rp->ops || end > rp->op_count) return false;
    for (uint32_t i = end; i > 1; ) {
        const wl_plan_op_t *op = &rp->ops[--i];
        if (op->op != WL_PLAN_OP_MAP) continue;
        if (!op->project_indices || column >= op->project_count) return false;
        column = op->project_indices[column];
    }
    if (column >= ncols) return false;
    if (source) *source = column;
    return true;
}

/* Canonical direct-column MAP lane grammar shared with JOIN suffix audits. */
bool
wl_columnar_eval_plan_cold_map_lane(const wl_plan_op_t *m, uint32_t i,
    uint32_t width, wirelog_column_type_t type)
{
    if (!m || m->op != WL_PLAN_OP_MAP || !m->project_count ||
        !m->project_indices || i >= m->project_count ||
        m->project_indices[i] >= width ||
        (m->map_exprs == NULL) != (m->map_expr_count == 0) ||
        (m->map_expr_count &&
        m->map_expr_count != m->project_count)) return false;
    if (!m->map_expr_count) return true;
    const wl_plan_expr_buffer_t *e = &m->map_exprs[i];
    if (!e->size && !e->data) return true;
    if (!e->data || e->size < 7 ||
        (e->data[0] != WL_PLAN_EXPR_VAR &&
        e->data[0] != WL_PLAN_EXPR_VAR_FLOAT &&
        e->data[0] != WL_PLAN_EXPR_VAR_STRING)) return false;
    uint16_t len; memcpy(&len, e->data + 1, sizeof(len));
    if ((uint32_t)len + 3 != e->size ||
        memcmp(e->data + 3, "col", 3)) return false;
    uint32_t col = 0;
    for (uint32_t j = 6; j < e->size; j++) {
        unsigned digit = (unsigned)e->data[j] - '0';
        if (digit > 9 || col > (UINT32_MAX - digit) / 10) return false;
        col = col * 10 + digit;
    }
    return col == m->project_indices[i] &&
           ((e->data[0] == WL_PLAN_EXPR_VAR_FLOAT) ==
           (type == WIRELOG_TYPE_FLOAT)) &&
           (e->data[0] != WL_PLAN_EXPR_VAR_STRING ||
           type == WIRELOG_TYPE_STRING);
}

static bool
wl_columnar_eval_plan_cold_map(const wl_plan_relation_t *rp, uint32_t stage,
    uint32_t width, uint32_t ncols, const wirelog_column_type_t *types)
{
    const wl_plan_op_t *m = &rp->ops[stage];
    if (!m->project_count || !m->project_indices) return false;
    for (uint32_t i = 0; i < m->project_count; i++) {
        uint32_t source;
        if (!wl_columnar_eval_plan_cold_column(rp, stage,
            m->project_indices[i], ncols, &source) ||
            !wl_columnar_eval_plan_cold_map_lane(m, i, width,
            types ? types[source] : WIRELOG_TYPE_INT64)) return false;
    }
    return true;
}

/* Audit stage-local FILTER/MAP operations ending before JOIN or terminal
 * CONSOLIDATE. Standalone scans require a MAP; FILTER-only JOIN prefixes
 * preserve the source layout. Arbitrary expressions remain unsupported. */
bool
wl_columnar_eval_plan_cold_scan_decode(const wl_plan_relation_t *rp,
    const char *name, uint32_t ncols, const wirelog_column_type_t *types,
    uint32_t end, wl_columnar_eval_plan_cold_projection_t *out)
{
    if (!rp || !name || !rp->ops || end < 2 ||
        end >= rp->op_count) return false;
    const wl_plan_op_t *v = &rp->ops[0];
    if (v->op != WL_PLAN_OP_VARIABLE || !v->relation_name ||
        strcmp(v->relation_name, name) ||
        (rp->ops[end].op != WL_PLAN_OP_JOIN &&
        (rp->ops[end].op != WL_PLAN_OP_CONSOLIDATE ||
        end != rp->op_count - 1))) return false;
    wl_columnar_eval_plan_cold_projection_t decoded = {0};
    uint32_t width = ncols;
    for (uint32_t i = 0; i <= end; i++) {
        const wl_plan_op_t *op = &rp->ops[i];
        if (i == end && op->op == WL_PLAN_OP_JOIN) break;
        if (op->opaque_data || op->materialized || op->right_relation ||
            (i && op->relation_name) || op->right_filter_expr.size ||
            op->right_filter_expr.data ||
            (op->op != WL_PLAN_OP_FILTER &&
            (op->filter_expr.size || op->filter_expr.data)) ||
            (op->delta_mode != WL_DELTA_AUTO &&
            op->delta_mode != WL_DELTA_FORCE_FULL)) return false;
        if (!i || i == end) continue;
        if (op->op == WL_PLAN_OP_MAP) {
            if (!wl_columnar_eval_plan_cold_map(rp, i, width, ncols,
                types)) return false;
            decoded.map = op; width = op->project_count;
        } else if (op->op == WL_PLAN_OP_FILTER) {
            wl_columnar_eval_plan_cold_predicate_t predicate;
            uint32_t source;
            if (!wl_columnar_eval_plan_cold_predicate_decode(op, width, NULL,
                &predicate) || !wl_columnar_eval_plan_cold_column(rp, i,
                predicate.column, ncols, &source) ||
                (types && types[source] != WIRELOG_TYPE_INT64)) return false;
            decoded.filter_count++;
        } else return false;
    }
    if (!decoded.map && rp->ops[end].op == WL_PLAN_OP_CONSOLIDATE) return false;
    if (out) *out = decoded;
    return true;
}

bool
wl_columnar_eval_plan_cold_projection_decode(const wl_plan_relation_t *rp,
    const char *name, uint32_t ncols, const wirelog_column_type_t *types,
    wl_columnar_eval_plan_cold_projection_t *out)
{
    return rp && rp->ops && rp->op_count &&
           rp->ops[rp->op_count - 1].op == WL_PLAN_OP_CONSOLIDATE &&
           wl_columnar_eval_plan_cold_scan_decode(rp, name, ncols, types,
               rp->op_count - 1, out);
}

bool
wl_columnar_eval_plan_cold_projection(const wl_plan_relation_t *rp,
    const char *name, uint32_t ncols, const wirelog_column_type_t *types)
{
    return wl_columnar_eval_plan_cold_projection_decode(rp, name, ncols,
               types, NULL);
}

/* Decode only a serialized direct column, with no expression evaluation. */
bool
wl_columnar_eval_plan_aggregate_column(const wl_plan_expr_buffer_t *expr,
    uint32_t width, uint32_t *column)
{
    if (!expr || !expr->data || expr->size < 7 ||
        (expr->data[0] != WL_PLAN_EXPR_VAR &&
        expr->data[0] != WL_PLAN_EXPR_VAR_STRING &&
        expr->data[0] != WL_PLAN_EXPR_VAR_FLOAT)) return false;
    uint16_t len; memcpy(&len, expr->data + 1, sizeof(len));
    if ((uint32_t)len + 3 != expr->size ||
        memcmp(expr->data + 3, "col", 3)) return false;
    uint32_t col = 0;
    for (uint32_t i = 6; i < expr->size; i++) {
        unsigned digit = (unsigned)expr->data[i] - '0';
        if (digit > 9 || col > (UINT32_MAX - digit) / 10) return false;
        col = col * 10 + digit;
    }
    if (col >= width) return false;
    if (column) *column = col;
    return true;
}

/* Separate from scan decoding: a reduction needs no materialized prefix. */
bool
wl_columnar_eval_plan_cold_aggregate_decode(const wl_plan_relation_t *rp,
    const char *name, uint32_t ncols, const wirelog_column_type_t *types,
    wl_columnar_eval_plan_cold_aggregate_t *out)
{
    if (!rp || !name || !ncols || !rp->ops || rp->op_count < 3)
        return false;
    uint32_t end = rp->op_count - 2, width = ncols;
    const wl_plan_op_t *reduce = &rp->ops[end];
    if (rp->ops[0].op != WL_PLAN_OP_VARIABLE ||
        !rp->ops[0].relation_name || strcmp(rp->ops[0].relation_name,
        name) != 0 ||
        rp->ops[end + 1].op != WL_PLAN_OP_CONSOLIDATE ||
        reduce->op != WL_PLAN_OP_REDUCE ||
        (reduce->agg_fn != WIRELOG_AGG_COUNT &&
        reduce->agg_fn != WIRELOG_AGG_SUM &&
        reduce->agg_fn != WIRELOG_AGG_MIN &&
        reduce->agg_fn != WIRELOG_AGG_MAX) ||
        reduce->group_by_count == UINT32_MAX ||
        (reduce->group_by_count && !reduce->group_by_indices) ||
        (!reduce->group_by_count && reduce->group_by_indices) ||
        reduce->aggregate_index > reduce->group_by_count ||
        reduce->agg_result_type != WL_PLAN_AGG_OPERAND_SCALAR)
        return false;
    wl_columnar_eval_plan_cold_aggregate_t decoded = {.reduce_index = end};
    for (uint32_t i = 0; i < rp->op_count; i++) {
        const wl_plan_op_t *op = &rp->ops[i];
        if (op->opaque_data || op->materialized || op->right_relation ||
            op->key_count || op->left_keys || op->right_keys ||
            (i && op->relation_name) || op->right_filter_expr.size ||
            op->right_filter_expr.data ||
            (op->delta_mode != WL_DELTA_AUTO &&
            op->delta_mode != WL_DELTA_FORCE_FULL) ||
            (op->op != WL_PLAN_OP_FILTER &&
            (op->filter_expr.data || op->filter_expr.size)) ||
            (op->op != WL_PLAN_OP_MAP &&
            (op->project_count || op->project_indices || op->map_exprs ||
            op->map_expr_count)) ||
            (i != end &&
            (op->agg_expr.data || op->agg_expr.size || op->group_by_count ||
            op->group_by_indices)))
            return false;
        if (!i || i >= end) continue;
        if (op->op == WL_PLAN_OP_MAP) {
            if (!wl_columnar_eval_plan_cold_map(rp, i, width, ncols,
                types)) return false;
            width = op->project_count;
        } else if (op->op == WL_PLAN_OP_FILTER) {
            wl_columnar_eval_plan_cold_predicate_t predicate;
            uint32_t source;
            if (!wl_columnar_eval_plan_cold_predicate_decode(op, width, NULL,
                &predicate) ||
                !wl_columnar_eval_plan_cold_column(rp, i, predicate.column,
                ncols, &source) ||
                (types && types[source] != WIRELOG_TYPE_INT64)) return false;
            decoded.filter_count++;
        } else return false;
    }
    for (uint32_t i = 0; i < reduce->group_by_count; i++) {
        uint32_t source, column = reduce->group_by_indices[i];
        if (column >= width || !wl_columnar_eval_plan_cold_column(rp, end,
            column, ncols,
            &source) || (types && types[source] != WIRELOG_TYPE_INT64 &&
            types[source] != WIRELOG_TYPE_STRING)) return false;
    }
    const wl_plan_expr_buffer_t *expr = &reduce->agg_expr;
    if (expr->data || expr->size) {
        uint32_t column;
        if (!wl_columnar_eval_plan_aggregate_column(expr, width, &column) ||
            !wl_columnar_eval_plan_cold_column(rp, end, column, ncols,
            &decoded.operand)) return false;
    }
    if (reduce->agg_fn != WIRELOG_AGG_COUNT &&
        (!expr->data || !expr->size || expr->data[0] != WL_PLAN_EXPR_VAR ||
        reduce->agg_operand_type != WL_PLAN_AGG_OPERAND_SCALAR ||
        (types && types[decoded.operand] != WIRELOG_TYPE_INT64))) return false;
    if (out) *out = decoded;
    return true;
}

int
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    uint32_t aggregate_join;
    if (sess->cold_relation &&
        wl_columnar_join_aggregate_decode(sess, rplan, &aggregate_join) &&
        (wl_columnar_session_cold_find(sess, rplan->ops[0].relation_name) ||
        wl_columnar_session_cold_find(sess,
        rplan->ops[aggregate_join].right_relation))) {
        int rc = wl_columnar_session_cold_execution_check(sess);
        if (rc) return rc;
        const char *cold_left = wl_columnar_session_cold_find(sess,
                rplan->ops[0].relation_name) ? rplan->ops[0].relation_name :
            NULL;
        if (aggregate_join > 1 || !cold_left) {
            if (cold_left) rc = wl_columnar_ops_cold_scan(rplan, stack, sess,
                        aggregate_join);
            else {
                wl_plan_relation_t prefix = *rplan;
                prefix.op_count = aggregate_join;
                rc = wl_columnar_eval_plan_body(&prefix, stack, sess, 0);
            }
            if (rc) return rc;
            bool mapped = false;
            for (uint32_t i = 1; i < aggregate_join; i++)
                mapped |= rplan->ops[i].op == WL_PLAN_OP_MAP;
            if (aggregate_join > 1 && !mapped) {
                eval_entry_t *entry = &stack->items[stack->top - 1];
                rc = wl_columnar_relation_rename_checked(entry->rel,
                        rplan->ops[0].relation_name);
                if (rc) return rc;
            }
            cold_left = NULL;
        }
        rc = wl_columnar_join_aggregate(sess, rplan, stack, aggregate_join,
                cold_left);
        return rc ? rc : col_op_consolidate(stack, sess);
    }
    if (sess->cold_relation && rplan->ops) {
        for (uint32_t i = 0; i < rplan->op_count; i++) {
            wl_columnar_session_cold_relation_t *cold =
                wl_columnar_session_cold_find(
                sess, rplan->ops[i].relation_name);
            if (!cold) continue;
            if (wl_columnar_session_cold_execution_check(sess)) return ENOTSUP;
            if (i == 0 && wl_columnar_join_cold_prefix(sess, rplan)) {
                int rc = wl_columnar_join_cold_left(sess, rplan, stack);
                return rc ? rc : wl_columnar_eval_plan_body(rplan, stack, sess,
                           2);
            }
            uint32_t join_index;
            if (i == 0 && wl_columnar_join_cold_scan_prefix(sess, rplan,
                &join_index)) {
                int rc = wl_columnar_ops_cold_scan(rplan, stack, sess,
                        join_index);
                return rc ? rc : wl_columnar_eval_plan_body(rplan, stack, sess,
                           join_index);
            }
            if (i == 0 && wl_columnar_eval_plan_cold_aggregate_decode(rplan,
                cold->name, cold->ncols, wl_columnar_session_cold_types(cold),
                NULL)) {
                int rc = wl_columnar_ops_cold_aggregate(rplan, stack, sess);
                return rc ? rc : col_op_consolidate(stack, sess);
            }
            if (!wl_columnar_eval_plan_cold_projection(rplan, cold->name,
                cold->ncols,
                wl_columnar_session_cold_types(cold))) return ENOTSUP;
            int rc = wl_columnar_ops_cold_projection(rplan, stack, sess);
            return rc ? rc : col_op_consolidate(stack, sess);
        }
    }
    uint32_t join_index;
    if (sess->cold_relation && wl_columnar_join_cold_scan_prefix(sess, rplan,
        &join_index)) {
        int rc = wl_columnar_session_cold_execution_check(sess);
        if (rc) return rc;
        wl_plan_relation_t prefix = *rplan;
        prefix.op_count = join_index;
        rc = wl_columnar_eval_plan_body(&prefix, stack, sess, 0);
        if (rc) return rc;
        /* A resident FILTER retains INLINE layout but uses a temporary name.
         * Restore the audited declaration identity on its owned prefix so a
         * cold right JOIN can validate functor provenance before capture. */
        bool mapped = false;
        for (uint32_t i = 1; i < join_index; i++)
            mapped |= rplan->ops[i].op == WL_PLAN_OP_MAP;
        if (!mapped) {
            eval_entry_t entry;
            rc = eval_stack_pop_relation(stack, &entry);
            if (rc) return rc;
            if (!entry.owned) rc = ENOTSUP;
            else rc = wl_columnar_relation_rename_checked(entry.rel,
                        rplan->ops[0].relation_name);
            if (!rc) rc = eval_stack_push(stack, entry.rel, true);
            if (rc) {
                int disposed = eval_stack_dispose_entry(stack, &entry);
                return disposed ? disposed : rc;
            }
        }
        return wl_columnar_eval_plan_body(rplan, stack, sess, join_index);
    }
    if (!sess->tdd_input_run)
        return wl_columnar_eval_plan_body(rplan, stack, sess, 0);
    wl_columnar_eval_tdd_run_t *run = sess->tdd_input_run;
    if (!run->evaluating || run->current_op
        || rplan->ops != run->manifest->owner_ops + run->slice->start
        || rplan->op_count != run->slice->count)
        return EBUSY;
    int rc = wl_columnar_eval_plan_body(rplan, stack, sess, 0);
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

/* Opt-in numeric segment dispatch. Ordinary col_eval_relation_plan and public
 * session admission remain unchanged. Host grants pay dispatch and retry work;
 * child work continues to use its original operation attempt and own grants.
 * HOST includes read-only validation before admission as well as current
 * dispatch/retirement; its fee is distinct from the 256-slot poll quantum.
 * The fund helper has10 post-poll slots, plus caller rc/guard3. Direct-return
 * CHILD/ACK routing and its outer result/return give caller19; READY has19
 * after its real FORWARD poll, within JOIN's total forward caller32. Replacing
 * the older cleanup caller16 gives FINISH249-16+19=252, within256 and the two
 * prepaid128 shares. Numeric validation before HOST is61; the fixture's engine
 * selection adds2 before that poll, financing63+252=315 within384. These are
 * logical source slots under MEMORY.md's exclusions, not elapsed time. */
enum {
    WL_COLUMNAR_EVAL_PLAN_NUMERIC_FORWARD_WORK = 128
};

static bool
wl_columnar_eval_plan_numeric_thread(
    const wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (!record->thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(record->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return record->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(record->owner_thread, pthread_self()) != 0;
#endif
}

bool
wl_columnar_eval_plan_numeric_owner_valid(const void *owner,
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
    return record && record->identity == record && record->frame == frame
           && record->session
           && ((record->phase > WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT
           && record->phase <= WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE)
           || (record->phase >=
           WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_INPUTS
           && record->phase <= WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE))
           && wl_columnar_eval_plan_numeric_thread(record);
}

bool
wl_columnar_eval_plan_numeric_batch_fallback_authorized(const void *owner,
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_col_session_t *session, const wl_evaluation_control_t *control,
    uint64_t attempt_id)
{
    const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
    return wl_columnar_eval_plan_numeric_owner_valid(owner, frame)
           && record->session == session && record->control == control
           && record->attempt_id == attempt_id && record->composed_mode
           && record->controlled_materialized_batch_fallback;
}

static int
wl_columnar_eval_plan_numeric_fund(wl_evaluation_control_t *funding,
    uint64_t id, const wl_evaluation_control_t *operation)
{
    if (!funding || funding == operation || !id
        || wl_evaluation_control_active_attempt_id(funding) != id)
        return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(funding, id,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK);
    if (rc) return rc;
    return wl_evaluation_control_active_attempt_id(funding) == id ? 0 : EINVAL;
}

/* Shared semantic leaves: old full validation retains its loops/fees; the
* new controlled context invokes only one entry or one pair per action. */
static bool
wl_columnar_eval_plan_private_entry_valid(
    const wl_columnar_eval_private_binding_t *entry)
{
    return entry && entry->name[0] && memchr(entry->name, '\0',
               sizeof(entry->name))
           && entry->relation &&
           wl_columnar_relation_snapshot_equal(entry->snapshot,
               wl_columnar_relation_snapshot(entry->relation));
}

static bool
wl_columnar_eval_plan_private_pair_distinct(
    const wl_columnar_eval_private_binding_t *left,
    const wl_columnar_eval_private_binding_t *right)
{
    return strcmp(left->name, right->name) != 0;
}

static int
wl_columnar_eval_plan_numeric_validate(
    const wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (!record || !wl_columnar_eval_plan_numeric_owner_valid(record,
        record->frame)) return EINVAL;
    if (record->session->cleanup_active != record->frame) return EBUSY;
    if (record->private_bindings) {
        const wl_columnar_eval_private_bindings_t *bindings =
            record->private_bindings;
        if (bindings->identity != (uintptr_t)bindings
            || bindings->session != record->session
            || bindings->plan != record->plan
            || bindings->control != record->control
            || bindings->attempt_id != record->attempt_id
            || !bindings->count
            || (!bindings->dynamic_entries
            && bindings->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX)
            || wl_evaluation_control_active_attempt_id(record->control)
            != record->attempt_id)
            return EINVAL;
        if (bindings->dynamic_entries) {
            if (!wl_columnar_session_observed_bindings_authorized(
                    record->session, bindings)) return EINVAL;
        }
        for (uint32_t i = 0; !bindings->dynamic_entries
            && i < bindings->count; ++i) {
            const wl_columnar_eval_private_binding_t *entry =
                &bindings->entries[i];
            if (!wl_columnar_eval_plan_private_entry_valid(
                    entry)) return EINVAL;
            for (uint32_t j = 0; j < i; ++j)
                if (!wl_columnar_eval_plan_private_pair_distinct(
                        &bindings->entries[j], entry))
                    return EINVAL;
        }
    }
    return wl_columnar_eval_stack_numeric_owner(record->frame, record);
}

int
wl_columnar_eval_private_binding_get(
    const wl_columnar_eval_private_bindings_t *bindings, uint32_t index,
    const char **name, col_rel_t **relation)
{
    if (!bindings || index >= bindings->count || !name || !relation)
        return EINVAL;
    col_relation_snapshot_t snapshot;
    if (bindings->dynamic_entries) {
        if (!wl_columnar_session_observed_bindings_authorized(
                bindings->session, bindings)) return EINVAL;
        const wl_columnar_eval_private_dynamic_binding_t *entry =
            &bindings->dynamic_entries[index];
        *name = entry->name;
        *relation = entry->relation;
        snapshot = entry->snapshot;
    } else {
        if (index >= WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX) return EINVAL;
        *name = bindings->entries[index].name;
        *relation = bindings->entries[index].relation;
        snapshot = bindings->entries[index].snapshot;
    }
    if (!*name || (*relation && !wl_columnar_relation_snapshot_equal(
            snapshot, wl_columnar_relation_snapshot(*relation))))
        return EINVAL;
    return 0;
}

int
wl_columnar_eval_private_bindings_begin(
    wl_columnar_eval_private_bindings_t *bindings, wl_col_session_t *session,
    const wl_plan_relation_t *plan, wl_evaluation_control_t *control,
    uint64_t attempt_id, const char *const *names,
    col_rel_t *const *relations, uint32_t count)
{
    if (!bindings || bindings->identity || !session || !plan || !plan->ops
        || !control || !attempt_id || !names || !relations || !count
        || count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX
        || session->base.evaluation_control != control
        || wl_evaluation_control_active_attempt_id(control) != attempt_id)
        return EINVAL;
    memset(bindings, 0, sizeof(*bindings));
    bindings->identity = (uintptr_t)bindings;
    bindings->session = session;
    bindings->plan = plan;
    bindings->control = control;
    bindings->attempt_id = attempt_id;
    bindings->count = count;
    for (uint32_t i = 0; i < count; ++i) {
        if (!names[i] || !relations[i]) goto invalid;
        size_t length = strnlen(names[i], sizeof(bindings->entries[i].name));
        if (!length || length == sizeof(bindings->entries[i].name))
            goto invalid;
        memcpy(bindings->entries[i].name, names[i], length + 1);
        bindings->entries[i].relation = relations[i];
        bindings->entries[i].snapshot =
            wl_columnar_relation_snapshot(relations[i]);
        if (!wl_columnar_relation_snapshot_valid(
                bindings->entries[i].snapshot)) goto invalid;
        for (uint32_t j = 0; j < i; ++j)
            if (!wl_columnar_eval_plan_private_pair_distinct(
                    &bindings->entries[j],
                    &bindings->entries[i])) goto invalid;
    }
    return 0;

invalid:
    memset(bindings, 0, sizeof(*bindings));
    return EINVAL;
}

int
wl_columnar_eval_plan_numeric_private_bindings_get(const void *owner,
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_col_session_t *session, wl_evaluation_control_t *control, uint64_t id,
    const wl_columnar_eval_private_bindings_t **out)
{
    const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
    if (!out || (uintptr_t)out > UINTPTR_MAX - sizeof(*out)
        || (record && !wl_columnar_eval_stack_copy_ranges_disjoint(record,
        sizeof(*record), out, sizeof(*out)))
        || !wl_columnar_eval_stack_marked_copy_output(session, out,
        sizeof(*out))) return EINVAL;
    /* Reject absent or nonself owners before output normalization.
     * This refusal intentionally leaves unproven output untouched. */
    if (!record || record->identity != owner) return EINVAL;
    /* A mismatching supplied session must not hide the actual marker.
     * This is read-only exclusion, independent of supplied frame/control/id
     * and native authorization; the complete validator still runs below. */
    if (record->session != session
        && !wl_columnar_eval_stack_marked_copy_output(record->session, out,
        sizeof(*out))) return EINVAL;
    if (record->frame != frame
        || record->session != session || record->control != control
        || record->attempt_id != id
        || wl_columnar_eval_plan_numeric_validate(record))
        return EINVAL;
    *out = NULL;
    if (record->private_bindings) {
        const wl_columnar_eval_private_bindings_t *bindings =
            record->private_bindings;
        if (bindings->identity != (uintptr_t)bindings
            || bindings->session != session || bindings->plan != record->plan
            || bindings->control != control || bindings->attempt_id != id)
            return EINVAL;
        *out = bindings;
    }
    return 0;
}

/* Only the current action is admitted. Completed metadata fees survive a
 * later refusal, but no proof or ownership token survives this activation. */
wl_columnar_join_setup_result_t
wl_columnar_join_setup_head(wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id, uint64_t work)
{
    if (!funding || funding == control || !funding_id)
        return (wl_columnar_join_setup_result_t){ EINVAL,
                                                  WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED };
    int rc = wl_evaluation_control_charge_attempt(funding, funding_id, work);
    if (rc) return (wl_columnar_join_setup_result_t){ rc,
                                                      WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED };
    rc = wl_evaluation_control_charge_attempt(control, id, 0);
    return (wl_columnar_join_setup_result_t){ rc, rc
               ? WL_COLUMNAR_JOIN_SETUP_ORIGINAL_STOPPED
               : WL_COLUMNAR_JOIN_SETUP_CHILD_DISPATCHED };
}

wl_columnar_join_setup_result_t
wl_columnar_join_setup_tail(wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_evaluation_control_charge_attempt(control, id, 0);
    if (rc) return (wl_columnar_join_setup_result_t){ rc,
                                                      WL_COLUMNAR_JOIN_SETUP_ORIGINAL_STOPPED };
    rc = wl_evaluation_control_charge_attempt(funding, funding_id, 0);
    return (wl_columnar_join_setup_result_t){ rc, rc
               ? WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED
               : WL_COLUMNAR_JOIN_SETUP_CHILD_DISPATCHED };
}

/* Only the complete HEADER in this activation can reach this private head.
 * Pointer/nonzero arguments stay immutable; active generations never do, so
 * both exact saved-ID charges remain mandatory for every actual action. */
static wl_columnar_join_setup_result_t
wl_columnar_eval_plan_binding_progress_head(wl_evaluation_control_t *control,
    uint64_t id, wl_evaluation_control_t *funding, uint64_t funding_id,
    uint64_t work)
{
    int rc = wl_evaluation_control_charge_attempt(funding, funding_id, work);
    if (rc) return (wl_columnar_join_setup_result_t){ rc,
                                                      WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED };
    rc = wl_evaluation_control_charge_attempt(control, id, 0);
    return (wl_columnar_join_setup_result_t){ rc, rc
               ? WL_COLUMNAR_JOIN_SETUP_ORIGINAL_STOPPED
               : WL_COLUMNAR_JOIN_SETUP_CHILD_DISPATCHED };
}

wl_columnar_join_setup_result_t
wl_columnar_eval_plan_numeric_private_bindings_get_funded(const void *owner,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id,
    const wl_columnar_eval_private_bindings_t **out)
{
    const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
    if (!out || (uintptr_t)out > UINTPTR_MAX - sizeof(*out)
        || (record && !wl_columnar_eval_stack_copy_ranges_disjoint(record,
        sizeof(*record), out, sizeof(*out)))
        || !wl_columnar_eval_stack_marked_copy_output(session, out,
        sizeof(*out))) goto invalid;
    if (!record || record->identity != owner) goto invalid;
    if (record->session != session
        && !wl_columnar_eval_stack_marked_copy_output(record->session, out,
        sizeof(*out))) goto invalid;
    if (!funding || funding == control || !funding_id
        || funding == record->control)
        return (wl_columnar_join_setup_result_t){ EINVAL,
                                                  WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED };
    int rc = wl_evaluation_control_charge_attempt(funding, funding_id,
            WL_COLUMNAR_JOIN_SETUP_HEADER_WORK);
    if (rc) return (wl_columnar_join_setup_result_t){ rc,
                                                      WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED };
    if (record->frame != frame || record->session != session
        || record->control != control || record->attempt_id != id
        || !control || !id
        || wl_columnar_eval_stack_numeric_owner(frame, record)
        || session->cleanup_active != frame) goto invalid;
    *out = NULL;
    const wl_columnar_eval_private_bindings_t *bindings =
        record->private_bindings;
    if (bindings && (bindings->identity != (uintptr_t)bindings
        || bindings->session != session || bindings->plan != record->plan
        || bindings->control != control || bindings->attempt_id != id
        || !bindings->count || (!bindings->dynamic_entries
        && bindings->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX)))
        goto invalid;
    rc = wl_evaluation_control_charge_attempt(control, id, 0);
    if (rc) return (wl_columnar_join_setup_result_t){ rc,
                                                      WL_COLUMNAR_JOIN_SETUP_ORIGINAL_STOPPED };
    wl_columnar_join_setup_result_t result =
        wl_columnar_join_setup_tail(control, id, funding, funding_id);
    if (result.protocol) return result;
    if (bindings && bindings->dynamic_entries) {
        result = wl_columnar_eval_plan_binding_progress_head(control, id,
                funding, funding_id,
                WL_COLUMNAR_JOIN_SETUP_DYNAMIC_WORK);
        if (result.protocol) return result;
        if (!wl_columnar_session_observed_bindings_authorized(session,
            bindings)) goto invalid;
        result = wl_columnar_join_setup_tail(control, id, funding, funding_id);
        if (result.protocol) return result;
    }
    for (uint32_t i = 0; bindings && !bindings->dynamic_entries
        && i < bindings->count; ++i) {
        result = wl_columnar_eval_plan_binding_progress_head(control, id,
                funding, funding_id,
                WL_COLUMNAR_JOIN_SETUP_ENTRY_WORK);
        if (result.protocol) return result;
        const wl_columnar_eval_private_binding_t *entry = &bindings->entries[i];
        if (!wl_columnar_eval_plan_private_entry_valid(entry)) goto invalid;
        result = wl_columnar_join_setup_tail(control, id, funding, funding_id);
        if (result.protocol) return result;
        for (uint32_t j = 0; j < i; ++j) {
            result = wl_columnar_eval_plan_binding_progress_head(control, id,
                    funding,
                    funding_id, WL_COLUMNAR_JOIN_SETUP_PAIR_WORK);
            if (result.protocol) return result;
            if (!wl_columnar_eval_plan_private_pair_distinct(
                    &bindings->entries[j], entry)) goto invalid;
            result = wl_columnar_join_setup_tail(control, id, funding,
                    funding_id);
            if (result.protocol) return result;
        }
    }
    result = wl_columnar_eval_plan_binding_progress_head(control, id, funding,
            funding_id,
            WL_COLUMNAR_JOIN_SETUP_FINISH_WORK);
    if (result.protocol) return result;
    if (wl_columnar_eval_stack_numeric_owner(frame, record)
        || (bindings && (bindings->identity != (uintptr_t)bindings
        || bindings->session != session || bindings->plan != record->plan
        || bindings->control != control || bindings->attempt_id != id)))
        goto invalid;
    result = wl_columnar_join_setup_tail(control, id, funding, funding_id);
    if (result.protocol) return result;
    *out = bindings;
    return result;
invalid:
    return (wl_columnar_join_setup_result_t){ EINVAL,
                                              WL_COLUMNAR_JOIN_SETUP_INVALID };
}

static int
wl_columnar_eval_plan_numeric_forward(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (!record->control || wl_evaluation_control_active_attempt_id(
            record->control) != record->attempt_id) return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(record->control,
            record->attempt_id,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_FORWARD_WORK);
    if (rc) return rc;
    return wl_evaluation_control_active_attempt_id(record->control)
           == record->attempt_id ? 0 : EINVAL;
}

wl_columnar_join_setup_result_t
wl_columnar_eval_plan_numeric_composed_join_setup_handoff(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t attempt_id,
    const wl_plan_op_t *op)
{
    if (!record) return (wl_columnar_join_setup_result_t){ EINVAL,
                                                           WL_COLUMNAR_JOIN_SETUP_INVALID };
    int rc = wl_columnar_eval_stack_numeric_owner(frame, record);
    if (rc) return (wl_columnar_join_setup_result_t){ rc,
                                                      WL_COLUMNAR_JOIN_SETUP_INVALID };
    if (!record->composed_busy || !record->composed_mode
        || record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
        || record->session != session || record->control != control
        || record->attempt_id != attempt_id || !record->plan
        || !record->plan->ops || record->cursor < record->begin
        || record->cursor >= record->end
        || record->cursor >= record->plan->op_count
        || op !=
        &record->plan->ops[record->cursor]) return (
            wl_columnar_join_setup_result_t){ EINVAL,
                                              WL_COLUMNAR_JOIN_SETUP_INVALID };
    if (op->op != WL_PLAN_OP_JOIN && op->op != WL_PLAN_OP_SEMIJOIN
        && op->op !=
        WL_PLAN_OP_ANTIJOIN) return (wl_columnar_join_setup_result_t){ EINVAL,
                                                                       WL_COLUMNAR_JOIN_SETUP_INVALID };
    rc = wl_columnar_eval_plan_numeric_forward(record);
    if (rc) return (wl_columnar_join_setup_result_t){ rc,
                                                      WL_COLUMNAR_JOIN_SETUP_ORIGINAL_STOPPED };
    record->pending_action = record->phase;
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
    return (wl_columnar_join_setup_result_t){ 0,
                                              WL_COLUMNAR_JOIN_SETUP_CHILD_DISPATCHED };
}

/* The parsed one-edge and two-edge recursive reachability rules lower to
 * these exact seven- and eight-op relations. Their final zero-worker EXCHANGE
 * is metadata for the ordinary multi-worker planner; under the controlled,
 * single-session composed evaluator it has no redistribution work to do.
 * Keep this bridge narrower than generic EXCHANGE support. */
/* These two local wrappers prove the immutable plan/extent before this
 * shared metadata observation. It carries no execution authority. */
static bool
wl_columnar_eval_plan_recursive_terminal_metadata_valid(
    const wl_plan_relation_t *plan, uint32_t end)
{
    static const uint8_t map_col0[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'
    };
    static const uint8_t map_col1[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'
    };
    const wl_plan_op_t *ops = plan->ops;
    uint32_t join_count = end == 7 ? 1 : 2;
    uint32_t last_join = 2 + join_count;
    uint32_t concat = last_join + 1;
    uint32_t consolidate = concat + 1;
    uint32_t exchange_index = consolidate + 1;
    for (uint32_t i = 0; i < end; ++i) {
        const wl_plan_op_t *op = &ops[i];
        bool join = i >= 3 && i <= last_join;
        bool projected = i == 1 || (join
            && (join_count == 1 || i == last_join));
        if (op->filter_expr.data || op->filter_expr.size
            || op->agg_expr.data || op->agg_expr.size
            || op->agg_fn != WIRELOG_AGG_COUNT
            || op->group_by_indices || op->group_by_count
            || op->aggregate_index || op->right_filter_expr.data
            || op->right_filter_expr.size
            || op->agg_operand_type != WL_PLAN_AGG_OPERAND_UNKNOWN
            || op->agg_result_type != WL_PLAN_AGG_OPERAND_UNKNOWN
            || op->delta_mode != WL_DELTA_AUTO || op->materialized
            || (i != 0 && i != 2 && op->relation_name)
            || (!join && (op->right_relation || op->left_keys
            || op->right_keys || op->key_count))
            || (!projected
            && (op->project_indices || op->project_count))
            || (i != 1 && (op->map_exprs || op->map_expr_count))
            || (i != exchange_index && op->opaque_data))
            return false;
    }
    if (!plan->name || strcmp(plan->name, "path") != 0 || !plan->delta_name
        || strcmp(plan->delta_name, "$d$path") != 0
        || ops[0].op != WL_PLAN_OP_VARIABLE || !ops[0].relation_name
        || strcmp(ops[0].relation_name, "edge") != 0
        || ops[0].delta_mode != WL_DELTA_AUTO
        || ops[1].op != WL_PLAN_OP_MAP
        || ops[2].op != WL_PLAN_OP_VARIABLE || !ops[2].relation_name
        || strcmp(ops[2].relation_name, "path") != 0
        || ops[2].delta_mode != WL_DELTA_AUTO
        || ops[1].project_count != 2 || !ops[1].project_indices
        || ops[1].project_indices[0] != 0 || ops[1].project_indices[1] != 1
        || ops[1].map_expr_count != 2 || !ops[1].map_exprs
        || ops[1].map_exprs[0].size != sizeof(map_col0)
        || !ops[1].map_exprs[0].data
        || memcmp(ops[1].map_exprs[0].data, map_col0, sizeof(map_col0))
        || ops[1].map_exprs[1].size != sizeof(map_col1)
        || !ops[1].map_exprs[1].data
        || memcmp(ops[1].map_exprs[1].data, map_col1, sizeof(map_col1))
        || ops[concat].op != WL_PLAN_OP_CONCAT
        || ops[consolidate].op != WL_PLAN_OP_CONSOLIDATE
        || ops[exchange_index].op != WL_PLAN_OP_EXCHANGE
        || !ops[exchange_index].opaque_data)
        return false;
    for (uint32_t i = 3; i <= last_join; ++i) {
        if (ops[i].op != WL_PLAN_OP_JOIN || !ops[i].right_relation
            || strcmp(ops[i].right_relation, "edge") != 0
            || ops[i].key_count != 1 || !ops[i].left_keys
            || !ops[i].right_keys || !ops[i].left_keys[0]
            || !ops[i].right_keys[0]
            || strcmp(ops[i].right_keys[0], "col0") != 0
            || ops[i].materialized || ops[i].delta_mode != WL_DELTA_AUTO)
            return false;
    }
    if (join_count == 1) {
        if (strcmp(ops[3].left_keys[0], "col1") != 0
            || ops[3].project_count != 2 || !ops[3].project_indices
            || ops[3].project_indices[0] != 0
            || ops[3].project_indices[1] != 3)
            return false;
    } else if (strcmp(ops[3].left_keys[0], "col1") != 0
        || ops[3].project_count || ops[3].project_indices
        || strcmp(ops[4].left_keys[0], "col3") != 0
        || ops[4].project_count != 2 || !ops[4].project_indices
        || ops[4].project_indices[0] != 0
        || ops[4].project_indices[1] != 5) {
        return false;
    }
    const wl_plan_op_exchange_t *exchange =
        (const wl_plan_op_exchange_t *)ops[exchange_index].opaque_data;
    bool valid = exchange->num_workers == 0 && exchange->key_col_count == 1
        && exchange->key_col_idxs && exchange->key_col_idxs[0] == 1
        && !exchange->edb_rel_name;
    if (join_count == 1)
        valid = valid && exchange->edb_key_col_count == 1
            && exchange->edb_key_col_idxs
            && exchange->edb_key_col_idxs[0] == 0;
    else
        valid = valid && exchange->edb_key_col_count == 0
            && !exchange->edb_key_col_idxs;
    return valid;
}

static bool
wl_columnar_eval_plan_composed_terminal_exchange_valid(
    const wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (!record || !record->composed_mode || !record->private_bindings
        || record->begin != 0 || !record->plan || !record->plan->ops
        || (record->end != 7 && record->end != 8)
        || record->plan->op_count != record->end)
        return false;
    return wl_columnar_eval_plan_recursive_terminal_metadata_valid(
        record->plan, record->end);
}

bool
wl_columnar_eval_plan_numeric_recursive_path_supported(
    const wl_plan_relation_t *plan)
{
    if (!plan || !plan->ops) return false;
    uint32_t end = plan->op_count;
    if (end != 7 && end != 8) return false;
    return wl_columnar_eval_plan_recursive_terminal_metadata_valid(plan, end);
}

void
wl_columnar_eval_plan_numeric_attempt_init(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    /* Fresh caller-owned storage only; never reset a live continuation. */
    if (record) memset(record, 0, sizeof(*record));
}

int
wl_columnar_eval_plan_numeric_attempt_begin(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!record || record->identity || record->phase || !plan || !plan->ops
        || begin >= end || end > plan->op_count || !frame || !session
        || !control || !id) return EINVAL;
    if (wl_columnar_eval_stack_frame_mutation_blocked(frame)) return EBUSY;
    int rc = wl_columnar_eval_plan_numeric_fund(funding, funding_id, control);
    if (rc) return rc;
    if (wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    record->identity = record; record->frame = frame;
    record->session = session; record->plan = plan;
    record->control = control; record->attempt_id = id;
    record->begin = begin; record->end = end; record->cursor = begin;
    record->source_mode = false;
    record->completed = 0; record->semantic_result = 0;
    record->protocol_result = 0;
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PREFLIGHT;
#if defined(WL_HAVE_C11_THREADS)
    record->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    record->owner_thread = GetCurrentThreadId();
#else
    record->owner_thread = pthread_self();
#endif
    record->thread_valid = true;
    rc = wl_columnar_eval_stack_numeric_claim(frame, session, record);
    if (rc) {
        record->control = NULL; record->identity = NULL;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
        record->thread_valid = false; record->frame = NULL;
        record->session = NULL; record->plan = NULL;
        wl_evaluation_control_release(control);
    }
    return rc;
}

/* Each lookup action visits one NULL slot or one name byte. The registry,
 * names and descriptors are immutable under the serialized host contract.
 * The virtual delta name matches snprintf(dname,256,"$d$%s",name), including
 * its 252-byte source-name truncation; full names remain unbounded. */
static col_rel_t *
wl_columnar_eval_plan_numeric_source_candidate(
    wl_columnar_eval_plan_numeric_attempt_t *record, uint32_t slot,
    const char **actual_name)
{
    if (record->private_bindings) {
        if (slot < record->private_bindings->count) {
            col_rel_t *relation = NULL;
            int rc = wl_columnar_eval_private_binding_get(
                record->private_bindings, slot, actual_name, &relation);
            if (rc) {
                record->semantic_result = rc;
                record->phase = record->composed_mode
                    ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
                    : WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
            }
            return relation;
        }
        slot -= record->private_bindings->count;
    }
    col_rel_t *candidate = record->source_registry[slot];
    *actual_name = candidate ? candidate->name : NULL;
    return candidate;
}

static void
wl_columnar_eval_plan_numeric_source_lookup(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    bool delta = record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_DELTA;
    if (record->source_slot == record->source_count) {
        if (!delta) {
            record->semantic_result = ENOENT;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
        } else {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SELECT;
        }
        return;
    }
    const char *actual_name = NULL;
    col_rel_t *candidate = wl_columnar_eval_plan_numeric_source_candidate(
        record, record->source_slot, &actual_name);
    if (record->semantic_result) return;
    if (!actual_name) {
        ++record->source_slot;
        record->source_byte = 0;
        return;
    }
    const char *name = record->plan->ops[record->source_begin].relation_name;
    size_t byte = record->source_byte;
    char wanted;
    if (!delta) wanted = name[byte];
    else if (byte < 3) wanted = "$d$"[byte];
    else if (byte == 255) wanted = '\0';
    else wanted = name[byte - 3];
    char actual = actual_name[byte];
    if (actual != wanted) {
        ++record->source_slot;
        record->source_byte = 0;
    } else if (actual) {
        ++record->source_byte;
    } else if (!candidate) {
        record->semantic_result = ENOENT;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
    } else if (delta) {
        record->source_delta = candidate;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SELECT;
    } else {
        record->source_full = candidate;
        record->source_slot = 0; record->source_byte = 0;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_DELTA;
    }
}

bool
wl_columnar_eval_plan_scalar_selector_supported(const wl_plan_op_t *op)
{
    if (op->op != WL_PLAN_OP_VARIABLE && op->op != WL_PLAN_OP_JOIN)
        return false;
    switch (op->delta_mode) {
    case WL_DELTA_AUTO:
    case WL_DELTA_FORCE_FULL:
    case WL_DELTA_FORCE_DELTA: return true;
    case WL_DELTA_FORCE_EMPTY:
    case WL_DELTA_FORCE_EMPTY_AFTER_SEED:
        return op->op == WL_PLAN_OP_VARIABLE;
    default: return false;
    }
}

static void
wl_columnar_eval_plan_numeric_source_select(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    const wl_plan_op_t *op = &record->plan->ops[record->source_begin];
    col_rel_t *full = record->source_full, *delta = record->source_delta;
    record->source_selected = full;
    if (op->delta_mode == WL_DELTA_FORCE_EMPTY) {
        record->source_empty = true;
    } else if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows) {
            record->source_selected = delta;
            record->source_is_delta = true;
        } else if (record->session->delta_seeded) {
            record->source_empty = true;
            record->source_is_delta = true;
        }
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && delta
        && delta->nrows && delta->nrows < full->nrows) {
        /* Includes FORCE_EMPTY_AFTER_SEED and unknown modes at iteration0,
         * matching the raw VARIABLE fallback to AUTO. */
        record->source_selected = delta;
        record->source_is_delta = true;
    }
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INIT;
}

/* A shape hint selects a reader; only its actual BEGIN/STEP establishes
 * authority. Normal FULL/AUTO borrow the original descriptor after END. */
static int
wl_columnar_eval_plan_source_init(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    r->source_bare = !r->source_selected->schema_ok
        && !r->source_selected->ncols && !r->source_selected->nrows;
    if (!r->source_bare)
        return wl_columnar_relation_scalar_source_init_attempt_controlled(
            &r->source, &r->source_initialized, r->control, r->attempt_id);
    int rc = wl_columnar_relation_empty_source_begin(&r->source_empty_reader,
            r->source_selected, r->control, r->attempt_id);
    r->source_initialized = r->source_empty_reader.identity ==
        &r->source_empty_reader && r->source_empty_reader.phase !=
        WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED;
    if (!r->source_initialized) r->source_bare = false;
    return rc;
}

static int
wl_columnar_eval_plan_source_hold(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    /* The scalar reader also admits an INLINE EDB (#2301). */
    if (!r->source_bare)
        return wl_columnar_relation_scalar_source_admit_retained_attempt_controlled(
            &r->source, r->source_selected, !r->source_empty,
            WL_COLUMNAR_RELATION_SOURCE_ADMIT_INLINE);
    bool done = false;
    return wl_columnar_relation_empty_source_step(&r->source_empty_reader,
               &done);
}

static int
wl_columnar_eval_plan_source_new(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    const char *name = r->source_is_delta ? "$empty_delta" : "$empty_skip";
    wl_columnar_memory_governor_ref_t *governor = r->session->memory_governor
        ? r->session->memory_governor : r->source_selected->memory_governor;
    if (r->source_bare)
        return
            wl_columnar_relation_empty_source_new_like_operator_attempt_controlled
            (
            &r->source_empty_reader, name, governor, r->source_output);
    return
        wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled(
        &r->source, name, governor, r->source_output);
}

static bool
wl_columnar_eval_plan_source_drained(
    const wl_columnar_eval_plan_numeric_attempt_t *r)
{
    return r->source_bare ? r->source_empty_reader.phase ==
           WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED : r->source.phase ==
           WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED;
}

static int
wl_columnar_eval_plan_source_end(wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!r->source_bare) {
        bool done = false;
        return wl_columnar_relation_scalar_source_end_step(&r->source,
                   r->semantic_result, funding, id, &done);
    }
    bool done = false;
    int rc = wl_columnar_relation_empty_source_end(&r->source_empty_reader,
            funding, id, &done);
    return rc == EAGAIN ? 0 : rc;
}

/* Only these authenticated non-stream END actions replace HOST384 with
 * dispatch1 plus the actual reader END charges. Receipt consumption remains
 * a separate HOST384 action. */
static int
wl_columnar_eval_plan_source_end_fund(wl_evaluation_control_t *funding,
    uint64_t id, const wl_evaluation_control_t *control)
{
    if (!funding || !id || wl_evaluation_control_identity(funding) ==
        wl_evaluation_control_identity(control)
        || wl_evaluation_control_active_attempt_id(funding) != id)
        return EINVAL;
    return wl_evaluation_control_charge_attempt(funding, id, 1);
}

/* Forward source actions are one registry byte/opcode or a finite state
 * transition. Child calls return directly; their results are consumed only
 * on a later separately funded host turn. */
static int
wl_columnar_eval_plan_numeric_source_step(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc;
    switch (record->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_PREFLIGHT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_FULL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_DELTA:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SELECT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL:
        rc = wl_columnar_eval_plan_numeric_forward(record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = record->source_initialized
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END
                : WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
            return 0;
        }
        break;
    default:
        break;
    }
    switch (record->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_PREFLIGHT: {
        wl_plan_op_type_t kind = record->plan->ops[record->cursor].op;
        bool supported = record->cursor == record->source_begin
            ? kind == WL_PLAN_OP_VARIABLE
            : kind == WL_PLAN_OP_FILTER || kind == WL_PLAN_OP_MAP
            || kind == WL_PLAN_OP_JOIN || kind == WL_PLAN_OP_SEMIJOIN
            || kind == WL_PLAN_OP_ANTIJOIN || kind == WL_PLAN_OP_CONSOLIDATE
            || kind == WL_PLAN_OP_REDUCE;
        if (!supported) {
            record->semantic_result = ENOTSUP;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
        } else if (++record->cursor == record->end) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_FULL;
        }
        return 0;
    }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_FULL:
        if (!record->plan->ops[record->source_begin].relation_name) {
            record->semantic_result = ENOENT;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
            return 0;
        }
    /* fall through */
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_DELTA:
        wl_columnar_eval_plan_numeric_source_lookup(record);
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SELECT:
        wl_columnar_eval_plan_numeric_source_select(record);
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INIT:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_eval_plan_source_init(record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_HOLD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_eval_plan_source_hold(record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SLOT:
        rc = wl_columnar_eval_stack_numeric_source_output(record->frame,
                record->session, record);
        if (rc) return rc;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_eval_plan_source_new(record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_eval_plan_source_end(record, funding, funding_id);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL:
        rc = wl_columnar_eval_stack_numeric_source_install(record->frame,
                record->session, record);
        if (rc) return rc;
        record->cursor = record->begin;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PREFLIGHT;
        return 0;
    default:
        return EBUSY;
    }
}

static int
wl_columnar_eval_plan_numeric_source_consume(
    wl_columnar_eval_plan_numeric_attempt_t *record, int child_result)
{
    record->protocol_result = 0;
    switch (record->pending_action) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INIT:
        if (child_result) {
            record->semantic_result = child_result;
            record->phase = record->source_initialized
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END
                : WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
        } else {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_HOLD;
        }
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_HOLD:
        if (!child_result && record->source_bare
            && record->source_empty_reader.phase !=
            WL_COLUMNAR_RELATION_EMPTY_SOURCE_HELD) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_HOLD;
            return 0;
        }
        if (child_result) {
            record->semantic_result = child_result;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END;
        } else {
            record->phase = record->source_empty
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SLOT
                : WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END;
        }
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW:
        if (child_result == ENOSPC)
            record->session->memory_budget_denied = true;
        record->semantic_result = child_result;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END:
        if (child_result) {
            record->protocol_result = child_result;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END;
            return 0;
        }
        if (!wl_columnar_eval_plan_source_drained(record)) {
            record->protocol_result = 0;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END;
            return 0;
        }
        if (!record->semantic_result) record->semantic_result = child_result;
        record->source_initialized = false;
        record->source_bare = false;
        record->phase = record->semantic_result
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL;
        return 0;
    default:
        return EINVAL;
    }
}

int
wl_columnar_eval_plan_numeric_source_attempt_begin(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!record || record->identity || record->phase || !plan || !plan->ops
        || begin >= end || end > plan->op_count || end - begin < 2
        || !frame || !session || !control || !id
        || (session->nrels && !session->rels)) return EINVAL;
    if (session->current_iteration || session->retraction_seeded
        || session->retraction_right_pass || session->tdd_input_run
        || session->tdd_subpass_active || session->tdd_outbound_only_active
        || session->diff_operators_active) return ENOTSUP;
    if (wl_columnar_eval_stack_frame_mutation_blocked(frame)) return EBUSY;
    int rc = wl_columnar_eval_plan_numeric_fund(funding, funding_id, control);
    if (rc) return rc;
    if (wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    record->identity = record; record->frame = frame;
    record->session = session; record->plan = plan;
    record->control = control; record->attempt_id = id;
    record->begin = begin + 1; record->end = end; record->cursor = begin;
    record->completed = 0; record->semantic_result = 0;
    record->protocol_result = 0;
    record->source_mode = true; record->source_installed = false;
    record->source_empty = false; record->source_is_delta = false;
    record->source_initialized = false; record->source_slot_borrowed = false;
    record->source_bare = false;
    record->source_empty_reader.identity = NULL;
    record->source_empty_reader.phase = WL_COLUMNAR_RELATION_EMPTY_SOURCE_INERT;
    record->source_registry = session->rels;
    record->source_count = session->nrels;
    record->source_slot = 0; record->source_byte = 0;
    record->source_begin = begin;
    record->source_full = NULL; record->source_delta = NULL;
    record->source_selected = NULL; record->source_output = NULL;
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_PREFLIGHT;
#if defined(WL_HAVE_C11_THREADS)
    record->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    record->owner_thread = GetCurrentThreadId();
#else
    record->owner_thread = pthread_self();
#endif
    record->thread_valid = true;
    rc = wl_columnar_eval_stack_numeric_source_claim(frame, session, record);
    if (rc) {
        record->control = NULL; record->identity = NULL;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
        record->thread_valid = false; record->frame = NULL;
        record->session = NULL; record->plan = NULL;
        record->source_registry = NULL; record->source_mode = false;
        wl_evaluation_control_release(control);
    }
    return rc;
}

int
wl_columnar_eval_plan_numeric_attempt_step(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_numeric_validate(record);
    if (rc) return rc;
    if (wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame)) return EBUSY;
    wl_columnar_filter_source_end_kind_t source_child =
        WL_COLUMNAR_FILTER_SOURCE_END_NONE;
    if (record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD) {
        rc = wl_columnar_filter_source_end_pending_owned(record->frame, record,
                funding, funding_id, &source_child);
        if (rc) return rc;
    }
    if ((record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END
        && record->source_initialized) ||
        source_child != WL_COLUMNAR_FILTER_SOURCE_END_NONE) {
        rc = wl_columnar_eval_plan_source_end_fund(funding, funding_id,
                record->control);
        if (rc) return rc;
        if (source_child != WL_COLUMNAR_FILTER_SOURCE_END_NONE) {
            record->pending_action = record->phase;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
            return wl_columnar_filter_source_end_step_owned(record->frame,
                       &record->child_status, record, funding, funding_id);
        }
        return wl_columnar_eval_plan_numeric_source_step(record, funding,
                   funding_id);
    }
    rc = wl_columnar_eval_plan_numeric_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    switch (record->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_PREFLIGHT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_FULL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_DELTA:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SELECT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INIT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_HOLD:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SLOT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL:
        return wl_columnar_eval_plan_numeric_source_step(record, funding,
                   funding_id);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_PREFLIGHT:
        rc = wl_columnar_eval_plan_numeric_forward(record);
        if (!rc) {
            wl_plan_op_type_t kind = record->plan->ops[record->cursor].op;
            if (kind != WL_PLAN_OP_FILTER && kind != WL_PLAN_OP_MAP
                && kind != WL_PLAN_OP_JOIN && kind != WL_PLAN_OP_SEMIJOIN
                && kind != WL_PLAN_OP_ANTIJOIN &&
                kind != WL_PLAN_OP_CONSOLIDATE && kind != WL_PLAN_OP_REDUCE)
                rc = ENOTSUP;
        }
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
        } else if (++record->cursor == record->end) {
            record->cursor = record->begin;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_READY;
        }
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_READY:
        rc = wl_columnar_eval_plan_numeric_forward(record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
            return 0;
        }
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        switch (record->plan->ops[record->cursor].op) {
        case WL_PLAN_OP_FILTER:
            return wl_columnar_filter_op_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_MAP:
            return wl_columnar_filter_map_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_JOIN:
            return wl_columnar_join_op_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            return wl_columnar_join_probe_op_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_REDUCE:
            return wl_columnar_ops_reduce_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_CONSOLIDATE:
            return wl_columnar_filter_consolidate_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        default:
            record->semantic_result = ENOTSUP;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
            return 0;
        }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_filter_op_attempt_step_owned(record->frame,
                   &record->child_status, record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_ACK_CHILD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_filter_op_attempt_ack_owned(record->frame, record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_ADVANCE:
        if (record->semantic_result || ++record->cursor == record->end)
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE;
        else
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_READY;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_RELEASE:
        /* Keep the original pin through ACK so even DONE status queries reject
        * funding from that operation's successor without dangling pointers. */
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE;
        return 0;
    default:
        return EBUSY;
    }
}

int
wl_columnar_eval_plan_numeric_consume_result(
    wl_columnar_eval_plan_numeric_attempt_t *record, int child_protocol,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_numeric_validate(record);
    if (rc) return rc;
    if (wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame)) return EBUSY;
    if (record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING) return EBUSY;
    rc = wl_columnar_eval_plan_numeric_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    if (record->pending_action >= WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INIT
        && record->pending_action <= WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END)
        return wl_columnar_eval_plan_numeric_source_consume(record,
                   child_protocol);
    record->protocol_result = child_protocol;
    if (child_protocol) {
        record->phase = record->pending_action;
        return 0;
    }
    if (record->pending_action == WL_COLUMNAR_EVAL_PLAN_NUMERIC_ACK_CHILD) {
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_ADVANCE;
        return 0;
    }
    switch (record->child_status.progress) {
    case WL_COLUMNAR_FILTER_PROGRESS_YIELD:
    case WL_COLUMNAR_FILTER_PROGRESS_RETAINED:
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD;
        break;
    case WL_COLUMNAR_FILTER_PROGRESS_DONE:
        record->semantic_result = record->child_status.semantic_result;
        if (!record->semantic_result) ++record->completed;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_ACK_CHILD;
        break;
    default:
        record->protocol_result = EINVAL;
        return EINVAL;
    }
    return 0;
}

int
wl_columnar_eval_plan_numeric_attempt_status(
    const wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_columnar_eval_plan_numeric_status_t *status,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!status) return EINVAL;
    int rc = wl_columnar_eval_plan_numeric_validate(record);
    if (rc) return rc;
    /* Validation proves the frame's numeric owner is this exact record. */
    if (!wl_columnar_eval_stack_protocol_copy_output(record->frame, status,
        sizeof(*status))) return EINVAL;
    rc = wl_columnar_eval_plan_numeric_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    status->phase = record->phase; status->completed = record->completed;
    status->semantic_result = record->semantic_result;
    status->protocol_result = record->protocol_result;
    status->retained = record->protocol_result != 0
        || (record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        && record->child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    return 0;
}

int
wl_columnar_eval_plan_numeric_attempt_ack(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_numeric_validate(record);
    if (rc) return rc;
    if (wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame)) return EBUSY;
    if (record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE
        || record->source_initialized || record->source_bare ||
        !record->control)
        return EBUSY;
    rc = wl_columnar_eval_plan_numeric_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    if (record->source_mode && !record->source_installed)
        rc = wl_columnar_eval_stack_numeric_source_release(record->frame,
                record->session, record);
    else
        rc = wl_columnar_eval_stack_numeric_unbind(record->frame,
                record->session,
                record);
    if (rc) return rc;
    wl_evaluation_control_t *control = record->control;
    record->control = NULL;
    record->identity = NULL; record->frame = NULL; record->session = NULL;
    record->plan = NULL; record->thread_valid = false;
    record->attempt_id = 0; record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
    wl_evaluation_control_release(control);
    return 0;
}

/* Separate genuine multi-entry claim; existing entry contracts are unchanged. */
int
wl_columnar_eval_plan_numeric_stack_attempt_begin(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    uint32_t expected_depth,
    wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!record || record->identity || record->phase || !plan || !plan->ops
        || begin >= end || end > plan->op_count || !frame || !session
        || !control || !id || !expected_depth
        || expected_depth > COL_STACK_MAX) return EINVAL;
    if (wl_columnar_eval_stack_frame_mutation_blocked(frame)) return EBUSY;
    int rc = wl_columnar_eval_plan_numeric_fund(funding, funding_id, control);
    if (rc) return rc;
    if (wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    record->identity = record; record->frame = frame;
    record->session = session; record->plan = plan;
    record->control = control; record->attempt_id = id;
    record->begin = begin; record->end = end; record->cursor = begin;
    record->source_mode = false;
    record->completed = 0; record->semantic_result = 0;
    record->protocol_result = 0;
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PREFLIGHT;
#if defined(WL_HAVE_C11_THREADS)
    record->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    record->owner_thread = GetCurrentThreadId();
#else
    record->owner_thread = pthread_self();
#endif
    record->thread_valid = true;
    rc = wl_columnar_eval_stack_numeric_stack_claim(frame, session, record,
            expected_depth, control, id);
    if (rc) {
        record->control = NULL; record->identity = NULL;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
        record->thread_valid = false; record->frame = NULL;
        record->session = NULL; record->plan = NULL;
        wl_evaluation_control_release(control);
    }
    return rc;
}

/* Full initial scalar composition is a sibling protocol. Distinct phases
 * prevent legacy consume/ACK from interpreting a later source push as the
 * old initial-empty-source transaction. Each child returns to the real host;
 * its result is consumed on a different, independently funded action. */
static int
wl_columnar_eval_plan_composed_charge(wl_evaluation_control_t *control,
    uint64_t id, uint64_t work)
{
    if (!control || !id ||
        wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(control, id, work);
    if (rc) return rc;
    return wl_evaluation_control_active_attempt_id(control) == id ? 0 : EINVAL;
}

/* The fresh funding generation belongs exclusively to this host action.
 * It cannot finish/reset/begin/transfer during the action. Original operation
 * identity remains mutable and retains every saved-ID check below. */
static int
wl_columnar_eval_plan_composed_fund(wl_evaluation_control_t *funding,
    uint64_t id, const wl_evaluation_control_t *operation)
{
    if (!funding || funding == operation || !id
        || wl_evaluation_control_active_attempt_id(funding) != id)
        return EINVAL;
    return wl_evaluation_control_charge_attempt(funding, id,
               WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK);
}

/* A static entry/pair scan has its own saved host funding cuts. Preserve
 * legacy numeric validation and never poll a sticky Original for cleanup. */
#ifdef WL_SESSION_TEST_HOOKS
/* Borrowed funding instrumentation may request a stop; it acquires no engine
 * context or mutable relation/receipt authority. */
void (*wl_columnar_eval_plan_test_static_validation_checkpoint)(
    wl_evaluation_control_t *, uint64_t, uint32_t, uint32_t);
#endif

static int
wl_columnar_eval_plan_composed_static_poll(wl_evaluation_control_t *funding,
    uint64_t id, uint32_t entry, uint32_t pair)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_plan_test_static_validation_checkpoint)
        wl_columnar_eval_plan_test_static_validation_checkpoint(funding, id,
            entry, pair);
#else
    (void)entry; (void)pair;
#endif
    return wl_evaluation_control_charge_attempt(funding, id, 0);
}

static int
wl_columnar_eval_plan_composed_validate_funded(
    const wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!record || !wl_columnar_eval_plan_numeric_owner_valid(record,
        record->frame)) return EINVAL;
    if (record->session->cleanup_active != record->frame) return EBUSY;
    if (record->private_bindings) {
        const wl_columnar_eval_private_bindings_t *bindings =
            record->private_bindings;
        if (bindings->identity != (uintptr_t)bindings
            || bindings->session != record->session
            || bindings->plan != record->plan
            || bindings->control != record->control
            || bindings->attempt_id != record->attempt_id
            || !bindings->count
            || (!bindings->dynamic_entries
            && bindings->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX)
            || wl_evaluation_control_active_attempt_id(record->control)
            != record->attempt_id)
            return EINVAL;
        if (bindings->dynamic_entries) {
            if (!wl_columnar_session_observed_bindings_authorized(
                    record->session, bindings)) return EINVAL;
        }
        if (!bindings->dynamic_entries
            && (!funding || funding == record->control ||
            !funding_id)) return EINVAL;
        for (uint32_t i = 0; !bindings->dynamic_entries
            && i < bindings->count; ++i) {
            int rc = wl_columnar_eval_plan_composed_static_poll(funding,
                    funding_id, i, UINT32_MAX);
            if (rc) return rc;
            const wl_columnar_eval_private_binding_t *entry =
                &bindings->entries[i];
            if (!wl_columnar_eval_plan_private_entry_valid(
                    entry)) return EINVAL;
            for (uint32_t j = 0; j < i; ++j) {
                rc = wl_columnar_eval_plan_composed_static_poll(funding,
                        funding_id, i, j);
                if (rc) return rc;
                if (!wl_columnar_eval_plan_private_pair_distinct(
                        &bindings->entries[j], entry)) return EINVAL;
            }
        }
    }
    if (record->private_bindings &&
        !record->private_bindings->dynamic_entries) {
        int rc = wl_columnar_eval_plan_composed_static_poll(funding, funding_id,
                UINT32_MAX, UINT32_MAX);
        if (rc) return rc;
    }
    return wl_columnar_eval_stack_numeric_owner(record->frame, record);
}

static int
wl_columnar_eval_plan_composed_validate(
    const wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (record && record->lftj_metadata_ended) return EBUSY;
    int rc = wl_columnar_eval_plan_composed_validate_funded(record, funding,
            funding_id);
    if (rc) return rc;
    if (!record->composed_mode || record->runtime_depth > COL_STACK_MAX
        || record->preflight_depth > COL_STACK_MAX
        || record->phase < WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_INPUTS
        || record->phase > WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
        return EINVAL;
    return 0;
}

static int
wl_columnar_eval_plan_numeric_composed_attempt_begin_internal(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    uint32_t initial_depth, wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id,
    bool controlled_materialized_batch_fallback,
    const wl_columnar_eval_private_bindings_t *private_bindings)
{
    if (!record || record->identity || record->phase || !plan || !plan->ops
        || begin >= end || end > plan->op_count || !frame || !session
        || !control || !id || initial_depth > COL_STACK_MAX
        || (session->nrels && !session->rels)) return EINVAL;
    if (private_bindings && (private_bindings->identity
        != (uintptr_t)private_bindings || private_bindings->session != session
        || private_bindings->plan != plan
        || private_bindings->control != control
        || private_bindings->attempt_id != id
        || !private_bindings->count
        || (!private_bindings->dynamic_entries
        && private_bindings->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX)))
        return EINVAL;
    if (private_bindings
        && session->nrels > UINT32_MAX - private_bindings->count)
        return EOVERFLOW;
    if (private_bindings && private_bindings->dynamic_entries
        && !wl_columnar_session_observed_bindings_authorized(session,
        private_bindings)) return EINVAL;
    if (private_bindings && !private_bindings->dynamic_entries) {
        for (uint32_t i = 0; i < private_bindings->count; ++i) {
            const wl_columnar_eval_private_binding_t *entry =
                &private_bindings->entries[i];
            if (!wl_columnar_eval_plan_private_entry_valid(
                    entry)) return EINVAL;
        }
    }
    if (controlled_materialized_batch_fallback) {
        if (!session->join_batch_bytes
            || begin != 0 || end != plan->op_count || initial_depth != 0
            )
            return ENOTSUP;
    }
    if (session->current_iteration || session->retraction_seeded
        || session->retraction_right_pass || session->tdd_input_run
        || session->tdd_subpass_active || session->tdd_outbound_only_active
        || session->diff_operators_active
        || (session->join_batch_bytes
        && !controlled_materialized_batch_fallback)
        || session->join_output_shared_count || session->coordinator
        || session->num_workers > 1) return ENOTSUP;
    if (wl_columnar_eval_stack_frame_mutation_blocked(frame)) return EBUSY;
    int rc = wl_columnar_eval_plan_composed_fund(funding, funding_id, control);
    if (rc) return rc;
    /* Two independently named first256 capacities: original pin retirement
     * and composed record reset/ACK. No parent ownership on a partial prefix. */
    for (unsigned i = 0; i < 8; ++i) {
        rc = wl_columnar_eval_plan_composed_charge(control, id, 192);
        if (rc) return rc;
    }
    rc = wl_columnar_eval_plan_composed_charge(control, id, 256);
    if (rc) return rc;
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    record->identity = record; record->frame = frame;
    record->session = session; record->plan = plan;
    record->control = control; record->attempt_id = id;
    record->private_bindings = private_bindings;
    record->begin = begin; record->end = end; record->cursor = begin;
    record->completed = 0; record->semantic_result = 0;
    record->protocol_result = 0;
    record->source_mode = true; record->source_installed = false;
    record->abort_supported = true; record->abort_requested = false;
    record->composed_busy = record->lftj_cleanup_authorized = false;
    memset(&record->lftj_metadata, 0, sizeof(record->lftj_metadata));
    record->lftj_metadata_ended = record->lftj_metadata_dispatch = false;
    record->source_empty = false; record->source_is_delta = false;
    record->source_initialized = false; record->source_slot_borrowed = false;
    record->source_bare = false;
    record->source_empty_reader.identity = NULL;
    record->source_empty_reader.phase = WL_COLUMNAR_RELATION_EMPTY_SOURCE_INERT;
    record->source_registry = session->rels;
    record->source_count = private_bindings ? private_bindings->count
        : session->nrels;
    if (private_bindings) record->source_count += session->nrels;
    record->source_slot = 0; record->source_byte = 0;
    record->source_begin = begin;
    record->source_full = NULL; record->source_delta = NULL;
    record->source_selected = NULL; record->source_output = NULL;
    record->composed_mode = true;
    record->controlled_materialized_batch_fallback =
        controlled_materialized_batch_fallback;
    record->join_host = NULL; record->join_host_initialized_bytes = 0;
    record->join_host_tail_count = 0;
    record->join_host_governor = NULL;
    record->join_host_admitted = record->join_host_committed = false;
    record->join_host_terminal = record->join_host_declined = false;
    record->join_host_busy = false;
    wl_columnar_memory_reservation_init(&record->join_host_credit);
    record->preflight_depth = initial_depth;
    record->runtime_depth = initial_depth;
    record->phase = initial_depth ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_INPUTS
        : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT;
#if defined(WL_HAVE_C11_THREADS)
    record->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    record->owner_thread = GetCurrentThreadId();
#else
    record->owner_thread = pthread_self();
#endif
    record->thread_valid = true;
    rc = initial_depth
        ? wl_columnar_eval_stack_numeric_stack_claim(frame, session, record,
            initial_depth, control, id)
        : wl_columnar_eval_stack_composed_source_claim(frame, session, record);
    if (rc) {
        record->control = NULL; record->identity = NULL;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
        record->thread_valid = false; record->frame = NULL;
        record->session = NULL; record->plan = NULL;
        record->source_registry = NULL; record->source_mode = false;
        record->private_bindings = NULL;
        record->composed_mode = false;
        record->controlled_materialized_batch_fallback = false;
        record->preflight_depth = 0; record->runtime_depth = 0;
        wl_evaluation_control_release(control);
    }
    return rc;
}

int
wl_columnar_eval_plan_numeric_composed_attempt_begin(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    uint32_t initial_depth, wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    return wl_columnar_eval_plan_numeric_composed_attempt_begin_internal(
        record, plan, begin, end, frame, session, initial_depth, control, id,
        funding, funding_id, false, NULL);
}

int
wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    uint32_t initial_depth, wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id,
    const wl_columnar_eval_private_bindings_t *private_bindings)
{
    if (!private_bindings) return EINVAL;
    return wl_columnar_eval_plan_numeric_composed_attempt_begin_internal(
        record, plan, begin, end, frame, session, initial_depth, control, id,
        funding, funding_id, false, private_bindings);
}

int
wl_columnar_eval_plan_numeric_composed_attempt_begin_bound_batch(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    uint32_t initial_depth, wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id,
    const wl_columnar_eval_private_bindings_t *private_bindings)
{
    if (!private_bindings || !private_bindings->dynamic_entries) return EINVAL;
    return wl_columnar_eval_plan_numeric_composed_attempt_begin_internal(
        record, plan, begin, end, frame, session, initial_depth, control, id,
        funding, funding_id, true, private_bindings);
}

int
wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    uint32_t initial_depth, wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    return wl_columnar_eval_plan_numeric_composed_attempt_begin_internal(
        record, plan, begin, end, frame, session, initial_depth, control, id,
        funding, funding_id, true, NULL);
}

static void
wl_columnar_eval_plan_composed_source_lookup(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    bool delta = record->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA;
    if (record->source_slot == record->source_count) {
        if (!delta) {
            record->semantic_result = ENOENT;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        } else {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT;
        }
        return;
    }
    const char *actual_name = NULL;
    col_rel_t *candidate = wl_columnar_eval_plan_numeric_source_candidate(
        record, record->source_slot, &actual_name);
    if (record->semantic_result) return;
    if (!actual_name) {
        ++record->source_slot;
        record->source_byte = 0;
        return;
    }
    const char *name = record->plan->ops[record->source_begin].relation_name;
    size_t byte = record->source_byte;
    char wanted;
    if (!delta) wanted = name[byte];
    else if (byte < 3) wanted = "$d$"[byte];
    else if (byte == 255) wanted = '\0';
    else wanted = name[byte - 3];
    char actual = actual_name[byte];
    if (actual != wanted) {
        ++record->source_slot;
        record->source_byte = 0;
    } else if (actual) {
        ++record->source_byte;
    } else if (!candidate) {
        record->semantic_result = ENOENT;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
    } else if (delta) {
        record->source_delta = candidate;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT;
    } else {
        record->source_full = candidate;
        record->source_slot = 0; record->source_byte = 0;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA;
    }
}

static void
wl_columnar_eval_plan_composed_source_select(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    const wl_plan_op_t *op = &record->plan->ops[record->source_begin];
    col_rel_t *full = record->source_full, *delta = record->source_delta;
    record->source_selected = full;
    if (op->delta_mode == WL_DELTA_FORCE_EMPTY) {
        record->source_empty = true;
    } else if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows) {
            record->source_selected = delta;
            record->source_is_delta = true;
        } else if (record->session->delta_seeded) {
            record->source_empty = true;
            record->source_is_delta = true;
        }
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && delta
        && delta->nrows && delta->nrows < full->nrows) {
        /* Includes FORCE_EMPTY_AFTER_SEED and unknown modes at iteration0,
         * matching the raw VARIABLE fallback to AUTO. */
        record->source_selected = delta;
        record->source_is_delta = true;
    }
    col_rel_t *append_delta = NULL;
    int rc = wl_columnar_session_delta_source(record->session,
            record->private_bindings, op, &append_delta);
    if (rc) {
        record->semantic_result = rc;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        return;
    }
    if (append_delta) {
        record->source_selected = append_delta;
        record->source_is_delta = true;
        record->source_empty = append_delta->nrows == 0;
    }
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT;
}

static int
wl_columnar_eval_plan_composed_source_step(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc;
    switch (record->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL:
        rc = record->phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL
            ? wl_columnar_eval_plan_composed_charge(record->control,
                record->attempt_id, 256)
            : wl_columnar_eval_plan_numeric_forward(record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = record->source_initialized
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
                : record->source_slot_borrowed
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
                : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            return 0;
        }
        break;
    default:
        break;
    }
    switch (record->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL:
        if (!record->plan->ops[record->source_begin].relation_name) {
            record->semantic_result = ENOENT;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            return 0;
        }
    /* fall through */
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA:
        wl_columnar_eval_plan_composed_source_lookup(record);
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT:
        wl_columnar_eval_plan_composed_source_select(record);
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_eval_plan_source_init(record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_eval_plan_source_hold(record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SLOT:
        rc = wl_columnar_eval_stack_composed_source_output(record->frame,
                record->session, record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
        } else record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_eval_plan_source_new(record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_eval_plan_source_end(record, funding, funding_id);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL:
        rc = wl_columnar_eval_stack_composed_source_install(record->frame,
                record->session, record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = record->source_slot_borrowed
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
                : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            return 0;
        }
        ++record->runtime_depth; ++record->completed;
        record->phase = ++record->cursor == record->end
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT:
        rc = wl_columnar_eval_stack_composed_source_abort(record->frame,
                record->session, record, funding, funding_id);
        record->protocol_result = rc;
        if (!rc) record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        return 0;
    default:
        return EBUSY;
    }
}

static int
wl_columnar_eval_plan_composed_source_consume(
    wl_columnar_eval_plan_numeric_attempt_t *record, int child_result)
{
    record->protocol_result = 0;
    switch (record->pending_action) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT:
        if (child_result) {
            record->semantic_result = child_result;
            record->phase = record->source_initialized
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
                : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        } else record->phase =
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD:
        if (!child_result && record->source_bare
            && record->source_empty_reader.phase !=
            WL_COLUMNAR_RELATION_EMPTY_SOURCE_HELD) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD;
            return 0;
        }
        if (child_result) {
            record->semantic_result = child_result;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
        } else record->phase = record->source_empty
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SLOT
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW:
        if (child_result ==
            ENOSPC) record->session->memory_budget_denied = true;
        record->semantic_result = child_result;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END:
        if (child_result) {
            record->protocol_result = child_result;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
            return 0;
        }
        if (!wl_columnar_eval_plan_source_drained(record)) {
            record->protocol_result = 0;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
            return 0;
        }
        if (!record->semantic_result) record->semantic_result = child_result;
        record->source_initialized = false;
        record->source_bare = false;
        record->phase = record->semantic_result
            ? record->source_slot_borrowed
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL;
        return 0;
    default:
        return EINVAL;
    }
}

/* Authentic failed-output continuation only. Success RELEASE and ordinary
 * input/scratch cleanup retain their old384 wrapper. The128 wrapper includes
 * native/context/classifier<=64, stop/funding/dispatch<=40 and caller20; leaf
 * ZERO/clear256 then fits actual384. No heap relation is dereferenced here. */
static int
wl_columnar_eval_plan_lftj_retirement_fund(wl_evaluation_control_t *funding,
    uint64_t id, const wl_evaluation_control_t *control)
{
    if (!funding || !id || wl_evaluation_control_identity(funding) ==
        wl_evaluation_control_identity(control)
        || wl_evaluation_control_active_attempt_id(funding) !=
        id) return EINVAL;
    return wl_evaluation_control_charge_attempt(funding, id, 128);
}

static int
wl_columnar_eval_plan_lftj_retirement_class(
    const wl_columnar_eval_plan_numeric_attempt_t *r, bool *retiring)
{
    *retiring = false;
    if ((r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
        && r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE)
        || !r->lftj.identity) return 0;
    const wl_columnar_lftj_attempt_t *a = &r->lftj;
    if (a->identity != a || a->numeric_owner != r || a->frame != r->frame
        || a->session != r->session || a->control != r->control
        || a->attempt_id != r->attempt_id || !a->thread_valid) return EINVAL;
    if (a->busy) return EBUSY;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(a->owner_thread, thrd_current())) return EINVAL;
#elif defined(_WIN32) || defined(_WIN64)
    if (a->owner_thread != GetCurrentThreadId()) return EINVAL;
#else
    if (!pthread_equal(a->owner_thread, pthread_self())) return EINVAL;
#endif
    const wl_columnar_relation_scalar_retirement_initializer_t *i =
        &a->retirement_initializer;
    if (i->identity && (i->identity != i || i->parent != a
        || i->retirement != &a->retirement ||
        i->relation != a->output)) return EINVAL;
    if (a->retirement_started &&
        (a->retirement.storage.identity != (uintptr_t)&a->retirement.storage
        || a->retirement.committed != a->retirement_committed)) return EINVAL;
    if (a->retirement_drained && !a->retirement_started) return EINVAL;
    if (a->output_owned && !a->output) return EINVAL;
    bool failed = a->semantic_result || r->semantic_result ||
        r->abort_requested;
    *retiring = failed && (a->output_owned || a->retirement_started
        || a->retirement_drained || i->identity);
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_plan_test_lftj_metadata_checkpoint)(
    wl_columnar_eval_plan_numeric_attempt_t *);
#endif

/* Fixed real-parent proof only. The existing outer composed validation is
 * its own HOST/protocol region; never recursively call it in these actions. */
static int wl_columnar_eval_plan_native_lftj_owner(
    const wl_columnar_eval_plan_numeric_attempt_t *);

static int
wl_columnar_eval_plan_lftj_metadata_parent(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    bool bound_header)
{
    if (!r || !r->session || !r->frame
        || !r->composed_busy || !r->composed_mode || !r->control
        || r->session->cleanup_active != r->frame
        || wl_evaluation_control_active_attempt_id(r->control) != r->attempt_id)
        return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_owner(r->frame, r);
    if (rc) return rc;
    wl_columnar_lftj_metadata_proof_t *p = &r->lftj_metadata;
    if (!r->plan || p->op_index < r->begin || p->op_index >= r->end
        || r->cursor != p->op_index
        || p->operation != &r->plan->ops[p->op_index]
        || p->operation->op != WL_PLAN_OP_LFTJ) return EINVAL;
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_HEADER) return 0;
    /* HEADER already proved the shape. Exact immutable identity/scalars
     * preserve that proof, including range and nonNULL array checks. */
    if (!p->operation->opaque_data) return EINVAL;
    if (r->session->tdd_input_run || r->session->diff_operators_active
        || r->session->current_iteration) return ENOTSUP;
    if (p->metadata != p->operation->opaque_data
        || p->k != p->metadata->k || p->names != p->metadata->rel_names
        || p->keys != p->metadata->key_cols ||
        p->name_index > p->k) return EINVAL;
    if (!bound_header) return 0;
    if (p->bindings != r->private_bindings) return EINVAL;
    if (p->bindings && (p->bindings->identity != (uintptr_t)p->bindings
        || p->bindings->session != r->session || p->bindings->plan != r->plan
        || p->bindings->control != r->control ||
        p->bindings->attempt_id != r->attempt_id
        || p->binding_count != p->bindings->count
        || p->dynamic_entries != p->bindings->dynamic_entries
        || p->binding_generation != p->bindings->generation)) return EINVAL;
    return 0;
}

static void
wl_columnar_eval_plan_lftj_metadata_checkpoint(
    wl_columnar_eval_plan_numeric_attempt_t *r)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_plan_test_lftj_metadata_checkpoint)
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint(r);
#else
    (void)r;
#endif
}

static int
wl_columnar_eval_plan_lftj_metadata_action(
    wl_columnar_eval_plan_numeric_attempt_t *r)
{
    wl_columnar_lftj_metadata_proof_t *p = &r->lftj_metadata;
    wl_columnar_eval_plan_lftj_metadata_checkpoint(r);
    int rc = wl_columnar_eval_plan_lftj_metadata_parent(r, false);
    if (rc) return rc;
    rc = wl_columnar_eval_plan_composed_charge(r->control, r->attempt_id, 128);
    if (rc) return rc;
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_HEADER) {
        /* Match raw minimum-header/policy order at the actual operator.
         * Reusable controlled preflight remains pure and separately bounded. */
        const wl_plan_op_lftj_t *metadata = p->operation->opaque_data;
        if (!metadata || metadata->k < 2 || !metadata->rel_names
            || !metadata->key_cols) return EINVAL;
        if (r->session->join_batch_bytes) {
            if (r->session->join_batch_strict) return ENOTSUP;
            col_join_batch_record_fallback(r->session,
                COL_JOIN_BATCH_EXCLUDED_LFTJ);
        }
        /* Any refusal returns through RELEASE, so a recorded policy cannot
        * replay. First-warning logging belongs to the preceding packet. */
        rc = wl_columnar_eval_plan_composed_charge(r->control, r->attempt_id,
                0);
        if (rc) return rc;
        rc = wl_columnar_lftj_metadata_header(p->operation, r->session);
        if (rc) return rc;
        p->metadata = p->operation->opaque_data;
        p->k = p->metadata->k; p->names = p->metadata->rel_names;
        p->keys = p->metadata->key_cols; p->name_index = 0;
        p->phase = WL_COLUMNAR_LFTJ_METADATA_NAMES;
        return 0;
    }
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_NAMES) {
        if (p->name_index >= p->k) return EINVAL;
        /* Resolve each value, including NULL, in actual source order. An
         * earlier source key/size error must precede a later missing name. */
        if (++p->name_index ==
            p->k) p->phase = WL_COLUMNAR_LFTJ_METADATA_VALIDATED;
        return 0;
    }
    if (p->phase != WL_COLUMNAR_LFTJ_METADATA_VALIDATED
        || p->name_index != p->k || r->cursor != p->op_index) return EINVAL;
    p->phase = WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER;
    r->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN;
    return 0;
}

static int
wl_columnar_eval_plan_lftj_binding_action(
    wl_columnar_eval_plan_numeric_attempt_t *r)
{
    wl_columnar_lftj_metadata_proof_t *p = &r->lftj_metadata;
    wl_columnar_eval_plan_lftj_metadata_checkpoint(r);
    bool header = p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER;
    int rc = wl_columnar_eval_plan_lftj_metadata_parent(r, !header);
    if (rc) return rc;
    if (p->name_index != p->k || r->cursor != p->op_index) return EINVAL;
    uint64_t work = p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR ? 128 : 192;
    rc = wl_columnar_eval_plan_composed_charge(r->control, r->attempt_id, work);
    if (rc) return rc;
    switch (p->phase) {
    case WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER: {
        const wl_columnar_eval_private_bindings_t *b = r->private_bindings;
        if (b && (b->identity != (uintptr_t)b || b->session != r->session
            || b->plan != r->plan || b->control != r->control
            || b->attempt_id != r->attempt_id || !b->count
            || (!b->dynamic_entries &&
            b->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX)))
            return EINVAL;
        p->bindings = b; p->binding_count = b ? b->count : 0;
        p->dynamic_entries = b ? b->dynamic_entries : NULL;
        p->binding_generation = b ? b->generation : 0;
        p->entry = p->pair = 0;
        p->phase = !b ? WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL
            : b->dynamic_entries ? WL_COLUMNAR_LFTJ_METADATA_BIND_DYNAMIC
            : WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY;
        return 0;
    }
    case WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY: {
        if (!p->bindings || p->dynamic_entries || p->entry >= p->binding_count
            || p->entry >= WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX) return EINVAL;
        const wl_columnar_eval_private_binding_t *e =
            &p->bindings->entries[p->entry];
        if (!wl_columnar_eval_plan_private_entry_valid(e)) return EINVAL;
        p->pair = 0;
        if (!p->entry) {
            if (++p->entry ==
                p->binding_count) p->phase =
                    WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL;
        } else p->phase = WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR;
        return 0;
    }
    case WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR:
        if (!p->bindings || p->dynamic_entries || p->entry >= p->binding_count
            || p->entry >= WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX ||
            p->pair >= p->entry)
            return EINVAL;
        if (!wl_columnar_eval_plan_private_pair_distinct(
                &p->bindings->entries[p->entry],
                &p->bindings->entries[p->pair])) return EINVAL;
        if (++p->pair == p->entry) {
            ++p->entry;
            p->phase = p->entry ==
                p->binding_count ? WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL
                : WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY;
        }
        return 0;
    case WL_COLUMNAR_LFTJ_METADATA_BIND_DYNAMIC:
        if (!p->bindings || !p->dynamic_entries
            || !wl_columnar_session_observed_bindings_authorized(r->session,
            p->bindings)) return EINVAL;
        p->entry = p->binding_count;
        p->phase = WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL;
        return 0;
    case WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL:
        if (p->entry != p->binding_count) return EINVAL;
        if (p->bindings &&
            r->session->nrels > UINT32_MAX - p->binding_count) return EOVERFLOW;
        p->registry = r->session->rels; p->registry_count = r->session->nrels;
        p->phase = WL_COLUMNAR_LFTJ_METADATA_PREFIX;
        return 0;
    default: return EINVAL;
    }
}

int
wl_columnar_eval_plan_numeric_lftj_begin_context_get(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_columnar_lftj_attempt_t *target,
    wl_columnar_lftj_begin_context_t *out)
{
    if (!out || !r || target != &r->lftj
        || r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN
        || (r->lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_PREFIX
        && r->lftj_metadata.phase !=
        WL_COLUMNAR_LFTJ_METADATA_INSTALL)) return EINVAL;
    if (!r->lftj_metadata_dispatch) return EBUSY;
    int rc = wl_columnar_eval_plan_lftj_metadata_parent(r, true);
    if (rc) return rc;
    const wl_columnar_lftj_metadata_proof_t *p = &r->lftj_metadata;
    if (p->name_index != p->k || p->entry != p->binding_count
        || r->cursor != p->op_index || p->registry != r->session->rels
        || p->registry_count != r->session->nrels) return EINVAL;
    out->frame = r->frame; out->session = r->session; out->control = r->control;
    out->attempt_id = r->attempt_id; out->metadata = p->metadata;
    out->bindings = p->bindings; out->registry = p->registry;
    out->registry_count = p->registry_count; out->phase = p->phase;
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint)(
    wl_columnar_eval_plan_numeric_attempt_t *, wl_evaluation_control_t *);
#endif

static bool
wl_columnar_eval_plan_metadata_result_disjoint(
    const wl_columnar_eval_plan_numeric_attempt_t *r, const int *out)
{
    if (!r || !out) return false;
    uintptr_t base = (uintptr_t)r, dest = (uintptr_t)out;
    return base <= UINTPTR_MAX - sizeof(*r) &&
           dest <= UINTPTR_MAX - sizeof(*out)
           && !(dest < base + sizeof(*r) && base < dest + sizeof(*out));
}

static int
wl_columnar_eval_plan_metadata_completed_stop(
    const wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_report_t *report)
{
    if (!r->control || wl_evaluation_control_active_attempt_id(r->control)
        || wl_evaluation_control_report(r->control, report)
        || report->attempt_id != r->attempt_id
        || (report->result != WL_EVALUATION_CONTROL_CANCELLED
        && report->result != WL_EVALUATION_CONTROL_WORK_LIMIT)
        || report->stop_reason != report->result
        || (r->semantic_result &&
        r->semantic_result != report->result)) return EINVAL;
    return 0;
}

/* Only the genuine empty prechild owner can abandon this metadata. Do not
 * inspect private binding storage or historic retirement/initializer fields. */
static int
wl_columnar_eval_plan_metadata_stopped_parent(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    bool terminal, wl_evaluation_control_report_t *report,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!r || !r->session || !r->frame || r->lftj_metadata_dispatch
        || !r->composed_mode || r->session->cleanup_active != r->frame
        || r->runtime_depth || r->source_installed || r->source_initialized
        || r->source_bare || r->source_slot_borrowed || r->source_output
        || r->join_host || r->join_host_admitted || r->join_host_governor
        || r->lftj.identity || r->lftj.inputs || r->lftj.output
        || r->lftj.row || r->lftj.output_types || r->lftj.scratch_reserved
        || r->lftj.scratch_committed || r->lftj.writer_active
        || r->lftj.output_owned || r->lftj.registered) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_owner(r->frame, r);
    if (rc) return rc;
    if (terminal) {
        if (!r->lftj_metadata_ended ||
            r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
            || r->lftj_metadata.phase !=
            WL_COLUMNAR_LFTJ_METADATA_INERT) return EINVAL;
    } else {
        const wl_columnar_lftj_metadata_proof_t *p = &r->lftj_metadata;
        if (r->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
            && p->phase == WL_COLUMNAR_LFTJ_METADATA_INERT) {
            /* First READY has not captured metadata. Keep the same empty
             * stopped-owner contract without dereferencing opaque storage. */
            if (r->lftj_metadata_ended || !funding || funding == r->control
                || !funding_id) return EINVAL;
            rc = wl_evaluation_control_charge_attempt(funding, funding_id, 0);
            if (rc) return rc;
            if (!r->plan || !r->plan->ops || r->begin >= r->end
                || r->end > r->plan->op_count || r->cursor != r->begin
                || r->completed || r->plan->ops[r->cursor].op != WL_PLAN_OP_LFTJ
                || p->operation || p->metadata || p->names || p->keys
                || p->op_index || p->k || p->name_index || p->bindings
                || p->dynamic_entries || p->binding_generation
                || p->binding_count || p->entry || p->pair || p->registry
                || p->registry_count || p->prefix_cleared) return EINVAL;
        } else if (r->lftj_metadata_ended
            || (r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT_LFTJ
            && r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT
            && r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
            && r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN
            && r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE)
            || p->phase < WL_COLUMNAR_LFTJ_METADATA_HEADER
            || p->phase > WL_COLUMNAR_LFTJ_METADATA_INSTALL
            || !r->plan || p->op_index < r->begin || p->op_index >= r->end
            || p->operation != &r->plan->ops[p->op_index]) return EINVAL;
    }
    return wl_columnar_eval_plan_metadata_completed_stop(r, report);
}

static int
wl_columnar_eval_plan_metadata_funding(
    const wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return !funding || funding == r->control || !id
           || wl_evaluation_control_identity(funding) ==
           wl_evaluation_control_identity(r->control)
           || wl_evaluation_control_active_attempt_id(funding) !=
           id ? EINVAL : 0;
}

int
wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding,
    uint64_t funding_id, int *result)
{
    if (r &&
        wl_columnar_eval_stack_frame_mutation_blocked(r->frame)) return EBUSY;
    if (!r || r->composed_busy) return r ? EBUSY : EINVAL;
    if (!wl_columnar_eval_plan_metadata_result_disjoint(r,
        result)) return EINVAL;
    wl_evaluation_control_report_t report;
    int rc = wl_columnar_eval_plan_metadata_stopped_parent(r, false, &report,
            funding, funding_id);
    if (rc) return rc;
    rc = wl_columnar_eval_plan_metadata_funding(r, funding, funding_id);
    if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *frame = r->frame;
    wl_col_session_t *session = r->session;
    wl_evaluation_control_t *control = r->control;
    uint64_t id = r->attempt_id;
    r->composed_busy = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint)
        wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint(r, funding);
#endif
    /* Busy and real prechild phase forbid frame unbind. Recheck the actual
     * stopped original/funding after the last hook, before callback-free work. */
    if (r->identity != r || r->frame != frame || r->session != session
        || session->cleanup_active != frame || r->control != control ||
        r->attempt_id != id)
        rc = EINVAL;
    if (!rc) rc = wl_columnar_eval_plan_metadata_completed_stop(r, &report);
    if (!rc &&
        wl_evaluation_control_active_attempt_id(funding) !=
        funding_id) rc = EINVAL;
    if (!rc) rc = wl_columnar_eval_stack_numeric_metadata_empty(r->frame, r,
                result);
    if (!rc) rc = wl_evaluation_control_charge_attempt(funding, funding_id,
                192);
    if (!rc) {
        memset(&r->lftj_metadata, 0, sizeof(r->lftj_metadata));
        r->semantic_result = report.result;
        r->abort_requested = false;
        r->lftj_metadata_dispatch = false;
        r->lftj_metadata_ended = true;
        r->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
        *result = report.result;
    }
    r->composed_busy = false;
    return rc;
}

int
wl_columnar_eval_plan_numeric_lftj_metadata_ack_ended(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding,
    uint64_t funding_id, int *result)
{
    if (r &&
        wl_columnar_eval_stack_frame_mutation_blocked(r->frame)) return EBUSY;
    if (!r || r->composed_busy) return r ? EBUSY : EINVAL;
    if (!wl_columnar_eval_plan_metadata_result_disjoint(r,
        result)) return EINVAL;
    wl_evaluation_control_report_t report;
    int rc = wl_columnar_eval_plan_metadata_stopped_parent(r, true, &report,
            funding, funding_id);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_metadata_result_disjoint(r->frame,
        result)) return EINVAL;
    rc = wl_columnar_eval_plan_metadata_funding(r, funding, funding_id);
    if (rc) return rc;
    r->composed_busy = true;
    rc = wl_evaluation_control_charge_attempt(funding, funding_id, 384);
    if (!rc) rc = wl_columnar_eval_stack_composed_source_release(r->frame,
                r->session, r);
    if (!rc) {
        wl_evaluation_control_t *control = r->control;
        int saved = report.result;
        r->control = NULL; r->identity = NULL;
        r->frame = NULL; r->session = NULL; r->plan = NULL;
        r->source_registry = NULL; r->source_output = NULL;
        r->source_full = r->source_delta = r->source_selected = NULL;
        r->source_mode = r->source_installed = r->thread_valid = false;
        r->attempt_id = 0; r->composed_mode = false;
        r->preflight_depth = r->runtime_depth = 0;
        r->private_bindings = NULL;
        r->controlled_materialized_batch_fallback = false;
        r->abort_supported = r->abort_requested = r->lftj_cleanup_authorized =
            false;
        r->join_host_terminal = r->join_host_declined = false;
        r->join_host_busy = false;
        r->lftj_metadata_ended = r->lftj_metadata_dispatch = false;
        r->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
        *result = saved;
        wl_evaluation_control_release(control); /* No subsequent control access. */
    }
    r->composed_busy = false;
    return rc;
}

/* Shared advancement follows either complete normal or native cleanup proof;
 * both callers hold composed_busy throughout this callback-free action. */
static int
wl_columnar_eval_plan_composed_unary_advance(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (record->join_host || record->join_host_admitted
        || record->join_host_governor) return EBUSY;
    record->join_host_declined = false;
    record->join_host_terminal = record->join_host_committed = false;
    record->join_host_busy = false;
    record->join_host_initialized_bytes = 0;
    if (record->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ADVANCE
        && record->child_status.committed) --record->runtime_depth;
    uint32_t consumed = 1u + record->join_host_tail_count;
    record->join_host_tail_count = 0;
    if (!record->semantic_result) record->completed += consumed;
    if (!record->semantic_result) record->cursor += consumed - 1u;
    record->phase = record->semantic_result ||
        ++record->cursor == record->end
        ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY;
    return 0;
}

/* The sole caller has completed the full composed attempt proof and set
 * composed_busy immediately before entry, without a callback or yield.
 * This lexical proof is never retained across a return. */
static int
wl_columnar_eval_plan_numeric_composed_attempt_step_validated_body(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = 0;
    if (record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        && (record->join_host || record->join_host_admitted
        || record->join_host_governor)) {
        if (!record->semantic_result) record->semantic_result = EINVAL;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH;
    }
    if (record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH) {
        if (!funding || funding == record->control || !funding_id
            || wl_evaluation_control_active_attempt_id(funding) != funding_id)
            return EINVAL;
        rc = wl_evaluation_control_charge_attempt(funding, funding_id,
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK);
        if (rc) return rc;
        if (!record->join_host || record->join_host_initialized_bytes
            <= wl_columnar_join_pipeline_numeric_host_size()) {
            record->pending_action = record->phase;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
            return wl_columnar_join_pipeline_numeric_setup_step(record,
                       funding, funding_id);
        }
        return wl_columnar_join_pipeline_numeric_step_funded(record, funding,
                   funding_id);
    }
    wl_columnar_filter_source_end_kind_t source_child =
        WL_COLUMNAR_FILTER_SOURCE_END_NONE;
    if (record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) {
        rc = wl_columnar_filter_source_end_pending_owned(record->frame, record,
                funding, funding_id, &source_child);
        if (rc) return rc;
    }
    if ((record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
        && record->source_initialized) ||
        source_child != WL_COLUMNAR_FILTER_SOURCE_END_NONE) {
        rc = wl_columnar_eval_plan_source_end_fund(funding, funding_id,
                record->control);
        if (rc) return rc;
        if (source_child != WL_COLUMNAR_FILTER_SOURCE_END_NONE) {
            record->pending_action = record->phase;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
            return wl_columnar_filter_source_end_step_owned(record->frame,
                       &record->child_status, record, funding, funding_id);
        }
        if (record->abort_requested
            && wl_evaluation_control_active_attempt_id(record->control) ==
            record->attempt_id)
            wl_evaluation_control_request_cancel(record->control);
        return wl_columnar_eval_plan_composed_source_step(record, funding,
                   funding_id);
    }
    bool scalar_cleanup = false;
    rc = wl_columnar_eval_plan_lftj_retirement_class(record, &scalar_cleanup);
    if (rc) return rc;
    rc = scalar_cleanup ? wl_columnar_eval_plan_lftj_retirement_fund(funding,
            funding_id, record->control)
        : wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    bool retained_lftj = record->lftj.identity == &record->lftj
        && (record->lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE
        || record->lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE)
        && record->lftj.semantic_result;
    if (record->abort_requested && !retained_lftj) {
        if (wl_evaluation_control_active_attempt_id(record->control)
            != record->attempt_id) return EINVAL;
        wl_evaluation_control_request_cancel(record->control);
    }
    switch (record->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_INPUTS: {
        rc = wl_columnar_eval_plan_composed_charge(record->control,
                record->attempt_id, 256);
        eval_stack_t *stack = NULL;
        if (!rc) rc = wl_columnar_eval_stack_operator_stack_owned(record->frame,
                    record->session, &stack, record);
        if (!rc && (record->source_slot >= record->runtime_depth
            || record->source_slot >= COL_STACK_MAX)) rc = EINVAL;
        if (!rc) {
            const col_rel_t *rel = stack->items[record->source_slot].rel;
            if (stack->top != record->runtime_depth
                || rel->compound_kind != WIRELOG_COMPOUND_KIND_NONE)
                rc = ENOTSUP;
        }
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        } else if (++record->source_slot == record->runtime_depth)
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT;
        return 0;
    }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT: {
        rc = wl_columnar_eval_plan_numeric_forward(record);
        if (!rc) {
            wl_plan_op_type_t kind = record->plan->ops[record->cursor].op;
            if (record->controlled_materialized_batch_fallback
                && (kind == WL_PLAN_OP_VARIABLE || kind == WL_PLAN_OP_JOIN)
                && !wl_columnar_eval_plan_scalar_selector_supported(
                    &record->plan->ops[record->cursor])) {
                record->semantic_result = ENOTSUP;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
                return 0;
            }
            if (kind != WL_PLAN_OP_VARIABLE && kind != WL_PLAN_OP_FILTER
                && kind != WL_PLAN_OP_MAP
                && !(kind == WL_PLAN_OP_JOIN
                && !record->plan->ops[record->cursor].materialized)
                && kind != WL_PLAN_OP_LFTJ
                && kind != WL_PLAN_OP_CONCAT
                && !(kind == WL_PLAN_OP_CONSOLIDATE
                && record->cursor + 1 == record->end)
                && !(kind == WL_PLAN_OP_EXCHANGE
                && record->cursor + 1 == record->end
                && wl_columnar_eval_plan_composed_terminal_exchange_valid(
                    record)))
                record->abort_supported = false;
            if (kind == WL_PLAN_OP_VARIABLE) {
                if (record->preflight_depth == COL_STACK_MAX) rc = ENOBUFS;
                else ++record->preflight_depth;
            } else if (kind == WL_PLAN_OP_CONCAT) {
                if (record->preflight_depth < 2) rc = EINVAL;
                else --record->preflight_depth;
            } else if (kind == WL_PLAN_OP_LFTJ) {
                if (record->preflight_depth == COL_STACK_MAX) rc = ENOBUFS;
                else ++record->preflight_depth;
            } else if (kind == WL_PLAN_OP_FILTER || kind == WL_PLAN_OP_MAP
                || kind == WL_PLAN_OP_JOIN || kind == WL_PLAN_OP_SEMIJOIN
                || kind == WL_PLAN_OP_ANTIJOIN || kind == WL_PLAN_OP_REDUCE
                || kind == WL_PLAN_OP_CONSOLIDATE) {
                if (!record->preflight_depth) rc = EINVAL;
            } else if (kind == WL_PLAN_OP_EXCHANGE) {
                if (record->cursor + 1 != record->end
                    || !record->preflight_depth
                    || !wl_columnar_eval_plan_composed_terminal_exchange_valid(
                        record))
                    rc = ENOTSUP;
            } else rc = ENOTSUP;
        }
        if (!rc && ++record->cursor == record->end) {
            if (record->preflight_depth != 1) rc = EINVAL;
            else {
                record->cursor = record->begin;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY;
            }
        }
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        }
        return 0;
    }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT_LFTJ:
        rc = wl_columnar_eval_plan_lftj_metadata_action(record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        }
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN:
        if (record->lftj_metadata.phase == WL_COLUMNAR_LFTJ_METADATA_PREFIX
            || record->lftj_metadata.phase ==
            WL_COLUMNAR_LFTJ_METADATA_INSTALL) {
            bool done = false;
            wl_columnar_eval_plan_lftj_metadata_checkpoint(record);
            /* No application hook inside this exact synchronous lineage. */
            record->lftj_metadata_dispatch = true;
            rc = wl_columnar_lftj_attempt_begin_prevalidated(&record->lftj,
                    record, &done);
            record->lftj_metadata_dispatch = false;
            if (!rc &&
                done) record->phase =
                    WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD;
        } else rc = wl_columnar_eval_plan_lftj_binding_action(record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        }
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY:
        rc = wl_columnar_eval_plan_numeric_forward(record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            return 0;
        }
        if (record->plan->ops[record->cursor].op == WL_PLAN_OP_VARIABLE) {
            record->source_begin = record->cursor;
            record->source_slot = 0; record->source_byte = 0;
            record->source_full = NULL; record->source_delta = NULL;
            record->source_selected = NULL; record->source_output = NULL;
            record->source_empty = false; record->source_is_delta = false;
            record->source_initialized = false;
            record->source_slot_borrowed = false;
            record->source_installed = false;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL;
            return 0;
        }
        if (record->plan->ops[record->cursor].op == WL_PLAN_OP_EXCHANGE) {
            /* The exact terminal zero-worker bridge is a charged, fixed-cost
             * no-op. Preflight authenticated it before any child ran. */
            if (record->cursor + 1 != record->end
                || !wl_columnar_eval_plan_composed_terminal_exchange_valid(
                    record)) {
                record->semantic_result = ENOTSUP;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            } else {
                ++record->completed;
                ++record->cursor;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_eval_plan_test_after_terminal_exchange)
                    wl_columnar_eval_plan_test_after_terminal_exchange(
                        record->control, record->attempt_id);
#endif
                rc = wl_columnar_eval_plan_composed_charge(record->control,
                        record->attempt_id, 0);
                if (rc) record->semantic_result = rc;
            }
            return 0;
        }
        if (record->plan->ops[record->cursor].op == WL_PLAN_OP_CONCAT) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_START;
            return 0;
        }
        if (record->plan->ops[record->cursor].op == WL_PLAN_OP_LFTJ) {
            if (record->lftj.identity) {
                rc = record->lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE
                    ? wl_columnar_eval_plan_native_lftj_owner(record) : EBUSY;
                if (rc) {
                    record->semantic_result = rc;
                    record->phase =
                        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
                    return 0;
                }
            }
            /* One current operator owns this proof. Reset meaningful fields,
             * preserving prior stack output, numeric owner and receipt. */
            wl_columnar_lftj_metadata_proof_t *proof = &record->lftj_metadata;
            proof->phase = WL_COLUMNAR_LFTJ_METADATA_HEADER;
            proof->operation = &record->plan->ops[record->cursor];
            proof->metadata = NULL;
            proof->names = NULL; proof->keys = NULL;
            proof->op_index = record->cursor; proof->k = 0;
            proof->name_index = 0; proof->bindings = NULL;
            proof->dynamic_entries = NULL; proof->binding_generation = 0;
            proof->binding_count = 0; proof->entry = 0; proof->pair = 0;
            proof->registry = NULL; proof->registry_count = 0;
            proof->prefix_cleared = false;
            /* Historical enum name: this is now runtime admission only;
             * whole-body PREFLIGHT checks the stack shape without metadata. */
            record->phase =
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT_LFTJ;
            return 0;
        }
        if (record->plan->ops[record->cursor].op == WL_PLAN_OP_JOIN
            && record->controlled_materialized_batch_fallback
            && !record->join_host_declined) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH;
            return 0;
        }
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        switch (record->plan->ops[record->cursor].op) {
        case WL_PLAN_OP_FILTER:
            return wl_columnar_filter_op_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_MAP:
            return wl_columnar_filter_map_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_JOIN:
            return wl_columnar_join_op_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            return wl_columnar_join_probe_op_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_REDUCE:
            return wl_columnar_ops_reduce_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        case WL_PLAN_OP_CONSOLIDATE:
            return wl_columnar_filter_consolidate_attempt_start_owned(
                &record->plan->ops[record->cursor], record->frame,
                record->session, record->control, record->attempt_id,
                &record->child_status, record);
        default:
            record->semantic_result = ENOTSUP;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            return 0;
        }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_filter_op_attempt_step_owned(record->frame,
                   &record->child_status, record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_filter_op_attempt_ack_owned(record->frame, record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD: {
        bool done = false;
        rc = wl_columnar_ops_lftj_attempt_step(&record->lftj, &done,
                funding, funding_id);
        if (rc) {
            record->pending_action = record->phase;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
            return rc;
        } else if (done) {
            if (record->lftj.semantic_result) {
                record->semantic_result = record->lftj.semantic_result;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            } else if (++record->runtime_depth > COL_STACK_MAX) {
                record->semantic_result = ENOBUFS;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            } else {
                ++record->completed;
                record->phase = ++record->cursor == record->end
                    ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
                    : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY;
            }
        }
        return 0;
    }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_START:
        rc = wl_columnar_eval_plan_numeric_forward(record);
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            return 0;
        }
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_merge_concat_attempt_start_owned(
            &record->plan->ops[record->cursor], record->frame, record->session,
            record->control, record->attempt_id, &record->child_status,
            funding, funding_id, record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_merge_concat_attempt_step_owned(record->frame,
                   &record->child_status, funding, funding_id, record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ACK:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_merge_concat_attempt_ack_owned(record->frame,
                   funding, funding_id, record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ADVANCE:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ADVANCE:
        return wl_columnar_eval_plan_composed_unary_advance(record);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SLOT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT:
        return wl_columnar_eval_plan_composed_source_step(record, funding,
                   funding_id);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE:
        if (record->lftj.identity
            && record->lftj.phase != WL_COLUMNAR_LFTJ_ATTEMPT_DONE) {
            bool done = false;
            rc = wl_columnar_ops_lftj_attempt_abort(&record->lftj, &done,
                    funding, funding_id);
            if (rc) {
                record->pending_action = record->phase;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
                return rc;
            }
            if (!done) return 0;
            if (!record->semantic_result && record->lftj.semantic_result)
                record->semantic_result = record->lftj.semantic_result;
        }
        if (record->lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_INERT) {
            memset(&record->lftj_metadata, 0, sizeof(record->lftj_metadata));
            record->lftj_metadata_dispatch = false;
            return 0;
        }
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
        return 0;
    default:
        return EBUSY;
    }
}

static int
wl_columnar_eval_plan_numeric_composed_cleanup_lftj_body(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    if ((record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
        && record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE)
        || !record->lftj.identity || record->lftj.identity != &record->lftj)
        return EBUSY;
    bool scalar_cleanup = false;
    rc = wl_columnar_eval_plan_lftj_retirement_class(record, &scalar_cleanup);
    if (rc) return rc;
    rc = scalar_cleanup ? wl_columnar_eval_plan_lftj_retirement_fund(funding,
            funding_id, record->control)
        : wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;

    /* Cleanup is authorized only by a terminal stop on the original
     * operation. A zero-work probe observes that sticky disposition without
     * spending or resetting its exhausted allowance. */
    bool authorized = record->lftj_cleanup_authorized
        && (record->lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE
        || record->lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE)
        && record->lftj.semantic_result;
    if (!authorized) {
        rc = wl_evaluation_control_charge_attempt(record->control,
                record->attempt_id, 0);
        if (rc != WL_EVALUATION_CONTROL_CANCELLED &&
            rc != WL_EVALUATION_CONTROL_WORK_LIMIT)
            return rc ? rc : EBUSY;
        record->lftj_cleanup_authorized = true;
        if (!record->semantic_result) record->semantic_result = rc;
        if (!record->lftj.semantic_result) record->lftj.semantic_result = rc;
    }

    bool done = false;
    rc = wl_columnar_ops_lftj_attempt_abort(&record->lftj, &done,
            funding, funding_id);
    if (rc) return rc;
    if (done) {
        record->phase = record->phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
    }
    return 0;
}

static int
wl_columnar_eval_plan_composed_cleanup_stop(
    const wl_columnar_eval_plan_numeric_attempt_t *record, int *stop)
{
    int rc = wl_evaluation_control_charge_attempt(record->control,
            record->attempt_id, 0);
    if (rc != WL_EVALUATION_CONTROL_CANCELLED
        && rc != WL_EVALUATION_CONTROL_WORK_LIMIT)
        return rc ? rc : EBUSY;
    *stop = rc;
    return 0;
}

int
wl_columnar_eval_plan_numeric_composed_cleanup_source(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    if (wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame)) return EBUSY;
    if (record->join_host || record->join_host_admitted
        || record->join_host_governor) return EBUSY;
    switch (record->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SLOT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT:
        break;
    /* Pending child results are receipts. Cleanup must not consume or
     * replace the exact result the framed host still has to deliver. */
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING:
    default:
        return EBUSY;
    }

    bool aborting = record->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT;
    if (aborting) {
        rc = wl_columnar_eval_stack_composed_source_abort_validate(
            record->frame, record->session, record);
        if (rc) return rc;
        if (!funding || funding == record->control || !funding_id
            || wl_evaluation_control_active_attempt_id(funding) != funding_id)
            return EINVAL;
    } else if (record->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END) {
        if (!record->source_initialized
            || (record->source_bare ? record->source_empty_reader.identity !=
            &record->source_empty_reader : record->source.identity !=
            &record->source)) return EBUSY;
    }

    int stop = 0;
    if (aborting) {
        /* An ABORT that was reached for an earlier semantic error still
         * needs a sticky control stop before this direct cleanup is allowed. */
        rc = wl_columnar_eval_plan_composed_cleanup_stop(record, &stop);
        if (rc) return rc;
    }

    rc = record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
        ? wl_columnar_eval_plan_source_end_fund(funding, funding_id,
            record->control)
        : wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;

    if (!aborting) {
        rc = wl_columnar_eval_plan_composed_cleanup_stop(record, &stop);
        if (rc) return rc;
    }

    if (!record->semantic_result) record->semantic_result = stop;
    if (aborting) {
        rc = wl_columnar_eval_stack_composed_source_abort_funded(
            record->frame, record->session, record);
        if (rc) return rc;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        return 0;
    }
    if (record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END) {
        /* End is a child protocol of its own. Preserve its receipt and let the
         * caller consume it with separate host funding before routing to an
         * abort or release action. */
        record->pending_action =
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_eval_plan_source_end(record, funding, funding_id);
    }

    /* Lookup/selection, slot creation, and installation are all skipped
     * after the stop. Only an already-owned source or output is drained. */
    record->phase = record->source_initialized
        ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
        : record->source_slot_borrowed
        ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
        : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
    return 0;
}

static int
wl_columnar_eval_plan_composed_consume_body(
    wl_columnar_eval_plan_numeric_attempt_t *record, int child_protocol)
{
    if (record->pending_action >=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT
        && record->pending_action <=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END)
        return wl_columnar_eval_plan_composed_source_consume(record,
                   child_protocol);
    record->protocol_result = child_protocol;
    if (child_protocol) {
        record->phase = record->pending_action;
        return 0;
    }
    if (record->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH) {
        if (record->child_status.progress != WL_COLUMNAR_FILTER_PROGRESS_DONE) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH;
        } else if (record->join_host_declined) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY;
        } else {
            if (!record->semantic_result)
                record->semantic_result = record->child_status.semantic_result;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ADVANCE;
        }
        return 0;
    }
    if (record->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK) {
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ADVANCE;
        return 0;
    }
    if (record->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ACK) {
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ADVANCE;
        return 0;
    }
    bool concat = record->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_START
        || record->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD;
    switch (record->child_status.progress) {
    case WL_COLUMNAR_FILTER_PROGRESS_YIELD:
    case WL_COLUMNAR_FILTER_PROGRESS_RETAINED:
        record->phase =
            concat ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD;
        break;
    case WL_COLUMNAR_FILTER_PROGRESS_DONE:
        if (!record->semantic_result)
            record->semantic_result = record->child_status.semantic_result;
        record->phase =
            concat ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ACK
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK;
        break;
    default:
        record->protocol_result = EINVAL;
        return EINVAL;
    }
    return 0;
}

int
wl_columnar_eval_plan_numeric_composed_consume_result(
    wl_columnar_eval_plan_numeric_attempt_t *record, int child_protocol,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (record && (record->composed_busy
        || wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame))) return EBUSY;
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    if (record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
        return EBUSY;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    return wl_columnar_eval_plan_composed_consume_body(record, child_protocol);
}

int
wl_columnar_eval_plan_numeric_composed_attempt_status(
    const wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_columnar_eval_plan_numeric_status_t *status,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!status) return EINVAL;
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    /* Validation proves the frame's numeric owner is this exact record. */
    if (!wl_columnar_eval_stack_protocol_copy_output(record->frame, status,
        sizeof(*status))) return EINVAL;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    status->phase = record->phase; status->completed = record->completed;
    status->semantic_result = record->semantic_result;
    status->protocol_result = record->protocol_result;
    status->retained = record->protocol_result != 0
        || ((record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        || record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD
        || record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH)
        && record->child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    return 0;
}

int
wl_columnar_eval_plan_numeric_composed_abort_request(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (record && (record->composed_busy
        || wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame))) return EBUSY;
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    /* Closure is finalized by the bounded preflight before any child owner
     * can be started. Unsupported plans remain usable through ADVANCE. */
    if (record->phase < WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY)
        return EBUSY;
    if (!record->abort_supported
        || record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
        return ENOTSUP;
    if (record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
        return EBUSY;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    if (wl_evaluation_control_active_attempt_id(record->control)
        != record->attempt_id) return EINVAL;
    if (!record->semantic_result)
        record->semantic_result = WL_EVALUATION_CONTROL_CANCELLED;
    record->abort_requested = true;
    wl_evaluation_control_request_cancel(record->control);
    return 0;
}

static void
wl_columnar_eval_plan_composed_ack_clear(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    wl_evaluation_control_t *control = record->control;
    record->control = NULL; record->identity = NULL;
    record->frame = NULL; record->session = NULL; record->plan = NULL;
    record->source_registry = NULL; record->source_mode = false;
    record->source_output = NULL; record->source_installed = false;
    record->thread_valid = false; record->attempt_id = 0;
    record->composed_mode = false; record->preflight_depth = 0;
    record->runtime_depth = 0;
    record->controlled_materialized_batch_fallback = false;
    record->join_host_terminal = record->join_host_declined = false;
    record->join_host_busy = false;
    record->private_bindings = NULL;
    record->lftj_metadata_dispatch = false;
    record->abort_supported = false; record->abort_requested = false;
    record->composed_busy = record->lftj_cleanup_authorized = false;
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
    wl_evaluation_control_release(control);
}

static int wl_columnar_eval_plan_native_lftj_owner(
    const wl_columnar_eval_plan_numeric_attempt_t *);

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_plan_test_lftj_ack_checkpoint)(
    wl_columnar_eval_plan_numeric_attempt_t *, wl_evaluation_control_t *,
    uint64_t, size_t);
#endif

static int
wl_columnar_eval_plan_composed_ack_poll(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t id, size_t offset)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_plan_test_lftj_ack_checkpoint)
        wl_columnar_eval_plan_test_lftj_ack_checkpoint(record, funding, id,
            offset);
#else
    (void)record; (void)offset;
#endif
    return wl_evaluation_control_charge_attempt(funding, id, 0);
}

static int
wl_columnar_eval_plan_composed_ack_body(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t id)
{
    record->composed_busy = true;
    int rc = wl_columnar_eval_plan_composed_ack_poll(record, funding, id,
            SIZE_MAX);
    if (!rc) rc =
            wl_columnar_eval_stack_composed_source_release_validate(
            record->frame,
            record->session, record);
    if (!rc && record->lftj.identity) {
        rc = record->lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE
            ? wl_columnar_eval_plan_native_lftj_owner(record) : EBUSY;
    }
    /* NULL identity is a neverborn or already terminal partial-clear lineage.
     * Do not read its potentially uninitialized historic storage. Retain the
     * canonical numeric/frame owner until ALL refusing checks have passed. */
    for (size_t offset = 0; !rc && offset < sizeof(record->lftj);) {
        rc = wl_columnar_eval_plan_composed_ack_poll(record, funding, id,
                offset);
        if (rc) break;
        size_t count = sizeof(record->lftj) - offset;
        if (count > 1024u) count = 1024u;
        memset((unsigned char *)&record->lftj + offset, 0, count);
        offset += count;
    }
    if (!rc) rc = wl_columnar_eval_plan_composed_ack_poll(record, funding, id,
                sizeof(record->lftj));
    if (!rc) rc = wl_columnar_eval_stack_composed_source_release(record->frame,
                record->session, record);
    if (!rc) wl_columnar_eval_plan_composed_ack_clear(record);
    else record->composed_busy = false;
    return rc;
}

int
wl_columnar_eval_plan_numeric_composed_attempt_ack(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (record && (record->composed_busy
        || wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame))) return EBUSY;
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    if (record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        || record->source_initialized || record->source_bare
        || !record->control || record->join_host || record->join_host_admitted
        || record->join_host_governor) return EBUSY;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    return wl_columnar_eval_plan_composed_ack_body(record, funding, funding_id);
}

/* Canonical read-only child proof. Registered native cleanup authenticates
 * the retained frame/key; actual READY authenticates the live composed owner
 * before reusing resource-free DONE storage. Financing grants no authority. */
static int
wl_columnar_eval_plan_native_lftj_owner(
    const wl_columnar_eval_plan_numeric_attempt_t *n)
{
    const wl_columnar_lftj_attempt_t *a = &n->lftj;
    if (a->identity != a || a->numeric_owner != n || a->frame != n->frame
        || a->session != n->session || a->control != n->control
        || a->attempt_id != n->attempt_id || !a->thread_valid
        || a->phase < WL_COLUMNAR_LFTJ_ATTEMPT_RESOLVE
        || a->phase > WL_COLUMNAR_LFTJ_ATTEMPT_DONE) return EINVAL;
    if (a->busy) return EBUSY;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(a->owner_thread, thrd_current())) return EINVAL;
#elif defined(_WIN32) || defined(_WIN64)
    if (a->owner_thread != GetCurrentThreadId()) return EINVAL;
#else
    if (!pthread_equal(a->owner_thread, pthread_self())) return EINVAL;
#endif
    wl_columnar_eval_plan_numeric_phase_t phase = n->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING ? n->pending_action
        : n->phase;
    if (a->phase != WL_COLUMNAR_LFTJ_ATTEMPT_DONE
        && phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
        && phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE) return EINVAL;
    if (a->phase != WL_COLUMNAR_LFTJ_ATTEMPT_DONE
        && (n->lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_CONSUMED
        || !a->metadata || a->metadata != n->lftj_metadata.metadata
        || a->input_count > n->lftj_metadata.k
        || a->cleanup_cursor > a->input_count || a->cleanup_input_stage > 3))
        return EINVAL;
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE
        && (a->writer_active || a->output_owned || a->output || a->inputs
        || a->row || a->scratch_reserved || a->scratch_committed
        || a->retirement_started || a->retirement_initializer.identity
        || a->writer_drain.identity))
        return EINVAL;
    if (a->writer_drain.identity
        && (a->writer_drain.identity != &a->writer_drain
        || (a->writer_drain.batch && a->writer_drain.batch != &a->writer)))
        return EINVAL;
    const wl_columnar_relation_scalar_retirement_initializer_t *i =
        &a->retirement_initializer;
    if (i->identity && (i->identity != i || i->parent != a
        || i->retirement != &a->retirement || i->relation != a->output))
        return EINVAL;
    if (a->retirement_started
        && (a->retirement.storage.identity != (uintptr_t)&a->retirement.storage
        || a->retirement.committed != a->retirement_committed)) return EINVAL;
    if (a->retirement_drained && !a->retirement_started) return EINVAL;
    return 0;
}

static int
wl_columnar_eval_plan_native_lftj_metadata_owner(
    const wl_columnar_eval_plan_numeric_attempt_t *n)
{
    const wl_columnar_lftj_metadata_proof_t *p = &n->lftj_metadata;
    bool preflight = n->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT_LFTJ;
    if ((!preflight &&
        n->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN)
        || n->lftj_metadata_dispatch || !n->plan || !n->plan->ops
        || n->end > n->plan->op_count || p->op_index < n->begin
        || p->op_index >= n->end || n->cursor != p->op_index
        || p->operation != &n->plan->ops[p->op_index]
        || p->operation->op != WL_PLAN_OP_LFTJ
        || n->source_initialized || n->source_slot_borrowed
        || n->join_host || n->join_host_admitted || n->join_host_governor)
        return EINVAL;
    if (preflight) {
        if (p->phase < WL_COLUMNAR_LFTJ_METADATA_HEADER
            || p->phase > WL_COLUMNAR_LFTJ_METADATA_VALIDATED) return EINVAL;
    } else if (p->phase < WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER
        || p->phase > WL_COLUMNAR_LFTJ_METADATA_INSTALL) return EINVAL;
    /* HEADER precedes capture and semantic validation. It owns no child
    * resources; malformed language metadata must still be clearable. */
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_HEADER) return 0;
    if (!p->metadata || p->metadata != p->operation->opaque_data
        || p->k < 2 || p->k > WL_LFTJ_MAX_K
        || p->k != p->metadata->k || !p->names || !p->keys
        || p->names != p->metadata->rel_names ||
        p->keys != p->metadata->key_cols)
        return EINVAL;
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_NAMES)
        return p->name_index < p->k ? 0 : EINVAL;
    if (p->name_index != p->k) return EINVAL;
    /* BIND_HEADER has not captured a binding tuple yet. */
    if (p->phase <= WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER) return 0;
    if (p->bindings != n->private_bindings || p->entry > p->binding_count
        || p->pair > p->entry) return EINVAL;
    const wl_columnar_eval_private_bindings_t *b = p->bindings;
    if (b) {
        if (b->identity != (uintptr_t)b || b->session != n->session
            || b->plan != n->plan || b->control != n->control
            || b->attempt_id != n->attempt_id || !b->count
            || p->binding_count != b->count ||
            p->dynamic_entries != b->dynamic_entries
            || p->binding_generation != b->generation
            || (!b->dynamic_entries &&
            b->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX))
            return EINVAL;
    } else if (p->binding_count || p->dynamic_entries || p->binding_generation)
        return EINVAL;
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY
        || p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR) {
        if (!b || p->dynamic_entries || p->entry >= p->binding_count
            || p->entry >= WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX
            || (p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR &&
            p->pair >= p->entry))
            return EINVAL;
    } else if (p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_DYNAMIC) {
        if (!b || !p->dynamic_entries || p->entry || p->pair) return EINVAL;
    } else if (p->entry != p->binding_count) return EINVAL;
    if (p->phase >= WL_COLUMNAR_LFTJ_METADATA_PREFIX
        && (p->registry != n->session->rels ||
        p->registry_count != n->session->nrels
        || p->prefix_cleared !=
        (p->phase == WL_COLUMNAR_LFTJ_METADATA_INSTALL)))
        return EINVAL;
    /* Identity is published last in INSTALL, with no hook before child
     * activation. Neverborn or prefix-cleared storage is not inspected. */
    return 0;
}

static int
wl_columnar_eval_plan_registered_cleanup_admit(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id,
    wl_columnar_eval_plan_cleanup_kind_t kind)
{
    if (!ctx) return EINVAL;
    bool unary = kind == WL_COLUMNAR_EVAL_PLAN_CLEANUP_UNARY;
    bool observed = kind == WL_COLUMNAR_EVAL_PLAN_CLEANUP_OBSERVED;
    bool provisional = false;
    int cause = 0;
    int rc;
    if (observed) {
        wl_columnar_session_observed_cleanup_result_t proof =
            wl_columnar_session_observed_cleanup_prove(ctx, funding, id, NULL);
        rc = proof.protocol; cause = proof.cause;
        provisional = proof.provisional;
    } else rc = unary
        ? wl_columnar_session_unary_cleanup_prove(ctx, funding, id, NULL,
                &cause)
        : wl_columnar_session_join_cleanup_prove(ctx, funding, id, NULL,
                &cause);
    if (rc) return rc;
    wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    /* Finance only: the embedded observation grants no child authority. */
    bool native_lftj = n->lftj.identity
        && n->lftj.phase != WL_COLUMNAR_LFTJ_ATTEMPT_DONE;
    bool native_metadata = n->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT_LFTJ
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN;
    rc = wl_evaluation_control_charge_attempt(funding, id,
            native_lftj || native_metadata ? 32 : 128);
    if (rc) return rc;
    if (!ctx->child_bound || !wl_columnar_eval_plan_numeric_owner_valid(n,
        ctx->frame)
        || n->session != ctx->session || n->plan != ctx->plan
        || n->control != ctx->control || n->attempt_id != ctx->attempt_id
        || n->private_bindings != ctx->private_bindings || !n->composed_mode
        || n->composed_busy || n->runtime_depth > COL_STACK_MAX
        || n->preflight_depth > COL_STACK_MAX
        || n->phase < WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_INPUTS
        || n->phase > WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
        return EINVAL;
    if (native_lftj || native_metadata) {
        rc = wl_evaluation_control_charge_attempt(funding, id, 8);
        if (rc) return rc;
    }
    if (n->lftj.identity) {
        if (native_metadata && native_lftj) return EINVAL;
        rc = wl_columnar_eval_plan_native_lftj_owner(n);
        if (rc) return rc;
    }
    if (native_metadata) {
        rc = wl_evaluation_control_charge_attempt(funding, id, 0);
        if (rc) return rc;
        rc = wl_columnar_eval_plan_native_lftj_metadata_owner(n);
        if (rc) return rc;
        /* Keep the nested stack/thread owner proof in its own real packet. */
        rc = wl_evaluation_control_charge_attempt(funding, id, 0);
        if (rc) return rc;
    }
    /* Unary authority never selects either live or neverborn LFTJ state.
     * This chain follows the existing LFTJ 32/8/zero packet boundaries. */
    if (unary && (n->lftj.identity || n->join_host || n->join_host_admitted
        || n->join_host_governor
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT_LFTJ
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ACK
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ADVANCE))
        return EINVAL;
    rc = wl_columnar_eval_stack_numeric_owner(n->frame, n);
    if (rc) return rc;
    if (!observed || !provisional
        || n->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING) {
        if (observed && provisional &&
            n->semantic_result) cause = n->semantic_result;
        if (!ctx->primary_result) ctx->primary_result = cause;
        if (!n->semantic_result) n->semantic_result = cause;
    }
    if (native_lftj && !n->lftj.semantic_result)
        n->lftj.semantic_result = cause;
    /* Error intent is now a native cleanup obligation, not permission to
     * request cancellation on whatever generation the controller holds. */
    n->abort_requested = false;
    return 0;
}

static void
wl_columnar_eval_plan_join_cleanup_redirect(
    wl_columnar_eval_plan_numeric_attempt_t *n)
{
    switch (n->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_INPUTS:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PREFLIGHT_LFTJ:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_BEGIN:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SLOT:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_START:
        n->phase = n->source_initialized
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
            : n->source_slot_borrowed
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        break;
    default: break;
    }
    if (n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        && (n->join_host || n->join_host_admitted || n->join_host_governor))
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH;
}

static int
wl_columnar_eval_plan_registered_cleanup_step(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id,
    wl_columnar_eval_plan_cleanup_kind_t kind)
{
    int rc = wl_columnar_eval_plan_registered_cleanup_admit(ctx, funding, id,
            kind);
    if (rc) return rc;
    wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING) return EBUSY;
    wl_columnar_eval_plan_join_cleanup_redirect(n);
    if (kind == WL_COLUMNAR_EVAL_PLAN_CLEANUP_OBSERVED
        && n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) {
        bool neverborn = false;
        rc = wl_columnar_filter_observed_neverborn_owned(n->frame, n,
                &neverborn);
        if (rc) return rc;
        if (neverborn) n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
    }
    n->composed_busy = true;
    switch (n->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD: {
        rc = wl_evaluation_control_charge_attempt(funding, id, 0);
        if (rc) break;
        bool done = false;
        rc = wl_columnar_ops_lftj_attempt_abort(&n->lftj, &done, funding, id);
        if (!rc &&
            done) n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        break;
    }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD: {
        bool bare;
        rc =
            kind == WL_COLUMNAR_EVAL_PLAN_CLEANUP_OBSERVED
            ? wl_columnar_filter_observed_native_cleanup_latch_owned(n->frame,
                n, funding, id, &bare) :
            kind == WL_COLUMNAR_EVAL_PLAN_CLEANUP_UNARY ?
            wl_columnar_filter_unary_native_cleanup_latch_owned(n->frame,
                n, funding, id, &bare)
            : wl_columnar_filter_op_native_cleanup_latch_owned(n->frame, n,
                funding, id, &bare);
        if (rc) break;
        rc = wl_evaluation_control_charge_attempt(funding, id, bare ? 56 : 32);
        if (rc) break;
        wl_columnar_filter_source_end_kind_t source_kind;
        rc = wl_columnar_filter_source_end_pending_owned(n->frame, n,
                funding, id, &source_kind);
        if (rc) break;
        bool ending = source_kind != WL_COLUMNAR_FILTER_SOURCE_END_NONE;
        if (ending) {
            rc = wl_columnar_eval_plan_source_end_fund(funding, id, n->control);
            if (rc) break;
        }
        n->pending_action = n->phase;
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        rc = ending ? wl_columnar_filter_source_end_step_owned(n->frame,
                &n->child_status, n, funding, id)
            : wl_columnar_filter_op_attempt_step_owned(n->frame,
                &n->child_status, n);
        break;
    }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK:
        n->pending_action = n->phase;
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        rc = wl_columnar_filter_op_attempt_ack_owned(n->frame, n);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD:
        rc = wl_columnar_merge_concat_native_cleanup_latch_owned(n->frame, n,
                funding, id);
        if (rc) break;
        rc = wl_evaluation_control_charge_attempt(funding, id, 0);
        if (rc) break;
        n->pending_action = n->phase;
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        rc = wl_columnar_merge_concat_attempt_step_owned(n->frame,
                &n->child_status, funding, id, n);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ACK:
        n->pending_action = n->phase;
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        rc = wl_columnar_merge_concat_attempt_ack_owned(n->frame, funding, id,
                n);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ADVANCE:
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ADVANCE:
        rc = wl_columnar_eval_plan_composed_unary_advance(n);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH:
        rc =
            wl_columnar_eval_plan_numeric_composed_attempt_step_validated_body(
            n,
            funding, id);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END:
        rc = wl_columnar_eval_plan_source_end_fund(funding, id, n->control);
        if (rc) break;
        rc = wl_columnar_eval_plan_composed_source_step(n, funding, id);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT:
        rc = wl_columnar_eval_plan_composed_source_step(n, funding, id);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE:
        if (n->source_initialized || n->source_slot_borrowed
            || n->join_host || n->join_host_admitted ||
            n->join_host_governor) rc = EBUSY;
        else if (n->lftj.identity
            && n->lftj.phase != WL_COLUMNAR_LFTJ_ATTEMPT_DONE) {
            rc = wl_evaluation_control_charge_attempt(funding, id, 0);
            if (rc) break;
            bool done = false;
            rc = wl_columnar_ops_lftj_attempt_abort(&n->lftj, &done, funding,
                    id);
        } else if (n->lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_INERT) {
            rc = wl_evaluation_control_charge_attempt(funding, id, 128);
            if (rc) break;
            memset(&n->lftj_metadata, 0, sizeof(n->lftj_metadata));
            n->lftj_metadata_dispatch = false;
        }else n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE:
        break;
    default: rc = EBUSY; break;
    }
    n->composed_busy = false;
    return rc;
}

int
wl_columnar_eval_plan_numeric_join_cleanup_step(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_step(ctx, funding, id,
               WL_COLUMNAR_EVAL_PLAN_CLEANUP_JOIN);
}

int
wl_columnar_eval_plan_unary_native_cleanup_step(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_step(ctx, funding, id,
               WL_COLUMNAR_EVAL_PLAN_CLEANUP_UNARY);
}

static int
wl_columnar_eval_plan_registered_cleanup_consume(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int protocol,
    wl_evaluation_control_t *funding, uint64_t id,
    wl_columnar_eval_plan_cleanup_kind_t kind)
{
    int rc = wl_columnar_eval_plan_registered_cleanup_admit(ctx, funding, id,
            kind);
    if (rc) return rc;
    wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (n->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING) return EBUSY;
    rc = wl_columnar_eval_plan_composed_fund(funding, id, n->control);
    if (rc) return rc;
    int cause = n->semantic_result;
    rc = wl_columnar_eval_plan_composed_consume_body(n, protocol);
    if (kind != WL_COLUMNAR_EVAL_PLAN_CLEANUP_OBSERVED || cause)
        n->semantic_result = cause;
    if (!rc) wl_columnar_eval_plan_join_cleanup_redirect(n);
    return rc;
}

int
wl_columnar_eval_plan_numeric_join_cleanup_consume(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int protocol,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_consume(ctx, protocol,
               funding, id, WL_COLUMNAR_EVAL_PLAN_CLEANUP_JOIN);
}

int
wl_columnar_eval_plan_unary_native_cleanup_consume(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int protocol,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_consume(ctx, protocol,
               funding, id, WL_COLUMNAR_EVAL_PLAN_CLEANUP_UNARY);
}

static int
wl_columnar_eval_plan_registered_cleanup_ack(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id,
    wl_columnar_eval_plan_cleanup_kind_t kind)
{
    int rc = wl_columnar_eval_plan_registered_cleanup_admit(ctx, funding, id,
            kind);
    if (rc) return rc;
    wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (n->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        || n->source_initialized || n->source_slot_borrowed
        || n->join_host || n->join_host_admitted || n->join_host_governor)
        return EBUSY;
    rc = wl_columnar_eval_plan_composed_fund(funding, id, n->control);
    if (rc) return rc;
    rc = wl_columnar_eval_plan_composed_ack_body(n, funding, id);
    if (!rc && kind == WL_COLUMNAR_EVAL_PLAN_CLEANUP_JOIN)
        wl_columnar_eval_stack_auto_child_acked(ctx);
    return rc;
}

int
wl_columnar_eval_plan_numeric_join_cleanup_ack(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_ack(ctx, funding, id,
               WL_COLUMNAR_EVAL_PLAN_CLEANUP_JOIN);
}

int
wl_columnar_eval_plan_unary_native_cleanup_ack(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_ack(ctx, funding, id,
               WL_COLUMNAR_EVAL_PLAN_CLEANUP_UNARY);
}

int
wl_columnar_eval_plan_observed_native_cleanup_step(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_step(ctx, funding, id,
               WL_COLUMNAR_EVAL_PLAN_CLEANUP_OBSERVED);
}
int
wl_columnar_eval_plan_observed_native_cleanup_consume(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int protocol,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_consume(ctx, protocol,
               funding, id, WL_COLUMNAR_EVAL_PLAN_CLEANUP_OBSERVED);
}
int
wl_columnar_eval_plan_observed_native_cleanup_ack(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_columnar_eval_plan_registered_cleanup_ack(ctx, funding, id,
               WL_COLUMNAR_EVAL_PLAN_CLEANUP_OBSERVED);
}

int
wl_columnar_eval_plan_numeric_native_cleanup_step(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_auto_cleanup_admit(ctx, funding, id);
    if (rc) return rc;
    wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    n->composed_busy = true;
    switch (n->phase) {
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD: {
        wl_columnar_filter_source_end_kind_t source_kind;
        rc = wl_columnar_filter_source_end_pending_owned(n->frame, n,
                funding, id, &source_kind);
        if (rc) break;
        n->pending_action = n->phase;
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        rc = source_kind != WL_COLUMNAR_FILTER_SOURCE_END_NONE
            ? wl_columnar_filter_source_end_step_owned(n->frame,
                &n->child_status, n, funding, id)
            : wl_columnar_filter_op_attempt_step_owned(n->frame,
                &n->child_status, n);
        break;
    }
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK:
        n->pending_action = n->phase;
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        rc = wl_columnar_filter_op_attempt_ack_owned(n->frame, n);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ADVANCE:
        rc = wl_columnar_eval_plan_composed_unary_advance(n);
        break;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE:
        n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
        break;
    default:
        rc = EBUSY;
        break;
    }
    n->composed_busy = false;
    return rc;
}

int
wl_columnar_eval_plan_numeric_native_cleanup_consume(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int child_protocol,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_auto_cleanup_admit(ctx, funding, id);
    if (rc) return rc;
    wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (n->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        || (n->pending_action !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        && n->pending_action !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK))
        return EBUSY;
    return wl_columnar_eval_plan_composed_consume_body(n, child_protocol);
}

int
wl_columnar_eval_plan_numeric_native_cleanup_ack(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_auto_cleanup_admit(ctx, funding, id);
    if (rc) return rc;
    if (ctx->child.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
        return EBUSY;
    /* This confined framed child has never acquired LFTJ identity. The full
     * native proof excludes all active/completed LFTJ state; generic ACK still
     * clears that whole record. Preserve the real source unbind in both paths. */
    rc = wl_columnar_eval_stack_composed_source_release(ctx->child.frame,
            ctx->child.session, &ctx->child);
    if (rc) return rc;
    wl_columnar_eval_plan_composed_ack_clear(&ctx->child);
    return 0;
}

int
wl_columnar_eval_plan_numeric_composed_attempt_step(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (record && (record->composed_busy
        || wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame))) return EBUSY;
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    if (record->composed_busy) return EBUSY;
    bool setup = record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
        && record->plan && record->plan->ops
        && record->cursor >= record->begin && record->cursor < record->end
        && record->cursor < record->plan->op_count
        && (record->plan->ops[record->cursor].op == WL_PLAN_OP_SEMIJOIN
        || record->plan->ops[record->cursor].op == WL_PLAN_OP_ANTIJOIN
        || (record->plan->ops[record->cursor].op == WL_PLAN_OP_JOIN
        && (!record->controlled_materialized_batch_fallback
        || record->join_host_declined)));
    if (setup) {
        if (!funding || funding == record->control || !funding_id)
            return EINVAL;
        rc = wl_evaluation_control_charge_attempt(funding, funding_id,
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK);
        if (rc) return rc;
    }
    record->composed_busy = true;
    if (setup) {
        wl_columnar_join_setup_result_t result =
            wl_columnar_join_op_attempt_start_funded(
            &record->plan->ops[record->cursor], record->frame, record->session,
            record->control, record->attempt_id, &record->child_status, record,
            funding, funding_id);
        rc = result.protocol;
        if (result.outcome == WL_COLUMNAR_JOIN_SETUP_ORIGINAL_STOPPED
            || result.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID) {
            if (!record->semantic_result) record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            rc = 0;
        }
    } else rc =
            wl_columnar_eval_plan_numeric_composed_attempt_step_validated_body(
            record, funding, funding_id);
    record->composed_busy = false; return rc;
}

int
wl_columnar_eval_plan_numeric_composed_cleanup_lftj(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_composed_validate(record, funding,
            funding_id);
    if (rc) return rc;
    if (wl_columnar_eval_stack_frame_mutation_blocked(
            record->frame)) return EBUSY;
    if (record->composed_busy) return EBUSY;
    record->composed_busy = true;
    rc = wl_columnar_eval_plan_numeric_composed_cleanup_lftj_body(record,
            funding, funding_id);
    record->composed_busy = false; return rc;
}
