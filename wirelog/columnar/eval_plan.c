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
            if (depth) return EINVAL;
            ++depth;
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
    }
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
                 * recorded as pipeline-not-consumable.  TDD bound runs
                 * refuse WIRELOG_JOIN_BATCH_BYTES, so the plan slices they
                 * evaluate, whose end is not a rule's end, do not get
                 * here. */
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

int
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
    wl_col_session_t *sess)
{
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
    WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK = 384,
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

static int
wl_columnar_eval_plan_numeric_fund(wl_evaluation_control_t *funding,
    uint64_t id, const wl_evaluation_control_t *operation)
{
    if (!funding || funding == operation || !id
        || wl_evaluation_control_active_attempt_id(funding) != id)
        return EINVAL;
    int rc = wl_evaluation_control_charge(funding,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK);
    if (rc) return rc;
    return wl_evaluation_control_active_attempt_id(funding) == id ? 0 : EINVAL;
}

static int
wl_columnar_eval_plan_numeric_validate(
    const wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (!record || !wl_columnar_eval_plan_numeric_owner_valid(record,
        record->frame)) return EINVAL;
    if (record->session->cleanup_active != record->frame) return EBUSY;
    return wl_columnar_eval_stack_numeric_owner(record->frame, record);
}

static int
wl_columnar_eval_plan_numeric_forward(
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (!record->control || wl_evaluation_control_active_attempt_id(
            record->control) != record->attempt_id) return EINVAL;
    int rc = wl_evaluation_control_charge(record->control,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_FORWARD_WORK);
    if (rc) return rc;
    return wl_evaluation_control_active_attempt_id(record->control)
           == record->attempt_id ? 0 : EINVAL;
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
    col_rel_t *candidate = record->source_registry[record->source_slot];
    if (!candidate || !candidate->name) {
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
    char actual = candidate->name[byte];
    if (actual != wanted) {
        ++record->source_slot;
        record->source_byte = 0;
    } else if (actual) {
        ++record->source_byte;
    } else if (delta) {
        record->source_delta = candidate;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SELECT;
    } else {
        record->source_full = candidate;
        record->source_slot = 0; record->source_byte = 0;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_DELTA;
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

/* Forward source actions are one registry byte/opcode or a finite state
 * transition. Child calls return directly; their results are consumed only
 * on a later separately funded host turn. */
static int
wl_columnar_eval_plan_numeric_source_step(
    wl_columnar_eval_plan_numeric_attempt_t *record)
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
        return wl_columnar_relation_scalar_source_init_attempt_controlled(
            &record->source, &record->source_initialized,
            record->control, record->attempt_id);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_HOLD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &record->source, record->source_selected, !record->source_empty);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SLOT:
        rc = wl_columnar_eval_stack_numeric_source_output(record->frame,
                record->session, record);
        if (rc) return rc;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW;
        return 0;
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return
            wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
            (
            &record->source,
            record->source_is_delta ? "$empty_delta" : "$empty_skip",
            record->session->memory_governor
                   ? record->session->memory_governor
                   : record->source_selected->memory_governor,
            record->source_output);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING;
        return wl_columnar_relation_scalar_source_end(&record->source,
                   record->semantic_result);
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
        if (record->source.phase !=
            WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) {
            record->protocol_result = child_result ? child_result : EBUSY;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END;
            return 0;
        }
        if (!record->semantic_result) record->semantic_result = child_result;
        record->source_initialized = false;
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
        return wl_columnar_eval_plan_numeric_source_step(record);
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
    if (record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE || !record->control)
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
    int rc = wl_evaluation_control_charge(control, work);
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
    return wl_evaluation_control_charge(funding,
               WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK);
}

static int
wl_columnar_eval_plan_composed_validate(
    const wl_columnar_eval_plan_numeric_attempt_t *record)
{
    int rc = wl_columnar_eval_plan_numeric_validate(record);
    if (rc) return rc;
    if (!record->composed_mode || record->runtime_depth > COL_STACK_MAX
        || record->preflight_depth > COL_STACK_MAX
        || record->phase < WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_INPUTS
        || record->phase > WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
        return EINVAL;
    return 0;
}

int
wl_columnar_eval_plan_numeric_composed_attempt_begin(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    const wl_plan_relation_t *plan, uint32_t begin, uint32_t end,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    uint32_t initial_depth, wl_evaluation_control_t *control, uint64_t id,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!record || record->identity || record->phase || !plan || !plan->ops
        || begin >= end || end > plan->op_count || !frame || !session
        || !control || !id || initial_depth > COL_STACK_MAX
        || (session->nrels && !session->rels)) return EINVAL;
    if (session->current_iteration || session->retraction_seeded
        || session->retraction_right_pass || session->tdd_input_run
        || session->tdd_subpass_active || session->tdd_outbound_only_active
        || session->diff_operators_active || session->join_batch_bytes
        || session->join_output_shared_count || session->coordinator
        || session->num_workers > 1) return ENOTSUP;
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
    record->begin = begin; record->end = end; record->cursor = begin;
    record->completed = 0; record->semantic_result = 0;
    record->protocol_result = 0;
    record->source_mode = true; record->source_installed = false;
    record->abort_supported = true; record->abort_requested = false;
    record->source_empty = false; record->source_is_delta = false;
    record->source_initialized = false; record->source_slot_borrowed = false;
    record->source_registry = session->rels;
    record->source_count = session->nrels;
    record->source_slot = 0; record->source_byte = 0;
    record->source_begin = begin;
    record->source_full = NULL; record->source_delta = NULL;
    record->source_selected = NULL; record->source_output = NULL;
    record->composed_mode = true;
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
        record->composed_mode = false;
        record->preflight_depth = 0; record->runtime_depth = 0;
        wl_evaluation_control_release(control);
    }
    return rc;
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
    col_rel_t *candidate = record->source_registry[record->source_slot];
    if (!candidate || !candidate->name) {
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
    char actual = candidate->name[byte];
    if (actual != wanted) {
        ++record->source_slot;
        record->source_byte = 0;
    } else if (actual) {
        ++record->source_byte;
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
        return wl_columnar_relation_scalar_source_init_attempt_controlled(
            &record->source, &record->source_initialized,
            record->control, record->attempt_id);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &record->source, record->source_selected, !record->source_empty);
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
        return
            wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
            (
            &record->source,
            record->source_is_delta ? "$empty_delta" : "$empty_skip",
            record->session->memory_governor ? record->session->memory_governor
                : record->source_selected->memory_governor,
            record->source_output);
    case WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END:
        record->pending_action = record->phase;
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        return wl_columnar_relation_scalar_source_end(&record->source,
                   record->semantic_result);
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
        if (!child_result && record->source.timestamps) child_result = ENOTSUP;
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
        if (record->source.phase !=
            WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) {
            record->protocol_result = child_result ? child_result : EBUSY;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END;
            return 0;
        }
        if (!record->semantic_result) record->semantic_result = child_result;
        record->source_initialized = false;
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

int
wl_columnar_eval_plan_numeric_composed_attempt_step(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_composed_validate(record);
    if (rc) return rc;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    if (record->abort_requested) {
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
            if (stack->top != record->runtime_depth || rel->timestamps
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
            if (kind != WL_PLAN_OP_VARIABLE && kind != WL_PLAN_OP_FILTER
                && kind != WL_PLAN_OP_MAP
                && !(kind == WL_PLAN_OP_JOIN
                && !record->plan->ops[record->cursor].materialized)
                && kind != WL_PLAN_OP_LFTJ
                && kind != WL_PLAN_OP_CONCAT
                && !(kind == WL_PLAN_OP_CONSOLIDATE
                && record->cursor + 1 == record->end))
                record->abort_supported = false;
            if (kind == WL_PLAN_OP_VARIABLE) {
                if (record->preflight_depth == COL_STACK_MAX) rc = ENOBUFS;
                else ++record->preflight_depth;
            } else if (kind == WL_PLAN_OP_CONCAT) {
                if (record->preflight_depth < 2) rc = EINVAL;
                else --record->preflight_depth;
            } else if (kind == WL_PLAN_OP_LFTJ) {
                if (record->preflight_depth != 0) rc = EINVAL;
                else rc = wl_columnar_ops_lftj_attempt_preflight(
                        &record->plan->ops[record->cursor], record->session);
                if (!rc) ++record->preflight_depth;
            } else if (kind == WL_PLAN_OP_FILTER || kind == WL_PLAN_OP_MAP
                || kind == WL_PLAN_OP_JOIN || kind == WL_PLAN_OP_SEMIJOIN
                || kind == WL_PLAN_OP_ANTIJOIN || kind == WL_PLAN_OP_REDUCE
                || kind == WL_PLAN_OP_CONSOLIDATE) {
                if (!record->preflight_depth) rc = EINVAL;
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
        if (record->plan->ops[record->cursor].op == WL_PLAN_OP_CONCAT) {
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_START;
            return 0;
        }
        if (record->plan->ops[record->cursor].op == WL_PLAN_OP_LFTJ) {
            rc = wl_columnar_ops_lftj_attempt_begin(&record->lftj,
                    &record->plan->ops[record->cursor], record->frame,
                    record->session, record->control, record->attempt_id,
                    record);
            if (rc) {
                record->semantic_result = rc;
                record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
            } else record->phase =
                    WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD;
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
        rc = wl_columnar_ops_lftj_attempt_step(&record->lftj, &done);
        if (rc) {
            record->semantic_result = rc;
            record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
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
        if (record->phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ADVANCE
            && record->child_status.committed) --record->runtime_depth;
        if (!record->semantic_result) ++record->completed;
        record->phase = record->semantic_result ||
            ++record->cursor == record->end
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY;
        return 0;
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
            (void)wl_columnar_ops_lftj_attempt_abort(&record->lftj, &done);
            if (!done) return 0;
            if (!record->semantic_result && record->lftj.semantic_result)
                record->semantic_result = record->lftj.semantic_result;
        }
        record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
        return 0;
    default:
        return EBUSY;
    }
}

int
wl_columnar_eval_plan_numeric_composed_consume_result(
    wl_columnar_eval_plan_numeric_attempt_t *record, int child_protocol,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_composed_validate(record);
    if (rc) return rc;
    if (record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
        return EBUSY;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
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
wl_columnar_eval_plan_numeric_composed_attempt_status(
    const wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_columnar_eval_plan_numeric_status_t *status,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!status) return EINVAL;
    int rc = wl_columnar_eval_plan_composed_validate(record);
    if (rc) return rc;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    status->phase = record->phase; status->completed = record->completed;
    status->semantic_result = record->semantic_result;
    status->protocol_result = record->protocol_result;
    status->retained = record->protocol_result != 0
        || ((record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        || record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD)
        && record->child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    return 0;
}

int
wl_columnar_eval_plan_numeric_composed_abort_request(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_composed_validate(record);
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

int
wl_columnar_eval_plan_numeric_composed_attempt_ack(
    wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_plan_composed_validate(record);
    if (rc) return rc;
    if (record->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        || !record->control) return EBUSY;
    rc = wl_columnar_eval_plan_composed_fund(funding, funding_id,
            record->control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_composed_source_release(record->frame,
            record->session, record);
    if (rc) return rc;
    wl_evaluation_control_t *control = record->control;
    record->control = NULL; record->identity = NULL;
    record->frame = NULL; record->session = NULL; record->plan = NULL;
    record->source_registry = NULL; record->source_mode = false;
    record->source_output = NULL; record->source_installed = false;
    record->thread_valid = false; record->attempt_id = 0;
    record->composed_mode = false; record->preflight_depth = 0;
    record->runtime_depth = 0;
    record->abort_supported = false; record->abort_requested = false;
    memset(&record->lftj, 0, sizeof(record->lftj));
    record->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT;
    wl_evaluation_control_release(control);
    return 0;
}
