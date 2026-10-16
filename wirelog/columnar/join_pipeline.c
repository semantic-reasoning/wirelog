#include "columnar/join_pipeline.h"
#include "columnar/join_batch.h"

#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

/* Row-local numeric bytecode: variables, literals and comparisons, a pure
 * function of the current row that reads no intern, extension or session
 * state.  MAP (Issue #1777) also admits integer and float arithmetic, whose
 * checked failures surface as the ordinary MAP's ERANGE; FILTER does not. */
static bool
wl_columnar_join_pipeline_expr_is_row_local(
    const wl_plan_expr_buffer_t *expr, bool allow_arithmetic)
{
    const uint8_t *data;
    uint32_t pos = 0;
    uint32_t depth = 0;

    if (!expr || !expr->data || expr->size == 0)
        return false;
    data = expr->data;
    while (pos < expr->size) {
        uint8_t tag = data[pos++];
        uint32_t payload = 0;

        switch ((wl_plan_expr_tag_t)tag) {
        case WL_PLAN_EXPR_VAR:
        case WL_PLAN_EXPR_VAR_FLOAT:
            if (expr->size - pos < 2u)
                return false;
            uint16_t name_len;
            memcpy(&name_len, data + pos, sizeof(name_len));
            payload = 2u + (uint32_t)name_len;
            if (payload > expr->size - pos)
                return false;
            pos += payload;
            depth++;
            break;
        case WL_PLAN_EXPR_CONST_INT:
        case WL_PLAN_EXPR_CONST_FLOAT:
            if (expr->size - pos < 8u)
                return false;
            pos += 8u;
            depth++;
            break;
        case WL_PLAN_EXPR_BOOL:
            if (expr->size - pos < 1u)
                return false;
            pos++;
            depth++;
            break;
        case WL_PLAN_EXPR_CMP_EQ:
        case WL_PLAN_EXPR_CMP_NEQ:
        case WL_PLAN_EXPR_CMP_LT:
        case WL_PLAN_EXPR_CMP_GT:
        case WL_PLAN_EXPR_CMP_LTE:
        case WL_PLAN_EXPR_CMP_GTE:
        case WL_PLAN_EXPR_CMP_FLOAT_EQ:
        case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
        case WL_PLAN_EXPR_CMP_FLOAT_LT:
        case WL_PLAN_EXPR_CMP_FLOAT_GT:
        case WL_PLAN_EXPR_CMP_FLOAT_LTE:
        case WL_PLAN_EXPR_CMP_FLOAT_GTE:
            if (depth < 2u)
                return false;
            depth--;
            break;
        case WL_PLAN_EXPR_ARITH_ADD:
        case WL_PLAN_EXPR_ARITH_SUB:
        case WL_PLAN_EXPR_ARITH_MUL:
        case WL_PLAN_EXPR_ARITH_DIV:
        case WL_PLAN_EXPR_ARITH_MOD:
        case WL_PLAN_EXPR_ARITH_BAND:
        case WL_PLAN_EXPR_ARITH_BOR:
        case WL_PLAN_EXPR_ARITH_BXOR:
        case WL_PLAN_EXPR_ARITH_SHL:
        case WL_PLAN_EXPR_ARITH_SHR:
        case WL_PLAN_EXPR_ARITH_FLOAT_ADD:
        case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
        case WL_PLAN_EXPR_ARITH_FLOAT_MUL:
        case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
            if (!allow_arithmetic || depth < 2u)
                return false;
            depth--;
            break;
        case WL_PLAN_EXPR_ARITH_BNOT:
            if (!allow_arithmetic || depth < 1u)
                return false;
            break;
        default:
            /* Strings, digests, UUIDs, aggregates, extensions and unknown
             * tags stay on the materialized path until their batch contract
             * is specified. */
            return false;
        }
    }
    return depth == 1u;
}

/* Whether @map is a row-local MAP the pipeline can evaluate per batch. */
static bool
wl_columnar_join_pipeline_map_is_consumable(const wl_plan_op_t *map)
{
    if (map->project_count == 0 || map->materialized)
        return false;
    if (map->map_expr_count == 0)
        return map->map_exprs == NULL && map->project_indices != NULL;
    if (!map->map_exprs)
        return false;
    /* An empty slot projects its column, as in col_op_map(). */
    for (uint32_t c = 0; c < map->map_expr_count; c++) {
        const wl_plan_expr_buffer_t *expr = &map->map_exprs[c];
        if ((expr->data && expr->size > 0)
            && !wl_columnar_join_pipeline_expr_is_row_local(expr, true))
            return false;
    }
    return true;
}

/* Whether @op is a SEMIJOIN/ANTIJOIN probe stage the pipeline can apply
 * per batch, judged from the plan alone: keyed, no right filter, not
 * materialized, and reading a named relation that is not an evaluator
 * temporary ("$" prefix: $d$, $r$ and the like). */
static bool
wl_columnar_join_pipeline_stage_is_consumable(const wl_plan_op_t *op)
{
    return op->key_count > 0 && op->right_filter_expr.size == 0
           && !op->materialized && op->right_relation
           && op->right_relation[0] != '$';
}

wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *sess, uint32_t *tail_end)
{
    const wl_plan_op_t *join;
    uint32_t i;

    if (tail_end)
        *tail_end = UINT32_MAX;
    if (!plan || !sess || join_index >= plan->op_count)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
    if (!plan->ops)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
    if (sess->join_batch_bytes == 0)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF;
    if (sess->coordinator || sess->diff_operators_active)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION;

    join = &plan->ops[join_index];
    if (join->op != WL_PLAN_OP_JOIN || join->key_count == 0
        || join->right_filter_expr.size != 0 || join->materialized)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN;

    /* The tail is the longest prefix of consumable operators: row-local
     * FILTERs, optionally ended by one row-local MAP. */
    i = join_index + 1u;
    while (i < plan->op_count) {
        const wl_plan_op_t *op = &plan->ops[i];
        if (op->op == WL_PLAN_OP_FILTER) {
            if (op->materialized
                || !wl_columnar_join_pipeline_expr_is_row_local(
                    &op->filter_expr, false))
                break;
            i++;
            continue;
        }
        if (op->op == WL_PLAN_OP_SEMIJOIN || op->op == WL_PLAN_OP_ANTIJOIN) {
            if (!wl_columnar_join_pipeline_stage_is_consumable(op))
                break;
            i++;
            continue;
        }
        if (op->op == WL_PLAN_OP_MAP
            && wl_columnar_join_pipeline_map_is_consumable(op))
            i++;
        break;
    }
    if (i == join_index + 1u) {
        /* Empty prefix: report why the first operator is not consumed. */
        if (i < plan->op_count && plan->ops[i].op == WL_PLAN_OP_FILTER)
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER;
        if (i < plan->op_count && plan->ops[i].op == WL_PLAN_OP_MAP)
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP;
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
    }
    if (tail_end)
        *tail_end = i - 1u;
    return WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
}

const char *
wl_columnar_join_pipeline_eligibility_name(
    wl_columnar_join_pipeline_eligibility_t reason)
{
    switch (reason) {
    case WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE: return "eligible";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF: return "off";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION: return "session-dependent";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN: return "join-ineligible";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE: return "unsupported-shape";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER: return
            "filter-not-row-local";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP: return "map-not-row-local";
    default: return "unknown";
    }
}

/* A SEMIJOIN/ANTIJOIN stage: its right relation as found at setup, the
 * key columns resolved against the stage input, and the probe set, built
 * on the first batch that reaches the stage. */
typedef struct {
    bool active;
    col_rel_t *right;
    uint32_t *lk;
    uint32_t *rk;
    uint64_t identity;
    uint64_t view_generation;
    uint64_t storage_generation;
    wl_columnar_join_probe_set_t set;
} wl_join_pipeline_stage_t;

static void
pipeline_stages_release(wl_join_pipeline_stage_t *stages, uint32_t count)
{
    if (!stages)
        return;
    for (uint32_t i = 0; i < count; i++) {
        wl_columnar_join_probe_set_release(&stages[i].set);
        free(stages[i].lk);
        free(stages[i].rk);
    }
    free(stages);
}

typedef struct {
    wl_col_session_t *sess;
    const wl_plan_relation_t *plan;
    uint32_t first_tail;
    uint32_t tail_end;
    wl_join_pipeline_stage_t *stages; /* indexed by op - first_tail */
    col_rel_t *out;
    uint32_t pending_begin;
    int error; /* errno of a failed FILTER/MAP, reported unchanged */
    bool begun;
} wl_join_pipeline_sink_t;

static wl_columnar_continuation_status_t
pipeline_sink_begin(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    if (!sink || !sink->out || !batch || !batch->payload || batch->rows == 0)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    sink->pending_begin = sink->out->nrows;
    sink->begun = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
pipeline_sink_reserve(void *context, uint64_t bytes, uint32_t rows)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;

    (void)bytes;
    wl_columnar_continuation_status_t status;
    uint64_t need;

    if (!sink || !sink->begun || !sink->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    need = (uint64_t)sink->out->nrows + rows;
    /* A representation limit, not a memory failure (#1382). */
    if (need > UINT32_MAX) {
        sink->error = EOVERFLOW;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    /* Same growth rule as the relation sink (Issue #2173): double first,
     * exact on a budget denial. */
    status = col_join_batch_sink_grow(sink->out, need);
    /* The publish loop destroys the output on failure, so a denied growth
     * is reported through the sink's error as a budget denial (#2185). */
    if (status == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED)
        sink->error = wl_columnar_join_budget_denial(sink->sess, NULL,
                ENOSPC);
    return status;
}

#ifdef WL_TEST_RELATION_RESIZE_HOOK
wl_columnar_continuation_status_t
wl_columnar_join_pipeline_test_reserve(col_rel_t *out, uint32_t rows,
    int *error_out)
{
    wl_join_pipeline_sink_t sink = { .out = out, .begun = true };
    wl_columnar_continuation_status_t status
        = pipeline_sink_reserve(&sink, 0, rows);
    if (error_out)
        *error_out = sink.error;
    return status;
}
#endif

/* Apply one probe stage to the batch on top of @batch_stack.  EAGAIN when
 * the stage's right relation changed since setup. */
static int
pipeline_apply_stage(wl_join_pipeline_sink_t *sink, const wl_plan_op_t *op,
    wl_join_pipeline_stage_t *stage, eval_stack_t *batch_stack)
{
    eval_entry_t input;
    col_rel_t *output = NULL;
    int rc;

    if (stage->right->relation_identity != stage->identity
        || stage->right->view_generation != stage->view_generation
        || stage->right->storage_generation != stage->storage_generation
        || stage->view_generation == UINT64_MAX
        || stage->storage_generation == UINT64_MAX)
        return EAGAIN;
    rc = eval_stack_pop_relation(batch_stack, &input);
    if (rc != 0)
        return rc;
    if (!stage->set.built) {
        rc = wl_columnar_join_probe_set_build(sink->sess,
                wl_columnar_join_output_governor(sink->sess, input.rel,
                stage->right), stage->right, stage->rk, op->key_count,
                op->op == WL_PLAN_OP_SEMIJOIN
                ? WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS
                : WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS_PLUS_ONE, &stage->set);
        if (rc != 0) {
            (void)eval_entry_dispose(&input);
            return rc;
        }
    }
    rc = op->op == WL_PLAN_OP_SEMIJOIN
        ? wl_columnar_semijoin_emit(op, sink->sess, input.rel, stage->right,
            stage->lk, stage->rk, &stage->set, &output)
        : wl_columnar_antijoin_emit(op, sink->sess, input.rel, stage->right,
            stage->lk, stage->rk, &stage->set, &output);
    (void)eval_entry_dispose(&input);
    if (rc != 0)
        return rc;
    rc = eval_stack_push(batch_stack, output, true);
    if (rc != 0)
        col_rel_destroy(output);
    return rc;
}

static wl_columnar_continuation_status_t
pipeline_sink_append(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    eval_stack_t batch_stack;
    eval_entry_t result;
    int rc;

    if (!sink || !sink->begun || !sink->out || !batch || !batch->payload)
        return WL_COLUMNAR_CONTINUATION_INVALID;

    eval_stack_init(&batch_stack);
    rc = eval_stack_push(&batch_stack, (col_rel_t *)batch->payload, false);
    if (rc != 0) {
        sink->error = rc;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    for (uint32_t i = sink->first_tail; i <= sink->tail_end; i++) {
        const wl_plan_op_t *op = &sink->plan->ops[i];
        if (op->op == WL_PLAN_OP_SEMIJOIN || op->op == WL_PLAN_OP_ANTIJOIN)
            rc = pipeline_apply_stage(sink, op,
                    &sink->stages[i - sink->first_tail], &batch_stack);
        else
            rc = op->op == WL_PLAN_OP_MAP
                ? col_op_map(op, &batch_stack, sink->sess)
                : wl_columnar_filter_op(op, &batch_stack, sink->sess);
        if (rc != 0) {
            (void)eval_stack_drain(&batch_stack);
            sink->error = rc;
            return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
        }
    }
    rc = eval_stack_pop_relation(&batch_stack, &result);
    if (rc != 0 || !result.rel) {
        (void)eval_stack_drain(&batch_stack);
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    if (result.rel->ncols != sink->out->ncols
        || result.rel->nrows > batch->rows
        || (uint64_t)sink->pending_begin + result.rel->nrows
        > sink->out->capacity) {
        (void)eval_entry_dispose(&result);
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        uint32_t dst = sink->pending_begin + row;
        for (uint32_t col = 0; col < result.rel->ncols; col++) {
            rc = col_rel_set_raw(sink->out, dst, col,
                    result.rel->columns[col][row]);
            if (rc != 0) {
                (void)eval_entry_dispose(&result);
                return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
            }
        }
    }
    sink->out->nrows = sink->pending_begin + result.rel->nrows;
    (void)eval_entry_dispose(&result);
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
pipeline_sink_commit(void *context, bool *committed)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    if (committed)
        *committed = false;
    if (!sink || !sink->begun || !sink->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    wl_columnar_relation_touch_view(sink->out);
    sink->begun = false;
    if (committed)
        *committed = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static void
pipeline_sink_abort(void *context)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    if (sink && sink->out && sink->begun)
        sink->out->nrows = sink->pending_begin;
    if (sink)
        sink->begun = false;
}

static int
pipeline_resolve_key(const col_rel_t *rel, const char *name, uint32_t *out)
{
    int index;
    if (!rel || !out || !name)
        return EINVAL;
    index = col_rel_col_idx(rel, name);
    /* The ordinary join operator preserves its historical column-0 fallback
     * for plans whose symbolic names were lowered without relation schemas.
     * Keep the pipeline on that same resolution contract. */
    *out = index >= 0 ? (uint32_t)index : 0u;
    return 0;
}

wl_columnar_join_successor_t
wl_columnar_join_pipeline_successor(const wl_plan_relation_t *plan,
    uint32_t first_unconsumed)
{
    /* Depth of the tracked entry below the top of the stack. */
    uint32_t depth = 0;

    if (!plan || (!plan->ops && plan->op_count > 0))
        return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
    for (uint32_t i = first_unconsumed; i < plan->op_count; i++) {
        const wl_plan_op_t *op = &plan->ops[i];
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
        case WL_PLAN_OP_LFTJ:
        case WL_PLAN_OP_K_FUSION:
            /* Push only. */
            depth++;
            break;
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_JOIN:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
        case WL_PLAN_OP_REDUCE:
            /* Pop one, push one: reads the tracked entry only at the top. */
            if (depth == 0)
                return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            break;
        /* CONCAT pops two and pushes their union; with fewer than two
         * entries it passes the stack through, which leaves depth 0. */
        case WL_PLAN_OP_CONCAT:
            depth = depth <= 1u ? 0u : depth - 1u;
            break;
        case WL_PLAN_OP_EXCHANGE: {
            const wl_plan_op_exchange_t *meta
                = (const wl_plan_op_exchange_t *)op->opaque_data;
            if (!meta)
                return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
            if (meta->num_workers <= 1u)
                break;
            /* Pops the top into the exchange buffers and pushes nothing. */
            if (depth == 0)
                return WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK;
            depth--;
            break;
        }
        default:
            return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
        }
    }
    return WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK;
}

/* Walk the statically consumable tail and set up its probe stages.  A
 * stage whose right relation is missing, or whose keys do not resolve in
 * range with compatible types against the stage input, ends the tail
 * before it (*@tail_end is lowered).  On success with at least one stage,
 * *@stages holds one slot per tail operator and *@final_template the
 * schema of the last stage's output; both are NULL when the tail has no
 * stage.  Errors are the output constructors' or ENOMEM. */
static int
pipeline_setup_stages(const wl_plan_relation_t *plan, uint32_t join_index,
    uint32_t *tail_end, wl_col_session_t *sess, const col_rel_t *left,
    const col_rel_t *right, const wl_plan_op_t *join,
    wl_join_pipeline_stage_t **stages_out, col_rel_t **final_template)
{
    wl_join_pipeline_stage_t *stages;
    col_rel_t *tpl = NULL;
    uint32_t count = *tail_end - join_index;
    bool any = false;
    int rc;

    *stages_out = NULL;
    *final_template = NULL;
    for (uint32_t i = join_index + 1u; i <= *tail_end; i++) {
        if (plan->ops[i].op == WL_PLAN_OP_SEMIJOIN
            || plan->ops[i].op == WL_PLAN_OP_ANTIJOIN)
            any = true;
    }
    if (!any)
        return 0;
    stages = (wl_join_pipeline_stage_t *)calloc(count, sizeof(*stages));
    if (!stages)
        return ENOMEM;
    tpl = col_rel_new_auto("$join_pipeline_shape",
            col_join_output_width(left, right, join));
    rc = tpl ? col_join_set_output_types(tpl, left, right, join) : ENOMEM;
    if (rc != 0) {
        col_rel_destroy(tpl);
        free(stages);
        return rc;
    }
    for (uint32_t i = join_index + 1u; i <= *tail_end; i++) {
        const wl_plan_op_t *op = &plan->ops[i];
        wl_join_pipeline_stage_t *stage = &stages[i - join_index - 1u];
        col_rel_t *stage_right;
        col_rel_t *next = NULL;
        bool usable = true;

        if (op->op != WL_PLAN_OP_SEMIJOIN && op->op != WL_PLAN_OP_ANTIJOIN)
            continue; /* FILTER keeps its input's schema; a MAP ends it */
        stage_right = session_find_rel(sess, op->right_relation);
        if (stage_right) {
            stage->lk = (uint32_t *)calloc(op->key_count, sizeof(uint32_t));
            stage->rk = (uint32_t *)calloc(op->key_count, sizeof(uint32_t));
            if (!stage->lk || !stage->rk) {
                rc = ENOMEM;
                break;
            }
            for (uint32_t k = 0; k < op->key_count && usable; k++) {
                if (pipeline_resolve_key(tpl,
                    op->left_keys ? op->left_keys[k] : NULL,
                    &stage->lk[k]) != 0
                    || pipeline_resolve_key(stage_right,
                    op->right_keys ? op->right_keys[k] : NULL,
                    &stage->rk[k]) != 0
                    || stage->lk[k] >= tpl->ncols
                    || stage->rk[k] >= stage_right->ncols)
                    usable = false;
            }
            if (usable && !col_join_key_types_compatible(tpl, stage->lk,
                stage_right, stage->rk, op->key_count))
                usable = false;
        } else {
            usable = false;
        }
        if (!usable) {
            /* The operator runs on its own after the tail, as today. */
            free(stage->lk);
            free(stage->rk);
            stage->lk = NULL;
            stage->rk = NULL;
            *tail_end = i - 1u;
            break;
        }
        rc = op->op == WL_PLAN_OP_SEMIJOIN
            ? wl_columnar_semijoin_new_output(op, sess, tpl, stage_right,
                &next)
            : wl_columnar_antijoin_new_output(op, sess, tpl, stage_right,
                &next);
        if (rc != 0)
            break;
        col_rel_destroy(tpl);
        tpl = next;
        stage->active = true;
        stage->right = stage_right;
        stage->identity = stage_right->relation_identity;
        stage->view_generation = stage_right->view_generation;
        stage->storage_generation = stage_right->storage_generation;
    }
    if (rc != 0) {
        col_rel_destroy(tpl);
        pipeline_stages_release(stages, count);
        return rc;
    }
    *stages_out = stages;
    *final_template = tpl;
    return 0;
}

int
wl_columnar_join_pipeline_try_eval(const wl_plan_relation_t *plan,
    uint32_t join_index, eval_stack_t *stack, wl_col_session_t *sess,
    uint32_t *next_index, wl_columnar_join_pipeline_decline_t *decline)
{
    wl_columnar_join_pipeline_eligibility_t eligibility;
    const wl_plan_op_t *join;
    const wl_plan_op_t *last;
    eval_entry_t left_entry;
    col_rel_t *right;
    wl_columnar_join_selection_t selection;
    col_rel_t *out = NULL;
    wl_columnar_continuation_t *continuation = NULL;
    wl_join_pipeline_sink_t sink_ctx;
    wl_columnar_continuation_sink_t sink;
    uint32_t tail_end = UINT32_MAX;
    uint32_t *lk = NULL;
    uint32_t *rk = NULL;
    wl_join_pipeline_stage_t *stages = NULL;
    col_rel_t *stage_template = NULL;
    int rc;

    if (next_index)
        *next_index = join_index;
    if (decline)
        *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NOT_ELIGIBLE;
    eligibility = wl_columnar_join_pipeline_preflight(plan, join_index,
            sess, &tail_end);
    /* Session- and JOIN-level exclusions decline before the operand is
     * examined; a shape exclusion is decided after it, so that what the
     * JOIN reads takes precedence over what follows it. */
    if (eligibility == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF
        || eligibility == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION
        || eligibility == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN)
        return 0;
    /* The preflight also reports a missing plan or an index past its end
     * as a shape exclusion; there is no JOIN to examine then. */
    if (!plan || !plan->ops || join_index >= plan->op_count)
        return 0;
    if (decline)
        *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NONE;
    if (!stack || stack->top == 0 || !sess)
        return EINVAL;
    join = &plan->ops[join_index];
    if (stack->items[stack->top - 1].kind
        != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || !stack->items[stack->top - 1].rel)
        return EINVAL;
    /* A missing right relation joins to an empty result in the ordinary
     * JOIN.  ENOENT from col_join_batch_producer_create below is a
     * different case, an unavailable arrangement, and stays a fallback. */
    right = session_find_rel(sess, join->right_relation);
    if (!right) {
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY;
        return 0;
    }
    /* Consume only a JOIN that would read the persistent relation itself.
     * A forced-empty right operand is left to the ordinary JOIN here; a
     * $d$/$r$ delta is declined below, after the key checks the ordinary
     * JOIN also runs against it. */
    selection = wl_columnar_join_select_right(join, sess, right,
            stack->items[stack->top - 1].is_delta);
    if (selection.force_empty) {
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY;
        return 0;
    }

    lk = (uint32_t *)calloc(join->key_count, sizeof(*lk));
    rk = (uint32_t *)calloc(join->key_count, sizeof(*rk));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        return ENOMEM;
    }
    for (uint32_t i = 0; i < join->key_count; i++) {
        rc = pipeline_resolve_key(stack->items[stack->top - 1].rel,
                join->left_keys ? join->left_keys[i] : NULL, &lk[i]);
        if (rc == 0)
            rc = pipeline_resolve_key(selection.right,
                    join->right_keys ? join->right_keys[i] : NULL, &rk[i]);
        if (rc != 0) {
            free(lk);
            free(rk);
            return ENOTSUP;
        }
    }
    /* A key that resolved out of range came from the column-0 fallback
     * above on a zero-column relation; the ordinary JOIN handles that
     * input, so decline it.  This check must precede the type check,
     * which indexes the column types by these keys. */
    for (uint32_t i = 0; i < join->key_count; i++) {
        if (lk[i] >= stack->items[stack->top - 1].rel->ncols
            || rk[i] >= selection.right->ncols) {
            free(lk);
            free(rk);
            if (decline)
                *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY;
            return 0;
        }
    }
    if (!col_join_key_types_compatible(stack->items[stack->top - 1].rel, lk,
        selection.right, rk, join->key_count)) {
        free(lk);
        free(rk);
        return EINVAL;
    }
    /* Today every substituted right operand also sets used_right_delta;
     * the pointer comparison also declines a future substitution that does
     * not set it. */
    if (selection.used_right_delta || selection.right != right) {
        free(lk);
        free(rk);
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_DELTA_RIGHT;
        return 0;
    }
    if (eligibility != WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE) {
        free(lk);
        free(rk);
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NOT_ELIGIBLE;
        return 0;
    }
    /* Probe stages: resolve each against the schema its input would have,
     * truncating the tail before a stage the pipeline cannot apply.  The
     * schemas come from zero-row templates built with the operators' own
     * output constructors; no operator runs. */
    rc = pipeline_setup_stages(plan, join_index, &tail_end, sess,
            stack->items[stack->top - 1].rel, right, join, &stages,
            &stage_template);
    if (rc != 0) {
        free(lk);
        free(rk);
        return rc;
    }
    if (tail_end == join_index) {
        free(lk);
        free(rk);
        pipeline_stages_release(stages, tail_end - join_index);
        col_rel_destroy(stage_template);
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NOT_ELIGIBLE;
        return 0;
    }
    last = &plan->ops[tail_end];
    /* Strict mode refuses a tail whose output another operator reads
     * before any producer exists; eval_plan records
     * pipeline-not-consumable once for the ENOTSUP. */
    if (sess->join_batch_strict
        && wl_columnar_join_pipeline_successor(plan, tail_end + 1u)
        == WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING) {
        free(lk);
        free(rk);
        pipeline_stages_release(stages, tail_end - join_index);
        col_rel_destroy(stage_template);
        return ENOTSUP;
    }

    rc = col_join_batch_producer_create(sess, join,
            stack->items[stack->top - 1].rel,
            stack->items[stack->top - 1].is_delta, lk, rk, join->key_count,
            sess->join_batch_bytes, &continuation);
    free(lk);
    free(rk);
    if (rc != 0) {
        pipeline_stages_release(stages, tail_end - join_index);
        col_rel_destroy(stage_template);
        return rc;
    }

    /* The output has the schema the sequential tail would publish: a MAP's
     * is new_auto(project_count), as in col_op_map(); a FILTER's is its
     * input's, which is the JOIN output's for a FILTER-only tail. */
    if (last->op == WL_PLAN_OP_MAP) {
        out = col_rel_new_auto("$join_pipeline", last->project_count);
        rc = out ? 0 : ENOMEM;
    } else if (stage_template) {
        /* The last stage's schema; new_like copies no timestamps. */
        out = col_rel_new_like("$join_pipeline", stage_template);
        rc = out ? 0 : ENOMEM;
    } else {
        out = col_rel_new_auto("$join_pipeline",
                col_join_output_width(stack->items[stack->top - 1].rel,
                right, join));
        rc = out ? col_join_set_output_types(out,
                stack->items[stack->top - 1].rel, right, join) : ENOMEM;
    }
    col_rel_destroy(stage_template);
    stage_template = NULL;
    if (rc != 0) {
        col_rel_destroy(out);
        wl_columnar_continuation_cancel(continuation);
        wl_columnar_continuation_destroy(continuation);
        pipeline_stages_release(stages, tail_end - join_index);
        return rc;
    }
    if (sess->memory_governor) {
        rc = col_rel_attach_memory_governor(out, sess->memory_governor);
        if (rc == 0)
            rc = col_rel_reserve_capacity_admitted(out, out->capacity, NULL);
        if (rc != 0) {
            rc = wl_columnar_join_budget_denial(sess, out, rc);
            col_rel_destroy(out);
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            pipeline_stages_release(stages, tail_end - join_index);
            return rc;
        }
    }

    left_entry = eval_stack_pop(stack);
    memset(&sink_ctx, 0, sizeof(sink_ctx));
    sink_ctx.sess = sess;
    sink_ctx.plan = plan;
    sink_ctx.first_tail = join_index + 1u;
    sink_ctx.tail_end = tail_end;
    sink_ctx.stages = stages;
    sink_ctx.out = out;
    memset(&sink, 0, sizeof(sink));
    sink.context = &sink_ctx;
    sink.begin = pipeline_sink_begin;
    sink.reserve = pipeline_sink_reserve;
    sink.append = pipeline_sink_append;
    sink.commit = pipeline_sink_commit;
    sink.abort = pipeline_sink_abort;

    for (;;) {
        uint64_t before = out->nrows;
        wl_columnar_continuation_status_t status
            = wl_columnar_continuation_publish(continuation, &sink);
        if (status == WL_COLUMNAR_CONTINUATION_DONE)
            break;
        if (status != WL_COLUMNAR_CONTINUATION_OK) {
            pipeline_sink_abort(&sink_ctx);
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            col_rel_destroy(out);
            pipeline_stages_release(stages, tail_end - join_index);
            if (eval_stack_repush_entry(stack, &left_entry) != 0)
                return EFAULT;
            if (status == WL_COLUMNAR_CONTINUATION_STALE)
                return EAGAIN;
            /* A FILTER/MAP failure keeps the operator's own errno (for
             * MAP, ERANGE for an expression error and ENOSPC for a denied
             * admission) instead of collapsing into ENOMEM.  An errno the
             * caller treats as a fallback (EAGAIN, ENOENT, ENOTSUP) re-runs
             * the restored input on the one-shot path. */
            return sink_ctx.error != 0 ? sink_ctx.error : ENOMEM;
        }
        /* Preserve the pipeline's post-FILTER/MAP output cap semantics. */
        if (wl_columnar_join_output_limit_reached(sess, out,
            out->nrows - before)) {
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            col_rel_destroy(out);
            pipeline_stages_release(stages, tail_end - join_index);
            if (eval_stack_repush_entry(stack, &left_entry) != 0)
                return EFAULT;
            return EOVERFLOW;
        }
    }
    wl_columnar_continuation_destroy(continuation);
    pipeline_stages_release(stages, tail_end - join_index);
    stages = NULL;
    rc = eval_entry_dispose(&left_entry);
    if (rc != 0) {
        col_rel_destroy(out);
        if (eval_stack_repush_entry(stack, &left_entry) != 0)
            return EFAULT;
        return rc;
    }
    rc = eval_stack_push(stack, out, true);
    if (rc != 0) {
        col_rel_destroy(out);
        return rc;
    }
    if (next_index)
        *next_index = tail_end;
    return 1;
}
