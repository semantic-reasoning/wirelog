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

static bool
wl_columnar_join_pipeline_expr_checked(
    const wl_plan_expr_buffer_t *expr, bool allow_arithmetic,
    wl_evaluation_control_t *control, int *error)
{
    const uint8_t *data;
    uint32_t pos = 0;
    uint32_t depth = 0;

    if (!expr || !expr->data || expr->size == 0)
        return false;
    data = expr->data;
    while (pos < expr->size) {
        *error = wl_evaluation_control_charge(control, 16);
        if (*error) return false;
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
            for (uint32_t consumed = 0; consumed < payload;) {
                uint32_t count = payload - consumed;
                if (count > 128u) count = 128u;
                *error = wl_evaluation_control_charge(control, count);
                if (*error) return false;
                pos += count; consumed += count;
                *error = wl_evaluation_control_charge(control, 0);
                if (*error) return false;
            }
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
        *error = wl_evaluation_control_charge(control, 0);
        if (*error) return false;
    }
    return depth == 1u;
}

wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *sess, uint32_t *map_index)
{
    const wl_plan_op_t *join;
    const wl_plan_op_t *map;
    uint32_t i;

    if (map_index)
        *map_index = UINT32_MAX;
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

    i = join_index + 1u;
    while (i < plan->op_count && plan->ops[i].op == WL_PLAN_OP_FILTER) {
        if (plan->ops[i].materialized)
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER;
        if (!wl_columnar_join_pipeline_expr_is_row_local(
                &plan->ops[i].filter_expr, false))
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER;
        i++;
    }
    if (i >= plan->op_count || plan->ops[i].op != WL_PLAN_OP_MAP)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;

    map = &plan->ops[i];
    if (map->project_count == 0 || map->materialized)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP;
    if (map->map_expr_count == 0) {
        if (map->map_exprs != NULL || !map->project_indices)
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP;
    } else {
        if (!map->map_exprs)
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP;
        /* An empty slot projects its column, as in col_op_map(). */
        for (uint32_t c = 0; c < map->map_expr_count; c++) {
            const wl_plan_expr_buffer_t *expr = &map->map_exprs[c];
            if ((expr->data && expr->size > 0)
                && !wl_columnar_join_pipeline_expr_is_row_local(expr, true))
                return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP;
        }
    }

    if (map_index)
        *map_index = i;
    return WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
}

int
wl_columnar_join_pipeline_preflight_controlled(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *session, uint32_t *map_index,
    wl_columnar_join_pipeline_eligibility_t *reason,
    wl_evaluation_control_t *control)
{
    if (!reason || !control) return EINVAL;
    *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
    if (map_index) *map_index = UINT32_MAX;
    int rc = wl_evaluation_control_charge(control, 64);
    if (rc) return rc;
    if (!plan || !session || !plan->ops || join_index >= plan->op_count)
        return 0;
    if (!session->join_batch_bytes) {
        *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF; return 0;
    }
    if (session->coordinator || session->diff_operators_active) {
        *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION; return 0;
    }
    const wl_plan_op_t *join = &plan->ops[join_index];
    if (join->op != WL_PLAN_OP_JOIN || !join->key_count
        || join->right_filter_expr.size || join->materialized) {
        *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN; return 0;
    }
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    uint32_t index = join_index + 1u;
    while (index < plan->op_count) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return rc;
        const wl_plan_op_t *filter = &plan->ops[index];
        bool is_filter = filter->op == WL_PLAN_OP_FILTER;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (!is_filter) break;
        if (filter->materialized) {
            *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER; return 0;
        }
        int error = 0;
        bool valid = wl_columnar_join_pipeline_expr_checked(
            &filter->filter_expr, false, control, &error);
        if (error) return error;
        if (!valid) {
            *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER; return 0;
        }
        index++;
    }
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    if (index >= plan->op_count || plan->ops[index].op != WL_PLAN_OP_MAP)
        return 0;
    const wl_plan_op_t *map = &plan->ops[index];
    if (!map->project_count || map->materialized) {
        *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP; return 0;
    }
    if (!map->map_expr_count) {
        if (map->map_exprs || !map->project_indices) {
            *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP; return 0;
        }
    } else if (!map->map_exprs) {
        *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP; return 0;
    }
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    for (uint32_t column = 0; column < map->map_expr_count; column++) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return rc;
        const wl_plan_expr_buffer_t *expression = &map->map_exprs[column];
        bool nonempty = expression->data && expression->size;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (!nonempty) continue;
        int error = 0;
        bool valid = wl_columnar_join_pipeline_expr_checked(expression, true,
                control, &error);
        if (error) return error;
        if (!valid) {
            *reason = WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP; return 0;
        }
    }
    *reason = WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
    if (map_index) *map_index = index;
    return 0;
}

int
wl_columnar_join_pipeline_plan_validate_controlled(
    const wl_plan_relation_t *plan, const wl_col_session_t *session,
    wl_evaluation_control_t *control, bool *supported)
{
    if (!supported || !control) return EINVAL;
    *supported = false;
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    if (!plan || !plan->ops || plan->op_count < 4u
        || plan->ops[0].op != WL_PLAN_OP_VARIABLE
        || (plan->ops[0].delta_mode != WL_DELTA_AUTO
        && plan->ops[0].delta_mode != WL_DELTA_FORCE_FULL)
        || plan->ops[1].op != WL_PLAN_OP_JOIN
        || (plan->ops[1].delta_mode != WL_DELTA_AUTO
        && plan->ops[1].delta_mode != WL_DELTA_FORCE_FULL)
        || plan->ops[plan->op_count - 1u].op != WL_PLAN_OP_CONSOLIDATE)
        return 0;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    uint32_t map_index;
    wl_columnar_join_pipeline_eligibility_t reason;
    rc = wl_columnar_join_pipeline_preflight_controlled(plan, 1u, session,
            &map_index, &reason, control);
    if (rc) return rc;
    *supported = reason == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE
        && map_index == plan->op_count - 2u;
    return wl_evaluation_control_charge(control, 0);
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

typedef struct {
    wl_col_session_t *sess;
    const wl_plan_relation_t *plan;
    uint32_t first_filter;
    uint32_t map_index;
    col_rel_t *out;
    uint32_t pending_begin;
    int error; /* errno of a failed FILTER/MAP, reported unchanged */
    bool begun;
} wl_join_pipeline_sink_t;

int
wl_columnar_join_pipeline_stage_retire(wl_columnar_join_batch_record_t *record,
    int primary)
{
    if (!record || !record->ready || !record->session)
        return primary > 0 ? primary : EINVAL;
    if (record->phase == WL_COLUMNAR_JOIN_BATCH_PIPELINE_INITIALIZING)
        return primary;
    if (record->phase != WL_COLUMNAR_JOIN_BATCH_PIPELINE
        || !record->pipeline.ready) return primary > 0 ? primary : EINVAL;
    /* Original embedded address remains valid in the pending-frame list;
     * cleanup_active has already been detached before mandatory disposal. */
    wl_columnar_eval_stack_cleanup_frame_t *frame =
        wl_columnar_eval_stack_cleanup_join_batch_frame(record);
    if (!frame) return primary > 0 ? primary : EINVAL;
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    if (!bulk) return primary > 0 ? primary : EINVAL;
    int cleanup = wl_columnar_relation_bulk_append_retire(bulk, 0);
    if (cleanup || bulk->active)
        return primary > 0 ? primary : cleanup ? cleanup : EINVAL;
    wl_columnar_relation_private_batch_t *writer =
        wl_columnar_eval_stack_cleanup_pipeline_private_batch(frame);
    if (!writer) return primary > 0 ? primary : EINVAL;
    cleanup = wl_columnar_relation_private_batch_retire(writer, 0);
    if (cleanup) return primary > 0 ? primary : cleanup;
    cleanup = wl_columnar_relation_source_group_drain(&bulk->sources, 0);
    if (cleanup) return primary > 0 ? primary : cleanup;
    wl_columnar_expr_operator_preparation_t *preparation =
        &record->pipeline.preparation;
    if (preparation->ready) {
        cleanup = wl_columnar_expr_operator_retire(preparation, 0, true, NULL);
        if (cleanup) return primary > 0 ? primary : cleanup;
    }
    if (record->pipeline.scratch_moved) {
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        if (!stack || record->pipeline.stage_slot >= COL_STACK_MAX)
            return primary > 0 ? primary : EINVAL;
        cleanup = eval_entry_dispose(
            &stack->items[record->pipeline.stage_slot]);
        if (cleanup) return primary > 0 ? primary : cleanup;
        cleanup = eval_entry_dispose(
            wl_columnar_eval_stack_cleanup_result(frame));
        if (cleanup) return primary > 0 ? primary : cleanup;
    }
    record->pipeline.stage = WL_COLUMNAR_JOIN_PIPELINE_STAGE_INERT;
    return primary;
}

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
    uint64_t need;
    int rc;
    bool denied = false;

    (void)bytes;
    if (!sink || !sink->begun || !sink->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    need = (uint64_t)sink->out->nrows + rows;
    if (need > UINT32_MAX)
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    rc = col_rel_reserve_capacity_admitted(sink->out, (uint32_t)need,
            &denied);
    if (rc == 0)
        return WL_COLUMNAR_CONTINUATION_OK;
    return rc == ENOMEM && denied ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
                        : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
}

#ifdef WL_TEST_RELATION_RESIZE_HOOK
wl_columnar_continuation_status_t
wl_columnar_join_pipeline_test_reserve(col_rel_t *out, uint32_t rows)
{
    wl_join_pipeline_sink_t sink = { .out = out, .begun = true };
    return pipeline_sink_reserve(&sink, 0, rows);
}
#endif

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
    for (uint32_t i = sink->first_filter; i < sink->map_index; i++) {
        rc = wl_columnar_filter_op(&sink->plan->ops[i], &batch_stack,
                sink->sess);
        if (rc != 0) {
            (void)eval_stack_drain(&batch_stack);
            sink->error = rc;
            return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
        }
    }
    rc = col_op_map(&sink->plan->ops[sink->map_index], &batch_stack,
            sink->sess);
    if (rc != 0) {
        (void)eval_stack_drain(&batch_stack);
        sink->error = rc;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
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

static int
wl_columnar_join_pipeline_finish_stage(wl_columnar_join_batch_record_t *record,
    bool mandatory)
{
    if (!record || record->phase != WL_COLUMNAR_JOIN_BATCH_PIPELINE
        || !record->pipeline.ready) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame =
        record->session->cleanup_active;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    if (!stack || !result) return EINVAL;
    int rc = mandatory ? 0 : wl_evaluation_control_charge(record->control, 96);
    if (rc) return rc;
    if (record->pipeline.scratch_slot >= COL_STACK_MAX
        || record->pipeline.stage_slot >= COL_STACK_MAX) return EINVAL;
    if (!mandatory) {
        rc = wl_evaluation_control_charge(record->control, 0);
        if (rc) return rc;
    }
    rc = wl_columnar_join_pipeline_stage_retire(record, 0);
    if (rc) return rc;
    if (record->pipeline.scratch_moved) {
        eval_entry_t *scratch = &stack->items[record->pipeline.scratch_slot];
        if (!scratch->rel || !scratch->owned || result->rel || result->owned
            || result->continuation || result->seg_boundaries) return EINVAL;
        *result = *scratch;
        *scratch = (eval_entry_t){0};
        record->pipeline.scratch_moved = false;
    }
    record->pipeline.begun = false;
    record->pipeline.published = false;
    record->pipeline.stage = WL_COLUMNAR_JOIN_PIPELINE_STAGE_INERT;
    stack->top = record->pipeline.stage_slot + 1u;
    return mandatory ? 0 : wl_evaluation_control_charge(record->control, 0);
}

static wl_columnar_continuation_status_t
wl_columnar_join_pipeline_sink_begin_controlled(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_columnar_join_batch_record_t *record = context;
    if (!record || !record->pipeline.ready || !record->output
        || !record->output->rel || !batch || !batch->rows || !batch->payload)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    record->reason = wl_evaluation_control_charge(record->control, 64);
    if (record->reason) return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    record->committed = false;
    record->pipeline.pending_begin = record->output->rel->nrows;
    record->pipeline.begun = true;
    record->pipeline.published = false;
    record->reason = wl_evaluation_control_charge(record->control, 0);
    return record->reason ? WL_COLUMNAR_CONTINUATION_SINK_FAILURE
        : WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
wl_columnar_join_pipeline_sink_reserve_controlled(void *context,
    uint64_t bytes, uint32_t rows)
{
    (void)bytes;
    wl_columnar_join_batch_record_t *record = context;
    if (!record || !record->pipeline.begun || !rows)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    record->reason = wl_evaluation_control_charge(record->control, 96);
    if (record->reason) return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    uint64_t need = (uint64_t)record->pipeline.pending_begin + rows;
    if (need > UINT32_MAX) {
        record->reason = EOVERFLOW;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    record->reason = wl_evaluation_control_charge(record->control, 0);
    if (record->reason) return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(
        record->session->cleanup_active);
    if (!bulk) {
        record->reason = EINVAL; return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    bool denied = false;
    record->reason = wl_columnar_relation_bulk_reserve_exact_controlled(bulk,
            record->output->rel, (uint32_t)need, &denied, record->control);
    if (!record->reason) return WL_COLUMNAR_CONTINUATION_OK;
    return denied ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
        : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
}

static wl_columnar_continuation_status_t
wl_columnar_join_pipeline_sink_append_controlled(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_columnar_join_batch_record_t *record = context;
    wl_columnar_eval_stack_cleanup_frame_t *frame =
        record->session->cleanup_active;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t *scratch = wl_columnar_eval_stack_cleanup_result(frame);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    record->reason = wl_evaluation_control_charge(record->control, 64);
    if (record->reason) goto failed;
    if (!stack || !scratch || !bulk || bulk->active
        || bulk->sources.descriptor_count || bulk->sources.owner_count
        || !scratch->owned || scratch->rel != batch->payload
        || record->pipeline.scratch_moved
        || record->pipeline.scratch_slot >= COL_STACK_MAX
        || record->pipeline.stage_slot >= COL_STACK_MAX
        || stack->items[record->pipeline.scratch_slot].rel
        || stack->items[record->pipeline.stage_slot].rel) {
        record->reason = EINVAL; goto failed;
    }
    record->reason = wl_evaluation_control_charge(record->control, 0);
    if (record->reason) goto failed;
    record->reason = wl_evaluation_control_charge(record->control, 128);
    if (record->reason) goto failed;
    stack->items[record->pipeline.scratch_slot] = *scratch;
    *scratch = (eval_entry_t){0};
    record->pipeline.scratch_moved = true;
    stack->items[record->pipeline.stage_slot] = (eval_entry_t){
        .rel = (col_rel_t *)batch->payload,
        .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION
    };
    stack->top = record->pipeline.stage_slot + 1u;
    record->pipeline.stage = WL_COLUMNAR_JOIN_PIPELINE_STAGE_TRANSFORM;
    record->reason = wl_evaluation_control_charge(record->control, 0);
    if (record->reason) goto failed;
    for (uint32_t index = record->pipeline.first_filter;
        index <= record->pipeline.map_index; index++){
        record->reason = wl_evaluation_control_charge(record->control, 16);
        if (record->reason) goto failed;
        record->pipeline.stage_index = index;
        record->reason = wl_evaluation_control_charge(record->control, 0);
        if (record->reason) goto failed;
        const wl_plan_op_t *op = &record->pipeline.plan->ops[index];
        record->reason = index == record->pipeline.map_index
            ? col_op_map(op, stack, record->session)
            : wl_columnar_filter_op(op, stack, record->session);
        if (record->reason) goto failed;
        wl_columnar_join_batch_checkpoint(record, 14, index);
        record->reason = wl_evaluation_control_charge(record->control, 0);
        if (record->reason) goto failed;
    }
    eval_entry_t *transformed = &stack->items[record->pipeline.stage_slot];
    record->reason = wl_evaluation_control_charge(record->control, 64);
    if (record->reason) goto failed;
    if (!transformed->rel || !transformed->owned
        || transformed->rel->ncols != record->output->rel->ncols
        || transformed->rel->nrows > batch->rows
        || (uint64_t)record->pipeline.pending_begin + transformed->rel->nrows
        > record->output->rel->capacity){
        record->reason = EINVAL; goto failed;
    }
    record->reason = wl_evaluation_control_charge(record->control, 0);
    if (record->reason) goto failed;
    record->pipeline.stage = WL_COLUMNAR_JOIN_PIPELINE_STAGE_APPEND;
    if (transformed->rel->nrows) {
        record->reason = wl_columnar_relation_bulk_append_controlled(bulk,
                record->output->rel, transformed->rel, record->control);
        record->committed = bulk->published;
    } else {
        /* A real checked epoch publishes the raw empty-batch view change.
         * Its cutoff consumes this input batch even when FILTER rejects all. */
        wl_columnar_relation_private_batch_t *writer =
            wl_columnar_eval_stack_cleanup_pipeline_private_batch(frame);
        if (!writer) {
            record->reason = EINVAL; goto failed;
        }
        record->reason =
            wl_columnar_relation_private_append_batch_begin_controlled(writer,
                record->output->rel, record->control);
        if (!record->reason) record->reason =
                wl_columnar_relation_private_touch_view_controlled(writer,
                    record->control, &record->committed);
        record->pipeline.published = record->committed;
        int cleanup = wl_columnar_relation_private_batch_retire(writer, 0);
        if (record->reason <= 0 && cleanup) record->reason = cleanup;
    }
    record->pipeline.published = record->committed;
    return record->committed || !record->reason
        ? WL_COLUMNAR_CONTINUATION_OK : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
failed:
    return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
}

static wl_columnar_continuation_status_t
wl_columnar_join_pipeline_sink_commit_controlled(void *context,
    bool *committed)
{
    wl_columnar_join_batch_record_t *record = context;
    *committed = record->committed;
    if (*committed)
        record->pipeline.stage = WL_COLUMNAR_JOIN_PIPELINE_STAGE_COMMITTED;
    return record->reason ? WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE
        : WL_COLUMNAR_CONTINUATION_OK;
}

static void
wl_columnar_join_pipeline_sink_abort_controlled(void *context)
{
    wl_columnar_join_batch_record_t *record = context;
    int cleanup = wl_columnar_join_pipeline_stage_retire(record, 0);
    if (record->reason <= 0 && cleanup) record->reason = cleanup;
}

int
wl_columnar_join_pipeline_publish_controlled(
    wl_columnar_join_batch_record_t *record)
{
    if (!record || !record->ready || !record->producer || !record->continuation
        || record->phase != WL_COLUMNAR_JOIN_BATCH_PIPELINE
        || !record->pipeline.ready) return EINVAL;
    if (record->pipeline.scratch_moved || record->pipeline.begun) {
        int cleanup = wl_columnar_join_pipeline_finish_stage(record, true);
        if (cleanup) return cleanup;
    }
    record->reason = 0;
    wl_columnar_continuation_sink_t sink = {record,
                                            wl_columnar_join_pipeline_sink_begin_controlled,
                                            wl_columnar_join_pipeline_sink_reserve_controlled,
                                            wl_columnar_join_pipeline_sink_append_controlled,
                                            wl_columnar_join_pipeline_sink_commit_controlled,
                                            wl_columnar_join_pipeline_sink_abort_controlled};
    wl_columnar_continuation_status_t status =
        wl_columnar_continuation_publish(record->continuation, &sink);
    if (record->committed && col_join_output_limit_reached(record->session,
        record->output->rel)) return EOVERFLOW;
    if (record->reason) return record->reason;
    if (status != WL_COLUMNAR_CONTINUATION_OK
        && status != WL_COLUMNAR_CONTINUATION_DONE)
        return status == WL_COLUMNAR_CONTINUATION_STALE ? EBUSY : EINVAL;
    if (record->pipeline.scratch_moved) {
        int cleanup = wl_columnar_join_pipeline_finish_stage(record, false);
        if (cleanup) return cleanup;
    }
    wl_columnar_join_batch_checkpoint(record, 15,
        wl_columnar_continuation_cursor(record->continuation)->sequence);
    return wl_evaluation_control_charge(record->control, 0);
}

int
wl_columnar_join_pipeline_try_eval_controlled(const wl_plan_relation_t *plan,
    uint32_t join_index, eval_stack_t *stack, wl_col_session_t *session,
    uint32_t *next_index, wl_evaluation_control_t *control)
{
    if (next_index) *next_index = join_index;
    uint32_t map_index = UINT32_MAX;
    wl_columnar_join_pipeline_eligibility_t eligibility;
    int rc = wl_columnar_join_pipeline_preflight_controlled(plan, join_index,
            session, &map_index, &eligibility, control);
    if (rc || eligibility != WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE) return rc;
    if (!stack || !stack->top || stack->top > COL_STACK_MAX - 3u
        || stack->items[stack->top - 1u].kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || !stack->items[stack->top - 1u].rel) return EINVAL;
    const wl_plan_op_t *join = &plan->ops[join_index];
    const wl_plan_op_t *map = &plan->ops[map_index];
    uint32_t left_slot = stack->top - 1u, output_slot = stack->top;
    uint32_t scratch_slot = output_slot + 1u, stage_slot = output_slot + 2u;
    col_rel_t *left = stack->items[left_slot].rel, *right = NULL;
    wl_columnar_relation_source_group_t *sources = NULL;
    eval_entry_t *result = NULL;
    wl_columnar_relation_private_batch_t *writer = NULL;
    rc = wl_columnar_eval_stack_cleanup_join_owner(session, stack, &sources,
            &result);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_private_batch_owner(session, stack,
            &writer, control);
    if (rc) return rc;
    rc = wl_columnar_join_controlled_lookup(session, join->right_relation,
            &right, control);
    if (rc) return rc;
    if (!right) return ENOENT;
    rc = wl_columnar_relation_source_group_acquire_controlled(sources, left,
            right, control);
    if (rc) return rc;
    rc = wl_columnar_join_controlled_shape(left, control);
    if (!rc) rc = wl_columnar_join_controlled_shape(right, control);
    if (rc) return rc;
    uint64_t width = join->project_count ? join->project_count
        : (uint64_t)left->ncols + right->ncols;
    if (width > UINT32_MAX) return EOVERFLOW;
    wl_columnar_join_batch_record_t *record = NULL;
    rc = wl_columnar_eval_stack_cleanup_join_batch_owner(session, stack,
            &record, control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) return rc;
    stack->items[output_slot] = (eval_entry_t){0};
    stack->items[scratch_slot] = (eval_entry_t){0};
    stack->items[stage_slot] = (eval_entry_t){0};
    stack->top = stage_slot + 1u;
    record->output = &stack->items[output_slot];
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    uint64_t type_bytes;
    if (!wl_columnar_memory_size_mul(width, sizeof(*record->types), &type_bytes)
        || type_bytes > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    record->types = type_bytes ? malloc((size_t)type_bytes) : NULL;
    if (type_bytes && !record->types) return ENOMEM;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    for (uint32_t column = 0; column < (uint32_t)width; column++) {
        rc = wl_evaluation_control_charge(control, 2);
        if (rc) return rc;
        uint32_t index = join->project_count && join->project_indices
            ? join->project_indices[column] : column;
        record->types[column] = index < left->ncols
            ? (left->column_types ? left->column_types[index] :
            WIRELOG_TYPE_INT64)
            : index - left->ncols < right->ncols
            ? (right->column_types ? right->column_types[index - left->ncols]
                : WIRELOG_TYPE_INT64) : WIRELOG_TYPE_INT64;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    rc = wl_columnar_join_batch_create_pipeline_controlled(record, join, left,
            right, (uint32_t)width);
    if (rc == ENOTSUP && !session->join_batch_strict && record->producer) {
        /* Only a genuinely drained creator exclusion may return to the
         * ordinary bounded JOIN followed by separate FILTER/MAP operators. */
        int cleanup = wl_columnar_join_batch_record_retire(record, 0);
        if (cleanup) return rc;
        cleanup = wl_columnar_relation_source_group_drain(sources, 0);
        if (cleanup) return rc;
        cleanup = eval_entry_dispose(result);
        if (cleanup) return rc;
        stack->top = left_slot + 1u;
        return 0;
    }
    if (rc) return rc;
    rc = wl_columnar_relation_new_auto_governed_controlled("$join_pipeline",
            map->project_count, COL_REL_INIT_CAP, false,
            session->memory_governor,
            &record->output->rel, control);
    if (record->output->rel) {
        record->output->owned = true;
        record->output->kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    }
    if (rc) return rc;
    record->phase = WL_COLUMNAR_JOIN_BATCH_PIPELINE_INITIALIZING;
    for (size_t at = 0; at < sizeof(record->pipeline);) {
        size_t count = sizeof(record->pipeline) - at;
        if (count > 128u) count = 128u;
        rc = wl_evaluation_control_charge(control, count);
        if (rc) return rc;
        memset((char *)&record->pipeline + at, 0, count); at += count;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 64);
    if (rc) return rc;
    record->pipeline.plan = plan;
    record->pipeline.first_filter = join_index + 1u;
    record->pipeline.map_index = map_index;
    record->pipeline.left_slot = left_slot;
    record->pipeline.scratch_slot = scratch_slot;
    record->pipeline.stage_slot = stage_slot;
    record->pipeline.ready = true;
    record->phase = WL_COLUMNAR_JOIN_BATCH_PIPELINE;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    while (!wl_columnar_continuation_is_done(record->continuation)) {
        rc = wl_columnar_join_pipeline_publish_controlled(record);
        if (rc) return rc;
    }
    rc = wl_columnar_join_batch_record_retire(record, 0);
    if (rc) return rc;
    rc = wl_columnar_relation_source_group_release_readers(sources, 0);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 96);
    if (!rc) rc = wl_columnar_relation_source_group_validate_writers(sources);
    if (!rc) rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    wl_columnar_relation_source_group_commit(sources);
    rc = wl_columnar_relation_source_group_drain(sources, 0);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 96);
    if (!rc) rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    rc = eval_entry_dispose(result);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    rc = wl_columnar_relation_private_append_batch_begin_controlled(writer,
            stack->items[output_slot].rel, control);
    if (!rc) rc =
            wl_columnar_relation_private_append_batch_end_controlled(writer);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 96);
    if (!rc) rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    rc = eval_entry_dispose(&stack->items[left_slot]);
    if (rc) return rc;
    stack->items[left_slot] = stack->items[output_slot];
    stack->items[output_slot] = (eval_entry_t){0};
    stack->top = left_slot + 1u;
    if (next_index) *next_index = map_index;
    return 1;
}

int
wl_columnar_join_pipeline_try_eval(const wl_plan_relation_t *plan,
    uint32_t join_index, eval_stack_t *stack, wl_col_session_t *sess,
    uint32_t *next_index)
{
    wl_columnar_join_pipeline_eligibility_t eligibility;
    const wl_plan_op_t *join;
    const wl_plan_op_t *map;
    eval_entry_t left_entry;
    col_rel_t *right;
    col_rel_t *out = NULL;
    wl_columnar_continuation_t *continuation = NULL;
    wl_join_pipeline_sink_t sink_ctx;
    wl_columnar_continuation_sink_t sink;
    uint32_t map_index = UINT32_MAX;
    uint32_t *lk = NULL;
    uint32_t *rk = NULL;
    int rc;

    if (next_index)
        *next_index = join_index;
    eligibility = wl_columnar_join_pipeline_preflight(plan, join_index,
            sess, &map_index);
    if (eligibility != WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE)
        return 0;
    if (!stack || stack->top == 0 || !sess)
        return EINVAL;
    join = &plan->ops[join_index];
    map = &plan->ops[map_index];
    if (stack->items[stack->top - 1].kind
        != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || !stack->items[stack->top - 1].rel)
        return EINVAL;
    right = session_find_rel(sess, join->right_relation);
    if (!right)
        return ENOENT;

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
            rc = pipeline_resolve_key(right,
                    join->right_keys ? join->right_keys[i] : NULL, &rk[i]);
        if (rc != 0) {
            free(lk);
            free(rk);
            return ENOTSUP;
        }
    }

    rc = col_join_batch_producer_create(sess, join,
            stack->items[stack->top - 1].rel,
            stack->items[stack->top - 1].is_delta, lk, rk, join->key_count,
            sess->join_batch_bytes, &continuation);
    free(lk);
    free(rk);
    if (rc != 0)
        return rc;

    out = col_rel_new_auto("$join_pipeline", map->project_count);
    if (!out) {
        wl_columnar_continuation_cancel(continuation);
        wl_columnar_continuation_destroy(continuation);
        return ENOMEM;
    }
    if (sess->memory_governor) {
        rc = col_rel_attach_memory_governor(out, sess->memory_governor);
        if (rc == 0)
            rc = col_rel_reserve_capacity_admitted(out, out->capacity, NULL);
        if (rc != 0) {
            col_rel_destroy(out);
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            return rc;
        }
    }

    left_entry = eval_stack_pop(stack);
    memset(&sink_ctx, 0, sizeof(sink_ctx));
    sink_ctx.sess = sess;
    sink_ctx.plan = plan;
    sink_ctx.first_filter = join_index + 1u;
    sink_ctx.map_index = map_index;
    sink_ctx.out = out;
    memset(&sink, 0, sizeof(sink));
    sink.context = &sink_ctx;
    sink.begin = pipeline_sink_begin;
    sink.reserve = pipeline_sink_reserve;
    sink.append = pipeline_sink_append;
    sink.commit = pipeline_sink_commit;
    sink.abort = pipeline_sink_abort;

    for (;;) {
        wl_columnar_continuation_status_t status
            = wl_columnar_continuation_publish(continuation, &sink);
        if (status == WL_COLUMNAR_CONTINUATION_DONE)
            break;
        if (status != WL_COLUMNAR_CONTINUATION_OK) {
            pipeline_sink_abort(&sink_ctx);
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            col_rel_destroy(out);
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
        if (col_join_output_limit_reached(sess, out)) {
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            col_rel_destroy(out);
            if (eval_stack_repush_entry(stack, &left_entry) != 0)
                return EFAULT;
            return EOVERFLOW;
        }
    }
    wl_columnar_continuation_destroy(continuation);
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
        *next_index = map_index;
    return 1;
}
