#include "columnar/join_pipeline.h"
#include "columnar/join_batch.h"

#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static bool
wl_columnar_join_pipeline_filter_is_row_local(
    const wl_plan_expr_buffer_t *expr)
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
        default:
            /* Strings, arithmetic, aggregates, extensions and unknown tags
             * stay on the materialized path until their batch contract is
             * specified. */
            return false;
        }
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
        if (!wl_columnar_join_pipeline_filter_is_row_local(
                &plan->ops[i].filter_expr))
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER;
        i++;
    }
    if (i >= plan->op_count || plan->ops[i].op != WL_PLAN_OP_MAP)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;

    map = &plan->ops[i];
    if (map->map_expr_count != 0 || map->map_exprs != NULL
        || map->project_count == 0 || !map->project_indices
        || map->materialized)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP;

    if (map_index)
        *map_index = i;
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

typedef struct {
    wl_col_session_t *sess;
    const wl_plan_relation_t *plan;
    uint32_t first_filter;
    uint32_t map_index;
    col_rel_t *out;
    uint32_t pending_begin;
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
    uint64_t need;
    int rc;

    (void)bytes;
    if (!sink || !sink->begun || !sink->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    need = (uint64_t)sink->out->nrows + rows;
    if (need > UINT32_MAX)
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    rc = col_rel_reserve_capacity_admitted(sink->out, (uint32_t)need,
            NULL);
    if (rc == 0)
        return WL_COLUMNAR_CONTINUATION_OK;
    return rc == ENOMEM ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
                        : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
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
    if (rc != 0)
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    for (uint32_t i = sink->first_filter; i < sink->map_index; i++) {
        rc = wl_columnar_filter_op(&sink->plan->ops[i], &batch_stack,
                sink->sess);
        if (rc != 0) {
            (void)eval_stack_drain(&batch_stack);
            return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
        }
    }
    rc = col_op_map(&sink->plan->ops[sink->map_index], &batch_stack,
            sink->sess);
    if (rc != 0) {
        (void)eval_stack_drain(&batch_stack);
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
            return status == WL_COLUMNAR_CONTINUATION_STALE ? EAGAIN : ENOMEM;
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
