#include "columnar/join_pipeline.h"

#include <stddef.h>
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
