/* Conservative pipeline preflight for Issue #1475. */
#include "../wirelog/columnar/join_pipeline.h"

#include <stdio.h>
#include <string.h>

static int failures;

#define CHECK(condition, message) do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL: %s\n", (message)); \
                failures++; \
            } \
} while (0)

static void
test_projection_pipeline(void)
{
    static const char *const keys[] = { "col0" };
    static const uint32_t project[] = { 0, 2 };
    static uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_JOIN, .right_relation = "right",
          .left_keys = keys, .right_keys = keys, .key_count = 1 },
        { .op = WL_PLAN_OP_FILTER,
          .filter_expr = { predicate, sizeof(predicate) } },
        { .op = WL_PLAN_OP_FILTER,
          .filter_expr = { predicate, sizeof(predicate) } },
        { .op = WL_PLAN_OP_MAP, .project_indices = project,
          .project_count = 2 },
        { .op = WL_PLAN_OP_CONSOLIDATE },
    };
    wl_plan_relation_t plan = { .ops = ops,
                                .op_count = sizeof(ops) / sizeof(ops[0]) };
    wl_col_session_t sess;
    uint32_t map_index = UINT32_MAX;

    memset(&sess, 0, sizeof(sess));
    sess.join_batch_bytes = 4096;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, &map_index)
        == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE,
        "JOIN -> FILTER* -> projection MAP is eligible");
    CHECK(map_index == 3, "preflight returns the consuming MAP index");
}

static void
test_conservative_exclusions(void)
{
    static const char *const keys[] = { "col0" };
    static const uint32_t project[] = { 0 };
    static uint8_t expr_data[] = { WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0,
                                   0, 0, 0, 0 };
    static uint8_t extension_filter[] = {
        WL_PLAN_EXPR_EXTENSION_CALL, 1, 0, 'f', 0, 0, 0, 0
    };
    static uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    static uint8_t valid_cmp[] = {
        WL_PLAN_EXPR_VAR, 1, 0, 'x',
        WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_EQ,
    };
    static uint8_t truncated_var[] = { WL_PLAN_EXPR_VAR, 2, 0, 'x' };
    static uint8_t underflow[] = { WL_PLAN_EXPR_CMP_EQ };
    static uint8_t residual[] = { WL_PLAN_EXPR_BOOL, 1, WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_expr_buffer_t exprs[] = {
        { expr_data, sizeof(expr_data) },
    };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_JOIN, .right_relation = "right",
          .left_keys = keys, .right_keys = keys, .key_count = 1 },
        { .op = WL_PLAN_OP_MAP, .project_indices = project,
          .project_count = 1 },
        { .op = WL_PLAN_OP_MAP, .project_indices = project,
          .project_count = 1 },
    };
    wl_plan_relation_t plan = { .ops = ops, .op_count = 2 };
    wl_col_session_t sess;

    memset(&sess, 0, sizeof(sess));
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF, "disabled knob is excluded");
    sess.join_batch_bytes = 4096;
    ops[1].map_exprs = exprs;
    ops[1].map_expr_count = 1;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
        "expression MAP is excluded until transactional consumption exists");
    ops[1].map_exprs = NULL;
    ops[1].map_expr_count = 0;
    ops[1].op = WL_PLAN_OP_FILTER;
    ops[1].filter_expr.data = valid_cmp;
    ops[1].filter_expr.size = sizeof(valid_cmp);
    ops[1].materialized = false;
    plan.op_count = 2;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE,
        "FILTER without MAP is excluded");
    ops[1].op = WL_PLAN_OP_MAP;
    ops[1].filter_expr.data = NULL;
    ops[1].filter_expr.size = 0;
    ops[1] = (wl_plan_op_t){ .op = WL_PLAN_OP_FILTER,
                             .filter_expr = { valid_cmp, sizeof(valid_cmp) } };
    ops[2] = (wl_plan_op_t){ .op = WL_PLAN_OP_MAP,
                             .project_indices = project, .project_count = 1 };
    plan.op_count = 3;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE,
        "numeric VAR/comparison FILTER is eligible");
    ops[0].materialized = false;
    ops[1].filter_expr = (wl_plan_expr_buffer_t){
        truncated_var, sizeof(truncated_var)
    };
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER,
        "truncated VAR payload is excluded");
    ops[1].filter_expr = (wl_plan_expr_buffer_t){
        underflow, sizeof(underflow)
    };
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER,
        "comparison stack underflow is excluded");
    ops[1].filter_expr = (wl_plan_expr_buffer_t){
        residual, sizeof(residual)
    };
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER,
        "residual expression value is excluded");
    ops[1].filter_expr = (wl_plan_expr_buffer_t){ predicate,
                                                  sizeof(predicate) };
    ops[1].materialized = true;
    uint32_t rejected_index = 7;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, &rejected_index)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER
        && rejected_index == UINT32_MAX,
        "rejected preflight clears map index");
    ops[1].materialized = false;
    sess.coordinator = (wl_col_session_t *)1;
    rejected_index = 7;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, &rejected_index)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION
        && rejected_index == UINT32_MAX,
        "worker/coordinator session is excluded and clears index");
    sess.coordinator = NULL;

    plan.op_count = 2;
    sess.diff_operators_active = true;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION,
        "differential execution is excluded");
    sess.diff_operators_active = false;
    ops[1].op = WL_PLAN_OP_MAP;
    ops[1].filter_expr.data = NULL;
    ops[1].filter_expr.size = 0;
    ops[1].map_exprs = exprs;
    ops[1].map_expr_count = 1;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
        "expression MAP is excluded until transactional consumption exists");
    ops[1].map_exprs = NULL;
    ops[1].map_expr_count = 0;
    ops[1].op = WL_PLAN_OP_FILTER;
    ops[1].filter_expr.data = extension_filter;
    ops[1].filter_expr.size = sizeof(extension_filter);
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER,
        "extension FILTER is excluded before continuation activation");
    ops[1].op = WL_PLAN_OP_MAP;
    ops[1].filter_expr.data = NULL;
    ops[1].filter_expr.size = 0;
    ops[0].materialized = true;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN,
        "materialization-cache insertion is excluded");
    ops[0].materialized = false;
    ops[0].key_count = 0;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN, "cross join is excluded");
    ops[0].key_count = 1;
    ops[1].op = WL_PLAN_OP_FILTER;
    ops[1].filter_expr.data = predicate;
    ops[1].filter_expr.size = sizeof(predicate);
    ops[1].materialized = true;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER,
        "materialized FILTER is excluded");
    ops[1].materialized = false;
    ops[1].op = WL_PLAN_OP_REDUCE;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE,
        "unsupported downstream operator is excluded");

    CHECK(strcmp(wl_columnar_join_pipeline_eligibility_name(
            WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE), "eligible") == 0,
        "eligible reason has a stable diagnostic name");
    CHECK(strcmp(wl_columnar_join_pipeline_eligibility_name(
            WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP), "map-not-row-local") == 0,
        "MAP exclusion has a stable diagnostic name");
    CHECK(strcmp(wl_columnar_join_pipeline_eligibility_name(
            WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER),
        "filter-not-row-local") == 0,
        "FILTER exclusion has a stable diagnostic name");

    plan.ops = NULL;
    plan.op_count = 1;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE,
        "missing plan storage is excluded safely");
}

int
main(void)
{
    test_projection_pipeline();
    test_conservative_exclusions();
    if (failures)
        fprintf(stderr, "%d join pipeline preflight checks failed\n",
            failures);
    return failures ? 1 : 0;
}
