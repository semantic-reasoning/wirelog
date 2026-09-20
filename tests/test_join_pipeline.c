/* Conservative pipeline preflight for Issue #1475. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/session.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

#define CHECK(condition, message) do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL: %s\n", (message)); \
                failures++; \
            } \
} while (0)

static wl_col_session_t *
pipeline_session(void)
{
    wl_col_session_t *sess = (wl_col_session_t *)calloc(1, sizeof(*sess));
    if (!sess)
        return NULL;
    sess->frontier_ops = &col_frontier_epoch_ops;
    sess->delta_pool = delta_pool_create(64, sizeof(col_rel_t), 1u << 20);
    wl_mem_ledger_init(&sess->mem_ledger, 0);
    if (!sess->delta_pool) {
        free(sess);
        return NULL;
    }
    sess->join_batch_bytes = 7u * 32u;
    return sess;
}

static void
pipeline_session_destroy(wl_col_session_t *sess)
{
    if (!sess)
        return;
    wl_workqueue_destroy(sess->wq);
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free(sess->rels);
    for (uint32_t i = 0; i < sess->arr_count; i++) {
        free(sess->arr_entries[i].rel_name);
        free(sess->arr_entries[i].key_cols);
        arr_free_contents(&sess->arr_entries[i].arr);
    }
    free(sess->arr_entries);
    session_rel_free_hash(sess);
    delta_pool_destroy(sess->delta_pool);
    free(sess);
}

static col_rel_t *
pipeline_relation(const char *name, const char *const *columns,
    uint32_t ncols, const int64_t *rows, uint32_t nrows)
{
    col_rel_t *rel = col_rel_new_auto(name, ncols);
    if (!rel || col_rel_set_schema(rel, ncols, columns) != 0) {
        col_rel_destroy(rel);
        return NULL;
    }
    for (uint32_t i = 0; i < nrows; i++) {
        if (col_rel_append_row(rel, rows + i * ncols) != 0) {
            col_rel_destroy(rel);
            return NULL;
        }
    }
    return rel;
}

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

static void
test_pipeline_consumes_batches(void)
{
    static const char *const left_names[] = { "k", "v" };
    static const char *const right_names[] = { "k", "r" };
    static const char *const keys[] = { "k" };
    static const uint32_t project[] = { 1, 3 };
    static uint8_t filter[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '3',
        WL_PLAN_EXPR_CONST_INT, 0xe9, 0x03, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_GTE,
    };
    static const int64_t left_rows[] = { 0, 10, 1, 20, 2, 30 };
    static const int64_t right_rows[] = {
        0, 0, 0, 1, 0, 2, 0, 3,
        1, 1000, 1, 1001, 1, 1002, 1, 1003,
        2, 2000, 2, 2001, 2, 2002, 2, 2003,
    };
    wl_col_session_t *sess = NULL;
    col_rel_t *left = NULL;
    col_rel_t *right = NULL;
    eval_stack_t stack;
    eval_entry_t result;
    wl_plan_op_t ops[3];
    wl_plan_relation_t plan;
    int rc;

    memset(ops, 0, sizeof(ops));
    ops[0].op = WL_PLAN_OP_JOIN;
    ops[0].right_relation = "right";
    ops[0].left_keys = keys;
    ops[0].right_keys = keys;
    ops[0].key_count = 1;
    ops[1].op = WL_PLAN_OP_FILTER;
    ops[1].filter_expr.data = filter;
    ops[1].filter_expr.size = sizeof(filter);
    ops[2].op = WL_PLAN_OP_MAP;
    ops[2].project_indices = project;
    ops[2].project_count = 2;
    plan.ops = ops;
    plan.op_count = 3;

    sess = pipeline_session();
    left = pipeline_relation("left", left_names, 2, left_rows, 3);
    right = pipeline_relation("right", right_names, 2, right_rows, 12);
    if (!sess || !left || !right) {
        CHECK(false, "pipeline fixture allocates");
        goto cleanup;
    }
    if (session_add_rel(sess, right) != 0) {
        CHECK(false, "pipeline right relation registration");
        right = NULL;
        goto cleanup;
    }
    right = NULL; /* session owns it */
    CHECK(col_session_get_arrangement(&sess->base, "right",
        (const uint32_t[]){ 0 }, 1) != NULL,
        "pipeline builds the persistent right arrangement");
    eval_stack_init(&stack);
    CHECK(eval_stack_push(&stack, left, true) == 0,
        "pipeline pushes owned left relation");
    left = NULL;
    rc = col_eval_relation_plan(&plan, &stack, sess);
    CHECK(rc == 0, "pipeline consumes the bounded continuation");
    result = eval_stack_pop(&stack);
    CHECK(result.rel && result.rel->ncols == 2 && result.rel->nrows == 7,
        "pipeline output contains only filtered projected rows");
    if (result.rel && result.rel->nrows == 7) {
        CHECK(result.rel->columns[0][0] == 20
            && result.rel->columns[1][0] == 1003,
            "pipeline preserves the first projected row");
        CHECK(result.rel->columns[0][6] == 30
            && result.rel->columns[1][6] == 2000,
            "pipeline preserves the last projected row");
    }
    (void)eval_entry_dispose(&result);
    (void)eval_stack_drain(&stack);

    {
        col_rel_t *limited_left = pipeline_relation("left", left_names, 2,
                left_rows, 3);
        eval_stack_t limited_stack;
        sess->join_output_limit = 5;
        eval_stack_init(&limited_stack);
        rc = limited_left ? eval_stack_push(&limited_stack, limited_left,
                true) : ENOMEM;
        CHECK(rc == 0, "pipeline limit fixture");
        if (rc == 0) {
            limited_left = NULL;
            rc = col_eval_relation_plan(&plan, &limited_stack, sess);
            CHECK(rc == EOVERFLOW && limited_stack.top == 1,
                "pipeline enforces the universal join output limit");
            (void)eval_stack_drain(&limited_stack);
        }
        if (limited_left)
            col_rel_destroy(limited_left);
    }

    {
        wl_plan_relation_t plain_plan = { .ops = &ops[0], .op_count = 1 };
        col_rel_t *plain_left = pipeline_relation("left", left_names, 2,
                left_rows, 3);
        eval_stack_t plain_stack;
        sess->join_output_limit = 0;
        sess->join_batch_strict = true;
        eval_stack_init(&plain_stack);
        rc = plain_left ? eval_stack_push(&plain_stack, plain_left, true)
                        : ENOMEM;
        CHECK(rc == 0, "plain JOIN strict fixture");
        if (rc == 0) {
            plain_left = NULL;
            rc = col_eval_relation_plan(&plain_plan, &plain_stack, sess);
            CHECK(rc == 0 && plain_stack.top == 1
                && plain_stack.items[0].rel
                && plain_stack.items[0].rel->nrows == 12,
                "strict bounded plain JOIN keeps the existing sink path");
            (void)eval_stack_drain(&plain_stack);
        }
        if (plain_left)
            col_rel_destroy(plain_left);
        sess->join_batch_strict = false;
    }

cleanup:
    if (left)
        col_rel_destroy(left);
    if (right)
        col_rel_destroy(right);
    pipeline_session_destroy(sess);
}

int
main(void)
{
    test_projection_pipeline();
    test_conservative_exclusions();
    test_pipeline_consumes_batches();
    if (failures)
        fprintf(stderr, "%d join pipeline preflight checks failed\n",
            failures);
    return failures ? 1 : 0;
}
