/* Conservative pipeline preflight for Issue #1475; expression MAPs #1777. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/session.h"

#include <errno.h>
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
        col_rel_destroy(sess->rels[i]);
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
    /* A string literal consults the session intern table, so this MAP
     * stays excluded even though numeric expression MAPs are admitted. */
    static uint8_t expr_data[] = { WL_PLAN_EXPR_CONST_STR, 1, 0, 's' };
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
        "intern-dependent string MAP is excluded");
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
        "intern-dependent string MAP is excluded");
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

/* ---- Issue #1777: row-local expression MAPs ---------------------------- */

#define PL_VAR(digit) WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', (digit)
#define PL_INT(byte) WL_PLAN_EXPR_CONST_INT, (byte), 0, 0, 0, 0, 0, 0, 0

static void
test_expression_map_preflight(void)
{
    static const char *const keys[] = { "col0" };
    static const uint32_t project[] = { 0, 1, 2 };
    static uint8_t sum[] = { PL_VAR('1'), PL_VAR('3'), WL_PLAN_EXPR_ARITH_ADD };
    static uint8_t bnot[] = { PL_VAR('0'), WL_PLAN_EXPR_ARITH_BNOT };
    static uint8_t float_div[] = {
        WL_PLAN_EXPR_VAR_FLOAT, 4, 0, 'c', 'o', 'l', '1',
        WL_PLAN_EXPR_CONST_FLOAT, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f,
        WL_PLAN_EXPR_ARITH_FLOAT_DIV,
    };
    static uint8_t cmp[] = { PL_VAR('1'), PL_INT(2), WL_PLAN_EXPR_CMP_LT };
    static uint8_t var_string[] = {
        WL_PLAN_EXPR_VAR_STRING, 4, 0, 'c', 'o', 'l', '1'
    };
    static uint8_t extension[] = {
        WL_PLAN_EXPR_EXTENSION_CALL, 1, 0, 'f', 0, 0, 0, 0
    };
    static uint8_t hash[] = { PL_VAR('1'), WL_PLAN_EXPR_ARITH_HASH };
    static uint8_t uuid4[] = { WL_PLAN_EXPR_ARITH_UUID4 };
    /* Leading operator: only its own depth guard rejects this, since the
     * two trailing variables would otherwise end at depth one. */
    static uint8_t add_underflow[] = {
        WL_PLAN_EXPR_ARITH_ADD, PL_VAR('1'), PL_VAR('3')
    };
    /* The trailing literal restores the final depth to one, so only the
     * unary operator's own operand check can reject this. */
    static uint8_t bnot_underflow[] = { WL_PLAN_EXPR_ARITH_BNOT, PL_INT(1) };
    static uint8_t residual[] = { PL_INT(1), PL_INT(2) };
    static uint8_t truncated[] = { WL_PLAN_EXPR_CONST_INT, 1, 0, 0 };
    static uint8_t arith_filter[] = {
        PL_VAR('1'), PL_INT(1), WL_PLAN_EXPR_ARITH_ADD, PL_INT(2),
        WL_PLAN_EXPR_CMP_GT,
    };
    static const struct {
        uint8_t *data;
        uint32_t size;
        wl_columnar_join_pipeline_eligibility_t expected;
        const char *message;
    } cases[] = {
        { sum, sizeof(sum), WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE,
          "integer arithmetic MAP is eligible" },
        { bnot, sizeof(bnot), WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE,
          "unary integer MAP is eligible" },
        { float_div, sizeof(float_div), WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE,
          "float arithmetic MAP is eligible" },
        { cmp, sizeof(cmp), WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE,
          "comparison-valued MAP is eligible" },
        { var_string, sizeof(var_string),
          WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "string variable MAP is excluded" },
        { extension, sizeof(extension), WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "extension MAP is excluded" },
        { hash, sizeof(hash), WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "digest MAP is excluded" },
        { uuid4, sizeof(uuid4), WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "non-deterministic MAP is excluded" },
        { add_underflow, sizeof(add_underflow),
          WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "binary operator stack underflow is excluded" },
        { bnot_underflow, sizeof(bnot_underflow),
          WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "unary operator stack underflow is excluded" },
        { residual, sizeof(residual), WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "residual MAP value is excluded" },
        { truncated, sizeof(truncated), WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
          "truncated MAP literal is excluded" },
    };
    wl_plan_expr_buffer_t exprs[2];
    wl_plan_op_t ops[2];
    wl_plan_relation_t plan = { .ops = ops, .op_count = 2 };
    wl_col_session_t sess;
    uint32_t map_index;

    memset(&sess, 0, sizeof(sess));
    sess.join_batch_bytes = 4096;
    memset(ops, 0, sizeof(ops));
    ops[0] = (wl_plan_op_t){ .op = WL_PLAN_OP_JOIN, .right_relation = "right",
                             .left_keys = keys, .right_keys = keys,
                             .key_count = 1 };
    ops[1] = (wl_plan_op_t){ .op = WL_PLAN_OP_MAP, .map_exprs = exprs,
                             .map_expr_count = 1, .project_count = 3 };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        exprs[0] = (wl_plan_expr_buffer_t){ cases[i].data, cases[i].size };
        map_index = 7;
        CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, &map_index)
            == cases[i].expected
            && map_index == (cases[i].expected
            == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE ? 1u : UINT32_MAX),
            cases[i].message);
    }

    /* Columns without an expression fall back to projection, so a mixed
     * MAP is judged by its non-empty expressions only. */
    exprs[0] = (wl_plan_expr_buffer_t){ sum, sizeof(sum) };
    exprs[1] = (wl_plan_expr_buffer_t){ NULL, 0 };
    ops[1].map_expr_count = 2;
    ops[1].project_indices = project;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE,
        "mixed expression/projection MAP is eligible");
    exprs[1] = (wl_plan_expr_buffer_t){ hash, sizeof(hash) };
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
        "one excluded expression excludes the whole MAP");
    ops[1].map_expr_count = 1;
    ops[1].map_exprs = NULL;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
        "expression count without storage is excluded");
    ops[1].map_exprs = exprs;
    ops[1].project_count = 0;
    CHECK(wl_columnar_join_pipeline_preflight(&plan, 0, &sess, NULL)
        == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
        "zero-width expression MAP is excluded");

    /* Admitting arithmetic in MAP must not widen the FILTER contract. */
    {
        wl_plan_op_t filtered[3];
        wl_plan_relation_t filtered_plan = { .ops = filtered, .op_count = 3 };
        exprs[0] = (wl_plan_expr_buffer_t){ sum, sizeof(sum) };
        filtered[0] = ops[0];
        filtered[1] = (wl_plan_op_t){ .op = WL_PLAN_OP_FILTER,
                                      .filter_expr = { arith_filter,
                                                       sizeof(arith_filter) } };
        filtered[2] = (wl_plan_op_t){ .op = WL_PLAN_OP_MAP, .map_exprs = exprs,
                                      .map_expr_count = 1,
                                      .project_count = 3 };
        CHECK(wl_columnar_join_pipeline_preflight(&filtered_plan, 0, &sess,
            NULL) == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER,
            "arithmetic FILTER stays excluded");
    }
}

#define PL_KEYS 4u
#define PL_FANOUT 64u
#define PL_JOIN_ROWS (PL_KEYS * PL_FANOUT)
/* join_batch_bytes = 7 * 32 bytes over four int64 join columns. */
#define PL_BATCH_ROWS 7u
/* FILTER col3 >= 1002 drops key 0 and the first two rows of key 1. */
#define PL_FILTERED_ROWS (PL_JOIN_ROWS - PL_FANOUT - 2u)

static uint32_t pl_map_calls;
static uint32_t pl_map_max_rows;
static uint32_t pl_fail_map_call;
static col_rel_t *pl_touch_on_map;

static void
pl_map_observer(eval_stack_t *stack, eval_entry_t *entry)
{
    (void)stack;
    pl_map_calls++;
    if (entry && entry->rel && entry->rel->nrows > pl_map_max_rows)
        pl_map_max_rows = entry->rel->nrows;
    if (pl_fail_map_call != 0 && pl_map_calls == pl_fail_map_call)
        wl_columnar_ops_test_map_fail_output_alloc = true;
    if (pl_touch_on_map) {
        wl_columnar_relation_touch_view(pl_touch_on_map);
        pl_touch_on_map = NULL;
    }
}

static void
pl_observe_reset(void)
{
    pl_map_calls = 0;
    pl_map_max_rows = 0;
    pl_fail_map_call = 0;
    pl_touch_on_map = NULL;
    wl_columnar_ops_test_map_fail_output_alloc = false;
}

static wl_col_session_t *
expr_pipeline_session(void)
{
    static const char *const right_names[] = { "k", "r" };
    int64_t rows[PL_JOIN_ROWS * 2u];
    wl_col_session_t *sess;
    col_rel_t *right;

    for (uint32_t k = 0; k < PL_KEYS; k++) {
        for (uint32_t j = 0; j < PL_FANOUT; j++) {
            rows[2u * (k * PL_FANOUT + j)] = k;
            rows[2u * (k * PL_FANOUT + j) + 1u] = (int64_t)k * 1000 + j;
        }
    }
    sess = pipeline_session();
    right = pipeline_relation("right", right_names, 2, rows, PL_JOIN_ROWS);
    if (!sess || !right) {
        col_rel_destroy(right);
        pipeline_session_destroy(sess);
        return NULL;
    }
    if (session_add_rel(sess, right) != 0) {
        col_rel_destroy(right);
        pipeline_session_destroy(sess);
        return NULL;
    }
    if (!col_session_get_arrangement(&sess->base, "right",
        (const uint32_t[]){ 0 }, 1)) {
        pipeline_session_destroy(sess);
        return NULL;
    }
    return sess;
}

typedef struct {
    int rc;
    uint32_t stack_top;
    uint32_t restored_left_rows;
    uint32_t nrows;
    int64_t (*rows)[3];
} pl_run_t;

static int
pl_row_cmp(const void *a, const void *b)
{
    const int64_t *x = (const int64_t *)a;
    const int64_t *y = (const int64_t *)b;
    for (uint32_t c = 0; c < 3; c++) {
        if (x[c] != y[c])
            return x[c] < y[c] ? -1 : 1;
    }
    return 0;
}

/* Evaluate @plan over a fresh four-row left input.  batch_bytes == 0 runs
 * the one-shot oracle path; otherwise the bounded pipeline is offered. */
static pl_run_t
pl_run(wl_col_session_t *sess, const wl_plan_relation_t *plan,
    uint32_t batch_bytes)
{
    static const char *const left_names[] = { "k", "v" };
    int64_t left_rows[PL_KEYS * 2u];
    pl_run_t run = { .rc = ENOMEM };
    eval_stack_t stack;
    uint32_t saved = sess->join_batch_bytes;
    col_rel_t *left;

    for (uint32_t k = 0; k < PL_KEYS; k++) {
        left_rows[2u * k] = k;
        left_rows[2u * k + 1u] = ((int64_t)k + 1) * 10;
    }
    left = pipeline_relation("left", left_names, 2, left_rows, PL_KEYS);
    eval_stack_init(&stack);
    if (!left || eval_stack_push(&stack, left, true) != 0) {
        col_rel_destroy(left);
        return run;
    }
    sess->join_batch_bytes = batch_bytes;
    run.rc = col_eval_relation_plan(plan, &stack, sess);
    sess->join_batch_bytes = saved;
    run.stack_top = stack.top;
    if (run.rc != 0 && stack.top == 1 && stack.items[0].rel)
        run.restored_left_rows = stack.items[0].rel->nrows;
    if (run.rc == 0 && stack.top == 1 && stack.items[0].rel
        && stack.items[0].rel->ncols == 3) {
        const col_rel_t *out = stack.items[0].rel;
        run.nrows = out->nrows;
        run.rows = calloc(out->nrows ? out->nrows : 1u, sizeof(*run.rows));
        if (run.rows) {
            for (uint32_t r = 0; r < out->nrows; r++) {
                for (uint32_t c = 0; c < 3; c++)
                    run.rows[r][c] = out->columns[c][r];
            }
            qsort(run.rows, out->nrows, sizeof(*run.rows), pl_row_cmp);
        }
    }
    (void)eval_stack_drain(&stack);
    return run;
}

static bool
pl_same_rows(const pl_run_t *a, const pl_run_t *b)
{
    return a->rc == 0 && b->rc == 0 && a->rows && b->rows
           && a->nrows == b->nrows
           && memcmp(a->rows, b->rows, (size_t)a->nrows * sizeof(*a->rows))
           == 0;
}

/* JOIN(left.k = right.k) -> FILTER(col3 >= 1002) -> MAP(@exprs, 3 cols). */
typedef struct {
    wl_plan_op_t ops[3];
    wl_plan_relation_t plan;
} pl_plan_t;

static void
pl_plan_init(pl_plan_t *p, wl_plan_expr_buffer_t *exprs, uint32_t nexprs)
{
    static const char *const keys[] = { "k" };
    static const uint32_t project[] = { 0, 1, 0 };
    static uint8_t filter[] = {
        PL_VAR('3'), WL_PLAN_EXPR_CONST_INT, 0xea, 0x03, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_GTE,
    };
    memset(p, 0, sizeof(*p));
    p->ops[0].op = WL_PLAN_OP_JOIN;
    p->ops[0].right_relation = "right";
    p->ops[0].left_keys = keys;
    p->ops[0].right_keys = keys;
    p->ops[0].key_count = 1;
    p->ops[1].op = WL_PLAN_OP_FILTER;
    p->ops[1].filter_expr = (wl_plan_expr_buffer_t){ filter, sizeof(filter) };
    p->ops[2].op = WL_PLAN_OP_MAP;
    p->ops[2].map_exprs = exprs;
    p->ops[2].map_expr_count = nexprs;
    p->ops[2].project_indices = project;
    p->ops[2].project_count = 3;
    p->plan.ops = p->ops;
    p->plan.op_count = 3;
}

/* col1 + col3, col3 * 3 - 1, col0 (projected) */
static uint8_t pl_sum[] = { PL_VAR('1'), PL_VAR('3'), WL_PLAN_EXPR_ARITH_ADD };
static uint8_t pl_scaled[] = {
    PL_VAR('3'), PL_INT(3), WL_PLAN_EXPR_ARITH_MUL, PL_INT(1),
    WL_PLAN_EXPR_ARITH_SUB,
};

static void
test_expression_pipeline_matches_oracle(void)
{
    wl_plan_expr_buffer_t exprs[] = {
        { pl_sum, sizeof(pl_sum) }, { pl_scaled, sizeof(pl_scaled) },
    };
    wl_col_session_t *sess = expr_pipeline_session();
    pl_plan_t p;
    pl_run_t got = { 0 };
    pl_run_t want = { 0 };
    uint32_t fallbacks;

    CHECK(sess != NULL, "expression pipeline fixture");
    if (!sess)
        return;
    pl_plan_init(&p, exprs, 2);
    pl_observe_reset();
    wl_columnar_ops_test_before_map_dispose = pl_map_observer;

    fallbacks = sess->join_batch_fallback_count;
    got = pl_run(sess, &p.plan, sess->join_batch_bytes);
    CHECK(got.rc == 0 && got.nrows == PL_FILTERED_ROWS,
        "expression pipeline produces every filtered row");
    CHECK(sess->join_batch_fallback_count == fallbacks,
        "expression pipeline is consumed without fallback");
    CHECK(pl_map_calls >= (PL_JOIN_ROWS + PL_BATCH_ROWS - 1u) / PL_BATCH_ROWS
        && pl_map_max_rows <= PL_BATCH_ROWS,
        "expression MAP only ever sees one bounded join batch");

    pl_observe_reset();
    want = pl_run(sess, &p.plan, 0);
    CHECK(want.rc == 0 && pl_map_calls == 1
        && pl_map_max_rows == PL_FILTERED_ROWS,
        "one-shot oracle materializes the filtered join for MAP");
    CHECK(pl_same_rows(&got, &want),
        "expression pipeline matches the one-shot multiset");
    if (got.rows && got.nrows == PL_FILTERED_ROWS) {
        /* Largest row: key 3, j 63 -> (40 + 3063, 3063 * 3 - 1, 3). */
        const int64_t *last = got.rows[got.nrows - 1u];
        CHECK(last[0] == 3103 && last[1] == 9188 && last[2] == 3,
            "expression values are computed per joined row");
    }

    free(got.rows);
    free(want.rows);
    wl_columnar_ops_test_before_map_dispose = NULL;
    pipeline_session_destroy(sess);
}

static void
test_expression_pipeline_failures_roll_back(void)
{
    /* col1 / (col0 - 3): only key 3, the last left row, divides by zero. */
    static uint8_t late_div[] = {
        PL_VAR('1'), PL_VAR('0'), PL_INT(3), WL_PLAN_EXPR_ARITH_SUB,
        WL_PLAN_EXPR_ARITH_DIV,
    };
    wl_plan_expr_buffer_t div_exprs[] = { { late_div, sizeof(late_div) } };
    wl_plan_expr_buffer_t exprs[] = {
        { pl_sum, sizeof(pl_sum) }, { pl_scaled, sizeof(pl_scaled) },
    };
    wl_col_session_t *sess = expr_pipeline_session();
    pl_plan_t div_plan;
    pl_plan_t p;
    pl_run_t run;

    CHECK(sess != NULL, "expression failure fixture");
    if (!sess)
        return;
    pl_plan_init(&div_plan, div_exprs, 1);
    pl_plan_init(&p, exprs, 2);
    wl_columnar_ops_test_before_map_dispose = pl_map_observer;

    pl_observe_reset();
    run = pl_run(sess, &div_plan.plan, sess->join_batch_bytes);
    CHECK(run.rc == ERANGE,
        "expression error keeps the ordinary MAP ERANGE policy");
    CHECK(pl_map_calls > 1,
        "expression error arrives after earlier batches were committed");
    CHECK(run.stack_top == 1 && run.restored_left_rows == PL_KEYS,
        "expression error restores the left input and publishes nothing");
    free(run.rows);
    pl_observe_reset();
    run = pl_run(sess, &div_plan.plan, 0);
    CHECK(run.rc == ERANGE, "one-shot oracle reports the same ERANGE");
    free(run.rows);

    pl_observe_reset();
    pl_fail_map_call = 5;
    run = pl_run(sess, &p.plan, sess->join_batch_bytes);
    CHECK(run.rc == ENOMEM && pl_map_calls == 5,
        "mid-stream MAP allocation failure stops at the fifth batch");
    CHECK(run.stack_top == 1 && run.restored_left_rows == PL_KEYS,
        "mid-stream sink failure publishes nothing");
    free(run.rows);

    pl_observe_reset();
    sess->join_output_limit = 50;
    run = pl_run(sess, &p.plan, sess->join_batch_bytes);
    sess->join_output_limit = 0;
    CHECK(run.rc == EOVERFLOW && run.stack_top == 1
        && run.restored_left_rows == PL_KEYS,
        "expression pipeline enforces the join output limit");
    free(run.rows);

    {
        pl_run_t want;
        uint32_t fallbacks = sess->join_batch_fallback_count;
        pl_observe_reset();
        want = pl_run(sess, &p.plan, 0);
        pl_observe_reset();
        pl_touch_on_map = session_find_rel(sess, "right");
        run = pl_run(sess, &p.plan, sess->join_batch_bytes);
        CHECK(run.rc == 0 && pl_same_rows(&run, &want),
            "stale input abandons the pipeline for the ordinary path");
        CHECK(sess->join_batch_fallback_count == fallbacks + 1u
            && sess->join_batch_last_reason
            == COL_JOIN_BATCH_EXCLUDED_PIPELINE,
            "stale pipeline records a fallback");
        free(run.rows);
        free(want.rows);

        pl_observe_reset();
        sess->join_batch_strict = true;
        pl_touch_on_map = session_find_rel(sess, "right");
        run = pl_run(sess, &p.plan, sess->join_batch_bytes);
        sess->join_batch_strict = false;
        CHECK(run.rc == ENOTSUP, "strict mode rejects a stale pipeline");
        free(run.rows);
    }

    pl_observe_reset();
    wl_columnar_ops_test_before_map_dispose = NULL;
    pipeline_session_destroy(sess);
}

static void
test_expression_pipeline_budget_sweep(void)
{
    wl_plan_expr_buffer_t exprs[] = {
        { pl_sum, sizeof(pl_sum) }, { pl_scaled, sizeof(pl_scaled) },
    };
    wl_col_session_t *sess = expr_pipeline_session();
    pl_plan_t p;
    pl_run_t want;
    bool saw_mid_stream_denial = false;
    bool saw_success = false;

    CHECK(sess != NULL, "expression budget fixture");
    if (!sess)
        return;
    pl_plan_init(&p, exprs, 2);
    pl_observe_reset();
    want = pl_run(sess, &p.plan, 0);
    CHECK(want.rc == 0, "budget sweep oracle");
    wl_columnar_ops_test_before_map_dispose = pl_map_observer;

    for (uint64_t budget = 256; budget <= (1u << 20) && !saw_success;
        budget += 256) {
        wl_columnar_memory_resolution_t resolution = { 0 };
        wl_columnar_memory_governor_ref_t *ref;
        wl_columnar_memory_governor_t *governor;
        pl_run_t run;

        resolution.budget_bytes = budget;
        resolution.usable_bytes = budget;
        resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
        resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
        resolution.status = WL_COLUMNAR_MEMORY_OK;
        ref = wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(ref != NULL, "budget sweep governor");
        if (!ref)
            break;
        governor = wl_columnar_memory_governor_ref_get(ref);
        sess->memory_governor = ref;
        pl_observe_reset();
        run = pl_run(sess, &p.plan, sess->join_batch_bytes);
        sess->memory_governor = NULL;
        if (run.rc == 0) {
            saw_success = true;
            CHECK(pl_same_rows(&run, &want),
                "governed expression pipeline matches the oracle");
        } else {
            CHECK(run.rc == ENOSPC,
                "budget denial reports ENOSPC");
            CHECK(run.stack_top == 1 && run.restored_left_rows == PL_KEYS,
                "budget denial restores the left input");
            if (pl_map_calls > 1)
                saw_mid_stream_denial = true;
        }
        CHECK(wl_columnar_memory_reserved(governor) == 0,
            "every pipeline reservation is released");
        free(run.rows);
        wl_columnar_memory_governor_ref_release(ref);
    }
    CHECK(saw_mid_stream_denial,
        "budget sweep denies a reservation after committed batches");
    CHECK(saw_success, "budget sweep reaches a sufficient budget");

    free(want.rows);
    pl_observe_reset();
    wl_columnar_ops_test_before_map_dispose = NULL;
    pipeline_session_destroy(sess);
}

static void
test_excluded_map_is_diagnosed(void)
{
    static uint8_t hash[] = { PL_VAR('1'), WL_PLAN_EXPR_ARITH_HASH };
    wl_plan_expr_buffer_t exprs[] = { { hash, sizeof(hash) } };
    wl_col_session_t *sess = expr_pipeline_session();
    pl_plan_t p;
    pl_run_t run;
    pl_run_t want;
    uint32_t fallbacks;

    CHECK(sess != NULL, "excluded MAP fixture");
    if (!sess)
        return;
    pl_plan_init(&p, exprs, 1);
    wl_columnar_ops_test_before_map_dispose = pl_map_observer;

    pl_observe_reset();
    want = pl_run(sess, &p.plan, 0);
    fallbacks = sess->join_batch_fallback_count;
    pl_observe_reset();
    run = pl_run(sess, &p.plan, sess->join_batch_bytes);
    CHECK(run.rc == 0 && pl_same_rows(&run, &want) && pl_map_calls == 1,
        "non-strict excluded MAP falls back to the one-shot path");
    CHECK(sess->join_batch_fallback_count == fallbacks + 1u
        && sess->join_batch_last_reason == COL_JOIN_BATCH_EXCLUDED_PIPELINE,
        "non-strict excluded MAP records a pipeline fallback");
    free(run.rows);
    free(want.rows);

    pl_observe_reset();
    sess->join_batch_strict = true;
    run = pl_run(sess, &p.plan, sess->join_batch_bytes);
    sess->join_batch_strict = false;
    CHECK(run.rc == ENOTSUP && pl_map_calls == 0,
        "strict excluded MAP returns ENOTSUP before evaluating it");
    free(run.rows);

    pl_observe_reset();
    wl_columnar_ops_test_before_map_dispose = NULL;
    pipeline_session_destroy(sess);
}

int
main(void)
{
    test_projection_pipeline();
    test_conservative_exclusions();
    test_pipeline_consumes_batches();
    test_expression_map_preflight();
    test_expression_pipeline_matches_oracle();
    test_expression_pipeline_failures_roll_back();
    test_expression_pipeline_budget_sweep();
    test_excluded_map_is_diagnosed();
    if (failures)
        fprintf(stderr, "%d join pipeline preflight checks failed\n",
            failures);
    return failures ? 1 : 0;
}
