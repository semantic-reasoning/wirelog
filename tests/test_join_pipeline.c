/* Conservative pipeline preflight for Issue #1475; expression MAPs #1777. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

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
    /* The ordinary JOIN over a $d$ right operand builds a delta
     * arrangement. */
    col_session_free_delta_arrangements(sess);
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

/* Right-operand selection, key-index range and key-type parity with the
 * ordinary JOIN.  Plan: JOIN(k) -> FILTER(true) -> MAP[v, r] over
 * right = {(0,100),(0,101),(1,200),(1,201)} and left = {(0,10),(1,20)}.
 * join_batch_bytes = 64 is two 32-byte join rows per batch, so a consumed
 * full-relation JOIN runs the MAP twice and a declined one runs it once. */
#define SEL_BATCH_BYTES 64u
#define SEL_MAX_ROWS 8u

typedef struct {
    int rc;
    uint32_t stack_top;
    bool left_kept;
    uint32_t nrows;
    int64_t rows[SEL_MAX_ROWS][2];
} sel_run_t;

static const char *const sel_names[] = { "k", "r" };

static int
sel_add(wl_col_session_t *sess, const char *name, const int64_t *rows,
    uint32_t nrows)
{
    col_rel_t *rel = pipeline_relation(name, sel_names, 2, rows, nrows);
    if (!rel)
        return ENOMEM;
    if (session_add_rel(sess, rel) != 0) {
        col_rel_destroy(rel);
        return ENOMEM;
    }
    return 0;
}

static wl_col_session_t *
sel_session(void)
{
    static const int64_t right[] = { 0, 100, 0, 101, 1, 200, 1, 201 };
    wl_col_session_t *sess = pipeline_session();
    if (!sess)
        return NULL;
    if (sel_add(sess, "right", right, 4) != 0
        || !col_session_get_arrangement(&sess->base, "right",
        (const uint32_t[]){ 0 }, 1)) {
        pipeline_session_destroy(sess);
        return NULL;
    }
    return sess;
}

static int
sel_row_cmp(const void *a, const void *b)
{
    const int64_t *x = (const int64_t *)a;
    const int64_t *y = (const int64_t *)b;
    for (uint32_t c = 0; c < 2; c++) {
        if (x[c] != y[c])
            return x[c] < y[c] ? -1 : 1;
    }
    return 0;
}

/* Evaluate @plan over @left (owned by the stack; NULL builds the default
 * two-row non-delta left) with the given batch budget. */
static sel_run_t
sel_eval(wl_col_session_t *sess, const wl_plan_relation_t *plan,
    uint32_t batch_bytes, col_rel_t *left)
{
    static const char *const left_names[] = { "k", "v" };
    static const int64_t left_rows[] = { 0, 10, 1, 20 };
    sel_run_t run;
    eval_stack_t stack;

    memset(&run, 0, sizeof(run));
    run.rc = ENOMEM;
    if (!left)
        left = pipeline_relation("left", left_names, 2, left_rows, 2);
    eval_stack_init(&stack);
    if (!left || eval_stack_push_delta(&stack, left, true, false) != 0) {
        col_rel_destroy(left);
        return run;
    }
    sess->join_batch_bytes = batch_bytes;
    run.rc = col_eval_relation_plan(plan, &stack, sess);
    run.stack_top = stack.top;
    run.left_kept = stack.top == 1 && stack.items[0].rel == left;
    if (run.rc == 0 && stack.top == 1 && stack.items[0].rel
        && stack.items[0].rel->ncols == 2
        && stack.items[0].rel->nrows <= SEL_MAX_ROWS) {
        const col_rel_t *out = stack.items[0].rel;
        run.nrows = out->nrows;
        for (uint32_t r = 0; r < out->nrows; r++) {
            run.rows[r][0] = out->columns[0][r];
            run.rows[r][1] = out->columns[1][r];
        }
        qsort(run.rows, run.nrows, sizeof(run.rows[0]), sel_row_cmp);
    } else if (run.rc == 0) {
        run.nrows = UINT32_MAX;
    }
    (void)eval_stack_drain(&stack);
    return run;
}

static bool
sel_rows_are(const sel_run_t *run, const int64_t (*want)[2], uint32_t n)
{
    if (run->rc != 0 || run->nrows != n)
        return false;
    for (uint32_t i = 0; i < n; i++) {
        if (run->rows[i][0] != want[i][0] || run->rows[i][1] != want[i][1])
            return false;
    }
    return true;
}

typedef struct {
    wl_plan_op_t ops[3];
    wl_plan_relation_t plan;
} sel_plan_t;

static void
sel_plan_init(sel_plan_t *p, wl_delta_mode_t mode)
{
    static const char *const keys[] = { "k" };
    static const uint32_t project[] = { 1, 3 };
    static uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    memset(p, 0, sizeof(*p));
    p->ops[0].op = WL_PLAN_OP_JOIN;
    p->ops[0].right_relation = "right";
    p->ops[0].left_keys = keys;
    p->ops[0].right_keys = keys;
    p->ops[0].key_count = 1;
    p->ops[0].delta_mode = mode;
    p->ops[1].op = WL_PLAN_OP_FILTER;
    p->ops[1].filter_expr = (wl_plan_expr_buffer_t){ predicate,
                                                     sizeof(predicate) };
    p->ops[2].op = WL_PLAN_OP_MAP;
    p->ops[2].project_indices = project;
    p->ops[2].project_count = 2;
    p->plan.ops = p->ops;
    p->plan.op_count = 3;
}

typedef enum {
    SEL_FORCE_DELTA,
    SEL_FORCE_EMPTY,
    SEL_AUTO,
    SEL_RETRACTION,
    SEL_FULL,
} sel_case_t;

static wl_col_session_t *
sel_case_session(sel_case_t c)
{
    static const int64_t delta[] = { 0, 101 };
    static const int64_t retracted[] = { 1, 201 };
    wl_col_session_t *sess = sel_session();
    if (!sess)
        return NULL;
    sess->current_iteration = c == SEL_RETRACTION ? 0u : 1u;
    sess->retraction_right_pass = c == SEL_RETRACTION;
    if ((c == SEL_FORCE_DELTA || c == SEL_AUTO || c == SEL_FULL)
        && sel_add(sess, "$d$right", delta, 1) != 0) {
        pipeline_session_destroy(sess);
        return NULL;
    }
    if (c == SEL_RETRACTION && sel_add(sess, "$r$right", retracted, 1) != 0) {
        pipeline_session_destroy(sess);
        return NULL;
    }
    return sess;
}

static void
test_pipeline_right_selection_matches_join(void)
{
    static const int64_t want_delta[][2] = { { 10, 101 } };
    static const int64_t want_retracted[][2] = { { 20, 201 } };
    static const int64_t want_full[][2] = {
        { 10, 100 }, { 10, 101 }, { 20, 200 }, { 20, 201 },
    };
    static const struct {
        sel_case_t c;
        wl_delta_mode_t mode;
        const int64_t (*want)[2];
        uint32_t nwant;
        const char *name;
    } cases[] = {
        { SEL_FORCE_DELTA, WL_DELTA_FORCE_DELTA, want_delta, 1,
          "FORCE_DELTA reads $d$right" },
        { SEL_FORCE_EMPTY, WL_DELTA_FORCE_DELTA, NULL, 0,
          "FORCE_DELTA without $d$right is empty" },
        { SEL_AUTO, WL_DELTA_AUTO, want_delta, 1,
          "AUTO with a non-delta left reads the smaller $d$right" },
        { SEL_RETRACTION, WL_DELTA_FORCE_FULL, want_retracted, 1,
          "retraction right pass reads $r$right" },
        { SEL_FULL, WL_DELTA_FORCE_FULL, want_full, 4,
          "FORCE_FULL reads the persistent relation" },
    };

    wl_columnar_ops_test_before_map_dispose = pl_map_observer;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        sel_plan_t p;
        sel_run_t off;
        sel_run_t on;
        uint32_t on_map_calls;
        wl_col_session_t *off_sess = sel_case_session(cases[i].c);
        wl_col_session_t *on_sess = sel_case_session(cases[i].c);

        CHECK(off_sess && on_sess, cases[i].name);
        if (!off_sess || !on_sess) {
            pipeline_session_destroy(off_sess);
            pipeline_session_destroy(on_sess);
            continue;
        }
        sel_plan_init(&p, cases[i].mode);
        pl_observe_reset();
        off = sel_eval(off_sess, &p.plan, 0, NULL);
        pl_observe_reset();
        on = sel_eval(on_sess, &p.plan, SEL_BATCH_BYTES, NULL);
        on_map_calls = pl_map_calls;
        if (!sel_rows_are(&off, cases[i].want, cases[i].nwant)
            || !sel_rows_are(&on, cases[i].want, cases[i].nwant)) {
            fprintf(stderr, "  %s: off rc=%d rows=%u, on rc=%d rows=%u\n",
                cases[i].name, off.rc, off.nrows, on.rc, on.nrows);
            CHECK(false, "batched JOIN reads the right operand the ordinary "
                "JOIN selects");
        }
        if (cases[i].c == SEL_FULL) {
            CHECK(on_map_calls == 2,
                "a full-relation JOIN is consumed batch by batch");
        } else {
            if (on_map_calls != 1)
                fprintf(stderr, "  %s: %u MAP calls\n", cases[i].name,
                    on_map_calls);
            CHECK(on_map_calls == 1,
                "a delta or empty right operand is left to the ordinary JOIN");
        }
        if (cases[i].c != SEL_FORCE_EMPTY && cases[i].c != SEL_FULL) {
            CHECK(on_sess->join_batch_last_reason
                == COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT,
                "a delta right operand records the delta-right fallback");
        }
        pipeline_session_destroy(off_sess);
        pipeline_session_destroy(on_sess);
    }

    {
        sel_plan_t p;
        sel_run_t on;
        wl_col_session_t *sess = sel_case_session(SEL_FORCE_DELTA);
        CHECK(sess != NULL, "strict delta-right fixture");
        if (sess) {
            sel_plan_init(&p, WL_DELTA_FORCE_DELTA);
            sess->join_batch_strict = true;
            pl_observe_reset();
            on = sel_eval(sess, &p.plan, SEL_BATCH_BYTES, NULL);
            CHECK(on.rc == ENOTSUP && on.left_kept && pl_map_calls == 0,
                "strict mode rejects a delta right operand with a tail and "
                "keeps the left input");
            pipeline_session_destroy(sess);
        }
    }
    pl_observe_reset();
    wl_columnar_ops_test_before_map_dispose = NULL;
}

static void
test_pipeline_key_types_match_join(void)
{
    static const char *const left_names[] = { "k", "v" };
    static const wirelog_column_type_t left_types[] = {
        WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_INT64,
    };
    static const int64_t left_rows[] = { 0, 10, 1, 20 };
    static const uint32_t budgets[] = { 0, SEL_BATCH_BYTES };

    wl_columnar_ops_test_before_map_dispose = pl_map_observer;
    for (size_t b = 0; b < sizeof(budgets) / sizeof(budgets[0]); b++) {
        sel_plan_t p;
        sel_run_t run;
        wl_col_session_t *sess = sel_session();
        col_rel_t *left = col_rel_new_auto("left", 2);
        int rc = !left ? ENOMEM : col_rel_set_schema(left, 2, left_names);
        if (rc == 0)
            rc = col_rel_set_column_types(left, left_types, 2);
        for (uint32_t r = 0; rc == 0 && r < 2; r++)
            rc = col_rel_append_row(left, left_rows + 2u * r);
        CHECK(sess && rc == 0, "key-type fixture");
        if (!sess || rc != 0) {
            col_rel_destroy(left);
            pipeline_session_destroy(sess);
            continue;
        }
        sel_plan_init(&p, WL_DELTA_FORCE_FULL);
        pl_observe_reset();
        run = sel_eval(sess, &p.plan, budgets[b], left);
        if (run.rc != EINVAL)
            fprintf(stderr, "  budget %u: rc=%d rows=%u\n", budgets[b],
                run.rc, run.nrows);
        CHECK(run.rc == EINVAL && pl_map_calls == 0,
            "a FLOAT key against an INT64 key is EINVAL");
        /* The ordinary JOIN disposes its left input on failure; the
         * pipeline fails before popping it. */
        CHECK(budgets[b] == 0 || run.left_kept,
            "the batched key-type failure keeps the left input");
        pipeline_session_destroy(sess);
    }
    pl_observe_reset();
    wl_columnar_ops_test_before_map_dispose = NULL;
}

/* Call the pipeline directly and require that it leaves the stack alone. */
static void
sel_expect_decline(wl_col_session_t *sess, col_rel_t *left,
    const char *message)
{
    sel_plan_t p;
    eval_stack_t stack;
    uint32_t next_index = UINT32_MAX;
    wl_columnar_join_pipeline_decline_t decline
        = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NONE;
    int rc;

    sel_plan_init(&p, WL_DELTA_FORCE_FULL);
    eval_stack_init(&stack);
    if (eval_stack_push_delta(&stack, left, true, false) != 0) {
        col_rel_destroy(left);
        CHECK(false, message);
        return;
    }
    sess->join_batch_bytes = SEL_BATCH_BYTES;
    rc = wl_columnar_join_pipeline_try_eval(&p.plan, 0, &stack, sess,
            &next_index, &decline);
    if (rc != 0)
        fprintf(stderr, "  %s: try_eval rc=%d\n", message, rc);
    CHECK(rc == 0 && stack.top == 1 && stack.items[0].rel == left
        && next_index == 0, message);
    CHECK(rc != 0 || decline == WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY,
        "a zero-column input is left to the ordinary JOIN");
    (void)eval_stack_drain(&stack);
}

/* A session whose "right" relation has no columns and no rows. */
static wl_col_session_t *
sel_zero_column_session(void)
{
    wl_col_session_t *sess = pipeline_session();
    col_rel_t *right = col_rel_new_auto("right", 0);
    if (!sess || !right || session_add_rel(sess, right) != 0) {
        col_rel_destroy(right);
        pipeline_session_destroy(sess);
        return NULL;
    }
    return sess;
}

static void
test_pipeline_zero_column_right_declines(void)
{
    static const char *const left_names[] = { "k", "v" };
    static const int64_t left_rows[] = { 0, 10, 1, 20 };
    static const uint32_t budgets[] = { 0, SEL_BATCH_BYTES };

    for (size_t b = 0; b < sizeof(budgets) / sizeof(budgets[0]); b++) {
        sel_plan_t p;
        sel_run_t run;
        wl_col_session_t *sess = sel_zero_column_session();
        CHECK(sess != NULL, "zero-column right fixture");
        if (!sess)
            continue;
        sel_plan_init(&p, WL_DELTA_FORCE_FULL);
        run = sel_eval(sess, &p.plan, budgets[b], NULL);
        if (run.rc != 0 || run.nrows != 0)
            fprintf(stderr, "  budget %u: rc=%d rows=%u\n", budgets[b],
                run.rc, run.nrows);
        CHECK(run.rc == 0 && run.nrows == 0,
            "an empty zero-column right side joins to an empty result");
        pipeline_session_destroy(sess);
    }

    {
        wl_col_session_t *sess = sel_zero_column_session();
        col_rel_t *left = pipeline_relation("left", left_names, 2,
                left_rows, 2);
        CHECK(sess && left, "zero-column right decline fixture");
        if (sess && left)
            sel_expect_decline(sess, left,
                "the pipeline declines a zero-column right side");
        else
            col_rel_destroy(left);
        pipeline_session_destroy(sess);
    }
}

static void
test_pipeline_zero_column_left_declines(void)
{
    /* Only the pipeline's own decision is asserted: the ordinary bounded
     * JOIN still rejects a zero-column left (issue #2170). */
    wl_col_session_t *sess = sel_session();
    col_rel_t *left = col_rel_new_auto("left", 0);
    CHECK(sess && left, "zero-column left fixture");
    if (sess && left)
        sel_expect_decline(sess, left,
            "the pipeline declines a zero-column left side");
    else
        col_rel_destroy(left);
    pipeline_session_destroy(sess);
}

/* ---- Successor classification and decline kinds (#1370 unit a1) ---- */

#define A1_MAX_ROWS 16u
#define A1_MAX_COLS 6u

typedef struct {
    int rc;
    uint32_t stack_top;
    bool left_kept;
    uint32_t ncols;
    uint32_t nrows;
    int64_t rows[A1_MAX_ROWS][A1_MAX_COLS];
} a1_run_t;

static uint32_t a1_reduce_calls;

static void
a1_reduce_observer(eval_stack_t *stack, eval_entry_t *entry)
{
    (void)stack;
    (void)entry;
    a1_reduce_calls++;
}

static int
a1_row_cmp(const void *a, const void *b)
{
    const int64_t *x = (const int64_t *)a;
    const int64_t *y = (const int64_t *)b;
    for (uint32_t c = 0; c < A1_MAX_COLS; c++) {
        if (x[c] != y[c])
            return x[c] < y[c] ? -1 : 1;
    }
    return 0;
}

/* Evaluate @plan over @left (NULL: the two-row sel left) with @below, when
 * non-NULL, pushed underneath it.  The stack owns both. */
static a1_run_t
a1_eval(wl_col_session_t *sess, const wl_plan_relation_t *plan,
    uint32_t batch_bytes, col_rel_t *below, col_rel_t *left)
{
    static const char *const left_names[] = { "k", "v" };
    static const int64_t left_rows[] = { 0, 10, 1, 20 };
    a1_run_t run;
    eval_stack_t stack;
    uint32_t base_top;

    memset(&run, 0, sizeof(run));
    run.rc = ENOMEM;
    eval_stack_init(&stack);
    if (below && eval_stack_push(&stack, below, true) != 0) {
        col_rel_destroy(below);
        col_rel_destroy(left);
        return run;
    }
    if (!left)
        left = pipeline_relation("left", left_names, 2, left_rows, 2);
    if (!left || eval_stack_push_delta(&stack, left, true, false) != 0) {
        col_rel_destroy(left);
        (void)eval_stack_drain(&stack);
        return run;
    }
    base_top = stack.top;
    a1_reduce_calls = 0;
    sess->join_batch_bytes = batch_bytes;
    run.rc = col_eval_relation_plan(plan, &stack, sess);
    run.stack_top = stack.top;
    run.left_kept = stack.top == base_top
        && stack.items[stack.top - 1].rel == left;
    if (run.rc == 0 && stack.top >= 1 && stack.items[stack.top - 1].rel) {
        const col_rel_t *out = stack.items[stack.top - 1].rel;
        run.ncols = out->ncols;
        if (out->ncols > A1_MAX_COLS || out->nrows > A1_MAX_ROWS) {
            run.nrows = UINT32_MAX;
        } else {
            run.nrows = out->nrows;
            for (uint32_t r = 0; r < out->nrows; r++) {
                for (uint32_t c = 0; c < out->ncols; c++)
                    run.rows[r][c] = out->columns[c][r];
            }
            qsort(run.rows, run.nrows, sizeof(run.rows[0]), a1_row_cmp);
        }
    }
    (void)eval_stack_drain(&stack);
    return run;
}

/* @want holds @n rows of @ncols values, already sorted. */
static bool
a1_rows_are(const a1_run_t *run, uint32_t ncols, const int64_t *want,
    uint32_t n)
{
    if (run->rc != 0 || run->ncols != ncols || run->nrows != n)
        return false;
    for (uint32_t r = 0; r < n; r++) {
        for (uint32_t c = 0; c < ncols; c++) {
            if (run->rows[r][c] != want[r * ncols + c])
                return false;
        }
    }
    return true;
}

static void
a1_report(const char *name, const a1_run_t *run)
{
    fprintf(stderr, "  %s: rc=%d top=%u ncols=%u nrows=%u\n", name, run->rc,
        run->stack_top, run->ncols, run->nrows);
}

static const char *const a1_keys[] = { "k" };
static const uint32_t a1_group_k[] = { 0 };

static void
a1_join(wl_plan_op_t *op, const char *right, wl_delta_mode_t mode)
{
    memset(op, 0, sizeof(*op));
    op->op = WL_PLAN_OP_JOIN;
    op->right_relation = right;
    op->left_keys = a1_keys;
    op->right_keys = a1_keys;
    op->key_count = 1;
    op->delta_mode = mode;
}

/* COUNT(*) grouped by column 0. */
static void
a1_count_by_k(wl_plan_op_t *op)
{
    memset(op, 0, sizeof(*op));
    op->op = WL_PLAN_OP_REDUCE;
    op->agg_fn = WIRELOG_AGG_COUNT;
    op->group_by_indices = a1_group_k;
    op->group_by_count = 1;
    op->aggregate_index = 1;
}

/* Codes for the classifier table: plain op values, plus these. */
#define A1_EXCHANGE_W1 101
#define A1_EXCHANGE_W4 104
#define A1_EXCHANGE_NULL 110
#define A1_UNKNOWN_OP 99

static void
test_successor_classifier_table(void)
{
    enum { SINK = WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK,
           MAT = WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING };
    static const struct {
        int ops[6];
        uint32_t n;
        int want;
        const char *name;
    } rows[] = {
        { { 0 }, 0, SINK, "end of plan" },
        { { WL_PLAN_OP_CONCAT }, 1, SINK, "CONCAT" },
        { { WL_PLAN_OP_CONSOLIDATE }, 1, SINK, "CONSOLIDATE" },
        { { WL_PLAN_OP_CONCAT, WL_PLAN_OP_CONSOLIDATE, A1_EXCHANGE_W1 }, 3,
          SINK, "CONCAT CONSOLIDATE EXCHANGE(W=1)" },
        { { WL_PLAN_OP_CONCAT, WL_PLAN_OP_CONSOLIDATE, A1_EXCHANGE_W4 }, 3,
          SINK, "CONCAT CONSOLIDATE EXCHANGE(W=4)" },
        { { WL_PLAN_OP_CONCAT, WL_PLAN_OP_CONSOLIDATE,
            WL_PLAN_OP_CONSOLIDATE }, 3, SINK,
          "CONCAT CONSOLIDATE CONSOLIDATE" },
        { { WL_PLAN_OP_VARIABLE, WL_PLAN_OP_JOIN, WL_PLAN_OP_CONCAT }, 3,
          SINK, "VAR JOIN CONCAT" },
        { { WL_PLAN_OP_VARIABLE, WL_PLAN_OP_JOIN, WL_PLAN_OP_SEMIJOIN,
            WL_PLAN_OP_CONCAT }, 4, SINK, "VAR JOIN SEMIJOIN CONCAT" },
        { { WL_PLAN_OP_K_FUSION, WL_PLAN_OP_CONCAT }, 2, SINK,
          "K_FUSION CONCAT" },
        { { WL_PLAN_OP_LFTJ, WL_PLAN_OP_CONCAT, WL_PLAN_OP_CONCAT }, 3, SINK,
          "LFTJ CONCAT CONCAT" },
        { { WL_PLAN_OP_REDUCE }, 1, MAT, "REDUCE" },
        { { WL_PLAN_OP_REDUCE, A1_EXCHANGE_W1 }, 2, MAT,
          "REDUCE EXCHANGE(W=1)" },
        { { WL_PLAN_OP_JOIN }, 1, MAT, "JOIN" },
        { { WL_PLAN_OP_CONCAT, WL_PLAN_OP_REDUCE }, 2, MAT, "CONCAT REDUCE" },
        { { WL_PLAN_OP_FILTER, WL_PLAN_OP_CONCAT }, 2, MAT, "FILTER CONCAT" },
        { { WL_PLAN_OP_VARIABLE, WL_PLAN_OP_VARIABLE, WL_PLAN_OP_CONCAT,
            WL_PLAN_OP_CONCAT, WL_PLAN_OP_REDUCE }, 5, MAT,
          "VAR VAR CONCAT CONCAT REDUCE" },
        { { A1_EXCHANGE_W4, WL_PLAN_OP_REDUCE }, 2, SINK,
          "EXCHANGE(W=4) REDUCE" },
        { { A1_EXCHANGE_NULL }, 1, MAT, "EXCHANGE without metadata" },
        { { A1_UNKNOWN_OP }, 1, MAT, "unknown op" },
    };
    wl_plan_op_exchange_t w1;
    wl_plan_op_exchange_t w4;

    memset(&w1, 0, sizeof(w1));
    memset(&w4, 0, sizeof(w4));
    w1.num_workers = 1;
    w4.num_workers = 4;
    for (size_t r = 0; r < sizeof(rows) / sizeof(rows[0]); r++) {
        wl_plan_op_t ops[7];
        wl_plan_relation_t plan;
        int got;

        memset(ops, 0, sizeof(ops));
        memset(&plan, 0, sizeof(plan));
        ops[0].op = WL_PLAN_OP_JOIN;
        for (uint32_t i = 0; i < rows[r].n; i++) {
            int code = rows[r].ops[i];
            wl_plan_op_t *op = &ops[i + 1u];
            if (code == A1_EXCHANGE_W1 || code == A1_EXCHANGE_W4
                || code == A1_EXCHANGE_NULL) {
                op->op = WL_PLAN_OP_EXCHANGE;
                op->opaque_data = code == A1_EXCHANGE_W1 ? (void *)&w1
                    : code == A1_EXCHANGE_W4 ? (void *)&w4 : NULL;
            } else {
                op->op = (wl_plan_op_type_t)code;
            }
        }
        plan.ops = ops;
        plan.op_count = rows[r].n + 1u;
        got = (int)wl_columnar_join_pipeline_successor(&plan, 1);
        if (got != rows[r].want)
            fprintf(stderr, "  JOIN %s: got %d, want %d\n", rows[r].name,
                got, rows[r].want);
        CHECK(got == rows[r].want,
            "the successor classifier follows the JOIN's stack entry");
    }
}

static col_rel_t *
a1_empty(const char *name, uint32_t ncols)
{
    static const char *const names[] = { "a", "b", "c", "d", "e", "f" };
    return pipeline_relation(name, names, ncols, NULL, 0);
}

static void
test_relation_sink_successor_is_not_a_fallback(void)
{
    static const int64_t want[] = {
        0, 10, 0, 100, 0, 10, 0, 101, 1, 20, 1, 200, 1, 20, 1, 201,
    };
    wl_plan_op_exchange_t w1;

    memset(&w1, 0, sizeof(w1));
    w1.num_workers = 1;
    for (int shape = 0; shape < 2; shape++) {
        for (int strict = 0; strict < 2; strict++) {
            wl_plan_op_t ops[4];
            wl_plan_relation_t plan;
            a1_run_t run;
            wl_col_session_t *sess = sel_session();

            CHECK(sess != NULL, "relation-sink fixture");
            if (!sess)
                continue;
            memset(&plan, 0, sizeof(plan));
            a1_join(&ops[0], "right", WL_DELTA_AUTO);
            memset(&ops[1], 0, sizeof(ops) - sizeof(ops[0]));
            ops[1].op = WL_PLAN_OP_CONCAT;
            ops[2].op = WL_PLAN_OP_CONSOLIDATE;
            ops[3].op = WL_PLAN_OP_EXCHANGE;
            ops[3].opaque_data = &w1;
            plan.ops = ops;
            plan.op_count = shape == 0 ? 2u : 4u;
            sess->join_batch_strict = strict != 0;
            run = a1_eval(sess, &plan, SEL_BATCH_BYTES, a1_empty("base", 4),
                    NULL);
            if (!a1_rows_are(&run, 4, want, 4)
                || sess->join_batch_fallback_count != 0) {
                a1_report(shape == 0 ? "JOIN CONCAT"
                    : "JOIN CONCAT CONSOLIDATE EXCHANGE", &run);
                fprintf(stderr, "  strict=%d fallbacks=%u\n", strict,
                    sess->join_batch_fallback_count);
            }
            CHECK(a1_rows_are(&run, 4, want, 4),
                "a JOIN feeding a relation sink returns the join rows");
            CHECK(sess->join_batch_fallback_count == 0,
                "a JOIN feeding a relation sink records no fallback");
            pipeline_session_destroy(sess);
        }
    }
}

static wl_col_session_t *
a1_boundary_session(void)
{
    static const char *const right2_names[] = { "k", "s" };
    static const int64_t right2[] = { 0, 7 };
    static const int64_t negated[] = { 1, 0 };
    wl_col_session_t *sess = sel_session();
    col_rel_t *r2;
    col_rel_t *neg;

    if (!sess)
        return NULL;
    r2 = pipeline_relation("right2", right2_names, 2, right2, 1);
    neg = pipeline_relation("$d$x", right2_names, 2, negated, 1);
    if (!r2 || session_add_rel(sess, r2) != 0) {
        col_rel_destroy(r2);
        col_rel_destroy(neg);
        pipeline_session_destroy(sess);
        return NULL;
    }
    if (!neg || session_add_rel(sess, neg) != 0) {
        col_rel_destroy(neg);
        pipeline_session_destroy(sess);
        return NULL;
    }
    if (!col_session_get_arrangement(&sess->base, "right2",
        (const uint32_t[]){ 0 }, 1)) {
        pipeline_session_destroy(sess);
        return NULL;
    }
    return sess;
}

static void
test_materializing_boundaries_are_explicit(void)
{
    static const int64_t want_count[] = { 0, 2, 1, 2 };
    static const int64_t want_join2[] = {
        0, 10, 0, 100, 0, 7, 0, 10, 0, 101, 0, 7,
    };
    static const int64_t want_anti[] = { 0, 10, 0, 100, 0, 10, 0, 101 };
    static const struct {
        const char *name;
        uint32_t ncols;
        const int64_t *want;
        uint32_t nwant;
    } cases[] = {
        { "JOIN -> REDUCE", 2, want_count, 2 },
        { "JOIN -> JOIN", 6, want_join2, 2 },
        { "JOIN -> ANTIJOIN($d$x)", 4, want_anti, 2 },
    };

    wl_columnar_ops_test_before_reduce_dispose = a1_reduce_observer;
    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
        for (int strict = 0; strict < 2; strict++) {
            wl_plan_op_t ops[2];
            wl_plan_relation_t plan;
            a1_run_t run;
            wl_col_session_t *sess = a1_boundary_session();

            CHECK(sess != NULL, "materializing-boundary fixture");
            if (!sess)
                continue;
            memset(&plan, 0, sizeof(plan));
            a1_join(&ops[0], "right", WL_DELTA_AUTO);
            if (c == 0) {
                a1_count_by_k(&ops[1]);
            } else if (c == 1) {
                a1_join(&ops[1], "right2", WL_DELTA_AUTO);
            } else {
                a1_join(&ops[1], "$d$x", WL_DELTA_AUTO);
                ops[1].op = WL_PLAN_OP_ANTIJOIN;
            }
            plan.ops = ops;
            plan.op_count = 2;
            sess->join_batch_strict = strict != 0;
            run = a1_eval(sess, &plan, SEL_BATCH_BYTES, NULL, NULL);
            if (!strict) {
                if (!a1_rows_are(&run, cases[c].ncols, cases[c].want,
                    cases[c].nwant))
                    a1_report(cases[c].name, &run);
                CHECK(a1_rows_are(&run, cases[c].ncols, cases[c].want,
                    cases[c].nwant),
                    "a materializing successor still gets the join rows");
                CHECK(sess->join_batch_fallback_count == 1
                    && sess->join_batch_last_reason
                    == COL_JOIN_BATCH_EXCLUDED_PIPELINE,
                    "a materializing successor records pipeline-not-"
                    "consumable once");
                CHECK(c != 0 || a1_reduce_calls == 1,
                    "the REDUCE runs on the materialized join output");
            } else {
                if (run.rc != ENOTSUP || !run.left_kept)
                    a1_report(cases[c].name, &run);
                CHECK(run.rc == ENOTSUP && run.left_kept
                    && a1_reduce_calls == 0,
                    "strict mode refuses a materializing successor before "
                    "the JOIN runs");
            }
            pipeline_session_destroy(sess);
        }
    }
    wl_columnar_ops_test_before_reduce_dispose = NULL;
}

typedef enum {
    A1_NB_FORCE_EMPTY,
    A1_NB_MISSING_RIGHT,
    A1_NB_ZERO_COLUMN_RIGHT,
} a1_nothing_case_t;

static void
test_nothing_to_bound_follows_the_ordinary_join(void)
{
    static const char *const names[] = {
        "forced-empty right operand", "missing right relation",
        "zero-column empty right relation",
    };

    wl_columnar_ops_test_before_reduce_dispose = a1_reduce_observer;
    for (int c = A1_NB_FORCE_EMPTY; c <= A1_NB_ZERO_COLUMN_RIGHT; c++) {
        for (int strict = 0; strict < 2; strict++) {
            wl_plan_op_t ops[2];
            wl_plan_relation_t plan;
            a1_run_t run;
            wl_col_session_t *sess = c == A1_NB_FORCE_EMPTY
                ? sel_case_session(SEL_FORCE_EMPTY)
                : c == A1_NB_MISSING_RIGHT ? pipeline_session()
                : sel_zero_column_session();

            CHECK(sess != NULL, "nothing-to-bound fixture");
            if (!sess)
                continue;
            memset(&plan, 0, sizeof(plan));
            a1_join(&ops[0], "right", c == A1_NB_FORCE_EMPTY
                ? WL_DELTA_FORCE_DELTA : WL_DELTA_AUTO);
            a1_count_by_k(&ops[1]);
            plan.ops = ops;
            plan.op_count = 2;
            sess->join_batch_strict = strict != 0;
            run = a1_eval(sess, &plan, SEL_BATCH_BYTES, NULL, NULL);
            if (run.rc != 0 || run.nrows != 0
                || sess->join_batch_fallback_count != 0) {
                a1_report(names[c], &run);
                fprintf(stderr, "  strict=%d fallbacks=%u\n", strict,
                    sess->join_batch_fallback_count);
            }
            CHECK(run.rc == 0 && run.nrows == 0 && a1_reduce_calls == 1,
                "a JOIN with nothing to bound returns the ordinary empty "
                "result in both modes");
            CHECK(sess->join_batch_fallback_count == 0,
                "a JOIN with nothing to bound records no fallback");
            pipeline_session_destroy(sess);
        }
    }
    wl_columnar_ops_test_before_reduce_dispose = NULL;

    /* A zero-column left still fails in the ordinary bounded JOIN (#2170);
     * this pins that until the issue is fixed. */
    for (int strict = 0; strict < 2; strict++) {
        sel_plan_t p;
        sel_run_t run;
        wl_col_session_t *sess = sel_session();
        col_rel_t *left = col_rel_new_auto("left", 0);

        CHECK(sess && left, "zero-column left fixture");
        if (!sess || !left) {
            col_rel_destroy(left);
            pipeline_session_destroy(sess);
            continue;
        }
        sel_plan_init(&p, WL_DELTA_FORCE_FULL);
        sess->join_batch_strict = strict != 0;
        run = sel_eval(sess, &p.plan, SEL_BATCH_BYTES, left);
        if (run.rc != EINVAL || run.stack_top != 0)
            fprintf(stderr, "  zero-column left, strict=%d: rc=%d top=%u\n",
                strict, run.rc, run.stack_top);
        CHECK(run.rc == EINVAL && run.stack_top == 0,
            "a zero-column left still fails with EINVAL (#2170)");
        pipeline_session_destroy(sess);
    }
}

static void
test_delta_right_decline_records_one_reason(void)
{
    static const int64_t want[][2] = { { 10, 101 } };
    sel_plan_t p;
    sel_run_t run;
    wl_col_session_t *sess = sel_case_session(SEL_FORCE_DELTA);

    CHECK(sess != NULL, "delta-right fixture");
    if (!sess)
        return;
    sel_plan_init(&p, WL_DELTA_FORCE_DELTA);
    run = sel_eval(sess, &p.plan, SEL_BATCH_BYTES, NULL);
    CHECK(sel_rows_are(&run, want, 1), "a delta right operand joins $d$");
    if (sess->join_batch_fallback_count != 1)
        fprintf(stderr, "  non-strict delta right: %u fallbacks\n",
            sess->join_batch_fallback_count);
    CHECK(sess->join_batch_fallback_count == 1
        && sess->join_batch_last_reason
        == COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT,
        "a delta right operand records delta-right once");
    pipeline_session_destroy(sess);

    sess = sel_case_session(SEL_FORCE_DELTA);
    CHECK(sess != NULL, "strict delta-right fixture");
    if (sess) {
        sess->join_batch_strict = true;
        run = sel_eval(sess, &p.plan, SEL_BATCH_BYTES, NULL);
        CHECK(run.rc == ENOTSUP && run.left_kept
            && sess->join_batch_fallback_count == 1
            && sess->join_batch_last_reason
            == COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT,
            "strict mode refuses a delta right operand as delta-right");
        pipeline_session_destroy(sess);
    }

    /* The operand decides before the shape: a delta right operand feeding
     * a REDUCE is a delta-right refusal, not pipeline-not-consumable. */
    sess = sel_case_session(SEL_FORCE_DELTA);
    CHECK(sess != NULL, "strict delta-right REDUCE fixture");
    if (sess) {
        wl_plan_op_t ops[2];
        wl_plan_relation_t plan;
        a1_run_t a1;

        memset(&plan, 0, sizeof(plan));
        a1_join(&ops[0], "right", WL_DELTA_FORCE_DELTA);
        a1_count_by_k(&ops[1]);
        plan.ops = ops;
        plan.op_count = 2;
        sess->join_batch_strict = true;
        a1 = a1_eval(sess, &plan, SEL_BATCH_BYTES, NULL, NULL);
        if (sess->join_batch_last_reason
            != COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT)
            fprintf(stderr, "  delta right -> REDUCE: rc=%d reason=%d\n",
                a1.rc, (int)sess->join_batch_last_reason);
        CHECK(a1.rc == ENOTSUP && a1.left_kept
            && sess->join_batch_last_reason
            == COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT,
            "a delta right operand is refused as delta-right whatever "
            "follows the JOIN");
        pipeline_session_destroy(sess);
    }
}

static wl_columnar_memory_governor_ref_t *
a1_governor(uint64_t budget)
{
    wl_columnar_memory_resolution_t resolution;
    memset(&resolution, 0, sizeof(resolution));
    resolution.budget_bytes = budget;
    resolution.usable_bytes = budget;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static uint64_t
a1_reserved(wl_columnar_memory_governor_ref_t *governor)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(governor));
}

#define A1_FANOUT 300u

/* One left row against A1_FANOUT right rows on the same key, with the
 * arrangement built before @governor is attached. */
static wl_col_session_t *
a1_fanout_session(wl_columnar_memory_governor_ref_t *governor,
    uint64_t batch_bytes)
{
    static const char *const right_names[] = { "k", "r" };
    int64_t rows[A1_FANOUT * 2u];
    wl_col_session_t *sess = pipeline_session();
    col_rel_t *right;

    for (uint32_t i = 0; i < A1_FANOUT; i++) {
        rows[2u * i] = 0;
        rows[2u * i + 1u] = i;
    }
    right = pipeline_relation("right", right_names, 2, rows, A1_FANOUT);
    if (!sess || !right || session_add_rel(sess, right) != 0) {
        col_rel_destroy(right);
        pipeline_session_destroy(sess);
        return NULL;
    }
    if (!col_session_get_arrangement(&sess->base, "right",
        (const uint32_t[]){ 0 }, 1)) {
        pipeline_session_destroy(sess);
        return NULL;
    }
    sess->memory_governor = governor;
    sess->join_batch_bytes = (uint32_t)batch_bytes;
    return sess;
}

static void
a1_fanout_session_destroy(wl_col_session_t *sess)
{
    if (sess)
        sess->memory_governor = NULL;
    pipeline_session_destroy(sess);
}

static a1_run_t
a1_fanout_reduce(uint64_t budget, uint64_t batch_bytes)
{
    static const char *const left_names[] = { "k", "v" };
    static const int64_t left_rows[] = { 0, 1 };
    wl_columnar_memory_governor_ref_t *governor = a1_governor(budget);
    wl_col_session_t *sess = governor
        ? a1_fanout_session(governor, batch_bytes) : NULL;
    wl_plan_op_t ops[2];
    wl_plan_relation_t plan;
    a1_run_t run;

    memset(&run, 0, sizeof(run));
    run.rc = ENOMEM;
    if (sess) {
        memset(&plan, 0, sizeof(plan));
        a1_join(&ops[0], "right", WL_DELTA_AUTO);
        a1_count_by_k(&ops[1]);
        plan.ops = ops;
        plan.op_count = 2;
        run = a1_eval(sess, &plan, (uint32_t)batch_bytes, NULL,
                pipeline_relation("left", left_names, 2, left_rows, 1));
    }
    a1_fanout_session_destroy(sess);
    if (governor)
        wl_columnar_memory_governor_ref_release(governor);
    return run;
}

static void
test_bounded_join_growth_denial_before_reduce(void)
{
    static const char *const probe_names[] = { "a", "b", "c", "d" };
    static const char *const left_names[] = { "k", "v" };
    static const int64_t left_rows[] = { 0, 1 };
    static const int64_t want_count[] = { 0, A1_FANOUT };
    col_rel_t *probe = pipeline_relation("probe", probe_names, 4, NULL, 0);
    wl_columnar_memory_governor_ref_t *governor = a1_governor(1u << 20);
    wl_col_session_t *sess = NULL;
    col_rel_t *left = NULL;
    wl_columnar_continuation_t *producer = NULL;
    uint64_t row_bytes = 0;
    uint64_t initial = 0;
    uint64_t grown = 0;
    uint64_t base = 0;
    uint64_t producer_bytes = 0;
    uint64_t batch_bytes;
    uint64_t budget;
    uint32_t lk = 0;
    uint32_t rk = 0;
    wl_plan_op_t join;
    a1_run_t run;

    CHECK(probe && governor, "growth-denial fixture");
    if (!probe || !governor
        || !col_rel_retained_bytes_for(probe, 1, &row_bytes)
        || !col_rel_retained_bytes_for(probe, COL_REL_INIT_CAP, &initial)
        || !col_rel_retained_bytes_for(probe, 2u * COL_REL_INIT_CAP,
        &grown)) {
        CHECK(false, "growth-denial sizes");
        col_rel_destroy(probe);
        if (governor)
            wl_columnar_memory_governor_ref_release(governor);
        return;
    }
    col_rel_destroy(probe);
    /* 64 rows per batch is the relation's initial capacity, so the
     * producer's scratch never grows. */
    batch_bytes = (uint64_t)COL_REL_INIT_CAP * row_bytes;

    /* Measure the producer's admission on the same fixture. */
    sess = a1_fanout_session(governor, batch_bytes);
    left = pipeline_relation("left", left_names, 2, left_rows, 1);
    a1_join(&join, "right", WL_DELTA_AUTO);
    if (sess && left) {
        base = a1_reserved(governor);
        if (col_join_batch_producer_create(sess, &join, left, false, &lk,
            &rk, 1, (uint32_t)batch_bytes, &producer) == 0)
            producer_bytes = a1_reserved(governor) - base;
        wl_columnar_continuation_destroy(producer);
    }
    col_rel_destroy(left);
    a1_fanout_session_destroy(sess);
    wl_columnar_memory_governor_ref_release(governor);
    CHECK(producer_bytes > 0 && grown > initial,
        "growth-denial premises: the producer is admitted and the output "
        "grows");

    /* The producer and the initial output fit; a later growth of the
     * bounded output is denied.  A budget this far above the producer
     * stays clear of the earlier denials that set memory_budget_denied. */
    budget = base + producer_bytes + initial + grown / 2u;
    wl_columnar_ops_test_before_reduce_dispose = a1_reduce_observer;
    run = a1_fanout_reduce(budget, batch_bytes);
    if (run.rc != ENOSPC || run.stack_top != 0 || a1_reduce_calls != 0) {
        a1_report("growth denial", &run);
        fprintf(stderr, "  budget=%llu reduce_calls=%u\n",
            (unsigned long long)budget, a1_reduce_calls);
    }
    CHECK(run.rc == ENOSPC && run.stack_top == 0 && a1_reduce_calls == 0,
        "a denied growth of the bounded join output fails with ENOSPC "
        "before the REDUCE runs");

    run = a1_fanout_reduce(1u << 20, batch_bytes);
    CHECK(a1_rows_are(&run, 2, want_count, 1) && a1_reduce_calls == 1,
        "the same JOIN -> REDUCE completes under a large budget");
    wl_columnar_ops_test_before_reduce_dispose = NULL;
}

/* Walk every relation plan, and the K-Fusion copies inside it, calling
 * @check on each operator list. */
typedef void (*a1_ops_check_t)(const wl_plan_op_t *ops, uint32_t n,
    void *context);

static void
a1_walk_plan(const wl_plan_t *plan, a1_ops_check_t check, void *context)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_relation_t *rel = &st->relations[r];
            check(rel->ops, rel->op_count, context);
            for (uint32_t i = 0; i < rel->op_count; i++) {
                const wl_plan_op_k_fusion_t *kf;
                if (rel->ops[i].op != WL_PLAN_OP_K_FUSION
                    || !rel->ops[i].opaque_data)
                    continue;
                kf = (const wl_plan_op_k_fusion_t *)rel->ops[i].opaque_data;
                for (uint32_t d = 0; d < kf->k; d++)
                    check(kf->k_ops[d], kf->k_op_counts[d], context);
            }
        }
    }
}

typedef struct {
    uint32_t joins;
    uint32_t sinks;
    uint32_t reduce_successors;
    uint32_t unknown_ops;
    uint32_t tail_sinks;
    uint32_t tails;
} a1_census_t;

static void
a1_census_ops(const wl_plan_op_t *ops, uint32_t n, void *context)
{
    a1_census_t *census = (a1_census_t *)context;
    wl_plan_relation_t plan;

    memset(&plan, 0, sizeof(plan));
    plan.ops = ops;
    plan.op_count = n;
    for (uint32_t i = 0; i < n; i++) {
        uint32_t t;
        if ((uint32_t)ops[i].op > (uint32_t)WL_PLAN_OP_EXCHANGE)
            census->unknown_ops++;
        if (ops[i].op != WL_PLAN_OP_JOIN)
            continue;
        census->joins++;
        if (wl_columnar_join_pipeline_successor(&plan, i + 1u)
            == WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK)
            census->sinks++;
        else if (i + 1u < n && ops[i + 1u].op == WL_PLAN_OP_REDUCE)
            census->reduce_successors++;
        t = i + 1u;
        while (t < n && ops[t].op == WL_PLAN_OP_FILTER)
            t++;
        if (t < n && ops[t].op == WL_PLAN_OP_MAP) {
            census->tails++;
            if (wl_columnar_join_pipeline_successor(&plan, t + 1u)
                == WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK)
                census->tail_sinks++;
        }
    }
}

static void
a1_census_dump(const wl_plan_op_t *ops, uint32_t n, void *context)
{
    (void)context;
    fprintf(stderr, "   ");
    for (uint32_t i = 0; i < n; i++)
        fprintf(stderr, " %d", (int)ops[i].op);
    fprintf(stderr, "\n");
}

static bool
a1_census(const char *source, a1_census_t *census)
{
    wirelog_error_t error = WIRELOG_OK;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    bool ok;

    memset(census, 0, sizeof(*census));
    ok = program && wirelog_optimize(program, &error)
        && wl_plan_from_program(program, &plan) == 0 && plan;
    if (ok)
        a1_walk_plan(plan, a1_census_ops, census);
    if (ok && census->unknown_ops != 0)
        a1_walk_plan(plan, a1_census_dump, NULL);
    wl_plan_free(plan);
    wirelog_program_free(program);
    return ok;
}

static void
test_successor_census_programs(void)
{
    static const char tc[] =
        ".decl e(x: int32, y: int32)\n"
        ".decl p(x: int32, y: int32)\n"
        "p(x, y) :- e(x, y).\n"
        "p(x, z) :- e(x, y), p(y, z).\n";
    static const char counted[] =
        ".decl e(x: int32, z: int32)\n"
        ".decl f(z: int32, y: int32)\n"
        ".decl c(x: int32, n: int32)\n"
        "c(x, count(y)) :- e(x, z), f(z, y).\n";
    static const char sg[] =
        ".decl par(x: int32, y: int32)\n"
        ".decl sg(x: int32, y: int32, d: int32)\n"
        "sg(x, y, 0) :- par(p, x), par(p, y), x != y.\n"
        "sg(x, y, d + 1) :- par(a, x), sg(a, b, d), par(b, y), d < 5.\n";
    a1_census_t census;

    CHECK(a1_census(tc, &census), "census: tc lowers");
    if (census.joins == 0 || census.sinks != census.joins
        || census.unknown_ops != 0)
        fprintf(stderr, "  tc: joins=%u sinks=%u unknown=%u\n",
            census.joins, census.sinks, census.unknown_ops);
    CHECK(census.joins > 0 && census.sinks == census.joins
        && census.unknown_ops == 0,
        "census: every tc JOIN feeds a relation sink");

    CHECK(a1_census(counted, &census), "census: count rule lowers");
    if (census.reduce_successors == 0 || census.unknown_ops != 0)
        fprintf(stderr, "  count: joins=%u sinks=%u reduce=%u\n",
            census.joins, census.sinks, census.reduce_successors);
    CHECK(census.reduce_successors > 0 && census.unknown_ops == 0,
        "census: a JOIN feeding a REDUCE is materializing");

    CHECK(a1_census(sg, &census), "census: sg lowers");
    if (census.tails == 0 || census.tail_sinks != census.tails
        || census.unknown_ops != 0)
        fprintf(stderr, "  sg: joins=%u tails=%u tail_sinks=%u\n",
            census.joins, census.tails, census.tail_sinks);
    CHECK(census.tails > 0 && census.tail_sinks == census.tails
        && census.unknown_ops == 0,
        "census: a JOIN -> FILTER* -> MAP tail feeds a relation sink");
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
    test_pipeline_right_selection_matches_join();
    test_pipeline_key_types_match_join();
    test_pipeline_zero_column_right_declines();
    test_pipeline_zero_column_left_declines();
    test_successor_classifier_table();
    test_relation_sink_successor_is_not_a_fallback();
    test_materializing_boundaries_are_explicit();
    test_nothing_to_bound_follows_the_ordinary_join();
    test_delta_right_decline_records_one_reason();
    test_bounded_join_growth_denial_before_reduce();
    test_successor_census_programs();
    if (failures)
        fprintf(stderr, "%d join pipeline preflight checks failed\n",
            failures);
    return failures ? 1 : 0;
}
