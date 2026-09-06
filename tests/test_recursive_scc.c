/*
 * Recursive SCC completeness and forced-delta skip contracts (#1376).
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <string.h>

static int failures;

#define CHECK(condition)                                                     \
        do {                                                                     \
            if (!(condition)) {                                                  \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);   \
                failures++;                                                      \
            }                                                                    \
        } while (0)

#define SCC_SEED                                               \
        ".decl e(x: int64, y: int64)\n"                            \
        ".decl q(x: int64, y: int64, z: int64)\n"                  \
        ".decl p(x: int64, y: int64)\n"                            \
        ".decl r(x: int64, y: int64, z: int64)\n"                  \
        ".output p\n.output r\n"                                  \
        "e(1,2). e(2,3). e(3,4). e(4,5). e(5,6). e(6,7).\n"    \
        "e(7,8). e(8,9). e(9,10). e(10,11). e(11,12).\n"        \
        "q(1,7,3). q(2,7,5).\n"

static const char original[] = SCC_SEED
    "p(x, y) :- e(x, y).\n"
    "r(x, y, z) :- q(x, y, z).\n"
    "p(x, z) :- p(x, y), p(y, z).\n"
    "r(x, k, z) :- p(x, y), r(y, k, z).\n"
    "p(x, z) :- r(x, k, y), e(y, z).\n";

static const char reordered[] = SCC_SEED
    "r(x, y, z) :- q(x, y, z).\n"
    "r(x, k, z) :- p(x, y), r(y, k, z).\n"
    "p(x, z) :- r(x, k, y), e(y, z).\n"
    "p(x, z) :- p(x, y), p(y, z).\n"
    "p(x, y) :- e(x, y).\n";

struct results {
    bool p[13][13];
    bool r[3];
    unsigned count;
};

static void
collect(const char *relation, const int64_t *row, uint32_t ncols, void *data)
{
    struct results *result = data;
    result->count++;
    if (strcmp(relation, "p") == 0) {
        bool valid = ncols == 2 && row[0] >= 1 && row[0] < row[1]
            && row[1] <= 12;
        CHECK(valid);
        if (valid) {
            CHECK(!result->p[row[0]][row[1]]);
            result->p[row[0]][row[1]] = true;
        }
    } else if (strcmp(relation, "r") == 0) {
        static const int64_t expected[3][3] = {
            { 1, 7, 3 }, { 2, 7, 5 }, { 1, 7, 5 }
        };
        bool found = false;
        for (unsigned i = 0; i < 3; i++) {
            if (ncols == 3 && memcmp(row, expected[i],
                sizeof(expected[i])) == 0) {
                CHECK(!result->r[i]);
                result->r[i] = true;
                found = true;
            }
        }
        CHECK(found);
    } else {
        CHECK(false);
    }
}

static void
check_scc(const char *src, uint32_t workers, bool pin_iterations)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    CHECK(prog != NULL);
    if (!prog)
        return;
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    if (wl_fusion_apply(prog, NULL) != 0 || wl_jpp_apply(prog, NULL) != 0
        || wl_sip_apply(prog, NULL) != 0) {
        CHECK(false);
        goto cleanup;
    }
    int rc = wl_plan_from_program(prog, &plan);
    CHECK(rc == 0);
    if (rc != 0)
        goto cleanup;
    rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
    CHECK(rc == 0);
    if (rc != 0)
        goto cleanup;
    rc = wl_session_load_facts(sess, prog);
    CHECK(rc == 0);
    if (rc != 0)
        goto cleanup;
    struct results result = { 0 };
    rc = wl_session_snapshot(sess, collect, &result);
    CHECK(rc == 0);
    CHECK(result.count == 69);
    for (unsigned x = 1; x < 12; x++)
        for (unsigned y = x + 1; y <= 12; y++)
            CHECK(result.p[x][y]);
    for (unsigned i = 0; i < 3; i++)
        CHECK(result.r[i]);
    uint32_t iterations = col_session_get_iteration_count(sess);
    printf("workers=%u reordered=%u rows=%u iterations=%u\n", workers,
        (unsigned)!pin_iterations, result.count, iterations);
    if (pin_iterations) {
        /* This fixture has exactly one recursive stratum and no later
         * stratum to reset current_iteration. The terminal empty round is
         * index 5 (six rounds). The accessor retains the last productive
         * index, 4, because the terminal round breaks before updating it. */
        CHECK(plan->stratum_count == 1);
        CHECK(COL_SESSION(sess)->current_iteration + 1 == 6);
        CHECK(iterations == 4);
    }
cleanup:
    if (sess)
        wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
}

static void
check_skip_contract(void)
{
    /* Real session lookup with stack-owned relations; no evaluation needs rows. */
    col_rel_t delta = { .name = "$d$p", .nrows = 1 };
    col_rel_t retraction = { .name = "$r$p", .nrows = 0 };
    col_rel_t *rels[] = { &delta, &retraction };
    wl_col_session_t sess = { .rels = rels, .nrels = 2, .rel_cap = 2 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "missing",
          .delta_mode = WL_DELTA_FORCE_DELTA },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "p",
          .delta_mode = WL_DELTA_FORCE_DELTA },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_relation_t plan = { .ops = ops, .op_count = 3 };
    /* The missing early alternative cannot suppress the later live one. */
    CHECK(!has_empty_forced_delta(&plan, &sess, 1));
    plan.op_count = 1;
    CHECK(has_empty_forced_delta(&plan, &sess, 1));
    CHECK(!has_empty_forced_delta(&plan, &sess, 0));
    sess.delta_seeded = true;
    CHECK(has_empty_forced_delta(&plan, &sess, 0));
    plan.ops = &ops[1];
    CHECK(!has_empty_forced_delta(&plan, &sess, 0));
    delta.nrows = 0;
    CHECK(has_empty_forced_delta(&plan, &sess, 0));
    delta.nrows = 1;
    sess.delta_seeded = false;
    sess.retraction_seeded = true;
    CHECK(has_empty_forced_delta(&plan, &sess, 0));
    CHECK(!has_empty_forced_delta(&plan, &sess, 1));
    retraction.nrows = 1;
    CHECK(!has_empty_forced_delta(&plan, &sess, 0));
    ops[1].right_relation = "missing";
    ops[1].op = WL_PLAN_OP_JOIN;
    CHECK(has_empty_forced_delta(&plan, &sess, 1));
    ops[1].right_relation = "p";
    CHECK(!has_empty_forced_delta(&plan, &sess, 1));
    ops[1].op = WL_PLAN_OP_SEMIJOIN;
    CHECK(!has_empty_forced_delta(&plan, &sess, 1));
    delta.nrows = 0;
    CHECK(has_empty_forced_delta(&plan, &sess, 1));
    session_rel_free_hash(&sess);
}

static void
check_empty_join(bool differential, bool present_delta, bool projected,
    bool owned, bool pooled)
{
    delta_pool_t *pool = pooled
        ? delta_pool_create(4, sizeof(col_rel_t), 4096) : NULL;
    CHECK(!pooled || pool != NULL);
    if (pooled && !pool)
        return;
    col_rel_t *left = pooled ? col_rel_pool_new_auto(pool, NULL, "left", 1)
        : col_rel_new_auto("left", 1);
    col_rel_t *right = col_rel_new_auto("right", 1);
    col_rel_t *delta = col_rel_new_auto("$d$right", 1);
    CHECK(left && right && delta);
    if (!left || !right || !delta) {
        col_rel_destroy(left);
        col_rel_destroy(right);
        col_rel_destroy(delta);
        delta_pool_destroy(pool);
        return;
    }
    const wirelog_column_type_t left_type[] = { WIRELOG_TYPE_FLOAT };
    const wirelog_column_type_t right_type[] = { WIRELOG_TYPE_INT64 };
    CHECK(col_rel_set_column_types(left, left_type, 1) == 0);
    CHECK(col_rel_set_column_types(right, right_type, 1) == 0);
    int64_t row[] = { 0 };
    CHECK(col_rel_append_row(right, row) == 0);
    col_rel_t *rels[] = { right, delta };
    wl_col_session_t sess = {
        .rels = rels, .nrels = present_delta ? 2 : 1,
        .rel_cap = 2, .current_iteration = 1
    };
    uint32_t projection[] = { 1, 0 };
    wl_plan_op_t op = {
        .op = WL_PLAN_OP_JOIN, .right_relation = "right",
        .delta_mode = WL_DELTA_FORCE_DELTA,
        .project_indices = projected ? projection : NULL,
        .project_count = projected ? 2 : 0
    };
    eval_stack_t stack;
    eval_stack_init(&stack);
    int rc = eval_stack_push(&stack, left, owned);
    CHECK(rc == 0);
    if (rc == 0) {
        rc = differential ? wl_columnar_join_diff_op(&op, &stack, &sess)
            : wl_columnar_join_op(&op, &stack, &sess);
        CHECK(rc == 0);
        CHECK(stack.top == (rc == 0 ? 1u : 0u));
        if (rc == 0) {
            eval_entry_t result = eval_stack_pop(&stack);
            CHECK(result.owned && result.rel != NULL);
            if (result.rel) {
                CHECK(result.rel->nrows == 0 && result.rel->ncols == 2);
                CHECK(result.rel->column_types != NULL);
                if (result.rel->column_types && result.rel->ncols == 2) {
                    CHECK(result.rel->column_types[projected ? 1 : 0]
                        == WIRELOG_TYPE_FLOAT);
                    CHECK(result.rel->column_types[projected ? 0 : 1]
                        == WIRELOG_TYPE_INT64);
                }
                col_rel_destroy(result.rel);
            }
        }
    } else {
        col_rel_destroy(left);
        left = NULL;
    }
    eval_stack_drain(&stack);
    if (!owned && left) {
        CHECK(left->ncols == 1 && left->column_types[0] == WIRELOG_TYPE_FLOAT);
        col_rel_destroy(left);
    }
    session_rel_free_hash(&sess);
    col_rel_destroy(right);
    col_rel_destroy(delta);
    delta_pool_destroy(pool);
}

int
main(void)
{
    check_scc(original, 1, true);
    check_scc(original, 8, true);
    check_scc(reordered, 1, false);
    check_scc(reordered, 8, false);
    check_skip_contract();
    for (unsigned bits = 0; bits < 32; bits++)
        check_empty_join((bits & 1u) != 0, (bits & 2u) != 0,
            (bits & 4u) != 0, (bits & 8u) != 0, (bits & 16u) != 0);
    return failures != 0;
}
