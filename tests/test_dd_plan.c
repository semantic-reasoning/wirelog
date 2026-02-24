/*
 * test_dd_plan.c - Tests for DD Execution Plan
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/ir/dd_plan.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/wirelog-parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
    do {                                      \
        tests_run++;                          \
        printf("  [%d] %s", tests_run, name); \
    } while (0)

#define PASS()                 \
    do {                       \
        tests_passed++;        \
        printf(" ... PASS\n"); \
    } while (0)

#define FAIL(msg)                         \
    do {                                  \
        tests_failed++;                   \
        printf(" ... FAIL: %s\n", (msg)); \
    } while (0)

/* ======================================================================== */
/* DD Plan Type Tests                                                       */
/* ======================================================================== */

static void
test_plan_types_exist(void)
{
    TEST("DD plan types: structs have non-zero size");

    if (sizeof(wl_dd_op_t) == 0) {
        FAIL("wl_dd_op_t has zero size");
        return;
    }

    if (sizeof(wl_dd_relation_plan_t) == 0) {
        FAIL("wl_dd_relation_plan_t has zero size");
        return;
    }

    if (sizeof(wl_dd_stratum_plan_t) == 0) {
        FAIL("wl_dd_stratum_plan_t has zero size");
        return;
    }

    if (sizeof(wl_dd_plan_t) == 0) {
        FAIL("wl_dd_plan_t has zero size");
        return;
    }

    PASS();
}

static void
test_plan_null_program(void)
{
    TEST("DD plan generate: NULL program returns -2");

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(NULL, &plan);

    if (rc != -2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected rc=-2, got %d", rc);
        FAIL(buf);
        return;
    }

    if (plan != NULL) {
        wl_dd_plan_free(plan);
        FAIL("plan should be NULL on error");
        return;
    }

    PASS();
}

static void
test_plan_free_null(void)
{
    TEST("DD plan free: NULL does not crash");

    wl_dd_plan_free(NULL);

    PASS();
}

static void
test_op_type_str(void)
{
    TEST("DD op type str: all types have names");

    if (strcmp(wl_dd_op_type_str(WL_DD_VARIABLE), "VARIABLE") != 0) {
        FAIL("VARIABLE name wrong");
        return;
    }

    if (strcmp(wl_dd_op_type_str(WL_DD_JOIN), "JOIN") != 0) {
        FAIL("JOIN name wrong");
        return;
    }

    if (strcmp(wl_dd_op_type_str(WL_DD_CONSOLIDATE), "CONSOLIDATE") != 0) {
        FAIL("CONSOLIDATE name wrong");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* EDB Collection Tests                                                     */
/* ======================================================================== */

static void
test_plan_edb_only(void)
{
    TEST("DD plan: EDB-only program -> edb_count = 1, 1 stratum");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n", &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected rc=0, got %d", rc);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!plan) {
        wirelog_program_free(prog);
        FAIL("plan is NULL");
        return;
    }

    if (plan->edb_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 EDB, got %u", plan->edb_count);
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (strcmp(plan->edb_relations[0], "a") != 0) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("EDB relation should be 'a'");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_simple_scan(void)
{
    TEST("DD plan: r(x) :- a(x). -> VARIABLE(a) op");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    /* Should have 1 stratum with relation "r" */
    if (plan->stratum_count < 1) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected >= 1 stratum");
        return;
    }

    /* Find relation "r" in any stratum */
    bool found_var = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            wl_dd_relation_plan_t *rp = &plan->strata[s].relations[r];
            if (strcmp(rp->name, "r") == 0) {
                /* First op should be VARIABLE("a") */
                if (rp->op_count >= 1 && rp->ops[0].op == WL_DD_VARIABLE
                    && rp->ops[0].relation_name
                    && strcmp(rp->ops[0].relation_name, "a") == 0) {
                    found_var = true;
                }
            }
        }
    }

    if (!found_var) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected VARIABLE(a) op for relation r");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_edb_list(void)
{
    TEST("DD plan: 3 EDB relations -> edb_count = 3");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl b(x: int32)\n"
                                                   ".decl c(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    /* a, b, c are EDB (no rules), r is IDB */
    if (plan->edb_count != 3) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 3 EDB, got %u", plan->edb_count);
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_stratum_count(void)
{
    TEST("DD plan: chain b->a, c->b -> 2 strata");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl b(x: int32)\n"
                                                   ".decl c(x: int32)\n"
                                                   "b(x) :- a(x).\n"
                                                   "c(x) :- b(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    if (plan->stratum_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 strata, got %u",
                 plan->stratum_count);
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Helper: find relation plan by name                                       */
/* ======================================================================== */

static wl_dd_relation_plan_t *
find_relation_plan(const wl_dd_plan_t *plan, const char *name)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            if (strcmp(plan->strata[s].relations[r].name, name) == 0)
                return &plan->strata[s].relations[r];
        }
    }
    return NULL;
}

/* ======================================================================== */
/* FILTER and PROJECT Translation Tests                                     */
/* ======================================================================== */

static void
test_plan_filter(void)
{
    TEST("DD plan: r(x) :- a(x), x > 5. -> VARIABLE + FILTER");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x), x > 5.\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* IR tree: PROJECT(FILTER(SCAN)) -> post-order: VARIABLE, FILTER, MAP */
    /* Check that a FILTER op exists with non-NULL filter_expr */
    bool found_filter = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_DD_FILTER) {
            if (rp->ops[i].filter_expr != NULL)
                found_filter = true;
            else {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("FILTER op has NULL filter_expr");
                return;
            }
        }
    }

    if (!found_filter) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected FILTER op for r(x) :- a(x), x > 5.");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_project(void)
{
    TEST("DD plan: r(x) :- a(x, y). -> VARIABLE + MAP");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* IR tree: PROJECT(SCAN) -> post-order: VARIABLE, MAP */
    bool found_map = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_DD_MAP) {
            if (rp->ops[i].project_count > 0)
                found_map = true;
            else {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("MAP op has project_count == 0");
                return;
            }
        }
    }

    if (!found_map) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected MAP op for r(x) :- a(x, y).");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_filter_project(void)
{
    TEST("DD plan: r(x) :- a(x,y), y>0. -> VARIABLE, FILTER, MAP order");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* IR tree: PROJECT(FILTER(SCAN))
     * Post-order: VARIABLE, FILTER, MAP */
    if (rp->op_count < 3) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 3 ops, got %u", rp->op_count);
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (rp->ops[0].op != WL_DD_VARIABLE) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("op[0] should be VARIABLE");
        return;
    }

    if (rp->ops[1].op != WL_DD_FILTER || rp->ops[1].filter_expr == NULL) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("op[1] should be FILTER with non-NULL filter_expr");
        return;
    }

    if (rp->ops[2].op != WL_DD_MAP || rp->ops[2].project_count == 0) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("op[2] should be MAP with project_count > 0");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* JOIN Translation Tests                                                   */
/* ======================================================================== */

static void
test_plan_join(void)
{
    TEST("DD plan: r(x,z) :- a(x,y), b(y,z). -> JOIN with keys");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, z: int32)\n"
                               ".decl r(x: int32, z: int32)\n"
                               "r(x, z) :- a(x, y), b(y, z).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* IR: PROJECT(JOIN(SCAN_a, SCAN_b))
     * Post-order: VARIABLE(a), VARIABLE(b), JOIN, MAP */
    bool found_join = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_DD_JOIN) {
            if (rp->ops[i].key_count < 1) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("JOIN op has key_count == 0");
                return;
            }
            if (!rp->ops[i].right_relation) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("JOIN op has NULL right_relation");
                return;
            }
            if (strcmp(rp->ops[i].right_relation, "b") != 0) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("JOIN right_relation should be 'b'");
                return;
            }
            found_join = true;
        }
    }

    if (!found_join) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected JOIN op");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_join_keys(void)
{
    TEST("DD plan: JOIN copies left_keys and right_keys");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, z: int32)\n"
                               ".decl r(x: int32, z: int32)\n"
                               "r(x, z) :- a(x, y), b(y, z).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* Find JOIN op and check keys */
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_DD_JOIN) {
            if (rp->ops[i].key_count != 1 || !rp->ops[i].left_keys
                || !rp->ops[i].right_keys) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("JOIN should have 1 key pair with non-NULL arrays");
                return;
            }
            if (strcmp(rp->ops[i].left_keys[0], "y") != 0) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("JOIN left_keys[0] should be 'y'");
                return;
            }
            if (strcmp(rp->ops[i].right_keys[0], "y") != 0) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("JOIN right_keys[0] should be 'y'");
                return;
            }

            wl_dd_plan_free(plan);
            wirelog_program_free(prog);
            PASS();
            return;
        }
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    FAIL("JOIN op not found");
}

/* ======================================================================== */
/* UNION, ANTIJOIN, AGGREGATE Translation Tests                             */
/* ======================================================================== */

static void
test_plan_union(void)
{
    TEST("DD plan: r(x):-a(x). r(x):-b(x). -> CONCAT + CONSOLIDATE");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl b(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n"
                                                   "r(x) :- b(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* UNION(PROJECT(SCAN_a), PROJECT(SCAN_b))
     * Should produce CONCAT and CONSOLIDATE ops */
    bool found_concat = false;
    bool found_consolidate = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_DD_CONCAT)
            found_concat = true;
        if (rp->ops[i].op == WL_DD_CONSOLIDATE)
            found_consolidate = true;
    }

    if (!found_concat) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected CONCAT op for union");
        return;
    }

    if (!found_consolidate) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected CONSOLIDATE op for union");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_antijoin(void)
{
    TEST("DD plan: r(x) :- a(x), !b(x). -> ANTIJOIN");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl b(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x), !b(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* PROJECT(ANTIJOIN(SCAN_a, SCAN_b)) */
    bool found_antijoin = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_DD_ANTIJOIN) {
            if (!rp->ops[i].right_relation) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("ANTIJOIN has NULL right_relation");
                return;
            }
            if (strcmp(rp->ops[i].right_relation, "b") != 0) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("ANTIJOIN right_relation should be 'b'");
                return;
            }
            found_antijoin = true;
        }
    }

    if (!found_antijoin) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected ANTIJOIN op");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_aggregate(void)
{
    TEST("DD plan: r(x, count(y)) :- a(x,y). -> REDUCE");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32, c: int32)\n"
                               "r(x, count(y)) :- a(x, y).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    wl_dd_relation_plan_t *rp = find_relation_plan(plan, "r");
    if (!rp) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("relation 'r' not found in plan");
        return;
    }

    /* AGGREGATE(SCAN) -> VARIABLE, REDUCE */
    bool found_reduce = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_DD_REDUCE) {
            if (rp->ops[i].group_by_count == 0) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("REDUCE has group_by_count == 0");
                return;
            }
            if (!rp->ops[i].group_by_indices) {
                wl_dd_plan_free(plan);
                wirelog_program_free(prog);
                FAIL("REDUCE has NULL group_by_indices");
                return;
            }
            found_reduce = true;
        }
    }

    if (!found_reduce) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected REDUCE op");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Recursive Stratum and Multi-Stratum Tests                                */
/* ======================================================================== */

static void
test_plan_recursive_stratum(void)
{
    TEST("DD plan: recursive tc(x,y) -> is_recursive = true");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(x: int32, y: int32)\n"
                               ".decl tc(x: int32, y: int32)\n"
                               "tc(x, y) :- edge(x, y).\n"
                               "tc(x, y) :- tc(x, z), edge(z, y).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    /* tc is recursive (self-loop), its stratum should be is_recursive */
    bool found_recursive = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        if (plan->strata[s].is_recursive) {
            /* Check that tc is in this stratum */
            for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
                if (strcmp(plan->strata[s].relations[r].name, "tc") == 0)
                    found_recursive = true;
            }
        }
    }

    if (!found_recursive) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("expected recursive stratum containing 'tc'");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_non_recursive_stratum(void)
{
    TEST("DD plan: non-recursive r(x) :- a(x). -> is_recursive = false");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    /* No stratum should be recursive */
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        if (plan->strata[s].is_recursive) {
            wl_dd_plan_free(plan);
            wirelog_program_free(prog);
            FAIL("no stratum should be recursive");
            return;
        }
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_multi_stratum_ordering(void)
{
    TEST("DD plan: chain a->b->c -> strata ordered by ID");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl b(x: int32)\n"
                                                   ".decl c(x: int32)\n"
                                                   "b(x) :- a(x).\n"
                                                   "c(x) :- b(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);

    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }

    if (plan->stratum_count < 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 2 strata, got %u",
                 plan->stratum_count);
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Strata should be ordered: stratum_id[0] < stratum_id[1] */
    if (plan->strata[0].stratum_id >= plan->strata[1].stratum_id) {
        wl_dd_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("strata should be ordered by stratum_id");
        return;
    }

    wl_dd_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog DD Plan Tests ===\n\n");

    /* Type and skeleton tests */
    test_plan_types_exist();
    test_plan_null_program();
    test_plan_free_null();
    test_op_type_str();

    /* EDB collection and SCAN translation */
    test_plan_edb_only();
    test_plan_simple_scan();
    test_plan_edb_list();
    test_plan_stratum_count();

    /* FILTER and PROJECT translation */
    test_plan_filter();
    test_plan_project();
    test_plan_filter_project();

    /* JOIN translation */
    test_plan_join();
    test_plan_join_keys();

    /* UNION, ANTIJOIN, AGGREGATE translation */
    test_plan_union();
    test_plan_antijoin();
    test_plan_aggregate();

    /* Recursive stratum and multi-stratum */
    test_plan_recursive_stratum();
    test_plan_non_recursive_stratum();
    test_plan_multi_stratum_ordering();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
