/*
 * test_dd_plan.c - Tests for DD Execution Plan
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/ir/dd_plan.h"
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

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
