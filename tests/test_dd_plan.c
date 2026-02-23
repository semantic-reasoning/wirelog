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

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
