/*
 * test_recursive_agg_plan.c - DD plan-level tests for monotone aggregation
 *                              in recursive strata (issue #69, RED phase)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * These tests exercise the DD plan generation layer to verify that:
 *   1. Non-monotone aggregations (COUNT, SUM, AVG) inside a recursive
 *      stratum are REJECTED with a clear error at plan-generation time.
 *   2. Monotone aggregations (MIN, MAX) inside a recursive stratum are
 *      ACCEPTED and produce a plan with is_recursive=true and a REDUCE op.
 *   3. Non-recursive strata with MIN/MAX aggregations are unaffected.
 *
 * RED PHASE: tests 1-8 are expected to FAIL until implementation lands.
 * The non-monotone rejection tests (2-6) will fail because the current
 * plan generator does not yet validate aggregation monotonicity.
 * The monotone acceptance tests (7-8) will fail because the plan generator
 * does not yet emit REDUCE ops inside recursive strata.
 *
 * Tests 9-10 (non-recursive MIN/MAX) are expected to PASS already.
 */

#include "../wirelog/backend/dd/dd_plan.h"
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
/* Helper: generate plan from Datalog source, return rc                    */
/* Returns: 0 success (plan set), non-zero on error (plan remains NULL)    */
/* ======================================================================== */

static int
plan_from_source(const char *src, wl_dd_plan_t **out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -10; /* parse failure */

    int rc = wl_dd_plan_generate(prog, out);
    wirelog_program_free(prog);
    return rc;
}

/* ======================================================================== */
/* Helper: find a REDUCE op in any recursive stratum of the plan           */
/* ======================================================================== */

static bool
plan_has_reduce_in_recursive_stratum(const wl_dd_plan_t *plan)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        if (!plan->strata[s].is_recursive)
            continue;
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_dd_relation_plan_t *rel = &plan->strata[s].relations[r];
            for (uint32_t o = 0; o < rel->op_count; o++) {
                if (rel->ops[o].op == WL_DD_REDUCE)
                    return true;
            }
        }
    }
    return false;
}

/* ======================================================================== */
/* Helper: find a REDUCE op with given agg_fn anywhere in the plan        */
/* ======================================================================== */

static bool
plan_has_reduce_with_fn(const wl_dd_plan_t *plan, wirelog_agg_fn_t fn)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_dd_relation_plan_t *rel = &plan->strata[s].relations[r];
            for (uint32_t o = 0; o < rel->op_count; o++) {
                if (rel->ops[o].op == WL_DD_REDUCE && rel->ops[o].agg_fn == fn)
                    return true;
            }
        }
    }
    return false;
}

/* ======================================================================== */
/* Test 1: Baseline - recursive TC plan generates without error            */
/* (Expected to PASS - establishes that plan generation works for          */
/*  recursive programs that have no aggregation)                           */
/* ======================================================================== */

static void
test_plan_recursive_no_agg_succeeds(void)
{
    TEST("plan: recursive TC (no agg) generates successfully");

    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(".decl Arc(x: int32, y: int32)\n"
                              ".decl Tc(x: int32, y: int32)\n"
                              "Tc(x, y) :- Arc(x, y).\n"
                              "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
                              &plan);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "plan_generate returned %d, expected 0", rc);
        FAIL(msg);
        return;
    }

    if (!plan) {
        FAIL("plan is NULL after rc=0");
        return;
    }

    wl_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Test 2: COUNT in recursive stratum must be rejected                     */
/* RED: currently fails because non-monotone check not implemented         */
/* ======================================================================== */

static void
test_plan_count_in_recursive_rejected(void)
{
    TEST("plan: COUNT in recursive stratum returns error (non-monotone)");

    /*
     * This program is intentionally ill-formed for monotone semantics:
     * Dist depends on itself via count() which is non-monotone.
     * The plan generator must detect this and return a non-zero error code.
     *
     * Expected: plan_generate returns != 0 and plan remains NULL.
     */
    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(".decl Edge(x: int32, y: int32)\n"
                              ".decl Dist(x: int32, d: int32)\n"
                              "Dist(x, count(y)) :- Edge(x, y).\n"
                              "Dist(x, count(y)) :- Dist(x, _), Edge(x, y).\n",
                              &plan);

    if (rc == 0) {
        /* plan was accepted - this is the bug we are testing for */
        if (plan)
            wl_dd_plan_free(plan);
        FAIL("COUNT in recursive stratum was accepted (should be rejected)");
        return;
    }

    if (plan != NULL) {
        wl_dd_plan_free(plan);
        FAIL("plan should be NULL when generation fails");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Test 3: SUM in recursive stratum must be rejected                       */
/* RED: currently fails because non-monotone check not implemented         */
/* ======================================================================== */

static void
test_plan_sum_in_recursive_rejected(void)
{
    TEST("plan: SUM in recursive stratum returns error (non-monotone)");

    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl TotalCost(x: int32, c: int32)\n"
        "TotalCost(x, sum(w)) :- Edge(x, _, w).\n"
        "TotalCost(x, sum(w)) :- TotalCost(x, _), Edge(x, _, w).\n",
        &plan);

    if (rc == 0) {
        if (plan)
            wl_dd_plan_free(plan);
        FAIL("SUM in recursive stratum was accepted (should be rejected)");
        return;
    }

    if (plan != NULL) {
        wl_dd_plan_free(plan);
        FAIL("plan should be NULL when generation fails");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Test 4: AVG in recursive stratum must be rejected                       */
/* RED: currently fails because non-monotone check not implemented         */
/* ======================================================================== */

static void
test_plan_avg_in_recursive_rejected(void)
{
    TEST("plan: AVG in recursive stratum returns error (non-monotone)");

    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(
        ".decl Score(x: int32, v: int32)\n"
        ".decl AvgScore(x: int32, a: int32)\n"
        "AvgScore(x, avg(v)) :- Score(x, v).\n"
        "AvgScore(x, avg(v)) :- AvgScore(x, _), Score(x, v).\n",
        &plan);

    if (rc == 0) {
        if (plan)
            wl_dd_plan_free(plan);
        FAIL("AVG in recursive stratum was accepted (should be rejected)");
        return;
    }

    if (plan != NULL) {
        wl_dd_plan_free(plan);
        FAIL("plan should be NULL when generation fails");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Test 5: COUNT in mutual recursion must be rejected                      */
/* RED: currently fails because non-monotone check not implemented         */
/* ======================================================================== */

static void
test_plan_count_in_mutual_recursion_rejected(void)
{
    TEST("plan: COUNT in mutually recursive SCC returns error");

    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(".decl Base(x: int32)\n"
                              ".decl A(x: int32, n: int32)\n"
                              ".decl B(x: int32, n: int32)\n"
                              "A(x, count(x)) :- Base(x).\n"
                              "A(x, n) :- B(x, n).\n"
                              "B(x, n) :- A(x, n).\n",
                              &plan);

    if (rc == 0) {
        if (plan)
            wl_dd_plan_free(plan);
        FAIL("COUNT in mutual recursion was accepted (should be rejected)");
        return;
    }

    if (plan != NULL) {
        wl_dd_plan_free(plan);
        FAIL("plan should be NULL when generation fails");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Test 6: SUM in self-recursive rule must be rejected                     */
/* RED: currently fails because non-monotone check not implemented         */
/* ======================================================================== */

static void
test_plan_sum_self_recursive_rejected(void)
{
    TEST("plan: SUM in self-recursive rule returns error");

    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(".decl Node(x: int32, v: int32)\n"
                              ".decl Agg(x: int32, s: int32)\n"
                              "Agg(x, sum(v)) :- Node(x, v).\n"
                              "Agg(x, sum(v)) :- Agg(x, _), Node(x, v).\n",
                              &plan);

    if (rc == 0) {
        if (plan)
            wl_dd_plan_free(plan);
        FAIL("SUM in self-recursive rule was accepted (should be rejected)");
        return;
    }

    if (plan != NULL) {
        wl_dd_plan_free(plan);
        FAIL("plan should be NULL when generation fails");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Test 7: MIN in recursive stratum must be ACCEPTED                       */
/* RED: currently fails because REDUCE not emitted in recursive strata     */
/* ======================================================================== */

static void
test_plan_min_in_recursive_accepted(void)
{
    TEST("plan: MIN in recursive stratum is accepted (monotone)");

    /*
     * SSSP-style: shortest path via MIN aggregation.
     * Dist(x, min(d)) :- Edge(src, x, d).
     * Dist(x, min(d)) :- Dist(z, dz), Edge(z, x, w), d = dz + w.
     *
     * MIN is monotone (values only decrease) so this must be accepted.
     * Expected: rc=0, plan has is_recursive=true stratum with REDUCE(MIN).
     */
    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl Dist(x: int32, d: int32)\n"
        "Dist(y, min(w))       :- Edge(0, y, w).\n"
        "Dist(y, min(dz + w))  :- Dist(z, dz), Edge(z, y, w).\n",
        &plan);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "MIN recursive program rejected (rc=%d), expected 0", rc);
        FAIL(msg);
        return;
    }

    if (!plan) {
        FAIL("plan is NULL after rc=0");
        return;
    }

    /* verify a REDUCE(MIN) op exists in a recursive stratum */
    if (!plan_has_reduce_in_recursive_stratum(plan)) {
        wl_dd_plan_free(plan);
        FAIL("no REDUCE op found in any recursive stratum");
        return;
    }

    if (!plan_has_reduce_with_fn(plan, WIRELOG_AGG_MIN)) {
        wl_dd_plan_free(plan);
        FAIL("no REDUCE(MIN) op found in plan");
        return;
    }

    wl_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Test 8: MAX in recursive stratum must be ACCEPTED                       */
/* RED: currently fails because REDUCE not emitted in recursive strata     */
/* ======================================================================== */

static void
test_plan_max_in_recursive_accepted(void)
{
    TEST("plan: MAX in recursive stratum is accepted (monotone)");

    /*
     * Longest path via MAX aggregation.
     * MaxDist(x, max(d)) :- Edge(0, x, d).
     * MaxDist(x, max(dz + w)) :- MaxDist(z, dz), Edge(z, x, w).
     *
     * MAX is monotone (values only increase) so this must be accepted.
     */
    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl MaxDist(x: int32, d: int32)\n"
        "MaxDist(y, max(w))       :- Edge(0, y, w).\n"
        "MaxDist(y, max(dz + w))  :- MaxDist(z, dz), Edge(z, y, w).\n",
        &plan);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "MAX recursive program rejected (rc=%d), expected 0", rc);
        FAIL(msg);
        return;
    }

    if (!plan) {
        FAIL("plan is NULL after rc=0");
        return;
    }

    if (!plan_has_reduce_in_recursive_stratum(plan)) {
        wl_dd_plan_free(plan);
        FAIL("no REDUCE op found in any recursive stratum");
        return;
    }

    if (!plan_has_reduce_with_fn(plan, WIRELOG_AGG_MAX)) {
        wl_dd_plan_free(plan);
        FAIL("no REDUCE(MAX) op found in plan");
        return;
    }

    wl_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Test 9: MIN in non-recursive stratum is accepted and produces REDUCE    */
/* (Expected to PASS with current codebase)                                */
/* ======================================================================== */

static void
test_plan_min_nonrecursive_accepted(void)
{
    TEST("plan: MIN in non-recursive stratum accepted and has REDUCE(MIN)");

    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(".decl Data(x: int32, v: int32)\n"
                              ".decl MinVal(x: int32, m: int32)\n"
                              "MinVal(x, min(v)) :- Data(x, v).\n",
                              &plan);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "plan_generate returned %d, expected 0", rc);
        FAIL(msg);
        return;
    }

    if (!plan) {
        FAIL("plan is NULL after rc=0");
        return;
    }

    if (!plan_has_reduce_with_fn(plan, WIRELOG_AGG_MIN)) {
        wl_dd_plan_free(plan);
        FAIL("no REDUCE(MIN) op found in non-recursive plan");
        return;
    }

    wl_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Test 10: MAX in non-recursive stratum is accepted and produces REDUCE   */
/* (Expected to PASS with current codebase)                                */
/* ======================================================================== */

static void
test_plan_max_nonrecursive_accepted(void)
{
    TEST("plan: MAX in non-recursive stratum accepted and has REDUCE(MAX)");

    wl_dd_plan_t *plan = NULL;
    int rc = plan_from_source(".decl Score(x: int32, v: int32)\n"
                              ".decl MaxScore(x: int32, m: int32)\n"
                              "MaxScore(x, max(v)) :- Score(x, v).\n",
                              &plan);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "plan_generate returned %d, expected 0", rc);
        FAIL(msg);
        return;
    }

    if (!plan) {
        FAIL("plan is NULL after rc=0");
        return;
    }

    if (!plan_has_reduce_with_fn(plan, WIRELOG_AGG_MAX)) {
        wl_dd_plan_free(plan);
        FAIL("no REDUCE(MAX) op found in non-recursive plan");
        return;
    }

    wl_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog Recursive Aggregation - Plan Tests (RED phase) ===\n"
           "\nNOTE: Tests 2-8 are expected to FAIL until implementation.\n\n");

    /* Baseline (should pass) */
    test_plan_recursive_no_agg_succeeds();

    /* Non-monotone rejection (RED - should fail until implemented) */
    test_plan_count_in_recursive_rejected();
    test_plan_sum_in_recursive_rejected();
    test_plan_avg_in_recursive_rejected();
    test_plan_count_in_mutual_recursion_rejected();
    test_plan_sum_self_recursive_rejected();

    /* Monotone acceptance (RED - should fail until implemented) */
    test_plan_min_in_recursive_accepted();
    test_plan_max_in_recursive_accepted();

    /* Non-recursive MIN/MAX (should pass already) */
    test_plan_min_nonrecursive_accepted();
    test_plan_max_nonrecursive_accepted();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
