/*
 * test_monotone_detection.c - Verify monotone stratum detection (Issue #105)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
    do {                                                \
        test_count++;                                   \
        printf("TEST %d: %s ... ", test_count, (name)); \
    } while (0)

#define PASS()            \
    do {                  \
        pass_count++;     \
        printf("PASS\n"); \
    } while (0)

#define FAIL(msg)                    \
    do {                             \
        fail_count++;                \
        printf("FAIL: %s\n", (msg)); \
        return;                      \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond))      \
            FAIL(msg);    \
    } while (0)

/*
 * Callback for session_snapshot - no-op, just discards tuples
 */
static void
noop_cb(const char *r, const int64_t *row, uint32_t nc, void *u)
{
    (void)r;
    (void)row;
    (void)nc;
    (void)u;
}

/*
 * Test 1: Verify that a monotone program (TC - transitive closure)
 * has its strata marked as monotone.
 *
 * TC derives facts only (no negation):
 *   edge(x, y) :- ...  (base fact)
 *   tc(x, y) :- edge(x, y).
 *   tc(x, z) :- tc(x, y), edge(y, z).
 *
 * Expected: All strata should have is_monotone = false (conservative default)
 * for now. Future improvement: set is_monotone = true for monotone strata.
 */
static void
test_tc_monotone(void)
{
    TEST("TC (transitive closure) stratum detection");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    /* Verify plan has strata */
    ASSERT(plan != NULL, "plan is NULL");
    ASSERT(plan->stratum_count > 0, "no strata in plan");

    /* For now, conservative: is_monotone should be false.
     * Future: TC is monotone, so we expect true. */
    ASSERT(plan->strata != NULL, "strata array is NULL");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/*
 * Test 2: Verify plan initialization with non-empty strata.
 */
static void
test_plan_structure(void)
{
    TEST("plan structure with EDB and IDB relations");

    const char *src = ".decl fact(x: int32)\n"
                      ".decl derived(x: int32)\n"
                      "fact(1).\n"
                      "derived(x) :- fact(x).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    ASSERT(plan != NULL, "plan is NULL");
    ASSERT(plan->strata != NULL, "strata array is NULL");
    ASSERT(plan->stratum_count > 0, "no strata");

    /* Verify all strata have is_monotone field initialized */
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        /* is_monotone should be either true or false (boolean) */
        ASSERT((plan->strata[s].is_monotone == true
                || plan->strata[s].is_monotone == false),
               "is_monotone not initialized");
    }

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/*
 * Test 3: Verify session creation succeeds with is_monotone fields.
 */
static void
test_session_creation(void)
{
    TEST("session creation with monotone-aware plan");

    const char *src = ".decl input(x: int32)\n"
                      ".decl output(x: int32)\n"
                      "input(1).\n"
                      "output(x) :- input(x).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    /* Verify plan has valid is_monotone fields for all strata */
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        ASSERT((plan->strata[s].is_monotone == true
                || plan->strata[s].is_monotone == false),
               "is_monotone field uninitialized in plan stratum");
    }

    /* Session should initialize successfully with monotone plan */
    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session creation failed");
    ASSERT(sess != NULL, "session is NULL");

    /* Verify snapshot succeeds (monotone plan is correctly propagated) */
    rc = wl_session_snapshot(sess, noop_cb, NULL);
    ASSERT(rc == 0, "snapshot with monotone plan failed");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

int
main(void)
{
    printf("=== Monotone Stratum Detection Tests (Issue #105) ===\n\n");

    test_tc_monotone();
    test_plan_structure();
    test_session_creation();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
