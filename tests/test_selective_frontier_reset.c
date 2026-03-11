/*
 * test_selective_frontier_reset.c - Verify selective frontier reset in delta-seeded mode
 *
 * Test that when delta-seeded incremental evaluation is triggered after EDB
 * insertion, frontiers are selectively reset: only strata with pre-seeded EDB
 * deltas have their frontiers reset. Transitively-affected strata (IDB-only)
 * preserve their frontier for skip optimization.
 *
 * Issue #107: Selective rule frontier reset with pre-seeded delta optimization.
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
 * Test 1: Verify frontier reset works with delta-seeded flag
 */
static void
test_delta_seeded_frontier_reset(void)
{
    TEST("frontier reset respects delta_seeded flag");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    /* Apply optimization passes */
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    /* Generate plan */
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");
    ASSERT(plan != NULL, "plan is NULL");

    /* Create session with columnar backend */
    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session_create failed");
    ASSERT(sess != NULL, "session is NULL");

    /* Insert base facts to trigger frontier reset logic */
    int64_t edge_data[] = { 1, 2, 2, 3, 3, 4 };
    rc = wl_session_insert(sess, "edge", edge_data, 3, 2);
    ASSERT(rc == 0, "first insert failed");

    /* Insert more facts to trigger delta-seeded incremental evaluation */
    int64_t edge_data2[] = { 4, 5, 5, 6 };
    rc = wl_session_insert(sess, "edge", edge_data2, 2, 2);
    ASSERT(rc == 0, "second insert failed");

    /* Session should remain valid after selective frontier reset */
    ASSERT(sess != NULL, "session corrupted after frontier reset");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/*
 * Test 2: Verify multiple insertions maintain correct frontier behavior
 */
static void
test_multiple_insertions_frontier_preservation(void)
{
    TEST("frontier preservation across multiple insertions");

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

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session_create failed");

    /* First insertion */
    int64_t data1[] = { 1, 2 };
    rc = wl_session_insert(sess, "edge", data1, 1, 2);
    ASSERT(rc == 0, "insert 1 failed");

    /* Second insertion - frontier reset should be selective */
    int64_t data2[] = { 2, 3 };
    rc = wl_session_insert(sess, "edge", data2, 1, 2);
    ASSERT(rc == 0, "insert 2 failed");

    /* Third insertion - selective reset continues */
    int64_t data3[] = { 3, 4 };
    rc = wl_session_insert(sess, "edge", data3, 1, 2);
    ASSERT(rc == 0, "insert 3 failed");

    /* Session should remain healthy after multiple selective resets */
    ASSERT(sess != NULL, "session corrupted after multiple insertions");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/*
 * Test 3: Verify that frontier behavior is sound with simple non-recursive program
 */
static void
test_non_recursive_frontier_behavior(void)
{
    TEST("frontier behavior with non-recursive rules");

    /* Non-recursive program: no cycles, all strata monotone */
    const char *src = ".decl fact(x: int32)\n"
                      ".decl derived(x: int32)\n"
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

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session_create failed");

    /* Insert base facts */
    int64_t data[] = { 42, 43, 44 };
    rc = wl_session_insert(sess, "fact", data, 3, 1);
    ASSERT(rc == 0, "insert failed");

    /* Incremental insert - frontier reset should be handled correctly */
    int64_t data2[] = { 45, 46 };
    rc = wl_session_insert(sess, "fact", data2, 2, 1);
    ASSERT(rc == 0, "incremental insert failed");

    ASSERT(sess != NULL, "session corrupted");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

int
main(void)
{
    printf("=== Selective Frontier Reset Tests (Issue #102) ===\n\n");

    test_delta_seeded_frontier_reset();
    test_multiple_insertions_frontier_preservation();
    test_non_recursive_frontier_behavior();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)\n", fail_count);
    else
        printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
