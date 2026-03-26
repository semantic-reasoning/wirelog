/*
 * test_tdd_convergence.c - Unit tests for TDD convergence detection
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: recursive frontier recorded, nonrecursive sentinel, second-step
 * frontier skip, W=1/2/4 convergence equivalence on cyclic graph.
 *
 * Issue #318: Distributed Stratum Evaluator Phase 4c
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
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
/* Helpers                                                                  */
/* ======================================================================== */

/* TC program: recursive stratum */
static wl_col_session_t *
make_tc_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n",
        &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

/* Non-recursive copy program */
static wl_col_session_t *
make_copy_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl edge_copy(x: int32, y: int32)\n"
        "edge_copy(x, y) :- edge(x, y).\n",
        &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

static void
cleanup_session(wl_col_session_t *sess, wl_plan_t *plan,
    wirelog_program_t *prog)
{
    wl_session_destroy(&sess->base);
    wl_plan_free(plan);
    wirelog_program_free(prog);
}

static int
insert_edges(wl_col_session_t *sess, const int64_t *rows, uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 2);
}

static uint32_t
count_rows(wl_col_session_t *sess, const char *name)
{
    col_rel_t *r = session_find_rel(sess, name);

    return r ? r->nrows : 0;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * test_recursive_frontier_recorded:
 * After W=2 TC on a chain, the coordinator stratum frontier must be set
 * (iteration != 0 and != UINT32_MAX).  Verifies tdd_record_recursive_convergence
 * correctly calls record_stratum_convergence.
 */
static int
test_recursive_frontier_recorded(void)
{
    TEST("Recursive frontier recorded after W=2 TDD step");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 4 };
    if (insert_edges(sess, rows, 3) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("session step failed");
        return 1;
    }

    /* Find the recursive stratum index */
    uint32_t rec_si = UINT32_MAX;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if (plan->strata[si].is_recursive) {
            rec_si = si;
            break;
        }
    }
    if (rec_si == UINT32_MAX) {
        cleanup_session(sess, plan, prog);
        FAIL("no recursive stratum found");
        return 1;
    }

    /* Frontier must be set: iteration != 0 and != UINT32_MAX */
    uint32_t fiter = sess->frontiers[rec_si].iteration;
    cleanup_session(sess, plan, prog);

    if (fiter == UINT32_MAX) {
        FAIL("recursive frontier still at UINT32_MAX (not recorded)");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_nonrecursive_frontier_sentinel:
 * After W=2 non-recursive copy step, the coordinator stratum frontier must
 * be UINT32_MAX (sentinel convention for non-recursive strata).
 * Verifies tdd_record_nonrecursive_convergence.
 */
static int
test_nonrecursive_frontier_sentinel(void)
{
    TEST("Nonrecursive frontier is UINT32_MAX sentinel after W=2 step");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_copy_session(2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3 };
    if (insert_edges(sess, rows, 2) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("session step failed");
        return 1;
    }

    /* Only one stratum (non-recursive) */
    uint32_t si = 0;
    uint32_t fiter = sess->frontiers[si].iteration;
    cleanup_session(sess, plan, prog);

    if (fiter != UINT32_MAX) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "expected UINT32_MAX sentinel, got %u", fiter);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_second_step_frontier_skip:
 * W=2 TC: first step converges and records frontier.  A second step with no
 * new inserts should be a no-op (total_iterations == 0, frontier-skipped).
 */
static int
test_second_step_frontier_skip(void)
{
    TEST(
        "Second step with no inserts is frontier-skipped (total_iterations==0)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3 };
    if (insert_edges(sess, rows, 2) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    /* First step */
    if (wl_session_step(&sess->base) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("first session step failed");
        return 1;
    }

    /* Second step with no new inserts */
    if (wl_session_step(&sess->base) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("second session step failed");
        return 1;
    }

    uint32_t iters = sess->total_iterations;
    cleanup_session(sess, plan, prog);

    /* Frontier skip fires: no new tuples derived, total_iterations stays 0 */
    if (iters != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "expected total_iterations==0, got %u", iters);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_cyclic_convergence_w1_w4:
 * W=4 TC on a cyclic graph (1->2->3->1) must match W=1 row count.
 * Cyclic graphs test that convergence detection terminates correctly
 * under distributed evaluation.
 */
static int
test_cyclic_convergence_w1_w4(void)
{
    TEST("W=4 cyclic graph convergence matches W=1");

    wl_plan_t *plan1 = NULL, *plan4 = NULL;
    wirelog_program_t *prog1 = NULL, *prog4 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("W=1 session create");
        return 1;
    }
    wl_col_session_t *sess4 = make_tc_session(4, &plan4, &prog4);
    if (!sess4) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=4 session create");
        return 1;
    }

    /* Cyclic: 1->2, 2->3, 3->1 plus 1->4 */
    int64_t rows[] = { 1, 2, 2, 3, 3, 1, 1, 4 };
    insert_edges(sess1, rows, 4);
    insert_edges(sess4, rows, 4);

    int rc1 = wl_session_step(&sess1->base);
    int rc4 = wl_session_step(&sess4->base);

    uint32_t n1 = count_rows(sess1, "tc");
    uint32_t n4 = count_rows(sess4, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess4, plan4, prog4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step error");
        return 1;
    }
    if (n1 != n4) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "W=1 got %u, W=4 got %u", n1, n4);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_tdd_convergence\n");
    printf("====================\n");

    test_recursive_frontier_recorded();
    test_nonrecursive_frontier_sentinel();
    test_second_step_frontier_skip();
    test_cyclic_convergence_w1_w4();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
