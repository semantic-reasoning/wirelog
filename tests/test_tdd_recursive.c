/*
 * test_tdd_recursive.c - Unit tests for col_eval_stratum_tdd recursive path
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: W=1 TC baseline, W=2 and W=4 correctness vs single-worker,
 * cyclic graph convergence, deep chain (exercises EVAL_STRIDE).
 *
 * Issue #318: Distributed Stratum Evaluator Phase 2
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

/*
 * Build a session with transitive closure:
 *   .decl edge(x: int32, y: int32)
 *   .decl tc(x: int32, y: int32)
 *   tc(x, y) :- edge(x, y).
 *   tc(x, z) :- tc(x, y), edge(y, z).
 *
 * tc is in a recursive stratum (depends on itself).
 */
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
 * test_tc_w1_baseline:
 * W=1 transitive closure on a 4-node chain: 1->2->3->4.
 * Expected 6 tc tuples: (1,2),(2,3),(3,4),(1,3),(2,4),(1,4).
 */
static int
test_tc_w1_baseline(void)
{
    TEST("W=1 TC baseline: chain 1->2->3->4 yields 6 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(1, &plan, &prog);
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

    uint32_t nrows = count_rows(sess, "tc");
    cleanup_session(sess, plan, prog);

    if (nrows != 6) {
        FAIL("expected 6 tc tuples");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w2_correctness:
 * W=2 TC on chain 1->2->3->4 must match W=1 row count.
 */
static int
test_tc_w2_correctness(void)
{
    TEST("W=2 TC correctness matches W=1 baseline (chain)");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 4 };
    insert_edges(sess1, rows, 3);
    insert_edges(sess2, rows, 3);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 tc row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w4_correctness:
 * W=4 TC on 6-node chain must match W=1.
 */
static int
test_tc_w4_correctness(void)
{
    TEST("W=4 TC correctness matches W=1 baseline (6-node chain)");

    wl_plan_t *plan1 = NULL, *plan4 = NULL;
    wirelog_program_t *prog1 = NULL, *prog4 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess4 = make_tc_session(4, &plan4, &prog4);
    if (!sess4) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=4 session create");
        return 1;
    }

    /* Chain: 1->2->3->4->5->6 (5 edges, 15 TC tuples) */
    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5, 5, 6 };
    insert_edges(sess1, rows, 5);
    insert_edges(sess4, rows, 5);

    int rc1 = wl_session_step(&sess1->base);
    int rc4 = wl_session_step(&sess4->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt4 = count_rows(sess4, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess4, plan4, prog4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt4) {
        FAIL("W=4 tc row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_cyclic:
 * Cyclic graph 1->2->3->1. TC must include all 9 pairs (including self-loops).
 * W=2 must match W=1.
 */
static int
test_tc_cyclic(void)
{
    TEST("W=2 TC cyclic graph convergence matches W=1 (3-cycle)");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 1 };
    insert_edges(sess1, rows, 3);
    insert_edges(sess2, rows, 3);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 cyclic: row count differs from W=1 baseline");
        return 1;
    }
    /* W=1 should also be 9 (all 3x3 pairs from the 3-cycle) */
    if (cnt1 != 9) {
        FAIL("W=1 cyclic: expected 9 tc tuples");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w2_empty_edb:
 * Empty edge set: TC must be empty with W=2.
 */
static int
test_tc_w2_empty_edb(void)
{
    TEST("W=2 TC with empty EDB yields empty tc");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    uint32_t nrows = count_rows(sess, "tc");
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (nrows != 0) {
        FAIL("expected 0 tc tuples from empty EDB");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_deep_chain:
 * Deep chain of EVAL_STRIDE*2+3 nodes to exercise stride-based iteration.
 * W=2 row count must match W=1.
 */
static int
test_tc_deep_chain(void)
{
    TEST("W=2 TC deep chain (20 nodes) matches W=1");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    /* Chain 1->2->...->20: 19 edges, 190 TC tuples (n*(n-1)/2 = 19*20/2=190) */
    const uint32_t N = 20;
    int64_t rows[38]; /* 2*19 */
    for (uint32_t i = 0; i < N - 1; i++) {
        rows[(size_t)i * 2] = (int64_t)i + 1;
        rows[(size_t)i * 2 + 1] = (int64_t)i + 2;
    }

    insert_edges(sess1, rows, N - 1);
    insert_edges(sess2, rows, N - 1);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 deep chain: row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_category_a_diamond:
 * Category A stratum (recursive + EXCHANGE + no IDB-IDB joins).
 * Diamond graph: 1->2, 1->3, 2->4, 3->4.
 * TC: (1,2),(1,3),(2,4),(3,4),(1,4) = 5 tuples.
 * W=2 must match W=1, exercising TDD for Category A after the #390 fix.
 */
static int
test_tc_category_a_diamond(void)
{
    TEST("Category A: W=2 TC diamond graph matches W=1 (no IDB-IDB join)");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 1, 3, 2, 4, 3, 4 };
    insert_edges(sess1, rows, 4);
    insert_edges(sess2, rows, 4);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != 5) {
        FAIL("W=1 expected 5 tc tuples");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 tc row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * make_triple_idb_session:
 * Build a session with a 3-IDB-body-atom rule:
 *   .decl e(x: int32, y: int32)
 *   .decl r(x: int32, y: int32)
 *   r(x, y) :- e(x, y).
 *   r(x, y) :- r(x, a), r(a, b), r(b, y).
 *
 * The recursive rule has 3 IDB body atoms (all r).
 * BDX mode must NOT be enabled for this stratum.
 */
static wl_col_session_t *
make_triple_idb_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl e(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- e(x, y).\n"
        "r(x, y) :- r(x, a), r(a, b), r(b, y).\n",
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

/*
 * test_triple_idb_guard:
 * 3-IDB-body-atom rule must fall back to single-threaded (BDX guard).
 * W=2 must still produce correct results matching W=1.
 */
static int
test_triple_idb_guard(void)
{
    TEST(
        "3-IDB guard: W=2 triple-join r(x,y):-r(x,a),r(a,b),r(b,y) matches W=1");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_triple_idb_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_triple_idb_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    /* Chain: 1->2->3->4->5 (4 edges) */
    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
    if (wl_session_insert(&sess1->base, "e", rows, 4, 2) != 0
        || wl_session_insert(&sess2->base, "e", rows, 4, 2) != 0) {
        cleanup_session(sess1, plan1, prog1);
        cleanup_session(sess2, plan2, prog2);
        FAIL("insert");
        return 1;
    }

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "r");
    uint32_t cnt2 = count_rows(sess2, "r");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL(
            "W=2 row count differs from W=1 (guard should force single-threaded)");
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
    printf("TDD Recursive Distributed Evaluator Tests\n");
    printf("==========================================\n");

    test_tc_w1_baseline();
    test_tc_w2_correctness();
    test_tc_w4_correctness();
    test_tc_cyclic();
    test_tc_w2_empty_edb();
    test_tc_deep_chain();
    test_tc_category_a_diamond();
    test_triple_idb_guard();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
