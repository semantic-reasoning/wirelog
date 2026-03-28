/*
 * test_td_multiworker.c - Multiworker correctness, edge case, and
 *                         determinism tests for TDD integration path
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: W=8 TC correctness, large-graph multiworker, cyclic W=8,
 * single-tuple edge case, empty input, W=1 fallback, determinism.
 *
 * Issue #321: Integration tests for distributed stratum evaluator
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

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

/*
 * Build a chain of N nodes: 1->2->3->...->N.
 * Returns heap-allocated edge array (caller frees).
 * nrows_out is set to N-1 (number of edges).
 */
static int64_t *
make_chain(uint32_t n, uint32_t *nrows_out)
{
    uint32_t nedges = n - 1;
    int64_t *rows = (int64_t *)malloc((size_t)nedges * 2 * sizeof(int64_t));

    if (!rows)
        return NULL;
    for (uint32_t i = 0; i < nedges; i++) {
        rows[(size_t)i * 2] = (int64_t)i + 1;
        rows[(size_t)i * 2 + 1] = (int64_t)i + 2;
    }
    *nrows_out = nedges;
    return rows;
}

/* ======================================================================== */
/* Correctness Tests                                                        */
/* ======================================================================== */

/*
 * test_tc_w1_baseline:
 * W=1 transitive closure on a 5-node chain: 1->2->3->4->5.
 * Expected 10 tc tuples: C(5,2) = 10.
 */
static int
test_tc_w1_baseline(void)
{
    TEST("W=1 TC baseline: chain 1->2->3->4->5 yields 10 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(1, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
    if (insert_edges(sess, rows, 4) != 0) {
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

    if (nrows != 10) {
        FAIL("expected 10 tc tuples");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w8_correctness:
 * W=8 TC on 5-node chain must match W=1 row count (10 tuples).
 */
static int
test_tc_w8_correctness(void)
{
    TEST("W=8 TC correctness matches W=1 baseline (5-node chain)");

    wl_plan_t *plan1 = NULL, *plan8 = NULL;
    wirelog_program_t *prog1 = NULL, *prog8 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess8 = make_tc_session(8, &plan8, &prog8);
    if (!sess8) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=8 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
    insert_edges(sess1, rows, 4);
    insert_edges(sess8, rows, 4);

    int rc1 = wl_session_step(&sess1->base);
    int rc8 = wl_session_step(&sess8->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt8 = count_rows(sess8, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess8, plan8, prog8);

    if (rc1 != 0 || rc8 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt8) {
        FAIL("W=8 tc row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w4_large_graph:
 * W=4 TC on 25-node chain must match W=1.
 * 25-node chain: 24 edges, C(25,2) = 300 TC tuples.
 */
static int
test_tc_w4_large_graph(void)
{
    TEST("W=4 TC large graph (25-node chain) matches W=1");

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

    uint32_t nedges = 0;
    int64_t *chain = make_chain(25, &nedges);
    if (!chain) {
        cleanup_session(sess1, plan1, prog1);
        cleanup_session(sess4, plan4, prog4);
        FAIL("make_chain alloc");
        return 1;
    }

    insert_edges(sess1, chain, nedges);
    insert_edges(sess4, chain, nedges);
    free(chain);

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
        FAIL("W=4 large graph: row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w8_large_graph:
 * W=8 TC on 25-node chain must match W=1.
 */
static int
test_tc_w8_large_graph(void)
{
    TEST("W=8 TC large graph (25-node chain) matches W=1");

    wl_plan_t *plan1 = NULL, *plan8 = NULL;
    wirelog_program_t *prog1 = NULL, *prog8 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess8 = make_tc_session(8, &plan8, &prog8);
    if (!sess8) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=8 session create");
        return 1;
    }

    uint32_t nedges = 0;
    int64_t *chain = make_chain(25, &nedges);
    if (!chain) {
        cleanup_session(sess1, plan1, prog1);
        cleanup_session(sess8, plan8, prog8);
        FAIL("make_chain alloc");
        return 1;
    }

    insert_edges(sess1, chain, nedges);
    insert_edges(sess8, chain, nedges);
    free(chain);

    int rc1 = wl_session_step(&sess1->base);
    int rc8 = wl_session_step(&sess8->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt8 = count_rows(sess8, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess8, plan8, prog8);

    if (rc1 != 0 || rc8 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt8) {
        FAIL("W=8 large graph: row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w8_cyclic:
 * Cyclic graph 1->2->3->1 with W=8 must match W=1.
 * Expected 9 TC tuples (all 3x3 pairs from 3-cycle).
 */
static int
test_tc_w8_cyclic(void)
{
    TEST("W=8 TC cyclic graph (3-cycle) matches W=1");

    wl_plan_t *plan1 = NULL, *plan8 = NULL;
    wirelog_program_t *prog1 = NULL, *prog8 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess8 = make_tc_session(8, &plan8, &prog8);
    if (!sess8) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=8 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 1 };
    insert_edges(sess1, rows, 3);
    insert_edges(sess8, rows, 3);

    int rc1 = wl_session_step(&sess1->base);
    int rc8 = wl_session_step(&sess8->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt8 = count_rows(sess8, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess8, plan8, prog8);

    if (rc1 != 0 || rc8 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt8) {
        FAIL("W=8 cyclic: row count differs from W=1 baseline");
        return 1;
    }
    if (cnt1 != 9) {
        FAIL("expected 9 tc tuples from 3-cycle");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Edge Case Tests                                                          */
/* ======================================================================== */

/*
 * test_tc_empty_w4:
 * Empty edge set with W=4: TC must be empty, no crash.
 */
static int
test_tc_empty_w4(void)
{
    TEST("W=4 TC with empty input yields 0 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(4, &plan, &prog);
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
        FAIL("expected 0 tc tuples from empty input");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_single_tuple_w4:
 * Single edge (1,2) with W=4: TC must contain exactly 1 tuple.
 */
static int
test_tc_single_tuple_w4(void)
{
    TEST("W=4 TC with single edge yields 1 tuple");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(4, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    if (insert_edges(sess, rows, 1) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    uint32_t nrows = count_rows(sess, "tc");
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (nrows != 1) {
        FAIL("expected 1 tc tuple from single edge");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w1_fallback:
 * W=1 on a 25-node chain verifies single-worker fallback at scale.
 * Expected C(25,2) = 300 TC tuples.
 */
static int
test_tc_w1_fallback(void)
{
    TEST("W=1 fallback: 25-node chain yields 300 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(1, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    uint32_t nedges = 0;
    int64_t *chain = make_chain(25, &nedges);
    if (!chain) {
        cleanup_session(sess, plan, prog);
        FAIL("make_chain alloc");
        return 1;
    }

    if (insert_edges(sess, chain, nedges) != 0) {
        free(chain);
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }
    free(chain);

    int rc = wl_session_step(&sess->base);
    uint32_t nrows = count_rows(sess, "tc");
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (nrows != 300) {
        FAIL("expected 300 tc tuples from 25-node chain");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Determinism Tests                                                        */
/* ======================================================================== */

/*
 * test_tc_determinism_w4:
 * Run W=4 TC on 5-node chain 5 times; all runs must produce identical
 * row counts, verifying deterministic behavior of the TDD path.
 */
static int
test_tc_determinism_w4(void)
{
    TEST("W=4 TC determinism: 5 runs produce identical row counts");

    const uint32_t RUNS = 5;
    uint32_t counts[5];

    for (uint32_t r = 0; r < RUNS; r++) {
        wl_plan_t *plan = NULL;
        wirelog_program_t *prog = NULL;
        wl_col_session_t *sess = make_tc_session(4, &plan, &prog);
        if (!sess) {
            FAIL("session create");
            return 1;
        }

        int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
        if (insert_edges(sess, rows, 4) != 0) {
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

        counts[r] = count_rows(sess, "tc");
        cleanup_session(sess, plan, prog);
    }

    for (uint32_t r = 1; r < RUNS; r++) {
        if (counts[r] != counts[0]) {
            FAIL("W=4 non-deterministic: row counts differ across runs");
            return 1;
        }
    }
    PASS();
    return 0;
}

/*
 * test_tc_determinism_w2:
 * Run W=2 TC on 5-node chain 5 times; all runs must produce identical
 * row counts.
 */
static int
test_tc_determinism_w2(void)
{
    TEST("W=2 TC determinism: 5 runs produce identical row counts");

    const uint32_t RUNS = 5;
    uint32_t counts[5];

    for (uint32_t r = 0; r < RUNS; r++) {
        wl_plan_t *plan = NULL;
        wirelog_program_t *prog = NULL;
        wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
        if (!sess) {
            FAIL("session create");
            return 1;
        }

        int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
        if (insert_edges(sess, rows, 4) != 0) {
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

        counts[r] = count_rows(sess, "tc");
        cleanup_session(sess, plan, prog);
    }

    for (uint32_t r = 1; r < RUNS; r++) {
        if (counts[r] != counts[0]) {
            FAIL("W=2 non-deterministic: row counts differ across runs");
            return 1;
        }
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Incremental and Multi-Stratum Tests                                      */
/* ======================================================================== */

/*
 * test_td_incremental_w4:
 * Incremental TDD: insert 3 edges, step, insert 2 more, step.
 * Batch 1: 1->2, 2->3, 3->4. Batch 2: 4->5, 5->6.
 * Full chain 1->6: TC = C(6,2) = 15 tuples.
 *
 * Issue #350: W=4 validates that multiworker incremental correctly
 * recomputes IDB across step boundaries.
 */
static int
test_td_incremental_w4(void)
{
    TEST("W=4 incremental: 3+2 edge batches yield 15 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(4, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    /* Batch 1: 3 edges */
    int64_t batch1[] = { 1, 2, 2, 3, 3, 4 };
    if (insert_edges(sess, batch1, 3) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert batch 1");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("step 1 failed");
        return 1;
    }

    /* Batch 2: 2 edges */
    int64_t batch2[] = { 4, 5, 5, 6 };
    if (insert_edges(sess, batch2, 2) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert batch 2");
        return 1;
    }

    rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("step 2 failed");
        return 1;
    }

    uint32_t nrows = count_rows(sess, "tc");
    cleanup_session(sess, plan, prog);

    if (nrows != 15) {
        FAIL("expected 15 tc tuples after incremental build of 6-node chain");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_td_multi_stratum_w2:
 * TC-based multi-stratum: non-recursive edge_copy feeds recursive tc.
 *   edge_copy(x, y) :- edge(x, y).
 *   tc(x, y) :- edge_copy(x, y).
 *   tc(x, z) :- tc(x, y), edge_copy(y, z).
 * Chain 1->2->3->4: W=2 must match W=1.
 */
static int
test_td_multi_stratum_w2(void)
{
    TEST("W=2 multi-stratum (edge_copy + tc) matches W=1");

    static const char prog_src[] =
        ".decl edge(x: int32, y: int32)\n"
        ".decl edge_copy(x: int32, y: int32)\n"
        "edge_copy(x, y) :- edge(x, y).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge_copy(x, y).\n"
        "tc(x, z) :- tc(x, y), edge_copy(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog1 = wirelog_parse_string(prog_src, &err);
    wirelog_program_t *prog2 = wirelog_parse_string(prog_src, &err);
    if (!prog1 || !prog2) {
        if (prog1) wirelog_program_free(prog1);
        if (prog2) wirelog_program_free(prog2);
        FAIL("parse");
        return 1;
    }

    wl_fusion_apply(prog1, NULL);
    wl_jpp_apply(prog1, NULL);
    wl_sip_apply(prog1, NULL);
    wl_fusion_apply(prog2, NULL);
    wl_jpp_apply(prog2, NULL);
    wl_sip_apply(prog2, NULL);

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    if (wl_plan_from_program(prog1, &plan1) != 0 ||
        wl_plan_from_program(prog2, &plan2) != 0) {
        wirelog_program_free(prog1);
        wirelog_program_free(prog2);
        if (plan1) wl_plan_free(plan1);
        if (plan2) wl_plan_free(plan2);
        FAIL("plan");
        return 1;
    }

    wl_session_t *s1 = NULL, *s2 = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan1, 1, &s1);
    int rc2 = wl_session_create(wl_backend_columnar(), plan2, 2, &s2);
    if (rc != 0 || rc2 != 0 || !s1 || !s2) {
        if (s1) wl_session_destroy(s1);
        if (s2) wl_session_destroy(s2);
        wl_plan_free(plan1);
        wl_plan_free(plan2);
        wirelog_program_free(prog1);
        wirelog_program_free(prog2);
        FAIL("session create");
        return 1;
    }

    int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    wl_session_insert(s1, "edge", edges, 3, 2);
    wl_session_insert(s2, "edge", edges, 3, 2);

    int rc_s1 = wl_session_step(s1);
    int rc_s2 = wl_session_step(s2);

    wl_col_session_t *cs1 = COL_SESSION(s1);
    wl_col_session_t *cs2 = COL_SESSION(s2);
    uint32_t cnt1 = count_rows(cs1, "tc");
    uint32_t cnt2 = count_rows(cs2, "tc");

    wl_session_destroy(s1);
    wl_session_destroy(s2);
    wl_plan_free(plan1);
    wl_plan_free(plan2);
    wirelog_program_free(prog1);
    wirelog_program_free(prog2);

    if (rc_s1 != 0 || rc_s2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 == 0) {
        FAIL("W=1 produced 0 tc rows");
        return 1;
    }
    if (cnt2 != cnt1) {
        FAIL("W=2 multi-stratum tc count differs from W=1");
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
    printf("Multiworker Integration Tests (Issue #321)\n");
    printf("==========================================\n");

    printf("\n-- Correctness --\n");
    test_tc_w1_baseline();
    test_tc_w8_correctness();
    test_tc_w4_large_graph();
    test_tc_w8_large_graph();
    test_tc_w8_cyclic();

    printf("\n-- Edge Cases --\n");
    test_tc_empty_w4();
    test_tc_single_tuple_w4();
    test_tc_w1_fallback();

    printf("\n-- Determinism --\n");
    test_tc_determinism_w4();
    test_tc_determinism_w2();

    printf("\n-- Incremental / Multi-Stratum --\n");
    test_td_incremental_w4();
    test_td_multi_stratum_w2();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
