/*
 * test_tdd_integration.c - Integration tests for TDD multiworker evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests cross-program correctness (reachability, same-generation,
 * multi-stratum), determinism, and edge cases through the full
 * parse-to-step pipeline.
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
 * Parameterized session builder: parses program_src, applies optimizer
 * passes, and creates a columnar session with num_workers.
 */
static wl_col_session_t *
make_session(const char *program_src, uint32_t num_workers,
    wl_plan_t **plan_out, wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(program_src, &err);
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
insert_facts(wl_col_session_t *sess, const char *rel,
    const int64_t *rows, uint32_t nrows)
{
    return wl_session_insert(&sess->base, rel, rows, nrows, 2);
}

static uint32_t
count_rows(wl_col_session_t *sess, const char *name)
{
    col_rel_t *r = session_find_rel(sess, name);

    return r ? r->nrows : 0;
}

/* ======================================================================== */
/* Program Sources                                                          */
/* ======================================================================== */

/* Reachability: multi-rule non-recursive (two rules -> one relation) */
static const char PROG_REACHABILITY[] =
    ".decl edge(x: int32, y: int32)\n"
    ".decl node(x: int32, y: int32)\n"
    "node(x, x) :- edge(x, y).\n"
    "node(y, y) :- edge(x, y).\n";

/* Transitive closure: canonical recursive program */
static const char PROG_TC[] =
    ".decl edge(x: int32, y: int32)\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n";

/* Same-generation: 3-body recursive join (Issue #352) */
static const char PROG_SAME_GEN[] =
    ".decl parent(x: int32, y: int32)\n"
    ".decl sg(x: int32, y: int32)\n"
    "sg(x, y) :- parent(x, p), parent(y, p).\n"
    "sg(x, y) :- parent(x, xp), sg(xp, yp), parent(y, yp).\n";

/* Multi-stratum: non-recursive edge_sym feeds recursive reach */
static const char PROG_MULTI_STRATUM[] =
    ".decl edge(x: int32, y: int32)\n"
    ".decl edge_sym(x: int32, y: int32)\n"
    "edge_sym(x, y) :- edge(x, y).\n"
    "edge_sym(y, x) :- edge(x, y).\n"
    ".decl reach(x: int32, y: int32)\n"
    "reach(x, y) :- edge_sym(x, y).\n"
    "reach(x, z) :- reach(x, y), edge_sym(y, z).\n";

/* ======================================================================== */
/* Correctness Tests                                                        */
/* ======================================================================== */

/*
 * test_reachability_w2:
 * Multi-rule non-recursive: two rules produce `node`.
 * Edges {(1,2),(2,3),(3,4)} -> node should have 4 distinct entries.
 * Uses W=2 to verify that multiworker dedup for non-recursive strata
 * correctly eliminates overlapping tuples from different partitions.
 */
static int
test_reachability_w2(void)
{
    TEST("Reachability multi-rule W=2: 3 edges yield 4 nodes");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(PROG_REACHABILITY, 2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    if (insert_facts(sess, "edge", edges, 3) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    uint32_t nrows = count_rows(sess, "node");
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (nrows != 4) {
        FAIL("expected 4 node rows");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_diamond_multiworker:
 * TC on a diamond graph (1->2, 1->3, 2->4, 3->4) with W=1,2,4.
 * Different topology from chain graphs tested elsewhere.
 * Expected TC: 8 tuples (1->2,1->3,1->4,2->4,3->4, plus 1->4 via both paths,
 * but dedup means 5 unique pairs; with both paths: (1,2),(1,3),(1,4),(2,4),(3,4)).
 * W=2 and W=4 must match W=1.
 */
static int
test_tc_diamond_multiworker(void)
{
    TEST("TC diamond graph: W=1,2,4 produce identical counts");

    wl_plan_t *p1 = NULL, *p2 = NULL, *p4 = NULL;
    wirelog_program_t *g1 = NULL, *g2 = NULL, *g4 = NULL;

    wl_col_session_t *s1 = make_session(PROG_TC, 1, &p1, &g1);
    wl_col_session_t *s2 = make_session(PROG_TC, 2, &p2, &g2);
    wl_col_session_t *s4 = make_session(PROG_TC, 4, &p4, &g4);

    if (!s1 || !s2 || !s4) {
        if (s1) cleanup_session(s1, p1, g1);
        if (s2) cleanup_session(s2, p2, g2);
        if (s4) cleanup_session(s4, p4, g4);
        FAIL("session create");
        return 1;
    }

    /* Diamond: 1->2, 1->3, 2->4, 3->4 */
    int64_t edges[] = { 1, 2, 1, 3, 2, 4, 3, 4 };
    insert_facts(s1, "edge", edges, 4);
    insert_facts(s2, "edge", edges, 4);
    insert_facts(s4, "edge", edges, 4);

    int rc1 = wl_session_step(&s1->base);
    int rc2 = wl_session_step(&s2->base);
    int rc4 = wl_session_step(&s4->base);

    uint32_t cnt1 = count_rows(s1, "tc");
    uint32_t cnt2 = count_rows(s2, "tc");
    uint32_t cnt4 = count_rows(s4, "tc");

    cleanup_session(s1, p1, g1);
    cleanup_session(s2, p2, g2);
    cleanup_session(s4, p4, g4);

    if (rc1 != 0 || rc2 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 == 0) {
        FAIL("W=1 baseline produced 0 tc rows");
        return 1;
    }
    if (cnt2 != cnt1 || cnt4 != cnt1) {
        FAIL("W=2 or W=4 tc count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_multi_stratum_multiworker:
 * 2-stratum program: non-recursive edge_sym -> recursive reach.
 * Chain 1->2->3: symmetrized reach should cover all 6 ordered pairs
 * (undirected connected component). W=2 and W=4 must match W=1.
 */
static int
test_multi_stratum_multiworker(void)
{
    TEST("Multi-stratum (2 strata): W=1,2,4 produce identical counts");

    wl_plan_t *p1 = NULL, *p2 = NULL, *p4 = NULL;
    wirelog_program_t *g1 = NULL, *g2 = NULL, *g4 = NULL;

    wl_col_session_t *s1 = make_session(PROG_MULTI_STRATUM, 1, &p1, &g1);
    wl_col_session_t *s2 = make_session(PROG_MULTI_STRATUM, 2, &p2, &g2);
    wl_col_session_t *s4 = make_session(PROG_MULTI_STRATUM, 4, &p4, &g4);

    if (!s1 || !s2 || !s4) {
        if (s1) cleanup_session(s1, p1, g1);
        if (s2) cleanup_session(s2, p2, g2);
        if (s4) cleanup_session(s4, p4, g4);
        FAIL("session create");
        return 1;
    }

    /* Directed chain: 1->2->3 */
    int64_t edges[] = { 1, 2, 2, 3 };
    insert_facts(s1, "edge", edges, 2);
    insert_facts(s2, "edge", edges, 2);
    insert_facts(s4, "edge", edges, 2);

    int rc1 = wl_session_step(&s1->base);
    int rc2 = wl_session_step(&s2->base);
    int rc4 = wl_session_step(&s4->base);

    uint32_t reach1 = count_rows(s1, "reach");
    uint32_t reach2 = count_rows(s2, "reach");
    uint32_t reach4 = count_rows(s4, "reach");

    cleanup_session(s1, p1, g1);
    cleanup_session(s2, p2, g2);
    cleanup_session(s4, p4, g4);

    if (rc1 != 0 || rc2 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (reach1 == 0) {
        FAIL("W=1 baseline produced 0 reach rows");
        return 1;
    }
    if (reach2 != reach1 || reach4 != reach1) {
        FAIL("W=2 or W=4 reach count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_same_generation_multiworker:
 * Issue #352: 3-body recursive join with self-join on non-col0 column.
 * sg(x,y) :- parent(x,p), parent(y,p).
 * sg(x,y) :- parent(x,xp), sg(xp,yp), parent(y,yp).
 * Parent edges: (1,0),(2,0),(3,1),(4,1),(5,2).
 * W=1 baseline: 13 sg rows. W=2 must match.
 */
static int
test_same_generation_multiworker(void)
{
    TEST("Same-generation 3-body join: W=2 matches W=1 (Issue #352)");

    wl_plan_t *p1 = NULL, *p2 = NULL;
    wirelog_program_t *g1 = NULL, *g2 = NULL;

    wl_col_session_t *s1 = make_session(PROG_SAME_GEN, 1, &p1, &g1);
    wl_col_session_t *s2 = make_session(PROG_SAME_GEN, 2, &p2, &g2);

    if (!s1 || !s2) {
        if (s1) cleanup_session(s1, p1, g1);
        if (s2) cleanup_session(s2, p2, g2);
        FAIL("session create");
        return 1;
    }

    int64_t parents[] = { 1, 0, 2, 0, 3, 1, 4, 1, 5, 2 };
    insert_facts(s1, "parent", parents, 5);
    insert_facts(s2, "parent", parents, 5);

    int rc1 = wl_session_step(&s1->base);
    int rc2 = wl_session_step(&s2->base);

    uint32_t cnt1 = count_rows(s1, "sg");
    uint32_t cnt2 = count_rows(s2, "sg");

    cleanup_session(s1, p1, g1);
    cleanup_session(s2, p2, g2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != 13) {
        FAIL("W=1 baseline: expected 13 sg rows");
        return 1;
    }
    if (cnt2 != cnt1) {
        FAIL("W=2 sg count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Determinism Tests                                                        */
/* ======================================================================== */

#define DETERMINISM_RUNS 5

/*
 * test_tc_determinism_w2:
 * Run TC W=2 on 6-node chain 5 times, verify identical row counts.
 */
static int
test_tc_determinism_w2(void)
{
    TEST("TC determinism W=2: 5 runs produce identical row counts");

    uint32_t counts[DETERMINISM_RUNS];

    for (uint32_t r = 0; r < DETERMINISM_RUNS; r++) {
        wl_plan_t *plan = NULL;
        wirelog_program_t *prog = NULL;
        wl_col_session_t *sess = make_session(PROG_TC, 2, &plan, &prog);
        if (!sess) {
            FAIL("session create");
            return 1;
        }

        int64_t edges[] = { 1, 2, 2, 3, 3, 4, 4, 5, 5, 6 };
        if (insert_facts(sess, "edge", edges, 5) != 0) {
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

    for (uint32_t r = 1; r < DETERMINISM_RUNS; r++) {
        if (counts[r] != counts[0]) {
            FAIL("W=2 non-deterministic: row counts differ across runs");
            return 1;
        }
    }
    PASS();
    return 0;
}

/*
 * test_tc_determinism_w4:
 * Run TC W=4 on 6-node chain 5 times, verify identical row counts.
 */
static int
test_tc_determinism_w4(void)
{
    TEST("TC determinism W=4: 5 runs produce identical row counts");

    uint32_t counts[DETERMINISM_RUNS];

    for (uint32_t r = 0; r < DETERMINISM_RUNS; r++) {
        wl_plan_t *plan = NULL;
        wirelog_program_t *prog = NULL;
        wl_col_session_t *sess = make_session(PROG_TC, 4, &plan, &prog);
        if (!sess) {
            FAIL("session create");
            return 1;
        }

        int64_t edges[] = { 1, 2, 2, 3, 3, 4, 4, 5, 5, 6 };
        if (insert_facts(sess, "edge", edges, 5) != 0) {
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

    for (uint32_t r = 1; r < DETERMINISM_RUNS; r++) {
        if (counts[r] != counts[0]) {
            FAIL("W=4 non-deterministic: row counts differ across runs");
            return 1;
        }
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Edge Case Tests                                                          */
/* ======================================================================== */

/*
 * test_single_tuple_w2:
 * Single edge (1,2) with W=2: TC must contain exactly 1 tuple.
 * Verifies partition logic when only 1 of W workers gets data.
 */
static int
test_single_tuple_w2(void)
{
    TEST("Single tuple W=2: TC yields 1 tuple");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(PROG_TC, 2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t edges[] = { 1, 2 };
    if (insert_facts(sess, "edge", edges, 1) != 0) {
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
 * test_empty_multi_stratum_w1:
 * Multi-stratum program with empty EDB and W=1.
 * All relations must be empty, no error.
 * Verifies multi-stratum empty path through the integration pipeline.
 */
static int
test_empty_multi_stratum_w1(void)
{
    TEST("Empty multi-stratum W=1: all relations empty");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(PROG_MULTI_STRATUM, 1, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    uint32_t sym_rows = count_rows(sess, "edge_sym");
    uint32_t reach_rows = count_rows(sess, "reach");
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (sym_rows != 0 || reach_rows != 0) {
        FAIL("expected 0 rows from empty EDB");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_w1_fallback_api:
 * W=1 through the TDD session API on TC 5-node chain.
 * Must produce correct result (10 tuples), confirming W=1 delegates
 * properly through the integration path.
 */
static int
test_w1_fallback_api(void)
{
    TEST("W=1 fallback: TC 5-node chain yields 10 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(PROG_TC, 1, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t edges[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
    if (insert_facts(sess, "edge", edges, 4) != 0) {
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
    if (nrows != 10) {
        FAIL("expected 10 tc tuples from 5-node chain");
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
    printf("TDD Integration Tests (Issue #321)\n");
    printf("===================================\n");

    printf("\n-- Correctness --\n");
    test_reachability_w2();
    test_tc_diamond_multiworker();
    test_same_generation_multiworker();
    test_multi_stratum_multiworker();

    printf("\n-- Determinism --\n");
    test_tc_determinism_w2();
    test_tc_determinism_w4();

    printf("\n-- Edge Cases --\n");
    test_single_tuple_w2();
    test_empty_multi_stratum_w1();
    test_w1_fallback_api();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
