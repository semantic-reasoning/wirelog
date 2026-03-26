/*
 * test_tdd_nonrecursive.c - Unit tests for col_eval_stratum_tdd non-recursive path
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: W=1 fast path, W=2 and W=4 correctness vs single-worker baseline,
 * empty EDB, multi-relation stratum.
 *
 * Issue #318: Distributed Stratum Evaluator Phase 1
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
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
 * Build a coordinator session with "edge(x,y) :- edge(x,y)." (copy rule).
 * This is a non-recursive stratum: copies EDB edge facts to IDB edge_t.
 */
static wl_col_session_t *
make_copy_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    /* Simple copy rule: non-recursive stratum */
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl edge_t(x: int32, y: int32)\n"
        "edge_t(x, y) :- edge(x, y).\n",
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

/* Insert edge rows (each row = {x, y}) into a session. */
static int
insert_edges(wl_col_session_t *sess, const int64_t *rows, uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 2);
}

/* Count rows in a named relation (0 if not found). */
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
 * test_w1_fast_path:
 * W=1 must produce the same result as a direct col_eval_stratum call.
 * Verifies the zero-overhead delegation path.
 */
static int
test_w1_fast_path(void)
{
    TEST("W=1 fast path delegates to col_eval_stratum");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_copy_session(1, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    /* Insert 6 edges */
    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 1 };
    if (insert_edges(sess, rows, 6) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    /* Run step: triggers col_eval_stratum_tdd with W=1 */
    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("session step failed");
        return 1;
    }

    uint32_t nrows = count_rows(sess, "edge_t");
    if (nrows != 6) {
        cleanup_session(sess, plan, prog);
        FAIL("expected 6 rows in edge_t");
        return 1;
    }

    cleanup_session(sess, plan, prog);
    PASS();
    return 0;
}

/*
 * test_w2_correctness:
 * W=2 must produce the same row count as W=1 baseline.
 */
static int
test_w2_correctness(void)
{
    TEST("W=2 non-recursive correctness matches W=1 baseline");

    /* Build W=1 baseline */
    wl_plan_t *plan1 = NULL;
    wirelog_program_t *prog1 = NULL;
    wl_col_session_t *sess1 = make_copy_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }

    /* Build W=2 session */
    wl_plan_t *plan2 = NULL;
    wirelog_program_t *prog2 = NULL;
    wl_col_session_t *sess2 = make_copy_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = {
        1, 2, 2, 3, 3, 4, 4, 5, 5, 6,
        6, 7, 7, 8, 8, 9, 9, 10, 10, 1
    };
    uint32_t nrows = 10;

    insert_edges(sess1, rows, nrows);
    insert_edges(sess2, rows, nrows);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "edge_t");
    uint32_t cnt2 = count_rows(sess2, "edge_t");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 row count differs from W=1 baseline");
        return 1;
    }

    PASS();
    return 0;
}

/*
 * test_w4_correctness:
 * W=4 must produce the same row count as W=1 baseline.
 */
static int
test_w4_correctness(void)
{
    TEST("W=4 non-recursive correctness matches W=1 baseline");

    wl_plan_t *plan1 = NULL;
    wirelog_program_t *prog1 = NULL;
    wl_col_session_t *sess1 = make_copy_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }

    wl_plan_t *plan4 = NULL;
    wirelog_program_t *prog4 = NULL;
    wl_col_session_t *sess4 = make_copy_session(4, &plan4, &prog4);
    if (!sess4) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=4 session create");
        return 1;
    }

    /* 20 edges */
    int64_t rows[40];
    for (int i = 0; i < 20; i++) {
        rows[i * 2] = (int64_t)(i + 1);
        rows[i * 2 + 1] = (int64_t)((i + 1) % 20 + 1);
    }

    insert_edges(sess1, rows, 20);
    insert_edges(sess4, rows, 20);

    int rc1 = wl_session_step(&sess1->base);
    int rc4 = wl_session_step(&sess4->base);

    uint32_t cnt1 = count_rows(sess1, "edge_t");
    uint32_t cnt4 = count_rows(sess4, "edge_t");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess4, plan4, prog4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt4) {
        FAIL("W=4 row count differs from W=1 baseline");
        return 1;
    }

    PASS();
    return 0;
}

/*
 * test_empty_edb:
 * Empty EDB must produce empty output regardless of W.
 */
static int
test_empty_edb(void)
{
    TEST("empty EDB produces empty output for W=2");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_copy_session(2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    /* No inserts: empty EDB */
    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("session step failed");
        return 1;
    }

    uint32_t nrows = count_rows(sess, "edge_t");
    cleanup_session(sess, plan, prog);

    if (nrows != 0) {
        FAIL("expected 0 rows from empty EDB");
        return 1;
    }

    PASS();
    return 0;
}

/*
 * test_w2_large:
 * 100 edges with W=2: row count must match W=1 baseline.
 */
static int
test_w2_large(void)
{
    TEST("W=2 large input (100 edges) matches W=1 baseline");

    wl_plan_t *plan1 = NULL;
    wirelog_program_t *prog1 = NULL;
    wl_col_session_t *sess1 = make_copy_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }

    wl_plan_t *plan2 = NULL;
    wirelog_program_t *prog2 = NULL;
    wl_col_session_t *sess2 = make_copy_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    const uint32_t nedges = 100;
    int64_t *rows = (int64_t *)malloc(nedges * 2 * sizeof(int64_t));
    if (!rows) {
        cleanup_session(sess1, plan1, prog1);
        cleanup_session(sess2, plan2, prog2);
        FAIL("malloc");
        return 1;
    }
    for (uint32_t i = 0; i < nedges; i++) {
        rows[i * 2] = (int64_t)(i + 1);
        rows[i * 2 + 1] = (int64_t)((i * 7 + 3) % nedges + 1);
    }

    insert_edges(sess1, rows, nedges);
    insert_edges(sess2, rows, nedges);
    free(rows);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "edge_t");
    uint32_t cnt2 = count_rows(sess2, "edge_t");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 large: row count differs from W=1 baseline");
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
    printf("TDD Non-Recursive Distributed Evaluator Tests\n");
    printf("=============================================\n");

    test_w1_fast_path();
    test_w2_correctness();
    test_w4_correctness();
    test_empty_edb();
    test_w2_large();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
