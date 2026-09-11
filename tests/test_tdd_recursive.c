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

#ifdef WL_TEST_ALLOC_WRAP
void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);

static long allocation_fail_at = -1;
static long allocation_calls;

static bool
fail_this_allocation(void)
{
    return allocation_fail_at >= 0
           && allocation_calls++ == allocation_fail_at;
}

void *
__wrap_malloc(size_t size)
{
    return fail_this_allocation() ? NULL : __real_malloc(size);
}

void *
__wrap_calloc(size_t count, size_t size)
{
    return fail_this_allocation() ? NULL : __real_calloc(count, size);
}

void *
__wrap_realloc(void *ptr, size_t size)
{
    return fail_this_allocation() ? NULL : __real_realloc(ptr, size);
}
#endif

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

static int
insert_filtered_edges(wl_col_session_t *sess, const int64_t *rows,
    uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 3);
}

static uint32_t
count_rows(wl_col_session_t *sess, const char *name)
{
    col_rel_t *r = session_find_rel(sess, name);

    return r ? r->nrows : 0;
}

static int
relation_rows_equal(wl_col_session_t *left, wl_col_session_t *right,
    const char *name)
{
    col_rel_t *lr = session_find_rel(left, name);
    col_rel_t *rr = session_find_rel(right, name);
    if (!lr || !rr)
        return lr == rr;
    if (lr->ncols != rr->ncols || lr->nrows != rr->nrows)
        return 0;

    col_rel_radix_sort_int64(lr);
    col_rel_radix_sort_int64(rr);

    for (uint32_t c = 0; c < lr->ncols; c++)
        for (uint32_t r = 0; r < lr->nrows; r++)
            if (lr->columns[c][r] != rr->columns[c][r])
                return 0;
    return 1;
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
/* BDX integration tests                                                    */
/* ======================================================================== */

/*
 * Self-join TC program:
 *   .decl edge(x: int32, y: int32)
 *   .decl r(x: int32, y: int32)
 *   r(x, y) :- edge(x, y).
 *   r(x, z) :- r(x, y), r(y, z).
 *
 * The second rule is an IDB-IDB self-join (Category C) → triggers BDX mode.
 */
static wl_col_session_t *
make_selfjoin_tc_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- edge(x, y).\n"
        "r(x, z) :- r(x, y), r(y, z).\n",
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
 * 5-node chain: 1→2→3→4→5   ⇒  TC has 10 tuples (4+3+2+1).
 */
static int
test_bdx_selfjoin_w2(void)
{
    TEST("BDX self-join W=2 correctness");

    wl_plan_t *p1, *p2;
    wirelog_program_t *pr1, *pr2;
    wl_col_session_t *s1 = make_selfjoin_tc_session(1, &p1, &pr1);
    wl_col_session_t *s2 = make_selfjoin_tc_session(2, &p2, &pr2);
    if (!s1 || !s2) {
        FAIL("session creation failed");
        return 1;
    }

    /* 5-node chain */
    int64_t edges[] = {1, 2, 2, 3, 3, 4, 4, 5};
    insert_edges(s1, edges, 4);
    insert_edges(s2, edges, 4);

    int rc1 = wl_session_step(&s1->base);
    int rc2 = wl_session_step(&s2->base);

    int equal = relation_rows_equal(s1, s2, "r");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s2, p2, pr2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (!equal) {
        FAIL("BDX W=2 row set differs from W=1");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_bdx_selfjoin_w4(void)
{
    TEST("BDX self-join W=4 correctness");

    wl_plan_t *p1, *p4;
    wirelog_program_t *pr1, *pr4;
    wl_col_session_t *s1 = make_selfjoin_tc_session(1, &p1, &pr1);
    wl_col_session_t *s4 = make_selfjoin_tc_session(4, &p4, &pr4);
    if (!s1 || !s4) {
        FAIL("session creation failed");
        return 1;
    }

    /* 5-node chain */
    int64_t edges[] = {1, 2, 2, 3, 3, 4, 4, 5};
    insert_edges(s1, edges, 4);
    insert_edges(s4, edges, 4);

    int rc1 = wl_session_step(&s1->base);
    int rc4 = wl_session_step(&s4->base);

    int equal = relation_rows_equal(s1, s4, "r");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s4, p4, pr4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (!equal) {
        FAIL("BDX W=4 row set differs from W=1");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_bdx_selfjoin_w4_shuffled_duplicates(void)
{
    TEST("BDX self-join W=4 shuffled duplicate edges matches W=1 rows");

    wl_plan_t *p1, *p4;
    wirelog_program_t *pr1, *pr4;
    wl_col_session_t *s1 = make_selfjoin_tc_session(1, &p1, &pr1);
    wl_col_session_t *s4 = make_selfjoin_tc_session(4, &p4, &pr4);
    if (!s1 || !s4) {
        FAIL("session creation failed");
        return 1;
    }

    int64_t edges[] = {
        3, 4, 1, 2, 2, 3, 4, 5, 2, 3,
        1, 2, 5, 6, 3, 4, 6, 7, 4, 5
    };
    insert_edges(s1, edges, 10);
    insert_edges(s4, edges, 10);

    int rc1 = wl_session_step(&s1->base);
    int rc4 = wl_session_step(&s4->base);

    int equal = relation_rows_equal(s1, s4, "r");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s4, p4, pr4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (!equal) {
        FAIL("BDX W=4 shuffled duplicate row set differs from W=1");
        return 1;
    }
    PASS();
    return 0;
}

static col_rel_t *
make_seed_relation(const char *name, const int64_t *rows, uint32_t nrows,
    uint32_t ncols)
{
    col_rel_t *rel = col_rel_new_auto(name, ncols);
    if (!rel)
        return NULL;
    for (uint32_t row = 0; row < nrows; row++) {
        if (col_rel_append_row(rel, rows + (size_t)row * ncols) != 0) {
            col_rel_destroy(rel);
            return NULL;
        }
    }
    return rel;
}

typedef struct {
    uint32_t nrows;
    uint32_t ncols;
    uint32_t capacity;
    int64_t **columns;
    char **col_names;
    col_delta_timestamp_t *timestamps;
    uint64_t view_generation;
    uint64_t storage_generation;
    uint64_t ledger_ts_bytes;
    uint64_t owned_ledger_bytes;
    uint64_t *dedup_slots;
    uint32_t dedup_cap;
    uint32_t dedup_count;
    int64_t first_row[2];
    col_delta_timestamp_t first_timestamp;
} bdx_seed_snapshot_t;

static void
capture_bdx_seed_snapshot(const col_rel_t *rel, bdx_seed_snapshot_t *snapshot)
{
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->nrows = rel->nrows;
    snapshot->ncols = rel->ncols;
    snapshot->capacity = rel->capacity;
    snapshot->columns = rel->columns;
    snapshot->col_names = rel->col_names;
    snapshot->timestamps = rel->timestamps;
    snapshot->view_generation = rel->view_generation;
    snapshot->storage_generation = rel->storage_generation;
    snapshot->ledger_ts_bytes = rel->ledger_ts_bytes;
    snapshot->owned_ledger_bytes = col_rel_owned_ledger_bytes(rel);
    snapshot->dedup_slots = rel->dedup_slots;
    snapshot->dedup_cap = rel->dedup_cap;
    snapshot->dedup_count = rel->dedup_count;
    if (rel->nrows > 0 && rel->ncols >= 2) {
        snapshot->first_row[0] = rel->columns[0][0];
        snapshot->first_row[1] = rel->columns[1][0];
    }
    if (rel->timestamps && rel->nrows > 0)
        snapshot->first_timestamp = rel->timestamps[0];
}

static bool
bdx_seed_snapshot_unchanged(const col_rel_t *rel,
    const bdx_seed_snapshot_t *snapshot)
{
    return rel->nrows == snapshot->nrows
           && rel->ncols == snapshot->ncols
           && rel->capacity == snapshot->capacity
           && rel->columns == snapshot->columns
           && rel->col_names == snapshot->col_names
           && rel->timestamps == snapshot->timestamps
           && rel->view_generation == snapshot->view_generation
           && rel->storage_generation == snapshot->storage_generation
           && rel->ledger_ts_bytes == snapshot->ledger_ts_bytes
           && col_rel_owned_ledger_bytes(rel) == snapshot->owned_ledger_bytes
           && rel->dedup_slots == snapshot->dedup_slots
           && rel->dedup_cap == snapshot->dedup_cap
           && rel->dedup_count == snapshot->dedup_count
           && (rel->nrows == 0 || (rel->columns[0][0]
           == snapshot->first_row[0]
           && rel->columns[1][0] == snapshot->first_row[1]))
           && (!rel->timestamps || rel->nrows == 0
           || memcmp(&rel->timestamps[0], &snapshot->first_timestamp,
           sizeof(snapshot->first_timestamp)) == 0);
}

static void
prepare_bdx_seed_metadata(col_rel_t *rel)
{
    rel->dedup_slots = (uint64_t *)calloc(4, sizeof(*rel->dedup_slots));
    rel->dedup_cap = 4;
    rel->dedup_count = 2;
    rel->dedup_slots[1] = 0x1111;
    rel->dedup_slots[3] = 0x3333;
    (void)col_rel_enable_timestamps(rel);
    rel->timestamps[0].iteration = 17;
    rel->timestamps[0].multiplicity = -4;
}

static int
test_bdx_seed_reader_denial_and_retry(void)
{
    TEST("BDX seed reader denial preserves cidb and retries");

    const int64_t initial[] = { 99, 100 };
    const int64_t worker_rows[] = { 2, 3, 1, 2, 2, 3 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 3, 2);
    col_rel_t *workers[] = { worker };
    wl_columnar_source_access_reader_t reader = { 0 };
    uint64_t generation;
    int rc;

    if (!cidb || !worker || col_rel_source_reader_acquire(cidb, &reader) != 0) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("seed reader setup");
        return 1;
    }
    generation = cidb->view_generation;
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != EBUSY || cidb->nrows != 1
        || cidb->columns[0][0] != 99 || cidb->columns[1][0] != 100
        || cidb->view_generation != generation) {
        col_rel_source_reader_release(&reader);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("active reader was not denied transactionally");
        return 1;
    }
    if (col_rel_source_reader_release(&reader) != 0
        || wl_columnar_eval_test_bdx_seed(cidb, workers, 1) != 0
        || cidb->nrows != 2
        || cidb->columns[0][0] != 1 || cidb->columns[1][0] != 2
        || cidb->columns[0][1] != 2 || cidb->columns[1][1] != 3) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("seed retry or dedup result");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_alias_denial_and_retry(void)
{
    TEST("BDX seed live-alias denial preserves cidb and retries");

    const int64_t initial[] = { 7, 8 };
    const int64_t worker_rows[] = { 4, 5, 4, 5 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 2, 2);
    col_rel_t *alias = col_rel_new_auto("alias", 2);
    col_rel_t *workers[] = { worker };
    int rc;

    if (!cidb || !worker || !alias
        || col_rel_install_shared_view(alias, cidb) != 0) {
        col_rel_destroy(alias);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("seed alias setup");
        return 1;
    }
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != EBUSY || cidb->nrows != 1
        || cidb->columns[0][0] != 7 || cidb->columns[1][0] != 8) {
        col_rel_storage_alias_release(alias);
        col_rel_destroy(alias);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("live alias was not denied transactionally");
        return 1;
    }
    if (col_rel_storage_alias_release(alias) != 0
        || wl_columnar_eval_test_bdx_seed(cidb, workers, 1) != 0
        || cidb->nrows != 1
        || cidb->columns[0][0] != 4 || cidb->columns[1][0] != 5) {
        col_rel_destroy(alias);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("alias retry or dedup result");
        return 1;
    }
    col_rel_destroy(alias);
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_staging_failure_is_atomic(void)
{
    TEST("BDX seed staging failure leaves cidb unchanged");

    const int64_t initial[] = { 11, 12 };
    const int64_t bad_rows[] = { 1, 2, 3 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *bad_worker = make_seed_relation("r", bad_rows, 1, 3);
    col_rel_t *workers[] = { bad_worker };
    uint64_t generation;
    int rc;

    if (!cidb || !bad_worker) {
        col_rel_destroy(cidb);
        col_rel_destroy(bad_worker);
        FAIL("seed staging setup");
        return 1;
    }
    generation = cidb->view_generation;
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != EINVAL || cidb->nrows != 1
        || cidb->ncols != 2 || cidb->columns[0][0] != 11
        || cidb->columns[1][0] != 12
        || cidb->view_generation != generation) {
        col_rel_destroy(cidb);
        col_rel_destroy(bad_worker);
        FAIL("schema staging failure changed cidb");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(bad_worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_second_worker_failure_is_atomic(void)
{
    TEST("BDX seed second-worker staging failure is atomic");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker0_rows[] = { 1, 2, 2, 3 };
    const int64_t worker1_rows[] = { 3, 4, 3, 4 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker0 = make_seed_relation("r", worker0_rows, 2, 2);
    col_rel_t *worker1 = make_seed_relation("r", worker1_rows, 2, 2);
    col_rel_t *workers[] = { worker0, worker1 };
    bdx_seed_snapshot_t snapshot;
    int rc;

    if (!cidb || !worker0 || !worker1) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker0);
        col_rel_destroy(worker1);
        FAIL("second-worker staging setup");
        return 1;
    }
    prepare_bdx_seed_metadata(cidb);
    capture_bdx_seed_snapshot(cidb, &snapshot);
    wl_columnar_eval_test_bdx_seed_fail_worker(1);
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 2);
    if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(cidb, &snapshot)) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker0);
        col_rel_destroy(worker1);
        FAIL("second-worker failure changed cidb");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker0);
    col_rel_destroy(worker1);
    PASS();
    return 0;
}

static int
test_bdx_seed_sort_failure_is_atomic(void)
{
    TEST("BDX seed sort failure is atomic");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker_rows[] = { 2, 3, 1, 2 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 2, 2);
    col_rel_t *workers[] = { worker };
    bdx_seed_snapshot_t snapshot;
    int rc;

    if (!cidb || !worker) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("sort failure setup");
        return 1;
    }
    prepare_bdx_seed_metadata(cidb);
    capture_bdx_seed_snapshot(cidb, &snapshot);
    wl_columnar_eval_test_bdx_seed_fail_sort_once();
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(cidb, &snapshot)) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("sort failure changed cidb");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_allocation_failure_is_atomic(void)
{
#ifdef WL_TEST_ALLOC_WRAP
    TEST("BDX seed allocation failure is atomic");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker_rows[] = { 2, 3, 1, 2 };
    bool observed_failure = false;

    for (long fail_at = 0; fail_at < 64 && !observed_failure; fail_at++) {
        col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker_rows, 2, 2);
        col_rel_t *workers[] = { worker };
        bdx_seed_snapshot_t snapshot;
        int rc;
        if (!cidb || !worker) {
            col_rel_destroy(cidb);
            col_rel_destroy(worker);
            FAIL("allocation failure setup");
            return 1;
        }
        prepare_bdx_seed_metadata(cidb);
        capture_bdx_seed_snapshot(cidb, &snapshot);
        allocation_calls = 0;
        allocation_fail_at = fail_at;
        rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
        allocation_fail_at = -1;
        if (rc == ENOMEM) {
            observed_failure = true;
            if (!bdx_seed_snapshot_unchanged(cidb, &snapshot)) {
                col_rel_destroy(cidb);
                col_rel_destroy(worker);
                FAIL("allocation failure changed cidb");
                return 1;
            }
        } else if (rc != 0) {
            col_rel_destroy(cidb);
            col_rel_destroy(worker);
            FAIL("unexpected allocation failure result");
            return 1;
        }
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
    }
    allocation_fail_at = -1;
    if (!observed_failure) {
        FAIL("allocation failure seam was not exercised");
        return 1;
    }
    PASS();
    return 0;
#else
    fputs("BDX seed allocation failure coverage skipped\n", stderr);
    return 0;
#endif
}

static int
test_bdx_seed_success_preserves_timestamps(void)
{
    TEST("BDX seed success preserves timestamp values and accounting");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker_rows[] = { 2, 3, 1, 2, 2, 3 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 3, 2);
    col_rel_t *workers[] = { worker };
    wl_mem_ledger_t ledger;
    int rc;

    if (!cidb || !worker || col_rel_enable_timestamps(cidb) != 0
        || col_rel_enable_timestamps(worker) != 0) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("timestamp success setup");
        return 1;
    }
    wl_mem_ledger_init(&ledger, 0);
    cidb->mem_ledger = &ledger;
    col_rel_ledger_reconcile(cidb, 0);
    worker->timestamps[0].iteration = 20;
    worker->timestamps[0].multiplicity = 2;
    worker->timestamps[1].iteration = 10;
    worker->timestamps[1].multiplicity = 1;
    worker->timestamps[2].iteration = 30;
    worker->timestamps[2].multiplicity = 3;
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != 0 || cidb->nrows != 2 || !cidb->timestamps
        || cidb->columns[0][0] != 1 || cidb->columns[1][0] != 2
        || cidb->timestamps[0].iteration != 10
        || cidb->timestamps[0].multiplicity != 1
        || cidb->columns[0][1] != 2 || cidb->columns[1][1] != 3
        || cidb->timestamps[1].iteration != 20
        || cidb->timestamps[1].multiplicity != 2
        || cidb->ledger_ts_bytes != col_rel_timestamp_ledger_bytes(cidb)
        || cidb->ledger_ts_bytes == 0) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("timestamp values or accounting not preserved");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

#ifdef WL_TEST_TDD_MERGE
static int
test_tdd_merge_transactional_publication(void)
{
    TEST("TDD merge stages worker results before relation publication");

    const int64_t initial[] = { 9, 9 };
    const int64_t worker0_rows[] = { 2, 3, 1, 2 };
    const int64_t worker1_rows[] = { 4, 5, 4, 5 };
    int rc;

    /* A live reader denies publication, then the same transaction retries. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *workers[] = { worker };
        wl_columnar_source_access_reader_t reader = { 0 };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker
            || col_rel_source_reader_acquire(target, &reader) != 0) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge reader setup");
            return 1;
        }
        capture_bdx_seed_snapshot(target, &snapshot);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EBUSY || !bdx_seed_snapshot_unchanged(target, &snapshot)
            || col_rel_source_reader_release(&reader) != 0) {
            col_rel_source_reader_release(&reader);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge reader denial or retry");
            return 1;
        }
        if (wl_columnar_eval_test_tdd_merge(&target, workers, 1) != 0
            || target->nrows != 3 || target->columns[0][0] != 1
            || target->columns[1][0] != 2) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge reader retry");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(worker);
    }

    /* A live alias is also denied without changing rows or metadata. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *alias = col_rel_new_auto("alias", 2);
        col_rel_t *workers[] = { worker };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker || !alias
            || col_rel_install_shared_view(alias, target) != 0) {
            col_rel_destroy(alias);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge alias setup");
            return 1;
        }
        capture_bdx_seed_snapshot(target, &snapshot);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EBUSY || !bdx_seed_snapshot_unchanged(target, &snapshot)
            || col_rel_storage_alias_release(alias) != 0) {
            col_rel_storage_alias_release(alias);
            col_rel_destroy(alias);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge alias denial or retry");
            return 1;
        }
        if (wl_columnar_eval_test_tdd_merge(&target, workers, 1) != 0) {
            col_rel_destroy(alias);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge alias retry");
            return 1;
        }
        col_rel_destroy(alias);
        col_rel_destroy(target);
        col_rel_destroy(worker);
    }

    /* Failure in the second worker must not publish the first worker. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker0 = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *worker1 = make_seed_relation("r", worker1_rows, 2, 2);
        col_rel_t *workers[] = { worker0, worker1 };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker0 || !worker1) {
            col_rel_destroy(target);
            col_rel_destroy(worker0);
            col_rel_destroy(worker1);
            FAIL("merge failure setup");
            return 1;
        }
        prepare_bdx_seed_metadata(target);
        capture_bdx_seed_snapshot(target, &snapshot);
        wl_columnar_eval_test_tdd_merge_fail_worker(1);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 2);
        if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(worker0);
            col_rel_destroy(worker1);
            FAIL("second worker failure changed target");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(worker0);
        col_rel_destroy(worker1);
    }

    /* Schema, overflow, and sort errors all preserve the target. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        const int64_t bad_schema_rows[] = { 1, 2, 3, 4, 5, 6 };
        col_rel_t *bad_schema = make_seed_relation("r", bad_schema_rows, 2,
                3);
        col_rel_t *valid_worker = make_seed_relation("r", worker0_rows, 2,
                2);
        col_rel_t *workers[] = { bad_schema };
        bdx_seed_snapshot_t snapshot;

        if (!target || !bad_schema || !valid_worker) {
            col_rel_destroy(target);
            col_rel_destroy(bad_schema);
            col_rel_destroy(valid_worker);
            FAIL("merge schema setup");
            return 1;
        }
        capture_bdx_seed_snapshot(target, &snapshot);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(bad_schema);
            col_rel_destroy(valid_worker);
            FAIL("schema failure changed target");
            return 1;
        }
        workers[0] = valid_worker;
        wl_columnar_eval_test_tdd_merge_fail_overflow_once();
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EOVERFLOW || !bdx_seed_snapshot_unchanged(target,
            &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(bad_schema);
            col_rel_destroy(valid_worker);
            FAIL("overflow failure changed target");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(bad_schema);
        col_rel_destroy(valid_worker);
    }

    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *workers[] = { worker };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge sort setup");
            return 1;
        }
        prepare_bdx_seed_metadata(target);
        capture_bdx_seed_snapshot(target, &snapshot);
        wl_columnar_eval_test_tdd_merge_fail_sort_once();
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("sort failure changed target");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(worker);
    }

    PASS();
    return 0;
}

static int
test_tdd_merge_schema_mismatch_rollback(void)
{
    TEST("TDD merge rejects complete schema mismatches before staging");

    const int64_t initial[] = { 9, 9 };
    const int64_t worker_rows[] = { 2, 3 };
    col_rel_t *target = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 1, 2);
    col_rel_t *workers[] = { worker };
    bdx_seed_snapshot_t snapshot;
    int rc;

    if (!target || !worker) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("schema mismatch setup");
        return 1;
    }
    prepare_bdx_seed_metadata(target);
    capture_bdx_seed_snapshot(target, &snapshot);

    worker->declared_ncols = target->declared_ncols + 1;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("declared width mismatch changed target");
        return 1;
    }
    worker->declared_ncols = target->declared_ncols;

    worker->schema_ok = false;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("schema state mismatch changed target");
        return 1;
    }
    worker->schema_ok = target->schema_ok;

    free(worker->column_types);
    worker->column_types = (wirelog_column_type_t *)malloc(
        worker->ncols * sizeof(*worker->column_types));
    if (!worker->column_types) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column type mismatch setup");
        return 1;
    }
    for (uint32_t col = 0; col < worker->ncols; col++)
        worker->column_types[col] = WIRELOG_TYPE_FLOAT;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column type mismatch changed target");
        return 1;
    }
    free(worker->column_types);
    worker->column_types = NULL;

    free(worker->col_names[0]);
    worker->col_names[0] = strdup("different");
    if (!worker->col_names[0]) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column name mismatch setup");
        return 1;
    }
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column name mismatch changed target");
        return 1;
    }
    free(worker->col_names[0]);
    worker->col_names[0] = strdup(target->col_names[0]);
    if (!worker->col_names[0]) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("compound mismatch setup");
        return 1;
    }

    worker->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    worker->compound_count = 1;
    worker->compound_arity_map = (uint32_t *)malloc(2 * sizeof(uint32_t));
    if (!worker->compound_arity_map) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("compound map mismatch setup");
        return 1;
    }
    worker->compound_arity_map[0] = 1;
    worker->compound_arity_map[1] = 1;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("compound schema mismatch changed target");
        return 1;
    }

    col_rel_destroy(target);
    col_rel_destroy(worker);
    PASS();
    return 0;
}
#endif

/* ======================================================================== */
/* Microbenchmark: Filtered-join arrangement cache (Issue #433)             */
/* ======================================================================== */

/*
 * make_filtered_tc_session:
 * Build a TC session with a constant filter in the right-side EDB atom:
 *   tc(x, y) :- edge(x, y, 1).
 *   tc(x, z) :- tc(x, y), edge(y, z, 1).
 *
 * Explicit comparisons such as y > 0 and z > 0 are lowered as FILTER
 * operators above the join; JPP does not move them into right_filter_expr.
 * Constants inside a right-side atom are collected as scan-local filters,
 * which triggers the filt_cache path in col_op_join.  With the filt_arr
 * optimization (Issue #433), the arrangement for the filtered edge relation
 * is cached across sub-passes rather than rebuilt each time.
 */
static wl_col_session_t *
make_filtered_tc_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32, tag: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y, 1).\n"
        "tc(x, z) :- tc(x, y), edge(y, z, 1).\n",
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
 * test_filt_arr_bench_w1:
 * W=1 filtered TC on a 10-node chain (9 edges, all y > 0).
 * Expected: 45 tuples (all pairs i<j in 1..10).
 *
 * Correctness regression for filt_arr arrangement cache (Issue #433):
 * verifies the cached arrangement for the filtered right-side EDB
 * produces the same results as the ephemeral-hash-table path.
 */
static int
test_filt_arr_bench_w1(void)
{
    TEST("Filt-arr bench W=1: 10-node chain filtered TC, 45 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_filtered_tc_session(1, &plan, &prog);
    if (!sess) {
        FAIL("session create failed");
        return 1;
    }

    /* 10-node chain: tag=1 edges (1,2)..(9,10). */
    int64_t edges[] = {
        1, 2, 1,  2, 3, 1,  3, 4, 1,  4, 5, 1,
        5, 6, 1,  6, 7, 1,  7, 8, 1,  8, 9, 1,
        9, 10, 1,
        /* This edge must be rejected by the atom filter. */
        10, 11, 0
    };
    if (insert_filtered_edges(sess, edges, 10) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert_edges failed");
        return 1;
    }

    uint64_t t0 = now_ns();
    int rc = wl_session_step(&sess->base);
    uint64_t elapsed_ms = (now_ns() - t0) / 1000000ULL;

    uint32_t nrows = count_rows(sess, "tc");
    uint32_t filt_cache_count = sess->filt_cache_count;
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (nrows != 45) {
        FAIL("expected 45 tc tuples");
        return 1;
    }
    if (filt_cache_count == 0) {
        FAIL("expected filtered edge cache to be populated");
        return 1;
    }
    printf(" [%llu ms]", (unsigned long long)elapsed_ms);
    PASS();
    return 0;
}

/*
 * test_filt_arr_bench_w4:
 * W=4 filtered TC must match W=1 result (45 tuples).
 * Validates that filt_arr caching is safe under K-fusion worker isolation.
 */
static int
test_filt_arr_bench_w4(void)
{
    TEST("Filt-arr bench W=4: matches W=1 filtered TC (45 tuples)");

    wl_plan_t *p1 = NULL, *p4 = NULL;
    wirelog_program_t *pr1 = NULL, *pr4 = NULL;
    wl_col_session_t *s1 = make_filtered_tc_session(1, &p1, &pr1);
    wl_col_session_t *s4 = make_filtered_tc_session(4, &p4, &pr4);
    if (!s1 || !s4) {
        if (s1) cleanup_session(s1, p1, pr1);
        if (s4) cleanup_session(s4, p4, pr4);
        FAIL("session create failed");
        return 1;
    }

    int64_t edges[] = {
        1, 2, 1,  2, 3, 1,  3, 4, 1,  4, 5, 1,
        5, 6, 1,  6, 7, 1,  7, 8, 1,  8, 9, 1,
        9, 10, 1,
        10, 11, 0
    };
    if (insert_filtered_edges(s1, edges, 10) != 0
        || insert_filtered_edges(s4, edges, 10) != 0) {
        cleanup_session(s1, p1, pr1);
        cleanup_session(s4, p4, pr4);
        FAIL("insert_edges failed");
        return 1;
    }

    int rc1 = wl_session_step(&s1->base);
    int rc4 = wl_session_step(&s4->base);

    uint32_t cnt1 = count_rows(s1, "tc");
    uint32_t cnt4 = count_rows(s4, "tc");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s4, p4, pr4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != 45) {
        FAIL("W=1 expected 45 tc tuples");
        return 1;
    }
    if (cnt1 != cnt4) {
        FAIL("W=4 must match W=1");
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
    test_bdx_selfjoin_w2();
    test_bdx_selfjoin_w4();
    test_bdx_selfjoin_w4_shuffled_duplicates();
    test_bdx_seed_reader_denial_and_retry();
    test_bdx_seed_alias_denial_and_retry();
    test_bdx_seed_staging_failure_is_atomic();
    test_bdx_seed_second_worker_failure_is_atomic();
    test_bdx_seed_sort_failure_is_atomic();
    test_bdx_seed_allocation_failure_is_atomic();
    test_bdx_seed_success_preserves_timestamps();
#ifdef WL_TEST_TDD_MERGE
    test_tdd_merge_transactional_publication();
    test_tdd_merge_schema_mismatch_rollback();
#endif
    test_filt_arr_bench_w1();
    test_filt_arr_bench_w4();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
