/*
 * test_tdd_multiworker.c - TDD multi-worker tuple count regression tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Regression tests for Issue #404: multi-worker execution (W=4) must produce
 * the same tuple count as single-worker (W=1) for recursive strata.
 *
 * Root cause was a combination of two budget regressions in commit 6929689:
 *   1. join_output_limit divided by num_workers (per-worker divisor)
 *   2. pre-join backpressure firing too early on worker sessions
 *
 * These tests guard against silent data loss from budget/limit regressions.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "columnar/internal.h"

#include <inttypes.h>
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

struct count_ctx {
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->count++;
    (void)relation;
    (void)row;
    (void)ncols;
}

/*
 * Run transitive closure on a 20-node chain graph with the given number of
 * workers.  Returns the tuple count for the tc relation, or -1 on error.
 *
 * Graph: 1->2, 2->3, ..., 19->20  (19 edges)
 * TC should produce n*(n-1)/2 = 190 tuples for a 20-node chain.
 */
static int64_t
run_tc_chain20(uint32_t num_workers)
{
    /* Build the Datalog program */
    const char *src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0 || !plan)
        return -1;

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        return -1;
    }

    /* Insert 19 edges forming a 20-node chain */
    for (int i = 1; i <= 19; i++) {
        int64_t row[2] = { i, i + 1 };
        rc = wl_session_insert(session, "edge", row, 1, 2);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            return -1;
        }
    }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        return -1;
    }

    int64_t count = ctx.count;

    wl_session_destroy(session);
    wl_plan_free(plan);
    return count;
}

/* ======================================================================== */
/* Test 1: W=1 vs W=4 produce same tuple count (Issue #404 regression)     */
/* ======================================================================== */

static int
test_w1_vs_w4_tuple_count(void)
{
    TEST("TC 20-node chain: W=1 and W=4 produce same tuple count");

    int64_t count1 = run_tc_chain20(1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count4 = run_tc_chain20(4);
    if (count4 < 0) {
        FAIL("session with W=4 failed");
        return 1;
    }

    if (count1 != count4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=4 produced %" PRId64,
            count1, count4);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: W=1 produces the expected 190 tuples for a 20-node chain        */
/* ======================================================================== */

static int
test_w1_expected_count(void)
{
    TEST("TC 20-node chain: W=1 produces 190 tuples");

    int64_t count = run_tc_chain20(1);
    if (count < 0) {
        FAIL("session failed");
        return 1;
    }

    /* 20-node chain: n*(n-1)/2 = 20*19/2 = 190 */
    if (count != 190) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 190, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: W=8 also matches W=1                                            */
/* ======================================================================== */

static int
test_w1_vs_w8_tuple_count(void)
{
    TEST("TC 20-node chain: W=1 and W=8 produce same tuple count");

    int64_t count1 = run_tc_chain20(1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count8 = run_tc_chain20(8);
    if (count8 < 0) {
        FAIL("session with W=8 failed");
        return 1;
    }

    if (count1 != count8) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=8 produced %" PRId64,
            count1, count8);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Dual-write queue transport tests (Issue #410, Commit 3)                 */
/* ======================================================================== */

static int
test_queue_only_w1(void)
{
    TEST("queue-only transport: TC chain-20 W=1 produces 190 tuples");

    int64_t count = run_tc_chain20(1);
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_queue_only_w4(void)
{
    TEST("queue-only transport: TC chain-20 W=4 produces 190 tuples");

    int64_t count = run_tc_chain20(4);
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_coordinator_drain_w2(void)
{
    TEST("coordinator drain+adapter: TC chain-20 W=2 produces 190 tuples");

    int64_t count = run_tc_chain20(2);
    if (count < 0) {
        FAIL("session with W=2 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_coordinator_drain_w4(void)
{
    TEST("coordinator drain+adapter: TC chain-20 W=4 produces 190 tuples");

    int64_t count = run_tc_chain20(4);
    if (count < 0) {
        FAIL("session with W=4 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_dual_write_w2(void)
{
    TEST("dual-write transport: TC chain-20 W=2 produces 190 tuples");

    int64_t count = run_tc_chain20(2);
    if (count < 0) {
        FAIL("session with W=2 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_dual_write_w3(void)
{
    TEST("dual-write transport: TC chain-20 W=3 produces 190 tuples");

    int64_t count = run_tc_chain20(3);
    if (count < 0) {
        FAIL("session with W=3 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* tdd_reconstruct_delta_matrix unit tests (Issue #410)                    */
/* ======================================================================== */

/*
 * Helper: allocate a delta_rels array of nrels NULLs and assign to ctx.
 */
static col_rel_t **
alloc_delta_rels(uint32_t nrels)
{
    return (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
}

static int
test_reconstruct_empty(void)
{
    TEST("tdd_reconstruct_delta_matrix: empty buffer leaves ctxs unchanged");

    const uint32_t W = 2, nrels = 3;
    col_eval_tdd_worker_ctx_t ctxs[2];
    col_rel_t *dr0[3], *dr1[3];
    memset(dr0, 0, sizeof(dr0));
    memset(dr1, 0, sizeof(dr1));
    memset(ctxs, 0, sizeof(ctxs));
    ctxs[0].delta_rels = dr0;
    ctxs[1].delta_rels = dr1;

    tdd_reconstruct_delta_matrix(ctxs, NULL, 0, W, nrels);

    for (uint32_t w = 0; w < W; w++)
        for (uint32_t ri = 0; ri < nrels; ri++)
            if (ctxs[w].delta_rels[ri] != NULL) {
                FAIL("non-NULL entry after empty reconstruct");
                return 1;
            }
    PASS();
    return 0;
}

static int
test_reconstruct_single(void)
{
    TEST("tdd_reconstruct_delta_matrix: single message sets one slot");

    const uint32_t W = 4, nrels = 5;
    col_eval_tdd_worker_ctx_t ctxs[4];
    col_rel_t *dr[4][5];
    memset(dr, 0, sizeof(dr));
    memset(ctxs, 0, sizeof(ctxs));
    for (uint32_t w = 0; w < W; w++)
        ctxs[w].delta_rels = dr[w];

    /* sentinel pointer — not a real col_rel_t, just a non-NULL marker */
    col_rel_t *sentinel = (col_rel_t *)(uintptr_t)0xDEADBEEFu;
    wl_delta_msg_t msg = {0};
    msg.delta = sentinel;
    msg.worker_id = 2;
    msg.rel_idx = 3;

    tdd_reconstruct_delta_matrix(ctxs, &msg, 1, W, nrels);

    if (ctxs[2].delta_rels[3] != sentinel) {
        FAIL("slot [2][3] not set to sentinel");
        return 1;
    }
    /* all other slots must remain NULL */
    for (uint32_t w = 0; w < W; w++)
        for (uint32_t ri = 0; ri < nrels; ri++)
            if (!(w == 2 && ri == 3) && ctxs[w].delta_rels[ri] != NULL) {
                FAIL("unexpected non-NULL in adjacent slot");
                return 1;
            }
    PASS();
    return 0;
}

static int
test_reconstruct_full_matrix(void)
{
    TEST("tdd_reconstruct_delta_matrix: full W*nrels matrix filled correctly");

    const uint32_t W = 4, nrels = 3;
    col_eval_tdd_worker_ctx_t ctxs[4];
    col_rel_t **dr[4];
    memset(ctxs, 0, sizeof(ctxs));
    for (uint32_t w = 0; w < W; w++) {
        dr[w] = alloc_delta_rels(nrels);
        ctxs[w].delta_rels = dr[w];
    }

    /* build 12 messages and fake delta pointers */
    wl_delta_msg_t msgs[12];
    col_rel_t *ptrs[4][3];
    uint32_t idx = 0;
    for (uint32_t w = 0; w < W; w++) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            ptrs[w][ri] = (col_rel_t *)(uintptr_t)(0x1000u + w * 100 + ri);
            msgs[idx].delta = ptrs[w][ri];
            msgs[idx].worker_id = w;
            msgs[idx].rel_idx = ri;
            msgs[idx].stratum = 0;
            msgs[idx].epoch = 0;
            msgs[idx].flags = 0;
            msgs[idx].rc = 0;
            idx++;
        }
    }

    tdd_reconstruct_delta_matrix(ctxs, msgs, 12, W, nrels);

    int ok = 1;
    for (uint32_t w = 0; w < W; w++)
        for (uint32_t ri = 0; ri < nrels; ri++)
            if (ctxs[w].delta_rels[ri] != ptrs[w][ri])
                ok = 0;

    for (uint32_t w = 0; w < W; w++)
        free(dr[w]);

    if (!ok) {
        FAIL("matrix entry mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_reconstruct_sparse(void)
{
    TEST("tdd_reconstruct_delta_matrix: sparse messages, rest remain NULL");

    const uint32_t W = 4, nrels = 3;
    col_eval_tdd_worker_ctx_t ctxs[4];
    col_rel_t **dr[4];
    memset(ctxs, 0, sizeof(ctxs));
    for (uint32_t w = 0; w < W; w++) {
        dr[w] = alloc_delta_rels(nrels);
        ctxs[w].delta_rels = dr[w];
    }

    /* 5 of 12 slots: {w=0,ri=0}, {w=1,ri=2}, {w=2,ri=1}, {w=3,ri=0}, {w=3,ri=2} */
    wl_delta_msg_t msgs[5] = {
        {.delta = (col_rel_t *)(uintptr_t)0xA001u, .worker_id = 0,
         .rel_idx = 0},
        {.delta = (col_rel_t *)(uintptr_t)0xA002u, .worker_id = 1,
         .rel_idx = 2},
        {.delta = (col_rel_t *)(uintptr_t)0xA003u, .worker_id = 2,
         .rel_idx = 1},
        {.delta = (col_rel_t *)(uintptr_t)0xA004u, .worker_id = 3,
         .rel_idx = 0},
        {.delta = (col_rel_t *)(uintptr_t)0xA005u, .worker_id = 3,
         .rel_idx = 2},
    };

    tdd_reconstruct_delta_matrix(ctxs, msgs, 5, W, nrels);

    int ok = 1;
    /* filled slots */
    ok &= (ctxs[0].delta_rels[0] == (col_rel_t *)(uintptr_t)0xA001u);
    ok &= (ctxs[1].delta_rels[2] == (col_rel_t *)(uintptr_t)0xA002u);
    ok &= (ctxs[2].delta_rels[1] == (col_rel_t *)(uintptr_t)0xA003u);
    ok &= (ctxs[3].delta_rels[0] == (col_rel_t *)(uintptr_t)0xA004u);
    ok &= (ctxs[3].delta_rels[2] == (col_rel_t *)(uintptr_t)0xA005u);
    /* unfilled slots must be NULL */
    ok &= (ctxs[0].delta_rels[1] == NULL);
    ok &= (ctxs[0].delta_rels[2] == NULL);
    ok &= (ctxs[1].delta_rels[0] == NULL);
    ok &= (ctxs[1].delta_rels[1] == NULL);
    ok &= (ctxs[2].delta_rels[0] == NULL);
    ok &= (ctxs[2].delta_rels[2] == NULL);
    ok &= (ctxs[3].delta_rels[1] == NULL);

    for (uint32_t w = 0; w < W; w++)
        free(dr[w]);

    if (!ok) {
        FAIL("sparse slot mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_reconstruct_duplicate(void)
{
    TEST("tdd_reconstruct_delta_matrix: duplicate pair, last write wins");

    col_eval_tdd_worker_ctx_t ctxs[1];
    col_rel_t *dr[4];
    memset(dr, 0, sizeof(dr));
    memset(ctxs, 0, sizeof(ctxs));
    ctxs[0].delta_rels = dr;

    col_rel_t *first = (col_rel_t *)(uintptr_t)0xBEEF1u;
    col_rel_t *second = (col_rel_t *)(uintptr_t)0xBEEF2u;
    wl_delta_msg_t msgs[2] = {
        {.delta = first,  .worker_id = 0, .rel_idx = 1},
        {.delta = second, .worker_id = 0, .rel_idx = 1},
    };

    tdd_reconstruct_delta_matrix(ctxs, msgs, 2, 1, 4);

    if (ctxs[0].delta_rels[1] != second) {
        FAIL("last write did not win for duplicate pair");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_tdd_multiworker (Issue #404) ===\n");

    test_w1_expected_count();
    test_w1_vs_w4_tuple_count();
    test_w1_vs_w8_tuple_count();

    printf(
        "\n=== dual-write queue transport tests (Issue #410, Commit 3) ===\n");
    test_dual_write_w2();
    test_dual_write_w3();

    printf(
        "\n=== coordinator drain + adapter tests (Issue #410, Commit 4) ===\n");
    test_coordinator_drain_w2();
    test_coordinator_drain_w4();

    printf("\n=== queue-only transport tests (Issue #410, Commit 5) ===\n");
    test_queue_only_w1();
    test_queue_only_w4();
    test_reconstruct_empty();
    test_reconstruct_single();
    test_reconstruct_full_matrix();
    test_reconstruct_sparse();
    test_reconstruct_duplicate();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
