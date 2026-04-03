/*
 * test_queue_transport.c - Queue transport equivalence and stress tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Comprehensive validation of the MPSC queue delta transport introduced in
 * Issue #410.  Tests cover correctness across W values, sparse producer
 * scenarios, graceful overflow behaviour, empty-subpass convergence,
 * error propagation, and repeated-session memory stability.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/util/lockfree_queue.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                              */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                             \
        do {                                       \
            tests_run++;                           \
            printf("  [%d] %s", tests_run, name);  \
        } while (0)
#define PASS()                  \
        do {                        \
            tests_passed++;         \
            printf(" ... PASS\n");  \
        } while (0)
#define FAIL(msg)                          \
        do {                                   \
            tests_failed++;                    \
            printf(" ... FAIL: %s\n", (msg));  \
        } while (0)

/* ======================================================================== */
/* Shared Helpers                                                            */
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
 * run_tc_chain20:
 * Run transitive closure on a 20-node chain (19 edges) with the given number
 * of workers.  Expected tuple count: 20*19/2 = 190.
 * Returns tuple count, or -1 on failure.
 */
static int64_t
run_tc_chain20(uint32_t num_workers)
{
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

    for (int i = 1; i <= 19; i++) {
        int64_t row[2] = { i, i + 1 };
        rc = wl_session_insert(session, "edge", row, 1, 2);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            return -1;
        }
    }

    struct count_ctx cctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &cctx);
    int64_t count = (rc == 0) ? cctx.count : -1;

    wl_session_destroy(session);
    wl_plan_free(plan);
    return count;
}

/*
 * run_tc_single_edge:
 * Run TC on a single edge (1->2) with the given number of workers.
 * Expected tuple count: 1.
 * Returns tuple count, or -1 on failure.
 */
static int64_t
run_tc_single_edge(uint32_t num_workers)
{
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

    int64_t row[2] = { 1, 2 };
    rc = wl_session_insert(session, "edge", row, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        return -1;
    }

    struct count_ctx cctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &cctx);
    int64_t count = (rc == 0) ? cctx.count : -1;

    wl_session_destroy(session);
    wl_plan_free(plan);
    return count;
}

/*
 * run_tc_no_facts:
 * Run TC with no EDB inserted, using the given number of workers.
 * Expected tuple count: 0 (empty result, clean convergence).
 * Returns tuple count, or -1 on failure.
 */
static int64_t
run_tc_no_facts(uint32_t num_workers)
{
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

    /* No facts inserted — snapshot must succeed with 0 tuples */
    struct count_ctx cctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &cctx);
    int64_t count = (rc == 0) ? cctx.count : -1;

    wl_session_destroy(session);
    wl_plan_free(plan);
    return count;
}

/* ======================================================================== */
/* Test 1: W scaling — W=1,2,4,8 produce identical IDB                     */
/* ======================================================================== */

static int
test_w_scaling(void)
{
    TEST("W scaling: W=1,2,4,8 produce identical 190-tuple TC result");

    int64_t c1 = run_tc_chain20(1);
    int64_t c2 = run_tc_chain20(2);
    int64_t c4 = run_tc_chain20(4);
    int64_t c8 = run_tc_chain20(8);

    if (c1 < 0 || c2 < 0 || c4 < 0 || c8 < 0) {
        FAIL("one or more sessions failed");
        return 1;
    }
    if (c1 != 190 || c2 != 190 || c4 != 190 || c8 != 190) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1:%" PRId64 " W=2:%" PRId64 " W=4:%" PRId64 " W=8:%" PRId64
            " (expected 190 each)",
            c1, c2, c4, c8);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: Sparse deltas — only 1 edge, W=4 same result as W=1             */
/* ======================================================================== */

static int
test_sparse_deltas(void)
{
    TEST("sparse deltas: single-edge TC, W=4 matches W=1 (1 tuple)");

    int64_t c1 = run_tc_single_edge(1);
    int64_t c4 = run_tc_single_edge(4);

    if (c1 < 0) {
        FAIL("W=1 session failed");
        return 1;
    }
    if (c4 < 0) {
        FAIL("W=4 session failed");
        return 1;
    }
    if (c1 != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "W=1 expected 1 tuple, got %" PRId64, c1);
        FAIL(msg);
        return 1;
    }
    if (c1 != c4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=4 produced %" PRId64,
            c1, c4);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: Queue overflow — enqueue returns -1 when ring is full, no hang   */
/* ======================================================================== */

static int
test_queue_overflow_graceful(void)
{
    TEST("queue overflow: enqueue returns -1 when full, does not hang");

    /*
     * Create a minimal 1-worker queue with capacity 2 (smallest allowed).
     * Fill both slots; the third enqueue must return -1 immediately.
     */
    wl_mpsc_queue_t *q = wl_mpsc_queue_create(1, 2);
    if (!q) {
        FAIL("wl_mpsc_queue_create returned NULL");
        return 1;
    }

    int r0 = wl_mpsc_enqueue(q, 0, (void *)0x1, 0, 0);
    int r1 = wl_mpsc_enqueue(q, 0, (void *)0x2, 0, 1);
    int r2 = wl_mpsc_enqueue(q, 0, (void *)0x3, 0, 2); /* must fail */

    wl_mpsc_queue_destroy(q);

    if (r0 != 0) {
        FAIL("first enqueue unexpectedly failed");
        return 1;
    }
    if (r1 != 0) {
        FAIL("second enqueue unexpectedly failed");
        return 1;
    }
    if (r2 != -1) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "third enqueue returned %d, expected -1 (full)", r2);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 4: Empty sub-pass convergence — no facts → 0 tuples, clean exit    */
/* ======================================================================== */

static int
test_empty_subpass_convergence(void)
{
    TEST("empty sub-pass: no EDB facts, W=4 converges with 0 tuples");

    int64_t count = run_tc_no_facts(4);
    if (count < 0) {
        FAIL("session with no facts failed");
        return 1;
    }
    if (count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "expected 0 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 5: Error propagation — queue-full enqueue returns error code        */
/* ======================================================================== */

static int
test_error_propagation(void)
{
    TEST("error propagation: full queue enqueue returns -1, dequeue_all "
        "drains remaining");

    /*
     * Verify the full error path at the queue API level:
     *   1. Worker fills its ring to capacity.
     *   2. Next enqueue returns -1 (ENOMEM signal).
     *   3. Coordinator can still drain the successfully queued messages.
     *
     * In the production path (eval.c), worker sets ctx->rc = ENOMEM on -1
     * return; coordinator detects rc != 0 after barrier and exits.
     */
    wl_mpsc_queue_t *q = wl_mpsc_queue_create(2, 4);
    if (!q) {
        FAIL("wl_mpsc_queue_create returned NULL");
        return 1;
    }

    /* Worker 0: fill 4 slots */
    int all_ok = 1;
    for (uint32_t i = 0; i < 4; i++)
        if (wl_mpsc_enqueue(q, 0, (void *)(uintptr_t)(i + 1), 0, i) != 0)
            all_ok = 0;

    /* Worker 0: 5th enqueue must fail */
    int overflow = wl_mpsc_enqueue(q, 0, (void *)0xFF, 0, 4);

    /* Coordinator drains: must get the 4 successful messages */
    wl_delta_msg_t buf[8];
    uint32_t drained = wl_mpsc_dequeue_all(q, buf, 8);

    wl_mpsc_queue_destroy(q);

    if (!all_ok) {
        FAIL("one of the first 4 enqueues unexpectedly failed");
        return 1;
    }
    if (overflow != -1) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "5th enqueue returned %d, expected -1", overflow);
        FAIL(msg);
        return 1;
    }
    if (drained != 4) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "dequeue_all returned %u messages, expected 4", drained);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 6: Memory stress — repeated sessions, ASAN coverage                 */
/* ======================================================================== */

static int
test_memory_stress(void)
{
    TEST("memory stress: 10 repeated TC sessions all produce 190 tuples");

    int failures = 0;
    for (int i = 0; i < 10; i++) {
        int64_t count = run_tc_chain20(4);
        if (count != 190)
            failures++;
    }

    if (failures > 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "%d of 10 sessions produced wrong tuple count", failures);
        FAIL(msg);
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
    printf("=== test_queue_transport (Issue #410) ===\n");

    test_w_scaling();
    test_sparse_deltas();
    test_queue_overflow_graceful();
    test_empty_subpass_convergence();
    test_error_propagation();
    test_memory_stress();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
