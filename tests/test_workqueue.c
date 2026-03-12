/*
 * test_workqueue.c - Unit tests for wirelog Work Queue (Phase B-lite)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests:
 *   1. Single-threaded drain mode (wl_workqueue_drain)
 *   2. Multi-threaded wait_all mode (wl_workqueue_wait_all)
 *   3. Multiple batch cycles (submit/wait/submit/wait)
 *   4. Parallel columnar session: same results with 1 and 2 workers
 */

#include "../wirelog/workqueue.h"
#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#ifndef _MSC_VER
#include <stdatomic.h>
#else
#include <windows.h>
/* MSVC fallback: use volatile int with interlocked operations instead of C11 atomics */
typedef volatile int atomic_int;
#define atomic_store(ptr, val) InterlockedExchange((LONG *)(ptr), (LONG)(val))
#define atomic_load(ptr) (*(ptr))
#define atomic_fetch_add(ptr, val) InterlockedAdd((LONG *)(ptr), (LONG)(val))
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                    \
    do {                                              \
        test_count++;                                 \
        printf("TEST %d: %s ... ", test_count, name); \
    } while (0)

#define PASS()            \
    do {                  \
        pass_count++;     \
        printf("PASS\n"); \
    } while (0)

#define FAIL(msg)                  \
    do {                           \
        fail_count++;              \
        printf("FAIL: %s\n", msg); \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond)) {    \
            FAIL(msg);    \
            return;       \
        }                 \
    } while (0)

/* ----------------------------------------------------------------
 * Test: workqueue create/destroy
 * ---------------------------------------------------------------- */

static void
test_create_destroy(void)
{
    TEST("workqueue create/destroy");

    wl_work_queue_t *wq = wl_workqueue_create(2);
    ASSERT(wq != NULL, "wl_workqueue_create(2) returned NULL");

    wl_workqueue_destroy(wq);

    /* NULL-safe destroy */
    wl_workqueue_destroy(NULL);

    /* Invalid: 0 workers */
    wq = wl_workqueue_create(0);
    ASSERT(wq == NULL, "wl_workqueue_create(0) should return NULL");

    PASS();
}

/* ----------------------------------------------------------------
 * Test: single-threaded drain
 * ---------------------------------------------------------------- */

typedef struct {
    int *counter;
    int increment;
} add_ctx_t;

static void
add_fn(void *arg)
{
    add_ctx_t *ctx = (add_ctx_t *)arg;
    *ctx->counter += ctx->increment;
}

static void
test_drain(void)
{
    TEST("workqueue drain (single-threaded)");

    wl_work_queue_t *wq = wl_workqueue_create(2);
    ASSERT(wq != NULL, "create failed");

    int counter = 0;
    add_ctx_t ctxs[4];
    for (int i = 0; i < 4; i++) {
        ctxs[i].counter = &counter;
        ctxs[i].increment = (i + 1) * 10;
        int rc = wl_workqueue_submit(wq, add_fn, &ctxs[i]);
        ASSERT(rc == 0, "submit failed");
    }

    /* Drain executes on calling thread, not workers */
    int rc = wl_workqueue_drain(wq);
    ASSERT(rc == 0, "drain failed");
    ASSERT(counter == 100, "expected 10+20+30+40=100");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: multi-threaded wait_all
 * ---------------------------------------------------------------- */

static atomic_int atomic_counter;

static void
atomic_add_fn(void *arg)
{
    int *val = (int *)arg;
    atomic_fetch_add(&atomic_counter, *val);
}

static void
test_wait_all(void)
{
    TEST("workqueue wait_all (multi-threaded)");

    wl_work_queue_t *wq = wl_workqueue_create(4);
    ASSERT(wq != NULL, "create failed");

    atomic_store(&atomic_counter, 0);

    int values[8];
    for (int i = 0; i < 8; i++) {
        values[i] = i + 1;
        int rc = wl_workqueue_submit(wq, atomic_add_fn, &values[i]);
        ASSERT(rc == 0, "submit failed");
    }

    int rc = wl_workqueue_wait_all(wq);
    ASSERT(rc == 0, "wait_all failed");
    ASSERT(atomic_load(&atomic_counter) == 36, "expected 1+2+3+4+5+6+7+8=36");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: multiple batch cycles
 * ---------------------------------------------------------------- */

static void
test_batch_cycles(void)
{
    TEST("workqueue multiple batch cycles");

    wl_work_queue_t *wq = wl_workqueue_create(2);
    ASSERT(wq != NULL, "create failed");

    for (int batch = 0; batch < 3; batch++) {
        atomic_store(&atomic_counter, 0);

        int values[4];
        for (int i = 0; i < 4; i++) {
            values[i] = 1;
            int rc = wl_workqueue_submit(wq, atomic_add_fn, &values[i]);
            ASSERT(rc == 0, "submit failed");
        }

        int rc = wl_workqueue_wait_all(wq);
        ASSERT(rc == 0, "wait_all failed");
        ASSERT(atomic_load(&atomic_counter) == 4, "expected 4 per batch");
    }

    wl_workqueue_destroy(wq);
    PASS();
}

/* ----------------------------------------------------------------
 * Snapshot counting callback
 * ---------------------------------------------------------------- */

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

/* ----------------------------------------------------------------
 * Test: parallel session produces same results as sequential
 * ---------------------------------------------------------------- */

static int64_t
run_session_with_workers(uint32_t num_workers)
{
    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).\n"
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
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* Load inline facts */
    rc = wl_session_load_facts(session, prog);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* Snapshot with evaluation */
    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    int64_t count = ctx.count;

    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    return count;
}

static void
test_parallel_session(void)
{
    TEST("parallel session: 1 worker vs 2 workers produce same results");

    int64_t count_1 = run_session_with_workers(1);
    ASSERT(count_1 >= 0, "session with 1 worker failed");

    int64_t count_2 = run_session_with_workers(2);
    ASSERT(count_2 >= 0, "session with 2 workers failed");

    char msg[128];
    snprintf(msg, sizeof(msg),
             "result mismatch: 1-worker=%" PRId64 " vs 2-worker=%" PRId64,
             count_1, count_2);
    ASSERT(count_1 == count_2, msg);

    /* Also test with 4 workers */
    int64_t count_4 = run_session_with_workers(4);
    ASSERT(count_4 >= 0, "session with 4 workers failed");
    ASSERT(count_1 == count_4, "1-worker vs 4-worker mismatch");

    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== workqueue tests ===\n\n");

    test_create_destroy();
    test_drain();
    test_wait_all();
    test_batch_cycles();
    test_parallel_session();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
           fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
