/*
 * test_lockfree_queue.c - Unit tests for lock-free MPSC delta queue
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests:
 *   1. create / destroy (basic lifecycle + invalid args)
 *   2. single producer enqueue/dequeue correctness
 *   3. multiple producers, coordinator dequeues all
 *   4. saturation: fill ring, verify enqueue fails, drain, re-enqueue
 *   5. concurrent producers + coordinator (thread safety under TSan)
 */

#include "util/lockfree_queue.h"
#include "../wirelog/thread.h"

#ifndef _MSC_VER
#include <stdatomic.h>
#else
#include <windows.h>
typedef volatile int atomic_int;
#define atomic_store(ptr, val) InterlockedExchange((LONG *)(ptr), (LONG)(val))
#define atomic_load(ptr)       (*(ptr))
#define atomic_fetch_add(ptr, val) InterlockedAdd((LONG *)(ptr), (LONG)(val))
#endif

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                         \
        do {                                                   \
            test_count++;                                      \
            printf("TEST %d: %s ... ", test_count, name);     \
        } while (0)

#define PASS()              \
        do {                    \
            pass_count++;       \
            printf("PASS\n");   \
        } while (0)

#define FAIL(msg)                   \
        do {                            \
            fail_count++;               \
            printf("FAIL: %s\n", msg);  \
        } while (0)

#define ASSERT(cond, msg)   \
        do {                    \
            if (!(cond)) {      \
                FAIL(msg);      \
                return;         \
            }                   \
        } while (0)

/* ----------------------------------------------------------------
 * Test 1: create / destroy lifecycle
 * ---------------------------------------------------------------- */

static void
test_create_destroy(void)
{
    TEST("create/destroy lifecycle");

    /* Normal creation */
    wl_mpsc_queue_t *q = wl_mpsc_queue_create(4, 16);
    ASSERT(q != NULL, "create(4, 16) returned NULL");
    wl_mpsc_queue_destroy(q);

    /* NULL-safe destroy */
    wl_mpsc_queue_destroy(NULL);

    /* Invalid: 0 workers */
    q = wl_mpsc_queue_create(0, 16);
    ASSERT(q == NULL, "create(0, 16) should return NULL");

    /* Invalid: capacity < 2 */
    q = wl_mpsc_queue_create(2, 1);
    ASSERT(q == NULL, "create(2, 1) should return NULL");

    /* Single worker */
    q = wl_mpsc_queue_create(1, 8);
    ASSERT(q != NULL, "create(1, 8) returned NULL");
    wl_mpsc_queue_destroy(q);

    PASS();
}

/* ----------------------------------------------------------------
 * Test 2: single producer enqueue/dequeue correctness
 * ---------------------------------------------------------------- */

static void
test_single_producer(void)
{
    TEST("single producer enqueue/dequeue");

    wl_mpsc_queue_t *q = wl_mpsc_queue_create(1, 8);
    ASSERT(q != NULL, "create failed");

    /* Empty queue dequeue returns 0 */
    wl_delta_msg_t out;
    int rc = wl_mpsc_dequeue(q, &out);
    ASSERT(rc == 0, "dequeue from empty should return 0");

    /* Enqueue 3 items */
    int a = 10, b = 20, c = 30;
    ASSERT(wl_mpsc_enqueue(q, 0, &a, 1, 0) == 0, "enqueue a failed");
    ASSERT(wl_mpsc_enqueue(q, 0, &b, 2, 1) == 0, "enqueue b failed");
    ASSERT(wl_mpsc_enqueue(q, 0, &c, 3, 2) == 0, "enqueue c failed");

    ASSERT(wl_mpsc_size(q) == 3, "size should be 3");

    /* Dequeue in FIFO order */
    rc = wl_mpsc_dequeue(q, &out);
    ASSERT(rc == 1, "dequeue 1 failed");
    ASSERT(out.delta == &a, "wrong delta ptr for item 1");
    ASSERT(out.stratum == 1, "wrong stratum for item 1");
    ASSERT(out.worker_id == 0, "wrong worker_id for item 1");
    ASSERT(out.rel_idx == 0, "wrong rel_idx for item 1");

    rc = wl_mpsc_dequeue(q, &out);
    ASSERT(rc == 1, "dequeue 2 failed");
    ASSERT(out.delta == &b, "wrong delta ptr for item 2");
    ASSERT(out.stratum == 2, "wrong stratum for item 2");
    ASSERT(out.rel_idx == 1, "wrong rel_idx for item 2");

    rc = wl_mpsc_dequeue(q, &out);
    ASSERT(rc == 1, "dequeue 3 failed");
    ASSERT(out.delta == &c, "wrong delta ptr for item 3");
    ASSERT(out.stratum == 3, "wrong stratum for item 3");
    ASSERT(out.rel_idx == 2, "wrong rel_idx for item 3");

    /* Now empty */
    rc = wl_mpsc_dequeue(q, &out);
    ASSERT(rc == 0, "should be empty after 3 dequeues");

    wl_mpsc_queue_destroy(q);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 2b: rel_idx field round-trip
 * ---------------------------------------------------------------- */

static void
test_rel_idx_field(void)
{
    TEST("rel_idx field round-trip");

    wl_mpsc_queue_t *q = wl_mpsc_queue_create(2, 16);
    ASSERT(q != NULL, "create failed");

    /* Worker 0 enqueues rel_idx 0 and 5; worker 1 enqueues rel_idx 99 */
    int d0 = 100, d1 = 200, d2 = 300;
    ASSERT(wl_mpsc_enqueue(q, 0, &d0, 0, 0) == 0, "enqueue rel 0 failed");
    ASSERT(wl_mpsc_enqueue(q, 0, &d1, 0, 5) == 0, "enqueue rel 5 failed");
    ASSERT(wl_mpsc_enqueue(q, 1, &d2, 1, 99) == 0, "enqueue rel 99 failed");

    /* Drain all 3 items (round-robin ordering not guaranteed across workers) */
    wl_delta_msg_t buf[4];
    uint32_t n = wl_mpsc_dequeue_all(q, buf, 4);
    ASSERT(n == 3, "should dequeue 3 items");

    /* Verify rel_idx values as a set: {0, 5, 99} */
    int seen_0 = 0, seen_5 = 0, seen_99 = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (buf[i].rel_idx == 0)  seen_0 = 1;
        if (buf[i].rel_idx == 5)  seen_5 = 1;
        if (buf[i].rel_idx == 99) seen_99 = 1;
    }
    ASSERT(seen_0,  "rel_idx 0 not found");
    ASSERT(seen_5,  "rel_idx 5 not found");
    ASSERT(seen_99, "rel_idx 99 not found");

    /* Verify per-worker FIFO: worker 0's items arrive in enqueue order */
    uint32_t w0_order = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (buf[i].worker_id == 0) {
            if (w0_order == 0) {
                ASSERT(buf[i].rel_idx == 0,
                    "worker 0 first item should be rel_idx 0");
                w0_order = 1;
            } else {
                ASSERT(buf[i].rel_idx == 5,
                    "worker 0 second item should be rel_idx 5");
            }
        }
    }

    wl_mpsc_queue_destroy(q);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 2c: epoch/flags/rc fields zero-initialized
 * ---------------------------------------------------------------- */

static void
test_new_fields(void)
{
    TEST("epoch/flags/rc fields zero-initialized on enqueue");

    wl_mpsc_queue_t *q = wl_mpsc_queue_create(1, 8);
    ASSERT(q != NULL, "create failed");

    int val = 42;
    ASSERT(wl_mpsc_enqueue(q, 0, &val, 1, 7) == 0, "enqueue failed");

    wl_delta_msg_t out;
    int rc = wl_mpsc_dequeue(q, &out);
    ASSERT(rc == 1, "dequeue failed");
    ASSERT(out.epoch == 0,  "epoch should be zero-initialized");
    ASSERT(out.flags == 0,  "flags should be zero-initialized");
    ASSERT(out.rc == 0,  "rc should be zero-initialized");

    wl_mpsc_queue_destroy(q);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 3: multiple producers, dequeue_all
 * ---------------------------------------------------------------- */

static void
test_multiple_producers(void)
{
    TEST("multiple producers, dequeue_all");

    /* 4 workers, 8 slots each; enqueue 4 items per worker = 16 total */
    const uint32_t nworkers = 4;
    const uint32_t per_worker = 4;

    wl_mpsc_queue_t *q = wl_mpsc_queue_create(nworkers, 8);
    ASSERT(q != NULL, "create failed");

    /* Each worker enqueues per_worker items */
    int values[4 * 4];
    for (uint32_t w = 0; w < nworkers; w++) {
        for (uint32_t i = 0; i < per_worker; i++) {
            int idx = (int)(w * per_worker + i);
            values[idx] = idx;
            int rc = wl_mpsc_enqueue(q, w, &values[idx], w * 10 + i, i);
            char msg[64];
            snprintf(msg, sizeof(msg), "enqueue w=%u i=%u failed", w, i);
            ASSERT(rc == 0, msg);
        }
    }

    /* dequeue_all should collect all 16 items */
    wl_delta_msg_t buf[32];
    uint32_t n = wl_mpsc_dequeue_all(q, buf, 32);
    ASSERT(n == nworkers * per_worker, "dequeue_all count mismatch");

    /* Verify no item is duplicated (check all values present) */
    int seen[16];
    memset(seen, 0, sizeof(seen));
    for (uint32_t i = 0; i < n; i++) {
        int *vp = (int *)buf[i].delta;
        ASSERT(vp != NULL, "NULL delta pointer in dequeued item");
        int v = *vp;
        ASSERT(v >= 0 && v < 16, "delta value out of range");
        ASSERT(seen[v] == 0, "duplicate delta value dequeued");
        seen[v] = 1;
    }
    for (int i = 0; i < 16; i++) {
        ASSERT(seen[i] == 1, "missing value in dequeue_all output");
    }

    ASSERT(wl_mpsc_size(q) == 0, "queue should be empty after dequeue_all");

    wl_mpsc_queue_destroy(q);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 4: saturation — fill, overflow, drain, re-fill
 * ---------------------------------------------------------------- */

static void
test_saturation(void)
{
    TEST("saturation: fill/overflow/drain/refill");

    /* capacity=4 (next_pow2 rounds 4 up to 4) */
    wl_mpsc_queue_t *q = wl_mpsc_queue_create(1, 4);
    ASSERT(q != NULL, "create failed");

    int vals[8];
    for (int i = 0; i < 8; i++)
        vals[i] = i;

    /* Fill to capacity */
    for (int i = 0; i < 4; i++) {
        int rc = wl_mpsc_enqueue(q, 0, &vals[i], (uint32_t)i, (uint32_t)i);
        char msg[64];
        snprintf(msg, sizeof(msg), "enqueue %d should succeed", i);
        ASSERT(rc == 0, msg);
    }
    ASSERT(wl_mpsc_size(q) == 4, "size should be 4 when full");

    /* 5th enqueue must fail */
    int rc = wl_mpsc_enqueue(q, 0, &vals[4], 4, 4);
    ASSERT(rc == -1, "enqueue into full ring should return -1");

    /* Drain via dequeue_all */
    wl_delta_msg_t buf[8];
    uint32_t n = wl_mpsc_dequeue_all(q, buf, 8);
    ASSERT(n == 4, "dequeue_all should return 4 items");
    ASSERT(wl_mpsc_size(q) == 0, "size should be 0 after drain");

    /* Can enqueue again after drain */
    for (int i = 4; i < 8; i++) {
        rc = wl_mpsc_enqueue(q, 0, &vals[i], (uint32_t)i, (uint32_t)i);
        char msg[64];
        snprintf(msg, sizeof(msg), "re-enqueue %d should succeed", i);
        ASSERT(rc == 0, msg);
    }
    n = wl_mpsc_dequeue_all(q, buf, 8);
    ASSERT(n == 4, "second dequeue_all should return 4 items");

    wl_mpsc_queue_destroy(q);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 5: concurrent producers + coordinator
 * ---------------------------------------------------------------- */

#define CONCURRENT_WORKERS  4
#define ITEMS_PER_WORKER    64
#define QUEUE_CAPACITY      128

struct producer_ctx {
    wl_mpsc_queue_t *q;
    uint32_t worker_id;
    int values[ITEMS_PER_WORKER];
    int enqueued;              /* items successfully enqueued */
};

static void *
producer_fn(void *arg)
{
    struct producer_ctx *ctx = (struct producer_ctx *)arg;
    for (int i = 0; i < ITEMS_PER_WORKER; i++) {
        ctx->values[i] = (int)(ctx->worker_id * 1000 + (uint32_t)i);
        /* Spin until enqueue succeeds (queue may be temporarily full). */
        while (wl_mpsc_enqueue(ctx->q, ctx->worker_id,
            &ctx->values[i],
            ctx->worker_id, (uint32_t)i) != 0)
            ;
        ctx->enqueued++;
    }
    return NULL;
}

static void
test_concurrent(void)
{
    TEST("concurrent producers + coordinator (thread safety)");

    wl_mpsc_queue_t *q = wl_mpsc_queue_create(CONCURRENT_WORKERS,
            QUEUE_CAPACITY);
    ASSERT(q != NULL, "create failed");

    struct producer_ctx ctxs[CONCURRENT_WORKERS];
    thread_t threads[CONCURRENT_WORKERS];

    for (int w = 0; w < CONCURRENT_WORKERS; w++) {
        ctxs[w].q = q;
        ctxs[w].worker_id = (uint32_t)w;
        ctxs[w].enqueued = 0;
        int rc = thread_create(&threads[w], producer_fn, &ctxs[w]);
        char msg[64];
        snprintf(msg, sizeof(msg), "thread_create failed for worker %d", w);
        ASSERT(rc == 0, msg);
    }

    /* Coordinator: keep dequeuing until all workers are done and queue empty. */
    const int total_expected = CONCURRENT_WORKERS * ITEMS_PER_WORKER;
    int total_received = 0;
    wl_delta_msg_t buf[QUEUE_CAPACITY];

    /* Join workers first so we know all items have been enqueued. */
    for (int w = 0; w < CONCURRENT_WORKERS; w++)
        thread_join(&threads[w]);

    /* Drain what's left after all producers finished. */
    uint32_t n = wl_mpsc_dequeue_all(q, buf, QUEUE_CAPACITY);
    total_received += (int)n;

    /* There may be more items than QUEUE_CAPACITY in the rings total.
     * Keep draining until empty. */
    while (1) {
        n = wl_mpsc_dequeue_all(q, buf, QUEUE_CAPACITY);
        if (n == 0)
            break;
        total_received += (int)n;
    }

    char msg[128];
    snprintf(msg, sizeof(msg),
        "expected %d items, got %d", total_expected, total_received);
    ASSERT(total_received == total_expected, msg);

    /* Verify each worker's enqueue count */
    for (int w = 0; w < CONCURRENT_WORKERS; w++) {
        snprintf(msg, sizeof(msg),
            "worker %d enqueued %d (expected %d)",
            w, ctxs[w].enqueued, ITEMS_PER_WORKER);
        ASSERT(ctxs[w].enqueued == ITEMS_PER_WORKER, msg);
    }

    wl_mpsc_queue_destroy(q);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== lockfree_queue tests ===\n\n");

    test_create_destroy();
    test_single_producer();
    test_rel_idx_field();
    test_new_fields();
    test_multiple_producers();
    test_saturation();
    test_concurrent();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n",
        pass_count, fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
