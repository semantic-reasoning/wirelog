/*
 * test_workqueue_capacity.c - Unit tests for dynamic workqueue ring capacity
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Issue #414: Test rewrite for dynamic WL_WQ_RING_CAP formula after issue #407.
 *
 * The workqueue now dynamically allocates ring capacity based on num_workers:
 *   capacity = max(256, next_pow2(num_workers * 2))
 *
 * This test validates:
 *   1. Capacity formula correctness across various worker counts (white-box)
 *   2. Minimum capacity enforcement (256 items minimum)
 *   3. Power-of-2 rounding for efficient modulo operations
 *   4. Submit W work items without overflow (black-box)
 *   5. Overflow protection for W > 2^31/2
 *   6. Boundary value testing: W ∈ {1, 127, 128, 129, 256, 257, 512, 1024}
 *   7. Interaction between dynamic capacity and work submission
 */

#include "workqueue.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

/* ================================================================
 * Test Framework
 * ================================================================ */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                    \
        do {                                              \
            test_count++;                                 \
            printf("TEST %d: %s ... ", test_count, name); \
            fflush(stdout);                               \
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

/* ================================================================
 * Helper: Calculate expected capacity for num_workers
 * ================================================================ */

static uint32_t
expected_capacity(uint32_t num_workers)
{
    /* Mirrors workqueue.c:164-171 */
    uint32_t min_cap = num_workers * 2u;
    if (min_cap < 256u)
        min_cap = 256u;

    uint32_t cap = 1u;
    while (cap < min_cap)
        cap <<= 1u;

    return cap;
}

/* ================================================================
 * Helper: Dummy work function for testing
 * ================================================================ */

static void
dummy_work(void *ctx)
{
    (void)ctx; /* unused */
}

/* ================================================================
 * Test 1: create/destroy lifecycle
 * ================================================================ */

static void
test_create_destroy(void)
{
    TEST("create/destroy lifecycle");

    /* Normal creation */
    wl_work_queue_t *wq = wl_workqueue_create(4);
    ASSERT(wq != NULL, "create(4) returned NULL");
    wl_workqueue_destroy(wq);

    /* NULL-safe destroy */
    wl_workqueue_destroy(NULL);

    /* Invalid: 0 workers */
    wq = wl_workqueue_create(0);
    ASSERT(wq == NULL, "create(0) should return NULL");

    /* Single worker */
    wq = wl_workqueue_create(1);
    ASSERT(wq != NULL, "create(1) returned NULL");
    wl_workqueue_destroy(wq);

    PASS();
}

/* ================================================================
 * Test 2: White-box capacity — W=1 (minimum floor)
 * ================================================================ */

static void
test_capacity_white_box_w1(void)
{
    TEST("white-box capacity W=1 (floor)");

    wl_work_queue_t *wq = wl_workqueue_create(1);
    ASSERT(wq != NULL, "create(1) failed");

    uint32_t expected = expected_capacity(1);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=1");
    ASSERT(actual == 256, "capacity for W=1 should be 256");
    ASSERT((actual & (actual - 1)) == 0, "capacity must be power of 2");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 3: White-box capacity — W=127 (just under floor)
 * ================================================================ */

static void
test_capacity_white_box_w127(void)
{
    TEST("white-box capacity W=127 (under floor)");

    wl_work_queue_t *wq = wl_workqueue_create(127);
    ASSERT(wq != NULL, "create(127) failed");

    uint32_t expected = expected_capacity(127);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=127");
    ASSERT(actual == 256, "capacity for W=127 should be 256 (floor)");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 4: White-box capacity — W=128 (exact floor boundary)
 * ================================================================ */

static void
test_capacity_white_box_w128(void)
{
    TEST("white-box capacity W=128 (exact floor)");

    wl_work_queue_t *wq = wl_workqueue_create(128);
    ASSERT(wq != NULL, "create(128) failed");

    uint32_t expected = expected_capacity(128);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=128");
    ASSERT(actual == 256, "capacity for W=128 should be 256");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 5: White-box capacity — W=129 (first value above floor)
 * ================================================================ */

static void
test_capacity_white_box_w129(void)
{
    TEST("white-box capacity W=129 (pow2 jump)");

    wl_work_queue_t *wq = wl_workqueue_create(129);
    ASSERT(wq != NULL, "create(129) failed");

    uint32_t expected = expected_capacity(129);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=129");
    ASSERT(actual == 512, "capacity for W=129 should be 512 (next pow2)");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 6: White-box capacity — W=256 (power-of-2 boundary)
 * ================================================================ */

static void
test_capacity_white_box_w256(void)
{
    TEST("white-box capacity W=256 (pow2)");

    wl_work_queue_t *wq = wl_workqueue_create(256);
    ASSERT(wq != NULL, "create(256) failed");

    uint32_t expected = expected_capacity(256);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=256");
    ASSERT(actual == 512, "capacity for W=256 should be 512");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 7: White-box capacity — W=257 (requires rounding up)
 * ================================================================ */

static void
test_capacity_white_box_w257(void)
{
    TEST("white-box capacity W=257 (rounding)");

    wl_work_queue_t *wq = wl_workqueue_create(257);
    ASSERT(wq != NULL, "create(257) failed");

    uint32_t expected = expected_capacity(257);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=257");
    ASSERT(actual == 1024, "capacity for W=257 should be 1024");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 8: White-box capacity — W=512 (issue #401 target)
 * ================================================================ */

static void
test_capacity_white_box_w512(void)
{
    TEST("white-box capacity W=512 (#401 target)");

    wl_work_queue_t *wq = wl_workqueue_create(512);
    ASSERT(wq != NULL, "create(512) failed");

    uint32_t expected = expected_capacity(512);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=512");
    ASSERT(actual == 1024, "capacity for W=512 should be 1024");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 9: White-box capacity — W=1024 (large scale)
 * ================================================================ */

static void
test_capacity_white_box_w1024(void)
{
    TEST("white-box capacity W=1024 (large scale)");

    wl_work_queue_t *wq = wl_workqueue_create(1024);
    ASSERT(wq != NULL, "create(1024) failed");

    uint32_t expected = expected_capacity(1024);
    uint32_t actual = wl_workqueue_capacity(wq);
    ASSERT(actual == expected, "capacity mismatch for W=1024");
    ASSERT(actual == 2048, "capacity for W=1024 should be 2048");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 10: Black-box validation — Submit W items without overflow
 * ================================================================ */

static void
test_submit_batch_no_overflow(void)
{
    TEST("black-box: submit full batch without overflow");

    uint32_t num_workers = 8;
    wl_work_queue_t *wq = wl_workqueue_create(num_workers);
    ASSERT(wq != NULL, "create(8) failed");

    /* Submit exactly num_workers items; should all succeed */
    for (uint32_t i = 0; i < num_workers; i++) {
        int ret = wl_workqueue_submit(wq, dummy_work, (void *)(uintptr_t)i);
        ASSERT(ret == 0, "submit failed for item");
    }

    /* Drain all items synchronously (avoids thread overhead) */
    int ret = wl_workqueue_drain(wq);
    ASSERT(ret == 0, "drain failed");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 11: Large batch submission — stress test at capacity
 * ================================================================ */

static void
test_large_batch_submission(void)
{
    TEST("large batch submission (near capacity)");

    uint32_t num_workers = 16;
    wl_work_queue_t *wq = wl_workqueue_create(num_workers);
    ASSERT(wq != NULL, "create(16) failed");

    uint32_t cap = wl_workqueue_capacity(wq);
    ASSERT(cap >= 256, "capacity should be >= 256");

    /* Submit a large batch (but less than capacity to avoid full) */
    uint32_t batch_size = (cap / 2);
    for (uint32_t i = 0; i < batch_size; i++) {
        int ret = wl_workqueue_submit(wq, dummy_work, NULL);
        ASSERT(ret == 0, "submit failed in large batch");
    }

    /* Drain and verify completion */
    int ret = wl_workqueue_drain(wq);
    ASSERT(ret == 0, "drain failed");

    wl_workqueue_destroy(wq);
    PASS();
}

/* ================================================================
 * Test 12: Overflow protection (W > UINT32_MAX / 2)
 * ================================================================ */

static void
test_overflow_protection(void)
{
    TEST("overflow protection for W > UINT32_MAX/2");

    uint32_t huge_workers = UINT32_MAX;
    wl_work_queue_t *wq = wl_workqueue_create(huge_workers);
    ASSERT(wq == NULL,
        "create(UINT32_MAX) should return NULL (overflow protection)");

    PASS();
}

/* ================================================================
 * Test 13: Capacity always power of 2
 * ================================================================ */

static void
test_capacity_power_of_two(void)
{
    TEST("all capacities are power of 2");

    uint32_t test_workers[] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512};
    for (size_t i = 0; i < sizeof(test_workers) / sizeof(test_workers[0]);
        i++) {
        uint32_t w = test_workers[i];
        wl_work_queue_t *wq = wl_workqueue_create(w);
        ASSERT(wq != NULL, "create failed");

        uint32_t cap = wl_workqueue_capacity(wq);
        ASSERT((cap & (cap - 1)) == 0, "capacity must be power of 2");
        ASSERT(cap >= 256, "capacity must be >= 256");

        wl_workqueue_destroy(wq);
    }

    PASS();
}

/* ================================================================
 * Test 14: Capacity accessor NULL-safe
 * ================================================================ */

static void
test_capacity_accessor_null_safe(void)
{
    TEST("capacity accessor NULL-safe");

    uint32_t cap = wl_workqueue_capacity(NULL);
    ASSERT(cap == 0, "capacity(NULL) should return 0");

    PASS();
}

/* ================================================================
 * Main
 * ================================================================ */

int
main(void)
{
    printf(
        "================================================================================\n");
    printf(
        "test_workqueue_capacity.c - Dynamic Ring Buffer Capacity Tests (Issue #414)\n");
    printf(
        "================================================================================\n\n");

    test_create_destroy();
    test_capacity_white_box_w1();
    test_capacity_white_box_w127();
    test_capacity_white_box_w128();
    test_capacity_white_box_w129();
    test_capacity_white_box_w256();
    test_capacity_white_box_w257();
    test_capacity_white_box_w512();
    test_capacity_white_box_w1024();
    test_submit_batch_no_overflow();
    test_large_batch_submission();
    test_overflow_protection();
    test_capacity_power_of_two();
    test_capacity_accessor_null_safe();

    printf(
        "\n================================================================================\n");
    printf("Results: %d/%d tests passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(
        "\n================================================================================\n");

    return (fail_count == 0) ? 0 : 1;
}
