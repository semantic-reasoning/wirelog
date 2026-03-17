/*
 * test_mem_ledger.c - Unit tests for wl_mem_ledger_t
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests:
 *   1. alloc/free tracking accuracy
 *   2. budget enforcement (over_budget)
 *   3. peak_bytes high-water mark
 *   4. concurrent multi-pthread consistency
 *   5. human-readable report output (smoke test)
 *   6. subsystem over-budget detection
 *   7. backpressure threshold
 *   8. bytes_remaining computation
 *
 * Issue #224: Memory Observability and Graceful Degradation for DOOP OOM
 */

#include "../wirelog/columnar/mem_ledger.h"

#include <pthread.h>
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
/* Test 1: alloc/free tracking accuracy                                     */
/* ======================================================================== */

static int
test_alloc_free_accuracy(void)
{
    TEST("alloc/free tracking accuracy");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0); /* unlimited */

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 1024);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_ARENA, 512);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_CACHE, 256);

    uint64_t cur = (uint64_t)atomic_load_explicit(&ledger.current_bytes,
                                                  memory_order_relaxed);
    uint64_t rel = (uint64_t)atomic_load_explicit(
        &ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION], memory_order_relaxed);
    uint64_t arena = (uint64_t)atomic_load_explicit(
        &ledger.subsys_bytes[WL_MEM_SUBSYS_ARENA], memory_order_relaxed);

    if (cur != 1792) {
        char msg[64];
        snprintf(msg, sizeof(msg), "current_bytes=%llu, want 1792",
                 (unsigned long long)cur);
        FAIL(msg);
        return 1;
    }
    if (rel != 1024) {
        FAIL("RELATION subsys_bytes wrong after alloc");
        return 1;
    }
    if (arena != 512) {
        FAIL("ARENA subsys_bytes wrong after alloc");
        return 1;
    }

    wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_RELATION, 512);
    cur = (uint64_t)atomic_load_explicit(&ledger.current_bytes,
                                         memory_order_relaxed);
    rel = (uint64_t)atomic_load_explicit(
        &ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION], memory_order_relaxed);

    if (cur != 1280) {
        char msg[64];
        snprintf(msg, sizeof(msg), "current_bytes=%llu after free, want 1280",
                 (unsigned long long)cur);
        FAIL(msg);
        return 1;
    }
    if (rel != 512) {
        FAIL("RELATION subsys_bytes wrong after free");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: budget enforcement (over_budget)                                 */
/* ======================================================================== */

static int
test_budget_enforcement(void)
{
    TEST("budget enforcement (over_budget)");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 1000); /* 1000 byte budget */

    if (wl_mem_ledger_over_budget(&ledger)) {
        FAIL("should not be over budget at start");
        return 1;
    }

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 999);
    if (wl_mem_ledger_over_budget(&ledger)) {
        FAIL("should not be over budget at 999/1000");
        return 1;
    }

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 2);
    if (!wl_mem_ledger_over_budget(&ledger)) {
        FAIL("should be over budget at 1001/1000");
        return 1;
    }

    /* Free back under budget */
    wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_RELATION, 2);
    if (wl_mem_ledger_over_budget(&ledger)) {
        FAIL("should not be over budget after free");
        return 1;
    }

    /* Unlimited budget (0) should never be over */
    wl_mem_ledger_t unlimited;
    wl_mem_ledger_init(&unlimited, 0);
    wl_mem_ledger_alloc(&unlimited, WL_MEM_SUBSYS_RELATION,
                        UINT64_MAX / 2); /* enormous */
    if (wl_mem_ledger_over_budget(&unlimited)) {
        FAIL("unlimited budget (0) should never be over_budget");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: peak_bytes high-water mark                                       */
/* ======================================================================== */

static int
test_peak_high_water(void)
{
    TEST("peak_bytes high-water mark");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 2000);
    uint64_t peak1 = (uint64_t)atomic_load_explicit(&ledger.peak_bytes,
                                                    memory_order_relaxed);
    if (peak1 != 2000) {
        char msg[64];
        snprintf(msg, sizeof(msg), "peak=%llu after 2000 alloc, want 2000",
                 (unsigned long long)peak1);
        FAIL(msg);
        return 1;
    }

    /* Free does not lower peak */
    wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_RELATION, 2000);
    uint64_t peak2 = (uint64_t)atomic_load_explicit(&ledger.peak_bytes,
                                                    memory_order_relaxed);
    if (peak2 != 2000) {
        FAIL("peak_bytes decreased after free (should be HWM)");
        return 1;
    }

    /* New alloc below old HWM does not change peak */
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_ARENA, 500);
    uint64_t peak3 = (uint64_t)atomic_load_explicit(&ledger.peak_bytes,
                                                    memory_order_relaxed);
    if (peak3 != 2000) {
        FAIL("peak_bytes changed for alloc below HWM");
        return 1;
    }

    /* Alloc above old HWM updates peak */
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_ARENA, 2000);
    uint64_t peak4 = (uint64_t)atomic_load_explicit(&ledger.peak_bytes,
                                                    memory_order_relaxed);
    if (peak4 != 2500) {
        char msg[64];
        snprintf(msg, sizeof(msg), "peak=%llu, want 2500",
                 (unsigned long long)peak4);
        FAIL(msg);
        return 1;
    }

    /* Subsystem peak also tracked */
    uint64_t arena_peak = (uint64_t)atomic_load_explicit(
        &ledger.subsys_peak[WL_MEM_SUBSYS_ARENA], memory_order_relaxed);
    if (arena_peak != 2500) {
        char msg[64];
        snprintf(msg, sizeof(msg), "ARENA subsys_peak=%llu, want 2500",
                 (unsigned long long)arena_peak);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 4: concurrent multi-pthread consistency                             */
/* ======================================================================== */

#define CONC_THREADS 8
#define CONC_OPS 10000
#define CONC_ALLOC_SZ 64

typedef struct {
    wl_mem_ledger_t *ledger;
    int subsys;
} conc_arg_t;

static void *
conc_worker(void *arg)
{
    conc_arg_t *a = (conc_arg_t *)arg;
    for (int i = 0; i < CONC_OPS; i++) {
        wl_mem_ledger_alloc(a->ledger, a->subsys, CONC_ALLOC_SZ);
        wl_mem_ledger_free(a->ledger, a->subsys, CONC_ALLOC_SZ);
    }
    return NULL;
}

static int
test_concurrent_consistency(void)
{
    TEST("concurrent multi-pthread consistency");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);

    pthread_t threads[CONC_THREADS];
    conc_arg_t args[CONC_THREADS];

    for (int i = 0; i < CONC_THREADS; i++) {
        args[i].ledger = &ledger;
        args[i].subsys = i % WL_MEM_SUBSYS_COUNT;
        pthread_create(&threads[i], NULL, conc_worker, &args[i]);
    }
    for (int i = 0; i < CONC_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    /* After equal alloc/free cycles, current_bytes must be 0 */
    uint64_t cur = (uint64_t)atomic_load_explicit(&ledger.current_bytes,
                                                  memory_order_relaxed);
    if (cur != 0) {
        char msg[80];
        snprintf(msg, sizeof(msg),
                 "current_bytes=%llu after balanced alloc/free, want 0",
                 (unsigned long long)cur);
        FAIL(msg);
        return 1;
    }

    /* Peak must be > 0 (some concurrent allocation occurred) */
    uint64_t peak = (uint64_t)atomic_load_explicit(&ledger.peak_bytes,
                                                   memory_order_relaxed);
    if (peak == 0) {
        FAIL("peak_bytes==0 after concurrent allocs (unexpected)");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 5: report output smoke test                                         */
/* ======================================================================== */

static int
test_report_output(void)
{
    TEST("report output smoke test (stderr)");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, (uint64_t)48 * 1024 * 1024 * 1024); /* 48GB */

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
                        (uint64_t)12 * 1024 * 1024 * 1024); /* 12GB */
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_ARENA,
                        (uint64_t)2 * 1024 * 1024 * 1024);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_CACHE,
                        (uint64_t)500 * 1024 * 1024);

    /* Just verify it doesn't crash; output goes to stderr */
    wl_mem_ledger_report(&ledger);

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 6: subsystem over-budget detection                                  */
/* ======================================================================== */

static int
test_subsys_over_budget(void)
{
    TEST("subsystem over-budget detection");

    /* Budget: 1000 bytes.  RELATION cap = 50% = 500 bytes. */
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 1000);

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 499);
    if (wl_mem_ledger_subsys_over_budget(&ledger, WL_MEM_SUBSYS_RELATION)) {
        FAIL("RELATION should not be over subsys budget at 499/500");
        return 1;
    }

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 2);
    if (!wl_mem_ledger_subsys_over_budget(&ledger, WL_MEM_SUBSYS_RELATION)) {
        FAIL("RELATION should be over subsys budget at 501/500");
        return 1;
    }

    /* Unlimited (budget=0) should never flag over */
    wl_mem_ledger_t unlimited;
    wl_mem_ledger_init(&unlimited, 0);
    wl_mem_ledger_alloc(&unlimited, WL_MEM_SUBSYS_RELATION, UINT64_MAX / 2);
    if (wl_mem_ledger_subsys_over_budget(&unlimited, WL_MEM_SUBSYS_RELATION)) {
        FAIL("unlimited budget: subsys_over_budget should return false");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 7: backpressure threshold                                           */
/* ======================================================================== */

static int
test_backpressure_threshold(void)
{
    TEST("backpressure threshold");

    /* Budget: 1000.  CACHE cap = 10% = 100 bytes. */
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 1000);

    /* At 0%, threshold=80 -> false */
    if (wl_mem_ledger_should_backpressure(&ledger, WL_MEM_SUBSYS_CACHE, 80)) {
        FAIL("backpressure at 0% (threshold 80) should be false");
        return 1;
    }

    /* At 79 bytes (79% of cap=100), threshold=80 -> false */
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_CACHE, 79);
    if (wl_mem_ledger_should_backpressure(&ledger, WL_MEM_SUBSYS_CACHE, 80)) {
        FAIL("backpressure at 79% (threshold 80) should be false");
        return 1;
    }

    /* At 80 bytes (80% of cap=100), threshold=80 -> true */
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_CACHE, 1);
    if (!wl_mem_ledger_should_backpressure(&ledger, WL_MEM_SUBSYS_CACHE, 80)) {
        FAIL("backpressure at 80% (threshold 80) should be true");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 8: bytes_remaining computation                                      */
/* ======================================================================== */

static int
test_bytes_remaining(void)
{
    TEST("bytes_remaining computation");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 1000);

    uint64_t rem = wl_mem_ledger_bytes_remaining(&ledger);
    if (rem != 1000) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remaining=%llu, want 1000",
                 (unsigned long long)rem);
        FAIL(msg);
        return 1;
    }

    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_ARENA, 300);
    rem = wl_mem_ledger_bytes_remaining(&ledger);
    if (rem != 700) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remaining=%llu after 300 alloc, want 700",
                 (unsigned long long)rem);
        FAIL(msg);
        return 1;
    }

    /* Over-budget: remaining clamped to 0 */
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_ARENA, 800);
    rem = wl_mem_ledger_bytes_remaining(&ledger);
    if (rem != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remaining=%llu when over budget, want 0",
                 (unsigned long long)rem);
        FAIL(msg);
        return 1;
    }

    /* Unlimited: remaining == UINT64_MAX */
    wl_mem_ledger_t unlimited;
    wl_mem_ledger_init(&unlimited, 0);
    rem = wl_mem_ledger_bytes_remaining(&unlimited);
    if (rem != UINT64_MAX) {
        FAIL("unlimited budget: bytes_remaining should be UINT64_MAX");
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
    printf("Memory Ledger Unit Tests (Issue #224)\n");
    printf("======================================\n\n");

    test_alloc_free_accuracy();
    test_budget_enforcement();
    test_peak_high_water();
    test_concurrent_consistency();
    test_report_output();
    test_subsys_over_budget();
    test_backpressure_threshold();
    test_bytes_remaining();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
