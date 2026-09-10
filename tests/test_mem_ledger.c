/*
 * test_mem_ledger.c - Unit tests for wl_mem_ledger_t
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests:
 *   1. alloc/free tracking accuracy
 *   2. post-allocation budget observation (over_budget)
 *   3. peak_bytes high-water mark
 *   4. concurrent thread consistency
 *   5. human-readable report output (smoke test)
 *   6. subsystem over-budget detection
 *   7. backpressure threshold
 *   8. bytes_remaining computation
 *   9. set_gauge moves the total by the difference and tracks the peak
 *  10. snapshot mirrors every counter
 *  11. subsystem table is complete (names, percentages sum to 100)
 *
 * Issue #224: Memory Observability and Graceful Degradation for DOOP OOM
 * Issue #1380: gauge/snapshot API and the CHANNEL/STORED/TEMPORARY classes
 */

#include "../wirelog/columnar/mem_ledger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include "wirelog/thread.h"
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
/* Test 2: post-allocation budget observation (over_budget)                   */
/* ======================================================================== */

static int
test_budget_enforcement(void)
{
    TEST("post-allocation budget observation (over_budget)");

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
        UINT64_MAX / 2);                 /* enormous */
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
/* Test 4: concurrent thread consistency (Unix-like systems only)           */
/* ======================================================================== */

#ifndef _WIN32

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
    TEST("concurrent thread consistency");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);

    wl_thread_t threads[CONC_THREADS];
    conc_arg_t args[CONC_THREADS];

    for (int i = 0; i < CONC_THREADS; i++) {
        args[i].ledger = &ledger;
        args[i].subsys = i % WL_MEM_SUBSYS_COUNT;
        wl_thread_create(&threads[i], conc_worker, &args[i]);
    }
    for (int i = 0; i < CONC_THREADS; i++) {
        wl_thread_join(&threads[i]);
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

#endif /* _WIN32 */

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
        (uint64_t)12 * 1024 * 1024 * 1024);                 /* 12GB */
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
/* Test 9: UINT64_MAX and invalid threshold boundaries                       */
/* ======================================================================== */

static int
test_overflow_boundaries(void)
{
    TEST("overflow-safe boundaries");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, UINT64_MAX);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, UINT64_MAX);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 1);

    uint64_t current = (uint64_t)atomic_load_explicit(
        &ledger.current_bytes, memory_order_relaxed);
    uint64_t relation = (uint64_t)atomic_load_explicit(
        &ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION], memory_order_relaxed);
    if (current != UINT64_MAX || relation != UINT64_MAX) {
        FAIL("large accounting increment wrapped instead of saturating");
        return 1;
    }
    if (wl_mem_ledger_bytes_remaining(&ledger) != 0
        || wl_mem_ledger_over_budget(&ledger)) {
        FAIL("UINT64_MAX budget boundary was not handled deterministically");
        return 1;
    }

    wl_mem_ledger_t threshold;
    wl_mem_ledger_init(&threshold, 1000);
    wl_mem_ledger_alloc(&threshold, WL_MEM_SUBSYS_CACHE, 100);
    if (!wl_mem_ledger_should_backpressure(
            &threshold, WL_MEM_SUBSYS_CACHE, 0)
        || !wl_mem_ledger_should_backpressure(
            &threshold, WL_MEM_SUBSYS_CACHE, 100)
        || wl_mem_ledger_should_backpressure(
            &threshold, WL_MEM_SUBSYS_CACHE, 101)) {
        FAIL("threshold 100/>100 boundary is incorrect");
        return 1;
    }

    /* The RELATION cap is floor(UINT64_MAX * 50 / 100), not a wrapped value. */
    wl_mem_ledger_t max_cap;
    wl_mem_ledger_init(&max_cap, UINT64_MAX);
    uint64_t relation_cap = (UINT64_MAX / 100) * 50
        + ((UINT64_MAX % 100) * 50) / 100;
    wl_mem_ledger_alloc(&max_cap, WL_MEM_SUBSYS_RELATION, relation_cap);
    if (wl_mem_ledger_subsys_over_budget(
            &max_cap, WL_MEM_SUBSYS_RELATION)) {
        FAIL("exact UINT64_MAX subsystem cap was over budget");
        return 1;
    }
    wl_mem_ledger_alloc(&max_cap, WL_MEM_SUBSYS_RELATION, 1);
    if (!wl_mem_ledger_subsys_over_budget(
            &max_cap, WL_MEM_SUBSYS_RELATION)) {
        FAIL("UINT64_MAX subsystem cap did not advance at cap + 1");
        return 1;
    }

    uint32_t percentage_sum = 0;
    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++)
        percentage_sum += wl_mem_subsys_pct[i];
    if (percentage_sum != 100) {
        FAIL("subsystem percentages do not sum to 100");
        return 1;
    }

    PASS();
    return 0;
}

/* Test 9: set_gauge (Issue #1380)                                          */
/* ======================================================================== */

static int
test_set_gauge(void)
{
    TEST("set_gauge moves total by the difference and tracks peak");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 1000);

    wl_mem_ledger_set_gauge(&ledger, WL_MEM_SUBSYS_STORED, 300);
    wl_mem_ledger_set_gauge(&ledger, WL_MEM_SUBSYS_STORED, 500); /* up   */
    wl_mem_ledger_set_gauge(&ledger, WL_MEM_SUBSYS_STORED, 200); /* down */

    wl_mem_ledger_snapshot_t snap;
    wl_mem_ledger_snapshot(&ledger, &snap);
    if (snap.subsys_bytes[WL_MEM_SUBSYS_STORED] != 200) {
        FAIL("STORED gauge should read the last value set");
        return 1;
    }
    if (snap.subsys_peak[WL_MEM_SUBSYS_STORED] != 500) {
        FAIL("STORED peak should be the largest gauge value");
        return 1;
    }
    if (snap.current_bytes != 1200) {
        char msg[64];
        snprintf(msg, sizeof(msg), "current_bytes=%llu, want 1200",
            (unsigned long long)snap.current_bytes);
        FAIL(msg);
        return 1;
    }
    if (snap.peak_bytes != 1500) {
        char msg[64];
        snprintf(msg, sizeof(msg), "peak_bytes=%llu, want 1500",
            (unsigned long long)snap.peak_bytes);
        FAIL(msg);
        return 1;
    }

    /* Gauge to zero returns the total to the allocated remainder. */
    wl_mem_ledger_set_gauge(&ledger, WL_MEM_SUBSYS_STORED, 0);
    wl_mem_ledger_snapshot(&ledger, &snap);
    if (snap.current_bytes != 1000
        || snap.subsys_bytes[WL_MEM_SUBSYS_STORED] != 0) {
        FAIL("gauge reset did not restore the total");
        return 1;
    }

    /* Out-of-range subsystem and NULL ledger are ignored. */
    wl_mem_ledger_set_gauge(&ledger, WL_MEM_SUBSYS_COUNT, 999);
    wl_mem_ledger_set_gauge(NULL, WL_MEM_SUBSYS_STORED, 999);
    wl_mem_ledger_snapshot(&ledger, &snap);
    if (snap.current_bytes != 1000) {
        FAIL("invalid gauge calls must not change the total");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 10: snapshot (Issue #1380)                                          */
/* ======================================================================== */

static int
test_snapshot(void)
{
    TEST("snapshot mirrors every counter");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 4096);
    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++)
        wl_mem_ledger_alloc(&ledger, i, (uint64_t)(i + 1) * 10);
    wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_RELATION, 5);

    wl_mem_ledger_snapshot_t snap;
    wl_mem_ledger_snapshot(&ledger, &snap);
    if (snap.total_budget != 4096) {
        FAIL("budget not mirrored");
        return 1;
    }
    uint64_t sum = 0;
    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++) {
        uint64_t want = (uint64_t)(i + 1) * 10;
        if (i == WL_MEM_SUBSYS_RELATION)
            want -= 5;
        if (snap.subsys_bytes[i] != want) {
            FAIL("subsystem current not mirrored");
            return 1;
        }
        if (snap.subsys_peak[i] != (uint64_t)(i + 1) * 10) {
            FAIL("subsystem peak not mirrored");
            return 1;
        }
        sum += want;
    }
    if (snap.current_bytes != sum || snap.peak_bytes != sum + 5) {
        FAIL("total/peak not mirrored");
        return 1;
    }

    /* NULL handling: NULL ledger zeroes the output, NULL output is a no-op. */
    memset(&snap, 0xFF, sizeof(snap));
    wl_mem_ledger_snapshot(NULL, &snap);
    if (snap.current_bytes != 0 || snap.peak_bytes != 0) {
        FAIL("NULL ledger must zero the snapshot");
        return 1;
    }
    wl_mem_ledger_snapshot(&ledger, NULL);

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 11: subsystem table (Issue #1380)                                   */
/* ======================================================================== */

static int
test_subsys_table(void)
{
    TEST("subsystem table is complete and percentages sum to 100");

    uint32_t pct = 0;
    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++) {
        if (!wl_mem_subsys_names[i] || wl_mem_subsys_names[i][0] == '\0') {
            FAIL("subsystem without a name");
            return 1;
        }
        pct += wl_mem_subsys_pct[i];
    }
    if (pct != 100) {
        FAIL("subsystem percentages must sum to 100");
        return 1;
    }
    if (WL_MEM_SUBSYS_COUNT != 8
        || strcmp(wl_mem_subsys_names[WL_MEM_SUBSYS_CHANNEL], "CHANNEL") != 0
        || strcmp(wl_mem_subsys_names[WL_MEM_SUBSYS_STORED], "STORED") != 0
        || strcmp(wl_mem_subsys_names[WL_MEM_SUBSYS_TEMPORARY],
        "TEMPORARY") != 0) {
        FAIL("Issue #1380 subsystems missing");
        return 1;
    }
    /* The join operator polls RELATION at 80% of its cap; that share is
     * part of the observable backpressure contract. */
    if (wl_mem_subsys_pct[WL_MEM_SUBSYS_RELATION] != 50) {
        FAIL("RELATION share must stay at 50%");
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
#ifndef _WIN32
    test_concurrent_consistency();
#endif
    test_report_output();
    test_subsys_over_budget();
    test_backpressure_threshold();
    test_bytes_remaining();
    test_overflow_boundaries();
    test_set_gauge();
    test_snapshot();
    test_subsys_table();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
