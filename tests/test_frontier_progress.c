/*
 * test_frontier_progress.c - Unit tests for per-worker frontier progress
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: init/destroy lifecycle, record, min_iteration, all_converged,
 * reset_stratum, edge cases (NULL, out-of-range, multi-stratum/multi-worker).
 *
 * Issue #317: Per-Worker Frontier Progress Protocol
 */

#include "../wirelog/columnar/progress.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

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
/* Tests: Init / Destroy                                                    */
/* ======================================================================== */

static void
test_init_basic(void)
{
    TEST("init basic (4 workers, 8 strata)");
    wl_frontier_progress_t p;
    int rc = wl_frontier_progress_init(&p, 4, 8);
    if (rc != 0) {
        FAIL("init returned non-zero");
        return;
    }
    if (!p.entries || p.num_workers != 4 || p.num_strata != 8) {
        FAIL("fields not set correctly");
        wl_frontier_progress_destroy(&p);
        return;
    }
    /* All entries must start as (outer_epoch=0, iteration=UINT32_MAX) */
    for (uint32_t si = 0; si < 8; si++) {
        for (uint32_t wi = 0; wi < 4; wi++) {
            const wl_worker_frontier_entry_t *e
                = &p.entries[si * 4 + wi];
            if (e->outer_epoch != 0 || e->iteration != UINT32_MAX) {
                FAIL("entry not initialized to sentinel");
                wl_frontier_progress_destroy(&p);
                return;
            }
        }
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_init_single_worker(void)
{
    TEST("init single worker single stratum");
    wl_frontier_progress_t p;
    int rc = wl_frontier_progress_init(&p, 1, 1);
    if (rc != 0) {
        FAIL("init returned non-zero");
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_init_invalid_args(void)
{
    TEST("init rejects NULL/zero");
    wl_frontier_progress_t p;
    if (wl_frontier_progress_init(NULL, 4, 8) != EINVAL) {
        FAIL("NULL p not rejected");
        return;
    }
    if (wl_frontier_progress_init(&p, 0, 8) != EINVAL) {
        FAIL("zero num_workers not rejected");
        return;
    }
    if (wl_frontier_progress_init(&p, 4, 0) != EINVAL) {
        FAIL("zero num_strata not rejected");
        return;
    }
    PASS();
}

static void
test_destroy_null_safe(void)
{
    TEST("destroy NULL is safe");
    wl_frontier_progress_destroy(NULL); /* must not crash */
    PASS();
}

/* ======================================================================== */
/* Tests: Record                                                            */
/* ======================================================================== */

static void
test_record_basic(void)
{
    TEST("record stores entry");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 4);

    wl_frontier_progress_record(&p, 0, 2, 5, 12);
    const wl_worker_frontier_entry_t *e = &p.entries[2 * 2 + 0];
    if (e->outer_epoch != 5 || e->iteration != 12) {
        FAIL("entry not stored correctly");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_record_out_of_range(void)
{
    TEST("record ignores out-of-range indices");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 4);
    /* These must not crash */
    wl_frontier_progress_record(&p, 2, 0, 1, 5);  /* worker_id >= num_workers */
    wl_frontier_progress_record(&p, 0, 4, 1, 5);  /* stratum_idx >= num_strata */
    wl_frontier_progress_record(NULL, 0, 0, 1, 5); /* NULL p */
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_record_overwrite(void)
{
    TEST("record overwrites previous entry for same slot");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 2);

    wl_frontier_progress_record(&p, 1, 0, 3, 7);
    wl_frontier_progress_record(&p, 1, 0, 4, 9); /* new epoch */
    const wl_worker_frontier_entry_t *e = &p.entries[0 * 2 + 1];
    if (e->outer_epoch != 4 || e->iteration != 9) {
        FAIL("overwrite did not update entry");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

/* ======================================================================== */
/* Tests: Min Iteration                                                     */
/* ======================================================================== */

static void
test_min_iteration_all_same(void)
{
    TEST("min_iteration: all workers same iteration");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 3, 2);

    wl_frontier_progress_record(&p, 0, 1, 2, 10);
    wl_frontier_progress_record(&p, 1, 1, 2, 10);
    wl_frontier_progress_record(&p, 2, 1, 2, 10);

    uint32_t min = wl_frontier_progress_min_iteration(&p, 1, 2);
    if (min != 10) {
        FAIL("expected min=10");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_min_iteration_different(void)
{
    TEST("min_iteration: returns minimum across workers");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 4, 1);

    wl_frontier_progress_record(&p, 0, 0, 1, 15);
    wl_frontier_progress_record(&p, 1, 0, 1, 7);
    wl_frontier_progress_record(&p, 2, 0, 1, 20);
    wl_frontier_progress_record(&p, 3, 0, 1, 3);

    uint32_t min = wl_frontier_progress_min_iteration(&p, 0, 1);
    if (min != 3) {
        FAIL("expected min=3");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_min_iteration_epoch_mismatch(void)
{
    TEST("min_iteration: excludes workers from different epoch");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 3, 1);

    /* Workers 0 and 1 in epoch 2, worker 2 still in epoch 1 */
    wl_frontier_progress_record(&p, 0, 0, 2, 5);
    wl_frontier_progress_record(&p, 1, 0, 2, 8);
    wl_frontier_progress_record(&p, 2, 0, 1, 2); /* old epoch - excluded */

    uint32_t min = wl_frontier_progress_min_iteration(&p, 0, 2);
    if (min != 5) {
        FAIL("expected min=5 (worker 2 excluded for epoch mismatch)");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_min_iteration_no_reports(void)
{
    TEST("min_iteration: returns UINT32_MAX when no reports");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 3);

    /* No records for (stratum=1, epoch=5) */
    uint32_t min = wl_frontier_progress_min_iteration(&p, 1, 5);
    if (min != UINT32_MAX) {
        FAIL("expected UINT32_MAX when no reports");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_min_iteration_null_safe(void)
{
    TEST("min_iteration: NULL/out-of-range returns UINT32_MAX");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 2);
    if (wl_frontier_progress_min_iteration(NULL, 0, 0) != UINT32_MAX) {
        FAIL("NULL p should return UINT32_MAX");
        wl_frontier_progress_destroy(&p);
        return;
    }
    if (wl_frontier_progress_min_iteration(&p, 99, 0) != UINT32_MAX) {
        FAIL("out-of-range stratum should return UINT32_MAX");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

/* ======================================================================== */
/* Tests: All Converged                                                     */
/* ======================================================================== */

static void
test_all_converged_true(void)
{
    TEST("all_converged: true when all workers reported");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 3, 1);

    wl_frontier_progress_record(&p, 0, 0, 7, 4);
    wl_frontier_progress_record(&p, 1, 0, 7, 6);
    wl_frontier_progress_record(&p, 2, 0, 7, 2);

    if (!wl_frontier_progress_all_converged(&p, 0, 7)) {
        FAIL("expected all_converged=true");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_all_converged_partial(void)
{
    TEST("all_converged: false when one worker missing");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 3, 1);

    wl_frontier_progress_record(&p, 0, 0, 3, 5);
    wl_frontier_progress_record(&p, 1, 0, 3, 5);
    /* worker 2 has not reported for epoch 3 */

    if (wl_frontier_progress_all_converged(&p, 0, 3)) {
        FAIL("expected all_converged=false (worker 2 missing)");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_all_converged_epoch_mismatch(void)
{
    TEST("all_converged: false when a worker is in wrong epoch");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 1);

    wl_frontier_progress_record(&p, 0, 0, 4, 3);
    wl_frontier_progress_record(&p, 1, 0, 3, 3); /* old epoch */

    if (wl_frontier_progress_all_converged(&p, 0, 4)) {
        FAIL("expected false (worker 1 in wrong epoch)");
        wl_frontier_progress_destroy(&p);
        return;
    }
    wl_frontier_progress_destroy(&p);
    PASS();
}

/* ======================================================================== */
/* Tests: Reset Stratum                                                     */
/* ======================================================================== */

static void
test_reset_stratum_basic(void)
{
    TEST("reset_stratum clears all worker slots");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 4, 3);

    /* Record some data for stratum 1 */
    for (uint32_t wi = 0; wi < 4; wi++)
        wl_frontier_progress_record(&p, wi, 1, 2, 8);

    /* Reset stratum 1 for epoch 3 */
    wl_frontier_progress_reset_stratum(&p, 1, 3);

    /* All worker slots for stratum 1 should be (epoch=3, iter=UINT32_MAX) */
    for (uint32_t wi = 0; wi < 4; wi++) {
        const wl_worker_frontier_entry_t *e
            = &p.entries[1 * 4 + wi];
        if (e->outer_epoch != 3 || e->iteration != UINT32_MAX) {
            FAIL("slot not reset correctly");
            wl_frontier_progress_destroy(&p);
            return;
        }
    }

    /* Other strata should be unaffected */
    for (uint32_t wi = 0; wi < 4; wi++) {
        const wl_worker_frontier_entry_t *e0
            = &p.entries[0 * 4 + wi];
        if (e0->outer_epoch != 0 || e0->iteration != UINT32_MAX) {
            FAIL("stratum 0 was incorrectly modified");
            wl_frontier_progress_destroy(&p);
            return;
        }
    }

    wl_frontier_progress_destroy(&p);
    PASS();
}

static void
test_reset_stratum_then_all_converged_false(void)
{
    TEST("reset_stratum then all_converged is false");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 2);

    /* Record and confirm convergence */
    wl_frontier_progress_record(&p, 0, 0, 1, 5);
    wl_frontier_progress_record(&p, 1, 0, 1, 5);
    if (!wl_frontier_progress_all_converged(&p, 0, 1)) {
        FAIL("pre-condition: should be converged before reset");
        wl_frontier_progress_destroy(&p);
        return;
    }

    /* Reset and verify no longer converged */
    wl_frontier_progress_reset_stratum(&p, 0, 2);
    if (wl_frontier_progress_all_converged(&p, 0, 2)) {
        FAIL("should not be converged after reset");
        wl_frontier_progress_destroy(&p);
        return;
    }

    wl_frontier_progress_destroy(&p);
    PASS();
}

/* ======================================================================== */
/* Tests: Multi-Stratum Independence                                        */
/* ======================================================================== */

static void
test_multi_stratum_independence(void)
{
    TEST("multiple strata are independent");
    wl_frontier_progress_t p;
    wl_frontier_progress_init(&p, 2, 4);

    /* Record different iterations per stratum */
    wl_frontier_progress_record(&p, 0, 0, 1, 3);
    wl_frontier_progress_record(&p, 1, 0, 1, 5);
    wl_frontier_progress_record(&p, 0, 1, 1, 10);
    wl_frontier_progress_record(&p, 1, 1, 1, 20);
    wl_frontier_progress_record(&p, 0, 2, 1, 7);
    wl_frontier_progress_record(&p, 1, 2, 1, 2);

    uint32_t min0 = wl_frontier_progress_min_iteration(&p, 0, 1);
    uint32_t min1 = wl_frontier_progress_min_iteration(&p, 1, 1);
    uint32_t min2 = wl_frontier_progress_min_iteration(&p, 2, 1);
    /* stratum 3: no records */
    uint32_t min3 = wl_frontier_progress_min_iteration(&p, 3, 1);

    if (min0 != 3 || min1 != 10 || min2 != 2 || min3 != UINT32_MAX) {
        FAIL("stratum mins not independent");
        wl_frontier_progress_destroy(&p);
        return;
    }

    wl_frontier_progress_destroy(&p);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_frontier_progress\n");

    /* Init / Destroy */
    test_init_basic();
    test_init_single_worker();
    test_init_invalid_args();
    test_destroy_null_safe();

    /* Record */
    test_record_basic();
    test_record_out_of_range();
    test_record_overwrite();

    /* Min iteration */
    test_min_iteration_all_same();
    test_min_iteration_different();
    test_min_iteration_epoch_mismatch();
    test_min_iteration_no_reports();
    test_min_iteration_null_safe();

    /* All converged */
    test_all_converged_true();
    test_all_converged_partial();
    test_all_converged_epoch_mismatch();

    /* Reset stratum */
    test_reset_stratum_basic();
    test_reset_stratum_then_all_converged_false();

    /* Multi-stratum */
    test_multi_stratum_independence();

    printf("\n%d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
