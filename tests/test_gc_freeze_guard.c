/*
 * test_gc_freeze_guard.c - Issue #561 unit tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that wl_compound_arena_gc_epoch_boundary respects the
 * arena->frozen flag (K-Fusion barrier invariant): while frozen, the
 * GC path must be a pure no-op that returns the unchanged
 * current_epoch and leaves arena state intact.  After unfreeze, the
 * normal generational-reclaim semantics resume.
 */

#include "../wirelog/arena/compound_arena.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* ======================================================================== */
/* Test harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            return;                                 \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_gc_skips_when_frozen(void)
{
    TEST("gc_epoch_boundary: skips when arena is frozen");
    wl_compound_arena_t *a = wl_compound_arena_create(0xCAFEu, 256, 0);
    ASSERT(a != NULL, "create failed");

    uint32_t epoch_snapshot = a->current_epoch;
    wl_compound_arena_freeze(a);
    ASSERT(a->frozen, "freeze flag not set");

    uint32_t ret = wl_compound_arena_gc_epoch_boundary(a);
    ASSERT(ret == epoch_snapshot,
        "frozen gc should return the unchanged current_epoch");
    ASSERT(a->current_epoch == epoch_snapshot,
        "frozen gc must not advance current_epoch");
    ASSERT(a->frozen, "frozen flag must remain set across skipped gc");

    wl_compound_arena_free(a);
    PASS();
}

static void
test_gc_resumes_after_unfreeze(void)
{
    TEST("gc_epoch_boundary: resumes after unfreeze");
    wl_compound_arena_t *a = wl_compound_arena_create(0xBEEFu, 256, 0);
    ASSERT(a != NULL, "create failed");

    uint32_t epoch_snapshot = a->current_epoch;

    /* Freeze + skipped GC: epoch unchanged. */
    wl_compound_arena_freeze(a);
    (void)wl_compound_arena_gc_epoch_boundary(a);
    ASSERT(a->current_epoch == epoch_snapshot,
        "frozen gc must not advance current_epoch");

    /* Unfreeze + GC: epoch must advance. */
    wl_compound_arena_unfreeze(a);
    ASSERT(!a->frozen, "unfreeze flag not cleared");
    (void)wl_compound_arena_gc_epoch_boundary(a);
    ASSERT(a->current_epoch == epoch_snapshot + 1,
        "post-unfreeze gc must advance current_epoch by one");

    wl_compound_arena_free(a);
    PASS();
}

static void
test_gc_double_call_after_freeze_safe(void)
{
    TEST("gc_epoch_boundary: double call while frozen is a stable no-op");
    wl_compound_arena_t *a = wl_compound_arena_create(0xD00Du, 256, 0);
    ASSERT(a != NULL, "create failed");

    uint32_t epoch_snapshot = a->current_epoch;
    uint64_t live_snapshot = a->live_handles;

    wl_compound_arena_freeze(a);

    uint32_t ret1 = wl_compound_arena_gc_epoch_boundary(a);
    uint32_t ret2 = wl_compound_arena_gc_epoch_boundary(a);
    ASSERT(ret1 == epoch_snapshot, "first frozen-gc returned wrong value");
    ASSERT(ret2 == epoch_snapshot, "second frozen-gc returned wrong value");
    ASSERT(a->current_epoch == epoch_snapshot,
        "frozen double-gc must not advance current_epoch");
    ASSERT(a->live_handles == live_snapshot,
        "frozen double-gc must not mutate live_handles");
    ASSERT(a->frozen, "frozen flag must persist across double-call");

    wl_compound_arena_free(a);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_gc_freeze_guard (Issue #561)\n");
    printf("=================================\n");

    test_gc_skips_when_frozen();
    test_gc_resumes_after_unfreeze();
    test_gc_double_call_after_freeze_safe();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
