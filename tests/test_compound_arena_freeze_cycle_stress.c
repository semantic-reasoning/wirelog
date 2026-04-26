/*
 * test_compound_arena_freeze_cycle_stress.c - Issue #582 stress harness
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Stress-test the K-Fusion arena freeze contract (R-5 borrow + freeze
 * guard) by running many alloc/freeze/lookup/unfreeze/gc cycles on a
 * single arena while several workers concurrently read the borrowed
 * compound_arena pointer.  The goal is a TSan-clean run: the arena
 * pointer is reused across cycles, but each cycle must establish a
 * happens-before boundary (workqueue submit/wait_all release+acquire
 * pair) so the freeze flag, lookup table, and live_handles counter
 * are visible to workers without explicit atomics.
 *
 * Note on naming: this is a freeze-cycle test, NOT an arena-rotation
 * test in the #550-C sense (rotate to a fresh arena while workers are
 * mid-flight).  Issue #582 used the word "rotation" loosely to mean
 * "iteration cycle".  When #550-C lands, a sibling test will exercise
 * the cross-arena swap path; this harness pins the in-arena cycle.
 *
 * Two configurations exercised:
 *   - W = 4, 500 cycles  (smaller fan-out, more turn-around per worker)
 *   - W = 8, 1000 cycles (larger fan-out, more cycles per worker)
 *
 * Each cycle:
 *   1. coordinator alloc()s a sentinel handle in the current epoch,
 *   2. coordinator freeze()s the arena,
 *   3. W workers concurrently
 *        a. lookup() the sentinel — must succeed,
 *        b. alloc() — must refuse (frozen guard),
 *   4. coordinator wait_all()s and unfreeze()s,
 *   5. coordinator gc_epoch_boundary() advances the epoch.
 *
 * Bound: max_epochs = WL_COMPOUND_EPOCH_MAX + 1 = 4096; the largest
 * configured cycle count (1000) leaves plenty of head room, so the
 * arena never saturates within a single configuration.  We assert that
 * up front to keep the harness focused on freeze visibility.
 *
 * Acceptance: every worker assertion holds AND TSan reports zero races
 * in build-tsan.
 */

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/workqueue.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Each worker is given exactly one task ctx per rotation; the workqueue
 * mutex pair establishes the happens-before relationship from
 * coordinator's freeze() to the worker reading arena->frozen. */
typedef struct {
    wl_compound_arena_t *arena;
    uint64_t sentinel_handle;
    uint32_t expected_payload_size;
    /* Worker writes outcome flags here; coordinator reads after wait_all. */
    int lookup_ok;
    int alloc_blocked;
} worker_task_t;

static void
worker_fn(void *ctx)
{
    worker_task_t *t = (worker_task_t *)ctx;
    uint32_t out_size = 0;
    const void *payload = wl_compound_arena_lookup(t->arena,
            t->sentinel_handle, &out_size);
    t->lookup_ok = (payload != NULL && out_size == t->expected_payload_size);

    /* Lookup must NOT mutate the arena.  Even if it did under a future
     * change, the freeze guard on alloc must still refuse new handles
     * for as long as arena->frozen holds. */
    uint64_t denied = wl_compound_arena_alloc(t->arena, 16u);
    t->alloc_blocked = (denied == WL_COMPOUND_HANDLE_NULL);
}

static int
run_freeze_cycle_stress(uint32_t num_workers, uint32_t cycles,
    const char *label)
{
    printf("  [W=%u cycles=%u] %s ... ", num_workers, cycles, label);
    fflush(stdout);

    wl_compound_arena_t *arena = wl_compound_arena_create(0xCAFEu, 4096u, 0u);
    if (!arena) {
        printf("FAIL: arena create\n");
        return 1;
    }

    /* Bound check (test contract): each cycle advances current_epoch by
     * one (gc_epoch_boundary at cycle tail), so the configured cycle
     * count must fit within the arena's epoch cap.  We deliberately do
     * NOT recover from saturation here — that's the cross-arena swap
     * path Option C in #550 introduces, and a sibling test will own it.
     * Keeping the bound assertion explicit makes accidental saturation
     * loud instead of silently re-creating the arena under workers. */
    if (cycles >= arena->max_epochs) {
        printf("FAIL: requested cycles=%u exceeds max_epochs=%u\n",
            cycles, arena->max_epochs);
        wl_compound_arena_free(arena);
        return 1;
    }

    wl_work_queue_t *wq = wl_workqueue_create(num_workers);
    if (!wq) {
        printf("FAIL: workqueue create\n");
        wl_compound_arena_free(arena);
        return 1;
    }

    /* Per-worker task slots reused across cycles.  Allocated up front
     * so the cycle loop is allocation-free and the test runs fast even
     * at W=8 × 1000 cycles. */
    worker_task_t *tasks
        = (worker_task_t *)calloc(num_workers, sizeof(worker_task_t));
    if (!tasks) {
        printf("FAIL: tasks alloc\n");
        wl_workqueue_destroy(wq);
        wl_compound_arena_free(arena);
        return 1;
    }

    int verdict = 0;
    /* clock() is portable across MSVC/glibc/musl; CLOCK_MONOTONIC is
     * POSIX-only and trips C2065 on the Windows builders.  Resolution
     * (CLOCKS_PER_SEC, typically µs) is more than enough for the
     * diagnostic print at the end of each configuration. */
    clock_t t0 = clock();

    for (uint32_t r = 0; r < cycles; r++) {
        uint64_t handle = wl_compound_arena_alloc(arena, 24u);
        if (handle == WL_COMPOUND_HANDLE_NULL) {
            printf("FAIL: cycle %u sentinel alloc returned NULL\n", r);
            verdict = 1;
            break;
        }

        wl_compound_arena_freeze(arena);

        for (uint32_t w = 0; w < num_workers; w++) {
            tasks[w].arena = arena;
            tasks[w].sentinel_handle = handle;
            tasks[w].expected_payload_size = 24u;
            tasks[w].lookup_ok = 0;
            tasks[w].alloc_blocked = 0;
            if (wl_workqueue_submit(wq, worker_fn, &tasks[w]) != 0) {
                printf("FAIL: submit at cycle %u worker %u\n", r, w);
                verdict = 1;
                break;
            }
        }
        if (verdict)
            break;

        if (wl_workqueue_wait_all(wq) != 0) {
            printf("FAIL: wait_all at cycle %u\n", r);
            verdict = 1;
            break;
        }

        for (uint32_t w = 0; w < num_workers; w++) {
            if (!tasks[w].lookup_ok) {
                printf("FAIL: cycle %u worker %u lookup failed\n", r, w);
                verdict = 1;
                break;
            }
            if (!tasks[w].alloc_blocked) {
                printf("FAIL: cycle %u worker %u frozen alloc accepted\n",
                    r, w);
                verdict = 1;
                break;
            }
        }
        if (verdict)
            break;

        wl_compound_arena_unfreeze(arena);
        (void)wl_compound_arena_gc_epoch_boundary(arena);
    }

    clock_t t1 = clock();
    double secs = (double)(t1 - t0) / (double)CLOCKS_PER_SEC;

    free(tasks);
    wl_workqueue_destroy(wq);
    wl_compound_arena_free(arena);

    if (verdict == 0)
        printf("PASS (%.3fs)\n", secs);
    return verdict;
}

int
main(void)
{
    printf("test_compound_arena_freeze_cycle_stress (Issue #582)\n");
    printf("====================================================\n");

    int rc = 0;
    rc |= run_freeze_cycle_stress(4u, 500u,
            "W=4 cycles under TSan must be race-free");
    rc |= run_freeze_cycle_stress(8u, 1000u,
            "W=8 cycles under TSan must be race-free");

    if (rc == 0)
        printf("\nAll freeze-cycle stress configurations passed.\n");
    else
        printf("\nFAILURE: at least one configuration failed.\n");
    return rc == 0 ? 0 : 1;
}
