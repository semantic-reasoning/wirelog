/*
 * test_gc_freeze_alloc_race.c - Issue #584 W=2 TSan race verification
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that the freeze guard composes correctly when arena_alloc
 * and gc_epoch_boundary run concurrently on the same frozen arena.
 *
 * The single-mutator contract documented at compound_arena.h:52
 * forbids unsynchronised concurrent writes; the K-Fusion path
 * therefore drains workers around the freeze barrier.  But the
 * freeze guard itself must be safe to *enter* concurrently from two
 * threads — both ops sample arena->frozen and bail before touching
 * any mutable state — and that property has so far only been
 * exercised in the lookup-vs-alloc combination
 * (test_compound_arena_freeze_cycle_stress.c).  Issue #584 calls out
 * the alloc-vs-gc combination explicitly.
 *
 * Test shape (W=2, many iterations):
 *
 *   coordinator alloc()s a sentinel handle,
 *   coordinator freeze()s the arena,
 *   submit two workers via wl_workqueue_submit:
 *     A. wl_compound_arena_alloc(arena, 16) — must return NULL,
 *     B. wl_compound_arena_gc_epoch_boundary(arena) — must return
 *        current_epoch unchanged (per #561 / #584 docs contract).
 *   wait_all, unfreeze, advance epoch on the coordinator side.
 *
 * The workqueue submit/wait_all pair supplies the happens-before
 * relationship coordinator-freeze -> worker-read; without it TSan
 * would (correctly) flag the non-atomic frozen flag.  If anyone
 * relaxes that synchronisation, this test surfaces the regression
 * before it lands in production code paths.
 */

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/workqueue.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    wl_compound_arena_t *arena;
    /* Coordinator records the unfrozen current_epoch BEFORE the
     * freeze + submit so the gc worker's return can be checked
     * against a stable snapshot.  Without this, a race that
     * advanced the epoch under the freeze guard would be invisible
     * to the assertion side. */
    uint32_t epoch_before_freeze;
    int alloc_denied;
    int gc_no_advance;
} race_task_t;

static void
worker_alloc_fn(void *ctx)
{
    race_task_t *t = (race_task_t *)ctx;
    uint64_t denied = wl_compound_arena_alloc(t->arena, 16u);
    t->alloc_denied = (denied == WL_COMPOUND_HANDLE_NULL);
}

static void
worker_gc_fn(void *ctx)
{
    race_task_t *t = (race_task_t *)ctx;
    /* Frozen-path contract (#584 docs): returns current_epoch unchanged. */
    uint32_t ret = wl_compound_arena_gc_epoch_boundary(t->arena);
    t->gc_no_advance = (ret == t->epoch_before_freeze
        && t->arena->current_epoch == t->epoch_before_freeze);
}

int
main(void)
{
    /* 200 iterations is comfortably below the 4095 epoch cap and runs
     * in well under a second even under TSan instrumentation. */
    static const uint32_t ITERATIONS = 200u;

    printf("test_gc_freeze_alloc_race (Issue #584)\n");
    printf("======================================\n");

    wl_compound_arena_t *arena = wl_compound_arena_create(0xCAFEu, 4096u, 0u);
    if (!arena) {
        printf("FAIL: arena create\n");
        return 1;
    }
    if (ITERATIONS >= arena->max_epochs) {
        printf("FAIL: ITERATIONS=%u exceeds max_epochs=%u\n",
            ITERATIONS, arena->max_epochs);
        wl_compound_arena_free(arena);
        return 1;
    }

    wl_work_queue_t *wq = wl_workqueue_create(2u);
    if (!wq) {
        printf("FAIL: workqueue create\n");
        wl_compound_arena_free(arena);
        return 1;
    }

    race_task_t alloc_task;
    race_task_t gc_task;

    int verdict = 0;

    for (uint32_t r = 0; r < ITERATIONS; r++) {
        /* Coordinator-side alloc seeds a fresh entry every iteration so
         * the gc path has something to (potentially) reclaim — though
         * the freeze guard prevents that in the assertion window. */
        uint64_t handle = wl_compound_arena_alloc(arena, 24u);
        if (handle == WL_COMPOUND_HANDLE_NULL) {
            printf("FAIL: iter %u sentinel alloc returned NULL\n", r);
            verdict = 1;
            break;
        }

        uint32_t epoch_snapshot = arena->current_epoch;
        wl_compound_arena_freeze(arena);

        alloc_task.arena = arena;
        alloc_task.epoch_before_freeze = epoch_snapshot;
        alloc_task.alloc_denied = 0;
        alloc_task.gc_no_advance = 0; /* unused for this task */

        gc_task.arena = arena;
        gc_task.epoch_before_freeze = epoch_snapshot;
        gc_task.alloc_denied = 0; /* unused for this task */
        gc_task.gc_no_advance = 0;

        if (wl_workqueue_submit(wq, worker_alloc_fn, &alloc_task) != 0
            || wl_workqueue_submit(wq, worker_gc_fn, &gc_task) != 0) {
            printf("FAIL: iter %u submit\n", r);
            verdict = 1;
            break;
        }

        if (wl_workqueue_wait_all(wq) != 0) {
            printf("FAIL: iter %u wait_all\n", r);
            verdict = 1;
            break;
        }

        if (!alloc_task.alloc_denied) {
            printf("FAIL: iter %u frozen alloc accepted\n", r);
            verdict = 1;
            break;
        }
        if (!gc_task.gc_no_advance) {
            printf("FAIL: iter %u frozen gc advanced epoch (was %u, "
                "now %u)\n",
                r, epoch_snapshot, arena->current_epoch);
            verdict = 1;
            break;
        }

        wl_compound_arena_unfreeze(arena);
        (void)wl_compound_arena_gc_epoch_boundary(arena);
    }

    wl_workqueue_destroy(wq);
    wl_compound_arena_free(arena);

    if (verdict == 0)
        printf("PASS: %u alloc/gc race iterations under freeze guard\n",
            ITERATIONS);
    return verdict;
}
