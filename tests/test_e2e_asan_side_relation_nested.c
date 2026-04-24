/*
 * test_e2e_asan_side_relation_nested.c - ASan stress: K=4 workers,
 * nested inline + side-relation arena GC (Issue #534 Task 4)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies zero memory leaks and zero use-after-free across a tight
 * alloc/store/retract/GC lifecycle combining:
 *   - Inline compound storage (col_rel_t inline/2 column for the inner
 *     metadata args encoded as physical inline slots)
 *   - Side-relation arena handle management (wl_compound_arena_t) for
 *     the outer scope() handle and nested metadata() handle
 *
 * K=4 worker threads each own an independent (col_rel_t, arena) pair so
 * there is no cross-worker sharing — the test is a lifecycle stress, not
 * a concurrency test.  ASan detects accumulated leaks on process exit and
 * any use-after-free triggered when the epoch GC reclaims a handle that
 * the inline column still holds as an int64_t key value.
 *
 * Tests:
 *
 *   test_e2e_asan_side_relation_nested
 *       K=4 workers, 2500 cycles each (10k total).  Each cycle:
 *         1. arena_alloc: inner  handle  h_meta  (metadata args payload)
 *         2. arena_alloc: outer  handle  h_scope (scope(h_meta) payload)
 *         3. Store h_meta as inline/2 compound args in a col_rel_t row.
 *         4. Store h_scope as the scalar key of the same row.
 *         5. arena_retain(-1) on both handles -> multiplicity reaches 0.
 *         6. arena_gc_epoch_boundary() reclaims both handles.
 *         7. Verify the inline row is still intact (no physical mutation).
 *       Under ASan: 0 leaks, 0 use-after-free at any cycle or on exit.
 *
 *   test_e2e_asan_arena_freeze_inline
 *       Validates that freezing the arena while an inline relation holds
 *       handle values does not cause a use-after-free when the arena is
 *       later unfrozen and GC'd.  Models the K-Fusion freeze/unfreeze
 *       lifecycle (arena frozen for the duration of parallel evaluation).
 */

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/wirelog-types.h"
#include "../wirelog/thread.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Constants                                                                */
/* ======================================================================== */

#define K_WORKERS          4
#define CYCLES_PER_WORKER  2500u  /* 4 * 2500 = 10k total lifecycle ops    */
#define ARENA_GEN_CAP      512u   /* arena generation capacity in bytes    */

/* Inline schema: key(scalar) + meta_args(inline/2).
 * Simulates scope(metadata(arg0, arg1)) where:
 *   - key column holds the outer scope-handle (int64 handle value)
 *   - inline/2 compound slots hold the two metadata arguments
 * Physical layout: [key, meta_arg0, meta_arg1] = 3 columns. */
#define META_ARITY  2u

/* ======================================================================== */
/* Test harness                                                              */
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
            goto cleanup;                           \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* ======================================================================== */
/* Per-worker state                                                         */
/* ======================================================================== */

typedef struct {
    uint32_t worker_id;
    uint32_t cycles;     /* CYCLES_PER_WORKER                              */
    uint32_t errors;     /* incremented on any unexpected failure          */
    int setup_rc;        /* non-zero if the worker failed to set up        */
} asan_worker_t;

/* ======================================================================== */
/* Worker body                                                              */
/* ======================================================================== */

/*
 * Each worker owns a private col_rel_t + wl_compound_arena_t.
 *
 * Cycle structure (simulates nested scope(metadata(arg0, arg1))):
 *   a) arena_alloc(arena, 8)     -> h_meta  (inner handle: metadata payload)
 *   b) arena_alloc(arena, 8)     -> h_scope (outer handle: scope payload)
 *   c) col_rel_append_row: key=0 placeholder
 *   d) wl_col_rel_store_inline_compound(row=0, logical_col=1, {arg0,arg1})
 *      -> physically stores two metadata args in inline slots
 *   e) col_rel_set(row=0, col=0, h_scope) -> store outer handle as key
 *   f) arena_retain(h_meta,  -1) -> meta multiplicity -> 0
 *   g) arena_retain(h_scope, -1) -> scope multiplicity -> 0
 *   h) arena_gc_epoch_boundary() -> reclaims both handles
 *   i) Verify inline slots still read back correctly (no physical mutation)
 *   j) Reset relation for next cycle (nrows = 0, reuse capacity)
 *
 * After all cycles: destroy arena + relation.
 * ASan detects any leak or use-after-free on the worker thread.
 */
static void *
asan_worker_main(void *arg)
{
    asan_worker_t *w = (asan_worker_t *)arg;

    /* --- Per-worker setup --- */
    col_rel_t *rel = NULL;
    int rc = col_rel_alloc(&rel, "asan_nested");
    if (rc != 0) {
        w->setup_rc = rc;
        return NULL;
    }

    const col_rel_logical_col_t logical[2] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u,         0u },
        { WIRELOG_COMPOUND_KIND_INLINE, META_ARITY, 1u },
    };
    if (col_rel_apply_compound_schema(rel, logical, 2u) != 0
        || col_rel_set_schema(rel, 1u + META_ARITY, NULL) != 0) {
        col_rel_destroy(rel);
        w->setup_rc = -1;
        return NULL;
    }

    /* Use worker_id as session_seed to keep handles unique across workers. */
    wl_compound_arena_t *arena = wl_compound_arena_create(
        w->worker_id, ARENA_GEN_CAP, 0u);
    if (!arena) {
        col_rel_destroy(rel);
        w->setup_rc = ENOMEM;
        return NULL;
    }

    /* --- Cycle loop --- */
    for (uint32_t c = 0; c < w->cycles; c++) {
        /* a) Allocate inner handle (metadata payload, 8 bytes). */
        uint64_t h_meta = wl_compound_arena_alloc(arena, 8u);
        if (h_meta == WL_COMPOUND_HANDLE_NULL) {
            /* Arena generation exhausted — start a fresh arena to continue
            * stress-testing.  The old arena is freed cleanly (0 leaks). */
            wl_compound_arena_free(arena);
            arena = wl_compound_arena_create(w->worker_id, ARENA_GEN_CAP, 0u);
            if (!arena) {
                w->errors++;
                break;
            }
            h_meta = wl_compound_arena_alloc(arena, 8u);
            if (h_meta == WL_COMPOUND_HANDLE_NULL) {
                w->errors++;
                continue;
            }
        }

        /* b) Allocate outer handle (scope payload, 8 bytes). */
        uint64_t h_scope = wl_compound_arena_alloc(arena, 8u);
        if (h_scope == WL_COMPOUND_HANDLE_NULL) {
            /* Arena full mid-cycle: retain + GC to free h_meta then retry. */
            wl_compound_arena_retain(arena, h_meta, -1);
            wl_compound_arena_gc_epoch_boundary(arena);
            continue;
        }

        /* c) Ensure relation has exactly one working row. */
        if (rel->nrows == 0u) {
            const int64_t zero[3] = { 0 };
            if (col_rel_append_row(rel, zero) != 0) {
                w->errors++;
                wl_compound_arena_retain(arena, h_meta,  -1);
                wl_compound_arena_retain(arena, h_scope, -1);
                wl_compound_arena_gc_epoch_boundary(arena);
                continue;
            }
        }

        /* d) Store inline/2 compound (metadata args). */
        const int64_t meta_args[2] = {
            (int64_t)(c * 3u + w->worker_id),
            (int64_t)(c * 5u + w->worker_id),
        };
        if (wl_col_rel_store_inline_compound(rel, 0u, 1u,
            meta_args, META_ARITY) != 0) {
            w->errors++;
        }

        /* e) Store outer scope handle in the key column. */
        col_rel_set(rel, 0u, 0u, (int64_t)h_scope);

        /* f-g) Retract both handles (multiplicity -> 0). */
        wl_compound_arena_retain(arena, h_meta,  -1);
        wl_compound_arena_retain(arena, h_scope, -1);

        /* h) GC reclaims both handles.  After this point h_meta and
         *    h_scope are invalid arena handles.  The inline slots in rel
         *    still hold the int64_t values written in step (d) — they are
         *    NOT arena pointers and must NOT be dereferenced via arena API.
         *    ASan verifies no arena memory is accessed after GC. */
        wl_compound_arena_gc_epoch_boundary(arena);

        /* i) Inline slots must be physically unchanged after GC.
         *    GC never touches col_rel_t column buffers. */
        int64_t got[2] = { 0, 0 };
        if (wl_col_rel_retrieve_inline_compound(rel, 0u, 1u, got,
            META_ARITY) != 0
            || got[0] != meta_args[0]
            || got[1] != meta_args[1]) {
            w->errors++;
        }

        /* j) Reset row count for next cycle (reuse buffer). */
        rel->nrows = 0u;
    }

    /* --- Teardown (ASan checks for leaks here) --- */
    wl_compound_arena_free(arena);
    col_rel_destroy(rel);
    return NULL;
}

/* ======================================================================== */
/* test_e2e_asan_side_relation_nested                                       */
/* ======================================================================== */

static void
test_e2e_asan_side_relation_nested(void)
{
    TEST("ASan: K=4 workers, 2500 cycles each, nested inline + arena GC");

    thread_t threads[K_WORKERS];
    asan_worker_t workers[K_WORKERS];
    bool thread_created[K_WORKERS];
    memset(thread_created, 0, sizeof(thread_created));

    for (int w = 0; w < K_WORKERS; w++) {
        workers[w].worker_id = (uint32_t)(w + 1);
        workers[w].cycles = CYCLES_PER_WORKER;
        workers[w].errors = 0u;
        workers[w].setup_rc = 0;
        int trc = thread_create(&threads[w], asan_worker_main, &workers[w]);
        ASSERT(trc == 0, "thread_create failed");
        thread_created[w] = true;
    }

    for (int w = 0; w < K_WORKERS; w++) {
        if (thread_created[w]) {
            thread_join(&threads[w]);
            thread_created[w] = false;
        }
    }

    /* Check that all workers set up successfully. */
    for (int w = 0; w < K_WORKERS; w++) {
        if (workers[w].setup_rc != 0) {
            char msg[128];
            snprintf(msg, sizeof(msg),
                "worker %d failed setup: rc=%d", w + 1, workers[w].setup_rc);
            FAIL(msg);
        }
    }

    uint32_t total_errors = 0u;
    for (int w = 0; w < K_WORKERS; w++)
        total_errors += workers[w].errors;

    if (total_errors != 0u) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "ASan stress: %u errors across K=%d workers (%u cycles each)",
            total_errors, K_WORKERS, CYCLES_PER_WORKER);
        FAIL(msg);
    }

    PASS();
cleanup:
    for (int w = 0; w < K_WORKERS; w++) {
        if (thread_created[w])
            thread_join(&threads[w]);
    }
}

/* ======================================================================== */
/* test_e2e_asan_arena_freeze_inline                                        */
/* ======================================================================== */

/*
 * Models K-Fusion arena freeze/unfreeze with concurrent inline access:
 *
 *   1. Alloc 4 handles into the arena.
 *   2. Store handles as key values in inline relation rows.
 *   3. arena_freeze(): arena is now read-only (K-Fusion parallel epoch).
 *   4. Verify inline slots are intact (frozen arena must not corrupt them).
 *   5. arena_unfreeze(): K-Fusion parallel epoch ends.
 *   6. arena_retain(-1) all handles.
 *   7. arena_gc_epoch_boundary(): reclaim all handles.
 *   8. col_rel_destroy(): release inline row memory.
 *
 * Under ASan: step 8 must not reference any arena memory (use-after-free).
 */
static void
test_e2e_asan_arena_freeze_inline(void)
{
    TEST("ASan: arena freeze/unfreeze does not corrupt nested inline slots");

    col_rel_t           *rel = NULL;
    wl_compound_arena_t *arena = NULL;

    int rc = col_rel_alloc(&rel, "freeze_inline");
    ASSERT(rc == 0 && rel, "col_rel_alloc failed");

    const col_rel_logical_col_t logical[2] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u,         0u },
        { WIRELOG_COMPOUND_KIND_INLINE, META_ARITY, 1u },
    };
    ASSERT(col_rel_apply_compound_schema(rel, logical, 2u) == 0,
        "apply_compound_schema");
    ASSERT(col_rel_set_schema(rel, 1u + META_ARITY, NULL) == 0,
        "set_schema");

    arena = wl_compound_arena_create(0xABu, ARENA_GEN_CAP, 0u);
    ASSERT(arena != NULL, "arena_create");

    /* Alloc 4 handles; store as inline rows. */
    const uint32_t NHANDLES = 4u;
    uint64_t handles[4];
    for (uint32_t i = 0; i < NHANDLES; i++) {
        handles[i] = wl_compound_arena_alloc(arena, 8u);
        ASSERT(handles[i] != WL_COMPOUND_HANDLE_NULL, "arena_alloc");

        const int64_t zero[3] = { 0 };
        ASSERT(col_rel_append_row(rel, zero) == 0, "append_row");
        col_rel_set(rel, i, 0u, (int64_t)handles[i]);
        const int64_t args[2] = { (int64_t)i * 11, (int64_t)i * 31 };
        ASSERT(wl_col_rel_store_inline_compound(rel, i, 1u, args, META_ARITY)
            == 0, "store_inline");
    }

    /* Freeze: simulates the start of a K-Fusion parallel epoch. */
    wl_compound_arena_freeze(arena);

    /* Verify inline slots are unchanged under freeze. */
    for (uint32_t i = 0; i < NHANDLES; i++) {
        int64_t got[2] = { 0, 0 };
        ASSERT(wl_col_rel_retrieve_inline_compound(rel, i, 1u, got,
            META_ARITY) == 0, "retrieve during freeze");
        ASSERT(got[0] == (int64_t)i * 11, "arg0 corrupted during freeze");
        ASSERT(got[1] == (int64_t)i * 31, "arg1 corrupted during freeze");
    }

    /* Verify arena rejects new allocations while frozen. */
    ASSERT(wl_compound_arena_alloc(arena, 8u) == WL_COMPOUND_HANDLE_NULL,
        "alloc during freeze should return NULL handle");

    /* Unfreeze: parallel epoch ends. */
    wl_compound_arena_unfreeze(arena);

    /* Retract + GC all handles. */
    for (uint32_t i = 0; i < NHANDLES; i++)
        wl_compound_arena_retain(arena, handles[i], -1);
    uint32_t reclaimed = wl_compound_arena_gc_epoch_boundary(arena);
    ASSERT(reclaimed == NHANDLES, "gc should reclaim all 4 handles");

    /* Destroy relation: must not chase any arena pointer. */
    col_rel_destroy(rel);
    rel = NULL;
    wl_compound_arena_free(arena);
    arena = NULL;

    PASS();
cleanup:
    if (rel)   col_rel_destroy(rel);
    if (arena) wl_compound_arena_free(arena);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_e2e_asan_side_relation_nested (Issue #534 Task 4)\n");
    printf("=======================================================\n");

    test_e2e_asan_side_relation_nested();
    test_e2e_asan_arena_freeze_inline();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
