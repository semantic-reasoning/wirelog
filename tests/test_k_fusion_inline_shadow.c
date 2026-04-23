/*
 * test_k_fusion_inline_shadow.c - K-Fusion deep-copy semantics over inline
 * compounds (Issue #532 Task 4)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates the Option (iii) "immutable-during-epoch" K-fusion isolation
 * strategy described in docs/ARCHITECTURE.md §4 "K-Fusion Parallelism".
 *
 * The contract under test:
 *
 *   - col_rel_t column buffers holding inline compound values are
 *     written once (epoch setup) and read-only during the epoch.
 *   - col_diff_arrangement_deep_copy gives each of K workers an
 *     independent arrangement (own ht_head / ht_next / key_cols)
 *     indexing the same immutable column buffers.
 *   - No writer coordination is therefore required across workers
 *     during the read phase (§4: "no cross-worker synchronization").
 *
 * Tests:
 *
 *   test_k_fusion_inline_shadow
 *       100k rows with an inline/2 compound are populated sequentially.
 *       K=4 workers, each using its own deep-copied arrangement, read
 *       every row's compound through wl_col_rel_retrieve_inline_compound
 *       concurrently and assert every (row, args) pair matches the
 *       deterministic generator. TSan clean == no races on the shared
 *       column buffers.
 *
 *   test_worker_isolation
 *       Two deep-copied arrangements; mutating worker-A state
 *       (reset_delta, ensure_ht_capacity) must leave worker-B state
 *       untouched — confirming the copies do not alias ht_head/ht_next.
 */

#include "../wirelog/columnar/diff_arrangement.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/wirelog-types.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define K_WORKERS 4
#define SHADOW_NROWS 100000u

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

/* Deterministic generator — every worker expects the same values for
 * every row, so divergence proves either visibility loss or a race. */
static inline int64_t
shadow_arg0(uint32_t row)
{
    return (int64_t)row * 10;
}
static inline int64_t
shadow_arg1(uint32_t row)
{
    return (int64_t)row * 100;
}

/* Build (scalar, inline/2) relation, apply compound schema, materialise
 * physical schema of 3 slots, and populate SHADOW_NROWS rows with the
 * deterministic generator. The returned relation is immutable for the
 * rest of the test run (Option (iii) epoch invariant). */
static int
build_shadow_relation(col_rel_t **out_rel, uint32_t nrows)
{
    col_rel_t *r = NULL;
    int rc = col_rel_alloc(&r, "k_fusion_shadow");
    if (rc != 0)
        return rc;

    const col_rel_logical_col_t logical[2] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
    };
    rc = col_rel_apply_compound_schema(r, logical, 2u);
    if (rc != 0) {
        col_rel_destroy(r);
        return rc;
    }
    rc = col_rel_set_schema(r, 3u, NULL); /* 1 + 2 physical slots */
    if (rc != 0) {
        col_rel_destroy(r);
        return rc;
    }

    /* Seed capacity in one shot to avoid realloc thrash on 100k rows. */
    for (uint32_t i = 0; i < nrows; i++) {
        const int64_t seed_row[3] = { (int64_t)i, 0, 0 };
        rc = col_rel_append_row(r, seed_row);
        if (rc != 0) {
            col_rel_destroy(r);
            return rc;
        }
        const int64_t args[2] = { shadow_arg0(i), shadow_arg1(i) };
        rc = wl_col_rel_store_inline_compound(r, i, 1u, args, 2u);
        if (rc != 0) {
            col_rel_destroy(r);
            return rc;
        }
    }
    *out_rel = r;
    return 0;
}

/* ======================================================================== */
/* test_k_fusion_inline_shadow                                              */
/* ======================================================================== */

typedef struct {
    col_diff_arrangement_t *arr;
    const col_rel_t *rel;
    uint32_t nrows;
    uint32_t worker_id;
    uint32_t errors;
} shadow_worker_t;

/* Worker body: every worker reads the full row range concurrently.  The
 * col_rel_t column buffers are the shared immutable substrate (Option
 * (iii)); the deep-copied arrangement is the worker-private indexing
 * scaffolding. */
static void *
shadow_worker_main(void *arg)
{
    shadow_worker_t *w = (shadow_worker_t *)arg;
    /* Spot-check the arrangement survived the deep-copy with its
     * worker_id intact before we hammer the column buffers. */
    if (w->arr == NULL || w->arr->worker_id != w->worker_id) {
        w->errors++;
        return NULL;
    }
    for (uint32_t i = 0; i < w->nrows; i++) {
        int64_t args[2] = { 0, 0 };
        int rc = wl_col_rel_retrieve_inline_compound(w->rel, i, 1u,
                args, 2u);
        if (rc != 0 || args[0] != shadow_arg0(i)
            || args[1] != shadow_arg1(i)) {
            w->errors++;
            /* Keep iterating so a single bad row does not hide systemic
             * corruption farther in. */
        }
    }
    return NULL;
}

static void
test_k_fusion_inline_shadow(void)
{
    TEST("K=4 deep-copy preserves inline compound visibility over 100k rows");
    col_rel_t *rel = NULL;
    col_diff_arrangement_t *master = NULL;
    col_diff_arrangement_t *copies[K_WORKERS] = { NULL };
    pthread_t threads[K_WORKERS];
    shadow_worker_t workers[K_WORKERS] = { 0 };
    bool thread_created[K_WORKERS] = { false };

    int rc = build_shadow_relation(&rel, SHADOW_NROWS);
    if (rc != 0) {
        tests_failed++;
        printf(" ... FAIL: fixture rc=%d\n", rc);
        return;
    }

    /* The scalar id column (physical col 0) is the join key for this
     * arrangement.  worker_id == 0 on the master — each copy stamps its
     * own id below. */
    const uint32_t key_cols[1] = { 0u };
    master = col_diff_arrangement_create(key_cols, 1u, 0u);
    ASSERT(master != NULL, "arrangement_create failed");
    master->current_nrows = SHADOW_NROWS;

    for (int w = 0; w < K_WORKERS; w++) {
        copies[w] = col_diff_arrangement_deep_copy(master);
        ASSERT(copies[w] != NULL, "deep_copy returned NULL");
        copies[w]->worker_id = (uint32_t)(w + 1);

        /* Deep-copy must not alias ht_head or ht_next with the master. */
        ASSERT(copies[w]->ht_head != master->ht_head,
            "ht_head pointer aliases master");
        ASSERT(copies[w]->ht_next != master->ht_next,
            "ht_next pointer aliases master");
    }

    for (int w = 0; w < K_WORKERS; w++) {
        workers[w].arr = copies[w];
        workers[w].rel = rel;
        workers[w].nrows = SHADOW_NROWS;
        workers[w].worker_id = (uint32_t)(w + 1);
        workers[w].errors = 0;
        int trc = pthread_create(&threads[w], NULL, shadow_worker_main,
                &workers[w]);
        ASSERT(trc == 0, "pthread_create failed");
        thread_created[w] = true;
    }

    for (int w = 0; w < K_WORKERS; w++) {
        if (thread_created[w]) {
            pthread_join(threads[w], NULL);
            thread_created[w] = false; /* avoid double-join in cleanup */
        }
    }

    uint32_t total_errors = 0;
    for (int w = 0; w < K_WORKERS; w++)
        total_errors += workers[w].errors;
    if (total_errors != 0u) {
        char msg[96];
        snprintf(msg, sizeof(msg),
            "cross-worker divergence: %u mismatched reads", total_errors);
        FAIL(msg);
    }

    PASS();
cleanup:
    /* Drain any threads still in flight (only reachable via a FAIL that
     * jumps here before the happy-path join loop runs). */
    for (int w = 0; w < K_WORKERS; w++) {
        if (thread_created[w])
            pthread_join(threads[w], NULL);
    }
    for (int w = 0; w < K_WORKERS; w++)
        col_diff_arrangement_destroy(copies[w]);
    col_diff_arrangement_destroy(master);
    col_rel_destroy(rel);
}

/* ======================================================================== */
/* test_worker_isolation                                                    */
/* ======================================================================== */

static void
test_worker_isolation(void)
{
    TEST("deep-copy isolates per-worker arrangement state");
    col_diff_arrangement_t *master = NULL;
    col_diff_arrangement_t *a = NULL;
    col_diff_arrangement_t *b = NULL;

    const uint32_t key_cols[1] = { 0u };
    master = col_diff_arrangement_create(key_cols, 1u, 0u);
    ASSERT(master != NULL, "arrangement_create failed");
    master->current_nrows = 128u;
    master->base_nrows = 0u;

    a = col_diff_arrangement_deep_copy(master);
    b = col_diff_arrangement_deep_copy(master);
    ASSERT(a != NULL && b != NULL, "deep_copy returned NULL");
    ASSERT(a != b && a->ht_head != b->ht_head && a->ht_next != b->ht_next,
        "worker copies share state");
    a->worker_id = 1u;
    b->worker_id = 2u;

    /* Mutate worker A: reset_delta rolls base_nrows up to current_nrows. */
    col_diff_arrangement_reset_delta(a);
    ASSERT(a->base_nrows == 128u, "reset_delta did not update worker A");
    ASSERT(b->base_nrows == 0u,
        "worker B base_nrows leaked from worker A mutation");

    /* Mutate worker A: force a hash-table grow, which reallocates
     * ht_next.  Worker B's ht_next capacity must stay at the initial
     * 1024-bucket default. */
    uint32_t b_cap_before = b->ht_cap;
    int rc = col_diff_arrangement_ensure_ht_capacity(a, 4096u);
    ASSERT(rc == 0, "ensure_ht_capacity rc");
    ASSERT(a->ht_cap >= 4096u, "worker A ht_cap did not grow");
    ASSERT(b->ht_cap == b_cap_before,
        "worker B ht_cap perturbed by worker A growth");

    /* Identity sanity: worker_id survives the deep copy unmodified. */
    ASSERT(a->worker_id == 1u && b->worker_id == 2u,
        "worker_id corrupted");

    PASS();
cleanup:
    col_diff_arrangement_destroy(a);
    col_diff_arrangement_destroy(b);
    col_diff_arrangement_destroy(master);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_k_fusion_inline_shadow (Issue #532 Task 4)\n");
    printf("===============================================\n");

    test_k_fusion_inline_shadow();
    test_worker_isolation();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
