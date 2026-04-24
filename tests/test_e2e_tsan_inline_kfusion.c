/*
 * test_e2e_tsan_inline_kfusion.c - TSan stress: K=4 concurrent readers over
 * nested inline compound columns (Issue #534 Task 3)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates zero data races on inline column access when K=4 K-Fusion
 * workers concurrently retrieve nested inline compound values from a
 * shared immutable col_rel_t (Option (iii) "immutable-during-epoch").
 *
 * Schema simulates scope(metadata(t, ts, loc, risk)):
 *   logical[0]: scalar key
 *   logical[1]: inline/4  (t, ts, loc, risk -- the "metadata" compound)
 * Physical layout: 5 slots [key, t, ts, loc, risk]
 *
 * Tests:
 *
 *   test_e2e_tsan_inline_kfusion
 *       K=4 workers each perform STRESS_ITERS (10k) full scans of
 *       STRESS_NROWS rows concurrently.  The col_rel_t column buffer is
 *       frozen after initial population (epoch invariant).  TSan clean
 *       == 0 data races on any inline slot.
 *
 *   test_e2e_tsan_concurrent_insert_delete
 *       100 insert/delete lifecycle cycles each with K=4 concurrent
 *       readers in the read epoch.  Main thread performs:
 *         (populate CYCLE_NROWS rows)
 *         -> (K=4 concurrent reads, relation frozen)
 *         -> (retract all rows: Z-set only, no physical mutation)
 *         -> (destroy relation)
 *       Validates lifecycle correctness under TSan across full epochs.
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/wirelog-types.h"
#include "../wirelog/thread.h"

#include <errno.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Constants                                                                */
/* ======================================================================== */

#define K_WORKERS      4
#define STRESS_NROWS   1000u
#define STRESS_ITERS   10000u    /* iterations per worker in TSan stress    */
#define OUTER_CYCLES   100u      /* insert/delete lifecycle cycles          */
#define CYCLE_NROWS    64u       /* rows per lifecycle cycle                */

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
/* Deterministic value generators for nested compound fields               */
/* ======================================================================== */

/*
 * scope(metadata(t, ts, loc, risk)) encoded as inline/4.
 * All K workers expect identical values for every row — divergence
 * indicates either a torn read or a data race on a shared inline slot.
 */
static inline int64_t meta_t(uint32_t row)    {
    return (int64_t)row * 7;
}
static inline int64_t meta_ts(uint32_t row)   {
    return (int64_t)row * 13;
}
static inline int64_t meta_loc(uint32_t row)  {
    return (int64_t)row * 17;
}
static inline int64_t meta_risk(uint32_t row) {
    return (int64_t)row * 19;
}

/* ======================================================================== */
/* Relation builder                                                         */
/* ======================================================================== */

/*
 * Build a (key:scalar, metadata:inline/4) relation with NROWS rows.
 * Physical columns: [key, t, ts, loc, risk] (5 total).
 * All rows populated with deterministic generator values.
 * The relation is IMMUTABLE after this call (epoch invariant for reads).
 */
static int
build_metadata_relation(col_rel_t **out_rel, const char *name, uint32_t nrows)
{
    col_rel_t *rel = NULL;
    int rc = col_rel_alloc(&rel, name);
    if (rc != 0)
        return rc;

    /* Schema: scalar key + inline/4 compound for metadata args */
    const col_rel_logical_col_t logical[2] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 4u, 1u },
    };
    rc = col_rel_apply_compound_schema(rel, logical, 2u);
    if (rc != 0) {
        col_rel_destroy(rel);
        return rc;
    }
    /* 1 scalar + 4 inline slots = 5 physical columns */
    const char *names[5] = { "key", "t", "ts", "loc", "risk" };
    rc = col_rel_set_schema(rel, 5u, names);
    if (rc != 0) {
        col_rel_destroy(rel);
        return rc;
    }

    /* Pre-allocate all rows in one pass to avoid per-row realloc during the
     * immutable epoch: a mid-read realloc would invalidate pointers workers
     * hold, turning a correctness test into undefined behaviour. */
    const int64_t zero[5] = { 0, 0, 0, 0, 0 };
    for (uint32_t i = 0; i < nrows; i++) {
        rc = col_rel_append_row(rel, zero);
        if (rc != 0) {
            col_rel_destroy(rel);
            return rc;
        }
    }

    /* Populate scope/metadata compound values. After this loop the relation
     * is epoch-frozen: no more writes until all reader threads have joined. */
    for (uint32_t i = 0; i < nrows; i++) {
        col_rel_set(rel, i, 0, (int64_t)i);
        const int64_t args[4] = {
            meta_t(i), meta_ts(i), meta_loc(i), meta_risk(i)
        };
        rc = wl_col_rel_store_inline_compound(rel, i, 1u, args, 4u);
        if (rc != 0) {
            col_rel_destroy(rel);
            return rc;
        }
    }

    *out_rel = rel;
    return 0;
}

/* ======================================================================== */
/* test_e2e_tsan_inline_kfusion                                             */
/* ======================================================================== */

typedef struct {
    const col_rel_t *rel;    /* shared, epoch-frozen (immutable)           */
    uint32_t nrows;
    uint32_t worker_id;
    uint32_t iters;          /* full-scan iterations per worker            */
    uint32_t errors;
} tsan_reader_t;

/*
 * Worker body: STRESS_ITERS full scans of the immutable col_rel_t.
 * All accesses are reads; TSan should report 0 races with concurrent
 * readers because there are no concurrent writers (epoch invariant).
 */
static void *
tsan_reader_main(void *arg)
{
    tsan_reader_t *w = (tsan_reader_t *)arg;
    for (uint32_t it = 0; it < w->iters; it++) {
        for (uint32_t i = 0; i < w->nrows; i++) {
            int64_t args[4] = { 0, 0, 0, 0 };
            int rc = wl_col_rel_retrieve_inline_compound(
                w->rel, i, 1u, args, 4u);
            if (rc != 0
                || args[0] != meta_t(i)
                || args[1] != meta_ts(i)
                || args[2] != meta_loc(i)
                || args[3] != meta_risk(i)) {
                w->errors++;
                /* Continue iterating: a single bad row must not mask
                 * systemic corruption farther in the buffer. */
            }
        }
    }
    return NULL;
}

static void
test_e2e_tsan_inline_kfusion(void)
{
    TEST("TSan: K=4 concurrent readers, 10k iters, nested inline/4 compound");

    col_rel_t    *rel = NULL;
    thread_t threads[K_WORKERS];
    tsan_reader_t workers[K_WORKERS];
    bool thread_created[K_WORKERS];
    memset(thread_created, 0, sizeof(thread_created));

    int rc = build_metadata_relation(&rel, "scope_metadata_stress",
            STRESS_NROWS);
    if (rc != 0) {
        tests_failed++;
        printf(" ... FAIL: relation build rc=%d\n", rc);
        return;
    }

    /* Spawn K=4 concurrent readers.  The relation is now epoch-frozen:
     * no writes will happen until all threads are joined below. */
    for (int w = 0; w < K_WORKERS; w++) {
        workers[w].rel = rel;
        workers[w].nrows = STRESS_NROWS;
        workers[w].worker_id = (uint32_t)(w + 1);
        workers[w].iters = STRESS_ITERS;
        workers[w].errors = 0u;
        int trc = thread_create(&threads[w], tsan_reader_main, &workers[w]);
        ASSERT(trc == 0, "thread_create failed");
        thread_created[w] = true;
    }

    for (int w = 0; w < K_WORKERS; w++) {
        if (thread_created[w]) {
            thread_join(&threads[w]);
            thread_created[w] = false;
        }
    }

    uint32_t total_errors = 0u;
    for (int w = 0; w < K_WORKERS; w++)
        total_errors += workers[w].errors;

    if (total_errors != 0u) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "nested inline/4 read divergence: %u mismatches across K=%d workers",
            total_errors, K_WORKERS);
        FAIL(msg);
    }

    PASS();
cleanup:
    /* Drain threads still in flight (only reachable via FAIL before the
     * happy-path join loop completes). */
    for (int w = 0; w < K_WORKERS; w++) {
        if (thread_created[w])
            thread_join(&threads[w]);
    }
    col_rel_destroy(rel);
}

/* ======================================================================== */
/* test_e2e_tsan_concurrent_insert_delete                                   */
/* ======================================================================== */

typedef struct {
    const col_rel_t *rel;
    uint32_t nrows;
    uint32_t cycle;
    uint32_t errors;
} cycle_reader_t;

static void *
cycle_reader_main(void *arg)
{
    cycle_reader_t *w = (cycle_reader_t *)arg;
    for (uint32_t i = 0; i < w->nrows; i++) {
        int64_t args[4] = { 0, 0, 0, 0 };
        int rc = wl_col_rel_retrieve_inline_compound(w->rel, i, 1u, args, 4u);
        if (rc != 0
            || args[0] != meta_t(i)
            || args[1] != meta_ts(i)
            || args[2] != meta_loc(i)
            || args[3] != meta_risk(i)) {
            w->errors++;
        }
    }
    return NULL;
}

/*
 * OUTER_CYCLES full insert/read/retract/destroy lifecycle cycles.
 *
 * Each cycle:
 *   1. Main thread creates a relation, populates CYCLE_NROWS rows (INSERT).
 *   2. Relation is epoch-frozen; K=4 workers read concurrently (READ EPOCH).
 *   3. Main thread retracts all rows (Z-set delta; physical slots intact).
 *   4. Relation destroyed (epoch end).
 *
 * TSan clean == no torn reads during step 2, no accesses to freed memory
 * in step 4.
 */
static void
test_e2e_tsan_concurrent_insert_delete(void)
{
    TEST(
        "TSan: 100 insert/delete cycles with K=4 concurrent readers per epoch");

    uint32_t total_errors = 0u;
    uint32_t failed_creates = 0u;

    for (uint32_t cycle = 0; cycle < OUTER_CYCLES; cycle++) {
        /* --- INSERT PHASE (sequential main thread) --- */
        col_rel_t *rel = NULL;
        int rc = build_metadata_relation(&rel, "cycle_rel", CYCLE_NROWS);
        if (rc != 0) {
            failed_creates++;
            continue; /* count failures but keep cycling */
        }

        /* --- READ EPOCH (K=4 concurrent workers, relation frozen) --- */
        thread_t threads[K_WORKERS];
        cycle_reader_t readers[K_WORKERS];
        bool created[K_WORKERS];
        memset(created, 0, sizeof(created));

        for (int w = 0; w < K_WORKERS; w++) {
            readers[w].rel = rel;
            readers[w].nrows = CYCLE_NROWS;
            readers[w].cycle = cycle;
            readers[w].errors = 0u;
            int trc = thread_create(&threads[w], cycle_reader_main,
                    &readers[w]);
            if (trc != 0) {
                /* Thread creation failure: clean up what we have. */
                for (int j = 0; j < w; j++) {
                    if (created[j])
                        thread_join(&threads[j]);
                }
                col_rel_destroy(rel);
                failed_creates++;
                goto next_cycle;
            }
            created[w] = true;
        }

        for (int w = 0; w < K_WORKERS; w++) {
            if (created[w])
                thread_join(&threads[w]);
        }
        for (int w = 0; w < K_WORKERS; w++)
            total_errors += readers[w].errors;

        /* --- RETRACT + DESTROY PHASE (sequential, epoch end) --- */
        for (uint32_t i = 0; i < CYCLE_NROWS; i++)
            wl_col_rel_retract_inline_compound(rel, i, 1u, -1);
        col_rel_destroy(rel);

next_cycle:
        (void)0; /* label target */
    }

    if (failed_creates > 0u) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%u cycle(s) failed to create fixture",
            failed_creates);
        FAIL(msg);
    }
    if (total_errors != 0u) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "%u mismatched reads across %u insert/delete cycles",
            total_errors, OUTER_CYCLES);
        FAIL(msg);
    }

    PASS();
    return;
cleanup:
    return; /* FAIL() sets tests_failed and falls through here */
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_e2e_tsan_inline_kfusion (Issue #534 Task 3)\n");
    printf("================================================\n");

    test_e2e_tsan_inline_kfusion();
    test_e2e_tsan_concurrent_insert_delete();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
