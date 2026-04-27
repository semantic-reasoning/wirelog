/*
 * test_stress_harness.c - Issue #594 parameterizable stress harness
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Single test binary that drives one of two K-Fusion arena workloads
 * with parameters supplied via environment variables.  The same
 * binary is registered under multiple meson tests with different
 * `env:` blocks so a single configurable harness backs the three CI
 * tiers #594 calls out:
 *
 *   - stress_harness_w2 (default suite, PR CI):  W=2,  R=200
 *   - stress_harness_w4 (suite: stress-nightly): W=4,  R=500
 *   - stress_harness_w8 (suite: stress-release): W=8,  R=1000
 *
 * Workloads (selected by WL_STRESS_WORKLOAD):
 *
 *   "freeze-cycle" (default):
 *       Verbatim lift of #582's freeze-cycle stress (worker_fn does
 *       lookup() + denied-alloc on the borrowed arena while frozen).
 *       Each cycle: alloc sentinel → freeze → submit W workers →
 *       wait_all → unfreeze → gc_epoch_boundary.
 *
 *   "apply-roundtrip":
 *       Pre-rotation handles must remain valid post-apply (#594
 *       acceptance bullet).  Sequence: allocate R handles, build a
 *       deterministic remap (old → old XOR magic) covering them all,
 *       call wl_handle_remap_apply_columns (#589) to rewrite the
 *       column in place, then verify every cell carries its expected
 *       new value.  This workload does NOT use W workers --
 *       wl_handle_remap_apply_columns is single-mutator by contract;
 *       W is accepted as input but ignored, with a one-line note
 *       printed to keep CI tier wiring uniform across workloads.
 *       (No #550 Option C rotation helper required: the apply pass
 *       rewrites the same arena in place; cross-arena swap is a
 *       separate concern that needs its own sibling test.)
 *
 * The pre-implementation review explicitly scoped this NOT to:
 *   - touch the already-merged #582/#584/#592 hardcoded tests
 *     (refactor-risk for zero feature value),
 *   - add a release.yml workflow (out of scope; the W=8 invocation
 *     is documented in docs/STRESS_BASELINE.md instead).
 *
 * Environment variables:
 *
 *   WL_STRESS_W         unsigned int [1, 64]         (default 2)
 *   WL_STRESS_R         unsigned int [1, 1_000_000]  (default 200)
 *   WL_STRESS_WORKLOAD  one of:                      (default "freeze-cycle")
 *                       "freeze-cycle"
 *                       "apply-roundtrip"
 *
 * The harness-level R cap (1M) is a sanity bound; each workload
 * applies its own tighter validation against domain-specific limits
 * (freeze-cycle requires R < arena->max_epochs because each cycle
 * advances current_epoch; apply-roundtrip is row-count and only
 * bounded by available memory).  Out-of-range / non-numeric W or
 * R, or unknown WORKLOAD, triggers a hard FAIL with a clear
 * diagnostic so a misconfigured CI tier surfaces loudly instead of
 * masquerading as a "passed at default".
 */

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/columnar/handle_remap.h"
#include "../wirelog/columnar/handle_remap_apply.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/workqueue.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ======================================================================== */
/* Env-var parsing                                                          */
/* ======================================================================== */

/* Parse a positive integer from @env_name, clamp to [1, max], FAIL with a
 * diagnostic if the value is missing-and-no-default, non-numeric, zero,
 * or above @max.  @default_value is used iff the env var is unset. */
static int
parse_env_uint(const char *env_name, uint32_t default_value,
    uint32_t max_value, uint32_t *out)
{
    const char *raw = getenv(env_name);
    if (!raw || raw[0] == '\0') {
        *out = default_value;
        return 0;
    }
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || v == 0
        || v > (unsigned long long)max_value) {
        fprintf(stderr,
            "FAIL: %s='%s' is not a positive integer <= %u\n",
            env_name, raw, max_value);
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

/* ======================================================================== */
/* Freeze-cycle workload                                                    */
/* ======================================================================== */

typedef struct {
    wl_compound_arena_t *arena;
    uint64_t sentinel_handle;
    uint32_t expected_payload_size;
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

    /* Frozen alloc must refuse; verifies the freeze guard composes
     * correctly with concurrent worker access. */
    uint64_t denied = wl_compound_arena_alloc(t->arena, 16u);
    t->alloc_blocked = (denied == WL_COMPOUND_HANDLE_NULL);
}

static int
run_freeze_cycle_workload(uint32_t num_workers, uint32_t cycles)
{
    printf("  [freeze-cycle W=%u R=%u] ", num_workers, cycles);
    fflush(stdout);

    wl_compound_arena_t *arena = wl_compound_arena_create(0xCAFEu, 4096u, 0u);
    if (!arena) {
        printf("FAIL: arena create\n");
        return 1;
    }

    /* Bound check: each cycle advances current_epoch by one
     * (gc_epoch_boundary at the cycle tail), so cycles must fit
     * the arena's epoch cap.  Mirrors #582's bound assertion. */
    if (cycles >= arena->max_epochs) {
        printf("FAIL: WL_STRESS_R=%u exceeds max_epochs=%u\n",
            cycles, arena->max_epochs);
        wl_compound_arena_free(arena);
        return 1;
    }

    wl_work_queue_t *wq = wl_workqueue_create(num_workers);
    if (!wq) {
        printf("FAIL: workqueue create (W=%u)\n", num_workers);
        wl_compound_arena_free(arena);
        return 1;
    }

    worker_task_t *tasks
        = (worker_task_t *)calloc(num_workers, sizeof(worker_task_t));
    if (!tasks) {
        printf("FAIL: tasks alloc\n");
        wl_workqueue_destroy(wq);
        wl_compound_arena_free(arena);
        return 1;
    }

    int verdict = 0;
    /* clock() is portable across MSVC/glibc/musl; the diagnostic
     * timing is informational only, not a gate. */
    clock_t t0 = clock();

    for (uint32_t r = 0; r < cycles; r++) {
        uint64_t handle = wl_compound_arena_alloc(arena, 24u);
        if (handle == WL_COMPOUND_HANDLE_NULL) {
            printf("FAIL: cycle %u sentinel alloc returned NULL\n", r);
            verdict = 1;
            break;
        }
        wl_compound_arena_freeze(arena);
        for (uint32_t k = 0; k < num_workers; k++) {
            tasks[k].arena = arena;
            tasks[k].sentinel_handle = handle;
            tasks[k].expected_payload_size = 24u;
            tasks[k].lookup_ok = 0;
            tasks[k].alloc_blocked = 0;
            if (wl_workqueue_submit(wq, worker_fn, &tasks[k]) != 0) {
                printf("FAIL: submit cycle %u worker %u\n", r, k);
                verdict = 1;
                break;
            }
        }
        if (verdict)
            break;
        if (wl_workqueue_wait_all(wq) != 0) {
            printf("FAIL: wait_all cycle %u\n", r);
            verdict = 1;
            break;
        }
        for (uint32_t k = 0; k < num_workers; k++) {
            if (!tasks[k].lookup_ok) {
                printf("FAIL: cycle %u worker %u lookup failed\n", r, k);
                verdict = 1;
                break;
            }
            if (!tasks[k].alloc_blocked) {
                printf("FAIL: cycle %u worker %u frozen alloc accepted\n",
                    r, k);
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

/* ======================================================================== */
/* Apply-roundtrip workload                                                 */
/* ======================================================================== */

/* Deterministic remap transform: keeps a handle non-zero after the
 * rewrite (XOR with a non-zero magic guarantees that), avoids
 * collisions with WL_COMPOUND_HANDLE_NULL, and is invertible by the
 * verification step. */
#define WL_STRESS_NEW_HANDLE_XOR ((int64_t)0xDEADBEEFCAFEBABEull)

static int
run_apply_roundtrip_workload(uint32_t num_workers, uint32_t rows)
{
    /* W is accepted for CI-tier uniformity but ignored: the apply
     * pass is single-mutator by contract (handle_remap_apply.c
     * touches columns[][] under the freeze invariant; concurrent
     * appliers would corrupt each other's prefix). */
    (void)num_workers;
    printf("  [apply-roundtrip W=%u (ignored, single-mutator) R=%u] ",
        num_workers, rows);
    fflush(stdout);

    /* Mirrors the test_handle_remap_apply.c (#589) shape but with
     * a configurable row count.  We use a synthetic col_rel_t
     * directly instead of a session, to keep the harness
     * dependency-light and focused on the apply contract. */
    col_rel_t *rel = NULL;
    if (col_rel_alloc(&rel, "stress_apply_roundtrip") != 0) {
        printf("FAIL: col_rel_alloc\n");
        return 1;
    }
    const char *names[1] = { "h0" };
    if (col_rel_set_schema(rel, 1u, names) != 0) {
        printf("FAIL: set_schema\n");
        col_rel_destroy(rel);
        return 1;
    }
    /* Populate rows: row i carries handle (i + 1), guaranteed
    * non-zero so the apply pass actually scans every cell. */
    int64_t row[1];
    for (uint32_t i = 0; i < rows; i++) {
        row[0] = (int64_t)(i + 1u);
        if (col_rel_append_row(rel, row) != 0) {
            printf("FAIL: append_row at i=%u\n", i);
            col_rel_destroy(rel);
            return 1;
        }
    }

    wl_handle_remap_t *remap = NULL;
    int rc = wl_handle_remap_create((size_t)rows, &remap);
    if (rc != 0 || !remap) {
        printf("FAIL: remap create\n");
        col_rel_destroy(rel);
        return 1;
    }
    for (uint32_t i = 0; i < rows; i++) {
        int64_t old_h = (int64_t)(i + 1u);
        int64_t new_h = old_h ^ WL_STRESS_NEW_HANDLE_XOR;
        if (wl_handle_remap_insert(remap, old_h, new_h) != 0) {
            printf("FAIL: remap_insert i=%u\n", i);
            wl_handle_remap_free(remap);
            col_rel_destroy(rel);
            return 1;
        }
    }

    clock_t t0 = clock();

    uint32_t handle_cols[1] = { 0u };
    uint64_t rewrites = 0;
    rc = wl_handle_remap_apply_columns(rel, handle_cols, 1u, remap,
            &rewrites);
    if (rc != 0) {
        printf("FAIL: apply_columns rc=%d\n", rc);
        wl_handle_remap_free(remap);
        col_rel_destroy(rel);
        return 1;
    }
    if (rewrites != (uint64_t)rows) {
        printf("FAIL: rewrites=%" PRIu64 " expected=%u\n",
            rewrites, rows);
        wl_handle_remap_free(remap);
        col_rel_destroy(rel);
        return 1;
    }

    /* Verification: every cell carries the expected post-apply
     * handle.  Stratified sampling caught no off-by-ones in #589's
     * 10K x 3 acceptance test; full sweep here keeps the
     * configurable harness honest at any R. */
    int verdict = 0;
    for (uint32_t i = 0; i < rows; i++) {
        int64_t got = rel->columns[0][i];
        int64_t want = (int64_t)(i + 1u) ^ WL_STRESS_NEW_HANDLE_XOR;
        if (got != want) {
            printf("FAIL: row %u got=0x%llx want=0x%llx\n",
                i, (unsigned long long)got,
                (unsigned long long)want);
            verdict = 1;
            break;
        }
    }

    clock_t t1 = clock();
    double secs = (double)(t1 - t0) / (double)CLOCKS_PER_SEC;

    wl_handle_remap_free(remap);
    col_rel_destroy(rel);

    if (verdict == 0)
        printf("PASS (%.3fs)\n", secs);
    return verdict;
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_stress_harness (Issue #594)\n");
    printf("================================\n");

    uint32_t W = 0;
    uint32_t R = 0;
    if (parse_env_uint("WL_STRESS_W", 2u, 64u, &W) != 0)
        return 1;
    if (parse_env_uint("WL_STRESS_R", 200u, 1000000u, &R) != 0)
        return 1;

    const char *workload = getenv("WL_STRESS_WORKLOAD");
    if (!workload || workload[0] == '\0')
        workload = "freeze-cycle";

    printf("Configuration: W=%u R=%u workload=%s "
        "(env-driven; defaults W=2 R=200 workload=freeze-cycle)\n",
        W, R, workload);

    int rc;
    if (strcmp(workload, "freeze-cycle") == 0) {
        rc = run_freeze_cycle_workload(W, R);
    } else if (strcmp(workload, "apply-roundtrip") == 0) {
        rc = run_apply_roundtrip_workload(W, R);
    } else {
        fprintf(stderr,
            "FAIL: WL_STRESS_WORKLOAD='%s' is not one of "
            "'freeze-cycle', 'apply-roundtrip'\n", workload);
        return 1;
    }

    if (rc == 0)
        printf("\nWorkload '%s' passed.\n", workload);
    else
        printf("\nFAILURE: workload '%s' failed.\n", workload);
    return rc == 0 ? 0 : 1;
}
