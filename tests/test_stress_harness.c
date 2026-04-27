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
 *   "nested-asan" (#595):
 *       Session-level side-relation churn under ASan instrumentation.
 *       Builds a mock columnar session with N=100 __compound_*
 *       side-relations (depth-2 nesting: 50 of them carry an inner
 *       handle in column 1 that points at an outer handle of another
 *       relation, exercising the f(g(...)) shape from
 *       handle_remap_apply_side.h:18-24).  For R cycles, each cycle:
 *       (1) allocates outer + nested handles via the compound arena,
 *       appending three rows per relation with distinct timestamps[]
 *       payloads so the multiplicity hash spans non-trivial buffer
 *       content, (2) hashes per-rel timestamps[] (Z-set multiplicity),
 *       (3) calls wl_handle_remap_apply_session_side_relations with a
 *       deterministic XOR remap covering every live handle -- this
 *       driver sweeps column 0 only (handle_remap_apply_side.h:99-109)
 *       and is what rewrites the row's own handle column, (4) for the
 *       50 nested-half relations, drives the per-relation primitive
 *       wl_handle_remap_apply_side_relation explicitly with column
 *       index 1 so the inner-handle column is rewritten end-to-end,
 *       genuinely exercising the depth-2 nested rewrite, (5) re-hashes
 *       timestamps[] and asserts byte equality (#590 contract: apply
 *       pass never touches Z-set weights), (6) invokes
 *       wl_handle_remap_invalidate_side_relation_caches to exercise
 *       the #591 free-then-NULL dedup_slots invalidation path, (7)
 *       drops multiplicity to zero with arena_retain(-1), and
 *       (8) calls arena_gc_epoch_boundary so the column-buffer code
 *       sees retired handles.  Single-mutator like apply-roundtrip,
 *       so W is accepted but ignored.  Coverage delta vs. existing
 *       ASan tests: #534 stresses inline col_rel_t storage (no
 *       session, K=4 worker pairs), #590 validates the apply
 *       primitives functionally (no GC churn).  This workload is the
 *       only path that tracks ASan accounting through the full
 *       session-level register/apply/invalidate/GC loop and the only
 *       harness path that drives the nested-arg primitive at depth 2.
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
 *                       "nested-asan"
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
#include "../wirelog/columnar/compound_side.h"
#include "../wirelog/columnar/delta_pool.h"
#include "../wirelog/columnar/handle_remap.h"
#include "../wirelog/columnar/handle_remap_apply.h"
#include "../wirelog/columnar/handle_remap_apply_side.h"
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
/* Nested-asan workload (#595)                                              */
/* ======================================================================== */

/* Number of side-relations registered on the mock session.  Mirrors the
 * 100-side-relations workload spec from #595 ("Workload: 100 side-
 * relations, delete+insert cycles"). */
#define WL_NESTED_ASAN_N_RELS    100u

/* Half of the relations carry a nested handle (depth-2 nesting per
 * handle_remap_apply_side.h:18-24).  The other half carry a NULL nested
 * arg slot to keep the multiplicity-preservation assertion symmetric
 * across "shallow" and "nested" rows. */
#define WL_NESTED_ASAN_NESTED_HALF (WL_NESTED_ASAN_N_RELS / 2u)

/* Per-handle payload size.  The arena rounds to 8-byte alignment, so
 * 16 bytes is a clean two-word reservation that exercises the entry
 * tracking arrays without bloating the per-cycle generation buffer. */
#define WL_NESTED_ASAN_PAYLOAD_SZ 16u

/* Rows appended per relation per cycle.  >1 is required so the
 * timestamps[] hash spans non-trivial buffer content: with one row
 * the apply pass never writes to that single slot anyway (it only
 * touches columns[][]), so byte-equality is trivially preserved by
 * construction even if the contract were broken.  Three rows with
 * distinct per-row timestamps[] payloads make the assertion span
 * 3 * sizeof(col_delta_timestamp_t) = 96 bytes per relation, so a
 * single byte flip anywhere in any row's slot diverges the hash. */
#define WL_NESTED_ASAN_ROWS_PER_REL 3u

/* Per-cycle hard cap for R: each cycle bumps current_epoch via
 * gc_epoch_boundary, and the brief calls out leaving "epoch headroom"
 * so the arena does not exhaust mid-test.  WL_COMPOUND_EPOCH_MAX is
 * 4095, so 1500 leaves >2x headroom. */
#define WL_NESTED_ASAN_R_CAP      1500u

/* Hash R rows of timestamps[] into a single 64-bit fingerprint.  The
 * mix avalanches every byte of every col_delta_timestamp_t entry, so
 * a single bit flip anywhere in the table changes the digest -- which
 * is the post-condition the #590 contract guarantees ("apply pass
 * never touches col_rel_t::timestamps[]").  NULL timestamps[] hashes
 * to 0; a relation with allocated-but-zeroed timestamps and a relation
 * with no timestamps therefore hash differently, which is the intent
 * (we want to catch a free-and-replace bug, not just a bit flip). */
static uint64_t
nested_asan_hash_timestamps(const col_rel_t *rel)
{
    if (!rel || !rel->timestamps || rel->nrows == 0u)
        return 0u;
    /* SplitMix64 mixer applied byte-by-byte over the timestamps[]
     * buffer.  Constants borrowed from the wider codebase's PRNG
     * (handle_remap.c #562) for consistency. */
    const uint8_t *p = (const uint8_t *)rel->timestamps;
    size_t n = (size_t)rel->nrows * sizeof(col_delta_timestamp_t);
    uint64_t h = 0x9E3779B97F4A7C15ull;
    for (size_t i = 0; i < n; i++) {
        h ^= (uint64_t)p[i];
        h *= 0xBF58476D1CE4E5B9ull;
        h ^= h >> 27;
    }
    return h;
}

/* Allocate a session-shaped struct with just enough wiring for
 * wl_compound_side_ensure / wl_handle_remap_apply_session_side_relations
 * / wl_handle_remap_invalidate_side_relation_caches to operate.  The
 * full session pipeline (parser/optimizer/plan) would pull half the
 * library into this binary's link line for no incremental coverage --
 * the mock-session pattern (test_diff_consolidate.c) is the canonical
 * way to keep the harness dependency-light. */
static wl_col_session_t *
nested_asan_make_mock_session(wl_compound_arena_t *arena)
{
    wl_col_session_t *s = (wl_col_session_t *)calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->frontier_ops = &col_frontier_epoch_ops;
    s->delta_pool = delta_pool_create(256, sizeof(col_rel_t), 1024 * 1024);
    if (!s->delta_pool) {
        free(s);
        return NULL;
    }
    wl_mem_ledger_init(&s->mem_ledger, 0);
    s->compound_arena = arena;
    return s;
}

static void
nested_asan_free_mock_session(wl_col_session_t *s)
{
    if (!s)
        return;
    /* Walk rels[] and destroy each registered side-relation.  Mirrors
     * col_session_destroy's per-rel free loop without pulling the
     * full session lifecycle. */
    for (uint32_t i = 0; i < s->nrels; i++) {
        if (s->rels[i])
            col_rel_destroy(s->rels[i]);
    }
    free(s->rels);
    /* The session-level rel hash is lazy-built by session_add_rel; if
     * it ever populated, free it. */
    session_rel_free_hash(s);
    delta_pool_destroy(s->delta_pool);
    /* compound_arena is owned by the workload, freed separately. */
    free(s);
}

/* Deterministic remap transform identical to apply-roundtrip's, so
 * any reader cross-reading the two workloads sees the same magic. */
#define WL_NESTED_ASAN_HANDLE_XOR ((int64_t)0xDEADBEEFCAFEBABEull)

static int
run_nested_asan_workload(uint32_t num_workers, uint32_t cycles)
{
    /* W is accepted for CI-tier uniformity but ignored: every API
     * touched here (compound arena, side-relation row append, apply
     * pass) is single-mutator by contract.  Same rationale as
     * apply-roundtrip; see that workload's note above. */
    (void)num_workers;
    printf("  [nested-asan W=%u (ignored, single-mutator) R=%u] ",
        num_workers, cycles);
    fflush(stdout);

    /* ASan diagnostic: the workload exercises the same code paths
     * regardless of instrumentation, but only build-san surfaces
     * leaks/UAF.  Emit a one-line warning on stderr if the developer
     * is running outside the sanitizer build so a green PASS is not
     * misread as "ASan-clean". */
    if (getenv("ASAN_OPTIONS") == NULL) {
        fprintf(stderr,
            "  note: ASAN_OPTIONS not set -- this run only exercises "
            "the workload functionally; rerun under build-san for the "
            "leak/UAF coverage that #595 calls out.\n");
    }

    /* Bound check: each cycle bumps the arena epoch via
     * gc_epoch_boundary, and the workload allocates 1.5x the
     * relation count per cycle (outer + half-nested).  We cap R well
     * below the arena epoch ceiling so the arena does not exhaust
     * mid-test.  Mirrors freeze-cycle's bound assertion. */
    if (cycles == 0u || cycles > WL_NESTED_ASAN_R_CAP) {
        printf("FAIL: WL_STRESS_R=%u out of range (1..%u for nested-asan)\n",
            cycles, WL_NESTED_ASAN_R_CAP);
        return 1;
    }

    wl_compound_arena_t *arena = wl_compound_arena_create(0xCAFEu, 4096u, 0u);
    if (!arena) {
        printf("FAIL: arena create\n");
        return 1;
    }
    if ((uint64_t)cycles * 2u >= (uint64_t)arena->max_epochs) {
        printf("FAIL: WL_STRESS_R=%u * 2 >= max_epochs=%u\n",
            cycles, arena->max_epochs);
        wl_compound_arena_free(arena);
        return 1;
    }

    wl_col_session_t *sess = nested_asan_make_mock_session(arena);
    if (!sess) {
        printf("FAIL: mock session create\n");
        wl_compound_arena_free(arena);
        return 1;
    }

    /* Register N=100 side-relations.  Schema is (handle, arg0): one
     * handle column (compulsory column 0 per #580) plus one nested-
     * arg slot.  Functor names are deterministic so the
     * __compound_<functor>_<arity> name predicate catches every
     * relation in the apply/invalidate sweeps. */
    col_rel_t **rels = (col_rel_t **)calloc(WL_NESTED_ASAN_N_RELS,
            sizeof(col_rel_t *));
    if (!rels) {
        printf("FAIL: rels[] calloc\n");
        nested_asan_free_mock_session(sess);
        wl_compound_arena_free(arena);
        return 1;
    }
    int verdict = 0;
    for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
        char functor[24];
        snprintf(functor, sizeof(functor), "functor_%u", i);
        int rc = wl_compound_side_ensure(sess, functor, 1u, &rels[i]);
        if (rc != 0 || !rels[i]) {
            printf("FAIL: side_ensure i=%u rc=%d\n", i, rc);
            verdict = 1;
            goto cleanup;
        }
    }

    clock_t t0 = clock();

    for (uint32_t r = 0; r < cycles; r++) {
        /* (1) Insert phase.  WL_NESTED_ASAN_ROWS_PER_REL outer handles
         * per relation, each carrying a distinct value in column 0;
         * the first WL_NESTED_ASAN_NESTED_HALF relations also get
         * matching inner handles in column 1 so the depth-2 apply
         * sweep below has cells to rewrite.  Track per-row handles
         * so the delete phase can drop multiplicity exactly. */
        uint64_t outer_h[WL_NESTED_ASAN_N_RELS][WL_NESTED_ASAN_ROWS_PER_REL];
        uint64_t inner_h[WL_NESTED_ASAN_N_RELS][WL_NESTED_ASAN_ROWS_PER_REL];
        int has_inner[WL_NESTED_ASAN_N_RELS];
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            has_inner[i] = (i < WL_NESTED_ASAN_NESTED_HALF) ? 1 : 0;
            for (uint32_t k = 0; k < WL_NESTED_ASAN_ROWS_PER_REL; k++) {
                outer_h[i][k] = wl_compound_arena_alloc(arena,
                        WL_NESTED_ASAN_PAYLOAD_SZ);
                inner_h[i][k] = WL_COMPOUND_HANDLE_NULL;
                if (outer_h[i][k] == WL_COMPOUND_HANDLE_NULL) {
                    printf("FAIL: cycle %u rel %u row %u outer alloc\n",
                        r, i, k);
                    verdict = 1;
                    goto cleanup;
                }
                if (has_inner[i]) {
                    inner_h[i][k] = wl_compound_arena_alloc(arena,
                            WL_NESTED_ASAN_PAYLOAD_SZ);
                    if (inner_h[i][k] == WL_COMPOUND_HANDLE_NULL) {
                        printf("FAIL: cycle %u rel %u row %u inner alloc\n",
                            r, i, k);
                        verdict = 1;
                        goto cleanup;
                    }
                }
                int64_t row[2] = {
                    (int64_t)outer_h[i][k],
                    (int64_t)inner_h[i][k],
                };
                if (col_rel_append_row(rels[i], row) != 0) {
                    printf("FAIL: cycle %u rel %u row %u append_row\n",
                        r, i, k);
                    verdict = 1;
                    goto cleanup;
                }
                /* Lazy-allocate timestamps[] sized to the relation's
                 * data capacity (NOT nrows): col_rel_append_row's
                 * grow path realloc's timestamps to capacity bytes
                 * whenever it grows the data buffer, and writes to
                 * timestamps[nrows] up to the capacity bound on every
                 * append.  Sizing to nrows would leave the slot for
                 * the NEXT row out-of-bounds and corrupt the heap.
                 * Once timestamps is non-NULL, append_row keeps it
                 * in sync; we only need to seed it on the first row
                 * of the relation. */
                if (rels[i]->timestamps == NULL && rels[i]->capacity > 0u) {
                    rels[i]->timestamps = (col_delta_timestamp_t *)calloc(
                        rels[i]->capacity, sizeof(col_delta_timestamp_t));
                    if (!rels[i]->timestamps) {
                        printf(
                            "FAIL: cycle %u rel %u row %u timestamps alloc\n",
                            r, i, k);
                        verdict = 1;
                        goto cleanup;
                    }
                }
                /* Distinct per-row timestamps[] payload so the post-
                 * apply byte-equality assertion spans non-trivial
                 * buffer content.  No public setter exists for
                 * col_delta_timestamp_t (columnar_nanoarrow.h:168);
                 * direct field writes match relation.c's
                 * append-path zeroing convention.  Pack (cycle, rel,
                 * row) into the int64 multiplicity field so a
                 * single bit flip anywhere in any of the three
                 * row slots diverges the hash. */
                col_delta_timestamp_t *slot
                    = &rels[i]->timestamps[rels[i]->nrows - 1u];
                slot->iteration = (uint32_t)((r << 8) | k);
                slot->stratum = i;
                slot->worker = k;
                slot->_reserved = 0u;
                slot->multiplicity = (int64_t)(((uint64_t)r << 32)
                    | ((uint64_t)i << 8)
                    | (uint64_t)k);
            }
        }

        /* (2) Multiplicity capture.  Hash every relation's
         * timestamps[] before the apply pass so we can prove the
         * #590 contract byte-by-byte. */
        uint64_t pre_hash[WL_NESTED_ASAN_N_RELS];
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++)
            pre_hash[i] = nested_asan_hash_timestamps(rels[i]);

        /* (3) Build a remap covering every live handle.  All
         * WL_NESTED_ASAN_ROWS_PER_REL outer handles per relation are
         * inserted, plus the inner handles for the nested half, plus
         * (sized for, populated at step 4b) identity entries
         * new_outer -> new_outer for the nested-half outer handles
         * so the per-relation primitive's implicit column-0 sweep
         * does not EIO on already-rewritten cells.  XORing with a
         * non-zero magic keeps post-remap handles non-zero (so the
         * apply pass unconditionally treats them as live cells, not
         * the NULL sentinel). */
        size_t remap_cap = ((size_t)WL_NESTED_ASAN_N_RELS
            + (size_t)WL_NESTED_ASAN_NESTED_HALF * 2u)
            * (size_t)WL_NESTED_ASAN_ROWS_PER_REL;
        wl_handle_remap_t *remap = NULL;
        if (wl_handle_remap_create(remap_cap, &remap) != 0 || !remap) {
            printf("FAIL: cycle %u remap create\n", r);
            verdict = 1;
            goto cleanup;
        }
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            for (uint32_t k = 0; k < WL_NESTED_ASAN_ROWS_PER_REL; k++) {
                int64_t old_o = (int64_t)outer_h[i][k];
                int64_t new_o = old_o ^ WL_NESTED_ASAN_HANDLE_XOR;
                if (wl_handle_remap_insert(remap, old_o, new_o) != 0) {
                    printf("FAIL: cycle %u rel %u row %u outer remap insert\n",
                        r, i, k);
                    wl_handle_remap_free(remap);
                    verdict = 1;
                    goto cleanup;
                }
                if (inner_h[i][k] != WL_COMPOUND_HANDLE_NULL) {
                    int64_t old_i = (int64_t)inner_h[i][k];
                    int64_t new_i = old_i ^ WL_NESTED_ASAN_HANDLE_XOR;
                    if (wl_handle_remap_insert(remap, old_i, new_i) != 0) {
                        printf(
                            "FAIL: cycle %u rel %u row %u inner remap insert\n",
                            r, i, k);
                        wl_handle_remap_free(remap);
                        verdict = 1;
                        goto cleanup;
                    }
                }
            }
        }

        /* (4a) Apply (column 0).  The session driver sweeps column 0
         * only (handle_remap_apply_side.h:99-109).  We covered every
         * live handle above, so EIO cannot fire -- assert that
         * contract loudly. */
        uint64_t rels_done = 0;
        uint64_t cells_done = 0;
        int rc = wl_handle_remap_apply_session_side_relations(sess, remap,
                &rels_done, &cells_done);
        if (rc != 0) {
            printf("FAIL: cycle %u apply_session rc=%d "
                "rels_done=%" PRIu64 " cells=%" PRIu64 "\n",
                r, rc, rels_done, cells_done);
            wl_handle_remap_free(remap);
            verdict = 1;
            goto cleanup;
        }
        if (rels_done != (uint64_t)WL_NESTED_ASAN_N_RELS) {
            printf("FAIL: cycle %u rels_done=%" PRIu64 " expected=%u\n",
                r, rels_done, WL_NESTED_ASAN_N_RELS);
            wl_handle_remap_free(remap);
            verdict = 1;
            goto cleanup;
        }
        if (cells_done == 0u) {
            printf("FAIL: cycle %u cells_done=0\n", r);
            wl_handle_remap_free(remap);
            verdict = 1;
            goto cleanup;
        }

        /* (4b) Apply (column 1, depth-2 nested half).  The session
         * driver sweeps column 0 only (handle_remap_apply_side.h:99-
         * 109); for the WL_NESTED_ASAN_NESTED_HALF relations carrying
         * an inner handle in column 1, drive the per-relation
         * primitive wl_handle_remap_apply_side_relation explicitly
         * with nested_cols=[1] so this workload genuinely exercises
         * the nested-arg rewrite under ASan, matching #595's "nested
         * handles handled correctly" acceptance.
         *
         * Note: the per-relation primitive ALWAYS rewrites column 0
         * implicitly in addition to the nested cols (header contract,
         * handle_remap_apply_side.h:55-58).  Step (4a) already
         * rewrote those cells to new_o, so the column-0 lookup at
         * step (4b) would EIO unless the remap also covers
         * new_o -> new_o.  Extend the remap with those identity
         * entries before invoking the per-relation primitive; this
         * mirrors the contract a multi-pass rotation helper would
         * face if it walked col 0 twice through the same remap. */
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            if (!has_inner[i])
                continue;
            for (uint32_t k = 0; k < WL_NESTED_ASAN_ROWS_PER_REL; k++) {
                int64_t new_o
                    = (int64_t)outer_h[i][k] ^ WL_NESTED_ASAN_HANDLE_XOR;
                if (wl_handle_remap_insert(remap, new_o, new_o) != 0) {
                    printf("FAIL: cycle %u rel %u row %u "
                        "identity remap insert\n", r, i, k);
                    wl_handle_remap_free(remap);
                    verdict = 1;
                    goto cleanup;
                }
            }
        }
        uint32_t nested_cols[] = { 1u };
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            if (!has_inner[i])
                continue;
            uint64_t nested_rewrites = 0;
            int nrc = wl_handle_remap_apply_side_relation(rels[i],
                    nested_cols, 1u, remap, &nested_rewrites);
            if (nrc != 0) {
                printf("FAIL: cycle %u rel %u nested apply rc=%d\n",
                    r, i, nrc);
                wl_handle_remap_free(remap);
                verdict = 1;
                goto cleanup;
            }
        }

        /* Post-apply verification.  Walk every row of every relation:
         *   column 0 must equal outer_h[i][k] ^ XOR (rewritten by 4a),
         *   column 1 of the nested half must equal inner_h[i][k] ^ XOR
         *     (rewritten by 4b),
         *   column 1 of the non-nested half must remain 0 (NULL
         *     sentinel; the apply pass skips zero cells). */
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            for (uint32_t k = 0; k < WL_NESTED_ASAN_ROWS_PER_REL; k++) {
                int64_t got0 = rels[i]->columns[0][k];
                int64_t want0
                    = (int64_t)outer_h[i][k] ^ WL_NESTED_ASAN_HANDLE_XOR;
                if (got0 != want0) {
                    printf("FAIL: cycle %u rel %u row %u col0 "
                        "got=0x%llx want=0x%llx\n",
                        r, i, k,
                        (unsigned long long)got0,
                        (unsigned long long)want0);
                    wl_handle_remap_free(remap);
                    verdict = 1;
                    goto cleanup;
                }
                int64_t got1 = rels[i]->columns[1][k];
                int64_t want1 = has_inner[i]
                    ? ((int64_t)inner_h[i][k] ^ WL_NESTED_ASAN_HANDLE_XOR)
                    : (int64_t)0;
                if (got1 != want1) {
                    printf("FAIL: cycle %u rel %u row %u col1 "
                        "got=0x%llx want=0x%llx\n",
                        r, i, k,
                        (unsigned long long)got1,
                        (unsigned long long)want1);
                    wl_handle_remap_free(remap);
                    verdict = 1;
                    goto cleanup;
                }
            }
        }

        /* (5) Multiplicity assert.  The hashes must be byte-equal
         * across the apply pass; if they diverge, the apply
         * implementation broke the #590 contract. */
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            uint64_t post = nested_asan_hash_timestamps(rels[i]);
            if (post != pre_hash[i]) {
                printf("FAIL: cycle %u rel %u timestamps hash mismatch "
                    "pre=0x%llx post=0x%llx\n",
                    r, i,
                    (unsigned long long)pre_hash[i],
                    (unsigned long long)post);
                wl_handle_remap_free(remap);
                verdict = 1;
                goto cleanup;
            }
        }

        /* (6) Cache invalidation.  Independent helper -- the apply
         * pass does not call it -- so we drive it explicitly to
         * exercise the #591 free-then-NULL dedup_slots path under
         * ASan.  Pre-seed dedup_slots so the free() actually has
         * something to release; otherwise we'd be exercising the
         * already-NULL fast path on every cycle and miss the leak
         * window the brief calls out. */
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            if (rels[i]->dedup_slots == NULL) {
                rels[i]->dedup_cap = 4u;
                rels[i]->dedup_count = 1u;
                rels[i]->dedup_slots = (uint64_t *)calloc(rels[i]->dedup_cap,
                        sizeof(uint64_t));
                if (!rels[i]->dedup_slots) {
                    printf("FAIL: cycle %u rel %u dedup_slots seed\n",
                        r, i);
                    wl_handle_remap_free(remap);
                    verdict = 1;
                    goto cleanup;
                }
            }
        }
        uint64_t invalidated = 0;
        rc = wl_handle_remap_invalidate_side_relation_caches(sess,
                &invalidated);
        if (rc != 0 || invalidated != (uint64_t)WL_NESTED_ASAN_N_RELS) {
            printf("FAIL: cycle %u invalidate rc=%d touched=%" PRIu64 "\n",
                r, rc, invalidated);
            wl_handle_remap_free(remap);
            verdict = 1;
            goto cleanup;
        }

        wl_handle_remap_free(remap);

        /* (7) Delete phase.  Drop multiplicity to zero on every
         * handle so the GC step actually has something to reclaim.
         * The apply pass rewrote the row's column 0 so the in-arena
         * record for the OLD handle is what carries the
         * multiplicity counter -- we retain on the original
         * pre-remap value, which is what we tracked locally. */
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++) {
            for (uint32_t k = 0; k < WL_NESTED_ASAN_ROWS_PER_REL; k++) {
                (void)wl_compound_arena_retain(arena, outer_h[i][k], -1);
                if (inner_h[i][k] != WL_COMPOUND_HANDLE_NULL)
                    (void)wl_compound_arena_retain(arena, inner_h[i][k], -1);
            }
        }

        /* (8) GC.  Reclaims retired entries; ASan watches the freed
         * generation buffers and screams if any column-buffer code
         * still holds a stale handle.  Frozen-arena no-op contract
         * does not apply (we never freeze in this workload). */
        (void)wl_compound_arena_gc_epoch_boundary(arena);

        /* (9) Logical row truncation.  The brief frames this as a
         * "delete+insert cycle": the rows we appended in step (1)
         * carry handles whose arena multiplicity is now zero and
         * whose generation has been retired by the GC.  Continuing
         * to carry those rows forward would force the next cycle's
         * remap to include the prior cycle's (rewritten) handles
         * just to satisfy the apply pass's "every non-zero cell
         * must be in the remap" precondition -- which would mask
         * the half-rotation EIO contract from #590 instead of
         * exercising it.  Truncating nrows preserves the column
         * buffers and timestamps[] allocation (so the next cycle's
         * append reuses the heap memory and ASan still tracks
         * cumulative leaks) while clearing the cells the next
         * cycle's apply pass would otherwise see. */
        for (uint32_t i = 0; i < WL_NESTED_ASAN_N_RELS; i++)
            rels[i]->nrows = 0u;
    }

    clock_t t1 = clock();
    double secs = (double)(t1 - t0) / (double)CLOCKS_PER_SEC;

    if (verdict == 0)
        printf("PASS (%.3fs)\n", secs);

cleanup:
    free(rels);
    nested_asan_free_mock_session(sess);
    wl_compound_arena_free(arena);
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
    } else if (strcmp(workload, "nested-asan") == 0) {
        rc = run_nested_asan_workload(W, R);
    } else {
        fprintf(stderr,
            "FAIL: WL_STRESS_WORKLOAD='%s' is not one of "
            "'freeze-cycle', 'apply-roundtrip', 'nested-asan'\n",
            workload);
        return 1;
    }

    if (rc == 0)
        printf("\nWorkload '%s' passed.\n", workload);
    else
        printf("\nFAILURE: workload '%s' failed.\n", workload);
    return rc == 0 ? 0 : 1;
}
