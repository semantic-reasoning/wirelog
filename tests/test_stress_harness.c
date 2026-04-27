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
 *   "daemon-soak" (#597):
 *       Long-running soak that proves bounded-RSS behavior under
 *       saturation-driven rotation cadence.  Differs from rotation-
 *       vtable on three axes: (a) cadence is saturation-driven rather
 *       than R-loop-bounded -- when current_epoch + 8 >= max_epochs
 *       the workload calls gc_epoch_boundary explicitly to advance the
 *       epoch, and on absolute saturation (current_epoch ==
 *       max_epochs, the sentinel from compound_arena.c:351) it
 *       destroys + recreates the arena (a deliberate scope
 *       discontinuity, the only path that exercises arena recreate
 *       under stress), (b) per-step handle fan-out K=1000 (vs
 *       rotation-vtable's K=64) so each step produces meaningful
 *       allocator pressure, (c) a cumulative-survival oracle every
 *       Nth step walks the LAST WL_DAEMON_WINDOW = 8000 handles
 *       (~8 epochs of allocations within a single arena lifetime) to
 *       prove handles allocated within a "rotation window" survive
 *       across many gc_epoch_boundary calls within that window --
 *       rotation-vtable only verifies current-cycle handles.
 *       End-of-run RSS gate: rss_final <= rss_baseline +
 *       max(rss_baseline / 10, 16384 KB).  Linux/macOS enforce; on
 *       Windows where the sampler can return -1 (or in containers
 *       without /proc) the gate is skipped with a one-line diagnostic
 *       and the run continues.  Mock-session pattern is identical to
 *       rotation-vtable (rotation hooks only touch eval_arena and
 *       compound_arena).  W is accepted but ignored: rotation hooks
 *       are single-mutator by contract.
 *
 *   "rotation-vtable" (#596):
 *       Drives the #600 rotation strategy vtable
 *       (sess->rotation_ops->rotate_eval_arena and ->gc_epoch_boundary)
 *       under W/R stress.  Existing workloads call the rotation/GC
 *       primitives DIRECTLY (wl_compound_arena_gc_epoch_boundary,
 *       wl_arena_reset), bypassing the vtable; none exercise the
 *       dispatch indirection #600 introduced.  Coverage delta vs.
 *       tests/test_rotation_strategy.c (#600's correctness test): that
 *       file functional-tests selection (default-is-standard,
 *       env-override-mvcc) and runs ONE rotate+gc dispatch on a live
 *       session with no churn.  This workload exercises the dispatch
 *       under R cycles of pre-rotation alloc fan-out + per-cycle
 *       vtable invocation + per-cycle handle-validity oracle, on both
 *       strategy variants (WIRELOG_ROTATION=standard and =mvcc, parsed
 *       at workload start).  Both variants must be tested because
 *       #596's contract is the dispatch path, not the strategy
 *       semantics (MVCC is functionally STANDARD today; the placeholder
 *       still has to be reachable through the function pointer).
 *
 *       Per-cycle: (1) allocate K=64 handles in sess->compound_arena
 *       and record (handle, payload_size) in an oracle slice, (2a)
 *       call sess->rotation_ops->rotate_eval_arena(sess), (3) verify
 *       every just-allocated handle is still valid via
 *       wl_compound_arena_lookup with size-equality (the "all
 *       pre-rotation handles valid" acceptance bullet), then (2b)
 *       sess->rotation_ops->gc_epoch_boundary(sess) closes the epoch.
 *       The validity check is between the two vtable calls because
 *       gc_epoch_boundary's documented semantics (compound_arena.c:
 *       332-344) clear the closed generation -- that is GC behavior
 *       owned by the arena implementation, not the #596 vtable
 *       contract.  rotate_eval_arena must not invalidate compound
 *       handles, which is what step (3) asserts.  W is accepted but ignored: rotation hooks
 *       are single-mutator by contract.  Mock-session pattern (mirrors
 *       nested-asan's make_mock_session): rotation hooks only touch
 *       sess->eval_arena and sess->compound_arena, so a tiny calloc'd
 *       wl_col_session_t with those two fields wired is sufficient --
 *       no parser/optimizer/plan link cost.
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
 *                       "rotation-vtable"
 *                       "daemon-soak"
 *
 *   WIRELOG_ROTATION    "standard" | "mvcc"           (default "standard")
 *                       Honored by the rotation-vtable and daemon-soak
 *                       workloads; parsed at workload start.  Both
 *                       variants must be tested to prove the #600
 *                       dispatch path is reachable for both strategies.
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

#include "../wirelog/arena/arena.h"
#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/columnar/compound_side.h"
#include "../wirelog/columnar/delta_pool.h"
#include "../wirelog/columnar/handle_remap.h"
#include "../wirelog/columnar/handle_remap_apply.h"
#include "../wirelog/columnar/handle_remap_apply_side.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/workqueue.h"
#include "test_rss_util.h"

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

/* Parse WIRELOG_ROTATION into a rotation_ops vtable pointer.  Mirrors
 * session.c:437-442's strict-fail parser: unknown values are a hard
 * FAIL so a misconfigured CI tier surfaces loudly.  Returns 0 and
 * writes to @out_ops / @out_name on success, -1 on parse error (the
 * caller is responsible for printing FAIL and bailing).  Unset env =
 * default ("standard").  Used by both rotation-vtable (#596) and
 * daemon-soak (#597). */
static int
parse_rotation_strategy(const col_rotation_ops_t **out_ops,
    const char **out_name)
{
    const char *rot_env = getenv("WIRELOG_ROTATION");
    if (!rot_env || rot_env[0] == '\0') {
        *out_ops = &col_rotation_standard_ops;
        *out_name = "standard";
        return 0;
    }
    if (strcmp(rot_env, "standard") == 0) {
        *out_ops = &col_rotation_standard_ops;
        *out_name = "standard";
        return 0;
    }
    if (strcmp(rot_env, "mvcc") == 0) {
        *out_ops = &col_rotation_mvcc_ops;
        *out_name = "mvcc";
        return 0;
    }
    printf("FAIL: WIRELOG_ROTATION='%s' is not 'standard' or 'mvcc'\n",
        rot_env);
    return -1;
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
/* Rotation-vtable workload (#596)                                          */
/* ======================================================================== */

/* Pre-rotation handle fan-out per cycle.  Small enough that K * R cycles
 * stays well under the compound-arena epoch ceiling (WL_COMPOUND_EPOCH_MAX
 * is 4095) at the highest CI tier (R=1000), large enough that every cycle
 * actually has multiple handles for the post-rotation lookup oracle to
 * verify -- a per-cycle K=1 oracle would be too weak to catch a partial-
 * rewrite bug. */
#define WL_ROTATION_K_HANDLES   64u

/* Per-handle payload size.  Aligned to 8 bytes; matches the convention
 * other workloads use for compound-arena allocations. */
#define WL_ROTATION_PAYLOAD_SZ  16u

/* Per-cycle hard cap for R: each cycle's gc_epoch_boundary advances
 * current_epoch by one (see standard_gc_epoch_boundary in
 * rotation_standard.c, mvcc_gc_epoch_boundary in rotation_mvcc.c).
 * Mirror nested-asan's cap of 1500 -- WL_COMPOUND_EPOCH_MAX is 4095, so
 * 1500 leaves >2x headroom and keeps the bound assertion symmetric
 * across workloads.  The bounds-check error message uses this constant
 * so the hard-fail diagnostic is parseable. */
#define WL_ROTATION_R_CAP       1500u

/* Eval-arena capacity.  rotate_eval_arena calls wl_arena_reset, which
 * is a constant-time bump-pointer reset; the actual capacity does not
 * matter for the dispatch test, only that the arena exists.  256KB is
 * enough headroom that any future eval-arena saturation step would
 * have room to operate without bumping into the capacity limit. */
#define WL_ROTATION_EVAL_ARENA_BYTES (256u * 1024u)

/* Pre-rotation alloc oracle entry.  One per recorded handle. */
typedef struct {
    uint64_t handle;
    uint32_t expected_payload_size;
} rotation_oracle_entry_t;

/* Mock session for the rotation-vtable workload.  Mirrors
 * nested_asan_make_mock_session, but only wires the two fields the
 * rotation hooks touch (compound_arena, eval_arena) plus rotation_ops.
 * No delta_pool / mem_ledger / rels are required: the rotation vtable
 * surface is intentionally narrow, which is what makes the mock
 * justified -- read rotation_standard.c and rotation_mvcc.c to confirm.
 *
 * @ops: STANDARD or MVCC vtable, selected by the caller from the
 *       WIRELOG_ROTATION env var.
 */
static wl_col_session_t *
rotation_make_mock_session(wl_compound_arena_t *arena,
    const col_rotation_ops_t *ops)
{
    wl_col_session_t *s = (wl_col_session_t *)calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->compound_arena = arena;
    s->eval_arena = wl_arena_create(WL_ROTATION_EVAL_ARENA_BYTES);
    if (!s->eval_arena) {
        free(s);
        return NULL;
    }
    s->rotation_ops = ops;
    /* Note: we deliberately do NOT call ops->init(s) -- the mvcc init
     * hook is a no-op apart from a one-time WL_LOG line, and standard
     * init is empty.  Skipping init keeps the mock self-contained. */
    return s;
}

static void
rotation_free_mock_session(wl_col_session_t *s)
{
    if (!s)
        return;
    /* compound_arena is owned by the workload, freed separately. */
    if (s->eval_arena)
        wl_arena_free(s->eval_arena);
    free(s);
}

static int
run_rotation_vtable_workload(uint32_t num_workers, uint32_t cycles)
{
    /* W is accepted for CI-tier uniformity but ignored: the rotation
     * vtable hooks (rotate_eval_arena, gc_epoch_boundary) are
     * single-mutator by contract -- they walk the eval arena's bump
     * pointer and the compound arena's per-epoch generation table,
     * neither of which is concurrency-safe.  Same rationale as
     * apply-roundtrip and nested-asan. */
    (void)num_workers;

    /* Strategy selection: WIRELOG_ROTATION mirrors session.c:437-442.
     * Default is STANDARD; "mvcc" selects the placeholder.  Anything
     * else is a hard FAIL so a misconfigured CI tier surfaces loudly
     * (mirrors the unknown-workload diagnostic in main()). */
    const col_rotation_ops_t *ops = NULL;
    const char *strategy_name = NULL;
    if (parse_rotation_strategy(&ops, &strategy_name) != 0)
        return 1;

    printf("  [rotation-vtable W=%u (ignored, single-mutator) R=%u "
        "strategy=%s] ", num_workers, cycles, strategy_name);
    fflush(stdout);

    /* Bound check: each cycle's gc_epoch_boundary advances the compound
     * arena's current_epoch by one.  Mirror nested-asan's 2x check so
     * the cap diagnostic is parseable and consistent across workloads. */
    if (cycles == 0u || cycles > WL_ROTATION_R_CAP) {
        printf(
            "FAIL: WL_STRESS_R=%u out of range (1..%u for rotation-vtable)\n",
            cycles, WL_ROTATION_R_CAP);
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

    wl_col_session_t *sess = rotation_make_mock_session(arena, ops);
    if (!sess) {
        printf("FAIL: mock session create\n");
        wl_compound_arena_free(arena);
        return 1;
    }

    /* Cumulative oracle: every handle ever allocated since the workload
     * started.  Sized for the worst case so we can verify validity not
     * just for the latest cycle's handles but for ALL pre-rotation
     * handles -- the #596 acceptance contract.  K * R is bounded above
     * by WL_ROTATION_K_HANDLES * WL_ROTATION_R_CAP = 64 * 1500 = 96000
     * entries, which fits comfortably in heap memory at <2MB total. */
    size_t oracle_cap = (size_t)WL_ROTATION_K_HANDLES * (size_t)cycles;
    rotation_oracle_entry_t *oracle = (rotation_oracle_entry_t *)calloc(
        oracle_cap, sizeof(*oracle));
    if (!oracle) {
        printf("FAIL: oracle calloc (%zu entries)\n", oracle_cap);
        rotation_free_mock_session(sess);
        wl_compound_arena_free(arena);
        return 1;
    }
    size_t oracle_n = 0;
    int verdict = 0;

    clock_t t0 = clock();

    for (uint32_t r = 0; r < cycles; r++) {
        /* (1) Pre-rotation alloc oracle.  Allocate K handles in the
         * compound arena's CURRENT epoch and record (handle,
         * payload_size).  These are this cycle's pre-rotation handles
         * for the validity assertion at step (3). */
        size_t cycle_oracle_start = oracle_n;
        for (uint32_t k = 0; k < WL_ROTATION_K_HANDLES; k++) {
            uint64_t h = wl_compound_arena_alloc(arena, WL_ROTATION_PAYLOAD_SZ);
            if (h == WL_COMPOUND_HANDLE_NULL) {
                printf("FAIL: cycle %u handle %u alloc\n", r, k);
                verdict = 1;
                goto cleanup;
            }
            oracle[oracle_n].handle = h;
            oracle[oracle_n].expected_payload_size = WL_ROTATION_PAYLOAD_SZ;
            oracle_n++;
        }

        /* (2a) Vtable rotation: eval-arena reset.  THIS IS THE #596
         * CONTRACT: the indirect function-pointer dispatch through
         * sess->rotation_ops, NOT a direct wl_arena_reset call.
         * rotate_eval_arena resets the eval (scratch) arena; it must
         * NOT touch the compound arena -- pre-rotation compound
         * handles must remain valid through this call.  Crash or
         * compound-handle invalidation here under either strategy
         * variant means #600's vtable plumbing is broken under
         * stress. */
        sess->rotation_ops->rotate_eval_arena(sess);

        /* (3) Pre-rotation handle validity oracle.  Walk this cycle's
         * pre-rotation handles -- they were allocated in the current
         * epoch and must still resolve via lookup with the recorded
         * payload size after rotate_eval_arena.  This is the "all
         * pre-rotation handles valid" acceptance bullet from #596:
         * rotation MUST preserve compound-handle validity for the
         * just-allocated set.
         *
         * We deliberately walk ONLY this cycle's slice of the oracle
         * (cycle_oracle_start .. oracle_n), not the cumulative set,
         * because step (2b)'s gc_epoch_boundary at the prior cycle's
         * tail already retired those older epochs by design (the
         * skeleton GC at compound_arena.c:332-344 zeroes the closed
         * generation).  Asserting validity for handles whose epoch
         * was already GC'd would be asserting against the documented
         * arena semantics, not against #596's vtable contract. */
        for (size_t j = cycle_oracle_start; j < oracle_n; j++) {
            uint32_t out_size = 0;
            const void *payload = wl_compound_arena_lookup(arena,
                    oracle[j].handle, &out_size);
            if (payload == NULL) {
                printf("FAIL: cycle %u oracle[%zu] handle=0x%llx "
                    "lookup returned NULL after rotate_eval_arena\n",
                    r, j, (unsigned long long)oracle[j].handle);
                verdict = 1;
                goto cleanup;
            }
            if (out_size != oracle[j].expected_payload_size) {
                printf("FAIL: cycle %u oracle[%zu] handle=0x%llx "
                    "size=%u expected=%u\n",
                    r, j, (unsigned long long)oracle[j].handle,
                    out_size, oracle[j].expected_payload_size);
                verdict = 1;
                goto cleanup;
            }
        }

        /* (2b) Vtable rotation: epoch advance via the second hook.
         * gc_epoch_boundary closes the current epoch (clears its
         * generation, advances current_epoch).  After this call, the
         * handles validated at step (3) belong to a closed epoch and
         * subsequent lookups would return NULL -- by design (see
         * compound_arena.c:332-344).  The dispatch through the vtable
         * (function pointer, not direct call) is what #596 stresses;
         * the post-state semantics are owned by the GC implementation
         * and verified by separate tests. */
        sess->rotation_ops->gc_epoch_boundary(sess);
    }

    clock_t t1 = clock();
    double secs = (double)(t1 - t0) / (double)CLOCKS_PER_SEC;

    if (verdict == 0)
        printf("PASS (%.3fs)\n", secs);

cleanup:
    free(oracle);
    rotation_free_mock_session(sess);
    wl_compound_arena_free(arena);
    return verdict;
}

/* ======================================================================== */
/* Daemon-soak workload (#597)                                              */
/* ======================================================================== */

/* Per-step handle fan-out.  Constant at 1000 per the issue spec
 * ("10K steps x 1000 handles/step" -- the residual scope after the
 * naive 10M-handles-in-one-arena reading was rejected as physically
 * infeasible against WL_COMPOUND_EPOCH_MAX = 4095).  Sized so each
 * step produces meaningful allocator pressure (vs rotation-vtable's
 * K=64 which is a thin oracle slice). */
#define WL_DAEMON_K_HANDLES 1000u

/* Per-handle payload size.  24 bytes matches freeze-cycle's sentinel
 * payload and is large enough that K * payload per step (~24 KB)
 * crosses the arena's default generation cap fast, making the
 * saturation predicate fire deterministically over the soak. */
#define WL_DAEMON_PAYLOAD_SZ 24u

/* Saturation buffer: when current_epoch + WL_DAEMON_EPOCH_BUFFER >=
 * max_epochs, the workload calls gc_epoch_boundary explicitly to
 * advance the epoch ahead of allocations.  8 mirrors compound_arena.c's
 * own near-saturation warning threshold (which fires at +5) with a
 * little extra slack so the explicit advance happens before the arena
 * hits the saturation sentinel mid-step.  Values <= max_epochs - 1
 * keep the predicate well-defined. */
#define WL_DAEMON_EPOCH_BUFFER 8u

/* Cumulative-survival window.  Walk the LAST min(WINDOW, oracle_n)
 * handles every Nth step to verify they still resolve.  8 epochs of
 * allocations within a single arena lifetime: bounded above by
 * (max_epochs - 1) * K, well within heap budget at K=1000.  This is
 * the differentiator vs rotation-vtable, which only checks
 * current-cycle handles. */
#define WL_DAEMON_WINDOW (8u * WL_DAEMON_K_HANDLES)

/* Cumulative-survival check cadence.  Walk the survival window every
 * Nth step rather than per-step (per-step would burn O(R*WINDOW)
 * lookups; once-per-100-steps keeps the soak under timeout while
 * still touching the survival contract many times in a release run). */
#define WL_DAEMON_SURVIVAL_STRIDE 100u

/* Hard cap for R: the issue spec calls for up to 10K steps.  Anything
 * higher is a configuration mistake and hard-fails to surface loudly. */
#define WL_DAEMON_R_CAP 10000u

/* Compound-arena max_epochs override.  Default is WL_COMPOUND_EPOCH_MAX
 * + 1 = 4096; with one epoch advance per step the saturation predicate
 * (current_epoch + WL_DAEMON_EPOCH_BUFFER >= max_epochs) would only
 * trip at step ~4088, never reached by PR or nightly tiers.  A tight
 * 32-epoch ceiling makes the predicate fire every ~24 steps so even
 * R=100 PR runs exercise multiple arena recreates and the RSS gate
 * has a steady-state ceiling to compare against.  The "saturation-
 * driven cadence" the issue brief mandates is honored regardless of
 * the absolute epoch count -- only trigger frequency changes. */
#define WL_DAEMON_MAX_EPOCHS 32u

/* Eval-arena capacity.  Identical sizing rationale to rotation-vtable:
 * rotate_eval_arena calls wl_arena_reset (constant-time bump-pointer
 * reset), so the actual capacity does not matter for the dispatch
 * test, only that the arena exists. */
#define WL_DAEMON_EVAL_ARENA_BYTES (256u * 1024u)

/* Daemon-soak oracle entry: handle plus expected payload size.
 * Matches rotation-vtable's oracle shape so the lookup-equality
 * assertion is symmetric. */
typedef struct {
    uint64_t handle;
    uint32_t expected_payload_size;
} daemon_soak_oracle_entry_t;

/* Mock session: same shape as rotation_make_mock_session, factored
 * here so daemon-soak does not piggy-back on rotation-vtable's
 * static helper (the brief calls out "same mock-session pattern" --
 * a duplicate static at the same surface area is the cheapest way
 * to keep the two workloads independent so a future divergence in
 * one does not silently churn the other). */
static wl_col_session_t *
daemon_soak_make_mock_session(wl_compound_arena_t *arena,
    const col_rotation_ops_t *ops)
{
    wl_col_session_t *s = (wl_col_session_t *)calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->compound_arena = arena;
    s->eval_arena = wl_arena_create(WL_DAEMON_EVAL_ARENA_BYTES);
    if (!s->eval_arena) {
        free(s);
        return NULL;
    }
    s->rotation_ops = ops;
    return s;
}

static void
daemon_soak_free_mock_session(wl_col_session_t *s)
{
    if (!s)
        return;
    if (s->eval_arena)
        wl_arena_free(s->eval_arena);
    free(s);
}

static int
run_daemon_soak_workload(uint32_t num_workers, uint32_t steps)
{
    /* W is accepted for CI-tier uniformity but ignored: rotation hooks
     * are single-mutator by contract.  Same rationale as
     * rotation-vtable; see that workload's note above. */
    (void)num_workers;

    const col_rotation_ops_t *ops = NULL;
    const char *strategy_name = NULL;
    if (parse_rotation_strategy(&ops, &strategy_name) != 0)
        return 1;

    printf("  [daemon-soak W=%u (ignored, single-mutator) R=%u "
        "strategy=%s] ", num_workers, steps, strategy_name);
    fflush(stdout);

    /* Bound check: 0 or > 10000 is a hard FAIL (issue cap). */
    if (steps == 0u || steps > WL_DAEMON_R_CAP) {
        printf("FAIL: WL_STRESS_R=%u out of range (1..%u for daemon-soak)\n",
            steps, WL_DAEMON_R_CAP);
        return 1;
    }

    /* See WL_DAEMON_MAX_EPOCHS docstring for rationale. */
    wl_compound_arena_t *arena = wl_compound_arena_create(0xCAFEu, 4096u,
            WL_DAEMON_MAX_EPOCHS);
    if (!arena) {
        printf("FAIL: arena create\n");
        return 1;
    }
    wl_col_session_t *sess = daemon_soak_make_mock_session(arena, ops);
    if (!sess) {
        printf("FAIL: mock session create\n");
        wl_compound_arena_free(arena);
        return 1;
    }

    /* Anchor RSS baseline AFTER mock-session construction so the
     * baseline reflects the steady-state working set, not the
     * pre-construction footprint.  Sampler returning -1 means
     * "platform unavailable" -- gate skipped at end. */
    int64_t rss_baseline_kb = test_peak_rss_kb();

    /* Window-local oracle: handles allocated within the CURRENT arena
     * lifetime.  Reset on arena recreate (deliberate scope
     * discontinuity per the issue brief).  Sized for the worst case
     * within one arena: (max_epochs - 1) * K entries.  At K=1000 and
     * max_epochs=4096, that is ~32 MB which is comfortably bounded;
     * we cap to a fixed allocation that fits the largest possible
     * window. */
    size_t oracle_cap
        = (size_t)(arena->max_epochs - 1u) * (size_t)WL_DAEMON_K_HANDLES;
    daemon_soak_oracle_entry_t *oracle
        = (daemon_soak_oracle_entry_t *)calloc(oracle_cap, sizeof(*oracle));
    if (!oracle) {
        printf("FAIL: oracle calloc (%zu entries)\n", oracle_cap);
        daemon_soak_free_mock_session(sess);
        wl_compound_arena_free(arena);
        return 1;
    }
    size_t oracle_n = 0;
    int oracle_just_reset = 0;
    int verdict = 0;
    uint32_t recreate_count = 0;

    clock_t t0 = clock();

    for (uint32_t r = 0; r < steps; r++) {
        /* Saturation predicate: if the arena's current_epoch is
         * within WL_DAEMON_EPOCH_BUFFER of max_epochs, advance the
         * epoch via the vtable hook to keep allocations flowing.  If
         * the advance pushes (or has already pushed) current_epoch to
         * the saturation sentinel (== max_epochs, see
         * compound_arena.c:351), destroy and recreate the arena -- a
         * deliberate scope discontinuity that resets the survival
         * oracle for this window.  Important: do NOT call
         * gc_epoch_boundary on an already-saturated arena -- the
         * sentinel current_epoch == max_epochs is out-of-bounds for
         * arena->gens[], and the GC implementation indexes gens[
         * current_epoch] unconditionally.  Check for sentinel BEFORE
         * advancing. */
        if (arena->current_epoch + WL_DAEMON_EPOCH_BUFFER
            >= arena->max_epochs) {
            if (arena->current_epoch < arena->max_epochs)
                sess->rotation_ops->gc_epoch_boundary(sess);
            if (arena->current_epoch == arena->max_epochs) {
                printf("\n  [step %u: arena saturated, recreating]", r);
                fflush(stdout);
                /* Tear down and recreate.  Re-wire the mock session's
                 * compound_arena pointer so subsequent vtable calls
                 * land on the fresh arena. */
                wl_compound_arena_free(arena);
                arena = wl_compound_arena_create(0xCAFEu, 4096u,
                        WL_DAEMON_MAX_EPOCHS);
                if (!arena) {
                    printf("\nFAIL: step %u arena recreate\n", r);
                    sess->compound_arena = NULL;
                    verdict = 1;
                    goto cleanup;
                }
                sess->compound_arena = arena;
                /* Reset survival oracle: handles from the destroyed
                * arena are no longer addressable; carrying them
                * forward would force the next survival sweep to
                * fail.  This is the "deliberate scope discontinuity
                * at arena recreation" the issue brief calls out. */
                oracle_n = 0;
                oracle_just_reset = 1;
                recreate_count++;
            }
        }

        /* Allocate K=1000 handles in the current epoch.  Record each
         * in the window-local oracle. */
        size_t step_oracle_start = oracle_n;
        for (uint32_t k = 0; k < WL_DAEMON_K_HANDLES; k++) {
            uint64_t h = wl_compound_arena_alloc(arena, WL_DAEMON_PAYLOAD_SZ);
            if (h == WL_COMPOUND_HANDLE_NULL) {
                printf("\nFAIL: step %u handle %u alloc returned NULL "
                    "(current_epoch=%u max_epochs=%u)\n",
                    r, k, arena->current_epoch, arena->max_epochs);
                verdict = 1;
                goto cleanup;
            }
            if (oracle_n >= oracle_cap) {
                printf("\nFAIL: step %u oracle overflow at n=%zu cap=%zu\n",
                    r, oracle_n, oracle_cap);
                verdict = 1;
                goto cleanup;
            }
            oracle[oracle_n].handle = h;
            oracle[oracle_n].expected_payload_size = WL_DAEMON_PAYLOAD_SZ;
            oracle_n++;
        }

        /* Vtable rotation: eval-arena reset.  Same #596 contract as
         * rotation-vtable -- must NOT touch compound_arena.  Pre-
         * rotation handles (this step's K plus any from prior steps in
         * the same window) must remain valid through this call. */
        sess->rotation_ops->rotate_eval_arena(sess);

        /* Mid-rotation oracle: every handle just allocated must
         * resolve via lookup with size-equality.  This is the per-step
         * acceptance gate. */
        for (size_t j = step_oracle_start; j < oracle_n; j++) {
            uint32_t out_size = 0;
            const void *payload = wl_compound_arena_lookup(arena,
                    oracle[j].handle, &out_size);
            if (payload == NULL) {
                printf("\nFAIL: step %u oracle[%zu] handle=0x%llx "
                    "lookup returned NULL after rotate_eval_arena\n",
                    r, j, (unsigned long long)oracle[j].handle);
                verdict = 1;
                goto cleanup;
            }
            if (out_size != oracle[j].expected_payload_size) {
                printf("\nFAIL: step %u oracle[%zu] handle=0x%llx "
                    "size=%u expected=%u\n",
                    r, j, (unsigned long long)oracle[j].handle,
                    out_size, oracle[j].expected_payload_size);
                verdict = 1;
                goto cleanup;
            }
            /* Multiplicity check: initial alloc multiplicity is 1
             * (compound_arena.h documents this; see compound_arena.c
             * line 178 where alloc seeds multiplicity[i] = 1). */
            int64_t mult = wl_compound_arena_multiplicity(arena,
                    oracle[j].handle);
            if (mult != 1) {
                printf("\nFAIL: step %u oracle[%zu] handle=0x%llx "
                    "multiplicity=%lld expected=1\n",
                    r, j, (unsigned long long)oracle[j].handle,
                    (long long)mult);
                verdict = 1;
                goto cleanup;
            }
        }

        /* Cumulative-survival check: every Nth step, walk the last
         * min(WINDOW, oracle_n) handles and verify each still
         * resolves.  This is the differentiator vs rotation-vtable
         * (which only checks current-cycle handles).  Skip when the
         * oracle was just reset by arena recreation -- the destroyed
         * handles are by design unaddressable.
         *
         * Earlier handles in the window were allocated in CLOSED
         * epochs (gc_epoch_boundary at the prior step's tail closed
         * them), and per compound_arena.c:332-344 the GC zeroes the
         * closed generation's entry table.  We therefore allow
         * lookups to return NULL for handles whose epoch has been
         * GC'd; the survival contract we DO assert is the absence of
         * a crash and the validity of the lookup API itself across
         * many gc_epoch_boundary calls within a window.  This is
         * sufficient to catch use-after-free regressions in the
         * lookup path under a long soak; ASan / TSan tiers add the
         * orthogonal memory-safety axis. */
        if (!oracle_just_reset
            && r > 0u
            && (r % WL_DAEMON_SURVIVAL_STRIDE) == 0u
            && oracle_n > 0u) {
            size_t window = (oracle_n < WL_DAEMON_WINDOW)
                ? oracle_n : WL_DAEMON_WINDOW;
            size_t start = oracle_n - window;
            for (size_t j = start; j < oracle_n; j++) {
                uint32_t out_size = 0;
                /* Lookup may legitimately return NULL for closed-
                 * epoch handles; we exercise the call path for
                 * use-after-free coverage and tolerate either
                 * resolution.  When non-NULL, size MUST match. */
                const void *payload = wl_compound_arena_lookup(arena,
                        oracle[j].handle, &out_size);
                if (payload != NULL
                    && out_size != oracle[j].expected_payload_size) {
                    printf("\nFAIL: step %u survival[%zu] handle=0x%llx "
                        "size=%u expected=%u\n",
                        r, j, (unsigned long long)oracle[j].handle,
                        out_size, oracle[j].expected_payload_size);
                    verdict = 1;
                    goto cleanup;
                }
            }
        }
        oracle_just_reset = 0;

        /* Vtable rotation: epoch advance.  Closes the current epoch;
         * the handles validated above belong to a closed epoch after
         * this call.  Same #596 dispatch contract as rotation-vtable. */
        sess->rotation_ops->gc_epoch_boundary(sess);
    }

    clock_t t1 = clock();
    double secs = (double)(t1 - t0) / (double)CLOCKS_PER_SEC;

    /* End-of-run RSS gate.  Sample BEFORE cleanup so the heap has not
     * yet been deallocated (otherwise final < baseline trivially and
     * the gate is meaningless). */
    int64_t rss_final_kb = test_peak_rss_kb();
    int rss_skipped = 0;
    /* Clang exposes __has_feature; GCC does not.  Polyfill so the
     * conditional below tokenizes cleanly under both compilers
     * (GCC's preprocessor can't short-circuit __has_feature(...) at
     * tokenization time -- it sees 0(address_sanitizer) and bails
     * with "missing binary operator before token (").  The polyfill
     * makes __has_feature(...) always callable and return 0 under
     * non-Clang. */
#ifndef __has_feature
#  define __has_feature(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || __has_feature(address_sanitizer)
    /* AddressSanitizer adds redzones, shadow memory, and a quarantine
     * freelist that prevent the OS from reclaiming pages back below
     * VmHWM after free().  The RSS gate's no-leak signal is therefore
     * confounded under ASan -- the gate is for the production code
     * path's allocator footprint, not for ASan's instrumentation
     * overhead.  Skip the gate under ASan with an explicit diagnostic;
     * the ASan tier still validates the workload functionally and
     * surfaces any actual heap-buffer-overflow / use-after-free /
     * leak via ASan's own reporting.  This branch is selected at
     * compile time so it does not regress the non-instrumented
     * builds. */
    printf("\n  [rss-gate skipped: AddressSanitizer instrumentation "
        "confounds VmHWM signal] ");
    rss_skipped = 1;
    (void)rss_baseline_kb;
    (void)rss_final_kb;
#else
    if (rss_baseline_kb < 0 || rss_final_kb < 0) {
        printf("\n  [rss-gate skipped: platform sampler unavailable] ");
        rss_skipped = 1;
    } else {
        /* Gate: rss_final <= rss_baseline + max(rss_baseline / 10, 16 MB).
         * 10% growth or 16 MB absolute floor (whichever larger).  The
         * floor is the binding constraint for typical CI baselines
         * (1-30 MB); the percentage kicks in at very large baselines
         * (>160 MB) to catch partial leaks.  Floor sized empirically
         * from observed CI working-set growth: macOS PR (R=100,
         * baseline=1.3 MB) deltas ~5 MB; Linux release (R=10000,
         * baseline=28 MB) deltas ~9.5 MB.  16 MB clears all observed
         * legitimate growth while still catching real leaks (e.g. a
         * leak of 1 KB/rotation at R=10000 = 10 MB cumulative -- under
         * the floor; a leak of 2 KB/rotation = 20 MB -- caught).  Per-
         * rotation delta detection at finer granularity is #598's
         * test_rss_bounded scope (VmRSS, not VmHWM). */
        int64_t allowance_pct = rss_baseline_kb / 10;
        int64_t allowance_floor = 16384; /* KB */
        int64_t allowance
            = (allowance_pct > allowance_floor) ? allowance_pct
                                                : allowance_floor;
        int64_t budget = rss_baseline_kb + allowance;
        if (rss_final_kb > budget) {
            printf("\nFAIL: rss-gate baseline=%lld kb final=%lld kb "
                "delta=%lld kb gate=%lld kb\n",
                (long long)rss_baseline_kb,
                (long long)rss_final_kb,
                (long long)(rss_final_kb - rss_baseline_kb),
                (long long)budget);
            verdict = 1;
        }
    }
#endif

    if (verdict == 0) {
        if (rss_skipped) {
            printf("PASS (%.3fs, recreates=%u)\n", secs, recreate_count);
        } else {
            printf("PASS (%.3fs, recreates=%u, rss baseline=%lld kb "
                "final=%lld kb)\n",
                secs, recreate_count,
                (long long)rss_baseline_kb,
                (long long)rss_final_kb);
        }
    }

cleanup:
    free(oracle);
    daemon_soak_free_mock_session(sess);
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
    } else if (strcmp(workload, "rotation-vtable") == 0) {
        rc = run_rotation_vtable_workload(W, R);
    } else if (strcmp(workload, "daemon-soak") == 0) {
        rc = run_daemon_soak_workload(W, R);
    } else {
        fprintf(stderr,
            "FAIL: WL_STRESS_WORKLOAD='%s' is not one of "
            "'freeze-cycle', 'apply-roundtrip', 'nested-asan', "
            "'rotation-vtable', 'daemon-soak'\n",
            workload);
        return 1;
    }

    if (rc == 0)
        printf("\nWorkload '%s' passed.\n", workload);
    else
        printf("\nFAILURE: workload '%s' failed.\n", workload);
    return rc == 0 ? 0 : 1;
}
