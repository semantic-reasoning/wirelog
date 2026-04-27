/*
 * test_rss_bounded.c - Issue #598 per-rotation RSS + ledger stability gates
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Per-rotation residual-scope test on top of #597's daemon-soak end-of-run
 * gate.  This test adds three independent signals that are NOT covered by
 * the daemon-soak workload:
 *
 *   1. Per-rotation sampling granularity at R=100 (not just end-of-run).
 *      Uses test_current_rss_kb() (VmRSS / resident_size / WorkingSetSize)
 *      because the monotonic peak sampler is unfit for per-rotation deltas.
 *
 *   2. Cumulative cap with NO 4 MB floor.  The PR-tier gate is 10% growth
 *      or a 1 MB absolute floor (whichever larger), strictly tighter than
 *      #597's max(10%, 4 MB).  Daemon-soak's wider floor is for an end-
 *      of-run signal at small baselines (~2 MB) where 10% would be too
 *      tight; #598's per-rotation gate is the canary for incremental drift
 *      and benefits from the tighter floor.
 *
 *   3. Per-rotation mem_ledger.current_bytes snapshot.  Production rotation
 *      hooks (rotation_standard.c) only call wl_arena_reset; they do NOT
 *      mutate wl_mem_ledger.  This gate asserts ledger.current_bytes is
 *      EXACTLY equal before and after each rotation -- any future change
 *      that introduces ledger churn into the rotation hot path will trip
 *      this canary.  Today the assertion is tautological; it becomes load-
 *      bearing when a ledger-touching rotation strategy lands.
 *
 * Strict per-rotation delta gate (Def#1) is opt-in via WL_RSS_BOUNDED_STRICT=1
 * and runs only in the nightly/release tiers; localhost machines that fail
 * the CoV self-check exit 77 (meson SKIP).
 *
 * The mock-session pattern mirrors test_stress_harness.c's daemon-soak
 * helper (which the architect identified as the canonical pattern for
 * exercising the rotation vtable).  Going through the full
 * parser/optimizer/plan -> wl_session_create() pipeline would only add
 * cost: rotation_ops touches eval_arena and compound_arena, neither
 * of which depends on plan/parser state.  We DO embed a real
 * wl_mem_ledger so the ledger snapshot is meaningful.
 */

#include "../wirelog/arena/arena.h"
#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/mem_ledger.h"
#include "test_rss_util.h"

#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Clang exposes __has_feature; GCC does not.  Polyfill so the
 * conditional below tokenizes cleanly under both compilers (mirrors
 * test_stress_harness.c's daemon-soak skip). */
#ifndef __has_feature
#  define __has_feature(x) 0
#endif

/* Test parameters -- match the daemon-soak workload's surface so a
 * future divergence in one does not silently churn the other.
 * WL_RSS_BOUNDED_MAX_EPOCHS=32 with WL_RSS_BOUNDED_EPOCH_BUFFER=8
 * mirrors the daemon-soak pattern: the saturation predicate fires
 * every ~24 steps so the working set plateaus at the arena's ceiling
 * regardless of R, which is exactly the bounded-RSS contract the
 * cumulative gate asserts.  Without saturation-recycle, R=1000 with
 * K=64 handles per epoch would accumulate ~1.5 MB of legitimate
 * allocator pressure (64 * 24 bytes * 1000 epochs + per-epoch
 * generation overhead) and trip the 10%/1 MB cumulative gate. */
#define WL_RSS_BOUNDED_K_HANDLES   64u
#define WL_RSS_BOUNDED_PAYLOAD_SZ  24u
#define WL_RSS_BOUNDED_WARMUP      5u
#define WL_RSS_BOUNDED_DEFAULT_R   100u
#define WL_RSS_BOUNDED_MAX_EPOCHS  32u
#define WL_RSS_BOUNDED_EPOCH_BUFFER 8u
#define WL_RSS_BOUNDED_EVAL_ARENA  (256u * 1024u)
#define WL_RSS_BOUNDED_COV_CEILING 0.05
#define WL_RSS_BOUNDED_NOISE_FLOOR_KB 256

#define SKIP_EXIT 77

/* Strict per-rotation delta gate threshold (Def#1): rss_after - rss_prev
 * <= rss_prev / 10.  Skipped when delta < WL_RSS_BOUNDED_NOISE_FLOOR_KB
 * to absorb page-size noise. */
#define WL_RSS_BOUNDED_STRICT_PCT 10

static int
parse_env_uint(const char *name, uint32_t default_val, uint32_t max_val,
    uint32_t *out)
{
    const char *s = getenv(name);
    if (!s || s[0] == '\0') {
        *out = default_val;
        return 0;
    }
    char *endp = NULL;
    unsigned long v = strtoul(s, &endp, 10);
    if (endp == s || *endp != '\0' || v == 0 || v > max_val) {
        printf("FAIL: %s='%s' is not a positive integer in [1, %u]\n",
            name, s, max_val);
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

static int
env_flag_enabled(const char *name)
{
    const char *s = getenv(name);
    return (s != NULL && s[0] == '1' && s[1] == '\0') ? 1 : 0;
}

static wl_col_session_t *
make_session(wl_compound_arena_t *arena)
{
    wl_col_session_t *s = (wl_col_session_t *)calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->compound_arena = arena;
    s->eval_arena = wl_arena_create(WL_RSS_BOUNDED_EVAL_ARENA);
    if (!s->eval_arena) {
        free(s);
        return NULL;
    }
    s->rotation_ops = &col_rotation_standard_ops;
    /* Embed a real ledger so the per-rotation snapshot is meaningful.
     * The session owns the ledger lifetime (see internal.h:910). */
    wl_mem_ledger_init(&s->mem_ledger, 0u /* unlimited */);
    return s;
}

static void
free_session(wl_col_session_t *s)
{
    if (!s)
        return;
    if (s->eval_arena)
        wl_arena_free(s->eval_arena);
    free(s);
}

/* Saturation predicate: when current_epoch + EPOCH_BUFFER >= max_epochs,
 * advance via gc_epoch_boundary to keep allocations flowing.  If the
 * advance pushes (or has already pushed) current_epoch to the saturation
 * sentinel (== max_epochs, see compound_arena.c:351), destroy and
 * recreate the arena so the working set plateaus.  Mirrors the pattern
 * in test_stress_harness.c's daemon-soak workload.
 *
 * Returns the (possibly newly recreated) arena via *arena_io.  Updates
 * sess->compound_arena to match.  Returns 0 on success, non-zero on
 * arena recreate failure. */
static int
maybe_recycle_arena(wl_col_session_t *sess, wl_compound_arena_t **arena_io)
{
    wl_compound_arena_t *arena = *arena_io;
    if (arena->current_epoch + WL_RSS_BOUNDED_EPOCH_BUFFER
        < arena->max_epochs) {
        return 0;
    }
    /* Advance the epoch ahead of allocations -- but only if we're not
     * already at the saturation sentinel (gc_epoch_boundary indexes
     * gens[current_epoch] unconditionally; current_epoch == max_epochs
     * is out of bounds). */
    if (arena->current_epoch < arena->max_epochs)
        sess->rotation_ops->gc_epoch_boundary(sess);
    if (arena->current_epoch == arena->max_epochs) {
        /* Tear down and recreate. */
        wl_compound_arena_free(arena);
        arena = wl_compound_arena_create(0xBEEFu, 4096u,
                WL_RSS_BOUNDED_MAX_EPOCHS);
        if (!arena)
            return -1;
        sess->compound_arena = arena;
        *arena_io = arena;
    }
    return 0;
}

/* CoV self-check: skip-on-noise pattern from tests/test_log_perf_gate.c.
 * Computes the coefficient of variation across @n warmup samples; if
 * stdev/mean exceeds the ceiling the run is too noisy to draw a
 * per-rotation conclusion and the test SKIPs. */
static double
compute_cov(const int64_t *samples, size_t n)
{
    if (n == 0)
        return 0.0;
    double sum = 0.0;
    for (size_t i = 0; i < n; i++)
        sum += (double)samples[i];
    double mean = sum / (double)n;
    if (mean <= 0.0)
        return 0.0;
    double sumsq = 0.0;
    for (size_t i = 0; i < n; i++) {
        double d = (double)samples[i] - mean;
        sumsq += d * d;
    }
    double stdev = sqrt(sumsq / (double)n);
    return stdev / mean;
}

int
main(void)
{
    printf("test_rss_bounded (Issue #598)\n");
    printf("=============================\n");

    /* WL_RSS_BOUNDED_R is the post-warmup rotation count.  Default 100
     * matches the brief; the nightly/release tiers override to 500/1000. */
    uint32_t R = 0;
    if (parse_env_uint("WL_RSS_BOUNDED_R", WL_RSS_BOUNDED_DEFAULT_R,
        100000u, &R) != 0)
        return 1;
    int strict = env_flag_enabled("WL_RSS_BOUNDED_STRICT");

    printf("  [R=%u strict=%d K=%u]\n", R, strict, WL_RSS_BOUNDED_K_HANDLES);

    /* Build the mock session + a real wl_mem_ledger embedded in the
     * session.  See file docstring for why we don't go through
     * wl_session_create. */
    wl_compound_arena_t *arena = wl_compound_arena_create(0xBEEFu, 4096u,
            WL_RSS_BOUNDED_MAX_EPOCHS);
    if (!arena) {
        printf("FAIL: arena create\n");
        return 1;
    }
    wl_col_session_t *sess = make_session(arena);
    if (!sess) {
        printf("FAIL: session create\n");
        wl_compound_arena_free(arena);
        return 1;
    }

    int verdict = 0;

    /* ASan compile-time skip of the RSS portion (mirror daemon-soak's
    * pattern from test_stress_harness.c).  Ledger and rotation
    * assertions still run under ASan -- the ledger contract is not
    * confounded by ASan instrumentation, only the VmRSS signal is. */
#if defined(__SANITIZE_ADDRESS__) || __has_feature(address_sanitizer)
    int rss_skipped = 1;
    printf("  [rss-bounded skipped: AddressSanitizer instrumentation "
        "confounds VmRSS signal]\n");
#else
    int rss_skipped = 0;
#endif

    /* ------------------------------------------------------------------
     * Warmup phase: 5 rotations to let the working set stabilize.
     * Sample VmRSS after each warmup rotation; computed CoV gates the
     * subsequent strict-mode delta assertions.
     * ------------------------------------------------------------------ */
    int64_t warmup_rss[WL_RSS_BOUNDED_WARMUP];
    for (uint32_t i = 0; i < WL_RSS_BOUNDED_WARMUP; i++) {
        if (maybe_recycle_arena(sess, &arena) != 0) {
            printf("FAIL: warmup %u arena recycle failed\n", i);
            verdict = 1;
            goto cleanup;
        }
        for (uint32_t k = 0; k < WL_RSS_BOUNDED_K_HANDLES; k++) {
            uint64_t h = wl_compound_arena_alloc(arena,
                    WL_RSS_BOUNDED_PAYLOAD_SZ);
            if (h == WL_COMPOUND_HANDLE_NULL) {
                printf("FAIL: warmup %u handle %u alloc returned NULL\n",
                    i, k);
                verdict = 1;
                goto cleanup;
            }
        }
        sess->rotation_ops->rotate_eval_arena(sess);
        sess->rotation_ops->gc_epoch_boundary(sess);
        warmup_rss[i] = test_current_rss_kb();
    }

    int64_t rss_warmup_kb = warmup_rss[WL_RSS_BOUNDED_WARMUP - 1u];
    uint64_t ledger_warmup_bytes = atomic_load_explicit(
        &sess->mem_ledger.current_bytes, memory_order_relaxed);

    /* CoV self-check.  rss_skipped covers the platform-unavailable and
     * ASan paths -- in both, warmup_rss[i] is -1 and CoV is meaningless. */
    if (!rss_skipped && rss_warmup_kb > 0) {
        double cov = compute_cov(warmup_rss, WL_RSS_BOUNDED_WARMUP);
        if (cov > WL_RSS_BOUNDED_COV_CEILING) {
            printf("[rss-bounded skipped: warmup CoV=%.3f exceeds %.3f "
                "ceiling]\n", cov, WL_RSS_BOUNDED_COV_CEILING);
            free_session(sess);
            wl_compound_arena_free(arena);
            return SKIP_EXIT;
        }
    }
    if (!rss_skipped && rss_warmup_kb < 0) {
        printf("  [rss-bounded skipped: platform sampler unavailable]\n");
        rss_skipped = 1;
    }

    /* ------------------------------------------------------------------
     * Per-rotation gate loop: R rotations post-warmup.
     * ------------------------------------------------------------------ */
    int64_t rss_prev_kb = rss_warmup_kb;
    for (uint32_t r = 0; r < R; r++) {
        if (maybe_recycle_arena(sess, &arena) != 0) {
            printf("FAIL: rotation %u arena recycle failed\n", r);
            verdict = 1;
            goto cleanup;
        }
        for (uint32_t k = 0; k < WL_RSS_BOUNDED_K_HANDLES; k++) {
            uint64_t h = wl_compound_arena_alloc(arena,
                    WL_RSS_BOUNDED_PAYLOAD_SZ);
            if (h == WL_COMPOUND_HANDLE_NULL) {
                printf("FAIL: rotation %u handle %u alloc returned NULL "
                    "(current_epoch=%u max_epochs=%u)\n",
                    r, k, arena->current_epoch, arena->max_epochs);
                verdict = 1;
                goto cleanup;
            }
        }
        sess->rotation_ops->rotate_eval_arena(sess);
        sess->rotation_ops->gc_epoch_boundary(sess);

        int64_t rss_after = rss_skipped ? -1 : test_current_rss_kb();
        uint64_t ledger_after = atomic_load_explicit(
            &sess->mem_ledger.current_bytes, memory_order_relaxed);

        /* Ledger stability gate: production rotation does not allocate
         * via the ledger; any change is a regression. */
        if (ledger_after != ledger_warmup_bytes) {
            printf("FAIL: rotation %u ledger drift: warmup=%" PRIu64
                " after=%" PRIu64 " (delta=%" PRId64 ")\n",
                r, ledger_warmup_bytes, ledger_after,
                (int64_t)ledger_after - (int64_t)ledger_warmup_bytes);
            verdict = 1;
            goto cleanup;
        }

        if (!rss_skipped && rss_after > 0) {
            /* PR-tier cumulative gate (Def#2): 10% growth or 1 MB floor. */
            int64_t allowance_pct = rss_warmup_kb / 10;
            int64_t allowance_floor = 1024; /* KB */
            int64_t allowance = (allowance_pct > allowance_floor)
                ? allowance_pct : allowance_floor;
            int64_t budget = rss_warmup_kb + allowance;
            if (rss_after > budget) {
                printf("FAIL: rotation %u rss-cumulative warmup=%" PRId64
                    " kb after=%" PRId64 " kb delta=%" PRId64
                    " kb gate=%" PRId64 " kb\n",
                    r, rss_warmup_kb, rss_after,
                    rss_after - rss_warmup_kb, budget);
                verdict = 1;
                goto cleanup;
            }

            /* Strict per-rotation delta gate (Def#1, nightly-only).
            * Skip when delta is below the page-size noise floor. */
            if (strict && rss_prev_kb > 0) {
                int64_t delta = rss_after - rss_prev_kb;
                if (delta >= WL_RSS_BOUNDED_NOISE_FLOOR_KB) {
                    int64_t strict_budget
                        = rss_prev_kb / WL_RSS_BOUNDED_STRICT_PCT;
                    if (delta > strict_budget) {
                        printf("FAIL: rotation %u rss-strict prev=%"
                            PRId64 " kb after=%" PRId64
                            " kb delta=%" PRId64 " kb budget=%" PRId64
                            " kb\n",
                            r, rss_prev_kb, rss_after, delta,
                            strict_budget);
                        verdict = 1;
                        goto cleanup;
                    }
                }
            }

            rss_prev_kb = rss_after;
        }
    }

    if (rss_skipped) {
        printf("PASS (R=%u, ledger stable @ %" PRIu64 " bytes, rss skipped)\n",
            R, ledger_warmup_bytes);
    } else {
        printf("PASS (R=%u, ledger stable @ %" PRIu64
            " bytes, rss warmup=%" PRId64 " kb final=%" PRId64 " kb)\n",
            R, ledger_warmup_bytes, rss_warmup_kb, rss_prev_kb);
    }

cleanup:
    free_session(sess);
    wl_compound_arena_free(arena);
    return verdict;
}
