/*
 * test_rotate_latency.c - Issue #599 rotate-latency p50/p99/p999 gate
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Companion to #598's RSS-bounded gate.  #598 asserts that the working
 * set is bounded across rotations; #599 asserts that each rotation is
 * fast.  Both gate the same rotation_ops vtable (selected by
 * WIRELOG_ROTATION env var; default `col_rotation_standard_ops`,
 * `pinned` selects `col_rotation_pinned_ops`); the workload shape (K=64
 * handles, 24-byte payload, compound_arena) is identical so a
 * divergence in either gate lights up a real regression rather than a
 * workload-shape change.
 *
 * The issue body literally says "p50 < 10ms, p99 < 100ms, p999 < 500ms".
 * Those values are wrong by ~1000x: standard_rotate_eval_arena is just
 * wl_arena_reset (one bump-pointer reset), and gc_epoch_boundary walks a
 * per-epoch generation table; the steady-state cost of the pair on
 * x86_64 is sub-microsecond.  This test uses microsecond budgets
 * (10/100/500 us) so the gate actually catches catastrophic regressions
 * rather than allowing five orders of magnitude of slowdown.  See
 * STRESS_BASELINE.md "Gate sensitivity" for the headroom analysis.
 *
 * W=1 mandated: rotation hooks walk the eval arena bump pointer + the
 * per-epoch generation table, neither concurrency-safe.  See
 * STRESS_BASELINE.md "Rotation hooks" section.
 *
 * Recycle-outside-timed-window design: the compound arena caps
 * max_epochs at WL_COMPOUND_DEFAULT_MAX_EPOCHS (4096; see
 * wirelog/arena/compound_arena.c).  At larger N the loop would hit
 * the saturation sentinel; the test sizes the arena to the platform
 * max and recycles (full destroy + recreate) OUTSIDE the timed window
 * when current_epoch nears exhaustion.  Only
 * rotate_eval_arena + gc_epoch_boundary is timed -- recycle is a
 * workload-shape event that #598's RSS-bounded test gates separately,
 * so steady-state semantics on the rotation hot path are preserved.
 * Warmup is capped at WL_ROTATE_LATENCY_RECYCLE_AT - 1 in
 * parse_env_uint() so warmup never crosses a recycle boundary; the
 * call site uses the no-recycle path consequently.
 *
 * Skip behavior:
 *   - Sanitizer build (compile-time): wall-clock signal is invalidated
 *     by ASan/TSan instrumentation; main() short-circuits to exit 77.
 *   - WIRELOG_PERF_GATE != "1": opt-in gate, default-suite invocation
 *     SKIPs (mirrors test_log_perf_gate.c).
 *   - cpufreq governor != "performance" (Linux): SKIP via
 *     wl_perf_stability_env_ok.
 *   - macOS / BSD / Windows: wl_perf_stability_env_ok returns 0; SKIP.
 *   - Warmup CoV > WL_ROTATE_LATENCY_COV_CEILING: jittery host; SKIP.
 *
 * The mock-session pattern mirrors test_rss_bounded.c (same helpers,
 * recreated locally to avoid a tests-of-tests dependency).
 */

#include "test_perf_util.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef __has_feature
#  define __has_feature(x) 0
#endif

#define SKIP_EXIT 77

/* Sanitizer compile-time SKIP -- mirrors the pattern at
 * test_rss_bounded.c:246-252 but covers BOTH ASan and TSan because
 * wall-clock measurements are confounded by either. */
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__) \
    || __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)

int
main(void)
{
    fprintf(stderr,
        "test_rotate_latency: SKIP: sanitizer build invalidates "
        "wall-clock timing\n");
    return SKIP_EXIT;
}

#else /* !sanitizer */

#include "../wirelog/arena/arena.h"
#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/mem_ledger.h"

/* Workload constants -- mirror #598 so a divergence between the
 * RSS-bounded gate and the latency gate is impossible without a
 * deliberate edit here. */
#define WL_ROTATE_LATENCY_K_HANDLES   64u
#define WL_ROTATE_LATENCY_PAYLOAD_SZ  24u
#define WL_ROTATE_LATENCY_DEFAULT_N   1000u
#define WL_ROTATE_LATENCY_DEFAULT_WARMUP 100u
#define WL_ROTATE_LATENCY_EVAL_ARENA  ((size_t)(256u * 1024u))
/* CoV ceiling deliberately wider than test_rss_bounded.c's 0.05.  The
 * rotation hot path runs at ~30-50 ns per sample on x86_64, which is
 * within an order of magnitude of clock_gettime's resolution; sub-ns
 * jitter trivially yields 25-40% CoV at this scale even on a quiet
 * dedicated host.  0.50 catches gross instability (process preemption,
 * cache thrash) without producing false SKIPs from clock-resolution
 * noise. */
#define WL_ROTATE_LATENCY_COV_CEILING 0.50

/* Recalibrated budgets (us, not the issue body's literal "ms").
 * Reasoning:
 *   - standard_rotate_eval_arena = wl_arena_reset (single bump-ptr reset)
 *   - standard_gc_epoch_boundary = walks the per-epoch gens table at K=64
 *   - both run at ~30-60 ns on x86_64 in steady state (observed locally
 *     on a Linux/x86_64 host: p50=33-35 ns, p99=46-62 ns)
 * 10/100/500 us is ~200x-3000x looser than observed: enough headroom
 * to absorb dispatcher noise, slow CI hardware, and small future
 * incremental cost without allowing a five-orders-of-magnitude
 * slowdown to slip through.  Tighten if the rotation hot path is ever
 * benchmarked on dedicated perf hardware and a realistic budget is
 * established. */
#define WL_ROTATE_LATENCY_P50_MAX_NS  10000u    /* 10 us  */
#define WL_ROTATE_LATENCY_P99_MAX_NS  100000u   /* 100 us */
#define WL_ROTATE_LATENCY_P999_MAX_NS 500000u   /* 500 us */

/* The compound arena caps max_epochs at WL_COMPOUND_DEFAULT_MAX_EPOCHS
 * (4096; see wirelog/arena/compound_arena.c:107).  Anything above that
 * is silently clamped, so a single arena cannot host N+warmup > ~4030
 * iterations without running into the saturation sentinel.  We size
 * the arena to the platform max and recycle (full destroy + recreate)
 * untimed when the epoch budget is near exhaustion -- the recycle is
 * outside the timed window, so steady-state semantics on the rotation
 * hot path are preserved. */
#define WL_ROTATE_LATENCY_MAX_EPOCHS  4096u
#define WL_ROTATE_LATENCY_RECYCLE_AT  (WL_ROTATE_LATENCY_MAX_EPOCHS - 64u)

/* Recycle the compound arena out-of-band (untimed).  Mirrors the
 * tear-down half of test_rss_bounded.c::maybe_recycle_arena.  Returns
 * 0 on success, non-zero on recreate failure. */
static int
recycle_arena(wl_col_session_t *sess, wl_compound_arena_t **arena_io)
{
    wl_compound_arena_free(*arena_io);
    wl_compound_arena_t *fresh = wl_compound_arena_create(0xBEEFu, 4096u,
            WL_ROTATE_LATENCY_MAX_EPOCHS);
    if (!fresh)
        return -1;
    *arena_io = fresh;
    sess->compound_arena = fresh;
    return 0;
}

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
        fprintf(stderr, "FAIL: %s='%s' is not a positive integer in [1, %u]\n",
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

/* Parse WIRELOG_ROTATION into a rotation_ops vtable pointer.  Mirrors
 * tests/test_stress_harness.c::parse_rotation_strategy and
 * session.c:437-442's strict-fail parser: unknown values are a hard
 * FAIL.  Unset env = default ("standard").  Returns 0 on success, -1
 * on parse error. */
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
    if (strcmp(rot_env, "pinned") == 0) {
        *out_ops = &col_rotation_pinned_ops;
        *out_name = "pinned";
        return 0;
    }
    fprintf(stderr,
        "FAIL: WIRELOG_ROTATION='%s' is not 'standard' or 'pinned'\n",
        rot_env);
    return -1;
}

static wl_col_session_t *
make_session(wl_compound_arena_t *arena, const col_rotation_ops_t *ops)
{
    wl_col_session_t *s = (wl_col_session_t *)calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->compound_arena = arena;
    s->eval_arena = wl_arena_create(WL_ROTATE_LATENCY_EVAL_ARENA);
    if (!s->eval_arena) {
        free(s);
        return NULL;
    }
    s->rotation_ops = ops;
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

/* Print the slowest 5 sample positions and values to stderr.  Triage
 * aid: a clustered tail (consecutive indices) suggests a cold-cache
 * effect; scattered indices suggest dispatcher jitter.  Uses a fixed
 * 5-slot stack tracker to avoid an allocation on the failure path
 * (where heap pressure may already be the failure cause). */
#define WL_ROTATE_LATENCY_TOPK 5u

static void
print_slowest_k(const wl_perf_ns_t *samples, size_t n)
{
    if (n == 0)
        return;
    size_t top_idx[WL_ROTATE_LATENCY_TOPK];
    wl_perf_ns_t top_val[WL_ROTATE_LATENCY_TOPK];
    size_t top_n = 0;

    for (size_t i = 0; i < n; i++) {
        wl_perf_ns_t v = samples[i];
        if (top_n < WL_ROTATE_LATENCY_TOPK) {
            /* Insert in descending order to keep top_val[0] = biggest. */
            size_t pos = top_n;
            while (pos > 0 && top_val[pos - 1] < v) {
                top_val[pos] = top_val[pos - 1];
                top_idx[pos] = top_idx[pos - 1];
                pos--;
            }
            top_val[pos] = v;
            top_idx[pos] = i;
            top_n++;
            continue;
        }
        /* top_val[top_n - 1] is the smallest of the current top-k.
         * Skip any sample not strictly bigger. */
        if (v <= top_val[top_n - 1])
            continue;
        size_t pos = top_n - 1;
        while (pos > 0 && top_val[pos - 1] < v) {
            top_val[pos] = top_val[pos - 1];
            top_idx[pos] = top_idx[pos - 1];
            pos--;
        }
        top_val[pos] = v;
        top_idx[pos] = i;
    }

    fprintf(stderr, "  slowest-%zu (index=value_ns):", top_n);
    for (size_t i = 0; i < top_n; i++)
        fprintf(stderr, " [%zu]=%" PRIu64, top_idx[i], top_val[i]);
    fprintf(stderr, "\n");
}

int
main(void)
{
    fprintf(stderr, "test_rotate_latency (Issue #599)\n");
    fprintf(stderr, "================================\n");

    /* Opt-in only; mirrors test_log_perf_gate.c.  Shared CI runners
     * exhibit several-percent jitter that the 100 us p99 budget cannot
     * absorb -- explicit opt-in keeps default CI silent. */
    const char *opt_in = getenv("WIRELOG_PERF_GATE");
    if (!opt_in || !*opt_in || opt_in[0] == '0') {
        fprintf(stderr,
            "test_rotate_latency: SKIP: set WIRELOG_PERF_GATE=1 to run "
            "(designed for dedicated perf hardware, not shared CI runners)\n");
        return SKIP_EXIT;
    }

    /* wl_perf_stability_env_ok prints a granular SKIP diagnostic
     * (sysfs missing vs governor wrong vs unsupported platform) on
     * failure; we only need to forward the exit code. */
    if (!wl_perf_stability_env_ok())
        return SKIP_EXIT;

    uint32_t N = 0;
    uint32_t warmup = 0;
    if (parse_env_uint("WL_ROTATE_LATENCY_N", WL_ROTATE_LATENCY_DEFAULT_N,
        1000000u, &N) != 0)
        return 1;
    if (parse_env_uint("WL_ROTATE_LATENCY_WARMUP",
        WL_ROTATE_LATENCY_DEFAULT_WARMUP, 100000u, &warmup) != 0)
        return 1;
    /* Cap warmup at RECYCLE_AT - 1 so warmup never crosses a recycle
     * boundary (warmup must complete before any recycle to preserve
     * steady-state semantics at measurement start).  Both
     * WL_ROTATE_LATENCY_DEFAULT_WARMUP (100) and parse_env_uint's
     * upper bound (100000) sit well above RECYCLE_AT (4032), so a
     * misconfigured WL_ROTATE_LATENCY_WARMUP must still be rejected. */
    if (warmup >= WL_ROTATE_LATENCY_RECYCLE_AT) {
        fprintf(stderr,
            "FAIL: WL_ROTATE_LATENCY_WARMUP=%u must be < %u "
            "(RECYCLE_AT); warmup must finish before any recycle to "
            "preserve steady-state semantics\n",
            warmup, WL_ROTATE_LATENCY_RECYCLE_AT);
        return 1;
    }
    int strict = env_flag_enabled("WL_ROTATE_LATENCY_STRICT");

    const col_rotation_ops_t *ops = NULL;
    const char *rot_name = NULL;
    if (parse_rotation_strategy(&ops, &rot_name) != 0)
        return 1;

    fprintf(stderr, "  [N=%u warmup=%u strict=%d K=%u rotation=%s]\n",
        N, warmup, strict, WL_ROTATE_LATENCY_K_HANDLES, rot_name);

    /* Build the mock session at the platform-max epoch budget.  The
     * arena recycle inside the loop is OUTSIDE the timed window, so
     * the measurement still observes the steady-state rotation hot
     * path; the arena recycle is a workload-shape event that #598's
     * RSS-bounded test gates separately. */
    wl_compound_arena_t *arena = wl_compound_arena_create(0xBEEFu, 4096u,
            WL_ROTATE_LATENCY_MAX_EPOCHS);
    if (!arena) {
        fprintf(stderr, "FAIL: arena create (max_epochs=%u)\n",
            WL_ROTATE_LATENCY_MAX_EPOCHS);
        return 1;
    }
    wl_col_session_t *sess = make_session(arena, ops);
    if (!sess) {
        fprintf(stderr, "FAIL: session create\n");
        wl_compound_arena_free(arena);
        return 1;
    }

    int verdict = 0;
    wl_perf_ns_t *samples = (wl_perf_ns_t *)calloc(N, sizeof(*samples));
    wl_perf_ns_t *unsorted = NULL;
    if (!samples) {
        fprintf(stderr, "FAIL: samples calloc N=%u\n", N);
        verdict = 1;
        goto cleanup;
    }

    /* Warmup: untimed; lets caches/branch predictors stabilize.
     * Warmup is capped (parse-time) to RECYCLE_AT - 1, so the recycle
     * branch is dead code here; the measurement loop owns recycle. */
    for (uint32_t i = 0; i < warmup; i++) {
        for (uint32_t k = 0; k < WL_ROTATE_LATENCY_K_HANDLES; k++) {
            uint64_t h = wl_compound_arena_alloc(arena,
                    WL_ROTATE_LATENCY_PAYLOAD_SZ);
            if (h == WL_COMPOUND_HANDLE_NULL) {
                fprintf(stderr, "FAIL: warmup %u handle %u alloc null "
                    "(current_epoch=%u max_epochs=%u)\n",
                    i, k, arena->current_epoch, arena->max_epochs);
                verdict = 1;
                goto cleanup;
            }
        }
        sess->rotation_ops->rotate_eval_arena(sess);
        sess->rotation_ops->gc_epoch_boundary(sess);
    }

    /* Measurement loop.  Allocations and arena recycle are OUTSIDE
     * the timed window; only the rotate_eval_arena + gc_epoch_boundary
     * pair is timed, which is the sole production hot path under
     * #600's vtable. */
    for (uint32_t i = 0; i < N; i++) {
        if (arena->current_epoch >= WL_ROTATE_LATENCY_RECYCLE_AT) {
            if (recycle_arena(sess, &arena) != 0) {
                fprintf(stderr, "FAIL: rotation %u arena recycle\n", i);
                verdict = 1;
                goto cleanup;
            }
        }
        for (uint32_t k = 0; k < WL_ROTATE_LATENCY_K_HANDLES; k++) {
            uint64_t h = wl_compound_arena_alloc(arena,
                    WL_ROTATE_LATENCY_PAYLOAD_SZ);
            if (h == WL_COMPOUND_HANDLE_NULL) {
                fprintf(stderr, "FAIL: rotation %u handle %u alloc null "
                    "(current_epoch=%u max_epochs=%u)\n",
                    i, k, arena->current_epoch, arena->max_epochs);
                verdict = 1;
                goto cleanup;
            }
        }
        wl_perf_ns_t t0 = wl_perf_now_ns();
        sess->rotation_ops->rotate_eval_arena(sess);
        sess->rotation_ops->gc_epoch_boundary(sess);
        wl_perf_ns_t t1 = wl_perf_now_ns();
        samples[i] = t1 - t0;
    }

    /* Jitter self-check: CoV across the first min(100, N) samples.
     * Ceiling is WL_ROTATE_LATENCY_COV_CEILING (0.50); see the macro
     * comment for the wider-than-RSS-gate rationale. */
    size_t cov_n = (N < 100u) ? (size_t)N : 100u;
    double cov = wl_perf_cov(samples, cov_n);
    if (cov > WL_ROTATE_LATENCY_COV_CEILING) {
        fprintf(stderr,
            "test_rotate_latency: SKIP: steady-state CoV=%.3f exceeds "
            "%.3f ceiling (jittery host)\n",
            cov, WL_ROTATE_LATENCY_COV_CEILING);
        free(samples);
        free_session(sess);
        wl_compound_arena_free(arena);
        return SKIP_EXIT;
    }

    /* Snapshot the un-sorted samples so positional triage info
     * survives the qsort below.  Free is unconditional in cleanup. */
    unsorted = (wl_perf_ns_t *)calloc(N, sizeof(*unsorted));
    if (!unsorted) {
        fprintf(stderr, "FAIL: unsorted calloc N=%u\n", N);
        verdict = 1;
        goto cleanup;
    }
    memcpy(unsorted, samples, (size_t)N * sizeof(*unsorted));

    /* Sort once, then read off all percentiles. */
    qsort(samples, N, sizeof(samples[0]), wl_perf_cmp_ns);
    wl_perf_ns_t p50 = wl_perf_percentile_ns(samples, N, 0.50);
    wl_perf_ns_t p95 = wl_perf_percentile_ns(samples, N, 0.95);
    wl_perf_ns_t p99 = wl_perf_percentile_ns(samples, N, 0.99);
    /* p999 is meaningful only when N is at least 10000; at smaller N
     * the floor(N*0.999) index degenerates to "max" which is not a
     * percentile estimate.  PR-tier prints "p999=N/A" instead. */
    int p999_meaningful = (N >= 10000u);
    wl_perf_ns_t p999 = p999_meaningful
        ? wl_perf_percentile_ns(samples, N, 0.999)
        : 0;
    wl_perf_ns_t pmax = samples[N - 1];

    /* Always-printed distribution: lets developers see actual numbers
     * even on PASS so a slow drift is visible before it trips the
     * gate.  p999 is suppressed when N<10000 (insufficient samples). */
    if (p999_meaningful) {
        fprintf(stdout,
            "[rotate-latency] N=%u p50=%" PRIu64 "ns p95=%" PRIu64
            "ns p99=%" PRIu64 "ns p999=%" PRIu64 "ns max=%" PRIu64
            "ns cov=%.3f rotation=%s\n",
            N, p50, p95, p99, p999, pmax, cov, rot_name);
    } else {
        fprintf(stdout,
            "[rotate-latency] N=%u p50=%" PRIu64 "ns p95=%" PRIu64
            "ns p99=%" PRIu64 "ns p999=N/A (insufficient samples) "
            "max=%" PRIu64 "ns cov=%.3f rotation=%s\n",
            N, p50, p95, p99, pmax, cov, rot_name);
    }
    fflush(stdout);

    int p50_breach = (p50 > WL_ROTATE_LATENCY_P50_MAX_NS);
    int p99_breach = (p99 > WL_ROTATE_LATENCY_P99_MAX_NS);
    /* Only enforce p999 when N is large enough to make it meaningful
     * AND the strict flag is set.  Strict-without-meaningful-N
     * (i.e. nightly-style strict at PR-tier N) silently skips the
     * p999 check rather than gating on samples[N-1]. */
    int p999_breach = strict && p999_meaningful
        && (p999 > WL_ROTATE_LATENCY_P999_MAX_NS);

    if (p50_breach) {
        fprintf(stderr,
            "FAIL: p50=%" PRIu64 "ns exceeds budget %u ns\n",
            p50, WL_ROTATE_LATENCY_P50_MAX_NS);
    }
    if (p99_breach) {
        fprintf(stderr,
            "FAIL: p99=%" PRIu64 "ns exceeds budget %u ns\n",
            p99, WL_ROTATE_LATENCY_P99_MAX_NS);
    }
    if (p999_breach) {
        fprintf(stderr,
            "FAIL: p999=%" PRIu64 "ns exceeds budget %u ns "
            "(strict)\n",
            p999, WL_ROTATE_LATENCY_P999_MAX_NS);
    }

    if (p50_breach || p99_breach || p999_breach) {
        /* Triage aid: print the 5 slowest sample positions from the
         * un-sorted buffer.  Clustered indices (consecutive) point at
         * a cold-cache or recycle artifact; scattered indices point
         * at dispatcher jitter. */
        print_slowest_k(unsorted, (size_t)N);
        verdict = 1;
        goto cleanup;
    }

    if (p999_meaningful) {
        fprintf(stderr,
            "PASS (N=%u, p50=%" PRIu64 "ns p99=%" PRIu64 "ns p999=%"
            PRIu64 "ns)\n",
            N, p50, p99, p999);
    } else {
        fprintf(stderr,
            "PASS (N=%u, p50=%" PRIu64 "ns p99=%" PRIu64 "ns)\n",
            N, p50, p99);
    }

cleanup:
    free(unsorted);
    free(samples);
    free_session(sess);
    wl_compound_arena_free(arena);
    return verdict;
}

#endif /* sanitizer guard */
