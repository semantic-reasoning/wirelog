/*
 * tests/test_log_perf_gate.c - Release-mode perf gate for WL_LOG disabled path.
 *
 * Objective: defend the "logging system must absolutely never impact
 * performance" invariant with a runnable, deterministic CI check.
 *
 * Methodology:
 *   - Two noinline variants run over 100M iterations after 1M warmup:
 *     run_nolog does a tight loop with no WL_LOG call; run_wllog places
 *     WL_LOG(JOIN, TRACE, "%d %d %d", a, b, c) in the body with runtime
 *     threshold forced to zero. Both take the same argument signature so
 *     the compiler cannot prove one side is "more alive" than the other.
 *   - Each variant runs >=5 trials; we compare medians.
 *   - Pre-conditions that would invalidate the measurement cause an
 *     explicit Meson SKIP (exit code 77), never a silent pass:
 *       - Any CPU not on the 'performance' governor.
 *       - Build not reaching TRACE in the compile-time ceiling (without
 *         that, the site is compiled out and the test is vacuous).
 *   - Fail condition (OR): wall delta > 1% OR per-iter delta > 1 ns.
 *     Either signal breached = regression.
 *
 * This is the ONLY objective gate on the principle. Keep it honest.
 */

#define _POSIX_C_SOURCE 200809L

#include "wirelog/util/log.h"

#include "../bench/bench_util.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
    ITERS    = 100 * 1000 * 1000,
    WARMUP   = 1 * 1000 * 1000,
    TRIALS   = 5,
    SKIP_EXIT = 77,   /* Meson "skip" exit code. */
};

__attribute__((noinline))
static uint64_t
run_nolog(uint64_t iters, int a, int b, int c)
{
    volatile uint64_t acc = 0;
    for (uint64_t i = 0; i < iters; ++i) {
        acc += (uint64_t)(a + b + c + (int)i);
    }
    return acc;
}

__attribute__((noinline))
static uint64_t
run_wllog(uint64_t iters, int a, int b, int c)
{
    volatile uint64_t acc = 0;
    for (uint64_t i = 0; i < iters; ++i) {
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_TRACE, "%d %d %d", a, b, c);
        acc += (uint64_t)(a + b + c + (int)i);
    }
    return acc;
}

static int
cmp_double_(const void *a, const void *b)
{
    double x = *(const double *)a;
    double y = *(const double *)b;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

static double
median_ms_(double *vals, int n)
{
    qsort(vals, (size_t)n, sizeof(vals[0]), cmp_double_);
    return (n & 1) ? vals[n / 2] : 0.5 * (vals[n / 2 - 1] + vals[n / 2]);
}

/*
 * Reads /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor (Linux only).
 * Returns true iff the governor is "performance". On non-Linux or read
 * failure, returns false so the caller can emit an explicit SKIP.
 */
static bool
cpufreq_is_performance_(void)
{
#if defined(__linux__)
    FILE *f = fopen("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
            "r");
    if (!f) return false;
    char buf[64] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    if (n == 0) return false;
    /* Strip trailing newline. */
    if (buf[n - 1] == '\n') buf[n - 1] = '\0';
    return strcmp(buf, "performance") == 0;
#else
    return false;
#endif
}

int
main(void)
{
    /* Opt-in only: shared CI runners exhibit several-percent wall-clock
     * noise which is above the 1% budget. Explicit opt-in lets operators
     * run the gate on dedicated perf hardware (merge-to-main / nightly)
     * while keeping per-PR CI silent. */
    const char *opt_in = getenv("WIRELOG_PERF_GATE");
    if (!opt_in || !*opt_in || opt_in[0] == '0') {
        fprintf(stderr,
            "test_log_perf_gate: SKIP: set WIRELOG_PERF_GATE=1 to run "
            "(designed for dedicated perf hardware, not shared CI runners)\n");
        return SKIP_EXIT;
    }

    /* Skip if the compile-time ceiling compiled the WL_LOG call out - the
     * test would be vacuous. This build-configuration is detected via the
     * preprocessor. */
    if (WL_LOG_COMPILE_MAX_LEVEL < WL_LOG_TRACE) {
        fprintf(stderr,
            "test_log_perf_gate: SKIP: WL_LOG_COMPILE_MAX_LEVEL=%d < TRACE; "
            "configure with -Dwirelog_log_max_level=trace to exercise the runtime guard\n",
            (int)WL_LOG_COMPILE_MAX_LEVEL);
        return SKIP_EXIT;
    }

    if (!cpufreq_is_performance_()) {
        fprintf(stderr,
            "test_log_perf_gate: SKIP: cpufreq governor is not 'performance'; "
            "set scaling_governor=performance on cpu0 to run this gate\n");
        return SKIP_EXIT;
    }

    /* Force threshold to 0 so WL_LOG's runtime branch evaluates false. */
    wl_log_init();
    memset((void *)wl_log_thresholds, 0, WL_LOG_SEC__COUNT);

    int a = 1, b = 2, c = 3;

    /* Warm up both variants. */
    (void)run_nolog(WARMUP, a, b, c);
    (void)run_wllog(WARMUP, a, b, c);

    double t_nolog[TRIALS];
    double t_wllog[TRIALS];

    for (int i = 0; i < TRIALS; ++i) {
        bench_time_t s = bench_time_now();
        uint64_t r = run_nolog(ITERS, a, b, c);
        bench_time_t e = bench_time_now();
        t_nolog[i] = bench_time_diff_ms(s, e);
        (void)r;
    }
    for (int i = 0; i < TRIALS; ++i) {
        bench_time_t s = bench_time_now();
        uint64_t r = run_wllog(ITERS, a, b, c);
        bench_time_t e = bench_time_now();
        t_wllog[i] = bench_time_diff_ms(s, e);
        (void)r;
    }

    double med_nolog = median_ms_(t_nolog, TRIALS);
    double med_wllog = median_ms_(t_wllog, TRIALS);

    double wall_delta_frac = (med_wllog - med_nolog) / med_nolog;
    double per_iter_ns =
        (med_wllog - med_nolog) * 1e6 / (double)ITERS; /* ms -> ns per iter */

    fprintf(stderr,
        "test_log_perf_gate: iters=%d trials=%d\n"
        "  med_nolog = %.3f ms\n"
        "  med_wllog = %.3f ms\n"
        "  wall_delta = %.4f (%.2f%%)\n"
        "  per_iter_delta = %.4f ns\n",
        ITERS, TRIALS,
        med_nolog, med_wllog,
        wall_delta_frac, wall_delta_frac * 100.0,
        per_iter_ns);

    /* Fail if EITHER signal breaches: wall-delta > 1% OR per-iter > 1 ns. */
    int fail = 0;
    if (wall_delta_frac > 0.01) {
        fprintf(stderr,
            "test_log_perf_gate: FAIL: wall delta %.4f exceeds 1%% budget\n",
            wall_delta_frac);
        fail = 1;
    }
    if (per_iter_ns > 1.0) {
        fprintf(stderr,
            "test_log_perf_gate: FAIL: per-iter delta %.4f ns exceeds 1 ns budget\n",
            per_iter_ns);
        fail = 1;
    }
    wl_log_shutdown();
    if (fail) return 1;

    puts("test_log_perf_gate OK");
    return 0;
}
