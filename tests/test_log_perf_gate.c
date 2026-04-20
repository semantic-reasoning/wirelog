/*
 * tests/test_log_perf_gate.c - Release-mode perf gate for WL_LOG disabled path.
 *
 * Objective: defend the "logging system must absolutely never impact
 * performance" invariant with a runnable, deterministic CI check.
 *
 * Methodology:
 *   - Two BENCH_NOINLINE variants run over 100M iterations after 1M warmup:
 *     run_nolog does a tight loop with no WL_LOG call; run_wllog places
 *     WL_LOG(JOIN, TRACE, "%d %d %d", a, b, c) in the body with runtime
 *     threshold forced to zero. Both take the same argument signature so
 *     the compiler cannot prove one side is "more alive" than the other.
 *   - Each variant runs TRIALS trials; we compare medians.
 *   - Before any measurement, bench_stability_prep() pins the thread to
 *     CPU 0 and raises the process priority class (Windows); on Linux
 *     stability is the operator's responsibility via cpufreq governor +
 *     taskset/chrt, which is pre-checked via
 *     /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor.
 *   - After baseline trials, the gate computes the coefficient of
 *     variation (CoV) of the nolog baseline. If CoV exceeds
 *     MAX_BASELINE_COV, the measurement is deemed too noisy and we SKIP
 *     (not FAIL) - this prevents shared CI runners from failing the
 *     build on kernel-scheduler noise while still catching real
 *     regressions on dedicated hardware.
 *   - Pre-conditions that would invalidate the measurement cause an
 *     explicit Meson SKIP (exit code 77), never a silent pass:
 *       * WIRELOG_PERF_GATE=1 not set (per-PR CI opt-out).
 *       * Build not reaching TRACE in the compile-time ceiling (without
 *         that, the site is compiled out and the test is vacuous).
 *       * Linux: cpufreq governor not 'performance'.
 *       * Windows: bench_stability_prep() failed (affinity/priority
 *         could not be set).
 *       * Other hosts (macOS, BSD): no Windows-equivalent stability
 *         design yet - SKIP by default.
 *       * Baseline CoV > MAX_BASELINE_COV (noise exceeds signal budget).
 *   - Fail condition (AND): wall delta > 1% AND per-iter delta > 1 ns.
 *     AND rather than OR because the baseline loop is intentionally
 *     trivial (`acc += a+b+c+i`, ~300 ps per iter on a 3 GHz host), so
 *     a single extra cycle for the threshold-byte load trips wall-delta
 *     without any real regression. per-iter-ns is the load-bearing
 *     invariant ("WL_LOG adds less than a nanosecond per disabled
 *     call"); wall-delta is retained as a companion sanity check.
 *     Both must trip together to constitute a regression. Thresholds
 *     are platform-independent: the invariant is "no measurable cost",
 *     not "no cost beyond platform X's noise floor".
 *
 * This is the ONLY objective gate on the principle. Keep it honest.
 */

#define _POSIX_C_SOURCE 200809L

#include "wirelog/util/log.h"

#include "../bench/bench_util.h"

#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
    ITERS    = 100 * 1000 * 1000,
    WARMUP   = 1 * 1000 * 1000,
    TRIALS   = 9,                 /* odd -> median is a real sample */
    SKIP_EXIT = 77,               /* Meson "skip" exit code. */
};

/* Baseline coefficient-of-variation ceiling. If the nolog trials'
 * stdev/mean exceeds this, the run is too noisy to draw a conclusion
 * at the 1% wall budget and the gate SKIPs. 3% is deliberately loose:
 * dedicated perf hardware comfortably stays under 1%; shared CI
 * runners typically straddle 2-5%; >3% means background load is
 * likely contaminating the measurement. */
#define MAX_BASELINE_COV 0.03

BENCH_NOINLINE
static uint64_t
run_nolog(uint64_t iters, int a, int b, int c)
{
    volatile uint64_t acc = 0;
    for (uint64_t i = 0; i < iters; ++i) {
        acc += (uint64_t)(a + b + c + (int)i);
    }
    return acc;
}

BENCH_NOINLINE
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

static double
mean_ms_(const double *vals, int n)
{
    double s = 0.0;
    for (int i = 0; i < n; ++i) s += vals[i];
    return s / (double)n;
}

/* Population stdev (n, not n-1) — we have the full sample, not an
 * estimator. */
static double
stdev_ms_(const double *vals, int n, double mean)
{
    double ss = 0.0;
    for (int i = 0; i < n; ++i) {
        double d = vals[i] - mean;
        ss += d * d;
    }
    return sqrt(ss / (double)n);
}

/*
 * Reads /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor (Linux only).
 * Returns true iff the governor is "performance". On non-Linux the Linux
 * governor does not apply; the caller dispatches separately.
 */
#if defined(__linux__)
static bool
cpufreq_is_performance_(void)
{
    FILE *f = fopen("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
            "r");
    if (!f) return false;
    char buf[64] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    if (n == 0) return false;
    if (buf[n - 1] == '\n') buf[n - 1] = '\0';
    return strcmp(buf, "performance") == 0;
}
#endif

/*
 * Platform-dispatched stability pre-check. Returns true iff the host
 * is configured well enough to trust the subsequent measurement. On
 * false the caller prints a platform-specific SKIP diagnostic.
 */
static bool
stability_env_ok_(void)
{
#if defined(__linux__)
    return cpufreq_is_performance_();
#elif defined(_WIN32)
    /* Active stability on Windows: pin + priority. If the kernel
     * refuses any of these (unusual outside sandboxes), SKIP. */
    return bench_stability_prep() == 1;
#else
    /* macOS / BSD / unknown: no Windows-equivalent design shipped
     * yet; conservative SKIP keeps the gate from producing false
     * signal on untested hosts. */
    return false;
#endif
}

static void
print_skip_platform_(void)
{
#if defined(__linux__)
    fprintf(stderr,
        "test_log_perf_gate: SKIP: cpufreq governor is not 'performance'; "
        "set scaling_governor=performance on cpu0 to run this gate\n");
#elif defined(_WIN32)
    fprintf(stderr,
        "test_log_perf_gate: SKIP: bench_stability_prep() failed "
        "(could not set thread affinity or priority class)\n");
#else
    fprintf(stderr,
        "test_log_perf_gate: SKIP: this host has no shipped stability "
        "design; runtime is Linux/Windows only (issue #508)\n");
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

    if (!stability_env_ok_()) {
        print_skip_platform_();
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

    /* Baseline self-check: if the nolog trials themselves are too
     * noisy, no signal at the 1% budget can be trusted. */
    double nolog_mean = mean_ms_(t_nolog, TRIALS);
    double nolog_stdev = stdev_ms_(t_nolog, TRIALS, nolog_mean);
    double nolog_cov = (nolog_mean > 0.0) ? nolog_stdev / nolog_mean : 0.0;

    double med_nolog = median_ms_(t_nolog, TRIALS);
    double med_wllog = median_ms_(t_wllog, TRIALS);

    double wall_delta_frac = (med_wllog - med_nolog) / med_nolog;
    double per_iter_ns =
        (med_wllog - med_nolog) * 1e6 / (double)ITERS; /* ms -> ns per iter */

    fprintf(stderr,
        "test_log_perf_gate: iters=%d trials=%d\n"
        "  med_nolog      = %.3f ms\n"
        "  med_wllog      = %.3f ms\n"
        "  nolog mean     = %.3f ms\n"
        "  nolog stdev    = %.4f ms (CoV %.3f%%)\n"
        "  wall_delta     = %.4f (%.2f%%)\n"
        "  per_iter_delta = %.4f ns\n",
        ITERS, TRIALS,
        med_nolog, med_wllog,
        nolog_mean, nolog_stdev, nolog_cov * 100.0,
        wall_delta_frac, wall_delta_frac * 100.0,
        per_iter_ns);

    if (nolog_cov > MAX_BASELINE_COV) {
        fprintf(stderr,
            "test_log_perf_gate: SKIP: baseline CoV %.3f%% exceeds %.1f%% "
            "ceiling; measurement too noisy for a 1%% wall budget "
            "(rerun on quieter hardware or close background load)\n",
            nolog_cov * 100.0, MAX_BASELINE_COV * 100.0);
        wl_log_shutdown();
        return SKIP_EXIT;
    }

    /* Fail only if BOTH signals trip: wall-delta > 1% AND per-iter > 1 ns.
     * The baseline loop is a ~300-ps body, so a single extra cycle shows
     * as a large wall-delta percentage without any real regression; the
     * absolute-cost check (per-iter-ns) carries the invariant. */
    int wall_breach = (wall_delta_frac > 0.01);
    int per_iter_breach = (per_iter_ns > 1.0);
    if (wall_breach) {
        fprintf(stderr,
            "test_log_perf_gate: note: wall delta %.4f exceeds 1%% budget "
            "(companion signal; per-iter check is the invariant)\n",
            wall_delta_frac);
    }
    if (per_iter_breach) {
        fprintf(stderr,
            "test_log_perf_gate: note: per-iter delta %.4f ns exceeds "
            "1 ns budget\n", per_iter_ns);
    }
    wl_log_shutdown();
    if (wall_breach && per_iter_breach) {
        fprintf(stderr,
            "test_log_perf_gate: FAIL: both wall-delta and per-iter-delta "
            "budgets breached; real regression\n");
        return 1;
    }

    puts("test_log_perf_gate OK");
    return 0;
}
