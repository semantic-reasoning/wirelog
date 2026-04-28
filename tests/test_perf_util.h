/*
 * test_perf_util.h - timing + percentile + CoV helpers (ns and ms)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Issue #599: rotate-latency gate samples per-rotation wall time and
 * gates p50/p99/p999.  This helper centralizes the small math surface
 * (ns clock, qsort comparator, percentile, coefficient of variation,
 * stability self-check) so the gate code stays a thin shell over the
 * rotation hot path.  Both ns- and ms-typed variants are provided so
 * the existing tests/test_log_perf_gate.c (ms-double pipeline) and
 * bench/bench_compound.c (us-double, also fits the ms helpers via
 * sort+pick) can consume the same implementation.
 *
 * Platform notes for wl_perf_now_ns:
 *   - Linux/POSIX: clock_gettime(CLOCK_MONOTONIC).  Aborts via perror+
 *                  exit(2) on syscall failure (measurement-critical).
 *   - macOS:       mach_absolute_time + mach_timebase_info (mirrors
 *                  bench/bench_util.h's bench_time_now/diff path).
 *                  Note: the double-precision scale loses sub-ns
 *                  precision after roughly 104 days of monotonic
 *                  uptime; benchmarks here run in seconds, well
 *                  within precision.
 *   - Windows:     QueryPerformanceCounter scaled by QueryPerformanceFrequency.
 *
 * wl_perf_stability_env_ok prints a SKIP-suitable diagnostic to stderr
 * on failure (sysfs missing, governor wrong, or unsupported platform)
 * and returns 0; returns 1 only when the host is configured for stable
 * timing.  Callers that just need to early-return on env failure can
 * print their own context-specific prefix and forward the return code.
 */

#ifndef WL_TEST_PERF_UTIL_H
#define WL_TEST_PERF_UTIL_H

/* clock_gettime / CLOCK_MONOTONIC require POSIX visibility on glibc;
 * publishers compiled under strict -std=c11 will not see these unless
 * _POSIX_C_SOURCE is set before <time.h> is reached.  Header-local
 * definition keeps consumers from having to remember to set it. */
#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#  define _POSIX_C_SOURCE 200809L
#endif

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef uint64_t wl_perf_ns_t;

#if defined(__APPLE__)
#include <mach/mach_time.h>

/* Note: the double-precision scale loses sub-ns precision after
 * approximately 104 days of monotonic uptime (2^53 ns).  Benchmarks
 * here measure intervals in seconds, well within precision. */
static inline wl_perf_ns_t
wl_perf_now_ns(void)
{
    static mach_timebase_info_data_t info;
    if (info.denom == 0)
        mach_timebase_info(&info);
    uint64_t t = mach_absolute_time();
    return (wl_perf_ns_t)((double)t * (double)info.numer
           / (double)info.denom);
}

#elif defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

static inline wl_perf_ns_t
wl_perf_now_ns(void)
{
    LARGE_INTEGER freq;
    LARGE_INTEGER ctr;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&ctr);
    /* Scale ticks -> ns without losing precision for typical freqs. */
    long long secs = ctr.QuadPart / freq.QuadPart;
    long long rem = ctr.QuadPart % freq.QuadPart;
    return (wl_perf_ns_t)((uint64_t)secs * 1000000000ull
           + (uint64_t)((rem * 1000000000ll) / freq.QuadPart));
}

#else /* POSIX */
#include <time.h>

static inline wl_perf_ns_t
wl_perf_now_ns(void)
{
    struct timespec ts;
    /* Measurement-critical: clock_gettime failure produces garbage
     * timing data.  Abort loudly rather than silently corrupt results. */
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("wl_perf_now_ns: clock_gettime(CLOCK_MONOTONIC)");
        exit(2);
    }
    return (wl_perf_ns_t)ts.tv_sec * 1000000000ull
           + (wl_perf_ns_t)ts.tv_nsec;
}

#endif

/* qsort comparator for wl_perf_ns_t.  Returns -1/0/1. */
static inline int
wl_perf_cmp_ns(const void *a, const void *b)
{
    wl_perf_ns_t x = *(const wl_perf_ns_t *)a;
    wl_perf_ns_t y = *(const wl_perf_ns_t *)b;
    if (x < y) return -1;
    if (x > y) return 1;
    return 0;
}

/* qsort comparator for double (ms or us variants share this).  Returns -1/0/1. */
static inline int
wl_perf_cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a;
    double y = *(const double *)b;
    if (x < y) return -1;
    if (x > y) return 1;
    return 0;
}

/* Percentile pick from a pre-sorted array.  Caller must qsort the
 * input with wl_perf_cmp_ns first.  pct is in [0.0, 1.0]; the index
 * is floor(n*pct) clamped to [0, n-1].  Returns 0 when n==0. */
static inline wl_perf_ns_t
wl_perf_percentile_ns(const wl_perf_ns_t *sorted, size_t n, double pct)
{
    if (n == 0)
        return 0;
    if (pct < 0.0)
        pct = 0.0;
    if (pct > 1.0)
        pct = 1.0;
    size_t idx = (size_t)floor((double)n * pct);
    if (idx >= n)
        idx = n - 1;
    return sorted[idx];
}

/* Percentile pick from a pre-sorted double array (ms or us, type
 * tells you nothing about units; caller's responsibility).  Caller
 * must qsort with wl_perf_cmp_double first.  pct in [0.0, 1.0]. */
static inline double
wl_perf_percentile_ms(const double *sorted, size_t n, double pct)
{
    if (n == 0)
        return 0.0;
    if (pct < 0.0)
        pct = 0.0;
    if (pct > 1.0)
        pct = 1.0;
    size_t idx = (size_t)floor((double)n * pct);
    if (idx >= n)
        idx = n - 1;
    return sorted[idx];
}

/* Median of a double array.  Sorts in place via qsort.  Returns the
 * average of the two middles for even n -- mirrors test_log_perf_gate.c's
 * pre-existing semantics.  For odd n the middle sample is returned. */
static inline double
wl_perf_median_ms_inplace(double *vals, size_t n)
{
    if (n == 0)
        return 0.0;
    qsort(vals, n, sizeof(vals[0]), wl_perf_cmp_double);
    if (n & 1u)
        return vals[n / 2];
    return 0.5 * (vals[n / 2 - 1] + vals[n / 2]);
}

/* Mean of a double array. */
static inline double
wl_perf_mean_ms(const double *vals, size_t n)
{
    if (n == 0)
        return 0.0;
    double s = 0.0;
    for (size_t i = 0; i < n; ++i)
        s += vals[i];
    return s / (double)n;
}

/* Population stdev (n, not n-1).  Caller passes the precomputed mean
 * to keep this allocation-free.  Mirrors test_log_perf_gate.c's
 * stdev_ms_ semantics: full-sample stdev, not an estimator. */
static inline double
wl_perf_stdev_ms(const double *vals, size_t n, double mean)
{
    if (n == 0)
        return 0.0;
    double ss = 0.0;
    for (size_t i = 0; i < n; ++i) {
        double d = vals[i] - mean;
        ss += d * d;
    }
    return sqrt(ss / (double)n);
}

/* Coefficient of variation (stdev/mean) over n samples.  Mirrors
 * compute_cov in tests/test_rss_bounded.c.  Returns 0 when n==0 or
 * mean<=0 (callers gate on a > ceiling check, so 0 is a safe sentinel
 * for "not noisy"). */
static inline double
wl_perf_cov(const wl_perf_ns_t *samples, size_t n)
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

/* Coefficient of variation over a double array.  ms or us, caller
 * picks the unit; CoV is dimensionless. */
static inline double
wl_perf_cov_ms(const double *samples, size_t n)
{
    if (n == 0)
        return 0.0;
    double mean = wl_perf_mean_ms(samples, n);
    if (mean <= 0.0)
        return 0.0;
    double stdev = wl_perf_stdev_ms(samples, n, mean);
    return stdev / mean;
}

/* Stability env pre-check.  Linux: cpufreq governor on cpu0 must be
 * "performance" -- mirrors tests/test_log_perf_gate.c:142-156.  Prints
 * a SKIP-suitable diagnostic to stderr on failure (distinguishing
 * "sysfs missing" from "governor wrong") and returns 0; returns 1 on
 * success.  Non-Linux platforms print a "no shipped stability design"
 * SKIP message and return 0. */
static inline int
wl_perf_stability_env_ok(void)
{
#if defined(__linux__)
    FILE *f = fopen("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
            "r");
    if (!f) {
        fprintf(stderr,
            "  SKIP: cpufreq sysfs not available "
            "(containerized runner?  /sys/devices/system/cpu/cpu0/"
            "cpufreq/scaling_governor unreadable)\n");
        return 0;
    }
    char buf[64] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    if (n == 0) {
        fprintf(stderr,
            "  SKIP: cpufreq scaling_governor read returned 0 bytes\n");
        return 0;
    }
    if (buf[n - 1] == '\n') buf[n - 1] = '\0';
    if (strcmp(buf, "performance") != 0) {
        fprintf(stderr,
            "  SKIP: cpufreq governor='%s', expected 'performance' "
            "(echo performance | sudo tee /sys/devices/system/cpu/"
            "cpu0/cpufreq/scaling_governor)\n", buf);
        return 0;
    }
    return 1;
#else
    fprintf(stderr,
        "  SKIP: this host has no shipped stability design "
        "(runtime gate is Linux-only today)\n");
    return 0;
#endif
}

#endif /* WL_TEST_PERF_UTIL_H */
