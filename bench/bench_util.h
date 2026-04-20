/*
 * bench_util.h - Benchmark Timing and Memory Utilities
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Cross-platform header-only utilities for wall-clock timing
 * and peak RSS memory measurement.
 */

#ifndef BENCH_UTIL_H
#define BENCH_UTIL_H

#include <stdint.h>

/* ================================================================
 * Compiler attribute shims
 * ================================================================ */

/* Portable noinline for bench/perf-gate call sites.
 * MSVC C mode does not parse __attribute__((...)), so keep the two
 * spellings behind a single macro used at call sites. */
#if defined(_MSC_VER) && !defined(__clang__)
#  define BENCH_NOINLINE __declspec(noinline)
#else
#  define BENCH_NOINLINE __attribute__((noinline))
#endif

/* ================================================================
 * Wall-clock timing
 * ================================================================ */

#if defined(__APPLE__)
#include <mach/mach_time.h>

typedef uint64_t bench_time_t;

static inline bench_time_t
bench_time_now(void)
{
    return mach_absolute_time();
}

static inline double
bench_time_diff_ms(bench_time_t start, bench_time_t end)
{
    static mach_timebase_info_data_t info;
    if (info.denom == 0)
        mach_timebase_info(&info);
    uint64_t elapsed = end - start;
    /* Convert to nanoseconds, then to milliseconds */
    double nanos = (double)elapsed * (double)info.numer / (double)info.denom;
    return nanos / 1e6;
}

#elif defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

typedef LARGE_INTEGER bench_time_t;

static inline bench_time_t
bench_time_now(void)
{
    bench_time_t t;
    QueryPerformanceCounter(&t);
    return t;
}

static inline double
bench_time_diff_ms(bench_time_t start, bench_time_t end)
{
    LARGE_INTEGER freq;
    QueryPerformanceFrequency(&freq);
    return (double)(end.QuadPart - start.QuadPart) * 1000.0
           / (double)freq.QuadPart;
}

#else /* POSIX (Linux, etc.) */
#include <time.h>

typedef struct timespec bench_time_t;

static inline bench_time_t
bench_time_now(void)
{
    bench_time_t t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t;
}

static inline double
bench_time_diff_ms(bench_time_t start, bench_time_t end)
{
    double sec = (double)(end.tv_sec - start.tv_sec);
    double nsec = (double)(end.tv_nsec - start.tv_nsec);
    return sec * 1000.0 + nsec / 1e6;
}

#endif

/* ================================================================
 * Peak RSS memory (KB)
 * ================================================================ */

#if defined(__APPLE__)
#include <mach/mach.h>

static inline int64_t
bench_peak_rss_kb(void)
{
    struct mach_task_basic_info info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    kern_return_t kr = task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                                 (task_info_t)&info, &count);
    if (kr != KERN_SUCCESS)
        return -1;
    return (int64_t)(info.resident_size_max / 1024);
}

#elif defined(_WIN32)
#include <psapi.h>

static inline int64_t
bench_peak_rss_kb(void)
{
    PROCESS_MEMORY_COUNTERS pmc;
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc)))
        return -1;
    return (int64_t)(pmc.PeakWorkingSetSize / 1024);
}

#elif defined(__linux__)
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline int64_t
bench_peak_rss_kb(void)
{
    FILE *f = fopen("/proc/self/status", "r");
    if (!f)
        return -1;
    int64_t rss = -1;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmHWM:", 6) == 0) {
            /* Format: "VmHWM:    1234 kB" */
            rss = strtol(line + 6, NULL, 10);
            break;
        }
    }
    fclose(f);
    return rss;
}

#else

static inline int64_t
bench_peak_rss_kb(void)
{
    return -1; /* unsupported platform */
}

#endif

/* ================================================================
 * Comparison helper for qsort (double)
 * ================================================================ */

static inline int
bench_cmp_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db)
        return -1;
    if (da > db)
        return 1;
    return 0;
}

#endif /* BENCH_UTIL_H */
