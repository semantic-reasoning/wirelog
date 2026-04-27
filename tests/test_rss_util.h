/*
 * test_rss_util.h - Cross-platform peak/current-RSS samplers for stress tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Issue #597: the daemon-soak workload anchors a baseline RSS sample
 * before the first step and re-samples after the last step to gate
 * end-of-run heap growth.  This helper centralizes the platform
 * branches so test code does not have to reach into bench/bench_util.h
 * (which pulls in the full perf-stability harness).  The implementation
 * mirrors bench/bench_util.h:107-149 verbatim so the two surfaces stay
 * in lockstep.
 *
 * Returns peak resident-set size in KB on success, -1 on sampler
 * failure or unsupported platform.  Note: getrusage(RUSAGE_SELF).ru_maxrss
 * is intentionally NOT used here -- it returns KB on Linux but bytes on
 * macOS, which is a known foot-gun.  This helper uses platform-native
 * APIs (Linux /proc/self/status VmHWM, macOS task_info, Windows
 * GetProcessMemoryInfo) so callers always get KB.
 *
 * Issue #598: extends this header with test_current_rss_kb(), which
 * samples the CURRENT working-set size (not the monotonic peak).  The
 * peak sampler test_peak_rss_kb() is appropriate for end-of-run gates
 * that care about the high-water mark over a soak.  test_current_rss_kb()
 * is appropriate for per-rotation sampling where the gate must observe
 * heap growth between two points in time -- VmHWM only ever rises, so
 * a per-rotation strict gate against VmHWM is mathematically broken
 * (VmHWM(N+1) - VmHWM(N) >= 0 always).  Callers that need a delta
 * between two rotations must use test_current_rss_kb().
 *
 * Platform notes for current_rss_kb:
 *   - Linux:   /proc/self/status VmRSS field.
 *   - macOS:   task_info MACH_TASK_BASIC_INFO resident_size.
 *   - Windows: PROCESS_MEMORY_COUNTERS.WorkingSetSize.
 *   - Else:    -1 (platform unavailable).
 */

#ifndef WL_TEST_RSS_UTIL_H
#define WL_TEST_RSS_UTIL_H

#include <stdint.h>

#if defined(__APPLE__)
#include <mach/mach.h>

static inline int64_t
test_peak_rss_kb(void)
{
    struct mach_task_basic_info info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    kern_return_t kr = task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
            (task_info_t)&info, &count);
    if (kr != KERN_SUCCESS)
        return -1;
    return (int64_t)(info.resident_size_max / 1024);
}

static inline int64_t
test_current_rss_kb(void)
{
    struct mach_task_basic_info info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    kern_return_t kr = task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
            (task_info_t)&info, &count);
    if (kr != KERN_SUCCESS)
        return -1;
    return (int64_t)(info.resident_size / 1024);
}

#elif defined(_WIN32)
#include <psapi.h>
#include <windows.h>

static inline int64_t
test_peak_rss_kb(void)
{
    PROCESS_MEMORY_COUNTERS pmc;
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc)))
        return -1;
    return (int64_t)(pmc.PeakWorkingSetSize / 1024);
}

static inline int64_t
test_current_rss_kb(void)
{
    PROCESS_MEMORY_COUNTERS pmc;
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc)))
        return -1;
    return (int64_t)(pmc.WorkingSetSize / 1024);
}

#elif defined(__linux__)
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Internal helper: parse a /proc/self/status field by tag (e.g. "VmHWM:"
 * or "VmRSS:") and return its value in KB.  Both fields share the same
 * "Tag:    1234 kB" line format, so the I/O logic is shared.  Returns -1
 * on failure (file missing, tag absent). */
static inline int64_t
test_proc_self_status_kb(const char *tag)
{
    FILE *f = fopen("/proc/self/status", "r");
    if (!f)
        return -1;
    int64_t kb = -1;
    char line[256];
    size_t taglen = strlen(tag);
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, tag, taglen) == 0) {
            kb = strtol(line + taglen, NULL, 10);
            break;
        }
    }
    fclose(f);
    return kb;
}

static inline int64_t
test_peak_rss_kb(void)
{
    return test_proc_self_status_kb("VmHWM:");
}

static inline int64_t
test_current_rss_kb(void)
{
    return test_proc_self_status_kb("VmRSS:");
}

#else

static inline int64_t
test_peak_rss_kb(void)
{
    return -1; /* unsupported platform */
}

static inline int64_t
test_current_rss_kb(void)
{
    return -1; /* unsupported platform */
}

#endif

#endif /* WL_TEST_RSS_UTIL_H */
