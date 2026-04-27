/*
 * test_rss_util.h - Cross-platform peak-RSS sampler for stress tests
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

#elif defined(__linux__)
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline int64_t
test_peak_rss_kb(void)
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
test_peak_rss_kb(void)
{
    return -1; /* unsupported platform */
}

#endif

#endif /* WL_TEST_RSS_UTIL_H */
