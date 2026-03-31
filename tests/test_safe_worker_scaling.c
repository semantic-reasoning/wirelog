/*
 * test_safe_worker_scaling.c - Issue #409: safe W=512+ worker scaling
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD RED phase: these tests encode expected behavior for Issue #409.
 * They will fail until the implementation is complete.
 *
 * Tests:
 *   1. Dynamic cap formula: cap = min(sqrt(RAM/8), RAM/40MB, 4096)
 *   2. Integer overflow guard: W=UINT32_MAX returns error, no crash
 *   3. Pre-flight memory check: W=10000 rejected before thread creation
 *   4. Env override WIRELOG_MAX_WORKERS=4096 accepted with stderr warning
 */

#define _GNU_SOURCE

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__linux__)
#    include <sys/sysinfo.h>
#elif defined(__APPLE__)
#    include <sys/sysctl.h>
#    include <sys/types.h>
#endif

#if !defined(_MSC_VER)
#    include <fcntl.h>
#    include <unistd.h>
#endif

/* MSVC portability: setenv/unsetenv are POSIX-only. */
#ifdef _MSC_VER
static int
setenv(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value);
}
static int
unsetenv(const char *name)
{
    return _putenv_s(name, "");
}
#endif

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)
#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)
#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)
#define SKIP()                              \
        do {                                    \
            printf(" ... SKIP\n");              \
            return 0;                           \
        } while (0)

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

static const char *SIMPLE_PROG =
    ".decl a(x: int32)\n"
    ".decl r(x: int32)\n"
    "r(x) :- a(x).\n";

static wl_plan_t *
build_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;
    return plan;
}

/*
 * query_system_ram: portable physical RAM detection for test skip guards.
 * Returns 0 if unsupported.
 */
static uint64_t
query_system_ram(void)
{
#if defined(__linux__)
    struct sysinfo info;
    if (sysinfo(&info) == 0)
        return (uint64_t)info.totalram * (uint64_t)info.mem_unit;
    return 0;
#elif defined(__APPLE__)
    int mib[2] = { CTL_HW, HW_MEMSIZE };
    uint64_t memsize = 0;
    size_t len = sizeof(memsize);
    if (sysctl(mib, 2, &memsize, &len, NULL, 0) == 0)
        return memsize;
    return 0;
#else
    return 0;
#endif
}

/* ======================================================================== */
/* Test 1: Dynamic cap scales with RAM                                      */
/*                                                                          */
/* Formula: cap = min((uint32_t)sqrt(ram_bytes/8), ram_bytes/40MB, 4096)   */
/*                                                                          */
/* Expected results:                                                        */
/*   32GB:  min(65536, 819,  4096) = 819                                   */
/*   64GB:  min(92681, 1638, 4096) = 1638                                  */
/*   256GB: min(185363, 6553, 4096) = 4096                                 */
/*                                                                          */
/* Requires: col_compute_worker_cap(uint64_t ram_bytes) in internal.h      */
/* ======================================================================== */

static int
test_dynamic_cap_scales_with_ram(void)
{
    TEST("dynamic cap formula: 32GB->819, 64GB->1638, 256GB->4096");

    struct {
        uint64_t ram_bytes;
        uint32_t expected_cap;
        const char *label;
    } cases[] = {
        { 32ULL  * 1024 * 1024 * 1024,  819, "32GB"  },
        { 64ULL  * 1024 * 1024 * 1024, 1638, "64GB"  },
        { 256ULL * 1024 * 1024 * 1024, 4096, "256GB" },
    };

    for (int i = 0; i < 3; i++) {
        uint32_t got = col_compute_worker_cap(cases[i].ram_bytes);
        if (got != cases[i].expected_cap) {
            char msg[128];
            snprintf(msg, sizeof(msg),
                "RAM=%s: expected cap=%u, got %u",
                cases[i].label, cases[i].expected_cap, got);
            FAIL(msg);
            return 1;
        }
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: Integer overflow guard                                           */
/*                                                                          */
/* W=UINT32_MAX must return a non-zero error code (EINVAL or ERANGE).      */
/* Must not crash, corrupt memory, or silently clamp.                      */
/* ======================================================================== */

static int
test_integer_overflow_guard(void)
{
    TEST("integer overflow guard: W=UINT32_MAX returns error, no crash");

    unsetenv("WIRELOG_MAX_WORKERS");

    wl_plan_t *plan = build_plan(SIMPLE_PROG);
    if (!plan) {
        FAIL("could not build plan");
        return 1;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, UINT32_MAX, &sess);
    wl_plan_free(plan);

    if (rc == 0) {
        /* Session was created — overflow guard is missing. */
        if (sess)
            wl_session_destroy(sess);
        FAIL("expected error for W=UINT32_MAX, but session_create succeeded");
        return 1;
    }

    /* Session must not have been allocated on error. */
    if (sess != NULL) {
        wl_session_destroy(sess);
        FAIL("rc != 0 but *out was set; memory leak risk");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: Pre-flight memory check                                         */
/*                                                                          */
/* W=10000 cost: 10000*20MB + 10000^2*8B ≈ 200GB + 800MB.                */
/* On machines with < 200GB RAM, wl_session_create must return ENOMEM      */
/* before spawning any threads.                                             */
/* ======================================================================== */

static int
test_preflight_memory_check(void)
{
    TEST("preflight memory check: W=10000 rejected before thread creation");

    unsetenv("WIRELOG_MAX_WORKERS");

    /* Skip on machines with >= 200GB RAM (unlikely in CI). */
    uint64_t ram = query_system_ram();
    if (ram >= 200ULL * 1024 * 1024 * 1024) {
        printf(" ... SKIP (host RAM >= 200GB)\n");
        return 0;
    }

    wl_plan_t *plan = build_plan(SIMPLE_PROG);
    if (!plan) {
        FAIL("could not build plan");
        return 1;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 10000, &sess);
    wl_plan_free(plan);

    if (rc == 0) {
        if (sess)
            wl_session_destroy(sess);
        FAIL(
            "expected ENOMEM for W=10000 (cost ~200GB), but session_create succeeded");
        return 1;
    }

    if (sess != NULL) {
        wl_session_destroy(sess);
        FAIL("rc != 0 but *out was set; threads may have been spawned");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 4: Env override WIRELOG_MAX_WORKERS=4096 accepted with warning     */
/*                                                                          */
/* The new hard limit must be 4096 (raised from 512).                     */
/* Setting WIRELOG_MAX_WORKERS=4096 must be accepted (rc=0, actual=4096). */
/* WL_MEM_REPORT=1 must produce a diagnostic on stderr.                   */
/* ======================================================================== */

static int
test_env_override_with_warning(void)
{
    TEST("env override WIRELOG_MAX_WORKERS=4096 accepted with stderr warning");

    setenv("WIRELOG_MAX_WORKERS", "4096", 1);
    setenv("WL_MEM_REPORT", "1", 1);

    /* Redirect stderr to a temporary file so we can verify a warning was emitted. */
    int warn_emitted = 0;
#if !defined(_MSC_VER)
    char tmppath[] = "/tmp/wl_test_stderr_XXXXXX";
    int tmpfd = mkstemp(tmppath);
    int saved_stderr = -1;
    if (tmpfd >= 0) {
        saved_stderr = dup(STDERR_FILENO);
        dup2(tmpfd, STDERR_FILENO);
    }
#endif

    wl_plan_t *plan = build_plan(SIMPLE_PROG);
    wl_session_t *sess = NULL;
    int rc = -1;
    if (plan)
        rc = wl_session_create(wl_backend_columnar(), plan, 4096, &sess);
    if (plan)
        wl_plan_free(plan);

#if !defined(_MSC_VER)
    if (tmpfd >= 0) {
        fflush(stderr);
        dup2(saved_stderr, STDERR_FILENO);
        close(saved_stderr);

        /* Check if anything was written to the captured stderr. */
        off_t len = lseek(tmpfd, 0, SEEK_END);
        warn_emitted = (len > 0);
        close(tmpfd);
        unlink(tmppath);
    }
#else
    warn_emitted = 1; /* cannot capture on MSVC, assume emitted */
#endif

    unsetenv("WIRELOG_MAX_WORKERS");
    unsetenv("WL_MEM_REPORT");

    if (!plan) {
        FAIL("could not build plan");
        return 1;
    }

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
            "session_create failed with rc=%d (WIRELOG_MAX_WORKERS=4096)", rc);
        FAIL(msg);
        return 1;
    }

    uint32_t actual = ((wl_col_session_t *)sess)->num_workers;
    wl_session_destroy(sess);

    if (actual != 4096) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected num_workers=4096, got %u", actual);
        FAIL(msg);
        return 1;
    }

    if (!warn_emitted) {
        FAIL(
            "WIRELOG_MAX_WORKERS=4096 accepted but no stderr warning was emitted");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_safe_worker_scaling (Issue #409) ===\n");

    test_dynamic_cap_scales_with_ram();
    test_integer_overflow_guard();
    test_preflight_memory_check();
    test_env_override_with_warning();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
