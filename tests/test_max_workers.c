/*
 * test_max_workers.c - WL_MAX_WORKERS clamping tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates that col_session_create enforces the WL_MAX_WORKERS cap:
 *   - Default cap: 16 workers
 *   - Env override: WIRELOG_MAX_WORKERS sets exact cap [1, 512]
 *   - Hard limit: values > 512 clamp to 512
 *   - Invalid env (0): falls back to default cap (16)
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

/* ======================================================================== */
/* Plan Helper                                                             */
/* ======================================================================== */

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

/* Helper: create session with num_workers, return actual num_workers. */
static int
get_actual_workers(wl_plan_t *plan, uint32_t num_workers, uint32_t *out)
{
    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0 || !sess)
        return rc ? rc : -1;
    *out = ((wl_col_session_t *)sess)->num_workers;
    wl_session_destroy(sess);
    return 0;
}

/* ======================================================================== */
/* Test 1: num_workers=1 — below default cap, no clamp                     */
/* ======================================================================== */

static int
test_no_clamp_at_1(void)
{
    TEST("WL_MAX_WORKERS clamp: num_workers=1 (no clamp)");

    unsetenv("WIRELOG_MAX_WORKERS");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not build plan");
        return 1;
    }

    uint32_t actual = 0;
    int rc = get_actual_workers(plan, 1, &actual);
    wl_plan_free(plan);

    if (rc != 0) {
        FAIL("session_create failed");
        return 1;
    }

    bool ok = (actual == 1);
    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 1, got %u", actual);
        FAIL(msg);
    } else {
        PASS();
    }
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 2: num_workers=16 — at default cap, no clamp                       */
/* ======================================================================== */

static int
test_no_clamp_at_16(void)
{
    TEST("WL_MAX_WORKERS clamp: num_workers=16 (no clamp)");

    unsetenv("WIRELOG_MAX_WORKERS");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not build plan");
        return 1;
    }

    uint32_t actual = 0;
    int rc = get_actual_workers(plan, 16, &actual);
    wl_plan_free(plan);

    if (rc != 0) {
        FAIL("session_create failed");
        return 1;
    }

    bool ok = (actual == 16);
    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 16, got %u", actual);
        FAIL(msg);
    } else {
        PASS();
    }
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 3: num_workers=196 — within RAM-based dynamic cap, accepted as-is  */
/* (Issue #409: dynamic cap replaces fixed default of 16)                   */
/* ======================================================================== */

static int
test_clamp_196_to_16(void)
{
    TEST("WL_MAX_WORKERS dynamic cap: num_workers within RAM cap accepted");

    unsetenv("WIRELOG_MAX_WORKERS");

    /* Dynamically compute a safe num_workers that is above the old fixed
     * default (16) but within the RAM-based cap on this machine. */
    uint64_t ram = col_detect_physical_memory();
    uint32_t ram_cap = col_compute_worker_cap(ram);
    uint32_t test_w = ram_cap > 32 ? ram_cap - 1 : 17;

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not build plan");
        return 1;
    }

    uint32_t actual = 0;
    int rc = get_actual_workers(plan, test_w, &actual);
    wl_plan_free(plan);

    if (rc != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "session_create failed (test_w=%u, ram_cap=%u)", test_w, ram_cap);
        FAIL(msg);
        return 1;
    }

    bool ok = (actual == test_w);
    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected %u (no clamp), got %u", test_w,
            actual);
        FAIL(msg);
    } else {
        PASS();
    }
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 4: WIRELOG_MAX_WORKERS=32, num_workers=64 → 32                    */
/* ======================================================================== */

static int
test_env_override_32(void)
{
    TEST(
        "WL_MAX_WORKERS env override: WIRELOG_MAX_WORKERS=32, num_workers=64 -> 32");

    setenv("WIRELOG_MAX_WORKERS", "32", 1);

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        unsetenv("WIRELOG_MAX_WORKERS");
        FAIL("could not build plan");
        return 1;
    }

    uint32_t actual = 0;
    int rc = get_actual_workers(plan, 64, &actual);
    wl_plan_free(plan);
    unsetenv("WIRELOG_MAX_WORKERS");

    if (rc != 0) {
        FAIL("session_create failed");
        return 1;
    }

    bool ok = (actual == 32);
    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 32, got %u", actual);
        FAIL(msg);
    } else {
        PASS();
    }
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 5: WIRELOG_MAX_WORKERS=0 — invalid, falls back to RAM-based cap    */
/* (Issue #409: default is now dynamic RAM cap, not fixed 16)               */
/* ======================================================================== */

static int
test_env_invalid_zero(void)
{
    /* Dynamically compute a safe num_workers within the RAM cap. */
    uint64_t ram = col_detect_physical_memory();
    uint32_t ram_cap = col_compute_worker_cap(ram);
    uint32_t test_w = ram_cap > 32 ? ram_cap - 1 : 17;

    TEST(
        "WL_MAX_WORKERS env invalid: WIRELOG_MAX_WORKERS=0 -> RAM cap fallback");

    setenv("WIRELOG_MAX_WORKERS", "0", 1);

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        unsetenv("WIRELOG_MAX_WORKERS");
        FAIL("could not build plan");
        return 1;
    }

    uint32_t actual = 0;
    int rc = get_actual_workers(plan, test_w, &actual);
    wl_plan_free(plan);
    unsetenv("WIRELOG_MAX_WORKERS");

    if (rc != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "session_create failed (test_w=%u, ram_cap=%u)", test_w, ram_cap);
        FAIL(msg);
        return 1;
    }

    /* Invalid env (val=0 rejected) falls back to RAM cap; test_w < RAM cap */
    bool ok = (actual == test_w);
    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected %u (RAM cap fallback), got %u",
            test_w, actual);
        FAIL(msg);
    } else {
        PASS();
    }
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 6: WIRELOG_MAX_WORKERS=999 — env override allows up to hard limit  */
/* (Issue #409: hard limit raised to 4096, env override accepted with warn) */
/* ======================================================================== */

static int
test_env_hard_limit(void)
{
    TEST(
        "WL_MAX_WORKERS env override: WIRELOG_MAX_WORKERS=999 allowed with warning");

    setenv("WIRELOG_MAX_WORKERS", "999", 1);

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        unsetenv("WIRELOG_MAX_WORKERS");
        FAIL("could not build plan");
        return 1;
    }

    uint32_t actual = 0;
    int rc = get_actual_workers(plan, 999, &actual);
    wl_plan_free(plan);
    unsetenv("WIRELOG_MAX_WORKERS");

    if (rc != 0) {
        FAIL("session_create failed");
        return 1;
    }

    /* Env override allows 999 (< hard limit of 4096) with warning if it
     * exceeds RAM-based cap. On this system, RAM cap ≈ 409, so warning issued
     * but session created with num_workers=999. */
    bool ok = (actual == 999);
    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 999 (env override), got %u",
            actual);
        FAIL(msg);
    } else {
        PASS();
    }
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_max_workers (WL_MAX_WORKERS cap) ===\n");

    test_no_clamp_at_1();
    test_no_clamp_at_16();
    test_clamp_196_to_16();
    test_env_override_32();
    test_env_invalid_zero();
    test_env_hard_limit();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
