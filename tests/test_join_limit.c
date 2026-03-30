/*
 * test_join_limit.c - Dynamic join output limit tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates that join_output_limit is correctly computed at session init:
 *   - Default: auto-detected from physical RAM / num_workers
 *   - Env override: WIRELOG_JOIN_OUTPUT_LIMIT sets exact value
 *   - Disable: WIRELOG_JOIN_OUTPUT_LIMIT=0 disables the limit
 *   - Scaling: 8-worker limit is ~1/8 of 1-worker limit
 *
 * Issue #221: Dynamic join output limit based on available memory
 */

#define _GNU_SOURCE

#include "../wirelog/columnar/columnar_nanoarrow.h"
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

/* ======================================================================== */
/* Test 1: Default limit is auto-detected (> 0)                            */
/* ======================================================================== */

static int
test_default_limit(void)
{
    TEST("Default join_output_limit > 0 (auto-detected from RAM)");

    /* Remove env override if present */
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return 1;
    }

    wl_col_session_t *sess = (wl_col_session_t *)session;
    bool ok = sess->join_output_limit > 0;

    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "join_output_limit == 0 (expected > 0), got %llu",
            (unsigned long long)sess->join_output_limit);
        FAIL(msg);
    }

    wl_session_destroy(session);
    wl_plan_free(plan);

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 2: Env override sets exact value                                    */
/* ======================================================================== */

static int
test_env_override(void)
{
    TEST("WIRELOG_JOIN_OUTPUT_LIMIT env override sets exact value");

    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", "12345", 1);

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
        FAIL("session_create failed");
        return 1;
    }

    wl_col_session_t *sess = (wl_col_session_t *)session;
    bool ok = (sess->join_output_limit == 12345);

    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected join_output_limit=12345, got %llu",
            (unsigned long long)sess->join_output_limit);
        FAIL(msg);
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 3: Env override of 0 disables the limit                            */
/* ======================================================================== */

static int
test_env_disable(void)
{
    TEST("WIRELOG_JOIN_OUTPUT_LIMIT=0 disables the limit");

    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", "0", 1);

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
        FAIL("session_create failed");
        return 1;
    }

    wl_col_session_t *sess = (wl_col_session_t *)session;
    bool ok = (sess->join_output_limit == 0);

    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected join_output_limit=0, got %llu",
            (unsigned long long)sess->join_output_limit);
        FAIL(msg);
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 4: Limit is independent of num_workers (Issue #386)                */
/* ======================================================================== */

static int
test_limit_scales_with_workers(void)
{
    TEST("join_output_limit is independent of num_workers (Issue #386)");

    /* Remove env override to exercise auto-detection path */
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *sess1 = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess1);
    if (rc != 0 || !sess1) {
        wl_plan_free(plan);
        FAIL("session_create (1 worker) failed");
        return 1;
    }

    wl_session_t *sess8 = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &sess8);
    if (rc != 0 || !sess8) {
        wl_session_destroy(sess1);
        wl_plan_free(plan);
        FAIL("session_create (8 workers) failed");
        return 1;
    }

    uint64_t limit1 = ((wl_col_session_t *)sess1)->join_output_limit;
    uint64_t limit8 = ((wl_col_session_t *)sess8)->join_output_limit;

    /* Issue #386: join_output_limit no longer scales with num_workers.
    * In timely-differential evaluation, workers do not simultaneously
    * produce peak join output; runtime backpressure handles memory. */
    bool ok = (limit1 > 0 && limit8 > 0 && limit1 == limit8);

    if (!ok) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "limit1=%llu limit8=%llu: expected equal",
            (unsigned long long)limit1, (unsigned long long)limit8);
        FAIL(msg);
    }

    wl_session_destroy(sess1);
    wl_session_destroy(sess8);
    wl_plan_free(plan);

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_join_limit (Issue #221) ===\n");

    test_default_limit();
    test_env_override();
    test_env_disable();
    test_limit_scales_with_workers();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
