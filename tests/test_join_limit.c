/*
 * test_join_limit.c - Dynamic join output limit tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates optional legacy join row-cap configuration:
 *   - Unset, empty and zero disable the cap
 *   - Env override: WIRELOG_JOIN_OUTPUT_LIMIT sets a positive cap
 *   - Malformed values fail session creation
 *   - Disabled caps still preserve join output high-water accounting
 *
 * Issue #1369: remove the implicit physical-memory-derived row cap
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
#include "plan_fixture.h"

#include <errno.h>
#include <stdio.h>
#include <stdint.h>
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
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }
    plan_fixture_hold(prog);
    return plan;
}

/* ======================================================================== */
/* Test 1: Unset limit leaves the optional row cap disabled                */
/* ======================================================================== */

static int
test_default_limit(void)
{
    TEST("Unset WIRELOG_JOIN_OUTPUT_LIMIT disables the row cap");

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
    bool ok = sess->join_output_limit == 0;

    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected join_output_limit=0, got %llu",
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

static int
test_empty_env_is_disabled(void)
{
    TEST("empty WIRELOG_JOIN_OUTPUT_LIMIT disables the row cap");
    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", "", 1);
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
    bool ok = rc == 0 && session != NULL
        && ((wl_col_session_t *)session)->join_output_limit == 0;
    if (session)
        wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
    if (ok)
        PASS();
    else
        FAIL("empty value did not disable the row cap");
    return ok ? 0 : 1;
}

static int
test_env_clamps_to_row_width(void)
{
    TEST("positive row cap is clamped to UINT32_MAX");
    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", "18446744073709551615", 1);
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
    bool ok = rc == 0 && session != NULL
        && ((wl_col_session_t *)session)->join_output_limit == UINT32_MAX;
    if (session)
        wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
    if (ok)
        PASS();
    else
        FAIL("positive cap was not clamped to UINT32_MAX");
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 4: Disabled cap is independent of configured worker count           */
/* ======================================================================== */

static int
test_limit_constant_across_workers(void)
{
    TEST("disabled row cap stays zero at W=1, W=4, W=8");

    /* Remove env override to exercise unset configuration. */
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

    wl_session_t *sess4 = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 4, &sess4);
    if (rc != 0 || !sess4) {
        wl_session_destroy(sess1);
        wl_plan_free(plan);
        FAIL("session_create (4 workers) failed");
        return 1;
    }

    wl_session_t *sess8 = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &sess8);
    if (rc != 0 || !sess8) {
        wl_session_destroy(sess1);
        wl_session_destroy(sess4);
        wl_plan_free(plan);
        FAIL("session_create (8 workers) failed");
        return 1;
    }

    uint64_t limit1 = ((wl_col_session_t *)sess1)->join_output_limit;
    uint64_t limit4 = ((wl_col_session_t *)sess4)->join_output_limit;
    uint64_t limit8 = ((wl_col_session_t *)sess8)->join_output_limit;

    /* These simple plans exercise coordinator session configuration only. */
    bool ok = (limit1 == 0 && limit4 == 0 && limit8 == 0);

    if (!ok) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "limit1=%llu limit4=%llu limit8=%llu: expected all zero",
            (unsigned long long)limit1,
            (unsigned long long)limit4,
            (unsigned long long)limit8);
        FAIL(msg);
    }

    wl_session_destroy(sess1);
    wl_session_destroy(sess4);
    wl_session_destroy(sess8);
    wl_plan_free(plan);

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

static int
test_invalid_env_values(void)
{
    static const char *const invalid[] = {
        "-1", "+1", " 1", "1 ", "1x", "18446744073709551616"
    };
    TEST("malformed row-cap values fail before session allocation");
    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
        FAIL("could not generate plan");
        return 1;
    }
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        setenv("WIRELOG_JOIN_OUTPUT_LIMIT", invalid[i], 1);
        wl_session_t *session = (wl_session_t *)(uintptr_t)1;
        int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
        if (rc != EINVAL || session != NULL) {
            if (session && session != (wl_session_t *)(uintptr_t)1)
                wl_session_destroy(session);
            wl_plan_free(plan);
            unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
            FAIL("invalid value did not return EINVAL with NULL output");
            return 1;
        }
    }
    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", "0", 1);
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    bool ok = rc == 0 && session != NULL
        && ((wl_col_session_t *)session)->join_output_limit == 0;
    if (session)
        wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
    if (ok)
        PASS();
    else
        FAIL("valid creation failed after malformed configuration");
    return ok ? 0 : 1;
}

static int
test_disabled_limit_records_high_water(void)
{
    TEST("disabled row cap still records the output high-water mark");
    wl_col_session_t session = {0};
    col_rel_t output = {0};
    output.nrows = 17;
    bool stopped = col_join_output_limit_reached(&session, &output);
    bool ok = !stopped && session.join_output_peak == 17;
    if (ok)
        PASS();
    else
        FAIL("zero cap stopped output or lost its high-water mark");
    return ok ? 0 : 1;
}

static void
test_shared_row_accounting(void)
{
    TEST("shared cap counts committed rows, zero increments and saturation");
    atomic_uint_fast64_t count;
    atomic_init(&count, 0);
    wl_col_session_t a = {0}, b = {0};
    col_rel_t out = {0};
    a.join_output_shared_count = b.join_output_shared_count = &count;
    a.join_output_shared_limit = b.join_output_shared_limit = 10;
    out.nrows = 7;
    bool ok = !wl_columnar_join_output_limit_reached(&a, &out, 7);
    ok = ok && !wl_columnar_join_output_limit_reached(&b, &out, 0)
        && atomic_load_explicit(&count, memory_order_relaxed) == 7;
    ok = ok && wl_columnar_join_output_limit_reached(&b, &out, 3)
        && atomic_load_explicit(&count, memory_order_relaxed) == 10;
    ok = ok && wl_columnar_join_output_limit_reached(&a, &out, 0)
        && atomic_load_explicit(&count, memory_order_relaxed) == 10;
    ok = ok && col_join_output_limit_reached(&a, &out)
        && atomic_load_explicit(&count, memory_order_relaxed) == 11;
    atomic_store_explicit(&count, 8, memory_order_relaxed);
    ok = ok && wl_columnar_join_output_limit_reached(&b, &out, 7)
        && atomic_load_explicit(&count, memory_order_relaxed) == 15;
    a.join_output_shared_limit = 0;
    out.nrows = 19;
    ok = ok && !wl_columnar_join_output_limit_reached(&a, &out, 7)
        && atomic_load_explicit(&count,
            memory_order_relaxed) == 15 && a.join_output_peak == 19;
    a.join_output_shared_limit = UINT64_MAX;
    atomic_store_explicit(&count, UINT64_MAX - 2, memory_order_relaxed);
    ok = ok && wl_columnar_join_output_limit_reached(&a, &out, 7)
        && atomic_load_explicit(&count, memory_order_relaxed) == UINT64_MAX;
    ok = ok && col_join_output_limit_reached(&a, &out)
        && atomic_load_explicit(&count, memory_order_relaxed) == UINT64_MAX;
    if (ok)
        PASS();
    else
        FAIL("incorrect shared row accounting");
}

static void *
shared_row_worker(void *opaque)
{
    wl_col_session_t sess = {0};
    sess.join_output_shared_count = opaque;
    sess.join_output_shared_limit = UINT64_MAX;
    for (uint32_t i = 0; i < 10000; i++)
        (void)wl_columnar_join_output_limit_reached(&sess, NULL, 7);
    return NULL;
}

static void
test_concurrent_shared_row_accounting(void)
{
    TEST("concurrent workers preserve every committed row increment");
    atomic_uint_fast64_t count;
    atomic_init(&count, 0);
    wl_thread_t threads[4];
    unsigned started = 0;
    bool ok = true;
    while (started < 4) {
        if (wl_thread_create(&threads[started], shared_row_worker, &count) != 0)
            break;
        started++;
    }
    for (unsigned i = 0; i < started; i++)
        if (wl_thread_join(&threads[i]) != 0)
            ok = false;
    if (ok && started == 4 && atomic_load_explicit(&count,
        memory_order_relaxed) == 280000)
        PASS();
    else
        FAIL("lost concurrent increments or thread failure");
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_join_limit (Issue #1369) ===\n");

    test_default_limit();
    test_env_override();
    test_env_disable();
    test_empty_env_is_disabled();
    test_env_clamps_to_row_width();
    test_limit_constant_across_workers();
    test_invalid_env_values();
    test_disabled_limit_records_high_water();
    test_shared_row_accounting();
    test_concurrent_shared_row_accounting();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
