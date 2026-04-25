/*
 * tests/test_rotation_strategy.c - Rotation strategy vtable selection (#600).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Drives col_session_create's rotation_ops selection path:
 *   test_rotation_default_is_standard   -- WIRELOG_ROTATION unset -> STANDARD
 *   test_rotation_env_override_mvcc     -- WIRELOG_ROTATION=mvcc -> MVCC
 *   test_rotation_dispatch_no_crash     -- vtable dispatch is callable on
 *                                          a live session and does not crash
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/backend.h"
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

#if defined(_MSC_VER) && !defined(__clang__)
#  include <process.h>
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}
static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}
#  define setenv   wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
#else
#  include <unistd.h>
#endif

/* ======================================================================== */
/* Test harness                                                             */
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

#define ASSERT(cond, msg)   \
        do {                    \
            if (!(cond)) {      \
                FAIL(msg);      \
                return;         \
            }                   \
        } while (0)

/* ======================================================================== */
/* Plan helper                                                              */
/* ======================================================================== */

static wl_plan_t *
build_minimal_plan(void)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n",
        &err);
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
/* Tests                                                                    */
/* ======================================================================== */

static void
test_rotation_default_is_standard(void)
{
    TEST("rotation: default selects STANDARD when WIRELOG_ROTATION unset");

    /* Defensive: ensure the env var is not leaking from another test. */
    unsetenv("WIRELOG_ROTATION");

    wl_plan_t *plan = build_minimal_plan();
    ASSERT(plan != NULL, "could not build plan");

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || session == NULL) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    wl_col_session_t *cs = COL_SESSION(session);
    if (cs->rotation_ops != &col_rotation_standard_ops) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("rotation_ops != &col_rotation_standard_ops");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_rotation_env_override_mvcc(void)
{
    TEST("rotation: WIRELOG_ROTATION=mvcc selects MVCC vtable");

    if (setenv("WIRELOG_ROTATION", "mvcc", 1) != 0) {
        FAIL("setenv WIRELOG_ROTATION=mvcc failed");
        return;
    }

    wl_plan_t *plan = build_minimal_plan();
    if (plan == NULL) {
        unsetenv("WIRELOG_ROTATION");
        FAIL("could not build plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || session == NULL) {
        wl_plan_free(plan);
        unsetenv("WIRELOG_ROTATION");
        FAIL("wl_session_create failed");
        return;
    }

    wl_col_session_t *cs = COL_SESSION(session);
    bool ok = (cs->rotation_ops == &col_rotation_mvcc_ops);

    wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_ROTATION");

    if (!ok) {
        FAIL(
            "rotation_ops != &col_rotation_mvcc_ops under WIRELOG_ROTATION=mvcc");
        return;
    }
    PASS();
}

static void
test_rotation_dispatch_no_crash(void)
{
    TEST("rotation: vtable dispatch on live session does not crash");

    unsetenv("WIRELOG_ROTATION");

    wl_plan_t *plan = build_minimal_plan();
    ASSERT(plan != NULL, "could not build plan");

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || session == NULL) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    wl_col_session_t *cs = COL_SESSION(session);
    if (cs->rotation_ops == NULL
        || cs->rotation_ops->rotate_eval_arena == NULL
        || cs->rotation_ops->gc_epoch_boundary == NULL) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("rotation_ops or required slots are NULL");
        return;
    }

    /* Direct dispatch through the vtable; rotate_eval_arena is a NULL-safe
     * wrapper around wl_arena_reset, gc_epoch_boundary around the compound
     * arena GC. Either failing here would manifest as a crash/UBSAN abort. */
    cs->rotation_ops->rotate_eval_arena(cs);
    cs->rotation_ops->gc_epoch_boundary(cs);

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("Rotation strategy vtable tests (#600):\n");

    test_rotation_default_is_standard();
    test_rotation_env_override_mvcc();
    test_rotation_dispatch_no_crash();

    printf("\nResults: %d run, %d passed, %d failed\n",
        tests_run, tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
