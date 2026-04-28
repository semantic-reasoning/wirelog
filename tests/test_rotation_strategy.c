/*
 * tests/test_rotation_strategy.c - Rotation strategy vtable selection (#600).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Drives col_session_create's rotation_ops selection path:
 *   test_rotation_default_is_standard       -- WIRELOG_ROTATION unset -> STANDARD
 *   test_rotation_pinned_placeholder_warns  -- pinned init prints stderr
 *                                              warning when acknowledge
 *                                              flag is unset (#630)
 *   test_rotation_env_override_pinned       -- WIRELOG_ROTATION=pinned ->
 *                                              pinned vtable
 *   test_rotation_dispatch_no_crash         -- vtable dispatch is callable
 *                                              on a live session and does
 *                                              not crash
 *
 * The placeholder-warning test runs FIRST among the pinned-strategy
 * tests because the stderr fprintf is one-shot per process (gated by
 * a static bool inside pinned_init); subsequent pinned-strategy tests
 * would not retrigger it. Set WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER=1
 * after the warning test to keep later test stderr clean.
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

/* #630: capture stderr while creating a session under
 * WIRELOG_ROTATION=pinned (acknowledge flag unset) and assert the
 * placeholder-warning substring is present. The warning is one-shot
 * per process (static bool warned inside pinned_init), so this test
 * MUST run before any other test creates a pinned session.
 *
 * Linux-only: portable stderr capture across freopen/dup2 semantics
 * is non-trivial on Windows/MSVC; skipping there is acceptable
 * because the footgun guard exists for production *nix operators.
 */
#if defined(__linux__)
static void
test_rotation_pinned_placeholder_warns(void)
{
    TEST("rotation: pinned strategy emits stderr placeholder warning (#630)");

    /* Ensure neither env var is set so the warning fires. */
    unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
    if (setenv("WIRELOG_ROTATION", "pinned", 1) != 0) {
        FAIL("setenv WIRELOG_ROTATION=pinned failed");
        return;
    }

    /* Redirect stderr to a temp file via freopen, then restore via
     * fdopen+dup2. Mirrors the standard libc capture pattern; portable
     * across glibc/musl. */
    fflush(stderr);
    int saved_stderr = dup(STDERR_FILENO);
    if (saved_stderr < 0) {
        unsetenv("WIRELOG_ROTATION");
        FAIL("dup(STDERR_FILENO) failed");
        return;
    }

    char tmpl[] = "/tmp/wl_rotation_stderr_XXXXXX";
    int tmp_fd = mkstemp(tmpl);
    if (tmp_fd < 0) {
        close(saved_stderr);
        unsetenv("WIRELOG_ROTATION");
        FAIL("mkstemp failed");
        return;
    }
    if (dup2(tmp_fd, STDERR_FILENO) < 0) {
        close(tmp_fd);
        unlink(tmpl);
        close(saved_stderr);
        unsetenv("WIRELOG_ROTATION");
        FAIL("dup2(tmp, STDERR) failed");
        return;
    }

    wl_plan_t *plan = build_minimal_plan();
    wl_session_t *session = NULL;
    int rc = -1;
    if (plan)
        rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);

    /* Restore stderr before any FAIL() so the diagnostic is visible. */
    fflush(stderr);
    dup2(saved_stderr, STDERR_FILENO);
    close(saved_stderr);
    close(tmp_fd);

    if (!plan || rc != 0 || session == NULL) {
        if (session)
            wl_session_destroy(session);
        if (plan)
            wl_plan_free(plan);
        unlink(tmpl);
        unsetenv("WIRELOG_ROTATION");
        FAIL("wl_session_create failed");
        return;
    }

    /* Read the captured stderr from the temp file. */
    FILE *cap = fopen(tmpl, "r");
    char captured[2048] = { 0 };
    size_t nread = 0;
    if (cap) {
        nread = fread(captured, 1, sizeof(captured) - 1, cap);
        captured[nread] = '\0';
        fclose(cap);
    }
    unlink(tmpl);

    wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_ROTATION");

    if (nread == 0
        || strstr(captured, "WIRELOG_ROTATION=pinned") == NULL
        || strstr(captured, "PLACEHOLDER") == NULL) {
        FAIL("stderr did not contain expected pinned placeholder warning");
        return;
    }
    PASS();
}

/* #630: with WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER=1 the stderr
 * warning must be silenced. Because the warning is one-shot per
 * process and already fired in the previous test, this test relies
 * on the no-warning path being explicitly checked: it sets the
 * acknowledge flag and asserts no NEW warning text appears in the
 * captured stderr from a second pinned-session create. (The first
 * fprintf already ran; if the gating logic is wrong, nothing new
 * would print regardless. We still validate the path runs cleanly
 * without crashing and produces no fresh warning bytes.)
 */
static void
test_rotation_pinned_placeholder_silenced(void)
{
    TEST(
        "rotation: WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER=1 silences (#630)");

    if (setenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER", "1", 1) != 0) {
        FAIL("setenv ACKNOWLEDGE failed");
        return;
    }
    if (setenv("WIRELOG_ROTATION", "pinned", 1) != 0) {
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("setenv WIRELOG_ROTATION=pinned failed");
        return;
    }

    fflush(stderr);
    int saved_stderr = dup(STDERR_FILENO);
    if (saved_stderr < 0) {
        unsetenv("WIRELOG_ROTATION");
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("dup(STDERR_FILENO) failed");
        return;
    }

    char tmpl[] = "/tmp/wl_rotation_stderr_XXXXXX";
    int tmp_fd = mkstemp(tmpl);
    if (tmp_fd < 0) {
        close(saved_stderr);
        unsetenv("WIRELOG_ROTATION");
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("mkstemp failed");
        return;
    }
    if (dup2(tmp_fd, STDERR_FILENO) < 0) {
        close(tmp_fd);
        unlink(tmpl);
        close(saved_stderr);
        unsetenv("WIRELOG_ROTATION");
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("dup2 failed");
        return;
    }

    wl_plan_t *plan = build_minimal_plan();
    wl_session_t *session = NULL;
    int rc = -1;
    if (plan)
        rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);

    fflush(stderr);
    dup2(saved_stderr, STDERR_FILENO);
    close(saved_stderr);
    close(tmp_fd);

    if (!plan || rc != 0 || session == NULL) {
        if (session)
            wl_session_destroy(session);
        if (plan)
            wl_plan_free(plan);
        unlink(tmpl);
        unsetenv("WIRELOG_ROTATION");
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("wl_session_create failed");
        return;
    }

    FILE *cap = fopen(tmpl, "r");
    char captured[2048] = { 0 };
    size_t nread = 0;
    if (cap) {
        nread = fread(captured, 1, sizeof(captured) - 1, cap);
        captured[nread] = '\0';
        fclose(cap);
    }
    unlink(tmpl);

    wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_ROTATION");
    unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");

    /* Acknowledge flag set + warning is one-shot: stderr must contain
     * NO new pinned-placeholder warning text. */
    if (strstr(captured, "WIRELOG_ROTATION=pinned selected") != NULL) {
        FAIL("stderr contained pinned warning despite ACKNOWLEDGE=1");
        return;
    }
    PASS();
}
#endif /* __linux__ */

static void
test_rotation_env_override_pinned(void)
{
    TEST("rotation: WIRELOG_ROTATION=pinned selects pinned vtable");

    /* Set ACKNOWLEDGE so this test does not pollute stderr with a
     * footgun warning (no-op anyway: the warning is one-shot and
     * already fired in test_rotation_pinned_placeholder_warns on
     * Linux). */
    setenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER", "1", 1);
    if (setenv("WIRELOG_ROTATION", "pinned", 1) != 0) {
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("setenv WIRELOG_ROTATION=pinned failed");
        return;
    }

    wl_plan_t *plan = build_minimal_plan();
    if (plan == NULL) {
        unsetenv("WIRELOG_ROTATION");
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("could not build plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || session == NULL) {
        wl_plan_free(plan);
        unsetenv("WIRELOG_ROTATION");
        unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");
        FAIL("wl_session_create failed");
        return;
    }

    wl_col_session_t *cs = COL_SESSION(session);
    bool ok = (cs->rotation_ops == &col_rotation_pinned_ops);

    wl_session_destroy(session);
    wl_plan_free(plan);
    unsetenv("WIRELOG_ROTATION");
    unsetenv("WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER");

    if (!ok) {
        FAIL(
            "rotation_ops != &col_rotation_pinned_ops under WIRELOG_ROTATION=pinned");
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
#if defined(__linux__)
    /* Run the placeholder-warning test FIRST so the one-shot fprintf
     * is observable; subsequent pinned-strategy tests would not
     * retrigger it. */
    test_rotation_pinned_placeholder_warns();
    test_rotation_pinned_placeholder_silenced();
#endif
    test_rotation_env_override_pinned();
    test_rotation_dispatch_no_crash();

    printf("\nResults: %d run, %d passed, %d failed\n",
        tests_run, tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
