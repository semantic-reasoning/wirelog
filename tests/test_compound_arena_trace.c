/*
 * test_compound_arena_trace.c - Issue #583 lifecycle observability
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Drives the COMPOUND section of the WL_LOG logger across the
 * compound-arena lifecycle and asserts that
 *
 *   WL_LOG=COMPOUND:5
 *
 * yields the lifecycle tags added in session.c (create/destroy) and
 * compound_arena.c (alloc/freeze/unfreeze).  The events share the
 * `lifecycle event=<verb>` prefix so a single grep over the COMPOUND
 * section reconstructs the full timeline.
 *
 * This test exercises the arena directly (no session) for the
 * alloc/freeze/unfreeze events so it stays cheap and avoids pulling
 * in the full backend.  Session create/destroy traces are exercised by
 * the existing test_session_compound_arena_lifecycle suite, which
 * already runs under the standard observability harness; #583 only
 * needs to verify that the new COMPOUND tags are emitted at the right
 * level.
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/util/log.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
#  define getpid   _getpid
#else
#  include <unistd.h>
#endif

/* ======================================================================== */
/* Test harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            goto cleanup;                           \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* ======================================================================== */
/* tmpfile helpers                                                          */
/* ======================================================================== */

static char tmp_path_[256];

static const char *
tmpdir_(void)
{
    const char *d = getenv("TMPDIR");
    if (d && *d) return d;
#if defined(_WIN32)
    d = getenv("TEMP");
    if (d && *d) return d;
    d = getenv("TMP");
    if (d && *d) return d;
    return ".";
#else
    return "/tmp";
#endif
}

static void
make_tmpfile_(const char *tag)
{
    const char *d = tmpdir_();
#if defined(_WIN32)
    const char sep = '\\';
#else
    const char sep = '/';
#endif
    snprintf(tmp_path_, sizeof(tmp_path_),
        "%s%cwl_compound_arena_trace_%s_%ld_%ld.log",
        d, sep, tag, (long)getpid(), (long)time(NULL));
    (void)remove(tmp_path_);
}

static size_t
read_file_(const char *path, char *buf, size_t bufsz)
{
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    size_t n = fread(buf, 1, bufsz - 1, f);
    fclose(f);
    buf[n] = '\0';
    return n;
}

static void
clear_env_(void)
{
    unsetenv("WL_LOG");
    unsetenv("WL_LOG_FILE");
    unsetenv("WL_DEBUG_JOIN");
    unsetenv("WL_CONSOLIDATION_LOG");
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_arena_lifecycle_emits_compound_traces(void)
{
    TEST("WL_LOG=COMPOUND:5 captures alloc/freeze/unfreeze lifecycle events");

    wl_compound_arena_t *arena = NULL;

    clear_env_();
    make_tmpfile_("lifecycle");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    setenv("WL_LOG", "COMPOUND:5", 1);
    wl_log_init();

    arena = wl_compound_arena_create(0xCAFEu, 256u, 0u);
    if (!arena) FAIL("arena create");

    uint64_t handle = wl_compound_arena_alloc(arena, 32u);
    ASSERT(handle != WL_COMPOUND_HANDLE_NULL, "alloc returned NULL handle");

    wl_compound_arena_freeze(arena);
    /* Frozen alloc must still emit no spurious COMPOUND alloc trace. */
    uint64_t denied = wl_compound_arena_alloc(arena, 16u);
    ASSERT(denied == WL_COMPOUND_HANDLE_NULL, "frozen alloc accepted");

    wl_compound_arena_unfreeze(arena);

    wl_log_shutdown();

    char buf[8192] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n > 0, "log file empty");
    ASSERT(strstr(buf, "[TRACE][COMPOUND]") != NULL,
        "missing [TRACE][COMPOUND] prefix");
    ASSERT(strstr(buf, "lifecycle event=alloc") != NULL,
        "missing lifecycle event=alloc");
    ASSERT(strstr(buf, "lifecycle event=freeze") != NULL,
        "missing lifecycle event=freeze");
    ASSERT(strstr(buf, "lifecycle event=unfreeze") != NULL,
        "missing lifecycle event=unfreeze");

    /* Frozen-denied allocations must NOT emit a lifecycle event=alloc. */
    int alloc_count = 0;
    const char *p = buf;
    const char *needle = "lifecycle event=alloc";
    size_t needle_len = strlen(needle);
    while ((p = strstr(p, needle)) != NULL) {
        alloc_count++;
        p += needle_len;
    }
    ASSERT(alloc_count == 1,
        "alloc trace should fire exactly once (frozen alloc must not emit)");

    PASS();
cleanup:
    if (arena)
        wl_compound_arena_free(arena);
    (void)remove(tmp_path_);
    clear_env_();
}

static void
test_no_compound_traces_when_disabled(void)
{
    TEST("WL_LOG unset -> zero COMPOUND lifecycle traces emitted");

    wl_compound_arena_t *arena = NULL;

    clear_env_();
    make_tmpfile_("disabled");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    /* Note: no WL_LOG -> all sections at NONE; lifecycle traces must
     * not be emitted (this is the "no overhead" acceptance criterion
     * surfaced as observable behaviour). */
    wl_log_init();

    arena = wl_compound_arena_create(0xBEEFu, 256u, 0u);
    if (!arena) FAIL("arena create");
    (void)wl_compound_arena_alloc(arena, 32u);
    wl_compound_arena_freeze(arena);
    wl_compound_arena_unfreeze(arena);

    wl_log_shutdown();

    char buf[4096] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n == 0, "log file should be empty when WL_LOG is unset");

    PASS();
cleanup:
    if (arena)
        wl_compound_arena_free(arena);
    (void)remove(tmp_path_);
    clear_env_();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_compound_arena_trace (Issue #583)\n");
    printf("======================================\n");

    test_arena_lifecycle_emits_compound_traces();
    test_no_compound_traces_when_disabled();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
