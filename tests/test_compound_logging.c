/*
 * tests/test_compound_logging.c - COMPOUND observability & error handling
 * (Issue #532 Task 6).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Drives the COMPOUND section of the WL_LOG logger end-to-end.  Each test
 * configures WL_LOG with a specific level for the COMPOUND section, routes
 * output to a tmpfile via WL_LOG_FILE, and asserts that the emitted text
 * carries the structured event tags added in relation.c/eval.c.
 *
 *   test_compound_logging       -- success-path TRACE emission carries the
 *                                  [TRACE][COMPOUND] prefix and path=inline
 *                                  tag.
 *   test_error_arity_overflow   -- store() with a compound column declared
 *                                  above WL_COMPOUND_INLINE_MAX_ARITY emits
 *                                  error=arity_overflow during schema apply.
 *   test_error_nesting          -- depth > WL_COMPOUND_INLINE_MAX_DEPTH
 *                                  emits error=depth_overflow.
 *   test_error_validation       -- row-out-of-range and arity-mismatch paths
 *                                  emit their respective error tags.
 *   test_observability_gates    -- WL_LOG unset produces zero output;
 *                                  COMPOUND:2 suppresses TRACE/DEBUG while
 *                                  WARN error messages still appear.
 *
 * The tests touch only the inline-tier surfaces (relation.c schema apply,
 * eval.c store/retrieve/retract) — they do not spin a session or plan.
 * K-Fusion contract references (§5 C1-C5) live in the instrumented
 * source; here we verify the user-facing message shape.
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/internal.h"
#include "../wirelog/util/log.h"
#include "../wirelog/wirelog-types.h"

#include <errno.h>
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
        "%s%cwl_compound_log_%s_%ld_%ld.log",
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
/* Fixtures                                                                 */
/* ======================================================================== */

/* (scalar, inline/2) schema — mirrors the smallest viable inline fixture.
 * Caller owns the returned relation and must col_rel_destroy it. */
static int
build_inline_fixture_(col_rel_t **out_rel)
{
    col_rel_t *rel = NULL;
    if (col_rel_alloc(&rel, "logging_fixture") != 0 || !rel)
        return -1;

    const col_rel_logical_col_t cols[2] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
    };
    if (col_rel_apply_compound_schema(rel, cols, 2) != 0) {
        col_rel_destroy(rel);
        return -1;
    }
    const char *names[3] = { "s", "c0", "c1" };
    if (col_rel_set_schema(rel, 3, names) != 0) {
        col_rel_destroy(rel);
        return -1;
    }
    const int64_t zero_row[3] = { 0, 0, 0 };
    if (col_rel_append_row(rel, zero_row) != 0) {
        col_rel_destroy(rel);
        return -1;
    }
    *out_rel = rel;
    return 0;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_compound_logging(void)
{
    TEST("compound logging: success path emits [TRACE][COMPOUND] path=inline");
    col_rel_t *rel = NULL;

    clear_env_();
    make_tmpfile_("trace");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    setenv("WL_LOG", "COMPOUND:5", 1);
    wl_log_init();

    if (build_inline_fixture_(&rel) != 0) FAIL("fixture failed");

    const int64_t args[2] = { 11, 22 };
    ASSERT(wl_col_rel_store_inline_compound(rel, 0, 1, args, 2) == 0,
        "store failed");

    wl_log_shutdown();

    char buf[4096] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n > 0, "log file empty");
    ASSERT(strstr(buf, "[TRACE][COMPOUND]") != NULL,
        "missing [TRACE][COMPOUND] prefix");
    ASSERT(strstr(buf, "event=store") != NULL, "missing event=store tag");
    ASSERT(strstr(buf, "path=inline") != NULL, "missing path=inline tag");
    ASSERT(strstr(buf, "arity=2") != NULL, "missing arity=2 tag");

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
    (void)remove(tmp_path_);
    clear_env_();
}

static void
test_error_arity_overflow(void)
{
    TEST("error: arity_overflow emitted for arity > MAX_ARITY");

    clear_env_();
    make_tmpfile_("arity");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    setenv("WL_LOG", "COMPOUND:5", 1);
    wl_log_init();

    /* Arity = MAX+1 must be rejected during layout. */
    const col_rel_logical_col_t cols[1] = {
        { WIRELOG_COMPOUND_KIND_INLINE,
          WL_COMPOUND_INLINE_MAX_ARITY + 1u, 1u },
    };
    uint32_t physical = 0u;
    int rc = col_rel_compute_physical_layout(cols, 1, &physical, NULL, NULL,
            NULL);
    ASSERT(rc == EINVAL, "layout did not reject oversize arity");

    wl_log_shutdown();

    char buf[2048] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n > 0, "log file empty");
    ASSERT(strstr(buf, "[WARN][COMPOUND]") != NULL,
        "missing [WARN][COMPOUND] prefix");
    ASSERT(strstr(buf, "error=arity_overflow") != NULL,
        "missing error=arity_overflow tag");
    /* Expected/got values must be present for actionable DX. */
    ASSERT(strstr(buf, "expected=") != NULL, "missing expected=");
    ASSERT(strstr(buf, "got=") != NULL, "missing got=");

    PASS();
cleanup:
    (void)remove(tmp_path_);
    clear_env_();
}

static void
test_error_nesting(void)
{
    TEST("error: depth_overflow emitted for depth > MAX_DEPTH");

    clear_env_();
    make_tmpfile_("depth");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    setenv("WL_LOG", "COMPOUND:5", 1);
    wl_log_init();

    const col_rel_logical_col_t cols[1] = {
        { WIRELOG_COMPOUND_KIND_INLINE, 2u,
          WL_COMPOUND_INLINE_MAX_DEPTH + 1u },
    };
    int rc = col_rel_compute_physical_layout(cols, 1, NULL, NULL, NULL, NULL);
    ASSERT(rc == EINVAL, "layout did not reject oversize depth");

    wl_log_shutdown();

    char buf[2048] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n > 0, "log file empty");
    ASSERT(strstr(buf, "error=depth_overflow") != NULL,
        "missing error=depth_overflow");

    PASS();
cleanup:
    (void)remove(tmp_path_);
    clear_env_();
}

static void
test_error_validation(void)
{
    TEST("error: row_oor and arity_mismatch tags");
    col_rel_t *rel = NULL;

    clear_env_();
    make_tmpfile_("valid");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    setenv("WL_LOG", "COMPOUND:5", 1);
    wl_log_init();

    if (build_inline_fixture_(&rel) != 0) FAIL("fixture failed");

    /* Row out of range. */
    const int64_t args[2] = { 1, 2 };
    ASSERT(wl_col_rel_store_inline_compound(rel, 42u, 1, args, 2) == EINVAL,
        "row_oor not rejected");

    /* Arity mismatch: schema declares arity=2, we pass 3. */
    const int64_t wrong[3] = { 1, 2, 3 };
    ASSERT(wl_col_rel_store_inline_compound(rel, 0, 1, wrong, 3) == EINVAL,
        "arity mismatch not rejected");

    /* Retrieve on a non-INLINE relation -> not_inline tag.  Needs at least
     * one row so the row_idx precondition passes and the locate path runs. */
    col_rel_t *scalar_rel = NULL;
    ASSERT(col_rel_alloc(&scalar_rel, "scalar_only") == 0, "scalar alloc");
    const char *snames[1] = { "x" };
    ASSERT(col_rel_set_schema(scalar_rel, 1, snames) == 0, "scalar schema");
    const int64_t srow[1] = { 0 };
    ASSERT(col_rel_append_row(scalar_rel, srow) == 0, "scalar append");
    int64_t out_args[2] = { 0, 0 };
    ASSERT(wl_col_rel_retrieve_inline_compound(scalar_rel, 0, 0,
        out_args, 2) == EINVAL, "not_inline not rejected");
    col_rel_destroy(scalar_rel);

    wl_log_shutdown();

    char buf[4096] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n > 0, "log file empty");
    ASSERT(strstr(buf, "error=row_oor") != NULL, "missing error=row_oor");
    ASSERT(strstr(buf, "error=arity_mismatch") != NULL,
        "missing error=arity_mismatch");
    ASSERT(strstr(buf, "error=not_inline") != NULL,
        "missing error=not_inline");

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
    (void)remove(tmp_path_);
    clear_env_();
}

static void
test_observability_gates(void)
{
    TEST("observability: WL_LOG unset -> silent; COMPOUND:2 -> WARN only");
    col_rel_t *rel = NULL;

    /* (a) Unset WL_LOG produces zero output. */
    clear_env_();
    make_tmpfile_("gate_off");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    wl_log_init();

    if (build_inline_fixture_(&rel) != 0) FAIL("fixture failed");
    const int64_t args[2] = { 5, 6 };
    (void)wl_col_rel_store_inline_compound(rel, 0, 1, args, 2);
    /* Trigger a WARN path too; must stay silent. */
    (void)wl_col_rel_store_inline_compound(rel, 99u, 1, args, 2);
    col_rel_destroy(rel);
    rel = NULL;

    wl_log_shutdown();

    char buf[2048] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n == 0, "output emitted with WL_LOG unset");
    (void)remove(tmp_path_);

    /* (b) COMPOUND:2 (WARN ceiling) suppresses TRACE/DEBUG but keeps
     * WARN error messages. */
    clear_env_();
    make_tmpfile_("gate_warn");
    setenv("WL_LOG_FILE", tmp_path_, 1);
    setenv("WL_LOG", "COMPOUND:2", 1);
    wl_log_init();

    if (build_inline_fixture_(&rel) != 0) FAIL("fixture failed");
    ASSERT(wl_col_rel_store_inline_compound(rel, 0, 1, args, 2) == 0,
        "store success");
    ASSERT(wl_col_rel_store_inline_compound(rel, 99u, 1, args, 2) == EINVAL,
        "row_oor rejected");

    wl_log_shutdown();

    memset(buf, 0, sizeof(buf));
    n = read_file_(tmp_path_, buf, sizeof(buf));
    ASSERT(n > 0, "WARN-level log empty");
    ASSERT(strstr(buf, "[WARN][COMPOUND]") != NULL,
        "WARN line missing");
    ASSERT(strstr(buf, "[TRACE][COMPOUND]") == NULL,
        "TRACE leaked past WARN ceiling");
    ASSERT(strstr(buf, "[DEBUG][COMPOUND]") == NULL,
        "DEBUG leaked past WARN ceiling");

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
    (void)remove(tmp_path_);
    clear_env_();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_compound_logging (Issue #532 Task 6)\n");
    printf("=========================================\n");

    test_compound_logging();
    test_error_arity_overflow();
    test_error_nesting();
    test_error_validation();
    test_observability_gates();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
