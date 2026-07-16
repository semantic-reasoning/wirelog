/*
 * test_wirelog-easy.c - Unit tests for the wirelog_easy convenience facade (Issue #441)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/wirelog-easy.h"
#include "../wirelog/util/log.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#endif

#ifdef _WIN32
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
#define SKIP(msg)                         \
        do {                                  \
            printf(" ... SKIP: %s\n", (msg)); \
        } while (0)

/* ======================================================================== */
/* Shared Datalog Programs                                                  */
/* ======================================================================== */

static const char *ACCESS_CONTROL_SRC
    = ".decl can(user: symbol, perm: symbol)\n"
    ".decl granted(user: symbol, perm: symbol)\n"
    "granted(U, P) :- can(U, P).\n";

/* ======================================================================== */
/* Delta Collector                                                          */
/* ======================================================================== */

#define MAX_DELTAS 64
#define MAX_COLS 8

typedef struct {
    int count;
    char relations[MAX_DELTAS][32];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
    int32_t diffs[MAX_DELTAS];
} delta_collector_t;

static void
collect_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    delta_collector_t *c = (delta_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 31);
    c->relations[idx][31] = '\0';
    c->ncols[idx] = ncols;
    c->diffs[idx] = diff;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

/* ======================================================================== */
/* Tuple Collector                                                          */
/* ======================================================================== */

typedef struct {
    int count;
    char relations[MAX_DELTAS][32];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
} tuple_collector_t;

static void
collect_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    tuple_collector_t *c = (tuple_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 31);
    c->relations[idx][31] = '\0';
    c->ncols[idx] = ncols;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

static bool
drive_access_control_trace(wirelog_easy_session_t *s)
{
    int64_t alice = wirelog_easy_intern(s, "alice");
    int64_t read = wirelog_easy_intern(s, "read");
    if (alice < 0 || read < 0)
        return false;

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    if (wirelog_easy_set_delta_cb(s, collect_delta, &deltas) != WIRELOG_OK)
        return false;

    int64_t row[2] = { alice, read };
    if (wirelog_easy_insert(s, "can", row, 2) != WIRELOG_OK)
        return false;
    if (wirelog_easy_step(s) != WIRELOG_OK)
        return false;

    bool found_delta = false;
    for (int i = 0; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "granted") == 0
            && deltas.ncols[i] == 2 && deltas.rows[i][0] == alice
            && deltas.rows[i][1] == read && deltas.diffs[i] == 1) {
            found_delta = true;
            break;
        }
    }
    if (!found_delta)
        return false;

    tuple_collector_t granted_t;
    memset(&granted_t, 0, sizeof(granted_t));
    if (wirelog_easy_snapshot(s, "granted", collect_tuple, &granted_t)
        != WIRELOG_OK)
        return false;

    for (int i = 0; i < granted_t.count; i++) {
        if (strcmp(granted_t.relations[i], "granted") == 0
            && granted_t.ncols[i] == 2 && granted_t.rows[i][0] == alice
            && granted_t.rows[i][1] == read)
            return true;
    }
    return false;
}

static bool
file_contains_substring(const char *path, const char *needle)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return false;

    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strstr(buf, needle) != NULL;
}

#ifndef _WIN32
static bool
capture_num_workers_log(const wirelog_easy_open_opts_t *opts, uint32_t expected)
{
    char path[128];
    snprintf(path, sizeof(path), "/tmp/wl-easy-num-test-%ld-%u.log",
        (long)getpid(), expected);
    unlink(path);

    char needle[32];
    snprintf(needle, sizeof(needle), "num_workers=%u", expected);

    setenv("WL_LOG", "SESSION:4", 1);
    wl_log_init();

    int saved_stderr = dup(STDERR_FILENO);
    if (saved_stderr < 0) {
        unsetenv("WL_LOG");
        return false;
    }

    int log_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (log_fd < 0) {
        close(saved_stderr);
        unsetenv("WL_LOG");
        return false;
    }
    if (dup2(log_fd, STDERR_FILENO) < 0) {
        close(log_fd);
        close(saved_stderr);
        unsetenv("WL_LOG");
        return false;
    }
    close(log_fd);

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t open_rc = wirelog_easy_open_opts(ACCESS_CONTROL_SRC, opts,
            &s);
    wirelog_error_t build_rc = WIRELOG_ERR_EXEC;
    if (open_rc == WIRELOG_OK && s)
        build_rc = wirelog_easy_set_delta_cb(s, NULL, NULL);

    fflush(stderr);
    bool restored = dup2(saved_stderr, STDERR_FILENO) >= 0;
    close(saved_stderr);

    bool found = file_contains_substring(path, needle);
    unsetenv("WL_LOG");
    wl_log_init();
    if (s)
        wirelog_easy_close(s);
    unlink(path);

    return restored && open_rc == WIRELOG_OK && build_rc == WIRELOG_OK
           && found;
}
#endif

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/* PARITY: paired -- mirrored by test_null_safety on the advanced side (#785). */
static void
test_open_close_null_safe(void)
{
    TEST("open NULL src + NULL out + close(NULL) safe");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(NULL, &s) == WIRELOG_OK) {
        FAIL("expected non-OK on NULL src");
        return;
    }
    if (wirelog_easy_open(ACCESS_CONTROL_SRC, NULL) == WIRELOG_OK) {
        FAIL("expected non-OK on NULL out");
        return;
    }
    /* Must not crash */
    wirelog_easy_close(NULL);
    PASS();
}

static void
test_open_parse_error(void)
{
    TEST("open invalid Datalog returns error");

    wirelog_easy_session_t *s = (wirelog_easy_session_t *)0xdeadbeef;
    wirelog_error_t rc = wirelog_easy_open("this is not datalog ::: !!!", &s);
    if (rc == WIRELOG_OK) {
        FAIL("parse should have failed");
        return;
    }
    if (s != NULL) {
        FAIL("*out should be NULL on error");
        return;
    }
    PASS();
}

/* PARITY: facade-only -- wirelog_session_create has no opts struct (#785). */
static void
test_open_opts_null_equiv_to_open(void)
{
    TEST("open_opts NULL opts equivalent to open");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open_opts(ACCESS_CONTROL_SRC, NULL,
        &s) != WIRELOG_OK || !s) {
        FAIL("open_opts failed");
        return;
    }
    if (!drive_access_control_trace(s)) {
        FAIL("access-control trace failed");
        wirelog_easy_close(s);
        return;
    }
    wirelog_easy_close(s);

    PASS();
}

/* PARITY: facade-only -- advanced API has no opts struct to size-validate (#785). */
static void
test_open_opts_zero_size_rejected(void)
{
    TEST("open_opts rejects zero size");

    wirelog_easy_open_opts_t opts = { 0 };
    wirelog_easy_session_t *s = (wirelog_easy_session_t *)0xdeadbeef;
    wirelog_error_t rc = wirelog_easy_open_opts(ACCESS_CONTROL_SRC, &opts, &s);
    if (rc != WIRELOG_ERR_EXEC) {
        FAIL("expected WIRELOG_ERR_EXEC");
        return;
    }
    if (s != NULL) {
        FAIL("*out should be NULL on error");
        return;
    }
    PASS();
}

/* PARITY: facade-only -- no _reserved field on the advanced API surface (#785). */
static void
test_open_opts_reserved_rejected(void)
{
    TEST("open_opts rejects reserved field before parsing");

    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts._reserved = (const void *)0x1;
    wirelog_easy_session_t *s = (wirelog_easy_session_t *)0xdeadbeef;
    wirelog_error_t rc
        = wirelog_easy_open_opts("this is not datalog", &opts, &s);
    if (rc != WIRELOG_ERR_EXEC) {
        FAIL("expected WIRELOG_ERR_EXEC");
        return;
    }
    if (s != NULL) {
        FAIL("*out should be NULL on error");
        return;
    }
    PASS();
}

/* PARITY: facade-only -- WIRELOG_EASY_OPEN_OPTS_INIT is a facade convenience macro (#785). */
static void
test_open_opts_init_macro(void)
{
    TEST("open_opts init macro sets defaults");

    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    if (opts.size != sizeof(wirelog_easy_open_opts_t)) {
        FAIL("unexpected size");
        return;
    }
    if (opts.num_workers != 0) {
        FAIL("unexpected num_workers");
        return;
    }
    if (opts.eager_build) {
        FAIL("unexpected eager_build");
        return;
    }
    if (opts._reserved != NULL) {
        FAIL("unexpected _reserved");
        return;
    }
    PASS();
}

/* PARITY: facade-only -- advanced wirelog_session_create is always eager; no eager_build flag (#785). */
static void
test_open_opts_eager_build_ok(void)
{
    TEST("open_opts eager_build opens usable session");

    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.eager_build = true;
    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open_opts(ACCESS_CONTROL_SRC, &opts,
        &s) != WIRELOG_OK || !s) {
        FAIL("open_opts eager_build failed");
        return;
    }
    if (!drive_access_control_trace(s)) {
        FAIL("access-control trace failed");
        wirelog_easy_close(s);
        return;
    }
    wirelog_easy_close(s);
    PASS();
}

/*
 * The codebase has no fixture for "parses-clean-but-plans-dirty" today, so
 * the eager-build error contract is currently exercised only via the
 * parse-error path (per the architect+critic synthesis plan, scope-narrow per
 * Critic MAJOR #6).
 */
/* PARITY: facade-only -- advanced parse error is covered by test_open_parse_error (#785). */
static void
test_open_opts_eager_build_propagates_parse_error(void)
{
    TEST("open_opts eager_build propagates parse error");

    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.eager_build = true;
    wirelog_easy_session_t *s = (wirelog_easy_session_t *)0xdeadbeef;
    wirelog_error_t rc
        = wirelog_easy_open_opts("definitely not datalog", &opts, &s);
    if (rc != WIRELOG_ERR_PARSE) {
        FAIL("expected WIRELOG_ERR_PARSE");
        return;
    }
    if (s != NULL) {
        FAIL("*out should be NULL on error");
        return;
    }
    PASS();
}

static void
test_num_workers_default_is_one(void)
{
    TEST("open_opts default num_workers logs one");

#ifdef _WIN32
    SKIP("POSIX fd redirection not available on Windows");
    return;
#else
    if (!capture_num_workers_log(NULL, 1)) {
        FAIL("expected num_workers=1 log");
        return;
    }
    PASS();
#endif
}

static void
test_num_workers_explicit_four(void)
{
    TEST("open_opts explicit num_workers logs four");

#ifdef _WIN32
    SKIP("POSIX fd redirection not available on Windows");
    return;
#else
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.num_workers = 4;
    if (!capture_num_workers_log(&opts, 4)) {
        FAIL("expected num_workers=4 log");
        return;
    }
    PASS();
#endif
}

static void
test_intern_returns_same_id(void)
{
    TEST("intern same string returns same id");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    int64_t a = wirelog_easy_intern(s, "alice");
    int64_t b = wirelog_easy_intern(s, "alice");
    if (a < 0 || b < 0 || a != b) {
        FAIL("intern returned inconsistent ids");
        wirelog_easy_close(s);
        return;
    }
    wirelog_easy_close(s);
    PASS();
}

static void
test_insert_step_delta(void)
{
    TEST("insert + step fires delta callback");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    int64_t alice = wirelog_easy_intern(s, "alice");
    int64_t read = wirelog_easy_intern(s, "read");
    if (alice < 0 || read < 0) {
        FAIL("intern failed");
        wirelog_easy_close(s);
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wirelog_easy_set_delta_cb(s, collect_delta, &deltas);

    int64_t row[2] = { alice, read };
    if (wirelog_easy_insert(s, "can", row, 2) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wirelog_easy_close(s);
        return;
    }

    bool found = false;
    for (int i = 0; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "granted") == 0
            && deltas.ncols[i] == 2 && deltas.rows[i][0] == alice
            && deltas.rows[i][1] == read && deltas.diffs[i] == 1) {
            found = true;
            break;
        }
    }
    wirelog_easy_close(s);
    if (!found) {
        FAIL("expected +granted(alice,read) delta not seen");
        return;
    }
    PASS();
}

static void
test_inline_compound_body_binding(void)
{
    TEST("inline compound body pattern binds public Datalog variables");

    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, metadata(Level, Ts, Host, Risk)), Risk > 80.\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t hot_row[5] = { 7, 1, 2, 3, 90 };
    int64_t cold_row[5] = { 8, 1, 2, 3, 40 };
    if (wirelog_easy_insert(s, "event", hot_row, 5) != WIRELOG_OK
        || wirelog_easy_insert(s, "event", cold_row, 5) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t hot;
    memset(&hot, 0, sizeof(hot));
    if (wirelog_easy_snapshot(s, "hot", collect_tuple, &hot) != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (hot.count != 1 || strcmp(hot.relations[0], "hot") != 0
        || hot.ncols[0] != 1 || hot.rows[0][0] != 7) {
        FAIL("expected only hot(7)");
        return;
    }
    PASS();
}

static void
test_inline_compound_body_join_binding(void)
{
    TEST("inline compound body variables participate in joins");

    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl threshold(risk: int64)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, metadata(Level, Ts, Host, Risk)), "
        "threshold(Risk).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t matched_row[5] = { 7, 1, 2, 3, 90 };
    int64_t unmatched_row[5] = { 8, 1, 2, 3, 40 };
    int64_t threshold_row[1] = { 90 };
    if (wirelog_easy_insert(s, "event", matched_row, 5) != WIRELOG_OK
        || wirelog_easy_insert(s, "event", unmatched_row, 5) != WIRELOG_OK
        || wirelog_easy_insert(s, "threshold", threshold_row,
        1) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t hot;
    memset(&hot, 0, sizeof(hot));
    if (wirelog_easy_snapshot(s, "hot", collect_tuple, &hot) != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (hot.count != 1 || strcmp(hot.relations[0], "hot") != 0
        || hot.ncols[0] != 1 || hot.rows[0][0] != 7) {
        FAIL("expected join to derive only hot(7)");
        return;
    }
    PASS();
}

static void
test_inline_compound_functor_mismatch_is_empty(void)
{
    TEST("inline compound functor mismatch does not match");

    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl bad(id: int64)\n"
        "bad(ID) :- event(ID, other(Level, Ts, Host, Risk)).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t row[5] = { 7, 1, 2, 3, 90 };
    if (wirelog_easy_insert(s, "event", row, 5) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t bad;
    memset(&bad, 0, sizeof(bad));
    if (wirelog_easy_snapshot(s, "bad", collect_tuple, &bad) != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (bad.count != 0) {
        FAIL("mismatched functor should derive no rows");
        return;
    }
    PASS();
}

static void
test_inline_compound_constant_child_filters(void)
{
    TEST("inline compound constant child filters rows");

    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, metadata(_, _, _, 90)).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t hot_row[5] = { 7, 1, 2, 3, 90 };
    int64_t cold_row[5] = { 8, 1, 2, 3, 40 };
    if (wirelog_easy_insert(s, "event", hot_row, 5) != WIRELOG_OK
        || wirelog_easy_insert(s, "event", cold_row, 5) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t hot;
    memset(&hot, 0, sizeof(hot));
    if (wirelog_easy_snapshot(s, "hot", collect_tuple, &hot) != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (hot.count != 1 || hot.ncols[0] != 1 || hot.rows[0][0] != 7) {
        FAIL("expected only row with constant child value 90");
        return;
    }
    PASS();
}

static void
test_inline_compound_duplicate_child_variables_filter(void)
{
    TEST("inline compound duplicate child variables filter rows");

    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl same(id: int64)\n"
        "same(ID) :- event(ID, metadata(X, X, _, _)).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t matched_row[5] = { 7, 4, 4, 3, 90 };
    int64_t unmatched_row[5] = { 8, 1, 2, 3, 90 };
    if (wirelog_easy_insert(s, "event", matched_row, 5) != WIRELOG_OK
        || wirelog_easy_insert(s, "event", unmatched_row, 5) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t same;
    memset(&same, 0, sizeof(same));
    if (wirelog_easy_snapshot(s, "same", collect_tuple, &same) != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (same.count != 1 || same.ncols[0] != 1 || same.rows[0][0] != 7) {
        FAIL("expected only row with equal duplicate child variables");
        return;
    }
    PASS();
}

static wirelog_error_t
make_compound4(wirelog_easy_session_t *s, const char *functor, int64_t a0,
    int64_t a1, int64_t a2, int64_t a3, uint64_t *handle)
{
    wirelog_compound_arg_t args[4] = {
        { WIRELOG_TYPE_INT64, a0 },
        { WIRELOG_TYPE_INT64, a1 },
        { WIRELOG_TYPE_INT64, a2 },
        { WIRELOG_TYPE_INT64, a3 },
    };
    return wirelog_easy_make_compound(s, functor, 4, args, handle);
}

static wirelog_error_t
make_compound1(wirelog_easy_session_t *s, const char *functor, int64_t a0,
    uint64_t *handle)
{
    wirelog_compound_arg_t args[1] = {
        { WIRELOG_TYPE_INT64, a0 },
    };
    return wirelog_easy_make_compound(s, functor, 1, args, handle);
}

static void
test_side_compound_body_field_binding(void)
{
    TEST("side compound body pattern binds fields through side join");

    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl host_hit(id: int64, host: symbol)\n"
        "host_hit(ID, Host) :- event(ID, Tenant, "
        "metadata(Level, Ts, Host, Risk)), Risk > 80.\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t tenant = wirelog_easy_intern(s, "acme");
    int64_t host_a = wirelog_easy_intern(s, "edge-a");
    int64_t host_b = wirelog_easy_intern(s, "edge-b");
    uint64_t hot_handle = 0;
    uint64_t cold_handle = 0;
    if (tenant < 0 || host_a < 0 || host_b < 0
        || make_compound4(s, "metadata", 1, 100, host_a, 90,
        &hot_handle) != WIRELOG_OK
        || make_compound4(s, "metadata", 1, 101, host_b, 40,
        &cold_handle) != WIRELOG_OK) {
        FAIL("compound allocation failed");
        wirelog_easy_close(s);
        return;
    }

    int64_t hot_row[3] = { 7, tenant, (int64_t)hot_handle };
    int64_t cold_row[3] = { 8, tenant, (int64_t)cold_handle };
    if (wirelog_easy_insert(s, "event", hot_row, 3) != WIRELOG_OK
        || wirelog_easy_insert(s, "event", cold_row, 3) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t host_hit;
    memset(&host_hit, 0, sizeof(host_hit));
    if (wirelog_easy_snapshot(s, "host_hit", collect_tuple, &host_hit)
        != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (host_hit.count != 1 || host_hit.ncols[0] != 2
        || host_hit.rows[0][0] != 7 || host_hit.rows[0][1] != host_a) {
        FAIL("expected only host_hit(7, edge-a)");
        return;
    }
    PASS();
}

static void
test_side_compound_constant_child_filters(void)
{
    TEST("side compound constant child filters rows");

    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, _, metadata(_, _, _, 90)).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t tenant = wirelog_easy_intern(s, "acme");
    uint64_t hot_handle = 0;
    uint64_t cold_handle = 0;
    if (tenant < 0
        || make_compound4(s, "metadata", 1, 100, 3, 90,
        &hot_handle) != WIRELOG_OK
        || make_compound4(s, "metadata", 1, 101, 3, 40,
        &cold_handle) != WIRELOG_OK) {
        FAIL("compound allocation failed");
        wirelog_easy_close(s);
        return;
    }

    int64_t hot_row[3] = { 7, tenant, (int64_t)hot_handle };
    int64_t cold_row[3] = { 8, tenant, (int64_t)cold_handle };
    if (wirelog_easy_insert(s, "event", hot_row, 3) != WIRELOG_OK
        || wirelog_easy_insert(s, "event", cold_row, 3) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t hot;
    memset(&hot, 0, sizeof(hot));
    if (wirelog_easy_snapshot(s, "hot", collect_tuple, &hot) != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (hot.count != 1 || hot.ncols[0] != 1 || hot.rows[0][0] != 7) {
        FAIL("expected only side row with constant child value 90");
        return;
    }
    PASS();
}

static void
test_side_compound_duplicate_child_variables_filter(void)
{
    TEST("side compound duplicate child variables filter rows");

    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl same(id: int64)\n"
        "same(ID) :- event(ID, _, metadata(X, X, _, _)).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t tenant = wirelog_easy_intern(s, "acme");
    uint64_t matched_handle = 0;
    uint64_t unmatched_handle = 0;
    if (tenant < 0
        || make_compound4(s, "metadata", 4, 4, 3, 90,
        &matched_handle) != WIRELOG_OK
        || make_compound4(s, "metadata", 1, 2, 3, 90,
        &unmatched_handle) != WIRELOG_OK) {
        FAIL("compound allocation failed");
        wirelog_easy_close(s);
        return;
    }

    int64_t matched_row[3] = { 7, tenant, (int64_t)matched_handle };
    int64_t unmatched_row[3] = { 8, tenant, (int64_t)unmatched_handle };
    if (wirelog_easy_insert(s, "event", matched_row, 3) != WIRELOG_OK
        || wirelog_easy_insert(s, "event", unmatched_row, 3) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t same;
    memset(&same, 0, sizeof(same));
    if (wirelog_easy_snapshot(s, "same", collect_tuple, &same)
        != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (same.count != 1 || same.ncols[0] != 1 || same.rows[0][0] != 7) {
        FAIL("expected only side row with equal duplicate child variables");
        return;
    }
    PASS();
}

static void
test_side_compound_wrong_functor_handle_no_match(void)
{
    TEST("side compound wrong functor handle does not match metadata pattern");

    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl seen(id: int64)\n"
        "seen(ID) :- event(ID, _, metadata(_, _, _, _)).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    int64_t tenant = wirelog_easy_intern(s, "acme");
    uint64_t wrong_handle = 0;
    if (tenant < 0
        || make_compound4(s, "other", 1, 2, 3, 4,
        &wrong_handle) != WIRELOG_OK) {
        FAIL("compound allocation failed");
        wirelog_easy_close(s);
        return;
    }

    int64_t row[3] = { 7, tenant, (int64_t)wrong_handle };
    if (wirelog_easy_insert(s, "event", row, 3) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t seen;
    memset(&seen, 0, sizeof(seen));
    if (wirelog_easy_snapshot(s, "seen", collect_tuple, &seen)
        != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (seen.count != 0) {
        FAIL("wrong functor handle should not join metadata side relation");
        return;
    }
    PASS();
}

static void
test_side_compound_nested_child_no_match(void)
{
    TEST("side compound nested child pattern is not recursive");

    const char *src
        = ".decl record(id: int64, scope_col: scope/1 side)\n"
        ".decl seen(id: int64)\n"
        "seen(ID) :- record(ID, scope(metadata(Level, Ts, Host, Risk))).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    uint64_t metadata_handle = 0;
    uint64_t scope_handle = 0;
    if (make_compound4(s, "metadata", 1, 100, 3, 90, &metadata_handle)
        != WIRELOG_OK
        || make_compound1(s, "scope", (int64_t)metadata_handle,
        &scope_handle) != WIRELOG_OK) {
        FAIL("compound allocation failed");
        wirelog_easy_close(s);
        return;
    }

    int64_t row[2] = { 7, (int64_t)scope_handle };
    if (wirelog_easy_insert(s, "record", row, 2) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t seen;
    memset(&seen, 0, sizeof(seen));
    if (wirelog_easy_snapshot(s, "seen", collect_tuple, &seen)
        != WIRELOG_OK) {
        FAIL("snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (seen.count != 0) {
        FAIL("nested side body destructuring should not match");
        return;
    }
    PASS();
}

static void
test_negated_side_compound_body_pattern_rejected(void)
{
    TEST("negated side compound body pattern is rejected");

    const char *src
        = ".decl all_events(id: int64)\n"
        ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl clean(id: int64)\n"
        "clean(ID) :- all_events(ID), !event(ID, metadata(_, _, _, 90)).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) == WIRELOG_OK || s) {
        wirelog_easy_close(s);
        FAIL("negated side compound pattern should fail open");
        return;
    }
    PASS();
}

/* Issue #920: a variable that appears only inside a negated body atom has an
 * unbounded range and must be rejected (range-restriction / safety). */
static void
test_unsafe_negation_variable_rejected(void)
{
    TEST("unsafe variable in negated atom is rejected");

    const char *src
        = ".decl a(x: symbol)\n"
        ".decl b(x: symbol, y: symbol)\n"
        ".decl c(x: symbol)\n"
        "c(X) :- a(X), !b(X, Y).\n"; /* Y bound only under negation */

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) == WIRELOG_OK || s) {
        wirelog_easy_close(s);
        FAIL("unsafe negation rule should fail open");
        return;
    }
    PASS();
}

/* Issue #920: the safe form, where the negated relation is projected to a key
 * relation first so every negated variable is positively bound, is accepted.
 * A wildcard under negation is likewise safe. */
static void
test_safe_negation_via_projection_accepted(void)
{
    TEST("safe negation (projected key / wildcard) is accepted");

    const char *src
        = ".decl a(x: symbol)\n"
        ".decl b(x: symbol, y: symbol)\n"
        ".decl b_key(x: symbol)\n"
        ".decl c(x: symbol)\n"
        ".decl d(x: symbol)\n"
        "b_key(X) :- b(X, Y).\n"          /* project away Y */
        "c(X) :- a(X), !b_key(X).\n"      /* X positively bound */
        "d(X) :- a(X), !b(X, _).\n";      /* wildcard column is safe */

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("safe negation rule should open");
        return;
    }
    wirelog_easy_close(s);
    PASS();
}

static void
test_side_compound_public_allocation_saturates(void)
{
    TEST("public side compound allocation reports saturation");

    if (setenv("WIRELOG_COMPOUND_MAX_EPOCHS", "2", 1) != 0) {
        FAIL("setenv failed");
        return;
    }

    const char *src
        = ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl seen(id: int64)\n"
        ".decl edge(x: int64, y: int64)\n"
        ".decl tc(x: int64, y: int64)\n"
        "seen(ID) :- event(ID, _).\n"
        "tc(X, Y) :- edge(X, Y).\n"
        "tc(X, Z) :- edge(X, Y), tc(Y, Z).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        unsetenv("WIRELOG_COMPOUND_MAX_EPOCHS");
        FAIL("open failed");
        return;
    }

    wirelog_compound_arg_t args[4] = {
        { WIRELOG_TYPE_INT64, 1 },
        { WIRELOG_TYPE_INT64, 2 },
        { WIRELOG_TYPE_INT64, 3 },
        { WIRELOG_TYPE_INT64, 4 },
    };
    uint64_t handle = 0;
    wirelog_error_t rc = wirelog_easy_make_compound(s, "metadata", 4, args,
            &handle);
    if (rc != WIRELOG_OK || handle == 0) {
        wirelog_easy_close(s);
        unsetenv("WIRELOG_COMPOUND_MAX_EPOCHS");
        FAIL("first compound allocation failed");
        return;
    }
    int64_t row[2] = { 1, (int64_t)handle };
    int64_t edge_12[2] = { 1, 2 };
    int64_t edge_23[2] = { 2, 3 };
    int64_t edge_34[2] = { 3, 4 };
    if (wirelog_easy_insert(s, "event", row, 2) != WIRELOG_OK
        || wirelog_easy_insert(s, "edge", edge_12, 2) != WIRELOG_OK
        || wirelog_easy_insert(s, "edge", edge_23, 2) != WIRELOG_OK
        || wirelog_easy_insert(s, "edge", edge_34, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        unsetenv("WIRELOG_COMPOUND_MAX_EPOCHS");
        FAIL("first insert/step failed");
        return;
    }

    handle = 123;
    rc = wirelog_easy_make_compound(s, "metadata", 4, args, &handle);
    wirelog_easy_close(s);
    unsetenv("WIRELOG_COMPOUND_MAX_EPOCHS");
    if (rc != WIRELOG_ERR_COMPOUND_SATURATED || handle != 0) {
        FAIL("expected WIRELOG_ERR_COMPOUND_SATURATED with cleared handle");
        return;
    }
    PASS();
}

/* PARITY: facade-only -- no wirelog_session_insert_sym variadic on advanced (#785). */
static void
test_insert_sym_variadic(void)
{
    TEST("insert_sym variadic helper");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wirelog_easy_set_delta_cb(s, collect_delta, &deltas);

    if (wirelog_easy_insert_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("insert_sym failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wirelog_easy_close(s);
        return;
    }
    bool found = false;
    for (int i = 0; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "granted") == 0
            && deltas.diffs[i] == 1) {
            found = true;
            break;
        }
    }
    wirelog_easy_close(s);
    if (!found) {
        FAIL("no granted delta after insert_sym");
        return;
    }
    PASS();
}

/* PARITY: facade-only -- no wirelog_session_remove_sym; row-form remove paired by test_insert_remove_roundtrip (#785). */
static void
test_remove_sym(void)
{
    TEST("remove_sym fires negative delta");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wirelog_easy_set_delta_cb(s, collect_delta, &deltas);

    if (wirelog_easy_insert_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("insert_sym failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("step 1 failed");
        wirelog_easy_close(s);
        return;
    }
    int after_step1 = deltas.count;

    if (wirelog_easy_remove_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("remove_sym failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("step 2 failed");
        wirelog_easy_close(s);
        return;
    }

    bool found_neg = false;
    for (int i = after_step1; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "granted") == 0
            && deltas.diffs[i] == -1) {
            found_neg = true;
            break;
        }
    }
    wirelog_easy_close(s);
    if (!found_neg) {
        FAIL("no -granted delta after remove_sym + step");
        return;
    }
    PASS();
}

static void
test_snapshot_filter(void)
{
    TEST("snapshot filters by relation name");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    if (wirelog_easy_insert_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK
        || wirelog_easy_insert_sym(s, "can", "bob", "write", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("insert_sym failed");
        wirelog_easy_close(s);
        return;
    }
    /* NOTE: Do NOT call wirelog_easy_step() before wirelog_easy_snapshot().  The
     * columnar backend's snapshot path re-evaluates all strata and appends
     * to the IDB relation rows; a prior step() already derived the IDB
     * tuples, so combining the two would double-count.  See the doc
     * comment on wirelog_easy_snapshot() in wirelog-easy.h. */

    tuple_collector_t granted_t;
    memset(&granted_t, 0, sizeof(granted_t));
    if (wirelog_easy_snapshot(s, "granted", collect_tuple, &granted_t)
        != WIRELOG_OK) {
        FAIL("snapshot granted failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t can_t;
    memset(&can_t, 0, sizeof(can_t));
    if (wirelog_easy_snapshot(s, "can", collect_tuple, &can_t) != WIRELOG_OK) {
        FAIL("snapshot can failed");
        wirelog_easy_close(s);
        return;
    }

    wirelog_easy_close(s);

    if (granted_t.count != 2) {
        FAIL("expected 2 granted tuples in snapshot");
        return;
    }
    /* Filter must reject tuples whose relation != "granted" */
    for (int i = 0; i < granted_t.count; i++) {
        if (strcmp(granted_t.relations[i], "granted") != 0) {
            FAIL("granted snapshot leaked non-granted tuple");
            return;
        }
    }
    for (int i = 0; i < can_t.count; i++) {
        if (strcmp(can_t.relations[i], "can") != 0) {
            FAIL("can snapshot leaked non-can tuple");
            return;
        }
    }
    PASS();
}

/* PARITY: facade-only -- wirelog_easy_print_delta is a stdout helper; no advanced equivalent (#785). */
static void
test_print_delta_integer_column(void)
{
    TEST("print_delta on integer column does not abort");

    static const char *SRC = ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n";
    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    wirelog_easy_set_delta_cb(s, wirelog_easy_print_delta, s);
    int64_t row[1] = { 42 };
    if (wirelog_easy_insert(s, "a", row, 1) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wirelog_easy_close(s);
        return;
    }
    wirelog_easy_close(s);
    PASS();
}

/* PARITY: facade-only -- wirelog_easy_print_delta is a stdout helper; no advanced equivalent (#785). */
static void
test_print_delta_unknown_relation_integer_fallback(void)
{
    TEST("print_delta on unknown relation falls back to integer rendering");

#ifdef _WIN32
    SKIP("fork not available on Windows");
    return;
#else
    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork failed");
        return;
    }
    if (pid == 0) {
        /* Child: exits 0 only if print_delta completes without aborting.
         * We pass a relation name that the program does NOT declare, so
         * wirelog_program_get_schema() returns NULL.  Pre-fix, the
         * fallback set as_string=true for every column and the ids below
         * would trigger abort() on reverse-intern.  Post-fix, the
         * schema-less branch renders raw int64 values and returns
         * cleanly. */
        fclose(stdout);
        fclose(stderr);

        wirelog_easy_session_t *s = NULL;
        if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s)
            _exit(2);
        int64_t row[2] = { 123456789, 987654321 };
        wirelog_easy_print_delta("no_such_relation", row, 2, 1, s);
        wirelog_easy_close(s);
        _exit(0);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        FAIL("waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        FAIL("print_delta aborted on schema-unavailable path");
        return;
    }
    PASS();
#endif
}

/* PARITY: facade-only -- wirelog_easy_print_delta is a stdout helper; no advanced equivalent (#785). */
static void
test_print_delta_abort_on_missed_symbol(void)
{
    TEST("print_delta aborts on missed reverse-intern");

#ifdef _WIN32
    SKIP("fork not available on Windows");
    return;
#else
    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork failed");
        return;
    }
    if (pid == 0) {
        /* Child: silence stdio so the abort message does not pollute
         * the parent's test log. */
        fclose(stdout);
        fclose(stderr);

        wirelog_easy_session_t *s = NULL;
        if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s)
            _exit(2);
        wirelog_easy_set_delta_cb(s, wirelog_easy_print_delta, s);
        /* Bogus, never-interned ids — printer must abort. */
        int64_t row[2] = { 999999, 888888 };
        wirelog_easy_insert(s, "can", row, 2);
        wirelog_easy_step(s);
        wirelog_easy_close(s);
        _exit(0);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        FAIL("waitpid failed");
        return;
    }
    if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGABRT) {
        FAIL("child did not abort as expected");
        return;
    }
    PASS();
#endif
}

static void
test_cleanup_order_no_use_after_free(void)
{
    TEST("open/use/close repeated has no leaks");

    for (int iter = 0; iter < 2; iter++) {
        wirelog_easy_session_t *s = NULL;
        if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
            FAIL("open failed");
            return;
        }
        if (wirelog_easy_insert_sym(s, "can", "alice", "read",
            (const char *)NULL)
            != WIRELOG_OK) {
            FAIL("insert_sym failed");
            wirelog_easy_close(s);
            return;
        }
        if (wirelog_easy_step(s) != WIRELOG_OK) {
            FAIL("step failed");
            wirelog_easy_close(s);
            return;
        }
        wirelog_easy_close(s);
    }
    PASS();
}

static void
test_intern_after_step_succeeds(void)
{
    TEST("intern after first step still succeeds (Option B contract)");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    int64_t alice = wirelog_easy_intern(s, "alice");
    int64_t read = wirelog_easy_intern(s, "read");
    if (alice < 0 || read < 0) {
        FAIL("intern failed");
        wirelog_easy_close(s);
        return;
    }
    int64_t row[2] = { alice, read };
    if (wirelog_easy_insert(s, "can", row, 2) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wirelog_easy_close(s);
        return;
    }
    /* After the plan has been built and stepped, interning a brand new
     * symbol must still succeed and return a fresh id, because the intern
     * table is aliased through the whole session lifetime. */
    int64_t late = wirelog_easy_intern(s, "late_symbol");
    /* And a new insert using that id must also succeed, proving the id is
     * actually visible to the running backend. */
    int64_t late_row[2] = { late, read };
    wirelog_error_t ins_rc
        = wirelog_easy_insert(s, "can", late_row, 2);
    wirelog_error_t step_rc = wirelog_easy_step(s);
    wirelog_easy_close(s);
    if (late < 0) {
        FAIL("late intern should have returned a non-negative id");
        return;
    }
    if (ins_rc != WIRELOG_OK || step_rc != WIRELOG_OK) {
        FAIL("insert/step using late-interned id failed");
        return;
    }
    PASS();
}

static void
test_delta_cb_multi_round_recursive_insert(void)
{
    TEST("delta_cb + recursive insert/step rounds produce expected deltas");

    /* Issue #662: when wirelog_easy_set_delta_cb has installed a callback, a
     * subsequent wirelog_easy_insert must take the same incremental path that
     * wirelog_easy_remove already takes, so that arrangement caches are
     * invalidated and the outer-epoch counter advances.  Without that
     * symmetry, a second insert+step round on a recursive stratum either
     * skips the new iteration (no delta surfaces) or trips the join over
     * stale arrangements and the easy facade reports WIRELOG_ERR_EXEC. */
    static const char *RECURSIVE_REACH_SRC
        = ".decl edge(a: int64, b: int64)\n"
        ".decl reach(a: int64, b: int64)\n"
        "reach(A, B) :- edge(A, B).\n"
        "reach(A, C) :- reach(A, B), edge(B, C).\n";

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(RECURSIVE_REACH_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    if (wirelog_easy_set_delta_cb(s, collect_delta, &deltas) != WIRELOG_OK) {
        FAIL("set_delta_cb failed");
        wirelog_easy_close(s);
        return;
    }

    int64_t e12[2] = { 1, 2 };
    if (wirelog_easy_insert(s, "edge", e12, 2) != WIRELOG_OK) {
        FAIL("insert edge(1,2) failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("first step returned non-OK");
        wirelog_easy_close(s);
        return;
    }
    int round1_count = deltas.count;
    if (round1_count == 0) {
        FAIL("first step produced no deltas");
        wirelog_easy_close(s);
        return;
    }

    int64_t e23[2] = { 2, 3 };
    if (wirelog_easy_insert(s, "edge", e23, 2) != WIRELOG_OK) {
        FAIL("insert edge(2,3) failed");
        wirelog_easy_close(s);
        return;
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("second step returned non-OK (issue #662 reproduction)");
        wirelog_easy_close(s);
        return;
    }

    /* The transitive consequence reach(1,3) must surface as a +1 delta
     * during round 2.  A bug that suppresses the new iteration (stale
     * outer_epoch) produces +reach(2,3) but not +reach(1,3); a bug that
     * trips the join (stale arrangements) returns non-OK above. */
    bool saw_reach_1_3 = false;
    for (int i = round1_count; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "reach") == 0
            && deltas.ncols[i] == 2 && deltas.rows[i][0] == 1
            && deltas.rows[i][1] == 3 && deltas.diffs[i] == 1) {
            saw_reach_1_3 = true;
            break;
        }
    }
    wirelog_easy_close(s);
    if (!saw_reach_1_3) {
        FAIL("expected +reach(1,3) delta in round 2 not seen");
        return;
    }
    PASS();
}

/* Issue #665: a 5-relation conjunctive rule was reported to surface
* WIRELOG_ERR_EXEC from wirelog_easy_step when the EDB prerequisites were
* inserted across separate epochs and only some were present at step time.
* PR #663 (Closes #662) made col_session_insert reroute to the incremental
* path under an installed delta callback; that reroute also covers the
* partial-conjunction shape from #665.  Pin the shape and the per-epoch
* delta semantics so future refactors do not silently regress it. */
static const char *ISSUE_665_PROGRAM_SRC
    = ".decl grant(user: symbol, perm: symbol)\n"
    ".decl principal_state(user: symbol, state: symbol)\n"
    ".decl session_state(scope: symbol, state: symbol)\n"
    ".decl session_active(state: symbol)\n"
    ".decl perm_state(user: symbol, perm: symbol, scope: symbol, "
    "state: symbol)\n"
    ".decl allow_bool(user: symbol, perm: symbol, scope: symbol)\n"
    "allow_bool(U, P, S) :-\n"
    "    grant(U, P),\n"
    "    principal_state(U, \"authenticated\"),\n"
    "    session_state(S, ST),\n"
    "    session_active(ST),\n"
    "    perm_state(U, P, S, \"armed\").\n";

static int
count_allow_bool_added(const delta_collector_t *deltas, int from)
{
    int hits = 0;
    for (int i = from; i < deltas->count; i++) {
        if (strcmp(deltas->relations[i], "allow_bool") == 0
            && deltas->diffs[i] == 1)
            hits++;
    }
    return hits;
}

static bool
drive_issue_665_partial_conjunction(wirelog_easy_session_t *s)
{
    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    if (wirelog_easy_set_delta_cb(s, collect_delta, &deltas) != WIRELOG_OK)
        return false;

    /* Step 1: only grant present. allow_bool requires four more EDB legs;
     * the partial-prereq step must succeed and emit no allow_bool delta. */
    if (wirelog_easy_insert_sym(s, "grant", "u", "p", (const char *)NULL)
        != WIRELOG_OK)
        return false;
    if (wirelog_easy_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, 0) != 0)
        return false;
    int after_grant = deltas.count;

    /* Step 2: add principal_state with the literal "authenticated". */
    if (wirelog_easy_insert_sym(s, "principal_state", "u", "authenticated",
        (const char *)NULL)
        != WIRELOG_OK)
        return false;
    if (wirelog_easy_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_grant) != 0)
        return false;
    int after_principal = deltas.count;

    /* Step 3: session_state(S, ST) — still missing session_active and
     * perm_state, so the join body is unsatisfied. */
    if (wirelog_easy_insert_sym(s, "session_state", "s", "st",
        (const char *)NULL)
        != WIRELOG_OK)
        return false;
    if (wirelog_easy_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_principal) != 0)
        return false;
    int after_session = deltas.count;

    /* Step 4: session_active(ST) — perm_state still missing. */
    if (wirelog_easy_insert_sym(s, "session_active", "st", (const char *)NULL)
        != WIRELOG_OK)
        return false;
    if (wirelog_easy_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_session) != 0)
        return false;
    int after_active = deltas.count;

    /* Step 5: perm_state(U, P, S, "armed") completes the conjunction.
     * Exactly one +allow_bool("u","p","s") must surface. */
    if (wirelog_easy_insert_sym(s, "perm_state", "u", "p", "s", "armed",
        (const char *)NULL)
        != WIRELOG_OK)
        return false;
    if (wirelog_easy_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_active) != 1)
        return false;

    /* Verify the bound row matches the inputs we interned through the
     * insert_sym helper: allow_bool(u, p, s). */
    int64_t u = wirelog_easy_intern(s, "u");
    int64_t p = wirelog_easy_intern(s, "p");
    int64_t scope = wirelog_easy_intern(s, "s");
    if (u < 0 || p < 0 || scope < 0)
        return false;
    bool matched = false;
    for (int i = after_active; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "allow_bool") != 0
            || deltas.diffs[i] != 1)
            continue;
        if (deltas.ncols[i] == 3 && deltas.rows[i][0] == u
            && deltas.rows[i][1] == p && deltas.rows[i][2] == scope) {
            matched = true;
            break;
        }
    }
    return matched;
}

/* PARITY: paired in test_wirelog_advanced.c by the same test name (#825). */
static void
test_issue_665_partial_conjunction_default_workers(void)
{
    TEST("issue 665: partial conjunctive EDB updates step OK across epochs");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(ISSUE_665_PROGRAM_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    bool ok = drive_issue_665_partial_conjunction(s);
    wirelog_easy_close(s);
    if (!ok) {
        FAIL("partial-prereq step regressed");
        return;
    }
    PASS();
}

/* PARITY: paired in test_wirelog_advanced.c by the same test name (#825). */
static void
test_issue_665_partial_conjunction_multi_worker(void)
{
    /* Singleton EDBs sit below the parallel keyed-join activation
     * threshold (col_join_should_parallelize_rows requires nrows >=
     * num_workers * WIRELOG_JOIN_PAR_MIN_LEFT_ROWS), so the joins still
     * run on the sequential path.  This variant locks the multi-worker
     * session plumbing — worker pool creation under an installed delta
     * callback, dispatch handoff, teardown — against the same per-epoch
     * delta contract that the default-workers test pins. */
    TEST("issue 665: partial conjunctive shape stable on num_workers=4");

    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.num_workers = 4;
    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open_opts(ISSUE_665_PROGRAM_SRC, &opts, &s) != WIRELOG_OK
        || !s) {
        FAIL("open_opts failed");
        return;
    }
    bool ok = drive_issue_665_partial_conjunction(s);
    wirelog_easy_close(s);
    if (!ok) {
        FAIL("partial-prereq step regressed under multi-worker");
        return;
    }
    PASS();
}

/* PARITY: facade-only -- this regression targets the easy query-mode API
 * contract; the advanced API has no snapshot-by-relation facade. */
static void
test_snapshot_rebuilds_idb_after_query_mode_input_changes(void)
{
    TEST("query snapshots rebuild IDB rows after input changes");

    const char *src
        = ".decl A(x: int32)\n"
        ".decl B(x: int32)\n"
        ".decl Q(x: int32)\n"
        "Q(x) :- A(x).\n"
        "Q(x) :- B(x).\n";
    int64_t row[] = { 7 };
    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    if (wirelog_easy_insert(s, "A", row, 1) != WIRELOG_OK
        || wirelog_easy_insert(s, "B", row, 1) != WIRELOG_OK) {
        FAIL("insert failed");
        wirelog_easy_close(s);
        return;
    }

    tuple_collector_t q;
    memset(&q, 0, sizeof(q));
    if (wirelog_easy_snapshot(s, "Q", collect_tuple, &q) != WIRELOG_OK
        || q.count != 1) {
        FAIL("initial snapshot should contain one Q row");
        wirelog_easy_close(s);
        return;
    }
    memset(&q, 0, sizeof(q));
    if (wirelog_easy_remove(s, "A", row, 1) != WIRELOG_OK
        || wirelog_easy_snapshot(s, "Q", collect_tuple, &q) != WIRELOG_OK
        || q.count != 1) {
        FAIL("retraction snapshot failed");
        wirelog_easy_close(s);
        return;
    }

    memset(&q, 0, sizeof(q));
    if (wirelog_easy_remove(s, "B", row, 1) != WIRELOG_OK
        || wirelog_easy_snapshot(s, "Q", collect_tuple, &q) != WIRELOG_OK
        || q.count != 0) {
        FAIL("fully retracted Q row must not remain materialized");
        wirelog_easy_close(s);
        return;
    }
    wirelog_easy_close(s);

    s = NULL;
    if (wirelog_easy_open(src, &s) != WIRELOG_OK || !s
        || wirelog_easy_insert(s, "B", row, 1) != WIRELOG_OK) {
        FAIL("incremental-insert setup failed");
        if (s)
            wirelog_easy_close(s);
        return;
    }
    memset(&q, 0, sizeof(q));
    if (wirelog_easy_snapshot(s, "Q", collect_tuple, &q) != WIRELOG_OK
        || q.count != 1
        || wirelog_easy_insert(s, "A", row, 1) != WIRELOG_OK) {
        FAIL("incremental-insert setup snapshot failed");
        wirelog_easy_close(s);
        return;
    }
    memset(&q, 0, sizeof(q));
    if (wirelog_easy_snapshot(s, "Q", collect_tuple, &q) != WIRELOG_OK
        || q.count != 1) {
        FAIL("input change must not duplicate Q rows");
        wirelog_easy_close(s);
        return;
    }
    wirelog_easy_close(s);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("wirelog_easy Tests (Issue #441)\n");
    printf("==========================\n\n");

    test_open_close_null_safe();
    test_open_parse_error();
    test_open_opts_null_equiv_to_open();
    test_open_opts_zero_size_rejected();
    test_open_opts_reserved_rejected();
    test_open_opts_init_macro();
    test_open_opts_eager_build_ok();
    test_open_opts_eager_build_propagates_parse_error();
    test_num_workers_default_is_one();
    test_num_workers_explicit_four();
    test_intern_returns_same_id();
    test_insert_step_delta();
    test_inline_compound_body_binding();
    test_inline_compound_body_join_binding();
    test_inline_compound_functor_mismatch_is_empty();
    test_inline_compound_constant_child_filters();
    test_inline_compound_duplicate_child_variables_filter();
    test_side_compound_body_field_binding();
    test_side_compound_constant_child_filters();
    test_side_compound_duplicate_child_variables_filter();
    test_side_compound_wrong_functor_handle_no_match();
    test_side_compound_nested_child_no_match();
    test_negated_side_compound_body_pattern_rejected();
    test_unsafe_negation_variable_rejected();
    test_safe_negation_via_projection_accepted();
    test_side_compound_public_allocation_saturates();
    test_insert_sym_variadic();
    test_remove_sym();
    test_snapshot_filter();
    test_print_delta_integer_column();
    test_print_delta_unknown_relation_integer_fallback();
    test_print_delta_abort_on_missed_symbol();
    test_cleanup_order_no_use_after_free();
    test_intern_after_step_succeeds();
    test_delta_cb_multi_round_recursive_insert();
    test_issue_665_partial_conjunction_default_workers();
    test_issue_665_partial_conjunction_multi_worker();
    test_snapshot_rebuilds_idb_after_query_mode_input_changes();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
