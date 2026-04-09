/*
 * test_wl_easy.c - Unit tests for the wl_easy convenience facade (Issue #441)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/wl_easy.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
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

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_open_close_null_safe(void)
{
    TEST("open NULL src + NULL out + close(NULL) safe");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(NULL, &s) == WIRELOG_OK) {
        FAIL("expected non-OK on NULL src");
        return;
    }
    if (wl_easy_open(ACCESS_CONTROL_SRC, NULL) == WIRELOG_OK) {
        FAIL("expected non-OK on NULL out");
        return;
    }
    /* Must not crash */
    wl_easy_close(NULL);
    PASS();
}

static void
test_open_parse_error(void)
{
    TEST("open invalid Datalog returns error");

    wl_easy_session_t *s = (wl_easy_session_t *)0xdeadbeef;
    wirelog_error_t rc = wl_easy_open("this is not datalog ::: !!!", &s);
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

static void
test_intern_returns_same_id(void)
{
    TEST("intern same string returns same id");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    int64_t a = wl_easy_intern(s, "alice");
    int64_t b = wl_easy_intern(s, "alice");
    if (a < 0 || b < 0 || a != b) {
        FAIL("intern returned inconsistent ids");
        wl_easy_close(s);
        return;
    }
    wl_easy_close(s);
    PASS();
}

static void
test_insert_step_delta(void)
{
    TEST("insert + step fires delta callback");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    int64_t alice = wl_easy_intern(s, "alice");
    int64_t read = wl_easy_intern(s, "read");
    if (alice < 0 || read < 0) {
        FAIL("intern failed");
        wl_easy_close(s);
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_easy_set_delta_cb(s, collect_delta, &deltas);

    int64_t row[2] = { alice, read };
    if (wl_easy_insert(s, "can", row, 2) != WIRELOG_OK) {
        FAIL("insert failed");
        wl_easy_close(s);
        return;
    }
    if (wl_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wl_easy_close(s);
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
    wl_easy_close(s);
    if (!found) {
        FAIL("expected +granted(alice,read) delta not seen");
        return;
    }
    PASS();
}

static void
test_insert_sym_variadic(void)
{
    TEST("insert_sym variadic helper");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_easy_set_delta_cb(s, collect_delta, &deltas);

    if (wl_easy_insert_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("insert_sym failed");
        wl_easy_close(s);
        return;
    }
    if (wl_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wl_easy_close(s);
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
    wl_easy_close(s);
    if (!found) {
        FAIL("no granted delta after insert_sym");
        return;
    }
    PASS();
}

static void
test_remove_sym(void)
{
    TEST("remove_sym fires negative delta");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_easy_set_delta_cb(s, collect_delta, &deltas);

    if (wl_easy_insert_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("insert_sym failed");
        wl_easy_close(s);
        return;
    }
    if (wl_easy_step(s) != WIRELOG_OK) {
        FAIL("step 1 failed");
        wl_easy_close(s);
        return;
    }
    int after_step1 = deltas.count;

    if (wl_easy_remove_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("remove_sym failed");
        wl_easy_close(s);
        return;
    }
    if (wl_easy_step(s) != WIRELOG_OK) {
        FAIL("step 2 failed");
        wl_easy_close(s);
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
    wl_easy_close(s);
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

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }

    if (wl_easy_insert_sym(s, "can", "alice", "read", (const char *)NULL)
        != WIRELOG_OK
        || wl_easy_insert_sym(s, "can", "bob", "write", (const char *)NULL)
        != WIRELOG_OK) {
        FAIL("insert_sym failed");
        wl_easy_close(s);
        return;
    }
    /* NOTE: Do NOT call wl_easy_step() before wl_easy_snapshot().  The
     * columnar backend's snapshot path re-evaluates all strata and appends
     * to the IDB relation rows; a prior step() already derived the IDB
     * tuples, so combining the two would double-count.  See the doc
     * comment on wl_easy_snapshot() in wl_easy.h. */

    tuple_collector_t granted_t;
    memset(&granted_t, 0, sizeof(granted_t));
    if (wl_easy_snapshot(s, "granted", collect_tuple, &granted_t)
        != WIRELOG_OK) {
        FAIL("snapshot granted failed");
        wl_easy_close(s);
        return;
    }

    tuple_collector_t can_t;
    memset(&can_t, 0, sizeof(can_t));
    if (wl_easy_snapshot(s, "can", collect_tuple, &can_t) != WIRELOG_OK) {
        FAIL("snapshot can failed");
        wl_easy_close(s);
        return;
    }

    wl_easy_close(s);

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

static void
test_print_delta_integer_column(void)
{
    TEST("print_delta on integer column does not abort");

    static const char *SRC = ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n";
    wl_easy_session_t *s = NULL;
    if (wl_easy_open(SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    wl_easy_set_delta_cb(s, wl_easy_print_delta, s);
    int64_t row[1] = { 42 };
    if (wl_easy_insert(s, "a", row, 1) != WIRELOG_OK) {
        FAIL("insert failed");
        wl_easy_close(s);
        return;
    }
    if (wl_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wl_easy_close(s);
        return;
    }
    wl_easy_close(s);
    PASS();
}

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

        wl_easy_session_t *s = NULL;
        if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s)
            _exit(2);
        wl_easy_set_delta_cb(s, wl_easy_print_delta, s);
        /* Bogus, never-interned ids — printer must abort. */
        int64_t row[2] = { 999999, 888888 };
        wl_easy_insert(s, "can", row, 2);
        wl_easy_step(s);
        wl_easy_close(s);
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
        wl_easy_session_t *s = NULL;
        if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
            FAIL("open failed");
            return;
        }
        if (wl_easy_insert_sym(s, "can", "alice", "read", (const char *)NULL)
            != WIRELOG_OK) {
            FAIL("insert_sym failed");
            wl_easy_close(s);
            return;
        }
        if (wl_easy_step(s) != WIRELOG_OK) {
            FAIL("step failed");
            wl_easy_close(s);
            return;
        }
        wl_easy_close(s);
    }
    PASS();
}

static void
test_intern_after_step_succeeds(void)
{
    TEST("intern after first step still succeeds (Option B contract)");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK || !s) {
        FAIL("open failed");
        return;
    }
    int64_t alice = wl_easy_intern(s, "alice");
    int64_t read = wl_easy_intern(s, "read");
    if (alice < 0 || read < 0) {
        FAIL("intern failed");
        wl_easy_close(s);
        return;
    }
    int64_t row[2] = { alice, read };
    if (wl_easy_insert(s, "can", row, 2) != WIRELOG_OK) {
        FAIL("insert failed");
        wl_easy_close(s);
        return;
    }
    if (wl_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        wl_easy_close(s);
        return;
    }
    /* After the plan has been built and stepped, interning a brand new
     * symbol must still succeed and return a fresh id, because the intern
     * table is aliased through the whole session lifetime. */
    int64_t late = wl_easy_intern(s, "late_symbol");
    /* And a new insert using that id must also succeed, proving the id is
     * actually visible to the running backend. */
    int64_t late_row[2] = { late, read };
    wirelog_error_t ins_rc
        = wl_easy_insert(s, "can", late_row, 2);
    wirelog_error_t step_rc = wl_easy_step(s);
    wl_easy_close(s);
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

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("wl_easy Tests (Issue #441)\n");
    printf("==========================\n\n");

    test_open_close_null_safe();
    test_open_parse_error();
    test_intern_returns_same_id();
    test_insert_step_delta();
    test_insert_sym_variadic();
    test_remove_sym();
    test_snapshot_filter();
    test_print_delta_integer_column();
    test_print_delta_abort_on_missed_symbol();
    test_cleanup_order_no_use_after_free();
    test_intern_after_step_succeeds();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
