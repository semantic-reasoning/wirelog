/*
 * test_easy_head_input_set.c - Issue #2100 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A rule head is a set: a row two rules derive appears once, and a rule
 * reading the head counts it once.  A head that also holds input -- an
 * inline fact or a host row -- used to keep that input beside the derived
 * copy, and every copy of an input row inserted more than once.  A snapshot
 * then emitted the tuple twice and an aggregate over the head counted it
 * twice.
 *
 * Every case reads `r` and the per-row count `n` through
 * wirelog_easy_snapshot() and requires each tuple exactly once.  It runs
 * with plain steps, with snapshots alone, and with delta-callback steps, on
 * one worker and on four.  WIRELOG_TDD_MIN_ROWS_PER_WORKER=1 makes the
 * four-worker runs use four workers on these small inputs; the session would
 * otherwise drop to one below 4096 rows per worker.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "wirelog/wirelog-easy.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value ? value : "");
}

#  define setenv wl_test_setenv_
#endif

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

#define MAX_ROWS 32

typedef struct {
    int64_t col[MAX_ROWS][2];
    uint32_t count;
} rows_t;

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    rows_t *rows = (rows_t *)user_data;

    (void)relation;
    if (rows->count < MAX_ROWS) {
        rows->col[rows->count][0] = ncols > 0 ? row[0] : 0;
        rows->col[rows->count][1] = ncols > 1 ? row[1] : 0;
    }
    rows->count++;
}

static void
ignore_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)diff;
    (void)user_data;
}

typedef enum {
    MODE_PLAIN_STEP,
    MODE_SNAPSHOT_ONLY,
    MODE_CALLBACK_STEP,
} mode_t_;

static const char *const MODE_NAME[] = {
    "plain steps", "snapshots alone", "delta-callback steps"
};

static char message[256];

/* `r` is a rule head; `n` counts each `r` row. */
#define HEAD_DECLS                                    \
        ".decl e(x: int64)\n"                         \
        ".decl r(x: int64)\n"                         \
        ".decl n(k: int64, c: int64)\n"               \
        "r(x) :- e(x).\n"                             \
        "n(x, count(x)) :- r(x).\n"

/* Evaluate as @mode would: a step (the snapshot below then reads the
 * stable model), or nothing (the snapshot below evaluates). */
static const char *
evaluate(wirelog_easy_session_t *s, mode_t_ mode, const char *when)
{
    if (mode == MODE_SNAPSHOT_ONLY)
        return NULL;
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        snprintf(message, sizeof(message), "%s: step failed", when);
        return message;
    }
    return NULL;
}

/* Require @relation to hold exactly @want (one or two columns), each once. */
static const char *
expect(wirelog_easy_session_t *s, const char *relation,
    const int64_t (*want)[2], uint32_t nwant, const char *when)
{
    rows_t rows;

    memset(&rows, 0, sizeof(rows));
    if (wirelog_easy_snapshot(s, relation, collect, &rows) != WIRELOG_OK) {
        snprintf(message, sizeof(message), "%s: snapshot failed", when);
        return message;
    }
    if (rows.count != nwant) {
        snprintf(message, sizeof(message), "%s: %s has %u rows, want %u",
            when, relation, rows.count, nwant);
        return message;
    }
    for (uint32_t w = 0; w < nwant; w++) {
        uint32_t seen = 0;
        for (uint32_t r = 0; r < rows.count && r < MAX_ROWS; r++)
            if (rows.col[r][0] == want[w][0] && rows.col[r][1] == want[w][1])
                seen++;
        if (seen != 1) {
            snprintf(message, sizeof(message),
                "%s: %s(%lld,%lld) seen %u times, want once", when, relation,
                (long long)want[w][0], (long long)want[w][1], seen);
            return message;
        }
    }
    return NULL;
}

static const char *
insert1(wirelog_easy_session_t *s, const char *relation, int64_t a)
{
    return wirelog_easy_insert(s, relation, &a, 1) == WIRELOG_OK
           ? NULL : "insert failed";
}

static const char *
remove1(wirelog_easy_session_t *s, const char *relation, int64_t a)
{
    return wirelog_easy_remove(s, relation, &a, 1) == WIRELOG_OK
           ? NULL : "remove failed";
}

#define TRY(expr)                      \
        do {                           \
            const char *why_ = (expr); \
            if (why_) {                \
                FAIL(why_);            \
                goto done;             \
            }                          \
        } while (0)

static wirelog_easy_session_t *
open_session(const char *program, mode_t_ mode, uint32_t workers)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;

    opts.num_workers = workers;
    if (wirelog_easy_open_opts(program, &opts, &s) != WIRELOG_OK)
        return NULL;
    if (mode == MODE_CALLBACK_STEP
        && wirelog_easy_set_delta_cb(s, ignore_delta, NULL) != WIRELOG_OK) {
        wirelog_easy_close(s);
        return NULL;
    }
    return s;
}

#define BEGIN(name)                                                   \
        tests_run++;                                                  \
        printf("  [%d] %s (%s, %u worker%s)", tests_run, (name),      \
            MODE_NAME[mode], workers, workers == 1 ? "" : "s")

/* An inline fact a rule also derives is one row of the head. */
static void
test_inline_fact_also_derived(mode_t_ mode, uint32_t workers)
{
    static const int64_t r7[][2] = { { 7, 0 } };
    static const int64_t n71[][2] = { { 7, 1 } };
    wirelog_easy_session_t *s;

    BEGIN("an inline fact a rule also derives is one row");
    s = open_session(HEAD_DECLS "r(7).\ne(7).\n", mode, workers);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(evaluate(s, mode, "first evaluation"));
    TRY(expect(s, "r", r7, 1, "first evaluation"));
    TRY(expect(s, "n", n71, 1, "first evaluation"));
    TRY(insert1(s, "e", 8));
    TRY(evaluate(s, mode, "later evaluation"));
    {
        static const int64_t r78[][2] = { { 7, 0 }, { 8, 0 } };
        static const int64_t n78[][2] = { { 7, 1 }, { 8, 1 } };
        TRY(expect(s, "r", r78, 2, "later evaluation"));
        TRY(expect(s, "n", n78, 2, "later evaluation"));
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    wirelog_easy_close(s);
}

/* An inline fact written twice is one row until both copies are removed. */
static void
test_inline_fact_written_twice(mode_t_ mode, uint32_t workers)
{
    static const int64_t r7[][2] = { { 7, 0 } };
    static const int64_t n71[][2] = { { 7, 1 } };
    wirelog_easy_session_t *s;

    BEGIN("an inline fact written twice is one row");
    s = open_session(HEAD_DECLS "r(7).\nr(7).\n", mode, workers);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(evaluate(s, mode, "first evaluation"));
    TRY(expect(s, "r", r7, 1, "first evaluation"));
    TRY(expect(s, "n", n71, 1, "first evaluation"));
    TRY(remove1(s, "r", 7));
    TRY(evaluate(s, mode, "one copy removed"));
    TRY(expect(s, "r", r7, 1, "one copy removed"));
    TRY(expect(s, "n", n71, 1, "one copy removed"));
    TRY(remove1(s, "r", 7));
    TRY(evaluate(s, mode, "both copies removed"));
    TRY(expect(s, "r", NULL, 0, "both copies removed"));
    TRY(expect(s, "n", NULL, 0, "both copies removed"));
    tests_passed++;
    printf(" ... PASS\n");
done:
    wirelog_easy_close(s);
}

/* Host rows into the head, inserted twice or duplicating a derived row, are
 * each one row of the head. */
static void
test_host_rows(mode_t_ mode, uint32_t workers)
{
    static const int64_t r1[][2] = { { 1, 0 } };
    static const int64_t n1[][2] = { { 1, 1 } };
    static const int64_t r17[][2] = { { 1, 0 }, { 7, 0 } };
    static const int64_t n17[][2] = { { 1, 1 }, { 7, 1 } };
    wirelog_easy_session_t *s;

    BEGIN("host rows into a rule head are each one row");
    s = open_session(HEAD_DECLS, mode, workers);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "e", 1));
    TRY(evaluate(s, mode, "load"));
    TRY(expect(s, "r", r1, 1, "load"));
    TRY(insert1(s, "r", 7));
    TRY(insert1(s, "r", 7));
    TRY(evaluate(s, mode, "host row twice"));
    TRY(expect(s, "r", r17, 2, "host row twice"));
    TRY(expect(s, "n", n17, 2, "host row twice"));
    TRY(insert1(s, "r", 1));
    TRY(evaluate(s, mode, "host row a rule derives"));
    TRY(expect(s, "r", r17, 2, "host row a rule derives"));
    TRY(expect(s, "n", n17, 2, "host row a rule derives"));
    TRY(remove1(s, "r", 7));
    TRY(evaluate(s, mode, "one of two copies removed"));
    TRY(expect(s, "r", r17, 2, "one of two copies removed"));
    TRY(remove1(s, "r", 7));
    TRY(evaluate(s, mode, "both copies removed"));
    TRY(expect(s, "r", r1, 1, "both copies removed"));
    TRY(expect(s, "n", n1, 1, "both copies removed"));
    tests_passed++;
    printf(" ... PASS\n");
done:
    wirelog_easy_close(s);
}

/* A recursive head holding a duplicated inline fact and a host row it also
 * derives is a set too. */
static void
test_recursive_head(mode_t_ mode, uint32_t workers)
{
    static const int64_t r12[][2] = { { 1, 0 }, { 2, 0 } };
    static const int64_t n12[][2] = { { 1, 1 }, { 2, 1 } };
    wirelog_easy_session_t *s;
    int64_t edge[2] = { 1, 2 };

    BEGIN("a recursive head holding input is a set");
    s = open_session(".decl edge(a: int64, b: int64)\n"
            ".decl r(x: int64)\n"
            ".decl n(k: int64, c: int64)\n"
            "r(1).\nr(1).\n"
            "r(y) :- r(x), edge(x, y).\n"
            "n(x, count(x)) :- r(x).\n", mode, workers);
    if (!s) {
        FAIL("open");
        return;
    }
    if (wirelog_easy_insert(s, "edge", edge, 2) != WIRELOG_OK) {
        FAIL("insert edge");
        goto done;
    }
    TRY(evaluate(s, mode, "first evaluation"));
    TRY(expect(s, "r", r12, 2, "first evaluation"));
    TRY(expect(s, "n", n12, 2, "first evaluation"));
    TRY(insert1(s, "r", 2));
    TRY(evaluate(s, mode, "host row it derives"));
    TRY(expect(s, "r", r12, 2, "host row it derives"));
    TRY(expect(s, "n", n12, 2, "host row it derives"));
    tests_passed++;
    printf(" ... PASS\n");
done:
    wirelog_easy_close(s);
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 4 };

    printf("test_easy_head_input_set (Issue #2100)\n");
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        for (int m = MODE_PLAIN_STEP; m <= MODE_CALLBACK_STEP; m++) {
            test_inline_fact_also_derived((mode_t_)m, workers[w]);
            test_inline_fact_written_twice((mode_t_)m, workers[w]);
            test_host_rows((mode_t_)m, workers[w]);
            /* A recursive head seeded by an inline fact does not evaluate
             * on more than one worker yet, before and after this fix. */
            if (workers[w] == 1)
                test_recursive_head((mode_t_)m, workers[w]);
        }
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
