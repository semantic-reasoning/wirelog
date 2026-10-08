/*
 * test_easy_callback_cleared.c - Issue #2108 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * With a delta callback installed, inserts and removals are staged for an
 * incremental evaluation.  A snapshot taken with the callback installed
 * re-evaluates in full, because its incremental route does not promise the
 * complete model.  Clearing the callback before that snapshot sent it down
 * the incremental route anyway: it ignored a staged removal, so `r(8)`
 * survived it, and the strata below the inserted relation emitted their
 * earlier rows again, so `n(7,1)` appeared twice.
 *
 * Each case stages changes with a callback installed, clears it, and
 * requires the same model from the next snapshots as a session that never
 * had a callback, after a load read by a snapshot or by a step, in a
 * session opened with one worker and with four.  Only the load can use
 * the four workers: the full re-evaluation after the callback is cleared
 * runs serially.
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

static char message[256];

/* Snapshot @relation and require exactly @want, each row once. */
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

#define TRY(expr)                      \
        do {                           \
            const char *why_ = (expr); \
            if (why_) {                \
                FAIL(why_);            \
                goto done;             \
            }                          \
        } while (0)

/* `r` copies `e`, `n` counts each `r` row and `t` sums `e`. */
static const char *const PROGRAM =
    ".decl e(x: int64)\n"
    ".decl r(x: int64)\n"
    ".decl n(k: int64, c: int64)\n"
    ".decl t(s: int64)\n"
    "r(x) :- e(x).\n"
    "n(x, count(x)) :- r(x).\n"
    "t(sum(x)) :- e(x).\n";

typedef enum {
    STAGE_REMOVE_THEN_INSERT,
    STAGE_INSERT_THEN_REMOVE,
    STAGE_INSERT,
    STAGE_REMOVE,
} stage_t;

static const char *const STAGE_NAME[] = {
    "removal then insert", "insert then removal", "insert", "removal"
};

/* The model each staging leaves, from a load of e(7) and e(8). */
typedef struct {
    int64_t r[3][2];
    uint32_t nr;
    int64_t n[3][2];
    int64_t t[1][2];
} model_t;

static const model_t MODEL[] = {
    { { { 7, 0 }, { 9, 0 } }, 2, { { 7, 1 }, { 9, 1 } }, { { 16, 0 } } },
    { { { 7, 0 }, { 9, 0 } }, 2, { { 7, 1 }, { 9, 1 } }, { { 16, 0 } } },
    { { { 7, 0 }, { 8, 0 }, { 9, 0 } }, 3,
      { { 7, 1 }, { 8, 1 }, { 9, 1 } }, { { 24, 0 } } },
    { { { 7, 0 } }, 1, { { 7, 1 } }, { { 7, 0 } } },
};

static const char *
change(wirelog_easy_session_t *s, bool insert, int64_t a)
{
    int rc = insert ? wirelog_easy_insert(s, "e", &a, 1)
                    : wirelog_easy_remove(s, "e", &a, 1);
    return rc == WIRELOG_OK ? NULL : insert ? "insert failed" : "remove failed";
}

static const char *
stage(wirelog_easy_session_t *s, stage_t st)
{
    const char *why = NULL;

    switch (st) {
    case STAGE_REMOVE_THEN_INSERT:
        if (!(why = change(s, false, 8)))
            why = change(s, true, 9);
        break;
    case STAGE_INSERT_THEN_REMOVE:
        if (!(why = change(s, true, 9)))
            why = change(s, false, 8);
        break;
    case STAGE_INSERT:
        why = change(s, true, 9);
        break;
    case STAGE_REMOVE:
        why = change(s, false, 8);
        break;
    }
    return why;
}

static const char *
expect_model(wirelog_easy_session_t *s, const model_t *m, const char *when)
{
    const char *why;

    if ((why = expect(s, "r", m->r, m->nr, when)) != NULL)
        return why;
    if ((why = expect(s, "n", m->n, m->nr, when)) != NULL)
        return why;
    return expect(s, "t", m->t, 1, when);
}

/* Changes staged with a callback installed and read by snapshots after the
 * callback is cleared give the model a session without one gives. */
static void
test_staged_then_cleared(stage_t st, bool load_by_step, uint32_t workers)
{
    static const int64_t r78[][2] = { { 7, 0 }, { 8, 0 } };
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;

    tests_run++;
    printf("  [%d] %s staged, callback cleared (load by %s, %u worker%s)",
        tests_run, STAGE_NAME[st], load_by_step ? "step" : "snapshot",
        workers, workers == 1 ? "" : "s");
    opts.num_workers = workers;
    if (wirelog_easy_open_opts(PROGRAM, &opts, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    if (wirelog_easy_set_delta_cb(s, ignore_delta, NULL) != WIRELOG_OK) {
        FAIL("set_delta_cb");
        goto done;
    }
    TRY(change(s, true, 7));
    TRY(change(s, true, 8));
    if (load_by_step && wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("load step failed");
        goto done;
    }
    TRY(expect(s, "r", r78, 2, "load"));
    TRY(stage(s, st));
    if (wirelog_easy_set_delta_cb(s, NULL, NULL) != WIRELOG_OK) {
        FAIL("clear delta_cb");
        goto done;
    }
    TRY(expect_model(s, &MODEL[st], "first snapshot"));
    TRY(expect_model(s, &MODEL[st], "second snapshot"));
    /* A plain step after those snapshots keeps the model. */
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        FAIL("step failed");
        goto done;
    }
    TRY(expect_model(s, &MODEL[st], "after a step"));
    tests_passed++;
    printf(" ... PASS\n");
done:
    wirelog_easy_close(s);
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 4 };

    printf("test_easy_callback_cleared (Issue #2108)\n");
    /* Let the four-worker loads use four workers on these small inputs. */
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++)
        for (int st = STAGE_REMOVE_THEN_INSERT; st <= STAGE_REMOVE; st++)
            for (int by_step = 0; by_step <= 1; by_step++)
                test_staged_then_cleared((stage_t)st, by_step != 0,
                    workers[w]);
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
