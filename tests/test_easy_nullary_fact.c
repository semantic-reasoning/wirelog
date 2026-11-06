/*
 * test_easy_nullary_fact.c - Issue #2231 regression.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * An inline fact of a zero-column relation (`.decl flag()` with `flag().`)
 * was accepted by the parser but never seeded: the IR fact buffer is sized
 * by fact count times column count, so it was never allocated, and fact
 * loading skips a relation whose buffer is NULL.  A rule reading `flag()`
 * derived nothing and `!flag()` held.
 *
 * Every case opens a program through the easy API, steps it and snapshots
 * the result on one, two and four workers.  A non-recursive stratum that
 * reads a zero-column relation runs on the coordinator whatever the worker
 * count (#2134), so the first group checks the seeded result, not worker
 * evaluation.
 *
 * The second group reads the fact from recursive strata, which do run on
 * the workers: WIRELOG_TDD_MIN_ROWS_PER_WORKER=1 makes them use two or
 * more on these small inputs, and each case checks the strategy the step
 * chose.  The hybrid worker setup gave each worker an empty copy of a
 * zero-column relation, so there `flag()` read as empty and `!flag()` as
 * true on every worker.  That loss also hit a zero-column row inserted by
 * the host, which one case covers.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "wirelog/wirelog-easy.h"
#include "wirelog/columnar/internal.h"

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

wl_session_t *wirelog_easy_testhook_inner(wirelog_easy_session_t *s);

#define MAX_ROWS 8

typedef struct {
    uint32_t count;
    int64_t values[MAX_ROWS];
    uint32_t bad_width;
} rows_t;

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    rows_t *r = (rows_t *)user_data;

    (void)relation;
    if (ncols != 1) {
        r->bad_width++;
        return;
    }
    if (r->count < MAX_ROWS)
        r->values[r->count] = row[0];
    r->count++;
}

static int
cmp_i64(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

/* Open @src on @workers workers, step once and snapshot @relation into
 * @out with its values sorted.  Returns NULL or the step that failed. */
static const char *
run(const char *src, uint32_t workers, const char *relation, rows_t *out)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    const char *why = NULL;

    memset(out, 0, sizeof(*out));
    opts.num_workers = workers;
    if (wirelog_easy_open_opts(src, &opts, &s) != WIRELOG_OK)
        why = "open";
    else if (wirelog_easy_step(s) != WIRELOG_OK)
        why = "step";
    else if (wirelog_easy_snapshot(s, relation, collect, out) != WIRELOG_OK)
        why = "snapshot";
    else if (out->bad_width || out->count > MAX_ROWS)
        why = "row of the wrong width or too many rows";
    if (s)
        wirelog_easy_close(s);
    if (!why)
        qsort(out->values, out->count, sizeof(out->values[0]), cmp_i64);
    return why;
}

static void
check(const char *name, const char *src, uint32_t workers,
    const char *relation, const int64_t *expected, uint32_t nexpected)
{
    rows_t got;
    const char *why;

    tests_run++;
    printf("  [%d] %s (%u worker%s)", tests_run, name, workers,
        workers == 1 ? "" : "s");
    why = run(src, workers, relation, &got);
    if (!why && got.count != nexpected)
        why = "wrong number of rows";
    for (uint32_t i = 0; !why && i < nexpected; i++)
        if (got.values[i] != expected[i])
            why = "wrong row";
    if (why) {
        tests_failed++;
        printf(" ... FAIL: %s (got %u rows)\n", why, got.count);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

typedef struct {
    uint32_t zero_width;
    uint32_t other;
} nullary_rows_t;

static void
count_nullary(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    nullary_rows_t *r = (nullary_rows_t *)user_data;

    (void)relation;
    (void)row;
    if (ncols == 0)
        r->zero_width++;
    else
        r->other++;
}

/* A rule for a zero-column head whose body filters must still derive a
 * zero-width row.  The fusion pass turns the head's projection into a
 * FLATMAP, whose lowering emitted no projection for zero columns, so the
 * head got the body's row instead. */
static void
check_derived_width(const char *name, const char *src, uint32_t workers)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    nullary_rows_t got = { 0 };
    const char *why = NULL;

    tests_run++;
    printf("  [%d] %s (%u worker%s)", tests_run, name, workers,
        workers == 1 ? "" : "s");
    opts.num_workers = workers;
    if (wirelog_easy_open_opts(src, &opts, &s) != WIRELOG_OK)
        why = "open";
    else if (wirelog_easy_step(s) != WIRELOG_OK)
        why = "step";
    else if (wirelog_easy_snapshot(s, "ok", count_nullary, &got)
        != WIRELOG_OK)
        why = "snapshot";
    else if (got.zero_width != 1 || got.other != 0)
        why = "head is not one zero-width row";
    if (s)
        wirelog_easy_close(s);
    if (why) {
        tests_failed++;
        printf(" ... FAIL: %s (%u zero-width, %u other)\n", why,
            got.zero_width, got.other);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

typedef struct {
    uint32_t count;
    uint32_t bad_width;
} pairs_t;

static void
count_pairs(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    pairs_t *r = (pairs_t *)user_data;

    (void)relation;
    (void)row;
    if (ncols != 2)
        r->bad_width++;
    else
        r->count++;
}

#define EDGES \
        ".decl e(x: int64, y: int64)\n.decl t(x: int64, y: int64)\n" \
        "e(1, 2).\ne(2, 3).\ne(3, 4).\n"

/* Evaluate a recursive stratum over e(1,2), e(2,3), e(3,4) and require
 * @nexpected rows of t.  With @host the zero-column row comes from
 * wirelog_easy_insert rather than an inline fact.  At two or more workers
 * the step must have run @strategy on at least two of them. */
static void
check_recursive(const char *name, const char *src, bool host,
    uint32_t workers, const char *strategy, uint32_t nexpected)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    const char *why = NULL;
    pairs_t got = { 0 };

    tests_run++;
    printf("  [%d] %s (%u worker%s)", tests_run, name, workers,
        workers == 1 ? "" : "s");
    opts.num_workers = workers;
    opts.eager_build = true;
    if (wirelog_easy_open_opts(src, &opts, &s) != WIRELOG_OK) {
        why = "open";
        goto done;
    }
    if (host) {
        int64_t unused = 0;
        if (wirelog_easy_insert(s, "flag", &unused, 0) != WIRELOG_OK) {
            why = "insert";
            goto done;
        }
    }
    wl_col_session_t *inner = COL_SESSION(wirelog_easy_testhook_inner(s));
    inner->tdd_audit.enabled = true;
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        why = "step";
        goto done;
    }
    if (workers > 1
        && (!inner->tdd_audit.strategy
        || strcmp(inner->tdd_audit.strategy, strategy) != 0
        || inner->tdd_audit.selected_workers < 2)) {
        printf(" [ran %s on %u]",
            inner->tdd_audit.strategy ? inner->tdd_audit.strategy : "none",
            inner->tdd_audit.selected_workers);
        why = "strategy or worker count";
        goto done;
    }
    if (wirelog_easy_snapshot(s, "t", count_pairs, &got) != WIRELOG_OK) {
        why = "snapshot";
        goto done;
    }
    if (got.bad_width || got.count != nexpected)
        why = "wrong number of rows";
done:
    if (s)
        wirelog_easy_close(s);
    if (why) {
        tests_failed++;
        printf(" ... FAIL: %s (got %u rows)\n", why, got.count);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 2, 4 };
    static const int64_t only_100[] = { 100 };
    static const int64_t mixed[] = { 1, 2, 3, 100 };

    printf("test_easy_nullary_fact (Issue #2231)\n");
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        uint32_t n = workers[w];

        check("nullary fact derives its rule",
            ".decl flag()\n.decl p(x: int64)\n"
            "flag().\np(100) :- flag().\n",
            n, "p", only_100, 1);
        check("nullary fact beside unary facts",
            ".decl flag()\n.decl e(x: int64)\n.decl p(x: int64)\n"
            "flag().\ne(1).\ne(2).\ne(3).\n"
            "p(x) :- e(x).\np(100) :- flag().\n",
            n, "p", mixed, 4);
        check("repeated nullary fact",
            ".decl flag()\n.decl p(x: int64)\n"
            "flag().\nflag().\np(100) :- flag().\n",
            n, "p", only_100, 1);
        check("nullary relation with a fact and a rule",
            ".decl flag()\n.decl e(x: int64)\n.decl p(x: int64)\n"
            "flag().\nflag() :- e(1).\np(100) :- flag().\n",
            n, "p", only_100, 1);
        check("nullary fact and a rule that fires on a filtered body",
            ".decl flag()\n.decl e(x: int64)\n.decl p(x: int64)\n"
            "flag().\ne(1).\nflag() :- e(1).\np(100) :- flag().\n",
            n, "p", only_100, 1);
        check("undeclared nullary fact",
            ".decl p(x: int64)\n"
            "flag().\np(100) :- flag().\n",
            n, "p", only_100, 1);
        check_derived_width("derived nullary head, constant in the body",
            ".decl ok()\n.decl e(x: int64)\ne(1).\ne(2).\n"
            "ok() :- e(1).\n", n);
        check_derived_width("derived nullary head, comparison in the body",
            ".decl ok()\n.decl e(x: int64)\ne(1).\ne(2).\n"
            "ok() :- e(x), x > 0.\n", n);
        check("negated nullary fact",
            ".decl flag()\n.decl e(x: int64)\n.decl q(x: int64)\n"
            "flag().\ne(1).\nq(x) :- e(x), !flag().\n",
            n, "q", NULL, 0);
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        uint32_t n = workers[w];

        check_recursive("closure, flag() in both rules",
            ".decl flag()\nflag().\n" EDGES
            "t(x, y) :- e(x, y), flag().\n"
            "t(x, z) :- t(x, y), e(y, z), flag().\n",
            false, n, "aligned", 6);
        check_recursive("closure, flag() in the base rule",
            ".decl flag()\nflag().\n" EDGES
            "t(x, y) :- e(x, y), flag().\n"
            "t(x, z) :- t(x, y), e(y, z).\n",
            false, n, "owner", 6);
        check_recursive("closure, flag() in the recursive rule",
            ".decl flag()\nflag().\n" EDGES
            "t(x, y) :- e(x, y).\n"
            "t(x, z) :- t(x, y), e(y, z), flag().\n",
            false, n, "aligned", 6);
        check_recursive("closure, !flag() in the recursive rule",
            ".decl flag()\nflag().\n" EDGES
            "t(x, y) :- e(x, y).\n"
            "t(x, z) :- t(x, y), e(y, z), !flag().\n",
            false, n, "owner", 3);
        check_recursive("self-join closure with flag()",
            ".decl flag()\nflag().\n" EDGES
            "t(x, y) :- e(x, y), flag().\n"
            "t(x, z) :- t(x, y), t(y, z), flag().\n",
            false, n, "bdx", 6);
        check_recursive("two-sided closure with flag()",
            ".decl flag()\nflag().\n" EDGES
            "t(x, y) :- e(x, y), flag().\n"
            "t(x, z) :- t(x, y), e(y, z), flag().\n"
            "t(x, z) :- e(x, y), t(y, z), flag().\n",
            false, n, "global_read", 6);
        check_recursive("closure, host-inserted flag()",
            ".decl flag()\n" EDGES
            "t(x, y) :- e(x, y), flag().\n"
            "t(x, z) :- t(x, y), e(y, z), flag().\n",
            true, n, "aligned", 6);
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
