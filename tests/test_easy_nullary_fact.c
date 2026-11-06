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

static unsigned owner_rows;

static void
count_owner_row(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
    owner_rows++;
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

/* Rows of @rel in a snapshot, or -1 when the snapshot fails. */
static int
owner_count(wirelog_easy_session_t *s, const char *rel)
{
    owner_rows = 0;
    return wirelog_easy_snapshot(s, rel, count_owner_row, NULL) != WIRELOG_OK
           ? -1 : (int)owner_rows;
}

/* `flag()` enters a recursive cycle through a rule with no join, which
 * the owner strategy accepts: t(1, 2) holds while flag() does, t closes
 * over the chain e(2, 3) .. e(@len, @len + 1) into @len rows, and flag()
 * is derived back once t(1, 4) holds.  Owner mode builds a dedup set for
 * every relation of the stratum, and the builder refused `flag` while it
 * held a row, so the step failed before any worker evaluated the stratum.
 * A chain longer than the owner strategy's iteration floor falls back
 * through the owner_tiny_frontier replay, which restores the
 * coordinator's saved relations; @replay names it when the step must take
 * it.  A second step after one more edge must extend t by a row and still
 * find one `flag()`.  With @host the row comes from wirelog_easy_insert
 * rather than an inline fact. */
static void
check_owner(const char *name, bool host, uint32_t len, uint32_t workers,
    bool callback, const char *replay)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    const char *why = NULL;
    char src[4096];
    char ran[96] = "";
    int n;
    int t = 0;

    tests_run++;
    printf("  [%d] %s (%u worker%s%s)", tests_run, name, workers,
        workers == 1 ? "" : "s", callback ? ", callback" : "");
    n = snprintf(src, sizeof(src),
            ".decl e(x: int64, y: int64)\n.decl t(x: int64, y: int64)\n"
            ".decl fp(x: int64)\n.decl flag()\n%s"
            "fp(100) :- flag().\nt(1, 2) :- flag().\n"
            "t(x, z) :- t(x, y), e(y, z).\nflag() :- t(1, 4).\n",
            host ? "" : "flag().\n");
    for (uint32_t i = 2; i <= len && n > 0 && (size_t)n < sizeof(src); i++)
        n += snprintf(src + n, sizeof(src) - (size_t)n, "e(%u, %u).\n", i,
                i + 1);
    if (n <= 0 || (size_t)n >= sizeof(src)) {
        why = "source too long";
        goto done;
    }
    opts.num_workers = workers;
    opts.eager_build = true;
    if (wirelog_easy_open_opts(src, &opts, &s) != WIRELOG_OK) {
        why = "open";
        goto done;
    }
    if (callback && wirelog_easy_set_delta_cb(s, ignore_delta, NULL)
        != WIRELOG_OK) {
        why = "set_delta_cb";
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
    snprintf(ran, sizeof(ran), " [ran %s on %u, replay %s]",
        inner->tdd_audit.strategy ? inner->tdd_audit.strategy : "none",
        inner->tdd_audit.selected_workers,
        inner->tdd_audit.replay ? inner->tdd_audit.replay : "none");
    if (workers > 1 && !callback
        && (!inner->tdd_audit.strategy
        || strcmp(inner->tdd_audit.strategy, "owner") != 0
        || inner->tdd_audit.selected_workers < 2
        || (replay && (!inner->tdd_audit.replay
        || strcmp(inner->tdd_audit.replay, replay) != 0)))) {
        why = "strategy, worker count or replay";
        goto done;
    }
    t = owner_count(s, "t");
    if (t != (int)len || owner_count(s, "flag") != 1
        || owner_count(s, "fp") != 1) {
        why = "wrong rows";
        goto done;
    }
    /* The stratum evaluates again over one more edge after the first
     * step, and after its fallback and restore when the step took one. */
    int64_t edge[2] = { (int64_t)len + 1, (int64_t)len + 2 };
    if (wirelog_easy_insert(s, "e", edge, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        why = "second step";
        goto done;
    }
    t = owner_count(s, "t");
    if (t != (int)len + 1 || owner_count(s, "flag") != 1
        || owner_count(s, "fp") != 1)
        why = "wrong rows after a second step";
done:
    if (s)
        wirelog_easy_close(s);
    if (why) {
        tests_failed++;
        printf("%s ... FAIL: %s (t has %d rows)\n", ran, why, t);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

/* One step of a cycle script: CY_EDGE inserts e(@a, @b), CY_FLAG inserts
 * and CY_UNFLAG removes the zero-width row of `flag`, and CY_STEP steps
 * and expects @t rows of t and @fp of fp.  Without a callback at two or
 * more workers a step with @strategy must have run it on at least two
 * workers. */
enum { CY_END, CY_EDGE, CY_FLAG, CY_UNFLAG, CY_STEP };

typedef struct {
    int op;
    int64_t a;
    int64_t b;
    int t;
    int fp;
    const char *strategy;
} cycle_op_t;

#define CY_E(a, b)         { CY_EDGE, a, b, 0, 0, NULL }
#define CY_F               { CY_FLAG, 0, 0, 0, 0, NULL }
#define CY_U               { CY_UNFLAG, 0, 0, 0, 0, NULL }
#define CY_S(t, fp, strat) { CY_STEP, 0, 0, t, fp, strat }
#define CY_X               { CY_END, 0, 0, 0, 0, NULL }

/* `flag()` both heads a rule of a recursive stratum and is read by it.
 * Holding a row, it matched neither worker branch for the stratum's
 * relations and was handed to every worker as a read-only view; the
 * first time a worker derived `flag()` again, the view refused the write
 * and the step returned WIRELOG_ERR_EXEC. */
static void
check_cycle(const char *name, const char *src, const cycle_op_t *ops,
    uint32_t workers, bool callback)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    wl_col_session_t *inner = NULL;
    char why[160] = "";
    int64_t unused = 0;

    tests_run++;
    printf("  [%d] %s (%u worker%s%s)", tests_run, name, workers,
        workers == 1 ? "" : "s", callback ? ", callback" : "");
    opts.num_workers = workers;
    opts.eager_build = true;
    if (wirelog_easy_open_opts(src, &opts, &s) != WIRELOG_OK) {
        snprintf(why, sizeof(why), "open");
        goto done;
    }
    if (callback && wirelog_easy_set_delta_cb(s, ignore_delta, NULL)
        != WIRELOG_OK) {
        snprintf(why, sizeof(why), "set_delta_cb");
        goto done;
    }
    inner = COL_SESSION(wirelog_easy_testhook_inner(s));
    for (uint32_t i = 0; ops[i].op != CY_END && !why[0]; i++) {
        const cycle_op_t *o = &ops[i];
        int64_t edge[2] = { o->a, o->b };
        int rc = WIRELOG_OK;
        int t;
        int fp;

        switch (o->op) {
        case CY_EDGE:
            rc = wirelog_easy_insert(s, "e", edge, 2);
            break;
        case CY_FLAG:
            rc = wirelog_easy_insert(s, "flag", &unused, 0);
            break;
        case CY_UNFLAG:
            rc = wirelog_easy_remove(s, "flag", &unused, 0);
            break;
        default:
            inner->tdd_audit.enabled = true;
            inner->tdd_audit.strategy = NULL;
            inner->tdd_audit.selected_workers = 0;
            rc = wirelog_easy_step(s);
            if (rc != WIRELOG_OK)
                break;
            if (o->strategy && workers > 1 && !callback
                && (!inner->tdd_audit.strategy
                || strcmp(inner->tdd_audit.strategy, o->strategy) != 0
                || inner->tdd_audit.selected_workers < 2)) {
                snprintf(why, sizeof(why), "op %u: ran %s on %u, want %s",
                    i, inner->tdd_audit.strategy
                    ? inner->tdd_audit.strategy : "none",
                    inner->tdd_audit.selected_workers, o->strategy);
                break;
            }
            t = owner_count(s, "t");
            fp = owner_count(s, "fp");
            if (t != o->t || fp != o->fp)
                snprintf(why, sizeof(why),
                    "op %u: t has %d rows and fp %d, want %d and %d", i, t,
                    fp, o->t, o->fp);
            break;
        }
        if (rc != WIRELOG_OK && !why[0])
            snprintf(why, sizeof(why), "op %u: rc %d", i, rc);
    }
done:
    if (s)
        wirelog_easy_close(s);
    if (why[0]) {
        tests_failed++;
        printf(" ... FAIL: %s\n", why);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

#define CYCLE_HEAD ".decl flag()\n.decl fp(x: int64)\n" EDGES \
        "fp(100) :- flag().\n"

static const cycle_op_t cycle_once[] = { CY_S(6, 1, "aligned"), CY_X };

static const cycle_op_t cycle_grows[] = {
    CY_S(6, 1, "aligned"), CY_E(4, 5), CY_S(10, 1, "aligned"), CY_E(5, 6),
    CY_S(15, 1, "aligned"), CY_X
};

static const cycle_op_t cycle_host[] = {
    CY_S(0, 0, NULL), CY_F, CY_S(6, 1, "aligned"), CY_X
};

/* Removing the input row retracts the cycle: nothing else supports
 * t(1, 2), so t(1, 4) and with it the derived `flag()` go too. */
static const cycle_op_t cycle_retract[] = {
    CY_S(6, 1, "aligned"), CY_U, CY_S(0, 0, "replicate"), CY_F,
    CY_S(6, 1, "aligned"), CY_X
};

static const cycle_op_t cycle_bdx[] = { CY_S(6, 1, "bdx"), CY_X };

static const cycle_op_t cycle_replicate[] = {
    CY_S(6, 1, "replicate"), CY_X
};

/* `!flag()` in a second recursive stratum; its strategy is not asserted,
 * only the rows. */
static const cycle_op_t cycle_negated[] = {
    CY_S(3, 1, NULL), CY_E(4, 5), CY_S(4, 1, NULL), CY_X
};

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
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        for (int cb = 0; cb < 2; cb++) {
            uint32_t n = workers[w];
            bool c = cb != 0;

            check_owner("owner cycle through flag(), short chain", false, 3,
                n, c, NULL);
            check_owner("owner cycle through host flag(), short chain",
                true, 3, n, c, NULL);
            check_owner("owner cycle through flag(), long chain", false,
                40, n, c, "owner_tiny_frontier");
            check_owner("owner cycle through host flag(), long chain", true,
                40, n, c, "owner_tiny_frontier");
        }
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        for (int cb = 0; cb < 2; cb++) {
            uint32_t n = workers[w];
            bool c = cb != 0;

            check_cycle("cycle through flag()", CYCLE_HEAD "flag().\n"
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- e(x, y), t(y, z).\n"
                "flag() :- t(1, 4).\n", cycle_once, n, c);
            check_cycle("cycle through flag(), other body order",
                CYCLE_HEAD "flag().\n"
                "t(x, y) :- flag(), e(x, y).\n"
                "t(x, z) :- t(x, y), e(y, z).\n"
                "flag() :- t(1, 4).\n", cycle_once, n, c);
            check_cycle("cycle through flag(), wildcard rule",
                CYCLE_HEAD "flag().\n"
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- e(x, y), t(y, z).\n"
                "flag() :- t(_, 4).\n", cycle_once, n, c);
            check_cycle("two-sided cycle through flag()",
                CYCLE_HEAD "flag().\n"
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), e(y, z).\n"
                "t(x, z) :- e(x, y), t(y, z).\n"
                "flag() :- t(1, 4).\n", cycle_once, n, c);
            check_cycle("cycle through flag(), growing chain",
                CYCLE_HEAD "flag().\n"
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- e(x, y), t(y, z).\n"
                "flag() :- t(1, 4).\n", cycle_grows, n, c);
            check_cycle("cycle through host flag()", CYCLE_HEAD
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- e(x, y), t(y, z).\n"
                "flag() :- t(1, 4).\n", cycle_host, n, c);
            check_cycle("cycle through flag(), removed and reinserted",
                CYCLE_HEAD "flag().\n"
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- e(x, y), t(y, z).\n"
                "flag() :- t(1, 4).\n", cycle_retract, n, c);
            check_cycle("self-join cycle through flag()",
                CYCLE_HEAD "flag().\n"
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), t(y, z).\n"
                "flag() :- t(1, 4).\n", cycle_bdx, n, c);
            check_cycle("cycle through flag() in the recursive rule",
                CYCLE_HEAD "flag().\n"
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), e(y, z), flag().\n"
                "flag() :- t(1, 4).\n", cycle_replicate, n, c);
            check_cycle("!flag() beside a cycle through flag()",
                ".decl flag()\n.decl fp(x: int64)\n"
                ".decl u(x: int64, y: int64)\n" EDGES
                "fp(100) :- flag().\nflag().\n"
                "u(x, y) :- e(x, y), flag().\n"
                "u(x, z) :- u(x, y), e(y, z).\n"
                "flag() :- u(1, 4).\n"
                "t(x, y) :- e(x, y).\n"
                "t(x, z) :- t(x, y), e(y, z), !flag().\n",
                cycle_negated, n, c);
        }
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
