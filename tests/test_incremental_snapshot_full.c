/*
 * test_incremental_snapshot_full.c - Issue #2114 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A snapshot after an incremental insert -- col_session_insert_incremental(),
 * or a compound made with wl_session_make_compound() -- re-evaluates only
 * the strata the inserted relation reaches, on top of the rows the last
 * evaluation left and with the new rows seeded as a delta.  A rule reading
 * the inserted relation through another head emitted its earlier rows a
 * second time, and an aggregate over the inserted relation was computed
 * over the delta alone and kept beside the old result.  Such a snapshot now
 * deduplicates the heads of the non-recursive strata it evaluates, and
 * evaluates in full when a reached stratum aggregates or negates, or a
 * non-recursive one joins.
 *
 * Each case loads 7 and 8, reads them with a snapshot, adds 9 the same way
 * -- some also remove e(7) before or after it, without a delta callback --
 * and requires the exact model from the next two snapshots, in a session
 * created with one worker and with four.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

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

#define MAX_ROWS 32

typedef struct {
    const char *relation;
    int64_t row[MAX_ROWS][2];
    uint32_t count;
} rows_t;

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    rows_t *rows = (rows_t *)user_data;

    if (strcmp(relation, rows->relation) != 0)
        return;
    if (rows->count < MAX_ROWS) {
        rows->row[rows->count][0] = ncols > 0 ? row[0] : 0;
        rows->row[rows->count][1] = ncols > 1 ? row[1] : 0;
    }
    rows->count++;
}

typedef struct {
    const char *name;
    bool compound; /* add values as f(v) compounds rather than e(v) rows */
    const char *program;
    const char *relation;           /* the relation the case checks */
    int64_t want[MAX_ROWS][2];      /* its rows after e(9), each once */
    uint32_t nwant;
    int remove7; /* 1: remove e(7) before adding 9; 2: after; 0: neither */
} case_t;

#define F_DECL ".decl __compound_f_1(handle: int64, arg0: int64)\n"

static const case_t CASES[] = {
    { "count over a rule head", false,
      ".decl e(x: int64)\n.decl r(x: int64)\n.decl n(k: int64, c: int64)\n"
      "r(x) :- e(x).\nn(x, count(x)) :- r(x).\n",
      "n", { { 7, 1 }, { 8, 1 }, { 9, 1 } }, 3 },
    { "plain rule over a rule head", false,
      ".decl e(x: int64)\n.decl r(x: int64)\n.decl n(k: int64, c: int64)\n"
      "r(x) :- e(x).\nn(x, x) :- r(x).\n",
      "n", { { 7, 7 }, { 8, 8 }, { 9, 9 } }, 3 },
    { "count over the inserted relation", false,
      ".decl e(x: int64)\n.decl n(k: int64, c: int64)\n"
      "n(x, count(x)) :- e(x).\n",
      "n", { { 7, 1 }, { 8, 1 }, { 9, 1 } }, 3 },
    { "sum over the inserted relation", false,
      ".decl e(x: int64)\n.decl n(k: int64)\nn(sum(x)) :- e(x).\n",
      "n", { { 24, 0 } }, 1 },
    { "closure below the inserted relation", false,
      ".decl e(x: int64)\n.decl r(x: int64)\n.decl edge(a: int64, b: int64)\n"
      ".decl reach(x: int64)\n"
      "edge(9, 10). edge(10, 11).\n"
      "r(x) :- e(x).\n"
      "reach(x) :- r(x).\nreach(y) :- reach(x), edge(x, y).\n",
      "reach", { { 7, 0 }, { 8, 0 }, { 9, 0 }, { 10, 0 }, { 11, 0 } }, 5 },
    { "negation of the inserted relation", false,
      ".decl e(x: int64)\n.decl s(x: int64)\n.decl p(x: int64)\n"
      "s(7). s(8). s(9). s(10).\n"
      "p(x) :- s(x), !e(x).\n",
      "p", { { 10, 0 } }, 1 },
    { "plain rule over a compound", true,
      F_DECL ".decl r(x: int64)\n.decl m(k: int64, c: int64)\n"
      "r(A) :- __compound_f_1(H, A).\nm(A, A) :- r(A).\n",
      "m", { { 7, 7 }, { 8, 8 }, { 9, 9 } }, 3 },
    { "sum over a compound", true,
      F_DECL ".decl r(x: int64)\n.decl n(k: int64)\n"
      "r(A) :- __compound_f_1(H, A).\nn(sum(A)) :- r(A).\n",
      "n", { { 24, 0 } }, 1 },
    { "self-join of a compound", true,
      F_DECL ".decl m(a: int64, b: int64)\n"
      "m(A, B) :- __compound_f_1(H, A), __compound_f_1(G, B), A < B.\n",
      "m", { { 7, 8 }, { 7, 9 }, { 8, 9 } }, 3 },
    { "join of a rule head with its own input", true,
      F_DECL ".decl r(x: int64)\n.decl m(a: int64, b: int64)\n"
      "r(A) :- __compound_f_1(H, A).\n"
      "m(A, B) :- r(A), __compound_f_1(G, B), A > B.\n",
      "m", { { 8, 7 }, { 9, 7 }, { 9, 8 } }, 3 },
    { "self-join of the inserted relation", false,
      ".decl e(x: int64)\n.decl m(a: int64, b: int64)\n"
      "m(x, y) :- e(x), e(y), x < y.\n",
      "m", { { 7, 8 }, { 7, 9 }, { 8, 9 } }, 3 },
    { "recursion negating the inserted relation", false,
      ".decl e(x: int64)\n.decl edge(a: int64, b: int64)\n"
      ".decl reach(x: int64)\n"
      "reach(1).\nedge(1, 2). edge(2, 9). edge(9, 10).\n"
      "reach(y) :- reach(x), edge(x, y), !e(y).\n",
      "reach", { { 1, 0 }, { 2, 0 } }, 2 },
    { "removal before the incremental insert", false,
      ".decl e(x: int64)\n.decl r(x: int64)\n.decl n(k: int64, c: int64)\n"
      "r(x) :- e(x).\nn(x, x) :- r(x).\n",
      "n", { { 8, 8 }, { 9, 9 } }, 2, 1 },
    { "removal after the incremental insert", false,
      ".decl e(x: int64)\n.decl r(x: int64)\n.decl n(k: int64, c: int64)\n"
      "r(x) :- e(x).\nn(x, x) :- r(x).\n",
      "n", { { 8, 8 }, { 9, 9 } }, 2, 2 },
};

/* Remove e(7) with the plain, callback-free removal. */
static int
remove7(wl_session_t *s)
{
    int64_t v = 7;
    return wl_session_remove(s, "e", &v, 1, 1);
}

/* Add @v to the case's input: an e(v) row through the incremental route,
 * or an f(v) compound. */
static int
add(wl_session_t *s, const case_t *c, int64_t v, bool incremental)
{
    if (c->compound) {
        wirelog_compound_arg_t arg = { WIRELOG_TYPE_INT64, v };
        uint64_t handle = 0;
        return wl_session_make_compound(s, "f", 1, &arg, &handle);
    }
    return incremental ? col_session_insert_incremental(s, "e", &v, 1, 1)
                       : wl_session_insert(s, "e", &v, 1, 1);
}

static char message[256];

/* Snapshot and require exactly the case's rows, each once. */
static const char *
expect(wl_session_t *s, const case_t *c, const char *when)
{
    rows_t rows;

    memset(&rows, 0, sizeof(rows));
    rows.relation = c->relation;
    if (wl_session_snapshot(s, collect, &rows) != 0) {
        snprintf(message, sizeof(message), "%s: snapshot failed", when);
        return message;
    }
    if (rows.count != c->nwant) {
        snprintf(message, sizeof(message), "%s: %s has %u rows, want %u",
            when, c->relation, rows.count, c->nwant);
        return message;
    }
    for (uint32_t w = 0; w < c->nwant; w++) {
        uint32_t seen = 0;
        for (uint32_t r = 0; r < rows.count && r < MAX_ROWS; r++)
            if (rows.row[r][0] == c->want[w][0]
                && rows.row[r][1] == c->want[w][1])
                seen++;
        if (seen != 1) {
            snprintf(message, sizeof(message),
                "%s: %s(%lld,%lld) seen %u times, want once", when,
                c->relation, (long long)c->want[w][0],
                (long long)c->want[w][1], seen);
            return message;
        }
    }
    return NULL;
}

static void
run_case(const case_t *c, uint32_t workers)
{
    wirelog_error_t err;
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *s = NULL;
    const char *why = NULL;

    tests_run++;
    printf("  [%d] %s (%u worker%s)", tests_run, c->name, workers,
        workers == 1 ? "" : "s");
    prog = wirelog_parse_string(c->program, &err);
    if (!prog) {
        why = "parse";
        goto done;
    }
    plan_fixture_hold(prog);
    if (wl_plan_from_program(prog, &plan) != 0) {
        why = "plan";
        goto done;
    }
    if (wl_session_create(wl_backend_columnar(), plan, workers, &s) != 0) {
        why = "session";
        goto done;
    }
    if (wl_session_load_facts(s, prog) != 0) {
        why = "inline facts";
        goto done;
    }
    if (add(s, c, 7, false) != 0 || add(s, c, 8, false) != 0) {
        why = "load";
        goto done;
    }
    if (wl_session_snapshot(s, collect, &(rows_t){ .relation = "" }) != 0) {
        why = "load snapshot";
        goto done;
    }
    if (c->remove7 == 1 && remove7(s) != 0) {
        why = "remove";
        goto done;
    }
    if (add(s, c, 9, true) != 0) {
        why = "incremental insert";
        goto done;
    }
    if (c->remove7 == 2 && remove7(s) != 0) {
        why = "remove";
        goto done;
    }
    if ((why = expect(s, c, "first snapshot")) != NULL)
        goto done;
    why = expect(s, c, "second snapshot");
done:
    if (s)
        wl_session_destroy(s);
    if (plan)
        wl_plan_free(plan);
    if (why) {
        tests_failed++;
        printf(" ... FAIL: %s\n", why);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 4 };

    printf("test_incremental_snapshot_full (Issue #2114)\n");
    /* Let the four-worker sessions use four workers on these small inputs. */
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++)
        for (uint32_t i = 0; i < sizeof(CASES) / sizeof(CASES[0]); i++)
            run_case(&CASES[i], workers[w]);
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
