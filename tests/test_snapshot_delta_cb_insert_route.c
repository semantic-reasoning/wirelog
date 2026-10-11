/*
 * test_snapshot_delta_cb_insert_route.c - Issue #2121 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A snapshot after an incremental insert brings the strata the insert
 * reaches up to date through the insert route (session_snapshot_insert,
 * #2114).  With a delta callback installed it evaluated in full instead
 * (#1030), and clearing the callback with an insert pending did the same
 * (#2108).  A snapshot fires no delta events on either route, so the
 * callback only told the snapshot to take the slower one.
 *
 * Each case drives a session with a recording delta callback through a
 * load of 7 and 8 read by a snapshot, then 9 added and read by a snapshot
 * and a step, 10 added and read by a step, and 11 added and read by a
 * snapshot and a step.  The steps right after a snapshot have nothing to
 * evaluate; the step after adding 10 is the one that reads rows a snapshot
 * left.  The reference model at each read comes from a new session with no
 * callback and one worker, given the values added so far, less any
 * removed, and read by one snapshot, so it evaluates in full.  One worker,
 * because plain steps and snapshots at more workers are not yet a sound
 * reference for these programs (#2134, #2135).  The case requires
 *   - each snapshot after an add to take the insert route or not, as the
 *     case's route field says;
 *   - the reference model after every snapshot;
 *   - no delta events from a snapshot, and from each step the difference
 *     between the reference models before and after it, or none once the
 *     callback is cleared.
 * Cases with clear set clear the callback after adding 9.  Every case runs
 * with the callback session created with one worker and with four.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "../wirelog/columnar/internal.h"
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

#define MAX_TUPLES 512
#define REL_LEN 32

/* A row of a relation with at most two columns, and the sign of a delta
 * event (+1 for a model row). */
typedef struct {
    char relation[REL_LEN];
    int64_t col[2];
    int32_t diff;
} tuple_t;

typedef struct {
    tuple_t t[MAX_TUPLES];
    uint32_t count;
    bool overflow;
} tuples_t;

static void
tuples_add(tuples_t *ts, const char *relation, const int64_t *row,
    uint32_t ncols, int32_t diff)
{
    if (ts->count == MAX_TUPLES) {
        ts->overflow = true;
        return;
    }
    tuple_t *t = &ts->t[ts->count++];
    memset(t, 0, sizeof(*t));
    snprintf(t->relation, sizeof(t->relation), "%s", relation);
    t->col[0] = ncols > 0 ? row[0] : 0;
    t->col[1] = ncols > 1 ? row[1] : 0;
    t->diff = diff;
}

static int
tuple_cmp(const void *a, const void *b)
{
    const tuple_t *x = (const tuple_t *)a;
    const tuple_t *y = (const tuple_t *)b;
    int c = strcmp(x->relation, y->relation);

    if (c != 0)
        return c;
    for (int i = 0; i < 2; i++)
        if (x->col[i] != y->col[i])
            return x->col[i] < y->col[i] ? -1 : 1;
    return (x->diff > y->diff) - (x->diff < y->diff);
}

static void
tuples_sort(tuples_t *ts)
{
    qsort(ts->t, ts->count, sizeof(ts->t[0]), tuple_cmp);
}

static void
collect_model(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    tuples_add((tuples_t *)user_data, relation, row, ncols, 1);
}

static tuples_t events;

static void
record_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)user_data;
    tuples_add(&events, relation, row, ncols, diff);
}

typedef struct {
    const char *name;
    bool compound; /* add values as f(v) compounds rather than e(v) rows */
    const char *program;
    bool route;   /* each snapshot after an add takes the insert route */
    bool remove7; /* remove e(7) after adding 9, so the snapshot after 9
                   * evaluates in full and the one after 11 takes the
                   * insert route */
    bool clear;   /* clear the callback after adding 9 */
} case_t;

#define F_DECL ".decl __compound_f_1(handle: int64, arg0: int64)\n"
#define E_R ".decl e(x: int64)\n.decl r(x: int64)\nr(x) :- e(x).\n"

static const case_t CASES[] = {
    { "join of two rule heads", false,
      E_R ".decl q(x: int64)\n.decl p(a: int64, b: int64)\n"
      "q(x) :- e(x), x > 7.\np(x, y) :- r(x), q(y).\n", true },
    { "linear closure", false,
      ".decl e(x: int64)\n.decl edge(a: int64, b: int64)\n"
      ".decl t(a: int64, b: int64)\n"
      "edge(x, x + 1) :- e(x).\nt(x, y) :- edge(x, y).\n"
      "t(x, z) :- t(x, y), edge(y, z).\n", true },
    { "nonlinear closure", false,
      ".decl e(x: int64)\n.decl edge(a: int64, b: int64)\n"
      ".decl t(a: int64, b: int64)\n"
      "edge(x, x + 1) :- e(x).\nt(x, y) :- edge(x, y).\n"
      "t(x, z) :- t(x, y), t(y, z).\n", true },
    { "join of a rule head with a compound", true,
      F_DECL ".decl r(x: int64)\n.decl m(a: int64, b: int64)\n"
      "r(A) :- __compound_f_1(H, A).\n"
      "m(A, B) :- r(A), __compound_f_1(G, B), A > B.\n", true },
    { "count over a rule head", false,
      E_R ".decl n(k: int64, c: int64)\nn(x, count(x)) :- r(x).\n", false },
    { "negation of the inserted relation", false,
      ".decl e(x: int64)\n.decl s(x: int64)\n.decl p(x: int64)\n"
      "s(7). s(8). s(9). s(10). s(11). s(12).\n"
      "p(x) :- s(x), !e(x).\n", false },
    { "removal staged with the callback installed", false,
      E_R ".decl p(a: int64, b: int64)\np(x, x) :- r(x).\n", false, true },
    { "insert into a rule head", false,
      ".decl s(x: int64)\n.decl e(x: int64)\n.decl p(a: int64, b: int64)\n"
      "s(1).\ne(2).\ne(x) :- s(x).\np(x, x) :- e(x).\n", false },
    { "callback cleared with an insert pending", false,
      E_R ".decl p(a: int64, b: int64)\np(x, x) :- r(x).\n", true, false,
      true },
    { "callback cleared with a removal staged", false,
      E_R ".decl p(a: int64, b: int64)\np(x, x) :- r(x).\n", false, true,
      true },
};

static char message[256];

typedef struct {
    const case_t *c;
    wirelog_program_t *prog;
    wl_plan_t *plan;
    wl_session_t *cb;   /* the session with a recording delta callback */
    int64_t live[8];    /* the values added and not removed, in order */
    uint32_t nlive;
    tuples_t before;    /* the reference model at the last read */
    tuples_t after;
    tuples_t got;
    bool cleared; /* the callback session's callback was cleared */
} run_t;

/* Add @v to @s: an e(v) row, through the incremental route when
 * @incremental is set, or an f(v) compound. */
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

static const char *
add_value(run_t *r, int64_t v)
{
    if (r->nlive == sizeof(r->live) / sizeof(r->live[0])
        || add(r->cb, r->c, v, true) != 0) {
        snprintf(message, sizeof(message), "adding %lld failed",
            (long long)v);
        return message;
    }
    r->live[r->nlive++] = v;
    return NULL;
}

static const char *
snapshot_into(wl_session_t *s, tuples_t *model, const char *when)
{
    memset(model, 0, sizeof(*model));
    if (wl_session_snapshot(s, collect_model, model) != 0) {
        snprintf(message, sizeof(message), "%s: snapshot failed", when);
        return message;
    }
    if (model->overflow) {
        snprintf(message, sizeof(message), "%s: model overflow", when);
        return message;
    }
    tuples_sort(model);
    return NULL;
}

/* Require @got and @want to hold the same tuples, counting repeats. */
static const char *
same(const tuples_t *got, const tuples_t *want, const char *what,
    const char *when)
{
    uint32_t n = got->count < want->count ? got->count : want->count;

    for (uint32_t i = 0; i < n; i++) {
        if (tuple_cmp(&got->t[i], &want->t[i]) != 0) {
            const tuple_t *g = &got->t[i];
            const tuple_t *w = &want->t[i];
            snprintf(message, sizeof(message),
                "%s: %s differ at %u: got %s(%lld,%lld)%+d, want "
                "%s(%lld,%lld)%+d", when, what, i, g->relation,
                (long long)g->col[0], (long long)g->col[1], g->diff,
                w->relation, (long long)w->col[0], (long long)w->col[1],
                w->diff);
            return message;
        }
    }
    if (got->count != want->count) {
        snprintf(message, sizeof(message), "%s: %u %s, want %u", when,
            got->count, what, want->count);
        return message;
    }
    return NULL;
}

static wl_session_t *open_session(run_t *r, uint32_t workers);

/* Read the reference model into @model, from a first evaluation, which
 * does not take the insert route. */
static const char *
reference(run_t *r, tuples_t *model, const char *when)
{
    wl_session_t *s = open_session(r, 1);
    uint32_t route0 = wl_columnar_session_test_insert_route_taken;
    const char *why = NULL;

    if (!s) {
        snprintf(message, sizeof(message), "%s: reference session", when);
        return message;
    }
    for (uint32_t i = 0; i < r->nlive && !why; i++)
        if (add(s, r->c, r->live[i], false) != 0) {
            snprintf(message, sizeof(message), "%s: reference add", when);
            why = message;
        }
    if (!why)
        why = snapshot_into(s, model, when);
    wl_session_destroy(s);
    if (!why && wl_columnar_session_test_insert_route_taken != route0) {
        snprintf(message, sizeof(message), "%s: reference took the insert "
            "route", when);
        why = message;
    }
    return why;
}

/* Snapshot the callback session and require the reference model, no delta
 * events, and the insert route taken @route times. */
static const char *
read_by_snapshot(run_t *r, uint32_t route, const char *when)
{
    const char *why;
    uint32_t route0 = wl_columnar_session_test_insert_route_taken;

    memset(&events, 0, sizeof(events));
    if ((why = snapshot_into(r->cb, &r->got, when)) != NULL)
        return why;
    uint32_t taken = wl_columnar_session_test_insert_route_taken - route0;
    if (taken != route) {
        snprintf(message, sizeof(message),
            "%s: insert route taken %u times, want %u", when, taken, route);
        return message;
    }
    if (events.count != 0) {
        snprintf(message, sizeof(message), "%s: snapshot fired %u delta "
            "events", when, events.count);
        return message;
    }
    if ((why = reference(r, &r->after, when)) != NULL)
        return why;
    if ((why = same(&r->got, &r->after, "model rows", when)) != NULL)
        return why;
    r->before = r->after;
    return NULL;
}

/* Step the callback session and require its delta events to be the
 * difference between the reference models before and after, or none once
 * its callback is cleared. */
static const char *
read_by_step(run_t *r, const char *when)
{
    const char *why;

    memset(&events, 0, sizeof(events));
    if (wl_session_step(r->cb) != 0) {
        snprintf(message, sizeof(message), "%s: step failed", when);
        return message;
    }
    if (events.overflow) {
        snprintf(message, sizeof(message), "%s: event overflow", when);
        return message;
    }
    if ((why = reference(r, &r->after, when)) != NULL)
        return why;

    /* The difference: rows only after as +1, rows only before as -1. */
    tuples_t *diff = &r->got;
    memset(diff, 0, sizeof(*diff));
    uint32_t i = 0, j = 0;
    while (i < r->before.count || j < r->after.count) {
        int c = i == r->before.count ? 1
            : j == r->after.count ? -1
            : tuple_cmp(&r->before.t[i], &r->after.t[j]);
        if (c == 0) {
            i++;
            j++;
        } else if (c < 0) {
            const tuple_t *t = &r->before.t[i++];
            tuples_add(diff, t->relation, t->col, 2, -1);
        } else {
            const tuple_t *t = &r->after.t[j++];
            tuples_add(diff, t->relation, t->col, 2, 1);
        }
    }
    if (r->cleared)
        diff->count = 0;
    tuples_sort(diff);
    tuples_sort(&events);
    if ((why = same(&events, diff, "delta events", when)) != NULL)
        return why;
    r->before = r->after;
    return NULL;
}

static wl_session_t *
open_session(run_t *r, uint32_t workers)
{
    wl_session_t *s = NULL;

    if (wl_session_create(wl_backend_columnar(), r->plan, workers, &s) != 0)
        return NULL;
    if (wl_session_load_facts(s, r->prog) != 0) {
        wl_session_destroy(s);
        return NULL;
    }
    return s;
}

static const char *
drive(run_t *r)
{
    const case_t *c = r->c;
    const uint32_t route = c->route ? 1u : 0u;
    const uint32_t route11 = c->route || c->remove7 ? 1u : 0u;
    const char *why;

    wl_session_set_delta_cb(r->cb, record_delta, NULL);
    if ((why = add_value(r, 7)) != NULL || (why = add_value(r, 8)) != NULL)
        return why;
    if ((why = read_by_snapshot(r, 0, "load")) != NULL)
        return why;
    if ((why = add_value(r, 9)) != NULL)
        return why;
    if (c->remove7) {
        int64_t v = 7;
        if (wl_session_remove(r->cb, "e", &v, 1, 1) != 0
            || r->live[0] != 7)
            return "remove failed";
        memmove(r->live, r->live + 1, --r->nlive * sizeof(r->live[0]));
    }
    if (c->clear) {
        wl_session_set_delta_cb(r->cb, NULL, NULL);
        r->cleared = true;
    }
    if ((why = read_by_snapshot(r, route, "after adding 9")) != NULL)
        return why;
    if ((why = read_by_step(r, "step after the snapshot")) != NULL)
        return why;
    if ((why = add_value(r, 10)) != NULL)
        return why;
    if ((why = read_by_step(r, "step after adding 10")) != NULL)
        return why;
    if ((why = add_value(r, 11)) != NULL)
        return why;
    if ((why = read_by_snapshot(r, route11, "after adding 11")) != NULL)
        return why;
    if ((why = read_by_step(r, "step after the last snapshot")) != NULL)
        return why;
    return read_by_snapshot(r, 0, "final");
}

static void
run_case(const case_t *c, uint32_t workers)
{
    static run_t r;
    wirelog_error_t err;
    const char *why = NULL;

    tests_run++;
    printf("  [%d] %s (%u worker%s)", tests_run, c->name, workers,
        workers == 1 ? "" : "s");
    memset(&r, 0, sizeof(r));
    r.c = c;
    r.prog = wirelog_parse_string(c->program, &err);
    if (!r.prog) {
        why = "parse";
        goto done;
    }
    plan_fixture_hold(r.prog);
    if (wl_plan_from_program(r.prog, &r.plan) != 0) {
        why = "plan";
        goto done;
    }
    r.cb = open_session(&r, workers);
    if (!r.cb) {
        why = "session";
        goto done;
    }
    why = drive(&r);
done:
    if (r.cb)
        wl_session_destroy(r.cb);
    if (r.plan)
        wl_plan_free(r.plan);
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

    printf("test_snapshot_delta_cb_insert_route (Issue #2121)\n");
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
