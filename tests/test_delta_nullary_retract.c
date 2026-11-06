/*
 * test_delta_nullary_retract.c - zero-width heads retract under a delta
 * callback.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * With a delta callback set, a step re-derives every head of an affected
 * stratum from scratch and diffs it against the head's pre-step snapshot.
 * The snapshot skipped zero-column relations, so a zero-width head was
 * never emptied before re-derivation: once derived, `ok()` stayed held
 * after the last body row was removed, no -1 was announced, and every rule
 * reading `ok()` kept its consequence.  Without a delta callback the same
 * program retracted correctly.
 *
 * A zero-width head is a set: it is held while any body row derives it and
 * is announced -1 exactly once, on the step after which nothing derives it.
 *
 * The easy-API cases run on sessions opened with one, two and four workers,
 * with a delta callback and without one (checking snapshots).
 * WIRELOG_TDD_MIN_ROWS_PER_WORKER=1 and
 * WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER=1 lower the rows a worker needs, so
 * a step whose driver holds enough rows may use workers on these small
 * inputs; whether a given step does depends on its row count, and no case
 * asserts it.  The filtered head `ok() :- e(1).` was stored at width one
 * until the FLATMAP lowering emitted a zero-column projection for it
 * (#2231).  Its negation case fails without that lowering; the case where
 * it feeds a unary relation passes at either width.
 *
 * The plan-level cases drive col_stratum_step_with_delta directly: once
 * without a delta observer, which takes the direct emission path, and once
 * through a step whose second head fails, which rolls back a zero-width
 * head that held its row or had been emptied before the step.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "wirelog/backend.h"
#include "wirelog/columnar/internal.h"
#include "wirelog/session.h"
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

#define MAX_EVENTS 64

typedef struct {
    char rel[16];
    uint32_t ncols;
    int32_t diff;
    int64_t v0;
} event_t;

typedef struct {
    event_t ev[MAX_EVENTS];
    uint32_t count;
    bool overflow;
} event_log_t;

static void
on_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    event_log_t *log = (event_log_t *)user_data;
    if (log->count >= MAX_EVENTS) {
        log->overflow = true;
        return;
    }
    event_t *e = &log->ev[log->count++];
    snprintf(e->rel, sizeof(e->rel), "%s", relation);
    e->ncols = ncols;
    e->diff = diff;
    e->v0 = (ncols > 0 && row) ? row[0] : 0;
}

/* Net diff and event count for @rel in @log. */
static int32_t
net(const event_log_t *log, const char *rel, uint32_t *events)
{
    int32_t sum = 0;
    *events = 0;
    for (uint32_t i = 0; i < log->count; i++)
        if (strcmp(log->ev[i].rel, rel) == 0) {
            sum += log->ev[i].diff;
            (*events)++;
        }
    return sum;
}

static unsigned snap_count;

static void
on_snap(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
    snap_count++;
}

static int
rows_of(wirelog_easy_session_t *s, const char *rel)
{
    snap_count = 0;
    int rc = wirelog_easy_snapshot(s, rel, on_snap, NULL);
    return rc != 0 ? -1 : (int)snap_count;
}

typedef struct {
    const char *name;
    unsigned workers;
    bool ok;
    char why[160];
    bool no_callback;
} ctx_t;

#define CHECK(c, cond, ...)                                          \
        do {                                                         \
            if ((c)->ok && !(cond)) {                                \
                (c)->ok = false;                                     \
                snprintf((c)->why, sizeof((c)->why), __VA_ARGS__);   \
            }                                                        \
        } while (0)

/* Expect @rel to have net @want and at most one event this step. */
static void
expect_net(ctx_t *c, const event_log_t *log, const char *step,
    const char *rel, int32_t want)
{
    if (c->no_callback)
        return; /* only the snapshots are checked */
    uint32_t events = 0;
    int32_t got = net(log, rel, &events);
    CHECK(c, !log->overflow, "%s: event log overflow", step);
    CHECK(c, got == want && events <= 1u,
        "%s: %s net %d over %u events, want %d", step, rel, (int)got,
        events, (int)want);
}

static wirelog_easy_session_t *
open_session(ctx_t *c, const char *src, event_log_t *log)
{
    wirelog_easy_open_opts_t o = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    o.num_workers = c->workers;
    o.eager_build = true;
    int rc = wirelog_easy_open_opts(src, &o, &s);
    CHECK(c, rc == 0 && s, "open rc=%d", rc);
    if (!s)
        return NULL;
    if (c->no_callback)
        return s;
    rc = wirelog_easy_set_delta_cb(s, on_delta, log);
    CHECK(c, rc == 0, "set_delta_cb rc=%d", rc);
    return s;
}

static int
do_step(ctx_t *c, wirelog_easy_session_t *s, event_log_t *log,
    const char *what)
{
    memset(log, 0, sizeof(*log));
    int rc = wirelog_easy_step(s);
    CHECK(c, rc == 0, "%s: step rc=%d", what, rc);
    return rc;
}

static void
report(ctx_t *c)
{
    tests_run++;
    printf("  %-44s W=%u%s", c->name, c->workers,
        c->no_callback ? " (no callback)" : "");
    if (c->ok) {
        tests_passed++;
        printf(" ... PASS\n");
    } else {
        tests_failed++;
        printf(" ... FAIL: %s\n", c->why);
    }
}

#define PROGRAM_HEAD                                    \
        ".decl ok()\n"                                  \
        ".decl e(x: int64)\n"                           \
        ".decl p(x: int64)\n"                           \
        ".decl q(x: int64)\n"                           \
        ".decl d(x: int64)\n"                           \
        "d(7).\n"                                       \
        "p(100) :- ok().\n"                             \
        "q(x) :- d(x), !ok().\n"

static const char *const unfiltered_src = PROGRAM_HEAD "ok() :- e(x).\n";
static const char *const filtered_src = PROGRAM_HEAD "ok() :- e(1).\n";

/* ok() derived from e(1) and e(2): held until both are removed. */
static void
test_two_supports(unsigned workers, bool no_callback)
{
    ctx_t c = { "unfiltered nullary head, two supports", workers, true,
                { 0 }, no_callback };
    event_log_t log;
    memset(&log, 0, sizeof(log));
    wirelog_easy_session_t *s = open_session(&c, unfiltered_src, &log);
    int64_t v1 = 1, v2 = 2;
    if (s) {
        CHECK(&c, wirelog_easy_insert(s, "e", &v1, 1) == 0, "insert e(1)");
        CHECK(&c, wirelog_easy_insert(s, "e", &v2, 1) == 0, "insert e(2)");
        if (do_step(&c, s, &log, "insert") == 0) {
            expect_net(&c, &log, "insert", "ok", +1);
            for (uint32_t i = 0; i < log.count; i++)
                if (strcmp(log.ev[i].rel, "ok") == 0)
                    CHECK(&c, log.ev[i].ncols == 0u, "ok width %u",
                        log.ev[i].ncols);
            CHECK(&c, rows_of(s, "ok") == 1, "insert: ok rows %d",
                rows_of(s, "ok"));
        }
        CHECK(&c, wirelog_easy_remove(s, "e", &v1, 1) == 0, "remove e(1)");
        if (do_step(&c, s, &log, "remove e(1)") == 0) {
            expect_net(&c, &log, "remove e(1)", "ok", 0);
            CHECK(&c, rows_of(s, "ok") == 1, "remove e(1): ok rows %d",
                rows_of(s, "ok"));
        }
        CHECK(&c, wirelog_easy_remove(s, "e", &v2, 1) == 0, "remove e(2)");
        if (do_step(&c, s, &log, "remove e(2)") == 0) {
            expect_net(&c, &log, "remove e(2)", "ok", -1);
            CHECK(&c, rows_of(s, "ok") == 0, "remove e(2): ok rows %d",
                rows_of(s, "ok"));
        }
        wirelog_easy_close(s);
    }
    report(&c);
}

/* p(100) :- ok(). follows ok() out. */
static void
test_dependent_unary(const char *name, const char *src, unsigned workers,
    bool no_callback)
{
    ctx_t c = { name, workers, true, { 0 }, no_callback };
    event_log_t log;
    memset(&log, 0, sizeof(log));
    wirelog_easy_session_t *s = open_session(&c, src, &log);
    int64_t v1 = 1;
    if (s) {
        CHECK(&c, wirelog_easy_insert(s, "e", &v1, 1) == 0, "insert e(1)");
        if (do_step(&c, s, &log, "insert") == 0) {
            expect_net(&c, &log, "insert", "ok", +1);
            expect_net(&c, &log, "insert", "p", +1);
            CHECK(&c, rows_of(s, "p") == 1, "insert: p rows %d",
                rows_of(s, "p"));
        }
        CHECK(&c, wirelog_easy_remove(s, "e", &v1, 1) == 0, "remove e(1)");
        if (do_step(&c, s, &log, "remove") == 0) {
            expect_net(&c, &log, "remove", "ok", -1);
            expect_net(&c, &log, "remove", "p", -1);
            for (uint32_t i = 0; i < log.count; i++)
                if (strcmp(log.ev[i].rel, "p") == 0)
                    CHECK(&c, log.ev[i].ncols == 1u && log.ev[i].v0 == 100,
                        "remove: p event width %u value %lld",
                        log.ev[i].ncols, (long long)log.ev[i].v0);
            CHECK(&c, rows_of(s, "ok") == 0, "remove: ok rows %d",
                rows_of(s, "ok"));
            CHECK(&c, rows_of(s, "p") == 0, "remove: p rows %d",
                rows_of(s, "p"));
        }
        wirelog_easy_close(s);
    }
    report(&c);
}

/* q(x) :- d(x), !ok(). flips with ok(): held, gone, held again. */
static void
test_negation_flip(const char *name, const char *src, unsigned workers,
    bool no_callback)
{
    ctx_t c = { name, workers, true, { 0 }, no_callback };
    event_log_t log;
    memset(&log, 0, sizeof(log));
    wirelog_easy_session_t *s = open_session(&c, src, &log);
    int64_t v1 = 1;
    if (s) {
        if (do_step(&c, s, &log, "initial") == 0) {
            expect_net(&c, &log, "initial", "q", +1);
            CHECK(&c, rows_of(s, "q") == 1, "initial: q rows %d",
                rows_of(s, "q"));
        }
        CHECK(&c, wirelog_easy_insert(s, "e", &v1, 1) == 0, "insert e(1)");
        if (do_step(&c, s, &log, "insert") == 0) {
            expect_net(&c, &log, "insert", "q", -1);
            CHECK(&c, rows_of(s, "q") == 0, "insert: q rows %d",
                rows_of(s, "q"));
        }
        CHECK(&c, wirelog_easy_remove(s, "e", &v1, 1) == 0, "remove e(1)");
        if (do_step(&c, s, &log, "remove") == 0) {
            expect_net(&c, &log, "remove", "ok", -1);
            expect_net(&c, &log, "remove", "q", +1);
            CHECK(&c, rows_of(s, "q") == 1, "remove: q rows %d",
                rows_of(s, "q"));
        }
        wirelog_easy_close(s);
    }
    report(&c);
}

/* ---- plan-level cases --------------------------------------------------- */

typedef struct {
    int plus;
    int minus;
    uint32_t events;
} plan_events_t;

static void
on_plan_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    plan_events_t *e = (plan_events_t *)user_data;

    (void)row;
    if (strcmp(relation, "a") != 0 || ncols != 0)
        return;
    e->events++;
    if (diff > 0)
        e->plus += diff;
    else
        e->minus -= diff;
}

static uint32_t
rel_rows(wl_col_session_t *sess, const char *name)
{
    col_rel_t *r = session_find_rel(sess, name);
    return r ? r->nrows : UINT32_MAX;
}

/* Head a = `input` projected to zero columns, stepped through
 * col_stratum_step_with_delta with no delta observer installed, so each
 * change of presence is emitted by the direct path: +1 when a first holds,
 * nothing while it stays held, -1 when its last support is removed. */
static void
test_direct_emission(void)
{
    ctx_t c = { "direct emission of a zero-width head", 1, true, { 0 },
                false };
    wl_plan_op_t a_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_MAP, .project_count = 0 },
    };
    wl_plan_relation_t rels[] = { { .name = "a", .ops = a_ops,
                                    .op_count = 2 } };
    wl_plan_stratum_t st = { .relations = rels, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &st, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    plan_events_t ev = { 0 };
    int64_t one = 1, two = 2;

    if (wl_session_create(wl_backend_columnar(), &plan, 1, &session) != 0) {
        CHECK(&c, false, "create");
        report(&c);
        return;
    }
    wl_col_session_t *sess = COL_SESSION(session);
    wl_session_set_delta_cb(session, on_plan_delta, &ev);
    CHECK(&c, !sess->delta_observer, "a delta observer is installed");
    CHECK(&c, wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_insert(session, "input", &two, 1, 1) == 0, "insert");
    CHECK(&c, col_stratum_step_with_delta(&st, sess, 0) == 0
        && ev.plus == 1 && ev.minus == 0 && rel_rows(sess, "a") == 1,
        "insert: +%d -%d, a rows %u", ev.plus, ev.minus,
        rel_rows(sess, "a"));
    memset(&ev, 0, sizeof(ev));
    CHECK(&c, col_stratum_step_with_delta(&st, sess, 0) == 0
        && ev.events == 0, "unchanged: %u events", ev.events);
    CHECK(&c, wl_session_remove(session, "input", &one, 1, 1) == 0,
        "remove 1");
    memset(&ev, 0, sizeof(ev));
    CHECK(&c, col_stratum_step_with_delta(&st, sess, 0) == 0
        && ev.events == 0 && rel_rows(sess, "a") == 1,
        "one support left: %u events, a rows %u", ev.events,
        rel_rows(sess, "a"));
    CHECK(&c, wl_session_remove(session, "input", &two, 1, 1) == 0,
        "remove 2");
    memset(&ev, 0, sizeof(ev));
    CHECK(&c, col_stratum_step_with_delta(&st, sess, 0) == 0
        && ev.plus == 0 && ev.minus == 1 && rel_rows(sess, "a") == 0,
        "last support removed: +%d -%d, a rows %u", ev.plus, ev.minus,
        rel_rows(sess, "a"));
    wl_session_destroy(session);
    report(&c);
}

/* Heads a (zero-width) and b (a copy of input) in one stratum.  The step
 * after a baseline fails on b by reading a relation that does not exist;
 * the rollback must leave both heads as they were before the step, whether
 * the baseline left a holding its row or emptied, and the retry must then
 * announce a's change. */
static void
test_failed_step_rollback(bool empty_baseline)
{
    ctx_t c = { empty_baseline
                ? "rollback of an emptied zero-width head"
                : "rollback of a held zero-width head", 1, true, { 0 },
                false };
    wl_plan_op_t a_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_MAP, .project_count = 0 },
    };
    wl_plan_op_t b_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "missing" },
    };
    wl_plan_relation_t rels[] = {
        { .name = "a", .ops = a_ops, .op_count = 2 },
        { .name = "b", .ops = b_ops, .op_count = 1 },
    };
    wl_plan_stratum_t st = { .relations = rels, .relation_count = 2 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &st, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    plan_events_t ev = { 0 };
    int64_t one = 1, two = 2;

    if (wl_session_create(wl_backend_columnar(), &plan, 1, &session) != 0) {
        CHECK(&c, false, "create");
        report(&c);
        return;
    }
    wl_col_session_t *sess = COL_SESSION(session);
    wl_session_set_delta_cb(session, on_plan_delta, &ev);
    CHECK(&c, wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0 && rel_rows(sess, "a") == 1,
        "baseline: a rows %u", rel_rows(sess, "a"));
    if (empty_baseline)
        CHECK(&c, wl_session_remove(session, "input", &one, 1, 1) == 0
            && wl_session_step(session) == 0 && rel_rows(sess, "a") == 0,
            "emptied baseline: a rows %u", rel_rows(sess, "a"));
    uint32_t before = rel_rows(sess, "a");
    uint32_t b_before = rel_rows(sess, "b");

    CHECK(&c, wl_session_insert(session, "input", &two, 1, 1) == 0,
        "insert 2");
    rels[1].op_count = 2;
    memset(&ev, 0, sizeof(ev));
    CHECK(&c, wl_session_step(session) != 0, "the failing step succeeded");
    CHECK(&c, rel_rows(sess, "a") == before
        && rel_rows(sess, "b") == b_before,
        "after the failed step: a rows %u (want %u), b rows %u (want %u)",
        rel_rows(sess, "a"), before, rel_rows(sess, "b"), b_before);

    rels[1].op_count = 1;
    memset(&ev, 0, sizeof(ev));
    CHECK(&c, wl_session_step(session) == 0, "retry failed");
    CHECK(&c, rel_rows(sess, "a") == 1
        && ev.plus == (empty_baseline ? 1 : 0) && ev.minus == 0,
        "retry: a rows %u, +%d -%d", rel_rows(sess, "a"), ev.plus,
        ev.minus);
    wl_session_destroy(session);
    report(&c);
}

int
main(void)
{
    static const unsigned widths[] = { 1u, 2u, 4u };

    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    printf("test_delta_nullary_retract\n");
    for (size_t i = 0; i < sizeof(widths) / sizeof(widths[0]); i++) {
        unsigned w = widths[i];
        for (int nc = 0; nc < 2; nc++) {
            test_two_supports(w, nc != 0);
            test_dependent_unary("unfiltered nullary head, dependent p",
                unfiltered_src, w, nc != 0);
            test_dependent_unary("filtered nullary head, dependent p",
                filtered_src, w, nc != 0);
            test_negation_flip("unfiltered nullary head, !ok() flips",
                unfiltered_src, w, nc != 0);
            test_negation_flip("filtered nullary head, !ok() flips",
                filtered_src, w, nc != 0);
        }
    }
    test_direct_emission();
    test_failed_step_rollback(false);
    test_failed_step_rollback(true);
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
