/*
 * test_easy_delta_head_input.c - Issue #2091 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A rule head can also hold input: an inline fact such as `reach(1).`, or a
 * row the host inserts into it.  With a delta callback installed, a step used
 * to drop that input from the relation and publish it as a retraction, so the
 * model lost the row and everything derived from it.
 *
 * The delta stream is checked against the model it describes.  Each case
 * folds every published event into a set: a +1 must name a row the set does
 * not hold and a -1 one it does.  After every step the set must equal what
 * wirelog_easy_snapshot() emits, and the step's own events must be exactly
 * the ones listed.  Every case runs on one worker and on four.
 */

#include "wirelog/wirelog-easy.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name, workers)                                      \
        do {                                                     \
            tests_run++;                                         \
            printf("  [%d] %s (%u worker%s)", tests_run, name,   \
                (workers), (workers) == 1 ? "" : "s");           \
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

#define MAX_ROWS 32

/* Events for `reach`, a one-column relation. */
typedef struct {
    const char *relation;    /* the one-column relation followed */
    int64_t held[MAX_ROWS];  /* the set the stream has built so far */
    uint32_t nheld;
    int64_t step_row[MAX_ROWS]; /* this step's events, in arrival order */
    int32_t step_diff[MAX_ROWS];
    uint32_t nstep;
    char error[160];         /* first inconsistency, if any */
} stream_t;

static char message[256];

static int
held_index(const stream_t *st, int64_t v)
{
    for (uint32_t i = 0; i < st->nheld; i++)
        if (st->held[i] == v)
            return (int)i;
    return -1;
}

static void
on_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    stream_t *st = (stream_t *)user_data;

    if (strcmp(relation, st->relation) != 0 || st->error[0])
        return;
    if (ncols != 1 || st->nstep >= MAX_ROWS || st->nheld >= MAX_ROWS) {
        snprintf(st->error, sizeof(st->error), "unexpected event shape");
        return;
    }
    st->step_row[st->nstep] = row[0];
    st->step_diff[st->nstep] = diff;
    st->nstep++;

    int at = held_index(st, row[0]);
    if (diff == 1 && at < 0) {
        st->held[st->nheld++] = row[0];
    } else if (diff == -1 && at >= 0) {
        st->held[at] = st->held[--st->nheld];
    } else {
        snprintf(st->error, sizeof(st->error), "%s(%lld) %+d %s",
            relation, (long long)row[0], (int)diff,
            diff == 1 ? "for a row already published"
                      : "for a row never published");
    }
}

typedef struct {
    int64_t row[MAX_ROWS];
    uint32_t count;
} rows_t;

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    rows_t *rows = (rows_t *)user_data;

    (void)relation;
    (void)ncols;
    if (rows->count < MAX_ROWS)
        rows->row[rows->count] = row[0];
    rows->count++;
}

/* One expected event; `want` lists end with diff 0. */
typedef struct {
    int64_t row;
    int32_t diff;
} event_t;

/* Step, then require exactly @want as this step's events (in any order),
 * no inconsistency in the stream so far, and a snapshot equal both to
 * @model and to the set the stream has built. */
static const char *
step_expect(wirelog_easy_session_t *s, stream_t *st, const event_t *want,
    const int64_t *model, uint32_t nmodel, const char *when)
{
    rows_t rows;
    uint32_t nwant = 0;

    st->nstep = 0;
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        snprintf(message, sizeof(message), "%s: step failed", when);
        return message;
    }
    if (st->error[0]) {
        snprintf(message, sizeof(message), "%s: %s", when, st->error);
        return message;
    }
    while (want[nwant].diff != 0)
        nwant++;
    for (uint32_t w = 0; w < nwant; w++) {
        uint32_t seen = 0;
        for (uint32_t e = 0; e < st->nstep; e++)
            if (st->step_row[e] == want[w].row
                && st->step_diff[e] == want[w].diff)
                seen++;
        if (seen != 1) {
            snprintf(message, sizeof(message),
                "%s: %s(%lld) %+d published %u times, want once", when,
                st->relation, (long long)want[w].row, (int)want[w].diff,
                seen);
            return message;
        }
    }
    if (st->nstep != nwant) {
        snprintf(message, sizeof(message), "%s: %u events, want %u", when,
            st->nstep, nwant);
        return message;
    }

    memset(&rows, 0, sizeof(rows));
    if (wirelog_easy_snapshot(s, st->relation, collect, &rows)
        != WIRELOG_OK) {
        snprintf(message, sizeof(message), "%s: snapshot failed", when);
        return message;
    }
    if (rows.count != nmodel || rows.count != st->nheld) {
        snprintf(message, sizeof(message),
            "%s: snapshot has %u rows, stream %u, want %u", when, rows.count,
            st->nheld, nmodel);
        return message;
    }
    for (uint32_t m = 0; m < nmodel; m++) {
        uint32_t seen = 0;
        for (uint32_t r = 0; r < rows.count; r++)
            if (rows.row[r] == model[m])
                seen++;
        if (seen != 1 || held_index(st, model[m]) < 0) {
            snprintf(message, sizeof(message),
                "%s: %s(%lld) seen %u times in the snapshot, %s in the "
                "stream", when, st->relation, (long long)model[m], seen,
                held_index(st, model[m]) < 0 ? "absent" : "present");
            return message;
        }
    }
    return NULL;
}

/* A snapshot that evaluates commits a model without publishing it.  Take
 * that model as the stream's state, and require it to be @model. */
static const char *
snapshot_commits(wirelog_easy_session_t *s, stream_t *st,
    const int64_t *model, uint32_t nmodel, const char *when)
{
    rows_t rows;

    memset(&rows, 0, sizeof(rows));
    if (wirelog_easy_snapshot(s, st->relation, collect, &rows) != WIRELOG_OK
        || rows.count > MAX_ROWS) {
        snprintf(message, sizeof(message), "%s: snapshot failed", when);
        return message;
    }
    if (rows.count != nmodel) {
        snprintf(message, sizeof(message), "%s: snapshot has %u rows, want "
            "%u", when, rows.count, nmodel);
        return message;
    }
    for (uint32_t m = 0; m < nmodel; m++) {
        uint32_t seen = 0;
        for (uint32_t r = 0; r < rows.count; r++)
            if (rows.row[r] == model[m])
                seen++;
        if (seen != 1) {
            snprintf(message, sizeof(message), "%s: %s(%lld) seen %u times",
                when, st->relation, (long long)model[m], seen);
            return message;
        }
    }
    memcpy(st->held, rows.row, sizeof(rows.row[0]) * rows.count);
    st->nheld = rows.count;
    return NULL;
}

static const char *
insert1(wirelog_easy_session_t *s, const char *relation, int64_t a)
{
    return wirelog_easy_insert(s, relation, &a, 1) == WIRELOG_OK
           ? NULL : "insert failed";
}

static const char *
insert2(wirelog_easy_session_t *s, const char *relation, int64_t a, int64_t b)
{
    int64_t row[2] = { a, b };
    return wirelog_easy_insert(s, relation, row, 2) == WIRELOG_OK
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

/* `reach` is a rule head that also holds an inline fact. */
static const char *const SEEDED =
    ".decl edge(a: int64, b: int64)\n"
    ".decl reach(x: int64)\n"
    "reach(1).\n"
    "reach(y) :- reach(x), edge(x, y).\n";

/* `reach` is a rule head with no inline fact; the host adds rows to it. */
static const char *const HOST_SEEDED =
    ".decl src(x: int64)\n"
    ".decl edge(a: int64, b: int64)\n"
    ".decl reach(x: int64)\n"
    "reach(x) :- src(x).\n"
    "reach(y) :- reach(x), edge(x, y).\n";

static const event_t NONE[] = { { 0, 0 } };

/* `r` is a rule head defined by a rule that does not read `r`. */
static const char *const COPIED =
    ".decl e(x: int64)\n"
    ".decl r(x: int64)\n"
    "r(x) :- e(x).\n";

static wirelog_easy_session_t *
open_with_stream(const char *program, uint32_t workers, stream_t *st)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;

    memset(st, 0, sizeof(*st));
    st->relation = program == COPIED ? "r" : "reach";
    opts.num_workers = workers;
    if (wirelog_easy_open_opts(program, &opts, &s) != WIRELOG_OK)
        return NULL;
    if (wirelog_easy_set_delta_cb(s, on_delta, st) != WIRELOG_OK) {
        wirelog_easy_close(s);
        return NULL;
    }
    return s;
}

/* The inline fact is part of the first published model, derives from it,
 * stays across idle and unrelated steps, and its removal retracts it and
 * what only it derived. */
static void
test_inline_fact(uint32_t workers)
{
    static const event_t first[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const event_t more[] = { { 3, 1 }, { 0, 0 } };
    static const event_t gone[] = { { 1, -1 }, { 2, -1 }, { 3, -1 },
                                    { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    static const int64_t m123[] = { 1, 2, 3 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("an inline fact in a rule head is published once and kept",
        workers);
    s = open_with_stream(SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert2(s, "edge", 1, 2));
    TRY(step_expect(s, &st, first, m12, 2, "first step"));
    TRY(step_expect(s, &st, NONE, m12, 2, "idle step"));
    TRY(insert2(s, "edge", 2, 3));
    TRY(step_expect(s, &st, more, m123, 3, "extending step"));
    TRY(remove1(s, "reach", 1));
    TRY(step_expect(s, &st, gone, NULL, 0, "removing the fact"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* A host row in a rule head is published +1 with its consequences, and its
 * removal publishes -1 for it and for what only it derived. */
static void
test_host_row(uint32_t workers)
{
    static const event_t load[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const event_t add[] = { { 7, 1 }, { 8, 1 }, { 0, 0 } };
    static const event_t drop[] = { { 7, -1 }, { 8, -1 }, { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    static const int64_t m1278[] = { 1, 2, 7, 8 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("a host row in a rule head is published +1 and -1", workers);
    s = open_with_stream(HOST_SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(step_expect(s, &st, load, m12, 2, "load"));
    TRY(insert1(s, "reach", 7));
    TRY(insert2(s, "edge", 7, 8));
    TRY(step_expect(s, &st, add, m1278, 4, "host row"));
    TRY(step_expect(s, &st, NONE, m1278, 4, "idle step"));
    TRY(remove1(s, "reach", 7));
    TRY(step_expect(s, &st, drop, m12, 2, "host removal"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* Input that duplicates a derived row changes nothing visible: inserting it
 * publishes nothing, and neither does removing it while it is still
 * derived.  Removing a row that is only derived does not retract it either,
 * because the step derives it again. */
static void
test_rows_that_stay_derived(uint32_t workers)
{
    static const event_t load[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("input that duplicates a derived row publishes nothing", workers);
    s = open_with_stream(HOST_SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(step_expect(s, &st, load, m12, 2, "load"));
    TRY(insert1(s, "reach", 2));
    TRY(step_expect(s, &st, NONE, m12, 2, "duplicate input"));
    TRY(remove1(s, "reach", 2));
    TRY(step_expect(s, &st, NONE, m12, 2, "removing the duplicate"));
    TRY(remove1(s, "reach", 1));
    TRY(step_expect(s, &st, NONE, m12, 2, "removing a derived row"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* A host row inserted and removed again before any step was never
 * published, so neither event may reach the stream. */
static void
test_row_removed_before_its_step(uint32_t workers)
{
    static const event_t load[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("a host row removed before its step publishes nothing", workers);
    s = open_with_stream(HOST_SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(step_expect(s, &st, load, m12, 2, "load"));
    TRY(insert1(s, "reach", 9));
    TRY(remove1(s, "reach", 9));
    TRY(step_expect(s, &st, NONE, m12, 2, "insert then remove"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* After its removal is published, a host row inserted again is new input
 * and publishes as new once more. */
static void
test_host_row_reinserted(uint32_t workers)
{
    static const event_t load[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const event_t add[] = { { 7, 1 }, { 8, 1 }, { 0, 0 } };
    static const event_t drop[] = { { 7, -1 }, { 8, -1 }, { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    static const int64_t m1278[] = { 1, 2, 7, 8 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("a host row removed and inserted again is published again",
        workers);
    s = open_with_stream(HOST_SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(insert2(s, "edge", 7, 8));
    TRY(step_expect(s, &st, load, m12, 2, "load"));
    TRY(insert1(s, "reach", 7));
    TRY(step_expect(s, &st, add, m1278, 4, "host row"));
    TRY(remove1(s, "reach", 7));
    TRY(step_expect(s, &st, drop, m12, 2, "host removal"));
    TRY(insert1(s, "reach", 7));
    TRY(step_expect(s, &st, add, m1278, 4, "inserted again"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* A removal made while no callback is installed is still part of what the
 * next callback step publishes. */
static void
test_removal_without_callback(uint32_t workers)
{
    static const event_t load[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const event_t add[] = { { 7, 1 }, { 8, 1 }, { 0, 0 } };
    static const event_t drop[] = { { 7, -1 }, { 8, -1 }, { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    static const int64_t m1278[] = { 1, 2, 7, 8 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("a removal made without a callback is published by the next step",
        workers);
    s = open_with_stream(HOST_SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(insert2(s, "edge", 7, 8));
    TRY(step_expect(s, &st, load, m12, 2, "load"));
    TRY(insert1(s, "reach", 7));
    TRY(step_expect(s, &st, add, m1278, 4, "host row"));
    if (wirelog_easy_set_delta_cb(s, NULL, NULL) != WIRELOG_OK) {
        FAIL("clear delta_cb");
        goto done;
    }
    TRY(remove1(s, "reach", 7));
    if (wirelog_easy_set_delta_cb(s, on_delta, &st) != WIRELOG_OK) {
        FAIL("set delta_cb again");
        goto done;
    }
    TRY(step_expect(s, &st, drop, m12, 2, "step after the removal"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* A snapshot that evaluates commits the removal it saw, so a row the host
 * inserts again afterwards is new to the next step. */
static void
test_snapshot_commits_a_removal(uint32_t workers)
{
    static const event_t load[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const event_t add[] = { { 7, 1 }, { 8, 1 }, { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    static const int64_t m1278[] = { 1, 2, 7, 8 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("a snapshot commits a removal before the row comes back", workers);
    s = open_with_stream(HOST_SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(insert2(s, "edge", 7, 8));
    TRY(step_expect(s, &st, load, m12, 2, "load"));
    TRY(insert1(s, "reach", 7));
    TRY(step_expect(s, &st, add, m1278, 4, "host row"));
    TRY(remove1(s, "reach", 7));
    TRY(snapshot_commits(s, &st, m12, 2, "evaluating snapshot"));
    TRY(insert1(s, "reach", 7));
    TRY(step_expect(s, &st, add, m1278, 4, "inserted again"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* Removing a host row from a head whose rule does not read the head still
 * re-derives it: the row stays while a rule derives it, and goes once
 * nothing does. */
static void
test_removal_from_a_copied_head(uint32_t workers)
{
    static const event_t two[] = { { 2, 1 }, { 0, 0 } };
    static const event_t one[] = { { 1, 1 }, { 0, 0 } };
    static const event_t gone[] = { { 1, -1 }, { 0, 0 } };
    static const int64_t m2[] = { 2 };
    static const int64_t m12[] = { 1, 2 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("a removal from a head its rule does not read re-derives it",
        workers);
    s = open_with_stream(COPIED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "e", 2));
    TRY(step_expect(s, &st, two, m2, 1, "load"));
    TRY(insert1(s, "r", 2));
    TRY(step_expect(s, &st, NONE, m2, 1, "input a rule derives too"));
    TRY(insert1(s, "r", 1));
    TRY(insert1(s, "e", 1));
    TRY(step_expect(s, &st, one, m12, 2, "input and its derivation"));
    TRY(remove1(s, "r", 1));
    TRY(step_expect(s, &st, NONE, m12, 2, "removing the input"));
    TRY(remove1(s, "e", 1));
    TRY(step_expect(s, &st, gone, m2, 1, "removing the derivation"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* A removal of an older host row must not make a newer, unpublished one
 * look published. */
static void
test_older_row_removed_beside_a_newer(uint32_t workers)
{
    static const event_t load[] = { { 1, 1 }, { 2, 1 }, { 0, 0 } };
    static const event_t add[] = { { 7, 1 }, { 8, 1 }, { 0, 0 } };
    static const event_t swap[] = { { 7, -1 }, { 8, -1 }, { 9, 1 },
                                    { 0, 0 } };
    static const int64_t m12[] = { 1, 2 };
    static const int64_t m1278[] = { 1, 2, 7, 8 };
    static const int64_t m129[] = { 1, 2, 9 };
    stream_t st;
    wirelog_easy_session_t *s;

    TEST("removing an older host row keeps a newer one unpublished",
        workers);
    s = open_with_stream(HOST_SEEDED, workers, &st);
    if (!s) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(insert2(s, "edge", 7, 8));
    TRY(step_expect(s, &st, load, m12, 2, "load"));
    TRY(insert1(s, "reach", 7));
    TRY(step_expect(s, &st, add, m1278, 4, "older host row"));
    TRY(insert1(s, "reach", 9));
    TRY(remove1(s, "reach", 7));
    TRY(step_expect(s, &st, swap, m129, 3, "newer row, older removed"));
    PASS();
done:
    wirelog_easy_close(s);
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 4 };

    printf("test_easy_delta_head_input (Issue #2091)\n");
    for (uint32_t i = 0; i < sizeof(workers) / sizeof(workers[0]); i++) {
        test_inline_fact(workers[i]);
        test_host_row(workers[i]);
        test_rows_that_stay_derived(workers[i]);
        test_row_removed_before_its_step(workers[i]);
        test_host_row_reinserted(workers[i]);
        test_removal_without_callback(workers[i]);
        test_snapshot_commits_a_removal(workers[i]);
        test_removal_from_a_copied_head(workers[i]);
        test_older_row_removed_beside_a_newer(workers[i]);
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
