/*
 * test_easy_plain_step.c - Issue #1994 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A session driven by wirelog_easy_step() with no delta callback installed
 * re-derives every rule in full on a step with pending input.  It used to
 * do so on top of the rows the previous step derived, so each step appended
 * another copy of every derived row, a retraction never removed the row it
 * had derived, and a step with nothing pending re-derived everything again.
 *
 * Every check reads the model through wirelog_easy_snapshot() after a
 * completed step.  With nothing pending that snapshot emits the stable model
 * without evaluating, so it observes what the steps left behind rather than
 * repairing it; test_plain_step_rederives_every_rule in
 * test_extension_replay.c proves that by counting addon invocations around
 * such a snapshot.
 *
 * The SEEDED cases cover a relation that is a rule head and also holds
 * input -- an inline fact or a host row.  Discarding derived rows must keep
 * that input, on plain steps, on snapshots alone, and in the multi-worker
 * recursive path.
 */

#include "wirelog/wirelog-easy.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

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

#define MAX_ROWS 64

typedef struct {
    int64_t col[MAX_ROWS][2];
    uint32_t count;
    int overflowed;
} rows_t;

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    rows_t *rows = (rows_t *)user_data;
    (void)relation;
    if (rows->count >= MAX_ROWS) {
        rows->overflowed = 1;
        return;
    }
    rows->col[rows->count][0] = ncols > 0 ? row[0] : 0;
    rows->col[rows->count][1] = ncols > 1 ? row[1] : 0;
    rows->count++;
}

static char message[256];

/* Snapshot @relation and require exactly @nwant rows, each of @want once.
 * An overflow or a repeated row fails rather than reading as a match. */
static const char *
expect_rows(wirelog_easy_session_t *s, const char *relation,
    const int64_t (*want)[2], uint32_t nwant, const char *when)
{
    rows_t rows;

    memset(&rows, 0, sizeof(rows));
    if (wirelog_easy_snapshot(s, relation, collect, &rows) != WIRELOG_OK) {
        snprintf(message, sizeof(message), "%s: snapshot failed", when);
        return message;
    }
    if (rows.overflowed || rows.count != nwant) {
        snprintf(message, sizeof(message), "%s: %s has %u rows, want %u",
            when, relation, rows.overflowed ? MAX_ROWS + 1u : rows.count,
            nwant);
        return message;
    }
    for (uint32_t w = 0; w < nwant; w++) {
        uint32_t seen = 0;
        for (uint32_t r = 0; r < rows.count; r++)
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
step(wirelog_easy_session_t *s, const char *when)
{
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        snprintf(message, sizeof(message), "%s: step failed", when);
        return message;
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

static const char *
remove2(wirelog_easy_session_t *s, const char *relation, int64_t a, int64_t b)
{
    int64_t row[2] = { a, b };
    return wirelog_easy_remove(s, relation, row, 2) == WIRELOG_OK
           ? NULL : "remove failed";
}

#define TRY(expr)                   \
        do {                        \
            const char *why_ = (expr); \
            if (why_) {             \
                FAIL(why_);         \
                goto done;          \
            }                       \
        } while (0)

/* Two strata: `twice` reads only `ready`, `echo` only `other`. */
static const char *const TWO_STRATA =
    ".decl ready(id: int64)\n"
    ".decl other(k: int64)\n"
    ".decl twice(id: int64, v: int64)\n"
    ".decl echo(k: int64)\n"
    "twice(id, id * 2) :- ready(id).\n"
    "echo(k) :- other(k).\n";

static void
test_idle_steps_keep_one_copy(void)
{
    static const int64_t want[][2] = { { 1, 2 }, { 2, 4 }, { 3, 6 } };
    wirelog_easy_session_t *s = NULL;

    TEST("idle plain steps leave one copy of every derived row");
    if (wirelog_easy_open(TWO_STRATA, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    for (int64_t i = 1; i <= 3; i++)
        TRY(insert1(s, "ready", i));
    TRY(step(s, "load"));
    for (int i = 0; i < 4; i++)
        TRY(step(s, "idle"));
    TRY(expect_rows(s, "twice", want, 3, "after four idle steps"));
    PASS();
done:
    wirelog_easy_close(s);
}

static void
test_mutating_steps_replace_the_model(void)
{
    static const int64_t loaded[][2] = { { 1, 2 }, { 2, 4 }, { 3, 6 } };
    static const int64_t after_remove[][2] = { { 2, 4 }, { 3, 6 } };
    static const int64_t after_insert[][2] = {
        { 2, 4 }, { 3, 6 }, { 9, 18 }
    };
    static const int64_t one_echo[][2] = { { 7, 0 } };
    wirelog_easy_session_t *s = NULL;

    TEST("plain steps after inserts and removals hold exactly the model");
    if (wirelog_easy_open(TWO_STRATA, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    for (int64_t i = 1; i <= 3; i++)
        TRY(insert1(s, "ready", i));
    TRY(step(s, "load"));
    TRY(insert1(s, "other", 7));
    TRY(step(s, "insert other"));
    TRY(expect_rows(s, "twice", loaded, 3, "unrelated insert"));
    TRY(expect_rows(s, "echo", one_echo, 1, "unrelated insert"));
    TRY(remove1(s, "other", 7));
    TRY(step(s, "remove other"));
    TRY(expect_rows(s, "echo", NULL, 0, "retracted other"));
    TRY(expect_rows(s, "twice", loaded, 3, "retracted other"));
    TRY(remove1(s, "ready", 1));
    TRY(step(s, "remove ready"));
    TRY(expect_rows(s, "twice", after_remove, 2, "retracted ready"));
    TRY(insert1(s, "ready", 9));
    TRY(step(s, "insert ready"));
    TRY(expect_rows(s, "twice", after_insert, 3, "inserted ready"));
    PASS();
done:
    wirelog_easy_close(s);
}

static void
test_recursive_rederivation(void)
{
    static const char *const TC =
        ".decl edge(a: int64, b: int64)\n"
        ".decl path(a: int64, b: int64)\n"
        "path(a, b) :- edge(a, b).\n"
        "path(a, c) :- path(a, b), edge(b, c).\n";
    static const int64_t chain[][2] = {
        { 1, 2 }, { 2, 3 }, { 1, 3 }
    };
    static const int64_t extended[][2] = {
        { 1, 2 }, { 2, 3 }, { 3, 4 }, { 1, 3 }, { 2, 4 }, { 1, 4 }
    };
    static const int64_t cut[][2] = { { 1, 2 }, { 3, 4 } };
    wirelog_easy_session_t *s = NULL;

    TEST("recursive plain steps re-derive the closure without residue");
    if (wirelog_easy_open(TC, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    TRY(insert2(s, "edge", 1, 2));
    TRY(insert2(s, "edge", 2, 3));
    TRY(step(s, "load"));
    TRY(step(s, "idle"));
    TRY(expect_rows(s, "path", chain, 3, "chain"));
    TRY(insert2(s, "edge", 3, 4));
    TRY(step(s, "extend"));
    TRY(step(s, "idle"));
    TRY(expect_rows(s, "path", extended, 6, "extended"));
    TRY(remove2(s, "edge", 2, 3));
    TRY(step(s, "cut"));
    TRY(expect_rows(s, "path", cut, 2, "cut"));
    PASS();
done:
    wirelog_easy_close(s);
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

/* An insert made while a delta callback was installed records only the
 * strata it reaches.  If the callback is removed before the next step, that
 * step is a plain one and discards every derived relation, so it must also
 * re-derive every stratum, not just the recorded ones. */
static void
test_callback_removed_before_step(void)
{
    static const int64_t loaded[][2] = { { 1, 2 }, { 2, 4 }, { 3, 6 } };
    static const int64_t one_echo[][2] = { { 7, 0 } };
    wirelog_easy_session_t *s = NULL;

    TEST("a step after the delta callback is removed re-derives everything");
    if (wirelog_easy_open(TWO_STRATA, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    if (wirelog_easy_set_delta_cb(s, ignore_delta, NULL) != WIRELOG_OK) {
        FAIL("set_delta_cb");
        goto done;
    }
    for (int64_t i = 1; i <= 3; i++)
        TRY(insert1(s, "ready", i));
    TRY(step(s, "load with callback"));
    TRY(insert1(s, "other", 7));
    if (wirelog_easy_set_delta_cb(s, NULL, NULL) != WIRELOG_OK) {
        FAIL("clear delta_cb");
        goto done;
    }
    TRY(step(s, "first plain step"));
    TRY(expect_rows(s, "twice", loaded, 3, "after callback removal"));
    TRY(expect_rows(s, "echo", one_echo, 1, "after callback removal"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* The removal half of the same transition: a removal staged with a callback
 * leaves its retraction relation behind, which a plain step must not read
 * in place of the relation itself. */
static void
test_callback_removal_before_plain_step(void)
{
    static const char *const TC =
        ".decl edge(a: int64, b: int64)\n"
        ".decl path(a: int64, b: int64)\n"
        "path(a, b) :- edge(a, b).\n"
        "path(a, c) :- path(a, b), edge(b, c).\n";
    static const int64_t cut[][2] = { { 1, 2 }, { 3, 4 } };
    wirelog_easy_session_t *s = NULL;

    TEST("a removal staged with a callback is honoured by a plain step");
    if (wirelog_easy_open(TC, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    if (wirelog_easy_set_delta_cb(s, ignore_delta, NULL) != WIRELOG_OK) {
        FAIL("set_delta_cb");
        goto done;
    }
    TRY(insert2(s, "edge", 1, 2));
    TRY(insert2(s, "edge", 2, 3));
    TRY(insert2(s, "edge", 3, 4));
    TRY(step(s, "load with callback"));
    TRY(remove2(s, "edge", 2, 3));
    if (wirelog_easy_set_delta_cb(s, NULL, NULL) != WIRELOG_OK) {
        FAIL("clear delta_cb");
        goto done;
    }
    TRY(step(s, "first plain step"));
    TRY(expect_rows(s, "path", cut, 2, "after staged removal"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* `reach` is a rule head that also carries an inline fact.  A full
 * re-evaluation discards what `reach` derived; it must keep the fact. */
static const char *const SEEDED =
    ".decl edge(a: int64, b: int64)\n"
    ".decl reach(x: int64)\n"
    "reach(1).\n"
    "reach(y) :- reach(x), edge(x, y).\n";

typedef enum { SEED_PLAIN_STEP, SEED_SNAPSHOT_ONLY } seed_mode_t;

static const char *
seed_sequence(wirelog_easy_session_t *s, seed_mode_t mode)
{
    static const int64_t r12[][2] = { { 1, 0 }, { 2, 0 } };
    static const int64_t r123[][2] = { { 1, 0 }, { 2, 0 }, { 3, 0 } };
    static const int64_t r1[][2] = { { 1, 0 } };
    static const int64_t host[][2] = {
        { 1, 0 }, { 5, 0 }, { 6, 0 }
    };
    const char *why;

#define SEED_STEP(when)                                       \
        do {                                                  \
            if (mode != SEED_SNAPSHOT_ONLY                    \
                && (why = step(s, (when))) != NULL)           \
            return why;                                   \
        } while (0)
#define SEED_EXPECT(want, n, when)                            \
        do {                                                  \
            if ((why = expect_rows(s, "reach", (want), (n), (when))) \
                != NULL)                                      \
            return why;                                   \
        } while (0)

    if ((why = insert2(s, "edge", 1, 2)) != NULL)
        return why;
    SEED_STEP("first");
    SEED_EXPECT(r12, 2, "first");
    if ((why = insert2(s, "edge", 2, 3)) != NULL)
        return why;
    SEED_STEP("extend");
    SEED_EXPECT(r123, 3, "extend");
    if ((why = remove2(s, "edge", 1, 2)) != NULL)
        return why;
    SEED_STEP("cut");
    SEED_EXPECT(r1, 1, "cut keeps the fact");
    SEED_STEP("idle");
    SEED_EXPECT(r1, 1, "idle keeps the fact");
    /* A host row in the same relation is input too, and so is its removal. */
    if ((why = insert1(s, "reach", 5)) != NULL
        || (why = insert2(s, "edge", 5, 6)) != NULL)
        return why;
    SEED_STEP("host row");
    SEED_EXPECT(host, 3, "host row derives");
    if ((why = remove1(s, "reach", 5)) != NULL)
        return why;
    SEED_STEP("host removal");
    SEED_EXPECT(r1, 1, "host removal retracts");
#undef SEED_STEP
#undef SEED_EXPECT
    return NULL;
}

/* `reach` has no inline fact, so its only input is what the host inserts
 * into it once evaluation has registered it. */
static const char *const HOST_SEEDED =
    ".decl src(x: int64)\n"
    ".decl edge(a: int64, b: int64)\n"
    ".decl reach(x: int64)\n"
    "reach(x) :- src(x).\n"
    "reach(y) :- reach(x), edge(x, y).\n";

static const char *
host_seed_sequence(wirelog_easy_session_t *s, seed_mode_t mode)
{
    static const int64_t r12[][2] = { { 1, 0 }, { 2, 0 } };
    static const int64_t r1278[][2] = {
        { 1, 0 }, { 2, 0 }, { 7, 0 }, { 8, 0 }
    };
    const char *why;

    if ((why = insert1(s, "src", 1)) != NULL
        || (why = insert2(s, "edge", 1, 2)) != NULL)
        return why;
    if (mode != SEED_SNAPSHOT_ONLY && (why = step(s, "first")) != NULL)
        return why;
    if ((why = expect_rows(s, "reach", r12, 2, "first")) != NULL)
        return why;
    if ((why = insert1(s, "reach", 7)) != NULL
        || (why = insert2(s, "edge", 7, 8)) != NULL)
        return why;
    if (mode != SEED_SNAPSHOT_ONLY && (why = step(s, "host row")) != NULL)
        return why;
    if ((why = expect_rows(s, "reach", r1278, 4, "host row derives")) != NULL)
        return why;
    if ((why = remove1(s, "reach", 7)) != NULL)
        return why;
    if (mode != SEED_SNAPSHOT_ONLY
        && (why = step(s, "host removal")) != NULL)
        return why;
    return expect_rows(s, "reach", r12, 2, "host removal retracts");
}

static void
run_seed_case(const char *name, seed_mode_t mode, uint32_t workers,
    bool host_rows)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    const char *why;

    TEST(name);
    opts.num_workers = workers;
    if (wirelog_easy_open_opts(host_rows ? HOST_SEEDED : SEEDED, &opts, &s)
        != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    why = host_rows ? host_seed_sequence(s, mode) : seed_sequence(s, mode);
    if (why)
        FAIL(why);
    else
        PASS();
    wirelog_easy_close(s);
}

/* A host row removed while a delta callback is installed goes through the
 * incremental removal path; the next plain step must not bring it back.
 * With @plain_removal the callback is cleared first, so the plain removal
 * path takes the row out.  Either way the relation need not hold the row:
 * a delta-callback step can already have dropped it. */
static void
run_callback_host_row_case(const char *name, bool plain_removal)
{
    static const int64_t r12[][2] = { { 1, 0 }, { 2, 0 } };
    wirelog_easy_session_t *s = NULL;

    TEST(name);
    if (wirelog_easy_open(HOST_SEEDED, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    if (wirelog_easy_set_delta_cb(s, ignore_delta, NULL) != WIRELOG_OK) {
        FAIL("set_delta_cb");
        goto done;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(step(s, "load with callback"));
    TRY(insert1(s, "reach", 7));
    TRY(insert2(s, "edge", 7, 8));
    TRY(step(s, "host row with callback"));
    if (!plain_removal) {
        TRY(remove1(s, "reach", 7));
        TRY(step(s, "host removal with callback"));
    }
    if (wirelog_easy_set_delta_cb(s, NULL, NULL) != WIRELOG_OK) {
        FAIL("clear delta_cb");
        goto done;
    }
    if (plain_removal)
        TRY(remove1(s, "reach", 7));
    TRY(insert1(s, "src", 1));
    TRY(step(s, "plain step"));
    TRY(expect_rows(s, "reach", r12, 2, "after the plain step"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* The removal finds the fact in the relation, before any step, while a
 * callback is installed: the incremental path's own commit. */
static void
test_callback_removal_of_an_inline_fact(void)
{
    wirelog_easy_session_t *s = NULL;

    TEST("an inline fact removed with a callback stays removed");
    if (wirelog_easy_open(SEEDED, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    if (wirelog_easy_set_delta_cb(s, ignore_delta, NULL) != WIRELOG_OK) {
        FAIL("set_delta_cb");
        goto done;
    }
    TRY(remove1(s, "reach", 1));
    if (wirelog_easy_set_delta_cb(s, NULL, NULL) != WIRELOG_OK) {
        FAIL("clear delta_cb");
        goto done;
    }
    TRY(insert2(s, "edge", 1, 2));
    TRY(step(s, "first plain step"));
    TRY(step(s, "second plain step"));
    TRY(insert2(s, "edge", 2, 3));
    TRY(step(s, "re-deriving step"));
    TRY(expect_rows(s, "reach", NULL, 0, "after the removed fact"));
    PASS();
done:
    wirelog_easy_close(s);
}

/* Each insert is input in its own right: the relation holds both copies of
 * a row inserted twice, and each removal must take one copy out of the
 * input as well, or the next re-derivation restores it. */
static void
test_twice_inserted_host_row(void)
{
    static const int64_t r12[][2] = { { 1, 0 }, { 2, 0 } };
    wirelog_easy_session_t *s = NULL;

    TEST("a host row inserted twice is gone after two removals");
    if (wirelog_easy_open(HOST_SEEDED, &s) != WIRELOG_OK) {
        FAIL("open");
        return;
    }
    TRY(insert1(s, "src", 1));
    TRY(insert2(s, "edge", 1, 2));
    TRY(step(s, "load"));
    TRY(insert1(s, "reach", 7));
    TRY(insert1(s, "reach", 7));
    TRY(step(s, "two host rows"));
    TRY(remove1(s, "reach", 7));
    TRY(step(s, "first removal"));
    TRY(remove1(s, "reach", 7));
    TRY(step(s, "second removal"));
    TRY(insert1(s, "src", 1));
    TRY(step(s, "re-deriving step"));
    TRY(expect_rows(s, "reach", r12, 2, "after both removals"));
    PASS();
done:
    wirelog_easy_close(s);
}

int
main(void)
{
    printf("test_easy_plain_step (Issue #1994)\n");
    test_idle_steps_keep_one_copy();
    test_mutating_steps_replace_the_model();
    test_recursive_rederivation();
    test_callback_removed_before_step();
    test_callback_removal_before_plain_step();
    run_seed_case("inline facts and host rows survive plain steps",
        SEED_PLAIN_STEP, 1, false);
    run_seed_case("inline facts and host rows survive snapshots alone",
        SEED_SNAPSHOT_ONLY, 1, false);
    run_seed_case("inline facts survive plain steps on four workers",
        SEED_PLAIN_STEP, 4, false);
    run_seed_case("host rows in an unseeded rule head survive plain steps",
        SEED_PLAIN_STEP, 1, true);
    run_seed_case("host rows in an unseeded rule head survive snapshots",
        SEED_SNAPSHOT_ONLY, 1, true);
    run_seed_case("host rows in an unseeded rule head on four workers",
        SEED_PLAIN_STEP, 4, true);
    run_callback_host_row_case(
        "a host row removed with a callback stays removed", false);
    run_callback_host_row_case(
        "a host row removed after the callback stays removed", true);
    test_callback_removal_of_an_inline_fact();
    test_twice_inserted_host_row();
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
