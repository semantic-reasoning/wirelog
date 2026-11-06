/*
 * test_easy_nullary_remove.c - removing a zero-width row (#2231).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * wirelog_easy_remove(s, "flag", row, 0) returned WIRELOG_OK and changed
 * nothing: both session remove entry points treated a relation with no
 * columns as one whose schema was never set.  A `flag()` seeded by an
 * inline fact or inserted by the host therefore held for the rest of the
 * session, and so did every rule that read it.
 *
 * A zero-width row has a multiplicity like any other row: each removal
 * lowers it by one, and the row is gone once every copy is removed and no
 * rule derives it.  A removal at a nonzero width from a relation that holds,
 * or has held, zero-width rows is refused, as an insert at that width is.
 *
 * The scripted cases run on sessions opened with one, two and four
 * workers, with a delta callback (checking the diffs each step delivers)
 * and without one (checking snapshots only).  Their strata are small and
 * non-recursive; the worker count changes the session, not the strategy
 * they run.  The closure cases read `flag()` from recursive strata and,
 * without a callback, check that the step that sees the removal still runs
 * on two or more workers.
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

#define MAX_EVENTS 64

typedef struct {
    char rel[16];
    int32_t diff;
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

    (void)row;
    (void)ncols;
    if (log->count >= MAX_EVENTS) {
        log->overflow = true;
        return;
    }
    snprintf(log->ev[log->count].rel, sizeof(log->ev[0].rel), "%s",
        relation);
    log->ev[log->count++].diff = diff;
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

/* Rows of @rel in a snapshot, or -1 when the snapshot fails. */
static int
rows_of(wirelog_easy_session_t *s, const char *rel)
{
    snap_count = 0;
    return wirelog_easy_snapshot(s, rel, on_snap, NULL) != WIRELOG_OK
           ? -1 : (int)snap_count;
}

enum { OP_END, OP_INS, OP_RM, OP_STEP, OP_NET, OP_SNAP };

/* One scripted operation.  OP_INS and OP_RM pass a row of @ncols values
 * and expect @want as the return code.  OP_NET expects @want as the net
 * diff of @rel over the last step, with at most one event; it is checked
 * only with a delta callback.  OP_SNAP expects @want rows of @rel. */
typedef struct {
    int op;
    const char *rel;
    uint32_t ncols;
    int want;
} op_t;

#define INS(rel, n)      { OP_INS, rel, n, WIRELOG_OK }
#define RM(rel, n, want) { OP_RM, rel, n, want }
#define STEP             { OP_STEP, NULL, 0, 0 }
#define NET(rel, want)   { OP_NET, rel, 0, want }
#define SNAP(rel, want)  { OP_SNAP, rel, 0, want }
#define END              { OP_END, NULL, 0, 0 }

static void
run_script(const char *name, const char *src, const op_t *ops,
    uint32_t workers, bool callback)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    event_log_t log;
    char why[160] = "";

    memset(&log, 0, sizeof(log));
    tests_run++;
    printf("  [%d] %s (W=%u%s)", tests_run, name, workers,
        callback ? ", callback" : "");
    opts.num_workers = workers;
    opts.eager_build = true;
    if (wirelog_easy_open_opts(src, &opts, &s) != WIRELOG_OK) {
        snprintf(why, sizeof(why), "open");
        goto done;
    }
    if (callback && wirelog_easy_set_delta_cb(s, on_delta, &log)
        != WIRELOG_OK) {
        snprintf(why, sizeof(why), "set_delta_cb");
        goto done;
    }
    for (uint32_t i = 0; ops[i].op != OP_END; i++) {
        const op_t *o = &ops[i];
        int64_t row[1] = { 1 };
        int rc;

        switch (o->op) {
        case OP_INS:
        case OP_RM:
            rc = o->op == OP_INS
                ? wirelog_easy_insert(s, o->rel, row, o->ncols)
                : wirelog_easy_remove(s, o->rel, row, o->ncols);
            if (rc != o->want)
                snprintf(why, sizeof(why), "op %u: %s %s/%u rc %d, want %d",
                    i, o->op == OP_INS ? "insert" : "remove", o->rel,
                    o->ncols, rc, o->want);
            break;
        case OP_STEP:
            memset(&log, 0, sizeof(log));
            rc = wirelog_easy_step(s);
            if (rc != WIRELOG_OK)
                snprintf(why, sizeof(why), "op %u: step rc %d", i, rc);
            break;
        case OP_NET:
            if (callback) {
                int32_t sum = 0;
                uint32_t events = 0;
                for (uint32_t e = 0; e < log.count; e++)
                    if (strcmp(log.ev[e].rel, o->rel) == 0) {
                        sum += log.ev[e].diff;
                        events++;
                    }
                if (log.overflow || sum != o->want || events > 1)
                    snprintf(why, sizeof(why),
                        "op %u: %s net %d over %u events, want %d", i,
                        o->rel, (int)sum, events, o->want);
            }
            break;
        case OP_SNAP:
            rc = rows_of(s, o->rel);
            if (rc != o->want)
                snprintf(why, sizeof(why), "op %u: %s has %d rows, want %d",
                    i, o->rel, rc, o->want);
            break;
        default:
            snprintf(why, sizeof(why), "op %u: bad op", i);
            break;
        }
        if (why[0])
            break;
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

#define EXEC WIRELOG_ERR_EXEC

#define FACT_SRC ".decl flag()\n.decl p(x: int64)\nflag().\n" \
        "p(100) :- flag().\n"
#define HOST_SRC ".decl flag()\n.decl p(x: int64)\np(100) :- flag().\n"
#define RULE_SRC ".decl flag()\n.decl e(x: int64)\n.decl p(x: int64)\n" \
        "flag().\nflag() :- e(1).\np(100) :- flag().\n"
#define DERIVED_SRC ".decl ok()\n.decl e(x: int64)\n.decl q(x: int64)\n" \
        ".decl p(x: int64)\ne(1).\nok() :- e(1).\nq(x) :- e(x).\n" \
        "p(100) :- ok().\n"

/* An inline fact removed before the first step never holds. */
static const op_t remove_before_step[] = {
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", 0), SNAP("p", 0), END
};

/* An inline fact removed after a step retracts its consequence. */
static const op_t remove_fact[] = {
    STEP, NET("p", 1), SNAP("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", -1), SNAP("p", 0), END
};

static const op_t remove_host_row[] = {
    INS("flag", 0), STEP, NET("p", 1), SNAP("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", -1), SNAP("p", 0), END
};

/* Two copies, one inline and one from the host, take two removals. */
static const op_t remove_two_copies[] = {
    INS("flag", 0), STEP, NET("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", 0), SNAP("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", -1), SNAP("p", 0), END
};

static const op_t remove_two_host_copies[] = {
    INS("flag", 0), INS("flag", 0), STEP, NET("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", 0), SNAP("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", -1), SNAP("p", 0), END
};

/* Removing more copies than were inserted is not an error. */
static const op_t remove_twice[] = {
    RM("flag", 0, WIRELOG_OK), RM("flag", 0, WIRELOG_OK), STEP,
    SNAP("p", 0), END
};

/* A snapshot sees a removal without a step in between. */
static const op_t remove_between_snapshots[] = {
    SNAP("p", 1), RM("flag", 0, WIRELOG_OK), SNAP("p", 0), END
};

/* A removal at width one from a held or an emptied zero-width relation is
 * refused and changes nothing. */
static const op_t remove_wrong_width_held[] = {
    STEP, RM("flag", 1, EXEC), STEP, NET("p", 0), SNAP("p", 1), END
};

static const op_t remove_wrong_width_emptied[] = {
    STEP, RM("flag", 0, WIRELOG_OK), STEP, SNAP("p", 0),
    RM("flag", 1, EXEC), END
};

/* A relation nothing has given a width yet, here an input relation that
 * never held a row: both removals are accepted and do nothing. */
static const op_t remove_never_inserted[] = {
    RM("flag", 0, WIRELOG_OK), RM("flag", 1, WIRELOG_OK), STEP,
    SNAP("p", 0), END
};

/* `flag` is also the head of a rule that does not fire: its two input
 * copies take two removals, and only the second retracts it. */
static const op_t remove_rule_head_copies[] = {
    INS("flag", 0), STEP, NET("flag", 1), NET("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("flag", 0), NET("p", 0),
    SNAP("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("flag", -1), NET("p", -1),
    SNAP("p", 0), END
};

/* The input row and a rule both support `flag()`; it holds until both
 * supports are removed, in either order. */
static const op_t remove_input_then_support[] = {
    INS("e", 1), STEP, NET("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", 0), SNAP("p", 1),
    RM("e", 1, WIRELOG_OK), STEP, NET("p", -1), SNAP("p", 0), END
};

static const op_t remove_support_then_input[] = {
    INS("e", 1), STEP, NET("p", 1),
    RM("e", 1, WIRELOG_OK), STEP, NET("p", 0), SNAP("p", 1),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", -1), SNAP("p", 0), END
};

static const op_t remove_input_late_support[] = {
    STEP, NET("p", 1), INS("e", 1), STEP, NET("p", 0),
    RM("flag", 0, WIRELOG_OK), STEP, NET("p", 0), SNAP("p", 1),
    RM("e", 1, WIRELOG_OK), STEP, NET("p", -1), SNAP("p", 0), END
};

/* Removing a row that only a rule derives is accepted and keeps it, at
 * zero width as at width one.  A step separates the two removals: with a
 * delta callback, one step takes removals from one relation only. */
static const op_t remove_derived_only[] = {
    STEP, SNAP("p", 1), SNAP("q", 1),
    RM("ok", 0, WIRELOG_OK), STEP, NET("p", 0), SNAP("p", 1),
    RM("q", 1, WIRELOG_OK), STEP, NET("q", 0), SNAP("q", 1), END
};

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

/* Rows of t, or -1 when the snapshot fails or a row has the wrong width. */
static int
pairs_of(wirelog_easy_session_t *s)
{
    pairs_t got = { 0 };

    if (wirelog_easy_snapshot(s, "t", count_pairs, &got) != WIRELOG_OK
        || got.bad_width)
        return -1;
    return (int)got.count;
}

/* Step @s and, at two or more workers without a callback, require
 * @strategy on at least two workers.  The audit must be enabled before
 * the step and read before any snapshot, which resets it. */
static const char *
step_on(wirelog_easy_session_t *s, uint32_t workers, bool callback,
    const char *strategy, char *ran, size_t ran_size)
{
    wl_col_session_t *inner = COL_SESSION(wirelog_easy_testhook_inner(s));

    inner->tdd_audit.enabled = true;
    if (wirelog_easy_step(s) != WIRELOG_OK)
        return "step";
    if (workers < 2 || callback)
        return NULL;
    snprintf(ran, ran_size, " [ran %s on %u]",
        inner->tdd_audit.strategy ? inner->tdd_audit.strategy : "none",
        inner->tdd_audit.selected_workers);
    if (!inner->tdd_audit.strategy
        || strcmp(inner->tdd_audit.strategy, strategy) != 0
        || inner->tdd_audit.selected_workers < 2)
        return "strategy or worker count";
    return NULL;
}

/* Evaluate a closure over e(1,2), e(2,3), e(3,4) that reads `flag()`,
 * expect @before rows of t, remove the zero-width row and expect @after.
 * With @host the row comes from wirelog_easy_insert rather than an inline
 * fact.  @first and @second name the strategies of the two steps. */
static void
check_closure(const char *name, const char *src, bool host,
    uint32_t workers, bool callback, const char *first, int before,
    const char *second, int after)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    event_log_t log;
    const char *why = NULL;
    char ran[64] = "";
    int64_t unused = 0;
    int got = 0;

    memset(&log, 0, sizeof(log));
    tests_run++;
    printf("  [%d] %s (W=%u%s)", tests_run, name, workers,
        callback ? ", callback" : "");
    opts.num_workers = workers;
    opts.eager_build = true;
    if (wirelog_easy_open_opts(src, &opts, &s) != WIRELOG_OK) {
        why = "open";
        goto done;
    }
    if (callback && wirelog_easy_set_delta_cb(s, on_delta, &log)
        != WIRELOG_OK) {
        why = "set_delta_cb";
        goto done;
    }
    if (host && wirelog_easy_insert(s, "flag", &unused, 0) != WIRELOG_OK) {
        why = "insert";
        goto done;
    }
    why = step_on(s, workers, callback, first, ran, sizeof(ran));
    if (why)
        goto done;
    if ((got = pairs_of(s)) != before) {
        why = "wrong rows before the removal";
        goto done;
    }
    if (wirelog_easy_remove(s, "flag", &unused, 0) != WIRELOG_OK) {
        why = "remove";
        goto done;
    }
    why = step_on(s, workers, callback, second, ran, sizeof(ran));
    if (why)
        goto done;
    if ((got = pairs_of(s)) != after)
        why = "wrong rows after the removal";
done:
    if (s)
        wirelog_easy_close(s);
    if (why) {
        tests_failed++;
        printf("%s ... FAIL: %s (t has %d rows)\n", ran, why, got);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 2, 4 };

    printf("test_easy_nullary_remove (Issue #2231)\n");
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        for (int cb = 0; cb < 2; cb++) {
            uint32_t n = workers[w];
            bool c = cb != 0;

            run_script("remove an inline fact before the first step",
                FACT_SRC, remove_before_step, n, c);
            run_script("remove an inline fact", FACT_SRC, remove_fact, n,
                c);
            run_script("remove a host row", HOST_SRC, remove_host_row, n,
                c);
            run_script("remove an inline and a host copy", FACT_SRC,
                remove_two_copies, n, c);
            run_script("remove two host copies", HOST_SRC,
                remove_two_host_copies, n, c);
            run_script("remove more copies than were inserted", FACT_SRC,
                remove_twice, n, c);
            run_script("remove between two snapshots", FACT_SRC,
                remove_between_snapshots, n, c);
            run_script("remove at width one from a held row", FACT_SRC,
                remove_wrong_width_held, n, c);
            run_script("remove at width one after emptying", FACT_SRC,
                remove_wrong_width_emptied, n, c);
            run_script("remove from a relation that never held a row",
                HOST_SRC, remove_never_inserted, n, c);
            run_script("remove input copies of a rule head", RULE_SRC,
                remove_rule_head_copies, n, c);
            run_script("remove the input row, then the rule's support",
                RULE_SRC, remove_input_then_support, n, c);
            run_script("remove the rule's support, then the input row",
                RULE_SRC, remove_support_then_input, n, c);
            run_script("remove the input row after a late support",
                RULE_SRC, remove_input_late_support, n, c);
            run_script("remove rows that only a rule derives", DERIVED_SRC,
                remove_derived_only, n, c);

            check_closure("closure, flag() in both rules",
                ".decl flag()\nflag().\n" EDGES
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), e(y, z), flag().\n",
                false, n, c, "aligned", 6, "replicate", 0);
            check_closure("closure, flag() in the base rule",
                ".decl flag()\nflag().\n" EDGES
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), e(y, z).\n",
                false, n, c, "owner", 6, "owner", 0);
            check_closure("self-join closure with flag()",
                ".decl flag()\nflag().\n" EDGES
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), t(y, z), flag().\n",
                false, n, c, "bdx", 6, "replicate", 0);
            check_closure("two-sided closure with flag()",
                ".decl flag()\nflag().\n" EDGES
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), e(y, z), flag().\n"
                "t(x, z) :- e(x, y), t(y, z), flag().\n",
                false, n, c, "global_read", 6, "global_read", 0);
            check_closure("closure, host-inserted flag()",
                ".decl flag()\n" EDGES
                "t(x, y) :- e(x, y), flag().\n"
                "t(x, z) :- t(x, y), e(y, z), flag().\n",
                true, n, c, "aligned", 6, "replicate", 0);
            check_closure("closure, !flag() in the recursive rule",
                ".decl flag()\nflag().\n" EDGES
                "t(x, y) :- e(x, y).\n"
                "t(x, z) :- t(x, y), e(y, z), !flag().\n",
                false, n, c, "owner", 3, "owner", 6);
        }
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
