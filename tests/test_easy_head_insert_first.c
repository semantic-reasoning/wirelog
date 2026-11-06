/*
 * test_easy_head_insert_first.c - a host row in a rule head before the
 * first step (#2231).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Session creation registers the relations that can hold input before any
 * evaluation: those no rule derives, and rule heads with an inline fact.
 * Another rule head is registered when evaluation first runs its stratum,
 * so until then wirelog_easy_insert and wirelog_easy_remove on it found no
 * relation and returned WIRELOG_ERR_EXEC, for `flag() :- e(1).` as for a
 * unary or a recursive binary head.  After one step both worked.
 *
 * A rule head with a `.decl` and no compound or graph column now takes
 * host rows and removals before the first step as it does after it,
 * checked against its declared width.  Other heads are still refused
 * before their first evaluation: a head without a `.decl` gets its width
 * and column types from that evaluation, and the plan records no starting
 * schema for a compound or graph column.  The refused cases pin that the
 * refusal leaves the session usable.
 *
 * Every case runs on sessions opened with one, two and four workers, with
 * a delta callback (checking the diffs each step delivers) and without one
 * (checking snapshots).  The closure cases, without a callback, at two or
 * more workers, also require the strategy their steps ran.
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

enum { OP_END, OP_INS, OP_RM, OP_STEP, OP_NET, OP_SNAP, OP_STRAT };

/* One scripted operation.  OP_INS and OP_RM pass the row (@v, @v + 1)
 * truncated to @ncols values and expect @want as the return code.  OP_NET
 * expects @want as the net diff of @rel over the last step, with no more
 * events than rows changed (at most one for a net of 0); it is checked
 * only with a delta callback.  OP_SNAP expects @want rows of @rel.  OP_STRAT requires the last step to have run the
 * strategy @rel on at least two workers; it is checked only at two or
 * more workers without a callback, and must come before any snapshot,
 * which resets the audit. */
typedef struct {
    int op;
    const char *rel;
    uint32_t ncols;
    int64_t v;
    int want;
} op_t;

#define INS(rel, n, v)      { OP_INS, rel, n, v, WIRELOG_OK }
#define INSW(rel, n, want)  { OP_INS, rel, n, 1, want }
#define RM(rel, n, v)       { OP_RM, rel, n, v, WIRELOG_OK }
#define RMW(rel, n, want)   { OP_RM, rel, n, 1, want }
#define STEP                { OP_STEP, NULL, 0, 0, 0 }
#define NET(rel, want)      { OP_NET, rel, 0, 0, want }
#define SNAP(rel, want)     { OP_SNAP, rel, 0, 0, want }
#define STRAT(name)         { OP_STRAT, name, 0, 0, 0 }
#define END                 { OP_END, NULL, 0, 0, 0 }

static void
run_script(const char *name, const char *src, const op_t *ops,
    uint32_t workers, bool callback)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    wl_col_session_t *inner = NULL;
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
    inner = COL_SESSION(wirelog_easy_testhook_inner(s));
    for (uint32_t i = 0; ops[i].op != OP_END; i++) {
        const op_t *o = &ops[i];
        int64_t row[2] = { o->v, o->v + 1 };
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
            inner->tdd_audit.enabled = true;
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
                if (log.overflow || sum != o->want
                    || events > (uint32_t)(o->want < 0 ? -o->want : o->want)
                    + (o->want == 0))
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
        case OP_STRAT:
            if (workers > 1 && !callback
                && (!inner->tdd_audit.strategy
                || strcmp(inner->tdd_audit.strategy, o->rel) != 0
                || inner->tdd_audit.selected_workers < 2))
                snprintf(why, sizeof(why), "op %u: ran %s on %u, want %s",
                    i, inner->tdd_audit.strategy
                    ? inner->tdd_audit.strategy : "none",
                    inner->tdd_audit.selected_workers, o->rel);
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

#define FLAG_SRC ".decl flag()\n.decl e(x: int64)\n.decl p(x: int64)\n" \
        "flag() :- e(1).\np(100) :- flag().\n"
#define UNARY_SRC ".decl e(x: int64)\n.decl q(x: int64)\n" \
        ".decl p(x: int64)\nq(x) :- e(x).\np(x) :- q(x).\n"
#define EDGES \
        ".decl e(x: int64, y: int64)\n.decl t(x: int64, y: int64)\n" \
        "e(1, 2).\ne(2, 3).\ne(3, 4).\n"
#define CLOSURE_SRC EDGES \
        "t(x, y) :- e(x, y).\nt(x, z) :- t(x, y), e(y, z).\n"
#define GUARD_SRC ".decl flag()\n" EDGES "flag() :- e(9, 9).\n" \
        "t(x, y) :- e(x, y), flag().\n" \
        "t(x, z) :- t(x, y), e(y, z), flag().\n"
#define UNDECL_FLAG_SRC ".decl e(x: int64)\n.decl p(x: int64)\n" \
        "flag() :- e(1).\np(100) :- flag().\n"
#define UNDECL_SYM_SRC ".decl e(x: symbol)\ne(\"a\").\nr(x) :- e(x).\n"
#define DECL_SYM_SRC ".decl e(x: symbol)\n.decl r(x: symbol)\ne(\"a\").\n" \
        "r(x) :- e(x).\n"
#define COMPOUND_SRC ".decl e(id: int64, h: pair/2 side)\n" \
        ".decl r(id: int64, h: pair/2 side)\nr(i, h) :- e(i, h).\n"

/* A zero-width row inserted before the first step holds, and its removal
 * retracts it. */
static const op_t nullary_insert[] = {
    INS("flag", 0, 1), STEP, NET("p", 1), SNAP("p", 1),
    RM("flag", 0, 1), STEP, NET("p", -1), SNAP("p", 0), END
};

static const op_t nullary_two_copies[] = {
    INS("flag", 0, 1), INS("flag", 0, 1), STEP, NET("p", 1),
    RM("flag", 0, 1), STEP, NET("p", 0), SNAP("p", 1),
    RM("flag", 0, 1), STEP, NET("p", -1), SNAP("p", 0), END
};

/* The host row and the rule both support `flag()`. */
static const op_t nullary_with_support[] = {
    INS("flag", 0, 1), INS("e", 1, 1), STEP, NET("p", 1),
    RM("flag", 0, 1), STEP, NET("p", 0), SNAP("p", 1),
    RM("e", 1, 1), STEP, NET("p", -1), SNAP("p", 0), END
};

/* Before the first step, an insert or removal at a width the `.decl`
 * contradicts is refused and a removal at the declared width changes
 * nothing. */
static const op_t nullary_wrong_width[] = {
    INSW("flag", 1, EXEC), RMW("flag", 1, EXEC), RM("flag", 0, 1), STEP,
    NET("p", 0), SNAP("p", 0), INS("flag", 0, 1), STEP, NET("p", 1),
    SNAP("p", 1), END
};

/* A removal before any insert or step finds the head empty. */
static const op_t nullary_remove_first[] = {
    RM("flag", 0, 1), RMW("flag", 1, EXEC), STEP, NET("p", 0),
    SNAP("p", 0), END
};

static const op_t unary_remove_first[] = {
    RM("q", 1, 7), RMW("q", 2, EXEC), STEP, NET("p", 0), SNAP("p", 0),
    END
};

/* A snapshot evaluates a row inserted before any step. */
static const op_t nullary_snapshot_only[] = {
    INS("flag", 0, 1), SNAP("p", 1), RM("flag", 0, 1), SNAP("p", 0), END
};

/* The input relation e has a declared width too: a removal at another
 * width before its first row is refused, as an insert is. */
static const op_t unary_insert[] = {
    RMW("e", 2, EXEC), INSW("e", 2, EXEC),
    RMW("q", 2, EXEC), INSW("q", 2, EXEC), RM("q", 1, 7), INS("q", 1, 7),
    STEP, NET("p", 1), SNAP("p", 1),
    RM("q", 1, 7), STEP, NET("p", -1), SNAP("p", 0), END
};

/* t(0, 1) inserted before the step extends the closure of e by four
 * rows: (0, 1) and its paths to 2, 3 and 4. */
static const op_t closure_insert[] = {
    INS("t", 2, 0), STEP, STRAT("owner"), NET("t", 10), SNAP("t", 10),
    RM("t", 2, 0), STEP, SNAP("t", 6), END
};

/* `flag()` inserted before the step guards a closure. */
static const op_t guarded_closure[] = {
    INS("flag", 0, 1), STEP, STRAT("aligned"), SNAP("t", 6),
    RM("flag", 0, 1), STEP, STRAT("replicate"), SNAP("t", 0), END
};

/* A refused insert at the wrong width leaves no trace: the head still
 * takes zero-width rows, after the first step as before it. */
static const op_t nullary_refused_after_step[] = {
    STEP, INSW("flag", 1, EXEC), INS("flag", 0, 1), STEP, NET("p", 1),
    SNAP("p", 1), END
};

/* An undeclared head still refuses a row before its first evaluation, at
 * any width, and the session keeps working; after the step the head takes
 * the row. */
static const op_t undeclared_nullary[] = {
    INSW("flag", 1, EXEC), INSW("flag", 0, EXEC), STEP, SNAP("p", 0),
    INS("flag", 0, 1), STEP, SNAP("p", 1), END
};

/* A declared symbol head takes a host row beside the one its rule
 * derives; its placeholder carries the declared column type, so the
 * merge of a multi-worker step accepts the rule's symbol rows. */
static const op_t declared_symbol[] = {
    INS("r", 1, 1000), STEP, NET("r", 2), SNAP("r", 2),
    RM("r", 1, 1000), STEP, NET("r", -1), SNAP("r", 1), STEP,
    SNAP("r", 1), END
};

/* The plan records no starting schema for a head with a compound
 * column, so it is refused before its first evaluation like an
 * undeclared head. */
static const op_t declared_compound[] = {
    INSW("r", 2, EXEC), RMW("r", 2, EXEC), STEP, SNAP("r", 0), END
};

static const op_t undeclared_symbol[] = {
    INSW("r", 1, EXEC), STEP, SNAP("r", 1), STEP, SNAP("r", 1), END
};

int
main(void)
{
    static const uint32_t workers[] = { 1, 2, 4 };

    printf("test_easy_head_insert_first (Issue #2231)\n");
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        for (int cb = 0; cb < 2; cb++) {
            uint32_t n = workers[w];
            bool c = cb != 0;

            run_script("zero-width row before the first step", FLAG_SRC,
                nullary_insert, n, c);
            run_script("two zero-width copies before the first step",
                FLAG_SRC, nullary_two_copies, n, c);
            run_script("zero-width row beside its rule's support",
                FLAG_SRC, nullary_with_support, n, c);
            run_script("wrong width before the first step", FLAG_SRC,
                nullary_wrong_width, n, c);
            run_script("zero-width removal before anything else",
                FLAG_SRC, nullary_remove_first, n, c);
            run_script("unary removal before anything else", UNARY_SRC,
                unary_remove_first, n, c);
            run_script("refused insert after the first step", FLAG_SRC,
                nullary_refused_after_step, n, c);
            run_script("zero-width row, snapshots only", FLAG_SRC,
                nullary_snapshot_only, n, c);
            run_script("unary head before the first step", UNARY_SRC,
                unary_insert, n, c);
            run_script("recursive head before the first step",
                CLOSURE_SRC, closure_insert, n, c);
            run_script("zero-width guard before the first step",
                GUARD_SRC, guarded_closure, n, c);
            run_script("undeclared zero-width head is refused",
                UNDECL_FLAG_SRC, undeclared_nullary, n, c);
            run_script("undeclared symbol head is refused",
                UNDECL_SYM_SRC, undeclared_symbol, n, c);
            run_script("declared symbol head before the first step",
                DECL_SYM_SRC, declared_symbol, n, c);
            run_script("declared compound head is refused", COMPOUND_SRC,
                declared_compound, n, c);
        }
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
