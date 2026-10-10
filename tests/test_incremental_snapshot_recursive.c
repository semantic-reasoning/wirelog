/*
 * test_incremental_snapshot_recursive.c - Issue #2114: snapshots after an
 * incremental insert into a program with recursion.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A snapshot after an incremental insert -- col_session_insert_incremental(),
 * or a compound made with wl_session_make_compound() -- used to re-evaluate
 * the strata the inserted relation reaches with the new rows seeded as a
 * delta that every later iteration kept reading, so a recursive rule whose
 * changed atom precedes the recursive one lost rows: with
 * a(x, z) :- e(x, y), a(y, z), inserting e(2,3) after e(1,2) never derived
 * a(1,3).  Such a snapshot now finds the new derivations with
 * occurrence-bound instances (wl_columnar_eval_tdd_insert_stratum).
 *
 * Each case evaluates a program over its old rows with a snapshot, adds the
 * new rows the incremental way, and requires every relation of the next two
 * snapshots to equal a fresh evaluation over old and new rows together, in
 * sessions created with one worker and with four.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
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

#define MAX_ROWS 64
#define MAX_TUPLES 512

typedef struct {
    const char *name;
    uint32_t nrows;
    int64_t row[MAX_ROWS][2];
} data_t;

typedef struct {
    const char *name;
    bool compound; /* new rows go in as __compound_f_2 compounds of e */
    const char *src;
    data_t old_rows[3];
    uint32_t nold;
    data_t new_rows;
} case_t;

/* Every tuple a snapshot emits, as (relation, a, b). */
typedef struct {
    struct {
        char rel[32];
        int64_t a, b;
    } t[MAX_TUPLES];
    uint32_t count;
    bool overflow;
} model_t;

static char message[256];

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    model_t *m = (model_t *)user_data;
    if (m->count >= MAX_TUPLES) {
        m->overflow = true;
        return;
    }
    snprintf(m->t[m->count].rel, sizeof(m->t[m->count].rel), "%s", relation);
    m->t[m->count].a = ncols > 0 ? row[0] : 0;
    m->t[m->count].b = ncols > 1 ? row[1] : 0;
    m->count++;
}

/* The tuples of relations a test reads: the side relation of a compound
 * carries handles that differ between sessions, so it is left out. */
static bool
compared(const char *rel)
{
    return strncmp(rel, "__compound_", 11) != 0;
}

static uint32_t
occurrences(const model_t *m, const char *rel, int64_t a, int64_t b)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < m->count; i++)
        if (strcmp(m->t[i].rel, rel) == 0 && m->t[i].a == a
            && m->t[i].b == b)
            n++;
    return n;
}

static const char *
same_model(const model_t *got, const model_t *want, const char *when)
{
    if (got->overflow || want->overflow)
        return "model too large for the test";
    for (uint32_t i = 0; i < want->count; i++) {
        if (!compared(want->t[i].rel))
            continue;
        uint32_t g = occurrences(got, want->t[i].rel, want->t[i].a,
                want->t[i].b);
        if (g != 1) {
            snprintf(message, sizeof(message), "%s: %s(%lld,%lld) seen %u "
                "times, a fresh evaluation once", when, want->t[i].rel,
                (long long)want->t[i].a, (long long)want->t[i].b, g);
            return message;
        }
    }
    for (uint32_t i = 0; i < got->count; i++) {
        if (!compared(got->t[i].rel))
            continue;
        if (occurrences(want, got->t[i].rel, got->t[i].a, got->t[i].b) != 1) {
            snprintf(message, sizeof(message), "%s: %s(%lld,%lld) is not in "
                "a fresh evaluation", when, got->t[i].rel,
                (long long)got->t[i].a, (long long)got->t[i].b);
            return message;
        }
    }
    return NULL;
}

static int
open_session(const char *src, uint32_t workers, wl_plan_t **plan,
    wl_session_t **sess)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);

    *plan = NULL;
    *sess = NULL;
    if (!prog)
        return -1;
    plan_fixture_hold(prog);
    /* The planning passes the session pipeline applies. */
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    if (wl_plan_from_program(prog, plan) != 0)
        return -1;
    if (wl_session_create(wl_backend_columnar(), *plan, workers, sess) != 0)
        return -1;
    return wl_session_load_facts(*sess, prog);
}

/* Add @d: plainly, incrementally, or as compounds of e. */
static int
add_rows(wl_session_t *s, const data_t *d, bool incremental, bool compound)
{
    for (uint32_t r = 0; r < d->nrows; r++) {
        int rc;
        if (compound) {
            wirelog_compound_arg_t args[2] = {
                { WIRELOG_TYPE_INT64, d->row[r][0] },
                { WIRELOG_TYPE_INT64, d->row[r][1] },
            };
            uint64_t handle = 0;
            rc = wl_session_make_compound(s, "f", 2, args, &handle);
        } else if (incremental) {
            rc = col_session_insert_incremental(s, d->name, d->row[r], 1, 2);
        } else {
            rc = wl_session_insert(s, d->name, d->row[r], 1, 2);
        }
        if (rc != 0)
            return rc;
    }
    return 0;
}

static const char *
run_case(const case_t *c, uint32_t workers)
{
    wl_plan_t *plan = NULL, *oplan = NULL;
    wl_session_t *s = NULL, *oracle = NULL;
    model_t *got = calloc(1, sizeof(*got));
    model_t *want = calloc(1, sizeof(*want));
    const char *why = NULL;

    if (!got || !want) {
        why = "memory";
        goto done;
    }
    if (open_session(c->src, workers, &oplan, &oracle) != 0) {
        why = "oracle session";
        goto done;
    }
    for (uint32_t d = 0; d < c->nold; d++)
        if (add_rows(oracle, &c->old_rows[d], false, false) != 0) {
            why = "oracle insert";
            goto done;
        }
    if (add_rows(oracle, &c->new_rows, false, c->compound) != 0
        || wl_session_snapshot(oracle, collect, want) != 0) {
        why = "oracle evaluation";
        goto done;
    }
    if (open_session(c->src, workers, &plan, &s) != 0) {
        why = "session";
        goto done;
    }
    for (uint32_t d = 0; d < c->nold; d++)
        if (add_rows(s, &c->old_rows[d], false, false) != 0) {
            why = "insert";
            goto done;
        }
    {
        model_t *first = calloc(1, sizeof(*first));
        int rc = first ? wl_session_snapshot(s, collect, first) : -1;
        free(first);
        if (rc != 0) {
            why = "first snapshot";
            goto done;
        }
    }
    if (add_rows(s, &c->new_rows, true, c->compound) != 0) {
        why = "incremental insert";
        goto done;
    }
    for (int round = 0; round < 2 && !why; round++) {
        memset(got, 0, sizeof(*got));
        if (wl_session_snapshot(s, collect, got) != 0) {
            why = "snapshot after the insert failed";
            break;
        }
        why = same_model(got, want, round == 0 ? "first snapshot after"
                : "second snapshot after");
    }
done:
    if (s)
        wl_session_destroy(s);
    if (oracle)
        wl_session_destroy(oracle);
    wl_plan_free(plan);
    wl_plan_free(oplan);
    free(got);
    free(want);
    return why;
}

#define F_DECL ".decl __compound_f_2(handle: int64, a0: int64, a1: int64)\n"

static const case_t CASES[] = {
    /* Shape A: the changed atom precedes the recursive one. */
    { "right-recursive closure (shape A)", false,
      ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n",
      { { "e", 1, { { 1, 2 } } } }, 1, { "e", 1, { { 2, 3 } } } },
    { "right-recursive closure, new first edge (shape A')", false,
      ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n",
      { { "e", 1, { { 2, 3 } } } }, 1, { "e", 1, { { 1, 2 } } } },
    /* Shape B: a base rule joining the changed relation with itself. */
    { "base rule joining its input twice (shape B)", false,
      ".decl e(x: int64, y: int64)\n.decl s(x: int64, y: int64)\n"
      ".decl a(x: int64, y: int64)\n"
      "a(x, z) :- e(x, y), e(y, z).\na(x, z) :- a(x, y), s(y, z).\n",
      { { "e", 1, { { 1, 2 } } }, { "s", 1, { { 3, 4 } } } }, 2,
      { "e", 1, { { 2, 3 } } } },
    /* Shape C: a changed atom beside two recursive atoms. */
    { "rule with two recursive atoms (shape C)", false,
      ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
      ".decl a(x: int64, y: int64)\n"
      "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, w), a(w, z).\n",
      { { "f", 2, { { 2, 3 }, { 3, 4 } } }, { "e", 1, { { 9, 9 } } } }, 2,
      { "e", 1, { { 1, 2 } } } },
    /* New rows joining old and new rows across a closure. */
    { "closure over overlapping rows", false,
      ".decl e(x: int64, y: int64)\n.decl tc(x: int64, y: int64)\n"
      "tc(x, y) :- e(x, y).\ntc(x, z) :- tc(x, y), tc(y, z).\n",
      { { "e", 4, { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 } } } }, 1,
      { "e", 3, { { 2, 3 }, { 4, 5 }, { 6, 7 } } } },
    /* Two strata: the second reads the first's head and the changed
     * relation itself, so both are its changed relations. */
    { "recursion feeding a downstream join", false,
      ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      ".decl b(x: int64, y: int64)\n"
      "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n"
      "b(x, z) :- a(x, y), e(y, z).\n",
      { { "e", 2, { { 1, 2 }, { 3, 4 } } } }, 1,
      { "e", 1, { { 2, 3 } } } },
    /* Mutual recursion. */
    { "mutual recursion", false,
      ".decl e(x: int64, y: int64)\n.decl p(x: int64, y: int64)\n"
      ".decl q(x: int64, y: int64)\n"
      "p(x, y) :- e(x, y).\np(x, z) :- q(x, y), e(y, z).\n"
      "q(x, y) :- p(x, y).\n",
      { { "e", 1, { { 1, 2 } } } }, 1, { "e", 1, { { 2, 3 } } } },
    /* Shape A over compound arguments, through the public API. */
    { "right-recursive closure over compounds", true,
      F_DECL ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      "e(x, y) :- __compound_f_2(H, x, y).\n"
      "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n",
      { { "e", 0, { { 0, 0 } } } }, 0, { "f", 2, { { 2, 3 }, { 1, 2 } } } },
};

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

/* A removal staged with a delta callback installed, the callback cleared,
 * then an incremental insert: the insert route cannot evaluate the removal,
 * so the snapshot must evaluate in full. */
static const char *
staged_removal_then_insert(uint32_t workers)
{
    static const char *const src =
        ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
        "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n";
    static const data_t old_rows = { "e", 2, { { 1, 2 }, { 2, 3 } } };
    static const data_t kept_rows = { "e", 1, { { 1, 2 } } };
    static const data_t new_rows = { "e", 1, { { 3, 4 } } };
    wl_plan_t *plan = NULL, *oplan = NULL;
    wl_session_t *s = NULL, *oracle = NULL;
    model_t *got = calloc(1, sizeof(*got));
    model_t *want = calloc(1, sizeof(*want));
    const char *why = NULL;

    if (!got || !want || open_session(src, workers, &oplan, &oracle) != 0
        || add_rows(oracle, &kept_rows, false, false) != 0
        || add_rows(oracle, &new_rows, false, false) != 0
        || wl_session_snapshot(oracle, collect, want) != 0
        || open_session(src, workers, &plan, &s) != 0
        || add_rows(s, &old_rows, false, false) != 0
        || wl_session_snapshot(s, collect, got) != 0) {
        why = "setup";
        goto done;
    }
    wl_session_set_delta_cb(s, ignore_delta, NULL);
    if (wl_session_remove(s, "e", old_rows.row[1], 1, 2) != 0) {
        why = "staged removal";
        goto done;
    }
    wl_session_set_delta_cb(s, NULL, NULL);
    if (add_rows(s, &new_rows, true, false) != 0) {
        why = "incremental insert";
        goto done;
    }
    memset(got, 0, sizeof(*got));
    if (wl_session_snapshot(s, collect, got) != 0) {
        why = "snapshot failed";
        goto done;
    }
    why = same_model(got, want, "snapshot after the removal and insert");
done:
    if (s)
        wl_session_destroy(s);
    if (oracle)
        wl_session_destroy(oracle);
    wl_plan_free(plan);
    wl_plan_free(oplan);
    free(got);
    free(want);
    return why;
}

/* A failure after the insert route has updated some heads must not leave
 * them in the model.  A refusal by the bound runner is redone in full in
 * the same snapshot; any other failure is returned, and the session keeps
 * asking for a full evaluation, so the next snapshot is right. */
static int inject_rc;

static int
fail_after_merge(const wl_plan_stratum_t *sp)
{
    (void)sp;
    return inject_rc;
}

static const char *
failure_after_update(int rc_injected, uint32_t workers)
{
    static const case_t c = {
        "two strata", false,
        ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
        ".decl b(x: int64, y: int64)\n"
        "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n"
        "b(x, z) :- a(x, y), e(y, z).\n",
        { { "e", 2, { { 1, 2 }, { 3, 4 } } } }, 1,
        { "e", 1, { { 2, 3 } } }
    };
    wl_plan_t *plan = NULL, *oplan = NULL;
    wl_session_t *s = NULL, *oracle = NULL;
    model_t *got = calloc(1, sizeof(*got));
    model_t *want = calloc(1, sizeof(*want));
    const char *why = NULL;
    int rc;

    if (!got || !want || open_session(c.src, workers, &oplan, &oracle) != 0
        || add_rows(oracle, &c.old_rows[0], false, false) != 0
        || add_rows(oracle, &c.new_rows, false, false) != 0
        || wl_session_snapshot(oracle, collect, want) != 0
        || open_session(c.src, workers, &plan, &s) != 0
        || add_rows(s, &c.old_rows[0], false, false) != 0
        || wl_session_snapshot(s, collect, got) != 0
        || add_rows(s, &c.new_rows, true, false) != 0) {
        why = "setup";
        goto done;
    }
    inject_rc = rc_injected;
    wl_columnar_eval_tdd_insert_test_after_merge = fail_after_merge;
    memset(got, 0, sizeof(*got));
    rc = wl_session_snapshot(s, collect, got);
    wl_columnar_eval_tdd_insert_test_after_merge = NULL;
    if (rc_injected == EINVAL) {
        /* Redone in full within the same snapshot. */
        if (rc != 0) {
            why = "a refusal after the update was not redone in full";
            goto done;
        }
        why = same_model(got, want, "snapshot with the refusal");
        goto done;
    }
    if (rc != rc_injected || !COL_SESSION(s)->pending_full_input_eval) {
        why = "the failure was not returned with a full evaluation pending";
        goto done;
    }
    memset(got, 0, sizeof(*got));
    if (wl_session_snapshot(s, collect, got) != 0) {
        why = "the retry failed";
        goto done;
    }
    why = same_model(got, want, "retry after the failure");
done:
    wl_columnar_eval_tdd_insert_test_after_merge = NULL;
    if (s)
        wl_session_destroy(s);
    if (oracle)
        wl_session_destroy(oracle);
    wl_plan_free(plan);
    wl_plan_free(oplan);
    free(got);
    free(want);
    return why;
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 4 };

    printf("test_incremental_snapshot_recursive (Issue #2114)\n");
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        for (uint32_t i = 0; i < sizeof(CASES) / sizeof(CASES[0]); i++) {
            const char *why;
            tests_run++;
            printf("  [%d] %s (%u worker%s)", tests_run, CASES[i].name,
                workers[w], workers[w] == 1 ? "" : "s");
            why = run_case(&CASES[i], workers[w]);
            if (why) {
                tests_failed++;
                printf(" ... FAIL: %s\n", why);
                continue;
            }
            tests_passed++;
            printf(" ... PASS\n");
        }
        {
            const char *why;
            tests_run++;
            printf("  [%d] removal staged before an incremental insert "
                "(%u worker%s)", tests_run, workers[w],
                workers[w] == 1 ? "" : "s");
            why = staged_removal_then_insert(workers[w]);
            if (why) {
                tests_failed++;
                printf(" ... FAIL: %s\n", why);
            } else {
                tests_passed++;
                printf(" ... PASS\n");
            }
        }
        static const int injected[] = { EINVAL, ENOMEM };
        for (uint32_t k = 0; k < 2; k++) {
            const char *why;
            tests_run++;
            printf("  [%d] %s after a head was updated (%u worker%s)",
                tests_run, injected[k] == EINVAL ? "runner refusal"
                : "allocation failure", workers[w],
                workers[w] == 1 ? "" : "s");
            why = failure_after_update(injected[k], workers[w]);
            if (why) {
                tests_failed++;
                printf(" ... FAIL: %s\n", why);
                continue;
            }
            tests_passed++;
            printf(" ... PASS\n");
        }
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
