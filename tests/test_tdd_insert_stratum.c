/*
 * test_tdd_insert_stratum.c - Issue #2114: evaluating an insert into a
 * stratum with occurrence-bound instances.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * wl_columnar_eval_tdd_insert_stratum() brings a stratum's heads up to date
 * after rows were appended to relations it reads, without re-deriving what
 * the heads already hold.  Round 0 runs every instance driven by a changed
 * relation, with that occurrence bound to the new rows and every other
 * occurrence to the whole relation; a recursive stratum then runs the
 * instances driven by its own heads, bound to the rows the last round added,
 * until a round adds nothing.  EDB relations are always read whole after
 * round 0, which is what the snapshot's semi-naive incremental route got
 * wrong (shapes A, B and C of #2114).
 *
 * Each case evaluates a program over its old rows, appends the new ones,
 * runs the stratum, and requires the heads to equal a fresh evaluation over
 * old and new rows together, and the returned head deltas to be exactly the
 * rows that evaluation adds -- in sessions created with one worker and with
 * four.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

#define MAX_ROWS 32
#define MAX_RELS 4

typedef struct {
    const char *name;
    uint32_t nrows;
    int64_t row[MAX_ROWS][2];
} data_t;

/* A set of two-column rows. */
typedef struct {
    int64_t row[MAX_ROWS * 4][2];
    uint32_t count;
} set_t;

typedef struct {
    const char *name;
    const char *src;
    const char *changed; /* the relation the new rows go to */
    data_t old_rows[MAX_RELS];
    uint32_t nold;
    data_t new_rows;
    const char *heads[MAX_RELS]; /* heads of the stratum under test */
    uint32_t nheads;
} case_t;

static char message[256];

static void
noop_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
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
    if (wl_plan_from_program(prog, plan) != 0)
        return -1;
    return wl_session_create(wl_backend_columnar(), *plan, workers, sess);
}

static int
insert_rows(wl_session_t *s, const data_t *d, bool incremental)
{
    int64_t flat[MAX_ROWS * 2];
    for (uint32_t r = 0; r < d->nrows; r++) {
        flat[2 * r] = d->row[r][0];
        flat[2 * r + 1] = d->row[r][1];
    }
    return incremental
           ? col_session_insert_incremental(s, d->name, flat, d->nrows, 2)
           : wl_session_insert(s, d->name, flat, d->nrows, 2);
}

/* The rows of @name in @s, as a set. */
static bool
read_set(wl_session_t *s, const char *name, set_t *out)
{
    col_rel_t *r = session_find_rel(COL_SESSION(s), name);

    memset(out, 0, sizeof(*out));
    if (r && r->ncols == 0 && r->nrows == 0)
        return true; /* never written: empty, and without a schema yet */
    if (!r || r->ncols != 2)
        return false;
    for (uint32_t i = 0; i < r->nrows; i++) {
        bool seen = false;
        for (uint32_t j = 0; j < out->count && !seen; j++)
            seen = out->row[j][0] == r->columns[0][i]
                && out->row[j][1] == r->columns[1][i];
        if (seen || out->count >= MAX_ROWS * 4)
            return false; /* a head must hold each row once */
        out->row[out->count][0] = r->columns[0][i];
        out->row[out->count][1] = r->columns[1][i];
        out->count++;
    }
    return true;
}

static bool
set_has(const set_t *s, int64_t a, int64_t b)
{
    for (uint32_t i = 0; i < s->count; i++)
        if (s->row[i][0] == a && s->row[i][1] == b)
            return true;
    return false;
}

static bool
set_equal(const set_t *x, const set_t *y)
{
    if (x->count != y->count)
        return false;
    for (uint32_t i = 0; i < x->count; i++)
        if (!set_has(y, x->row[i][0], x->row[i][1]))
            return false;
    return true;
}

/* A private copy of rows [base_nrows, nrows) of @name, without delta
 * timestamps: the rows the incremental insert appended. */
static col_rel_t *
appended_rows(wl_col_session_t *coord, const char *name)
{
    col_rel_t *r = session_find_rel(coord, name);
    col_rel_t *d = NULL;

    if (!r || wl_columnar_relation_new_like_governed_checked_mode("$new", r,
        coord->memory_governor, false, &d) != 0)
        return NULL;
    for (uint32_t i = r->base_nrows; i < r->nrows; i++) {
        int64_t row[2] = { r->columns[0][i], r->columns[1][i] };
        if (col_rel_append_row(d, row) != 0) {
            col_rel_destroy(d);
            return NULL;
        }
    }
    return d;
}

static bool
find_relation(const wl_plan_t *plan, const char *name, uint32_t *si_out,
    uint32_t *ri_out)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            if (sp->relations[ri].name
                && strcmp(sp->relations[ri].name, name) == 0) {
                *si_out = si;
                *ri_out = ri;
                return true;
            }
        }
    }
    return false;
}

/* Set on the coordinator before the call, to show the executor runs on a
 * clone that does not inherit them: each one makes the bound runner refuse. */
static bool flags_on_coordinator;

static const char *
run_case(const case_t *c, uint32_t workers)
{
    wl_plan_t *plan = NULL, *oplan = NULL;
    wl_session_t *s = NULL, *oracle = NULL;
    col_rel_t *delta = NULL;
    col_rel_t *head_delta[MAX_RELS] = { NULL };
    set_t before[MAX_RELS], after, want, added;
    const char *why = NULL;
    uint32_t si = 0, ri = 0, nrels = 0;

    /* The oracle: a fresh evaluation over old and new rows together. */
    if (open_session(c->src, workers, &oplan, &oracle) != 0) {
        why = "oracle session";
        goto done;
    }
    for (uint32_t d = 0; d < c->nold; d++)
        if (insert_rows(oracle, &c->old_rows[d], false) != 0) {
            why = "oracle insert";
            goto done;
        }
    if (insert_rows(oracle, &c->new_rows, false) != 0
        || wl_session_snapshot(oracle, noop_tuple, NULL) != 0) {
        why = "oracle evaluation";
        goto done;
    }

    /* The session under test: old rows evaluated, new rows appended. */
    if (open_session(c->src, workers, &plan, &s) != 0) {
        why = "session";
        goto done;
    }
    for (uint32_t d = 0; d < c->nold; d++)
        if (insert_rows(s, &c->old_rows[d], false) != 0) {
            why = "insert";
            goto done;
        }
    if (wl_session_snapshot(s, noop_tuple, NULL) != 0) {
        why = "first evaluation";
        goto done;
    }
    for (uint32_t h = 0; h < c->nheads; h++)
        if (!read_set(s, c->heads[h], &before[h])) {
            why = "head before the insert is not a set";
            goto done;
        }
    if (insert_rows(s, &c->new_rows, true) != 0) {
        why = "incremental insert";
        goto done;
    }
    delta = appended_rows(COL_SESSION(s), c->changed);
    if (!delta || delta->nrows != c->new_rows.nrows || delta->timestamps) {
        why = "delta";
        goto done;
    }
    if (!find_relation(plan, c->heads[0], &si, &ri)) {
        why = "head not in the plan";
        goto done;
    }
    nrels = plan->strata[si].relation_count;
    if (nrels > MAX_RELS) {
        why = "stratum too wide for the test";
        goto done;
    }
    {
        const char *const changed[] = { c->changed };
        col_rel_t *const deltas[] = { delta };
        wl_col_session_t *coord = COL_SESSION(s);
        if (flags_on_coordinator) {
            coord->delta_seeded = true;
            coord->retraction_seeded = true;
            coord->retraction_right_pass = true;
            coord->diff_operators_active = true;
            coord->join_batch_bytes = 1u << 20;
            coord->join_batch_strict = true;
        }
        int rc = wl_columnar_eval_tdd_insert_stratum(coord,
                &plan->strata[si], changed, deltas, 1, head_delta);
        if (flags_on_coordinator) {
            bool kept = coord->delta_seeded && coord->retraction_seeded
                && coord->retraction_right_pass
                && coord->diff_operators_active
                && coord->join_batch_bytes == (1u << 20)
                && coord->join_batch_strict;
            coord->delta_seeded = false;
            coord->retraction_seeded = false;
            coord->retraction_right_pass = false;
            coord->diff_operators_active = false;
            coord->join_batch_bytes = 0;
            coord->join_batch_strict = false;
            if (!kept) {
                why = "the coordinator's own flags were changed";
                goto done;
            }
        }
        if (rc != 0) {
            snprintf(message, sizeof(message), "insert_stratum rc %d", rc);
            why = message;
            goto done;
        }
    }
    for (uint32_t h = 0; h < c->nheads; h++) {
        uint32_t hs, hr;
        if (!find_relation(plan, c->heads[h], &hs, &hr) || hs != si) {
            why = "heads are not one stratum";
            goto done;
        }
        if (!read_set(s, c->heads[h], &after)) {
            snprintf(message, sizeof(message), "%s holds a row twice",
                c->heads[h]);
            why = message;
            goto done;
        }
        if (!read_set(oracle, c->heads[h], &want)) {
            why = "oracle head";
            goto done;
        }
        if (!set_equal(&after, &want)) {
            snprintf(message, sizeof(message),
                "%s has %u rows, a fresh evaluation %u", c->heads[h],
                after.count, want.count);
            why = message;
            goto done;
        }
        /* The returned delta is exactly what the insert added. */
        memset(&added, 0, sizeof(added));
        col_rel_t *hd = head_delta[hr];
        if (hd) {
            if (hd->timestamps) {
                why = "a head delta carries timestamps";
                goto done;
            }
            for (uint32_t i = 0; i < hd->nrows; i++) {
                if (set_has(&added, hd->columns[0][i], hd->columns[1][i])
                    || set_has(&before[h], hd->columns[0][i],
                    hd->columns[1][i])) {
                    why = "a head delta holds a row twice or an old row";
                    goto done;
                }
                added.row[added.count][0] = hd->columns[0][i];
                added.row[added.count][1] = hd->columns[1][i];
                added.count++;
            }
        }
        if (added.count + before[h].count != want.count) {
            snprintf(message, sizeof(message),
                "%s delta has %u rows, the insert added %u", c->heads[h],
                added.count, want.count - before[h].count);
            why = message;
            goto done;
        }
    }
done:
    for (uint32_t h = 0; h < MAX_RELS; h++)
        col_rel_destroy(head_delta[h]);
    col_rel_destroy(delta);
    if (s)
        wl_session_destroy(s);
    if (oracle)
        wl_session_destroy(oracle);
    wl_plan_free(plan);
    wl_plan_free(oplan);
    return why;
}

static const case_t CASES[] = {
    /* Shape A: the changed atom precedes the recursive one. */
    { "right-recursive closure (shape A)",
      ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n",
      "e", { { "e", 1, { { 1, 2 } } } }, 1,
      { "e", 1, { { 2, 3 } } }, { "a" }, 1 },
    /* Shape A': the new row extends the path at the other end. */
    { "right-recursive closure, new head row (shape A')",
      ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n",
      "e", { { "e", 1, { { 2, 3 } } } }, 1,
      { "e", 1, { { 1, 2 } } }, { "a" }, 1 },
    /* Shape B: a base rule joining the changed relation with itself. */
    { "base rule joining its input twice (shape B)",
      ".decl e(x: int64, y: int64)\n.decl s(x: int64, y: int64)\n"
      ".decl a(x: int64, y: int64)\n"
      "a(x, z) :- e(x, y), e(y, z).\na(x, z) :- a(x, y), s(y, z).\n",
      "e", { { "e", 1, { { 1, 2 } } }, { "s", 1, { { 3, 4 } } } }, 2,
      { "e", 1, { { 2, 3 } } }, { "a" }, 1 },
    /* Shape C: a changed atom beside two recursive atoms. */
    { "rule with two recursive atoms (shape C)",
      ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
      ".decl a(x: int64, y: int64)\n"
      "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, w), a(w, z).\n",
      "e",
      { { "f", 2, { { 2, 3 }, { 3, 4 } } }, { "e", 1, { { 9, 9 } } } }, 2,
      { "e", 1, { { 1, 2 } } }, { "a" }, 1 },
    /* Two heads recursive through each other. */
    { "mutual recursion",
      ".decl e(x: int64, y: int64)\n.decl p(x: int64, y: int64)\n"
      ".decl q(x: int64, y: int64)\n"
      "p(x, y) :- e(x, y).\np(x, z) :- q(x, y), e(y, z).\n"
      "q(x, y) :- p(x, y).\n",
      "e", { { "e", 1, { { 1, 2 } } } }, 1,
      { "e", 1, { { 2, 3 } } }, { "p", "q" }, 2 },
    /* New rows that only join each other: new with new. */
    { "new rows joining new rows",
      ".decl e(x: int64, y: int64)\n.decl tc(x: int64, y: int64)\n"
      "tc(x, y) :- e(x, y).\ntc(x, z) :- tc(x, y), tc(y, z).\n",
      "e", { { "e", 1, { { 1, 2 } } } }, 1,
      { "e", 3, { { 2, 3 }, { 3, 4 }, { 4, 5 } } }, { "tc" }, 1 },
    /* A one-atom rule that only filters: its slice is a VARIABLE and a
     * FILTER, the shape that would pass a delta's timestamps through. */
    { "one-atom filter rule",
      ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      "a(x, y) :- e(x, y), x < y.\na(x, z) :- a(x, y), e(y, z).\n",
      "e", { { "e", 2, { { 1, 2 }, { 5, 4 } } } }, 1,
      { "e", 2, { { 2, 3 }, { 4, 1 } } }, { "a" }, 1 },
    /* A non-recursive stratum: round 0 only. */
    { "non-recursive join",
      ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
      ".decl m(x: int64, z: int64)\n"
      "m(x, z) :- e(x, y), f(y, z).\n",
      "e", { { "e", 1, { { 1, 2 } } }, { "f", 2, { { 2, 5 }, { 3, 6 } } } },
      2, { "e", 2, { { 7, 3 }, { 1, 2 } } }, { "m" }, 1 },
};

/* A changed relation's delta must be a private copy without timestamps; a
 * timestamped one is refused before anything is mutated. */
static const char *
timestamped_delta_refused(uint32_t workers)
{
    static const data_t old_e = { "e", 1, { { 1, 2 } } };
    static const data_t new_e = { "e", 1, { { 2, 3 } } };
    wl_plan_t *plan = NULL;
    wl_session_t *s = NULL;
    col_rel_t *delta = NULL;
    col_rel_t *head_delta[MAX_RELS] = { NULL };
    set_t before, after;
    const char *why = NULL;
    uint32_t si, ri;

    if (open_session(".decl e(x: int64, y: int64)\n"
        ".decl a(x: int64, y: int64)\n"
        "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n", workers,
        &plan, &s) != 0
        || insert_rows(s, &old_e, false) != 0
        || wl_session_snapshot(s, noop_tuple, NULL) != 0
        || !read_set(s, "a", &before)
        || insert_rows(s, &new_e, true) != 0) {
        why = "setup";
        goto done;
    }
    delta = appended_rows(COL_SESSION(s), "e");
    if (!delta || col_rel_enable_timestamps(delta) != 0 || !delta->timestamps
        || !find_relation(plan, "a", &si, &ri)) {
        why = "delta";
        goto done;
    }
    {
        const char *const changed[] = { "e" };
        col_rel_t *const deltas[] = { delta };
        if (wl_columnar_eval_tdd_insert_stratum(COL_SESSION(s),
            &plan->strata[si], changed, deltas, 1, head_delta) != EINVAL) {
            why = "a timestamped delta was not refused";
            goto done;
        }
    }
    if (!read_set(s, "a", &after) || !set_equal(&before, &after)
        || head_delta[ri])
        why = "a refused call changed the stratum";
done:
    for (uint32_t h = 0; h < MAX_RELS; h++)
        col_rel_destroy(head_delta[h]);
    col_rel_destroy(delta);
    if (s)
        wl_session_destroy(s);
    wl_plan_free(plan);
    return why;
}

/* A stratum the binder refuses is left as it was: admission builds every
 * manifest before anything is mutated. */
static const char *
refused_stratum_unchanged(uint32_t workers)
{
    static const data_t old_e = { "e", 1, { { 1, 2 } } };
    static const data_t old_s = { "s", 2, { { 1, 2 }, { 3, 4 } } };
    static const data_t new_e = { "e", 1, { { 3, 4 } } };
    wl_plan_t *plan = NULL;
    wl_session_t *s = NULL;
    col_rel_t *delta = NULL;
    col_rel_t *head_delta[MAX_RELS] = { NULL };
    set_t before, after;
    const char *why = NULL;
    uint32_t si, ri;

    if (open_session(".decl e(x: int64, y: int64)\n"
        ".decl s(x: int64, y: int64)\n.decl p(x: int64, y: int64)\n"
        "p(x, y) :- s(x, y), !e(x, y).\n", workers, &plan, &s) != 0
        || insert_rows(s, &old_e, false) != 0
        || insert_rows(s, &old_s, false) != 0
        || wl_session_snapshot(s, noop_tuple, NULL) != 0
        || !read_set(s, "p", &before)
        || insert_rows(s, &new_e, true) != 0) {
        why = "setup";
        goto done;
    }
    delta = appended_rows(COL_SESSION(s), "e");
    if (!delta || !find_relation(plan, "p", &si, &ri)) {
        why = "delta";
        goto done;
    }
    {
        const char *const changed[] = { "e" };
        col_rel_t *const deltas[] = { delta };
        int rc = wl_columnar_eval_tdd_insert_stratum(COL_SESSION(s),
                &plan->strata[si], changed, deltas, 1, head_delta);
        if (rc != ENOTSUP) {
            snprintf(message, sizeof(message), "rc %d, want ENOTSUP", rc);
            why = message;
            goto done;
        }
    }
    if (!read_set(s, "p", &after) || !set_equal(&before, &after)
        || head_delta[ri])
        why = "a refused stratum was changed";
done:
    for (uint32_t h = 0; h < MAX_RELS; h++)
        col_rel_destroy(head_delta[h]);
    col_rel_destroy(delta);
    if (s)
        wl_session_destroy(s);
    wl_plan_free(plan);
    return why;
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 4 };

    printf("test_tdd_insert_stratum (Issue #2114)\n");
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++) {
        for (uint32_t i = 0; i < sizeof(CASES) / sizeof(CASES[0]); i++) {
            const char *why;
            tests_run++;
            printf("  [%d] %s (%u worker%s)", tests_run, CASES[i].name,
                workers[w], workers[w] == 1 ? "" : "s");
            why = run_case(&CASES[i], workers[w]);
            if (why) {
                FAIL(why);
                continue;
            }
            tests_passed++;
            printf(" ... PASS\n");
        }
        tests_run++;
        printf("  [%d] a refused stratum is left unchanged (%u worker%s)",
            tests_run, workers[w], workers[w] == 1 ? "" : "s");
        const char *why = refused_stratum_unchanged(workers[w]);
        if (why) {
            FAIL(why);
        } else {
            tests_passed++;
            printf(" ... PASS\n");
        }
        /* Every case again with the coordinator's operand-selecting and
         * batching flags set. */
        flags_on_coordinator = true;
        for (uint32_t i = 0; i < sizeof(CASES) / sizeof(CASES[0]); i++) {
            tests_run++;
            printf("  [%d] %s, coordinator flags set (%u worker%s)",
                tests_run, CASES[i].name, workers[w],
                workers[w] == 1 ? "" : "s");
            why = run_case(&CASES[i], workers[w]);
            if (why) {
                FAIL(why);
                continue;
            }
            tests_passed++;
            printf(" ... PASS\n");
        }
        flags_on_coordinator = false;
        tests_run++;
        printf("  [%d] a timestamped delta is refused (%u worker%s)",
            tests_run, workers[w], workers[w] == 1 ? "" : "s");
        why = timestamped_delta_refused(workers[w]);
        if (why) {
            FAIL(why);
        } else {
            tests_passed++;
            printf(" ... PASS\n");
        }
    }
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
