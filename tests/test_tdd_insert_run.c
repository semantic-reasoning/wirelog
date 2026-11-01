/*
 * test_tdd_insert_run.c - Issue #2114: running insertion instances.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * wl_columnar_eval_tdd_plan_insert_bindings() lists the instances that find
 * an insert's new derivations; this runs them with the bound-slice runner.
 * The runner was written for the verified CSPA shapes only.  For insertion
 * instances it has to accept any rule the binder accepts: any relation name
 * and width, expressions in MAP and FILTER, a constant in a right operand,
 * inline compound columns, and a serial run on one worker with no
 * partition.
 *
 * Each case binds the driving occurrence to a small DELTA relation and
 * every other occurrence to the session's own relation, and requires the
 * exact rows the rule derives from them.  The last case runs every
 * instance the binder lists for several programs, CSPA after the session's
 * planning passes among them, and requires the runner to accept each one:
 * whatever admission accepts, the runner must run.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

static int snapshot_token;
/* What the last instance run_round ran and failed returned. */
static int last_rc;

/* A worker copies its coordinator's bounded join knob
 * (WIRELOG_JOIN_BATCH_BYTES and WIRELOG_JOIN_BATCH_STRICT, read when the
 * session is created).  Clear it on each worker these tests create, so
 * that the environment does not decide which JOIN path a test takes; the
 * tests of the knob set it afterwards. */
static void
worker_clear_join_batch(wl_col_session_t *worker)
{
    worker->join_batch_bytes = 0;
    worker->join_batch_strict = false;
}

/* The bounded join knob run_instance gives each worker it creates, and
 * what those workers recorded: bounded runs by route and fallbacks
 * (#2201). */
static uint64_t knob_bytes;
static bool knob_strict;
static uint32_t knob_runs[3];
static uint32_t knob_fallbacks;
static uint8_t knob_reason;

/* A FULL read of override_name binds override_rel when it is set. */
static const char *override_name;
static col_rel_t *override_rel;
static char message[256];

#define MAX_COLS 4
#define MAX_ROWS 16

typedef struct {
    int64_t row[MAX_ROWS][MAX_COLS];
    uint32_t count;
    uint32_t ncols;
    bool timestamped; /* some instance output carried delta timestamps */
} rows_t;

typedef struct {
    const char *name;
    uint32_t ncols;
    uint32_t nrows;
    int64_t row[MAX_ROWS][MAX_COLS];
} data_t;

typedef struct {
    wl_plan_t *plan;
    wl_session_t *session;
} fixture_t;

static void
noop_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
}

static void
fixture_close(fixture_t *f)
{
    if (f->session)
        wl_session_destroy(f->session);
    if (f->plan)
        wl_plan_free(f->plan);
    memset(f, 0, sizeof(*f));
}

/* Plan @src, open a one-worker session, insert @data and evaluate once, so
 * every relation exists with its schema. */
static const char *
fixture_open(fixture_t *f, const char *src, bool passes, const data_t *data,
    uint32_t ndata)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);

    memset(f, 0, sizeof(*f));
    if (!prog)
        return "parse";
    plan_fixture_hold(prog);
    if (passes) {
        wl_fusion_apply(prog, NULL);
        wl_jpp_apply(prog, NULL);
        wl_sip_apply(prog, NULL);
    }
    if (wl_plan_from_program(prog, &f->plan) != 0)
        return "plan";
    if (wl_session_create(wl_backend_columnar(), f->plan, 1, &f->session)
        != 0)
        return "session";
    for (uint32_t d = 0; d < ndata; d++) {
        int64_t flat[MAX_ROWS * MAX_COLS];
        for (uint32_t r = 0; r < data[d].nrows; r++)
            for (uint32_t c = 0; c < data[d].ncols; c++)
                flat[r * data[d].ncols + c] = data[d].row[r][c];
        if (wl_session_insert(f->session, data[d].name, flat, data[d].nrows,
            data[d].ncols) != 0)
            return "insert";
    }
    if (wl_session_snapshot(f->session, noop_tuple, NULL) != 0)
        return "snapshot";
    return NULL;
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

static wl_columnar_eval_tdd_input_t
capture(const wl_columnar_eval_tdd_plan_read_t *read, col_rel_t *r)
{
    return (wl_columnar_eval_tdd_input_t){
               .read = read, .relation = r, .name = r->name,
               .snapshot = &snapshot_token, .partition = NULL,
               .worker_index = 0, .worker_count = 1,
               .view_generation = r->view_generation,
               .storage_generation = r->storage_generation,
               .ncols = r->ncols, .declared_ncols = r->declared_ncols,
               .schema_ok = r->schema_ok,
               .column_names = (const char *const *)r->col_names,
               .column_types = r->column_types,
               .compound_kind = r->compound_kind,
               .compound_count = r->compound_count,
               .compound_arity_len = r->compound_arity_len,
               .compound_arity_map = r->compound_arity_map,
               .inline_physical_offset = r->inline_physical_offset,
               .has_graph_column = r->has_graph_column,
               .graph_col_idx = r->graph_col_idx,
    };
}

/* A private copy of the session relation @name holding only @rows: the
 * delta of an insert into it, with the relation's own schema. */
static col_rel_t *
delta_of(wl_col_session_t *coord, const char *name, const data_t *rows)
{
    col_rel_t *full = session_find_rel(coord, name);
    col_rel_t *delta = NULL;

    if (!full || col_rel_deep_copy(full, &delta, NULL) != 0 || !delta)
        return NULL;
    delta->nrows = 0;
    delta->base_nrows = 0;
    delta->sorted_nrows = 0;
    for (uint32_t r = 0; r < rows->nrows; r++) {
        if (col_rel_append_row(delta, rows->row[r]) != 0) {
            col_rel_destroy(delta);
            return NULL;
        }
    }
    return delta;
}

/* Run instance @slice_index of @m on a worker clone, binding the driver to
 * @delta and every other read to the session relation it names.  The rows
 * go to @out when it is not NULL. */
static int
run_instance(wl_col_session_t *coord, const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *m, uint32_t slice_index,
    col_rel_t *delta, rows_t *out)
{
    const wl_columnar_eval_tdd_plan_slice_t *slice = &m->slices[slice_index];
    wl_columnar_eval_tdd_input_t inputs[8];
    wl_col_session_t worker;
    wl_columnar_eval_tdd_run_t run;
    col_rel_t *result = NULL;
    int rc;

    if (slice->read_count > 8)
        return E2BIG;
    for (uint32_t r = 0; r < slice->read_count; r++) {
        const wl_columnar_eval_tdd_plan_read_t *read =
            &m->reads[slice->read_start + r];
        col_rel_t *rel = read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA && delta
            ? delta : session_find_rel(coord, read->relation_name);
        if (read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_FULL && override_name
            && strcmp(read->relation_name, override_name) == 0)
            rel = override_rel;
        if (!rel)
            return ENOENT;
        inputs[r] = capture(read, rel);
    }
    memset(&worker, 0, sizeof(worker));
    rc = col_worker_session_create(coord, 0, NULL, 0, &worker);
    if (rc != 0)
        return rc;
    worker_clear_join_batch(&worker);
    worker.join_batch_bytes = knob_bytes;
    worker.join_batch_strict = knob_strict;
    memset(&run, 0, sizeof(run));
    rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, m,
            slice_index, &snapshot_token, NULL, 0, 1, inputs,
            slice->read_count, &run);
    if (rc == 0)
        rc = wl_columnar_eval_tdd_run_bound_slice_finish(&run, &result);
    for (uint32_t r = 0; r < 3; r++)
        knob_runs[r] += worker.join_batch_runs[r];
    knob_fallbacks += worker.join_batch_fallback_count;
    if (worker.join_batch_fallback_count)
        knob_reason = worker.join_batch_last_reason;
    if (rc == 0 && result && out) {
        out->ncols = result->ncols;
        out->timestamped |= result->timestamps != NULL;
        for (uint32_t i = 0; i < result->nrows && out->count < MAX_ROWS; i++) {
            for (uint32_t c = 0; c < result->ncols && c < MAX_COLS; c++)
                out->row[out->count][c] = result->columns[c][i];
            out->count++;
        }
    }
    if (result)
        col_rel_destroy(result);
    int destroy_rc = col_worker_session_destroy(&worker);
    return rc != 0 ? rc : destroy_rc;
}

/* Run every instance of @head's round (round 0 with @changed) and collect
 * what they derive.  @delta binds each driver. */
static const char *
run_round(fixture_t *f, const char *head, const char *changed,
    col_rel_t *delta, rows_t *out)
{
    uint32_t si, ri;
    wl_columnar_eval_tdd_plan_manifest_t m;
    const char *const set[] = { changed };
    wl_col_session_t *coord = COL_SESSION(f->session);

    if (!find_relation(f->plan, head, &si, &ri))
        return "head not in the plan";
    if (wl_columnar_eval_tdd_plan_insert_bindings(&f->plan->strata[si], ri,
        set, 1, true, &m) != 0)
        return "bindings failed";
    memset(out, 0, sizeof(*out));
    for (uint32_t s = 0; s < m.slice_count; s++) {
        int rc = run_instance(coord, &f->plan->strata[si], &m, s, delta, out);
        if (rc != 0) {
            last_rc = rc;
            snprintf(message, sizeof(message), "%s instance %u: rc %d", head,
                s, rc);
            wl_columnar_eval_tdd_plan_bindings_free(&m);
            return message;
        }
    }
    wl_columnar_eval_tdd_plan_bindings_free(&m);
    return NULL;
}

/* @got holds exactly the rows of @want, as a bag. */
static const char *
same_rows(const rows_t *got, const int64_t (*want)[MAX_COLS], uint32_t nwant,
    uint32_t ncols)
{
    bool used[MAX_ROWS] = { false };

    if (got->count != nwant) {
        snprintf(message, sizeof(message), "%u rows, want %u", got->count,
            nwant);
        return message;
    }
    if (nwant && got->ncols != ncols)
        return "wrong output width";
    for (uint32_t w = 0; w < nwant; w++) {
        bool found = false;
        for (uint32_t g = 0; g < got->count && !found; g++) {
            if (used[g])
                continue;
            bool eq = true;
            for (uint32_t c = 0; c < ncols; c++)
                eq &= got->row[g][c] == want[w][c];
            if (eq)
                used[g] = found = true;
        }
        if (!found) {
            snprintf(message, sizeof(message), "missing row (%lld, %lld)",
                (long long)want[w][0], (long long)want[w][1]);
            return message;
        }
    }
    return NULL;
}

#define BEGIN(name)                                     \
        do {                                            \
            tests_run++;                                \
            printf("  [%d] %s", tests_run, (name));     \
        } while (0)

#define CHECK(expr)                    \
        do {                           \
            const char *why_ = (expr); \
            if (why_) {                \
                FAIL(why_);            \
                goto done;             \
            }                          \
        } while (0)

/* e and f after the insert: (7, 6) is the new row of e, (6, 3) of f. */
static const data_t EF[] = {
    { "e", 2, 3, { { 1, 2 }, { 5, 6 }, { 7, 6 } } },
    { "f", 2, 4, { { 2, 3 }, { 2, 20 }, { 6, 4 }, { 6, 3 } } },
};
static const data_t NEW_E = { "e", 2, 1, { { 7, 6 } } };
static const data_t NEW_F = { "f", 2, 1, { { 6, 3 } } };

/* An arithmetic head over a join filtered by a comparison. */
static void
test_expressions(void)
{
    static const int64_t by_e[][MAX_COLS] = { { 7, 5 }, { 7, 4 } };
    static const int64_t by_f[][MAX_COLS] = { { 5, 4 }, { 7, 4 } };
    fixture_t f;
    rows_t got;
    col_rel_t *de = NULL, *df = NULL;

    BEGIN("arithmetic head and comparison filter");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl t(x: int64, y: int64)\n"
        "t(x, w + 1) :- e(x, y), f(y, w), w < 10.\n", false, EF, 2));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    df = delta_of(COL_SESSION(f.session), "f", &NEW_F);
    if (!de || !df) {
        FAIL("delta");
        goto done;
    }
    /* e(7, 6) joins f(6, 4) and f(6, 3). */
    CHECK(run_round(&f, "t", "e", de, &got));
    CHECK(same_rows(&got, by_e, 2, 2));
    /* f(6, 3) joins e(5, 6) and e(7, 6). */
    CHECK(run_round(&f, "t", "f", df, &got));
    CHECK(same_rows(&got, by_f, 2, 2));
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    col_rel_destroy(df);
    fixture_close(&f);
}

/* A constant in the right operand becomes a filter on that operand. */
static void
test_constant(void)
{
    static const int64_t by_e[][MAX_COLS] = { { 7 } };
    static const int64_t by_f[][MAX_COLS] = { { 5 }, { 7 } };
    fixture_t f;
    rows_t got;
    col_rel_t *de = NULL, *df = NULL;

    BEGIN("constant in the right operand");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl u(x: int64)\n"
        "u(x) :- e(x, y), f(y, 3).\n", false, EF, 2));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    df = delta_of(COL_SESSION(f.session), "f", &NEW_F);
    if (!de || !df) {
        FAIL("delta");
        goto done;
    }
    CHECK(run_round(&f, "u", "e", de, &got));
    CHECK(same_rows(&got, by_e, 1, 1));
    CHECK(run_round(&f, "u", "f", df, &got));
    CHECK(same_rows(&got, by_f, 2, 1));
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    col_rel_destroy(df);
    fixture_close(&f);
}

/* A self-join: one instance per occurrence, the other read FULL. */
static void
test_self_join(void)
{
    static const data_t data[] = {
        { "e", 2, 3, { { 1, 2 }, { 2, 3 }, { 3, 4 } } },
    };
    static const data_t added = { "e", 2, 1, { { 2, 3 } } };
    /* Driven at e(x, y): (2, 3) joins (3, 4).  Driven at e(y, z): (1, 2)
     * joins (2, 3). */
    static const int64_t want[][MAX_COLS] = { { 2, 4 }, { 1, 3 } };
    fixture_t f;
    rows_t got;
    col_rel_t *de = NULL;

    BEGIN("self-join, one instance per occurrence");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl m(x: int64, z: int64)\n"
        "m(x, z) :- e(x, y), e(y, z).\n", false, data, 1));
    de = delta_of(COL_SESSION(f.session), "e", &added);
    if (!de) {
        FAIL("delta");
        goto done;
    }
    CHECK(run_round(&f, "m", "e", de, &got));
    CHECK(same_rows(&got, want, 2, 2));
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    fixture_close(&f);
}

/* Inline compound columns are physical payload columns of the relation. */
static void
test_inline_compound(void)
{
    static const data_t data[] = {
        { "ev", 3, 2, { { 1, 10, 11 }, { 4, 40, 41 } } },
    };
    static const data_t added = { "ev", 3, 1, { { 4, 40, 41 } } };
    static const int64_t want[][MAX_COLS] = { { 4, 40 } };
    fixture_t f;
    rows_t got;
    col_rel_t *dev = NULL;

    BEGIN("inline compound column");
    CHECK(fixture_open(&f,
        ".decl ev(id: int64, lbl: pair/2 inline)\n"
        ".decl o(id: int64, p: int64)\n"
        "o(i, p) :- ev(i, pair(p, q)).\n", false, data, 1));
    dev = delta_of(COL_SESSION(f.session), "ev", &added);
    if (!dev) {
        FAIL("delta");
        goto done;
    }
    CHECK(run_round(&f, "o", "ev", dev, &got));
    CHECK(same_rows(&got, want, 1, 2));
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(dev);
    fixture_close(&f);
}

/* A recursive stratum's head, read FULL, may carry delta timestamps; the
 * instance still derives exactly its rows, and its output carries none. */
static void
test_timestamped_head(void)
{
    static const data_t data[] = {
        { "e", 2, 2, { { 1, 2 }, { 2, 3 } } },
    };
    static const data_t added = { "e", 2, 1, { { 1, 2 } } };
    /* a(x, y) :- e(x, y) gives (1, 2); e(1, 2) joined with a(2, 3) gives
     * (1, 3). */
    static const int64_t want[][MAX_COLS] = { { 1, 2 }, { 1, 3 } };
    fixture_t f;
    rows_t got;
    col_rel_t *de = NULL;
    col_rel_t *head;

    BEGIN("a timestamped head read FULL");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
        "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n", false, data,
        1));
    head = session_find_rel(COL_SESSION(f.session), "a");
    if (!head || !head->timestamps) {
        FAIL("precondition: the head is not timestamped");
        goto done;
    }
    de = delta_of(COL_SESSION(f.session), "e", &added);
    if (!de) {
        FAIL("delta");
        goto done;
    }
    CHECK(run_round(&f, "a", "e", de, &got));
    CHECK(same_rows(&got, want, 2, 2));
    if (got.timestamped) {
        FAIL("an instance output carries timestamps");
        goto done;
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    fixture_close(&f);
}

/* Run every instance the binder lists for every relation of @src, round 0
 * with @changed and the later rounds, binding each driver to the relation
 * it names.  Every one must run. */
static const char *
run_all(const char *src, bool passes, const data_t *data, uint32_t ndata,
    const char *const *changed, uint32_t nchanged)
{
    fixture_t f;
    const char *why = NULL;
    wl_col_session_t *coord;

    why = fixture_open(&f, src, passes, data, ndata);
    if (why)
        goto done;
    coord = COL_SESSION(f.session);
    for (uint32_t si = 0; si < f.plan->stratum_count && !why; si++) {
        const wl_plan_stratum_t *sp = &f.plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count && !why; ri++) {
            for (int round0 = 1; round0 >= 0 && !why; round0--) {
                wl_columnar_eval_tdd_plan_manifest_t m;
                int rc = wl_columnar_eval_tdd_plan_insert_bindings(sp, ri,
                        changed, nchanged, round0 != 0, &m);
                if (rc != 0) {
                    snprintf(message, sizeof(message),
                        "%s: bindings rc %d", sp->relations[ri].name, rc);
                    why = message;
                    break;
                }
                for (uint32_t s = 0; s < m.slice_count && !why; s++) {
                    rc = run_instance(coord, sp, &m, s, NULL, NULL);
                    if (rc != 0) {
                        snprintf(message, sizeof(message),
                            "%s round %s instance %u: runner rc %d",
                            sp->relations[ri].name, round0 ? "0" : "n", s,
                            rc);
                        why = message;
                    }
                }
                wl_columnar_eval_tdd_plan_bindings_free(&m);
            }
        }
    }
done:
    fixture_close(&f);
    return why;
}

static void
test_runner_accepts_every_instance(void)
{
    static const char *const e[] = { "e" };
    static const char *const ef[] = { "e", "f" };
    static const char *const assign[] = { "assign" };
    static const data_t e_f[] = {
        { "e", 2, 2, { { 1, 2 }, { 2, 3 } } },
        { "f", 2, 1, { { 3, 4 } } },
    };
    static const data_t e_s[] = {
        { "e", 2, 2, { { 1, 2 }, { 2, 3 } } },
        { "s", 2, 1, { { 3, 5 } } },
    };
    static const data_t cspa[] = {
        { "assign", 2, 3, { { 1, 2 }, { 2, 3 }, { 3, 1 } } },
        { "dereference", 2, 2, { { 1, 2 }, { 2, 3 } } },
    };

    BEGIN("the runner accepts every instance the binder lists");
    CHECK(run_all(".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
        "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n", false,
        e_f, 1, e, 1));
    CHECK(run_all(".decl e(x: int64, y: int64)\n.decl s(x: int64, y: int64)\n"
        ".decl a(x: int64, y: int64)\n"
        "a(x, z) :- e(x, y), e(y, z).\na(x, z) :- a(x, y), s(y, z).\n", false,
        e_s, 2, e, 1));
    CHECK(run_all(".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl a(x: int64, y: int64)\n"
        "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, w), a(w, z).\n", false,
        e_f, 2, e, 1));
    CHECK(run_all(".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl t(x: int64, y: int64)\n.decl u(x: int64)\n"
        "t(x, w + 1) :- e(x, y), f(y, w), w < 10.\n"
        "u(x) :- e(x, y), f(y, 3).\n", false, EF, 2, ef, 2));
    CHECK(run_all(
            ".decl assign(x: int32, y: int32)\n"
            ".decl dereference(x: int32, y: int32)\n"
            ".decl valueFlow(x: int32, y: int32)\n"
            ".decl memoryAlias(x: int32, y: int32)\n"
            ".decl valueAlias(x: int32, y: int32)\n"
            "valueFlow(y, x) :- assign(y, x).\n"
            "valueFlow(x, x) :- assign(x, _).\n"
            "valueFlow(x, x) :- assign(_, x).\n"
            "memoryAlias(x, x) :- assign(_, x).\n"
            "memoryAlias(x, x) :- assign(x, _).\n"
            "valueFlow(x, y) :- valueFlow(x, z), valueFlow(z, y).\n"
            "valueFlow(x, y) :- assign(x, z), memoryAlias(z, y).\n"
            "memoryAlias(x, w) :- dereference(y, x), valueAlias(y, z), "
            "dereference(z, w).\n"
            "valueAlias(x, y) :- valueFlow(z, x), valueFlow(z, y).\n"
            "valueAlias(x, y) :- valueFlow(z, x), memoryAlias(z, w), "
            "valueFlow(w, y).\n", true, cspa, 2, assign, 1));
    tests_passed++;
    printf(" ... PASS\n");
done:
    return;
}

/* What try_begin sets on its worker clone before beginning. */
typedef enum {
    WORKER_PLAIN,
    WORKER_DIFF_ACTIVE,
    WORKER_JOIN_BATCH_BYTES,
    WORKER_JOIN_BATCH_STRICT,
} worker_state_t;

/* Begin instance @s of @m with @inputs on a fresh worker clone, which gets
 * @state, and return what begin returns.  A refused begin must leave the
 * run handle empty and the worker free. */
static int
try_begin(wl_col_session_t *coord, const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *m, uint32_t s,
    const wl_columnar_eval_tdd_input_t *inputs, uint32_t count,
    const void *snapshot, worker_state_t state, bool *left_clean)
{
    wl_col_session_t worker;
    wl_columnar_eval_tdd_run_t run;
    col_rel_t *out = NULL;

    memset(&worker, 0, sizeof(worker));
    if (col_worker_session_create(coord, 0, NULL, 0, &worker) != 0)
        return -1;
    worker_clear_join_batch(&worker);
    worker.diff_operators_active = state == WORKER_DIFF_ACTIVE;
    if (state == WORKER_JOIN_BATCH_BYTES)
        worker.join_batch_bytes = 224;
    worker.join_batch_strict = state == WORKER_JOIN_BATCH_STRICT;
    memset(&run, 0, sizeof(run));
    int rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, m, s,
            snapshot, NULL, 0, 1, inputs, count, &run);
    *left_clean = !run.worker && !run.slots && !worker.tdd_input_run;
    if (rc == 0)
        (void)wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out);
    if (out)
        col_rel_destroy(out);
    worker.diff_operators_active = false;
    worker_clear_join_batch(&worker);
    if (col_worker_session_destroy(&worker) != 0)
        return -1;
    return rc;
}

/* Inputs that do not match the instance are refused before the run takes
 * anything, and so is a worker whose differential operators are active.  A
 * worker that asks for bounded join batching (join_batch_bytes or
 * join_batch_strict) runs (#2201). */
static void
test_refused_inputs(void)
{
    static int other_snapshot;
    static int some_partition;
    fixture_t f;
    uint32_t si, ri;
    wl_columnar_eval_tdd_plan_manifest_t m;
    wl_columnar_eval_tdd_input_t inputs[2], bad[2];
    const char *const set[] = { "e" };
    bool clean = false;
    bool have_manifest = false;

    BEGIN("mismatched inputs and differential workers are refused");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl t(x: int64, y: int64)\n"
        "t(x, w + 1) :- e(x, y), f(y, w), w < 10.\n", false, EF, 2));
    if (!find_relation(f.plan, "t", &si, &ri)
        || wl_columnar_eval_tdd_plan_insert_bindings(&f.plan->strata[si], ri,
        set, 1, true, &m) != 0 || m.slice_count != 1
        || m.slices[0].read_count != 2) {
        FAIL("bindings");
        goto done;
    }
    have_manifest = true;
    wl_col_session_t *coord = COL_SESSION(f.session);
    const wl_plan_stratum_t *sp = &f.plan->strata[si];
    for (uint32_t r = 0; r < 2; r++)
        inputs[r] = capture(&m.reads[m.slices[0].read_start + r],
                session_find_rel(coord, m.reads[m.slices[0].read_start + r]
                .relation_name));
    /* The inputs as captured run. */
    if (try_begin(coord, sp, &m, 0, inputs, 2, &snapshot_token, WORKER_PLAIN,
        &clean) != 0) {
        FAIL("the matching inputs did not run");
        goto done;
    }
    /* Another snapshot than the one the inputs were captured under. */
    if (try_begin(coord, sp, &m, 0, inputs, 2, &other_snapshot, WORKER_PLAIN,
        &clean) != EINVAL || !clean) {
        FAIL("a snapshot mismatch was not refused cleanly");
        goto done;
    }
    /* A FULL read handed a partition. */
    memcpy(bad, inputs, sizeof(bad));
    for (uint32_t r = 0; r < 2; r++)
        if (bad[r].read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_FULL)
            bad[r].partition = &some_partition;
    if (try_begin(coord, sp, &m, 0, bad, 2, &snapshot_token, WORKER_PLAIN,
        &clean) != EINVAL || !clean) {
        FAIL("a partitioned FULL read was not refused cleanly");
        goto done;
    }
    /* Fewer inputs than the instance reads. */
    if (try_begin(coord, sp, &m, 0, inputs, 1, &snapshot_token, WORKER_PLAIN,
        &clean) != EINVAL || !clean) {
        FAIL("a missing input was not refused cleanly");
        goto done;
    }
    /* Differential operators choose their own operands. */
    if (try_begin(coord, sp, &m, 0, inputs, 2, &snapshot_token,
        WORKER_DIFF_ACTIVE, &clean) != ENOTSUP || !clean) {
        FAIL("an active differential worker was not refused cleanly");
        goto done;
    }
    /* A bound run batches its joins under the knob (#2201): a worker that
     * asks for it, or only for its strict mode, which without a byte
     * budget batches nothing, runs. */
    if (try_begin(coord, sp, &m, 0, inputs, 2, &snapshot_token,
        WORKER_JOIN_BATCH_BYTES, &clean) != 0) {
        FAIL("a worker with join_batch_bytes set did not run");
        goto done;
    }
    if (try_begin(coord, sp, &m, 0, inputs, 2, &snapshot_token,
        WORKER_JOIN_BATCH_STRICT, &clean) != 0) {
        FAIL("a worker with join_batch_strict set did not run");
        goto done;
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    if (have_manifest)
        wl_columnar_eval_tdd_plan_bindings_free(&m);
    fixture_close(&f);
}

/* Issue #2122: rows [@begin, @end) of @src, as a private relation with
 * @src's name and schema: one range of an instance's leading input. */
static col_rel_t *
range_of(const col_rel_t *src, uint32_t begin, uint32_t end)
{
    col_rel_t *copy = NULL;
    if (col_rel_deep_copy(src, &copy, NULL) != 0 || !copy)
        return NULL;
    copy->nrows = 0;
    copy->base_nrows = 0;
    copy->sorted_nrows = 0;
    for (uint32_t r = begin; r < end; r++) {
        if (col_rel_append_row(copy, col_rel_row(src, r)) != 0) {
            col_rel_destroy(copy);
            return NULL;
        }
    }
    return copy;
}

/* When set, the leading input carries this partition instead of the
 * run's own. */
static const void *range_input_partition;
/* When set, every other input claims range 1 of 2, with no partition. */
static bool range_others_ranged;

/* Run instance @s of @m with its leading read bound to range @k of @count
 * of the relation it would otherwise read (the delta when the leading read
 * is the driver), and append what it derives to @out.  @partition marks
 * the leading input; @run_index and @run_count are what the run is told,
 * which a refusal case sets apart from the input's own. */
static int
run_range(wl_col_session_t *coord, const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *m, uint32_t s,
    col_rel_t *delta, uint32_t k, uint32_t count, uint32_t run_index,
    uint32_t run_count, rows_t *out)
{
    static int partition_token;
    const wl_columnar_eval_tdd_plan_slice_t *slice = &m->slices[s];
    wl_columnar_eval_tdd_input_t inputs[8];
    wl_col_session_t worker;
    wl_columnar_eval_tdd_run_t run;
    col_rel_t *result = NULL, *range = NULL;
    int rc;

    if (slice->read_count > 8)
        return E2BIG;
    for (uint32_t r = 0; r < slice->read_count; r++) {
        const wl_columnar_eval_tdd_plan_read_t *read =
            &m->reads[slice->read_start + r];
        col_rel_t *rel = read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
            ? delta : session_find_rel(coord, read->relation_name);
        if (!rel)
            return ENOENT;
        if (read->op_index == slice->start) {
            uint32_t n = rel->nrows;
            range = range_of(rel, (uint32_t)((uint64_t)n * k / count),
                    (uint32_t)((uint64_t)n * (k + 1) / count));
            if (!range)
                return ENOMEM;
            inputs[r] = capture(read, range);
            inputs[r].partition = range_input_partition
                ? range_input_partition : &partition_token;
            inputs[r].worker_index = k;
            inputs[r].worker_count = count;
        } else {
            inputs[r] = capture(read, rel);
            if (range_others_ranged) {
                inputs[r].worker_index = 1;
                inputs[r].worker_count = 2;
            }
        }
    }
    memset(&worker, 0, sizeof(worker));
    rc = col_worker_session_create(coord, 0, NULL, 0, &worker);
    if (rc != 0) {
        col_rel_destroy(range);
        return rc;
    }
    worker_clear_join_batch(&worker);
    memset(&run, 0, sizeof(run));
    rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, m, s,
            &snapshot_token, &partition_token, run_index, run_count, inputs,
            slice->read_count, &run);
    if (rc == 0)
        rc = wl_columnar_eval_tdd_run_bound_slice_finish(&run, &result);
    if (rc == 0 && result && out) {
        out->ncols = result->ncols;
        for (uint32_t i = 0; i < result->nrows && out->count < MAX_ROWS; i++) {
            for (uint32_t c = 0; c < result->ncols && c < MAX_COLS; c++)
                out->row[out->count][c] = result->columns[c][i];
            out->count++;
        }
    }
    if (result)
        col_rel_destroy(result);
    col_rel_destroy(range);
    int destroy_rc = col_worker_session_destroy(&worker);
    return rc != 0 ? rc : destroy_rc;
}

/* Issue #2122: an instance's output is a bag over the rows of its leading
 * input, so running it once per range of that input, DELTA or FULL, and
 * pooling the outputs gives exactly the rows of one unsplit run. */
static void
test_leading_ranges(void)
{
    static const data_t data[] = {
        { "e", 2, 6, { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 2, 5 }, { 5, 6 },
                       { 4, 2 } } },
    };
    static const data_t added = { "e", 2, 3, { { 2, 3 }, { 5, 6 },
                                               { 4, 2 } } };
    static const uint32_t counts[] = { 2, 3, 4 };
    fixture_t f;
    uint32_t si, ri;
    wl_columnar_eval_tdd_plan_manifest_t m;
    const char *const set[] = { "e" };
    bool have_manifest = false;
    col_rel_t *de = NULL;
    rows_t whole, pooled;

    BEGIN("ranges of the leading input pool to the unsplit run");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl m(x: int64, z: int64)\n"
        "m(x, z) :- e(x, y), e(y, z), x != z.\n", false, data, 1));
    if (!find_relation(f.plan, "m", &si, &ri)
        || wl_columnar_eval_tdd_plan_insert_bindings(&f.plan->strata[si], ri,
        set, 1, true, &m) != 0 || m.slice_count != 2) {
        FAIL("bindings");
        goto done;
    }
    have_manifest = true;
    wl_col_session_t *coord = COL_SESSION(f.session);
    const wl_plan_stratum_t *sp = &f.plan->strata[si];
    de = delta_of(coord, "e", &added);
    if (!de) {
        FAIL("delta");
        goto done;
    }
    /* Slice 0 leads with its DELTA driver, slice 1 with a FULL read. */
    for (uint32_t s = 0; s < m.slice_count; s++) {
        memset(&whole, 0, sizeof(whole));
        if (run_instance(coord, sp, &m, s, de, &whole) != 0
            || whole.count == 0) {
            FAIL("the unsplit run");
            goto done;
        }
        int64_t want[MAX_ROWS][MAX_COLS];
        for (uint32_t i = 0; i < whole.count; i++)
            memcpy(want[i], whole.row[i], sizeof(want[i]));
        for (uint32_t c = 0; c < sizeof(counts) / sizeof(counts[0]); c++) {
            memset(&pooled, 0, sizeof(pooled));
            for (uint32_t k = 0; k < counts[c]; k++) {
                int rc = run_range(coord, sp, &m, s, de, k, counts[c], k,
                        counts[c], &pooled);
                if (rc != 0) {
                    snprintf(message, sizeof(message),
                        "instance %u, range %u of %u: rc %d", s, k,
                        counts[c], rc);
                    FAIL(message);
                    goto done;
                }
            }
            CHECK(same_rows(&pooled, (const int64_t (*)[MAX_COLS]) want,
                whole.count, whole.ncols));
        }
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    if (have_manifest)
        wl_columnar_eval_tdd_plan_bindings_free(&m);
    fixture_close(&f);
}

/* Issue #2122: only the leading read may carry the run's partition, and it
 * must carry the run's range index and count. */
static void
test_range_refusals(void)
{
    static const data_t data[] = {
        { "e", 2, 3, { { 1, 2 }, { 2, 3 }, { 3, 4 } } },
    };
    static const data_t added = { "e", 2, 1, { { 2, 3 } } };
    fixture_t f;
    uint32_t si, ri;
    wl_columnar_eval_tdd_plan_manifest_t m;
    const char *const set[] = { "e" };
    bool have_manifest = false;
    col_rel_t *de = NULL;

    BEGIN("a range on the wrong read or with the wrong index is refused");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl m(x: int64, z: int64)\n"
        "m(x, z) :- e(x, y), e(y, z).\n", false, data, 1));
    if (!find_relation(f.plan, "m", &si, &ri)
        || wl_columnar_eval_tdd_plan_insert_bindings(&f.plan->strata[si], ri,
        set, 1, true, &m) != 0 || m.slice_count != 2) {
        FAIL("bindings");
        goto done;
    }
    have_manifest = true;
    wl_col_session_t *coord = COL_SESSION(f.session);
    const wl_plan_stratum_t *sp = &f.plan->strata[si];
    de = delta_of(coord, "e", &added);
    if (!de) {
        FAIL("delta");
        goto done;
    }
    /* The run is told range 1 of 2 while its leading input is range 0. */
    if (run_range(coord, sp, &m, 0, de, 0, 2, 1, 2, NULL) != EINVAL) {
        FAIL("a leading range with another index was not refused");
        goto done;
    }
    /* The run is told 3 ranges while its leading input is one of 2. */
    if (run_range(coord, sp, &m, 1, de, 0, 2, 0, 3, NULL) != EINVAL) {
        FAIL("a leading range with another count was not refused");
        goto done;
    }
    /* The leading input belongs to another partition than the run. */
    static int other_partition;
    range_input_partition = &other_partition;
    int other_rc = run_range(coord, sp, &m, 0, de, 0, 2, 0, 2, NULL);
    range_input_partition = NULL;
    if (other_rc != EINVAL) {
        FAIL("a leading range of another partition was not refused");
        goto done;
    }
    /* An input other than the leading one claims a range of its own. */
    range_others_ranged = true;
    other_rc = run_range(coord, sp, &m, 0, de, 0, 2, 0, 2, NULL);
    range_others_ranged = false;
    if (other_rc != EINVAL) {
        FAIL("a range on an input other than the leading one was not "
            "refused");
        goto done;
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    if (have_manifest)
        wl_columnar_eval_tdd_plan_bindings_free(&m);
    fixture_close(&f);
}

/* Run @head's round 0 with @changed under the knob @bytes/@strict.  With
 * @want_rc zero the rows must equal @want; without the knob no JOIN may run
 * bounded; with it the workers must have recorded bounded runs on the
 * ordinary route when @ordinary is set and on the pipeline route when
 * @pipeline is, none on the differential route, and, when @reason is zero,
 * no fallback, else at least one with @reason last (#2201).  A nonzero
 * @want_rc is what an instance must fail with instead. */
static const char *
knob_round(fixture_t *f, const char *head, const char *changed,
    col_rel_t *delta, uint64_t bytes, bool strict, bool ordinary,
    bool pipeline, uint8_t reason, int want_rc,
    const int64_t (*want)[MAX_COLS], uint32_t nwant, uint32_t ncols)
{
    rows_t got;
    const char *why;

    knob_bytes = bytes;
    knob_strict = strict;
    memset(knob_runs, 0, sizeof(knob_runs));
    knob_fallbacks = 0;
    knob_reason = 0;
    last_rc = 0;
    why = run_round(f, head, changed, delta, &got);
    knob_bytes = 0;
    knob_strict = false;
    if (want_rc != 0) {
        if (last_rc == want_rc)
            return NULL;
        snprintf(message, sizeof(message), "rc %d, want %d", last_rc,
            want_rc);
        return message;
    }
    if (why)
        return why;
    why = same_rows(&got, want, nwant, ncols);
    if (why)
        return why;
    if (bytes == 0)
        return knob_runs[COL_JOIN_BATCH_RUN_ORDINARY]
               + knob_runs[COL_JOIN_BATCH_RUN_PIPELINE] == 0
               ? NULL : "a run without the knob ran bounded";
    if ((ordinary && knob_runs[COL_JOIN_BATCH_RUN_ORDINARY] == 0)
        || (pipeline && knob_runs[COL_JOIN_BATCH_RUN_PIPELINE] == 0)
        || knob_runs[COL_JOIN_BATCH_RUN_DIFFERENTIAL] != 0
        || (reason == 0 && knob_fallbacks != 0)
        || (reason != 0 && (knob_fallbacks == 0 || knob_reason != reason))) {
        snprintf(message, sizeof(message),
            "runs ordinary %u pipeline %u differential %u, %u fallbacks, "
            "last reason %u",
            knob_runs[COL_JOIN_BATCH_RUN_ORDINARY],
            knob_runs[COL_JOIN_BATCH_RUN_PIPELINE],
            knob_runs[COL_JOIN_BATCH_RUN_DIFFERENTIAL], knob_fallbacks,
            knob_reason);
        return message;
    }
    return NULL;
}

/* e, f and g for the three-atom chain: (7, 6) is the new row of e and
 * (6, 3) of f. */
static const data_t EFG[] = {
    { "e", 2, 3, { { 1, 2 }, { 5, 6 }, { 7, 6 } } },
    { "f", 2, 4, { { 2, 3 }, { 2, 20 }, { 6, 4 }, { 6, 3 } } },
    { "g", 2, 4, { { 3, 30 }, { 4, 40 }, { 1, 10 }, { 2, 20 } } },
};
static const char CHAIN3[] =
    ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
    ".decl g(x: int64, y: int64)\n.decl r(x: int64, v: int64)\n"
    "r(x, v) :- e(x, y), f(y, w), g(w, v).\n";

/* Issue #2201: a bound run's JOINs run the bounded producer under the
 * knob, with the rows the one-shot JOIN derives.  A JOIN followed by a
 * MAP that ends the instance runs on the pipeline route, with the right
 * operand FULL and with it the driven delta, and strict mode refuses
 * none.  In a three-atom chain the first JOIN feeds the second: it runs on
 * the ordinary route, with a FULL right operand and with the driven delta,
 * and is recorded as pipeline-not-consumable, which strict mode refuses
 * with ENOTSUP. */
static void
test_bound_runs_batch_joins(void)
{
    static const data_t chain[] = {
        { "e", 2, 3, { { 1, 2 }, { 2, 3 }, { 3, 4 } } },
    };
    static const data_t added = { "e", 2, 1, { { 2, 3 } } };
    static const int64_t self_want[][MAX_COLS] = { { 2, 4 }, { 1, 3 } };
    static const int64_t by_e[][MAX_COLS] = { { 7, 5 }, { 7, 4 } };
    static const int64_t by_f[][MAX_COLS] = { { 5, 4 }, { 7, 4 } };
    /* e(7, 6) reaches g(4, 40) and g(3, 30); f(6, 3) is reached from
     * e(5, 6) and e(7, 6) and reaches g(3, 30). */
    static const int64_t chain_e[][MAX_COLS] = { { 7, 40 }, { 7, 30 } };
    static const int64_t chain_f[][MAX_COLS] = { { 5, 30 }, { 7, 30 } };
    static const uint64_t bytes[] = { 0, 224, 224 };
    static const bool strict[] = { false, false, true };
    const uint8_t pipe = COL_JOIN_BATCH_EXCLUDED_PIPELINE;
    fixture_t f;
    col_rel_t *de = NULL, *df = NULL;

    BEGIN("bound runs batch their joins under the knob");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl m(x: int64, z: int64)\n"
        "m(x, z) :- e(x, y), e(y, z).\n", false, chain, 1));
    de = delta_of(COL_SESSION(f.session), "e", &added);
    if (!de) {
        FAIL("delta");
        goto done;
    }
    for (uint32_t k = 0; k < 3; k++)
        CHECK(knob_round(&f, "m", "e", de, bytes[k], strict[k], false, true,
            0, 0, self_want, 2, 2));
    col_rel_destroy(de);
    de = NULL;
    fixture_close(&f);

    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl t(x: int64, y: int64)\n"
        "t(x, w + 1) :- e(x, y), f(y, w), w < 10.\n", false, EF, 2));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    df = delta_of(COL_SESSION(f.session), "f", &NEW_F);
    if (!de || !df) {
        FAIL("delta");
        goto done;
    }
    for (uint32_t k = 0; k < 3; k++) {
        CHECK(knob_round(&f, "t", "e", de, bytes[k], strict[k], false, true,
            0, 0, by_e, 2, 2));
        CHECK(knob_round(&f, "t", "f", df, bytes[k], strict[k], false, true,
            0, 0, by_f, 2, 2));
    }
    col_rel_destroy(de);
    col_rel_destroy(df);
    de = df = NULL;
    fixture_close(&f);

    CHECK(fixture_open(&f, CHAIN3, false, EFG, 3));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    df = delta_of(COL_SESSION(f.session), "f", &NEW_F);
    if (!de || !df) {
        FAIL("delta");
        goto done;
    }
    for (uint32_t k = 0; k < 2; k++) {
        CHECK(knob_round(&f, "r", "e", de, bytes[k], false, true, true,
            pipe, 0, chain_e, 2, 2));
        CHECK(knob_round(&f, "r", "f", df, bytes[k], false, true, true,
            pipe, 0, chain_f, 2, 2));
    }
    CHECK(knob_round(&f, "r", "e", de, 224, true, false, false, 0, ENOTSUP,
        NULL, 0, 0));
    CHECK(knob_round(&f, "r", "f", df, 224, true, false, false, 0, ENOTSUP,
        NULL, 0, 0));
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    col_rel_destroy(df);
    fixture_close(&f);
}

/* Issue #2201: the bounded producer of a bound run reads the relation the
 * input slot holds, not the session relation of that name.  The FULL read
 * of f is bound to a private relation named f whose rows the session's f
 * does not have; on the pipeline route (a JOIN followed by a MAP) and on
 * the ordinary route (the first JOIN of a three-atom chain) the rows
 * derive from the private relation, with and without the knob. */
static void
test_bound_join_reads_slot_not_name(void)
{
    static const data_t private_f = { "f", 2, 2, { { 6, 1 }, { 6, 2 } } };
    /* e(7, 6) joins the private f(6, 1) and f(6, 2), not the session's
     * f(6, 4) and f(6, 3); in the chain those reach g(1, 10) and
     * g(2, 20). */
    static const int64_t joined[][MAX_COLS] = { { 7, 1 }, { 7, 2 } };
    static const int64_t chained[][MAX_COLS] = { { 7, 10 }, { 7, 20 } };
    static const uint64_t bytes[] = { 0, 224, 224 };
    static const bool strict[] = { false, false, true };
    fixture_t f;
    col_rel_t *de = NULL, *pf = NULL;

    BEGIN("bound join reads the slot's relation, not the name's");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl j(x: int64, w: int64)\n"
        "j(x, w) :- e(x, y), f(y, w).\n", false, EF, 2));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    pf = delta_of(COL_SESSION(f.session), "f", &private_f);
    if (!de || !pf) {
        FAIL("relations");
        goto done;
    }
    override_name = "f";
    override_rel = pf;
    for (uint32_t k = 0; k < 3; k++)
        CHECK(knob_round(&f, "j", "e", de, bytes[k], strict[k], false, true,
            0, 0, joined, 2, 2));
    override_name = NULL;
    override_rel = NULL;
    col_rel_destroy(de);
    de = NULL;
    col_rel_destroy(pf);
    pf = NULL;
    fixture_close(&f);

    CHECK(fixture_open(&f, CHAIN3, false, EFG, 3));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    pf = delta_of(COL_SESSION(f.session), "f", &private_f);
    if (!de || !pf) {
        FAIL("relations");
        goto done;
    }
    override_name = "f";
    override_rel = pf;
    for (uint32_t k = 0; k < 2; k++)
        CHECK(knob_round(&f, "r", "e", de, bytes[k], false, true, true,
            COL_JOIN_BATCH_EXCLUDED_PIPELINE, 0, chained, 2, 2));
    tests_passed++;
    printf(" ... PASS\n");
done:
    override_name = NULL;
    override_rel = NULL;
    col_rel_destroy(de);
    col_rel_destroy(pf);
    fixture_close(&f);
}

/* Issue #2201: an empty FULL right operand in a bound run is left to the
 * ordinary JOIN, which joins it in one pass without a producer, on the
 * pipeline's tail as on any session: no bounded run, no fallback, and an
 * empty result in record and strict mode. */
static void
test_bound_join_empty_right(void)
{
    static const data_t no_rows = { "f", 2, 0, { { 0 } } };
    static const bool strict[] = { false, true };
    fixture_t f;
    rows_t got;
    col_rel_t *de = NULL, *pf = NULL;

    BEGIN("bound join over an empty right operand runs one-shot");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl t(x: int64, y: int64)\n"
        "t(x, w + 1) :- e(x, y), f(y, w), w < 10.\n", false, EF, 2));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    pf = delta_of(COL_SESSION(f.session), "f", &no_rows);
    if (!de || !pf || pf->nrows != 0) {
        FAIL("relations");
        goto done;
    }
    override_name = "f";
    override_rel = pf;
    for (uint32_t k = 0; k < 2; k++) {
        knob_bytes = 224;
        knob_strict = strict[k];
        memset(knob_runs, 0, sizeof(knob_runs));
        knob_fallbacks = 0;
        const char *why = run_round(&f, "t", "e", de, &got);
        knob_bytes = 0;
        knob_strict = false;
        CHECK(why);
        CHECK(got.count == 0 ? NULL : "rows from an empty right operand");
        CHECK(knob_runs[COL_JOIN_BATCH_RUN_ORDINARY]
            + knob_runs[COL_JOIN_BATCH_RUN_PIPELINE] == 0
            && knob_fallbacks == 0
            ? NULL : "an empty right operand ran bounded or fell back");
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    override_name = NULL;
    override_rel = NULL;
    col_rel_destroy(de);
    col_rel_destroy(pf);
    fixture_close(&f);
}

/* Issue #2201: a batch budget below one output row is a fallback in a
 * bound run as on any session.  Without strict mode the JOIN runs one-shot
 * with the knob-off rows and records row-too-large last, on the pipeline's
 * tail and on the first JOIN of a three-atom chain alike; strict mode
 * refuses the instance with ENOTSUP. */
static void
test_bound_join_row_too_large(void)
{
    static const int64_t by_e[][MAX_COLS] = { { 7, 5 }, { 7, 4 } };
    static const int64_t chain_e[][MAX_COLS] = { { 7, 40 }, { 7, 30 } };
    const uint8_t row = COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE;
    fixture_t f;
    col_rel_t *de = NULL;

    BEGIN("bound join with a sub-row batch budget falls back");
    CHECK(fixture_open(&f,
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl t(x: int64, y: int64)\n"
        "t(x, w + 1) :- e(x, y), f(y, w), w < 10.\n", false, EF, 2));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    if (!de) {
        FAIL("delta");
        goto done;
    }
    CHECK(knob_round(&f, "t", "e", de, 8, false, false, false, row, 0,
        by_e, 2, 2));
    CHECK(knob_round(&f, "t", "e", de, 8, true, false, false, 0, ENOTSUP,
        NULL, 0, 0));
    col_rel_destroy(de);
    de = NULL;
    fixture_close(&f);

    CHECK(fixture_open(&f, CHAIN3, false, EFG, 3));
    de = delta_of(COL_SESSION(f.session), "e", &NEW_E);
    if (!de) {
        FAIL("delta");
        goto done;
    }
    CHECK(knob_round(&f, "r", "e", de, 8, false, false, false, row, 0,
        chain_e, 2, 2));
    CHECK(knob_round(&f, "r", "e", de, 8, true, false, false, 0, ENOTSUP,
        NULL, 0, 0));
    tests_passed++;
    printf(" ... PASS\n");
done:
    col_rel_destroy(de);
    fixture_close(&f);
}

int
main(void)
{
    printf("test_tdd_insert_run (Issue #2114)\n");
    test_expressions();
    test_constant();
    test_self_join();
    test_inline_compound();
    test_timestamped_head();
    test_runner_accepts_every_instance();
    test_refused_inputs();
    test_leading_ranges();
    test_range_refusals();
    test_bound_runs_batch_joins();
    test_bound_join_reads_slot_not_name();
    test_bound_join_empty_right();
    test_bound_join_row_too_large();
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
