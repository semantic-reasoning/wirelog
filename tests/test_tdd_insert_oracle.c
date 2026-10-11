/*
 * test_tdd_insert_oracle.c - Issue #2120: the insertion executor against
 * full evaluation over existing programs.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * wl_columnar_eval_tdd_insert_stratum() brings a stratum's heads up to date
 * after rows were appended to relations it reads.  This test drives it the
 * way session_snapshot_insert() does -- strata in plan order, each with every
 * input relation that gained rows and every head delta an earlier stratum
 * returned as a changed relation -- and requires each head to equal what a
 * full evaluation over the same rows derives.
 *
 * Programs: the aggregate-free workloads under bench/workloads whose data is
 * in the tree (cc and sssp aggregate; cspa's rows are capped to keep the run
 * short), the programs of test_tdd_insert_stratum.c and
 * test_incremental_snapshot_recursive.c that read no compounds, and
 * bipartite.dl over an odd cycle.  Each runs with the fusion and JPP passes,
 * with and without SIP, in sessions created with one worker and with four;
 * the oracle is a one-worker snapshot planned the same way.
 *
 * Two modes:
 *  - from empty: the session is evaluated once with no input rows, then
 *    every input row is the changed delta.  Heads start empty, except that
 *    rows a program states as facts of its own heads (reach, bipartite) and
 *    rows a rule head reads from a file (polonius) are evaluated there too,
 *    since the executor never treats a stratum's own head as changed.  Here
 *    every input's delta is its whole relation, so a defect that reads a
 *    changed relation's delta where it should read the whole relation goes
 *    unseen.
 *  - split: half of each input's rows are evaluated first and the other half
 *    is the changed delta, which separates the two.  Only for programs whose
 *    every stratum is admitted: a refused stratum is evaluated here by
 *    col_eval_stratum() on top of what its heads hold, so this test requires
 *    them empty, as a full evaluation leaves them after clearing its heads.
 *
 * A stratum the snapshot's insert route would refuse (negation, aggregates,
 * an operator the binder cannot bind) is evaluated with col_eval_stratum(),
 * and its heads are passed on as changed relations.  Each program states
 * whether all, some or none of its strata are admitted, so that a binder
 * that refuses everything cannot pass.  The executor runs its instances on
 * one worker either way; the worker count changes the evaluations around it.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define MAX_INPUTS 32
#define MAX_HEADS 32
#define MAX_COLS 16
#define MAX_EMBEDDED 8

typedef enum {
    ADMIT_ALL,  /* every stratum is admitted */
    ADMIT_SOME, /* at least one admitted and at least one refused */
    ADMIT_NONE, /* every stratum is refused */
} admit_t;

typedef struct {
    const char *name;
    uint32_t nrows;
    int64_t row[MAX_EMBEDDED][2];
} embedded_t;

typedef struct {
    const char *name;
    const char *workload;  /* bench/workloads/<workload>.dl, or NULL */
    const char *datadir;   /* inputs from bench/data/<datadir>/<rel>.csv */
    const char *edge_file; /* or every input from bench/data/<edge_file> */
    uint32_t max_rows;     /* rows read per file; 0 reads them all */
    const char *src;       /* the program, when workload is NULL */
    embedded_t rows[2];
    uint32_t nrels;
    admit_t with_sip;
    admit_t without_sip;
} case_t;

#define E2(n) ".decl " n "(x: int64, y: int64)\n"

static const case_t CASES[] = {
    { "andersen", "andersen", "andersen", NULL, 0, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    { "csda", "csda", "csda", NULL, 0, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    /* Capped: the whole data set takes seconds per session. */
    { "cspa", "cspa", "cspa", NULL, 60, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    { "dyck2", "dyck2", "dyck", NULL, 0, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    /* With SIP the binder refuses galen's SEMIJOIN, which is not a
     * prefilter there; without it every stratum is admitted. */
    { "galen", "galen", "galen", NULL, 0, NULL, { { 0 } }, 0,
      ADMIT_NONE, ADMIT_ALL },
    { "tc", "tc", NULL, "graph_100.csv", 0, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    { "reach", "reach", NULL, "graph_100.csv", 0, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    { "sg", "sg", NULL, "tree_100.csv", 0, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    { "bipartite", "bipartite", NULL, "tree_100.csv", 0, NULL, { { 0 } }, 0,
      ADMIT_ALL, ADMIT_ALL },
    { "polonius", "polonius", "polonius", NULL, 0, NULL, { { 0 } }, 0,
      ADMIT_SOME, ADMIT_SOME },
    { "ddisasm", "ddisasm", "ddisasm", NULL, 0, NULL, { { 0 } }, 0,
      ADMIT_SOME, ADMIT_SOME },
    { "crdt", "crdt", "crdt-small", NULL, 0, NULL, { { 0 } }, 0,
      ADMIT_SOME, ADMIT_SOME },
    /* The programs of test_tdd_insert_stratum.c, with "closure over
     * overlapping rows" and "recursion feeding a downstream join" from
     * test_incremental_snapshot_recursive.c; old and new rows together. */
    { "shape A", NULL, NULL, NULL, 0,
      E2("e") E2("a") "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n",
      { { "e", 2, { { 1, 2 }, { 2, 3 } } } }, 1, ADMIT_ALL, ADMIT_ALL },
    { "shape B", NULL, NULL, NULL, 0,
      E2("e") E2("s") E2("a")
      "a(x, z) :- e(x, y), e(y, z).\na(x, z) :- a(x, y), s(y, z).\n",
      { { "e", 2, { { 1, 2 }, { 2, 3 } } }, { "s", 1, { { 3, 4 } } } }, 2,
      ADMIT_ALL, ADMIT_ALL },
    { "shape C", NULL, NULL, NULL, 0,
      E2("e") E2("f") E2("a")
      "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, w), a(w, z).\n",
      { { "f", 2, { { 2, 3 }, { 3, 4 } } },
        { "e", 2, { { 9, 9 }, { 1, 2 } } } }, 2, ADMIT_ALL, ADMIT_ALL },
    { "mutual recursion", NULL, NULL, NULL, 0,
      E2("e") E2("p") E2("q")
      "p(x, y) :- e(x, y).\np(x, z) :- q(x, y), e(y, z).\n"
      "q(x, y) :- p(x, y).\n",
      { { "e", 2, { { 1, 2 }, { 2, 3 } } } }, 1, ADMIT_ALL, ADMIT_ALL },
    { "new rows joining new rows", NULL, NULL, NULL, 0,
      E2("e") E2("tc")
      "tc(x, y) :- e(x, y).\ntc(x, z) :- tc(x, y), tc(y, z).\n",
      { { "e", 4, { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 4, 5 } } } }, 1,
      ADMIT_ALL, ADMIT_ALL },
    { "closure over overlapping rows", NULL, NULL, NULL, 0,
      E2("e") E2("tc")
      "tc(x, y) :- e(x, y).\ntc(x, z) :- tc(x, y), tc(y, z).\n",
      { { "e", 7, { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 2, 3 },
                    { 4, 5 }, { 6, 7 } } } }, 1, ADMIT_ALL, ADMIT_ALL },
    { "one-atom filter rule", NULL, NULL, NULL, 0,
      E2("e") E2("a")
      "a(x, y) :- e(x, y), x < y.\na(x, z) :- a(x, y), e(y, z).\n",
      { { "e", 4, { { 1, 2 }, { 5, 4 }, { 2, 3 }, { 4, 1 } } } }, 1,
      ADMIT_ALL, ADMIT_ALL },
    { "non-recursive join", NULL, NULL, NULL, 0,
      E2("e") E2("f") ".decl m(x: int64, z: int64)\n"
      "m(x, z) :- e(x, y), f(y, z).\n",
      { { "e", 3, { { 1, 2 }, { 7, 3 }, { 1, 2 } } },
        { "f", 2, { { 2, 5 }, { 3, 6 } } } }, 2, ADMIT_ALL, ADMIT_ALL },
    { "recursion feeding a downstream join", NULL, NULL, NULL, 0,
      E2("e") E2("a") E2("b")
      "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n"
      "b(x, z) :- a(x, y), e(y, z).\n",
      { { "e", 3, { { 1, 2 }, { 3, 4 }, { 2, 3 } } } }, 1,
      ADMIT_ALL, ADMIT_ALL },
    /* The program test_tdd_insert_stratum.c expects to be refused, with its
     * old rows only. */
    { "negation of an input", NULL, NULL, NULL, 0,
      E2("e") E2("s") E2("p") "p(x, y) :- s(x, y), !e(x, y).\n",
      { { "e", 1, { { 1, 2 } } }, { "s", 2, { { 1, 2 }, { 3, 4 } } } }, 2,
      ADMIT_NONE, ADMIT_NONE },
    /* bipartite.dl over an odd cycle, so that its downstream stratum
     * derives rows: tree_100 is bipartite. */
    { "bipartite over an odd cycle", NULL, NULL, NULL, 0,
      E2("edge") ".decl blue(x: int64)\n.decl red(x: int64)\n"
      ".decl not_bipartite(x: int64)\n"
      "blue(1).\nred(y) :- blue(x), edge(x, y).\n"
      "blue(y) :- red(x), edge(x, y).\n"
      "not_bipartite(x) :- blue(x), red(x).\n",
      { { "edge", 4, { { 1, 2 }, { 2, 3 }, { 3, 1 }, { 3, 4 } } } }, 1,
      ADMIT_ALL, ADMIT_ALL },
};

/* One input relation's rows, row-major. */
typedef struct {
    char name[64];
    uint32_t ncols;
    uint32_t nrows;
    int64_t *v;
} input_t;

static input_t inputs[MAX_INPUTS];
static uint32_t ninputs;

static char message[512];

static void
noop_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
}

/* The file at @path, with its block comments blanked out: the parser reads
 * line comments only, and some workloads open with a block comment. */
static char *
read_program(const char *path)
{
    FILE *f = fopen(path, "rb");
    char *buf = NULL;
    long n;

    if (!f)
        return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (n = ftell(f)) < 0
        || fseek(f, 0, SEEK_SET) != 0)
        goto done;
    buf = (char *)malloc((size_t)n + 1);
    if (!buf || fread(buf, 1, (size_t)n, f) != (size_t)n) {
        free(buf);
        buf = NULL;
        goto done;
    }
    buf[n] = '\0';
    for (char *p = buf; *p; p++) {
        if (p[0] != '/' || p[1] != '*')
            continue;
        char *end = strstr(p + 2, "*/");
        char *stop = end ? end + 2 : p + strlen(p);
        for (; p < stop; p++)
            if (*p != '\n')
                *p = ' ';
        p--;
    }
done:
    fclose(f);
    return buf;
}

/* Read up to @max_rows rows (0: all) of a headerless comma-separated file of
 * integers into @in. */
static bool
read_csv(const char *path, uint32_t max_rows, input_t *in)
{
    FILE *f = fopen(path, "r");
    char line[1024];
    size_t cap = 0, used = 0;

    in->ncols = 0;
    in->nrows = 0;
    in->v = NULL;
    if (!f)
        return false;
    while (fgets(line, sizeof(line), f)) {
        int64_t vals[MAX_COLS];
        uint32_t c = 0;
        char *s = line, *end;

        if (line[0] == '\n' || line[0] == '\r' || line[0] == '\0')
            continue;
        for (;;) {
            if (c == MAX_COLS)
                goto bad;
            vals[c++] = strtoll(s, &end, 10);
            if (end == s)
                goto bad;
            if (*end != ',')
                break;
            s = end + 1;
        }
        if (*end != '\n' && *end != '\r' && *end != '\0')
            goto bad;
        if (in->ncols == 0)
            in->ncols = c;
        if (c != in->ncols)
            goto bad;
        if (used + c > cap) {
            size_t ncap = cap ? cap * 2 : 1024;
            int64_t *nv = (int64_t *)realloc(in->v, ncap * sizeof(*nv));
            if (!nv)
                goto bad;
            in->v = nv;
            cap = ncap;
        }
        memcpy(in->v + used, vals, c * sizeof(*vals));
        used += c;
        in->nrows++;
        if (max_rows && in->nrows == max_rows)
            break;
    }
    fclose(f);
    return in->nrows > 0;
bad:
    fclose(f);
    free(in->v);
    in->v = NULL;
    return false;
}

static void
free_inputs(void)
{
    for (uint32_t i = 0; i < ninputs; i++)
        free(inputs[i].v);
    ninputs = 0;
}

/* Fill inputs[] for @c: the embedded rows, or every relation @prog reads
 * from a file, from the case's data.  NULL on success. */
static const char *
load_inputs(const case_t *c, const char *bench,
    const struct wirelog_program *prog)
{
    char path[1024];

    ninputs = 0;
    if (!c->workload) {
        for (uint32_t r = 0; r < c->nrels; r++) {
            const embedded_t *e = &c->rows[r];
            input_t *in = &inputs[ninputs++];
            snprintf(in->name, sizeof(in->name), "%s", e->name);
            in->ncols = 2;
            in->nrows = e->nrows;
            in->v = (int64_t *)malloc(sizeof(int64_t) * 2 * e->nrows);
            if (!in->v)
                return "memory";
            memcpy(in->v, e->row, sizeof(int64_t) * 2 * e->nrows);
        }
        return NULL;
    }
    for (uint32_t r = 0; r < prog->relation_count; r++) {
        if (!prog->relations[r].has_input)
            continue;
        if (ninputs == MAX_INPUTS)
            return "too many inputs for the test";
        input_t *in = &inputs[ninputs];
        snprintf(in->name, sizeof(in->name), "%s", prog->relations[r].name);
        if (c->edge_file)
            snprintf(path, sizeof(path), "%s/data/%s", bench, c->edge_file);
        else
            snprintf(path, sizeof(path), "%s/data/%s/%s.csv", bench,
                c->datadir, in->name);
        if (!read_csv(path, c->max_rows, in)) {
            snprintf(message, sizeof(message), "cannot read %s", path);
            return message;
        }
        ninputs++;
    }
    return ninputs ? NULL : "no inputs";
}

static int
open_session(const char *src, bool sip, uint32_t workers, wl_plan_t **plan,
    wl_session_t **sess)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);

    *plan = NULL;
    *sess = NULL;
    if (!prog)
        return -1;
    plan_fixture_hold(prog);
    /* Fusion, JPP and optionally SIP, as other tests of the insert route
     * plan; the session pipeline may apply further passes. */
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    if (sip)
        wl_sip_apply(prog, NULL);
    if (wl_plan_from_program(prog, plan) != 0)
        return -1;
    if (wl_session_create(wl_backend_columnar(), *plan, workers, sess) != 0)
        return -1;
    return wl_session_load_facts(*sess, prog);
}

static bool
is_head(const wl_plan_t *plan, const char *name)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++)
            if (strcmp(sp->relations[ri].name, name) == 0)
                return true;
    }
    return false;
}

/* A relation's rows, row-major and sorted. */
typedef struct {
    uint32_t ncols;
    uint32_t nrows;
    int64_t *v;
} rows_t;

static uint32_t sort_ncols;

static int
row_cmp(const void *a, const void *b)
{
    const int64_t *x = (const int64_t *)a, *y = (const int64_t *)b;
    for (uint32_t c = 0; c < sort_ncols; c++)
        if (x[c] != y[c])
            return x[c] < y[c] ? -1 : 1;
    return 0;
}

/* @r's rows sorted, or empty when @r is absent or empty.  Returns false
 * only when out of memory. */
static bool
rows_load(const col_rel_t *r, rows_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!r || r->nrows == 0)
        return true;
    out->ncols = r->ncols;
    out->nrows = r->nrows;
    out->v = (int64_t *)malloc(sizeof(int64_t) * r->ncols * r->nrows + 1);
    if (!out->v)
        return false;
    for (uint32_t i = 0; i < r->nrows; i++)
        for (uint32_t c = 0; c < r->ncols; c++)
            out->v[(size_t)i * r->ncols + c] = r->columns[c][i];
    sort_ncols = out->ncols;
    qsort(out->v, out->nrows, sizeof(int64_t) * out->ncols, row_cmp);
    return true;
}

static bool
rows_has_duplicate(const rows_t *r)
{
    sort_ncols = r->ncols;
    for (uint32_t i = 1; i < r->nrows; i++)
        if (row_cmp(r->v + (size_t)i * r->ncols,
            r->v + (size_t)(i - 1) * r->ncols) == 0)
            return true;
    return false;
}

static void
rows_dedup(rows_t *r)
{
    uint32_t w = 0;

    sort_ncols = r->ncols;
    for (uint32_t i = 0; i < r->nrows; i++) {
        if (w > 0 && row_cmp(r->v + (size_t)i * r->ncols,
            r->v + (size_t)(w - 1) * r->ncols) == 0)
            continue;
        memmove(r->v + (size_t)w * r->ncols, r->v + (size_t)i * r->ncols,
            sizeof(int64_t) * r->ncols);
        w++;
    }
    r->nrows = w;
}

static bool
rows_equal(const rows_t *a, const rows_t *b)
{
    if (a->nrows != b->nrows)
        return false;
    if (a->nrows == 0)
        return true;
    return a->ncols == b->ncols
           && memcmp(a->v, b->v, sizeof(int64_t) * a->ncols * a->nrows) == 0;
}

/* @rows minus @base, both sorted and unique. */
static void
rows_minus(rows_t *rows, const rows_t *base)
{
    uint32_t w = 0, j = 0;

    sort_ncols = rows->ncols;
    for (uint32_t i = 0; i < rows->nrows; i++) {
        const int64_t *row = rows->v + (size_t)i * rows->ncols;
        int cmp = 1;
        while (j < base->nrows
            && (cmp = row_cmp(base->v + (size_t)j * base->ncols, row)) < 0)
            j++;
        if (j < base->nrows && cmp == 0)
            continue;
        memmove(rows->v + (size_t)w * rows->ncols, row,
            sizeof(int64_t) * rows->ncols);
        w++;
    }
    rows->nrows = w;
}

/* May the snapshot's insert route evaluate @sp?  The same questions
 * session_snapshot_delta_safe() and session_snapshot_insert_admitted() ask. */
static bool
admitted(const wl_plan_stratum_t *sp)
{
    if (!sp->is_monotone || sp->has_aggregate)
        return false;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        for (int round = 0; round < (sp->is_recursive ? 2 : 1); round++) {
            wl_columnar_eval_tdd_plan_manifest_t m;
            int rc = wl_columnar_eval_tdd_plan_insert_bindings(sp, ri, NULL,
                    0, round == 0, &m);
            wl_columnar_eval_tdd_plan_bindings_free(&m);
            if (rc != 0)
                return false;
        }
    }
    return true;
}

/* An untimed private copy of @r's rows [from, nrows). */
static col_rel_t *
copy_rows(wl_col_session_t *coord, const col_rel_t *r, uint32_t from)
{
    col_rel_t *d = NULL;

    if (wl_columnar_relation_new_like_governed_checked_mode(r->name, r,
        coord->memory_governor, false, &d) != 0)
        return NULL;
    for (uint32_t i = from; i < r->nrows; i++) {
        if (col_rel_append_row(d, col_rel_row(r, i)) != 0) {
            col_rel_destroy(d);
            return NULL;
        }
    }
    return d;
}

/* The full evaluation the session under test must reach: every input row,
 * one worker.  Inputs that are rule heads go in after a first evaluation,
 * as in the session under test, where they cannot go in before one. */
static const char *
open_oracle(const char *src, bool sip, wl_plan_t **plan, wl_session_t **o)
{
    bool head_inputs = false;

    if (open_session(src, sip, 1, plan, o) != 0)
        return "oracle session";
    for (uint32_t i = 0; i < ninputs; i++)
        head_inputs |= is_head(*plan, inputs[i].name);
    if (head_inputs && wl_session_snapshot(*o, noop_tuple, NULL) != 0)
        return "oracle first evaluation";
    for (uint32_t i = 0; i < ninputs; i++)
        if (wl_session_insert(*o, inputs[i].name, inputs[i].v,
            inputs[i].nrows, inputs[i].ncols) != 0)
            return "oracle insert";
    if (wl_session_snapshot(*o, noop_tuple, NULL) != 0)
        return "oracle evaluation";
    return NULL;
}

/* Rows of input @i that the baseline evaluates: none from empty, the first
 * half when split. */
static uint32_t
baseline_rows(uint32_t i, bool split)
{
    return split ? inputs[i].nrows / 2 : 0;
}

static const char *
run_mode(const char *src, bool sip, bool split,
    uint32_t workers, wl_session_t *oracle, admit_t expect)
{
    wl_plan_t *plan = NULL;
    wl_session_t *s = NULL;
    wl_col_session_t *coord;
    const char **names = NULL;
    col_rel_t **deltas = NULL;
    rows_t *base = NULL;
    uint32_t nbase = 0, nchanged = 0, capacity = 0;
    uint32_t nadmitted = 0, nrefused = 0;
    bool head_inputs = false;
    const char *why = NULL;
    int rc;

    if (open_session(src, sip, workers, &plan, &s) != 0) {
        why = "session";
        goto done;
    }
    coord = COL_SESSION(s);

    /* Baseline: the split half of every input that is not a rule head. */
    for (uint32_t i = 0; i < ninputs; i++) {
        uint32_t n = baseline_rows(i, split);
        if (is_head(plan, inputs[i].name)) {
            head_inputs = true;
            continue;
        }
        if (n > 0 && wl_session_insert(s, inputs[i].name, inputs[i].v, n,
            inputs[i].ncols) != 0) {
            why = "baseline insert";
            goto done;
        }
    }
    if (wl_session_snapshot(s, noop_tuple, NULL) != 0) {
        why = "baseline evaluation";
        goto done;
    }
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            if (!session_find_rel(coord, sp->relations[ri].name)) {
                snprintf(message, sizeof(message), "head %s is not "
                    "registered after the baseline evaluation",
                    sp->relations[ri].name);
                why = message;
                goto done;
            }
        }
        nbase += sp->relation_count;
        capacity += sp->relation_count;
    }
    /* Inputs that are rule heads, whole, in the baseline. */
    if (head_inputs) {
        for (uint32_t i = 0; i < ninputs; i++)
            if (is_head(plan, inputs[i].name)
                && wl_session_insert(s, inputs[i].name, inputs[i].v,
                inputs[i].nrows, inputs[i].ncols) != 0) {
                why = "head input insert";
                goto done;
            }
        if (wl_session_snapshot(s, noop_tuple, NULL) != 0) {
            why = "baseline evaluation with the head inputs";
            goto done;
        }
    }
    base = (rows_t *)calloc(nbase ? nbase : 1, sizeof(*base));
    capacity += ninputs;
    names = (const char **)calloc(capacity ? capacity : 1, sizeof(*names));
    deltas = (col_rel_t **)calloc(capacity ? capacity : 1, sizeof(*deltas));
    if (!base || !names || !deltas) {
        why = "memory";
        goto done;
    }
    {
        uint32_t k = 0;
        for (uint32_t si = 0; si < plan->stratum_count; si++) {
            const wl_plan_stratum_t *sp = &plan->strata[si];
            for (uint32_t ri = 0; ri < sp->relation_count; ri++, k++) {
                if (!rows_load(session_find_rel(coord,
                    sp->relations[ri].name), &base[k])) {
                    why = "memory";
                    goto done;
                }
                rows_dedup(&base[k]);
            }
        }
    }

    /* The changed rows, appended the incremental way. */
    for (uint32_t i = 0; i < ninputs; i++) {
        uint32_t from = baseline_rows(i, split);
        col_rel_t *rel;
        if (is_head(plan, inputs[i].name) || from == inputs[i].nrows)
            continue;
        rc = col_session_insert_incremental(s, inputs[i].name,
                inputs[i].v + (size_t)from * inputs[i].ncols,
                inputs[i].nrows - from, inputs[i].ncols);
        rel = session_find_rel(coord, inputs[i].name);
        if (rc != 0 || !rel) {
            why = "incremental insert";
            goto done;
        }
        deltas[nchanged] = copy_rows(coord, rel, rel->base_nrows);
        if (!deltas[nchanged]
            || deltas[nchanged]->nrows != inputs[i].nrows - from) {
            col_rel_destroy(deltas[nchanged]);
            deltas[nchanged] = NULL;
            why = "changed rows";
            goto done;
        }
        names[nchanged++] = inputs[i].name;
    }

    /* As the snapshot does before evaluating: no frontier recorded by the
     * baseline may skip an iteration of a refused stratum. */
    for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA; si++)
        coord->frontier_ops->reset_stratum_frontier(coord, si,
            coord->outer_epoch);
    for (uint32_t ri = 0; ri < MAX_RULES; ri++)
        coord->frontier_ops->reset_rule_frontier(coord, ri,
            coord->outer_epoch);
    coord->diff_operators_active = false;

    for (uint32_t si = 0, k = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        col_rel_t *head_delta[MAX_HEADS] = { NULL };
        bool adm = admitted(sp);

        if (sp->relation_count > MAX_HEADS) {
            why = "stratum too wide for the test";
            goto done;
        }
        if (adm) {
            nadmitted++;
            rc = wl_columnar_eval_tdd_insert_stratum(coord, sp, names,
                    (col_rel_t *const *)deltas, nchanged, head_delta);
            if (rc != 0) {
                snprintf(message, sizeof(message), "stratum %u (%s): "
                    "insert_stratum rc %d", si, sp->relations[0].name, rc);
                why = message;
                goto done;
            }
        } else {
            nrefused++;
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                if (base[k + ri].nrows) {
                    snprintf(message, sizeof(message), "harness: refused "
                        "head %s has baseline rows", sp->relations[ri].name);
                    why = message;
                    goto done;
                }
            }
            rc = col_eval_stratum(sp, coord, si);
            if (rc != 0) {
                snprintf(message, sizeof(message), "stratum %u (%s): "
                    "col_eval_stratum rc %d", si, sp->relations[0].name, rc);
                why = message;
                goto done;
            }
            if (coord->eval_arena)
                wl_arena_reset(coord->eval_arena);
        }
        for (uint32_t ri = 0; ri < sp->relation_count; ri++, k++) {
            const char *hn = sp->relations[ri].name;
            col_rel_t *head = session_find_rel(coord, hn);
            rows_t got, want, added;
            bool ok = true;

            if (!rows_load(head, &got)
                || !rows_load(session_find_rel(COL_SESSION(oracle), hn),
                &want)) {
                free(got.v);
                ok = false;
            }
            if (!ok) {
                snprintf(message, sizeof(message), "memory");
                for (uint32_t r = ri; r < sp->relation_count; r++)
                    col_rel_destroy(head_delta[r]);
                why = message;
                goto done;
            }
            rows_dedup(&want);
            if (adm && rows_has_duplicate(&got)) {
                snprintf(message, sizeof(message), "stratum %u: %s holds a "
                    "row twice", si, hn);
                ok = false;
            }
            if (!adm)
                rows_dedup(&got);
            if (ok && !rows_equal(&got, &want)) {
                snprintf(message, sizeof(message), "stratum %u%s: %s has %u "
                    "rows, a full evaluation %u", si,
                    adm ? "" : " (refused)", hn, got.nrows, want.nrows);
                ok = false;
            }
            if (ok && adm) {
                /* The returned delta: exactly the rows the head gained. */
                rows_minus(&want, &base[k]);
                if (!rows_load(head_delta[ri], &added)) {
                    snprintf(message, sizeof(message), "memory");
                    ok = false;
                } else if ((head_delta[ri] && head_delta[ri]->timestamps)
                    || !rows_equal(&added, &want)) {
                    snprintf(message, sizeof(message), "stratum %u: %s "
                        "delta has %u rows, the head gained %u", si, hn,
                        added.nrows, want.nrows);
                    ok = false;
                }
                free(added.v);
            }
            free(got.v);
            free(want.v);
            if (!ok) {
                why = message;
                for (uint32_t r = ri; r < sp->relation_count; r++)
                    col_rel_destroy(head_delta[r]);
                goto done;
            }
            /* What later strata read as changed: the delta, or a refused
             * stratum's whole head, which was empty before. */
            if (!adm && head && head->nrows)
                head_delta[ri] = copy_rows(coord, head, 0);
            if (head_delta[ri]) {
                names[nchanged] = hn;
                deltas[nchanged++] = head_delta[ri];
            } else if (!adm && head && head->nrows) {
                why = "memory";
                goto done;
            }
        }
    }
    if ((expect == ADMIT_ALL && nrefused != 0)
        || (expect == ADMIT_SOME && (nadmitted == 0 || nrefused == 0))
        || (expect == ADMIT_NONE && nadmitted != 0)) {
        snprintf(message, sizeof(message), "%u strata admitted and %u "
            "refused, not what the case states", nadmitted, nrefused);
        why = message;
    }
done:
    for (uint32_t i = 0; deltas && i < nchanged; i++)
        col_rel_destroy(deltas[i]);
    for (uint32_t k = 0; base && k < nbase; k++)
        free(base[k].v);
    free(base);
    free((void *)names);
    free((void *)deltas);
    if (s)
        wl_session_destroy(s);
    wl_plan_free(plan);
    return why;
}

static void
report(const case_t *c, bool sip, bool split, uint32_t workers,
    const char *why)
{
    tests_run++;
    printf("  [%d] %s, %s SIP, %s (%u worker%s)", tests_run, c->name,
        sip ? "with" : "without", split ? "split" : "from empty", workers,
        workers == 1 ? "" : "s");
    if (why) {
        tests_failed++;
        printf(" ... FAIL: %s\n", why);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

static void
run_case(const case_t *c, const char *bench)
{
    static const uint32_t workers[] = { 1, 4 };
    char path[1024];
    char *src = NULL;
    const char *why = NULL;

    if (c->workload) {
        snprintf(path, sizeof(path), "%s/workloads/%s.dl", bench,
            c->workload);
        src = read_program(path);
        if (!src) {
            snprintf(message, sizeof(message), "cannot read %s", path);
            why = message;
        }
    } else {
        src = (char *)malloc(strlen(c->src) + 1);
        if (src)
            memcpy(src, c->src, strlen(c->src) + 1);
        else
            why = "memory";
    }
    if (!why) {
        wirelog_error_t err;
        wirelog_program_t *prog = wirelog_parse_string(src, &err);
        if (!prog) {
            why = "parse";
        } else {
            why = load_inputs(c, bench, prog);
            wirelog_program_free(prog);
        }
    }
    for (int sip = 1; sip >= 0; sip--) {
        admit_t expect = sip ? c->with_sip : c->without_sip;
        wl_plan_t *oplan = NULL;
        wl_session_t *oracle = NULL;
        const char *owhy = why ? why : open_oracle(src, sip, &oplan, &oracle);
        for (int split = 0; split <= (expect == ADMIT_ALL); split++)
            for (uint32_t w = 0; w < 2; w++)
                report(c, sip, split, workers[w], owhy ? owhy
                    : run_mode(src, sip, split, workers[w], oracle,
                    expect));
        if (oracle)
            wl_session_destroy(oracle);
        wl_plan_free(oplan);
    }
    free_inputs();
    free(src);
}

int
main(void)
{
    const char *bench = getenv("WIRELOG_TEST_BENCH_DIR");

    printf("test_tdd_insert_oracle (Issue #2120)\n");
    if (!bench || !*bench) {
        printf("WIRELOG_TEST_BENCH_DIR is not set\n");
        return 1;
    }
    for (uint32_t i = 0; i < sizeof(CASES) / sizeof(CASES[0]); i++)
        run_case(&CASES[i], bench);
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
