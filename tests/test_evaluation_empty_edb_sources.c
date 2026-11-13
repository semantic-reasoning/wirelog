/* #2301: never-inserted EDB sources through controlled sessions.
 *
 * Session creation gives no schema to an EDB without a FLOAT, STRING or
 * INLINE column, so until its first row VARIABLE borrows its descriptor
 * with width 0 and schema_ok false (#2297). Depending on the compiled body,
 * unobserved STEP either lends it to authentic empty-source readers or
 * constructs an authenticated private empty input. Both routes, observed
 * STEP and SNAPSHOT are checked for parity. Each
 * controlled result is compared with an uncontrolled session over the same
 * plan. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h> /* EAGAIN */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)

extern void (*wl_columnar_eval_stack_test_physical_publication)(
    wl_col_session_t *, const col_rel_t *, const col_rel_t *);

/* ---- captures ------------------------------------------------------------ */

typedef struct {
    int64_t v[2];
    unsigned width;
    int diff;
} row_t;

typedef struct {
    row_t rows[32];
    unsigned count;
} capture_t;

static void
on_delta(const char *name, const int64_t *v, uint32_t width, int diff,
    void *data)
{
    capture_t *c = data;
    if (strcmp(name, "out") != 0)
        return;
    CHECK(width && width <= 2 && c->count < 32);
    row_t *r = &c->rows[c->count++];
    memset(r, 0, sizeof(*r));
    memcpy(r->v, v, width * sizeof(*v));
    r->width = width;
    r->diff = diff;
}

static void
on_tuple(const char *name, const int64_t *v, uint32_t width, void *data)
{
    on_delta(name, v, width, 1, data);
}

static int
row_order(const void *a, const void *b)
{
    const row_t *x = a, *y = b;
    if (x->width != y->width)
        return (x->width > y->width) - (x->width < y->width);
    for (unsigned i = 0; i < x->width; i++)
        if (x->v[i] != y->v[i])
            return (x->v[i] > y->v[i]) - (x->v[i] < y->v[i]);
    return (x->diff > y->diff) - (x->diff < y->diff);
}

static void
equal_capture(capture_t *a, capture_t *b, const char *what, unsigned kind)
{
    qsort(a->rows, a->count, sizeof(row_t), row_order);
    qsort(b->rows, b->count, sizeof(row_t), row_order);
    bool same = a->count == b->count;
    for (unsigned i = 0; same && i < a->count; i++)
        same = !row_order(&a->rows[i], &b->rows[i]);
    if (!same)
        fprintf(stderr, "kind%u %s: %u rows, expected %u\n", kind, what,
            a->count, b->count);
    CHECK(same);
}

/* ---- programs ------------------------------------------------------------ */

enum {
    K_SINGLE, K_UNION_A, K_UNION_C, K_UNION_BOTH, K_FILTER, K_COUNT,
    K_GROUPED, K_TWO_COL, K_INT_EXPR, K_INT32, K_UINT32, K_UINT64, K_BOOL,
    K_FLOAT, K_FLOAT_EXPR, K_COUNT_KINDS
};

#define UNION_TEXT ".decl a(x:int64)\n.decl c(x:int64)\n.decl out(x:int64)\n" \
        "out(x) :- a(x).\nout(x) :- c(x).\n"

static const struct {
    const char *text;
    const char *bare[2];     /* EDBs left without rows */
    const char *insert;      /* EDB that gets rows, or NULL */
    wl_plan_op_type_t first; /* consumer after the first VARIABLE */
    const char *output;      /* that consumer's private output */
} cases[K_COUNT_KINDS] = {
    { ".decl a(x:int64)\n.decl out(x:int64)\nout(x) :- a(x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    { UNION_TEXT, { "a", NULL }, "c", WL_PLAN_OP_MAP, "$map" },
    { UNION_TEXT, { "c", NULL }, "a", WL_PLAN_OP_MAP, "$map" },
    { UNION_TEXT, { "a", "c" }, NULL, WL_PLAN_OP_MAP, "$map" },
    { ".decl a(x:int64)\n.decl out(x:int64)\nout(x) :- a(x), x > 1.\n",
      { "a", NULL }, NULL, WL_PLAN_OP_FILTER, "$filter" },
    { ".decl a(x:int64)\n.decl out(n:int64)\nout(count(x)) :- a(x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_REDUCE, "$reduce" },
    { ".decl a(k:int64,x:int64)\n.decl out(k:int64,n:int64)\n"
      "out(k,count(x)) :- a(k,x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_REDUCE, "$reduce" },
    { ".decl a(x:int64,y:int64)\n.decl out(y:int64,x:int64)\n"
      "out(y,x) :- a(x,y).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    /* An integer expression, which the observed routes read through the
     * general observed transaction (#2301). */
    { ".decl a(x:int64)\n.decl out(y:int64)\nout(x + 1) :- a(x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    { ".decl a(x:int32)\n.decl out(x:int32)\nout(x) :- a(x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    /* The parser takes no uint32, uint64 or bool declaration; the fixture
     * rewrites the plan's int32 lane of `a` to that type (lane_type). */
    { ".decl a(x:int32)\n.decl out(x:int32)\nout(x) :- a(x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    { ".decl a(x:int32)\n.decl out(x:int32)\nout(x) :- a(x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    { ".decl a(x:int32)\n.decl out(x:int32)\nout(x) :- a(x).\n",
      { "a", NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    /* A FLOAT-declared EDB has a schema from session creation, so it is
     * never bare; these are parity controls. */
    { ".decl a(x:float)\n.decl out(x:float)\nout(x) :- a(x).\n",
      { NULL, NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
    { ".decl a(x:float)\n.decl out(y:float)\nout(x + 1.25) :- a(x).\n",
      { NULL, NULL }, NULL, WL_PLAN_OP_MAP, "$map" },
};

static bool
is_union(unsigned kind)
{
    return kind == K_UNION_A || kind == K_UNION_C || kind == K_UNION_BOTH;
}

/* The sole-EDB VARIABLE,[FILTER],MAP,CONSOLIDATE missing-source route.
 * UNION and REDUCE retain their real bare-source readers on unobserved STEP. */
static bool
private_empty_step(unsigned kind)
{
    return kind == K_SINGLE || kind == K_FILTER || kind == K_TWO_COL
           || kind == K_INT_EXPR || kind == K_INT32 || kind == K_UINT32
           || kind == K_UINT64 || kind == K_BOOL;
}

static unsigned
bare_count(unsigned kind)
{
    return (cases[kind].bare[0] != NULL) + (cases[kind].bare[1] != NULL);
}

static void
check_body(const wl_plan_t *plan, unsigned kind)
{
    const wl_plan_relation_t *out = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; s++)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++)
            if (!strcmp(plan->strata[s].relations[r].name, "out"))
                out = &plan->strata[s].relations[r];
    CHECK(out && out->op_count >= 2);
    bool same = out->ops[0].op == WL_PLAN_OP_VARIABLE
        && out->ops[1].op == cases[kind].first;
    if (is_union(kind))
        same = same && out->op_count == 6
            && out->ops[2].op == WL_PLAN_OP_VARIABLE
            && out->ops[3].op == WL_PLAN_OP_MAP
            && out->ops[4].op == WL_PLAN_OP_CONCAT
            && out->ops[5].op == WL_PLAN_OP_CONSOLIDATE;
    if (!same) {
        fprintf(stderr, "kind%u body:", kind);
        for (uint32_t i = 0; i < out->op_count; i++)
            fprintf(stderr, " %d", (int)out->ops[i].op);
        fprintf(stderr, "\n");
    }
    CHECK(same);
}

/* The populated UNION branch derives {1} and {2}; every other body is empty. */
static const int64_t branch_rows[] = {1, 2, 2};

static void
literal(unsigned kind, capture_t *e)
{
    if (!is_union(kind) || kind == K_UNION_BOTH)
        return;
    for (unsigned i = 0; i < 2; i++)
        on_delta("out", &branch_rows[i], 1, 1, e);
}

/* ---- witnesses and stops --------------------------------------------------- */

static wl_session_t *active;
static const col_rel_t *bare_rel[2];
static unsigned bare_published;
static unsigned stop_mode; /* 0 none, 1 cancel, 2 work limit */
static const char *stop_at;
static bool stop_hit;

static void
request_stop(wl_evaluation_control_t *control)
{
    stop_hit = true;
    if (stop_mode == 1)
        wl_evaluation_control_request_cancel(control);
    else
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

static void
publication(wl_col_session_t *s, const col_rel_t *in, const col_rel_t *out)
{
    if (&s->base != active)
        return;
    /* A route that gives the EDB a schema first publishes it non-bare. */
    if (in && (in == bare_rel[0] || in == bare_rel[1]) && !in->schema_ok
        && !in->ncols && !in->nrows)
        bare_published++;
    if (stop_mode && stop_at && !stop_hit && !strcmp(out->name, stop_at))
        request_stop(s->base.evaluation_control);
}

/* Distinct empty-source records seen: VARIABLE's and the consumer's. */
static const void *records[8];
static unsigned record_count, empty_calls, empty_stop_at;
static wl_evaluation_control_t *empty_control;
static unsigned private_calls, private_stop_at, private_phases[64];

/* Source-derived constructor vector: INIT, descriptor/owner/header/recipe,
 * each real fixed run slot, then source drain and output publication. */
static void
private_trace(unsigned inputs)
{
    unsigned at = 0;
    for (unsigned input = 0; input < inputs; ++input) {
        for (unsigned phase = 620; phase <= 627; ++phase) {
            CHECK(at < private_calls && private_phases[at] == phase); ++at;
        }
        for (unsigned run = 0; run < COL_MAX_RUNS; ++run) {
            CHECK(at < private_calls && private_phases[at] == 628); ++at;
        }
        const unsigned final[] = {629, 631, 632};
        for (unsigned i = 0; i < 3; ++i) {
            CHECK(at < private_calls && private_phases[at] == final[i]); ++at;
        }
    }
    CHECK(at == private_calls);
}

static void
private_checkpoint(wl_evaluation_control_t *control, unsigned phase)
{
    if (control != empty_control) return;
    CHECK(wl_evaluation_control_active_attempt_id(control));
    CHECK(private_calls < sizeof(private_phases) / sizeof(private_phases[0]));
    private_phases[private_calls++] = phase;
    if (private_calls == private_stop_at && stop_mode && !stop_hit)
        request_stop(control);
}

static void
empty_checkpoint(const wl_columnar_relation_empty_source_t *a)
{
    bool known = false;
    for (unsigned i = 0; i < record_count; i++)
        known |= records[i] == (const void *)a;
    if (!known && record_count < 8)
        records[record_count++] = a;
    if (++empty_calls == empty_stop_at && stop_mode && !stop_hit)
        request_stop(empty_control);
}

static int
step(wl_session_t *s)
{
    for (unsigned i = 0; i < 1000000; i++) {
        int rc = wl_session_step(s);
        if (rc != EAGAIN)
            return rc;
    }
    CHECK(false);
    return EIO;
}

/* ---- fixture --------------------------------------------------------------- */

/* Declares the lane of `a` as UINT32, UINT64 or BOOL for those kinds, in
 * both the plan's lane types and its declaration schema. */
static void
lane_type(wl_plan_t *plan, unsigned kind)
{
    wirelog_column_type_t type;
    if (kind == K_UINT32)
        type = WIRELOG_TYPE_UINT32;
    else if (kind == K_UINT64)
        type = WIRELOG_TYPE_UINT64;
    else if (kind == K_BOOL)
        type = WIRELOG_TYPE_BOOL;
    else
        return;
    unsigned found = 0;
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        if (strcmp(plan->edb_relations[i], "a"))
            continue;
        CHECK(plan->edb_column_types && plan->edb_column_types[i]
            && plan->edb_column_type_counts[i] == 1
            && plan->edb_column_types[i][0] == WIRELOG_TYPE_INT32);
        ((wirelog_column_type_t *)plan->edb_column_types[i])[0] = type;
        if (plan->edb_schemas && plan->edb_schemas[i].columns) {
            CHECK(plan->edb_schemas[i].column_count == 1);
            ((wirelog_column_t *)plan->edb_schemas[i].columns)[0].type = type;
        }
        found++;
    }
    CHECK(found == 1);
}

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_columnar_memory_governor_ref_t *governor;
    wl_evaluation_control_t *control;
    wl_session_t *s, *raw;
} fixture_t;

static void
fixture(fixture_t *f, unsigned kind)
{
    memset(f, 0, sizeof(*f));
    wirelog_error_t error;
    f->program = wirelog_parse_string(cases[kind].text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    lane_type(f->plan, kind);
    check_body(f->plan, kind);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &f->control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->s));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1, &f->raw));
    if (cases[kind].insert) {
        CHECK(!wl_session_insert(f->s, cases[kind].insert, branch_rows, 3, 1));
        CHECK(!wl_session_insert(f->raw, cases[kind].insert, branch_rows, 3,
            1));
    }
    for (unsigned i = 0; i < 2; i++) {
        bare_rel[i] = NULL;
        if (!cases[kind].bare[i])
            continue;
        col_rel_t *r = session_find_rel((wl_col_session_t *)f->s,
                cases[kind].bare[i]);
        CHECK(r && !r->schema_ok && !r->ncols && !r->nrows
            && r->storage_owner == r);
        bare_rel[i] = r;
    }
    if (kind == K_FLOAT || kind == K_FLOAT_EXPR) {
        col_rel_t *r = session_find_rel((wl_col_session_t *)f->s, "a");
        CHECK(r && r->schema_ok && r->ncols == 1);
    }
}

static void
fixture_destroy(fixture_t *f)
{
    wl_columnar_eval_stack_test_physical_publication = NULL;
    wl_columnar_relation_test_empty_source_checkpoint = NULL;
    wl_columnar_relation_test_empty_input_checkpoint = NULL;
    CHECK(!wl_session_try_destroy(&f->s));
    wl_session_destroy(f->raw);
    wl_evaluation_control_release(f->control);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->governor)) == 0);
    wl_columnar_memory_governor_ref_release(f->governor);
}

static void
still_bare(const fixture_t *f, unsigned kind,
    const col_relation_snapshot_t *image)
{
    for (unsigned i = 0; i < 2; i++) {
        if (!cases[kind].bare[i])
            continue;
        col_rel_t *r = session_find_rel((wl_col_session_t *)f->s,
                cases[kind].bare[i]);
        CHECK(r == bare_rel[i] && !r->schema_ok && !r->ncols && !r->nrows);
        if (image)
            CHECK(wl_columnar_relation_snapshot_equal(image[i],
                wl_columnar_relation_snapshot(r)));
    }
}

/* The head has the uncontrolled session's width and column types. */
static const char *where = "";
static void
same_head(const fixture_t *f, unsigned kind)
{
    col_rel_t *a = session_find_rel((wl_col_session_t *)f->s, "out");
    col_rel_t *b = session_find_rel((wl_col_session_t *)f->raw, "out");
    CHECK((a == NULL) == (b == NULL));
    if (!a)
        return;
    if (a->ncols != b->ncols)
        fprintf(stderr, "kind%u head width %u, raw %u\n", kind, a->ncols,
            b->ncols);
    CHECK(a->ncols == b->ncols && a->nrows == b->nrows && !a->timestamps);
    for (uint32_t c = 0; c < a->ncols; c++) {
        wirelog_column_type_t x = a->column_types ? a->column_types[c]
            : WIRELOG_TYPE_INT64;
        wirelog_column_type_t y = b->column_types ? b->column_types[c]
            : WIRELOG_TYPE_INT64;
        if (x != y)
            fprintf(stderr, "kind%u %s: head column %u type %d, raw %d\n",
                kind, where, c, (int)x, (int)y);
        CHECK(x == y);
    }
}

/* The head's rows read from the relation itself. */
static void
head_capture(wl_session_t *s, capture_t *c)
{
    c->count = 0;
    col_rel_t *out = session_find_rel((wl_col_session_t *)s, "out");
    if (!out)
        return;
    CHECK(out->ncols && out->ncols <= 2 && out->nrows <= 32);
    for (uint32_t r = 0; r < out->nrows; r++) {
        int64_t v[2] = {0, 0};
        for (uint32_t col = 0; col < out->ncols; col++)
            v[col] = out->columns[col][r];
        on_delta("out", v, out->ncols, 1, c);
    }
}

/* ---- the matrix ------------------------------------------------------------ */

static void
run_cell(unsigned kind, bool snapshot, bool observed)
{
    fixture_t f;
    fixture(&f, kind);
    where = snapshot ? (observed ? "snapshot observed" : "snapshot")
        : (observed ? "step observed" : "step");
    capture_t ev = {0}, raw_ev = {0}, image = {0}, raw_image = {0};
    capture_t want = {0}, raw_want = {0};
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, &ev);
        wl_session_set_delta_cb(f.raw, on_delta, &raw_ev);
    }
    col_relation_snapshot_t images[2] = {{0}};
    for (unsigned i = 0; i < 2; i++)
        if (bare_rel[i])
            images[i] = wl_columnar_relation_snapshot(bare_rel[i]);
    active = f.s; bare_published = 0;
    record_count = empty_calls = empty_stop_at = 0; stop_mode = 0;
    private_calls = private_stop_at = 0;
    empty_control = f.control;
    wl_columnar_eval_stack_test_physical_publication = publication;
    wl_columnar_relation_test_empty_source_checkpoint = empty_checkpoint;
    wl_columnar_relation_test_empty_input_checkpoint = private_checkpoint;
    int rc = snapshot ? wl_session_snapshot(f.s, on_tuple, &image)
        : step(f.s);
    wl_columnar_eval_stack_test_physical_publication = NULL;
    wl_columnar_relation_test_empty_source_checkpoint = NULL;
    wl_columnar_relation_test_empty_input_checkpoint = NULL;
    if (rc)
        fprintf(stderr, "kind%u snapshot%u observed%u: rc %d\n", kind,
            snapshot, observed, rc);
    CHECK(rc == 0);
    CHECK(!(snapshot ? wl_session_snapshot(f.raw, on_tuple, &raw_image)
        : step(f.raw)));
    unsigned bare = bare_count(kind);
    if (!snapshot && !observed) {
        if (private_empty_step(kind)) {
            CHECK(record_count == 0 && bare_published == 0);
            private_trace(1);
        } else {
            CHECK(private_calls == 0);
            CHECK(record_count == (bare ? 2u : 0u));
            CHECK(bare_published == bare);
        }
        still_bare(&f, kind, images);
    } else {
        CHECK(!bare_published && !record_count);
        if (bare && (snapshot || kind != K_SINGLE)) private_trace(bare);
        else CHECK(private_calls == 0);
        if (!snapshot && (kind == K_SINGLE || kind == K_FILTER)) {
            col_rel_t *a = session_find_rel((wl_col_session_t *)f.s, "a");
            CHECK(kind == K_SINGLE ? a->ncols == 1 && a->schema_ok
                : !a->ncols && !a->schema_ok);
        }
    }
    literal(kind, &want);
    literal(kind, &raw_want);
    if (snapshot) {
        equal_capture(&image, &raw_image, "image against raw", kind);
        if (kind != K_COUNT && kind != K_GROUPED)
            equal_capture(&image, &want, "image", kind);
    }
    if (observed && !snapshot) {
        equal_capture(&ev, &raw_ev, "events against raw", kind);
        if (kind != K_COUNT && kind != K_GROUPED)
            equal_capture(&ev, &want, "events", kind);
    }
    same_head(&f, kind);

    /* Later operations: repeat with nothing new, then the first rows. */
    if (!snapshot && !observed) {
        CHECK(!step(f.s) && !step(f.raw));
        still_bare(&f, kind, images);
        const int64_t rows[] = {5, 6, 7, 8};
        unsigned width = kind == K_GROUPED || kind == K_TWO_COL ? 2u : 1u;
        for (unsigned i = 0; i < 2; i++) {
            if (!cases[kind].bare[i] || kind == K_FLOAT
                || kind == K_FLOAT_EXPR)
                continue;
            CHECK(!wl_session_insert(f.s, cases[kind].bare[i], rows,
                4 / width, width));
            CHECK(!wl_session_insert(f.raw, cases[kind].bare[i], rows,
                4 / width, width));
        }
        int first = step(f.s), raw_first = step(f.raw);
        if (first || raw_first)
            fprintf(stderr, "kind%u first rows: rc %d, raw %d\n", kind, first,
                raw_first);
        CHECK(!first && !raw_first);
        capture_t a = {0}, b = {0};
        head_capture(f.s, &a);
        head_capture(f.raw, &b);
        equal_capture(&a, &b, "head after first rows", kind);
        CHECK(bare == 0 || a.count > 0);
        same_head(&f, kind);
    }
    fixture_destroy(&f);
}

/* ---- deterministic stops on the unobserved STEP ---------------------------- */

static void
stopped(unsigned kind, unsigned mode, const char *at, unsigned empty_at,
    unsigned private_at)
{
    fixture_t f;
    fixture(&f, kind);
    where = "after a stop";
    col_relation_snapshot_t images[2] = {{0}};
    for (unsigned i = 0; i < 2; i++)
        if (bare_rel[i])
            images[i] = wl_columnar_relation_snapshot(bare_rel[i]);
    col_rel_t *old = session_find_rel((wl_col_session_t *)f.s, "out");
    col_relation_snapshot_t old_image = wl_columnar_relation_snapshot(old);
    active = f.s; bare_published = 0;
    record_count = empty_calls = 0; empty_stop_at = empty_at;
    private_calls = 0; private_stop_at = private_at;
    empty_control = f.control;
    stop_mode = mode; stop_at = at; stop_hit = false;
    wl_columnar_eval_stack_test_physical_publication = publication;
    wl_columnar_relation_test_empty_source_checkpoint = empty_checkpoint;
    wl_columnar_relation_test_empty_input_checkpoint = private_checkpoint;
    int rc = step(f.s);
    wl_columnar_eval_stack_test_physical_publication = NULL;
    wl_columnar_relation_test_empty_source_checkpoint = NULL;
    wl_columnar_relation_test_empty_input_checkpoint = NULL;
    int expected = mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
        : WL_EVALUATION_CONTROL_WORK_LIMIT;
    if (rc != expected || !stop_hit)
        fprintf(stderr,
            "kind%u stop%u at %s/reader%u/private%u: rc %d hit %d\n", kind,
            mode,
            at ? at : "-", empty_at, private_at, rc, stop_hit);
    CHECK(stop_hit && rc == expected);
    still_bare(&f, kind, images);
    CHECK(session_find_rel((wl_col_session_t *)f.s, "out") == old);
    if (old)
        CHECK(wl_columnar_relation_snapshot_equal(old_image,
            wl_columnar_relation_snapshot(old)));
    wl_evaluation_control_report_t report = {0}, again = {0};
    CHECK(!wl_evaluation_control_report(f.control, &report));
    CHECK(report.result == expected && report.stop_reason == expected
        && !report.cleanup_status);
    CHECK(!wl_evaluation_control_report(f.control, &again));
    CHECK(!memcmp(&report, &again, sizeof(report)));
    CHECK(!wl_evaluation_control_reset(f.control));
    stop_mode = 0;
    CHECK(!step(f.s) && !step(f.raw));
    still_bare(&f, kind, images);
    same_head(&f, kind);
    /* Both readers were drained: the first insert is admitted. */
    const int64_t rows[] = {9, 10};
    unsigned width = kind == K_GROUPED || kind == K_TWO_COL ? 2u : 1u;
    CHECK(!wl_session_insert(f.s, cases[kind].bare[0], rows, 2 / width,
        width));
    CHECK(!wl_session_insert(f.raw, cases[kind].bare[0], rows, 2 / width,
        width));
    CHECK(!step(f.s) && !step(f.raw));
    capture_t a = {0}, b = {0};
    head_capture(f.s, &a);
    head_capture(f.raw, &b);
    equal_capture(&a, &b, "head after a stop", kind);
    CHECK(a.count > 0);
    fixture_destroy(&f);
}

/* An EDB declared with INT32, UINT32, UINT64 or BOOL lanes, and no FLOAT,
 * STRING or compound column, is untyped once it has rows, as in the
 * uncontrolled session: the first call sees rows inserted before it, the
 * next a further insert with one repeated row, the last nothing new. The
 * head matches the uncontrolled session in rows, width and type. The INT64
 * integer-expression body runs the same rounds. */
static void
untyped_rows(unsigned kind, bool snapshot, bool observed)
{
    fixture_t f;
    fixture(&f, kind);
    where = snapshot ? "untyped rows snapshot" : "untyped rows step";
    capture_t ev = {0}, raw_ev = {0};
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, &ev);
        wl_session_set_delta_cb(f.raw, on_delta, &raw_ev);
    }
    const int64_t rows[] = {1, 0, 1};
    for (unsigned round = 0; round < 3; round++) {
        if (round < 2) {
            const int64_t *r = round ? rows + 1 : rows;
            uint32_t n = round ? 2u : 1u;
            CHECK(!wl_session_insert(f.s, "a", r, n, 1));
            CHECK(!wl_session_insert(f.raw, "a", r, n, 1));
        }
        capture_t image = {0}, raw_image = {0};
        int rc = snapshot ? wl_session_snapshot(f.s, on_tuple, &image)
            : step(f.s);
        int raw_rc = snapshot ? wl_session_snapshot(f.raw, on_tuple,
                &raw_image) : step(f.raw);
        if (rc || raw_rc)
            fprintf(stderr, "%s kind%u observed%u round %u: rc %d, raw %d\n",
                where, kind, observed, round, rc, raw_rc);
        CHECK(!rc && !raw_rc);
        if (snapshot)
            equal_capture(&image, &raw_image, "untyped image", kind);
        capture_t a = {0}, b = {0};
        head_capture(f.s, &a);
        head_capture(f.raw, &b);
        equal_capture(&a, &b, "untyped head", kind);
        CHECK(a.count == (round ? 2u : 1u));
        same_head(&f, kind);
    }
    if (observed && !snapshot)
        equal_capture(&ev, &raw_ev, "untyped events", kind);
    fixture_destroy(&f);
}

static unsigned
empty_calls_of(unsigned kind, unsigned *constructor_calls)
{
    fixture_t f;
    fixture(&f, kind);
    record_count = empty_calls = empty_stop_at = 0; stop_mode = 0;
    private_calls = private_stop_at = 0; empty_control = f.control;
    wl_columnar_relation_test_empty_source_checkpoint = empty_checkpoint;
    wl_columnar_relation_test_empty_input_checkpoint = private_checkpoint;
    CHECK(!step(f.s));
    wl_columnar_relation_test_empty_source_checkpoint = NULL;
    wl_columnar_relation_test_empty_input_checkpoint = NULL;
    unsigned n = empty_calls;
    *constructor_calls = private_calls;
    if (private_empty_step(kind)) {
        CHECK(n == 0 && record_count == 0); private_trace(1);
    } else CHECK(private_calls == 0);
    fixture_destroy(&f);
    return n;
}

static void
run_matrix(void)
{
    for (unsigned kind = 0; kind < K_COUNT_KINDS; kind++)
        for (unsigned snapshot = 0; snapshot < 2; snapshot++)
            for (unsigned observed = 0; observed < 2; observed++)
                run_cell(kind, snapshot, observed);
    for (unsigned kind = K_INT32; kind < K_FLOAT; kind++)
        for (unsigned snapshot = 0; snapshot < 2; snapshot++)
            for (unsigned observed = 0; observed < 2; observed++)
                untyped_rows(kind, snapshot, observed);
    for (unsigned snapshot = 0; snapshot < 2; snapshot++)
        for (unsigned observed = 0; observed < 2; observed++)
            untyped_rows(K_INT_EXPR, snapshot, observed);
}

static void
run_stops(unsigned kind)
{
    for (unsigned mode = 1; mode < 3; mode++) {
        /* At the consumer's publication, or at CONCAT for a UNION. */
        if (cases[kind].first != WL_PLAN_OP_REDUCE)
            stopped(kind, mode, is_union(kind) ? "$concat"
                    : cases[kind].output, 0, 0);
        unsigned constructor_calls = 0;
        unsigned n = empty_calls_of(kind, &constructor_calls);
        if (private_empty_step(kind)) {
            CHECK(n == 0 && constructor_calls > 0);
            /* Stop each real source/recipe/drain/output boundary. */
            for (unsigned at = 1; at <= constructor_calls; at++)
                stopped(kind, mode, NULL, 0, at);
        } else {
            CHECK(constructor_calls == 0 && n >= 2 * bare_count(kind));
            /* VARIABLE and the consumer still hold the real bare EDB. */
            for (unsigned at = 1; at <= n; at++)
                stopped(kind, mode, NULL, at, 0);
        }
    }
}

static int
usage(const char *program)
{
    fprintf(stderr, "usage: %s [--matrix | --stops KIND(0..12)]\n", program);
    return 2;
}

int
main(int argc, char **argv)
{
    if (argc == 1) {
        run_matrix();
        for (unsigned kind = 0; kind < K_FLOAT; ++kind)
            run_stops(kind);
    } else if (argc == 2 && !strcmp(argv[1], "--matrix")) {
        run_matrix();
    } else if (argc == 3 && !strcmp(argv[1], "--stops")) {
        /* Accept at most two decimal digits, before creating any fixture. */
        const char *value = argv[2];
        if (value[0] < '0' || value[0] > '9') return usage(argv[0]);
        unsigned kind = (unsigned)(value[0] - '0');
        if (value[1]) {
            if (value[1] < '0' || value[1] > '9' || value[2])
                return usage(argv[0]);
            kind = kind * 10u + (unsigned)(value[1] - '0');
        }
        if (kind >= K_FLOAT) return usage(argv[0]);
        run_stops(kind);
    } else return usage(argv[0]);
    puts("evaluation_empty_edb_sources: OK");
    return 0;
}
