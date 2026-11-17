/* #2301: EDBs whose rows come from inline facts, through controlled
 * sessions.
 *
 * Each program of programs[] is run through a controlled session and an
 * uncontrolled session over the same plan, with the facts loaded into both
 * or into neither. A script of STEPs (or evaluating SNAPSHOTs), inserts and
 * removals is applied to both: every call must return the same code in both
 * sessions, and after each evaluation the head images must agree, as must
 * the observer events of an observed STEP. A controlled evaluating SNAPSHOT
 * that the general private-head transaction serves delivers observer events
 * (#2295) while the uncontrolled one fires none, so events are compared on
 * the STEP only. Each head of head_input_programs[] has both inline facts
 * and a rule; head_input() checks the qualified STEP and refusal boundaries. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)

extern void (*wl_columnar_eval_stack_test_physical_publication)(
    wl_col_session_t *, const col_rel_t *, const col_rel_t *);

/* ---- captures ------------------------------------------------------------ */

#define MAXW 3u
#define MAXR 32u

typedef struct {
    int64_t v[MAXW];
    unsigned width;
    int diff;
} row_t;

typedef struct {
    row_t rows[MAXR];
    unsigned count;
    const char *head;
} capture_t;

static void
on_delta(const char *name, const int64_t *v, uint32_t width, int diff,
    void *data)
{
    capture_t *c = data;
    if (strcmp(name, c->head))
        return;
    CHECK(width <= MAXW && c->count < MAXR);
    row_t *r = &c->rows[c->count++];
    memset(r, 0, sizeof(*r));
    if (width)
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

static const char *where;

static void
equal_capture(capture_t *a, capture_t *b, const char *what)
{
    qsort(a->rows, a->count, sizeof(row_t), row_order);
    qsort(b->rows, b->count, sizeof(row_t), row_order);
    bool same = a->count == b->count;
    for (unsigned i = 0; same && i < a->count; i++)
        same = !row_order(&a->rows[i], &b->rows[i]);
    if (!same) {
        fprintf(stderr, "%s, %s: %u rows, other %u\n", where, what, a->count,
            b->count);
        for (unsigned i = 0; i < a->count; i++)
            fprintf(stderr, "  got w%u %lld %lld %lld diff%d\n",
                a->rows[i].width, (long long)a->rows[i].v[0],
                (long long)a->rows[i].v[1], (long long)a->rows[i].v[2],
                a->rows[i].diff);
        for (unsigned i = 0; i < b->count; i++)
            fprintf(stderr, "  other w%u %lld %lld %lld diff%d\n",
                b->rows[i].width, (long long)b->rows[i].v[0],
                (long long)b->rows[i].v[1], (long long)b->rows[i].v[2],
                b->rows[i].diff);
    }
    CHECK(same);
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

/* ---- publication witness ------------------------------------------------- */

static wl_session_t *active;
static unsigned map_inline;

static void
publication(wl_col_session_t *s, const col_rel_t *in, const col_rel_t *out)
{
    if (&s->base != active || strcmp(out->name, "$map") || !in)
        return;
    if (in->compound_kind == WIRELOG_COMPOUND_KIND_INLINE)
        map_inline++;
}

/* ---- scripts ------------------------------------------------------------- */

enum { OP_EVAL, OP_INSERT, OP_REMOVE, OP_END };

typedef struct {
    unsigned op;
    const char *rel;
    int64_t row[MAXW];
    unsigned width;
} op_t;

typedef struct {
    const char *text;
    const char *head;
    bool inline_source; /* "a" has an INLINE compound column */
    const op_t *script;
    const int64_t *first; /* the head after the first evaluation */
    unsigned first_rows, head_width;
} program_t;

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_columnar_memory_governor_ref_t *governor;
    wl_evaluation_control_t *control;
    wl_session_t *s, *raw;
} fixture_t;

static void
fixture(fixture_t *f, const char *text)
{
    memset(f, 0, sizeof(*f));
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
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
}

static void
fixture_destroy(fixture_t *f)
{
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
image(wl_session_t *s, const char *head, capture_t *c)
{
    memset(c, 0, sizeof(*c));
    c->head = head;
    CHECK(!wl_session_snapshot(s, on_tuple, c));
}

static void
run_program(const program_t *p, bool snapshot, bool observed, bool loaded)
{
    fixture_t f;
    fixture(&f, p->text);
    char label[96];
    snprintf(label, sizeof(label), "%s snapshot%u observed%u loaded%u",
        p->head, snapshot, observed, loaded);
    where = label;
    if (loaded) {
        CHECK(wl_session_load_facts(f.s, f.program) == 0);
        CHECK(wl_session_load_facts(f.raw, f.program) == 0);
    }
    if (p->inline_source) {
        col_rel_t *a = session_find_rel((wl_col_session_t *)f.s, "a");
        CHECK(a && a->compound_kind == WIRELOG_COMPOUND_KIND_INLINE);
        CHECK(a->nrows == (loaded ? 3u : 0u));
    }
    capture_t ev = {.head = p->head}, raw_ev = {.head = p->head};
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, &ev);
        wl_session_set_delta_cb(f.raw, on_delta, &raw_ev);
    }
    active = f.s; map_inline = 0;
    wl_columnar_eval_stack_test_physical_publication = publication;
    unsigned evals = 0;
    for (const op_t *op = p->script; op->op != OP_END; op++) {
        int rc, raw;
        if (op->op == OP_EVAL) {
            capture_t img = {.head = p->head}, raw_img = {.head = p->head};
            rc = snapshot ? wl_session_snapshot(f.s, on_tuple, &img)
                : step(f.s);
            raw = snapshot ? wl_session_snapshot(f.raw, on_tuple, &raw_img)
                : step(f.raw);
            if (rc || raw)
                fprintf(stderr, "%s eval %u: rc %d, raw %d\n", where, evals,
                    rc, raw);
            CHECK(!rc && !raw);
            if (snapshot)
                equal_capture(&img, &raw_img, "snapshot image");
            capture_t a, b;
            image(f.s, p->head, &a);
            image(f.raw, p->head, &b);
            equal_capture(&a, &b, "head image");
            if (!evals++ && loaded) {
                capture_t want = {.head = p->head};
                for (unsigned r = 0; r < p->first_rows; r++)
                    on_delta(p->head, &p->first[(size_t)r * p->head_width],
                        p->head_width, 1, &want);
                equal_capture(&a, &want, "first head");
            }
            continue;
        }
        const int64_t *row = op->row;
        if (op->op == OP_INSERT) {
            rc = wl_session_insert(f.s, op->rel, row, 1, op->width);
            raw = wl_session_insert(f.raw, op->rel, row, 1, op->width);
        } else {
            rc = wl_session_remove(f.s, op->rel, row, 1, op->width);
            raw = wl_session_remove(f.raw, op->rel, row, 1, op->width);
        }
        if (rc != raw)
            fprintf(stderr, "%s op %u on %s: rc %d, raw %d\n", where, op->op,
                op->rel, rc, raw);
        CHECK(rc == raw);
    }
    wl_columnar_eval_stack_test_physical_publication = NULL;
    if (p->inline_source && loaded)
        CHECK(map_inline >= 1);
    if (observed && !snapshot)
        equal_capture(&ev, &raw_ev, "events");
    fixture_destroy(&f);
}

/* ---- programs ------------------------------------------------------------ */

/* Flat facts in an EDB with an inline compound column: session creation
 * gives the EDB its INLINE layout before the facts load, so the facts are
 * stored as physical lanes and MAP reads an INLINE source. */
static const op_t inline_script[] = {
    {OP_EVAL, NULL, {0}, 0},
    {OP_INSERT, "a", {4, 1, 1}, 3},
    {OP_INSERT, "a", {1, 10, 20}, 3},
    {OP_EVAL, NULL, {0}, 0},
    {OP_REMOVE, "a", {2, 30, 40}, 3},
    {OP_EVAL, NULL, {0}, 0},
    {OP_END, NULL, {0}, 0},
};
static const int64_t inline_first[] = {1, 10, 20, 2, 30, 40, 3, -5, 6};

/* A nullary facts EDB, with and without a declaration of the head. */
static const op_t nullary_script[] = {
    {OP_EVAL, NULL, {0}, 0},
    {OP_INSERT, "flag", {0}, 0},
    {OP_EVAL, NULL, {0}, 0},
    {OP_REMOVE, "flag", {0}, 0},
    {OP_EVAL, NULL, {0}, 0},
    {OP_INSERT, "flag", {0}, 0},
    {OP_EVAL, NULL, {0}, 0},
    {OP_END, NULL, {0}, 0},
};
static const int64_t nullary_first[] = {0};

static const program_t programs[] = {
    {
        ".decl a(id:int64,p:pair/2 inline)\n"
        ".decl out(id:int64,x:int64,y:int64)\n"
        "a(1,10,20).\na(2,30,40).\na(3,-5,6).\n"
        "out(i,x,y) :- a(i,pair(x,y)).\n",
        "out", true, inline_script, inline_first, 3, 3,
    },
    {
        "flag().\nout() :- flag().\n",
        "out", false, nullary_script, nullary_first, 1, 0,
    },
    {
        ".decl out()\nflag().\nout() :- flag().\n",
        "out", false, nullary_script, nullary_first, 1, 0,
    },
};

/* Canonical seed heads can run an ordinary STEP with or without loaded facts.
 * Evaluating SNAPSHOT remains a separate refused route. */
static const char *const head_input_programs[] = {
    "a(1).\na(2).\nout(7).\nout(x) :- a(x).\n",
    ".decl a(x:int64)\n.decl out(x:int64)\n"
    "a(1).\na(2).\nout(7).\nout(x) :- a(x).\n",
    "a(1).\na(2).\n.decl out(x:int64)\nout(7).\nout(x) :- a(x).\n",
};

extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
extern int wl_columnar_session_test_seed_host_view(const wl_col_session_t *,
    uint64_t[32], wl_evaluation_control_t **);
static wl_evaluation_control_t *cutoff_control;
static wl_session_t *cutoff_session;
static col_rel_t *cutoff_shadow;
static unsigned cutoff_calls;

static void
cancel_head_cutoff(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)index;
    (void)bindings;
    if (phase != 14 || cutoff_calls)
        return;
    CHECK(control == cutoff_control && cutoff_shadow->nrows == 1
        && !cutoff_shadow->base_nrows);
    uint64_t seed[32];
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(cutoff_session),
        seed, NULL) && seed[1] && seed[3] && seed[4] && seed[9] == 2);
    cutoff_calls++;
    wl_evaluation_control_request_cancel(control);
}

static void
head_input(unsigned which, bool snapshot, bool observed, bool loaded)
{
    fixture_t f;
    fixture(&f, head_input_programs[which]);
    char label[64];
    snprintf(label, sizeof(label), "head input %u snapshot%u observed%u "
        "loaded%u", which, snapshot, observed, loaded);
    where = label;
    if (loaded) {
        CHECK(!wl_session_load_facts(f.s, f.program));
        CHECK(!wl_session_load_facts(f.raw, f.program));
    }
    capture_t ev = {.head = "out"}, raw_ev = {.head = "out"};
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, &ev);
        wl_session_set_delta_cb(f.raw, on_delta, &raw_ev);
    }
    bool supported = !snapshot;
    int refusal = EINVAL;
    col_rel_t *old = session_find_rel(COL_SESSION(f.s), "out");
    col_relation_snapshot_t old_image = wl_columnar_relation_snapshot(old);
    col_rel_t *shadow = wl_columnar_session_seed_rows(COL_SESSION(f.s), "out");
    if (supported && loaded && (which == 0 || (which == 1 && observed))) {
        CHECK(shadow && shadow->nrows == 1 && !shadow->base_nrows);
        cutoff_control = f.control;
        cutoff_session = f.s;
        cutoff_shadow = shadow;
        cutoff_calls = 0;
        wl_columnar_session_test_observed_transaction_checkpoint =
            cancel_head_cutoff;
        CHECK(step(f.s) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_columnar_session_test_observed_transaction_checkpoint = NULL;
        CHECK(cutoff_calls == 1 && !ev.count && !shadow->base_nrows);
        CHECK(session_find_rel(COL_SESSION(f.s), "out") == old
            && wl_columnar_relation_snapshot_equal(old_image,
            wl_columnar_relation_snapshot(old)));
        CHECK(!wl_evaluation_control_reset(f.control));
        cutoff_control = NULL;
        cutoff_session = NULL;
        cutoff_shadow = NULL;
    }
    static const int64_t first[] = {1, 2, 7}, second[] = {1, 2, 3, 7};
    for (unsigned round = 0; round < 3; round++) {
        if (round == 1) {
            const int64_t row[] = {3};
            CHECK(!wl_session_insert(f.raw, "a", row, 1, 1));
            if (supported)
                CHECK(!wl_session_insert(f.s, "a", row, 1, 1));
        }
        ev.count = raw_ev.count = 0;
        capture_t img = {.head = "out"};
        if (supported || !round) {
            int rc = snapshot ? wl_session_snapshot(f.s, on_tuple, &img)
                : step(f.s);
            CHECK(rc == (supported ? 0 : refusal) && !img.count);
            if (!supported) {
                CHECK(!ev.count);
                CHECK(session_find_rel(COL_SESSION(f.s), "out") == old);
                CHECK(wl_columnar_relation_snapshot_equal(old_image,
                    wl_columnar_relation_snapshot(old)));
            }
        }
        capture_t raw_img = {.head = "out"};
        CHECK(!(snapshot ? wl_session_snapshot(f.raw, on_tuple, &raw_img)
            : step(f.raw)));
        capture_t got, expect = {.head = "out"};
        image(f.raw, "out", &got);
        const int64_t *rows = round ? second : first;
        for (unsigned r = 0; r < (round ? 4u : 3u); r++)
            if (loaded || rows[r] == 3)
                on_delta("out", &rows[r], 1, 1, &expect);
        equal_capture(&got, &expect, "uncontrolled head");
        if (supported) {
            capture_t actual;
            image(f.s, "out", &actual);
            equal_capture(&actual, &expect, "controlled head");
            col_rel_t *a = session_find_rel(COL_SESSION(f.s), "out");
            col_rel_t *b = session_find_rel(COL_SESSION(f.raw), "out");
            CHECK(a->ncols == b->ncols && a->schema_ok == b->schema_ok
                && a->declared_ncols == b->declared_ncols);
            CHECK(shadow == wl_columnar_session_seed_rows(COL_SESSION(f.s),
                "out") && (!shadow || shadow->base_nrows == shadow->nrows));
            capture_t events = {.head = "out"};
            if (observed) {
                if (!round && loaded)
                    for (unsigned r = 0; r < 3; r++)
                        on_delta("out", &first[r], 1, 1, &events);
                else if (round == 1) {
                    const int64_t row[] = {3};
                    on_delta("out", row, 1, 1, &events);
                }
            }
            equal_capture(&ev, &events, "literal events");
            equal_capture(&ev, &raw_ev, "head events");
        }
    }
    /* Head writes still obey set difference when a seed overlaps a rule row. */
    if (supported && loaded) {
        const op_t writes[] = {
            {OP_INSERT, "out", {9}, 1}, {OP_REMOVE, "out", {9}, 1},
            {OP_INSERT, "out", {2}, 1}, {OP_REMOVE, "out", {2}, 1},
        };
        for (unsigned i = 0; i < sizeof(writes) / sizeof(*writes); i++) {
            const op_t *op = &writes[i];
            CHECK(!(op->op == OP_INSERT
                ? wl_session_insert(f.s, op->rel, op->row, 1, 1)
                : wl_session_remove(f.s, op->rel, op->row, 1, 1)));
            CHECK(!(op->op == OP_INSERT
                ? wl_session_insert(f.raw, op->rel, op->row, 1, 1)
                : wl_session_remove(f.raw, op->rel, op->row, 1, 1)));
            ev.count = raw_ev.count = 0;
            CHECK(!step(f.s) && !step(f.raw));
            capture_t actual, reference, expect = {.head = "out"};
            image(f.s, "out", &actual);
            image(f.raw, "out", &reference);
            for (unsigned r = 0; r < 4; r++)
                on_delta("out", &second[r], 1, 1, &expect);
            if (!i)
                on_delta("out", op->row, 1, 1, &expect);
            equal_capture(&actual, &expect, "head writes");
            equal_capture(&actual, &reference, "raw head writes");
            capture_t events = {.head = "out"};
            if (observed && i < 2)
                on_delta("out", op->row, 1, i ? -1 : 1, &events);
            equal_capture(&ev, &events, "head write events");
            equal_capture(&ev, &raw_ev, "raw head write events");
            CHECK(shadow->base_nrows == shadow->nrows);
        }
    }
    fixture_destroy(&f);
}

int
main(void)
{
    for (unsigned p = 0; p < sizeof(programs) / sizeof(programs[0]); p++)
        for (unsigned snapshot = 0; snapshot < 2; snapshot++)
            for (unsigned observed = 0; observed < 2; observed++)
                for (unsigned loaded = 0; loaded < 2; loaded++)
                    run_program(&programs[p], snapshot, observed, loaded);
    for (unsigned p = 0; p < 3; p++)
        for (unsigned snapshot = 0; snapshot < 2; snapshot++)
            for (unsigned observed = 0; observed < 2; observed++)
                for (unsigned loaded = 0; loaded < 2; loaded++)
                    head_input(p, snapshot, observed, loaded);
    puts("evaluation_facts_sources: OK");
    return 0;
}
