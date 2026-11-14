/* #2301: EDBs whose rows come from inline facts, through controlled
 * sessions.
 *
 * Each program is run through a controlled session and an uncontrolled
 * session over the same plan, with the facts loaded into both or into
 * neither. A script of STEPs (or evaluating SNAPSHOTs), inserts and
 * removals is applied to both: every call must return the same code in both
 * sessions, and after each evaluation the head images must agree, as must
 * the observer events of an observed STEP. A controlled evaluating SNAPSHOT
 * that the general private-head transaction serves delivers observer events
 * (#2295) while the uncontrolled one fires none, so events are compared on
 * the STEP only. */
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

int
main(void)
{
    for (unsigned p = 0; p < sizeof(programs) / sizeof(programs[0]); p++)
        for (unsigned snapshot = 0; snapshot < 2; snapshot++)
            for (unsigned observed = 0; observed < 2; observed++)
                for (unsigned loaded = 0; loaded < 2; loaded++)
                    run_program(&programs[p], snapshot, observed, loaded);
    puts("evaluation_facts_sources: OK");
    return 0;
}
