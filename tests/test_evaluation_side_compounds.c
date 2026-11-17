/* #2301: side compound EDBs in controlled single-worker sessions.
 *
 * A column declared `pair/2 side` holds the handle that
 * wl_session_make_compound returns; the compound's arguments are a row of
 * the side relation `__compound_pair_2`, and a body that destructures the
 * column joins with that relation. This test runs such programs through a
 * controlled session (an evaluation control, a minimum-budget enforcing
 * governor, one worker) and an uncontrolled one with the same operations.
 *
 * make_compound after a completed controlled evaluation: the test checks
 * that it returns 0 after a completed STEP, a completed SNAPSHOT and a
 * completed observed STEP, also when the side relation does not exist yet,
 * and that a later evaluation reads the new compound, both through a row
 * inserted with it and through a row inserted before it with the handle it
 * was then given. On the recursive edge host and on the observed
 * single-rule host it checks EBUSY beside an insert those hosts admit in
 * the same state.
 *
 * The qualification corpus runs bodies that copy the handle column,
 * destructure it, filter on a field, match a constant or a repeated field,
 * join it after a flat atom, UNION it with a flat EDB, read it beside an
 * inline column, COUNT over it and close it transitively, with STEPs,
 * SNAPSHOTs, an observed STEP, removals and a row whose handle no compound
 * holds. After each evaluation both sessions return 0 and hold the image a
 * model of the rows and compounds gives, with the copy head's handles read
 * back as compound indexes; an observed STEP delivers the model's change.
 * After each operation, and after each EAGAIN return except in the
 * transitive closure, every relation registered in the controlled session
 * owns its storage and has no timestamps, pool or arena ownership, shared
 * column or alias borrow; every publication the physical publication hook
 * observes has such an input and output, and an output without a compound
 * layout. Both sessions hold e, the copy head and `__compound_pair_2` with
 * no compound layout, the side relation with one row per compound made.
 * A destructuring case publishes a `$join`. At the first EAGAIN return of
 * a marked evaluation, make_compound and an insert return EBUSY and the
 * side relation is unchanged. A cancellation and a work-limit stop at the
 * first publication of listed operators of a first or second evaluation
 * are checked: the stop code, a clean walk, an unchanged side relation, and
 * the same code for an insert and a make_compound after the stop and after
 * the reset, which both sessions then take when it is 0; the retry is
 * checked as above. The checker is tested on synthetic descriptors, and an
 * EDB with an inline column and no side column carries the INLINE layout.
 * The parser refuses a fact and a rule head that write a side compound
 * term.
 *
 * Not checked here: more than one worker; cold and spill sources;
 * memory-budget denials; plans the controlled preflight refuses;
 * make_compound inside a delta callback; failed-retry custody, which the
 * compound admission refuses; side compounds of other functors and
 * arities; and handle values across sessions, which the test does not
 * compare. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)

static const char *where = "";

/* ---- sessions ------------------------------------------------------------ */

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_columnar_memory_governor_ref_t *governor;
    wl_evaluation_control_t *control;
    wl_session_t *s;   /* controlled */
    wl_session_t *raw; /* uncontrolled */
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
    wl_session_destroy(f->s);
    wl_session_destroy(f->raw);
    wl_evaluation_control_release(f->control);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->governor)) == 0);
    wl_columnar_memory_governor_ref_release(f->governor);
}

/* ---- images and events --------------------------------------------------- */

#define MAX_ROWS 64

typedef struct {
    const char *head;
    uint32_t count;
    int64_t rows[MAX_ROWS][4];
    int diff[MAX_ROWS];
} image_t;

static void
image_add(image_t *m, const int64_t *v, uint32_t width, int diff)
{
    CHECK(width <= 4 && m->count < MAX_ROWS);
    memset(m->rows[m->count], 0, sizeof(m->rows[m->count]));
    memcpy(m->rows[m->count], v, width * sizeof(*v));
    m->diff[m->count] = diff;
    m->count++;
}

static void
on_tuple(const char *name, const int64_t *v, uint32_t width, void *data)
{
    image_t *m = data;
    if (m && !strcmp(name, m->head))
        image_add(m, v, width, 1);
}

static image_t *events;

static void
on_delta(const char *name, const int64_t *v, uint32_t width, int diff,
    void *data)
{
    (void)data;
    if (events && !strcmp(name, events->head))
        image_add(events, v, width, diff);
}

static int
row_cmp(const void *a, const void *b)
{
    return memcmp(a, b, 4 * sizeof(int64_t));
}

/* Sort rows with their diffs, then compare. */
static void
image_sort(image_t *m)
{
    for (uint32_t i = 1; i < m->count; i++)
        for (uint32_t j = i; j > 0; j--) {
            int c = row_cmp(m->rows[j - 1], m->rows[j]);
            if (c < 0 || (c == 0 && m->diff[j - 1] <= m->diff[j]))
                break;
            int64_t t[4];
            memcpy(t, m->rows[j], sizeof(t));
            memcpy(m->rows[j], m->rows[j - 1], sizeof(t));
            memcpy(m->rows[j - 1], t, sizeof(t));
            int d = m->diff[j];
            m->diff[j] = m->diff[j - 1];
            m->diff[j - 1] = d;
        }
}

static bool
image_equal(image_t *a, image_t *b)
{
    image_sort(a);
    image_sort(b);
    if (a->count != b->count)
        return false;
    for (uint32_t i = 0; i < a->count; i++)
        if (row_cmp(a->rows[i], b->rows[i]) || a->diff[i] != b->diff[i])
            return false;
    return true;
}

static void
image_print(const char *label, const image_t *m)
{
    fprintf(stderr, "%s: %s %s (%u):", where, label, m->head, m->count);
    for (uint32_t i = 0; i < m->count; i++)
        fprintf(stderr, " (%lld,%lld,%lld)%+d", (long long)m->rows[i][0],
            (long long)m->rows[i][1], (long long)m->rows[i][2], m->diff[i]);
    fprintf(stderr, "\n");
}

/* The bound turns a livelock into a prompt CHECK failure. */
static int
until_done(wl_session_t *s, bool snapshot, image_t *m)
{
    for (unsigned i = 0; i < 200000; i++) {
        int rc = snapshot ? wl_session_snapshot(s, on_tuple, m)
            : wl_session_step(s);
        if (rc != EAGAIN)
            return rc;
    }
    CHECK(false);
    return EIO;
}

/* ---- compounds ----------------------------------------------------------- */

static int
make_pair(wl_session_t *s, int64_t x, int64_t y, uint64_t *handle)
{
    wirelog_compound_arg_t args[2] = {
        {WIRELOG_TYPE_INT64, x}, {WIRELOG_TYPE_INT64, y}
    };
    *handle = UINT64_C(123);
    return wl_session_make_compound(s, "pair", 2, args, handle);
}

static uint32_t
side_rows(wl_session_t *s)
{
    const col_rel_t *r = session_find_rel((wl_col_session_t *)s,
            "__compound_pair_2");
    return r ? r->nrows : 0;
}

#define DESTR ".decl e(id:int64,h:pair/2 side)\n" \
        ".decl out(id:int64,x:int64,y:int64)\n" \
        "out(i,x,y) :- e(i,pair(x,y)).\n"

/* Evaluate both sessions (@how: 's' STEP, 'S' SNAPSHOT) and require 0 and
 * the literal image @want of @n rows (x of id i is 10 * i, y is x + 1), and
 * with an observer the literal events @ev of @nev rows. */
static void
evaluate(fixture_t *f, char how, const int64_t *want, uint32_t n,
    const int64_t *ev, const int *evd, uint32_t nev, bool observed)
{
    image_t expect = {.head = "out"}, expect_ev = {.head = "out"};
    for (uint32_t i = 0; i < n; i++)
        image_add(&expect, (int64_t[3]){want[i], 10 * want[i],
                                        10 * want[i] + 1}, 3, 1);
    for (uint32_t i = 0; i < nev; i++)
        image_add(&expect_ev, (int64_t[3]){ev[i], 10 * ev[i],
                                           10 * ev[i] + 1}, 3, evd[i]);
    wl_session_t *both[2] = {f->s, f->raw};
    for (int k = 0; k < 2; k++) {
        image_t ev_seen = {.head = "out"}, img = {.head = "out"};
        events = &ev_seen;
        CHECK(!until_done(both[k], how == 'S', &img));
        events = NULL;
        if (how == 's') {
            img.count = 0;
            CHECK(!until_done(both[k], true, &img));
        }
        if (!image_equal(&img, &expect)) {
            image_print(k ? "uncontrolled" : "controlled", &img);
            image_print("expected", &expect);
        }
        CHECK(image_equal(&img, &expect));
        if (observed && how == 's') {
            if (!image_equal(&ev_seen, &expect_ev)) {
                image_print(k ? "uncontrolled events" : "controlled events",
                    &ev_seen);
                image_print("expected events", &expect_ev);
            }
            CHECK(image_equal(&ev_seen, &expect_ev));
        }
    }
}

/* make_compound pair(10 * id, 10 * id + 1) in both sessions, require 0,
 * and insert e(id, handle) unless @insert is false. */
static void
make_and_insert(fixture_t *f, int64_t id, bool insert, uint64_t handles[2])
{
    wl_session_t *both[2] = {f->s, f->raw};
    for (int k = 0; k < 2; k++) {
        uint32_t before = side_rows(both[k]);
        int rc = make_pair(both[k], 10 * id, 10 * id + 1, &handles[k]);
        if (rc)
            fprintf(stderr, "%s: %s make_compound %d\n", where,
                k ? "uncontrolled" : "controlled", rc);
        CHECK(rc == 0 && handles[k] != WIRELOG_COMPOUND_HANDLE_NULL);
        CHECK(side_rows(both[k]) == before + 1);
        if (insert)
            CHECK(!wl_session_insert(both[k], "e",
                (int64_t[2]){id, (int64_t)handles[k]}, 1, 2));
    }
}

/* After each kind of completed controlled evaluation, make_compound
 * returns 0 and the next evaluation reads the compound. */
static void
after_completion(char first, bool observed, bool make_first)
{
    fixture_t f;
    fixture(&f, DESTR);
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, NULL);
        wl_session_set_delta_cb(f.raw, on_delta, NULL);
    }
    uint64_t h[2];
    static const int plus[] = {1, 1, 1};
    if (make_first) {
        make_and_insert(&f, 1, true, h);
        evaluate(&f, first, (int64_t[]){1}, 1, (int64_t[]){1}, plus, 1,
            observed);
    } else {
        /* The side relation does not exist when the evaluation completes. */
        CHECK(!session_find_rel((wl_col_session_t *)f.s,
            "__compound_pair_2"));
        evaluate(&f, first, NULL, 0, NULL, NULL, 0, observed);
        make_and_insert(&f, 1, true, h);
        evaluate(&f, 's', (int64_t[]){1}, 1, (int64_t[]){1}, plus, 1,
            observed);
    }
    make_and_insert(&f, 2, true, h);
    evaluate(&f, 's', (int64_t[]){1, 2}, 2, (int64_t[]){2}, plus, 1,
        observed);
    /* A second make_compound with no insert between the evaluations. */
    make_and_insert(&f, 3, true, h);
    make_and_insert(&f, 4, false, h);
    CHECK(!wl_session_insert(f.s, "e", (int64_t[2]){4, (int64_t)h[0]}, 1, 2));
    CHECK(!wl_session_insert(f.raw, "e", (int64_t[2]){4, (int64_t)h[1]}, 1,
        2));
    evaluate(&f, first, (int64_t[]){1, 2, 3, 4}, 4, (int64_t[]){3, 4}, plus,
        2, observed);
    fixture_destroy(&f);
}

static wl_controlled_step_state_t
step_state(wl_session_t *s)
{
    return s->backend->session_controlled_step_state(s);
}

/* A row inserted with a handle no compound holds yet derives nothing; the
 * make_compound that later yields that handle, with no insert after it,
 * makes the next evaluation derive the row. Handles here are the arena's
 * consecutive allocations within one epoch: the third is predicted from
 * the first two in each session. */
static void
forward_reference(bool observed)
{
    fixture_t f;
    fixture(&f, DESTR);
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, NULL);
        wl_session_set_delta_cb(f.raw, on_delta, NULL);
    }
    uint64_t h0[2], h1[2], h2[2];
    make_and_insert(&f, 1, true, h0);
    make_and_insert(&f, 2, false, h1);
    static const int plus[] = {1};
    evaluate(&f, 's', (int64_t[]){1}, 1, (int64_t[]){1}, plus, 1, observed);
    wl_session_t *both[2] = {f.s, f.raw};
    uint64_t next[2];
    for (int k = 0; k < 2; k++) {
        next[k] = h1[k] + (h1[k] - h0[k]);
        CHECK(!wl_session_insert(both[k], "e",
            (int64_t[2]){3, (int64_t)next[k]}, 1, 2));
    }
    evaluate(&f, 's', (int64_t[]){1}, 1, NULL, NULL, 0, observed);
    wl_controlled_step_state_t state = step_state(f.s);
    if (state != WL_CONTROLLED_STEP_COMPLETE)
        fprintf(stderr, "%s: state %d after the evaluation\n", where,
            (int)state);
    CHECK(state == WL_CONTROLLED_STEP_COMPLETE);
    make_and_insert(&f, 3, false, h2);
    CHECK(h2[0] == next[0] && h2[1] == next[1]);
    /* The compound alone makes the completed host dirty, as an insert
     * does. */
    state = step_state(f.s);
    if (state != WL_CONTROLLED_STEP_DIRTY)
        fprintf(stderr, "%s: state %d after make_compound\n", where,
            (int)state);
    CHECK(state == WL_CONTROLLED_STEP_DIRTY);
    evaluate(&f, 's', (int64_t[]){1, 3}, 2, (int64_t[]){3}, plus, 1,
        observed);
    fixture_destroy(&f);
}

/* Hosts that refuse make_compound after a completed controlled evaluation,
 * each beside an insert that it admits in the same state; the uncontrolled
 * session returns 0 for both. */
static void
refusal(const char *text, const char *input, uint32_t width, bool observed)
{
    fixture_t f;
    fixture(&f, text);
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, NULL);
        wl_session_set_delta_cb(f.raw, on_delta, NULL);
    }
    int64_t row[2] = {1, 2};
    CHECK(!wl_session_insert(f.s, input, row, 1, width));
    CHECK(!wl_session_insert(f.raw, input, row, 1, width));
    int first = until_done(f.s, false, NULL);
    if (first)
        fprintf(stderr, "%s: controlled first STEP %d\n", where, first);
    CHECK(!first);
    CHECK(!until_done(f.raw, false, NULL));
    uint64_t h = 0;
    int rc = make_pair(f.s, 1, 2, &h);
    if (rc != EBUSY)
        fprintf(stderr, "%s: controlled make_compound %d\n", where, rc);
    CHECK(rc == EBUSY);
    CHECK(!make_pair(f.raw, 1, 2, &h));
    row[0] = 2; row[1] = 3;
    CHECK(!wl_session_insert(f.s, input, row, 1, width));
    CHECK(!wl_session_insert(f.raw, input, row, 1, width));
    CHECK(!until_done(f.s, false, NULL));
    CHECK(!until_done(f.raw, false, NULL));
    fixture_destroy(&f);
}

/* ---- the qualification corpus -------------------------------------------- */

enum {
    SH_OWNER = 1u, SH_TIMESTAMPS = 2u, SH_POOL = 4u, SH_ARENA = 8u,
    SH_SHARED = 16u, SH_ALIAS = 32u
};

static unsigned
shape_bits(const col_rel_t *r)
{
    unsigned bits = 0;
    if (r->storage_owner != r
        || r->storage_owner_identity != r->relation_identity)
        bits |= SH_OWNER;
    if (r->timestamps)
        bits |= SH_TIMESTAMPS;
    if (r->pool_owned)
        bits |= SH_POOL;
    if (r->arena_owned)
        bits |= SH_ARENA;
    if (r->col_shared)
        bits |= SH_SHARED;
    if (r->storage_owner == r && col_rel_storage_alias_borrow_count(r))
        bits |= SH_ALIAS;
    return bits;
}

/* A relation held flat: no compound layout and no arity map. */
static bool
layout_flat(const col_rel_t *r)
{
    return r->compound_kind == WIRELOG_COMPOUND_KIND_NONE
           && !r->compound_arity_map;
}

/* Each check fires on a synthetic descriptor the library never sees. */
static void
checker_self_test(void)
{
    col_rel_t r, other;
    memset(&r, 0, sizeof(r));
    memset(&other, 0, sizeof(other));
    r.storage_owner = &r;
    CHECK(!shape_bits(&r) && layout_flat(&r));
    r.storage_owner = &other;
    CHECK(shape_bits(&r) == SH_OWNER);
    r.storage_owner = &r;
    r.pool_owned = true;
    CHECK(shape_bits(&r) == SH_POOL);
    r.pool_owned = false;
    r.compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
    CHECK(!layout_flat(&r));
    r.compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    CHECK(!layout_flat(&r));
    r.compound_kind = WIRELOG_COMPOUND_KIND_NONE;
    uint32_t map[1] = {2};
    r.compound_arity_map = map;
    CHECK(!layout_flat(&r));
    image_t a = {.head = "x"}, b = {.head = "x"};
    image_add(&a, (int64_t[2]){1, 2}, 2, 1);
    image_add(&b, (int64_t[2]){1, 3}, 2, 1);
    CHECK(!image_equal(&a, &b));
    b.count = 0;
    image_add(&b, (int64_t[2]){1, 2}, 2, -1);
    CHECK(!image_equal(&a, &b));
    image_add(&b, (int64_t[2]){1, 2}, 2, 1);
    CHECK(!image_equal(&a, &b));
}

static bool
walk_ok(wl_session_t *s, const char *after)
{
    wl_col_session_t *cs = (wl_col_session_t *)s;
    for (uint32_t i = 0; i < cs->nrels; i++) {
        const col_rel_t *r = cs->rels[i];
        unsigned bits = r ? shape_bits(r) : 0;
        if (bits) {
            fprintf(stderr, "%s, after %s: %s bits %#x\n", where, after,
                r->name ? r->name : "?", bits);
            return false;
        }
    }
    return true;
}

/* The publication hook: shapes, layouts, counts and the stop. */
static wl_session_t *active, *raw_active;
static unsigned published, raw_published, joins;
static unsigned stop_mode; /* 0 none, 1 cancel, 2 work limit */
static const char *stop_at;
static bool stop_hit;

extern void (*wl_columnar_eval_stack_test_physical_publication)(
    wl_col_session_t *, const col_rel_t *, const col_rel_t *);

static void
publication(wl_col_session_t *s, const col_rel_t *in, const col_rel_t *out)
{
    if (&s->base == raw_active) {
        raw_published++;
        return;
    }
    if (&s->base != active)
        return;
    published++;
    unsigned bits = (in ? shape_bits(in) : 0) | shape_bits(out);
    if (bits || out->compound_kind != WIRELOG_COMPOUND_KIND_NONE)
        fprintf(stderr, "%s: publication %s bits %#x kind %d\n", where,
            out->name, bits, (int)out->compound_kind);
    CHECK(!bits && out->compound_kind == WIRELOG_COMPOUND_KIND_NONE);
    if (getenv("SIDE_TRACE"))
        fprintf(stderr, "%s: publish %s from %s\n", where, out->name,
            in && in->name ? in->name : "-");
    if (!strcmp(out->name, "$join"))
        joins++;
    if (stop_mode && !stop_hit && !strcmp(out->name, stop_at)) {
        stop_hit = true;
        if (stop_mode == 1)
            wl_evaluation_control_request_cancel(s->base.evaluation_control);
        else
            CHECK(wl_evaluation_control_charge(s->base.evaluation_control,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
}

/* The model: the compounds made, in order, and the input rows. An e row
 * names its compound by index, or by -1 for a value no compound holds. */
#define MAXH 8
#define MAXE 16

typedef struct {
    uint32_t nh;
    int64_t hx[MAXH], hy[MAXH];
    uint64_t h[2][MAXH]; /* per session: controlled, uncontrolled */
    uint32_t ne;
    int64_t eid[MAXE], ea[MAXE];
    int ek[MAXE];
    uint32_t nc;
    int64_t cid[MAXE], cx[MAXE];
} model_t;

typedef enum {
    K_COPY, K_DESTR, K_FILTER, K_CONST, K_DUP, K_GATE, K_UNION, K_MIXED,
    K_COUNT, K_TC
} kind_t;

typedef struct {
    const char *name, *text, *head, *aux;
    kind_t kind;
    uint32_t ewidth;   /* physical width of an e row */
    bool join;         /* destructures, so a $join publication is required */
    bool walk_eagain;  /* walk at each EAGAIN */
    const char *script;
} qcase_t;

#define SIDE_E ".decl e(id:int64,h:pair/2 side)\n"
static const qcase_t qcases[] = {
    {"copy", SIDE_E ".decl r(id:int64,h:pair/2 side)\nr(i,h) :- e(i,h).\n",
     "r", NULL, K_COPY, 2, false, true, "H|H|I1,0|I2,1|s|S|R1,0|s|H|I3,2|S"},
    {"destr", DESTR, "out", NULL, K_DESTR, 2,
     true, true, "H|H|I1,0|s|I2,1|s|S|R1,0|s|H|I3,2|S|I1,0|s"},
    {"destr_snapshot_first", DESTR, "out", NULL,
     K_DESTR, 2, true, true, "H|I1,0|S|H|I2,1|S|R2,1|s"},
    {"destr_observed", DESTR, "out", NULL,
     K_DESTR, 2, true, true, "O|H|I1,0|s|H|I2,1|s|R1,0|s|D4|s|H|I3,2|s|S"},
    {"destr_pending", DESTR, "out", NULL,
     K_DESTR, 2, true, true, "H|I1,0|P|s|H|I2,1|P|s|S"},
    {"filter", SIDE_E ".decl out(id:int64,y:int64)\n"
     "out(i,y) :- e(i,pair(x,y)), x > 15.\n", "out", NULL, K_FILTER, 2, true,
     true, "H|H|I1,0|I2,1|s|H|I3,2|s|R2,1|S"},
    {"const_child", SIDE_E ".decl out(id:int64)\n"
     "out(i) :- e(i,pair(_,21)).\n", "out", NULL, K_CONST, 2, true, true,
     "H|H|H|I1,0|I2,1|I3,2|s|R2,1|s|S"},
    {"dup_child", SIDE_E ".decl out(id:int64)\n"
     "out(i) :- e(i,pair(x,x)).\n", "out", NULL, K_DUP, 2, true, true,
     "H|X|I1,0|I2,1|s|S|R2,1|s"},
    {"gate", SIDE_E ".decl gate(id:int64)\n.decl hot(id:int64,y:int64)\n"
     "hot(i,y) :- gate(i), e(i,pair(_,y)).\n", "hot", "gate", K_GATE, 2,
     true, true, "H|H|I1,0|I2,1|C1,0|s|C2,0|s|R1,0|S"},
    {"union", SIDE_E ".decl c(id:int64,x:int64)\n.decl out(id:int64,x:int64)\n"
     "out(i,x) :- e(i,pair(x,_)).\nout(i,x) :- c(i,x).\n", "out", "c",
     K_UNION, 2, true, true, "H|I1,0|C9,9|s|H|I2,1|C10,20|s|S|R1,0|s"},
    {"mixed", ".decl e(id:int64,p:pair/2 inline,h:pair/2 side)\n"
     ".decl out(id:int64,a:int64,x:int64)\n"
     "out(i,a,x) :- e(i,pair(a,_),pair(x,_)).\n", "out", NULL, K_MIXED, 4,
     true, true, "H|I1,0|s|H|I2,1|S|R1,0|s"},
    {"count", SIDE_E ".decl n(x:int64,c:int64)\n"
     "n(x,count(i)) :- e(i,pair(x,_)).\n", "n", NULL, K_COUNT, 2, true, true,
     "H|I1,0|I2,0|s|H|I3,1|s|R1,0|S"},
    {"tc", ".decl e(h:pair/2 side)\n.decl t(x:int64,y:int64)\n"
     "t(x,y) :- e(pair(x,y)).\nt(x,z) :- t(x,y), e(pair(y,z)).\n", "t",
     NULL, K_TC, 1, true, false, "T1,2|T2,3|I0,0|I0,1|s|T3,4|I0,2|s|S"},
};

/* The expected image of @k from the model. */
static void
model_image(const qcase_t *k, const model_t *m, image_t *out)
{
    out->count = 0;
    out->head = k->head;
    if (k->kind == K_UNION)
        for (uint32_t i = 0; i < m->nc; i++)
            image_add(out, (int64_t[2]){m->cid[i], m->cx[i]}, 2, 1);
    if (k->kind == K_COUNT) {
        for (uint32_t h = 0; h < m->nh; h++) {
            int64_t n = 0;
            for (uint32_t i = 0; i < m->ne; i++)
                if (m->ek[i] >= 0 && m->hx[m->ek[i]] == m->hx[h])
                    n++;
            bool first = true;
            for (uint32_t g = 0; g < h; g++)
                if (m->hx[g] == m->hx[h])
                    first = false;
            if (n && first)
                image_add(out, (int64_t[2]){m->hx[h], n}, 2, 1);
        }
        return;
    }
    if (k->kind == K_TC) {
        /* Closure over the pairs the e rows name. */
        int64_t px[MAXE], py[MAXE];
        uint32_t np = 0;
        for (uint32_t i = 0; i < m->ne; i++)
            if (m->ek[i] >= 0) {
                px[np] = m->hx[m->ek[i]];
                py[np++] = m->hy[m->ek[i]];
            }
        for (uint32_t a = 0; a < np; a++)
            for (uint32_t b = 0; b < np; b++) {
                /* path from px[a] reaching py[b] through the chain */
                int64_t at = py[a];
                bool reach = a == b;
                for (uint32_t hop = 0; hop < np && !reach && at != py[b];
                    hop++) {
                    bool moved = false;
                    for (uint32_t c = 0; c < np && !moved; c++)
                        if (px[c] == at) {
                            at = py[c];
                            moved = true;
                        }
                    if (!moved)
                        break;
                }
                if (reach || at == py[b]) {
                    bool dup = false;
                    for (uint32_t r = 0; r < out->count; r++)
                        if (out->rows[r][0] == px[a]
                            && out->rows[r][1] == py[b])
                            dup = true;
                    if (!dup)
                        image_add(out, (int64_t[2]){px[a], py[b]}, 2, 1);
                }
            }
        return;
    }
    for (uint32_t i = 0; i < m->ne; i++) {
        int kx = m->ek[i];
        if (kx < 0)
            continue;
        int64_t id = m->eid[i], x = m->hx[kx], y = m->hy[kx];
        bool gated = false;
        switch (k->kind) {
        case K_COPY:
            image_add(out, (int64_t[2]){id, kx}, 2, 1);
            break;
        case K_DESTR:
            image_add(out, (int64_t[3]){id, x, y}, 3, 1);
            break;
        case K_FILTER:
            if (x > 15)
                image_add(out, (int64_t[2]){id, y}, 2, 1);
            break;
        case K_CONST:
            if (y == 21)
                image_add(out, (int64_t[1]){id}, 1, 1);
            break;
        case K_DUP:
            if (x == y)
                image_add(out, (int64_t[1]){id}, 1, 1);
            break;
        case K_GATE:
            for (uint32_t g = 0; g < m->nc; g++)
                if (m->cid[g] == id)
                    gated = true;
            if (gated)
                image_add(out, (int64_t[2]){id, y}, 2, 1);
            break;
        case K_UNION:
            image_add(out, (int64_t[2]){id, x}, 2, 1);
            break;
        case K_MIXED:
            image_add(out, (int64_t[3]){id, m->ea[i], x}, 3, 1);
            break;
        default:
            break;
        }
    }
}

/* Replace each handle in the copy head by its compound index. */
static void
map_handles(const qcase_t *k, const model_t *m, int session, image_t *img)
{
    if (k->kind != K_COPY)
        return;
    for (uint32_t r = 0; r < img->count; r++)
        for (uint32_t h = 0; h < m->nh; h++)
            if ((uint64_t)img->rows[r][1] == m->h[session][h]) {
                img->rows[r][1] = h;
                break;
            }
}

/* Events expected between two model images: removed rows -1, added +1. */
static void
image_delta(image_t *before, image_t *after, image_t *delta)
{
    delta->count = 0;
    delta->head = after->head;
    for (uint32_t i = 0; i < before->count; i++) {
        bool kept = false;
        for (uint32_t j = 0; j < after->count; j++)
            if (!row_cmp(before->rows[i], after->rows[j]))
                kept = true;
        if (!kept)
            image_add(delta, before->rows[i], 4, -1);
    }
    for (uint32_t j = 0; j < after->count; j++) {
        bool old = false;
        for (uint32_t i = 0; i < before->count; i++)
            if (!row_cmp(before->rows[i], after->rows[j]))
                old = true;
        if (!old)
            image_add(delta, after->rows[j], 4, 1);
    }
}

static void
layouts(const qcase_t *k, fixture_t *f, const model_t *m)
{
    wl_session_t *both[2] = {f->s, f->raw};
    for (int s = 0; s < 2; s++) {
        wl_col_session_t *cs = (wl_col_session_t *)both[s];
        const col_rel_t *e = session_find_rel(cs, "e");
        CHECK(e);
        /* An EDB with a side column is held flat, also beside an inline
         * column (mixed). */
        if (!layout_flat(e))
            fprintf(stderr, "%s: e kind %d\n", where, (int)e->compound_kind);
        CHECK(layout_flat(e));
        CHECK(!e->ncols || e->ncols == k->ewidth);
        if (k->kind == K_COPY) {
            const col_rel_t *r = session_find_rel(cs, "r");
            CHECK(!r || layout_flat(r));
        }
        const col_rel_t *side = session_find_rel(cs, "__compound_pair_2");
        CHECK(m->nh ? side != NULL : true);
        if (side) {
            CHECK(layout_flat(side) && side->ncols == 3);
            CHECK(side->nrows == m->nh);
        }
    }
}

static void
e_row(const qcase_t *k, const model_t *m, int s, int64_t id, int kx,
    int64_t raw, int64_t *row)
{
    int64_t h = kx >= 0 ? (int64_t)m->h[s][kx] : raw;
    if (k->ewidth == 1) {
        row[0] = h;
    } else if (k->ewidth == 4) {
        row[0] = id; row[1] = 100 + id; row[2] = 200 + id; row[3] = h;
    } else {
        row[0] = id; row[1] = h;
    }
}

static void
qevaluate(const qcase_t *k, fixture_t *f, const model_t *m, char how,
    bool observed, image_t *last, bool pending, unsigned stop,
    const char *at)
{
    image_t want, delta;
    model_image(k, m, &want);
    image_delta(last, &want, &delta);
    wl_session_t *both[2] = {f->s, f->raw};
    for (int s = 0; s < 2; s++) {
        image_t seen = {.head = k->head}, img = {.head = k->head};
        events = &seen;
        bool checked_pending = false;
        if (!s && stop) {
            stop_mode = stop; stop_at = at; stop_hit = false;
        }
        int rc;
        for (unsigned i = 0;; i++) {
            CHECK(i < 200000);
            rc = how == 'S' ? wl_session_snapshot(both[s], on_tuple, &img)
                : wl_session_step(both[s]);
            if (rc != EAGAIN)
                break;
            if (!s && k->walk_eagain)
                CHECK(walk_ok(both[s], "EAGAIN"));
            if (!s && pending && !checked_pending) {
                /* Mid-evaluation, make_compound and insert both wait. */
                checked_pending = true;
                uint32_t rows = side_rows(both[s]);
                uint64_t h;
                CHECK(make_pair(both[s], 1, 1, &h) == EBUSY);
                int64_t row[4];
                e_row(k, m, 0, 99, 0, 0, row);
                CHECK(wl_session_insert(both[s], "e", row, 1, k->ewidth)
                    == EBUSY);
                CHECK(side_rows(both[s]) == rows);
            }
        }
        events = NULL;
        if (!s && pending)
            CHECK(checked_pending);
        if (!s && stop) {
            int expected = stop == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : WL_EVALUATION_CONTROL_WORK_LIMIT;
            if (rc != expected || !stop_hit)
                fprintf(stderr, "%s: stop at %s rc %d hit %d\n", where, at,
                    rc, stop_hit);
            CHECK(rc == expected && stop_hit);
            stop_mode = 0;
            return;
        }
        if (rc)
            fprintf(stderr, "%s: %s %c rc %d\n", where,
                s ? "uncontrolled" : "controlled", how, rc);
        CHECK(rc == 0);
        if (how == 's') {
            img.count = 0;
            CHECK(!until_done(both[s], true, &img));
        }
        map_handles(k, m, s, &img);
        map_handles(k, m, s, &seen);
        if (!image_equal(&img, &want)) {
            image_print(s ? "uncontrolled" : "controlled", &img);
            image_print("expected", &want);
        }
        CHECK(image_equal(&img, &want));
        if (observed && how == 's') {
            if (!image_equal(&seen, &delta)) {
                image_print(s ? "uncontrolled events" : "controlled events",
                    &seen);
                image_print("expected events", &delta);
            }
            CHECK(image_equal(&seen, &delta));
        }
    }
    *last = want;
}

/* In a stopped or reset state, an insert of e row @id and a make_compound
 * get the same code; when admitted, the uncontrolled session and the model
 * take them too. */
static void
same_code(const qcase_t *k, fixture_t *f, model_t *m, int64_t id,
    const char *after)
{
    uint64_t h = 0;
    int64_t row[4];
    int rc_m = make_pair(f->s, 7 * id, 7 * id + 1, &h);
    int kx = 0;
    if (!rc_m) {
        CHECK(m->nh < MAXH);
        m->h[0][m->nh] = h;
        CHECK(!make_pair(f->raw, 7 * id, 7 * id + 1, &m->h[1][m->nh]));
        m->hx[m->nh] = 7 * id;
        m->hy[m->nh] = 7 * id + 1;
        kx = (int)m->nh++;
    }
    e_row(k, m, 0, id, kx, 0, row);
    int rc_i = wl_session_insert(f->s, "e", row, 1, k->ewidth);
    if (rc_i != rc_m || getenv("SIDE_TRACE"))
        fprintf(stderr, "%s: after %s insert %d make %d\n", where, after,
            rc_i, rc_m);
    CHECK(rc_i == rc_m);
    if (!rc_i) {
        e_row(k, m, 1, id, kx, 0, row);
        CHECK(!wl_session_insert(f->raw, "e", row, 1, k->ewidth));
        CHECK(m->ne < MAXE);
        m->eid[m->ne] = id;
        m->ea[m->ne] = 100 + id;
        m->ek[m->ne++] = kx;
    }
}

/* Run @k. With @stop, stop the controlled session's evaluation number
 * @at_eval at its first publication named @at, check the stop, insert a
 * row and make a compound in the stopped state beside the uncontrolled
 * session, reset, and retry. */
static void
run_qcase(const qcase_t *k, unsigned stop, const char *at, unsigned at_eval)
{
    fixture_t f;
    fixture(&f, k->text);
    char label[96];
    snprintf(label, sizeof(label), "%s%s%s stop%u eval%u", k->name,
        at ? " at " : "", at ? at : "", stop, at_eval);
    where = label;
    active = f.s; raw_active = f.raw;
    published = raw_published = joins = 0;
    wl_columnar_eval_stack_test_physical_publication = publication;
    model_t m;
    memset(&m, 0, sizeof(m));
    image_t last = {.head = k->head};
    bool observed = false, pending = false;
    unsigned evals = 0;
    wl_session_t *both[2] = {f.s, f.raw};
    CHECK(walk_ok(f.s, "create"));
    for (const char *p = k->script; *p;) {
        char op = *p++;
        int64_t a = 0, b = 0;
        if (*p && *p != '|') {
            a = strtoll(p, (char **)&p, 10);
            if (*p == ',')
                b = strtoll(p + 1, (char **)&p, 10);
        }
        if (*p == '|')
            p++;
        if (op == 'H' || op == 'X' || op == 'T') {
            /* H: pair(10n, 10n + 1); X: pair(5, 5); T: pair(a, b). */
            CHECK(m.nh < MAXH);
            int64_t x = op == 'H' ? 10 * ((int64_t)m.nh + 1)
                : op == 'X' ? 5 : a;
            int64_t y = op == 'H' ? x + 1 : op == 'X' ? 5 : b;
            for (int s = 0; s < 2; s++) {
                uint32_t rows = side_rows(both[s]);
                int rc = make_pair(both[s], x, y, &m.h[s][m.nh]);
                if (rc)
                    fprintf(stderr, "%s: %s make_compound %d\n", where,
                        s ? "uncontrolled" : "controlled", rc);
                CHECK(rc == 0 && side_rows(both[s]) == rows + 1);
            }
            m.hx[m.nh] = x;
            m.hy[m.nh++] = y;
        } else if (op == 'I' || op == 'R' || op == 'D') {
            int kx = op == 'D' ? -1 : (int)b;
            CHECK(kx < (int)m.nh);
            for (int s = 0; s < 2; s++) {
                int64_t row[4];
                e_row(k, &m, s, a, kx, 7, row);
                int rc = op == 'R'
                    ? wl_session_remove(both[s], "e", row, 1, k->ewidth)
                    : wl_session_insert(both[s], "e", row, 1, k->ewidth);
                CHECK(rc == 0);
            }
            if (op == 'R') {
                for (uint32_t i = 0; i < m.ne; i++)
                    if (m.eid[i] == a && m.ek[i] == kx) {
                        m.eid[i] = m.eid[m.ne - 1];
                        m.ek[i] = m.ek[m.ne - 1];
                        m.ea[i] = m.ea[m.ne - 1];
                        m.ne--;
                        break;
                    }
            } else {
                CHECK(m.ne < MAXE);
                m.eid[m.ne] = a;
                m.ea[m.ne] = 100 + a;
                m.ek[m.ne++] = kx;
            }
        } else if (op == 'C') {
            for (int s = 0; s < 2; s++)
                CHECK(!wl_session_insert(both[s], k->aux, (int64_t[2]){a, b},
                    1, k->kind == K_GATE ? 1 : 2));
            m.cid[m.nc] = a;
            m.cx[m.nc++] = b;
        } else if (op == 'O') {
            observed = true;
            for (int s = 0; s < 2; s++)
                wl_session_set_delta_cb(both[s], on_delta, NULL);
        } else if (op == 'P') {
            pending = true;
        } else {
            CHECK(op == 's' || op == 'S');
            evals++;
            if (stop && evals == at_eval) {
                uint32_t rows = side_rows(f.s);
                qevaluate(k, &f, &m, op, observed, &last, false, stop, at);
                CHECK(walk_ok(f.s, "stop"));
                CHECK(side_rows(f.s) == rows);
                same_code(k, &f, &m, 7, "stop");
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(walk_ok(f.s, "reset"));
                same_code(k, &f, &m, 8, "reset");
            }
            qevaluate(k, &f, &m, op, observed, &last, pending, 0, NULL);
            pending = false;
            CHECK(walk_ok(f.s, op == 'S' ? "snapshot" : "step"));
        }
        if (op != 's' && op != 'S')
            CHECK(walk_ok(f.s, "operation"));
        layouts(k, &f, &m);
    }
    CHECK(published >= 1 && raw_published == 0);
    /* A destructuring body joins with the side relation. */
    if (k->join && joins < 1)
        fprintf(stderr, "%s: no $join publication\n", where);
    CHECK(!k->join || joins >= 1);
    wl_columnar_eval_stack_test_physical_publication = NULL;
    fixture_destroy(&f);
}

static const qcase_t *
find_qcase(const char *name)
{
    for (size_t i = 0; i < sizeof(qcases) / sizeof(qcases[0]); i++)
        if (!strcmp(qcases[i].name, name))
            return &qcases[i];
    CHECK(false);
    return NULL;
}

/* Programs the parser refuses: a fact and a rule head that write a whole
 * side compound term. */
static void
parser_refusals(void)
{
    static const char *const texts[] = {
        SIDE_E "e(1,pair(2,3)).\n",
        ".decl c(id:int64,x:int64,y:int64)\n"
        ".decl r(id:int64,h:pair/2 side)\nr(i,pair(x,y)) :- c(i,x,y).\n",
    };
    for (size_t i = 0; i < 2; i++) {
        wirelog_error_t error = WIRELOG_OK;
        wirelog_program_t *p = wirelog_parse_string(texts[i], &error);
        CHECK(!p && error != WIRELOG_OK);
    }
}

/* The control for the layout check: an EDB with an inline column and no
 * side column carries the INLINE layout in both sessions. */
static void
inline_control(void)
{
    fixture_t f;
    fixture(&f, ".decl e(id:int64,p:pair/2 inline)\n.decl out(id:int64)\n"
        "out(i) :- e(i,pair(_,_)).\n");
    wl_session_t *both[2] = {f.s, f.raw};
    for (int s = 0; s < 2; s++) {
        const col_rel_t *e = session_find_rel((wl_col_session_t *)both[s],
                "e");
        CHECK(e && !layout_flat(e)
            && e->compound_kind == WIRELOG_COMPOUND_KIND_INLINE);
    }
    fixture_destroy(&f);
}

static void
qualification(void)
{
    checker_self_test();
    inline_control();
    parser_refusals();
    for (size_t i = 0; i < sizeof(qcases) / sizeof(qcases[0]); i++)
        run_qcase(&qcases[i], 0, NULL, 0);
    static const struct {
        const char *name, *at[4];
        unsigned eval;
    } stops[] = {
        {"copy", {"$map"}, 1},
        {"destr", {"$join", "$consol"}, 1},
        {"destr", {"$join", "$consol"}, 2},
        {"union", {"$join", "$concat", "$consol"}, 1},
        {"union", {"$join", "$concat"}, 2},
    };
    for (size_t i = 0; i < sizeof(stops) / sizeof(stops[0]); i++)
        for (unsigned stop = 1; stop <= 2; stop++)
            for (unsigned a = 0; a < 4 && stops[i].at[a]; a++)
                run_qcase(find_qcase(stops[i].name), stop, stops[i].at[a],
                    stops[i].eval);
}

int
main(void)
{
    static const char modes[] = {'s', 'S'};
    for (int m = 0; m < 2; m++)
        for (int observed = 0; observed < 2; observed++)
            for (int make_first = 0; make_first < 2; make_first++) {
                char label[64];
                snprintf(label, sizeof(label), "after %c%s%s", modes[m],
                    observed ? " observed" : "",
                    make_first ? " made first" : "");
                where = label;
                after_completion(modes[m], observed, make_first);
            }
    where = "forward";
    forward_reference(false);
    where = "forward observed";
    forward_reference(true);
    where = "recursive edge host";
    refusal(".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        "path(x,y) :- edge(x,y).\npath(x,z) :- path(x,y), edge(y,z).\n",
        "edge", 2, false);
    where = "observed single-rule host";
    refusal(".decl in(x:int64)\n.decl out(x:int64)\nout(x) :- in(x).\n",
        "in", 1, true);
    qualification();
    printf("ok\n");
    return 0;
}
