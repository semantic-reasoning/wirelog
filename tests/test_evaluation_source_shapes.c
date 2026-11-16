/* #2301: the shapes of the relations that a controlled single-worker session
 * holds and that its operators publish.
 *
 * The controlled scalar source reader refuses a relation without a storage
 * owner (a lazy owner), and the controlled admissions refuse an alias, a
 * pool- or arena-owned relation and a shared column. This test runs a corpus
 * of generated programs through a controlled session and an uncontrolled one,
 * with the same scripted operations on both, and checks:
 *
 *  - after each scripted operation, and after each EAGAIN return of a
 *    controlled evaluation, that every relation registered in the controlled
 *    session, seed shadows `$in$` among them, owns its storage and has no
 *    timestamps, no pool or arena ownership, no shared column and no alias
 *    borrow, and that no retired `$out$` relation is registered;
 *  - the same for the output of every publication that the physical
 *    publication hook observes in the controlled session and for its input,
 *    which for a binary operator is the left input only, and that the output
 *    and the left input of each CONCAT and CONSOLIDATE it observes have no
 *    compound layout;
 *  - that both sessions return 0 for each scripted insert, removal and fact
 *    load, complete each evaluation with 0 and hold the same image of the
 *    head each case names after it (the EAGAIN returns of the two are not
 *    compared one by one), and that every relation registered in the
 *    controlled session after it, other than an EDB and a `$` relation, has
 *    an uncontrolled counterpart and no compound layout in either session;
 *  - after a cancellation or a work-limit stop at a publication, that the
 *    evaluation returns the stop code and the walk is clean; after the reset,
 *    that the walk is clean; and after the retry, all of the above.
 *
 * The checker is tested on synthetic descriptors, and at every stamp of the
 * uncontrolled serial recursion on the stamped delta. The walk finds a
 * timestamped relation registered in the uncontrolled session only at some
 * stamps: the stamped delta is not registered and its head gets timestamps
 * after the hook, so in this corpus the walk finds one only at stamps of
 * mutual_rec, where the other head already carries them, and the test
 * requires such a find there. The stamp hook counts the controlled session's
 * stamps, and the test requires that count to stay 0 over the whole run, the
 * stopped and retried evaluations included. The corpus includes heads
 * declared with an inline compound column; the parser refuses a rule that
 * copies a whole compound into a head, which is checked too.
 *
 * Not checked here: the segment boundaries of stack entries, which a test
 * cannot see; relations that exist only within a single call, except as the
 * hook passes them; the right input of a binary operator except through the
 * codes it yields and, when it is an observed output, at its publication;
 * plans that the controlled preflight refuses (K_FUSION, EXCHANGE, worker and
 * TDD plans); SIDE compound EDBs; more than one worker; cold or spill
 * sources; and memory-budget denials. */
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

static const char *where = "";

/* ---- the shape checker --------------------------------------------------- */

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

/* Each bit fires on a synthetic descriptor that the library never sees. */
static void
checker_self_test(void)
{
    static col_rel_t r, other;
    static col_delta_timestamp_t stamp[1];
    static bool shared[1] = {true};
    memset(&r, 0, sizeof(r));
    r.storage_owner = &r;
    r.relation_identity = r.storage_owner_identity = 1;
    r.ncols = 1;
    CHECK(shape_bits(&r) == 0);
    r.storage_owner = NULL;
    CHECK(shape_bits(&r) == SH_OWNER);
    r.storage_owner = &other;
    CHECK(shape_bits(&r) == SH_OWNER);
    r.storage_owner = &r;
    r.storage_owner_identity = 2;
    CHECK(shape_bits(&r) == SH_OWNER);
    r.storage_owner_identity = 1;
    r.timestamps = stamp;
    CHECK(shape_bits(&r) == SH_TIMESTAMPS);
    r.timestamps = NULL;
    r.pool_owned = true;
    CHECK(shape_bits(&r) == SH_POOL);
    r.pool_owned = false;
    r.arena_owned = true;
    CHECK(shape_bits(&r) == SH_ARENA);
    r.arena_owned = false;
    r.col_shared = shared;
    CHECK(shape_bits(&r) == SH_SHARED);
    r.col_shared = NULL;
    atomic_store_explicit(&r.storage_alias_borrows, 1, memory_order_relaxed);
    CHECK(shape_bits(&r) == SH_ALIAS);
    atomic_store_explicit(&r.storage_alias_borrows, 0, memory_order_relaxed);
    CHECK(shape_bits(&r) == 0);
}

typedef struct {
    unsigned live, retired, shadows, bits;
    char first[96];
} walk_t;

static void
walk(wl_session_t *s, walk_t *w)
{
    wl_col_session_t *cs = (wl_col_session_t *)s;
    memset(w, 0, sizeof(*w));
    for (uint32_t i = 0; i < cs->nrels; i++) {
        const col_rel_t *r = cs->rels[i];
        if (!r)
            continue;
        w->live++;
        if (r->name && !strncmp(r->name, "$out$", 5))
            w->retired++;
        if (r->name && !strncmp(r->name, "$in$", 4))
            w->shadows++;
        unsigned bits = shape_bits(r);
        if (bits && !w->bits)
            snprintf(w->first, sizeof(w->first), "%s bits %#x",
                r->name ? r->name : "?", bits);
        w->bits |= bits;
    }
}

static walk_t totals;

static void
walk_clean(wl_session_t *s, const char *after)
{
    walk_t w;
    walk(s, &w);
    if (w.bits)
        fprintf(stderr, "%s, after %s: %s\n", where, after, w.first);
    CHECK(!w.bits && w.live >= 1);
    totals.retired += w.retired;
    totals.shadows += w.shadows;
}

/* ---- the publication hook ------------------------------------------------ */

static wl_session_t *active, *raw_active;
static unsigned published, raw_published, concats, consolidations;
static unsigned stop_mode; /* 0 none, 1 cancel, 2 work limit */
static const char *stop_at;
static bool stop_hit;

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
    if (bits)
        fprintf(stderr, "%s: publication %s from %s bits %#x\n", where,
            out->name, in && in->name ? in->name : "-", bits);
    CHECK(!bits);
    if (getenv("SHAPES_TRACE"))
        fprintf(stderr, "%s: publish %s from %s kind %d\n", where,
            out->name, in && in->name ? in->name : "-",
            in ? (int)in->compound_kind : -1);
    /* Every output this hook observes, and the left input of each CONCAT
     * and CONSOLIDATE it observes, has no compound layout. */
    CHECK(out->compound_kind == WIRELOG_COMPOUND_KIND_NONE);
    bool merge = !strcmp(out->name, "$concat") || !strcmp(out->name, "$consol");
    if (merge)
        CHECK(in && in->compound_kind == WIRELOG_COMPOUND_KIND_NONE);
    if (!strcmp(out->name, "$concat"))
        concats++;
    if (!strcmp(out->name, "$consol"))
        consolidations++;
    if (stop_mode && !stop_hit && !strcmp(out->name, stop_at)) {
        stop_hit = true;
        if (stop_mode == 1)
            wl_evaluation_control_request_cancel(s->base.evaluation_control);
        else
            CHECK(wl_evaluation_control_charge(s->base.evaluation_control,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
}

/* ---- sessions ------------------------------------------------------------ */

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

/* Every relation registered in the controlled session, other than an EDB
 * and a `$` relation, has an uncontrolled counterpart, and neither holds a
 * compound layout: the heads each case names and the intermediate heads,
 * among them the inline-declared `m` of inline_idb_chain that a later
 * stratum reads back as a compound. */
static void
idb_layouts(const fixture_t *f, const char *name)
{
    const wl_col_session_t *cs = (const wl_col_session_t *)f->s;
    unsigned checked = 0;
    bool m_seen = false;
    for (uint32_t i = 0; i < cs->nrels; i++) {
        const col_rel_t *r = cs->rels[i];
        if (!r || !r->name || r->name[0] == '$')
            continue;
        bool edb = false;
        for (uint32_t e = 0; e < f->plan->edb_count; e++)
            if (!strcmp(r->name, f->plan->edb_relations[e]))
                edb = true;
        if (edb)
            continue;
        const col_rel_t *raw = session_find_rel((wl_col_session_t *)f->raw,
                r->name);
        if (r->compound_kind != WIRELOG_COMPOUND_KIND_NONE
            || (raw && raw->compound_kind != WIRELOG_COMPOUND_KIND_NONE))
            fprintf(stderr, "%s: %s compound_kind %d, uncontrolled %d\n",
                where, r->name, (int)r->compound_kind,
                raw ? (int)raw->compound_kind : -1);
        CHECK(raw && r->compound_kind == WIRELOG_COMPOUND_KIND_NONE
            && raw->compound_kind == WIRELOG_COMPOUND_KIND_NONE);
        checked++;
        if (!strcmp(r->name, "m"))
            m_seen = true;
    }
    CHECK(checked >= 1);
    if (!strcmp(name, "inline_idb_chain")) {
        /* The control: the EDB `a`, declared inline, carries the layout. */
        const col_rel_t *a = session_find_rel((wl_col_session_t *)f->s, "a");
        const col_rel_t *raw_a = session_find_rel((wl_col_session_t *)f->raw,
                "a");
        CHECK(m_seen && a && raw_a
            && a->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            && raw_a->compound_kind == WIRELOG_COMPOUND_KIND_INLINE);
    }
}

static int
until_done(wl_session_t *s, bool snapshot, wirelog_on_tuple_fn on_tuple,
    void *data)
{
    /* The bound is about nine times the most retries one evaluation of the
     * corpus took under MIN_BUDGET when this test was written; it turns a
     * livelock into a prompt CHECK failure. Each intermediate state of the
     * controlled session is walked too. */
    for (unsigned i = 0; i < 200000; i++) {
        int rc = snapshot ? wl_session_snapshot(s, on_tuple, data)
            : wl_session_step(s);
        if (rc != EAGAIN)
            return rc;
        if (s == active)
            walk_clean(s, "EAGAIN");
    }
    CHECK(false);
    return EIO;
}

/* ---- head images --------------------------------------------------------- */

#define MAXW 4u
#define MAXR 64u

typedef struct {
    int64_t v[MAXR][MAXW];
    unsigned count, width;
    const char *head;
} image_t;

static void
on_tuple(const char *name, const int64_t *v, uint32_t width, void *data)
{
    image_t *m = data;
    if (strcmp(name, m->head) != 0)
        return;
    CHECK(width <= MAXW && m->count < MAXR);
    memset(m->v[m->count], 0, sizeof(m->v[0]));
    if (width)
        memcpy(m->v[m->count], v, width * sizeof(*v));
    m->width = width;
    m->count++;
}

static void
on_delta(const char *name, const int64_t *v, uint32_t width, int diff,
    void *data)
{
    (void)name; (void)v; (void)width; (void)diff; (void)data;
}

static int
row_order(const void *a, const void *b)
{
    const int64_t *x = a, *y = b;
    for (unsigned c = 0; c < MAXW; c++)
        if (x[c] != y[c])
            return x[c] < y[c] ? -1 : 1;
    return 0;
}

static void
image(wl_session_t *s, const char *head, image_t *m)
{
    memset(m, 0, sizeof(*m));
    m->head = head;
    CHECK(!until_done(s, true, on_tuple, m));
    qsort(m->v, m->count, sizeof(m->v[0]), row_order);
}

/* ---- the corpus ---------------------------------------------------------- */

/* A script is a list of calls separated by '|': `i<rel>:<rows>` inserts and
 * `r<rel>:<rows>` removes rows such as `1,2;3,4`; `s` is a STEP, `S` an
 * evaluating SNAPSHOT, `O` installs a delta observer and `F` loads the
 * program's facts. */
typedef struct {
    const char *name, *text, *script, *head;
} case_t;

#define UNION2 ".decl a(x:int64)\n.decl c(x:int64)\n.decl out(x:int64)\n" \
        "out(x):-a(x).\nout(x):-c(x).\n"
#define UNION3 ".decl a(x:int64)\n.decl c(x:int64)\n.decl d(x:int64)\n" \
        ".decl out(x:int64)\nout(x):-a(x).\nout(x):-c(x).\nout(x):-d(x).\n"
#define TC_EP ".decl e(x:int64,y:int64)\n.decl p(x:int64,y:int64)\n" \
        "p(x,y):-e(x,y).\np(x,z):-p(x,y),e(y,z).\n"
#define TC_EDGE ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n" \
        "path(x,y):-edge(x,y).\npath(x,z):-path(x,y),edge(y,z).\n"
#define INLINE_UNION ".decl a(id:int64,p:pair/2 inline)\n" \
        ".decl c(id:int64,x:int64,y:int64)\n" \
        ".decl out(id:int64,x:int64,y:int64)\n" \
        "out(i,x,y) :- a(i,pair(x,y)).\nout(i,x,y) :- c(i,x,y).\n"

static const case_t cases[] = {
    {"union", UNION2, "ia:1;2|ic:2;3|s|S|ia:4|s|S", "out"},
    {"union_obs", UNION2, "O|ia:1;2|ic:2;3|s|ia:4|s|S", "out"},
    {"union_remove", UNION2, "ia:1;2|ic:2;3|s|ra:1|s|S|ra:2|s|S", "out"},
    {"union_remove_obs", UNION2, "O|ia:1;2|ic:2;3|s|ra:1|s|S", "out"},
    {"union3", UNION3, "ia:1|ic:2|id:3|s|S|id:4|s|S", "out"},
    {"union3_obs_many", UNION3, "O|ia:1|s|ic:2|s|id:3|s|ra:1|s|ia:1|s|S",
     "out"},
    {"union_float",
     ".decl a(x:float)\n.decl c(x:float)\n.decl out(x:float)\n"
     "out(x):-a(x).\nout(x):-c(x).\n", "ia:1|ic:2|s|S", "out"},
    {"union_expr",
     ".decl a(x:int64)\n.decl c(x:int64)\n.decl out(x:int64)\n"
     "out(x+1):-a(x).\nout(x):-c(x), x > 0.\n", "O|ia:1;2|ic:2;3|s|ra:1|s|S",
     "out"},
    {"count_union",
     ".decl a(x:int64)\n.decl c(x:int64)\n.decl m(x:int64)\n"
     ".decl out(n:int64)\nm(x):-a(x).\nm(x):-c(x).\nout(count(x)):-m(x).\n",
     "ia:1;2|ic:2;3|s|S|rc:3|s|S", "out"},
    {"selfjoin_union",
     ".decl a(x:int64,y:int64)\n.decl out(x:int64,y:int64)\n"
     "out(x,z):-a(x,y),a(y,z).\nout(x,y):-a(x,y).\n",
     "O|ia:1,2;2,3|s|ia:3,4|s|ra:1,2|s|S", "out"},
    {"tc", TC_EP, "ie:1,2;2,3;3,4|s|S|ie:4,5|s|S", "p"},
    {"tc_obs", TC_EP, "O|ie:1,2;2,3;3,4|s|ie:4,5|s|S", "p"},
    {"tc_remove", TC_EP, "ie:1,2;2,3;3,4|s|re:2,3|s|S", "p"},
    {"tc_snapshot_first", TC_EP, "ie:1,2;2,3;3,4|S|ie:4,5|S|s", "p"},
    {"tc_edge", TC_EDGE, "iedge:1,2;2,3;3,4|s|S|iedge:4,5|s|S|redge:2,3|s|S",
     "path"},
    {"tc_edge_snap", TC_EDGE, "iedge:1,2;2,3;3,4|S|iedge:4,5|S|s|S", "path"},
    {"tc_edge_obs", TC_EDGE, "O|iedge:1,2;2,3;3,4|s|iedge:4,5|s|S", "path"},
    {"tc_edge_obs_remove", TC_EDGE,
     "O|iedge:1,2;2,3;3,4|s|redge:2,3|s|iedge:2,3|s|S", "path"},
    {"tc_edge_rev",
     ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
     "path(x,y):-edge(x,y).\npath(x,z):-edge(x,y),path(y,z).\n",
     "iedge:1,2;2,3;3,4|s|S|iedge:4,5|s|S", "path"},
    {"mutual_rec",
     ".decl e(x:int64,y:int64)\n.decl odd(x:int64,y:int64)\n"
     ".decl even(x:int64,y:int64)\nodd(x,y):-e(x,y).\n"
     "even(x,z):-odd(x,y),e(y,z).\nodd(x,z):-even(x,y),e(y,z).\n",
     "ie:1,2;2,3;3,4|s|S", "odd"},
    {"tc_union_body",
     ".decl edge(x:int64,y:int64)\n.decl extra(x:int64,y:int64)\n"
     ".decl path(x:int64,y:int64)\npath(x,y):-edge(x,y).\n"
     "path(x,y):-extra(x,y).\npath(x,z):-path(x,y),edge(y,z).\n",
     "iedge:1,2;2,3|iextra:3,4|s|S|iedge:4,5|s|S", "path"},
    {"tc_down", TC_EP ".decl out(x:int64)\nout(x):-p(x,y).\n",
     "ie:1,2;2,3;3,4|s|S|ie:4,5|s|S", "out"},
    {"tc_down_union",
     TC_EP ".decl out(x:int64)\nout(x):-p(x,y).\nout(y):-p(x,y).\n",
     "ie:1,2;2,3|S|s", "out"},
    {"facts_union", UNION2 "a(1).\na(2).\nc(3).\n", "F|s|S|ia:5|s|S", "out"},
    {"neg",
     ".decl a(x:int64)\n.decl c(x:int64)\n.decl out(x:int64)\n"
     "out(x):-a(x),!c(x).\n", "ia:1;2;3|ic:2|s|S|ra:1|s|S", "out"},
    {"count",
     ".decl a(k:int64,x:int64)\n.decl out(k:int64,n:int64)\n"
     "out(k,count(x)):-a(k,x).\n", "ia:1,1;1,2;2,5|s|S|ia:1,3|s|S", "out"},
    {"join_filter",
     ".decl a(x:int64,y:int64)\n.decl b(y:int64,z:int64)\n"
     ".decl out(x:int64,z:int64)\nout(x,z):-a(x,y),b(y,z),x<z.\n",
     "ia:1,2;5,2|ib:2,3|s|S|ib:2,9|s|S", "out"},
    {"two_strata_union",
     ".decl a(x:int64)\n.decl c(x:int64)\n.decl m(x:int64)\n"
     ".decl out(x:int64)\nm(x):-a(x).\nm(x):-c(x).\nout(x):-m(x).\n"
     "out(x):-a(x).\n", "ia:1;2|ic:3|s|S|ia:7|s|S|ra:1|s|S", "out"},
    {"two_strata_neg",
     ".decl a(x:int64)\n.decl c(x:int64)\n.decl m(x:int64)\n"
     ".decl out(x:int64)\nm(x):-a(x),!c(x).\nout(x):-m(x).\n"
     "out(x):-c(x).\n", "ia:1;2|ic:2|s|S|rc:2|s|S", "out"},
    {"inline_union", INLINE_UNION,
     "ia:1,10,20;2,30,40|ic:3,5,6|s|S|ia:4,7,8|s|S", "out"},
    /* Heads declared with an inline compound column, written from flat
     * columns; one of them, `m` of inline_idb_chain, is read back as a
     * compound by a later stratum. */
    {"inline_head",
     ".decl c(id:int64,x:int64,y:int64)\n.decl out(id:int64,p:pair/2 inline)\n"
     "out(i,x,y) :- c(i,x,y).\n", "ic:1,10,20;2,30,40|s|S|ic:3,5,6|s|S",
     "out"},
    {"inline_head_union",
     ".decl a(id:int64,p:pair/2 inline)\n.decl c(id:int64,x:int64,y:int64)\n"
     ".decl out(id:int64,p:pair/2 inline)\nout(i,x,y) :- c(i,x,y).\n"
     "out(i,x,y) :- a(i,pair(x,y)).\n",
     "ia:1,10,20;2,30,40|ic:2,30,40;3,5,6|s|S|ra:1,10,20|s|S", "out"},
    {"inline_idb_chain",
     ".decl a(id:int64,p:pair/2 inline)\n.decl c(id:int64,x:int64,y:int64)\n"
     ".decl m(id:int64,p:pair/2 inline)\n.decl out(id:int64,x:int64,y:int64)\n"
     "m(i,x,y) :- c(i,x,y).\nout(i,x,y) :- m(i,pair(x,y)).\n"
     "out(i,x,y) :- a(i,pair(x,y)).\n",
     "ia:1,10,20|ic:2,30,40;3,5,6|s|S|ic:4,7,8|s|S", "out"},
    /* Facts loaded into a rule head also go to its seed shadow `$in$out`,
     * which the walk then checks. The script removes only a loaded fact and
     * evaluates nothing; evaluation_facts_sources checks the controlled
     * evaluation of heads with input, and its broader support stays open
     * (#2301). */
    {"head_rows", UNION2 "out(9).\n", "F|ia:1|rout:9", "out"},
};

static void
parse_rows(const char *p, int64_t *buf, uint32_t *rows, uint32_t *cols)
{
    uint32_t c = 0, n = 0;
    *rows = *cols = 0;
    while (*p && *p != '|') {
        char *end;
        CHECK(n < 64);
        buf[n++] = strtoll(p, &end, 10);
        CHECK(end != p);
        c++;
        p = end;
        if (*p == ',') {
            p++;
            continue;
        }
        if (!*cols)
            *cols = c;
        CHECK(c == *cols);
        (*rows)++;
        c = 0;
        if (*p == ';')
            p++;
    }
}

/* The uncontrolled serial recursion stamps the delta of its head. The
 * checker sees the timestamps on that delta at every stamp. The delta is not
 * registered, and its head gets timestamps after this hook, so the walk finds
 * a registered timestamped relation only where another head of the recursion
 * already carries them: in this corpus, at stamps of mutual_rec. main
 * requires such a find there, and requires the count of controlled sessions
 * reaching the stamp to stay 0. */
extern void (*wl_columnar_eval_serial_test_after_delta_stamp)(
    wl_col_session_t *, col_rel_t *, col_rel_t *);
static unsigned raw_stamped, delta_stamped, stamp_found, ctl_stamped;
static unsigned mutual_found;

static void
after_stamp(wl_col_session_t *s, col_rel_t *source, col_rel_t *delta)
{
    (void)source;
    if (&s->base == active) {
        ctl_stamped++;
        return;
    }
    if (&s->base != raw_active)
        return;
    raw_stamped++;
    if (shape_bits(delta) & SH_TIMESTAMPS)
        delta_stamped++;
    walk_t w;
    walk(&s->base, &w);
    if (w.bits & SH_TIMESTAMPS) {
        if (getenv("SHAPES_TRACE"))
            fprintf(stderr, "%s: uncontrolled %s\n", where, w.first);
        stamp_found++;
        if (!strncmp(where, "mutual_rec ", 11))
            mutual_found++;
    }
}

/* Run @k on both sessions; with @fault, stop the controlled session's first
 * evaluation at its first publication named @at, then reset and retry. */
static void
run_case(const case_t *k, unsigned fault, const char *at)
{
    fixture_t f;
    fixture(&f, k->text);
    char label[96];
    snprintf(label, sizeof(label), "%s fault%u%s%s", k->name, fault,
        at ? " at " : "", at ? at : "");
    where = label;
    active = f.s; raw_active = f.raw;
    published = raw_published = concats = consolidations = 0;
    memset(&totals, 0, sizeof(totals));
    stop_mode = 0; stop_at = at; stop_hit = false;
    wl_columnar_eval_stack_test_physical_publication = publication;
    wl_columnar_eval_serial_test_after_delta_stamp = after_stamp;
    bool faulted = false;
    walk_clean(f.s, "create");
    for (const char *p = k->script; *p;) {
        char op = *p++;
        if (op == 'i' || op == 'r') {
            char rel[16];
            unsigned n = 0;
            while (*p != ':') {
                CHECK(n + 1 < sizeof(rel));
                rel[n++] = *p++;
            }
            rel[n] = 0;
            p++;
            int64_t buf[64];
            uint32_t rows, cols;
            parse_rows(p, buf, &rows, &cols);
            while (*p && *p != '|')
                p++;
            int rc = op == 'i' ? wl_session_insert(f.s, rel, buf, rows, cols)
                : wl_session_remove(f.s, rel, buf, rows, cols);
            int raw = op == 'i' ? wl_session_insert(f.raw, rel, buf, rows,
                    cols) : wl_session_remove(f.raw, rel, buf, rows, cols);
            if (rc != raw || rc)
                fprintf(stderr, "%s: %c%s rc %d, uncontrolled %d\n", where, op,
                    rel, rc, raw);
            CHECK(rc == raw && !rc);
            CHECK(session_find_rel((wl_col_session_t *)f.s, rel));
            walk_clean(f.s, op == 'i' ? "insert" : "remove");
        } else if (op == 'O') {
            wl_session_set_delta_cb(f.s, on_delta, NULL);
            wl_session_set_delta_cb(f.raw, on_delta, NULL);
            walk_clean(f.s, "set_delta_cb");
        } else if (op == 'F') {
            CHECK(!wl_session_load_facts(f.s, f.program));
            CHECK(!wl_session_load_facts(f.raw, f.program));
            walk_clean(f.s, "load_facts");
        } else {
            CHECK(op == 's' || op == 'S');
            bool snapshot = op == 'S';
            image_t ctl, raw;
            memset(&ctl, 0, sizeof(ctl));
            ctl.head = k->head;
            if (fault && !faulted) {
                faulted = true;
                stop_mode = fault;
                int rc = until_done(f.s, snapshot, on_tuple, &ctl);
                int expected = fault == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                    : WL_EVALUATION_CONTROL_WORK_LIMIT;
                if (rc != expected || !stop_hit)
                    fprintf(stderr, "%s: rc %d, expected %d, hit %u\n",
                        where, rc, expected, stop_hit);
                CHECK(rc == expected && stop_hit && !ctl.count);
                stop_mode = 0;
                walk_clean(f.s, "stop");
                CHECK(!wl_evaluation_control_reset(f.control));
                walk_clean(f.s, "reset");
            }
            CHECK(!until_done(f.s, snapshot, on_tuple, &ctl));
            memset(&raw, 0, sizeof(raw));
            raw.head = k->head;
            CHECK(!until_done(f.raw, snapshot, on_tuple, &raw));
            walk_clean(f.s, snapshot ? "snapshot" : "step");
            image(f.s, k->head, &ctl);
            image(f.raw, k->head, &raw);
            walk_clean(f.s, "image");
            const col_rel_t *head = session_find_rel((wl_col_session_t *)f.s,
                    k->head);
            const col_rel_t *raw_head = session_find_rel(
                (wl_col_session_t *)f.raw, k->head);
            CHECK(head && raw_head && head->ncols == raw_head->ncols);
            idb_layouts(&f, k->name);
            CHECK(ctl.count && ctl.count == raw.count
                && !memcmp(ctl.v, raw.v, sizeof(ctl.v[0]) * ctl.count));
        }
        if (*p == '|')
            p++;
    }
    /* The hook saw the controlled work and only it. */
    bool evaluates = strchr(k->script, 's') || strchr(k->script, 'S');
    CHECK((published >= 1 || !evaluates) && !raw_published);
    if (fault)
        CHECK(faulted);
    if (evaluates && (strstr(k->text, ":-c(x).") || strstr(k->name, "union")))
        CHECK(concats >= 1 && consolidations >= 1);
    if (!strcmp(k->name, "head_rows"))
        CHECK(totals.shadows >= 1);
    /* No call creates a retired head relation in the controlled session. */
    CHECK(!totals.retired);
    wl_columnar_eval_stack_test_physical_publication = NULL;
    wl_columnar_eval_serial_test_after_delta_stamp = NULL;
    active = raw_active = NULL;
    if (getenv("SHAPES_TRACE"))
        fprintf(stderr, "%s: published %u raw %u concat %u consol %u\n",
            where, published, raw_published, concats, consolidations);
    fixture_destroy(&f);
}

/* A rule cannot copy a compound into a head as a whole: the parser refuses
 * a compound term in a head and a variable bound to a whole inline column,
 * so a head is written from its slots. */
static void
parser_refusals(void)
{
    static const char *const refused[] = {
        ".decl a(id:int64,p:pair/2 inline)\n"
        ".decl out(id:int64,p:pair/2 inline)\n"
        "out(i,pair(x,y)) :- a(i,pair(x,y)).\n",
        ".decl a(id:int64,p:pair/2 inline)\n"
        ".decl out(id:int64,p:pair/2 inline)\nout(i,p) :- a(i,p).\n",
    };
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        wirelog_error_t error = WIRELOG_OK;
        wirelog_program_t *program = wirelog_parse_string(refused[i], &error);
        CHECK(!program && error != WIRELOG_OK);
    }
}

static const case_t *
find_case(const char *name)
{
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        if (!strcmp(cases[i].name, name))
            return &cases[i];
    CHECK(false);
    return NULL;
}

int
main(void)
{
    checker_self_test();
    parser_refusals();
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        run_case(&cases[i], 0, NULL);
    /* Recursive cases reached the uncontrolled stamp and the checker saw the
     * stamped delta each time; at stamps of mutual_rec the walk also found a
     * registered timestamped relation. If either control stops holding, it
     * needs another failing state. */
    if (!raw_stamped || delta_stamped != raw_stamped)
        fprintf(stderr, "the uncontrolled recursion no longer stamps its "
            "delta (%u of %u)\n", delta_stamped, raw_stamped);
    if (!mutual_found)
        fprintf(stderr, "the walk found no timestamped relation at a "
            "mutual_rec stamp (%u finds elsewhere)\n", stamp_found);
    CHECK(raw_stamped && delta_stamped == raw_stamped && mutual_found);
    /* A cancellation and a work-limit stop at the first publication of each
     * listed operator in the first evaluation, then a reset and a retry. */
    static const struct {
        const char *name, *at[4];
    } stops[] = {
        {"union", {"$map", "$concat", "$consol"}},
        {"union_remove", {"$map", "$concat", "$consol"}},
        {"count_union", {"$map", "$concat", "$reduce", "$consol"}},
        {"tc_edge", {"$map", "$join", "$concat", "$consol"}},
        {"inline_head_union", {"$map", "$concat", "$consol"}},
    };
    for (size_t i = 0; i < sizeof(stops) / sizeof(stops[0]); i++)
        for (unsigned fault = 1; fault <= 2; fault++)
            for (unsigned a = 0; a < 4 && stops[i].at[a]; a++)
                run_case(find_case(stops[i].name), fault, stops[i].at[a]);
    /* No controlled evaluation, the stopped and retried ones included,
     * reached the stamp of the uncontrolled serial recursion. */
    CHECK(!ctl_stamped);
    printf("ok\n");
    return 0;
}
