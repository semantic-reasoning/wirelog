/* #2301: inline compound EDB sources through controlled sessions.
 *
 * An EDB declared with an inline compound column and no side compound
 * column stores each compound as contiguous int64 lanes (compound_kind
 * INLINE). The generated rule bodies below read it through VARIABLE and
 * MAP, and the UNION body also through CONCAT and CONSOLIDATE. Each
 * controlled result is compared with a literal tuple set and with an
 * uncontrolled session over the same plan. */
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

extern void (*wl_columnar_eval_stack_test_physical_publication)(
    wl_col_session_t *, const col_rel_t *, const col_rel_t *);

#define MAXW 5u
#define MAXR 64u

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
    CHECK(width && width <= MAXW && c->count < MAXR);
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
equal_capture(capture_t *a, capture_t *b, const char *what)
{
    qsort(a->rows, a->count, sizeof(row_t), row_order);
    qsort(b->rows, b->count, sizeof(row_t), row_order);
    bool same = a->count == b->count;
    for (unsigned i = 0; same && i < a->count; i++)
        same = !row_order(&a->rows[i], &b->rows[i]);
    if (!same) {
        fprintf(stderr, "%s: actual %u rows, expected %u\n", what, a->count,
            b->count);
        for (unsigned i = 0; i < a->count; i++)
            fprintf(stderr, "  got w%u %lld %lld %lld diff%d\n",
                a->rows[i].width, (long long)a->rows[i].v[0],
                (long long)a->rows[i].v[1], (long long)a->rows[i].v[2],
                a->rows[i].diff);
    }
    CHECK(same);
}

static void
add(capture_t *c, unsigned width, const int64_t *v, int diff)
{
    on_delta(c->head, v, width, diff, c);
}

/* ---- programs ---------------------------------------------------------- */

enum { K_PAIR, K_UNION, K_WIDE, K_COUNT };

static const char *const programs[K_COUNT] = {
    ".decl a(id:int64,p:pair/2 inline)\n"
    ".decl out(id:int64,x:int64,y:int64)\n"
    "out(i,x,y) :- a(i,pair(x,y)).\n",
    ".decl a(id:int64,p:pair/2 inline)\n"
    ".decl c(id:int64,x:int64,y:int64)\n"
    ".decl out(id:int64,x:int64,y:int64)\n"
    "out(i,x,y) :- a(i,pair(x,y)).\n"
    "out(i,x,y) :- c(i,x,y).\n",
    ".decl a(k:int64,b:box/1 inline,t:triple/3 inline,s1:int64,s2:int64,"
    "s3:int64,s4:int64,s5:int64,s6:int64,s7:int64)\n"
    ".decl out(s7:int64,z:int64,v:int64,k:int64,x:int64)\n"
    "out(s7,z,v,k,x) :- a(k,box(v),triple(x,y,z),s1,s2,s3,s4,s5,s6,s7).\n",
};

/* Physical rows for a. The duplicate row collapses under set semantics. */
static const int64_t pair_rows[] = {
    1, 10, 20,   2, 30, 40,   1, 10, 20,   3, -5, 6,
};
static const int64_t union_rows[] = {
    2, 30, 40,   9, 1, 2,
};
static const int64_t wide_rows[] = {
    1, 7, 100, 101, 102, 11, 12, 13, 14, 15, 16, 17,
    2, 8, 200, 201, 202, 21, 22, 23, 24, 25, 26, 27,
    1, 7, 100, 101, 102, 11, 12, 13, 14, 15, 16, 17,
};

static unsigned
physical_width(unsigned kind)
{
    return kind == K_WIDE ? 12u : 3u;
}

/* empty leaves only the INLINE source a without rows. Session creation gave
 * the INLINE source a its schema. The plain c always has rows: a
 * never-inserted EDB without a FLOAT, STRING or INLINE column has no
 * physical schema, which is a different source representation. */
static void
populate(wl_session_t *s, unsigned kind, bool empty)
{
    if (!empty && kind == K_WIDE)
        CHECK(!wl_session_insert(s, "a", wide_rows, 3, 12));
    else if (!empty)
        CHECK(!wl_session_insert(s, "a", pair_rows, 4, 3));
    if (kind == K_UNION)
        CHECK(!wl_session_insert(s, "c", union_rows, 2, 3));
}

static void
literal(unsigned kind, bool empty, capture_t *e)
{
    if (empty) {
        if (kind == K_UNION) {
            add(e, 3, &union_rows[0], 1); add(e, 3, &union_rows[3], 1);
        }
        return;
    }
    if (kind == K_WIDE) {
        const int64_t r1[] = {17, 102, 7, 1, 100};
        const int64_t r2[] = {27, 202, 8, 2, 200};
        add(e, 5, r1, 1); add(e, 5, r2, 1);
        return;
    }
    for (unsigned r = 0; r < 4; r++)
        if (r != 2)
            add(e, 3, &pair_rows[r * 3], 1);
    if (kind == K_UNION)
        add(e, 3, &union_rows[3], 1); /* {2,30,40} is already present */
}

/* ---- generated plan shape ---------------------------------------------- */

static void
check_body(const wl_plan_t *plan, unsigned kind)
{
    static const wl_plan_op_type_t single[] = {
        WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP, WL_PLAN_OP_CONSOLIDATE,
    };
    static const wl_plan_op_type_t unioned[] = {
        WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP, WL_PLAN_OP_VARIABLE,
        WL_PLAN_OP_MAP, WL_PLAN_OP_CONCAT, WL_PLAN_OP_CONSOLIDATE,
    };
    const wl_plan_op_type_t *want = kind == K_UNION ? unioned : single;
    uint32_t count = kind == K_UNION ? 6u : 3u;
    const wl_plan_relation_t *out = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; s++)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++)
            if (!strcmp(plan->strata[s].relations[r].name, "out"))
                out = &plan->strata[s].relations[r];
    CHECK(out);
    bool same = out->op_count == count;
    for (uint32_t i = 0; same && i < count; i++)
        same = out->ops[i].op == want[i];
    if (!same) {
        fprintf(stderr, "kind%u body:", kind);
        for (uint32_t i = 0; i < out->op_count; i++)
            fprintf(stderr, " %d", (int)out->ops[i].op);
        fprintf(stderr, "\n");
    }
    CHECK(same);
}

/* ---- publication witness and deterministic stops ------------------------ */

static wl_session_t *active;
static unsigned map_inline, concat_seen;
static unsigned stop_mode; /* 0 none, 1 cancel, 2 work limit */
static const char *stop_at;
static bool stop_hit;

static void
publication(wl_col_session_t *s, const col_rel_t *in, const col_rel_t *out)
{
    if (&s->base != active)
        return;
    if (!strcmp(out->name, "$map") && in
        && in->compound_kind == WIRELOG_COMPOUND_KIND_INLINE) {
        CHECK(out->compound_kind == WIRELOG_COMPOUND_KIND_NONE
            && !out->compound_arity_map && !out->timestamps);
        map_inline++;
    }
    if (!strcmp(out->name, "$concat"))
        concat_seen++;
    if (stop_mode && !stop_hit && !strcmp(out->name, stop_at)) {
        stop_hit = true;
        if (stop_mode == 1)
            wl_evaluation_control_request_cancel(s->base.evaluation_control);
        else
            CHECK(wl_evaluation_control_charge(s->base.evaluation_control,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
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

/* ---- fixture ------------------------------------------------------------ */

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
    f->program = wirelog_parse_string(programs[kind], &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
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
    col_rel_t *a = session_find_rel((wl_col_session_t *)f->s, "a");
    CHECK(a && a->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
        && a->ncols == physical_width(kind));
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

/* The source keeps its INLINE layout, map and image across the call. */
typedef struct {
    col_rel_t *rel;
    col_relation_snapshot_t image;
    uint32_t map[16], len;
} source_t;

static void
source_save(source_t *p, wl_session_t *s)
{
    p->rel = session_find_rel((wl_col_session_t *)s, "a");
    CHECK(p->rel && p->rel->compound_kind == WIRELOG_COMPOUND_KIND_INLINE);
    p->len = p->rel->compound_arity_len;
    CHECK(p->len <= 16);
    memcpy(p->map, p->rel->compound_arity_map, p->len * sizeof(uint32_t));
    p->image = wl_columnar_relation_snapshot(p->rel);
}

static void
source_kept(const source_t *p, wl_session_t *s, bool strict)
{
    col_rel_t *now = session_find_rel((wl_col_session_t *)s, "a");
    CHECK(now == p->rel && now->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
        && now->compound_arity_len == p->len
        && !memcmp(now->compound_arity_map, p->map,
        p->len * sizeof(uint32_t)));
    col_relation_snapshot_t image = wl_columnar_relation_snapshot(now);
    if (strict)
        CHECK(wl_columnar_relation_snapshot_equal(p->image, image));
    else
        CHECK(image.relation_identity == p->image.relation_identity
            && image.view_generation == p->image.view_generation);
}

/* ---- controlled versus uncontrolled ------------------------------------- */

static void
run_case(unsigned kind, bool snapshot, bool observed, bool empty,
    unsigned fault)
{
    fixture_t f;
    fixture(&f, kind);
    populate(f.s, kind, empty);
    populate(f.raw, kind, empty);
    capture_t events = {.head = "out"}, raw_events = {.head = "out"};
    capture_t image = {.head = "out"}, raw_image = {.head = "out"};
    capture_t want = {.head = "out"};
    literal(kind, empty, &want);
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, &events);
        wl_session_set_delta_cb(f.raw, on_delta, &raw_events);
    }
    source_t src;
    source_save(&src, f.s);
    col_rel_t *old = session_find_rel((wl_col_session_t *)f.s, "out");
    col_relation_snapshot_t old_image = wl_columnar_relation_snapshot(old);
    active = f.s; map_inline = concat_seen = 0;
    stop_mode = fault; stop_hit = false;
    stop_at = kind == K_UNION ? "$concat" : "$map";
    wl_columnar_eval_stack_test_physical_publication = publication;
    int rc = snapshot ? wl_session_snapshot(f.s, on_tuple, &image)
        : step(f.s);
    int expected = fault == 1 ? WL_EVALUATION_CONTROL_CANCELLED
        : fault == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
    if (rc != expected)
        fprintf(stderr, "kind%u snapshot%u observed%u empty%u fault%u: "
            "rc %d, expected %d\n", kind, snapshot, observed, empty, fault,
            rc, expected);
    CHECK(rc == expected);
    if (fault) {
        /* Nothing is published before the stop; reset and retry finishes. */
        CHECK(stop_hit && !image.count && !events.count);
        source_kept(&src, f.s, true);
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
        CHECK(!(snapshot ? wl_session_snapshot(f.s, on_tuple, &image)
            : step(f.s)));
    }
    CHECK(!(snapshot ? wl_session_snapshot(f.raw, on_tuple, &raw_image)
        : step(f.raw)));
    wl_columnar_eval_stack_test_physical_publication = NULL;
    source_kept(&src, f.s, false);
    /* The controlled route did the work: MAP read the INLINE source, and
     * the UNION body went through CONCAT. */
    if (!empty)
        CHECK(map_inline >= 1);
    if (kind == K_UNION)
        CHECK(concat_seen >= 1);
    col_rel_t *head = session_find_rel((wl_col_session_t *)f.s, "out");
    CHECK(head && head->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && !head->compound_arity_map && !head->timestamps);
    if (snapshot) {
        equal_capture(&image, &want, "snapshot image");
        equal_capture(&raw_image, &want, "raw snapshot image");
    }
    if (observed) {
        equal_capture(&events, &want, "events");
        /* An evaluating raw SNAPSHOT emits its tuple image without the STEP
         * notification transaction, so events are compared with the raw
         * session for STEP only. */
        if (!snapshot) {
            equal_capture(&raw_events, &want, "raw events");
        } else {
            CHECK(!raw_events.count);
        }
    }
    capture_t final = {.head = "out"};
    CHECK(!wl_session_snapshot(f.s, on_tuple, &final));
    equal_capture(&final, &want, "final image");
    fixture_destroy(&f);
}

/* Consumers that still refuse an INLINE input keep a typed ENOTSUP: the
 * controlled session publishes nothing and leaves the source as it was,
 * while the uncontrolled session computes the rows (open in #2301). */
static void
refused_consumer(const char *text, const char *head, unsigned want_rows)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    wl_evaluation_control_t *control = NULL;
    CHECK(governor && !wl_evaluation_control_create(UINT64_C(1000000000),
        &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    options.memory_governor = governor;
    wl_session_t *s = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &s));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    const int64_t b[] = {10, 7, 30, 8};
    for (unsigned i = 0; i < 2; i++) {
        wl_session_t *x = i ? raw : s;
        CHECK(!wl_session_insert(x, "a", pair_rows, 4, 3));
        CHECK(!wl_session_insert(x, "b", b, 2, 2));
    }
    source_t src;
    source_save(&src, s);
    capture_t events = {.head = head}, raw_events = {.head = head};
    wl_session_set_delta_cb(s, on_delta, &events);
    wl_session_set_delta_cb(raw, on_delta, &raw_events);
    col_rel_t *old = session_find_rel((wl_col_session_t *)s, head);
    int rc = step(s);
    if (rc != ENOTSUP)
        fprintf(stderr, "%s: rc %d, expected ENOTSUP\n", head, rc);
    CHECK(rc == ENOTSUP && !events.count);
    source_kept(&src, s, true);
    CHECK(session_find_rel((wl_col_session_t *)s, head) == old);
    CHECK(!step(raw) && raw_events.count == want_rows);
    CHECK(!wl_session_try_destroy(&s));
    wl_session_destroy(raw);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)) == 0);
    wl_columnar_memory_governor_ref_release(governor);
}

int
main(void)
{
    for (unsigned kind = 0; kind < K_COUNT; kind++)
        for (unsigned snapshot = 0; snapshot < 2; snapshot++)
            for (unsigned observed = 0; observed < 2; observed++) {
                run_case(kind, snapshot, observed, false, 0);
                run_case(kind, snapshot, observed, true, 0);
                for (unsigned fault = 1; fault < 3; fault++)
                    run_case(kind, snapshot, observed, false, fault);
            }
    /* x > 0 keeps ids 1 and 2; the join matches x = 10 and x = 30. */
    refused_consumer(".decl a(id:int64,p:pair/2 inline)\n"
        ".decl b(k:int64,v:int64)\n.decl f(id:int64,y:int64)\n"
        "f(i,y) :- a(i,pair(x,y)), x > 0.\n", "f", 2);
    refused_consumer(".decl a(id:int64,p:pair/2 inline)\n"
        ".decl b(k:int64,v:int64)\n.decl j(id:int64,z:int64)\n"
        "j(i,z) :- a(i,pair(x,y)), b(x,z).\n", "j", 2);
    puts("evaluation_compound_sources: OK");
    return 0;
}
