/* Natural ordinary JOIN over INLINE sources: physical lanes and real retry. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

typedef struct { int64_t x, y; int sign; } row_t;
typedef struct { row_t rows[32]; unsigned count; } capture_t;
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    wl_session_t *s, *raw;
    unsigned kind, route, left_width, right_width;
    int64_t ys[5], tag, floating;
    capture_t events;
} fixture_t;
static fixture_t *active;
static unsigned inline_seen, pairs, filter_rows, mode;
static uint32_t right_row;
static bool stopped, joined_rejected;

static int
order(const void *a, const void *b)
{
    const row_t *x = a, *y = b;
    if (x->x != y->x) return (x->x > y->x) - (x->x < y->x);
    if (x->y != y->y) return (x->y > y->y) - (x->y < y->y);
    return (x->sign > y->sign) - (x->sign < y->sign);
}
static void
capture(const char *name, const int64_t *row, uint32_t width, int diff,
    void *arg)
{
    capture_t *c = arg;
    CHECK(!strcmp(name, "out") && width == 2 && (diff == 1 || diff == -1)
        && c->count < 32);
    c->rows[c->count++] = (row_t){row[0], row[1], diff};
}
static void tuple(const char *name, const int64_t *row, uint32_t width,
    void *arg)
{
    capture(name, row, width, 1, arg);
}
static void
same(capture_t *a, capture_t *b)
{
    qsort(a->rows, a->count, sizeof(row_t), order);
    qsort(b->rows, b->count, sizeof(row_t), order);
    CHECK(a->count == b->count);
    for (unsigned i = 0; i < a->count;
        ++i) CHECK(!order(&a->rows[i], &b->rows[i]));
}
static bool
contains(const capture_t *c, const row_t *r)
{
    for (unsigned i = 0; i < c->count; ++i)
        if (c->rows[i].x == r->x && c->rows[i].y == r->y) return true;
    return false;
}
static capture_t
difference(const capture_t *before, const capture_t *after)
{
    capture_t c = {0};
    const capture_t *sides[] = {before, after};
    for (unsigned s = 0; s < 2; ++s)
        for (unsigned i = 0; i < sides[s]->count; ++i)
            if (!contains(sides[1 - s], &sides[s]->rows[i])) {
                c.rows[c.count] = sides[s]->rows[i];
                c.rows[c.count++].sign = s ? 1 : -1;
            }
    return c;
}
static int
evaluate(fixture_t *f, wl_session_t *s, capture_t *image)
{
    for (unsigned turn = 0; turn < 100000; ++turn) {
        int rc = f->route == 2 ? wl_session_snapshot(s, tuple, image)
            : wl_session_step(s);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}
static void
image(wl_session_t *s, capture_t *want, bool strings)
{
    col_rel_t *out = session_find_rel(COL_SESSION(s), "out");
    CHECK(out && out->ncols == 2);
    capture_t got = {0};
    for (uint32_t i = 0; i < out->nrows; ++i)
        capture("out",
            (int64_t[]){col_rel_get(out, i, 0), col_rel_get(out, i, 1)},
            2, 1, &got);
    same(&got, want);
    if (out->nrows) CHECK(out->column_types
            && out->column_types[0] == WIRELOG_TYPE_INT64
            && out->column_types[1] ==
            (strings ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64)
            && out->compound_kind == WIRELOG_COMPOUND_KIND_NONE);
}

/* Independent logical rows. Predicate-false y0/"x" has flag1 and matches a
 * key, so it genuinely reaches the post-JOIN FILTER. No production evaluator
 * or uncontrolled result is used to derive this reference. */
static capture_t
expected(fixture_t *f, bool changed, bool removed)
{
    capture_t c = {0};
    const int64_t xs[] = {1, 2, 3};
    const int64_t keys[] = {1, 2, 1};
    const unsigned rhs[] = {0, 3, 4};
    const int64_t rkeys[] = {1, 2, 2};
    unsigned left_count = changed && f->kind ? 3 : 2;
    unsigned right_count = changed && !f->kind ? 3 : 2;
    for (unsigned l = 0; l < left_count; ++l)
        for (unsigned r = 0; r < right_count; ++r) {
            if ((removed && !r) ||
                (f->kind != 3 && keys[l] != rkeys[r])) continue;
            c.rows[c.count++] = (row_t){xs[l], f->ys[rhs[r]], 1};
        }
    return c;
}
static void
observe_source(const wl_columnar_relation_scalar_source_t *s, unsigned bit)
{
    CHECK(s->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD && s->descriptor
        && s->width == s->descriptor->ncols && s->columns ==
        (const int64_t *const *)s->descriptor->columns);
    if (s->descriptor->compound_kind != WIRELOG_COMPOUND_KIND_INLINE) return;
    CHECK(s->descriptor->compound_arity_map &&
        s->descriptor->compound_arity_len);
    inline_seen |= bit;
    if (active->kind >= 2 && bit == 1)
        CHECK(s->width == 4 && s->types[0] == WIRELOG_TYPE_STRING
            && s->types[1] == WIRELOG_TYPE_FLOAT);
    if (active->kind >= 2 && bit == 2)
        CHECK(s->width == 4 && s->types[1] == WIRELOG_TYPE_STRING);
}
static void
checkpoint(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    CHECK(active && control == active->control);
    if (phase == 44) ++filter_rows;
    if (phase != 17 && phase != 18) return;
    wl_col_session_t *session = COL_SESSION(active->s);
    wl_columnar_eval_serial_framed_attempt_t *root = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
        session, &root));
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(session->cleanup_active,
        &ctx, &root->child));
    CHECK(ctx->source_initialized && ctx->join_right_initialized &&
        ctx->pin_owned
        && root->child.control == control);
    observe_source(&ctx->source, 1); observe_source(&ctx->join_right, 2);
    if (phase == 17) {
        ++pairs; right_row = (uint32_t)(progress % ctx->join_right.nrows);
    }
    if (phase != 18) return; /* phase18 is after a real matching row gather. */
    unsigned y_lane = active->kind == 1 ? 3 : 1;
    if (ctx->join_right.columns[y_lane][right_row] ==
        active->ys[1]) joined_rejected = true;
    const col_rel_t *held = active->kind ? ctx->join_right.descriptor
        : ctx->source.descriptor;
    if (!mode || stopped || pairs < 2
        || held->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        || progress != 1) return;
    stopped = true;
    /* Both original readers are HELD here; the ordinary retirement must
     * release them after cancellation/WORK, with no borrowed new permission. */
    if (mode == 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge_attempt(control,
            wl_evaluation_control_active_attempt_id(control), UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}
static void
insert_left(fixture_t *f, wl_session_t *s, int64_t k, int64_t x)
{
    int64_t row[] = {f->kind >= 2 ? f->tag : 100,
                     f->kind >= 2 ? f->floating : 200, k, x};
    CHECK(!wl_session_insert(s, "a", f->kind == 1 ? &row[2] : row, 1,
        f->left_width));
}
static void
right_row_values(fixture_t *f, unsigned n, int64_t *row)
{
    int64_t key = n == 3 || n == 4 ? 2 : 1, flag = n == 2 ? 0 : 1;
    if (!f->kind) {
        row[0] = key; row[1] = f->ys[n]; row[2] = flag;
    }else if (f->kind == 1) {
        row[0] = 300; row[1] = 400; row[2] = key; row[3] = f->ys[n];
        row[4] = flag;
    } else {
        row[0] = key; row[1] = f->ys[n]; row[2] = 500; row[3] = flag;
    }
}
static void
right_change(fixture_t *f, wl_session_t *s, unsigned n, bool remove)
{
    int64_t row[5]; right_row_values(f, n, row);
    CHECK(!(remove ? wl_session_remove(s, "b", row, 1, f->right_width)
        : wl_session_insert(s, "b", row, 1, f->right_width)));
}
static void
run(unsigned kind, unsigned route, unsigned fault)
{
    fixture_t f = {.kind = kind, .route = route};
    const char *texts[] = {
        ".decl a(p:pair/2 inline,k:int64,x:int64)\n.decl b(k:int64,y:int64,flag:int64)\n"
        ".decl out(x:int64,y:int64)\nout(x,y):-a(pair(v,w),k,x),b(k,y,1),x<y.\n",
        ".decl a(k:int64,x:int64)\n.decl b(p:pair/2 inline,k:int64,y:int64,flag:int64)\n"
        ".decl out(x:int64,y:int64)\nout(x,y):-a(k,x),b(pair(v,w),k,y,1),x<y.\n",
        ".decl a(p:pair(symbol,float) inline,k:int64,x:int64)\n"
        ".decl b(k:int64,p:pair(symbol,int64) inline,flag:int64)\n"
        ".decl out(x:int64,y:string)\nout(x,y):-a(pair(tag,v),k,x),b(k,pair(y,w),1),strlen(y)>2.\n",
        ".decl a(p:pair(symbol,float) inline,k:int64,x:int64)\n"
        ".decl b(k:int64,p:pair(symbol,int64) inline,flag:int64)\n"
        ".decl out(x:int64,y:string)\nout(x,y):-a(pair(tag,v),k,x),b(j,pair(y,w),1),strlen(y)>2.\n"
    };
    wirelog_error_t error;
    f.program = wirelog_parse_string(texts[kind], &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    CHECK(f.plan->stratum_count == 1 && f.plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *p = &f.plan->strata[0].relations[0];
    const wl_plan_op_t *join = NULL;
    for (uint32_t i = 0; i < p->op_count; ++i)
        if (p->ops[i].op == WL_PLAN_OP_JOIN) {
            CHECK(!join); join = &p->ops[i];
        }
    CHECK(join && !strcmp(join->right_relation,
        "b") && join->right_filter_expr.size
        && join->key_count == (kind == 3 ? 0u : 1u));
    if (kind != 3) CHECK(!strcmp(join->left_keys[0],
            kind == 1 ? "col0" : "col2")
            && !strcmp(join->right_keys[0], kind == 1 ? "col2" : "col0"));
    f.left_width = kind == 1 ? 2 : 4;
    f.right_width = !kind ? 3 : kind == 1 ? 5 : 4;
    f.tag = wl_intern_put(f.plan->intern, "tag");
    f.floating = wl_columnar_float_to_bits(1.5);
    int64_t values[] = {10, 0, 99, 20, 30};
    const char *strings[] = {"keep", "x", "ignored", "also", "more"};
    for (unsigned i = 0; i < 5;
        ++i) f.ys[i] = kind >= 2 ? wl_intern_put(f.plan->intern,
                strings[i]) : values[i];
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &f.control));
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .usable_bytes =
         WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = f.control;
    options.memory_governor = f.governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.s));
    CHECK(!wl_session_create(wl_backend_columnar(), f.plan, 1, &f.raw));
    if (route == 0) wl_session_set_delta_cb(f.s, capture, &f.events);
    active = &f; mode = 0; inline_seen = pairs = filter_rows = 0;
    stopped = joined_rejected = false;
    wl_columnar_join_test_attempt_checkpoint = checkpoint;
    wl_session_t *sessions[] = {f.s, f.raw};
    for (unsigned i = 0; i < 2; ++i) {
        insert_left(&f, sessions[i], 1, 1); insert_left(&f, sessions[i], 2, 2);
        insert_left(&f, sessions[i], 1, 1); /* Genuine duplicate input. */
        capture_t empty = {0};
        CHECK(!evaluate(&f, sessions[i], &empty) && !empty.count);
    }
    capture_t old = {0};
    for (unsigned stage = 0; stage < 3; ++stage) {
        for (unsigned i = 0; i < 2; ++i) {
            if (!stage) for (unsigned n = 0; n < 4;
                    ++n) right_change(&f, sessions[i], n, false);
            else if (stage == 1) {
                if (kind) insert_left(&f, sessions[i], 1, 3);
                else right_change(&f, sessions[i], 4, false);
            } else right_change(&f, sessions[i], 0, true);
        }
        capture_t snap = {0}, raw_snap = {0};
        if (stage == 1 && fault) {
            mode = fault; pairs = 0; stopped = false;
            col_rel_t *head = session_find_rel(COL_SESSION(f.s), "out");
            col_rel_t *left = session_find_rel(COL_SESSION(f.s), "a"),
                *right = session_find_rel(COL_SESSION(f.s), "b");
            col_relation_snapshot_t ls = wl_columnar_relation_snapshot(left),
                rs = wl_columnar_relation_snapshot(right);
            uint32_t lm[8] = {0}, rm[8] = {0};
            uint32_t ln = left->compound_arity_len,
                rn = right->compound_arity_len;
            CHECK(ln <= 8 && rn <= 8);
            if (ln) memcpy(lm, left->compound_arity_map, ln * sizeof(*lm));
            if (rn) memcpy(rm, right->compound_arity_map, rn * sizeof(*rm));
            int rc = evaluate(&f, f.s, &snap);
            CHECK(stopped &&
                rc ==
                (fault ==
                1 ? WL_EVALUATION_CONTROL_CANCELLED :
                WL_EVALUATION_CONTROL_WORK_LIMIT)
                && !snap.count && !f.events.count &&
                COL_SESSION(f.s)->pending_input_change
                && session_find_rel(COL_SESSION(f.s), "out") == head);
            image(f.s, &old, kind >= 2);
            CHECK(wl_columnar_relation_snapshot_equal(ls,
                wl_columnar_relation_snapshot(left))
                && wl_columnar_relation_snapshot_equal(rs,
                wl_columnar_relation_snapshot(right)));
            CHECK(left->compound_arity_len == ln &&
                right->compound_arity_len == rn);
            if (ln) CHECK(!memcmp(lm, left->compound_arity_map,
                    ln * sizeof(*lm)));
            if (rn) CHECK(!memcmp(rm, right->compound_arity_map,
                    rn * sizeof(*rm)));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control,
                &report) && report.result == rc
                && report.consumed && !report.cleanup_status);
            CHECK(!wl_evaluation_control_reset(f.control)); mode = 0;
        }
        CHECK(!evaluate(&f, f.s, &snap) && !evaluate(&f, f.raw, &raw_snap));
        capture_t want = expected(&f, stage > 0, stage == 2);
        image(f.s, &want, kind >= 2); image(f.raw, &want, kind >= 2);
        if (route == 2) {
            same(&snap, &want); same(&raw_snap, &want);
        }
        if (route == 0) {
            capture_t ev = difference(&old, &want); same(&f.events, &ev);
            f.events.count = 0;
        }
        old = want;
        snap.count = raw_snap.count = 0;
        CHECK(!evaluate(&f, f.s, &snap) && !evaluate(&f, f.raw, &raw_snap));
        CHECK(!f.events.count);
        if (route == 2) {
            same(&snap, &want); same(&raw_snap, &want);
        }
    }
    CHECK(inline_seen == (kind == 0 ? 1u : kind == 1 ? 2u : 3u)
        && pairs && filter_rows && joined_rejected);
    wl_columnar_join_test_attempt_checkpoint = NULL; active = NULL;
    CHECK(!wl_session_try_destroy(&f.s) && !wl_session_try_destroy(&f.raw));
    wl_evaluation_control_release(f.control); wl_plan_free(f.plan);
    wirelog_program_free(f.program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f.governor)));
    wl_columnar_memory_governor_ref_release(f.governor);
}
int main(int argc, char **argv)
{
    CHECK(argc == 2); unsigned route = !strcmp(argv[1], "observed") ? 0
        : !strcmp(argv[1], "plain") ? 1 : 2;
    CHECK(route != 2 || !strcmp(argv[1], "snapshot"));
    for (unsigned kind = 0; kind < 4; ++kind)
        for (unsigned fault = 0; fault < 3; ++fault) run(kind, route, fault);
    puts("INLINE JOIN physical source/predicate/signed retry histories passed");
    return 0;
}
