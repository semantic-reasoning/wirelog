/* Natural ordinary JOIN over INLINE sources: physical lanes and real retry. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/columnar/join_pipeline.h"
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
    const wl_plan_op_t *join;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    wl_session_t *s, *raw;
    unsigned kind, route, left_width, right_width;
    int64_t ys[5], tag, floating;
    capture_t events;
} fixture_t;
static fixture_t *active;
static bool configured, streamed, stream_strict;
static unsigned stream_produced, stream_acked, stream_rejected, stream_kept,
    stream_cells;
static uint64_t stream_attempt, stream_generation;
static void
configure(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}
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
            c.rows[c.count++] = (row_t){xs[l] + (streamed &&
                                        f->kind == 1 ? 1 : 0), f->ys[rhs[r]],
                                        1};
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
    if (streamed && f->kind == 2) k = wl_columnar_float_to_bits((double)k);
    int64_t row[] = {f->kind >= 2 ? f->tag : 100,
                     f->kind >= 2 ? f->floating : 200, k, x};
    CHECK(!wl_session_insert(s, "a", f->kind == 1 ? &row[2] : row, 1,
        f->left_width));
}
static void
right_row_values(fixture_t *f, unsigned n, int64_t *row)
{
    int64_t key = streamed && n == 2 ? 91 : n == 3 || n == 4 ? 2 : 1;
    int64_t flag = streamed &&
        f->kind == 2 ? (n == 1 ? 0 : 100) : n == 2 ? 0 : 1;
    if (streamed && f->kind == 2) key = wl_columnar_float_to_bits((double)key);
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
stream_stop_now(wl_evaluation_control_t *control)
{
    CHECK(!stopped); stopped = true;
    if (mode % 2) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge_attempt(control,
            wl_evaluation_control_active_attempt_id(control), UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}
static void
stream_prepare(const wl_columnar_join_batch_prepare_t *a)
{
    if (a->phase < 4) return;
    const col_rel_t *rel[] = {a->left, a->right};
    const wl_columnar_source_access_reader_t *readers[] = {&a->left_reader,
                                                           &a->right_reader};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(readers[i]->identity == (uintptr_t)readers[i]
            && readers[i]->secondary_owner == &rel[i]->descriptor_access
            && readers[i]->owner == &rel[i]->storage_owner->source_access);
        if (rel[i]->compound_kind ==
            WIRELOG_COMPOUND_KIND_INLINE) inline_seen |= 1u << i;
    }
    if (active->kind == 2) CHECK(a->left->column_types[0] == WIRELOG_TYPE_STRING
            && a->left->column_types[1] == WIRELOG_TYPE_FLOAT
            && a->left->column_types[2] == WIRELOG_TYPE_FLOAT
            && a->right->column_types[0] == WIRELOG_TYPE_FLOAT
            && a->right->column_types[1] == WIRELOG_TYPE_STRING);
}
static void
stream_event(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t work)
{
    (void)extent; (void)work;
    CHECK(active && control == active->control);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(id);
    if (id != stream_attempt) {
        stream_attempt = id; stream_generation = 0;
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE) ++stream_produced;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK) {
        CHECK(generation == ++stream_generation); ++stream_acked;
    }
    bool before = event == WL_COLUMNAR_JOIN_PIPELINE_TEST_BEFORE_ACK;
    bool after = event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK;
    if (!stopped && generation >= 2 &&
        ((before && (mode == 5 || mode == 6)) ||
        (after && (mode == 7 || mode == 8))))
        stream_stop_now(control);
}
static void
stream_write(wl_evaluation_control_t *control, unsigned phase, uint64_t column,
    const col_rel_t *batch)
{
    CHECK(batch && batch->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && batch->ncols >= 2 && batch->column_types);
    if (phase != 1) return;
    ++stream_cells;
    if (!stopped && (mode == 1 || mode == 2) && column == 1) {
        CHECK(!batch->nrows); /* first cell is private; row not published */
        stream_stop_now(control);
    }
}
static uint32_t
stream_lane(const fixture_t *f, uint32_t physical)
{
    if (!f->join->project_count) return physical;
    for (uint32_t c = 0; c < f->join->project_count; ++c)
        if (f->join->project_indices[c] == physical) return c;
    CHECK(false); return 0;
}
static void
stream_filter(wl_evaluation_control_t *control, unsigned phase, uint64_t row)
{
    if (phase != 13 || control != active->control) return;
    wl_columnar_eval_serial_framed_attempt_t *root = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(COL_SESSION(
            active->s)->cleanup_active,
        COL_SESSION(active->s), &root));
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(COL_SESSION(
            active->s)->cleanup_active,
        &ctx, &root->child));
    CHECK(ctx->source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
        && ctx->source.descriptor->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && row < ctx->source.nrows);
    uint32_t y = stream_lane(active,
            active->left_width + (active->kind == 1 ? 3 : 1));
    CHECK(y < ctx->width && ctx->source.types[y] ==
        (active->kind == 2 ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64));
    /* The only matching rejected fact is literal y=0 / y="x". The other
     * rejected-looking input has a nonmatching key91. */
    bool reject = ctx->values[y] == active->ys[1];
    if (reject) ++stream_rejected; else ++stream_kept;
    if (reject && !stopped &&
        (mode == 3 || mode == 4)) stream_stop_now(control);
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
    const char *unfiltered[] = {
        ".decl a(p:pair/2 inline,k:int64,x:int64)\n.decl b(k:int64,y:int64,flag:int64)\n"
        ".decl out(x:int64,y:int64)\nout(x,y):-a(pair(v,w),k,x),b(k,y,flag).\n",
        ".decl a(k:int64,x:int64)\n.decl b(p:pair/2 inline,k:int64,y:int64,flag:int64)\n"
        ".decl out(x:int64,y:int64)\nout(x,y):-a(k,x),b(pair(v,w),k,y,flag).\n",
        ".decl a(p:pair(symbol,float) inline,k:int64,x:int64)\n"
        ".decl b(k:int64,p:pair(symbol,int64) inline,flag:int64)\n"
        ".decl out(x:int64,y:string)\nout(x,y):-a(pair(tag,v),k,x),b(k,pair(y,w),flag).\n"
    };
    const char *stream_texts[] = {
        ".decl a(p:pair/2 inline,k:int64,x:int64)\n.decl b(k:int64,y:int64,flag:int64)\n"
        ".decl out(x:int64,y:int64)\nout(x,y):-a(pair(v,w),k,x),b(k,y,flag),x<y.\n",
        ".decl a(k:int64,x:int64)\n.decl b(p:pair/2 inline,k:int64,y:int64,flag:int64)\n"
        ".decl out(x:int64,y:int64)\nout(x+1,y):-a(k,x),b(pair(v,w),k,y,flag),x<y.\n",
        ".decl a(p:pair(symbol,float) inline,k:float,x:int64)\n"
        ".decl b(k:float,p:pair(symbol,int64) inline,flag:int64)\n"
        ".decl out(x:int64,y:string)\nout(x,y):-a(pair(tag,v),k,x),b(k,pair(y,w),flag),x<flag.\n"
    };
    wirelog_error_t error;
    f.program =
        wirelog_parse_string(streamed ? stream_texts[kind] : configured ?
            unfiltered[kind] : texts[kind],
            &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    CHECK(f.plan->stratum_count == 1 && f.plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *p = &f.plan->strata[0].relations[0];
    const wl_plan_op_t *join = NULL;
    for (uint32_t i = 0; i < p->op_count; ++i)
        if (p->ops[i].op == WL_PLAN_OP_JOIN) {
            CHECK(!join); join = &p->ops[i];
        }
    CHECK(join && !strcmp(join->right_relation,
        "b") &&
        (configured ? !join->right_filter_expr.size :
        join->right_filter_expr.size)
        && join->key_count == (kind == 3 ? 0u : 1u));
    if (kind != 3) CHECK(!strcmp(join->left_keys[0],
            kind == 1 ? "col0" : "col2")
            && !strcmp(join->right_keys[0], kind == 1 ? "col2" : "col0"));
    f.join = join;
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
    configure("WIRELOG_JOIN_BATCH_BYTES",
        streamed ? "64" : configured ? "256" : NULL);
    configure("WIRELOG_JOIN_BATCH_STRICT",
        streamed && stream_strict ? "1" : NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.s));
    CHECK(!wl_session_create(wl_backend_columnar(), f.plan, 1, &f.raw));
    if (route == 0) wl_session_set_delta_cb(f.s, capture, &f.events);
    active = &f; mode = 0; inline_seen = pairs = filter_rows = 0;
    stopped = joined_rejected = false;
    stream_produced = stream_acked = stream_rejected = stream_kept =
        stream_cells = 0;
    stream_attempt = stream_generation = 0;
    wl_columnar_join_test_attempt_checkpoint = streamed ? NULL : checkpoint;
    if (streamed) {
        uint32_t ji = (uint32_t)(join - p->ops), tail = UINT32_MAX;
        CHECK(wl_columnar_join_pipeline_preflight(p, ji, COL_SESSION(f.s),
            &tail)
            == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE && tail > ji);
        wl_columnar_join_batch_test_prepare_checkpoint = stream_prepare;
        wl_columnar_join_pipeline_test_numeric_checkpoint = stream_event;
        wl_columnar_join_batch_test_control_checkpoint = stream_write;
        wl_columnar_filter_test_attempt_checkpoint = stream_filter;
    }
    wl_session_t *sessions[] = {f.s, f.raw};
    for (unsigned i = 0; i < 2; ++i) {
        insert_left(&f, sessions[i], 1, 1); insert_left(&f, sessions[i], 2, 2);
        if (!streamed) insert_left(&f, sessions[i], 1, 1); /* Duplicate control. */
        capture_t empty = {0};
        CHECK(!evaluate(&f, sessions[i], &empty) && !empty.count);
    }
    capture_t old = {0};
    for (unsigned stage = 0; stage < 3; ++stage) {
        for (unsigned i = 0; i < 2; ++i) {
            if (!stage) for (unsigned n = 0; n < 4;
                    ++n) {
                    if (streamed || !configured || n == 0 || n == 3)
                        right_change(&f, sessions[i], n, false);
                }
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
            if (streamed && fault == 9) COL_SESSION(f.s)->join_output_limit = 3;
            int rc = evaluate(&f, f.s, &snap);
            int wanted = streamed && fault == 9 ? EOVERFLOW
                : (fault %
                2 ? WL_EVALUATION_CONTROL_CANCELLED :
                WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK((streamed && fault == 9 ? !stopped : stopped) &&
                rc == wanted
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
            if (streamed && fault == 9) COL_SESSION(f.s)->join_output_limit = 4;
        }
        int controlled_rc = evaluate(&f, f.s, &snap);
        int raw_rc = evaluate(&f, f.raw, &raw_snap);
        if (controlled_rc || raw_rc) fprintf(stderr,
                "kind=%u route=%u fault=%u stage=%u controlled=%d raw=%d\n",
                kind, route, fault, stage, controlled_rc, raw_rc);
        CHECK(!controlled_rc && !raw_rc);
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
    CHECK(inline_seen == (kind == 0 ? 1u : kind == 1 ? 2u : 3u));
    if (streamed) CHECK(stream_produced && stream_acked >= 2 &&
            stream_rejected && stream_kept
            && (kind != 2 || stream_cells));
    else CHECK(pairs &&
            (configured ? !filter_rows : filter_rows && joined_rejected));
    wl_columnar_join_batch_test_prepare_checkpoint = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    wl_columnar_join_batch_test_control_checkpoint = NULL;
    wl_columnar_filter_test_attempt_checkpoint = NULL;
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
    streamed = argc >= 2 && !strncmp(argv[1], "streamed-", 9);
    CHECK(argc == (streamed ? 4 : 2));
    configured = streamed || !strncmp(argv[1], "configured-", 11);
    const char *name = streamed ? argv[1] + 9 : configured ? argv[1] +
        11 : argv[1];
    unsigned route = !strcmp(name, "observed") ? 0 : !strcmp(name,
            "plain") ? 1 : 2;
    CHECK(route != 2 || !strcmp(name, "snapshot"));
    if (streamed) {
        unsigned kind = (unsigned)strtoul(argv[2], NULL, 10);
        stream_strict = !strcmp(argv[3], "1");
        CHECK(kind < 3 && (!strcmp(argv[3], "0") || stream_strict));
        for (unsigned fault = 0; fault < 10; ++fault) {
            if (kind != 2 && (fault == 1 || fault == 2)) continue;
            run(kind, route, fault);
        }
    } else for (unsigned kind = 0; kind < (configured ? 3u : 4u); ++kind)
            for (unsigned fault = 0; fault < 3;
                ++fault) run(kind, route, fault);
    puts("INLINE JOIN physical source/predicate/signed retry histories passed");
    return 0;
}
