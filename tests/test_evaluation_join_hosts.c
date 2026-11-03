/* Actual session JOIN producer, private sink and rollback qualification. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    wl_session_t *session, *raw;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    unsigned width[2], rows[2], kind, events, output_width;
    int64_t data[2][8 * 129];
    bool seen[8][8];
} fixture_t;

static fixture_t *active;
static unsigned hits[15], target;
static unsigned consumer_ordinal, stop_consumer_ordinal;
static wl_columnar_filter_stage_t stop_consumer_stage;
static bool stopped, work_stop;
static unsigned denial_event, denial_phase;
static bool sink_allocation_fault;
static wl_columnar_memory_reservation_t peer;
static uint64_t attempt, last_generation;

static int
finish(wl_session_t *session)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false);
    return EIO;
}

static void
add(char *out, size_t cap, size_t *used, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    int n = vsnprintf(out + *used, cap - *used, format, args);
    va_end(args);
    CHECK(n >= 0 && (size_t)n < cap - *used);
    *used += (size_t)n;
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int diff,
    void *userdata)
{
    if (strcmp(name, "output")) return;
    fixture_t *f = userdata;
    CHECK(width == f->output_width && diff == 1);
    bool narrow = f->kind == 8 || f->kind == 9;
    unsigned a = (unsigned)row[narrow ? 0 : 1],
        b = (unsigned)row[narrow ? 1 : f->width[0] + 1];
    if (f->kind == 7) {
        CHECK(a > 0 && !(b % 2)); --a; b /= 2;
    }
    CHECK(a < 8 && b < 8 && a < b && !f->seen[a][b]);
    if (narrow) {
        CHECK(row[0] == a && row[1] == b);
        f->seen[a][b] = true; ++f->events; return;
    }
    int64_t key = f->kind == 11 ? wl_columnar_float_to_bits(1.0) : 1;
    CHECK(row[0] == key && row[f->width[0]] == key);
    for (unsigned c = 1; c < f->width[0]; ++c)
        CHECK(row[c] == (int64_t)(100 * (c - 1) + a + (f->kind == 7 ? 1 : 0)));
    for (unsigned c = 1; c < f->width[1]; ++c)
        CHECK(row[f->width[0] + c] ==
            (int64_t)(100 * (c - 1) + b) * (f->kind == 7 ? 2 : 1));
    f->seen[a][b] = true;
    ++f->events;
}

static void
reserve_peer(void)
{
    wl_columnar_memory_governor_t *gov =
        wl_columnar_memory_governor_ref_get(active->governor);
    uint64_t available = (UINT64_C(512) << 20)
        - wl_columnar_memory_reserved(gov);
    CHECK(available > 1 && !peer.governor);
    CHECK(wl_columnar_memory_reserve(gov, available - 1, &peer));
}

static void
prepare_checkpoint(const wl_columnar_join_batch_prepare_t *a)
{
    if (a->phase == denial_phase && !peer.governor) reserve_peer();
}

static void
checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t producer_work)
{
    CHECK(active && control == active->control && event < 15);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(id && (!attempt || attempt == id));
    attempt = id;
    wl_col_session_t *session = (wl_col_session_t *)active->session;
    CHECK(session->cleanup_active);
    if (!active->frame) active->frame = session->cleanup_active;
    CHECK(active->frame == session->cleanup_active);
    ++hits[event];
    if (event == denial_event && !peer.governor) reserve_peer();
    if (sink_allocation_fault &&
        event == WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK_SCHEMA)
        wl_columnar_relation_test_constructor_fail_allocation = 1;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK) {
        CHECK(generation == last_generation + 1 && extent <= 64
            && producer_work > 0);
        last_generation = generation;
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK)
        CHECK(generation && producer_work);
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED) {
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(active->frame, session,
            &ctx));
        wl_columnar_eval_plan_numeric_attempt_t *r = &ctx->child;
        bool empty_cleanup = true;
        CHECK(wl_columnar_filter_empty_end_pending_owned(active->frame, r,
            &empty_cleanup) == EBUSY && !empty_cleanup);
        const col_rel_t *input = NULL; const wl_plan_op_t *op = NULL;
        uint64_t gen = 0; bool schema = false;
        CHECK(!wl_columnar_join_pipeline_test_stream_view(r, &input, &gen,
            &schema, &op));
        CHECK(input && (schema ? !gen && !input->nrows : gen == generation));
        CHECK(wl_columnar_join_pipeline_stream_authorized(r, active->frame,
            r->join_host, WL_COLUMNAR_JOIN_STREAM_START, op, input, gen,
            schema));
        CHECK(!wl_columnar_join_pipeline_stream_authorized(r, active->frame,
            r->join_host, WL_COLUMNAR_JOIN_STREAM_START, op, input, gen + 1,
            schema));
        CHECK(!wl_columnar_join_pipeline_stream_authorized(r, active->frame,
            r->join_host, WL_COLUMNAR_JOIN_STREAM_START,
            &r->plan->ops[r->cursor],
            input, gen, schema));
        int foreign;
        CHECK(wl_columnar_eval_stack_stream_dispatch_owned(active->frame, r,
            &foreign, op, WL_COLUMNAR_JOIN_STREAM_START, true) == EINVAL);
        eval_stack_t *stack = NULL;
        CHECK(!wl_columnar_eval_stack_cleanup_stack(active->frame)
            && !wl_columnar_eval_stack_cleanup_result(active->frame));
        CHECK(wl_columnar_eval_stack_operator_stack_owned(active->frame,
            session, &stack, r) == EBUSY && !stack);
        CHECK(wl_columnar_eval_stack_numeric_unbind(active->frame, session,
            r) == EBUSY);
        CHECK(wl_columnar_eval_stack_unary_reset_owned(active->frame,
            r) == EBUSY);
        CHECK(wl_columnar_eval_stack_unary_begin_attempt_owned(active->frame,
            control, id, r) == EBUSY);
        wl_columnar_eval_plan_numeric_attempt_t copy = *r;
        CHECK(wl_columnar_eval_stack_stream_enter_owned(active->frame, &copy,
            r->join_host, input, gen, schema, false) == EBUSY);
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_RESTORED) {
        eval_stack_t *stack =
            wl_columnar_eval_stack_cleanup_stack(active->frame);
        CHECK(stack && stack->top == 1 && stack->items[0].rel);
        if (active->kind == 5)
            CHECK(stack->items[0].owned && !stack->items[0].rel->nrows);
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCER_DONE
        && active->rows[0] && active->rows[1] && active->kind != 5)
        CHECK(producer_work);
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER) {
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(active->frame, session,
            &ctx));
        wl_columnar_eval_plan_numeric_attempt_t *r = &ctx->child;
        CHECK(r->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
            && r->pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH);
        CHECK(wl_columnar_eval_stack_numeric_setup_nonstream(active->frame,
            r) == EBUSY);
        wl_columnar_eval_plan_numeric_attempt_t copy = *r;
        CHECK(wl_columnar_eval_stack_numeric_setup_nonstream(active->frame,
            &copy) == EBUSY);
        wl_columnar_filter_context_t *consumer = NULL;
        CHECK(!wl_columnar_eval_stack_filter_context_owned(active->frame,
            &consumer, &ctx->child));
        if (consumer->stage ==
            WL_COLUMNAR_FILTER_STAGE_INERT) ++consumer_ordinal;
        if (stop_consumer_stage
            && (consumer_ordinal != stop_consumer_ordinal
            || consumer->stage != stop_consumer_stage)) return;
        if (stop_consumer_stage == WL_COLUMNAR_FILTER_STAGE_SOURCE)
            CHECK(consumer->source_initialized);
        if (stop_consumer_stage == WL_COLUMNAR_FILTER_STAGE_CONTROL)
            CHECK(consumer->committed);
        if (stop_consumer_stage == WL_COLUMNAR_FILTER_STAGE_FINISH)
            CHECK(!consumer->control && !consumer->pin_owned);
    }
    if (event != target || stopped) return;
    stopped = true;
    if (work_stop) CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
observe(fixture_t *f)
{
    active = f; attempt = last_generation = 0;
    f->frame = NULL;
    consumer_ordinal = 0;
    memset(hits, 0, sizeof(hits));
    wl_columnar_join_pipeline_test_numeric_checkpoint = checkpoint;
}

static void
init_with_filter(fixture_t *f, unsigned kind, bool timestamps,
    unsigned budget_rows, unsigned comparisons)
{
    memset(f, 0, sizeof(*f));
    f->kind = kind;
    f->width[0] = kind == 1 ? 128 : 2;
    f->width[1] = kind == 1 ? 129 : 2;
    f->rows[0] = kind == 4 || kind == 9 ? 0 : 8;
    f->rows[1] = kind == 3 ? 0 : 8;
    f->output_width = kind == 8 || kind == 9 ? 2 : f->width[0] + f->width[1];
    char text[32768] = {0}; size_t used = 0;
    for (unsigned s = 0; s < 2; ++s) {
        add(text, sizeof(text), &used, ".decl %c(k:%s", 'a' + s,
            kind == 11 ? "float" : "int64");
        for (unsigned c = 1; c < f->width[s]; ++c)
            add(text, sizeof(text), &used, ",p%u:int64", c);
        add(text, sizeof(text), &used, ")\n");
        for (unsigned r = 0; r < 8; ++r)
            for (unsigned c = 0; c < f->width[s]; ++c)
                f->data[s][r * f->width[s] + c] = c
                    ? 100 * (c - 1) + r + (kind == 2 && !s ? 10 : 0) : 1;
    }
    if (kind == 11)
        for (unsigned s = 0; s < 2; ++s)
            for (unsigned r = 0; r < 8; ++r)
                f->data[s][r * f->width[s]] = wl_columnar_float_to_bits(1.0);
    if (kind == 10)
        for (unsigned r = 0; r < 8; ++r)
            f->data[0][r * f->width[0] + 1] = -(int64_t)(r + 1);
    add(text, sizeof(text), &used, ".decl output(");
    for (unsigned s = 0; s < 2; ++s)
        for (unsigned c = 0; c < f->width[s]; ++c)
            add(text, sizeof(text), &used, "%s%c%u:%s",
                s || c ? "," : "", 'a' + s, c,
                !c && kind == 11 ? "float" : "int64");
    add(text, sizeof(text), &used, ")\noutput(");
    for (unsigned s = 0; s < 2; ++s)
        for (unsigned c = 0; c < f->width[s]; ++c)
            if (c && kind == 7) add(text, sizeof(text), &used,
                    s ? ",b%u * 2" : ",a%u + 1", c);
            else if (c && kind == 10 && !s) add(text, sizeof(text), &used,
                    ",a%u / b1", c);
            else if (c) add(text, sizeof(text), &used, ",%c%u", 'a' + s, c);
            else add(text, sizeof(text), &used, "%sk", s ? "," : "");
    add(text, sizeof(text), &used, ") :- a(k");
    for (unsigned c = 1; c < f->width[0]; ++c)
        add(text, sizeof(text), &used, ",a%u", c);
    add(text, sizeof(text), &used, "), b(k");
    for (unsigned c = 1; c < f->width[1]; ++c)
        add(text, sizeof(text), &used, ",b%u", c);
    add(text, sizeof(text), &used, "), a1 < b1.\n");
    if (kind == 8 || kind == 9)
        strcpy(text, ".decl a(k:int64,p1:int64)\n.decl b(k:int64,p1:int64)\n"
            ".decl output(x:int64,y:int64)\noutput(x,y):-a(k,x),b(k,y),x<y.\n");
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    const wl_plan_relation_t *head = &f->plan->strata[0].relations[0];
    wl_plan_op_t *ops = (wl_plan_op_t *)head->ops;
    CHECK(head->op_count == 5 && head->ops[1].op == WL_PLAN_OP_JOIN);
    if (comparisons) {
        /* Extend the actual parsed predicate before either session exists.
         * (old predicate == true) preserves every row with depth at most2. */
        CHECK(ops[2].op == WL_PLAN_OP_FILTER);
        wl_plan_expr_buffer_t *expr = &ops[2].filter_expr;
        uint32_t old_size = expr->size;
        expr->data = realloc(expr->data, old_size + 3u * comparisons);
        CHECK(expr->data);
        for (unsigned i = 0; i < comparisons; ++i) {
            uint32_t at = old_size + 3u * i;
            expr->data[at] = WL_PLAN_EXPR_BOOL;
            expr->data[at + 1u] = 1;
            expr->data[at + 2u] = WL_PLAN_EXPR_CMP_EQ;
        }
        expr->size = old_size + 3u * comparisons;
    }
    if (kind == 5) ops[0].delta_mode = WL_DELTA_FORCE_EMPTY;
    if (kind == 6) {
        ops[0].delta_mode = WL_DELTA_FORCE_DELTA;
        ops[1].delta_mode = WL_DELTA_FORCE_FULL;
    }
    CHECK(!wl_evaluation_control_create(0, &f->control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->session));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1, &f->raw));
    uint64_t bytes = (f->width[0] + f->width[1]) * sizeof(int64_t)
        + (timestamps ? sizeof(col_delta_timestamp_t) : 0);
    ((wl_col_session_t *)f->session)->join_batch_bytes = budget_rows
        ? bytes * budget_rows : 1;
    ((wl_col_session_t *)f->raw)->join_batch_bytes = budget_rows
        ? bytes * budget_rows : 1;
    for (unsigned s = 0; s < 2; ++s) {
        char name[2] = {(char)('a' + s), 0};
        wl_session_t *sessions[] = {f->session, f->raw};
        for (unsigned i = 0; i < 2; ++i) {
            CHECK(!wl_session_insert(sessions[i], name, f->data[s],
                f->rows[s] ? f->rows[s] : 1, f->width[s]));
            if (!f->rows[s]) CHECK(!wl_session_remove(sessions[i], name,
                    f->data[s], 1, f->width[s]));
            if (timestamps) {
                col_rel_t *rel =
                    session_find_rel((wl_col_session_t *)sessions[i], name);
                CHECK(!col_rel_enable_timestamps(rel));
                for (unsigned r = 0; r < rel->nrows; ++r)
                    rel->timestamps[r] = (col_delta_timestamp_t){
                        .iteration = 100 + r, .stratum = s, .worker = r,
                        ._reserved = 7, .multiplicity = (int64_t)r - 4
                    };
                wl_columnar_relation_touch_storage(rel);
            }
        }
    }
    if (kind == 6) {
        wl_session_t *sessions[] = {f->session, f->raw};
        for (unsigned i = 0; i < 2; ++i) {
            col_rel_t *full = session_find_rel((wl_col_session_t *)sessions[i],
                    "a");
            col_rel_t *delta = col_rel_new_like("$d$a", full);
            CHECK(delta);
            bool denied = false;
            CHECK(!col_rel_append_rows_atomic(delta,
                f->data[0] + 2 * f->width[0],
                1, f->width[0], &denied));
            if (timestamps &&
                !delta->timestamps) CHECK(!col_rel_enable_timestamps(delta));
            CHECK(!session_add_rel((wl_col_session_t *)sessions[i], delta));
        }
    }
}

static void
init(fixture_t *f, unsigned kind, bool timestamps, unsigned budget_rows)
{
    init_with_filter(f, kind, timestamps, budget_rows, 0);
}

static void
destroy(fixture_t *f)
{
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL; active = NULL;
    CHECK(!wl_session_try_destroy(&f->session));
    CHECK(!wl_session_try_destroy(&f->raw));
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
    wl_evaluation_control_release(f->control);
}

static void
verify(fixture_t *f, wl_session_t *session)
{
    unsigned saved_events = f->events;
    bool saved[8][8]; memcpy(saved, f->seen, sizeof(saved));
    memset(f->seen, 0, sizeof(f->seen)); f->events = 0;
    col_rel_t *out = session_find_rel((wl_col_session_t *)session, "output");
    unsigned expected = f->kind == 6 ? 5 : (f->kind < 2 || f->kind == 7 ||
        f->kind == 8 || f->kind == 11) ? 28 : 0;
    CHECK(out && out->nrows == expected && out->ncols == f->output_width &&
        !out->timestamps);
    if (f->kind == 11) CHECK(out->column_types
            && out->column_types[0] == WIRELOG_TYPE_FLOAT
            && out->column_types[2] == WIRELOG_TYPE_FLOAT);
    int64_t row[257];
    for (unsigned r = 0; r < out->nrows; ++r) {
        for (unsigned c = 0; c < out->ncols; ++c) row[c] = out->columns[c][r];
        capture("output", row, out->ncols, 1, f);
    }
    CHECK(f->events == expected);
    for (unsigned a = 0; a < 8; ++a)
        for (unsigned b = 0; b < 8; ++b)
            CHECK(f->seen[a][b] == (expected && a < b &&
                (f->kind != 6 || a == 2)));
    f->events = saved_events; memcpy(f->seen, saved, sizeof(saved));
    for (unsigned s = 0; s < 2; ++s) {
        char name[2] = {(char)('a' + s), 0};
        col_rel_t *input = session_find_rel((wl_col_session_t *)session, name);
        CHECK(input && input->nrows == f->rows[s]);
        for (unsigned r = 0; r < input->nrows; ++r) {
            for (unsigned c = 0; c < input->ncols; ++c)
                CHECK(input->columns[c][r] == f->data[s][r * input->ncols + c]);
            if (input->timestamps) {
                col_delta_timestamp_t stamp = input->timestamps[r];
                CHECK(stamp.iteration == 100 + r && stamp.stratum == s
                    && stamp.worker == r && stamp._reserved == 7
                    && stamp.multiplicity == (int64_t)r - 4);
            }
        }
    }
}

static void
never_inserted_domains(void)
{
    static const char text[] =
        ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
        ".decl output(x:int64,y:int64)\noutput(x,y) :- a(k,x), b(k,y).\n";
    for (unsigned filtered = 0; filtered < 2; ++filtered)
        for (unsigned empty = 1; empty < 4; ++empty)
            for (unsigned batch = 0; batch < 2; ++batch) {
                fixture_t f = {0};
                wirelog_error_t error;
                f.program = wirelog_parse_string(text, &error);
                CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
                if (filtered) {
                    /* Explicit internal-plan variant on the real session host;
                     * the base program above is compiler-generated unchanged. */
                    const uint8_t predicate[] = {WL_PLAN_EXPR_VAR, 4, 0,
                                                 'c', 'o', 'l', '1',
                                                 WL_PLAN_EXPR_CONST_INT, 23, 0,
                                                 0, 0, 0, 0, 0, 0,
                                                 WL_PLAN_EXPR_CMP_EQ};
                    wl_plan_op_t *join = NULL;
                    for (uint32_t i = 0;
                        i < f.plan->strata[0].relations[0].op_count; ++i) {
                        wl_plan_op_t *op = &((wl_plan_op_t *)
                            f.plan->strata[0].relations[0].ops)[i];
                        if (op->op == WL_PLAN_OP_JOIN) join = op;
                    }
                    CHECK(join && !join->right_filter_expr.data
                        && !join->right_filter_expr.size);
                    join->right_filter_expr.data = malloc(sizeof(predicate));
                    CHECK(join->right_filter_expr.data);
                    memcpy(join->right_filter_expr.data, predicate,
                        sizeof(predicate));
                    join->right_filter_expr.size = sizeof(predicate);
                }
                CHECK(!wl_evaluation_control_create(0, &f.control));
                wl_columnar_memory_resolution_t resolution = {
                    .budget_bytes = 1u << 24, .usable_bytes = 1u << 24,
                        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
                };
                f.governor =
                    wl_columnar_memory_governor_ref_create(&resolution);
                CHECK(f.governor);
                wl_session_options_t options; wl_session_options_init(&options);
                options.evaluation_control = f.control;
                options.memory_governor = f.governor;
                CHECK(!wl_session_create_with_options(wl_backend_columnar(),
                    f.plan, 1, &options, &f.session));
                CHECK(!wl_session_create(wl_backend_columnar(), f.plan, 1,
                    &f.raw));
                wl_session_t *sessions[] = {f.session, f.raw};
                int64_t rows[][2] = {{7, 19}, {7, 23}};
                for (unsigned i = 0; i < 2; ++i) {
                    ((wl_col_session_t *)sessions[i])->join_batch_bytes =
                        batch ? 64 : 0;
                    for (unsigned side = 0; side < 2; ++side) {
                        char name[] = {(char)('a' + side), 0};
                        if (!(empty & (1u << side))) CHECK(!wl_session_insert(
                                    sessions[i], name, rows[side], 1, 2));
                        else {
                            const col_rel_t *rel = session_find_rel(
                                (wl_col_session_t *)sessions[i], name);
                            CHECK(rel && !rel->schema_ok && !rel->ncols
                                && !rel->nrows && rel->storage_owner == rel);
                        }
                    }
                    int result = finish(sessions[i]);
                    if (result) fprintf(stderr,
                            "bare session side=%u batch=%u i=%u result=%d\n",
                            empty, batch, i, result);
                    CHECK(!result);
                }
                const col_rel_t *out = session_find_rel(
                    (wl_col_session_t *)f.session, "output");
                const col_rel_t *raw = session_find_rel(
                    (wl_col_session_t *)f.raw, "output");
                CHECK(out && raw && !out->nrows && !raw->nrows
                    && out->ncols == 2 && raw->ncols == 2 && !out->timestamps);
                for (unsigned c = 0; c < 2; ++c) CHECK(out->column_types[c]
                        == raw->column_types[c]);
                /* Terminal ACK releases the original descriptors: first insertion
                 * into the same never-inserted relation and a dirty successor work. */
                for (unsigned i = 0; i < 2; ++i) {
                    for (unsigned side = 0; side < 2; ++side) {
                        char name[] = {(char)('a' + side), 0};
                        if (empty & (1u << side)) CHECK(!wl_session_insert(
                                    sessions[i], name, rows[side], 1, 2));
                    }
                    /* Dirty batch admission remains a separate policy unit.
                     * This witness checks descriptor release with its admitted
                     * ordinary successor after the configured fresh empty run. */
                    ((wl_col_session_t *)sessions[i])->join_batch_bytes = 0;
                    CHECK(!finish(sessions[i]));
                    const col_rel_t *head = session_find_rel(
                        (wl_col_session_t *)sessions[i], "output");
                    CHECK(head && head->nrows == 1 && head->ncols == 2
                        && head->columns[0][0] == 19 &&
                        head->columns[1][0] == 23);
                }
                destroy(&f);
            }
}

/* This first destination preserves the existing unobserved STEP admission.
* Observed/SNAPSHOT and strict/capped streaming are separate host units. */
static void
domains(void)
{
    never_inserted_domains();
    for (unsigned kind = 0; kind < 12; ++kind)
        for (unsigned ts = 0; ts < 2; ++ts) {
            if (kind == 10) continue; /* Separate ERANGE qualification. */
            fixture_t f; init(&f, kind, ts, 2); observe(&f);
            int rc = finish(f.session);
            if (rc) fprintf(stderr,
                    "kind=%u ts=%u rc=%d exchange=%u consumer=%u restored=%u sink=%u ack=%u\n",
                    kind, ts, rc, hits[11], hits[12], hits[13], hits[3],
                    hits[5]);
            CHECK(!rc);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED] == 1
                && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCER_DONE] == 1);
            unsigned pairs = kind == 6 ? 8 : kind == 3 || kind == 4 ||
                kind == 5 || kind == 9 ? 0 : 64;
            unsigned output = kind == 6 ? 5 : kind < 2 || kind == 7 ||
                kind == 8 || kind == 11 ? 28 : 0;
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED] ==
                (pairs ? pairs / 2 : 1)
                && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_RESTORED] ==
                (pairs ? pairs / 2 : 1)
                && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER] > 0);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK] == output
                && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK] == pairs / 2);
            verify(&f, f.session);
            CHECK(!finish(f.raw)); verify(&f, f.raw);
            CHECK(!finish(f.session)); verify(&f, f.session);
            destroy(&f);
        }
    for (unsigned ts = 0; ts < 2; ++ts)
        for (unsigned rows = 0; rows < 2; ++rows) {
            fixture_t f; init(&f, 0, ts, rows); observe(&f);
            CHECK(!finish(f.session)); verify(&f, f.session);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED] == rows);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK] == rows * 64);
            destroy(&f);
        }
}

static void
recovery(void)
{
    static const wl_columnar_filter_stage_t stages[] = {
        WL_COLUMNAR_FILTER_STAGE_BATCH, WL_COLUMNAR_FILTER_STAGE_SOURCE,
        WL_COLUMNAR_FILTER_STAGE_DECIDE, WL_COLUMNAR_FILTER_STAGE_CONTROL,
        WL_COLUMNAR_FILTER_STAGE_FINISH, WL_COLUMNAR_FILTER_STAGE_RESET,
        WL_COLUMNAR_FILTER_STAGE_DONE
    };
    unsigned schema_begin = 14 + 2 * (sizeof(stages) / sizeof(*stages));
    for (unsigned point = 0; point < schema_begin + 4; ++point)
        for (unsigned work = 0; work < 2; ++work) {
            fixture_t f; init(&f, point >= schema_begin ? 5 : 0, true, 2);
            observe(&f);
            target = point <
                14 ? point + 1 : WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER;
            stop_consumer_stage = point < 14 ||
                point >= schema_begin ? 0 : stages[(point - 14) / 2];
            stop_consumer_ordinal = point < 14 ? 0 : 1 + (point - 14) % 2;
            if (point >= schema_begin) target = 11 + point - schema_begin;
            stopped = false; work_stop = work;
            if (work) CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_C(1000000000)));
            int rc = finish(f.session);
            CHECK(stopped && rc == (work ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED));
            col_rel_t *head = session_find_rel((wl_col_session_t *)f.session,
                    "output");
            CHECK(!head || !head->nrows);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && report.result == rc && report.attempt_id == attempt);
            target = 0;
            stop_consumer_stage = 0;
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!wl_evaluation_control_configure(f.control, 0));
            observe(&f);
            CHECK(!finish(f.session)); verify(&f, f.session);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK] ==
                (f.kind == 5 ? 0 : 32));
            destroy(&f);
        }
}

static void
failures(void)
{
    for (unsigned fail = 0; fail < 10; ++fail) {
        fixture_t f; init(&f, 0, true, 2); observe(&f);
        if (fail < 2 ||
            fail == 9) wl_columnar_join_test_attempt_fail_allocation = fail ==
                9 ? 2 : (int)fail;
        else wl_columnar_join_batch_test_prepare_fail_allocation = fail - 1;
        int rc = finish(f.session);
        if (rc != ENOMEM) fprintf(stderr, "failure=%u rc=%d bootstrap=%u\n",
                fail, rc, hits[9]);
        CHECK(rc == ENOMEM && !f.events);
        wl_columnar_join_test_attempt_fail_allocation = -1;
        wl_columnar_join_batch_test_prepare_fail_allocation = 0;
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!wl_evaluation_control_configure(f.control, 0));
        observe(&f);
        CHECK(!finish(f.session)); verify(&f, f.session);
        CHECK(!f.events);
        destroy(&f);
    }
    for (unsigned fault = 0; fault < 8; ++fault) {
        fixture_t f; init(&f, 0, true, 2); observe(&f);
        wl_columnar_memory_reservation_init(&peer);
        sink_allocation_fault = fault == 0;
        denial_event = fault == 1 ? WL_COLUMNAR_JOIN_PIPELINE_TEST_BOOTSTRAP
            : fault == 2 ? WL_COLUMNAR_JOIN_PIPELINE_TEST_KEYS
            : fault == 3 ? WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK : 0;
        static const unsigned phases[] = {1, 7, 11, 12};
        denial_phase = fault >= 4 ? phases[fault - 4] : 0;
        wl_columnar_join_batch_test_prepare_checkpoint = denial_phase
            ? prepare_checkpoint : NULL;
        int rc = finish(f.session);
        CHECK(rc == (sink_allocation_fault ? ENOMEM : ENOSPC));
        col_rel_t *head = session_find_rel((wl_col_session_t *)f.session,
                "output");
        CHECK(!head || !head->nrows);
        if (!sink_allocation_fault) {
            CHECK(peer.governor && peer.bytes
                && ((wl_col_session_t *)f.session)->memory_budget_denied);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(f.governor)) >=
                peer.bytes);
            CHECK(wl_columnar_memory_rollback(&peer));
        }
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        wl_columnar_join_batch_test_prepare_checkpoint = NULL;
        sink_allocation_fault = false; denial_event = denial_phase = 0;
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!wl_evaluation_control_configure(f.control, 0));
        observe(&f);
        CHECK(!finish(f.session)); verify(&f, f.session);
        destroy(&f);
    }
    fixture_t expression; init(&expression, 10, true, 2); observe(&expression);
    CHECK(finish(expression.session) == ERANGE);
    CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED] > 1
        && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER] > 0
        && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK] > 0);
    CHECK(finish(expression.raw) == ERANGE);
    col_rel_t *head = session_find_rel((wl_col_session_t *)expression.session,
            "output");
    CHECK(!head || !head->nrows);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(expression.control, &report)
        && report.execution_status == ERANGE && report.result == ERANGE);
    destroy(&expression);
}

/* The actual composed consumer pays parent384+metadata256+child384.
 * Each framed/facade layer adds its existing384. */
static void
funding_composed(void)
{
    uint64_t measured = 0, setup_measured = 0;
    for (unsigned trial = 0; trial < 3; ++trial) {
        fixture_t f; init_with_filter(&f, 0, true, 2, trial == 2 ? 1000 : 0);
        wl_col_session_t *session = (wl_col_session_t *)f.session;
        CHECK(!wl_evaluation_control_begin(f.control,
            f.session->operation_admission));
        uint64_t original = wl_evaluation_control_active_attempt_id(f.control);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(!wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
            &frame, f.control, original));
        wl_columnar_eval_plan_numeric_attempt_t record;
        wl_columnar_eval_plan_numeric_attempt_init(&record);
        wl_evaluation_control_t *grants[3] = {0}; int owners[3];
        for (unsigned i = 0; i < 3; ++i) {
            CHECK(!wl_evaluation_control_create(0, &grants[i]));
            CHECK(!wl_evaluation_control_attach(grants[i], &owners[i]));
        }
        unsigned turn = 0, refusals = 0, paid = 0, setup_turns = 0;
        int protocol = 0;
        bool started = false, acked = false, short_next = true;
        bool setup_started = false, setup_counted = false;
        uint64_t setup_original_start = 0;
        while (!acked) {
            CHECK(turn < 100000);
            unsigned slot = turn++ % 3;
            wl_evaluation_control_t *grant = grants[slot]; uint64_t work = 0;
            bool consumer = started &&
                record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
                && !wl_columnar_join_pipeline_numeric_funding_work(&record,
                    &work) && work == 384;
            bool setup = started && record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
                && (!record.join_host || record.join_host_initialized_bytes
                <= wl_columnar_join_pipeline_numeric_host_size());
            bool underfund = trial && consumer && short_next;
            if (setup) ++setup_turns;
            if (setup && !setup_started) {
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    original, &setup_original_start));
                setup_started = true;
            }
            CHECK(!wl_evaluation_control_reset(grant));
            CHECK(!wl_evaluation_control_configure(grant,
                underfund ? 1023 : consumer ? 1024 : setup
                || (started && record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH)
                ? WL_COLUMNAR_JOIN_PIPELINE_SETUP_MAX_WORK : 384));
            CHECK(!wl_evaluation_control_begin(grant, &owners[slot]));
            uint64_t id = wl_evaluation_control_active_attempt_id(grant);
            int rc;
            if (!started) {
                rc =
                    wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback
                    (
                    &record, &f.plan->strata[0].relations[0], 0, 5, frame,
                    session,
                    0, f.control, original, grant, id);
                CHECK(!rc); started = true;
            } else if (record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING) {
                rc =
                    wl_columnar_eval_plan_numeric_composed_consume_result(
                    &record,
                    protocol, grant, id);
                CHECK(!rc);
            } else if (record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE) {
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(frame);
                CHECK(stack && stack->top == 1 && stack->items[0].owned);
                col_rel_t *out = stack->items[0].rel;
                CHECK(out && out->nrows == 28 && out->ncols == 4);
                for (unsigned r = 0; r < out->nrows; ++r) {
                    int64_t row[4];
                    for (unsigned c = 0; c < 4;
                        ++c) row[c] = out->columns[c][r];
                    capture("output", row, 4, 1, &f);
                }
                CHECK(f.events == 28);
                rc = wl_columnar_eval_plan_numeric_composed_attempt_ack(&record,
                        grant, id);
                CHECK(!rc); acked = true;
            } else {
                rc =
                    wl_columnar_eval_plan_numeric_composed_attempt_step(&record,
                        grant, id);
                if (underfund) {
                    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
                        && record.phase ==
                        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH);
                    CHECK(wl_evaluation_control_active_attempt_id(f.control) ==
                        original);
                    ++refusals; short_next = false;
                } else {
                    protocol = rc;
                    CHECK(!rc ||
                        record.phase ==
                        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
                    if (consumer) {
                        ++paid; short_next = true;
                    }
                }
            }
            if (setup && record.join_host_initialized_bytes
                > wl_columnar_join_pipeline_numeric_host_size()
                && !setup_counted) {
                uint64_t work;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    original, &work));
                work -= setup_original_start;
                if (!trial) setup_measured = work;
                else CHECK(work == setup_measured + (trial == 2 ? 2000 : 0));
                setup_counted = true;
            }
            (void)wl_evaluation_control_finish(grant, &owners[slot], 0, 0);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(grant, &report));
            if (underfund) CHECK(report.consumed == 640);
            else if (consumer) CHECK(report.consumed == 1024);
        }
        CHECK(setup_counted && paid > 64 && refusals == (trial ? paid : 0));
        if (trial == 2) CHECK(setup_turns > 2000);
        CHECK(!wl_columnar_eval_stack_cleanup_finish(&frame));
        CHECK(!wl_evaluation_control_finish(f.control,
            f.session->operation_admission, 0, 0));
        wl_evaluation_control_report_t original_report;
        CHECK(!wl_evaluation_control_report(f.control, &original_report));
        if (!trial) measured = original_report.consumed;
        else if (trial == 1) CHECK(original_report.consumed == measured);
        else CHECK(original_report.consumed > measured);
        if (trial == 2) {
            CHECK(!finish(f.raw));
            verify(&f, f.raw);
        }
        for (unsigned i = 0; i < 3; ++i) {
            CHECK(!wl_evaluation_control_detach(grants[i], &owners[i]));
            wl_evaluation_control_release(grants[i]);
        }
        destroy(&f);
    }
}

typedef struct {
    fixture_t fixture;
    wl_columnar_eval_plan_numeric_attempt_t record;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_evaluation_control_t *grant;
    uint64_t grant_id, consumed, original;
    int grant_owner, protocol;
} setup_fixture_t;

static void
setup_grant_begin(setup_fixture_t *s, uint64_t limit)
{
    CHECK(!wl_evaluation_control_reset(s->grant));
    CHECK(!wl_evaluation_control_configure(s->grant, limit));
    CHECK(!wl_evaluation_control_begin(s->grant, &s->grant_owner));
    s->grant_id = wl_evaluation_control_active_attempt_id(s->grant);
}

static void
setup_grant_end(setup_fixture_t *s)
{
    (void)wl_evaluation_control_finish(s->grant, &s->grant_owner, 0, 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(s->grant, &report));
    s->consumed = report.consumed;
}

static int
setup_turn(setup_fixture_t *s, uint64_t limit)
{
    setup_grant_begin(s, limit);
    bool pending = s->record.phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
    int rc = pending
        ? wl_columnar_eval_plan_numeric_composed_consume_result(&s->record,
            s->protocol, s->grant, s->grant_id)
        : wl_columnar_eval_plan_numeric_composed_attempt_step(&s->record,
            s->grant, s->grant_id);
    if (!pending) s->protocol = rc;
    setup_grant_end(s);
    return rc;
}

static void
setup_consume(setup_fixture_t *s)
{
    CHECK(s->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
    CHECK(!setup_turn(s, 384) && s->consumed == 384);
    CHECK(s->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH);
}

static uint64_t
setup_original_work(setup_fixture_t *s)
{
    uint64_t work;
    CHECK(!wl_evaluation_control_test_active_consumed(s->fixture.control,
        s->original, &work));
    return work;
}

static void
setup_begin_limit(setup_fixture_t *s, uint64_t limit)
{
    memset(s, 0, sizeof(*s));
    init(&s->fixture, 0, true, 2);
    CHECK(!wl_evaluation_control_configure(s->fixture.control, limit));
    CHECK(!wl_evaluation_control_begin(s->fixture.control,
        s->fixture.session->operation_admission));
    s->original = wl_evaluation_control_active_attempt_id(s->fixture.control);
    wl_col_session_t *session = (wl_col_session_t *)s->fixture.session;
    CHECK(!wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
        &s->frame, s->fixture.control, s->original));
    wl_columnar_eval_plan_numeric_attempt_init(&s->record);
    CHECK(!wl_evaluation_control_create(0, &s->grant));
    CHECK(!wl_evaluation_control_attach(s->grant, &s->grant_owner));
    setup_grant_begin(s, 0);
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback(
            &s->record, &s->fixture.plan->strata[0].relations[0], 0, 5,
            s->frame, session, 0, s->fixture.control, s->original,
            s->grant, s->grant_id));
    setup_grant_end(s);
    for (unsigned turns = 0; s->record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH; ++turns) {
        CHECK(turns < 1000 && !setup_turn(s, 0));
    }
    CHECK(!s->record.join_host && !s->record.private_bindings
        && s->record.controlled_materialized_batch_fallback);
}

static void
setup_begin(setup_fixture_t *s)
{
    setup_begin_limit(s, 0);
}

static void
setup_close_result(setup_fixture_t *s, int result)
{
    for (unsigned turns = 0; s->record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE; ++turns) {
        CHECK(turns < 100000 && !setup_turn(s, 0));
    }
    CHECK(s->record.semantic_result == result && !s->record.join_host
        && !s->record.join_host_governor && !s->record.join_host_admitted);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(s->frame);
    CHECK(stack);
    if (!result) {
        CHECK(stack->top == 1 && stack->items[0].owned);
        col_rel_t *out = stack->items[0].rel;
        CHECK(out && out->nrows == 28 && out->ncols == 4);
        for (unsigned r = 0; r < out->nrows; ++r) {
            int64_t row[4];
            for (unsigned c = 0; c < 4; ++c) row[c] = out->columns[c][r];
            capture("output", row, 4, 1, &s->fixture);
        }
        CHECK(s->fixture.events == 28);
    }
    setup_grant_begin(s, 384);
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_ack(&s->record,
        s->grant, s->grant_id));
    setup_grant_end(s);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&s->frame));
    int finish_result = wl_evaluation_control_active_attempt_id(
        s->fixture.control) == s->original ? result : 0;
    CHECK(wl_evaluation_control_finish(s->fixture.control,
        s->fixture.session->operation_admission, finish_result, 0)
        == finish_result);
    CHECK(!wl_evaluation_control_detach(s->grant, &s->grant_owner));
    wl_evaluation_control_release(s->grant);
    destroy(&s->fixture);
}

static void
funding_setup(void)
{
    static const uint64_t limits[] = {383, 575, 703, 959, 1087, 1279, 1280};
    static const uint64_t paid[] = {0, 384, 576, 704, 960, 1088, 1280};
    for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
        setup_fixture_t s; setup_begin(&s);
        wl_columnar_memory_governor_t *gov =
            wl_columnar_memory_governor_ref_get(s.fixture.governor);
        uint64_t reserved = wl_columnar_memory_reserved(gov);
        uint64_t original = setup_original_work(&s);
        int rc = setup_turn(&s, limits[i]);
        CHECK(rc == (i == 6 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT)
            && s.consumed == paid[i] && !s.record.semantic_result);
        CHECK((s.record.join_host_governor != NULL) == (i >= 3)
            && s.record.join_host_admitted == (i >= 4)
            && (s.record.join_host != NULL) == (i >= 5)
            && s.record.join_host_committed == (i == 6));
        size_t bytes = wl_columnar_join_pipeline_numeric_host_size();
        CHECK(wl_columnar_memory_reserved(gov) == reserved
            + (i >= 4 ? bytes : 0));
        if (i < 2) CHECK(setup_original_work(&s) == original);
        void *allocated = s.record.join_host;
        if (s.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
            setup_consume(&s);
        if (i != 6) {
            CHECK(!setup_turn(&s, WL_COLUMNAR_JOIN_PIPELINE_SETUP_MAX_WORK));
            setup_consume(&s);
        }
        CHECK(s.record.join_host && s.record.join_host_committed
            && !s.record.join_host_initialized_bytes);
        if (allocated) CHECK(s.record.join_host == allocated);
        CHECK(wl_columnar_memory_reserved(gov) == reserved + bytes);
        unsigned phases = 0, classified = 0, headers = 0;
        while (s.record.join_host_initialized_bytes <= bytes) {
            CHECK(++phases < 100);
            uint64_t before = s.record.join_host_initialized_bytes;
            bool zero = before < bytes;
            original = setup_original_work(&s);
            CHECK(setup_turn(&s, zero ? 511 : 575)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(s.consumed == 384 || (!zero && s.consumed == 512));
            CHECK(s.record.join_host_initialized_bytes == before
                && setup_original_work(&s) == original
                && !s.record.semantic_result);
            setup_consume(&s);
            CHECK(!setup_turn(&s, zero ? 512 : 576)
                && s.consumed == (zero ? 512 : 576));
            uint64_t delta = setup_original_work(&s) - original;
            if (zero) {
                uint64_t slice = bytes - before;
                if (slice > 256) slice = 256;
                CHECK(delta == slice
                    && s.record.join_host_initialized_bytes == before + slice);
            } else {
                CHECK(delta == 16 || delta <= 1);
                if (delta == 16) ++headers;
                if (delta == 1) ++classified;
            }
            setup_consume(&s);
        }
        CHECK(headers == 1 && classified >= 3 &&
            s.record.join_host_initialized_bytes
            == bytes + 1u);
        setup_close_result(&s, 0);
    }
}

static setup_fixture_t *setup_turnover_fixture;
static unsigned setup_turnover_phase, setup_turnover_hits;
static bool setup_turnover_funding;
static uint64_t setup_turnover_successor, setup_turnover_paid;

static void
setup_governor_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    setup_fixture_t *s = setup_turnover_fixture;
    if (!s || token != &s->record.join_host_credit
        || phase != setup_turnover_phase || setup_turnover_hits) return;
    CHECK(control == s->fixture.control && s->record.join_host_busy);
    ++setup_turnover_hits;
    /* Both checked module entries reject reentry before partial-host access. */
    CHECK(wl_columnar_join_pipeline_numeric_setup_step(&s->record,
        s->grant, s->grant_id) == EBUSY);
    CHECK(wl_columnar_join_pipeline_numeric_step(&s->record,
        s->grant, s->grant_id) == EBUSY);
    CHECK(wl_columnar_join_pipeline_numeric_step_funded(&s->record,
        s->grant, s->grant_id) == EBUSY);
    wl_evaluation_control_t *changed = setup_turnover_funding
        ? s->grant : control;
    const void *owner = setup_turnover_funding ? (const void *)&s->grant_owner
        : s->fixture.session->operation_admission;
    CHECK(!wl_evaluation_control_finish(changed, owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(changed, &report));
    setup_turnover_paid = report.consumed;
    CHECK(!wl_evaluation_control_reset(changed)
        && !wl_evaluation_control_begin(changed, owner));
    setup_turnover_successor = wl_evaluation_control_active_attempt_id(changed);
}

static void
funding_setup_turnover(void)
{
    static const unsigned seams[] = {WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
                                     WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
                                     WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF};
    for (unsigned funding = 0; funding < 2; ++funding)
        for (unsigned seam = 0; seam < sizeof(seams) / sizeof(seams[0]);
            ++seam) {
            setup_fixture_t s; setup_begin(&s);
            setup_turnover_fixture = &s;
            setup_turnover_phase = seams[seam];
            setup_turnover_hits = 0; setup_turnover_funding = funding != 0;
            wl_columnar_memory_governor_test_checkpoint =
                setup_governor_checkpoint;
            int rc = setup_turn(&s, WL_COLUMNAR_JOIN_PIPELINE_SETUP_MAX_WORK);
            wl_columnar_memory_governor_test_checkpoint = NULL;
            setup_turnover_fixture = NULL;
            CHECK(setup_turnover_hits == 1 && !s.record.join_host_busy);
            /* setup_turn finished the replacement funding grant itself. */
            if (funding) {
                wl_evaluation_control_report_t successor;
                CHECK(!wl_evaluation_control_report(s.grant, &successor)
                    && successor.attempt_id == setup_turnover_successor
                    && !successor.consumed && !successor.result);
                CHECK(rc == EINVAL && !s.record.join_host_terminal
                    && !s.record.semantic_result
                    && s.record.join_host_admitted
                    && s.record.join_host_committed == (seam != 0)
                    && setup_turnover_paid == (seam ? 1280 : 960));
                setup_consume(&s);
                setup_close_result(&s, 0);
            } else {
                uint64_t successor_work;
                CHECK(!wl_evaluation_control_test_active_consumed(
                        s.fixture.control, setup_turnover_successor,
                        &successor_work));
                CHECK(!rc && !successor_work && s.record.join_host_terminal
                    && s.record.child_status.semantic_result == EINVAL
                    && !s.record.join_host_committed);
                setup_consume(&s);
                setup_close_result(&s, EINVAL);
            }
        }
}

static void
metadata_ready(setup_fixture_t *s)
{
    size_t bytes = wl_columnar_join_pipeline_numeric_host_size();
    for (unsigned turns = 0; !s->record.join_host
        || s->record.join_host_initialized_bytes <= bytes; ++turns) {
        CHECK(turns < 1000 && !setup_turn(s, 0));
    }
    setup_consume(s);
}

static void
metadata_refusal(setup_fixture_t *s, uint64_t limit, uint64_t paid)
{
    wl_columnar_eval_plan_numeric_attempt_t record = s->record;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(s->frame);
    eval_stack_t before = {0};
    if (stack) before = *stack;
    size_t bytes = wl_columnar_join_pipeline_numeric_host_size();
    unsigned char *host = malloc(bytes);
    CHECK(host);
    /* Byte snapshots are only compared, never operated as ownership tokens. */
    memcpy(host, s->record.join_host, bytes);
    uint64_t original = setup_original_work(s);
    CHECK(setup_turn(s, limit) == WL_EVALUATION_CONTROL_WORK_LIMIT
        && s->consumed == paid
        && !memcmp(&record, &s->record, sizeof(record))
        && wl_columnar_eval_stack_cleanup_stack(s->frame) == stack
        && (!stack || !memcmp(&before, stack, sizeof(before)))
        && !memcmp(host, s->record.join_host, bytes)
        && setup_original_work(s) == original);
    free(host);
}

static setup_fixture_t *metadata_fixture;
static unsigned metadata_starts, metadata_steps, metadata_acks;
static bool metadata_cancel;

static void
metadata_checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t producer_work)
{
    (void)generation; (void)extent; (void)producer_work;
    setup_fixture_t *s = metadata_fixture;
    CHECK(s && control == s->fixture.control);
    if (event != WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER) return;
    wl_columnar_filter_context_t *context = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(s->frame, &context,
        &s->record));
    if (context->stage == WL_COLUMNAR_FILTER_STAGE_INERT) ++metadata_starts;
    else if (context->stage == WL_COLUMNAR_FILTER_STAGE_DONE) ++metadata_acks;
    else ++metadata_steps;
    uint64_t original = setup_original_work(s);
    int owner;
    wl_evaluation_control_t *grant = NULL;
    CHECK(!wl_evaluation_control_create(256, &grant)
        && !wl_evaluation_control_attach(grant, &owner)
        && !wl_evaluation_control_begin(grant, &owner));
    uint64_t id = wl_evaluation_control_active_attempt_id(grant);
    wl_col_session_t *session = (wl_col_session_t *)s->fixture.session;
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    wl_columnar_eval_cleanup_receipt_key_t key, sentinel;
    memset(&key, 0xa5, sizeof(key));
    sentinel = key;
    uint64_t next_id = session->cleanup_receipt_next_id;
    uint64_t next_epoch = session->cleanup_receipt_next_plan_epoch;
    uint64_t generation_before = session->cleanup_receipt_session_generation;
    uint64_t reserved = session->cleanup_reserved_bytes;
    uint32_t active_count = session->cleanup_active_count;
    wl_columnar_eval_cleanup_receipt_t receipts[
        sizeof(session->cleanup_receipts) /
        sizeof(session->cleanup_receipts[0])];
    memcpy(receipts, session->cleanup_receipts, sizeof(receipts));
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, &nested) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
        &nested, grant, id) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
        &nested, NULL, 0) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            session, &nested, grant, id, 1, &key) == EBUSY);
    CHECK(!nested && !memcmp(&key, &sentinel, sizeof(key))
        && session->cleanup_active == s->frame
        && session->cleanup_active_count == active_count
        && session->cleanup_reserved_bytes == reserved
        && session->cleanup_receipt_next_id == next_id
        && session->cleanup_receipt_next_plan_epoch == next_epoch
        && session->cleanup_receipt_session_generation == generation_before
        && !memcmp(receipts, session->cleanup_receipts, sizeof(receipts)));
    uint64_t grant_work;
    CHECK(!wl_evaluation_control_test_active_consumed(grant, id, &grant_work)
        && !grant_work);
    CHECK(wl_columnar_eval_stack_cleanup_begin(NULL, &nested) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            session, &nested, grant, id, 0, &key) == EINVAL);
    CHECK(!memcmp(&key, &sentinel, sizeof(key)));
    wl_columnar_eval_stack_cleanup_frame_t *existing = s->frame;
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, NULL) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, &existing) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
        NULL, grant, id) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
        &existing, grant, id) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            session, &existing, grant, id, 1, &key) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            session, &nested, grant, id, 1, NULL) == EINVAL);
    CHECK(existing == s->frame && !nested
        && !memcmp(&key, &sentinel, sizeof(key)));
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&s->record,
        grant, id) == EBUSY);
    CHECK(wl_columnar_join_pipeline_numeric_step(&s->record, grant, id)
        == EBUSY);
    /* The new checked entry accepts JOIN_BATCH, never its pending receipt. */
    CHECK(wl_columnar_join_pipeline_numeric_step_funded(&s->record, grant, id)
        == EINVAL && setup_original_work(s) == original);
    CHECK(!wl_evaluation_control_finish(grant, &owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant, &report)
        && report.consumed == 256);
    CHECK(!wl_evaluation_control_detach(grant, &owner));
    wl_evaluation_control_release(grant);
    if (metadata_cancel) wl_evaluation_control_request_cancel(control);
}

static void
metadata_nested_retry(setup_fixture_t *s)
{
    /* A parked stream permits nesting between consumer dispatches. */
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(
            (wl_col_session_t *)s->fixture.session, &nested));
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    wl_columnar_eval_cleanup_receipt_key_t key;
    wl_col_session_t *session = (wl_col_session_t *)s->fixture.session;
    CHECK(!wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            session, &nested, s->fixture.control, s->original, 1, &key));
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_prepare(nested, &key, 0,
        false));
    wl_columnar_eval_cleanup_outcome_t outcome;
    CHECK(!wl_columnar_eval_stack_cleanup_finish_receipted(&nested, &key,
        &outcome) && !nested
        && outcome == WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_FREED);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_ack(session, &key));
}

static void
funding_metadata(void)
{
    setup_fixture_t s; setup_begin(&s); metadata_ready(&s);
    metadata_refusal(&s, 383, 0);
    metadata_refusal(&s, 639, 384);
    metadata_refusal(&s, 640, 640);
    uint64_t original = setup_original_work(&s);
    CHECK(!setup_turn(&s, 641) && s.consumed == 641
        && setup_original_work(&s) > original);
    setup_consume(&s);
    metadata_fixture = &s;
    metadata_starts = metadata_steps = metadata_acks = 0;
    wl_columnar_join_pipeline_test_numeric_checkpoint = metadata_checkpoint;
    unsigned consumers = 0;
    bool parked_nested = false;
    for (unsigned turns = 0; s.record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE; ++turns) {
        CHECK(turns < 100000);
        uint64_t work = 0;
        bool consumer = s.record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
            && !wl_columnar_join_pipeline_numeric_funding_work(&s.record,
                &work) && work == 384;
        if (!consumer) {
            CHECK(!setup_turn(&s, 0)); continue;
        }
        const col_rel_t *input;
        uint64_t generation;
        bool schema;
        const wl_plan_op_t *op;
        if (!parked_nested && !wl_columnar_join_pipeline_test_stream_view(
                &s.record, &input, &generation, &schema, &op)) {
            metadata_nested_retry(&s);
            parked_nested = true;
        }
        metadata_refusal(&s, 383, 0);
        metadata_refusal(&s, 639, 384);
        /* Exactly640 paid metadata precedes the isolated383/384 child fee. */
        metadata_refusal(&s, 1023, 640);
        CHECK(!setup_turn(&s, 1024) && s.consumed == 1024);
        setup_consume(&s);
        ++consumers;
    }
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    metadata_fixture = NULL;
    CHECK(parked_nested && consumers > 64 && metadata_starts > 1 &&
        metadata_steps
        && metadata_acks == metadata_starts);
    setup_close_result(&s, 0);
}

static void
funding_metadata_cancel(void)
{
    setup_fixture_t s; setup_begin(&s); metadata_ready(&s);
    metadata_fixture = &s;
    metadata_starts = metadata_steps = metadata_acks = 0;
    metadata_cancel = true;
    wl_columnar_join_pipeline_test_numeric_checkpoint = metadata_checkpoint;
    setup_close_result(&s, WL_EVALUATION_CONTROL_CANCELLED);
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    metadata_fixture = NULL;
    metadata_cancel = false;
    CHECK(metadata_starts == 1 && metadata_steps
        && metadata_acks == metadata_starts);
}

static void *
metadata_foreign_thread(void *argument)
{
    setup_fixture_t *s = argument;
    int owner;
    wl_evaluation_control_t *grant = NULL;
    CHECK(!wl_evaluation_control_create(0, &grant)
        && !wl_evaluation_control_attach(grant, &owner)
        && !wl_evaluation_control_begin(grant, &owner));
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&s->record,
        grant, wl_evaluation_control_active_attempt_id(grant)) == EINVAL);
    CHECK(!wl_evaluation_control_finish(grant, &owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant, &report) && !report.consumed);
    CHECK(!wl_evaluation_control_detach(grant, &owner));
    wl_evaluation_control_release(grant);
    return NULL;
}

static void
funding_metadata_authority(void)
{
    setup_fixture_t s; setup_begin(&s); metadata_ready(&s);
    uint64_t original = setup_original_work(&s);
    wl_columnar_eval_plan_numeric_attempt_t record = s.record, copy = s.record;
    setup_grant_begin(&s, 0);
    CHECK(wl_columnar_join_pipeline_numeric_step_funded(&copy, s.grant,
        s.grant_id) == EINVAL);
    CHECK(wl_columnar_join_pipeline_numeric_step_funded(&s.record,
        s.fixture.control, s.original) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&s.record,
        s.fixture.control, s.original) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&s.record,
        s.grant, s.grant_id + 1u) == EINVAL);
    setup_grant_end(&s); CHECK(!s.consumed);
    setup_grant_begin(&s, 0);
    uint64_t stale = s.grant_id;
    CHECK(!wl_evaluation_control_finish(s.grant, &s.grant_owner, 0, 0)
        && !wl_evaluation_control_reset(s.grant)
        && !wl_evaluation_control_begin(s.grant, &s.grant_owner));
    CHECK(wl_columnar_join_pipeline_numeric_step_funded(&s.record, s.grant,
        stale) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&s.record,
        s.grant, stale) == EINVAL);
    setup_grant_end(&s); CHECK(!s.consumed);
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(
            (wl_col_session_t *)s.fixture.session, &nested));
    CHECK(setup_turn(&s, 0) == EBUSY && !s.consumed);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, metadata_foreign_thread, &s)
        && !wl_thread_join(&thread));
    CHECK(!memcmp(&record, &s.record, sizeof(record))
        && setup_original_work(&s) == original);
    setup_close_result(&s, 0);
    for (unsigned stop = 0; stop < 3; ++stop) {
        setup_begin_limit(&s, UINT64_C(1000000000)); metadata_ready(&s);
        int result = stop == 0 ? WL_EVALUATION_CONTROL_CANCELLED
            : stop == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : EINVAL;
        uint64_t successor = 0;
        if (stop == 0) wl_evaluation_control_request_cancel(s.fixture.control);
        else if (stop == 1) CHECK(wl_evaluation_control_charge(
                    s.fixture.control, UINT64_MAX) == result);
        else {
            const void *owner = s.fixture.session->operation_admission;
            CHECK(!wl_evaluation_control_finish(s.fixture.control, owner, 0, 0)
                && !wl_evaluation_control_reset(s.fixture.control)
                && !wl_evaluation_control_begin(s.fixture.control, owner));
            successor =
                wl_evaluation_control_active_attempt_id(s.fixture.control);
        }
        CHECK(!setup_turn(&s, 641) && s.consumed == 641);
        if (successor) {
            uint64_t work;
            CHECK(!wl_evaluation_control_test_active_consumed(s.fixture.control,
                successor, &work) && !work);
        }
        setup_close_result(&s, result);
    }
}

static uint64_t selected_limit;
static uint64_t selected_original, selected_original_work;
static unsigned selected_hits, public_refusals, public_paid;
static bool short_public;

static uint64_t
public_grant_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t proposed)
{
    selected_limit = 0;
    CHECK(active && session == (wl_col_session_t *)active->session);
    if (stopped || action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE
        || !session->cleanup_active) return proposed;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
        (wl_col_session_t *)session, &ctx));
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
        || ctx->child.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH)
        return proposed; /* Includes every real pending receipt consume. */
    uint64_t work = 0;
    CHECK(!wl_columnar_join_pipeline_numeric_funding_work(&ctx->child, &work));
    if (work != 384) return proposed;
    CHECK(wl_columnar_session_controlled_batch_authorized(session,
        active->control, ctx->attempt_id, ctx->operation_identity));
    selected_original = ctx->attempt_id;
    CHECK(!wl_evaluation_control_test_active_consumed(active->control,
        selected_original, &selected_original_work));
    selected_hits = hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER];
    selected_limit = short_public ? 1791 : 1792;
    return selected_limit;
}

static void
public_grant_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *grant, int protocol)
{
    if (!selected_limit) return;
    CHECK(active && session == (wl_col_session_t *)active->session
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_active_attempt_id(grant)
        && !wl_evaluation_control_report(grant, &report)
        && report.limit == selected_limit && !report.execution_status);
    CHECK(wl_evaluation_control_active_attempt_id(active->control) ==
        selected_original);
    if (selected_limit == 1791) {
        CHECK(protocol == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.result == protocol && report.consumed == 1408
            && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER] == selected_hits);
        uint64_t original_work;
        CHECK(!wl_evaluation_control_test_active_consumed(active->control,
            selected_original, &original_work)
            && original_work == selected_original_work);
        ++public_refusals;
    } else {
        CHECK(!protocol && !report.result && report.consumed == 1792
            && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER] ==
            selected_hits + 1);
        ++public_paid;
    }
    short_public = !short_public;
}

/* Real classifier, facade/root dispatch, and exact previous receipt delivery.
 * Finished reports prove full facade1792, including640 of checked metadata
 * before the isolated child383/384 refusal/success;
 * no active consumed sample or standalone root capability is fabricated. */
static void
funding_public(void)
{
    uint64_t measured = 0;
    for (unsigned mode = 0; mode < 3; ++mode) {
        fixture_t f; init(&f, 0, true, 2); observe(&f);
        target = mode == 2 ? WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER : 0;
        stopped = false; work_stop = false;
        public_refusals = public_paid = 0; short_public = true;
        if (mode) {
            wl_columnar_session_test_framed_grant_limit = public_grant_limit;
            wl_columnar_session_test_framed_grant_finished =
                public_grant_finished;
        }
        int rc = finish(f.session);
        wl_columnar_session_test_framed_grant_limit = NULL;
        wl_columnar_session_test_framed_grant_finished = NULL;
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report));
        if (mode == 2) {
            CHECK(stopped && rc == WL_EVALUATION_CONTROL_CANCELLED
                && public_refusals && public_paid);
            col_rel_t *head = session_find_rel((wl_col_session_t *)f.session,
                    "output");
            CHECK(!head || !head->nrows);
            target = 0;
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!wl_evaluation_control_configure(f.control, 0));
            observe(&f);
            CHECK(!finish(f.session));
        } else {
            CHECK(!rc && !report.result);
            if (!mode) measured = report.consumed;
            else CHECK(public_refusals == public_paid && public_paid > 64
                    && report.consumed == measured);
        }
        verify(&f, f.session);
        destroy(&f);
    }
}

static void
funding(void)
{
    funding_setup(); funding_setup_turnover();
    funding_metadata(); funding_metadata_cancel(); funding_metadata_authority();
    funding_composed(); funding_public();
}

int
main(int argc, char **argv)
{
    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "domains"))) domains();
    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "recovery"))) recovery();
    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "failures"))) failures();
    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "funding"))) funding();
    if (argc > 2 || (argc == 2 && strcmp(argv[1], "domains")
        && strcmp(argv[1], "recovery") && strcmp(argv[1],
        "failures") && strcmp(argv[1], "funding"))) return 2;
    return 0;
}
