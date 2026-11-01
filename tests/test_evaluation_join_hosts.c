/* Actual session JOIN producer, private sink and rollback qualification. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
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
init(fixture_t *f, unsigned kind, bool timestamps, unsigned budget_rows)
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

/* Direct composed entry pays one384, direct framed entry pays384+384, and
 * the facade adds another384. This test isolates the first of these levels. */
static void
funding_composed(void)
{
    uint64_t measured = 0;
    for (unsigned trial = 0; trial < 2; ++trial) {
        fixture_t f; init(&f, 0, true, 2);
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
        unsigned turn = 0, refusals = 0, paid = 0;
        int protocol = 0;
        bool started = false, acked = false, short_next = true;
        while (!acked) {
            CHECK(turn < 100000);
            unsigned slot = turn++ % 3;
            wl_evaluation_control_t *grant = grants[slot]; uint64_t work = 0;
            bool consumer = started &&
                record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
                && !wl_columnar_join_pipeline_numeric_funding_work(&record,
                    &work) && work == 384;
            bool underfund = trial && consumer && short_next;
            CHECK(!wl_evaluation_control_reset(grant));
            CHECK(!wl_evaluation_control_configure(grant,
                underfund ? 383 : 384));
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
            (void)wl_evaluation_control_finish(grant, &owners[slot], 0, 0);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(grant, &report));
            if (underfund) CHECK(!report.consumed);
            else if (consumer) CHECK(report.consumed == 384);
        }
        CHECK(paid > 64 && refusals == (trial ? paid : 0));
        CHECK(!wl_columnar_eval_stack_cleanup_finish(&frame));
        CHECK(!wl_evaluation_control_finish(f.control,
            f.session->operation_admission, 0, 0));
        wl_evaluation_control_report_t original_report;
        CHECK(!wl_evaluation_control_report(f.control, &original_report));
        if (!trial) measured = original_report.consumed;
        else CHECK(original_report.consumed == measured);
        for (unsigned i = 0; i < 3; ++i) {
            CHECK(!wl_evaluation_control_detach(grants[i], &owners[i]));
            wl_evaluation_control_release(grants[i]);
        }
        destroy(&f);
    }
}

static uint64_t selected_limit;
static uint64_t selected_original;
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
    selected_hits = hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER];
    selected_limit = short_public ? 1151 : 1152;
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
    if (selected_limit == 1151) {
        CHECK(protocol == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.result == protocol && report.consumed == 768
            && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER] == selected_hits);
        ++public_refusals;
    } else {
        CHECK(!protocol && !report.result && report.consumed == 1152
            && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER] ==
            selected_hits + 1);
        ++public_paid;
    }
    short_public = !short_public;
}

/* Real classifier, facade/root dispatch, and exact previous receipt delivery.
 * Finished reports prove full facade1152 and its nested root's remaining768;
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
