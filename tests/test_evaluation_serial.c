#include "evaluation_guard_fixture.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
/* #1821 real backend fresh-head result bridge and retained ownership. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); abort(); \
                      } } while (0)

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    (void)token; (void)phase;
}

typedef struct {
    wl_evaluation_control_t *control;
    uint64_t id;
    int owner, finish_status;
} grant_t;

typedef struct {
    wl_col_session_t session;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_columnar_eval_serial_framed_attempt_t *ctx;
    wl_evaluation_control_t *operation;
    uint64_t id;
    int owner, protocol;
    grant_t grants[2];
    unsigned next, calls, witnesses;
    col_rel_t *inputs[3];
} fixture_t;

static wl_evaluation_control_t *
control_new(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(limit, &control));
    CHECK(!wl_evaluation_control_attach(control, owner));
    return control;
}

static grant_t *
grant_begin(fixture_t *f, uint64_t limit)
{
    grant_t *grant = &f->grants[f->next];
    f->next ^= 1;
    CHECK(!f->ctx || f->ctx->pending_funding != grant->control);
    CHECK(!wl_evaluation_control_reset(grant->control));
    CHECK(!wl_evaluation_control_configure(grant->control, limit));
    CHECK(!wl_evaluation_control_begin(grant->control, &grant->owner));
    grant->id = wl_evaluation_control_active_attempt_id(grant->control);
    return grant;
}

/* This is the actual action suffix: finish the grant and return to the
 * external driver. The child's protocol is consumed on a later host turn.
 * Both grant objects/owner keys survive the return and completed-report read. */
static int
host_step(fixture_t *f, grant_t *grant, bool consume)
{
    int rc = consume ? wl_columnar_eval_serial_framed_consume_result(f->ctx,
            f->protocol, grant->control, grant->id)
        : wl_columnar_eval_serial_framed_attempt_step(f->ctx,
            grant->control, grant->id);
    grant->finish_status = wl_evaluation_control_finish(grant->control,
            &grant->owner, 0, 0);
    return rc;
}

static void
turn(fixture_t *f)
{
    bool consume = f->ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING;
    grant_t *grant = grant_begin(f, 0);
    ++f->calls;
    CHECK(f->calls == f->witnesses + 1);
    int rc = host_step(f, grant, consume);
    CHECK(!grant->finish_status);
    if (!consume) f->protocol = rc;
    else CHECK(!rc);
    ++f->witnesses; /* External observer runs after the actual host return. */
    CHECK(f->calls == f->witnesses);
}

static void
until(fixture_t *f, wl_columnar_eval_serial_framed_phase_t phase)
{
    unsigned turns = 0;
    while (f->ctx->phase != phase) {
        CHECK(f->ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE);
        CHECK(++turns < 200000);
        turn(f);
    }
}

static void
run(fixture_t *f)
{
    unsigned turns = 0;
    while (f->ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY &&
        f->ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE) {
        CHECK(++turns < 200000);
        turn(f);
    }
}

static col_rel_t *
source_new(const char *name, uint32_t width, uint32_t rows, bool floating,
    int offset)
{
    col_rel_t *rel = NULL;
    CHECK(!col_rel_alloc(&rel, name));
    char **names = calloc(width ? width : 1, sizeof(*names));
    wirelog_column_type_t *types = calloc(width ? width : 1, sizeof(*types));
    int64_t *row = calloc(width ? width : 1, sizeof(*row));
    CHECK(names && types && row);
    for (uint32_t c = 0; c < width; ++c) {
        names[c] = malloc(32); CHECK(names[c]);
        snprintf(names[c], 32, "col%u", c);
        types[c] = floating ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
    }
    CHECK(!col_rel_set_schema(rel, width, (const char *const *)names));
    if (width) CHECK(!col_rel_set_column_types(rel, types, width));
    for (uint32_t r = 0; r < rows; ++r) {
        int value = (int)rows - 1 - (int)r + offset;
        for (uint32_t c = 0; c < width; ++c)
            row[c] = floating ? wl_columnar_float_to_bits(value + c + 0.5)
                : value + c;
        CHECK(!col_rel_append_row(rel, row));
    }
    for (uint32_t c = 0; c < width; ++c) free(names[c]);
    free(names); free(types); free(row);
    return rel;
}

static void
fixture_init(fixture_t *f, uint32_t width, uint32_t rows, bool floating,
    bool governed, uint64_t limit)
{
    memset(f, 0, sizeof(*f));
    wl_mem_ledger_init(&f->session.mem_ledger, 0);
    f->operation = control_new(limit, &f->owner);
    CHECK(!wl_evaluation_control_begin(f->operation, &f->owner));
    f->id = wl_evaluation_control_active_attempt_id(f->operation);
    for (unsigned i = 0; i < 2; ++i)
        f->grants[i].control = control_new(0, &f->grants[i].owner);
    f->inputs[0] = source_new("a", width, rows, floating, 0);
    f->inputs[1] = source_new("b", width, rows, floating, 2);
    f->inputs[2] = source_new("c", width, rows, floating, -1);
    f->session.rels = f->inputs; f->session.nrels = 3;
    f->session.rel_cap = 3;
    if (governed) {
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = 1u << 24, .usable_bytes = 1u << 24,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
                .status = WL_COLUMNAR_MEMORY_OK
        };
        f->session.memory_governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(f->session.memory_governor);
    }
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f->session, &f->frame));
    CHECK(!wl_columnar_eval_stack_framed_context(f->frame, &f->session,
        &f->ctx));
}

static int
host_begin(fixture_t *f, const wl_plan_relation_t *plan, grant_t *grant)
{
    int rc = wl_columnar_eval_serial_framed_attempt_begin(f->frame,
            &f->session, plan, f->operation, f->id, grant->control, grant->id);
    grant->finish_status = wl_evaluation_control_finish(grant->control,
            &grant->owner, 0, 0);
    return rc;
}

static void
begin(fixture_t *f, const wl_plan_relation_t *plan)
{
    grant_t *grant = grant_begin(f, 0);
    CHECK(!host_begin(f, plan, grant));
    CHECK(!grant->finish_status && f->ctx->identity == f->ctx);
}

static int
host_discard(fixture_t *f, grant_t *grant)
{
    int rc = wl_columnar_eval_serial_framed_attempt_discard(f->ctx,
            grant->control, grant->id);
    grant->finish_status = wl_evaluation_control_finish(grant->control,
            &grant->owner, 0, 0);
    return rc;
}

static void
discard(fixture_t *f)
{
    grant_t *grant = grant_begin(f, 0);
    CHECK(!host_discard(f, grant)); CHECK(!grant->finish_status);
}

static int
host_close(fixture_t *f, grant_t *grant, int *result, bool *committed)
{
    int rc = wl_columnar_eval_serial_framed_attempt_close(f->ctx, result,
            committed, grant->control, grant->id);
    grant->finish_status = wl_evaluation_control_finish(grant->control,
            &grant->owner, 0, 0);
    return rc;
}

static int
close_frame(fixture_t *f)
{
    CHECK(f->ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY);
    grant_t *grant = grant_begin(f, 0);
    int result = EINVAL; bool committed = true;
    CHECK(!host_close(f, grant, &result, &committed));
    CHECK(!grant->finish_status && !committed);
    f->ctx = NULL; f->frame = NULL;
    CHECK(!f->session.cleanup_active && !f->session.cleanup_active_count
        && !f->session.cleanup_reserved_bytes && !f->session.cleanup_pending);
    return result;
}

static void
fixture_destroy(fixture_t *f)
{
    CHECK(!f->frame && !f->ctx);
    if (wl_evaluation_control_active_attempt_id(f->operation))
        (void)wl_evaluation_control_finish(f->operation, &f->owner, 0, 0);
    CHECK(!wl_evaluation_control_detach(f->operation, &f->owner));
    wl_evaluation_control_release(f->operation);
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_evaluation_control_active_attempt_id(f->grants[i].control));
        CHECK(!wl_evaluation_control_detach(f->grants[i].control,
            &f->grants[i].owner));
        wl_evaluation_control_release(f->grants[i].control);
    }
    for (unsigned i = 0; i < 3; ++i) col_rel_destroy(f->inputs[i]);
    if (f->session.memory_governor) {
        wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(
            f->session.memory_governor);
        CHECK(!atomic_load_explicit(&g->reserved_bytes, memory_order_acquire));
        wl_columnar_memory_governor_ref_release(f->session.memory_governor);
    }
}

static void
compare_raw(fixture_t *f, const wl_plan_relation_t *plan)
{
    wl_col_session_t raw = {0}; wl_mem_ledger_init(&raw.mem_ledger, 0);
    col_rel_t *inputs[3] = {NULL};
    for (unsigned i = 0; i < 3; ++i)
        CHECK(!col_rel_deep_copy(f->inputs[i], &inputs[i], NULL));
    raw.rels = inputs; raw.nrels = 3; raw.rel_cap = 3;
    eval_stack_t stack = {0};
    CHECK(!col_eval_relation_plan(plan, &stack, &raw));
    CHECK(stack.top == 1 && f->ctx->candidate);
    const col_rel_t *a = f->ctx->candidate, *b = stack.items[0].rel;
    CHECK(!strcmp(a->name, plan->name) && a->ncols == b->ncols
        && a->nrows == b->nrows && !a->timestamps && !b->timestamps
        && a->has_graph_column == b->has_graph_column
        && a->graph_col_idx == b->graph_col_idx
        && a->declared_ncols == b->declared_ncols);
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
        CHECK(a->column_types && b->column_types &&
            a->column_types[c] == b->column_types[c]);
        for (uint32_t r = 0; r < a->nrows; ++r)
            CHECK(a->columns[c][r] == b->columns[c][r]);
    }
    CHECK(a->sorted_nrows == a->nrows &&
        a->run_count == (a->nrows != 0));
    if (a->nrows) CHECK(a->run_ends[0] == a->nrows);
    CHECK(!eval_stack_drain(&stack));
    for (uint32_t i = 0; i < raw.arr_count; ++i) {
        CHECK(!raw.arr_entries[i].pin_count);
        free(raw.arr_entries[i].rel_name); free(raw.arr_entries[i].key_cols);
        arr_free_contents(&raw.arr_entries[i].arr);
        col_arr_detach_memory_governor(&raw.arr_entries[i].arr);
    }
    free(raw.arr_entries); session_rel_free_hash(&raw);
    for (unsigned i = 0; i < 3; ++i) col_rel_destroy(inputs[i]);
}

static void
test_shapes(void)
{
    const unsigned widths[] = {0, 1, 17, 129};
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned empty = 0; empty < 2; ++empty)
            for (unsigned floating = 0; floating < 2; ++floating)
                for (unsigned governed = 0; governed < 2; ++governed) {
                    fixture_t f;
                    fixture_init(&f, widths[w], empty ? 0 : 4, floating,
                        governed, 0);
                    wl_plan_op_t ops[] = {
                        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
                         .delta_mode = WL_DELTA_FORCE_FULL},
                        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "b",
                         .delta_mode = WL_DELTA_FORCE_FULL},
                        {.op = WL_PLAN_OP_CONCAT},
                        {.op = WL_PLAN_OP_CONSOLIDATE}
                    };
                    wl_plan_relation_t plan = {.name = "output", .ops = ops,
                                               .op_count = 4};
                    begin(&f, &plan); run(&f);
                    CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY
                        && !f.ctx->primary_result && f.ctx->child_bound
                        && f.ctx->child.phase ==
                        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
                    CHECK(!f.ctx->source_initialized &&
                        !f.ctx->batch_initialized
                        && !f.ctx->input.rel && !f.ctx->scratch.allocated
                        && !f.ctx->scratch.admitted &&
                        !f.ctx->scratch.ref_held);
                    CHECK(f.session.nrels == 3 && f.session.rels == f.inputs);
                    col_rel_t *out = f.ctx->candidate;
                    uint32_t expected_rows = empty ? 0 : widths[w] ? 6 : 1;
                    CHECK(out->nrows == expected_rows);
                    for (uint32_t r = 0; r < expected_rows; ++r)
                        for (uint32_t c = 0; c < widths[w]; ++c)
                            CHECK(out->columns[c][r] == (floating ?
                                wl_columnar_float_to_bits(r + c + 0.5) :
                                (int64_t)r + c));
                    compare_raw(&f, &plan);
                    discard(&f); run(&f); CHECK(!close_frame(&f));
                    fixture_destroy(&f);
                }
}

/* A exercises the explicitly preserved, complete pre-rewrite body. C tests
 * must instead use the actual stratum relations consumed by production. */
static void
test_generated_union(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl a(x:int64)\n.decl b(x:int64)\n.decl c(x:int64)\n"
        ".decl output(x:int64)\noutput(x):-a(x).\n"
        "output(x):-b(x).\noutput(x):-c(x).\n", &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *body = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "output")) {
                CHECK(plan->strata[s].bodies);
                static wl_plan_relation_t preserved;
                preserved = plan->strata[s].relations[r];
                preserved.ops = plan->strata[s].bodies[r].ops;
                preserved.op_count = plan->strata[s].bodies[r].op_count;
                body = &preserved;
            }
    CHECK(body && body->ops[body->op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
    for (unsigned mask = 0; mask < 8; ++mask) {
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        for (unsigned i = 0; i < 3; ++i)
            if (mask & (1u << i)) f.inputs[i]->nrows = 0;
        begin(&f, body); run(&f);
        CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
        compare_raw(&f, body);
        discard(&f); run(&f); CHECK(!close_frame(&f)); fixture_destroy(&f);
    }
    wl_plan_free(plan); wirelog_program_free(program);
}

static void
test_refusal_and_identity(void)
{
    fixture_t f; fixture_init(&f, 1, 3, false, false, 0);
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    grant_t *grant = grant_begin(&f, 383);
    CHECK(host_begin(&f, &plan, grant) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!f.ctx->identity && !f.ctx->pin_owned);
    begin(&f, &plan);
    wl_columnar_eval_serial_framed_attempt_t copied = *f.ctx;
    grant = grant_begin(&f, 0);
    CHECK(wl_columnar_eval_serial_framed_attempt_step(&copied,
        grant->control, grant->id) == EINVAL);
    CHECK(!wl_evaluation_control_finish(grant->control, &grant->owner, 0, 0));
    wl_columnar_eval_plan_numeric_attempt_t foreign;
    wl_columnar_eval_plan_numeric_attempt_init(&foreign);
    CHECK(wl_columnar_eval_stack_numeric_source_claim(f.frame, &f.session,
        &foreign) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    CHECK(wl_columnar_eval_stack_framed_normalize(f.ctx) == EBUSY);
    until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN);
    /* Parent HOST384 succeeds, nested C HOST384 refuses. A genuine child
     * has not been bound; a later matching completed grant retries begin. */
    grant = grant_begin(&f, 384);
    int rc = host_step(&f, grant, false);
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && grant->finish_status == rc
        && f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && !f.ctx->child.identity);
    f.protocol = rc; turn(&f);
    CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN
        && !f.ctx->child_bound && !f.ctx->primary_result);
    run(&f); CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
    discard(&f);
    until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF);
    CHECK(!f.ctx->child_bound && !f.ctx->child.identity);
    CHECK(wl_columnar_eval_stack_numeric_source_claim(f.frame, &f.session,
        &foreign) == EBUSY);
    run(&f); CHECK(!close_frame(&f)); fixture_destroy(&f);
}

static void
test_failures(void)
{
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    for (unsigned fault = 0; fault < 4; ++fault) {
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        begin(&f, &plan);
        until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_NEW);
        if (fault == 0) wl_evaluation_control_request_cancel(f.operation);
        if (fault == 1)
            wl_columnar_relation_test_constructor_fail_allocation = 1;
        if (fault == 2)
            wl_columnar_eval_serial_test_framed_fail_allocation = 0;
        if (fault == 3) {
            wl_columnar_memory_governor_t *g =
                wl_columnar_memory_governor_ref_get(f.session.memory_governor);
            uint64_t available = atomic_load_explicit(&g->usable_bytes,
                    memory_order_acquire) - wl_columnar_memory_reserved(g);
            CHECK(wl_columnar_memory_reserve_checked(g, available, &peer) ==
                WL_COLUMNAR_MEMORY_ADMISSION_OK);
        }
        run(&f);
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        wl_columnar_eval_serial_test_framed_fail_allocation = -1;
        CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE);
        CHECK(f.ctx->primary_result == (fault == 0 ?
            WL_EVALUATION_CONTROL_CANCELLED : fault == 3 ? ENOSPC : ENOMEM));
        CHECK(f.session.memory_budget_denied == (fault == 3));
        CHECK(close_frame(&f) == (fault == 0 ? WL_EVALUATION_CONTROL_CANCELLED
            : fault == 3 ? ENOSPC : ENOMEM));
        if (fault == 3) CHECK(wl_columnar_memory_rollback(&peer));
        fixture_destroy(&f);
    }
}

static void
test_retained_and_consume_refusal(void)
{
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    for (unsigned stage = 0; stage < 2; ++stage) {
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        begin(&f, &plan);
        unsigned turns = 0;
        /* Stop after A has consumed Cstep's direct return, but before the
         * next Cconsume: the original C_PENDING result remains authentic. */
        while (!(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD &&
            f.ctx->child.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
            && (stage == 0 ? f.ctx->child.pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT :
            f.ctx->child.pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD))) {
            CHECK(++turns < 200000); turn(&f);
        }
        int saved = f.ctx->inner_protocol;
        CHECK(!saved);
        grant_t *grant = grant_begin(&f, 384);
        int rc = host_step(&f, grant, false);
        CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
            grant->finish_status == rc && f.ctx->pending_inner_consume);
        f.protocol = rc; turn(&f);
        CHECK(f.ctx->inner_protocol == saved && !f.ctx->primary_result &&
            f.ctx->child.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
        run(&f); CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
        discard(&f); run(&f); CHECK(!close_frame(&f)); fixture_destroy(&f);
    }
    for (unsigned retained = 0; retained < 2; ++retained) {
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        begin(&f, &plan);
        until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN);
        col_rel_t *original = f.ctx->input.rel;
        CHECK(original && f.ctx->candidate && f.ctx->materialized);
        wl_columnar_source_access_reader_t reader = {0};
        if (!retained) CHECK(!col_rel_source_reader_acquire(original, &reader));
        if (!retained) {
            turn(&f);
            CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN
                && f.ctx->cleanup_result == EBUSY && !f.ctx->cursor
                && f.ctx->input.rel == original && f.ctx->candidate);
            CHECK(!col_rel_source_reader_release(&reader));
        }
        run(&f); CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
        if (retained) CHECK(!col_rel_source_reader_acquire(f.ctx->candidate,
                &reader));
        discard(&f);
        if (retained) {
            turn(&f);
            CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CANDIDATE_ABORT
                && f.ctx->cleanup_result == EBUSY && f.ctx->candidate_owned
                && f.ctx->candidate && f.ctx->child_bound);
            CHECK(!col_rel_source_reader_release(&reader));
        }
        run(&f); CHECK(!close_frame(&f)); fixture_destroy(&f);
    }
}

/* A real peer denies the next row growth after initial buffers are owned. */
static void
test_growth_quota_and_retry(void)
{
    fixture_t f;
    fixture_init(&f, 1, COL_REL_INIT_CAP + 1, false, true, 0);
    f.inputs[0]->has_graph_column = true;
    f.inputs[0]->graph_col_idx = 0; f.inputs[0]->declared_ncols = 1;
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    begin(&f, &plan);
    unsigned turns = 0;
    while (!(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATHER
        && f.ctx->candidate && f.ctx->row == f.ctx->candidate->capacity)) {
        CHECK(++turns < 200000); turn(&f);
    }
    CHECK(f.ctx->row == COL_REL_INIT_CAP &&
        f.ctx->candidate->nrows == COL_REL_INIT_CAP);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(f.session.memory_governor);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    uint64_t available = atomic_load_explicit(&g->usable_bytes,
            memory_order_acquire) - wl_columnar_memory_reserved(g);
    CHECK(wl_columnar_memory_reserve_checked(g, available, &peer) ==
        WL_COLUMNAR_MEMORY_ADMISSION_OK);
    until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END);
    CHECK(f.ctx->primary_result == ENOSPC && f.session.memory_budget_denied
        && f.ctx->candidate && f.ctx->candidate_owned
        && f.ctx->candidate->nrows == COL_REL_INIT_CAP
        && f.ctx->candidate->memory_budget_denial_pending);
    run(&f); CHECK(close_frame(&f) == ENOSPC);
    CHECK(f.session.nrels == 3 && f.session.rels == f.inputs);
    CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(!wl_columnar_memory_reserved(g));
    CHECK(wl_evaluation_control_finish(f.operation, &f.owner, ENOSPC,
        0) == ENOSPC);
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.owner));
    f.id = wl_evaluation_control_active_attempt_id(f.operation);
    f.session.memory_budget_denied = false;
    CHECK(f.inputs[0]->nrows == COL_REL_INIT_CAP + 1 &&
        f.inputs[0]->columns[0][0] == COL_REL_INIT_CAP &&
        f.inputs[0]->columns[0][COL_REL_INIT_CAP] == 0);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f.session, &f.frame));
    CHECK(!wl_columnar_eval_stack_framed_context(f.frame, &f.session, &f.ctx));
    begin(&f, &plan); run(&f);
    CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
    col_rel_t *output = f.ctx->candidate;
    CHECK(output->nrows == COL_REL_INIT_CAP + 1 && output->has_graph_column
        && !output->graph_col_idx && output->declared_ncols == 1);
    for (uint32_t r = 0; r < output->nrows; ++r)
        CHECK(output->columns[0][r] == r);
    compare_raw(&f, &plan);
    discard(&f); run(&f); CHECK(!close_frame(&f)); fixture_destroy(&f);
}

static void
test_bootstrap_limits(void)
{
    const uint64_t limits[] = {191, 192, 75263, 75264, 75519, 75520};
    const uint64_t consumed[] = {0, 192, 75072, 75264, 75264, 75520};
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    for (unsigned i = 0; i < 6; ++i) {
        fixture_t f; fixture_init(&f, 1, 2, false, true, limits[i]);
        grant_t *grant = grant_begin(&f, 0);
        int rc = host_begin(&f, &plan, grant);
        CHECK(rc == (i == 5 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
        CHECK(!grant->finish_status);
        if (i == 5) {
            run(&f); CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE);
            CHECK(close_frame(&f) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        } else {
            CHECK(!f.ctx->identity && !f.ctx->pin_owned);
            CHECK(!wl_columnar_eval_stack_cleanup_finish(&f.frame));
            f.ctx = NULL;
        }
        CHECK(wl_evaluation_control_finish(f.operation, &f.owner, 0, 0) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.consumed == consumed[i]);
        fixture_destroy(&f);
    }
}

/* Publication fixtures own real heap arrays and actual RESERVED credits.
 * Preparation happens before parent begin; no live registry/token is copied. */
static uint64_t
registry_prepare(fixture_t *f, unsigned count, bool hole, bool built,
    bool zero_tokens)
{
    CHECK(!f->ctx->identity && count && count <= 3);
    if (!zero_tokens) {
        wl_columnar_memory_reservation_init(&f->session.rels_reservation);
        wl_columnar_memory_reservation_init(&f->session.rel_hash_reservation);
    } else if (f->session.memory_governor) {
        wl_columnar_memory_reservation_init(&f->session.rels_reservation);
    }
    f->session.rels = calloc(count, sizeof(*f->session.rels));
    CHECK(f->session.rels);
    f->session.nrels = count; f->session.rel_cap = count;
    for (unsigned i = 0; i < count; ++i)
        if (!(hole && i == 1)) f->session.rels[i] = f->inputs[i];
    if (f->session.memory_governor) {
        CHECK(wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(f->session.memory_governor),
                count * sizeof(*f->session.rels), &f->session.rels_reservation)
            == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    }
    if (built) CHECK(!session_rel_build_hash(&f->session));
    if (!f->session.memory_governor) return 0;
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f->session.memory_governor));
    CHECK(reserved >= f->session.cleanup_reserved_bytes);
    return reserved - f->session.cleanup_reserved_bytes;
}

static void
registry_destroy(fixture_t *f)
{
    for (uint32_t i = 0; i < f->session.nrels; ++i) {
        col_rel_t *rel = f->session.rels[i];
        if (rel && rel != f->inputs[0] && rel != f->inputs[1]
            && rel != f->inputs[2]) CHECK(!col_rel_destroy_checked(rel));
    }
    free((void *)f->session.rels); f->session.rels = NULL;
    if (atomic_load_explicit(&f->session.rels_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)
        CHECK(wl_columnar_memory_rollback(&f->session.rels_reservation));
    session_rel_free_hash(&f->session);
    f->session.nrels = f->session.rel_cap = 0;
}

static int
host_publish(fixture_t *f, grant_t *grant)
{
    int rc = wl_columnar_eval_serial_framed_attempt_publish(f->ctx,
            grant->control, grant->id);
    grant->finish_status = wl_evaluation_control_finish(grant->control,
            &grant->owner, 0, 0);
    return rc;
}

static void
publish(fixture_t *f)
{
    grant_t *grant = grant_begin(f, 0);
    CHECK(!host_publish(f, grant) && !grant->finish_status);
}

static int
close_published(fixture_t *f, bool expected_committed)
{
    CHECK(f->ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE);
    grant_t *grant = grant_begin(f, 0);
    int result = EINVAL; bool committed = !expected_committed;
    CHECK(!host_close(f, grant, &result, &committed));
    CHECK(!grant->finish_status && committed == expected_committed);
    f->frame = NULL; f->ctx = NULL;
    CHECK(!f->session.cleanup_active && !f->session.cleanup_pending
        && !f->session.cleanup_active_count &&
        !f->session.cleanup_reserved_bytes);
    return result;
}

static void
until_image(fixture_t *f, wl_columnar_session_hash_registry_phase_t phase)
{
    unsigned turns = 0;
    while (!(f->ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE
        && f->ctx->publication.phase == phase)) {
        CHECK(++turns < 200000 &&
            f->ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE);
        turn(f);
    }
}

static void
test_publication_shapes(void)
{
    const unsigned widths[] = {0, 1, 17, 129};
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned flags = 0; flags < 32; ++flags) {
            bool governed = flags & 1, built = flags & 2, hole = flags & 4;
            if (governed && (flags & 16)) continue;
            fixture_t f; fixture_init(&f, widths[w], flags & 8 ? 0 : 4,
                false, governed, 0);
            registry_prepare(&f, hole ? 3 : 1, hole, built,
                !governed && !(flags & 16));
            col_rel_t **old = f.session.rels;
            uint32_t old_count = f.session.nrels;
            begin(&f, &plan); run(&f); compare_raw(&f, &plan);
            col_rel_t *candidate = f.ctx->candidate;
            CHECK(candidate && !session_rel_hash_lookup(&f.session, "output"));
            publish(&f);
            until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATE_RELEASE);
            CHECK(f.ctx->committed && !f.ctx->candidate_owned &&
                !f.ctx->candidate);
            CHECK(session_rel_hash_lookup(&f.session, "output") == candidate);
            CHECK(f.session.nrels == old_count + !hole);
            CHECK(f.session.rels[hole ? 1 : old_count] == candidate);
            CHECK(f.ctx->child.identity == &f.ctx->child
                && f.ctx->child.source_registry == old
                && f.ctx->publication.image.expected_rels == old);
            CHECK(session_rel_hash_lookup(&f.session, "a") == f.inputs[0]);
            if (hole) CHECK(session_rel_hash_lookup(&f.session,
                    "c") == f.inputs[2]);
            until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF);
            CHECK(!f.ctx->child.identity && !f.ctx->child_bound
                && f.ctx->publication.image.expected_rels == old);
            turn(&f); /* Post-ACK redirect; old storage remains until its own turn. */
            CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE
                && f.ctx->publication.image.expected_rels == old);
            grant_t *poor = grant_begin(&f, 383);
            CHECK(host_step(&f, poor,
                false) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(poor->finish_status == WL_EVALUATION_CONTROL_WORK_LIMIT
                && f.ctx->publication.image.expected_rels == old);
            run(&f);
            CHECK(f.ctx->publication.drained &&
                !f.ctx->publication.image.expected_rels);
            CHECK(session_rel_hash_lookup(&f.session, "output") == candidate);
            CHECK(!close_published(&f, true));
            if (!governed) {
                CHECK(f.session.rels_reservation.identity == ((flags & 16)
                    ? &f.session.rels_reservation : NULL));
                CHECK(f.session.rel_hash_reservation.identity == ((flags & 16)
                    ? &f.session.rel_hash_reservation : NULL));
            }
            registry_destroy(&f); fixture_destroy(&f);
        }
    /* Exact raw collision chains and a name that requires many byte turns. */
    fixture_t f; fixture_init(&f, 1, 4, true, true, 0);
    free(f.inputs[1]->name); f.inputs[1]->name = strdup("q");
    free(f.inputs[2]->name); f.inputs[2]->name = strdup("Q");
    CHECK(f.inputs[1]->name && f.inputs[2]->name);
    registry_prepare(&f, 3, false, true, false);
    char name[300]; memset(name, 'x', sizeof(name)-1); name[sizeof(name)-1] = 0;
    plan.name = name; begin(&f, &plan); run(&f); compare_raw(&f, &plan);
    col_rel_t *candidate = f.ctx->candidate;
    publish(&f); run(&f); CHECK(!close_published(&f, true));
    CHECK(session_rel_hash_lookup(&f.session, name) == candidate
        && session_rel_hash_lookup(&f.session, "a") == f.inputs[0]
        && session_rel_hash_lookup(&f.session, "q") == f.inputs[1]
        && session_rel_hash_lookup(&f.session, "Q") == f.inputs[2]);
    registry_destroy(&f); fixture_destroy(&f);
}

typedef struct {
    const void *identity;
    wl_columnar_memory_governor_t *governor;
    uint64_t bytes, replacement, owner, state;
    uint8_t transition;
} registry_token_snapshot_t;

static registry_token_snapshot_t
registry_token_snapshot(const wl_columnar_memory_reservation_t *token)
{
    return (registry_token_snapshot_t){token->identity, token->governor,
                                       token->bytes, token->replacement_bytes,
                                       atomic_load_explicit(&token->owner_bits,
                                           memory_order_acquire),
                                       atomic_load_explicit(&token->state,
                                           memory_order_acquire),
                                       token->transition_kind};
}

static void
registry_token_matches(const wl_columnar_memory_reservation_t *token,
    registry_token_snapshot_t saved)
{
    registry_token_snapshot_t now = registry_token_snapshot(token);
    CHECK(now.identity == saved.identity && now.governor == saved.governor
        && now.bytes == saved.bytes && now.replacement == saved.replacement
        && now.owner == saved.owner && now.state == saved.state
        && now.transition == saved.transition);
}

static fixture_t *registry_fault_fixture;
static unsigned registry_stop_move, registry_reserve_phase;
static bool registry_turnover, registry_reverse_refusal, registry_late_admit;
static bool registry_reserve_final_tail;
static wl_columnar_session_hash_registry_phase_t registry_stop_phase;

static void
registry_stop_original(wl_evaluation_control_t *control)
{
    if (registry_turnover) {
        fixture_t *f = registry_fault_fixture;
        CHECK(control == f->operation);
        CHECK(!wl_evaluation_control_finish(control, &f->owner, 0, 0));
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_begin(control, &f->owner));
        CHECK(wl_evaluation_control_active_attempt_id(control) != f->id);
    } else wl_evaluation_control_request_cancel(control);
}

static void
registry_checkpoint(wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    if (!registry_fault_fixture || ctx != registry_fault_fixture->ctx) return;
    bool move = registry_stop_move && ctx->publication.phase ==
        WL_COLUMNAR_SESSION_HASH_REGISTRY_MOVE &&
        ctx->publication.move == registry_stop_move;
    bool admit = registry_late_admit && ctx->publication.phase ==
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_R &&
        ctx->publication.credit_rels;
    bool allocation = registry_stop_phase &&
        ctx->publication.phase == registry_stop_phase;
    if (!move && !admit && !allocation) return;
    registry_stop_move = 0; registry_late_admit = false;
    registry_stop_phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_INERT;
    registry_stop_original(ctx->control);
}

static int
registry_refuse_reverse(wl_columnar_eval_serial_framed_attempt_t *ctx,
    unsigned index, bool reverse)
{
    (void)ctx; (void)index;
    if (reverse && registry_reverse_refusal) {
        registry_reverse_refusal = false; return EBUSY;
    }
    return 0;
}

static void
registry_reserve_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (registry_fault_fixture && phase == registry_reserve_phase
        && token ==
        &registry_fault_fixture->ctx->publication.image.rels_reservation){
        registry_reserve_phase = 0;
        registry_stop_original(control);
    }
}

static void
registry_reserve_work_checkpoint(wl_evaluation_control_t *control,
    uint64_t work)
{
    (void)work;
    if (registry_fault_fixture && registry_reserve_final_tail
        && atomic_load_explicit(
            &registry_fault_fixture->ctx->publication.image.rels_reservation.
            state,
            memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED) {
        registry_reserve_final_tail = false;
        registry_stop_original(control);
    }
}

static void
registry_clear_hooks(void)
{
    registry_fault_fixture = NULL;
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;
    wl_columnar_session_hash_test_registry_move = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    registry_reserve_final_tail = false;
    registry_stop_phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_INERT;
    registry_stop_move = registry_reserve_phase = 0;
    registry_turnover = registry_reverse_refusal = registry_late_admit = false;
}

static void
test_publication_reverse(void)
{
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    for (unsigned move = 1; move <= 4; ++move)
        for (unsigned turnover = 0; turnover < 2; ++turnover) {
            fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
            uint64_t baseline = registry_prepare(&f, 3, false, true, false);
            col_rel_t **old = f.session.rels;
            uint32_t *head = f.session.rel_hash_head,
                *next = f.session.rel_hash_next;
            registry_token_snapshot_t r =
                registry_token_snapshot(&f.session.rels_reservation);
            registry_token_snapshot_t h =
                registry_token_snapshot(&f.session.rel_hash_reservation);
            begin(&f, &plan); run(&f);
            registry_fault_fixture = &f; registry_stop_move = move;
            registry_turnover = turnover; registry_reverse_refusal = true;
            wl_columnar_eval_serial_test_framed_checkpoint =
                registry_checkpoint;
            wl_columnar_session_hash_test_registry_move =
                registry_refuse_reverse;
            publish(&f);
            until_image(&f, WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE);
            int expected = turnover ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(f.ctx->primary_result == expected);
            unsigned turns = 0;
            while (!f.ctx->cleanup_result) {
                CHECK(++turns < 10); turn(&f);
            }
            CHECK(f.ctx->cleanup_result == EBUSY && f.ctx->publication.identity
                && f.ctx->publication.image.rels &&
                f.ctx->primary_result == expected);
            registry_clear_hooks(); run(&f);
            CHECK(close_published(&f, false) == expected);
            CHECK(f.session.rels == old && f.session.rel_hash_head == head
                && f.session.rel_hash_next == next && f.session.nrels == 3);
            registry_token_matches(&f.session.rels_reservation, r);
            registry_token_matches(&f.session.rel_hash_reservation, h);
            CHECK(!session_rel_hash_lookup(&f.session, "output"));
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(
                        f.session.memory_governor)) == baseline);
            if (turnover) {
                CHECK(!wl_evaluation_control_finish(f.operation, &f.owner, 0,
                    0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.operation,
                    &report) && !report.consumed);
            }
            registry_destroy(&f); fixture_destroy(&f);
        }
}

static void
test_publication_failures(void)
{
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    /* Discover the reached allocation sites until the first complete attempt. */
    for (int ordinal = 0; ; ++ordinal) {
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        uint64_t baseline = registry_prepare(&f, 1, false, false, false);
        begin(&f, &plan); run(&f);
        wl_columnar_session_hash_test_registry_allocation = ordinal;
        publish(&f); run(&f);
        wl_columnar_session_hash_test_registry_allocation = -1;
        bool committed = f.ctx->committed;
        CHECK(close_published(&f, committed) == (committed ? 0 : ENOMEM));
        if (!committed) CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(
                        f.session.memory_governor)) == baseline);
        registry_destroy(&f); fixture_destroy(&f);
        if (committed) {
            CHECK(ordinal == 4); break;
        }
    }
    const wl_columnar_session_hash_registry_phase_t phases[] = {
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_R,
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_E,
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_H
    };
    for (unsigned i = 0; i < 3; ++i) {
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        uint64_t baseline = registry_prepare(&f, 1, false, false, false);
        begin(&f, &plan); run(&f); publish(&f); until_image(&f, phases[i]);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(f.session.memory_governor);
        uint64_t available = atomic_load_explicit(&g->usable_bytes,
                memory_order_acquire) - wl_columnar_memory_reserved(g);
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        CHECK(wl_columnar_memory_reserve_checked(g, available, &peer)
            == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        run(&f); CHECK(close_published(&f, false) == ENOSPC);
        CHECK(f.session.memory_budget_denied &&
            wl_columnar_memory_rollback(&peer));
        CHECK(wl_columnar_memory_reserved(g) == baseline);
        registry_destroy(&f); fixture_destroy(&f);
    }
    const unsigned hooks[] = {WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
                              WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
                              WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED};
    for (unsigned i = 0; i < 5; ++i)
        for (unsigned turnover = 0; turnover < 2; ++turnover) {
            fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
            uint64_t baseline = registry_prepare(&f, 1, false, false, false);
            begin(&f, &plan); run(&f); publish(&f);
            until_image(&f, WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_R);
            registry_fault_fixture = &f; registry_turnover = turnover;
            if (i < 3) {
                registry_reserve_phase = hooks[i];
                wl_columnar_memory_governor_test_checkpoint =
                    registry_reserve_checkpoint;
            } else if (i == 3) {
                registry_reserve_final_tail = true;
                wl_columnar_memory_governor_test_work_checkpoint =
                    registry_reserve_work_checkpoint;
            } else {
                registry_late_admit = true;
                wl_columnar_eval_serial_test_framed_checkpoint =
                    registry_checkpoint;
            }
            turn(&f);
            CHECK(f.ctx->publication.phase ==
                WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE);
            CHECK(f.ctx->publication.credit_rels == (i == 4));
            registry_clear_hooks(); run(&f);
            CHECK(close_published(&f,
                false) ==
                (turnover ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(
                        f.session.memory_governor)) == baseline);
            if (turnover) {
                CHECK(!wl_evaluation_control_finish(f.operation, &f.owner, 0,
                    0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.operation,
                    &report) && !report.consumed);
            }
            registry_destroy(&f); fixture_destroy(&f);
        }
    const wl_columnar_session_hash_registry_phase_t after_allocations[] = {
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_E,
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_H,
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_NEXT,
        WL_COLUMNAR_SESSION_HASH_REGISTRY_ZERO_R
    };
    for (unsigned i = 0; i < 4; ++i) {
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        uint64_t baseline = registry_prepare(&f, 1, false, false, false);
        begin(&f, &plan); run(&f);
        registry_fault_fixture = &f; registry_stop_phase = after_allocations[i];
        wl_columnar_eval_serial_test_framed_checkpoint = registry_checkpoint;
        publish(&f); until_image(&f, WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE);
        CHECK(f.ctx->publication.image.rels);
        if (i > 0) CHECK(f.ctx->publication.image.expected_rel_contents);
        if (i > 1) CHECK(f.ctx->publication.image.hash_head);
        if (i > 2) CHECK(f.ctx->publication.image.hash_next);
        registry_clear_hooks(); run(&f);
        CHECK(close_published(&f, false) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                f.session.memory_governor)) == baseline);
        registry_destroy(&f); fixture_destroy(&f);
    }
    /* Original all-zero destinations remain unsupported with new governor credit. */
    {
        fixture_t f; fixture_init(&f, 1, 4, false, true, 0);
        registry_prepare(&f, 1, false, false, true);
        registry_token_snapshot_t saved =
            registry_token_snapshot(&f.session.rel_hash_reservation);
        CHECK(!saved.identity);
        begin(&f, &plan); run(&f); publish(&f);
        CHECK(!f.ctx->publication.identity && f.ctx->primary_result == ENOTSUP);
        run(&f); CHECK(close_published(&f, false) == ENOTSUP);
        registry_token_matches(&f.session.rel_hash_reservation, saved);
        registry_destroy(&f); fixture_destroy(&f);
    }
    /* The genuine A result can have an empty private name; publication
     * enforces the raw registry's stricter nonempty head-name contract. */
    plan.name = "";
    for (unsigned governed = 0; governed < 2; ++governed) {
        fixture_t f; fixture_init(&f, 1, 4, false, governed, 0);
        registry_prepare(&f, 1, false, true, !governed);
        registry_token_snapshot_t r =
            registry_token_snapshot(&f.session.rels_reservation);
        registry_token_snapshot_t h =
            registry_token_snapshot(&f.session.rel_hash_reservation);
        col_rel_t **old = f.session.rels;
        begin(&f, &plan); run(&f);
        CHECK(f.ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY
            && f.ctx->candidate && !f.ctx->candidate->name[0]);
        publish(&f);
        CHECK(!f.ctx->publication.identity && !f.ctx->publication.image.rels
            && !f.ctx->publication.image.rels_reservation.identity
            && !f.ctx->publication.image.hash_reservation.identity
            && f.ctx->primary_result == EINVAL);
        registry_token_matches(&f.session.rels_reservation, r);
        registry_token_matches(&f.session.rel_hash_reservation, h);
        run(&f); CHECK(close_published(&f, false) == EINVAL);
        CHECK(f.session.rels == old && f.session.nrels == 1);
        registry_token_matches(&f.session.rels_reservation, r);
        registry_token_matches(&f.session.rel_hash_reservation, h);
        registry_destroy(&f); fixture_destroy(&f);
    }
    plan.name = "output";
    for (unsigned governed = 0; governed < 2; ++governed) {
        fixture_t f; fixture_init(&f, 1, 4, false, governed, 0);
        registry_prepare(&f, 1, false, true, false);
        /* A real released token, without fabricated state or live-token copy. */
        wl_columnar_memory_governor_ref_t *temporary = NULL;
        if (governed) session_rel_free_hash(&f.session);
        else {
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = 1024, .usable_bytes = 1024,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
                .status = WL_COLUMNAR_MEMORY_OK
            };
            temporary = wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(temporary && wl_columnar_memory_reserve_checked(
                    wl_columnar_memory_governor_ref_get(temporary), 1,
                    &f.session.rel_hash_reservation) ==
                WL_COLUMNAR_MEMORY_ADMISSION_OK);
            CHECK(wl_columnar_memory_rollback(&f.session.rel_hash_reservation));
        }
        registry_token_snapshot_t saved =
            registry_token_snapshot(&f.session.rel_hash_reservation);
        col_rel_t **old = f.session.rels;
        begin(&f, &plan); run(&f); publish(&f);
        CHECK(!f.ctx->publication.identity && f.ctx->primary_result == ENOTSUP);
        registry_token_matches(&f.session.rel_hash_reservation, saved);
        run(&f); CHECK(close_published(&f, false) == ENOTSUP);
        CHECK(f.session.rels == old);
        registry_token_matches(&f.session.rel_hash_reservation, saved);
        registry_destroy(&f); fixture_destroy(&f);
        if (temporary) wl_columnar_memory_governor_ref_release(temporary);
    }
}

/* Actual created session and actual generated relation plan. No preserved
 * body substitution, fabricated READY state, or test-only backend bypass. */
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_col_session_t *session;
    const wl_plan_relation_t *relation;
    const wl_plan_stratum_t *stratum;
    wl_evaluation_control_t *operation;
    wl_columnar_memory_governor_ref_t *governor;
    uint64_t id;
    grant_t grants[3];
    wl_columnar_eval_serial_framed_host_request_t previous;
    wl_columnar_eval_serial_framed_host_reply_t reply;
    int protocol;
    unsigned next, calls, witnesses;
} entry_fixture_t;

typedef struct {
    const char *name;
    const int64_t *rows;
    uint32_t count, width;
} entry_input_t;

static void
entry_init_inputs(entry_fixture_t *f, const char *source,
    const entry_input_t *inputs, uint32_t input_count, uint64_t limit)
{
    memset(f, 0, sizeof(*f));
    wirelog_error_t error;
    f->program = wirelog_parse_string(source, &error); CHECK(f->program);
    CHECK(!wl_plan_from_program(f->program, &f->plan));
    for (uint32_t s = 0; s < f->plan->stratum_count; ++s)
        for (uint32_t r = 0; r < f->plan->strata[s].relation_count; ++r)
            if (!strcmp(f->plan->strata[s].relations[r].name, "output")) {
                f->stratum = &f->plan->strata[s];
                f->relation = &f->stratum->relations[r];
            }
    CHECK(f->relation && !f->stratum->is_recursive);
    CHECK(!wl_evaluation_control_create(limit, &f->operation));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution); CHECK(governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = f->operation;
    options.memory_governor = governor;
    wl_session_t *base = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &base));
    f->governor = governor;
    f->session = (wl_col_session_t *)base;
    CHECK(!session_find_rel(f->session, "output"));
    for (uint32_t i = 0; i < input_count; ++i) {
        CHECK(inputs[i].name && (inputs[i].rows || !inputs[i].count));
        if (inputs[i].count)
            CHECK(!wl_session_insert(base, inputs[i].name, inputs[i].rows,
                inputs[i].count, inputs[i].width));
    }
    /* Non-owner bookkeeping probes make a misplaced heads clear or frontier
     * reset observable. No engine phase, READY state or token is fabricated. */
    f->session->eval_stratum_heads_final = true;
    if (!f->session->progress.entries)
        CHECK(!wl_frontier_progress_init(&f->session->progress, 1,
            f->plan->stratum_count));
    wl_frontier_progress_record(&f->session->progress, 0, 0,
        f->session->outer_epoch, 7);
    CHECK(!wl_evaluation_control_begin(f->operation,
        base->operation_admission));
    f->id = wl_evaluation_control_active_attempt_id(f->operation);
    for (unsigned i = 0; i < 3; ++i)
        f->grants[i].control = control_new(0, &f->grants[i].owner);
}

static void
entry_init(entry_fixture_t *f, const char *source, const int64_t *rows,
    uint32_t count, uint32_t width, uint64_t limit)
{
    const entry_input_t input = {"input", rows, count, width};
    entry_init_inputs(f, source, &input, 1, limit);
}

static wl_columnar_eval_serial_framed_attempt_t *
entry_context(entry_fixture_t *f)
{
    if (!f->session->cleanup_active) return NULL;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(f->session->cleanup_active,
        f->session, &ctx));
    CHECK(ctx->identity == ctx);
    return ctx;
}

static grant_t *
entry_grant(entry_fixture_t *f, uint64_t limit)
{
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(f);
    for (unsigned i = 0; i < 3; ++i) {
        grant_t *grant = &f->grants[f->next]; f->next = (f->next + 1) % 3;
        if ((ctx && ctx->pending_funding == grant->control)
            || (f->previous.previous_valid &&
            f->previous.previous_funding == grant->control)) continue;
        CHECK(!wl_evaluation_control_reset(grant->control));
        CHECK(!wl_evaluation_control_configure(grant->control, limit));
        CHECK(!wl_evaluation_control_begin(grant->control, &grant->owner));
        grant->id = wl_evaluation_control_active_attempt_id(grant->control);
        return grant;
    }
    CHECK(false); return NULL;
}

/* This is the actual host suffix used for the source certificate. All protocol
 * storage, queue dispatch and diagnostics are outside this function's return. */
static int
entry_host(entry_fixture_t *f,
    const wl_columnar_eval_serial_framed_host_request_t *request,
    grant_t *grant)
{
    int rc = wl_columnar_eval_serial_framed_relation(f->relation, f->session,
            false, request);
    (void)wl_evaluation_control_finish(grant->control,
        &grant->owner, 0, 0);
    return rc;
}

static int
entry_witness_job(entry_fixture_t *f,
    const wl_columnar_eval_serial_framed_host_request_t *request,
    grant_t *grant)
{
    (void)request; (void)grant;
    CHECK(f->calls == f->witnesses + 1);
    ++f->witnesses;
    return 0;
}

static int
entry_turn(entry_fixture_t *f, bool status, uint64_t limit)
{
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(f);
    bool consume = !status && ((ctx && ctx->phase ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING)
        || (!ctx && f->previous.previous_valid
        && f->previous.previous_kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT));
    grant_t *grant = entry_grant(f, limit);
    wl_columnar_eval_serial_framed_host_request_t request = f->previous;
    request.funding = grant->control; request.funding_id = grant->id;
    request.operation = f->operation; request.operation_id = f->id;
    request.action = status ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS
        : WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE;
    request.reply = &f->reply;
    /* entry_host itself is the queued action callback. Protocol storage and
     * the next job's dispatch occur in this external driver AFTER that actual
     * callback returns; no enclosing action-job wrapper extends its suffix. */
    struct {
        int (*run)(entry_fixture_t *,
            const wl_columnar_eval_serial_framed_host_request_t *, grant_t *);
        const wl_columnar_eval_serial_framed_host_request_t *request;
        grant_t *grant;
    } queue[] = {{entry_host, &request, grant},
                 {entry_witness_job, NULL, NULL}};
    ++f->calls;
    for (unsigned i = 0; i < 2; ++i) {
        int protocol = queue[i].run(f, queue[i].request, queue[i].grant);
        if (!i) f->protocol = protocol;
        else CHECK(!protocol);
    }
    CHECK(f->calls == f->witnesses);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant->control, &report)
        && report.attempt_id == grant->id && !report.cleanup_status
        && (!report.execution_status || report.execution_status == EOVERFLOW)
        && !wl_evaluation_control_active_attempt_id(grant->control));
    if (!status && (!consume || !f->protocol)) {
        f->previous.previous_valid = !consume;
        f->previous.previous_kind = f->reply.kind;
        f->previous.previous_protocol = f->protocol;
        f->previous.previous_funding = grant->control;
        f->previous.previous_funding_id = grant->id;
    }
    return f->protocol;
}

static void
entry_complete(entry_fixture_t *f)
{
    unsigned turns = 0;
    while (f->reply.kind != WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL) {
        CHECK(++turns < 200000);
        int protocol = entry_turn(f, false, 0);
        if (protocol) {
            /* A child error is consumed through its genuine pending receipt;
             * it is not the final semantic outcome of this relation. */
            wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(f);
            CHECK(f->previous.previous_valid
                && f->previous.previous_protocol == protocol
                && f->previous.previous_kind == f->reply.kind
                && ((ctx && ctx->phase ==
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
                && ctx->pending_funding == f->previous.previous_funding
                && ctx->pending_funding_id == f->previous.previous_funding_id)
                || (!ctx && f->reply.kind ==
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT)));
        }
    }
    CHECK(!f->session->cleanup_active && !f->session->cleanup_pending
        && !f->session->cleanup_reserved_bytes);
}

static void
entry_finish_original(entry_fixture_t *f)
{
    int result = f->reply.execution_result;
    bool stopped = result == WL_EVALUATION_CONTROL_CANCELLED
        || result == WL_EVALUATION_CONTROL_WORK_LIMIT;
    /* A zero execution status cannot manufacture an engine stop at finish. */
    CHECK(wl_evaluation_control_finish(f->operation,
        f->session->base.operation_admission, stopped ? 0 : result, 0)
        == result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->operation, &report)
        && report.attempt_id == f->id && report.result == result
        && !report.cleanup_status
        && report.execution_status == (stopped ? 0 : result)
        && report.stop_reason ==
        (stopped ? result : WL_EVALUATION_CONTROL_NONE));
}

static void
entry_destroy(entry_fixture_t *f)
{
    CHECK(!f->session->cleanup_active && !f->session->cleanup_pending);
    if (wl_evaluation_control_active_attempt_id(f->operation) == f->id)
        entry_finish_original(f);
    for (unsigned i = 0; i < 3; ++i) {
        CHECK(!wl_evaluation_control_active_attempt_id(f->grants[i].control));
        CHECK(!wl_evaluation_control_detach(f->grants[i].control,
            &f->grants[i].owner));
        wl_evaluation_control_release(f->grants[i].control);
    }
    CHECK(f->session->memory_governor);
    wl_session_destroy(&f->session->base);
    wl_evaluation_control_release(f->operation);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    if (f->governor) {
        CHECK(!wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(f->governor)));
        wl_columnar_memory_governor_ref_release(f->governor);
    }
}

static void
entry_compare_inputs(entry_fixture_t *f, const char *source,
    const entry_input_t *inputs, uint32_t input_count)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL; CHECK(!wl_plan_from_program(program, &plan));
    wl_session_t *base = NULL;
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &base));
    for (uint32_t i = 0; i < input_count; ++i)
        if (inputs[i].count)
            CHECK(!wl_session_insert(base, inputs[i].name, inputs[i].rows,
                inputs[i].count, inputs[i].width));
    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "output"))
                rp = &plan->strata[s].relations[r];
    CHECK(rp);
    CHECK(!wl_columnar_eval_serial_framed_relation(rp,
        (wl_col_session_t *)base, false, NULL));
    const col_rel_t *a = session_rel_hash_lookup(f->session, "output");
    const col_rel_t *b = session_find_rel((wl_col_session_t *)base, "output");
    CHECK(a && b && !strcmp(a->name, "output") && a->nrows == b->nrows
        && a->ncols == b->ncols && !a->timestamps && !b->timestamps
        && a->declared_ncols == b->declared_ncols
        && a->has_graph_column == b->has_graph_column
        && a->graph_col_idx == b->graph_col_idx);
    CHECK(a->sorted_nrows == a->nrows && a->run_count == (a->nrows != 0));
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
        CHECK(a->column_types && b->column_types
            && a->column_types[c] == b->column_types[c]);
        for (uint32_t r = 0; r < a->nrows; ++r)
            CHECK(a->columns[c][r] == b->columns[c][r]);
    }
    wl_session_destroy(base); wl_plan_free(plan); wirelog_program_free(program);
}

static void
entry_compare(entry_fixture_t *f, const char *source, const int64_t *rows,
    uint32_t count, uint32_t width)
{
    const entry_input_t input = {"input", rows, count, width};
    entry_compare_inputs(f, source, &input, 1);
}

static char *
entry_program(unsigned width, bool floating)
{
    char *source = calloc(1, 32768); CHECK(source);
    size_t offset = 0;
    for (unsigned declaration = 0; declaration < 2; ++declaration) {
        offset += snprintf(source + offset, 32768 - offset, ".decl %s(",
                declaration ? "output" : "input");
        for (unsigned c = 0; c < width; ++c)
            offset += snprintf(source + offset, 32768 - offset,
                    "%sc%u:%s", c ? "," : "", c,
                    floating ? "float" : "int64");
        offset += snprintf(source + offset, 32768 - offset, ")\n");
    }
    offset += snprintf(source + offset, 32768 - offset, "output(");
    for (unsigned c = 0; c < width; ++c)
        offset += snprintf(source + offset, 32768 - offset,
                "%sx%u", c ? "," : "", c);
    offset += snprintf(source + offset, 32768 - offset, ") :- input(");
    for (unsigned c = 0; c < width; ++c)
        offset += snprintf(source + offset, 32768 - offset,
                "%sx%u", c ? "," : "", c);
    offset += snprintf(source + offset, 32768 - offset, ").\n");
    CHECK(offset < 32768); return source;
}

static void
entry_parent_observation(entry_fixture_t *f, uint64_t out[64])
{
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(f);
    if (!ctx) return;
    unsigned n = 0;
#define OBSERVE(x) (out[n++] = (uint64_t)(x))
#define ADDRESS(x) OBSERVE((uintptr_t)(x))
    ADDRESS(ctx); OBSERVE(ctx->phase); OBSERVE(ctx->pending_action);
    ADDRESS(ctx->pending_funding); OBSERVE(ctx->pending_funding_id);
    ADDRESS(ctx->child.identity); OBSERVE(ctx->child.phase);
    OBSERVE(ctx->child.cursor); OBSERVE(ctx->child.completed);
    ADDRESS(ctx->candidate); OBSERVE(ctx->candidate_owned);
    OBSERVE(ctx->row); OBSERVE(ctx->cursor); OBSERVE(ctx->name_byte);
    OBSERVE(ctx->primary_result); OBSERVE(ctx->cleanup_result);
    OBSERVE(ctx->committed); OBSERVE(ctx->child_bound);
    OBSERVE(ctx->gate_owned);
    OBSERVE(ctx->source_initialized); OBSERVE(ctx->batch_initialized);
    ADDRESS(ctx->input.rel); OBSERVE(ctx->input.owned);
    ADDRESS(ctx->publication.identity); OBSERVE(ctx->publication.phase);
    OBSERVE(ctx->publication.committed); OBSERVE(ctx->publication.drained);
    OBSERVE(ctx->publication.cursor); OBSERVE(ctx->publication.move);
    ADDRESS(ctx->publication.image.rels);
    ADDRESS(ctx->publication.image.expected_rel_contents);
    ADDRESS(ctx->publication.image.hash_head);
    ADDRESS(ctx->publication.image.hash_next);
    ADDRESS(ctx->publication.reverse_source);
    ADDRESS(ctx->publication.reverse_destination);
    for (unsigned i = 0; i < 4; ++i) OBSERVE(ctx->publication.journal[i]);
#undef ADDRESS
#undef OBSERVE
    CHECK(n <= 64);
}

static void
entry_outer_guards(entry_fixture_t *f)
{
    uint64_t before = wl_test_evaluation_guard_state(f->session);
    uint64_t parent_before[64] = {0}, parent_after[64] = {0};
    entry_parent_observation(f, parent_before);
    CHECK(wl_session_step(&f->session->base) == ENOTSUP);
    CHECK(wl_session_snapshot(&f->session->base, NULL, NULL) == ENOTSUP);
    CHECK(f->session->base.backend->session_step(&f->session->base) == ENOTSUP);
    CHECK(f->session->base.backend->session_snapshot(&f->session->base,
        NULL, NULL) == ENOTSUP);
    CHECK(col_eval_stratum(f->stratum, f->session, 0) == ENOTSUP);
    CHECK(col_eval_stratum_tdd(f->stratum, f->session, 0) == ENOTSUP);
    CHECK(col_eval_stratum_multiworker(f->stratum, f->session, 0,
        f->session, 1) == ENOTSUP);
    CHECK(wl_columnar_eval_serial_framed_relation(f->relation, f->session,
        false, NULL) == ENOTSUP);
    CHECK(wl_test_evaluation_guard_state(f->session) == before);
    entry_parent_observation(f, parent_after);
    CHECK(!memcmp(parent_before, parent_after, sizeof(parent_before)));
}

static void
test_entry_shapes(void)
{
    const unsigned widths[] = {0, 1, 17, 129};
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned floating = 0; floating < 2; ++floating)
            for (unsigned empty = 0; empty < 2; ++empty) {
                /* Create leaves empty nullary/all-INT sources without a
                 * schema. Nonempty insert initializes even nullary schemas;
                 * preserve their positive literal and raw parity checks. */
                bool unsupported = empty && (!widths[w] || !floating);
                char *source = entry_program(widths[w], floating);
                int64_t *rows = calloc(4 * (widths[w] ? widths[w] : 1),
                        sizeof(*rows)); CHECK(rows);
                const int values[] = {3, 1, 3, 2};
                for (unsigned r = 0; r < 4; ++r)
                    for (unsigned c = 0; c < widths[w]; ++c)
                        rows[r * widths[w] + c] = floating
                            ? wl_columnar_float_to_bits(values[r] + c + 0.5)
                            : values[r] + c;
                entry_fixture_t f;
                entry_init(&f, source, rows, empty ? 0 : 4, widths[w], 0);
                CHECK(f.relation->op_count >= 2
                    && f.relation->ops[0].op == WL_PLAN_OP_VARIABLE
                    && f.relation->ops[f.relation->op_count - 1].op ==
                    WL_PLAN_OP_CONSOLIDATE);
                entry_outer_guards(&f);
                CHECK(!entry_turn(&f, false, 0));
                CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
                    && entry_context(&f));
                entry_outer_guards(&f);
                entry_complete(&f);
                if (unsupported) {
                    CHECK(f.reply.execution_result == ENOTSUP
                        && !f.reply.committed
                        && !session_rel_hash_lookup(f.session, "output"));
                    entry_destroy(&f); free(rows); free(source);
                    continue;
                }
                CHECK(!f.reply.execution_result && f.reply.committed);
                const col_rel_t *out = session_rel_hash_lookup(f.session,
                        "output");
                CHECK(out && out->nrows == (empty ? 0 : widths[w] ? 3 : 1)
                    && out->ncols == widths[w]);
                for (unsigned r = 0; r < out->nrows; ++r)
                    for (unsigned c = 0; c < out->ncols; ++c)
                        CHECK(out->columns[c][r] == (floating
                            ? wl_columnar_float_to_bits(r + c + 1.5)
                            : (int64_t)(r + c + 1)));
                entry_compare(&f, source, rows, empty ? 0 : 4, widths[w]);
                CHECK(f.calls == f.witnesses && f.calls > 10);
                entry_destroy(&f); free(rows); free(source);
            }
}

static const char entry_generated_map[] =
    ".decl input(x:int64)\n.decl output(y:int64)\n"
    "output(x+1) :- input(x).\n";
static unsigned entry_map_checkpoint_count;

static void
entry_map_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)control;
    if (phase == 109 && progress == 0) ++entry_map_checkpoint_count;
}

static void
entry_map_fill_rows(int64_t *rows, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i) rows[i] = i + 1;
}

static unsigned
entry_map_trial(uint64_t limit, bool require_stop, bool require_success)
{
    int64_t rows[4096]; entry_map_fill_rows(rows, 4096);
    entry_fixture_t f; entry_init(&f, entry_generated_map, rows, 4096, 1,
        limit);
    const wl_plan_op_t *map = NULL; unsigned maps = 0;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_MAP) {
            map = &f.relation->ops[i]; ++maps;
        }
    CHECK(maps == 1 && map->map_exprs && map->map_exprs[0].data
        && map->map_exprs[0].size);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(f.session->memory_governor);
    uint64_t reserved = wl_columnar_memory_reserved(governor);
    col_rel_t **registry = f.session->rels;
    uint32_t registry_count = f.session->nrels;
    col_rel_t *input = session_find_rel(f.session, "input");
    CHECK(input && input->nrows == 4096 && input->ncols == 1);
    const int64_t *input_values = input->columns[0];
    entry_map_checkpoint_count = 0;
    wl_columnar_filter_test_attempt_checkpoint = entry_map_checkpoint;
    entry_complete(&f);
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    unsigned reached = entry_map_checkpoint_count;
    if (require_stop) {
        CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !f.reply.committed && !session_find_rel(f.session, "output")
            && session_find_rel(f.session, "input") == input
            && f.session->rels == registry && f.session->nrels == registry_count
            && input->columns[0] == input_values && input->nrows == 4096
            && wl_columnar_memory_reserved(governor) == reserved);
        for (uint32_t i = 0; i < 4096; ++i)
            CHECK(input_values[i] == (int64_t)i + 1);
    } else if (require_success) {
        CHECK(!f.reply.execution_result && f.reply.committed);
    }
    entry_destroy(&f);
    return reached;
}

static void
test_entry_generated_map_work_limit(void)
{
    const unsigned target = 1023;
    uint64_t low = 1, high = UINT64_C(1) << 30;
    CHECK(entry_map_trial(high, false, true) == 4096);
    while (low < high) {
        uint64_t mid = low + (high - low) / 2;
        if (entry_map_trial(mid, false, false) > target) high = mid;
        else low = mid + 1;
    }
    uint64_t limit = low;
    CHECK(entry_map_trial(limit - 1, true, false) <= target);
    CHECK(entry_map_trial(limit, true, false) == target + 1);
    CHECK(entry_map_trial(limit, true, false) == target + 1);

    int64_t rows[4096]; entry_map_fill_rows(rows, 4096);
    entry_fixture_t f; entry_init(&f, entry_generated_map, rows, 4096, 1,
        limit);
    entry_complete(&f);
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !f.reply.committed && !session_find_rel(f.session, "output"));
    uint64_t stopped_id = f.id;
    CHECK(wl_evaluation_control_finish(f.operation,
        f.session->base.operation_admission,
        WL_EVALUATION_CONTROL_WORK_LIMIT, 0)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_configure(f.operation, 0));
    CHECK(!wl_evaluation_control_begin(f.operation,
        f.session->base.operation_admission));
    f.id = wl_evaluation_control_active_attempt_id(f.operation);
    CHECK(f.id && f.id != stopped_id);
    f.previous.previous_valid = false;
    f.reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
    entry_map_checkpoint_count = 0;
    wl_columnar_filter_test_attempt_checkpoint = entry_map_checkpoint;
    entry_complete(&f);
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    CHECK(!f.reply.execution_result && f.reply.committed
        && entry_map_checkpoint_count == 4096);
    const col_rel_t *out = session_rel_hash_lookup(f.session, "output");
    CHECK(out && out->nrows == 4096 && out->ncols == 1);
    for (uint32_t i = 0; i < 4096; ++i)
        CHECK(out->columns[0][i] == (int64_t)i + 2);
    entry_compare(&f, entry_generated_map, rows, 4096, 1);
    entry_destroy(&f);
}

static const char entry_simple[] =
    ".decl input(x:int64)\n.decl output(x:int64)\n"
    "output(x) :- input(x).\n";
static const int64_t entry_rows[] = {3, 1, 3, 2};

static const char entry_generated_count_reduce[] =
    ".decl input(x:int64)\n.decl output(n:int64)\n"
    "output(count(x)) :- input(x).\n";
static unsigned entry_reduce_checkpoint_count;
static uint64_t entry_reduce_last_progress;
static bool entry_reduce_target_seen;

static void
entry_reduce_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)control;
    if (phase != 36) return;
    ++entry_reduce_checkpoint_count;
    entry_reduce_last_progress = progress;
    if (progress == 1023) entry_reduce_target_seen = true;
}

typedef struct {
    unsigned checkpoints;
    uint64_t last_progress, consumed;
    int result, cleanup_status;
    wl_evaluation_control_stop_t stop_reason;
    bool target_seen, committed;
} entry_reduce_trial_t;

static entry_reduce_trial_t
entry_reduce_trial(uint64_t limit, bool require_success)
{
    int64_t rows[4096]; entry_map_fill_rows(rows, 4096);
    entry_fixture_t f; entry_init(&f, entry_generated_count_reduce, rows,
        4096, 1, limit);
    const wl_plan_op_t *reduce = NULL; unsigned reduces = 0;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_REDUCE) {
            reduce = &f.relation->ops[i]; ++reduces;
        }
    CHECK(reduces == 1 && reduce->agg_fn == WIRELOG_AGG_COUNT
        && reduce->group_by_count == 0);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(f.session->memory_governor);
    uint64_t reserved = wl_columnar_memory_reserved(governor);
    col_rel_t **registry = f.session->rels;
    uint32_t registry_count = f.session->nrels;
    col_rel_t *input = session_find_rel(f.session, "input");
    CHECK(input && input->nrows == 4096 && input->ncols == 1);
    const int64_t *input_values = input->columns[0];
    entry_reduce_checkpoint_count = 0;
    entry_reduce_last_progress = UINT64_MAX;
    entry_reduce_target_seen = false;
    wl_columnar_ops_test_reduce_attempt_checkpoint = entry_reduce_checkpoint;
    entry_complete(&f);
    wl_columnar_ops_test_reduce_attempt_checkpoint = NULL;

    entry_reduce_trial_t trial = {
        .checkpoints = entry_reduce_checkpoint_count,
        .last_progress = entry_reduce_last_progress,
        .target_seen = entry_reduce_target_seen,
        .committed = f.reply.committed
    };
    if (f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT) {
        CHECK(!f.reply.committed && !session_find_rel(f.session, "output")
            && session_rel_hash_lookup(f.session, "input") == input
            && f.session->rels == registry && f.session->nrels == registry_count
            && input->columns[0] == input_values && input->nrows == 4096
            && wl_columnar_memory_reserved(governor) == reserved);
        for (uint32_t i = 0; i < 4096; ++i)
            CHECK(input_values[i] == (int64_t)i + 1);
    } else {
        CHECK(!f.reply.execution_result && f.reply.committed);
        const col_rel_t *out = session_rel_hash_lookup(f.session, "output");
        CHECK(out && out->nrows == 1 && out->ncols == 1
            && out->columns[0][0] == 4096);
    }
    if (require_success)
        CHECK(!f.reply.execution_result && f.reply.committed
            && trial.checkpoints == 4096 && trial.last_progress == 4095);
    else if (!trial.target_seen)
        CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT);

    entry_finish_original(&f);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && report.attempt_id == f.id && !report.cleanup_status);
    trial.consumed = report.consumed;
    trial.result = report.result;
    trial.stop_reason = report.stop_reason;
    trial.cleanup_status = report.cleanup_status;
    entry_destroy(&f);
    return trial;
}

static void
test_entry_generated_reduce_work_limit(void)
{
    const unsigned target = 1023;
    const entry_reduce_trial_t high = entry_reduce_trial(UINT64_C(1) << 30,
            true);
    uint64_t low = 1, upper = UINT64_C(1) << 30;
    while (low < upper) {
        uint64_t mid = low + (upper - low) / 2;
        entry_reduce_trial_t trial = entry_reduce_trial(mid, false);
        if (trial.target_seen) upper = mid;
        else low = mid + 1;
    }
    const uint64_t limit = low;
    const entry_reduce_trial_t below = entry_reduce_trial(limit - 1, false);
    const entry_reduce_trial_t boundary = entry_reduce_trial(limit, false);
    const entry_reduce_trial_t repeat = entry_reduce_trial(limit, false);
    CHECK(high.checkpoints == 4096 && high.last_progress == 4095
        && high.committed && high.result == 0);
    CHECK(!below.target_seen && below.last_progress == target - 1
        && below.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && below.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(boundary.target_seen && boundary.last_progress == target
        && boundary.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && boundary.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(repeat.target_seen && repeat.last_progress == target
        && repeat.result == boundary.result
        && repeat.consumed == boundary.consumed
        && repeat.stop_reason == boundary.stop_reason
        && repeat.cleanup_status == boundary.cleanup_status);

    int64_t rows[4096]; entry_map_fill_rows(rows, 4096);
    entry_fixture_t f; entry_init(&f, entry_generated_count_reduce, rows,
        4096, 1, limit);
    entry_complete(&f);
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !f.reply.committed && !session_find_rel(f.session, "output"));
    const uint64_t stopped_id = f.id;
    entry_finish_original(&f);
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_configure(f.operation, 0));
    CHECK(!wl_evaluation_control_begin(f.operation,
        f.session->base.operation_admission));
    f.id = wl_evaluation_control_active_attempt_id(f.operation);
    CHECK(f.id && f.id != stopped_id);
    f.previous.previous_valid = false;
    f.reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
    entry_complete(&f);
    CHECK(!f.reply.execution_result && f.reply.committed);
    const col_rel_t *out = session_rel_hash_lookup(f.session, "output");
    CHECK(out && out->nrows == 1 && out->ncols == 1
        && out->columns[0][0] == 4096);
    entry_compare(&f, entry_generated_count_reduce, rows, 4096, 1);
    entry_destroy(&f);
}

static void
entry_until(entry_fixture_t *f, wl_columnar_eval_serial_framed_phase_t phase)
{
    unsigned turns = 0;
    while (!entry_context(f) || entry_context(f)->phase != phase) {
        CHECK(++turns < 200000
            && f->reply.kind != WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL);
        CHECK(!entry_turn(f, false, 0));
    }
}

static void
test_entry_receipts(void)
{
    entry_fixture_t f; entry_init(&f, entry_simple, entry_rows, 4, 1, 0);
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(
        f.session->memory_governor);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    CHECK(entry_turn(&f, false, 383) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED
        && !f.session->cleanup_active &&
        wl_columnar_memory_reserved(g) == baseline);
    /* C384 succeeds, A's distinct384 refuses after actual controlled frame
     * allocation. The newly empty frame must close before this real return. */
    CHECK(entry_turn(&f, false, 767) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT
        && !f.session->cleanup_active && !f.session->cleanup_pending
        && wl_columnar_memory_reserved(g) == baseline);
    wl_evaluation_control_t *prior = f.previous.previous_funding;
    uint64_t prior_id = f.previous.previous_funding_id;
    CHECK(entry_turn(&f, true, 0) == ENOENT);
    CHECK(f.previous.previous_valid && f.previous.previous_funding == prior
        && f.previous.previous_funding_id == prior_id);
    CHECK(entry_turn(&f, false, 383) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.previous.previous_valid && f.previous.previous_funding == prior
        && f.previous.previous_funding_id == prior_id);
    CHECK(!entry_turn(&f, false, 0));
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
        && !f.previous.previous_valid && !f.session->cleanup_active);
    entry_until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    /* No remaining grant for composition after actual C384+A384. */
    CHECK(entry_turn(&f, false, 768) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(entry_context(&f)->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING);
    entry_outer_guards(&f);
    prior = f.previous.previous_funding;
    prior_id = f.previous.previous_funding_id;
    CHECK(!entry_turn(&f, true, 0));
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_STATUS
        && f.reply.status.phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && f.previous.previous_funding == prior
        && f.previous.previous_funding_id == prior_id);
    CHECK(entry_turn(&f, false, 767) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.previous.previous_valid && f.previous.previous_funding == prior
        && entry_context(&f)->pending_funding == prior);
    entry_complete(&f);
    CHECK(f.reply.committed && !f.reply.execution_result);
    entry_compare(&f, entry_simple, entry_rows, 4, 1); entry_destroy(&f);
}

static void
test_entry_bootstrap_failures(void)
{
    uint64_t frame_cost = 1726 + 224 *
        ((wl_columnar_eval_stack_test_frame_size() + 15) / 16);
    const uint64_t limits[] = {1, 191, 192, 767, 768, 895,
                               768 + frame_cost - 1, 768 + frame_cost,
                               768 + frame_cost + 75519};
    for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
        entry_fixture_t f;
        entry_init(&f, entry_simple, entry_rows, 4, 1, limits[i]);
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f.session->memory_governor));
        CHECK(entry_turn(&f, false, 0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(f.reply.kind ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT
            && !f.session->cleanup_active && !f.session->cleanup_pending
            && !f.session->cleanup_reserved_bytes);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                f.session->memory_governor)) == baseline);
        CHECK(!entry_turn(&f, false, 0));
        CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL
            && f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !f.reply.committed && !session_find_rel(f.session, "output"));
        entry_destroy(&f);
    }
    for (unsigned quota = 0; quota < 2; ++quota) {
        entry_fixture_t f; entry_init(&f, entry_simple, entry_rows, 4, 1, 0);
        wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(
            f.session->memory_governor);
        uint64_t baseline = wl_columnar_memory_reserved(g);
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        if (quota) CHECK(wl_columnar_memory_reserve_checked(g,
                atomic_load_explicit(&g->usable_bytes,
                memory_order_acquire) - baseline, &peer)
                == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        else wl_columnar_eval_stack_test_fail_allocation = 0;
        CHECK(entry_turn(&f, false, 0) == (quota ? ENOSPC : ENOMEM));
        wl_columnar_eval_stack_test_fail_allocation = -1;
        CHECK(f.reply.kind ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT
            && !f.session->cleanup_active && !f.session->cleanup_pending);
        CHECK(!entry_turn(&f, false, 0));
        CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL
            && f.reply.execution_result == (quota ? ENOSPC : ENOMEM)
            && !f.reply.committed);
        if (quota) CHECK(wl_columnar_memory_rollback(&peer));
        CHECK(wl_columnar_memory_reserved(g) == baseline);
        CHECK(wl_evaluation_control_finish(f.operation,
            f.session->base.operation_admission, f.reply.execution_result, 0)
            == (quota ? ENOSPC : ENOMEM));
        CHECK(!wl_evaluation_control_reset(f.operation));
        CHECK(!wl_evaluation_control_begin(f.operation,
            f.session->base.operation_admission));
        f.id = wl_evaluation_control_active_attempt_id(f.operation);
        f.previous.previous_valid = false;
        f.reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
        entry_complete(&f);
        CHECK(!f.reply.execution_result && f.reply.committed);
        entry_compare(&f, entry_simple, entry_rows, 4, 1); entry_destroy(&f);
    }
}

static void
test_entry_retained_publication(void)
{
    entry_fixture_t f; entry_init(&f, entry_simple, entry_rows, 4, 1, 0);
    entry_until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN);
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
    CHECK(ctx->input.rel && ctx->input.owned);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(ctx->input.rel, &reader));
    for (unsigned i = 0; i < 3; ++i) {
        CHECK(!entry_turn(&f, false, 0));
        CHECK(entry_context(&f) == ctx && ctx->phase ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN
            && ctx->cleanup_result == EBUSY
            && !session_rel_hash_lookup(f.session, "output"));
    }
    CHECK(!entry_turn(&f, true, 0) && f.reply.status.retained);
    CHECK(!col_rel_source_reader_release(&reader));
    unsigned turns = 0;
    while (!entry_context(&f)->committed) {
        CHECK(++turns < 200000 && !entry_turn(&f, false, 0));
    }
    col_rel_t *published = session_rel_hash_lookup(f.session, "output");
    CHECK(published && !ctx->candidate && ctx->publication.committed);
    entry_outer_guards(&f);
    wl_evaluation_control_request_cancel(f.operation);
    entry_outer_guards(&f);
    CHECK(entry_turn(&f, false, 383) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(entry_context(&f) == ctx && ctx->committed
        && session_rel_hash_lookup(f.session, "output") == published);
    CHECK(!entry_turn(&f, true, 0) && f.reply.status.committed);
    entry_complete(&f);
    CHECK(!f.reply.execution_result && f.reply.committed
        && session_rel_hash_lookup(f.session, "output") == published);
    entry_compare(&f, entry_simple, entry_rows, 4, 1);
    /* No post-cutoff original/HOST poll is invented. finish does not observe
     * a merely requested cancellation; the next attempt's charge does. */
    CHECK(!wl_evaluation_control_finish(f.operation,
        f.session->base.operation_admission, 0, 0));
    CHECK(!wl_evaluation_control_begin(f.operation,
        f.session->base.operation_admission));
    CHECK(wl_evaluation_control_charge(f.operation, 0)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(f.operation,
        f.session->base.operation_admission, 0, 0)
        == WL_EVALUATION_CONTROL_CANCELLED);
    entry_destroy(&f);
}

static entry_fixture_t *entry_fault_fixture;
static unsigned entry_stop_move;
static bool entry_turnover;

static void
entry_move_checkpoint(wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    entry_fixture_t *f = entry_fault_fixture;
    if (!f || ctx != entry_context(f) || ctx->committed
        || !ctx->publication.identity || !entry_stop_move
        || ctx->publication.journal[entry_stop_move - 1] != 2) return;
    entry_stop_move = 0;
    if (!entry_turnover) wl_evaluation_control_request_cancel(f->operation);
    else {
        CHECK(!wl_evaluation_control_finish(f->operation,
            f->session->base.operation_admission, 0, 0));
        CHECK(!wl_evaluation_control_reset(f->operation));
        CHECK(!wl_evaluation_control_begin(f->operation,
            f->session->base.operation_admission));
        CHECK(wl_evaluation_control_active_attempt_id(f->operation) != f->id);
    }
}

static void
test_entry_reverse(void)
{
    for (unsigned move = 1; move <= 4; ++move)
        for (unsigned turnover = 0; turnover < 2; ++turnover) {
            entry_fixture_t f;
            entry_init(&f, entry_simple, entry_rows, 4, 1, 0);
            col_rel_t **old = f.session->rels;
            uint32_t count = f.session->nrels;
            uint64_t baseline = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(
                    f.session->memory_governor));
            entry_fault_fixture = &f; entry_stop_move = move;
            entry_turnover = turnover;
            wl_columnar_eval_serial_test_framed_checkpoint =
                entry_move_checkpoint;
            entry_complete(&f);
            wl_columnar_eval_serial_test_framed_checkpoint = NULL;
            entry_fault_fixture = NULL;
            CHECK(!entry_stop_move && !f.reply.committed
                && f.reply.execution_result ==
                (turnover ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED)
                && f.session->rels == old && f.session->nrels == count
                && !session_rel_hash_lookup(f.session, "output")
                && wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(
                        f.session->memory_governor)) == baseline);
            if (turnover) {
                CHECK(!wl_evaluation_control_finish(f.operation,
                    f.session->base.operation_admission, 0, 0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.operation, &report)
                    && report.attempt_id != f.id && !report.consumed);
            } else entry_finish_original(&f);
            CHECK(!wl_evaluation_control_reset(f.operation));
            CHECK(!wl_evaluation_control_begin(f.operation,
                f.session->base.operation_admission));
            f.id = wl_evaluation_control_active_attempt_id(f.operation);
            f.previous.previous_valid = false;
            f.reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
            entry_complete(&f);
            CHECK(!f.reply.execution_result && f.reply.committed);
            entry_compare(&f, entry_simple, entry_rows, 4, 1);
            entry_destroy(&f);
        }
}

static void
test_entry_generated_operators(void)
{
    const struct {
        const char *source;
        wl_plan_op_type_t required;
        uint32_t rows;
    } cases[] = {
        {".decl input(x:int64)\n.decl output(y:int64)\n"
         "output(x+1) :- input(x), x>1.\n", WL_PLAN_OP_FILTER, 2},
        {".decl input(x:int64)\n.decl output(x:int64,y:int64)\n"
         "output(x,y) :- input(x), input(y).\n", WL_PLAN_OP_JOIN, 9},
        {".decl input(x:int64)\n.decl output(n:int64)\n"
         "output(count(x)) :- input(x).\n", WL_PLAN_OP_REDUCE, 1},
        {".decl input(x:int64)\n.decl output(x:int64)\n"
         "output(x) :- input(x).\noutput(x+1) :- input(x).\n",
         WL_PLAN_OP_CONCAT, 4},
        {".decl input(x:int64)\n.decl output(x:int64)\n"
         "output(x) :- input(x).\noutput(x+1) :- input(x).\n"
         "output(x+2) :- input(x).\n", WL_PLAN_OP_CONCAT, 5}
    };
    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        entry_fixture_t f; entry_init(&f, cases[i].source, entry_rows, 4, 1, 0);
        bool reached = false;
        for (uint32_t op = 0; op < f.relation->op_count; ++op)
            reached |= f.relation->ops[op].op == cases[i].required;
        CHECK(reached); entry_complete(&f);
        CHECK(f.reply.committed && !f.reply.execution_result);
        const col_rel_t *out = session_rel_hash_lookup(f.session, "output");
        CHECK(out && out->nrows == cases[i].rows);
        if (i == 0) CHECK(out->columns[0][0] == 3 && out->columns[0][1] == 4);
        if (i == 1) for (uint32_t r = 0; r < 9; ++r)
                CHECK(out->columns[0][r] == (int64_t)(r / 3 + 1)
                    && out->columns[1][r] == (int64_t)(r % 3 + 1));
        if (i == 2) CHECK(out->columns[0][0] == 4);
        if (i >= 3) for (uint32_t r = 0; r < out->nrows; ++r)
                CHECK(out->columns[0][r] == (int64_t)r + 1);
        entry_compare(&f, cases[i].source, entry_rows, 4, 1); entry_destroy(&f);
    }
    test_entry_generated_map_work_limit();
    test_entry_generated_reduce_work_limit();
}

typedef struct {
    wl_evaluation_control_t *operation;
    uint64_t cancel_after;
    unsigned cancel_phase;
    uint64_t phase17_hits, phase30_hits, phase28_hits, phase46_hits;
    uint64_t last_progress;
    bool cancelled;
} entry_join_probe_t;

static entry_join_probe_t *entry_join_probe;

static void
entry_join_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    entry_join_probe_t *probe = entry_join_probe;
    if (!probe || control != probe->operation) return;
    if (phase == 17) ++probe->phase17_hits;
    if (phase == 30) ++probe->phase30_hits;
    if (phase == 28) ++probe->phase28_hits;
    if (phase == 46) ++probe->phase46_hits;
    probe->last_progress = progress;
    uint64_t hits = phase == probe->cancel_phase
        ? (phase == 17 ? probe->phase17_hits
            : phase == 30 ? probe->phase30_hits
            : phase == 28 ? probe->phase28_hits : 0) : 0;
    if (probe->cancel_after && hits == probe->cancel_after) {
        probe->cancelled = true;
        (void)wl_evaluation_control_request_cancel(control);
    }
}

static const char entry_filter_product[] =
    ".decl input(x:int64)\n"
    ".decl right(y:int64,flag:int64)\n"
    ".decl output(x:int64,y:int64)\n"
    "output(x,y) :- input(x), right(y,1).\n";

static const char entry_filter_keyed[] =
    ".decl input(k:int64)\n"
    ".decl right(k:int64,flag:int64)\n"
    ".decl output(k:int64)\n"
    "output(k) :- input(k), right(k,1).\n";

static const wl_plan_op_t *
entry_assert_generated_join(entry_fixture_t *f, const char *right_name,
    uint32_t key_count)
{
    CHECK(f->relation->op_count >= 2
        && f->relation->ops[0].op == WL_PLAN_OP_VARIABLE
        && f->relation->ops[f->relation->op_count - 1].op ==
        WL_PLAN_OP_CONSOLIDATE);
    const wl_plan_op_t *found = NULL;
    for (uint32_t i = 0; i < f->relation->op_count; ++i) {
        const wl_plan_op_t *op = &f->relation->ops[i];
        if (op->op != WL_PLAN_OP_JOIN || !op->right_relation
            || strcmp(op->right_relation, right_name)) continue;
        CHECK(!found);
        found = op;
    }
    CHECK(found && found->key_count == key_count
        && found->right_filter_expr.data
        && found->right_filter_expr.size);
    return found;
}

static void
entry_large_inputs(int64_t input_rows[64], int64_t right_rows[513 * 2],
    bool keyed)
{
    for (uint32_t i = 0; i < 64; ++i)
        input_rows[i] = keyed ? (int64_t)(i + 1) * 1024 : 100 + i;
    for (uint32_t i = 0; i < 513; ++i) {
        right_rows[i * 2] = keyed
            ? (int64_t)(1000 + i / 2) * 1024 : 2000 + i;
        right_rows[i * 2 + 1] = (i & 1u) ? 0 : 1;
    }
}

static void
entry_assert_filtered_selection(const entry_join_probe_t *probe,
    uint32_t expected_rows)
{
    CHECK(probe->phase46_hits == expected_rows);
}

static void
test_entry_generated_right_filter_large(void)
{
    int64_t input_rows[64], right_rows[513 * 2];
    entry_large_inputs(input_rows, right_rows, false);
    const entry_input_t inputs[] = {
        {"input", input_rows, 64, 1},
        {"right", right_rows, 513, 2}
    };
    const uint32_t products = 64 * 257;
    entry_fixture_t f;
    entry_init_inputs(&f, entry_filter_product, inputs, 2, 0);
    const wl_plan_op_t *join = entry_assert_generated_join(&f, "right", 0);
    CHECK(join->right_filter_expr.size > 0 && f.session->memory_governor);
    entry_join_probe_t probe = {.operation = f.operation};
    entry_join_probe = &probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&f);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(!f.reply.execution_result && f.reply.committed
        && probe.phase17_hits == products);
    entry_assert_filtered_selection(&probe, 257);
    const col_rel_t *out = session_rel_hash_lookup(f.session, "output");
    CHECK(out && out->nrows == products && out->ncols == 2);
    uint32_t row = 0;
    for (uint32_t x = 0; x < 64; ++x)
        for (uint32_t y = 0; y < 257; ++y, ++row)
            CHECK(out->columns[0][row] == input_rows[x]
                && out->columns[1][row] == right_rows[(2 * y) * 2]);
    entry_compare_inputs(&f, entry_filter_product, inputs, 2);
    entry_destroy(&f);
}

static void
entry_prepare_keyed_collision(int64_t input_rows[64],
    int64_t right_rows[513 * 2])
{
    entry_large_inputs(input_rows, right_rows, true);
}

static void
entry_assert_unpublished_and_unchanged(entry_fixture_t *f,
    col_rel_t **old_rels, uint32_t old_nrels, col_rel_t *input,
    col_rel_t *right, const int64_t *input_rows, const int64_t *right_rows,
    uint64_t reserved)
{
    bool inputs_unchanged = input->nrows == 64 && right->nrows == 513
        && right->ncols == 2
        && !memcmp(input->columns[0], input_rows, 64 * sizeof(int64_t));
    for (uint32_t r = 0; inputs_unchanged && r < 513; ++r)
        inputs_unchanged = right->columns[0][r] == right_rows[2 * r]
            && right->columns[1][r] == right_rows[2 * r + 1];
    CHECK(!f->reply.committed
        && !session_rel_hash_lookup(f->session, "output")
        && f->session->rels == old_rels && f->session->nrels == old_nrels
        && session_find_rel(f->session, "input") == input
        && session_find_rel(f->session, "right") == right
        && inputs_unchanged
        && (!f->governor || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->governor)) == reserved));
    CHECK(!f->session->cleanup_active && !f->session->cleanup_pending
        && !f->session->cleanup_reserved_bytes);
}

static void
entry_retry_after_finished_stop(entry_fixture_t *f, const char *source,
    const entry_input_t *inputs, uint32_t input_count)
{
    CHECK(!wl_evaluation_control_reset(f->operation));
    CHECK(!wl_evaluation_control_configure(f->operation, 0));
    CHECK(!wl_evaluation_control_begin(f->operation,
        f->session->base.operation_admission));
    f->id = wl_evaluation_control_active_attempt_id(f->operation);
    CHECK(f->id);
    f->previous.previous_valid = false;
    memset(&f->reply, 0, sizeof(f->reply));
    f->reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
    entry_complete(f);
    CHECK(!f->reply.execution_result && f->reply.committed);
    entry_compare_inputs(f, source, inputs, input_count);
}

static void
entry_retry_after_stop(entry_fixture_t *f, const char *source,
    const entry_input_t *inputs, uint32_t input_count)
{
    entry_finish_original(f);
    entry_retry_after_finished_stop(f, source, inputs, input_count);
}

static void
test_entry_generated_right_filter_collisions_and_stop(void)
{
    int64_t input_rows[64], right_rows[513 * 2];
    entry_prepare_keyed_collision(input_rows, right_rows);
    const entry_input_t inputs[] = {
        {"input", input_rows, 64, 1},
        {"right", right_rows, 513, 2}
    };
    const uint64_t pairs = 64u * 257u;

    entry_fixture_t complete;
    entry_init_inputs(&complete, entry_filter_keyed, inputs, 2, 0);
    const wl_plan_op_t *join = entry_assert_generated_join(&complete, "right",
            1);
    CHECK(join->key_count == 1 && complete.session->memory_governor);
    entry_join_probe_t full_probe = {.operation = complete.operation};
    entry_join_probe = &full_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&complete);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(!complete.reply.execution_result && complete.reply.committed
        && full_probe.phase17_hits == pairs
        && full_probe.phase30_hits == pairs);
    entry_assert_filtered_selection(&full_probe, 257);
    const col_rel_t *empty = session_rel_hash_lookup(complete.session,
            "output");
    CHECK(empty && !empty->nrows);
    entry_compare_inputs(&complete, entry_filter_keyed, inputs, 2);
    entry_finish_original(&complete);
    wl_evaluation_control_report_t complete_report;
    CHECK(!wl_evaluation_control_report(complete.operation, &complete_report)
        && complete_report.result == 0 && complete_report.consumed > 0);
    uint64_t measured_total = complete_report.consumed;
    entry_destroy(&complete);

    entry_fixture_t prefix_anchor;
    col_rel_t **prefix_rels;
    uint32_t prefix_nrels;
    col_rel_t *prefix_input, *prefix_right;
    uint64_t prefix_reserved;
    entry_init_inputs(&prefix_anchor, entry_filter_keyed, inputs, 2, 0);
    (void)entry_assert_generated_join(&prefix_anchor, "right", 1);
    prefix_rels = prefix_anchor.session->rels;
    prefix_nrels = prefix_anchor.session->nrels;
    prefix_input = session_find_rel(prefix_anchor.session, "input");
    prefix_right = session_find_rel(prefix_anchor.session, "right");
    prefix_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(prefix_anchor.governor));
    entry_join_probe_t anchor_probe = {
        .operation = prefix_anchor.operation,
        .cancel_after = 257,
        .cancel_phase = 28
    };
    entry_join_probe = &anchor_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&prefix_anchor);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(anchor_probe.cancelled && anchor_probe.phase28_hits == 257
        && !anchor_probe.phase17_hits
        && prefix_anchor.reply.execution_result ==
        WL_EVALUATION_CONTROL_CANCELLED);
    entry_assert_unpublished_and_unchanged(&prefix_anchor, prefix_rels,
        prefix_nrels, prefix_input, prefix_right, input_rows, right_rows,
        prefix_reserved);
    entry_finish_original(&prefix_anchor);
    wl_evaluation_control_report_t anchor_report;
    CHECK(!wl_evaluation_control_report(prefix_anchor.operation,
        &anchor_report)
        && anchor_report.result == WL_EVALUATION_CONTROL_CANCELLED
        && anchor_report.consumed > 0);
    uint64_t measured_k28 = anchor_report.consumed;
    entry_retry_after_finished_stop(&prefix_anchor, entry_filter_keyed,
        inputs, 2);
    entry_destroy(&prefix_anchor);

    /* To candidate 128, the source charges 576 initially, then 448 for
     * each additional candidate: parent WORK, match key CELL, phase17 CELL. */
    const uint64_t local_prefix = 576 + 127 * 448;
    entry_fixture_t prefix_short;
    entry_init_inputs(&prefix_short, entry_filter_keyed, inputs, 2,
        measured_k28 + local_prefix - 1);
    (void)entry_assert_generated_join(&prefix_short, "right", 1);
    prefix_rels = prefix_short.session->rels;
    prefix_nrels = prefix_short.session->nrels;
    prefix_input = session_find_rel(prefix_short.session, "input");
    prefix_right = session_find_rel(prefix_short.session, "right");
    prefix_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(prefix_short.governor));
    entry_join_probe_t short_probe = {.operation = prefix_short.operation};
    entry_join_probe = &short_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&prefix_short);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(prefix_short.reply.execution_result ==
        WL_EVALUATION_CONTROL_WORK_LIMIT && short_probe.phase17_hits == 127
        && short_probe.phase30_hits == 127
        && !prefix_short.reply.committed
        && !session_rel_hash_lookup(prefix_short.session, "output"));
    entry_assert_unpublished_and_unchanged(&prefix_short, prefix_rels,
        prefix_nrels, prefix_input, prefix_right, input_rows, right_rows,
        prefix_reserved);
    entry_finish_original(&prefix_short);
    wl_evaluation_control_report_t short_report;
    CHECK(!wl_evaluation_control_report(prefix_short.operation,
        &short_report)
        && short_report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    entry_retry_after_finished_stop(&prefix_short, entry_filter_keyed,
        inputs, 2);
    entry_destroy(&prefix_short);

    entry_fixture_t prefix_exact;
    entry_init_inputs(&prefix_exact, entry_filter_keyed, inputs, 2,
        measured_k28 + local_prefix);
    (void)entry_assert_generated_join(&prefix_exact, "right", 1);
    prefix_rels = prefix_exact.session->rels;
    prefix_nrels = prefix_exact.session->nrels;
    prefix_input = session_find_rel(prefix_exact.session, "input");
    prefix_right = session_find_rel(prefix_exact.session, "right");
    prefix_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(prefix_exact.governor));
    entry_join_probe_t exact_probe = {
        .operation = prefix_exact.operation,
        .cancel_after = 128,
        .cancel_phase = 17
    };
    entry_join_probe = &exact_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&prefix_exact);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(exact_probe.cancelled && exact_probe.phase17_hits == 128
        && exact_probe.phase30_hits == 127
        && prefix_exact.reply.execution_result ==
        WL_EVALUATION_CONTROL_CANCELLED
        && !prefix_exact.reply.committed);
    entry_assert_unpublished_and_unchanged(&prefix_exact, prefix_rels,
        prefix_nrels, prefix_input, prefix_right, input_rows, right_rows,
        prefix_reserved);
    entry_finish_original(&prefix_exact);
    wl_evaluation_control_report_t prefix_report;
    CHECK(!wl_evaluation_control_report(prefix_exact.operation,
        &prefix_report)
        && prefix_report.result == WL_EVALUATION_CONTROL_CANCELLED
        && prefix_report.consumed == measured_k28 + local_prefix);
    entry_retry_after_finished_stop(&prefix_exact, entry_filter_keyed,
        inputs, 2);
    entry_destroy(&prefix_exact);

    entry_fixture_t exact;
    entry_init_inputs(&exact, entry_filter_keyed, inputs, 2, measured_total);
    entry_complete(&exact);
    CHECK(!exact.reply.execution_result && exact.reply.committed);
    entry_finish_original(&exact);
    wl_evaluation_control_report_t exact_report;
    CHECK(!wl_evaluation_control_report(exact.operation, &exact_report)
        && exact_report.consumed == measured_total);
    entry_destroy(&exact);

    entry_fixture_t short_one;
    entry_init_inputs(&short_one, entry_filter_keyed, inputs, 2,
        measured_total - 1);
    join = entry_assert_generated_join(&short_one, "right", 1);
    CHECK(join->right_filter_expr.data);
    col_rel_t **old_rels = short_one.session->rels;
    uint32_t old_nrels = short_one.session->nrels;
    col_rel_t *old_input = session_find_rel(short_one.session, "input");
    col_rel_t *old_right = session_find_rel(short_one.session, "right");
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(short_one.governor));
    entry_complete(&short_one);
    CHECK(short_one.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    entry_assert_unpublished_and_unchanged(&short_one, old_rels, old_nrels,
        old_input, old_right, input_rows, right_rows, reserved);
    entry_retry_after_stop(&short_one, entry_filter_keyed, inputs, 2);
    entry_destroy(&short_one);

    entry_fixture_t cancelled;
    entry_init_inputs(&cancelled, entry_filter_keyed, inputs, 2, 0);
    (void)entry_assert_generated_join(&cancelled, "right", 1);
    old_rels = cancelled.session->rels;
    old_nrels = cancelled.session->nrels;
    old_input = session_find_rel(cancelled.session, "input");
    old_right = session_find_rel(cancelled.session, "right");
    reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(cancelled.governor));
    entry_join_probe_t cancel_probe = {
        .operation = cancelled.operation,
        .cancel_after = 777,
        .cancel_phase = 30
    };
    entry_join_probe = &cancel_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&cancelled);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(cancel_probe.cancelled && cancel_probe.phase30_hits == 777
        && cancelled.reply.execution_result ==
        WL_EVALUATION_CONTROL_CANCELLED);
    entry_assert_unpublished_and_unchanged(&cancelled, old_rels, old_nrels,
        old_input, old_right, input_rows, right_rows, reserved);
    entry_retry_after_stop(&cancelled, entry_filter_keyed, inputs, 2);
    entry_destroy(&cancelled);

    int64_t product_input[64], product_right[513 * 2];
    entry_large_inputs(product_input, product_right, false);
    const entry_input_t product_inputs[] = {
        {"input", product_input, 64, 1},
        {"right", product_right, 513, 2}
    };
    entry_fixture_t product;
    entry_init_inputs(&product, entry_filter_product, product_inputs, 2, 0);
    (void)entry_assert_generated_join(&product, "right", 0);
    entry_join_probe_t pair_cancel = {
        .operation = product.operation,
        .cancel_after = 8192,
        .cancel_phase = 17
    };
    entry_join_probe = &pair_cancel;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&product);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(pair_cancel.cancelled && pair_cancel.phase17_hits == 8192
        && product.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !product.reply.committed
        && !session_rel_hash_lookup(product.session, "output"));
    entry_retry_after_stop(&product, entry_filter_product, product_inputs, 2);
    entry_destroy(&product);
}

static void
test_entry_generated_right_filter_product_work_boundary(void)
{
    int64_t input_rows[64], right_rows[513 * 2];
    entry_large_inputs(input_rows, right_rows, false);
    const entry_input_t inputs[] = {
        {"input", input_rows, 64, 1},
        {"right", right_rows, 513, 2}
    };

    /* Capture the actual original-control allowance through the exact
     * candidate checkpoint. Product output-cell and B2 charges vary by row,
     * so this is a fixture-specific empirical boundary, not a source tariff. */
    entry_fixture_t anchor;
    entry_init_inputs(&anchor, entry_filter_product, inputs, 2, 0);
    (void)entry_assert_generated_join(&anchor, "right", 0);
    col_rel_t **old_rels = anchor.session->rels;
    uint32_t old_nrels = anchor.session->nrels;
    col_rel_t *old_input = session_find_rel(anchor.session, "input");
    col_rel_t *old_right = session_find_rel(anchor.session, "right");
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(anchor.governor));
    entry_join_probe_t anchor_probe = {
        .operation = anchor.operation,
        .cancel_after = 128,
        .cancel_phase = 17
    };
    entry_join_probe = &anchor_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&anchor);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(anchor_probe.cancelled && anchor_probe.phase46_hits == 257
        && anchor_probe.phase17_hits == 128
        && anchor.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED);
    entry_assert_unpublished_and_unchanged(&anchor, old_rels, old_nrels,
        old_input, old_right, input_rows, right_rows, reserved);
    entry_finish_original(&anchor);
    wl_evaluation_control_report_t anchor_report;
    CHECK(!wl_evaluation_control_report(anchor.operation, &anchor_report)
        && anchor_report.result == WL_EVALUATION_CONTROL_CANCELLED
        && anchor_report.consumed > 0);
    uint64_t checkpoint_allowance = anchor_report.consumed;
    entry_retry_after_finished_stop(&anchor, entry_filter_product, inputs, 2);
    entry_destroy(&anchor);

    /* The one-unit-short charge cannot admit the 128-unit candidate
     * checkpoint charge, so the callback sees exactly 127 candidates. */
    entry_fixture_t short_one;
    entry_init_inputs(&short_one, entry_filter_product, inputs, 2,
        checkpoint_allowance - 1);
    (void)entry_assert_generated_join(&short_one, "right", 0);
    old_rels = short_one.session->rels;
    old_nrels = short_one.session->nrels;
    old_input = session_find_rel(short_one.session, "input");
    old_right = session_find_rel(short_one.session, "right");
    reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(short_one.governor));
    entry_join_probe_t short_probe = {.operation = short_one.operation};
    entry_join_probe = &short_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&short_one);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(short_one.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && short_probe.phase46_hits == 257
        && short_probe.phase17_hits == 127 && !short_one.reply.committed);
    entry_assert_unpublished_and_unchanged(&short_one, old_rels, old_nrels,
        old_input, old_right, input_rows, right_rows, reserved);
    entry_finish_original(&short_one);
    wl_evaluation_control_report_t short_report;
    CHECK(!wl_evaluation_control_report(short_one.operation, &short_report)
        && short_report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    entry_retry_after_finished_stop(&short_one, entry_filter_product, inputs,
        2);
    entry_destroy(&short_one);

    /* At the measured allowance the same callback is reachable exactly;
     * cancellation makes the cutoff deterministic before candidate output. */
    entry_fixture_t exact;
    entry_init_inputs(&exact, entry_filter_product, inputs, 2,
        checkpoint_allowance);
    (void)entry_assert_generated_join(&exact, "right", 0);
    entry_join_probe_t exact_probe = {
        .operation = exact.operation,
        .cancel_after = 128,
        .cancel_phase = 17
    };
    entry_join_probe = &exact_probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    entry_complete(&exact);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(exact_probe.cancelled && exact_probe.phase46_hits == 257
        && exact_probe.phase17_hits == 128
        && exact.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !exact.reply.committed);
    entry_finish_original(&exact);
    wl_evaluation_control_report_t exact_report;
    CHECK(!wl_evaluation_control_report(exact.operation, &exact_report)
        && exact_report.result == WL_EVALUATION_CONTROL_CANCELLED
        && exact_report.consumed == checkpoint_allowance);
    entry_retry_after_finished_stop(&exact, entry_filter_product, inputs, 2);
    entry_destroy(&exact);
}

static wl_columnar_eval_serial_framed_host_request_t
entry_request(entry_fixture_t *f, grant_t *grant)
{
    wl_columnar_eval_serial_framed_host_request_t request = f->previous;
    request.funding = grant->control; request.funding_id = grant->id;
    request.operation = f->operation; request.operation_id = f->id;
    request.action = WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE;
    request.reply = &f->reply;
    return request;
}

typedef struct {
    entry_fixture_t *fixture;
    grant_t *grant;
    wl_columnar_eval_serial_framed_host_request_t request;
    int protocol;
} entry_foreign_thread_t;

static void *
entry_foreign_thread(void *argument)
{
    entry_foreign_thread_t *call = argument;
    call->protocol = entry_host(call->fixture, &call->request, call->grant);
    return NULL;
}

static void
test_entry_authority(void)
{
    entry_fixture_t f; entry_init(&f, entry_simple, entry_rows, 4, 1, 0);
    uint64_t cold = wl_test_evaluation_guard_state(f.session);
    for (unsigned bad = 0; bad < 4; ++bad) {
        grant_t *fresh = entry_grant(&f, 0);
        wl_columnar_eval_serial_framed_host_request_t invalid =
            entry_request(&f, fresh);
        if (bad == 0) ++invalid.funding_id;
        if (bad == 1) invalid.funding_id = 0;
        if (bad == 2) {
            invalid.funding = f.operation; invalid.funding_id = f.id;
        }
        if (bad == 3) {
            CHECK(fresh->id > 1); --invalid.funding_id;
        }
        CHECK(entry_host(&f, &invalid, fresh) == EINVAL
            && f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED
            && !f.session->cleanup_active
            && wl_test_evaluation_guard_state(f.session) == cold);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_active_attempt_id(fresh->control)
            && !wl_evaluation_control_report(fresh->control, &report)
            && report.attempt_id == fresh->id && !report.consumed
            && !report.result && !report.cleanup_status &&
            !report.execution_status);
    }
    CHECK(!entry_turn(&f, false, 0));
    uint64_t before = wl_test_evaluation_guard_state(f.session);
    grant_t *grant = entry_grant(&f, 0);
    wl_columnar_eval_serial_framed_host_request_t request = entry_request(&f,
            grant);
    ++request.operation_id;
    CHECK(entry_host(&f, &request, grant) == EINVAL
        && wl_test_evaluation_guard_state(f.session) == before);
    grant = entry_grant(&f, 0); request = entry_request(&f, grant);
    wl_plan_relation_t copy = *f.relation;
    CHECK(wl_columnar_eval_serial_framed_relation(&copy, f.session, false,
        &request) == EINVAL);
    CHECK(!wl_evaluation_control_finish(grant->control, &grant->owner, 0, 0));
    CHECK(wl_test_evaluation_guard_state(f.session) == before);
    grant = entry_grant(&f, 0);
    entry_foreign_thread_t call = {.fixture = &f, .grant = grant,
                                   .request = entry_request(&f, grant)};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, entry_foreign_thread, &call));
    CHECK(!wl_thread_join(&thread));
    CHECK(call.protocol == EBUSY &&
        wl_test_evaluation_guard_state(f.session) == before);

    grant = entry_grant(&f, 0); request = entry_request(&f, grant);
    wl_columnar_eval_stack_cleanup_frame_t *foreign = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(f.session, &foreign));
    uint64_t foreign_before = wl_test_evaluation_guard_state(f.session);
    CHECK(entry_host(&f, &request, grant) == EBUSY
        && wl_test_evaluation_guard_state(f.session) == foreign_before
        && f.session->cleanup_active == foreign);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&foreign));
    CHECK(wl_test_evaluation_guard_state(f.session) == before);

    entry_until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING);
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
    wl_evaluation_control_t *pending = ctx->pending_funding;
    uint64_t pending_id = ctx->pending_funding_id;
    grant = entry_grant(&f, 0); request = entry_request(&f, grant);
    ++request.previous_funding_id;
    before = wl_test_evaluation_guard_state(f.session);
    CHECK(entry_host(&f, &request, grant) == EINVAL
        && ctx->pending_funding == pending &&
        ctx->pending_funding_id == pending_id
        && wl_test_evaluation_guard_state(f.session) == before);
    entry_complete(&f); CHECK(!f.reply.execution_result && f.reply.committed);
    entry_compare(&f, entry_simple, entry_rows, 4, 1); entry_destroy(&f);
}

static void
test_entry_unsupported_generated(void)
{
    static const char source[] =
        ".decl input(x:int64)\n.decl a(x:int64)\n.decl b(x:int64)\n"
        ".decl output(x:int64)\noutput(x) :- input(x), a(x), b(x).\n";
    entry_fixture_t f; entry_init(&f, source, entry_rows, 4, 1, 0);
    bool lftj = false;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        lftj |= f.relation->ops[i].op == WL_PLAN_OP_LFTJ;
    CHECK(lftj);
    uint64_t before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.session->memory_governor));
    entry_complete(&f);
    CHECK(f.reply.execution_result == ENOTSUP && !f.reply.committed
        && !session_rel_hash_lookup(f.session, "output")
        && before == wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f.session->memory_governor)));
    entry_destroy(&f);
}

static const char entry_filter_large[] =
    ".decl input(x:int64)\n"
    ".decl output(y:int64)\n"
    "output(x+1) :- input(x), x>1.\n";

typedef struct {
    wl_evaluation_control_t *operation;
    uint64_t target_row, phase13_hits;
    bool reached_target;
} entry_filter_probe_t;

static entry_filter_probe_t *entry_filter_probe;

static void
entry_filter_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    entry_filter_probe_t *probe = entry_filter_probe;
    if (!probe || control != probe->operation || phase != 13) return;
    ++probe->phase13_hits;
    if (progress == probe->target_row) probe->reached_target = true;
}

static const wl_plan_op_t *
entry_assert_generated_filter(entry_fixture_t *f)
{
    const wl_plan_op_t *found = NULL;
    for (uint32_t i = 0; i < f->relation->op_count; ++i) {
        const wl_plan_op_t *op = &f->relation->ops[i];
        if (op->op != WL_PLAN_OP_FILTER) continue;
        CHECK(!found);
        found = op;
    }
    CHECK(found && found->filter_expr.data && found->filter_expr.size);
    return found;
}

typedef struct {
    int result;
    uint64_t consumed, phase13_hits;
    bool reached_target;
} entry_filter_trial_t;

static entry_filter_trial_t
entry_filter_run_trial(const int64_t *rows, uint32_t count, uint64_t limit,
    uint64_t target_row)
{
    entry_fixture_t f;
    entry_init(&f, entry_filter_large, rows, count, 1, limit);
    (void)entry_assert_generated_filter(&f);
    entry_filter_probe_t probe = {
        .operation = f.operation, .target_row = target_row
    };
    entry_filter_probe = &probe;
    wl_columnar_filter_test_attempt_checkpoint = entry_filter_checkpoint;
    entry_complete(&f);
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    entry_filter_probe = NULL;
    CHECK(!f.reply.execution_result || f.reply.execution_result ==
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    entry_finish_original(&f);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && report.result == f.reply.execution_result
        && !report.cleanup_status);
    entry_filter_trial_t trial = {
        .result = f.reply.execution_result,
        .consumed = report.consumed,
        .phase13_hits = probe.phase13_hits,
        .reached_target = probe.reached_target
    };
    entry_destroy(&f);
    return trial;
}

static void
entry_retry_large_filter(entry_fixture_t *f, const int64_t *rows,
    uint32_t count)
{
    CHECK(!wl_evaluation_control_reset(f->operation));
    CHECK(!wl_evaluation_control_configure(f->operation, 0));
    CHECK(!wl_evaluation_control_begin(f->operation,
        f->session->base.operation_admission));
    f->id = wl_evaluation_control_active_attempt_id(f->operation);
    CHECK(f->id);
    f->previous.previous_valid = false;
    memset(&f->reply, 0, sizeof(f->reply));
    f->reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
    entry_complete(f);
    CHECK(!f->reply.execution_result && f->reply.committed);
    entry_compare(f, entry_filter_large, rows, count, 1);
    const col_rel_t *out = session_rel_hash_lookup(f->session, "output");
    CHECK(out && out->nrows == count - 1 && out->ncols == 1);
    for (uint32_t i = 0; i < out->nrows; ++i)
        CHECK(out->columns[0][i] == (int64_t)i + 3);
}

static void
test_entry_generated_filter_work_boundary(void)
{
    const uint32_t row_count = 4096;
    const uint64_t target_row = 1024;
    int64_t *rows = malloc((size_t)row_count * sizeof(*rows));
    CHECK(rows);
    for (uint32_t i = 0; i < row_count; ++i) rows[i] = (int64_t)i + 1;

    /* Search fresh operations for the first allowance that reaches the
     * known-true target row after predicate evaluation. This observes the
     * actual configured limit without turning cleanup-inclusive report totals
     * into a phase-local tariff. */
    uint64_t high = 256;
    entry_filter_trial_t trial = entry_filter_run_trial(rows, row_count, high,
            target_row);
    while (!trial.reached_target) {
        CHECK(high <= UINT64_MAX / 2);
        high *= 2;
        trial = entry_filter_run_trial(rows, row_count, high, target_row);
    }
    uint64_t low = 1;
    while (low < high) {
        uint64_t mid = low + (high - low) / 2;
        trial = entry_filter_run_trial(rows, row_count, mid, target_row);
        if (trial.reached_target) high = mid;
        else low = mid + 1;
    }
    const uint64_t limit = low;

    entry_filter_trial_t short_trial = entry_filter_run_trial(rows,
            row_count, limit - 1, target_row);
    CHECK(short_trial.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !short_trial.reached_target);
    entry_filter_trial_t exact_trial = entry_filter_run_trial(rows,
            row_count, limit, target_row);
    CHECK(exact_trial.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && exact_trial.reached_target
        && exact_trial.phase13_hits == target_row + 1);
    entry_filter_trial_t repeated_trial = entry_filter_run_trial(rows,
            row_count, limit, target_row);
    CHECK(repeated_trial.result == exact_trial.result
        && repeated_trial.consumed == exact_trial.consumed
        && repeated_trial.phase13_hits == exact_trial.phase13_hits
        && repeated_trial.reached_target);

    entry_fixture_t retry;
    entry_init(&retry, entry_filter_large, rows, row_count, 1, limit);
    (void)entry_assert_generated_filter(&retry);
    col_rel_t **old_rels = retry.session->rels;
    uint32_t old_nrels = retry.session->nrels;
    col_rel_t *old_input = session_find_rel(retry.session, "input");
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(retry.governor));
    entry_filter_probe_t probe = {
        .operation = retry.operation, .target_row = target_row
    };
    entry_filter_probe = &probe;
    wl_columnar_filter_test_attempt_checkpoint = entry_filter_checkpoint;
    entry_complete(&retry);
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    entry_filter_probe = NULL;
    CHECK(retry.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && probe.reached_target && probe.phase13_hits == target_row + 1
        && !retry.reply.committed
        && !session_rel_hash_lookup(retry.session, "output")
        && retry.session->rels == old_rels && retry.session->nrels == old_nrels
        && session_find_rel(retry.session, "input") == old_input
        && old_input->nrows == row_count
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(retry.governor)) == reserved
        && !retry.session->cleanup_active && !retry.session->cleanup_pending
        && !retry.session->cleanup_reserved_bytes);
    for (uint32_t i = 0; i < row_count; ++i)
        CHECK(old_input->columns[0][i] == rows[i]);
    entry_finish_original(&retry);
    wl_evaluation_control_report_t retry_report;
    CHECK(!wl_evaluation_control_report(retry.operation, &retry_report)
        && retry_report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && retry_report.consumed == exact_trial.consumed);
    entry_retry_large_filter(&retry, rows, row_count);
    entry_destroy(&retry);
    free(rows);
}

int
main(void)
{
    test_entry_shapes(); test_entry_generated_operators();
    test_entry_generated_right_filter_large();
    test_entry_generated_right_filter_collisions_and_stop();
    test_entry_generated_right_filter_product_work_boundary();
    test_entry_receipts(); test_entry_bootstrap_failures();
    test_entry_retained_publication(); test_entry_reverse();
    test_entry_authority(); test_entry_unsupported_generated();
    test_entry_generated_filter_work_boundary();

    test_shapes(); test_generated_union(); test_refusal_and_identity();
    test_failures(); test_retained_and_consume_refusal();
    test_growth_quota_and_retry(); test_bootstrap_limits();
    test_publication_shapes(); test_publication_reverse();
    test_publication_failures();
    puts("evaluation_serial: real result bridge, publication and drain passed");
    return 0;
}
