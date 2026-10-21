#include "evaluation_guard_fixture.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
/* #1821 real backend fresh-head result bridge and retained ownership. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
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
    wl_columnar_eval_cleanup_receipt_key_t receipt_key;
    wl_evaluation_control_t *operation;
    uint64_t id, operation_identity;
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
    f->operation_identity = f->id + 1000;
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
    CHECK(!wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            &f->session, &f->frame, NULL, f->id, f->operation_identity,
            &f->receipt_key));
    CHECK(!wl_columnar_eval_stack_framed_context(f->frame, &f->session,
        &f->ctx));
}

static int
host_begin(fixture_t *f, const wl_plan_relation_t *plan, grant_t *grant)
{
    int rc = wl_columnar_eval_serial_framed_attempt_begin(f->frame,
            &f->session, plan, f->operation, f->id, f->operation_identity,
            grant->control, grant->id);
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
    wl_columnar_eval_cleanup_receipt_t receipt;
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(&f->session,
        &f->receipt_key, &receipt));
    CHECK(receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        && receipt.result == result && receipt.committed == committed);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_ack(&f->session,
        &f->receipt_key));
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
    CHECK(!wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            &f.session, &f.frame, NULL, f.id, f.operation_identity,
            &f.receipt_key));
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
            CHECK(!wl_columnar_eval_stack_cleanup_receipt_cancel_begin(
                    f.frame, &f.receipt_key));
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

static void
test_cleanup_receipt_attempt_identity(void)
{
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t plan = {.name = "output", .ops = ops, .op_count = 2};
    fixture_t f;
    fixture_init(&f, 1, 2, false, false, 0);
    grant_t *grant = grant_begin(&f, 0);
    CHECK(wl_columnar_eval_serial_framed_attempt_begin(f.frame, &f.session,
        &plan, f.operation, f.id + 1, f.operation_identity, grant->control,
        grant->id) == EINVAL);
    CHECK(wl_columnar_eval_serial_framed_attempt_begin(f.frame, &f.session,
        &plan, f.operation, f.id, f.operation_identity + 1,
        grant->control, grant->id) == EINVAL);
    grant->finish_status = wl_evaluation_control_finish(grant->control,
            &grant->owner, 0, 0);
    CHECK(!grant->finish_status && f.frame && !f.ctx->identity);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_cancel_begin(f.frame,
        &f.receipt_key));
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f.frame));
    f.ctx = NULL;
    fixture_destroy(&f);
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
    uint64_t id, operation_identity;
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
    /* The session reads WIRELOG_JOIN_BATCH_BYTES and _STRICT at creation,
     * and the framed bootstrap refuses the knob (e3580a61); these fixtures
     * exercise the entry itself.  test_entry_refuses_join_batch_knob pins
     * the refusal. */
    f->session->join_batch_bytes = 0;
    f->session->join_batch_strict = false;
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
    f->operation_identity = f->id + 1000;
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
entry_turn_action(entry_fixture_t *f,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t limit)
{
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(f);
    bool consume = action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS
        && ((ctx && ctx->phase ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING)
        || (!ctx && f->previous.previous_valid
        && f->previous.previous_kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT));
    grant_t *grant = entry_grant(f, limit);
    wl_columnar_eval_serial_framed_host_request_t request = f->previous;
    request.funding = grant->control; request.funding_id = grant->id;
    request.operation = f->operation; request.operation_id = f->id;
    request.operation_identity = f->operation_identity;
    request.action = action;
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
    if (action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS
        && (!consume || !f->protocol)) {
        f->previous.previous_valid = !consume;
        f->previous.previous_kind = f->reply.kind;
        f->previous.previous_protocol = f->protocol;
        f->previous.previous_funding = grant->control;
        f->previous.previous_funding_id = grant->id;
    }
    return f->protocol;
}

static int
entry_turn(entry_fixture_t *f, bool status, uint64_t limit)
{
    return entry_turn_action(f, status
        ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS
        : WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE, limit);
}

static int
entry_receipt_turn(entry_fixture_t *f,
    wl_columnar_eval_serial_framed_host_action_t action,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    grant_t *grant = entry_grant(f, 0);
    wl_columnar_eval_serial_framed_host_request_t request = {0};
    request.funding = grant->control;
    request.funding_id = grant->id;
    request.operation = f->operation;
    request.operation_id = f->id;
    request.operation_identity = f->operation_identity;
    request.action = action;
    request.receipt_key = *key;
    request.reply = &f->reply;
    return entry_host(f, &request, grant);
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
    if (f->reply.cleanup_receipt_key.receipt_id) {
        wl_columnar_eval_cleanup_receipt_t receipt;
        CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(f->session,
            &f->reply.cleanup_receipt_key, &receipt));
        CHECK(receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED);
        CHECK(!wl_columnar_eval_stack_cleanup_receipt_ack(f->session,
            &f->reply.cleanup_receipt_key));
    }
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

static const char entry_crc_eth_map[] =
    ".decl input(x:int64)\n.decl output(y:int64)\n"
    "output(crc32_ethernet(x)) :- input(x).\n";
static const char entry_crc_cast_map[] =
    ".decl input(x:int64)\n.decl output(y:int64)\n"
    "output(crc32_castagnoli(x)) :- input(x).\n";
static const char entry_crc_eth_filter[] =
    ".decl input(x:int64)\n.decl output(x:int64)\n"
    "output(x) :- input(x), crc32_ethernet(x) >= 0.\n";
static const char entry_crc_cast_filter[] =
    ".decl input(x:int64)\n.decl output(x:int64)\n"
    "output(x) :- input(x), crc32_castagnoli(x) >= 0.\n";

static void
entry_assert_generated_crc(const wl_plan_op_t *op, bool map,
    uint8_t expected)
{
    CHECK((map && op->op == WL_PLAN_OP_MAP && op->map_exprs)
        || (!map && op->op == WL_PLAN_OP_FILTER));
    const wl_plan_expr_buffer_t *expr = map ? &op->map_exprs[0]
        : &op->filter_expr;
    CHECK(expr->data && expr->size);
    uint32_t pos = 0, matches = 0;
    while (pos < expr->size) {
        uint8_t tag = expr->data[pos++];
        if (tag == expected) ++matches;
        if (tag == WL_PLAN_EXPR_VAR || tag == WL_PLAN_EXPR_VAR_FLOAT) {
            CHECK(expr->size - pos >= 2);
            uint16_t length;
            memcpy(&length, expr->data + pos, sizeof(length));
            pos += 2;
            CHECK(length <= expr->size - pos);
            pos += length;
        } else if (tag == WL_PLAN_EXPR_CONST_INT
            || tag == WL_PLAN_EXPR_CONST_FLOAT) {
            CHECK(expr->size - pos >= 8);
            pos += 8;
        } else if (tag == WL_PLAN_EXPR_BOOL) {
            CHECK(pos < expr->size);
            ++pos;
        }
    }
    CHECK(matches == 1);
}

static void
entry_compare_crc_values(entry_fixture_t *f, const char *source,
    const int64_t *rows, uint32_t count)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_session_t *base = NULL;
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &base));
    if (count)
        CHECK(!wl_session_insert(base, "input", rows, count, 1));
    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "output"))
                rp = &plan->strata[s].relations[r];
    CHECK(rp && !wl_columnar_eval_serial_framed_relation(rp,
        (wl_col_session_t *)base, false, NULL));
    const col_rel_t *a = session_rel_hash_lookup(f->session, "output");
    const col_rel_t *b = session_find_rel((wl_col_session_t *)base, "output");
    CHECK(a && b && a->nrows == b->nrows && a->ncols == b->ncols
        && a->sorted_nrows == a->nrows
        && a->run_count == (a->nrows != 0)
        && a->column_types && a->column_types[0] == WIRELOG_TYPE_INT64);
    if (b->column_types)
        CHECK(b->column_types[0] == a->column_types[0]);
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
        for (uint32_t r = 0; r < a->nrows; ++r)
            CHECK(a->columns[c][r] == b->columns[c][r]);
    }
    wl_session_destroy(base);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

typedef struct {
    wl_evaluation_control_t *operation;
    bool target_seen, cancel;
    uint64_t target_hits, total_hits;
} entry_crc_probe_t;

static entry_crc_probe_t *entry_crc_probe;

static void entry_retry_after_stop(entry_fixture_t *f, const char *source,
    const entry_input_t *inputs, uint32_t input_count);

static void
entry_crc_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed)
{
    (void)failed;
    entry_crc_probe_t *probe = entry_crc_probe;
    if (!probe || control != probe->operation || phase != 25) return;
    ++probe->total_hits;
    if (progress != 4) return;
    probe->target_seen = true;
    ++probe->target_hits;
    if (probe->cancel) {
        probe->cancel = false;
        (void)wl_evaluation_control_request_cancel(control);
    }
}

typedef struct {
    uint64_t consumed;
    int result;
    bool target_seen;
} entry_crc_trial_t;

static entry_crc_trial_t
entry_crc_limit_trial(uint64_t limit)
{
    static const int64_t rows[] = {1, 0, -1, INT64_MIN, INT64_MAX, 42, -42, 7};
    entry_fixture_t f;
    entry_init(&f, entry_crc_eth_map, rows,
        sizeof(rows) / sizeof(rows[0]), 1, limit);
    const wl_plan_op_t *map = NULL;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_MAP) map = &f.relation->ops[i];
    CHECK(map);
    entry_assert_generated_crc(map, true, WL_PLAN_EXPR_ARITH_CRC32_ETH);
    entry_crc_probe_t probe = {.operation = f.operation};
    entry_crc_probe = &probe;
    wl_columnar_expr_test_checkpoint = entry_crc_checkpoint;
    entry_complete(&f);
    wl_columnar_expr_test_checkpoint = NULL;
    entry_crc_probe = NULL;
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        || (!f.reply.execution_result && f.reply.committed));
    if (f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT)
        CHECK(!f.reply.committed && !session_rel_hash_lookup(f.session,
            "output") && !f.session->cleanup_active
            && !f.session->cleanup_pending
            && !f.session->cleanup_reserved_bytes);
    entry_finish_original(&f);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && report.result == f.reply.execution_result
        && !report.cleanup_status);
    entry_crc_trial_t trial = {
        .consumed = report.consumed,
        .result = report.result,
        .target_seen = probe.target_seen
    };
    entry_destroy(&f);
    return trial;
}

static void
test_entry_generated_integer_crc(void)
{
    static const int64_t rows[] = {1, 0, -1, INT64_MIN, INT64_MAX, 42, -42};
    const struct {
        const char *source;
        bool map;
        uint8_t tag;
    } cases[] = {
        {entry_crc_eth_map, true, WL_PLAN_EXPR_ARITH_CRC32_ETH},
        {entry_crc_cast_map, true, WL_PLAN_EXPR_ARITH_CRC32_CAST},
        {entry_crc_eth_filter, false, WL_PLAN_EXPR_ARITH_CRC32_ETH},
        {entry_crc_cast_filter, false, WL_PLAN_EXPR_ARITH_CRC32_CAST}
    };
    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); ++c) {
        entry_fixture_t f;
        entry_init(&f, cases[c].source, rows,
            sizeof(rows) / sizeof(rows[0]), 1, 0);
        const wl_plan_op_t *op = NULL;
        for (uint32_t i = 0; i < f.relation->op_count; ++i)
            if (f.relation->ops[i].op == (cases[c].map
                ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER)) op = &f.relation->ops[i];


        CHECK(op);
        entry_assert_generated_crc(op, cases[c].map, cases[c].tag);
        entry_complete(&f);
        CHECK(!f.reply.execution_result && f.reply.committed);
        const col_rel_t *out = session_rel_hash_lookup(f.session, "output");
        CHECK(out && out->nrows == sizeof(rows) / sizeof(rows[0]));
        if (cases[c].map) {
            uint16_t endian = 1;
            int64_t expected = cases[c].tag == WL_PLAN_EXPR_ARITH_CRC32_ETH
                ? (*(uint8_t *)&endian ? INT64_C(2844319735)
                    : INT64_C(304476159))
                : (*(uint8_t *)&endian ? INT64_C(3306475437)
                    : INT64_C(2118332809));
            bool found = false;
            for (uint32_t i = 0; i < out->nrows; ++i)
                found |= out->columns[0][i] == expected;
            CHECK(found);
        } else {
            for (uint32_t i = 0; i < out->nrows; ++i) {
                bool found = false;
                for (uint32_t r = 0; r < sizeof(rows) / sizeof(rows[0]); ++r)
                    found |= out->columns[0][i] == rows[r];
                CHECK(found);
            }
        }
        entry_compare_crc_values(&f, cases[c].source, rows,
            sizeof(rows) / sizeof(rows[0]));
        entry_destroy(&f);
    }

    static const char symbol_crc[] =
        ".decl input(x:symbol)\n.decl output(y:int64)\n"
        "output(crc32_ethernet(x)) :- input(x).\n";
    entry_fixture_t symbol;
    entry_init(&symbol, symbol_crc, rows,
        sizeof(rows) / sizeof(rows[0]), 1, 0);
    const wl_plan_op_t *symbol_map = NULL;
    for (uint32_t i = 0; i < symbol.relation->op_count; ++i)
        if (symbol.relation->ops[i].op == WL_PLAN_OP_MAP)
            symbol_map = &symbol.relation->ops[i];
    CHECK(symbol_map);
    entry_assert_generated_crc(symbol_map, true,
        WL_PLAN_EXPR_ARITH_CRC32_ETH_S);
    col_rel_t **symbol_rels = symbol.session->rels;
    uint32_t symbol_nrels = symbol.session->nrels;
    col_rel_t *symbol_input = session_find_rel(symbol.session, "input");
    entry_complete(&symbol);
    CHECK(symbol.reply.execution_result == ENOTSUP
        && !symbol.reply.committed
        && !session_rel_hash_lookup(symbol.session, "output")
        && symbol.session->rels == symbol_rels
        && symbol.session->nrels == symbol_nrels
        && session_find_rel(symbol.session, "input") == symbol_input);
    entry_destroy(&symbol);

    /* Find the repeatable allowance that admits the fourth byte checkpoint.
     * With one int64 operand this checkpoint is inside the fixed CRC loop. */
    uint64_t low = 1, high = UINT64_C(1) << 20;
    CHECK(entry_crc_limit_trial(high).result == 0);
    while (low < high) {
        uint64_t mid = low + (high - low) / 2;
        if (entry_crc_limit_trial(mid).target_seen) high = mid;
        else low = mid + 1;
    }
    const uint64_t limit = low;
    entry_crc_trial_t below = entry_crc_limit_trial(limit - 1);
    entry_crc_trial_t exact = entry_crc_limit_trial(limit);
    entry_crc_trial_t repeat = entry_crc_limit_trial(limit);
    CHECK(!below.target_seen
        && below.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && exact.target_seen
        && exact.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && repeat.target_seen && repeat.result == exact.result
        && repeat.consumed == exact.consumed);

    static const int64_t cancel_rows[] = {1, 0, -1, INT64_MIN,
                                          INT64_MAX, 42, -42, 7};
    entry_fixture_t cancelled;
    entry_init(&cancelled, entry_crc_eth_map, cancel_rows,
        sizeof(cancel_rows) / sizeof(cancel_rows[0]), 1, 0);
    col_rel_t **old_rels = cancelled.session->rels;
    uint32_t old_nrels = cancelled.session->nrels;
    col_rel_t *old_input = session_find_rel(cancelled.session, "input");
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(
            cancelled.session->memory_governor));
    entry_crc_probe_t cancel_probe = {
        .operation = cancelled.operation, .cancel = true
    };
    entry_crc_probe = &cancel_probe;
    wl_columnar_expr_test_checkpoint = entry_crc_checkpoint;
    entry_complete(&cancelled);
    wl_columnar_expr_test_checkpoint = NULL;
    entry_crc_probe = NULL;
    CHECK(cancel_probe.target_hits == 1 && cancel_probe.total_hits == 4
        && cancelled.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !cancelled.reply.committed
        && !session_rel_hash_lookup(cancelled.session, "output")
        && cancelled.session->rels == old_rels
        && cancelled.session->nrels == old_nrels
        && session_find_rel(cancelled.session, "input") == old_input
        && old_input->nrows == sizeof(cancel_rows) / sizeof(cancel_rows[0])
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            cancelled.session->memory_governor)) == reserved
        && !cancelled.session->cleanup_active
        && !cancelled.session->cleanup_pending
        && !cancelled.session->cleanup_reserved_bytes);
    for (uint32_t i = 0; i < old_input->nrows; ++i)
        CHECK(old_input->columns[0][i] == cancel_rows[i]);
    entry_finish_original(&cancelled);
    CHECK(!wl_evaluation_control_reset(cancelled.operation));
    CHECK(!wl_evaluation_control_configure(cancelled.operation, 0));
    CHECK(!wl_evaluation_control_begin(cancelled.operation,
        cancelled.session->base.operation_admission));
    cancelled.id = wl_evaluation_control_active_attempt_id(
        cancelled.operation);
    CHECK(cancelled.id);
    cancelled.previous.previous_valid = false;
    memset(&cancelled.reply, 0, sizeof(cancelled.reply));
    cancelled.reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
    entry_complete(&cancelled);
    CHECK(!cancelled.reply.execution_result && cancelled.reply.committed);
    entry_compare_crc_values(&cancelled, entry_crc_eth_map, cancel_rows,
        sizeof(cancel_rows) / sizeof(cancel_rows[0]));
    entry_destroy(&cancelled);
}

static const char entry_hash_map[] =
    ".decl input(x:int64)\n.decl output(y:int64)\n"
    "output(hash(x)) :- input(x).\n";
static const char entry_hash_filter[] =
    ".decl input(x:int64)\n.decl output(x:int64)\n"
    "output(x) :- input(x), hash(x) >= 0.\n";
static const char entry_hash_symbol_map[] =
    ".decl input(x:symbol)\n.decl output(y:int64)\n"
    "output(hash(x)) :- input(x).\n";

typedef struct {
    wl_evaluation_control_t *operation;
    bool target_seen, cancel;
    uint64_t target_hits;
} entry_hash_probe_t;

static entry_hash_probe_t *entry_hash_probe;

static void
entry_hash_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed)
{
    (void)failed;
    entry_hash_probe_t *probe = entry_hash_probe;
    if (!probe || control != probe->operation || phase != 27 || progress != 1)
        return;
    probe->target_seen = true;
    ++probe->target_hits;
    if (probe->cancel) {
        probe->cancel = false;
        (void)wl_evaluation_control_request_cancel(control);
    }
}

typedef struct {
    uint64_t consumed;
    int result;
    bool target_seen;
} entry_hash_trial_t;

static entry_hash_trial_t
entry_hash_limit_trial(uint64_t limit)
{
    static const int64_t rows[] = {0, 1, -1, INT64_MIN, INT64_MAX, 42, -42};
    entry_fixture_t f;
    entry_init(&f, entry_hash_map, rows,
        sizeof(rows) / sizeof(rows[0]), 1, limit);
    const wl_plan_op_t *map = NULL;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_MAP) map = &f.relation->ops[i];
    CHECK(map && map->map_exprs && map->map_exprs[0].size);
    entry_assert_generated_crc(map, true, WL_PLAN_EXPR_ARITH_HASH);
    col_rel_t **registry = f.session->rels;
    uint32_t nrels = f.session->nrels;
    col_rel_t *input = session_find_rel(f.session, "input");
    CHECK(input && input->nrows == sizeof(rows) / sizeof(rows[0]));
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.session->memory_governor));
    entry_hash_probe_t probe = {.operation = f.operation};
    entry_hash_probe = &probe;
    wl_columnar_expr_test_checkpoint = entry_hash_checkpoint;
    entry_complete(&f);
    wl_columnar_expr_test_checkpoint = NULL;
    entry_hash_probe = NULL;
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        || (!f.reply.execution_result && f.reply.committed));
    if (f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT) {
        CHECK(!f.reply.committed && !session_rel_hash_lookup(f.session,
            "output") && !f.session->cleanup_active
            && !f.session->cleanup_pending
            && !f.session->cleanup_reserved_bytes
            && f.session->rels == registry && f.session->nrels == nrels
            && session_find_rel(f.session, "input") == input
            && input->nrows == sizeof(rows) / sizeof(rows[0])
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                f.session->memory_governor)) == reserved);
        for (uint32_t r = 0; r < input->nrows; ++r)
            CHECK(input->columns[0][r] == rows[r]);
    }
    entry_finish_original(&f);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && report.result == f.reply.execution_result
        && !report.cleanup_status);
    entry_hash_trial_t trial = {
        .consumed = report.consumed,
        .result = report.result,
        .target_seen = probe.target_seen
    };
    entry_destroy(&f);
    return trial;
}

static void
test_entry_generated_integer_hash(void)
{
    static const int64_t rows[] = {0, 1, -1, INT64_MIN, INT64_MAX, 42, -42};
    const struct { int64_t value; uint64_t little, big; } vectors[] = {
        {0, UINT64_C(14374147212387527897), UINT64_C(14374147212387527897)},
        {1, UINT64_C(3439722301264460078), UINT64_C(17540194650411207667)},
        {-1, UINT64_C(5841669975847748627), UINT64_C(5841669975847748627)},
        {INT64_MIN, UINT64_C(9407778237848358495),
         UINT64_C(13234421416632580310)},
        {INT64_MAX, UINT64_C(11687913294787043142),
         UINT64_C(17884634274034869649)},
        {42, UINT64_C(15395265915043915720), UINT64_C(3337142596522283180)},
        {-42, UINT64_C(4321076967882013291), UINT64_C(17120161371623954849)}
    };
    uint16_t endian = 1;
    bool little_endian = *(uint8_t *)&endian != 0;
    entry_fixture_t map_fixture;
    entry_init(&map_fixture, entry_hash_map, rows,
        sizeof(rows) / sizeof(rows[0]), 1, 0);
    int64_t collision_id = wl_intern_put(map_fixture.plan->intern, "zero");
    CHECK(collision_id == 0 && !strcmp(wl_intern_reverse(
            map_fixture.plan->intern, collision_id), "zero"));
    const wl_plan_op_t *map = NULL;
    for (uint32_t i = 0; i < map_fixture.relation->op_count; ++i)
        if (map_fixture.relation->ops[i].op == WL_PLAN_OP_MAP)
            map = &map_fixture.relation->ops[i];
    CHECK(map && map->map_exprs && map->map_exprs[0].size);
    entry_assert_generated_crc(map, true, WL_PLAN_EXPR_ARITH_HASH);
    entry_complete(&map_fixture);
    CHECK(!map_fixture.reply.execution_result && map_fixture.reply.committed);
    const col_rel_t *out = session_rel_hash_lookup(map_fixture.session,
            "output");
    CHECK(out && out->nrows == sizeof(rows) / sizeof(rows[0])
        && out->ncols == 1 && out->column_types
        && out->column_types[0] == WIRELOG_TYPE_INT64);
    for (uint32_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); ++i) {
        uint64_t expected = little_endian ? vectors[i].little : vectors[i].big;
        bool found = false;
        for (uint32_t r = 0; r < out->nrows; ++r)
            found |= out->columns[0][r] == (int64_t)expected;
        CHECK(found);
    }
    entry_compare_crc_values(&map_fixture, entry_hash_map, rows,
        sizeof(rows) / sizeof(rows[0]));
    entry_destroy(&map_fixture);

    entry_fixture_t filter_fixture;
    entry_init(&filter_fixture, entry_hash_filter, rows,
        sizeof(rows) / sizeof(rows[0]), 1, 0);
    const wl_plan_op_t *filter = NULL;
    for (uint32_t i = 0; i < filter_fixture.relation->op_count; ++i)
        if (filter_fixture.relation->ops[i].op == WL_PLAN_OP_FILTER)
            filter = &filter_fixture.relation->ops[i];
    CHECK(filter && filter->filter_expr.size);
    entry_assert_generated_crc(filter, false, WL_PLAN_EXPR_ARITH_HASH);
    entry_complete(&filter_fixture);
    CHECK(!filter_fixture.reply.execution_result &&
        filter_fixture.reply.committed);
    entry_compare_crc_values(&filter_fixture, entry_hash_filter, rows,
        sizeof(rows) / sizeof(rows[0]));
    entry_destroy(&filter_fixture);

    entry_fixture_t symbol;
    entry_init(&symbol, entry_hash_symbol_map, rows,
        sizeof(rows) / sizeof(rows[0]), 1, 0);
    const wl_plan_op_t *symbol_map = NULL;
    for (uint32_t i = 0; i < symbol.relation->op_count; ++i)
        if (symbol.relation->ops[i].op == WL_PLAN_OP_MAP)
            symbol_map = &symbol.relation->ops[i];
    CHECK(symbol_map);
    entry_assert_generated_crc(symbol_map, true, WL_PLAN_EXPR_ARITH_HASH_S);
    col_rel_t **old_rels = symbol.session->rels;
    uint32_t old_nrels = symbol.session->nrels;
    col_rel_t *old_input = session_find_rel(symbol.session, "input");
    entry_complete(&symbol);
    CHECK(symbol.reply.execution_result == ENOTSUP
        && !symbol.reply.committed && !session_rel_hash_lookup(symbol.session,
        "output") && symbol.session->rels == old_rels
        && symbol.session->nrels == old_nrels
        && session_find_rel(symbol.session, "input") == old_input);
    entry_destroy(&symbol);

    uint64_t low = 1, high = UINT64_C(1) << 20;
    CHECK(entry_hash_limit_trial(high).result == 0);
    while (low < high) {
        uint64_t mid = low + (high - low) / 2;
        if (entry_hash_limit_trial(mid).target_seen) high = mid;
        else low = mid + 1;
    }
    entry_hash_trial_t below = entry_hash_limit_trial(low - 1);
    entry_hash_trial_t exact = entry_hash_limit_trial(low);
    entry_hash_trial_t repeat = entry_hash_limit_trial(low);
    CHECK(!below.target_seen
        && below.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && exact.target_seen && exact.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && repeat.target_seen && repeat.result == exact.result
        && repeat.consumed == exact.consumed);

    entry_fixture_t cancelled;
    entry_init(&cancelled, entry_hash_map, rows,
        sizeof(rows) / sizeof(rows[0]), 1, 0);
    col_rel_t **registry = cancelled.session->rels;
    uint32_t nrels = cancelled.session->nrels;
    col_rel_t *input = session_find_rel(cancelled.session, "input");
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(
            cancelled.session->memory_governor));
    entry_hash_probe_t cancel_probe = {
        .operation = cancelled.operation, .cancel = true
    };
    entry_hash_probe = &cancel_probe;
    wl_columnar_expr_test_checkpoint = entry_hash_checkpoint;
    entry_complete(&cancelled);
    wl_columnar_expr_test_checkpoint = NULL;
    entry_hash_probe = NULL;
    CHECK(cancel_probe.target_hits == 1
        && cancelled.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !cancelled.reply.committed
        && !session_rel_hash_lookup(cancelled.session, "output")
        && cancelled.session->rels == registry &&
        cancelled.session->nrels == nrels
        && session_find_rel(cancelled.session, "input") == input
        && input->nrows == sizeof(rows) / sizeof(rows[0])
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            cancelled.session->memory_governor)) == reserved);
    for (uint32_t r = 0; r < input->nrows; ++r)
        CHECK(input->columns[0][r] == rows[r]);
    entry_finish_original(&cancelled);
    CHECK(!wl_evaluation_control_reset(cancelled.operation));
    CHECK(!wl_evaluation_control_configure(cancelled.operation, 0));
    CHECK(!wl_evaluation_control_begin(cancelled.operation,
        cancelled.session->base.operation_admission));
    cancelled.id = wl_evaluation_control_active_attempt_id(cancelled.operation);
    CHECK(cancelled.id);
    cancelled.previous.previous_valid = false;
    memset(&cancelled.reply, 0, sizeof(cancelled.reply));
    cancelled.reply.kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
    entry_complete(&cancelled);
    CHECK(!cancelled.reply.execution_result && cancelled.reply.committed);
    entry_destroy(&cancelled);
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

static const char entry_generated_grouped_count[] =
    ".decl input(k:int64,x:int64)\n.decl output(k:int64,n:int64)\n"
    "output(k,count(x)) :- input(k,x).\n";

static entry_reduce_trial_t
entry_grouped_count_trial(uint64_t limit, bool require_success)
{
    int64_t rows[4096 * 2];
    for (uint32_t i = 0; i < 4096; ++i) {
        rows[2 * i] = (int64_t)(i % 4);
        rows[2 * i + 1] = (int64_t)i + 1;
    }
    entry_input_t input = {"input", rows, 4096, 2};
    entry_fixture_t f;
    entry_init_inputs(&f, entry_generated_grouped_count, &input, 1, limit);
    const wl_plan_op_t *reduce = NULL;
    unsigned reduces = 0;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_REDUCE) {
            reduce = &f.relation->ops[i];
            ++reduces;
        }
    CHECK(reduces == 1 && reduce->agg_fn == WIRELOG_AGG_COUNT
        && reduce->group_by_count == 1 && reduce->group_by_indices
        && reduce->group_by_indices[0] == 0 && reduce->aggregate_index == 1);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(f.session->memory_governor);
    uint64_t reserved = wl_columnar_memory_reserved(governor);
    col_rel_t **registry = f.session->rels;
    uint32_t registry_count = f.session->nrels;
    col_rel_t *source = session_find_rel(f.session, "input");
    CHECK(source && source->nrows == 4096 && source->ncols == 2);
    const int64_t *source_keys = source->columns[0];
    const int64_t *source_values = source->columns[1];
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
            && session_rel_hash_lookup(f.session, "input") == source
            && f.session->rels == registry && f.session->nrels == registry_count
            && source->columns[0] == source_keys
            && source->columns[1] == source_values && source->nrows == 4096
            && wl_columnar_memory_reserved(governor) == reserved);
        for (uint32_t i = 0; i < 4096; ++i)
            CHECK(source_keys[i] == (int64_t)(i % 4)
                && source_values[i] == (int64_t)i + 1);
    } else {
        CHECK(!f.reply.execution_result && f.reply.committed);
        const col_rel_t *out = session_rel_hash_lookup(f.session, "output");
        CHECK(out && out->nrows == 4 && out->ncols == 2);
        bool found[4] = {false, false, false, false};
        for (uint32_t r = 0; r < out->nrows; ++r) {
            int64_t key = out->columns[0][r];
            CHECK(key >= 0 && key < 4 && !found[key]
                && out->columns[1][r] == 1024);
            found[key] = true;
        }
        CHECK(found[0] && found[1] && found[2] && found[3]);
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
test_entry_generated_grouped_count_work_limit(void)
{
    const unsigned target = 1023;
    const entry_reduce_trial_t high = entry_grouped_count_trial(
        UINT64_C(1) << 30, true);
    uint64_t low = 1, upper = UINT64_C(1) << 30;
    while (low < upper) {
        uint64_t mid = low + (upper - low) / 2;
        entry_reduce_trial_t trial = entry_grouped_count_trial(mid, false);
        if (trial.target_seen) upper = mid;
        else low = mid + 1;
    }
    const uint64_t limit = low;
    const entry_reduce_trial_t below = entry_grouped_count_trial(limit - 1,
            false);
    const entry_reduce_trial_t boundary = entry_grouped_count_trial(limit,
            false);
    const entry_reduce_trial_t repeat = entry_grouped_count_trial(limit,
            false);
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

    int64_t rows[4096 * 2];
    for (uint32_t i = 0; i < 4096; ++i) {
        rows[2 * i] = (int64_t)(i % 4);
        rows[2 * i + 1] = (int64_t)i + 1;
    }
    entry_input_t input = {"input", rows, 4096, 2};
    entry_fixture_t f;
    entry_init_inputs(&f, entry_generated_grouped_count, &input, 1, limit);
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
    CHECK(out && out->nrows == 4 && out->ncols == 2);
    bool found[4] = {false, false, false, false};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        int64_t key = out->columns[0][r];
        CHECK(key >= 0 && key < 4 && !found[key]
            && out->columns[1][r] == 1024);
        found[key] = true;
    }
    CHECK(found[0] && found[1] && found[2] && found[3]);
    entry_compare_inputs(&f, entry_generated_grouped_count, &input, 1);
    entry_destroy(&f);
}

typedef struct {
    entry_fixture_t *fixture;
    unsigned phase46_hits, phase47_hits;
    uint64_t phase47_last_progress;
    bool target_seen;
} entry_numeric_reduce_probe_t;

static entry_numeric_reduce_probe_t *entry_numeric_reduce_probe;

static void
entry_numeric_reduce_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    entry_numeric_reduce_probe_t *probe = entry_numeric_reduce_probe;
    if (!probe) return;
    bool belongs = control == probe->fixture->operation;
    for (unsigned i = 0; i < 3; ++i)
        belongs |= control == probe->fixture->grants[i].control;
    CHECK(belongs && wl_evaluation_control_active_attempt_id(control));
    if (phase == 46) ++probe->phase46_hits;
    if (phase == 47) {
        ++probe->phase47_hits;
        probe->phase47_last_progress = progress;
        /* phase 47 runs after the private group value is updated. */
        if (progress == 1023) probe->target_seen = true;
    }
}

typedef struct {
    const char *name;
    const char *source;
    wirelog_agg_fn_t fn;
    bool floating;
    int64_t expected_integer[4];
    double expected_float[4];
} entry_numeric_reduce_case_t;

static const entry_numeric_reduce_case_t entry_numeric_reduce_cases[] = {
    {.name = "MIN", .source = ".decl input(k:int64,x:int64)\n"
         ".decl output(k:int64,n:int64)\n"
         "output(k,min(x)) :- input(k,x).\n", .fn = WIRELOG_AGG_MIN,
     .expected_integer = {-2048, -2047, -2046, -2045}},
    {.name = "SUM", .source = ".decl input(k:int64,x:int64)\n"
         ".decl output(k:int64,n:int64)\n"
         "output(k,sum(x)) :- input(k,x).\n", .fn = WIRELOG_AGG_SUM,
     .expected_integer = {-2048, -1024, 0, 1024}},
    {.name = "MAX", .source = ".decl input(k:int64,x:int64)\n"
         ".decl output(k:int64,n:int64)\n"
         "output(k,max(x)) :- input(k,x).\n", .fn = WIRELOG_AGG_MAX,
     .expected_integer = {2044, 2045, 2046, 2047}},
    {.name = "FLOAT MIN", .source = ".decl input(k:int64,x:float)\n"
         ".decl output(k:int64,n:float)\n"
         "output(k,min(x)) :- input(k,x).\n", .fn = WIRELOG_AGG_MIN,
     .floating = true,
     .expected_float = {-512.0, -511.75, -511.5, -511.25}},
    {.name = "FLOAT SUM", .source = ".decl input(k:int64,x:float)\n"
         ".decl output(k:int64,n:float)\n"
         "output(k,sum(x)) :- input(k,x).\n", .fn = WIRELOG_AGG_SUM,
     .floating = true,
     .expected_float = {-512.0, -256.0, 0.0, 256.0}},
    {.name = "FLOAT MAX", .source = ".decl input(k:int64,x:float)\n"
         ".decl output(k:int64,n:float)\n"
         "output(k,max(x)) :- input(k,x).\n", .fn = WIRELOG_AGG_MAX,
     .floating = true,
     .expected_float = {511.0, 511.25, 511.5, 511.75}},
    {.name = "FLOAT AVG", .source = ".decl input(k:int64,x:float)\n"
         ".decl output(k:int64,n:float)\n"
         "output(k,average(x)) :- input(k,x).\n", .fn = WIRELOG_AGG_AVG,
     .floating = true,
     .expected_float = {-0.5, -0.25, 0.0, 0.25}}
};

static uint32_t
entry_numeric_reduce_float_index(uint32_t row)
{
    const uint32_t group_index = row / 4;
    if (group_index == 0) return 512;
    if (group_index == 1) return 1023;
    if (group_index == 2) return 0;
    const uint32_t remaining = group_index - 3;
    return remaining < 511 ? remaining + 1 : remaining + 2;
}

static int64_t
entry_numeric_reduce_input_value(uint32_t row, bool floating)
{
    if (!floating) return (int64_t)row - 2048;
    const uint32_t key = row % 4;
    const uint32_t value_index = entry_numeric_reduce_float_index(row);
    const double value = (double)((int64_t)4 * value_index + key - 2048)
        / 4.0;
    return (int64_t)wl_columnar_float_to_bits(value);
}

static void
entry_numeric_reduce_fill_rows(int64_t rows[4096 * 2])
{
    for (uint32_t i = 0; i < 4096; ++i) {
        rows[2 * i] = (int64_t)(i % 4);
        rows[2 * i + 1] = entry_numeric_reduce_input_value(i, false);
    }
}

static void
entry_numeric_reduce_fill_float_rows(int64_t rows[4096 * 2])
{
    for (uint32_t i = 0; i < 4096; ++i) {
        rows[2 * i] = (int64_t)(i % 4);
        rows[2 * i + 1] = entry_numeric_reduce_input_value(i, true);
    }
}

static void
entry_numeric_reduce_assert_output(const col_rel_t *out,
    const entry_numeric_reduce_case_t *test_case)
{
    CHECK(out && out->nrows == 4 && out->ncols == 2);
    bool found[4] = {false, false, false, false};
    if (test_case->floating)
        CHECK(out->column_types
            && out->column_types[0] == WIRELOG_TYPE_INT64
            && out->column_types[1] == WIRELOG_TYPE_FLOAT);
    for (uint32_t r = 0; r < out->nrows; ++r) {
        int64_t key = out->columns[0][r];
        CHECK(key >= 0 && key < 4 && !found[key]);
        if (test_case->floating)
            CHECK(out->columns[1][r] == (int64_t)
                wl_columnar_float_to_bits(test_case->expected_float[key]));
        else
            CHECK(out->columns[1][r] == test_case->expected_integer[key]);
        found[key] = true;
    }
    CHECK(found[0] && found[1] && found[2] && found[3]);
}

typedef struct {
    unsigned phase46_hits, phase47_hits;
    uint64_t phase47_last_progress, consumed;
    int result, cleanup_status;
    wl_evaluation_control_stop_t stop_reason;
    bool target_seen, committed;
} entry_numeric_reduce_trial_t;

static entry_numeric_reduce_trial_t
entry_numeric_reduce_trial(const entry_numeric_reduce_case_t *test_case,
    uint64_t limit, bool require_success)
{
    int64_t rows[4096 * 2];
    if (test_case->floating) entry_numeric_reduce_fill_float_rows(rows);
    else entry_numeric_reduce_fill_rows(rows);
    entry_input_t input = {"input", rows, 4096, 2};
    entry_fixture_t f;
    entry_init_inputs(&f, test_case->source, &input, 1, limit);
    const wl_plan_op_t *reduce = NULL;
    unsigned reduces = 0;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_REDUCE) {
            reduce = &f.relation->ops[i];
            ++reduces;
        }
    CHECK(reduces == 1 && reduce->agg_fn == test_case->fn
        && reduce->group_by_count == 1 && reduce->group_by_indices
        && reduce->group_by_indices[0] == 0 && reduce->aggregate_index == 1
        && reduce->agg_operand_type == (test_case->floating
            ? WL_PLAN_AGG_OPERAND_FLOAT : WL_PLAN_AGG_OPERAND_SCALAR)
        && reduce->agg_result_type == (test_case->floating
            ? WL_PLAN_AGG_OPERAND_FLOAT : WL_PLAN_AGG_OPERAND_SCALAR));
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(f.session->memory_governor);
    uint64_t reserved = wl_columnar_memory_reserved(governor);
    col_rel_t **registry = f.session->rels;
    uint32_t registry_count = f.session->nrels;
    col_rel_t *source = session_find_rel(f.session, "input");
    CHECK(source && source->nrows == 4096 && source->ncols == 2);
    if (test_case->floating)
        CHECK(source->column_types
            && source->column_types[0] == WIRELOG_TYPE_INT64
            && source->column_types[1] == WIRELOG_TYPE_FLOAT);
    const int64_t *source_keys = source->columns[0];
    const int64_t *source_values = source->columns[1];
    entry_numeric_reduce_probe_t probe = {.fixture = &f,
                                          .phase47_last_progress = UINT64_MAX};
    entry_numeric_reduce_probe = &probe;
    wl_columnar_ops_test_reduce_attempt_checkpoint =
        entry_numeric_reduce_checkpoint;
    entry_complete(&f);
    wl_columnar_ops_test_reduce_attempt_checkpoint = NULL;
    entry_numeric_reduce_probe = NULL;

    entry_numeric_reduce_trial_t trial = {
        .phase46_hits = probe.phase46_hits,
        .phase47_hits = probe.phase47_hits,
        .phase47_last_progress = probe.phase47_last_progress,
        .target_seen = probe.target_seen,
        .committed = f.reply.committed
    };
    if (f.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT) {
        CHECK(!f.reply.committed && !session_find_rel(f.session, "output")
            && session_rel_hash_lookup(f.session, "input") == source
            && f.session->rels == registry && f.session->nrels == registry_count
            && source->columns[0] == source_keys
            && source->columns[1] == source_values && source->nrows == 4096
            && wl_columnar_memory_reserved(governor) == reserved);
        for (uint32_t i = 0; i < 4096; ++i)
            CHECK(source_keys[i] == (int64_t)(i % 4)
                && source_values[i] == entry_numeric_reduce_input_value(i,
                test_case->floating));
    } else {
        CHECK(!f.reply.execution_result && f.reply.committed);
        entry_numeric_reduce_assert_output(
            session_rel_hash_lookup(f.session, "output"), test_case);
    }
    if (require_success)
        CHECK(!f.reply.execution_result && f.reply.committed
            && trial.phase46_hits == 4 && trial.phase47_hits == 4092
            && trial.phase47_last_progress == 4095);
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
test_entry_generated_grouped_numeric_reduce_work_limit(void)
{
    for (unsigned i = 0; i < sizeof(entry_numeric_reduce_cases)
        / sizeof(entry_numeric_reduce_cases[0]); ++i) {
        const entry_numeric_reduce_case_t *test_case =
            &entry_numeric_reduce_cases[i];
        const entry_numeric_reduce_trial_t high = entry_numeric_reduce_trial(
            test_case, UINT64_C(1) << 30, true);
        uint64_t low = 1, upper = UINT64_C(1) << 30;
        while (low < upper) {
            uint64_t mid = low + (upper - low) / 2;
            entry_numeric_reduce_trial_t trial = entry_numeric_reduce_trial(
                test_case, mid, false);
            if (trial.target_seen) upper = mid;
            else low = mid + 1;
        }
        const uint64_t limit = low;
        const entry_numeric_reduce_trial_t below = entry_numeric_reduce_trial(
            test_case, limit - 1, false);
        const entry_numeric_reduce_trial_t boundary =
            entry_numeric_reduce_trial(test_case, limit, false);
        const entry_numeric_reduce_trial_t repeat =
            entry_numeric_reduce_trial(test_case, limit, false);
        CHECK(high.committed && high.result == 0
            && !below.target_seen && below.phase47_last_progress == 1022
            && below.result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && below.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(boundary.target_seen
            && boundary.phase47_last_progress == 1023
            && boundary.phase47_hits == 1020
            && boundary.result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && boundary.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(repeat.target_seen && repeat.phase47_last_progress == 1023
            && repeat.phase47_hits == boundary.phase47_hits
            && repeat.result == boundary.result
            && repeat.consumed == boundary.consumed
            && repeat.stop_reason == boundary.stop_reason
            && repeat.cleanup_status == boundary.cleanup_status);

        int64_t rows[4096 * 2];
        if (test_case->floating) entry_numeric_reduce_fill_float_rows(rows);
        else entry_numeric_reduce_fill_rows(rows);
        entry_input_t input = {"input", rows, 4096, 2};
        entry_fixture_t f;
        entry_init_inputs(&f, test_case->source, &input, 1, limit);
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
        entry_numeric_reduce_assert_output(
            session_rel_hash_lookup(f.session, "output"), test_case);
        entry_compare_inputs(&f, test_case->source, &input, 1);
        entry_destroy(&f);
    }
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

/* The controlled framed bootstrap is not a bounded JOIN: with
 * join_batch_bytes set it refuses with ENOTSUP before taking a frame,
 * admitting a byte or touching the output, and leaves the operation's
 * attempt active.  The refusal still leaves BOOTSTRAP_RECEIPT in
 * reply.kind, so a host must act on the protocol rc.  Strict mode alone
 * does not refuse: the guard reads the byte knob. */
static void
test_entry_refuses_join_batch_knob(void)
{
    entry_fixture_t f; entry_init(&f, entry_simple, entry_rows, 4, 1, 0);
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(
        f.session->memory_governor);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    f.session->join_batch_bytes = 224;
    /* One ADVANCE built as entry_turn builds it; entry_turn itself would
     * record the receipt for a later turn to consume. */
    grant_t *grant = entry_grant(&f, 0);
    wl_columnar_eval_serial_framed_host_request_t request = f.previous;
    request.funding = grant->control; request.funding_id = grant->id;
    request.operation = f.operation; request.operation_id = f.id;
    request.operation_identity = f.operation_identity;
    request.action = WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE;
    request.reply = &f.reply;
    CHECK(entry_host(&f, &request, grant) == ENOTSUP);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant->control, &report)
        && report.attempt_id == grant->id && !report.cleanup_status
        && !wl_evaluation_control_active_attempt_id(grant->control));
    CHECK(!entry_context(&f) && !f.session->cleanup_active
        && !f.session->cleanup_pending && !f.session->cleanup_reserved_bytes
        && !session_find_rel(f.session, "output")
        && wl_columnar_memory_reserved(g) == baseline
        && wl_evaluation_control_active_attempt_id(f.operation) == f.id
        && f.session->join_batch_bytes == 224);
    f.session->join_batch_bytes = 0;
    f.session->join_batch_strict = true;
    CHECK(!entry_turn(&f, false, 0));
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
        && entry_context(&f));
    f.session->join_batch_strict = false;
    entry_complete(&f);
    entry_destroy(&f);
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
test_entry_abort_and_unsupported_abort(void)
{
    entry_fixture_t supported;
    entry_init(&supported, entry_simple, entry_rows, 4, 1, 0);
    CHECK(!entry_turn(&supported, false, 0));
    CHECK(entry_context(&supported)->phase ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP
        && entry_context(&supported)->abort_supported);
    CHECK(!entry_turn_action(&supported,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(entry_context(&supported)->abort_requested
        && entry_context(&supported)->primary_result ==
        WL_EVALUATION_CONTROL_CANCELLED);
    entry_complete(&supported);
    CHECK(supported.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL
        && supported.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !supported.reply.committed && !session_find_rel(supported.session,
        "output"));
    entry_finish_original(&supported);
    entry_destroy(&supported);

    static const char join_source[] =
        ".decl input(x:int64)\n.decl rhs(x:int64)\n"
        ".decl output(x:int64)\noutput(x) :- input(x), rhs(x).\n";
    const entry_input_t inputs[] = {
        {"input", entry_rows, 4, 1}, {"rhs", entry_rows, 4, 1}
    };
    entry_fixture_t join_abort;
    entry_init_inputs(&join_abort, join_source, inputs, 2, 0);
    CHECK(!entry_turn(&join_abort, false, 0));
    entry_until(&join_abort, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&join_abort);
    CHECK(ctx && ctx->abort_supported
        && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    CHECK(!entry_turn_action(&join_abort,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(entry_context(&join_abort)->abort_requested);
    entry_complete(&join_abort);
    CHECK(join_abort.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !join_abort.reply.committed
        && !session_find_rel(join_abort.session, "output"));
    entry_finish_original(&join_abort);
    entry_destroy(&join_abort);

    entry_fixture_t unsupported_pending;
    entry_init_inputs(&unsupported_pending, join_source, inputs, 2, 0);
    CHECK(!entry_turn(&unsupported_pending, false, 0));
    entry_until(&unsupported_pending, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    CHECK(!entry_turn(&unsupported_pending, false, 0));
    ctx = entry_context(&unsupported_pending);
    CHECK(ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && ctx->abort_supported);
    wl_columnar_eval_plan_numeric_phase_t child_phase = ctx->child.phase;
    CHECK(!entry_turn_action(&unsupported_pending,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(entry_context(&unsupported_pending) == ctx
        && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
        && !ctx->pending_funding && !ctx->pending_funding_id
        && ctx->child.phase == child_phase && ctx->abort_requested);
    entry_complete(&unsupported_pending);
    CHECK(unsupported_pending.reply.execution_result ==
        WL_EVALUATION_CONTROL_CANCELLED && !unsupported_pending.reply.committed
        && !session_find_rel(unsupported_pending.session, "output"));
    entry_finish_original(&unsupported_pending);
    entry_destroy(&unsupported_pending);

    entry_fixture_t nested;
    entry_init(&nested, entry_generated_map, entry_rows, 4, 1, 0);
    CHECK(!entry_turn(&nested, false, 0));
    entry_until(&nested, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    unsigned turns = 0;
    while (entry_context(&nested)->child.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING) {
        CHECK(++turns < 200000);
        CHECK(!entry_turn(&nested, false, 0));
    }
    CHECK(entry_context(&nested)->phase ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING);
    CHECK(!entry_turn_action(&nested,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(entry_context(&nested)->abort_requested
        && entry_context(&nested)->phase ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    entry_complete(&nested);
    CHECK(nested.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !nested.reply.committed);
    entry_finish_original(&nested);
    entry_destroy(&nested);
}

static void
test_entry_abort_publication_cutoff(void)
{
    entry_fixture_t before;
    entry_init(&before, entry_simple, entry_rows, 4, 1, 0);
    CHECK(!entry_turn(&before, false, 0));
    entry_until(&before, WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
    CHECK(!entry_turn(&before, false, 0));
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&before);
    CHECK(ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE
        && !ctx->publication.committed);
    unsigned turns = 0;
    while (ctx->publication.phase !=
        WL_COLUMNAR_SESSION_HASH_REGISTRY_CUTOFF) {
        CHECK(++turns < 200000 && !entry_turn(&before, false, 0));
    }
    CHECK(!entry_turn_action(&before, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    entry_complete(&before);
    CHECK(before.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !before.reply.committed
        && !session_rel_hash_lookup(before.session, "output"));
    entry_finish_original(&before);
    entry_destroy(&before);

    entry_fixture_t after;
    entry_init(&after, entry_simple, entry_rows, 4, 1, 0);
    CHECK(!entry_turn(&after, false, 0));
    entry_until(&after, WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
    CHECK(!entry_turn(&after, false, 0));
    ctx = entry_context(&after);
    turns = 0;
    while (!ctx->publication.committed)
        CHECK(++turns < 200000 && !entry_turn(&after, false, 0));
    col_rel_t *published = session_rel_hash_lookup(after.session, "output");
    CHECK(published && !entry_turn_action(&after,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    entry_complete(&after);
    CHECK(after.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && after.reply.committed
        && session_rel_hash_lookup(after.session, "output") == published);
    entry_finish_original(&after);
    entry_destroy(&after);
}

static void
test_entry_receipt_only_continuation(void)
{
    entry_fixture_t f;
    entry_init(&f, entry_simple, entry_rows, 4, 1, 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_cleanup_receipt_key_t key;
    CHECK(!wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            f.session, &frame, f.operation, f.id, f.operation_identity, &key));
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_prepare(frame, &key, 37,
        true));
    col_rel_t *owned = NULL;
    CHECK(!col_rel_deep_copy(session_find_rel(f.session, "input"), &owned,
        NULL));
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(owned, &reader));
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    stack->items[0] = (eval_entry_t){.rel = owned, .owned = true,
                                     .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION};
    stack->top = 1;
    wl_columnar_eval_cleanup_outcome_t outcome;
    CHECK(wl_columnar_eval_stack_cleanup_finish_receipted(&frame, &key,
        &outcome) == EBUSY && !frame
        && outcome == WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_TRANSFERRED_TO_PENDING);
    CHECK(!f.session->cleanup_active && f.session->cleanup_pending_count == 1);

    CHECK(!entry_receipt_turn(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS,
        &key));
    CHECK(f.reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING
        && f.reply.execution_result == 37 && f.reply.committed
        && f.reply.cleanup_result == EBUSY);
    CHECK(entry_receipt_turn(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, &key)
        == EBUSY);
    CHECK(f.reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING
        && f.reply.execution_result == 37 && f.reply.committed);
    wl_columnar_eval_stack_cleanup_frame_t *pending =
        f.session->cleanup_pending;
    CHECK(pending && f.session->cleanup_pending_count == 1);
    f.session->cleanup_pending = NULL;
    f.session->cleanup_pending_count = 0;
    CHECK(entry_receipt_turn(&f,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY, &key) == ENOENT);
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED);
    f.session->cleanup_pending = pending;
    f.session->cleanup_pending_count = 1;
    CHECK(!col_rel_source_reader_release(&reader));
    CHECK(!entry_receipt_turn(&f,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY, &key));
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL
        && f.reply.execution_result == 37 && f.reply.committed
        && !f.reply.cleanup_result && !f.session->cleanup_pending);
    CHECK(!entry_receipt_turn(&f,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK, &key));
    CHECK(f.reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL
        && f.reply.execution_result == 37 && f.reply.committed);
    CHECK(entry_receipt_turn(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS, &key)
        == ENOENT);
    entry_finish_original(&f);
    entry_destroy(&f);
}

static void
test_cleanup_receipt_transfer_retry(void)
{
    wl_col_session_t session = {0};
    wl_mem_ledger_init(&session.mem_ledger, 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame1 = NULL, *frame2 = NULL;
    wl_columnar_eval_cleanup_receipt_key_t key1, key2;
    CHECK(!wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            &session, &frame1, NULL, 17, 99, &key1));
    col_rel_t *relation1 = source_new("held1", 1, 2, false, 0);
    CHECK(!eval_stack_push(wl_columnar_eval_stack_cleanup_stack(frame1),
        relation1, true));
    wl_columnar_source_access_reader_t reader1 = {0};
    CHECK(!col_rel_source_reader_acquire(relation1, &reader1));
    CHECK(!wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            &session, &frame2, NULL, 18, 100, &key2));
    col_rel_t *relation2 = source_new("held2", 1, 2, false, 0);
    CHECK(!eval_stack_push(wl_columnar_eval_stack_cleanup_stack(frame2),
        relation2, true));
    wl_columnar_source_access_reader_t reader2 = {0};
    CHECK(!col_rel_source_reader_acquire(relation2, &reader2));
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame2) == EBUSY && frame2);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_prepare(frame2, &key2, 8,
        false));
    wl_columnar_eval_cleanup_outcome_t outcome;
    CHECK(wl_columnar_eval_stack_cleanup_finish_receipted(&frame2, &key2,
        &outcome) == EBUSY && !frame2
        && outcome == WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_TRANSFERRED_TO_PENDING
        && session.cleanup_pending_count == 1);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame1) == EBUSY && frame1);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_prepare(frame1, &key1, 7,
        true));
    CHECK(wl_columnar_eval_stack_cleanup_finish_receipted(&frame1, &key1,
        &outcome) == EBUSY && !frame1 && session.cleanup_pending_count == 2);
    wl_columnar_eval_cleanup_receipt_t receipt;
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(&session, &key1,
        &receipt));
    CHECK(receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED
        && receipt.result == 7 && receipt.committed
        && receipt.cleanup_result == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_receipt_ack(&session, &key1) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_receipt_retry(&session, &key1)
        == EBUSY && session.cleanup_pending_count == 2);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(&session, &key1,
        &receipt));
    CHECK(receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED
        && receipt.cleanup_result == EBUSY);
    CHECK(!col_rel_source_reader_release(&reader1));
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_retry(&session, &key1)
        && session.cleanup_pending_count == 1);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(&session, &key1,
        &receipt));
    CHECK(receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        && !receipt.cleanup_result && receipt.result == 7 && receipt.committed);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(&session, &key2,
        &receipt));
    CHECK(receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED
        && receipt.result == 8 && !receipt.committed);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_ack(&session, &key1));
    CHECK(!col_rel_source_reader_release(&reader2));
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_retry(&session, &key2)
        && !session.cleanup_pending && !session.cleanup_pending_count);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(&session, &key2,
        &receipt));
    CHECK(receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        && receipt.result == 8 && !receipt.committed);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_ack(&session, &key2));
    CHECK(wl_columnar_eval_stack_cleanup_receipt_read(&session, &key2,
        &receipt) == ENOENT);
    wl_col_session_t other_session = {0};
    wl_columnar_eval_cleanup_receipt_key_t other_key;
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_reserve(&other_session,
        17, 99, &other_key));
    CHECK(wl_columnar_eval_stack_cleanup_receipt_read(&other_session, &key1,
        &receipt) == ENOENT);
    CHECK(wl_columnar_eval_stack_cleanup_receipt_read(&session, &other_key,
        &receipt) == ENOENT);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_release(&other_session,
        &other_key));
    wl_columnar_eval_cleanup_receipt_key_t key;
    wl_columnar_eval_cleanup_receipt_key_t keys[
        WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES];
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i)
        CHECK(!wl_columnar_eval_stack_cleanup_receipt_reserve(&session, 17,
            99, &keys[i]));
    CHECK(wl_columnar_eval_stack_cleanup_receipt_reserve(&session, 17, 99,
        &key) == ENOBUFS);
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i)
        CHECK(!wl_columnar_eval_stack_cleanup_receipt_release(&session,
            &keys[i]));
    session.cleanup_receipt_next_id = UINT64_MAX;
    CHECK(wl_columnar_eval_stack_cleanup_receipt_reserve(&session, 17, 99,
        &key) == EOVERFLOW && key.receipt_id == 0);
    session.cleanup_receipt_next_id = 101;
    session.cleanup_receipt_next_plan_epoch = UINT64_MAX;
    CHECK(wl_columnar_eval_stack_cleanup_receipt_reserve(&session, 17, 99,
        &key) == EOVERFLOW && key.receipt_id == 0);
    session.cleanup_receipt_next_plan_epoch = 101;
}

static void
test_cleanup_receipt_generation_overflow(void)
{
#ifdef WL_SESSION_TEST_HOOKS
    wl_col_session_t session = {0};
    wl_columnar_eval_cleanup_receipt_key_t key;
    wl_columnar_eval_stack_test_receipt_next_session_generation(UINT64_MAX);
    CHECK(wl_columnar_eval_stack_cleanup_receipt_reserve(&session, 17, 99,
        &key) == EOVERFLOW);
#endif
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
    test_entry_generated_integer_crc();
    test_entry_generated_integer_hash();
    test_entry_generated_reduce_work_limit();
    test_entry_generated_grouped_count_work_limit();
    test_entry_generated_grouped_numeric_reduce_work_limit();
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
test_entry_generated_join_abort_cleanup_retry(void)
{
    int64_t input_rows[64], right_rows[513 * 2];
    entry_large_inputs(input_rows, right_rows, false);
    const entry_input_t inputs[] = {
        {"input", input_rows, 64, 1},
        {"right", right_rows, 513, 2}
    };
    const uint32_t candidates = 64u * 257u;
    entry_fixture_t f;
    entry_init_inputs(&f, entry_filter_product, inputs, 2, 0);
    (void)entry_assert_generated_join(&f, "right", 0);
    col_rel_t **old_rels = f.session->rels;
    uint32_t old_nrels = f.session->nrels;
    col_rel_t *input = session_find_rel(f.session, "input");
    col_rel_t *right = session_find_rel(f.session, "right");
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.session->memory_governor));

    entry_join_probe_t probe = {.operation = f.operation};
    entry_join_probe = &probe;
    wl_columnar_join_test_attempt_checkpoint = entry_join_checkpoint;
    CHECK(!entry_turn(&f, false, 0));
    entry_until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    entry_join_probe = NULL;
    CHECK(probe.phase17_hits == candidates);

    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
    CHECK(ctx && ctx->abort_supported && ctx->candidate && !ctx->committed);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(ctx->candidate, &reader));

    /* The framed host serializes JOIN work on its owner thread, so ABORT is
     * issued after JOIN work has returned while its result remains private.
     * The held reader forces candidate cleanup to retain and retry. */
    CHECK(!entry_turn_action(&f,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(entry_context(&f)->abort_requested);
    bool cleanup_refused = false;
    for (unsigned turn = 0; turn < 200000; ++turn) {
        if (entry_context(&f)->cleanup_result == EBUSY) {
            cleanup_refused = true;
            break;
        }
        CHECK(entry_context(&f)
            && !entry_turn(&f, false, 0));
    }
    CHECK(cleanup_refused && !f.reply.committed
        && !session_rel_hash_lookup(f.session, "output"));
    CHECK(!col_rel_source_reader_release(&reader));
    entry_complete(&f);
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !f.reply.committed && !session_rel_hash_lookup(f.session, "output"));
    entry_assert_unpublished_and_unchanged(&f, old_rels, old_nrels, input,
        right, input_rows, right_rows, reserved);
    entry_finish_original(&f);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && report.result == WL_EVALUATION_CONTROL_CANCELLED
        && report.consumed > 0);

    entry_retry_after_finished_stop(&f, entry_filter_product, inputs, 2);
    entry_destroy(&f);
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
    request.operation_identity = f->operation_identity;
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
test_entry_generated_lftj(void)
{
    static const char source[] =
        ".decl input(x:int64)\n.decl a(x:int64)\n.decl b(x:int64)\n"
        ".decl output(x:int64)\noutput(x) :- input(x), a(x), b(x).\n";
    const entry_input_t inputs[] = {
        {"input", entry_rows, 4, 1},
        {"a", entry_rows, 4, 1},
        {"b", entry_rows, 4, 1}
    };
    entry_fixture_t f;
    entry_init_inputs(&f, source, inputs, 3, 0);
    bool lftj = false;
    bool map = false, consolidate = false;
    for (uint32_t i = 0; i < f.relation->op_count; ++i) {
        lftj |= f.relation->ops[i].op == WL_PLAN_OP_LFTJ;
        map |= f.relation->ops[i].op == WL_PLAN_OP_MAP;
        consolidate |= f.relation->ops[i].op == WL_PLAN_OP_CONSOLIDATE;
    }
    CHECK(lftj && map && consolidate);
    uint64_t before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.session->memory_governor));
    CHECK(!entry_turn(&f, false, 0));
    entry_until(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    CHECK(!entry_turn(&f, false, 0));
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
    CHECK(ctx && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && !ctx->abort_supported);
    wl_evaluation_control_t *pending_funding = ctx->pending_funding;
    uint64_t pending_funding_id = ctx->pending_funding_id;
    wl_columnar_eval_plan_numeric_phase_t child_phase = ctx->child.phase;
    CHECK(entry_turn_action(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0)
        == ENOTSUP);
    CHECK(entry_context(&f) == ctx
        && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && ctx->pending_funding == pending_funding
        && ctx->pending_funding_id == pending_funding_id
        && ctx->child.phase == child_phase && !ctx->abort_requested
        && f.previous.previous_valid
        && f.previous.previous_funding == pending_funding
        && f.previous.previous_funding_id == pending_funding_id);
    entry_complete(&f);
    CHECK(!f.reply.execution_result && f.reply.committed);
    col_rel_t *output = session_find_rel(f.session, "output");
    CHECK(output && output->ncols == 1 && output->nrows == 3);
    CHECK(col_rel_get(output, 0, 0) == 1
        && col_rel_get(output, 1, 0) == 2
        && col_rel_get(output, 2, 0) == 3);
    entry_destroy(&f);

    entry_fixture_t measured;
    entry_init_inputs(&measured, source, inputs, 3, 0);
    entry_complete(&measured);
    entry_finish_original(&measured);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(measured.operation, &report)
        && report.consumed > 1);
    uint64_t limit = report.consumed - 1;
    uint64_t exact_limit = report.consumed;
    entry_destroy(&measured);

    entry_fixture_t exact;
    entry_init_inputs(&exact, source, inputs, 3, exact_limit);
    entry_complete(&exact);
    CHECK(!exact.reply.execution_result && exact.reply.committed);
    entry_destroy(&exact);

    entry_fixture_t stopped;
    entry_init_inputs(&stopped, source, inputs, 3, limit);
    col_rel_t **rels_before = stopped.session->rels;
    uint32_t nrels_before = stopped.session->nrels;
    uint64_t reserved_before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(stopped.session->memory_governor));
    entry_complete(&stopped);
    CHECK(stopped.reply.execution_result ==
        WL_EVALUATION_CONTROL_WORK_LIMIT && !stopped.reply.committed
        && !session_rel_hash_lookup(stopped.session, "output")
        && stopped.session->rels == rels_before
        && stopped.session->nrels == nrels_before
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            stopped.session->memory_governor)) == reserved_before);
    entry_retry_after_stop(&stopped, source, inputs, 3);
    entry_destroy(&stopped);
}

static void
test_entry_generated_lftj_float_nonleading_duplicate_keys(void)
{
    static const char source[] =
        ".decl input(k:float, x:int64)\n"
        ".decl a(y:int64, k:float)\n"
        ".decl b(z:int64, k:float)\n"
        ".decl output(k:float, x:int64, y:int64, z:int64)\n"
        "output(k,x,y,z) :- input(k,x), a(y,k), b(z,k).\n";
    const int64_t input_rows[] = {
        (int64_t)0x4004000000000000, 10,
        (int64_t)0x3ff0000000000000, 11,
        (int64_t)0x4004000000000000, 12,
        (int64_t)0xc008000000000000, 13
    };
    const int64_t a_rows[] = {
        20, (int64_t)0x4004000000000000,
        21, (int64_t)0x3ff0000000000000,
        22, (int64_t)0x4004000000000000,
        23, (int64_t)0xc008000000000000
    };
    const int64_t b_rows[] = {
        30, (int64_t)0x4004000000000000,
        31, (int64_t)0x3ff0000000000000,
        32, (int64_t)0x4004000000000000,
        33, (int64_t)0xc008000000000000
    };
    const entry_input_t inputs[] = {
        {"input", input_rows, 4, 2},
        {"a", a_rows, 4, 2},
        {"b", b_rows, 4, 2}
    };
    entry_fixture_t f;
    entry_init_inputs(&f, source, inputs, 3, 0);
    const wl_plan_op_lftj_t *metadata = NULL;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_LFTJ)
            metadata = f.relation->ops[i].opaque_data;
    CHECK(metadata && metadata->k == 3
        && metadata->key_cols[0] == 0
        && metadata->key_cols[1] == 1
        && metadata->key_cols[2] == 1);
    entry_complete(&f);
    CHECK(!f.reply.execution_result && f.reply.committed);
    const col_rel_t *output = session_rel_hash_lookup(f.session, "output");
    CHECK(output && output->nrows == 10);
    static const int64_t expected[][4] = {
        {(int64_t)0xc008000000000000, 13, 23, 33},
        {0x3ff0000000000000, 11, 21, 31},
        {0x4004000000000000, 10, 20, 30},
        {0x4004000000000000, 10, 20, 32},
        {0x4004000000000000, 10, 22, 30},
        {0x4004000000000000, 10, 22, 32},
        {0x4004000000000000, 12, 20, 30},
        {0x4004000000000000, 12, 20, 32},
        {0x4004000000000000, 12, 22, 30},
        {0x4004000000000000, 12, 22, 32}
    };
    bool seen[sizeof(expected) / sizeof(expected[0])] = {false};
    CHECK(output->ncols == 4);
    for (uint32_t row = 0; row < output->nrows; ++row) {
        bool found = false;
        for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); ++i) {
            if (output->columns[0][row] == expected[i][0]
                && output->columns[1][row] == expected[i][1]
                && output->columns[2][row] == expected[i][2]
                && output->columns[3][row] == expected[i][3]) {
                CHECK(!seen[i]);
                seen[i] = true;
                found = true;
                break;
            }
        }
        CHECK(found);
    }
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); ++i)
        CHECK(seen[i]);
    entry_compare_inputs(&f, source, inputs, 3);
    entry_finish_original(&f);
    entry_destroy(&f);
}

static void
test_entry_generated_lftj_widest_output_row(void)
{
    enum { INPUT_COUNT = 8, INPUT_WIDTH = 8, OUTPUT_WIDTH = 64 };
    char source[8192] = {0};
    size_t used = 0;
    int n = 0;
    for (uint32_t r = 0; r < INPUT_COUNT; ++r) {
        n = snprintf(source + used, sizeof(source) - used,
                ".decl r%u(k:int64", r);
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
        for (uint32_t c = 1; c < INPUT_WIDTH; ++c) {
            n = snprintf(source + used, sizeof(source) - used,
                    ",v%u_%u:int64", r, c);
            CHECK(n > 0 && (size_t)n < sizeof(source) - used);
            used += (size_t)n;
        }
        n = snprintf(source + used, sizeof(source) - used, ")\n");
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
    }
    n = snprintf(source + used, sizeof(source) - used, ".decl output(");
    CHECK(n > 0 && (size_t)n < sizeof(source) - used);
    used += (size_t)n;
    for (uint32_t c = 0; c < OUTPUT_WIDTH; ++c) {
        n = snprintf(source + used, sizeof(source) - used, "%so%u:int64",
                c ? "," : "", c);
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
    }
    n = snprintf(source + used, sizeof(source) - used, ")\noutput(");
    CHECK(n > 0 && (size_t)n < sizeof(source) - used);
    used += (size_t)n;
    for (uint32_t r = 0; r < INPUT_COUNT; ++r) {
        for (uint32_t c = 0; c < INPUT_WIDTH; ++c) {
            n = snprintf(source + used, sizeof(source) - used, "%s%s",
                    r || c ? "," : "", c ? "v" : "k");
            CHECK(n > 0 && (size_t)n < sizeof(source) - used);
            used += (size_t)n;
            if (c) {
                n = snprintf(source + used, sizeof(source) - used, "%u_%u",
                        r, c);
                CHECK(n > 0 && (size_t)n < sizeof(source) - used);
                used += (size_t)n;
            }
        }
    }
    n = snprintf(source + used, sizeof(source) - used, ") :- ");
    CHECK(n > 0 && (size_t)n < sizeof(source) - used);
    used += (size_t)n;
    for (uint32_t r = 0; r < INPUT_COUNT; ++r) {
        n = snprintf(source + used, sizeof(source) - used, "%sr%u(k",
                r ? "," : "", r);
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
        for (uint32_t c = 1; c < INPUT_WIDTH; ++c) {
            n = snprintf(source + used, sizeof(source) - used, ",v%u_%u",
                    r, c);
            CHECK(n > 0 && (size_t)n < sizeof(source) - used);
            used += (size_t)n;
        }
        n = snprintf(source + used, sizeof(source) - used, ")%s",
                r + 1u == INPUT_COUNT ? ".\n" : "");
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
    }

    char names[INPUT_COUNT][8];
    int64_t rows[INPUT_COUNT][INPUT_WIDTH];
    entry_input_t inputs[INPUT_COUNT];
    for (uint32_t r = 0; r < INPUT_COUNT; ++r) {
        n = snprintf(names[r], sizeof(names[r]), "r%u", r);
        CHECK(n > 0 && (size_t)n < sizeof(names[r]));
        rows[r][0] = 7;
        for (uint32_t c = 1; c < INPUT_WIDTH; ++c)
            rows[r][c] = 100 * (int64_t)r + c;
        inputs[r] = (entry_input_t){names[r], rows[r], 1, INPUT_WIDTH};
    }

    entry_fixture_t f;
    entry_init_inputs(&f, source, inputs, INPUT_COUNT, 0);
    const wl_plan_op_lftj_t *metadata = NULL;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        if (f.relation->ops[i].op == WL_PLAN_OP_LFTJ)
            metadata = f.relation->ops[i].opaque_data;
    CHECK(metadata && metadata->k == INPUT_COUNT);
    entry_complete(&f);
    CHECK(!f.reply.execution_result && f.reply.committed);
    const col_rel_t *output = session_rel_hash_lookup(f.session, "output");
    CHECK(output && output->nrows == 1 && output->ncols == OUTPUT_WIDTH);
    for (uint32_t r = 0; r < INPUT_COUNT; ++r) {
        CHECK(output->columns[r * INPUT_WIDTH][0] == 7);
        for (uint32_t c = 1; c < INPUT_WIDTH; ++c)
            CHECK(output->columns[r * INPUT_WIDTH + c][0]
                == 100 * (int64_t)r + c);
    }
    entry_destroy(&f);
}

static bool
entry_lftj_phase_reached(entry_fixture_t *f,
    wl_columnar_lftj_attempt_phase_t target,
    wl_columnar_lftj_join_phase_t join_target)
{
    for (unsigned turns = 0; f->reply.kind !=
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL; ++turns) {
        CHECK(turns < 200000);
        wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(f);
        if (ctx && ctx->child.lftj.identity == &ctx->child.lftj
            && ctx->child.lftj.phase == target
            && (target != WL_COLUMNAR_LFTJ_ATTEMPT_JOIN
            || ctx->child.lftj.join_phase == join_target))
            return true;
        (void)entry_turn(f, false, 0);
    }
    return false;
}

static uint64_t
entry_lftj_phase_minimum_limit(const char *source,
    const entry_input_t *inputs, uint32_t input_count,
    wl_columnar_lftj_attempt_phase_t target,
    wl_columnar_lftj_join_phase_t join_target)
{
    entry_fixture_t baseline;
    entry_init_inputs(&baseline, source, inputs, input_count, 0);
    entry_complete(&baseline);
    CHECK(!baseline.reply.execution_result && baseline.reply.committed);
    entry_finish_original(&baseline);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(baseline.operation, &report)
        && report.consumed > 0);
    uint64_t low = 1, high = report.consumed;
    entry_destroy(&baseline);

    while (low < high) {
        uint64_t middle = low + (high - low) / 2u;
        entry_fixture_t trial;
        entry_init_inputs(&trial, source, inputs, input_count, middle);
        bool reached = entry_lftj_phase_reached(&trial, target, join_target);
        entry_complete(&trial);
        CHECK(trial.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
            || (!trial.reply.execution_result && trial.reply.committed));
        entry_finish_original(&trial);
        entry_destroy(&trial);
        if (reached) high = middle;
        else low = middle + 1u;
    }
    return low;
}

static void
entry_assert_lftj_inputs_unchanged(entry_fixture_t *f,
    const entry_input_t *inputs, uint32_t input_count)
{
    for (uint32_t i = 0; i < input_count; ++i) {
        const col_rel_t *rel = session_find_rel(f->session, inputs[i].name);
        CHECK(rel && rel->nrows == inputs[i].count
            && rel->ncols == inputs[i].width);
        for (uint32_t r = 0; r < inputs[i].count; ++r)
            for (uint32_t c = 0; c < inputs[i].width; ++c)
                CHECK(rel->columns[c][r]
                    == inputs[i].rows[(size_t)r * inputs[i].width + c]);
    }
}

static void
test_entry_generated_lftj_stops_inside_child(void)
{
    static const char source[] =
        ".decl input(x:int64)\n.decl a(x:int64)\n.decl b(x:int64)\n"
        ".decl output(x:int64)\noutput(x) :- input(x), a(x), b(x).\n";
    const entry_input_t inputs[] = {
        {"input", entry_rows, 4, 1},
        {"a", entry_rows, 4, 1},
        {"b", entry_rows, 4, 1}
    };
    const struct {
        wl_columnar_lftj_attempt_phase_t phase;
        wl_columnar_lftj_join_phase_t join_phase;
        bool cancel;
    } targets[] = {
        {WL_COLUMNAR_LFTJ_ATTEMPT_COPY, WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK,
         false},
        {WL_COLUMNAR_LFTJ_ATTEMPT_SORT, WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK,
         false},
        {WL_COLUMNAR_LFTJ_ATTEMPT_JOIN,
         WL_COLUMNAR_LFTJ_JOIN_PRODUCT_EMIT, false},
        {WL_COLUMNAR_LFTJ_ATTEMPT_SORT, WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK,
         true}
    };

    for (size_t i = 0; i < sizeof(targets) / sizeof(targets[0]); ++i) {
        int expected_result = targets[i].cancel
            ? WL_EVALUATION_CONTROL_CANCELLED
            : WL_EVALUATION_CONTROL_WORK_LIMIT;
        uint64_t limit = targets[i].cancel ? 0
            : entry_lftj_phase_minimum_limit(source, inputs, 3,
                targets[i].phase, targets[i].join_phase);
        entry_fixture_t stopped;
        entry_init_inputs(&stopped, source, inputs, 3, limit);
        col_rel_t **old_rels = stopped.session->rels;
        uint32_t old_nrels = stopped.session->nrels;
        uint32_t old_arr_count = stopped.session->arr_count;
        col_arr_entry_t *old_arr_entries = stopped.session->arr_entries;
        uint64_t reserved = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(
                stopped.session->memory_governor));
        CHECK(entry_lftj_phase_reached(&stopped, targets[i].phase,
            targets[i].join_phase));
        wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&stopped);
        CHECK(ctx);
        if (targets[i].cancel)
            wl_evaluation_control_request_cancel(stopped.operation);
        for (unsigned turns = 0; !ctx->child.lftj.semantic_result; ++turns) {
            CHECK(turns < 100);
            (void)entry_turn(&stopped, false, 0);
        }
        CHECK(ctx->child.lftj.semantic_result == expected_result);
        entry_complete(&stopped);
        CHECK(stopped.reply.execution_result == expected_result
            && !stopped.reply.committed
            && !session_rel_hash_lookup(stopped.session, "output")
            && stopped.session->rels == old_rels
            && stopped.session->nrels == old_nrels
            && stopped.session->arr_count == old_arr_count
            && stopped.session->arr_entries == old_arr_entries
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                stopped.session->memory_governor)) == reserved);
        entry_assert_lftj_inputs_unchanged(&stopped, inputs, 3);
        entry_retry_after_stop(&stopped, source, inputs, 3);
        entry_destroy(&stopped);
    }
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
    test_entry_generated_join_abort_cleanup_retry();
    test_entry_generated_right_filter_collisions_and_stop();
    test_entry_generated_right_filter_product_work_boundary();
    test_entry_receipts(); test_entry_abort_and_unsupported_abort();
    test_entry_abort_publication_cutoff();
    test_entry_receipt_only_continuation();
    test_cleanup_receipt_transfer_retry();
    test_entry_bootstrap_failures();
    test_entry_refuses_join_batch_knob();
    test_entry_retained_publication(); test_entry_reverse();
    test_entry_authority(); test_entry_generated_lftj();
    test_entry_generated_lftj_float_nonleading_duplicate_keys();
    test_entry_generated_lftj_widest_output_row();
    test_entry_generated_lftj_stops_inside_child();
    test_entry_generated_filter_work_boundary();

    test_shapes(); test_generated_union(); test_refusal_and_identity();
    test_failures(); test_retained_and_consume_refusal();
    test_growth_quota_and_retry(); test_bootstrap_limits();
    test_cleanup_receipt_attempt_identity();
    test_publication_shapes(); test_publication_reverse();
    test_publication_failures();
    test_cleanup_receipt_generation_overflow();
    puts("evaluation_serial: real result bridge, publication and drain passed");
    return 0;
}
