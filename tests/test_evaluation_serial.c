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
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if !defined(_WIN32)
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

extern void (*wl_columnar_session_test_observed_preflight_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
extern bool wl_columnar_eval_delta_test_fail_observer_stream_release_once;
extern bool wl_columnar_eval_delta_test_fail_prepare_current_alloc;
extern uint32_t wl_columnar_eval_delta_test_stream_validation_rows;
extern size_t wl_columnar_session_test_observer_precharged_events;
extern void (*wl_columnar_session_test_staging_prepared)(
    wl_evaluation_control_t *);
extern void (*wl_columnar_session_test_before_successor_registration)(
    col_rel_t *);
extern void (*wl_columnar_session_test_successor_registry_charge)(
    wl_evaluation_control_t *, unsigned, uint32_t);
extern void (*wl_columnar_eval_delta_test_observer_boundary)(
    wl_col_session_t *, unsigned);
extern void (*wl_columnar_eval_serial_test_framed_checkpoint)(
    wl_columnar_eval_serial_framed_attempt_t *);

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
    wl_evaluation_control_t *cancel_control;
    bool cancel_on_first;
    uint32_t count;
    char relations[16][32];
    uint32_t widths[16];
    int64_t rows[16][8];
} snapshot_capture_t;

static void
capture_snapshot_tuple(const char *relation, const int64_t *row,
    uint32_t ncols, void *user_data)
{
    snapshot_capture_t *capture = user_data;
    CHECK(capture && capture->count < 16 && relation && strlen(relation) < 32
        && ncols <= 8 && (row || ncols == 0));
    uint32_t at = capture->count++;
    strcpy(capture->relations[at], relation);
    capture->widths[at] = ncols;
    if (ncols)
        memcpy(capture->rows[at], row, (size_t)ncols * sizeof(*row));
    if (capture->cancel_on_first && at == 0)
        wl_evaluation_control_request_cancel(capture->cancel_control);
}

static void
check_snapshot_equal(const snapshot_capture_t *left,
    const snapshot_capture_t *right)
{
    CHECK(left->count == right->count);
    for (uint32_t i = 0; i < left->count; ++i) {
        CHECK(!strcmp(left->relations[i], right->relations[i]));
        CHECK(left->widths[i] == right->widths[i]);
        CHECK(!memcmp(left->rows[i], right->rows[i],
            (size_t)left->widths[i] * sizeof(left->rows[i][0])));
    }
}

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
            false, grant->control, grant->id);
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
        &plan, f.operation, f.id + 1, f.operation_identity, false,
        grant->control,
        grant->id) == EINVAL);
    CHECK(wl_columnar_eval_serial_framed_attempt_begin(f.frame, &f.session,
        &plan, f.operation, f.id, f.operation_identity + 1, false,
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
        if (governed) {
            session_rel_free_hash(&f.session);
            CHECK(atomic_load_explicit(&f.session.rel_hash_reservation.state,
                memory_order_seq_cst)
                == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
            CHECK(wl_columnar_memory_reserve_checked(
                    wl_columnar_memory_governor_ref_get(
                        f.session.memory_governor),
                    1, &f.session.rel_hash_reservation)
                == WL_COLUMNAR_MEMORY_ADMISSION_OK);
            CHECK(wl_columnar_memory_rollback(&f.session.rel_hash_reservation));
        }else {
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
    CHECK(wl_session_step(&f->session->base) == EBUSY);
    CHECK(wl_session_snapshot(&f->session->base, NULL, NULL) == EBUSY);
    CHECK(f->session->base.backend->session_step(&f->session->base) == EBUSY);
    CHECK(f->session->base.backend->session_snapshot(&f->session->base,
        NULL, NULL) == EBUSY);
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
test_controlled_session_step_resume(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    const int64_t rows[] = {7, 3, 11};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(1, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_insert(session, "input", rows, 3, 1));

    CHECK(wl_session_step(session) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !session_find_rel((wl_col_session_t *)session, "output"));
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));

    wl_columnar_session_test_fail_framed_funding_before_stop = 1;
    CHECK(wl_session_step(session) == EAGAIN);
    uint64_t first_id = wl_evaluation_control_active_attempt_id(control);
    CHECK(first_id != 0);
    CHECK(wl_session_insert(session, "input", rows, 1, 1) == EBUSY);
    CHECK(wl_session_snapshot(session, NULL, NULL) == EBUSY);
    CHECK(wl_evaluation_control_reset(control) == EBUSY);
    CHECK(wl_evaluation_control_configure(control, 100) == EBUSY);

    int resumed_rc = wl_session_step(session);
    CHECK(!resumed_rc);
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.attempt_id == first_id && report.result == 0);
    const col_rel_t *output = session_find_rel((wl_col_session_t *)session,
            "output");
    CHECK(output && output->nrows == 3);
    CHECK(output->columns[0][0] == 3 && output->columns[0][1] == 7
        && output->columns[0][2] == 11);

    /* Completed controlled STEP is a stable no-op on later calls. */
    int step_rc = EAGAIN;
    for (unsigned turn = 0; turn < 1000 && step_rc == EAGAIN; ++turn)
        step_rc = wl_session_step(session);
    CHECK(!step_rc);
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.attempt_id == first_id + 1 && report.result == 0
        && report.consumed == 0);
    output = session_find_rel((wl_col_session_t *)session, "output");
    CHECK(output && output->nrows == 3);

    CHECK(!wl_session_try_destroy(&session) && !session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
check_recursive_manifest_op_defaults(const wl_plan_op_t *op)
{
    CHECK(op->filter_expr.data == NULL && op->filter_expr.size == 0);
    CHECK(op->agg_expr.data == NULL && op->agg_expr.size == 0);
    CHECK(op->agg_fn == WIRELOG_AGG_COUNT);
    CHECK(op->group_by_indices == NULL && op->group_by_count == 0
        && op->aggregate_index == 0);
    CHECK(op->right_filter_expr.data == NULL
        && op->right_filter_expr.size == 0);
    CHECK(op->agg_operand_type == WL_PLAN_AGG_OPERAND_UNKNOWN
        && op->agg_result_type == WL_PLAN_AGG_OPERAND_UNKNOWN);
    CHECK(op->delta_mode == WL_DELTA_AUTO && !op->materialized);

    if (op->op != WL_PLAN_OP_VARIABLE)
        CHECK(op->relation_name == NULL);
    if (op->op != WL_PLAN_OP_JOIN)
        CHECK(op->right_relation == NULL && op->left_keys == NULL
            && op->right_keys == NULL && op->key_count == 0);
    if (op->op != WL_PLAN_OP_MAP && op->op != WL_PLAN_OP_JOIN)
        CHECK(op->project_indices == NULL && op->project_count == 0);
    if (op->op != WL_PLAN_OP_MAP)
        CHECK(op->map_exprs == NULL && op->map_expr_count == 0);
    if (op->op != WL_PLAN_OP_EXCHANGE)
        CHECK(op->opaque_data == NULL);
}

static void
test_recursive_reachability_plan_manifest(void)
{
    static const char source[] =
        ".decl edge(x:int64, y:int64)\n"
        ".decl path(x:int64, y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,z) :- path(x,y), edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    CHECK(plan->edb_count == 1 && plan->stratum_count == 1);
    CHECK(!strcmp(plan->edb_relations[0], "edge")
        && plan->edb_declared_width[0] == 2
        && plan->edb_column_type_counts[0] == 2
        && plan->edb_column_types[0][0] == WIRELOG_TYPE_INT64
        && plan->edb_column_types[0][1] == WIRELOG_TYPE_INT64
        && !plan->edb_has_graph_column[0]
        && plan->edb_graph_col_index[0] == 0);

    const wl_plan_stratum_t *stratum = &plan->strata[0];
    CHECK(stratum->stratum_id == 0 && stratum->is_recursive
        && stratum->is_monotone && !stratum->has_aggregate
        && stratum->relation_count == 1 && stratum->rule_refs
        && stratum->bodies);
    const wl_plan_relation_t *relation = &stratum->relations[0];
    CHECK(!strcmp(relation->name, "path")
        && !strcmp(relation->delta_name, "$d$path")
        && relation->op_count == 7 && !relation->recursive_agg.has_spec
        && relation->recursive_agg.fn == WIRELOG_AGG_COUNT
        && relation->recursive_agg.operand_type
        == WL_PLAN_AGG_OPERAND_UNKNOWN
        && relation->recursive_agg.group_by_count == 0
        && relation->recursive_agg.aggregate_index == 0);
    CHECK(stratum->rule_refs[0].count == 2
        && !strcmp(stratum->rule_refs[0].names[0], "edge")
        && !strcmp(stratum->rule_refs[0].names[1], "path"));

    static const wl_plan_op_type_t body_ops[] = {
        WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP, WL_PLAN_OP_VARIABLE,
        WL_PLAN_OP_JOIN, WL_PLAN_OP_MAP, WL_PLAN_OP_CONCAT,
        WL_PLAN_OP_CONSOLIDATE
    };
    const wl_plan_body_t *body = &stratum->bodies[0];
    CHECK(body->op_count == sizeof(body_ops) / sizeof(body_ops[0]));
    for (uint32_t i = 0; i < body->op_count; i++) {
        CHECK(body->ops[i].op == body_ops[i]);
        check_recursive_manifest_op_defaults(&body->ops[i]);
    }
    CHECK(!strcmp(body->ops[0].relation_name, "edge")
        && !strcmp(body->ops[2].relation_name, "path")
        && !strcmp(body->ops[3].right_relation, "edge")
        && body->ops[3].key_count == 1
        && !strcmp(body->ops[3].left_keys[0], "col1")
        && !strcmp(body->ops[3].right_keys[0], "col0"));
    static const uint8_t body_map_col0[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'
    };
    static const uint8_t body_map_col1[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'
    };
    static const uint8_t body_map_col3[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '3'
    };
    CHECK(body->ops[1].project_count == 2
        && body->ops[1].project_indices[0] == 0
        && body->ops[1].project_indices[1] == 1
        && body->ops[1].map_expr_count == 2
        && body->ops[1].map_exprs[0].size == sizeof(body_map_col0)
        && !memcmp(body->ops[1].map_exprs[0].data, body_map_col0,
        sizeof(body_map_col0))
        && body->ops[1].map_exprs[1].size == sizeof(body_map_col1)
        && !memcmp(body->ops[1].map_exprs[1].data, body_map_col1,
        sizeof(body_map_col1)));
    CHECK(body->ops[3].project_indices == NULL
        && body->ops[3].project_count == 0);
    CHECK(body->ops[4].project_count == 2
        && body->ops[4].project_indices[0] == 0
        && body->ops[4].project_indices[1] == 3
        && body->ops[4].map_expr_count == 2
        && body->ops[4].map_exprs[0].size == sizeof(body_map_col0)
        && !memcmp(body->ops[4].map_exprs[0].data, body_map_col0,
        sizeof(body_map_col0))
        && body->ops[4].map_exprs[1].size == sizeof(body_map_col3)
        && !memcmp(body->ops[4].map_exprs[1].data, body_map_col3,
        sizeof(body_map_col3)));

    static const wl_plan_op_type_t lowered_ops[] = {
        WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP, WL_PLAN_OP_VARIABLE,
        WL_PLAN_OP_JOIN, WL_PLAN_OP_CONCAT, WL_PLAN_OP_CONSOLIDATE,
        WL_PLAN_OP_EXCHANGE
    };
    for (uint32_t i = 0; i < relation->op_count; i++) {
        CHECK(relation->ops[i].op == lowered_ops[i]);
        check_recursive_manifest_op_defaults(&relation->ops[i]);
    }
    const wl_plan_op_t *ops = relation->ops;
    CHECK(!strcmp(ops[0].relation_name, "edge")
        && !strcmp(ops[2].relation_name, "path")
        && ops[0].delta_mode == WL_DELTA_AUTO
        && ops[2].delta_mode == WL_DELTA_AUTO);
    CHECK(ops[1].project_count == 2 && ops[1].project_indices[0] == 0
        && ops[1].project_indices[1] == 1 && ops[1].map_expr_count == 2);
    static const uint8_t map_col0[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'
    };
    static const uint8_t map_col1[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'
    };
    CHECK(ops[1].map_exprs[0].size == sizeof(map_col0)
        && !memcmp(ops[1].map_exprs[0].data, map_col0, sizeof(map_col0))
        && ops[1].map_exprs[1].size == sizeof(map_col1)
        && !memcmp(ops[1].map_exprs[1].data, map_col1, sizeof(map_col1)));
    CHECK(!strcmp(ops[3].right_relation, "edge")
        && ops[3].key_count == 1
        && !strcmp(ops[3].left_keys[0], "col1")
        && !strcmp(ops[3].right_keys[0], "col0")
        && ops[3].project_count == 2
        && ops[3].project_indices[0] == 0
        && ops[3].project_indices[1] == 3);
    CHECK(ops[1].relation_name == NULL && ops[1].right_relation == NULL
        && ops[1].left_keys == NULL && ops[1].right_keys == NULL
        && ops[1].key_count == 0 && !ops[1].materialized);
    CHECK(ops[3].relation_name == NULL && ops[3].map_exprs == NULL
        && ops[3].map_expr_count == 0 && !ops[3].materialized);
    CHECK(ops[4].relation_name == NULL && ops[4].right_relation == NULL
        && ops[4].project_indices == NULL && ops[4].map_exprs == NULL
        && ops[4].opaque_data == NULL);
    CHECK(ops[5].relation_name == NULL && ops[5].right_relation == NULL
        && ops[5].project_indices == NULL && ops[5].map_exprs == NULL
        && ops[5].opaque_data == NULL);
    CHECK(ops[6].relation_name == NULL && ops[6].right_relation == NULL
        && ops[6].left_keys == NULL && ops[6].right_keys == NULL
        && ops[6].key_count == 0 && ops[6].project_indices == NULL
        && ops[6].project_count == 0 && ops[6].map_exprs == NULL
        && ops[6].map_expr_count == 0);
    const wl_plan_op_exchange_t *exchange = ops[6].opaque_data;
    CHECK(exchange && exchange->num_workers == 0
        && exchange->key_col_count == 1 && exchange->key_col_idxs[0] == 1
        && !exchange->edb_rel_name && exchange->edb_key_col_count == 1
        && exchange->edb_key_col_idxs[0] == 0);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_recursive_two_join_plan_manifest(void)
{
    static const char source[] =
        ".decl edge(x:int64, y:int64)\n"
        ".decl path(x:int64, y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,w) :- path(x,y), edge(y,z), edge(z,w).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].is_recursive
        && plan->edb_count == 1 && !strcmp(plan->edb_relations[0], "edge"));
    const wl_plan_stratum_t *stratum = &plan->strata[0];
    CHECK(stratum->relation_count == 1);
    const wl_plan_relation_t *relation = &stratum->relations[0];
    CHECK(!strcmp(relation->name, "path")
        && !strcmp(relation->delta_name, "$d$path")
        && relation->op_count == 8);
    static const wl_plan_op_type_t expected_ops[] = {
        WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP, WL_PLAN_OP_VARIABLE,
        WL_PLAN_OP_JOIN, WL_PLAN_OP_JOIN, WL_PLAN_OP_CONCAT,
        WL_PLAN_OP_CONSOLIDATE, WL_PLAN_OP_EXCHANGE
    };
    for (uint32_t i = 0; i < relation->op_count; ++i) {
        CHECK(relation->ops[i].op == expected_ops[i]);
        check_recursive_manifest_op_defaults(&relation->ops[i]);
    }
    const wl_plan_op_t *ops = relation->ops;
    CHECK(!strcmp(ops[0].relation_name, "edge")
        && !strcmp(ops[2].relation_name, "path")
        && !strcmp(ops[3].right_relation, "edge")
        && !strcmp(ops[4].right_relation, "edge"));
    CHECK(ops[1].project_count == 2 && ops[1].project_indices
        && ops[1].project_indices[0] == 0
        && ops[1].project_indices[1] == 1
        && ops[1].map_expr_count == 2 && ops[1].map_exprs);
    static const uint8_t map_col0[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'
    };
    static const uint8_t map_col1[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'
    };
    CHECK(ops[1].map_exprs[0].size == sizeof(map_col0)
        && !memcmp(ops[1].map_exprs[0].data, map_col0, sizeof(map_col0))
        && ops[1].map_exprs[1].size == sizeof(map_col1)
        && !memcmp(ops[1].map_exprs[1].data, map_col1, sizeof(map_col1)));
    for (uint32_t i = 3; i <= 4; ++i) {
        CHECK(ops[i].key_count == 1 && ops[i].left_keys
            && ops[i].right_keys
            && !strcmp(ops[i].right_keys[0], "col0"));
    }
    CHECK(!strcmp(ops[3].left_keys[0], "col1")
        && ops[3].project_count == 0 && !ops[3].project_indices
        && !strcmp(ops[4].left_keys[0], "col3")
        && ops[4].project_count == 2 && ops[4].project_indices
        && ops[4].project_indices[0] == 0
        && ops[4].project_indices[1] == 5);
    const wl_plan_op_exchange_t *exchange = ops[7].opaque_data;
    CHECK(exchange && exchange->num_workers == 0
        && exchange->key_col_count == 1 && exchange->key_col_idxs
        && exchange->key_col_idxs[0] == 1 && !exchange->edb_rel_name
        && exchange->edb_key_col_count == 0
        && !exchange->edb_key_col_idxs);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static unsigned controlled_recursive_stop_phase;
static bool controlled_recursive_stop_fired;
static bool controlled_recursive_stop_enabled;
static bool controlled_recursive_stop_unpublished;
static bool controlled_recursive_convergence_unpublished;
static int controlled_recursive_edge_write_status;
static wl_col_session_t *controlled_recursive_session_under_test;
static bool controlled_recursive_boundary_enabled;
static bool controlled_recursive_precommit_seen;
static bool controlled_recursive_precommit_old_path_preserved;
static bool controlled_recursive_precommit_snapshot_preserved;
static bool controlled_recursive_postcommit_seen;
static bool controlled_recursive_postcommit_published_once;
static int controlled_recursive_postcommit_edge_write_status;
static const col_rel_t *controlled_recursive_old_path;
static const col_rel_t *controlled_recursive_committed_path;
static uint64_t controlled_recursive_old_path_identity;
static col_relation_snapshot_t controlled_recursive_edge_snapshot;
static uint64_t controlled_recursive_boundary_attempt_id;

static void
controlled_recursive_stop_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint32_t iteration)
{
    if (phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_CONVERGENCE
        && controlled_recursive_session_under_test)
        controlled_recursive_convergence_unpublished
            = !session_find_rel(controlled_recursive_session_under_test,
                "path");
    if (controlled_recursive_boundary_enabled
        && controlled_recursive_session_under_test
        && phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_BEFORE_COMMIT) {
        const col_rel_t *edge = session_find_rel(
            controlled_recursive_session_under_test, "edge");
        controlled_recursive_precommit_seen = true;
        controlled_recursive_precommit_old_path_preserved
            = session_find_rel(controlled_recursive_session_under_test,
                "path") == controlled_recursive_old_path;
        controlled_recursive_precommit_snapshot_preserved = edge
            && wl_columnar_relation_snapshot_equal(
            controlled_recursive_edge_snapshot,
            wl_columnar_relation_snapshot(edge));
        controlled_recursive_boundary_attempt_id
            = wl_evaluation_control_active_attempt_id(control);
        static const int64_t mutation[] = { 5, 6 };
        controlled_recursive_edge_write_status = edge
            ? col_rel_append_row((col_rel_t *)edge, mutation) : EINVAL;
        return;
    }
    if (controlled_recursive_boundary_enabled
        && controlled_recursive_session_under_test
        && phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_COMMIT) {
        const col_rel_t *edge = session_find_rel(
            controlled_recursive_session_under_test, "edge");
        controlled_recursive_committed_path = session_find_rel(
            controlled_recursive_session_under_test, "path");
        controlled_recursive_postcommit_seen = true;
        controlled_recursive_postcommit_published_once
            = controlled_recursive_committed_path
            && controlled_recursive_committed_path
            ->relation_identity != controlled_recursive_old_path_identity
            && controlled_recursive_committed_path->nrows == 10;
        controlled_recursive_boundary_attempt_id
            = wl_evaluation_control_active_attempt_id(control);
        static const int64_t mutation[] = { 5, 6 };
        controlled_recursive_postcommit_edge_write_status = edge
            ? col_rel_append_row((col_rel_t *)edge, mutation) : EINVAL;
        snapshot_capture_t snapshot = {0};
        CHECK(wl_session_snapshot((wl_session_t *)
            controlled_recursive_session_under_test,
            capture_snapshot_tuple, &snapshot) == EBUSY);
        return;
    }
    if (!controlled_recursive_stop_enabled)
        return;
    if (controlled_recursive_stop_fired
        || phase != controlled_recursive_stop_phase)
        return;
    if ((phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_ALLOCATION_PRECHARGE
        || phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_ALLOCATION_ADMITTED)
        && iteration < 2)
        return;
    controlled_recursive_stop_fired = true;
    controlled_recursive_stop_unpublished
        = controlled_recursive_session_under_test
        && !session_find_rel(controlled_recursive_session_under_test, "path");
    if (phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_BODY
        && controlled_recursive_session_under_test) {
        static const int64_t mutation[] = { 4, 5 };
        const col_rel_t *edge = session_find_rel(
            controlled_recursive_session_under_test, "edge");
        controlled_recursive_edge_write_status = edge
            ? col_rel_append_row((col_rel_t *)edge, mutation) : EINVAL;
    }
    if (phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_BODY
        || phase == WL_COLUMNAR_SESSION_TEST_RECURSIVE_ALLOCATION_ADMITTED)
        wl_evaluation_control_request_cancel(control);
    else
        (void)wl_evaluation_control_charge(control, UINT64_MAX);
}

static wl_session_t *
controlled_recursive_session(const int64_t *edges, uint32_t edge_count,
    wl_plan_t **plan_out, wirelog_program_t **program_out,
    wl_evaluation_control_t **control_out)
{
    static const char source[] =
        ".decl edge(x:int64, y:int64)\n"
        ".decl path(x:int64, y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,z) :- path(x,y), edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    /* These source-fixed tariffs certify the legacy full-sort route. The
     * prefix-discovery route has separate boundary and STEP qualification. */
    ((wl_col_session_t *)session)->diff_enabled = false;
    CHECK(!wl_session_insert(session, "edge", edges, edge_count, 2));
    *plan_out = plan;
    *program_out = program;
    *control_out = control;
    return session;
}

static wl_session_t *
controlled_recursive_two_join_session(const int64_t *edges,
    uint32_t edge_count, wl_plan_t **plan_out,
    wirelog_program_t **program_out, wl_evaluation_control_t **control_out)
{
    static const char source[] =
        ".decl edge(x:int64, y:int64)\n"
        ".decl path(x:int64, y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,w) :- path(x,y), edge(y,z), edge(z,w).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    /* These source-fixed tariffs certify the legacy full-sort route. The
     * prefix-discovery route has separate boundary and STEP qualification. */
    ((wl_col_session_t *)session)->diff_enabled = false;
    CHECK(!wl_session_insert(session, "edge", edges, edge_count, 2));
    *plan_out = plan;
    *program_out = program;
    *control_out = control;
    return session;
}

static int
controlled_recursive_run(wl_session_t *session,
    wl_evaluation_control_t *control)
{
    int rc = wl_session_step(session);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    for (unsigned turn = 0; rc == EAGAIN && id && turn < 10000; ++turn) {
        rc = wl_session_step(session);
        if (rc == EAGAIN)
            CHECK(wl_evaluation_control_active_attempt_id(control) == id);
    }
    return rc;
}

static void
check_two_join_path(const col_rel_t *path, uint32_t expected_rows)
{
    static const int64_t expected_initial[][2] = {
        {1, 2}, {1, 4}, {2, 3}, {2, 5}, {3, 4}, {4, 5}
    };
    static const int64_t expected_successor[][2] = {
        {1, 2}, {1, 4}, {1, 6}, {2, 3}, {2, 5},
        {3, 4}, {3, 6}, {4, 5}, {5, 6}
    };
    const int64_t (*expected)[2] = expected_rows == 6
        ? expected_initial : expected_successor;
    CHECK(expected_rows == 6 || expected_rows == 9);
    CHECK(path && path->nrows == expected_rows && path->ncols == 2
        && path->schema_ok && path->col_names
        && !strcmp(path->col_names[0], "x")
        && !strcmp(path->col_names[1], "y")
        && path->sorted_nrows == path->nrows);
    for (uint32_t i = 0; i < expected_rows; ++i)
        CHECK(path->columns[0][i] == expected[i][0]
            && path->columns[1][i] == expected[i][1]);
}

static void
test_controlled_recursive_two_join_step(void)
{
    const int64_t edges[] = {1, 2, 2, 3, 3, 4, 4, 5};
    /* Three body frames: four seed edges, two length-three paths, then
     * empty convergence. Each frame costs 224 per 16-byte zeroing chunk.
     * Each round constructs MAP, two JOINs, CONCAT, CONSOLIDATE, and the
     * detached result: 18 descriptors, zeroed at 224 per eight-byte chunk.
     * Frames and descriptors embed ABI-dependent thread IDs (e.g. Windows).
     * Keep all other work pinned independently of observed consumption. */
    const uint64_t expected_work = UINT64_C(1271246) + UINT64_C(3) * 224
        * ((wl_columnar_eval_stack_test_frame_size() + 15) / 16)
        + UINT64_C(18) * 224 * ((sizeof(col_rel_t) + 7) / 8);
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = controlled_recursive_two_join_session(edges, 4,
            &plan, &program, &control);
    CHECK(!wl_evaluation_control_configure(control, expected_work));
    CHECK(!controlled_recursive_run(session, control));
    const col_rel_t *path = session_find_rel((wl_col_session_t *)session,
            "path");
    check_two_join_path(path, 6);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == 0 && report.consumed == expected_work);

    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
    session = controlled_recursive_two_join_session(edges, 4, &plan,
            &program, &control);
    CHECK(!wl_evaluation_control_configure(control, expected_work - 1));
    CHECK(controlled_recursive_run(session, control)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!session_find_rel((wl_col_session_t *)session, "path"));
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && report.consumed == expected_work - 1);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!controlled_recursive_run(session, control));
    check_two_join_path(session_find_rel((wl_col_session_t *)session, "path"),
        6);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == 0 && report.consumed == expected_work);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static unsigned controlled_two_join_start_count;
static bool controlled_two_join_second_started;
static bool controlled_two_join_stopped;
static bool controlled_two_join_old_path_preserved;
static const col_rel_t *controlled_two_join_old_path;
static wl_col_session_t *controlled_two_join_session_under_test;

static void
controlled_two_join_stop_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    (void)progress;
    if (phase == 1 && ++controlled_two_join_start_count == 2)
        controlled_two_join_second_started = true;
    if (phase == 17 && controlled_two_join_second_started
        && !controlled_two_join_stopped) {
        controlled_two_join_stopped = true;
        controlled_two_join_old_path_preserved
            = controlled_two_join_session_under_test
            && session_find_rel(controlled_two_join_session_under_test,
                "path") == controlled_two_join_old_path
            && controlled_two_join_old_path
            && controlled_two_join_old_path->nrows == 6;
        wl_evaluation_control_request_cancel(control);
    }
}

static void
test_controlled_recursive_two_join_successor_stop(void)
{
    const int64_t edges[] = {1, 2, 2, 3, 3, 4, 4, 5};
    const int64_t next_edge[] = {5, 6};
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = controlled_recursive_two_join_session(edges, 4,
            &plan, &program, &control);
    CHECK(!controlled_recursive_run(session, control));
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    const col_rel_t *old_path = session_find_rel(columnar, "path");
    check_two_join_path(old_path, 6);
    CHECK(!wl_session_insert(session, "edge", next_edge, 1, 2));
    CHECK(session_find_rel(columnar, "path") == old_path
        && old_path->nrows == 6);

    controlled_two_join_start_count = 0;
    controlled_two_join_second_started = false;
    controlled_two_join_stopped = false;
    controlled_two_join_old_path_preserved = false;
    controlled_two_join_old_path = old_path;
    controlled_two_join_session_under_test = columnar;
    wl_columnar_join_test_attempt_checkpoint =
        controlled_two_join_stop_checkpoint;
    int rc = controlled_recursive_run(session, control);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    controlled_two_join_session_under_test = NULL;
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED
        && controlled_two_join_second_started
        && controlled_two_join_stopped
        && controlled_two_join_old_path_preserved);
    CHECK(session_find_rel(columnar, "path") == old_path
        && old_path->nrows == 6);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == WL_EVALUATION_CONTROL_CANCELLED
        && report.consumed > 0);

    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!controlled_recursive_run(session, control));
    check_two_join_path(session_find_rel(columnar, "path"), 9);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_recursive_step_publication_and_stops(void)
{
    const int64_t edges[] = {1, 2, 2, 3, 3, 4};
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = controlled_recursive_session(edges, 3, &plan,
            &program, &control);
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(!session_find_rel(columnar, "path"));
    controlled_recursive_stop_phase =
        WL_COLUMNAR_SESSION_TEST_RECURSIVE_CONVERGENCE;
    controlled_recursive_stop_enabled = false;
    controlled_recursive_stop_fired = false;
    controlled_recursive_convergence_unpublished = false;
    controlled_recursive_session_under_test = columnar;
    wl_columnar_session_test_recursive_step_checkpoint =
        controlled_recursive_stop_checkpoint;
    CHECK(!controlled_recursive_run(session, control));
    wl_columnar_session_test_recursive_step_checkpoint = NULL;
    controlled_recursive_session_under_test = NULL;
    CHECK(controlled_recursive_convergence_unpublished);
    const col_rel_t *path = session_find_rel(columnar, "path");
    wl_evaluation_control_report_t successful_report;
    CHECK(!wl_evaluation_control_report(control, &successful_report));
    /* Pinned single-worker cost for this graph under the documented row,
     * transition, four-lane allocation, and managed-lane precharge tariffs.
     * Limits below use this source-fixed oracle; the unlimited run checks
     * repeatability only. Four body frames cover seed, length-two,
     * length-three, and empty-convergence rounds. Frame zeroing costs
     * 224 per 16-byte chunk. Each round constructs MAP, JOIN, CONCAT,
     * CONSOLIDATE, and the detached result: 20 descriptors, zeroed at 224
     * per eight-byte chunk. Both sizes include ABI-dependent thread IDs. */
    const uint64_t expected_work = UINT64_C(1391081) + UINT64_C(4) * 224
        * ((wl_columnar_eval_stack_test_frame_size() + 15) / 16)
        + UINT64_C(20) * 224 * ((sizeof(col_rel_t) + 7) / 8);
    CHECK(successful_report.consumed == expected_work);
    CHECK(path && path->nrows == 6 && path->ncols == 2
        && path->capacity == 6
        && path->schema_ok && path->col_names
        && !strcmp(path->col_names[0], "x")
        && !strcmp(path->col_names[1], "y")
        && path->sorted_nrows == path->nrows);
    const int64_t expected[][2] = {
        {1, 2}, {1, 3}, {1, 4}, {2, 3}, {2, 4}, {3, 4}
    };
    for (uint32_t i = 0; i < 6; ++i) {
        CHECK(path->columns[0][i] == expected[i][0]
            && path->columns[1][i] == expected[i][1]);
    }
    uint64_t completed_attempt_id = successful_report.attempt_id;
    CHECK(!wl_session_step(session));
    CHECK(!wl_evaluation_control_report(control, &successful_report)
        && successful_report.attempt_id == completed_attempt_id
        && successful_report.consumed == expected_work);
    const int64_t later_edge[] = { 4, 5 };
    const col_rel_t *edge_const = session_find_rel(columnar, "edge");
    CHECK(edge_const && !col_rel_append_row((col_rel_t *)edge_const,
        later_edge));
    snapshot_capture_t stale_capture = {0};
    CHECK(wl_session_snapshot(session, capture_snapshot_tuple,
        &stale_capture) == EBUSY);
    CHECK(wl_session_step(session) == EINVAL);
    CHECK(wl_session_insert(session, "edge", later_edge, 1, 2) == EBUSY);
    CHECK(!wl_evaluation_control_report(control, &successful_report)
        && successful_report.attempt_id == completed_attempt_id
        && successful_report.consumed == expected_work);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);

    session = controlled_recursive_session(edges, 3, &plan, &program,
            &control);
    columnar = (wl_col_session_t *)session;
    CHECK(!wl_evaluation_control_configure(control, expected_work));
    CHECK(!controlled_recursive_run(session, control));
    wl_evaluation_control_report_t boundary_report;
    CHECK(!wl_evaluation_control_report(control, &boundary_report)
        && boundary_report.limit == expected_work
        && boundary_report.consumed == expected_work
        && boundary_report.result == 0
        && session_find_rel(columnar, "path"));
    /* The successful controlled step remains parked until the caller
     * acknowledges completion with its next step call. */
    CHECK(!wl_session_step(session));
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);

    session = controlled_recursive_session(edges, 3, &plan, &program,
            &control);
    columnar = (wl_col_session_t *)session;
    CHECK(!wl_evaluation_control_configure(control, expected_work - 1u));
    int short_rc = controlled_recursive_run(session, control);
    CHECK(short_rc == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !session_find_rel(columnar, "path")
        && !columnar->cleanup_active && !columnar->cleanup_pending);
    CHECK(!wl_evaluation_control_report(control, &boundary_report)
        && boundary_report.limit == expected_work - 1u
        && boundary_report.consumed == expected_work - 1u
        && boundary_report.result == short_rc);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!controlled_recursive_run(session, control));
    CHECK(session_find_rel(columnar, "path"));
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);

    for (unsigned stop_case = 0; stop_case < 5; ++stop_case) {
        session = controlled_recursive_session(edges, 3, &plan, &program,
                &control);
        columnar = (wl_col_session_t *)session;
        if (stop_case != 0)
            CHECK(!wl_evaluation_control_configure(control, UINT64_C(1000000)));
        controlled_recursive_stop_phase = stop_case == 0
            ? WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_BODY
            : stop_case == 1
            ? WL_COLUMNAR_SESSION_TEST_RECURSIVE_DURING_MERGE
            : stop_case == 2
            ? WL_COLUMNAR_SESSION_TEST_RECURSIVE_ALLOCATION_PRECHARGE
            : stop_case == 3
            ? WL_COLUMNAR_SESSION_TEST_RECURSIVE_ALLOCATION_ADMITTED
            : WL_COLUMNAR_SESSION_TEST_RECURSIVE_SCHEMA_ADOPTED;
        controlled_recursive_stop_fired = false;
        controlled_recursive_stop_enabled = true;
        controlled_recursive_stop_unpublished = false;
        controlled_recursive_edge_write_status = 0;
        controlled_recursive_session_under_test = columnar;
        wl_columnar_session_test_recursive_step_checkpoint =
            controlled_recursive_stop_checkpoint;
        int rc = controlled_recursive_run(session, control);
        wl_columnar_session_test_recursive_step_checkpoint = NULL;
        controlled_recursive_session_under_test = NULL;
        CHECK(controlled_recursive_stop_fired
            && controlled_recursive_stop_unpublished
            && rc == ((stop_case == 0 || stop_case == 3)
                ? WL_EVALUATION_CONTROL_CANCELLED
                : WL_EVALUATION_CONTROL_WORK_LIMIT));
        if (stop_case == 0)
            CHECK(controlled_recursive_edge_write_status == EBUSY);
        CHECK(!session_find_rel(columnar, "path")
            && !columnar->cleanup_active && !columnar->cleanup_pending);
        const col_rel_t *edge = session_find_rel(columnar, "edge");
        CHECK(edge && edge->nrows == 3);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(control, &report)
            && report.result == rc && report.consumed > 0);
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_configure(control, 0));
        CHECK(!controlled_recursive_run(session, control));
        path = session_find_rel(columnar, "path");
        CHECK(path && path->nrows == 6 && path->ncols == 2
            && path->capacity == 6
            && path->schema_ok && path->col_names
            && !strcmp(path->col_names[0], "x")
            && !strcmp(path->col_names[1], "y")
            && path->sorted_nrows == path->nrows);
        const int64_t expected[][2] = {
            {1, 2}, {1, 3}, {1, 4}, {2, 3}, {2, 4}, {3, 4}
        };
        for (uint32_t i = 0; i < 6; ++i) {
            CHECK(path->columns[0][i] == expected[i][0]
                && path->columns[1][i] == expected[i][1]);
        }
        wl_session_destroy(session);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

static void
test_controlled_recursive_step_after_public_edge_append(void)
{
    const int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    const int64_t later_edge[] = { 4, 5 };
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = controlled_recursive_session(edges, 3, &plan,
            &program, &control);
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(!controlled_recursive_run(session, control));
    const col_rel_t *path = session_find_rel(columnar, "path");
    CHECK(path && path->nrows == 6);
    CHECK(!wl_session_insert(session, "edge", later_edge, 1, 2));
    CHECK(session_find_rel(columnar, "path") == path
        && path->nrows == 6);
    controlled_recursive_old_path = path;
    controlled_recursive_old_path_identity = path->relation_identity;
    controlled_recursive_edge_snapshot = wl_columnar_relation_snapshot(
        session_find_rel(columnar, "edge"));
    controlled_recursive_boundary_enabled = true;
    controlled_recursive_precommit_seen = false;
    controlled_recursive_precommit_old_path_preserved = false;
    controlled_recursive_precommit_snapshot_preserved = false;
    controlled_recursive_postcommit_seen = false;
    controlled_recursive_postcommit_published_once = false;
    controlled_recursive_edge_write_status = 0;
    controlled_recursive_postcommit_edge_write_status = 0;
    controlled_recursive_boundary_attempt_id = 0;
    controlled_recursive_session_under_test = columnar;
    wl_columnar_session_test_recursive_step_checkpoint =
        controlled_recursive_stop_checkpoint;
    int successor_rc = controlled_recursive_run(session, control);
    wl_columnar_session_test_recursive_step_checkpoint = NULL;
    controlled_recursive_session_under_test = NULL;
    controlled_recursive_boundary_enabled = false;
    CHECK(!successor_rc);
    CHECK(controlled_recursive_precommit_seen
        && controlled_recursive_precommit_old_path_preserved
        && controlled_recursive_precommit_snapshot_preserved
        && controlled_recursive_edge_write_status == EBUSY);
    CHECK(controlled_recursive_postcommit_seen
        && controlled_recursive_postcommit_published_once
        && controlled_recursive_postcommit_edge_write_status == EBUSY
        && controlled_recursive_boundary_attempt_id != 0);
    path = session_find_rel(columnar, "path");
    CHECK(path && path->nrows == 10
        && path == controlled_recursive_committed_path);
    wl_evaluation_control_report_t successor_report;
    CHECK(!wl_evaluation_control_report(control, &successor_report)
        && successor_report.attempt_id
        == controlled_recursive_boundary_attempt_id
        && successor_report.result == 0);
    /* Four edges require five body rounds (including empty convergence),
     * with five operator descriptors per round, using the tariffs above. */
    const uint64_t successor_work = UINT64_C(1859706) + UINT64_C(5) * 224
        * ((wl_columnar_eval_stack_test_frame_size() + 15) / 16)
        + UINT64_C(25) * 224 * ((sizeof(col_rel_t) + 7) / 8);
    CHECK(successor_report.consumed == successor_work);
    const int64_t expected[][2] = {
        { 1, 2 }, { 1, 3 }, { 1, 4 }, { 1, 5 },
        { 2, 3 }, { 2, 4 }, { 2, 5 },
        { 3, 4 }, { 3, 5 }, { 4, 5 }
    };
    for (uint32_t i = 0; path && i < 10; i++)
        CHECK(path->columns[0][i] == expected[i][0]
            && path->columns[1][i] == expected[i][1]);
    const int64_t next_edge[] = { 5, 6 };
    CHECK(!wl_session_insert(session, "edge", next_edge, 1, 2));
    CHECK(!controlled_recursive_run(session, control));
    path = session_find_rel(columnar, "path");
    CHECK(path && path->nrows == 15);
    const int64_t third_expected[][2] = {
        { 1, 2 }, { 1, 3 }, { 1, 4 }, { 1, 5 }, { 1, 6 },
        { 2, 3 }, { 2, 4 }, { 2, 5 }, { 2, 6 },
        { 3, 4 }, { 3, 5 }, { 3, 6 },
        { 4, 5 }, { 4, 6 }, { 5, 6 }
    };
    for (uint32_t i = 0; path && i < 15; i++)
        CHECK(path->columns[0][i] == third_expected[i][0]
            && path->columns[1][i] == third_expected[i][1]);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_recursive_successor_work_boundaries(void)
{
    static const int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    static const int64_t later_edge[] = { 4, 5 };
    /* Same four-edge recomputation: five frames and 25 descriptors. */
    const uint64_t successor_work = UINT64_C(1859706) + UINT64_C(5) * 224
        * ((wl_columnar_eval_stack_test_frame_size() + 15) / 16)
        + UINT64_C(25) * 224 * ((sizeof(col_rel_t) + 7) / 8);
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = controlled_recursive_session(edges, 3, &plan,
            &program, &control);
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(!controlled_recursive_run(session, control));
    CHECK(!wl_session_insert(session, "edge", later_edge, 1, 2));
    CHECK(!wl_evaluation_control_configure(control, successor_work));
    CHECK(!controlled_recursive_run(session, control));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.limit == successor_work
        && report.consumed == successor_work && report.result == 0);
    const col_rel_t *path = session_find_rel(columnar, "path");
    CHECK(path && path->nrows == 10);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);

    session = controlled_recursive_session(edges, 3, &plan, &program,
            &control);
    columnar = (wl_col_session_t *)session;
    CHECK(!controlled_recursive_run(session, control));
    const col_rel_t *old_path = session_find_rel(columnar, "path");
    CHECK(old_path && old_path->nrows == 6);
    const uint64_t old_path_identity = old_path->relation_identity;
    CHECK(!wl_session_insert(session, "edge", later_edge, 1, 2));
    CHECK(!wl_evaluation_control_configure(control, successor_work - 1u));
    int rc = controlled_recursive_run(session, control);
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
        && session_find_rel(columnar, "path") == old_path
        && old_path->nrows == 6
        && !columnar->cleanup_active && !columnar->cleanup_pending);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.limit == successor_work - 1u
        && report.consumed == successor_work - 1u
        && report.result == rc);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!controlled_recursive_run(session, control));
    path = session_find_rel(columnar, "path");
    CHECK(path && path->relation_identity != old_path_identity
        && path->nrows == 10);
    static const int64_t expected[][2] = {
        { 1, 2 }, { 1, 3 }, { 1, 4 }, { 1, 5 },
        { 2, 3 }, { 2, 4 }, { 2, 5 },
        { 3, 4 }, { 3, 5 }, { 4, 5 }
    };
    for (uint32_t i = 0; i < 10; i++)
        CHECK(path->columns[0][i] == expected[i][0]
            && path->columns[1][i] == expected[i][1]);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_recursive_successor_stop_preserves_published_path(void)
{
    const int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    const int64_t later_edge[] = { 4, 5 };
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = controlled_recursive_session(edges, 3, &plan,
            &program, &control);
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(!controlled_recursive_run(session, control));
    const col_rel_t *old_path = session_find_rel(columnar, "path");
    CHECK(old_path && old_path->nrows == 6);
    CHECK(!wl_session_insert(session, "edge", later_edge, 1, 2));

    controlled_recursive_stop_phase =
        WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_BODY;
    controlled_recursive_stop_fired = false;
    controlled_recursive_stop_enabled = true;
    controlled_recursive_stop_unpublished = true;
    controlled_recursive_session_under_test = columnar;
    wl_columnar_session_test_recursive_step_checkpoint =
        controlled_recursive_stop_checkpoint;
    int rc = controlled_recursive_run(session, control);
    wl_columnar_session_test_recursive_step_checkpoint = NULL;
    controlled_recursive_session_under_test = NULL;
    controlled_recursive_stop_enabled = false;
    CHECK(controlled_recursive_stop_fired &&
        !controlled_recursive_stop_unpublished
        && rc == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(session_find_rel(columnar, "path") == old_path
        && old_path->nrows == 6 && !columnar->cleanup_active
        && !columnar->cleanup_pending);
    snapshot_capture_t stale_capture = {0};
    int snapshot_rc = wl_session_snapshot(session, capture_snapshot_tuple,
            &stale_capture);
    CHECK(snapshot_rc == ENOTSUP && stale_capture.count == 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == rc);

    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!controlled_recursive_run(session, control));
    const col_rel_t *path = session_find_rel(columnar, "path");
    CHECK(path && path->nrows == 10);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_recursive_snapshot_rejects_prelease_mutation(void)
{
    const int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    const int64_t changed_edge[] = { 4, 5 };
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = controlled_recursive_session(edges, 3, &plan,
            &program, &control);
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(wl_session_step(session) == EAGAIN);
    const col_rel_t *edge = session_find_rel(columnar, "edge");
    CHECK(edge && !col_rel_append_row((col_rel_t *)edge, changed_edge));
    int rc = controlled_recursive_run(session, control);
    CHECK(rc == EBUSY && !session_find_rel(columnar, "path")
        && !columnar->cleanup_active && !columnar->cleanup_pending);
    edge = session_find_rel(columnar, "edge");
    CHECK(edge && edge->nrows == 4);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == EBUSY);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_recursive_step_requires_edge_before_attempt(void)
{
    static const char source[] =
        ".decl edge(x:int64, y:int64)\n"
        ".decl path(x:int64, y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,z) :- path(x,y), edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(wl_session_step(session) == ENOTSUP);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control, &report) == ENOENT);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_recursive_step_general_join_chain(void)
{
    static const char source[] =
        ".decl edge(x:int64, y:int64)\n"
        ".decl walk(x:int64, y:int64)\n"
        "walk(x,y) :- edge(x,y).\n"
        "walk(x,v) :- walk(x,y), edge(y,z), edge(z,w), edge(w,v).\n";
    const int64_t edges[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_insert(session, "edge", edges, 4, 2));
    int rc = controlled_recursive_run(session, control);
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(!rc);
    col_rel_t *walk = session_find_rel(columnar, "walk");
    CHECK(walk && walk->nrows == 5);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control,
        &report) && report.consumed > 0);
    const col_rel_t *edge = session_find_rel(columnar, "edge");
    CHECK(edge && edge->nrows == 4);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

typedef struct {
    wl_evaluation_control_t *control;
    int64_t rows[8];
    int32_t diffs[8];
    uint32_t count;
    bool cancel_on_first;
} controlled_observer_capture_t;

static void
controlled_observer_capture(const char *relation, const int64_t *row,
    uint32_t ncols, int diff, void *data)
{
    controlled_observer_capture_t *capture = data;
    CHECK(capture && relation && !strcmp(relation, "output") && row
        && ncols == 1 && (diff == 1 || diff == -1)
        && capture->count < 8);
    capture->rows[capture->count] = row[0];
    capture->diffs[capture->count++] = diff;
    if (capture->cancel_on_first && capture->count == 1)
        wl_evaluation_control_request_cancel(capture->control);
}

static void
controlled_observer_snapshot_capture(const char *relation,
    const int64_t *row, uint32_t ncols, void *data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)data;
}

static unsigned controlled_observer_framed_lookup_hits;
static wl_columnar_source_access_reader_t
    controlled_observer_registration_reader;
static uint32_t controlled_registry_scan_indices[2][8];
static unsigned controlled_registry_scan_counts[2];
static unsigned controlled_registry_stop_count;
static wl_evaluation_control_t *controlled_observer_boundary_control;
static unsigned controlled_observer_boundary_hits;

static void
controlled_observer_cancel_after_evaluation(wl_col_session_t *sess,
    unsigned boundary)
{
    (void)sess;
    if (boundary == 1) {
        controlled_observer_boundary_hits++;
        wl_columnar_eval_delta_test_observer_boundary = NULL;
        wl_evaluation_control_request_cancel(
            controlled_observer_boundary_control);
    }
}

typedef struct {
    wl_session_t *session;
    unsigned *replacement_count;
    unsigned count;
} controlled_observer_replacement_t;

static void
controlled_observer_replacement_capture(const char *relation,
    const int64_t *row, uint32_t ncols, int diff, void *data)
{
    unsigned *count = data;
    CHECK(relation && !strcmp(relation, "output") && row && ncols == 1
        && diff == 1);
    ++*count;
}

static void
controlled_observer_replace_callback(const char *relation,
    const int64_t *row, uint32_t ncols, int diff, void *data)
{
    controlled_observer_replacement_t *state = data;
    CHECK(relation && !strcmp(relation, "output") && row && ncols == 1
        && diff == 1);
    if (++state->count == 1)
        wl_session_set_delta_cb(state->session,
            controlled_observer_replacement_capture,
            state->replacement_count);
}

typedef struct {
    wl_session_t *session;
    unsigned count;
} controlled_observer_clear_t;

static void
controlled_observer_clear_callback(const char *relation,
    const int64_t *row, uint32_t ncols, int diff, void *data)
{
    controlled_observer_clear_t *state = data;
    CHECK(relation && !strcmp(relation, "output") && row && ncols == 1
        && diff == 1);
    ++state->count;
    wl_session_set_delta_cb(state->session, NULL, NULL);
}

static void
controlled_observer_count_rule_start(
    wl_columnar_eval_serial_framed_attempt_t *attempt)
{
    if (attempt->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP)
        controlled_observer_framed_lookup_hits++;
}

static void
controlled_observer_hold_output_for_registration(col_rel_t *output)
{
    CHECK(output && !controlled_observer_registration_reader.owner);
    CHECK(!col_rel_source_reader_acquire(output,
        &controlled_observer_registration_reader));
    wl_columnar_session_test_before_successor_registration = NULL;
}

static void
controlled_observer_stop_registry_scan(wl_evaluation_control_t *control,
    unsigned stage, uint32_t index)
{
    CHECK(stage == 1 || stage == 2);
    unsigned stage_index = stage - 1;
    CHECK(controlled_registry_scan_counts[stage_index] < 8);
    controlled_registry_scan_indices[stage_index]
    [controlled_registry_scan_counts[stage_index]++] = index;
    if ((controlled_registry_stop_count == 0 && stage == 1 && index == 0)
        || (controlled_registry_stop_count == 1 && stage == 2 && index == 0)) {
        controlled_registry_stop_count++;
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
}

typedef struct {
    uint32_t count;
    int64_t first;
} controlled_observer_order_capture_t;

static void
controlled_observer_order_capture(const char *relation, const int64_t *row,
    uint32_t ncols, int diff, void *data)
{
    controlled_observer_order_capture_t *capture = data;
    CHECK(capture && relation && !strcmp(relation, "output") && row
        && ncols == 1 && diff == 1);
    CHECK(row[0] == capture->first + capture->count);
    capture->count++;
}

extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
static bool observed_multi_authenticated;
static unsigned observed_multi_stop_phase;
static uint32_t observed_multi_stop_index;
static bool observed_multi_stop_work, observed_multi_stop_in_child;
static void
observed_multi_stop(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    if (bindings && !observed_multi_authenticated) {
        const char *name = NULL;
        col_rel_t *relation = NULL;
        CHECK(!wl_columnar_eval_private_binding_get(bindings, 0, &name,
            &relation));
        CHECK(name && relation);
        wl_columnar_eval_private_bindings_t forged = *bindings;
        forged.identity = (uintptr_t)&forged;
        CHECK(wl_columnar_eval_private_binding_get(&forged, 0, &name,
            &relation) == EINVAL);
        wl_columnar_eval_private_bindings_t *live =
            (wl_columnar_eval_private_bindings_t *)bindings;
        live->generation++;
        CHECK(wl_columnar_eval_private_binding_get(bindings, 0, &name,
            &relation) == EINVAL);
        live->generation--;
        CHECK(!wl_columnar_eval_private_binding_get(bindings, 0, &name,
            &relation));
        observed_multi_authenticated = true;
    }
    if (phase == observed_multi_stop_phase &&
        index == observed_multi_stop_index
        && (!observed_multi_stop_in_child || bindings)){
        wl_columnar_session_test_observed_transaction_checkpoint = NULL;
        if (observed_multi_stop_work)
            CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else
            wl_evaluation_control_request_cancel(control);
    }
}

typedef struct {
    char name[384];
    int64_t row[257];
    uint32_t width;
    int diff;
} observed_multi_event_t;
typedef struct {
    observed_multi_event_t events[128];
    unsigned count;
    wl_evaluation_control_t *cancel_control;
} observed_multi_capture_t;

static void
observed_multi_capture(const char *name, const int64_t *row, uint32_t width,
    int diff, void *data)
{
    observed_multi_capture_t *capture = data;
    CHECK(capture->count < 128 && width <= 257 && strlen(name) < 384);
    observed_multi_event_t *event = &capture->events[capture->count++];
    memset(event, 0, sizeof(*event));
    strcpy(event->name, name);
    memcpy(event->row, row, width * sizeof(*row));
    event->width = width; event->diff = diff;
    if (capture->count == 1 && capture->cancel_control)
        wl_evaluation_control_request_cancel(capture->cancel_control);
}

static int
observed_multi_order(const void *a, const void *b)
{
    const observed_multi_event_t *left = a, *right = b;
    int rc = strcmp(left->name, right->name);
    if (rc) return rc;
    if (left->width != right->width) return left->width < right->width ? -1 : 1;
    for (uint32_t i = 0; i < left->width; ++i)
        if (left->row[i] != right->row[i]) return left->row[i] <
                   right->row[i] ? -1 : 1;
    return (left->diff > right->diff) - (left->diff < right->diff);
}

static int
observed_multi_step(wl_session_t *session)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false);
    return EINVAL;
}

static const char recursive_transaction_source[] =
    ".decl links(x:int64,y:int64)\n.decl seeds(x:int64,y:int64)\n"
    ".decl extra(x:int64,y:int64)\n.decl reach(x:int64,y:int64)\n"
    ".decl back(x:int64,y:int64)\n.decl result(x:int64,y:int64)\n"
    "reach(x,y) :- seeds(x,y).\nreach(x,y) :- extra(x,y).\n"
    "reach(x,z) :- back(x,y), links(y,z), x != z.\n"
    "back(x,y) :- reach(x,y).\nresult(x,y) :- back(x,y).\n";

static void
test_controlled_recursive_transaction(void)
{
    for (unsigned trial = 0; trial < 4; ++trial) {
        bool empty = trial == 1;
        wirelog_error_t error;
        wirelog_program_t *program =
            wirelog_parse_string(recursive_transaction_source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options); options.evaluation_control = control;
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor); options.memory_governor = governor;
        wl_session_t *session = NULL, *reference = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
        int64_t links[] = {1, 2, 2, 3, 3, 4, 4, 2, 2, 3};
        int64_t seed[] = {0, 1};
        CHECK(!wl_session_insert(session, "links", links, 5, 2));
        CHECK(!wl_session_insert(reference, "links", links, 5, 2));
        if (!empty) {
            CHECK(!wl_session_insert(session, "seeds", seed, 1, 2));
            CHECK(!wl_session_insert(reference, "seeds", seed, 1, 2));
        }
        observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
        observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
        CHECK(actual && expected);
        wl_session_set_delta_cb(session, observed_multi_capture, actual);
        wl_session_set_delta_cb(reference, observed_multi_capture, expected);
        if (trial >= 2) {
            CHECK(!wl_evaluation_control_configure(control,
                UINT64_C(1000000000)));
            observed_multi_stop_phase = trial == 2 ? 19 : 21;
            observed_multi_stop_index = 0;
            observed_multi_stop_work = trial == 3;
            wl_columnar_session_test_observed_transaction_checkpoint =
                observed_multi_stop;
        }
        int rc = observed_multi_step(session);
        if (trial >= 2) {
            CHECK(!wl_columnar_session_test_observed_transaction_checkpoint);
            CHECK(rc ==
                (trial ==
                2 ? WL_EVALUATION_CONTROL_CANCELLED :
                WL_EVALUATION_CONTROL_WORK_LIMIT));
            CHECK(!actual->count &&
                !session_find_rel((wl_col_session_t *)session, "reach"));
            observed_multi_stop_work = false;
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control, 0));
            rc = observed_multi_step(session);
        }
        if (rc) fprintf(stderr, "recursive transaction rc=%d empty=%u\n", rc,
                empty);
        CHECK(!rc && !wl_session_step(reference));
        CHECK(actual->count == expected->count);
        CHECK(actual->count == (empty ? 0 : 12));
        qsort(actual->events, actual->count, sizeof(actual->events[0]),
            observed_multi_order);
        qsort(expected->events, expected->count, sizeof(expected->events[0]),
            observed_multi_order);
        CHECK(!memcmp(actual->events, expected->events,
            actual->count * sizeof(actual->events[0])));
        const char *heads[] = {"reach", "back", "result"};
        for (unsigned h = 0; h < 3; ++h) {
            col_rel_t *a = session_find_rel((wl_col_session_t *)session,
                    heads[h]);
            col_rel_t *b = session_find_rel((wl_col_session_t *)reference,
                    heads[h]);
            CHECK(a && a->ncols == 2 && a->nrows == (b ? b->nrows : 0));
            for (uint32_t r = 0; r < a->nrows; ++r)
                for (uint32_t c = 0; c < 2;
                    ++c) CHECK(a->columns[c][r] == b->columns[c][r]);
        }
        wl_session_destroy(session); wl_session_destroy(reference);
        wl_evaluation_control_release(control); wl_plan_free(plan);
        wirelog_program_free(program);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
        free(actual); free(expected);
    }
}

static void
recursive_snapshot_count(const char *name, const int64_t *row, uint32_t width,
    void *data)
{
    CHECK(name && row && width);
    ++*(unsigned *)data;
}

typedef struct {
    wl_session_t *session;
    observed_multi_capture_t *capture;
    bool clear;
} recursive_callback_change_t;

static void
recursive_callback_change(const char *relation, const int64_t *row,
    uint32_t width, int diff, void *data)
{
    recursive_callback_change_t *change = data;
    observed_multi_capture(relation, row, width, diff, change->capture);
    wl_session_set_delta_cb(change->session,
        change->clear ? NULL : observed_multi_capture, change->capture);
}

static void
test_controlled_recursive_transaction_successors(void)
{
    for (unsigned mode = 0; mode < 4; ++mode) {
        wirelog_error_t error;
        wirelog_program_t *program =
            wirelog_parse_string(recursive_transaction_source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options); options.evaluation_control = control;
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor); options.memory_governor = governor;
        wl_session_t *session = NULL, *reference = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
        int64_t links[] = {1, 2, 2, 3, 3, 1};
        CHECK(!wl_session_insert(session, "links", links, 3, 2));
        CHECK(!wl_session_insert(reference, "links", links, 3, 2));
        observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
        observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
        CHECK(actual && expected);
        recursive_callback_change_t actual_change = {session, actual,
                                                     mode == 2};
        recursive_callback_change_t expected_change = {reference, expected,
                                                       mode == 2};
        if (mode >= 2) {
            wl_session_set_delta_cb(session, recursive_callback_change,
                &actual_change);
            wl_session_set_delta_cb(reference, recursive_callback_change,
                &expected_change);
        } else if (mode) {
            wl_session_set_delta_cb(session, observed_multi_capture, actual);
            wl_session_set_delta_cb(reference, observed_multi_capture,
                expected);
        }
        uint64_t baseline = 0;
        for (unsigned cycle = 0; cycle < 12; ++cycle) {
            actual->count = expected->count = 0;
            const char *input = cycle % 2 ? "extra" : "seeds";
            int64_t seed[] = {0, cycle % 2 ? 2 : 1};
            if (cycle % 4 < 2) {
                CHECK(!wl_session_insert(session, input, seed, 1, 2));
                CHECK(!wl_session_insert(reference, input, seed, 1, 2));
            } else {
                CHECK(!wl_session_remove(session, input, seed, 1, 2));
                CHECK(!wl_session_remove(reference, input, seed, 1, 2));
            }
            if (mode == 1 && cycle == 7) actual->cancel_control = control;
            int rc = observed_multi_step(session);
            if (rc) fprintf(stderr,
                    "recursive successor rc=%d mode=%u cycle=%u\n", rc, mode,
                    cycle);
            CHECK(!rc && !wl_session_step(reference));
            if (mode == 1 && cycle == 7) {
                unsigned snapshots = 0;
                CHECK(wl_session_snapshot(session, recursive_snapshot_count,
                    &snapshots)
                    == WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(!snapshots && !wl_evaluation_control_reset(control));
                actual->cancel_control = NULL;
            }
            if (!cycle) {
                unsigned snapshots = 0;
                CHECK(!wl_session_snapshot(session, recursive_snapshot_count,
                    &snapshots));
                CHECK(snapshots == 9);
            }
            if (actual->count != expected->count) fprintf(stderr,
                    "events mode=%u cycle=%u actual=%u expected=%u\n", mode,
                    cycle,
                    actual->count, expected->count);
            CHECK(actual->count == expected->count);
            qsort(actual->events, actual->count, sizeof(actual->events[0]),
                observed_multi_order);
            qsort(expected->events, expected->count,
                sizeof(expected->events[0]), observed_multi_order);
            CHECK(!memcmp(actual->events, expected->events,
                actual->count * sizeof(actual->events[0])));
            const char *heads[] = {"reach", "back", "result"};
            for (unsigned h = 0; h < 3; ++h) {
                col_rel_t *a = session_find_rel((wl_col_session_t *)session,
                        heads[h]);
                col_rel_t *b = session_find_rel((wl_col_session_t *)reference,
                        heads[h]);
                CHECK(a && a->ncols == 2 &&
                    a->nrows == (cycle % 4 == 3 ? 0 : 3));
                CHECK(a->nrows == (b ? b->nrows : 0));
                for (uint32_t r = 0; r < a->nrows; ++r)
                    for (uint32_t c = 0; c < 2;
                        ++c) CHECK(a->columns[c][r] == b->columns[c][r]);
            }
            CHECK(((wl_col_session_t *)session)->nrels == 6);
            unsigned delivered = actual->count;
            CHECK(!observed_multi_step(session) && actual->count == delivered);
            if (cycle % 4 == 3 && mode != 2) {
                uint64_t reserved =
                    wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor));
                if (cycle == 3) baseline = reserved;
                else CHECK(reserved == baseline);
            }
        }
        wl_session_destroy(session); wl_session_destroy(reference);
        wl_evaluation_control_release(control); wl_plan_free(plan);
        wirelog_program_free(program);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
        free(actual); free(expected);
    }
}

static void
test_controlled_recursive_transaction_stops(void)
{
    const unsigned phases[] = {0, 18, 24, 19, 22, 23, 13, 14};
    const uint32_t indices[] = {0, 0, 2, 0, 0, 0, 4, 6};
    for (unsigned point = 0; point < 8; ++point) {
        for (unsigned outcome = 0; outcome < 3; ++outcome) {
            wirelog_error_t error;
            wirelog_program_t *program =
                wirelog_parse_string(recursive_transaction_source, &error);
            CHECK(program);
            wl_plan_t *plan = NULL;
            CHECK(!wl_plan_from_program(program, &plan));
            wl_evaluation_control_t *control = NULL;
            CHECK(!wl_evaluation_control_create(0, &control));
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = control;
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
            };
            wl_columnar_memory_governor_ref_t *governor =
                wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor); options.memory_governor = governor;
            wl_session_t *session = NULL, *reference = NULL;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1, &options, &session));
            CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                &reference));
            const int64_t edge[] = {1, 2}, seed[] = {0, 1}, extra[] = {2, 3};
            CHECK(!wl_session_insert(session, "links", edge, 1, 2));
            CHECK(!wl_session_insert(reference, "links", edge, 1, 2));
            CHECK(!wl_session_insert(session, "seeds", seed, 1, 2));
            CHECK(!wl_session_insert(reference, "seeds", seed, 1, 2));
            observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
            observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
            CHECK(actual && expected);
            wl_session_set_delta_cb(session, observed_multi_capture, actual);
            wl_session_set_delta_cb(reference, observed_multi_capture,
                expected);
            CHECK(!observed_multi_step(session) && !wl_session_step(reference));
            col_rel_t *old[3]; col_relation_snapshot_t snapshots[3];
            const char *names[] = {"reach", "back", "result"};
            for (unsigned h = 0; h < 3; ++h) {
                old[h] = session_find_rel((wl_col_session_t *)session,
                        names[h]);
                CHECK(old[h] && old[h]->nrows == 2);
                snapshots[h] = wl_columnar_relation_snapshot(old[h]);
            }
            actual->count = expected->count = 0;
            CHECK(!wl_session_insert(session, "links", extra, 1, 2));
            CHECK(!wl_session_insert(reference, "links", extra, 1, 2));
            CHECK(!wl_evaluation_control_configure(control,
                UINT64_C(1000000000)));
            observed_multi_stop_phase = phases[point];
            observed_multi_stop_index = indices[point];
            observed_multi_stop_work = outcome != 1;
            wl_columnar_session_test_observed_transaction_checkpoint =
                observed_multi_stop;
            int rc = observed_multi_step(session);
            if (wl_columnar_session_test_observed_transaction_checkpoint)
                fprintf(stderr,
                    "missed recursive stop phase=%u outcome=%u rc=%d\n",
                    phases[point], outcome, rc);
            CHECK(!wl_columnar_session_test_observed_transaction_checkpoint);
            CHECK(rc ==
                (outcome ==
                1 ? WL_EVALUATION_CONTROL_CANCELLED :
                WL_EVALUATION_CONTROL_WORK_LIMIT));
            observed_multi_stop_work = false;
            CHECK(!actual->count);
            for (unsigned h = 0; h < 3; ++h) {
                CHECK(session_find_rel((wl_col_session_t *)session,
                    names[h]) == old[h]);
                CHECK(wl_columnar_relation_snapshot_equal(snapshots[h],
                    wl_columnar_relation_snapshot(old[h])));
            }
            wl_evaluation_control_report_t stopped, resumed;
            CHECK(!wl_evaluation_control_report(control, &stopped));
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control, 0));
            if (outcome == 2) {
                CHECK(wl_session_try_destroy(&session) == EBUSY && session);
                wl_evaluation_control_request_cancel(control);
                CHECK(observed_multi_step(session) ==
                    WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(!actual->count);
            } else {
                CHECK(!observed_multi_step(session) &&
                    !wl_session_step(reference));
                CHECK(!wl_evaluation_control_report(control, &resumed));
                CHECK(resumed.attempt_id > stopped.attempt_id &&
                    !resumed.limit);
                CHECK(actual->count == 3 && actual->count == expected->count);
                qsort(actual->events, actual->count, sizeof(actual->events[0]),
                    observed_multi_order);
                qsort(expected->events, expected->count,
                    sizeof(expected->events[0]), observed_multi_order);
                CHECK(!memcmp(actual->events, expected->events,
                    actual->count * sizeof(actual->events[0])));
                CHECK(!observed_multi_step(session) && actual->count == 3);
            }
            unsigned turns = 0;
            while (session) {
                rc = wl_session_try_destroy(&session);
                CHECK(!rc || rc == EBUSY);
                CHECK(++turns < 10000);
            }
            wl_session_destroy(reference);
            wl_evaluation_control_release(control);
            wl_plan_free(plan); wirelog_program_free(program);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
            free(actual); free(expected);
        }
    }
}

static void
test_controlled_recursive_transaction_shapes(void)
{
    static const char *const sources[] = {
        ".decl seed(x:int64)\n.decl a(x:int64)\n.decl b(x:int64)\n"
        ".decl output(x:int64)\na(x) :- seed(x).\na(x) :- b(x).\n"
        "b(x) :- a(x).\noutput(x) :- b(x).\n",
        ".decl seed(x:int64,y:int64,z:int64)\n.decl a(x:int64,y:int64,z:int64)\n"
        ".decl b(x:int64,y:int64,z:int64)\na(x,y,z) :- seed(x,y,z).\n"
        "a(x,y,z) :- b(x,y,z), x != z.\nb(x,y,z) :- a(x,y,z).\n",
        ".decl seed(x:int64)\n.decl edges(x:int64,y:int64)\n"
        ".decl a(x:int64)\n.decl b(x:int64)\n.decl c(x:int64)\n"
        ".decl d(x:int64)\n.decl output(x:int64)\n"
        "a(x) :- seed(x).\na(y) :- b(x), edges(x,y).\nb(x) :- a(x).\n"
        "c(x) :- a(x).\nc(y) :- d(x), edges(x,y).\nd(x) :- c(x).\n"
        "output(x) :- d(x).\n"
    };
    const uint32_t widths[] = {1, 3, 1}, totals[] = {6, 4, 15};
    for (unsigned shape = 0; shape < 3; ++shape) {
        uint64_t exact = 0;
        for (unsigned trial = 0; trial < 3; ++trial) {
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(sources[shape],
                    &error);
            CHECK(program);
            wl_plan_t *plan = NULL;
            CHECK(!wl_plan_from_program(program, &plan));
            if (shape == 2) {
                unsigned recursive_strata = 0;
                for (uint32_t si = 0; si < plan->stratum_count; ++si)
                    recursive_strata += plan->strata[si].is_recursive;
                CHECK(recursive_strata == 2);
            }
            wl_evaluation_control_t *control = NULL;
            uint64_t limit = !trial ? 0 : trial == 1 ? exact : exact - 1;
            CHECK(!wl_evaluation_control_create(limit, &control));
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = control;
            wl_session_t *session = NULL, *reference = NULL;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1, &options, &session));
            CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                &reference));
            const int64_t seed[] = {1, 2, 3, 4, 5, 6}, edges[] = {1, 2, 2, 3};
            uint32_t rows = shape == 2 ? 1 : 2;
            CHECK(!wl_session_insert(session, "seed", seed, rows,
                widths[shape]));
            CHECK(!wl_session_insert(reference, "seed", seed, rows,
                widths[shape]));
            if (shape == 2) {
                CHECK(!wl_session_insert(session, "edges", edges, 2, 2));
                CHECK(!wl_session_insert(reference, "edges", edges, 2, 2));
            }
            observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
            observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
            CHECK(actual && expected);
            wl_session_set_delta_cb(session, observed_multi_capture, actual);
            wl_session_set_delta_cb(reference, observed_multi_capture,
                expected);
            int rc = observed_multi_step(session);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(control, &report));
            if (!trial) {
                CHECK(!rc); exact = report.consumed; CHECK(exact > 1);
            }else if (trial == 1) CHECK(!rc && report.consumed == exact);
            else {
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !actual->count);
                CHECK(report.consumed <= limit);
                for (uint32_t si = 0; si < plan->stratum_count; ++si)
                    for (uint32_t ri = 0; ri < plan->strata[si].relation_count;
                        ++ri)
                        CHECK(!session_find_rel((wl_col_session_t *)session,
                            plan->strata[si].relations[ri].name));
                CHECK(!wl_evaluation_control_reset(control));
                CHECK(!wl_evaluation_control_configure(control, 0));
                CHECK(!observed_multi_step(session));
            }
            CHECK(!wl_session_step(reference));
            CHECK(actual->count == totals[shape] &&
                actual->count == expected->count);
            qsort(actual->events, actual->count, sizeof(actual->events[0]),
                observed_multi_order);
            qsort(expected->events, expected->count,
                sizeof(expected->events[0]), observed_multi_order);
            CHECK(!memcmp(actual->events, expected->events,
                actual->count * sizeof(actual->events[0])));
            wl_session_destroy(session); wl_session_destroy(reference);
            wl_evaluation_control_release(control); wl_plan_free(plan);
            wirelog_program_free(program);
            free(actual); free(expected);
        }
    }
}

static void
test_controlled_recursive_transaction_refusals(void)
{
    static const char *const sources[] = {
        ".decl seed(x:int32)\n.decl a(x:int32)\n"
        "a(x) :- seed(x).\na(x) :- a(x).\n",
        ".decl seed()\n.decl a()\na() :- seed().\na() :- a().\n",
        ".decl seed(x:int64)\n.decl a(x:int64)\n"
        "a(x) :- seed(x).\na(x+1) :- a(x).\n",
        ".decl seed(x:int64)\n.decl a(x:int64)\n"
        "a(7).\na(x) :- seed(x).\na(x) :- a(x).\n"
    };
    for (unsigned shape = 0; shape < 4; ++shape) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(sources[shape],
                &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(100000, &control));
        wl_session_options_t options;
        wl_session_options_init(&options); options.evaluation_control = control;
        wl_session_t *session = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        const int64_t row[] = {1};
        CHECK(!wl_session_insert(session, "seed", row, 1, shape == 1 ? 0 : 1));
        observed_multi_capture_t *capture = calloc(1, sizeof(*capture));
        CHECK(capture);
        wl_session_set_delta_cb(session, observed_multi_capture, capture);
        col_rel_t *seed = session_find_rel((wl_col_session_t *)session, "seed");
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(seed);
        int rc = observed_multi_step(session);
        CHECK(rc == ENOTSUP || (shape == 3 && rc == EINVAL));
        CHECK(!capture->count && wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(seed)));
        col_rel_t *a = session_find_rel((wl_col_session_t *)session, "a");
        CHECK(!a || (shape == 3 && !a->nrows));
        wl_session_destroy(session); wl_evaluation_control_release(control);
        wl_plan_free(plan); wirelog_program_free(program); free(capture);
    }
}

/* This is an explicitly unoptimized internal plan view. The default optimizer's
* K_FUSION route remains refused; these assertions do not qualify that route. */
static void
test_controlled_recursive_transaction_nonlinear_view(void)
{
    static const char source[] =
        ".decl links(x:int64,y:int64)\n.decl reach(x:int64,y:int64)\n"
        "reach(x,y) :- links(x,y).\nreach(x,z) :- reach(x,y), reach(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *owner = NULL, *raw_plan = NULL;
    CHECK(!wl_plan_from_program(program, &owner));
    CHECK(!wl_plan_from_program(program, &raw_plan));
    CHECK(owner->stratum_count == 1 && owner->strata[0].relation_count == 1);
    bool fusion = false;
    for (uint32_t i = 0; i < owner->strata[0].relations[0].op_count; ++i)
        fusion |= owner->strata[0].relations[0].ops[i].op ==
            WL_PLAN_OP_K_FUSION;
    CHECK(fusion);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), owner, 1,
        &options, &session));
    const int64_t edges[] = {1, 2, 2, 3, 3, 4, 4, 5, 5, 2, 2, 3};
    CHECK(!wl_session_insert(session, "links", edges, 6, 2));
    CHECK(observed_multi_step(session) == ENOTSUP);
    CHECK(!session_find_rel((wl_col_session_t *)session, "reach"));
    wl_session_destroy(session);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, UINT64_C(1000000000)));

    wl_plan_t view = *owner;
    wl_plan_stratum_t stratum = owner->strata[0];
    wl_plan_relation_t relation = stratum.relations[0];
    relation.ops = stratum.bodies[0].ops;
    relation.op_count = stratum.bodies[0].op_count;
    stratum.relations = &relation; view.strata = &stratum;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), &view, 1,
        &options, &session));
    wl_session_t *reference = NULL;
    CHECK(!wl_session_create(wl_backend_columnar(), raw_plan, 1, &reference));
    CHECK(!wl_session_insert(session, "links", edges, 6, 2));
    CHECK(!wl_session_insert(reference, "links", edges, 6, 2));
    observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
    observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
    CHECK(actual && expected);
    wl_session_set_delta_cb(session, observed_multi_capture, actual);
    wl_session_set_delta_cb(reference, observed_multi_capture, expected);
    observed_multi_stop_phase = 24; observed_multi_stop_index = 2;
    observed_multi_stop_work = true;
    wl_columnar_session_test_observed_transaction_checkpoint =
        observed_multi_stop;
    CHECK(observed_multi_step(session) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_columnar_session_test_observed_transaction_checkpoint &&
        !actual->count);
    CHECK(!session_find_rel((wl_col_session_t *)session, "reach"));
    observed_multi_stop_work = false;
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!observed_multi_step(session) && !wl_session_step(reference));
    CHECK(actual->count == 20 && actual->count == expected->count);
    qsort(actual->events, actual->count, sizeof(actual->events[0]),
        observed_multi_order);
    qsort(expected->events, expected->count, sizeof(expected->events[0]),
        observed_multi_order);
    CHECK(!memcmp(actual->events, expected->events,
        actual->count * sizeof(actual->events[0])));
    wl_session_destroy(session); wl_session_destroy(reference);
    wl_evaluation_control_release(control);
    /* Only the original plans own the body operations and names. */
    wl_plan_free(owner); wl_plan_free(raw_plan); wirelog_program_free(program);
    free(actual); free(expected);
}

static void
test_controlled_observed_multi(void)
{
    const char *source =
        ".decl a(x:int64,y:int64)\n.decl b(x:int64,y:int64)\n"
        ".decl first(x:int64,y:int64)\n.decl second(x:int64,y:int64)\n"
        ".decl third(x:int64,y:int64)\n.decl joined(x:int64,y:int64)\n"
        "first(x,y) :- a(x,y).\nsecond(x,y) :- b(x,y).\n"
        "third(x,y) :- first(x,y).\njoined(x,y) :- first(x,y), b(x,y).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = control;
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor); options.memory_governor = governor;
    wl_session_t *session = NULL, *reference = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
    observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
    observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
    CHECK(actual && expected);
    uint64_t resource_baseline = 0;
    const int64_t anchor[2] = {99, 17};
    CHECK(!wl_session_insert(session, "a", anchor, 1, 2));
    CHECK(!wl_session_insert(reference, "a", anchor, 1, 2));
    wl_session_set_delta_cb(session, observed_multi_capture, actual);
    wl_session_set_delta_cb(reference, observed_multi_capture, expected);
    for (unsigned cycle = 0; cycle < 24; ++cycle) {
        const char *input = cycle % 4 < 2 ? "a" : "b";
        int64_t row[2] = { cycle % 4 < 2 ? (int64_t)(cycle / 4) : 99, 17 };
        bool insert = !(cycle % 2);
        if (insert) {
            CHECK(!wl_session_insert(session, input, row, 1, 2));
            CHECK(!wl_session_insert(reference, input, row, 1, 2));
        } else {
            CHECK(!wl_session_remove(session, input, row, 1, 2));
            CHECK(!wl_session_remove(reference, input, row, 1, 2));
        }
        actual->count = expected->count = 0;
        actual->cancel_control = cycle == 7 ? control : NULL;
        if (cycle == 0 || cycle == 3) {
            observed_multi_stop_phase = cycle ? 13 : 8;
            observed_multi_stop_index = cycle ? 4 : 1;
            wl_columnar_session_test_observed_transaction_checkpoint =
                observed_multi_stop;
            col_rel_t *old = session_find_rel((wl_col_session_t *)session,
                    "second");
            CHECK(observed_multi_step(session) ==
                WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(!wl_columnar_session_test_observed_transaction_checkpoint &&
                !actual->count);
            CHECK(session_find_rel((wl_col_session_t *)session,
                "second") == old);
            CHECK(!wl_evaluation_control_active_attempt_id(control));
            CHECK(!wl_evaluation_control_reset(control));
        }
        int rc = observed_multi_step(session);
        if (rc) fprintf(stderr, "multi cycle %u rc %d\n", cycle, rc);
        CHECK(!rc);
        CHECK(!wl_session_step(reference));
        CHECK(actual->count == expected->count);
        qsort(actual->events, actual->count, sizeof(actual->events[0]),
            observed_multi_order);
        qsort(expected->events, expected->count, sizeof(expected->events[0]),
            observed_multi_order);
        CHECK(!memcmp(actual->events, expected->events,
            actual->count * sizeof(actual->events[0])));
        CHECK(((wl_col_session_t *)session)->nrels == 6);
        if (cycle == 7) {
            CHECK(wl_session_snapshot(session,
                controlled_observer_snapshot_capture, NULL)
                == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(!wl_evaluation_control_reset(control));
        }
        if (cycle % 4 == 3) {
            uint64_t reserved =
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                        governor));
            if (cycle == 3) resource_baseline = reserved;
            else CHECK(reserved == resource_baseline);
        }
        unsigned delivered = actual->count;
        CHECK(!observed_multi_step(session) && actual->count == delivered);
        const char *heads[] = {"first", "second", "third", "joined"};
        for (unsigned h = 0; h < 4; ++h) {
            col_rel_t *a = session_find_rel((wl_col_session_t *)session,
                    heads[h]);
            col_rel_t *b = session_find_rel((wl_col_session_t *)reference,
                    heads[h]);
            CHECK(a && a->nrows == (b ? b->nrows : 0)
                && (!a->nrows || a->ncols == b->ncols));
            for (uint32_t r = 0; r < a->nrows; ++r)
                for (uint32_t c = 0; c < a->ncols;
                    ++c) CHECK(a->columns[c][r] == b->columns[c][r]);
        }
    }
    CHECK(observed_multi_authenticated);
    wl_session_destroy(session); wl_session_destroy(reference);
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
    free(actual); free(expected);
}

static unsigned incremental_step_phase, incremental_step_hits;
static bool incremental_step_work, incremental_step_stopped;
static unsigned incremental_step_suffix_mask;

static void
incremental_step_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (phase == 204 && progress < 8)
        incremental_step_suffix_mask |= 1u << progress;
    if (phase != incremental_step_phase) return;
    ++incremental_step_hits;
    if (incremental_step_stopped) return;
    incremental_step_stopped = true;
    if (incremental_step_work)
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
test_controlled_incremental_consolidate_step(void)
{
    const char *source =
        ".decl input(x:int64,y:int64)\n"
        ".decl plain(x:int64,y:int64)\n"
        ".decl swapped(x:int64,y:int64)\n"
        "plain(x,y) :- input(x,y).\n"
        "swapped(y,x) :- input(x,y).\n";
    const int64_t rows[] = {1, 9, 2, 8, 3, 7, 3, 6, 0, 5, -1, 4, 5, 3, 2, 2, 4,
                            1};
    const unsigned phases[] = {208, 204, 209, 207};
    for (unsigned point = 0; point < 4; ++point)
        for (unsigned mode = 0; mode < 2; ++mode) {
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(source, &error);
            CHECK(program);
            wl_plan_t *plan = NULL;
            CHECK(!wl_plan_from_program(program, &plan));
            wl_evaluation_control_t *control = NULL;
            CHECK(!wl_evaluation_control_create(1000000000, &control));
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = control;
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
            };
            wl_columnar_memory_governor_ref_t *governor =
                wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor); options.memory_governor = governor;
            wl_session_t *session = NULL, *reference = NULL;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1,
                &options, &session));
            CHECK(((wl_col_session_t *)session)->diff_enabled);
            CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                &reference));
            ((wl_col_session_t *)reference)->diff_enabled = false;
            observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
            observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
            CHECK(actual && expected);
            wl_session_set_delta_cb(session, observed_multi_capture, actual);
            wl_session_set_delta_cb(reference, observed_multi_capture,
                expected);
            CHECK(!wl_session_insert(session, "input", rows, 9, 2));
            CHECK(!wl_session_insert(reference, "input", rows, 9, 2));
            incremental_step_phase = phases[point];
            incremental_step_work = mode != 0;
            incremental_step_stopped = false; incremental_step_hits = 0;
            incremental_step_suffix_mask = 0;
            wl_columnar_filter_test_attempt_checkpoint =
                incremental_step_checkpoint;
            CHECK(observed_multi_step(session) == (mode
                ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                WL_EVALUATION_CONTROL_CANCELLED));
            wl_columnar_filter_test_attempt_checkpoint = NULL;
            CHECK(incremental_step_stopped && incremental_step_hits);
            CHECK(!actual->count);
            CHECK(!session_find_rel((wl_col_session_t *)session, "plain"));
            CHECK(!session_find_rel((wl_col_session_t *)session, "swapped"));
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control, 0));
            incremental_step_phase = 0; incremental_step_suffix_mask = 0;
            wl_columnar_filter_test_attempt_checkpoint =
                incremental_step_checkpoint;
            CHECK(!observed_multi_step(session));
            wl_columnar_filter_test_attempt_checkpoint = NULL;
            CHECK(!wl_session_step(reference));
            /* Both generated identity and permuted MAPs reach the optimized
             * child. Cancellation retries start from scratch; work-limit
             * resume may have already completed part of suffix sorting. */
            if (!mode) CHECK((incremental_step_suffix_mask & (1u << 1))
                    && (incremental_step_suffix_mask & (1u << 3)));
            const char *heads[] = {"plain", "swapped"};
            for (unsigned h = 0; h < 2; ++h) {
                col_rel_t *a = session_find_rel((wl_col_session_t *)session,
                        heads[h]);
                col_rel_t *b = session_find_rel((wl_col_session_t *)reference,
                        heads[h]);
                CHECK(a && b && a->nrows == b->nrows && a->ncols == b->ncols);
                for (uint32_t row = 0; row < a->nrows; ++row)
                    for (uint32_t col = 0; col < a->ncols; ++col)
                        CHECK(a->columns[col][row] == b->columns[col][row]);
            }
            CHECK(actual->count == expected->count);
            qsort(actual->events, actual->count, sizeof(actual->events[0]),
                observed_multi_order);
            qsort(expected->events, expected->count,
                sizeof(expected->events[0]), observed_multi_order);
            CHECK(!memcmp(actual->events, expected->events,
                actual->count * sizeof(actual->events[0])));
            unsigned delivered = actual->count;
            CHECK(!observed_multi_step(session) && actual->count == delivered);
            uint64_t baseline = 0;
            const int64_t extra[] = {7, 0};
            for (unsigned cycle = 0; cycle < 4; ++cycle) {
                CHECK(!wl_evaluation_control_reset(control));
                CHECK(!wl_evaluation_control_configure(control, 1000000000));
                actual->count = expected->count = 0;
                if (!(cycle % 2)) {
                    CHECK(!wl_session_insert(session, "input", extra, 1, 2));
                    CHECK(!wl_session_insert(reference, "input", extra, 1, 2));
                } else {
                    CHECK(!wl_session_remove(session, "input", extra, 1, 2));
                    CHECK(!wl_session_remove(reference, "input", extra, 1, 2));
                }
                col_rel_t *old = session_find_rel((wl_col_session_t *)session,
                        "plain");
                incremental_step_phase = phases[point];
                incremental_step_stopped = false;
                wl_columnar_filter_test_attempt_checkpoint =
                    incremental_step_checkpoint;
                CHECK(observed_multi_step(session) == (mode
                    ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                    WL_EVALUATION_CONTROL_CANCELLED));
                wl_columnar_filter_test_attempt_checkpoint = NULL;
                CHECK(incremental_step_stopped && !actual->count);
                CHECK(session_find_rel((wl_col_session_t *)session,
                    "plain") == old);
                CHECK(!wl_evaluation_control_reset(control));
                CHECK(!wl_evaluation_control_configure(control, 0));
                CHECK(!observed_multi_step(session));
                CHECK(!wl_session_step(reference));
                CHECK(actual->count == expected->count);
                qsort(actual->events, actual->count, sizeof(actual->events[0]),
                    observed_multi_order);
                qsort(expected->events, expected->count,
                    sizeof(expected->events[0]), observed_multi_order);
                CHECK(!memcmp(actual->events, expected->events,
                    actual->count * sizeof(actual->events[0])));
                uint64_t reserved =
                    wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor));
                if (cycle == 1) baseline = reserved;
                if (cycle == 3) CHECK(reserved == baseline);
            }
            CHECK(!wl_session_try_destroy(&session));
            wl_session_destroy(reference);
            wl_evaluation_control_release(control);
            wl_plan_free(plan); wirelog_program_free(program);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
            free(actual); free(expected);
        }
}

static char *
observed_wide_program(unsigned width, bool floating, const char *head)
{
    char *source = calloc(1, 65536);
    CHECK(source);
    size_t offset = 0;
    const char *names[] = {"input", head, "mirror", "other", "independent"};
    for (unsigned d = 0; d < 5; ++d) {
        offset += snprintf(source + offset, 65536 - offset, ".decl %s(",
                names[d]);
        for (unsigned c = 0; c < width; ++c)
            offset += snprintf(source + offset, 65536 - offset, "%sc%u:%s",
                    c ? "," : "", c, floating ? "float" : "int64");
        offset += snprintf(source + offset, 65536 - offset, ")\n");
    }
    for (unsigned rule = 0; rule < 3; ++rule) {
        unsigned d = rule == 2 ? 4 : rule + 1;
        offset += snprintf(source + offset, 65536 - offset, "%s(", names[d]);
        for (unsigned c = 0; c < width; ++c)
            offset += snprintf(source + offset, 65536 - offset, "%sx%u",
                    c ? "," : "", c);
        offset += snprintf(source + offset, 65536 - offset, ") :- %s(",
                names[d - 1]);
        for (unsigned c = 0; c < width; ++c)
            offset += snprintf(source + offset, 65536 - offset, "%sx%u",
                    c ? "," : "", c);
        offset += snprintf(source + offset, 65536 - offset, ").\n");
    }
    CHECK(offset < 65536);
    return source;
}

static void
test_controlled_observed_wide_public(void)
{
    const unsigned widths[] = {0, 2, 257};
    for (unsigned fixture = 0; fixture < 3; ++fixture) {
        unsigned width = widths[fixture];
        char head[321]; memset(head, 'h', sizeof(head) - 1); head[320] = '\0';
        char *source = observed_wide_program(width, fixture != 0, head);
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options); options.evaluation_control = control;
        wl_session_t *session = NULL, *reference = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
        observed_multi_capture_t *actual = calloc(1, sizeof(*actual));
        observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
        CHECK(actual && expected);
        wl_session_set_delta_cb(session, observed_multi_capture, actual);
        wl_session_set_delta_cb(reference, observed_multi_capture, expected);
        CHECK(!observed_multi_step(session));
        CHECK(!wl_session_step(reference));
        CHECK(!actual->count && !expected->count);
        int64_t rows[257];
        for (unsigned cycle = 0; cycle < 4; ++cycle) {
            const char *input = cycle < 2 ? "input" : "other";
            for (unsigned c = 0; c < width; ++c)
                rows[c] = wl_columnar_float_to_bits((double)(cycle / 2 + c) +
                        0.5);
            if (!(cycle % 2)) {
                CHECK(!wl_session_insert(session, input, rows, 1, width));
                CHECK(!wl_session_insert(reference, input, rows, 1, width));
            } else {
                CHECK(!wl_session_remove(session, input, rows, 1, width));
                CHECK(!wl_session_remove(reference, input, rows, 1, width));
            }
            actual->count = expected->count = 0;
            int rc = observed_multi_step(session);
            if (rc) fprintf(stderr, "wide fixture %u cycle %u rc %d\n", fixture,
                    cycle, rc);
            CHECK(!rc);
            CHECK(!wl_session_step(reference));
            CHECK(actual->count == expected->count);
            qsort(actual->events, actual->count, sizeof(actual->events[0]),
                observed_multi_order);
            qsort(expected->events, expected->count,
                sizeof(expected->events[0]), observed_multi_order);
            CHECK(!memcmp(actual->events, expected->events,
                actual->count * sizeof(actual->events[0])));
            const char *heads[] = {head, "mirror", "independent"};
            for (unsigned h = 0; h < 3; ++h) {
                col_rel_t *a = session_find_rel((wl_col_session_t *)session,
                        heads[h]);
                col_rel_t *b = session_find_rel((wl_col_session_t *)reference,
                        heads[h]);
                CHECK(a && a->ncols == width && a->nrows == (b ? b->nrows : 0));
                for (uint32_t r = 0; r < a->nrows; ++r)
                    for (uint32_t c = 0; c < width;
                        ++c) CHECK(a->columns[c][r] == b->columns[c][r]);
            }
        }
        wl_session_destroy(session); wl_session_destroy(reference);
        wl_evaluation_control_release(control); wl_plan_free(plan);
        wirelog_program_free(program);
        free(actual); free(expected); free(source);
    }
}

static void
test_controlled_observed_transaction_budget(void)
{
    uint64_t exact = 0;
    const uint64_t teardown_limits[] = {64, 128, 4096, 12000};
    for (unsigned trial = 0; trial < 9; ++trial) {
        bool teardown = trial >= 3 && trial < 7;
        uint64_t limit = trial == 7 ? 4096 : trial == 8 ? UINT64_C(1000000000)
            : teardown ? teardown_limits[trial - 3]
            : trial == 0 ? 0 : trial == 1 ? exact : exact - 1;
        char *source = observed_wide_program(2, true, "output");
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(limit, &control));
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor);
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        options.memory_governor = governor;
        wl_session_t *session = NULL, *reference = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        const int64_t row[] = { INT64_C(0x3ff0000000000000),
                                INT64_C(0x4000000000000000) };
        CHECK(!wl_session_insert(session, "input", row, 1, 2));
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
        CHECK(!wl_session_insert(reference, "input", row, 1, 2));
        observed_multi_capture_t *capture = calloc(1, sizeof(*capture));
        observed_multi_capture_t *expected = calloc(1, sizeof(*expected));
        CHECK(capture && expected);
        wl_session_set_delta_cb(session, observed_multi_capture, capture);
        wl_session_set_delta_cb(reference, observed_multi_capture, expected);
        if (trial == 8) {
            observed_multi_stop_phase = 8;
            observed_multi_stop_index = 1;
            observed_multi_stop_work = observed_multi_stop_in_child = true;
            wl_columnar_session_test_observed_transaction_checkpoint =
                observed_multi_stop;
        }
        int rc = observed_multi_step(session);
        if (trial == 8) {
            CHECK(!wl_columnar_session_test_observed_transaction_checkpoint);
            observed_multi_stop_work = observed_multi_stop_in_child = false;
        }
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(control, &report));
        if (trial == 0) {
            CHECK(!rc && capture->count == 2);
            exact = report.consumed;
            CHECK(exact > teardown_limits[3]);
        } else if (trial == 1) {
            CHECK(!rc && report.consumed == exact && capture->count == 2);
        } else {
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !capture->count);
            CHECK(!session_find_rel((wl_col_session_t *)session, "output"));
            CHECK(!session_find_rel((wl_col_session_t *)session, "mirror"));
            if (!teardown) {
                CHECK(!wl_evaluation_control_reset(control));
                CHECK(!wl_evaluation_control_configure(control, 0));
                CHECK(!observed_multi_step(session) && capture->count == 2);
                wl_evaluation_control_report_t resumed;
                CHECK(!wl_evaluation_control_report(control, &resumed));
                CHECK(resumed.attempt_id > report.attempt_id && !resumed.limit);
            }
        }
        if (teardown) {
            CHECK(wl_session_try_destroy(&session) == EBUSY && session);
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control, 0));
            wl_evaluation_control_request_cancel(control);
            CHECK(observed_multi_step(session) ==
                WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(!capture->count);
        }
        if (!teardown) {
            CHECK(!wl_session_step(reference));
            CHECK(capture->count == expected->count);
            qsort(capture->events, capture->count, sizeof(capture->events[0]),
                observed_multi_order);
            qsort(expected->events, expected->count,
                sizeof(expected->events[0]), observed_multi_order);
            CHECK(!memcmp(capture->events, expected->events,
                capture->count * sizeof(capture->events[0])));
            unsigned delivered = capture->count;
            CHECK(!observed_multi_step(session) && capture->count == delivered);
        }
        wl_session_destroy(reference);
        unsigned attempts = 0;
        while (session) {
            rc = wl_session_try_destroy(&session);
            CHECK(!rc || rc == EBUSY);
            CHECK(++attempts < 10000);
        }
        wl_evaluation_control_release(control); wl_plan_free(plan);
        wirelog_program_free(program);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
        free(expected); free(capture); free(source);
    }
}

static void
test_controlled_observed_missing_private_head(void)
{
    char *source = observed_wide_program(2, true, "output");
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    const int64_t row[] = { INT64_C(0x3ff0000000000000),
                            INT64_C(0x4000000000000000) };
    CHECK(!wl_session_insert(session, "input", row, 1, 2));
    col_rel_t *old = source_new("output", 2, 1, true, 11);
    CHECK(!session_add_rel((wl_col_session_t *)session, old));
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(old);
    observed_multi_capture_t *capture = calloc(1, sizeof(*capture));
    CHECK(capture);
    wl_session_set_delta_cb(session, observed_multi_capture, capture);
    const wl_plan_relation_t *first = NULL;
    for (uint32_t si = 0; si < plan->stratum_count; ++si)
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ++ri)
            if (!strcmp(plan->strata[si].relations[ri].name, "output"))
                first = &plan->strata[si].relations[ri];
    CHECK(first && first->ops[0].op == WL_PLAN_OP_VARIABLE);
    wl_plan_op_t *op = (wl_plan_op_t *)&first->ops[0];
    const char *input_name = op->relation_name;
    /* A required private head is deliberately not computed yet. Its older
     * public descriptor must never satisfy the private dependency lookup. */
    op->relation_name = first->name;
    int missing_rc = observed_multi_step(session);
    CHECK(missing_rc == ENOENT);
    CHECK(!capture->count && session_find_rel((wl_col_session_t *)session,
        "output") == old);
    CHECK(wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(old)));
    CHECK(!session_find_rel((wl_col_session_t *)session, "mirror"));
    op->relation_name = input_name;
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!observed_multi_step(session));
    CHECK(capture->count == 3); /* old output removed, both new heads inserted */
    wl_session_destroy(session);
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
    free(source); free(capture);
}

static void
test_controlled_observed_step(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    const int64_t rows[] = {7, 3, 7, 11};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(6, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_insert(session, "input", rows, 4, 1));
    CHECK(!session_find_rel((wl_col_session_t *)session, "output"));
    controlled_observer_capture_t capture = {
        .control = control, .cancel_on_first = true
    };
    wl_session_set_delta_cb(session, controlled_observer_capture, &capture);
    controlled_observer_framed_lookup_hits = 0;
    wl_columnar_eval_serial_test_framed_checkpoint =
        controlled_observer_count_rule_start;

    CHECK(wl_session_step(session) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!session_find_rel((wl_col_session_t *)session, "output")
        && capture.count == 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));

    /* Cancellation during stream setup discards its reader lease. The
     * committed output survives, and a retry must not rerun the framed rule. */
    controlled_observer_boundary_control = control;
    controlled_observer_boundary_hits = 0;
    wl_columnar_eval_delta_test_observer_boundary =
        controlled_observer_cancel_after_evaluation;
    int observed_rc = wl_session_step(session);
    CHECK(observed_rc == WL_EVALUATION_CONTROL_CANCELLED);
    wl_columnar_eval_delta_test_observer_boundary = NULL;
    CHECK(controlled_observer_boundary_hits == 1);
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    const col_rel_t *output = session_find_rel((wl_col_session_t *)session,
            "output");
    CHECK(output && output->nrows == 3 && capture.count == 0);
    CHECK(controlled_observer_framed_lookup_hits == 1);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_session_snapshot(session, controlled_observer_snapshot_capture,
        NULL) == EBUSY);

    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    wl_columnar_eval_delta_test_fail_observer_stream_release_once = true;
    int retry_rc = wl_session_step(session);
    CHECK(retry_rc == EIO);
    CHECK(capture.count == 3);
    CHECK(wl_session_snapshot(session,
        controlled_observer_snapshot_capture, NULL) == EBUSY);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!controlled_recursive_run(session, control));
    CHECK(capture.count == 3 && controlled_observer_framed_lookup_hits == 1);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == 0
        && report.stop_reason == WL_EVALUATION_CONTROL_NONE);
    CHECK(capture.rows[0] == 3 && capture.rows[1] == 7
        && capture.rows[2] == 11);
    CHECK(capture.diffs[0] == 1 && capture.diffs[1] == 1
        && capture.diffs[2] == 1);
    CHECK(controlled_observer_framed_lookup_hits == 1);
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;

    /* A completed observed STEP admits a supported one-row EDB mutation.
     * The successor captures the prior head, recomputes, and publishes the
     * signed set difference exactly once. */
    int64_t added_row = 13;
    CHECK(!wl_session_insert(session, "input", &added_row, 1, 1));
    CHECK(wl_session_snapshot(session,
        controlled_observer_snapshot_capture, NULL) == EBUSY);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!controlled_recursive_run(session, control));
    CHECK(capture.count == 4 && capture.rows[3] == 13
        && capture.diffs[3] == 1);
    output = session_find_rel((wl_col_session_t *)session, "output");
    CHECK(output && output->nrows == 4 && output->columns[0][0] == 3
        && output->columns[0][1] == 7 && output->columns[0][2] == 11
        && output->columns[0][3] == 13);
    CHECK(!wl_session_snapshot(session,
        controlled_observer_snapshot_capture, NULL));

    int64_t removed_row = 3;
    CHECK(!wl_session_remove(session, "input", &removed_row, 1, 1));
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!controlled_recursive_run(session, control));
    CHECK(capture.count == 5 && capture.rows[4] == 3
        && capture.diffs[4] == -1);
    output = session_find_rel((wl_col_session_t *)session, "output");
    CHECK(output && output->nrows == 3 && output->columns[0][0] == 7
        && output->columns[0][1] == 11 && output->columns[0][2] == 13);
    CHECK(!wl_session_snapshot(session,
        controlled_observer_snapshot_capture, NULL));

    /* Cancellation requested by user code is after the final cutoff. It does
     * not truncate delivery or change this attempt's successful result. */
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!controlled_recursive_run(session, control));
    CHECK(capture.count == 5);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == 0 && report.consumed == 0);

    /* Destruction after publication drains the retained stream lease without
     * replaying the callback batch or the framed rule. */
    wl_evaluation_control_t *destroy_control = NULL;
    CHECK(!wl_evaluation_control_create(0, &destroy_control));
    wl_session_options_init(&options);
    options.evaluation_control = destroy_control;
    wl_session_t *destroy_session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &destroy_session));
    CHECK(!wl_session_insert(destroy_session, "input", rows, 4, 1));
    controlled_observer_capture_t destroy_capture = {
        .control = destroy_control, .cancel_on_first = true
    };
    wl_session_set_delta_cb(destroy_session, controlled_observer_capture,
        &destroy_capture);
    controlled_observer_framed_lookup_hits = 0;
    wl_columnar_eval_serial_test_framed_checkpoint =
        controlled_observer_count_rule_start;
    wl_columnar_eval_delta_test_fail_observer_stream_release_once = true;
    CHECK(wl_session_step(destroy_session) == EIO);
    CHECK(destroy_capture.count == 3
        && controlled_observer_framed_lookup_hits == 1);
    CHECK(wl_session_snapshot(destroy_session,
        controlled_observer_snapshot_capture, NULL) == EBUSY);
    wl_session_destroy(destroy_session);
    CHECK(destroy_capture.count == 3
        && controlled_observer_framed_lookup_hits == 1);
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;
    wl_evaluation_control_release(destroy_control);

    /* A fresh run with one unit less than the measured complete budget stops
     * before publication. Its committed rule is retained and retry emits the
     * canonical batch once. */
    wl_evaluation_control_t *measure_control = NULL;
    CHECK(!wl_evaluation_control_create(0, &measure_control));
    wl_session_options_init(&options);
    options.evaluation_control = measure_control;
    wl_session_t *measure = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &measure));
    CHECK(!wl_session_insert(measure, "input", rows, 4, 1));
    controlled_observer_capture_t measure_capture = {
        .control = measure_control
    };
    wl_session_set_delta_cb(measure, controlled_observer_capture,
        &measure_capture);
    CHECK(!wl_session_step(measure));
    CHECK(!wl_evaluation_control_report(measure_control, &report));
    uint64_t complete_budget = report.consumed;
    CHECK(complete_budget > 6 && measure_capture.count == 3);
    wl_session_destroy(measure);
    wl_evaluation_control_release(measure_control);
    CHECK(complete_budget > 6);

    /* A bounded attempt stops partway through the charged sortedness scan.
     * The lease and cursor remain live; retry completes without re-evaluation. */
    wl_evaluation_control_t *validation_control = NULL;
    CHECK(!wl_evaluation_control_create(complete_budget - 6,
        &validation_control));
    wl_session_options_init(&options);
    options.evaluation_control = validation_control;
    wl_session_t *validation_session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &validation_session));
    CHECK(!wl_session_insert(validation_session, "input", rows, 4, 1));
    controlled_observer_capture_t validation_capture = {
        .control = validation_control
    };
    wl_session_set_delta_cb(validation_session, controlled_observer_capture,
        &validation_capture);
    controlled_observer_framed_lookup_hits = 0;
    wl_columnar_eval_serial_test_framed_checkpoint =
        controlled_observer_count_rule_start;
    wl_columnar_session_test_observer_precharged_events = 0;
    CHECK(wl_session_step(validation_session)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_columnar_eval_delta_test_stream_validation_rows > 0
        && wl_columnar_eval_delta_test_stream_validation_rows < 3);
    CHECK(wl_columnar_session_test_observer_precharged_events == 0);
    CHECK(validation_capture.count == 0
        && controlled_observer_framed_lookup_hits == 1);
    CHECK(wl_session_snapshot(validation_session,
        controlled_observer_snapshot_capture, NULL) == EBUSY);
    CHECK(!wl_evaluation_control_reset(validation_control));
    CHECK(!wl_session_step(validation_session));
    CHECK(validation_capture.count == 3
        && controlled_observer_framed_lookup_hits == 1);
    wl_session_destroy(validation_session);
    wl_evaluation_control_release(validation_control);
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;

    wl_evaluation_control_t *limited_control = NULL;
    CHECK(!wl_evaluation_control_create(complete_budget - 1,
        &limited_control));
    wl_session_options_init(&options);
    options.evaluation_control = limited_control;
    wl_session_t *limited = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &limited));
    CHECK(!wl_session_insert(limited, "input", rows, 4, 1));
    controlled_observer_capture_t limited_capture = {
        .control = limited_control
    };
    wl_session_set_delta_cb(limited, controlled_observer_capture,
        &limited_capture);
    wl_columnar_session_test_observer_precharged_events = 0;
    CHECK(wl_session_step(limited) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_report(limited_control, &report)
        && report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    const col_rel_t *limited_output = session_find_rel(
        (wl_col_session_t *)limited, "output");
    CHECK(limited_output && limited_output->nrows == 3
        && limited_capture.count == 0);
    CHECK(wl_columnar_eval_delta_test_stream_validation_rows == 3
        && wl_columnar_session_test_observer_precharged_events > 0);
    CHECK(wl_session_snapshot(limited,
        controlled_observer_snapshot_capture, NULL) == EBUSY);
    CHECK(!wl_evaluation_control_reset(limited_control));
    CHECK(!wl_session_step(limited));
    CHECK(limited_capture.count == 3);
    wl_session_destroy(limited);
    wl_evaluation_control_release(limited_control);

    /* A stopped prepublication stream retains admission and its output
     * reader. Cancel and acknowledge it before checked destruction. */
    wl_evaluation_control_t *destroy_limited_control = NULL;
    CHECK(!wl_evaluation_control_create(complete_budget - 1,
        &destroy_limited_control));
    wl_session_options_init(&options);
    options.evaluation_control = destroy_limited_control;
    wl_session_t *destroy_limited = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &destroy_limited));
    CHECK(!wl_session_insert(destroy_limited, "input", rows, 4, 1));
    controlled_observer_capture_t destroy_limited_capture = {
        .control = destroy_limited_control
    };
    wl_session_set_delta_cb(destroy_limited, controlled_observer_capture,
        &destroy_limited_capture);
    controlled_observer_framed_lookup_hits = 0;
    wl_columnar_eval_serial_test_framed_checkpoint =
        controlled_observer_count_rule_start;
    CHECK(wl_session_step(destroy_limited)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    col_rel_t *destroy_limited_output = session_find_rel(
        (wl_col_session_t *)destroy_limited, "output");
    CHECK(destroy_limited_output && destroy_limited_output->nrows == 3
        && destroy_limited_capture.count == 0);
    wl_columnar_source_access_writer_t output_writer = { 0 };
    CHECK(col_rel_source_writer_acquire(destroy_limited_output,
        &output_writer) == EBUSY);
    CHECK(controlled_observer_framed_lookup_hits == 1);
    CHECK(wl_session_try_destroy(&destroy_limited) == EBUSY
        && destroy_limited);
    CHECK(!wl_evaluation_control_reset(destroy_limited_control));
    CHECK(!wl_evaluation_control_configure(destroy_limited_control, 0));
    wl_evaluation_control_request_cancel(destroy_limited_control);
    CHECK(wl_session_step(destroy_limited) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_session_try_destroy(&destroy_limited) && !destroy_limited);
    CHECK(destroy_limited_capture.count == 0
        && controlled_observer_framed_lookup_hits == 1);
    CHECK(!wl_evaluation_control_active_attempt_id(destroy_limited_control));
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;
    wl_evaluation_control_release(destroy_limited_control);

    /* Empty input still has a declared unary schema. It produces no events,
     * while the virtual baseline remains invisible until normal commit. */
    wl_evaluation_control_t *empty_control = NULL;
    CHECK(!wl_evaluation_control_create(0, &empty_control));
    wl_session_options_init(&options);
    options.evaluation_control = empty_control;
    wl_session_t *empty_session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &empty_session));
    CHECK(!wl_session_insert(empty_session, "input", rows, 0, 1));
    controlled_observer_capture_t empty_capture = { .control = empty_control };
    wl_session_set_delta_cb(empty_session, controlled_observer_capture,
        &empty_capture);
    int empty_rc = wl_session_step(empty_session);
    CHECK(!empty_rc);
    CHECK(empty_capture.count == 0);
    const col_rel_t *empty_output = session_find_rel(
        (wl_col_session_t *)empty_session, "output");
    CHECK(empty_output && empty_output->nrows == 0);
    wl_session_destroy(empty_session);
    wl_evaluation_control_release(empty_control);

    /* Callback replacement affects remaining events; clearing cancels only
     * subsequent notifications from the current batch. */
    for (unsigned mode = 0; mode < 2; ++mode) {
        wl_evaluation_control_t *callback_control = NULL;
        CHECK(!wl_evaluation_control_create(0, &callback_control));
        wl_session_options_init(&options);
        options.evaluation_control = callback_control;
        wl_session_t *callback_session = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &callback_session));
        CHECK(!wl_session_insert(callback_session, "input", rows, 4, 1));
        unsigned replacement_count = 0;
        controlled_observer_replacement_t replacement = {
            .session = callback_session,
            .replacement_count = &replacement_count
        };
        controlled_observer_clear_t clear = { .session = callback_session };
        if (mode == 0)
            wl_session_set_delta_cb(callback_session,
                controlled_observer_replace_callback, &replacement);
        else
            wl_session_set_delta_cb(callback_session,
                controlled_observer_clear_callback, &clear);
        CHECK(!wl_session_step(callback_session));
        CHECK(mode == 0 ? replacement.count == 1
            && replacement_count == 2 : clear.count == 1);
        wl_session_destroy(callback_session);
        wl_evaluation_control_release(callback_control);
    }

    /* One unit below a measured successor budget retains the old model and
     * resumes the private attempt without rerunning its framed rule. */
    wl_evaluation_control_t *successor_measure_control = NULL;
    CHECK(!wl_evaluation_control_create(0, &successor_measure_control));
    wl_session_options_init(&options);
    options.evaluation_control = successor_measure_control;
    wl_session_t *successor_measure = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &successor_measure));
    CHECK(!wl_session_insert(successor_measure, "input", rows, 4, 1));
    controlled_observer_capture_t successor_measure_capture = {
        .control = successor_measure_control
    };
    wl_session_set_delta_cb(successor_measure, controlled_observer_capture,
        &successor_measure_capture);
    CHECK(!wl_session_step(successor_measure));
    int64_t successor_row = 13;
    CHECK(!wl_session_insert(successor_measure, "input", &successor_row,
        1, 1));
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    CHECK(!wl_session_step(successor_measure));
    CHECK(!wl_evaluation_control_report(successor_measure_control, &report));
    uint64_t successor_budget = report.consumed;
    CHECK(successor_budget > 1 && successor_measure_capture.count == 4);
    CHECK(!wl_session_snapshot(successor_measure,
        controlled_observer_snapshot_capture, NULL));
    int64_t failed_prepare_row = 17;
    CHECK(!wl_session_insert(successor_measure, "input",
        &failed_prepare_row, 1, 1));
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    wl_columnar_eval_delta_test_fail_prepare_current_alloc = true;
    CHECK(wl_session_step(successor_measure) == ENOMEM);
    CHECK(successor_measure_capture.count == 4);
    output = session_find_rel((wl_col_session_t *)successor_measure,
            "output");
    CHECK(output && output->nrows == 4 && output->columns[0][3] == 13);
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    CHECK(!wl_session_step(successor_measure));
    CHECK(successor_measure_capture.count == 5
        && successor_measure_capture.rows[4] == 17
        && successor_measure_capture.diffs[4] == 1);
    int64_t contended_row = 19;
    CHECK(!wl_session_insert(successor_measure, "input", &contended_row,
        1, 1));
    controlled_observer_framed_lookup_hits = 0;
    wl_columnar_eval_serial_test_framed_checkpoint =
        controlled_observer_count_rule_start;
    wl_columnar_session_test_before_successor_registration =
        controlled_observer_hold_output_for_registration;
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    CHECK(wl_session_step(successor_measure) == EBUSY);
    CHECK(controlled_observer_registration_reader.owner
        && successor_measure_capture.count == 5);
    output = session_find_rel((wl_col_session_t *)successor_measure,
            "output");
    CHECK(output && output->nrows == 5 && output->columns[0][4] == 17);
    unsigned contended_lookup_hits = controlled_observer_framed_lookup_hits;
    CHECK(!col_rel_source_reader_release(
            &controlled_observer_registration_reader));
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    CHECK(!wl_session_step(successor_measure));
    CHECK(successor_measure_capture.count == 6
        && successor_measure_capture.rows[5] == 19
        && successor_measure_capture.diffs[5] == 1
        && controlled_observer_framed_lookup_hits == contended_lookup_hits);
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;

    int64_t registry_scan_row = 21;
    CHECK(!wl_session_insert(successor_measure, "input", &registry_scan_row,
        1, 1));
    memset(controlled_registry_scan_indices, 0,
        sizeof(controlled_registry_scan_indices));
    memset(controlled_registry_scan_counts, 0,
        sizeof(controlled_registry_scan_counts));
    controlled_registry_stop_count = 0;
    controlled_observer_framed_lookup_hits = 0;
    wl_columnar_eval_serial_test_framed_checkpoint =
        controlled_observer_count_rule_start;
    wl_columnar_session_test_successor_registry_charge =
        controlled_observer_stop_registry_scan;
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    CHECK(!wl_evaluation_control_configure(successor_measure_control,
        UINT64_MAX - 1));
    CHECK(wl_session_step(successor_measure)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(controlled_registry_stop_count == 1
        && controlled_registry_scan_counts[0] == 1
        && controlled_registry_scan_indices[0][0] == 0
        && successor_measure_capture.count == 6);
    unsigned registry_scan_lookup_hits = controlled_observer_framed_lookup_hits;
    output = session_find_rel((wl_col_session_t *)successor_measure,
            "output");
    CHECK(output && output->nrows == 6 && output->columns[0][5] == 19);
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    CHECK(!wl_evaluation_control_configure(successor_measure_control,
        UINT64_MAX - 1));
    CHECK(wl_session_step(successor_measure)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(controlled_registry_stop_count == 2
        && controlled_registry_scan_counts[0] >= 3
        && controlled_registry_scan_indices[0][1] == 1
        && controlled_registry_scan_indices[1][0] == 0
        && successor_measure_capture.count == 6
        && controlled_observer_framed_lookup_hits
        == registry_scan_lookup_hits);
    CHECK(!wl_evaluation_control_reset(successor_measure_control));
    CHECK(!wl_evaluation_control_configure(successor_measure_control, 0));
    CHECK(!wl_session_step(successor_measure));
    CHECK(controlled_registry_scan_counts[1] == 3
        && controlled_registry_scan_indices[1][1] == 1
        && successor_measure_capture.count == 7
        && successor_measure_capture.rows[6] == 21
        && successor_measure_capture.diffs[6] == 1
        && controlled_observer_framed_lookup_hits
        == registry_scan_lookup_hits);
    wl_columnar_session_test_successor_registry_charge = NULL;
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;
    wl_session_destroy(successor_measure);
    wl_evaluation_control_release(successor_measure_control);

    wl_evaluation_control_t *successor_limited_control = NULL;
    CHECK(!wl_evaluation_control_create(0, &successor_limited_control));
    wl_session_options_init(&options);
    options.evaluation_control = successor_limited_control;
    wl_session_t *successor_limited = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &successor_limited));
    CHECK(!wl_session_insert(successor_limited, "input", rows, 4, 1));
    controlled_observer_capture_t successor_limited_capture = {
        .control = successor_limited_control
    };
    wl_session_set_delta_cb(successor_limited, controlled_observer_capture,
        &successor_limited_capture);
    CHECK(!wl_session_step(successor_limited));
    CHECK(!wl_session_insert(successor_limited, "input", &successor_row,
        1, 1));
    CHECK(!wl_evaluation_control_reset(successor_limited_control));
    CHECK(!wl_evaluation_control_configure(successor_limited_control,
        successor_budget - 1));
    controlled_observer_framed_lookup_hits = 0;
    wl_columnar_eval_serial_test_framed_checkpoint =
        controlled_observer_count_rule_start;
    CHECK(wl_session_step(successor_limited)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(successor_limited_capture.count == 3
        && controlled_observer_framed_lookup_hits != 0);
    const col_rel_t *limited_head = session_find_rel(
        (wl_col_session_t *)successor_limited, "output");
    CHECK(limited_head && limited_head->nrows == 3
        && limited_head->columns[0][0] == 3
        && limited_head->columns[0][1] == 7
        && limited_head->columns[0][2] == 11);
    unsigned limited_lookup_hits = controlled_observer_framed_lookup_hits;
    CHECK(wl_session_snapshot(successor_limited,
        controlled_observer_snapshot_capture, NULL) == EBUSY);
    CHECK(!wl_evaluation_control_reset(successor_limited_control));
    CHECK(!wl_evaluation_control_configure(successor_limited_control, 0));
    wl_evaluation_control_request_cancel(successor_limited_control);
    int successor_cancel_rc = wl_session_step(successor_limited);
    CHECK(successor_cancel_rc == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(successor_limited_capture.count == 3);
    limited_head = session_find_rel((wl_col_session_t *)successor_limited,
            "output");
    CHECK(limited_head && limited_head->nrows == 3
        && limited_head->columns[0][0] == 3
        && limited_head->columns[0][1] == 7
        && limited_head->columns[0][2] == 11);
    CHECK(!wl_evaluation_control_reset(successor_limited_control));
    CHECK(!wl_session_step(successor_limited));
    CHECK(successor_limited_capture.count == 4
        && successor_limited_capture.rows[3] == 13
        && successor_limited_capture.diffs[3] == 1
        && controlled_observer_framed_lookup_hits == limited_lookup_hits);
    wl_columnar_eval_serial_test_framed_checkpoint = NULL;
    wl_session_destroy(successor_limited);
    wl_evaluation_control_release(successor_limited_control);

    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static const char controlled_observed_filter_source[] =
    ".decl input(x:int64)\n"
    ".decl output(x:int64)\n"
    "output(x) :- input(x), x > 0.\n";

static wl_plan_t *
controlled_observed_filter_plan(wirelog_program_t **program)
{
    wirelog_error_t error;
    *program = wirelog_parse_string(controlled_observed_filter_source, &error);
    CHECK(*program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(*program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *r = &plan->strata[0].relations[0];
    CHECK(r->op_count == 4 && r->ops[0].op == WL_PLAN_OP_VARIABLE
        && r->ops[1].op == WL_PLAN_OP_FILTER
        && r->ops[1].filter_expr.data && r->ops[1].filter_expr.size
        && r->ops[2].op == WL_PLAN_OP_MAP
        && r->ops[3].op == WL_PLAN_OP_CONSOLIDATE);
    return plan;
}

static bool
controlled_observed_filter_has(const col_rel_t *r, int64_t value)
{
    for (uint32_t i = 0; i < r->nrows; ++i)
        if (r->columns[0][i] == value) return true;
    return false;
}

static void
controlled_observed_filter_compare(wl_session_t *controlled,
    wl_session_t *reference)
{
    const col_rel_t *actual = session_find_rel((wl_col_session_t *)controlled,
            "output");
    const col_rel_t *expected = session_find_rel((wl_col_session_t *)reference,
            "output");
    CHECK(actual && expected && actual->ncols == 1 && expected->ncols == 1
        && actual->nrows == expected->nrows);
    for (uint32_t i = 0; i < actual->nrows; ++i) {
        CHECK(controlled_observed_filter_has(expected, actual->columns[0][i]));
        CHECK(!i || actual->columns[0][i - 1] < actual->columns[0][i]);
    }
}

static void
controlled_staging_cancel(wl_evaluation_control_t *control)
{
    wl_columnar_session_test_staging_prepared = NULL;
    wl_evaluation_control_request_cancel(control);
}

static void
test_controlled_observed_filter_successors(void)
{
    const int64_t changes[] = {-9, -9, 3, 3, 12, 8};
    const bool inserts[] = {true, false, true, false, true, false};
    for (unsigned i = 0; i < 6; ++i) {
        wirelog_program_t *program = NULL;
        wl_plan_t *plan = controlled_observed_filter_plan(&program);
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor);
        options.memory_governor = governor;
        wl_session_t *session = NULL, *reference = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        wl_session_options_init(&options);
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &reference));
        const int64_t rows[] = {-9, 0, 3, 3, 8};
        CHECK(!wl_session_insert(session, "input", rows, 5, 1));
        CHECK(!wl_session_insert(reference, "input", rows, 5, 1));
        controlled_observer_capture_t capture = {.control = control};
        wl_session_set_delta_cb(session, controlled_observer_capture, &capture);
        CHECK(!controlled_recursive_run(session, control));
        CHECK(!wl_session_step(reference));
        CHECK(capture.count == 2 && capture.rows[0] == 3 && capture.rows[1] == 8
            && capture.diffs[0] == 1 && capture.diffs[1] == 1);
        controlled_observed_filter_compare(session, reference);
        const col_rel_t *old = session_find_rel((wl_col_session_t *)session,
                "output");
        uint64_t identity = old->relation_identity;
        bool before = controlled_observed_filter_has(old, changes[i]);
        uint32_t events = capture.count;
        int (*mutate)(wl_session_t *, const char *, const int64_t *,
            uint32_t, uint32_t) = inserts[i] ? wl_session_insert
            : wl_session_remove;
        CHECK(!mutate(session, "input", &changes[i], 1, 1));
        CHECK(!mutate(reference, "input", &changes[i], 1, 1));
        CHECK(session_find_rel((wl_col_session_t *)session, "output") == old
            && old->relation_identity == identity && capture.count == events);
        CHECK(wl_session_snapshot(session, controlled_observer_snapshot_capture,
            NULL) == EBUSY);
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!controlled_recursive_run(session, control));
        CHECK(!wl_session_step(reference));
        controlled_observed_filter_compare(session, reference);
        bool after = controlled_observed_filter_has(session_find_rel(
                    (wl_col_session_t *)reference, "output"), changes[i]);
        CHECK(capture.count == events + (before != after));
        if (before != after)
            CHECK(capture.rows[events] == changes[i]
                && capture.diffs[events] == (after ? 1 : -1));
        CHECK(!wl_session_step(session) && capture.count == events
            + (before != after));
        CHECK(((wl_col_session_t *)session)->nrels == 2);
        CHECK(!session_find_rel((wl_col_session_t *)session, "$r$input"));
        uint64_t cycle_baseline = 0;
        for (unsigned cycle = 0; cycle < 24; ++cycle) {
            int64_t later = cycle % 4 < 2 ? 17 : -17;
            bool insert = cycle % 2 == 0;
            capture.count = 0;
            capture.cancel_on_first = cycle == 5;
            mutate = insert ? wl_session_insert : wl_session_remove;
            CHECK(!mutate(session, "input", &later, 1, 1));
            CHECK(!mutate(reference, "input", &later, 1, 1));
            CHECK(!wl_evaluation_control_reset(control));
            if (cycle == 1) {
                wl_columnar_session_test_staging_prepared =
                    controlled_staging_cancel;
                CHECK(controlled_recursive_run(session,
                    control) == WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(!wl_columnar_session_test_staging_prepared &&
                    !capture.count);
                CHECK(controlled_observed_filter_has(session_find_rel(
                        (wl_col_session_t *)session, "output"), 17));
                CHECK(!wl_evaluation_control_reset(control));
            }
            CHECK(!controlled_recursive_run(session, control));
            CHECK(!wl_session_step(reference));
            controlled_observed_filter_compare(session, reference);
            CHECK(capture.count == (later > 0 ? 1u : 0u));
            if (capture.count) CHECK(capture.rows[0] == later
                    && capture.diffs[0] == (insert ? 1 : -1));
            CHECK(((wl_col_session_t *)session)->nrels == 2);
            CHECK(!session_find_rel((wl_col_session_t *)session, "$r$input"));
            if (cycle == 5) {
                CHECK(wl_session_snapshot(session,
                    controlled_observer_snapshot_capture, NULL) ==
                    WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(capture.count == 1);
                CHECK(!wl_evaluation_control_reset(control));
            }
            CHECK(!wl_session_step(session));
            if (cycle % 4 == 3) {
                uint64_t current = wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor));
                if (cycle == 3) cycle_baseline = current;
                else CHECK(current == cycle_baseline);
            }
            CHECK(capture.count == (later > 0 ? 1u : 0u));
        }
        wl_session_destroy(reference);
        wl_session_destroy(session);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
        /* The program's intern table also retains the supplied governor. */
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

static wl_evaluation_control_t *controlled_observed_filter_control;
static uint64_t controlled_observed_filter_visits;
static bool controlled_observed_filter_cancel;
static wl_col_session_t *controlled_observed_filter_session;
static const void *controlled_observed_filter_baseline;

static void
controlled_observed_filter_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    if (control != controlled_observed_filter_control || phase != 13) return;
    if (controlled_observed_filter_baseline)
        CHECK(controlled_observed_filter_session->delta_observer
            == controlled_observed_filter_baseline);
    CHECK(progress == controlled_observed_filter_visits);
    controlled_observed_filter_visits++;
    if (controlled_observed_filter_cancel && progress == 127)
        wl_evaluation_control_request_cancel(control);
}

static void
test_controlled_observed_filter_stops(void)
{
    enum { ROWS = 512 };
    int64_t rows[ROWS];
    for (unsigned i = 0; i < ROWS; ++i) rows[i] = -(int64_t)i - 1;
    /* All initial rows are rejected. A successor inserts one qualifying row
     * after that rejected prefix, so stopping the scan cannot emit it. */
    for (unsigned successor = 0; successor < 2; ++successor) {
        uint64_t calibrated_limit = 0;
        for (unsigned trial = 0; trial < 4; ++trial) {
            bool limited = (trial & 1) != 0;
            bool destroy_stopped = trial >= 2;
            wirelog_program_t *program = NULL;
            wl_plan_t *plan = controlled_observed_filter_plan(&program);
            wl_evaluation_control_t *control = NULL;
            CHECK(!wl_evaluation_control_create(0, &control));
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = control;
            wl_session_t *session = NULL;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1, &options, &session));
            CHECK(!wl_session_insert(session, "input", rows, ROWS, 1));
            controlled_observer_capture_t capture = {.control = control};
            wl_session_set_delta_cb(session, controlled_observer_capture,
                &capture);
            const col_rel_t *old = NULL;
            uint64_t old_identity = 0;
            if (successor) {
                CHECK(!controlled_recursive_run(session, control));
                old = session_find_rel((wl_col_session_t *)session, "output");
                CHECK(old && !old->nrows && !capture.count);
                old_identity = old->relation_identity;
                int64_t value = 9;
                CHECK(!wl_session_insert(session, "input", &value, 1, 1));
                CHECK(!wl_evaluation_control_reset(control));
            }
            CHECK(!wl_evaluation_control_configure(control,
                limited ? calibrated_limit : 0));
            controlled_observed_filter_control = control;
            controlled_observed_filter_visits = 0;
            controlled_observed_filter_cancel = !limited;
            wl_columnar_filter_test_attempt_checkpoint =
                controlled_observed_filter_checkpoint;
            int rc = controlled_recursive_run(session, control);
            wl_columnar_filter_test_attempt_checkpoint = NULL;
            controlled_observed_filter_control = NULL;
            CHECK(rc == (limited ? WL_EVALUATION_CONTROL_WORK_LIMIT
                    : WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(!capture.count && controlled_observed_filter_visits > 0
                && controlled_observed_filter_visits < ROWS);
            if (!limited) CHECK(controlled_observed_filter_visits == 128);
            const col_rel_t *head =
                session_find_rel((wl_col_session_t *)session,
                    "output");
            CHECK(head == old);
            if (head) CHECK(head->relation_identity == old_identity
                    && head->nrows == 0);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(control, &report)
                && report.result == rc && !report.cleanup_status);
            /* Empirical allowance for reaching the rejected scan, not an
             * independent exact-cost oracle: cancellation includes cleanup. */
            if (!limited) calibrated_limit = report.consumed;
            CHECK(calibrated_limit > 0);
            if (destroy_stopped) {
                controlled_observed_filter_control = control;
                controlled_observed_filter_cancel = false;
                wl_columnar_filter_test_attempt_checkpoint =
                    controlled_observed_filter_checkpoint;
                if (limited) {
                    CHECK(wl_session_try_destroy(&session) == EBUSY && session);
                    CHECK(!wl_evaluation_control_reset(control));
                    CHECK(!wl_evaluation_control_configure(control, 0));
                    wl_evaluation_control_request_cancel(control);
                    CHECK(controlled_recursive_run(session, control)
                        == WL_EVALUATION_CONTROL_CANCELLED);
                }
                uint64_t visits = controlled_observed_filter_visits;
                CHECK(!wl_session_try_destroy(&session) && !session);
                CHECK(!capture.count && controlled_observed_filter_visits
                    == visits);
                wl_columnar_filter_test_attempt_checkpoint = NULL;
                controlled_observed_filter_control = NULL;
                wl_evaluation_control_release(control);
                wl_plan_free(plan);
                wirelog_program_free(program);
                continue;
            }
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control, 0));
            controlled_observed_filter_session = (wl_col_session_t *)session;
            controlled_observed_filter_baseline =
                controlled_observed_filter_session->delta_observer;
            CHECK(controlled_observed_filter_baseline);
            controlled_observed_filter_control = control;
            controlled_observed_filter_cancel = false;
            controlled_observed_filter_visits = 0;
            wl_columnar_filter_test_attempt_checkpoint =
                controlled_observed_filter_checkpoint;
            CHECK(!controlled_recursive_run(session, control));
            CHECK(controlled_observed_filter_visits == ROWS + successor);
            wl_columnar_filter_test_attempt_checkpoint = NULL;
            controlled_observed_filter_control = NULL;
            controlled_observed_filter_baseline = NULL;
            controlled_observed_filter_session = NULL;
            head = session_find_rel((wl_col_session_t *)session, "output");
            CHECK(head && head->nrows == successor &&
                capture.count == successor);
            if (successor) CHECK(capture.rows[0] == 9 && capture.diffs[0] == 1);
            CHECK(!wl_session_step(session) && capture.count == successor);
            col_rel_t *input = session_find_rel((wl_col_session_t *)session,
                    "input");
            wl_columnar_source_access_writer_t writer = {0};
            CHECK(!col_rel_source_writer_acquire(input, &writer));
            CHECK(!wl_columnar_source_access_writer_release(&writer));
            wl_session_destroy(session);
            wl_evaluation_control_release(control);
            wl_plan_free(plan);
            wirelog_program_free(program);
        }
    }
}

static void
test_controlled_observed_preflight_rejections(void)
{
    static const char unsupported[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x+1) :- input(x), x > 0.\n";
    static const char wrong_type[] =
        ".decl input(x:float)\n"
        ".decl output(x:float)\n"
        "output(x) :- input(x).\n";
    static const char valid[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    const char *sources[] = { unsupported, wrong_type, valid, valid };
    for (unsigned i = 0; i < 4; ++i) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(sources[i], &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *session = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        if (i != 1) {
            const int64_t input[] = { 1 };
            CHECK(!wl_session_insert(session, "input", input, 1, 1));
        }
        col_rel_t *registered_empty = NULL;
        if (i == 2) {
            CHECK(!col_rel_alloc(&registered_empty, "output"));
            CHECK(!col_rel_set_schema(registered_empty, 1, NULL));
            CHECK(!session_add_rel((wl_col_session_t *)session,
                registered_empty));
        }
        if (i == 3)
            CHECK(!wl_columnar_eval_delta_observer_begin(
                    (wl_col_session_t *)session, UINT64_MAX));
        controlled_observer_capture_t capture = { .control = control };
        wl_session_set_delta_cb(session, controlled_observer_capture,
            &capture);
        if (i == 1) {
            CHECK(!observed_multi_step(session));
            CHECK(capture.count == 0);
            wl_session_destroy(session);
            wl_evaluation_control_release(control);
            wl_plan_free(plan);
            wirelog_program_free(program);
            continue;
        }
        CHECK(wl_session_step(session) == ENOTSUP);
        CHECK(!wl_evaluation_control_active_attempt_id(control));
        wl_evaluation_control_report_t report;
        if (i == 2)
            CHECK(!wl_evaluation_control_report(control, &report)
                && report.result == ENOTSUP && report.consumed > 0);
        else
            CHECK(wl_evaluation_control_report(control, &report) == ENOENT);
        if (registered_empty) {
            CHECK(session_find_rel((wl_col_session_t *)session, "output")
                == registered_empty && registered_empty->nrows == 0);
        }
        if (i == 3) {
            wl_columnar_eval_delta_observer_set_active(
                (wl_col_session_t *)session, false);
            CHECK(!wl_columnar_eval_delta_observer_discard(
                    (wl_col_session_t *)session));
        }
        wl_session_destroy(session);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
    }

}

static uint64_t observed_admission_max_byte;
static unsigned observed_admission_events;

static void
observed_admission_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t byte)
{
    if (phase != 2) return;
    if (byte > observed_admission_max_byte) observed_admission_max_byte = byte;
    if (byte == 64) wl_evaluation_control_request_cancel(control);
}

static void
observed_admission_capture(const char *name, const int64_t *row,
    uint32_t width, int diff, void *data)
{
    (void)data;
    CHECK(name && row && width == 1 && diff == 1);
    observed_admission_events++;
}

static void
test_controlled_observed_admission_work(void)
{
    char prefix[97], source[16384], input_name[128], name[128];
    memset(prefix, 'a', sizeof(prefix) - 1);
    prefix[sizeof(prefix) - 1] = 0;
    int n = snprintf(input_name, sizeof(input_name), "%s_input", prefix);
    CHECK(n > 0 && (size_t)n < sizeof(input_name));
    size_t used = 0;
    for (unsigned i = 0; i < 32; ++i) {
        n = snprintf(source + used, sizeof(source) - used,
                ".decl %s_%u(x:int64)\n", prefix, i);
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
    }
    n = snprintf(source + used, sizeof(source) - used,
            ".decl %s(x:int64)\n.decl output(x:int64)\n"
            "output(x) :- %s(x).\n", input_name, input_name);
    CHECK(n > 0 && (size_t)n < sizeof(source) - used);
    for (unsigned exact = 0; exact < 2; ++exact) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        CHECK(plan->edb_count == 33 && plan->stratum_count == 1);
        wl_evaluation_control_t *control = NULL;
        /* Init + EDB slot + 64 compared bytes = 66 admitted actions.
         * The one-short budget cannot reach the deterministic cutoff. */
        CHECK(!wl_evaluation_control_create(exact ? 66 : 65, &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *session = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        int64_t value = 7;
        for (unsigned i = 0; i < 31; ++i) {
            n = snprintf(name, sizeof(name), "%s_%u", prefix, i);
            CHECK(n > 0 && (size_t)n < sizeof(name));
            CHECK(!wl_session_insert(session, name, &value, 1, 1));
        }
        CHECK(!wl_session_insert(session, input_name, &value, 1, 1));
        wl_col_session_t *col = (wl_col_session_t *)session;
        CHECK(col->nrels == 33);
        observed_admission_events = 0;
        wl_session_set_delta_cb(session, observed_admission_capture, NULL);
        /* Unsupported many-stratum shape is rejected by its count without
         * dereferencing fictitious trailing strata or beginning an attempt. */
        plan->stratum_count = UINT32_MAX;
        CHECK(wl_session_step(session) == ENOTSUP);
        plan->stratum_count = 1;
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == ENOENT);
        controlled_observer_framed_lookup_hits = 0;
        wl_columnar_eval_serial_test_framed_checkpoint =
            controlled_observer_count_rule_start;
        observed_admission_max_byte = 0;
        wl_columnar_session_test_observed_preflight_checkpoint =
            observed_admission_checkpoint;
        CHECK(controlled_recursive_run(session, control)
            == (exact ? WL_EVALUATION_CONTROL_CANCELLED
                : WL_EVALUATION_CONTROL_WORK_LIMIT));
        wl_columnar_session_test_observed_preflight_checkpoint = NULL;
        CHECK(observed_admission_max_byte == (exact ? 64u : 63u));
        CHECK(!wl_evaluation_control_report(control, &report)
            && report.consumed == (exact ? 66u : 65u)
            && !report.cleanup_status);
        CHECK(!controlled_observer_framed_lookup_hits
            && !observed_admission_events && !col->delta_observer
            && !session_find_rel(col, "output") && col->nrels == 33);
        /* Cancellation releases an owner-free refusal: adding a registry
         * entry invalidates the partial cursor before the next attempt. */
        if (exact) {
            n = snprintf(name, sizeof(name), "%s_31", prefix);
            CHECK(n > 0 && (size_t)n < sizeof(name));
            CHECK(!wl_session_insert(session, name, &value, 1, 1));
            CHECK(!wl_session_remove(session, name, &value, 1, 1));
            CHECK(col->nrels == 34);
        }
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_configure(control, 0));
        CHECK(!controlled_recursive_run(session, control));
        CHECK(controlled_observer_framed_lookup_hits > 0
            && observed_admission_events == 1);
        const col_rel_t *output = session_find_rel(col, "output");
        CHECK(output && output->nrows == 1 && output->columns[0][0] == 7);
        wl_columnar_eval_serial_test_framed_checkpoint = NULL;
        CHECK(!wl_session_try_destroy(&session) && !session);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

static void
test_controlled_observed_large_output(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    enum { LARGE_UNIQUE_ROWS = 5003, LARGE_INPUT_ROWS = LARGE_UNIQUE_ROWS + 2 };
    int64_t *large_rows = calloc(LARGE_INPUT_ROWS, sizeof(*large_rows));
    CHECK(large_rows);
    for (uint32_t i = 0; i < LARGE_UNIQUE_ROWS; ++i)
        large_rows[i] = (int64_t)i - (LARGE_UNIQUE_ROWS / 2);
    large_rows[LARGE_UNIQUE_ROWS] = large_rows[0];
    large_rows[LARGE_UNIQUE_ROWS + 1] = 0;
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_insert(session, "input", large_rows, LARGE_INPUT_ROWS,
        1));
    controlled_observer_order_capture_t capture = {
        .first = -(LARGE_UNIQUE_ROWS / 2)
    };
    wl_session_set_delta_cb(session, controlled_observer_order_capture,
        &capture);
    CHECK(!controlled_recursive_run(session, control));
    CHECK(capture.count == LARGE_UNIQUE_ROWS);
    const col_rel_t *output = session_find_rel((wl_col_session_t *)session,
            "output");
    CHECK(output && output->nrows == LARGE_UNIQUE_ROWS
        && output->sorted_nrows == output->nrows
        && output->run_count == 1
        && output->run_ends[0] == output->nrows);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
    free(large_rows);
}

static bool controlled_session_join_stop;
static unsigned controlled_session_join_stop_phase;
static uint64_t controlled_session_join_phase17_hits;
static uint64_t controlled_session_join_cancel_after;

static void
controlled_session_join_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    (void)progress;
    if (phase == 17)
        ++controlled_session_join_phase17_hits;
    if (!controlled_session_join_stop && phase == 17
        && controlled_session_join_phase17_hits
        == controlled_session_join_cancel_after){
        controlled_session_join_stop = true;
        controlled_session_join_stop_phase = phase;
        wl_evaluation_control_request_cancel(control);
    }
}

static void
test_controlled_session_join_step(void)
{
    static const char product[] =
        ".decl left(x:int64)\n"
        ".decl right(y:int64)\n"
        ".decl output(x:int64,y:int64)\n"
        "output(x,y) :- left(x), right(y).\n";
    static const char keyed_rejected[] =
        ".decl left(k:int64,x:int64)\n"
        ".decl right(k:int64,y:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- left(k,x), right(k,y), y < 0.\n";
    enum { LEFT_COUNT = 64, RIGHT_COUNT = 513 };
    int64_t product_left[LEFT_COUNT];
    int64_t product_right[RIGHT_COUNT];
    int64_t keyed_left[LEFT_COUNT * 2];
    int64_t keyed_right[RIGHT_COUNT * 2];
    const char *sources[] = { product, keyed_rejected };
    const int64_t *left_rows[] = { product_left, keyed_left };
    const int64_t *right_rows[] = { product_right, keyed_right };
    const uint32_t left_width[] = { 1, 2 };
    const uint32_t right_width[] = { 1, 2 };
    const uint32_t left_count[] = { LEFT_COUNT, LEFT_COUNT };
    const uint32_t right_count[] = { RIGHT_COUNT, RIGHT_COUNT };

    for (uint32_t i = 0; i < LEFT_COUNT; ++i) {
        product_left[i] = (int64_t)i + 1;
        keyed_left[i * 2] = 1;
        keyed_left[i * 2 + 1] = (int64_t)i + 1;
    }
    for (uint32_t i = 0; i < RIGHT_COUNT; ++i) {
        product_right[i] = (int64_t)i + 1;
        keyed_right[i * 2] = 1;
        keyed_right[i * 2 + 1] = (int64_t)i + 1;
    }

    for (unsigned fixture = 0; fixture < 2; ++fixture) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(sources[fixture],
                &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        bool has_join = false;
        for (uint32_t si = 0; si < plan->stratum_count; ++si)
            for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ++ri)
                for (uint32_t oi = 0;
                    oi < plan->strata[si].relations[ri].op_count; ++oi)
                    has_join |= plan->strata[si].relations[ri].ops[oi].op
                        == WL_PLAN_OP_JOIN;
        CHECK(has_join);

        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *session = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        CHECK(!wl_session_insert(session, "left", left_rows[fixture],
            left_count[fixture], left_width[fixture]));
        CHECK(!wl_session_insert(session, "right", right_rows[fixture],
            right_count[fixture], right_width[fixture]));
        controlled_session_join_stop = false;
        controlled_session_join_stop_phase = 0;
        controlled_session_join_phase17_hits = 0;
        controlled_session_join_cancel_after = 128;
        wl_columnar_join_test_attempt_checkpoint =
            controlled_session_join_checkpoint;
        CHECK(wl_session_step(session) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_columnar_join_test_attempt_checkpoint = NULL;
        CHECK(controlled_session_join_stop
            && controlled_session_join_stop_phase == 17);
        CHECK(controlled_session_join_phase17_hits == 128);
        CHECK(!session_find_rel((wl_col_session_t *)session, "output"));
        wl_evaluation_control_report_t stopped;
        CHECK(!wl_evaluation_control_report(control, &stopped)
            && stopped.result == WL_EVALUATION_CONTROL_CANCELLED
            && stopped.consumed > 0);
        uint64_t allowance = stopped.consumed;
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_configure(control, allowance - 1));
        controlled_session_join_stop = false;
        controlled_session_join_phase17_hits = 0;
        controlled_session_join_cancel_after = UINT64_MAX;
        wl_columnar_join_test_attempt_checkpoint =
            controlled_session_join_checkpoint;
        CHECK(wl_session_step(session) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_columnar_join_test_attempt_checkpoint = NULL;
        CHECK(controlled_session_join_phase17_hits == 127);
        CHECK(!session_find_rel((wl_col_session_t *)session, "output"));
        wl_evaluation_control_report_t limited;
        CHECK(!wl_evaluation_control_report(control, &limited)
            && limited.result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && limited.consumed > 0);
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_configure(control, 0));
        CHECK(!wl_session_step(session));
        const col_rel_t *output = session_find_rel((wl_col_session_t *)session,
                "output");
        CHECK(output && output->ncols == (fixture == 0 ? 2 : 1));
        if (fixture == 0) {
            CHECK(output->nrows == LEFT_COUNT * RIGHT_COUNT);
            for (uint32_t i = 0; i < output->nrows; ++i) {
                CHECK(output->columns[0][i]
                    == (int64_t)(i / RIGHT_COUNT) + 1);
                CHECK(output->columns[1][i]
                    == (int64_t)(i % RIGHT_COUNT) + 1);
            }
        } else {
            CHECK(output->nrows == 0);
        }
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(control, &report)
            && report.result == 0 && report.consumed > 0);
        wl_session_destroy(session);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

static void
test_controlled_step_preflight_byte_cursor(void)
{
    uint64_t common_overhead = UINT64_MAX;
    const unsigned quanta[] = {1, 7, 64};
    for (unsigned qi = 0; qi < sizeof(quanta) / sizeof(quanta[0]); ++qi) {
        char candidate_name[253], target_name[253], source[2048];
        memset(candidate_name, 'a', 251);
        memset(target_name, 'a', 251);
        candidate_name[251] = 'y';
        target_name[251] = 'z';
        candidate_name[252] = '\0';
        target_name[252] = '\0';
        int written = snprintf(source, sizeof(source),
                ".decl %s(x:int64)\n"
                ".decl %s(x:int64)\n"
                "%s(x) :- %s(x).\n",
                candidate_name, target_name, target_name, candidate_name);
        CHECK(written > 0 && (size_t)written < sizeof(source));
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(UINT64_C(1000000), &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *base = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &base));
        wl_col_session_t *session = (wl_col_session_t *)base;
        const int64_t row[] = {7};
        CHECK(!wl_session_insert(base, candidate_name, row, 1, 1));
        wl_columnar_session_test_preflight_quantum = quanta[qi];

        uint64_t operation_id = 0;
        bool observed_long_prefix = false;
        for (unsigned turn = 0; turn < 1000; ++turn) {
            wl_columnar_session_test_preflight_state_t before = {0}, after;
            int before_rc = wl_columnar_session_test_preflight_snapshot(base,
                    &before);
            int rc = wl_session_step(base);
            CHECK(rc == EAGAIN);
            uint64_t active_id = wl_evaluation_control_active_attempt_id(
                control);
            CHECK(active_id);
            if (!operation_id) operation_id = active_id;
            CHECK(active_id == operation_id);
            CHECK(!wl_columnar_session_test_preflight_snapshot(base, &after));
            CHECK(after.entry_charged);
            if (before_rc == 0)
                CHECK(after.charged_actions - before.charged_actions
                    <= quanta[qi]);
            else
                CHECK(after.charged_actions <= quanta[qi]);
            if (before_rc == 0 && before.target_name_byte_active
                && after.target_name_byte_active
                && before.registry_slot == after.registry_slot) {
                CHECK(after.name_byte > before.name_byte);
                CHECK(after.charged_actions - before.charged_actions
                    == after.name_byte - before.name_byte);
            }
            CHECK(!session_find_rel(session, target_name));
            if (after.target_name_byte_active && after.registry_slot == 0
                && after.name_byte >= 192 && after.name_byte < 251) {
                uint64_t overhead = after.charged_actions - after.name_byte;
                if (common_overhead == UINT64_MAX)
                    common_overhead = overhead;
                else
                    CHECK(overhead == common_overhead);
                observed_long_prefix = true;
                break;
            }
        }
        CHECK(observed_long_prefix && operation_id);

        wl_evaluation_control_request_cancel(control);
        CHECK(wl_session_step(base) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!wl_evaluation_control_active_attempt_id(control)
            && !session_find_rel(session, target_name));
        wl_evaluation_control_report_t stopped;
        CHECK(!wl_evaluation_control_report(control, &stopped)
            && stopped.attempt_id == operation_id
            && stopped.result == WL_EVALUATION_CONTROL_CANCELLED);

        wl_columnar_session_test_preflight_quantum = 64;
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_configure(control, UINT64_C(1000000)));
        int rc = wl_session_step(base);
        uint64_t fresh_id = wl_evaluation_control_active_attempt_id(control);
        CHECK(rc == EAGAIN && fresh_id > operation_id);
        bool done = false;
        for (unsigned turn = 0; turn < 10000; ++turn) {
            rc = wl_session_step(base);
            if (rc == EAGAIN) {
                CHECK(wl_evaluation_control_active_attempt_id(control)
                    == fresh_id);
                continue;
            }
            CHECK(!rc);
            done = true;
            break;
        }
        CHECK(done && !wl_evaluation_control_active_attempt_id(control));
        const col_rel_t *output = session_find_rel(session, target_name);
        CHECK(output && output->nrows == 1 && output->ncols == 1
            && output->columns[0][0] == 7);

        CHECK(!wl_session_try_destroy(&base) && !base);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
    }
    wl_columnar_session_test_preflight_quantum = 64;
}

static void
test_controlled_step_preflight_work_limit_boundary(void)
{
    const char *source =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000), &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *base = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &base));
    const int64_t row[] = {7};
    CHECK(!wl_session_insert(base, "input", row, 1, 1));
    wl_col_session_t *session = (wl_col_session_t *)base;
    wl_columnar_session_test_preflight_quantum = 1;

    uint64_t needed = 0;
    for (unsigned turn = 0; turn < 1000; ++turn) {
        int rc = wl_session_step(base);
        CHECK(rc == EAGAIN);
        wl_columnar_session_test_preflight_state_t state;
        CHECK(!wl_columnar_session_test_preflight_snapshot(base, &state));
        if (state.ready) {
            needed = state.charged_actions;
            break;
        }
    }
    CHECK(needed > 1);
    wl_evaluation_control_request_cancel(control);
    CHECK(wl_session_step(base) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!session_find_rel(session, "output"));

    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, needed - 1));
    int rc = EAGAIN;
    for (unsigned turn = 0; turn < 1000 && rc == EAGAIN; ++turn)
        rc = wl_session_step(base);
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && report.consumed == needed - 1);
    CHECK(!session_find_rel(session, "output"));

    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, needed));
    rc = EAGAIN;
    bool ready = false;
    for (unsigned turn = 0; turn < 1000 && rc == EAGAIN; ++turn) {
        rc = wl_session_step(base);
        if (rc == EAGAIN) {
            wl_columnar_session_test_preflight_state_t state;
            CHECK(!wl_columnar_session_test_preflight_snapshot(base, &state));
            if (state.ready) {
                ready = true;
                CHECK(state.charged_actions == needed);
                break;
            }
        }
    }
    CHECK(ready && rc == EAGAIN && !session_find_rel(session, "output"));
    wl_evaluation_control_request_cancel(control);
    CHECK(wl_session_step(base) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == WL_EVALUATION_CONTROL_CANCELLED
        && report.consumed == needed);
    CHECK(!session_find_rel(session, "output"));

    wl_columnar_session_test_preflight_quantum = 64;
    CHECK(!wl_session_try_destroy(&base) && !base);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_step_scheduler_empty_strata(void)
{
    enum { EMPTY_STRATA = 70 };
    const char *source =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count > 0);
    uint32_t total_strata = EMPTY_STRATA + plan->stratum_count;
    wl_plan_stratum_t *strata = calloc(total_strata, sizeof(*strata));
    CHECK(strata);
    memcpy(strata + EMPTY_STRATA, plan->strata,
        plan->stratum_count * sizeof(*strata));
    for (uint32_t i = EMPTY_STRATA; i < total_strata; ++i)
        strata[i].stratum_id += EMPTY_STRATA;
    wl_plan_t expanded = *plan;
    expanded.strata = strata;
    expanded.stratum_count = total_strata;

    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000), &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *base = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), &expanded, 1,
        &options, &base));
    const int64_t row[] = {7};
    CHECK(!wl_session_insert(base, "input", row, 1, 1));
    wl_col_session_t *session = (wl_col_session_t *)base;
    wl_columnar_session_test_preflight_quantum = 64;

    uint64_t operation_id = 0;
    uint32_t previous_eval_stratum = 0;
    bool previous_ready = false;
    bool scheduler_yielded = false;
    int rc = EAGAIN;
    for (unsigned turn = 0; turn < 1000 && rc == EAGAIN; ++turn) {
        rc = wl_session_step(base);
        wl_columnar_session_test_preflight_state_t state;
        CHECK(!wl_columnar_session_test_preflight_snapshot(base, &state));
        if (!operation_id) {
            operation_id = wl_evaluation_control_active_attempt_id(control);
            CHECK(operation_id);
        }
        CHECK(state.call_actions <= 64);
        CHECK(state.eval_stratum <= total_strata);
        CHECK(!session_find_rel(session, "output")
            || state.eval_stratum == total_strata);
        if (previous_ready && state.ready
            && state.eval_stratum > previous_eval_stratum) {
            CHECK(state.eval_stratum - previous_eval_stratum <= 64);
        }
        if (state.ready && state.eval_stratum > 0
            && state.eval_stratum < EMPTY_STRATA) {
            CHECK(state.call_actions == 64);
            scheduler_yielded = true;
        }
        previous_eval_stratum = state.eval_stratum;
        previous_ready = state.ready;
        if (rc == EAGAIN) {
            CHECK(wl_evaluation_control_active_attempt_id(control)
                == operation_id);
            CHECK(!session_find_rel(session, "output"));
        }
    }
    CHECK(!rc && scheduler_yielded);
    const col_rel_t *output = session_find_rel(session, "output");
    CHECK(output && output->nrows == 1 && output->columns[0][0] == 7);

    CHECK(!wl_session_try_destroy(&base) && !base);
    wl_evaluation_control_release(control);
    free(strata);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_batch_join_filter_map_step(void)
{
    char a_name[101], b_name[101], source[1024];
    memset(a_name, 'a', sizeof(a_name) - 1); a_name[100] = '\0';
    memset(b_name, 'b', sizeof(b_name) - 1); b_name[100] = '\0';
    int written = snprintf(source, sizeof(source),
            ".decl %s(k:int64,x:int64)\n"
            ".decl %s(k:int64,y:int64)\n"
            ".decl output(k:int64,x:int64,k2:int64,y:int64)\n"
            "output(k,x,k,y) :- %s(k,x), %s(k,y), x < y.\n",
            a_name, b_name, a_name, b_name);
    CHECK(written > 0 && (size_t)written < sizeof(source));
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *output = NULL;
    for (uint32_t si = 0; si < plan->stratum_count; ++si)
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ++ri)
            if (!strcmp(plan->strata[si].relations[ri].name, "output"))
                output = &plan->strata[si].relations[ri];
    CHECK(output && output->op_count == 5
        && output->ops[0].op == WL_PLAN_OP_VARIABLE
        && output->ops[1].op == WL_PLAN_OP_JOIN
        && output->ops[2].op == WL_PLAN_OP_FILTER
        && output->ops[3].op == WL_PLAN_OP_MAP
        && output->ops[4].op == WL_PLAN_OP_CONSOLIDATE);

    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(1, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    columnar->join_batch_bytes = 224;
    const int64_t left[] = {1, 1};
    const int64_t right[] = {1, 2};
    CHECK(!wl_session_insert(session, a_name, left, 1, 2));
    CHECK(!wl_session_insert(session, b_name, right, 1, 2));

    int rc = wl_session_step(session);
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
    wl_evaluation_control_report_t stopped_report;
    CHECK(!wl_evaluation_control_report(control, &stopped_report)
        && stopped_report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && stopped_report.attempt_id);
    CHECK(!wl_evaluation_control_active_attempt_id(control)
        && !columnar->cleanup_active
        && !session_find_rel(columnar, "output")
        && !columnar->controlled_materialized_batch_published_count);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, UINT64_C(1000000)));

    rc = wl_session_step(session);
    CHECK(rc == EAGAIN);
    uint64_t operation_id = wl_evaluation_control_active_attempt_id(control);
    CHECK(operation_id && !columnar->cleanup_active);
    CHECK(wl_session_snapshot(session, NULL, NULL) == EBUSY);
    wl_evaluation_control_request_cancel(control);
    CHECK(wl_session_step(session) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_evaluation_control_active_attempt_id(control)
        && !columnar->cleanup_active
        && !session_find_rel(columnar, "output")
        && !columnar->controlled_materialized_batch_published_count);
    uint64_t cancelled_id = operation_id;
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, UINT64_C(1000000)));

    rc = wl_session_step(session);
    CHECK(rc == EAGAIN);
    operation_id = wl_evaluation_control_active_attempt_id(control);
    CHECK(operation_id > cancelled_id && !columnar->cleanup_active);
    bool finished = false;
    for (unsigned turn = 0; turn < 10000; ++turn) {
        rc = wl_session_step(session);
        if (rc == EAGAIN) {
            CHECK(wl_evaluation_control_active_attempt_id(control)
                == operation_id);
            continue;
        }
        CHECK(!rc);
        finished = true;
        break;
    }
    CHECK(finished && !wl_evaluation_control_active_attempt_id(control));
    const col_rel_t *result = session_find_rel(columnar, "output");
    CHECK(result && result->nrows == 1 && result->ncols == 4);
    CHECK(result->columns[0][0] == 1 && result->columns[1][0] == 1
        && result->columns[2][0] == 1 && result->columns[3][0] == 2);
    CHECK(columnar->controlled_materialized_batch_published_count == 1);
    CHECK(!wl_session_try_destroy(&session) && !session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

typedef struct {
    bool present;
    uint32_t rows;
} controlled_batch_delta_t;

static void
controlled_batch_add_delta(wl_col_session_t *session, const char *name,
    const col_rel_t *schema, const int64_t *rows, uint32_t row_count)
{
    col_rel_t *delta = col_rel_new_like(name, schema);
    CHECK(delta);
    delta->nrows = 0;
    delta->base_nrows = 0;
    delta->sorted_nrows = 0;
    delta->run_count = 0;
    bool denied = false;
    CHECK(!col_rel_append_rows_atomic(delta, rows, row_count, schema->ncols,
        &denied));
    CHECK(!session_add_rel(session, delta));
}

static int
controlled_batch_run_to_terminal(wl_session_t *session,
    wl_evaluation_control_t *control)
{
    int rc = wl_session_step(session);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    for (unsigned turn = 0; rc == EAGAIN && id && turn < 10000; ++turn) {
        rc = wl_session_step(session);
        if (rc == EAGAIN)
            CHECK(wl_evaluation_control_active_attempt_id(control) == id);
    }
    return rc;
}

static uint32_t controlled_batch_join_scan_checkpoints;

static void
controlled_batch_join_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    (void)progress;
    /* Ordinary JOIN phase 17 fires after a candidate pair is selected. */
    if (phase == 17) {
        ++controlled_batch_join_scan_checkpoints;
        wl_evaluation_control_request_cancel(control);
    }
}

static void
test_controlled_batch_delta_selectors(void)
{
    static const char source[] =
        ".decl a(k:int64,x:int64)\n"
        ".decl b(k:int64,y:int64)\n"
        ".decl output(k:int64,x:int64,k2:int64,y:int64)\n"
        "output(k,x,k,y) :- a(k,x), b(k,y), x < y.\n";
    const int64_t left_rows[] = {1, 1, 1, 2};
    const int64_t right_rows[] = {1, 3, 1, 4};
    const int64_t left_delta_equal[] = {7, 70, 8, 80};
    const int64_t left_delta_larger[] = {7, 70, 8, 80, 9, 90};
    const int64_t left_delta_smaller[] = {7, 70};
    const int64_t right_delta_equal[] = {7, 70, 8, 80};
    const int64_t right_delta_larger[] = {7, 70, 8, 80, 9, 90};
    const int64_t right_delta_smaller[] = {7, 70};
    struct selector_case {
        controlled_batch_delta_t left, right;
        wl_delta_mode_t left_mode, right_mode;
        bool reject;
    } cases[] = {
        {{false, 0}, {false, 0}, WL_DELTA_AUTO, WL_DELTA_AUTO, false},
        {{true, 0}, {true, 0}, WL_DELTA_AUTO, WL_DELTA_AUTO, false},
        {{true, 2}, {false, 0}, WL_DELTA_AUTO, WL_DELTA_AUTO, false},
        {{true, 3}, {false, 0}, WL_DELTA_AUTO, WL_DELTA_AUTO, false},
        {{false, 0}, {true, 2}, WL_DELTA_AUTO, WL_DELTA_AUTO, false},
        {{false, 0}, {true, 3}, WL_DELTA_AUTO, WL_DELTA_AUTO, false},
        {{true, 1}, {false, 0}, WL_DELTA_AUTO, WL_DELTA_AUTO, true},
        {{false, 0}, {true, 1}, WL_DELTA_AUTO, WL_DELTA_AUTO, true},
        {{true, 1}, {true, 1}, WL_DELTA_FORCE_FULL,
         WL_DELTA_FORCE_FULL, false},
        {{false, 0}, {false, 0}, WL_DELTA_FORCE_DELTA,
         WL_DELTA_AUTO, true},
        {{false, 0}, {false, 0}, WL_DELTA_FORCE_EMPTY,
         WL_DELTA_AUTO, true},
        {{false, 0}, {false, 0}, WL_DELTA_FORCE_EMPTY_AFTER_SEED,
         WL_DELTA_AUTO, true},
        {{false, 0}, {false, 0}, (wl_delta_mode_t)99,
         WL_DELTA_AUTO, true},
        {{false, 0}, {false, 0}, WL_DELTA_AUTO,
         WL_DELTA_AUTO, true},
        {{false, 0}, {false, 0}, WL_DELTA_AUTO,
         WL_DELTA_AUTO, true},
        {{false, 0}, {false, 0}, WL_DELTA_AUTO,
         WL_DELTA_AUTO, false},
        {{true, 2}, {true, 3}, WL_DELTA_AUTO, WL_DELTA_AUTO, false},
        {{false, 0}, {false, 0}, WL_DELTA_AUTO,
         WL_DELTA_FORCE_DELTA, true},
        {{false, 0}, {false, 0}, WL_DELTA_AUTO,
         WL_DELTA_FORCE_EMPTY, true},
        {{false, 0}, {false, 0}, WL_DELTA_AUTO,
         WL_DELTA_FORCE_EMPTY_AFTER_SEED, true},
        {{false, 0}, {false, 0}, WL_DELTA_AUTO,
         (wl_delta_mode_t)99, true}
    };

    for (unsigned ci = 0; ci < sizeof(cases) / sizeof(cases[0]); ++ci) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        const wl_plan_relation_t *output_const = NULL;
        for (uint32_t si = 0; si < plan->stratum_count; ++si)
            for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ++ri)
                if (!strcmp(plan->strata[si].relations[ri].name, "output"))
                    output_const = &plan->strata[si].relations[ri];
        wl_plan_relation_t *output = (wl_plan_relation_t *)output_const;
        CHECK(output && output->op_count == 5);
        wl_plan_op_t *mutable_ops = (wl_plan_op_t *)output->ops;
        mutable_ops[0].delta_mode = cases[ci].left_mode;
        mutable_ops[1].delta_mode = cases[ci].right_mode;
        if (ci == 14) output->op_count = 4;

        wl_evaluation_control_t *control = NULL;
        uint64_t limit = UINT64_C(1000000);
        CHECK(!wl_evaluation_control_create(limit, &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *base = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &base));
        wl_col_session_t *session = (wl_col_session_t *)base;
        session->join_batch_bytes = 224;
        if (ci == 13) session->join_batch_strict = true;
        const int64_t *left_delta_rows = cases[ci].left.rows == 1
            ? left_delta_smaller : cases[ci].left.rows == 2
            ? left_delta_equal : left_delta_larger;
        const int64_t *right_delta_rows = cases[ci].right.rows == 1
            ? right_delta_smaller : cases[ci].right.rows == 2
            ? right_delta_equal : right_delta_larger;
        const int64_t no_output_left[] = {1, 4, 1, 5};
        const int64_t no_output_right[] = {1, 3, 1, 3};
        const int64_t *selected_left = ci == 15
            ? no_output_left : left_rows;
        const int64_t *selected_right = ci == 15
            ? no_output_right : right_rows;
        CHECK(!wl_session_insert(base, "a", selected_left, 2, 2));
        CHECK(!wl_session_insert(base, "b", selected_right, 2, 2));
        const col_rel_t *left_full = session_find_rel(session, "a");
        const col_rel_t *right_full = session_find_rel(session, "b");
        CHECK(left_full && right_full);
        if (cases[ci].left.present)
            controlled_batch_add_delta(session, "$d$a", left_full,
                left_delta_rows, cases[ci].left.rows);
        if (cases[ci].right.present)
            controlled_batch_add_delta(session, "$d$b", right_full,
                right_delta_rows, cases[ci].right.rows);

        if (ci == 15) {
            controlled_batch_join_scan_checkpoints = 0;
            wl_columnar_join_test_attempt_checkpoint =
                controlled_batch_join_checkpoint;
        }
        int rc = ci == 15
            ? controlled_batch_run_to_terminal(base, control)
            : wl_session_step(base);
        if (ci == 15)
            wl_columnar_join_test_attempt_checkpoint = NULL;
        if (cases[ci].reject) {
            CHECK(rc == ENOTSUP);
            CHECK(!wl_evaluation_control_active_attempt_id(control)
                && !session->cleanup_active
                && !session_find_rel(session, "output")
                && !session->controlled_materialized_batch_published_count);
        } else {
            if (ci == 15) {
                CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED
                    && controlled_batch_join_scan_checkpoints > 0);
                CHECK(!session_find_rel(session, "output")
                    && !session->controlled_materialized_batch_published_count
                    && !wl_evaluation_control_active_attempt_id(control));
                wl_evaluation_control_report_t cancelled_report;
                CHECK(!wl_evaluation_control_report(control,
                    &cancelled_report)
                    && cancelled_report.result
                    == WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(!wl_evaluation_control_reset(control));
                CHECK(!wl_evaluation_control_configure(control,
                    UINT64_C(1000000)));
                rc = wl_session_step(base);
            }
            uint64_t id = wl_evaluation_control_active_attempt_id(control);
            CHECK((rc == 0 || (rc == EAGAIN && id))
                && !session->cleanup_active);
            bool done = rc == 0;
            for (unsigned turn = 0; turn < 10000; ++turn) {
                rc = wl_session_step(base);
                if (rc == EAGAIN) {
                    CHECK(wl_evaluation_control_active_attempt_id(control)
                        == id);
                    continue;
                }
                CHECK(!rc);
                done = true;
                break;
            }
            CHECK(done && !wl_evaluation_control_active_attempt_id(control));
            const col_rel_t *result = session_find_rel(session, "output");
            /* Full inputs produce the four key-1 combinations. Equal/larger
             * delta contents are
             * deliberately disjoint so accidental delta selection differs. */
            CHECK(result && result->nrows == (ci == 15 ? 0u : 4u)
                && result->ncols == 4);
            if (ci != 15) CHECK(result->columns[1][0] == 1
                    && result->columns[3][0] == 3
                    && result->columns[1][1] == 1 && result->columns[3][1] == 4
                    && result->columns[1][2] == 2 && result->columns[3][2] == 3
                    && result->columns[1][3] == 2 &&
                    result->columns[3][3] == 4);
            CHECK(session->controlled_materialized_batch_published_count
                == 1);
        }
        CHECK(!wl_session_try_destroy(&base) && !base);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

static void
test_controlled_batch_child_domain_rejection(void)
{
    static const char source[] =
        ".decl a(k:int64,x:int64)\n"
        ".decl b(k:int64,y:int64)\n"
        ".decl output(k:int64,x:int64,k2:int64,y:int64)\n"
        "output(k,x,k,y) :- a(k,x), b(k,y), x < y.\n";
    const int64_t matching_left[] = {1, 1, 1, 2};
    const int64_t matching_right[] = {1, 3, 1, 4};
    const int64_t empty_left[] = {1, 1};
    const int64_t empty_right[] = {2, 3};

    for (unsigned child = 0; child < 2; ++child)
        for (unsigned empty = 0; empty < 2; ++empty) {
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(source, &error);
            CHECK(program);
            wl_plan_t *plan = NULL;
            CHECK(!wl_plan_from_program(program, &plan));
            wl_plan_relation_t *output = NULL;
            for (uint32_t si = 0; si < plan->stratum_count; ++si)
                for (uint32_t ri = 0;
                    ri < plan->strata[si].relation_count; ++ri)
                    if (!strcmp(plan->strata[si].relations[ri].name,
                        "output"))
                        output = (wl_plan_relation_t *)&plan->strata[si].
                            relations[ri];
            CHECK(output && output->op_count == 5);
            wl_plan_op_t *ops = (wl_plan_op_t *)output->ops;
            uint8_t *byte = child == 0 ? ops[2].filter_expr.data
                : ops[3].map_exprs && ops[3].map_expr_count
                ? ops[3].map_exprs[0].data : NULL;
            CHECK(byte && (child == 0 ? ops[2].filter_expr.size
                : ops[3].map_exprs[0].size));
            uint8_t original = byte[0];
            byte[0] = UINT8_C(0xff);

            wl_evaluation_control_t *control = NULL;
            CHECK(!wl_evaluation_control_create(UINT64_C(1000000),
                &control));
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = control;
            wl_session_t *base = NULL;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(),
                plan, 1, &options, &base));
            wl_col_session_t *session = (wl_col_session_t *)base;
            session->join_batch_bytes = 224;
            CHECK(!wl_session_insert(base, "a", empty
                ? empty_left : matching_left, empty ? 1 : 2, 2));
            CHECK(!wl_session_insert(base, "b", empty
                ? empty_right : matching_right, empty ? 1 : 2, 2));

            CHECK(controlled_batch_run_to_terminal(base, control)
                == ENOTSUP);
            CHECK(!wl_evaluation_control_active_attempt_id(control)
                && !session->cleanup_active && !session->cleanup_pending
                && !session->cleanup_active_count
                && !session->cleanup_reserved_bytes
                && !session_find_rel(session, "output")
                && !session->controlled_materialized_batch_published_count);
            CHECK(session_find_rel(session, "a")->nrows == (empty ? 1u : 2u)
                && session_find_rel(session, "b")->nrows == (empty ? 1u : 2u));

            byte[0] = original;
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control,
                UINT64_C(1000000)));
            CHECK(!controlled_batch_run_to_terminal(base, control));
            const col_rel_t *result = session_find_rel(session, "output");
            CHECK(result && result->ncols == 4
                && result->nrows == (empty ? 0u : 4u)
                && session->controlled_materialized_batch_published_count
                == 1);
            CHECK(!wl_session_try_destroy(&base) && !base);
            wl_evaluation_control_release(control);
            wl_plan_free(plan);
            wirelog_program_free(program);
        }
}

static void
test_controlled_session_step_prefix_resume(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl left(x:int64)\n"
        ".decl right(x:int64)\n"
        "left(x) :- input(x).\n"
        "right(x) :- input(x).\n";
    const int64_t rows[] = {9, 2, 6};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(2000000, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_insert(session, "input", rows, 3, 1));

    wl_columnar_session_test_preflight_quantum = 7;
    wl_columnar_session_test_exhaust_after_relation = 1;
    int rc = wl_session_step(session);
    uint64_t operation_id = wl_evaluation_control_active_attempt_id(control);
    CHECK(rc == EAGAIN && operation_id);
    unsigned yields = 0;
    for (unsigned turn = 0; turn < 10000; ++turn) {
        rc = wl_session_step(session);
        if (rc == EAGAIN) {
            CHECK(wl_evaluation_control_active_attempt_id(control)
                == operation_id);
            yields++;
            continue;
        }
        break;
    }
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && yields > 1);
    wl_columnar_session_test_exhaust_after_relation = 0;
    wl_columnar_session_test_preflight_state_t stopped_state;
    CHECK(!wl_columnar_session_test_preflight_snapshot(session,
        &stopped_state));
    CHECK(stopped_state.cursor_advance_pending);
    wl_columnar_session_test_preflight_quantum = 64;
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    const col_rel_t *left = session_find_rel((wl_col_session_t *)session,
            "left");
    const col_rel_t *right = session_find_rel((wl_col_session_t *)session,
            "right");
    CHECK((left != NULL) != (right != NULL));
    const col_rel_t *prefix = left ? left : right;
    CHECK(prefix && prefix->nrows == 3);
    CHECK(prefix->columns[0][0] == 2 && prefix->columns[0][1] == 6
        && prefix->columns[0][2] == 9);

    CHECK(wl_session_insert(session, "input", rows, 1, 1) == EBUSY);
    CHECK(wl_session_snapshot(session, NULL, NULL) == EBUSY);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!wl_session_step(session));

    left = session_find_rel((wl_col_session_t *)session, "left");
    right = session_find_rel((wl_col_session_t *)session, "right");
    CHECK(left && right && left->nrows == 3 && right->nrows == 3);
    CHECK(prefix->nrows == 3 && prefix->columns[0][0] == 2
        && prefix->columns[0][1] == 6 && prefix->columns[0][2] == 9);
    CHECK(left->columns[0][0] == 2 && left->columns[0][1] == 6
        && left->columns[0][2] == 9);
    CHECK(right->columns[0][0] == 2 && right->columns[0][1] == 6
        && right->columns[0][2] == 9);
    CHECK(wl_session_insert(session, "input", rows, 1, 1) == EBUSY);

    CHECK(!wl_session_try_destroy(&session) && !session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_controlled_session_step_destroy_refusal(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    const int64_t input_rows[] = {3, 7, 11};
    CHECK(!wl_session_insert(session, "input", input_rows, 3, 1));

    wl_columnar_session_test_fail_framed_funding_before_stop = 1;
    CHECK(wl_session_step(session) == EAGAIN);
    uint64_t attempt_id = wl_evaluation_control_active_attempt_id(control);
    CHECK(attempt_id != 0);
    wl_session_t *same_session = session;
    CHECK(wl_session_try_destroy(&session) == EBUSY
        && session == same_session
        && wl_evaluation_control_active_attempt_id(control) == attempt_id);
    CHECK(wl_session_step(session) == 0);
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    CHECK(!wl_session_try_destroy(&session) && !session);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.attempt_id == attempt_id && report.result == 0);

    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

#if !defined(_WIN32)
static void
test_controlled_session_step_void_destroy_precondition(void)
{
    pid_t child = fork();
    CHECK(child >= 0);
    if (child == 0) {
        static const char source[] =
            ".decl input(x:int64)\n"
            ".decl output(x:int64)\n"
            "output(x) :- input(x).\n";
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        if (!program) _exit(3);
        wl_plan_t *plan = NULL;
        if (wl_plan_from_program(program, &plan) != 0) _exit(4);
        wl_evaluation_control_t *control = NULL;
        if (wl_evaluation_control_create(100000, &control) != 0) _exit(5);
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *session = NULL;
        if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session) != 0)
            _exit(6);
        wl_columnar_session_test_fail_framed_funding_before_stop = 1;
        if (wl_session_step(session) != EAGAIN) _exit(7);
        wl_session_destroy(session);
        _exit(8);
    }
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}
#endif

static void
test_controlled_session_snapshot_publication(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    const int64_t rows[] = {7, 3, 11};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));

    wl_session_t *reference = NULL;
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
    CHECK(!wl_session_insert(reference, "input", rows, 3, 1));
    CHECK(!wl_session_step(reference));
    snapshot_capture_t expected = {0};
    CHECK(!wl_session_snapshot(reference, capture_snapshot_tuple, &expected));

    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_insert(session, "input", rows, 3, 1));
    CHECK(!wl_session_step(session));

    wl_evaluation_control_report_t step_report, report;
    CHECK(!wl_evaluation_control_report(control, &step_report));
    CHECK(wl_session_snapshot(session, NULL, NULL) == EINVAL);
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.attempt_id == step_report.attempt_id
        && report.result == step_report.result
        && !wl_evaluation_control_active_attempt_id(control));

    snapshot_capture_t actual = {0};
    wl_evaluation_control_request_cancel(control);
    CHECK(wl_session_snapshot(session, capture_snapshot_tuple, &actual)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual.count == 0
        && !wl_evaluation_control_active_attempt_id(control));
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.result == WL_EVALUATION_CONTROL_CANCELLED
        && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
        && report.consumed == 0);
    CHECK(!wl_evaluation_control_reset(control));

    actual = (snapshot_capture_t){.cancel_control = control,
                                  .cancel_on_first = true};
    CHECK(!wl_session_snapshot(session, capture_snapshot_tuple, &actual));
    check_snapshot_equal(&actual, &expected);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == 0
        && report.stop_reason == WL_EVALUATION_CONTROL_NONE
        && report.consumed > 0);

    actual = (snapshot_capture_t){0};
    CHECK(wl_session_snapshot(session, capture_snapshot_tuple, &actual)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual.count == 0);
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_evaluation_control_reset(control));

    actual = (snapshot_capture_t){0};
    CHECK(!wl_session_snapshot(session, capture_snapshot_tuple, &actual));
    check_snapshot_equal(&actual, &expected);
    CHECK(wl_session_insert(session, "input", rows, 1, 1) == EBUSY);

    CHECK(!wl_session_try_destroy(&session) && !session);
    wl_evaluation_control_release(control);
    wl_session_destroy(reference);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

extern void (*wl_columnar_session_test_snapshot_checkpoint)(
    wl_evaluation_control_t *, unsigned, size_t);
extern int wl_columnar_session_test_snapshot_allocation;
static unsigned snapshot_stop_phase;
static unsigned snapshot_checkpoint_hits;

static void
snapshot_prepare_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, size_t index)
{
    if (phase == snapshot_stop_phase && (phase != 2 || index == 2)
        && (phase != 4 || index == 16)) {
        ++snapshot_checkpoint_hits;
        wl_evaluation_control_request_cancel(control);
    }
}

typedef struct {
    size_t rows;
    uint64_t hash;
} snapshot_wide_capture_t;

static void
snapshot_wide_capture(const char *name, const int64_t *row,
    uint32_t width, void *data)
{
    snapshot_wide_capture_t *capture = data;
    CHECK(name && row && width == 4);
    for (const unsigned char *p = (const unsigned char *)name; *p; ++p)
        capture->hash = capture->hash * 31 + *p;
    for (uint32_t i = 0; i < width; ++i)
        capture->hash = capture->hash * 31 + (uint64_t)row[i];
    ++capture->rows;
}

static void
test_controlled_snapshot_preparation(void)
{
    static const char source[] =
        ".decl input(a:int64,b:int64,c:int64,d:int64)\n"
        ".decl first_snapshot_result_with_a_long_common_prefix(a:int64,b:int64,c:int64,d:int64)\n"
        ".decl second_snapshot_result_with_a_long_common_prefix(a:int64,b:int64,c:int64,d:int64)\n"
        "first_snapshot_result_with_a_long_common_prefix(a,b,c,d) :- input(a,b,c,d).\n"
        "second_snapshot_result_with_a_long_common_prefix(a,b,c,d) :- input(a,b,c,d).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    int64_t rows[128 * 4];
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i)
        rows[i] = (int64_t)i;
    wl_session_t *reference = NULL;
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
    CHECK(!wl_session_insert(reference, "input", rows, 128, 4));
    CHECK(!wl_session_step(reference));
    snapshot_wide_capture_t expected = {0};
    CHECK(!wl_session_snapshot(reference, snapshot_wide_capture, &expected));
    CHECK(expected.rows == 256);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(64) << 20,
            .usable_bytes = UINT64_C(64) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    options.memory_governor = governor;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_insert(session, "input", rows, 128, 4));
    CHECK(!controlled_recursive_run(session, control));
    wl_col_session_t *col = (wl_col_session_t *)session;
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(governor);
    uint64_t reserved = wl_columnar_memory_reserved(g);
    col_rel_t **registry = col->rels;
    uint32_t count = col->nrels;
    for (unsigned mode = 0; mode < 10; ++mode) {
        CHECK(!wl_evaluation_control_reset(control));
        /* Bootstrap256 + stratum1 + first plan entry1. */
        CHECK(!wl_evaluation_control_configure(control,
            mode < 2 ? 257 + mode : 0));
        snapshot_stop_phase = mode < 2 ? 1 : mode == 2 ? 2 : mode ==
            3 ? 3 : mode == 8 ? 4 : mode == 9 ? 5 : 0;
        snapshot_checkpoint_hits = 0;
        wl_columnar_session_test_snapshot_checkpoint =
            snapshot_prepare_checkpoint;
        wl_columnar_session_test_snapshot_allocation =
            mode == 4 ? 0 : mode == 5 ? 1 : -1;
        wl_columnar_source_access_writer_t writer = {0};
        if (mode == 6) {
            col_rel_t *first = session_find_rel(col,
                    plan->strata[0].relations[0].name);
            CHECK(first && !col_rel_source_writer_acquire(first, &writer));
        }
        wl_columnar_memory_reservation_t pressure;
        wl_columnar_memory_reservation_init(&pressure);
        if (mode == 7)
            CHECK(wl_columnar_memory_reserve_checked(g,
                g->budget_bytes - reserved, &pressure)
                == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        snapshot_wide_capture_t actual = {0};
        int rc = wl_session_snapshot(session, snapshot_wide_capture, &actual);
        CHECK(rc == (mode == 0 ? WL_EVALUATION_CONTROL_WORK_LIMIT
            : mode < 4 ? WL_EVALUATION_CONTROL_CANCELLED
            : mode < 6 ? ENOMEM : mode == 6 ? EBUSY : mode == 7 ? ENOSPC
            : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(!actual.rows && !actual.hash && col->rels == registry
            && col->nrels == count && !col->cleanup_pending
            && !wl_evaluation_control_active_attempt_id(control));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(control, &report)
            && report.result == rc && !report.cleanup_status);
        if (mode < 2)
            CHECK(report.consumed == 257 + mode
                && snapshot_checkpoint_hits == mode);
        if (mode == 2 || mode == 3 || mode >= 8)
            CHECK(snapshot_checkpoint_hits == 1);
        if (mode ==
            6) CHECK(!wl_columnar_source_access_writer_release(&writer));
        if (mode == 7) CHECK(wl_columnar_memory_rollback(&pressure));
        CHECK(wl_columnar_memory_reserved(g) == reserved);
        wl_columnar_session_test_snapshot_checkpoint = NULL;
        wl_columnar_session_test_snapshot_allocation = -1;
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_configure(control, 0));
        CHECK(!wl_session_snapshot(session, snapshot_wide_capture, &actual));
        CHECK(actual.rows == expected.rows && actual.hash == expected.hash
            && wl_columnar_memory_reserved(g) == reserved);
        for (uint32_t i = 0; i < col->nrels; ++i) {
            if (!col->rels[i]) continue;
            wl_columnar_source_access_writer_t check = {0};
            CHECK(!col_rel_source_writer_acquire(col->rels[i], &check));
            CHECK(!wl_columnar_source_access_writer_release(&check));
        }
    }
    /* Internal immutable projection fixture: compare duplicate entries,
     * missing names and an empty stratum with the raw cached emitter. The
     * projection is restored before destroying the original owned plan. */
    const wl_plan_stratum_t *saved_strata = plan->strata;
    uint32_t saved_count = plan->stratum_count;
    wl_plan_relation_t projected_relations[4];
    projected_relations[0] = saved_strata[0].relations[0];
    projected_relations[1] = projected_relations[0];
    projected_relations[2] = projected_relations[0];
    projected_relations[2].name = "absent_snapshot_relation";
    projected_relations[3] = projected_relations[0];
    wl_plan_stratum_t projection[2] = {0};
    projection[0].relation_count = 4;
    projection[0].relations = projected_relations;
    plan->strata = projection;
    plan->stratum_count = 2;
    snapshot_wide_capture_t projected_expected = {0}, projected_actual = {0};
    CHECK(!wl_session_snapshot(reference, snapshot_wide_capture,
        &projected_expected));
    CHECK(!wl_session_snapshot(session, snapshot_wide_capture,
        &projected_actual));
    CHECK(projected_expected.rows == 384
        && projected_actual.rows == projected_expected.rows
        && projected_actual.hash == projected_expected.hash);
    /* Exercise the staging allocator's optional-governor branch separately
     * from the still-owned relation storage credits. */
    col->memory_governor = NULL;
    projected_actual = (snapshot_wide_capture_t){0};
    CHECK(!wl_session_snapshot(session, snapshot_wide_capture,
        &projected_actual));
    col->memory_governor = governor;
    CHECK(projected_actual.rows == projected_expected.rows
        && projected_actual.hash == projected_expected.hash
        && wl_columnar_memory_reserved(g) == reserved);
    plan->stratum_count = 0;
    projected_actual = (snapshot_wide_capture_t){0};
    CHECK(!wl_session_snapshot(session, snapshot_wide_capture,
        &projected_actual) && !projected_actual.rows);
    plan->strata = saved_strata;
    plan->stratum_count = saved_count;
    CHECK(!wl_session_try_destroy(&session) && !session);
    CHECK(!wl_columnar_memory_reserved(g));
    wl_columnar_memory_governor_ref_release(governor);
    wl_evaluation_control_release(control);
    wl_session_destroy(reference);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

/* Host admission evidence for the serial coverage matrix. These injected
 * modes exercise refusal only; they do not claim differential/LFTJ execution. */
static void
test_controlled_session_admission_matrix(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    enum {
        ITERATION, RETRACTION, RIGHT_RETRACTION, DIFFERENTIAL,
        STRICT_BATCH, OTHER_BATCH_SHAPE, LFTJ, EXCHANGE, K_FUSION, CASES
    };
    for (unsigned mode = 0; mode < CASES; ++mode) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        CHECK(program);
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        CHECK(plan->stratum_count == 1
            && plan->strata[0].relation_count == 1);
        wl_plan_op_t *ops = (wl_plan_op_t *)plan->strata[0].relations[0].ops;
        CHECK(plan->strata[0].relations[0].op_count == 3
            && ops[0].op == WL_PLAN_OP_VARIABLE);
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *base = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &base));
        wl_col_session_t *session = (wl_col_session_t *)base;
        const int64_t rows[] = {7, 3, 11};
        CHECK(!wl_session_insert(base, "input", rows, 3, 1));
        col_rel_t *input = session_find_rel(session, "input");
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(input);
        uint32_t registered = session->nrels;
        snapshot_capture_t capture = {0};
        CHECK(wl_session_snapshot(base, capture_snapshot_tuple, &capture)
            == ENOTSUP);
        CHECK(!capture.count);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == ENOENT);

        switch (mode) {
        case ITERATION: session->current_iteration = 1; break;
        case RETRACTION: session->retraction_seeded = true; break;
        case RIGHT_RETRACTION: session->retraction_right_pass = true; break;
        case DIFFERENTIAL: session->diff_operators_active = true; break;
        case STRICT_BATCH:
            session->join_batch_bytes = 224;
            session->join_batch_strict = true;
            break;
        case OTHER_BATCH_SHAPE: session->join_batch_bytes = 224; break;
        case LFTJ: ops[0].op = WL_PLAN_OP_LFTJ; break;
        case EXCHANGE: ops[0].op = WL_PLAN_OP_EXCHANGE; break;
        case K_FUSION: ops[0].op = WL_PLAN_OP_K_FUSION; break;
        default: CHECK(false);
        }
        controlled_observer_framed_lookup_hits = 0;
        wl_columnar_eval_serial_test_framed_checkpoint =
            controlled_observer_count_rule_start;
        CHECK(controlled_recursive_run(base, control) == ENOTSUP);
        CHECK(!controlled_observer_framed_lookup_hits
            && !wl_evaluation_control_active_attempt_id(control)
            && !session_find_rel(session, "output")
            && session->nrels == registered && !session->delta_observer
            && !session->cleanup_active && !session->cleanup_pending
            && !session->cleanup_active_count
            && !session->cleanup_reserved_bytes);
        CHECK(wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(input)));
        CHECK(input->nrows == 3 && input->columns[0][0] == 7
            && input->columns[0][1] == 3 && input->columns[0][2] == 11);
        if (mode <= STRICT_BATCH) {
            /* Static exclusions do not begin a control attempt. */
            CHECK(wl_evaluation_control_report(control, &report) == ENOENT);
        } else {
            /* Shape/operator rejection occurs in charged preflight. */
            CHECK(!wl_evaluation_control_report(control, &report)
                && report.result == ENOTSUP && report.consumed > 0
                && !report.cleanup_status);
        }
        session->current_iteration = 0;
        session->retraction_seeded = false;
        session->retraction_right_pass = false;
        session->diff_operators_active = false;
        session->join_batch_bytes = 0;
        session->join_batch_strict = false;
        ops[0].op = WL_PLAN_OP_VARIABLE;
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!controlled_recursive_run(base, control));
        CHECK(controlled_observer_framed_lookup_hits == 1);
        wl_columnar_eval_serial_test_framed_checkpoint = NULL;
        CHECK(!wl_session_snapshot(base, capture_snapshot_tuple, &capture));
        CHECK(capture.count == 3);
        const col_rel_t *output = session_find_rel(session, "output");
        CHECK(output && output->nrows == 3 && output->columns[0][0] == 3
            && output->columns[0][1] == 7 && output->columns[0][2] == 11);
        CHECK(!wl_session_try_destroy(&base) && !base);
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        wirelog_program_free(program);
    }
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
    CHECK(!wl_session_try_destroy(&base) && !base);
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

static bool
entry_composed_progress_unchanged(
    const wl_columnar_eval_plan_numeric_attempt_t *a,
    const wl_columnar_eval_plan_numeric_attempt_t *b)
{
    return a->phase == b->phase && a->pending_action == b->pending_action
           && a->cursor == b->cursor && a->completed == b->completed
           && a->semantic_result == b->semantic_result
           && a->protocol_result == b->protocol_result
           && a->abort_requested == b->abort_requested
           && a->control == b->control && a->attempt_id == b->attempt_id
           && a->frame == b->frame && a->session == b->session
           && a->plan == b->plan && a->lftj.phase == b->lftj.phase
           && a->lftj.semantic_result == b->lftj.semantic_result
           && a->child_status.progress == b->child_status.progress
           && a->child_status.execution_result ==
           b->child_status.execution_result
           && a->child_status.cleanup_result == b->child_status.cleanup_result
           && a->child_status.semantic_result == b->child_status.semantic_result
           && a->child_status.committed == b->child_status.committed;
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
    /* First consume the outer receipt for the child step that produced the
     * child PENDING result. A second step now consumes that inner result and
     * creates the nested framed receipt we exercise below. */
    CHECK(!entry_turn_action(&nested,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE, 0));
    CHECK(entry_context(&nested)->phase ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
        && entry_context(&nested)->child.phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
    CHECK(!entry_turn_action(&nested,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE, 0));
    ctx = entry_context(&nested);
    CHECK(ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && ctx->pending_inner_consume
        && ctx->pending_funding == nested.previous.previous_funding
        && ctx->pending_funding_id == nested.previous.previous_funding_id);
    wl_columnar_eval_plan_numeric_attempt_t child_before = ctx->child;
    wl_evaluation_control_t *pending_grant = nested.previous.previous_funding;
    uint64_t pending_grant_id = nested.previous.previous_funding_id;
    wl_columnar_eval_serial_framed_host_reply_kind_t pending_kind =
        nested.previous.previous_kind;
    int pending_protocol = nested.previous.previous_protocol;

    /* A stale tuple cannot consume the outer PENDING receipt or latch abort. */
    grant_t *wrong_grant = entry_grant(&nested, 0);
    wl_columnar_eval_serial_framed_host_request_t wrong = nested.previous;
    wrong.funding = wrong_grant->control;
    wrong.funding_id = wrong_grant->id;
    wrong.operation = nested.operation;
    wrong.operation_id = nested.id;
    wrong.operation_identity = nested.operation_identity;
    wrong.action = WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT;
    wrong.previous_funding_id++;
    wrong.reply = &nested.reply;
    CHECK(entry_host(&nested, &wrong, wrong_grant) == EINVAL);
    CHECK(ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && !ctx->abort_requested
        && ctx->pending_funding == pending_grant
        && ctx->pending_funding_id == pending_grant_id
        && entry_composed_progress_unchanged(&child_before, &ctx->child)
        && nested.previous.previous_valid
        && nested.previous.previous_funding == pending_grant
        && nested.previous.previous_funding_id == pending_grant_id
        && nested.previous.previous_kind == pending_kind
        && nested.previous.previous_protocol == pending_protocol);

    /* Refusing the fresh outer grant also leaves both pending layers intact. */
    CHECK(entry_turn_action(&nested,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 383)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && !ctx->abort_requested
        && ctx->pending_funding == pending_grant
        && ctx->pending_funding_id == pending_grant_id
        && entry_composed_progress_unchanged(&child_before, &ctx->child)
        && nested.previous.previous_valid
        && nested.previous.previous_funding == pending_grant
        && nested.previous.previous_funding_id == pending_grant_id
        && nested.previous.previous_kind == pending_kind
        && nested.previous.previous_protocol == pending_protocol);

    /* The correct ABORT consumes only the framed receipt. It does not run
     * cleanup in the same request; the next request first latches child stop. */
    CHECK(!entry_turn_action(&nested,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(ctx->abort_requested
        && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
        && !ctx->pending_funding && !ctx->pending_funding_id
        && ctx->child.phase == child_before.phase
        && ctx->child.cursor == child_before.cursor
        && ctx->child.completed == child_before.completed);
    CHECK(!entry_turn_action(&nested,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(ctx->abort_requested && ctx->child.abort_requested);
    CHECK(ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD);
    CHECK(ctx->pending_funding == NULL);
    CHECK(ctx->pending_funding_id == 0);
    CHECK(ctx->child.phase == child_before.phase
        && ctx->child.cursor == child_before.cursor
        && ctx->child.completed == child_before.completed);
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
test_entry_generated_lftj_bounded_source_resolution(void)
{
    enum { FILLER_COUNT = 16, INPUT_COUNT = 3 };
    static const int64_t one[] = {7};
    static const char prefix[] =
        "shared_lftj_registry_name_prefix_for_bounded_resolution_";
    char names[INPUT_COUNT][96];
    char fillers[FILLER_COUNT][96];
    char source[8192] = {0};
    size_t used = 0;
    int n = 0;
    for (uint32_t i = 0; i < INPUT_COUNT; ++i) {
        n = snprintf(names[i], sizeof(names[i]), "%sinput_%u", prefix, i);
        CHECK(n > 0 && (size_t)n < sizeof(names[i]));
        n = snprintf(source + used, sizeof(source) - used,
                ".decl %s(k:int64)\n", names[i]);
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
    }
    n = snprintf(source + used, sizeof(source) - used,
            ".decl output(k:int64)\noutput(k) :- %s(k), %s(k), %s(k).\n",
            names[0], names[1], names[2]);
    CHECK(n > 0 && (size_t)n < sizeof(source) - used);
    used += (size_t)n;

    entry_input_t inputs[INPUT_COUNT + FILLER_COUNT];
    for (uint32_t i = 0; i < INPUT_COUNT; ++i)
        inputs[i] = (entry_input_t){names[i], one, 1, 1};
    for (uint32_t i = 0; i < FILLER_COUNT; ++i) {
        n = snprintf(fillers[i], sizeof(fillers[i]), "%sunused_%02u", prefix,
                i);
        CHECK(n > 0 && (size_t)n < sizeof(fillers[i]));
        n = snprintf(source + used, sizeof(source) - used,
                ".decl %s(k:int64)\n", fillers[i]);
        CHECK(n > 0 && (size_t)n < sizeof(source) - used);
        used += (size_t)n;
        inputs[INPUT_COUNT + i] = (entry_input_t){fillers[i], one, 1, 1};
    }

    entry_fixture_t f;
    entry_init_inputs(&f, source, inputs, INPUT_COUNT + FILLER_COUNT, 0);
    bool has_lftj = false;
    for (uint32_t i = 0; i < f.relation->op_count; ++i)
        has_lftj |= f.relation->ops[i].op == WL_PLAN_OP_LFTJ;
    CHECK(has_lftj);
    /* Resolution must leave the normal registry hash untouched while it
     * scans long shared prefixes in the captured relation array. */
    CHECK(f.session->rel_hash_head && f.session->rel_hash_next);
    void *hash_head = f.session->rel_hash_head;
    void *hash_next = f.session->rel_hash_next;

    bool partial_lookup = false;
    for (unsigned turns = 0; turns < 200000; ++turns) {
        wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
        if (ctx && ctx->child.lftj.identity == &ctx->child.lftj
            && ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_RESOLVE
            && ctx->child.lftj.inputs
            && (ctx->child.lftj.inputs[0].lookup_slot
            || ctx->child.lftj.inputs[0].lookup_name_byte)) {
            wl_columnar_lftj_attempt_t *attempt = &ctx->child.lftj;
            CHECK(attempt->inputs[0].lookup_slot == 0);
            CHECK(attempt->inputs[0].lookup_name_byte > 0);
            CHECK(attempt->input_count == 0);
            CHECK(f.session->rel_hash_head == hash_head
                && f.session->rel_hash_next == hash_next);
            for (uint32_t i = 0; i < INPUT_COUNT + FILLER_COUNT; ++i)
                CHECK(atomic_load_explicit(
                        &f.session->rels[i]->source_access.state,
                        memory_order_acquire) == 0);
            partial_lookup = true;
            break;
        }
        CHECK(f.reply.kind != WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL);
        CHECK(!entry_turn(&f, false, 0));
    }
    CHECK(partial_lookup);
    entry_complete(&f);
    CHECK(!f.reply.execution_result && f.reply.committed);
    const col_rel_t *output = session_rel_hash_lookup(f.session, "output");
    CHECK(output && output->nrows == 1 && output->ncols == 1
        && output->columns[0][0] == 7);
    entry_destroy(&f);
}

static void
test_entry_generated_lftj_empty_sources(void)
{
    static const char source[] =
        ".decl input(k:int64)\n.decl a(k:int64)\n.decl b(k:int64)\n"
        ".decl output(k:int64)\n"
        "output(k) :- input(k), a(k), b(k).\n";
    static const int64_t one[] = {7};
    for (uint32_t empty = 0; empty < 3; ++empty) {
        entry_input_t inputs[] = {
            {"input", one, 1, 1}, {"a", one, 1, 1}, {"b", one, 1, 1}
        };
        inputs[empty].rows = NULL;
        inputs[empty].count = 0;
        entry_fixture_t f;
        entry_init_inputs(&f, source, inputs, 3, 0);
        col_rel_t *empty_relation = col_rel_new_auto(inputs[empty].name, 1);
        CHECK(empty_relation && !session_add_rel(f.session, empty_relation));
        entry_complete(&f);
        CHECK(!f.reply.execution_result && f.reply.committed);
        const col_rel_t *output = session_rel_hash_lookup(f.session,
                "output");
        CHECK(output && output->ncols == 1 && output->nrows == 0);
        entry_destroy(&f);
    }
}

static void
test_entry_generated_lftj_duplicate_names_rejected(void)
{
    static const char source[] =
        ".decl input(k:int64)\n.decl a(k:int64)\n.decl b(k:int64)\n"
        ".decl output(k:int64)\n"
        "output(k) :- input(k), a(k), b(k).\n";
    static const int64_t rows[] = {7};
    static const int64_t different_rows[] = {99};
    const entry_input_t inputs[] = {
        {"input", rows, 1, 1}, {"a", rows, 1, 1}, {"b", rows, 1, 1}
    };
    for (unsigned cached_hash = 0; cached_hash < 2; ++cached_hash) {
        for (unsigned duplicate_later_input = 0;
            duplicate_later_input < 2; ++duplicate_later_input) {
            uint32_t duplicate_slot = duplicate_later_input ? 2u : 0u;
            entry_fixture_t f;
            entry_init_inputs(&f, source, inputs, 3, 0);
            CHECK(f.session->nrels < f.session->rel_cap);
            col_rel_t *duplicate = col_rel_new_like(
                inputs[duplicate_slot].name, f.session->rels[duplicate_slot]);
            bool denied = false;
            CHECK(duplicate && !col_rel_append_rows_atomic(duplicate,
                different_rows, 1, 1, &denied) && !denied);
            uint32_t original_count = f.session->nrels;
            f.session->rels[f.session->nrels++] = duplicate;
            if (cached_hash)
                CHECK(!session_rel_build_hash(f.session));
            else
                session_rel_free_hash(f.session);

            wl_columnar_source_access_writer_t writer = {0};
            CHECK(!wl_columnar_source_access_writer_acquire(
                    &f.session->rels[0]->source_access, &writer));
            entry_complete(&f);
            CHECK(f.reply.execution_result == ENOTSUP && !f.reply.committed);
            CHECK(atomic_load_explicit(&f.session->rels[0]->source_access.state,
                memory_order_acquire) == WL_COLUMNAR_SOURCE_ACCESS_WRITER);
            bool output_present = false;
            for (uint32_t i = 0; i < f.session->nrels; ++i)
                output_present |= f.session->rels[i]
                    && !strcmp(f.session->rels[i]->name, "output");
            CHECK(!output_present);
            CHECK(!wl_columnar_source_access_writer_release(&writer));

            f.session->rels[original_count] = NULL;
            f.session->nrels = original_count;
            col_rel_destroy(duplicate);
            if (cached_hash) {
                session_rel_free_hash(f.session);
                CHECK(!session_rel_build_hash(f.session));
            } else {
                CHECK(!f.session->rel_hash_head && !f.session->rel_hash_next);
            }
            entry_destroy(&f);
        }
    }
}

static void
test_entry_generated_lftj_oversized_source_rejected(void)
{
    static const char source[] =
        ".decl input(k:int64)\n.decl a(k:int64)\n.decl b(k:int64)\n"
        ".decl output(k:int64)\n"
        "output(k) :- input(k), a(k), b(k).\n";
    static const int64_t one[] = {7};
    entry_fixture_t f;
    entry_init(&f, source, one, 1, 1, 0);
    col_rel_t *wide = col_rel_new_auto("input",
            WL_COLUMNAR_LFTJ_CONTROLLED_MAX_COLUMNS + 1u);
    CHECK(wide);
    uint32_t input_slot = f.session->nrels;
    for (uint32_t i = 0; i < f.session->nrels; ++i)
        if (!strcmp(f.session->rels[i]->name, "input")) {
            input_slot = i;
            break;
        }
    CHECK(input_slot < f.session->nrels);
    col_rel_t *old = f.session->rels[input_slot];
    f.session->rels[input_slot] = wide;
    col_rel_destroy(old);

    entry_complete(&f);
    CHECK(f.reply.execution_result == ENOTSUP && !f.reply.committed);
    CHECK(atomic_load_explicit(&wide->source_access.state,
        memory_order_acquire) == 0);
    bool output_present = false;
    for (uint32_t i = 0; i < f.session->nrels; ++i)
        output_present |= f.session->rels[i]
            && !strcmp(f.session->rels[i]->name, "output");
    CHECK(!output_present);
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

static void
entry_assert_lftj_inputs_unchanged(entry_fixture_t *f,
    const entry_input_t *inputs, uint32_t input_count);

static unsigned lftj_writer_release_failures;
static int
lftj_writer_release_fail_once(
    wl_columnar_relation_mutation_single_t *single, unsigned slot)
{
    (void)single;
    if (slot == 0 && lftj_writer_release_failures++ == 0)
        return EIO;
    return 0;
}

static void
test_entry_generated_lftj_writer_end_retry(void)
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
    uint64_t reserved_before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.session->memory_governor));

    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    for (unsigned turn = 0; turn < 200000; ++turn) {
        ctx = entry_context(&f);
        if (ctx && ctx->child.lftj.identity == &ctx->child.lftj
            && ctx->child.lftj.writer_active)
            break;
        CHECK(f.reply.kind != WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL);
        CHECK(!entry_turn(&f, false, 0));
    }
    CHECK(ctx && ctx->child.lftj.identity == &ctx->child.lftj
        && ctx->child.lftj.writer_active
        && ctx->child.lftj.writer.raw.active);
    lftj_writer_release_failures = 0;
    wl_columnar_relation_test_single_release = lftj_writer_release_fail_once;
    int rc = 0;
    for (unsigned turn = 0; turn < 32 && !lftj_writer_release_failures;
        ++turn)
        rc = entry_turn_action(&f,
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0);
    wl_columnar_relation_test_single_release = NULL;
    CHECK(!rc && lftj_writer_release_failures == 1);
    ctx = entry_context(&f);
    CHECK(ctx && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && ctx->child.lftj.writer_active
        && ctx->child.lftj.writer.raw.active
        && ctx->child.lftj.writer.phase ==
        WL_COLUMNAR_RELATION_BATCH_DRAINING);

    /* The next host call supplies a fresh grant, consumes the child refusal,
     * then retries the same retained writer-finalization stage. */
    CHECK(!entry_turn(&f, false, 0));
    entry_complete(&f);
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !f.reply.committed && !session_rel_hash_lookup(f.session, "output")
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f.session->memory_governor)) == reserved_before);
    entry_destroy(&f);
}

static void
test_entry_generated_lftj_successful_writer_end_retry(void)
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
    CHECK(entry_lftj_phase_reached(&f, WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE,
        WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK));
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
    CHECK(ctx && ctx->child.lftj.writer_active
        && !ctx->child.lftj.semantic_result
        && ctx->child.lftj.writer.raw.active
        && !session_rel_hash_lookup(f.session, "output"));

    lftj_writer_release_failures = 0;
    wl_columnar_relation_test_single_release = lftj_writer_release_fail_once;
    int rc = 0;
    for (unsigned turn = 0; turn < 32 && !lftj_writer_release_failures;
        ++turn)
        rc = entry_turn(&f, false, 0);
    wl_columnar_relation_test_single_release = NULL;
    CHECK(!rc && lftj_writer_release_failures == 1);
    ctx = entry_context(&f);
    CHECK(ctx && ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE
        && ctx->child.lftj.writer_active
        && ctx->child.lftj.writer.raw.active
        && ctx->child.lftj.writer.phase ==
        WL_COLUMNAR_RELATION_BATCH_DRAINING
        && !ctx->child.lftj.semantic_result
        && !session_rel_hash_lookup(f.session, "output"));

    entry_complete(&f);
    const col_rel_t *output = session_rel_hash_lookup(f.session, "output");
    CHECK(!f.reply.execution_result && f.reply.committed
        && output && output->nrows == 3 && output->ncols == 1
        && output->columns[0][0] == 1 && output->columns[0][1] == 2
        && output->columns[0][2] == 3);
    entry_assert_lftj_inputs_unchanged(&f, inputs, 3);
    entry_destroy(&f);
}

static void
test_entry_generated_lftj_resumable_reader_acquire(void)
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
    col_rel_t *input = session_find_rel(f.session, "input");
    CHECK(input);
    CHECK(entry_lftj_phase_reached(&f, WL_COLUMNAR_LFTJ_ATTEMPT_ACQUIRE,
        WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK));

    wl_columnar_source_access_writer_t descriptor_writer = { 0 };
    CHECK(!wl_columnar_source_access_writer_acquire(
            &input->descriptor_access, &descriptor_writer));
    CHECK(!entry_turn(&f, false, 0));
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
    CHECK(ctx && ctx->child.lftj.cursor == 0
        && ctx->child.lftj.inputs[0].reader.acquire_stage == 0
        && !ctx->child.lftj.semantic_result);
    CHECK(!wl_columnar_source_access_writer_release(&descriptor_writer));

    CHECK(!entry_turn(&f, false, 0));
    ctx = entry_context(&f);
    CHECK(ctx && ctx->child.lftj.cursor == 0
        && ctx->child.lftj.inputs[0].reader.acquire_stage == 1
        && ctx->child.lftj.inputs[0].reader.owner
        == &input->descriptor_access
        && !ctx->child.lftj.inputs[0].reader.secondary_owner);

    wl_columnar_source_access_writer_t storage_writer = { 0 };
    CHECK(!wl_columnar_source_access_writer_acquire(&input->source_access,
        &storage_writer));
    for (unsigned retry = 0; retry < 2; ++retry) {
        CHECK(!entry_turn(&f, false, 0));
        ctx = entry_context(&f);
        CHECK(ctx && ctx->child.lftj.cursor == 0
            && ctx->child.lftj.inputs[0].reader.acquire_stage == 1
            && ctx->child.lftj.inputs[0].reader.owner
            == &input->descriptor_access
            && !ctx->child.lftj.semantic_result);
    }
    CHECK(!wl_columnar_source_access_writer_release(&storage_writer));
    for (unsigned retry = 0; retry < 16; ++retry) {
        ctx = entry_context(&f);
        if (ctx && ctx->child.lftj.cursor == 1)
            break;
        CHECK(!entry_turn(&f, false, 0));
    }
    ctx = entry_context(&f);
    CHECK(ctx && ctx->child.lftj.cursor == 1
        && ctx->child.lftj.inputs[0].reader.acquire_stage == 0
        && ctx->child.lftj.inputs[0].reader.owner == &input->source_access
        && ctx->child.lftj.inputs[0].reader.secondary_owner
        == &input->descriptor_access);

    entry_complete(&f);
    const col_rel_t *output = session_rel_hash_lookup(f.session, "output");
    CHECK(!f.reply.execution_result && f.reply.committed
        && output && output->nrows == 3);
    entry_assert_lftj_inputs_unchanged(&f, inputs, 3);
    CHECK(atomic_load_explicit(&input->source_access.state,
        memory_order_acquire) == 0
        && atomic_load_explicit(&input->descriptor_access.state,
        memory_order_acquire) == 0);
    entry_destroy(&f);
}

static void
test_entry_generated_lftj_partial_reader_acquire_abort(void)
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
    col_rel_t *input = session_find_rel(f.session, "input");
    CHECK(input && entry_lftj_phase_reached(&f,
        WL_COLUMNAR_LFTJ_ATTEMPT_ACQUIRE,
        WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK));
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    for (unsigned retry = 0; retry < 16; ++retry) {
        ctx = entry_context(&f);
        if (ctx && ctx->child.lftj.inputs[0].reader.acquire_stage == 1)
            break;
        CHECK(!entry_turn(&f, false, 0));
    }
    ctx = entry_context(&f);
    CHECK(ctx && ctx->child.lftj.inputs[0].reader.acquire_stage == 1
        && ctx->child.lftj.inputs[0].reader.owner
        == &input->descriptor_access);
    CHECK(!entry_turn_action(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    entry_complete(&f);
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !f.reply.committed && !session_rel_hash_lookup(f.session, "output")
        && atomic_load_explicit(&input->descriptor_access.state,
        memory_order_acquire) == 0
        && atomic_load_explicit(&input->source_access.state,
        memory_order_acquire) == 0);
    entry_assert_lftj_inputs_unchanged(&f, inputs, 3);
    entry_destroy(&f);
}

static void
test_entry_generated_lftj_abort_cleanup_retry(void)
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
    col_rel_t **old_rels = f.session->rels;
    uint32_t old_nrels = f.session->nrels;
    uint32_t old_arr_count = f.session->arr_count;
    col_arr_entry_t *old_arr_entries = f.session->arr_entries;
    uint64_t reserved_before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.session->memory_governor));

    CHECK(entry_lftj_phase_reached(&f, WL_COLUMNAR_LFTJ_ATTEMPT_SORT,
        WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK));
    wl_columnar_eval_serial_framed_attempt_t *ctx = entry_context(&f);
    CHECK(ctx && ctx->abort_supported
        && ctx->child.lftj.identity == &ctx->child.lftj
        && ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_SORT);
    bool lftj_done = false;
    CHECK(wl_columnar_ops_lftj_attempt_abort(&ctx->child.lftj, &lftj_done,
        f.operation, f.id) == EINVAL
        && !lftj_done
        && ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_SORT
        && !ctx->child.lftj.semantic_result);
    grant_t *denied = entry_grant(&f, 1);
    CHECK(!wl_evaluation_control_charge_attempt(denied->control, denied->id,
        1));
    CHECK(wl_columnar_ops_lftj_attempt_abort(&ctx->child.lftj, &lftj_done,
        denied->control, denied->id)
        == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !lftj_done
        && ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_SORT
        && !ctx->child.lftj.semantic_result);
    CHECK(wl_evaluation_control_finish(denied->control, &denied->owner,
        0, 0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!entry_turn_action(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(ctx->abort_requested);

    for (unsigned turn = 0; turn < 16
        && ctx->child.lftj.phase != WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE; ++turn)
        CHECK(!entry_turn_action(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE);
    for (unsigned turn = 0; turn < 16
        && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING; ++turn)
        CHECK(!entry_turn_action(&f, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 0));
    CHECK(ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING);
    uint64_t reserved_at_refusal = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.session->memory_governor));
    int cleanup_refusal = entry_turn_action(&f,
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT, 1152);
    CHECK(cleanup_refusal == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        && ctx->child.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        && ctx->child.pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
        && ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE
        && ctx->child.lftj.semantic_result
        == WL_EVALUATION_CONTROL_CANCELLED
        && ctx->child.lftj.inputs[0].reader.owner
        && f.session->rels == old_rels && f.session->nrels == old_nrels
        && !session_rel_hash_lookup(f.session, "output")
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f.session->memory_governor)) == reserved_at_refusal);

    entry_complete(&f);
    CHECK(f.reply.execution_result == WL_EVALUATION_CONTROL_CANCELLED
        && !f.reply.committed && !session_rel_hash_lookup(f.session, "output")
        && f.session->rels == old_rels && f.session->nrels == old_nrels
        && f.session->arr_count == old_arr_count
        && f.session->arr_entries == old_arr_entries
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f.session->memory_governor)) == reserved_before);
    entry_assert_lftj_inputs_unchanged(&f, inputs, 3);
    entry_finish_original(&f);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && report.result == WL_EVALUATION_CONTROL_CANCELLED
        && report.consumed > 0);
    entry_retry_after_finished_stop(&f, source, inputs, 3);
    entry_destroy(&f);
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

static void
test_entry_generated_lftj_allocation_precharge(void)
{
    enum { INPUT_COUNT = 3, ROW_COUNT = 256 };
    static const char source[] =
        ".decl input(k:int64)\n.decl a(k:int64)\n.decl b(k:int64)\n"
        ".decl output(k:int64)\noutput(k) :- input(k), a(k), b(k).\n";
    int64_t rows[ROW_COUNT];
    for (uint32_t i = 0; i < ROW_COUNT; ++i)
        rows[i] = (int64_t)i + 1;
    const entry_input_t inputs[] = {
        {"input", rows, ROW_COUNT, 1},
        {"a", rows, ROW_COUNT, 1},
        {"b", rows, ROW_COUNT, 1}
    };
    /* Three output cells plus the allocation action; each nonempty input pays
     * two full scalar arrays and two allocation actions. */
    const uint64_t allocation_tariff = 3u + 8u
        + INPUT_COUNT * (2u * ROW_COUNT + 2u * 8u);
    const uint64_t precharge_prefix = entry_lftj_phase_minimum_limit(source,
            inputs, INPUT_COUNT,
            WL_COLUMNAR_LFTJ_ATTEMPT_ALLOC_PRECHARGE,
            WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK);
    CHECK(precharge_prefix > 0
        && allocation_tariff > WL_EVALUATION_CONTROL_QUANTUM);

    wl_columnar_lftj_test_clear_hooks();
    entry_fixture_t short_one;
    entry_init_inputs(&short_one, source, inputs, INPUT_COUNT,
        precharge_prefix + allocation_tariff - 1u);
    uint64_t short_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(
            short_one.session->memory_governor));
    CHECK(entry_lftj_phase_reached(&short_one,
        WL_COLUMNAR_LFTJ_ATTEMPT_ALLOC_PRECHARGE,
        WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK));
    entry_complete(&short_one);
    CHECK(short_one.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !short_one.reply.committed
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            short_one.session->memory_governor)) == short_reserved);
    wl_columnar_lftj_test_hook_state_t short_hooks;
    wl_columnar_lftj_test_get_hook_state(&short_hooks);
    CHECK(short_hooks.controlled_allocation_work_total == allocation_tariff
        && short_hooks.controlled_allocation_work_charged
        < allocation_tariff
        && !short_hooks.controlled_row_allocations
        && !short_hooks.controlled_input_array_allocations
        && !short_hooks.controlled_input_array_bytes);
    entry_retry_after_stop(&short_one, source, inputs, INPUT_COUNT);
    const col_rel_t *short_output = session_rel_hash_lookup(short_one.session,
            "output");
    CHECK(short_output && short_output->nrows == ROW_COUNT);
    entry_destroy(&short_one);

    wl_columnar_lftj_test_clear_hooks();
    entry_fixture_t exact;
    entry_init_inputs(&exact, source, inputs, INPUT_COUNT,
        precharge_prefix + allocation_tariff);
    uint64_t exact_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(exact.session->memory_governor));
    CHECK(entry_lftj_phase_reached(&exact,
        WL_COLUMNAR_LFTJ_ATTEMPT_ALLOC_PRECHARGE,
        WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK));
    entry_complete(&exact);
    CHECK(exact.reply.execution_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !exact.reply.committed
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            exact.session->memory_governor)) == exact_reserved);
    wl_columnar_lftj_test_hook_state_t exact_hooks;
    wl_columnar_lftj_test_get_hook_state(&exact_hooks);
    CHECK(exact_hooks.controlled_allocation_work_total == allocation_tariff
        && exact_hooks.controlled_allocation_work_charged == allocation_tariff
        && exact_hooks.controlled_row_allocations == 1
        && exact_hooks.controlled_input_array_allocations
        == 2u * INPUT_COUNT
        && exact_hooks.controlled_input_array_bytes
        == 2u * INPUT_COUNT * ROW_COUNT * sizeof(int64_t));
    entry_retry_after_stop(&exact, source, inputs, INPUT_COUNT);
    const col_rel_t *exact_output = session_rel_hash_lookup(exact.session,
            "output");
    CHECK(exact_output && exact_output->nrows == ROW_COUNT);
    entry_destroy(&exact);

    {
        static const int64_t one[] = {7};
        const entry_input_t with_empty[] = {
            {"input", one, 1, 1}, {"a", NULL, 0, 1}, {"b", one, 1, 1}
        };
        wl_columnar_lftj_test_clear_hooks();
        entry_fixture_t empty;
        entry_init_inputs(&empty, source, with_empty, INPUT_COUNT, 0);
        col_rel_t *empty_relation = col_rel_new_auto("a", 1);
        CHECK(empty_relation && !session_add_rel(empty.session,
            empty_relation));
        entry_complete(&empty);
        CHECK(!empty.reply.execution_result && empty.reply.committed);
        const col_rel_t *output = session_rel_hash_lookup(empty.session,
                "output");
        CHECK(output && output->nrows == 0);
        wl_columnar_lftj_test_hook_state_t empty_hooks;
        wl_columnar_lftj_test_get_hook_state(&empty_hooks);
        CHECK(empty_hooks.controlled_row_allocations == 1
            && empty_hooks.controlled_input_array_allocations == 4
            && empty_hooks.controlled_input_array_bytes
            == 4u * sizeof(int64_t));
        entry_destroy(&empty);
    }

    {
        uint64_t total = UINT64_MAX - 1u;
        CHECK(wl_columnar_lftj_test_add_allocation_work(&total, 2u)
            == EOVERFLOW && total == UINT64_MAX - 1u);
        CHECK(!wl_columnar_lftj_test_add_allocation_work(&total, 1u)
            && total == UINT64_MAX);
        CHECK(wl_columnar_lftj_test_add_allocation_work(NULL, 1u) == EINVAL);
    }
    wl_columnar_lftj_test_clear_hooks();
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

static void
scalar_retirement_custom_release(struct ArrowSchema *schema)
{
    (void)schema;
}

static void
test_scalar_retirement_receipt(void)
{
    const uint32_t widths[] = {0, 1, 3, 257};
    for (unsigned test = 0; test < 4; ++test) {
        uint32_t width = widths[test];
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor);
        col_rel_t *relation = NULL;
        CHECK(!wl_columnar_relation_new_auto_governed("retire", width, 4,
            false, governor, &relation));
        int64_t row[257] = {0};
        CHECK(!col_rel_append_row(relation, row));
        uint64_t retained = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor));
        CHECK(retained);
        wl_mem_ledger_t ledger;
        wl_mem_ledger_init(&ledger, 0);
        if (width == 3) {
            relation->mem_ledger = &ledger;
            wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
                col_rel_owned_ledger_bytes(relation));
        }
        int owner;
        wl_evaluation_control_t *control = control_new(0, &owner);
        CHECK(!wl_evaluation_control_begin(control, &owner));
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        wl_columnar_relation_scalar_retirement_t receipt = {0};
        if (width == 1) {
            wl_columnar_source_access_reader_t pin = {0};
            CHECK(!col_rel_source_reader_acquire(relation, &pin));
            CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt,
                relation));
            CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt,
                control, id) == EAGAIN);
            CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
            CHECK(!col_rel_source_reader_release(&pin));
            void (*release)(struct ArrowSchema *) = relation->schema.release;
            relation->schema.release = scalar_retirement_custom_release;
            CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt,
                relation));
            CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt,
                control, id) == EAGAIN);
            CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt,
                control, id) == ENOTSUP);
            CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
            relation->schema.release = release;
        }
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt,
            relation));
        int rc;
        do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&receipt,
                    control, id);
        } while (rc == EAGAIN);
        CHECK(!rc && receipt.ready && relation->nrows == 1);
        CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
        CHECK(relation->nrows == 1 && !strcmp(relation->name, "retire"));
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor)) == retained);
        wl_columnar_source_access_reader_t reader = {0};
        CHECK(!col_rel_source_reader_acquire(relation, &reader));
        CHECK(!col_rel_source_reader_release(&reader));
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt,
            relation));
        do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&receipt,
                    control, id);
        } while (rc == EAGAIN);
        CHECK(!rc && !wl_columnar_relation_scalar_retirement_commit(&receipt));
        CHECK(wl_columnar_relation_scalar_retirement_cancel(&receipt) ==
            EINVAL);
        bool freed = false;
        unsigned calls = 0;
        do {
            /* Physical frees receive one unit; credit/ledger tails alternate
             * refused one-unit grants with enough funding to make progress. */
            (void)wl_evaluation_control_finish(control, &owner, 0, 0);
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control,
                (receipt.storage.relation && receipt.phase != 12)
                || calls % 2 ? 1 : 1024));
            CHECK(!wl_evaluation_control_begin(control, &owner));
            id = wl_evaluation_control_active_attempt_id(control);
            rc = wl_columnar_relation_scalar_retirement_step(&receipt, control,
                    id);
            CHECK(rc == 0 || rc == EAGAIN ||
                rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
            if (!receipt.storage.relation) freed = true;
            CHECK(++calls < 10000);
        } while (rc);
        CHECK(freed &&
            receipt.storage.phase ==
            WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_DONE);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        CHECK(!wl_columnar_relation_scalar_retirement_step(&receipt, control,
            id));
        wl_mem_ledger_snapshot_t snapshot;
        wl_mem_ledger_snapshot(&ledger, &snapshot);
        CHECK(snapshot.current_bytes == 0);
        (void)wl_evaluation_control_finish(control, &owner, 0, 0);
        CHECK(!wl_evaluation_control_detach(control, &owner));
        wl_evaluation_control_release(control);
        wl_columnar_memory_governor_ref_release(governor);
    }
}

int
main(void)
{
    test_recursive_reachability_plan_manifest();
    test_recursive_two_join_plan_manifest();
    test_controlled_recursive_two_join_step();
    test_controlled_recursive_two_join_successor_stop();
    test_controlled_recursive_step_publication_and_stops();
    test_controlled_recursive_step_after_public_edge_append();
    test_controlled_recursive_successor_work_boundaries();
    test_controlled_recursive_successor_stop_preserves_published_path();
    test_controlled_recursive_snapshot_rejects_prelease_mutation();
    test_controlled_recursive_step_requires_edge_before_attempt();
    test_controlled_recursive_step_general_join_chain();
    test_controlled_session_step_resume();
    test_controlled_recursive_transaction();
    test_controlled_recursive_transaction_successors();
    test_controlled_recursive_transaction_stops();
    test_controlled_recursive_transaction_shapes();
    test_controlled_recursive_transaction_refusals();
    test_controlled_recursive_transaction_nonlinear_view();
    test_controlled_observed_multi();
    test_controlled_incremental_consolidate_step();
    test_controlled_observed_wide_public();
    test_controlled_observed_transaction_budget();
    test_controlled_observed_missing_private_head();
    test_controlled_observed_step();
    test_scalar_retirement_receipt();
    test_controlled_observed_filter_successors();
    test_controlled_observed_filter_stops();
    test_controlled_observed_preflight_rejections();
    test_controlled_observed_large_output();
    test_controlled_observed_admission_work();
    test_controlled_session_join_step();
    test_controlled_step_preflight_byte_cursor();
    test_controlled_step_preflight_work_limit_boundary();
    test_controlled_step_scheduler_empty_strata();
    test_controlled_batch_join_filter_map_step();
    test_controlled_batch_delta_selectors();
    test_controlled_batch_child_domain_rejection();
    test_controlled_session_step_prefix_resume();
    test_controlled_session_step_destroy_refusal();
#if !defined(_WIN32)
    test_controlled_session_step_void_destroy_precondition();
#endif
    test_controlled_session_snapshot_publication();
    test_controlled_snapshot_preparation();
    test_controlled_session_admission_matrix();
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
    test_entry_generated_lftj_bounded_source_resolution();
    test_entry_generated_lftj_empty_sources();
    test_entry_generated_lftj_duplicate_names_rejected();
    test_entry_generated_lftj_oversized_source_rejected();
    test_entry_generated_lftj_widest_output_row();
    test_entry_generated_lftj_writer_end_retry();
    test_entry_generated_lftj_successful_writer_end_retry();
    test_entry_generated_lftj_resumable_reader_acquire();
    test_entry_generated_lftj_partial_reader_acquire_abort();
    test_entry_generated_lftj_abort_cleanup_retry();
    test_entry_generated_lftj_stops_inside_child();
    test_entry_generated_lftj_allocation_precharge();
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
