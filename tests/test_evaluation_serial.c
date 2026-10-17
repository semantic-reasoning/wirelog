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

int
main(void)
{
    test_shapes(); test_generated_union(); test_refusal_and_identity();
    test_failures(); test_retained_and_consume_refusal();
    test_growth_quota_and_retry(); test_bootstrap_limits();
    test_publication_shapes(); test_publication_reverse();
    test_publication_failures();
    puts("evaluation_serial: real result bridge, publication and drain passed");
    return 0;
}
