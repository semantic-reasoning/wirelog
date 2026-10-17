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

int
main(void)
{
    test_shapes(); test_generated_union(); test_refusal_and_identity();
    test_failures(); test_retained_and_consume_refusal();
    test_growth_quota_and_retry(); test_bootstrap_limits();
    puts("evaluation_serial: real result bridge, named copy and drain passed");
    return 0;
}
