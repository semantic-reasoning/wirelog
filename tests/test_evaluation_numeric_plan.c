/* #1821 production numeric plan composition, original authority and host turns. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/thread.h"
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
    wl_col_session_t session;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_columnar_eval_plan_numeric_attempt_t record;
    wl_evaluation_control_t *operation, *funding;
    uint64_t operation_id, funding_id;
    int operation_owner, funding_owner;
    col_rel_t *input;
    unsigned calls, witnesses;
    int pending_protocol;
} fixture_t;

static wl_evaluation_control_t *
control_new(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(limit, &control));
    CHECK(!wl_evaluation_control_attach(control, owner));
    CHECK(!wl_evaluation_control_begin(control, owner));
    return control;
}

static col_rel_t *
source_new(uint32_t width, uint32_t rows)
{
    col_rel_t *rel = NULL;
    CHECK(!col_rel_alloc(&rel, "input"));
    char **names = calloc(width ? width : 1, sizeof(*names));
    int64_t *values = calloc(width ? width : 1, sizeof(*values));
    CHECK(names && values);
    for (uint32_t c = 0; c < width; ++c) {
        names[c] = malloc(32); CHECK(names[c]);
        snprintf(names[c], 32, "col%u", c);
    }
    CHECK(!col_rel_set_schema(rel, width, (const char *const *)names));
    for (uint32_t r = 0; r < rows; ++r) {
        for (uint32_t c = 0; c < width; ++c) values[c] = (int64_t)r + c;
        CHECK(!col_rel_append_row(rel, values));
    }
    for (uint32_t c = 0; c < width; ++c) free(names[c]);
    free(names); free(values);
    return rel;
}

static void
fixture_init(fixture_t *f, uint32_t width, uint32_t rows, uint64_t limit)
{
    memset(f, 0, sizeof(*f));
    wl_mem_ledger_init(&f->session.mem_ledger, 0);
    f->operation = control_new(limit, &f->operation_owner);
    f->funding = control_new(0, &f->funding_owner);
    f->operation_id = wl_evaluation_control_active_attempt_id(f->operation);
    f->funding_id = wl_evaluation_control_active_attempt_id(f->funding);
    f->input = source_new(width, rows);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f->session, &f->frame));
    CHECK(!eval_stack_push(wl_columnar_eval_stack_cleanup_stack(f->frame),
        f->input, false));
    wl_columnar_eval_plan_numeric_attempt_init(&f->record);
}

static void
fixture_begin(fixture_t *f, const wl_plan_relation_t *plan,
    uint32_t begin, uint32_t end)
{
    CHECK(!wl_columnar_eval_plan_numeric_attempt_begin(&f->record, plan,
        begin, end, f->frame, &f->session, f->operation, f->operation_id,
        f->funding, f->funding_id));
}

/* Two actual FIFO jobs. Requeued engine work remains behind the already
 * queued witness; consume_result runs on a separate engine dispatch. */
typedef struct {
    unsigned entries[4], head, count;
} queue_t;

static void
enqueue(queue_t *q, unsigned job)
{
    CHECK(q->count < 4);
    q->entries[(q->head + q->count) % 4] = job;
    ++q->count;
}

static void
engine_dispatch(fixture_t *f)
{
    ++f->calls;
    CHECK(f->calls == f->witnesses + 1);
    if (f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING) {
        CHECK(!wl_columnar_eval_plan_numeric_consume_result(&f->record,
            f->pending_protocol, f->funding, f->funding_id));
    } else {
        f->pending_protocol = wl_columnar_eval_plan_numeric_attempt_step(
            &f->record, f->funding, f->funding_id);
        CHECK(!f->pending_protocol ||
            (f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING &&
            f->record.pending_action >=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INIT &&
            f->record.pending_action <=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END));
    }
}

/* One real host action pops one job and returns to the external scheduler.
 * Queue the engine continuation before entering the child. A final DONE
 * sentinel drains without another engine call, result consume or grant. */
static void
numeric_plan_host_turn(queue_t *q, fixture_t *f)
{
    CHECK(q->count);
    unsigned job = q->entries[q->head];
    q->head = (q->head + 1) % 4; --q->count;
    if (job == 0) {
        if (f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE) return;
        enqueue(q, 0);
        engine_dispatch(f);
        return;
    }
    ++f->witnesses;
    CHECK(f->witnesses == f->calls);
    if (f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE) enqueue(q, 1);
}

static void
run(fixture_t *f)
{
    queue_t q = {{0}, 0, 0};
    enqueue(&q, 0); enqueue(&q, 1);
    unsigned turns = 0;
    while (q.count) {
        CHECK(++turns < 100000);
        numeric_plan_host_turn(&q, f);
    }
    CHECK(f->calls == f->witnesses && f->calls > 1);
}

static void
fixture_close(fixture_t *f)
{
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY);
    int result = f->record.semantic_result;
    CHECK(!wl_columnar_eval_plan_numeric_attempt_ack(&f->record,
        f->funding, f->funding_id));
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    CHECK(!f->session.cleanup_active_count &&
        !f->session.cleanup_reserved_bytes);
    uint64_t active = wl_evaluation_control_active_attempt_id(f->operation);
    if (active && active != f->operation_id) result = 0;
    if (active)
        CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner,
            result, 0) == result);
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f->operation, &f->operation_owner));
    CHECK(!wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->operation);
    wl_evaluation_control_release(f->funding);
    col_rel_destroy(f->input);
}

static void
compare_raw(fixture_t *f, const wl_plan_relation_t *plan,
    uint32_t begin, uint32_t end)
{
    eval_stack_t raw = {0};
    CHECK(!eval_stack_push(&raw, f->input, false));
    for (uint32_t i = begin; i < end; ++i) {
        int rc = plan->ops[i].op == WL_PLAN_OP_FILTER
            ? wl_columnar_filter_op(&plan->ops[i], &raw, &f->session)
            : col_op_map(&plan->ops[i], &raw, &f->session);
        CHECK(!rc);
    }
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack->top == 1 && raw.top == 1);
    col_rel_t *actual = stack->items[0].rel, *expected = raw.items[0].rel;
    CHECK(actual->nrows == expected->nrows && actual->ncols == expected->ncols);
    CHECK((actual->timestamps != NULL) == (expected->timestamps != NULL));
    CHECK((actual->column_types != NULL) == (expected->column_types != NULL));
    if (actual->timestamps)
        for (uint32_t r = 0; r < actual->nrows; ++r) {
            CHECK(actual->timestamps[r].stratum ==
                expected->timestamps[r].stratum);
            CHECK(actual->timestamps[r].worker ==
                expected->timestamps[r].worker);
            CHECK(actual->timestamps[r]._reserved ==
                expected->timestamps[r]._reserved);
            CHECK(actual->timestamps[r].iteration ==
                expected->timestamps[r].iteration);
            CHECK(actual->timestamps[r].multiplicity ==
                expected->timestamps[r].multiplicity);
        }
    for (uint32_t c = 0; c < actual->ncols; ++c) {
        CHECK(!strcmp(actual->col_names[c], expected->col_names[c]));
        if (actual->column_types)
            CHECK(actual->column_types[c] == expected->column_types[c]);
        for (uint32_t r = 0; r < actual->nrows; ++r)
            CHECK(col_rel_get(actual, r, c) == col_rel_get(expected, r, c));
    }
    CHECK(!eval_stack_drain(&raw));
}

static void
source_float(fixture_t *f)
{
    uint32_t width = f->input->ncols, rows = f->input->nrows;
    col_rel_t *rel = source_new(width, 0);
    wirelog_column_type_t *types = calloc(width, sizeof(*types));
    int64_t *values = calloc(width, sizeof(*values));
    CHECK(types && values);
    for (uint32_t c = 0; c < width; ++c) types[c] = WIRELOG_TYPE_FLOAT;
    CHECK(!col_rel_set_column_types(rel, types, width));
    for (uint32_t r = 0; r < rows; ++r) {
        for (uint32_t c = 0; c < width; ++c)
            values[c] = wl_columnar_float_to_bits(r ==
                    0 ? -0.0 : (double)r - 4.5);
        CHECK(!col_rel_append_row(rel, values));
    }
    free(types); free(values);
    wl_columnar_eval_stack_cleanup_stack(f->frame)->items[0].rel = rel;
    col_rel_destroy(f->input); f->input = rel;
}

static void
test_generated(bool floating)
{
    const char *text = floating
        ? ".decl input(x: float)\n.decl output(y: float)\n"
        "output(x + 1.25) :- input(x), x > -3.0.\n"
        : ".decl input(x: int64)\n.decl output(y: int64)\n"
        "output(x + 1) :- input(x), x > 2.\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *relation = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "output"))
                relation = &plan->strata[s].relations[r];
    CHECK(relation);
    uint32_t begin = 0, end;
    while (begin < relation->op_count
        && relation->ops[begin].op != WL_PLAN_OP_FILTER) ++begin;
    end = begin;
    while (end < relation->op_count
        && (relation->ops[end].op == WL_PLAN_OP_FILTER
        || relation->ops[end].op == WL_PLAN_OP_MAP)) ++end;
    CHECK(end - begin >= 2);
    for (unsigned rows = 0; rows <= 133; rows += 133) {
        fixture_t f; fixture_init(&f, 1, rows, 0);
        if (floating) source_float(&f);
        fixture_begin(&f, relation, begin, end); run(&f);
        CHECK(!f.record.semantic_result && f.record.completed == end - begin);
        compare_raw(&f, relation, begin, end); fixture_close(&f);
    }
    const wl_plan_op_t *filter = NULL, *map = NULL;
    for (uint32_t i = begin; i < end; ++i) {
        if (relation->ops[i].op ==
            WL_PLAN_OP_FILTER) filter = &relation->ops[i];
        if (relation->ops[i].op == WL_PLAN_OP_MAP) map = &relation->ops[i];
    }
    CHECK(filter && map);
    /* Compose actual generated operators in both orders; their bytecode and
     * expression tables remain owned by the real generated plan. */
    for (unsigned order = 0; order < 2; ++order) {
        wl_plan_op_t chain_ops[3] = {*filter, *map, *filter};
        if (order) {
            chain_ops[0] = *map; chain_ops[1] = *filter; chain_ops[2] = *map;
        }
        wl_plan_relation_t chain = {.ops = chain_ops, .op_count = 3};
        const uint32_t widths[] = {1, 17, 129};
        for (unsigned wi = 0; wi < 3; ++wi) {
            fixture_t f; fixture_init(&f, widths[wi], 17, 0);
            if (floating) source_float(&f);
            CHECK(!col_rel_enable_timestamps(f.input));
            for (uint32_t r = 0; r < f.input->nrows; ++r) {
                f.input->timestamps[r].iteration = r;
                f.input->timestamps[r].multiplicity = r % 2 ? -3 : 5;
            }
            fixture_begin(&f, &chain, 0, 3); run(&f);
            CHECK(!f.record.semantic_result && f.record.completed == 3);
            compare_raw(&f, &chain, 0, 3); fixture_close(&f);
        }
    }
    wl_plan_free(plan); wirelog_program_free(program);
}

static void
test_boundaries(void)
{
    wl_plan_op_t ops[3] = {{0}, {0}, {0}};
    for (unsigned i = 0; i < 3; ++i) {
        ops[i].op = WL_PLAN_OP_MAP; ops[i].project_count = 1;
    }
    wl_plan_relation_t plan = {.ops = ops, .op_count = 3};
    for (unsigned bad = 0; bad < 3; ++bad) {
        fixture_t f; fixture_init(&f, 1, 7, 0);
        ops[bad].op = WL_PLAN_OP_CONCAT;
        fixture_begin(&f, &plan, 0, 3); run(&f);
        CHECK(f.record.semantic_result == ENOTSUP && !f.record.completed);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel
            == f.input);
        fixture_close(&f); ops[bad].op = WL_PLAN_OP_MAP;
    }
    wl_plan_op_t nullary = {.op = WL_PLAN_OP_MAP, .project_count = 0};
    wl_plan_relation_t nullary_plan = {.ops = &nullary, .op_count = 1};
    fixture_t zero; fixture_init(&zero, 0, 3, 0);
    fixture_begin(&zero, &nullary_plan, 0, 1); run(&zero);
    CHECK(!zero.record.semantic_result && zero.record.completed == 1);
    compare_raw(&zero, &nullary_plan, 0, 1); fixture_close(&zero);
    fixture_t f; fixture_init(&f, 1, 7, 1);
    fixture_begin(&f, &plan, 0, 3); run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    fixture_close(&f);
}

static void
test_literal_prefixes(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    /* Independent contract literals: one128 preflight admission and one128
     * READY admission, before the child's224 entry admission. */
    const uint64_t limits[] = {1, 127, 128, 255, 256};
    const uint64_t consumed[] = {0, 0, 128, 128, 256};
    for (unsigned i = 0; i < 5; ++i) {
        fixture_t f; fixture_init(&f, 1, 3, limits[i]);
        fixture_begin(&f, &plan, 0, 1); run(&f);
        CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!f.record.completed);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            f.record.semantic_result, 0) == f.record.semantic_result);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.attempt_id == f.operation_id);
        CHECK(report.consumed == consumed[i]);
        fixture_close(&f);
    }
}

static void
test_authority(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 1);
    wl_columnar_eval_stack_cleanup_frame_t *original = f.frame;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    CHECK(f.frame == original);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_controlled(f.frame,
        f.operation, f.operation_id) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_finish(f.frame, 0) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_reset(f.frame) == EBUSY);
    const col_rel_t *input_slot = NULL;
    col_rel_t **output_slot = NULL;
    bool committed = false;
    wl_columnar_filter_context_t *context = NULL;
    CHECK(wl_columnar_eval_stack_unary_input(f.frame, &input_slot) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_output(f.frame, &output_slot) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_commit_attempt_controlled(f.frame,
        &committed) == EBUSY);
    CHECK(wl_columnar_eval_stack_filter_context(f.frame, &context) == EBUSY);
    CHECK(!input_slot && !output_slot && !context && !committed);
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f.session, &nested));
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f.record, f.funding,
        f.funding_id) == EBUSY);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_filter_map_attempt_start(&op, f.frame, &f.session,
        f.operation, f.operation_id, &status) == EBUSY);
    CHECK(status.semantic_result == 119);
    CHECK(wl_columnar_filter_op_attempt_step(f.frame, &status) == EBUSY);
    CHECK(wl_columnar_filter_op_attempt_ack(f.frame) == EBUSY);
    wl_columnar_eval_plan_numeric_attempt_t copy = f.record;
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&copy, f.funding,
        f.funding_id) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_consume_result(&f.record, 0,
        f.funding, f.funding_id) == EBUSY);
    wl_evaluation_control_t *poor = control_new(1, &copy);
    uint64_t id = wl_evaluation_control_active_attempt_id(poor);
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f.record, poor, id)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PREFLIGHT);
    CHECK(wl_evaluation_control_finish(poor, &copy, 0, 0)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(poor, &copy));
    wl_evaluation_control_release(poor);
    run(&f); CHECK(!f.record.semantic_result); fixture_close(&f);
}

static void *
wrong_thread(void *data)
{
    fixture_t *f = data;
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f->record, f->funding,
        f->funding_id) == EINVAL);
    return NULL;
}

typedef struct {
    fixture_t *fixture;
    const wl_plan_op_t *op;
} owned_join_thread_t;

static owned_join_thread_t *owned_join_active;
static unsigned owned_join_reentries;

static void
owned_join_reentry(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase != 1) return;
    fixture_t *f = owned_join_active->fixture;
    CHECK(control == f->operation);
    wl_columnar_filter_context_t *context = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, &context,
        &f->record));
    CHECK(context->busy && context->stage == WL_COLUMNAR_FILTER_STAGE_RUNNING);
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_join_op_attempt_start_owned(owned_join_active->op,
        f->frame, &f->session, f->operation, f->operation_id, &status,
        &f->record) == EBUSY);
    CHECK(status.semantic_result == 119);
    ++owned_join_reentries;
}

static void *
owned_join_wrong_thread(void *data)
{
    owned_join_thread_t *call = data;
    fixture_t *f = call->fixture;
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_join_op_attempt_start_owned(call->op, f->frame,
        &f->session, f->operation, f->operation_id, &status,
        &f->record) == EINVAL);
    CHECK(status.semantic_result == 119);
    return NULL;
}

/* Each actual owning invocation has a distinct host grant and returns before
 * the fixture schedules another. Assertions and teardown are external work. */
static int
owned_join_action(fixture_t *f, const wl_plan_op_t *op,
    wl_columnar_filter_status_t *status, unsigned action)
{
    if (wl_evaluation_control_active_attempt_id(f->funding) != f->funding_id)
        return EINVAL;
    int rc = wl_evaluation_control_charge(f->funding, 384);
    if (rc) return rc;
    if (!action)
        return wl_columnar_join_op_attempt_start_owned(op, f->frame,
                   &f->session, f->operation, f->operation_id, status,
                   &f->record);
    if (action == 1)
        return wl_columnar_filter_op_attempt_step_owned(f->frame, status,
                   &f->record);
    return wl_columnar_filter_op_attempt_ack_owned(f->frame, &f->record);
}

static void
test_owned_join_entry(void)
{
    wl_plan_op_t map = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &map, .op_count = 1};
    wl_plan_op_t join = {.op = WL_PLAN_OP_JOIN, .right_relation = "input",
                         .delta_mode = WL_DELTA_FORCE_FULL};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 1);
    col_rel_t *right = source_new(1, 3);
    col_rel_t *registry[] = {right};
    f.session.rels = registry; f.session.nrels = 1;
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_join_op_attempt_start(&join, f.frame, &f.session,
        f.operation, f.operation_id, &status) == EBUSY);
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &f.session,
        NULL, 0, &status, &f.record) == EINVAL);
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &f.session,
        f.operation, f.operation_id + 1, &status, &f.record) == EINVAL);
    int alien_owner = 0;
    wl_evaluation_control_t *alien = control_new(0, &alien_owner);
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &f.session,
        alien, wl_evaluation_control_active_attempt_id(alien), &status,
        &f.record) == EINVAL);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_finish(alien, &alien_owner, 0, 0));
    CHECK(!wl_evaluation_control_report(alien, &report) && !report.consumed);
    CHECK(!wl_evaluation_control_detach(alien, &alien_owner));
    wl_evaluation_control_release(alien);
    wl_columnar_eval_plan_numeric_attempt_t copy = f.record;
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &f.session,
        f.operation, f.operation_id, &status, &copy) == EBUSY);
    wl_col_session_t foreign = {0};
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &foreign,
        f.operation, f.operation_id, &status, &f.record) == EINVAL);
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f.session, &nested));
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &f.session,
        f.operation, f.operation_id, &status, &f.record) == EBUSY);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    owned_join_thread_t call = {&f, &join};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, owned_join_wrong_thread, &call));
    CHECK(!wl_thread_join(&thread));
    CHECK(status.semantic_result == 119);
    wl_columnar_filter_context_t *context = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &context,
        &f.record));
    CHECK(context->stage == WL_COLUMNAR_FILTER_STAGE_INERT && !context->busy
        && !context->pin_owned && !context->source_initialized
        && !context->join_initialized);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        f.input);

    owned_join_active = &call; owned_join_reentries = 0;
    wl_columnar_join_test_attempt_checkpoint = owned_join_reentry;
    CHECK(!owned_join_action(&f, &join, &status, 0));
    wl_columnar_join_test_attempt_checkpoint = NULL;
    owned_join_active = NULL;
    CHECK(owned_join_reentries == 1);
    wl_columnar_filter_status_t sentinel = {.semantic_result = 119};
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &f.session,
        f.operation, f.operation_id, &sentinel, &f.record) == EBUSY);
    CHECK(sentinel.semantic_result == 119);
    unsigned turns = 0;
    while (status.progress != WL_COLUMNAR_FILTER_PROGRESS_DONE) {
        CHECK(++turns < 10000);
        CHECK(!owned_join_action(&f, &join, &status, 1));
    }
    CHECK(!status.semantic_result && status.committed);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].owned);
    col_rel_t *result = stack->items[0].rel;
    CHECK(result->ncols == 2 && result->nrows == 21);
    for (int64_t x = 0; x < 7; ++x)
        for (int64_t y = 0; y < 3; ++y) {
            unsigned matches = 0;
            for (uint32_t r = 0; r < result->nrows; ++r)
                matches += result->columns[0][r] == x
                    && result->columns[1][r] == y;
            CHECK(matches == 1);
        }
    CHECK(!owned_join_action(&f, &join, &status, 2));
    /* The owning record can still execute its original MAP after JOIN ACK. */
    run(&f); CHECK(!f.record.semantic_result && f.record.completed == 1);
    fixture_close(&f); col_rel_destroy(right);
}

static void
test_owned_join_successor(void)
{
    wl_plan_op_t map = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &map, .op_count = 1};
    wl_plan_op_t join = {.op = WL_PLAN_OP_JOIN, .right_relation = "input"};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 1);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation,
        &report) && !report.consumed);
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    uint64_t next = wl_evaluation_control_active_attempt_id(f.operation);
    CHECK(next != f.operation_id);
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_join_op_attempt_start_owned(&join, f.frame, &f.session,
        f.operation, next, &status, &f.record) == EINVAL);
    CHECK(status.semantic_result == 119);
    wl_columnar_filter_context_t *context = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &context,
        &f.record));
    CHECK(context->stage == WL_COLUMNAR_FILTER_STAGE_INERT && !context->busy
        && !context->pin_owned && !context->source_initialized
        && !context->join_initialized);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        f.input);
    /* Retire the original record through its stale-ID refusal, never execute
     * its MAP using the successor's allowance. */
    run(&f);
    CHECK(f.record.semantic_result == EINVAL && !f.record.completed);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_report(f.operation, &report));
    CHECK(report.attempt_id == next && !report.consumed);
    fixture_close(&f);
}

/* The host pays each separate owning action; status checks and scheduling are
 * external. This direct return has the same caller envelope as owned JOIN. */
static int
owned_consolidate_action(fixture_t *f, const wl_plan_op_t *op,
    wl_columnar_filter_status_t *status, unsigned action)
{
    if (wl_evaluation_control_active_attempt_id(f->funding) != f->funding_id)
        return EINVAL;
    int rc = wl_evaluation_control_charge(f->funding, 384);
    if (rc) return rc;
    if (!action)
        return wl_columnar_filter_consolidate_attempt_start_owned(op,
                   f->frame, &f->session, f->operation, f->operation_id,
                   status, &f->record);
    if (action == 1)
        return wl_columnar_filter_op_attempt_step_owned(f->frame, status,
                   &f->record);
    return wl_columnar_filter_op_attempt_ack_owned(f->frame, &f->record);
}

static void
consolidate_sentinel(const wl_columnar_filter_status_t *status)
{
    CHECK(status->progress == WL_COLUMNAR_FILTER_PROGRESS_NOT_STARTED
        && !status->execution_result && !status->cleanup_result
        && status->semantic_result == 119 && !status->committed);
}

static void *
owned_consolidate_wrong_thread(void *data)
{
    fixture_t *f = data;
    wl_plan_op_t op = {.op = WL_PLAN_OP_CONSOLIDATE};
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f->frame,
        &f->session, f->operation, f->operation_id, &status,
        &f->record) == EINVAL);
    consolidate_sentinel(&status);
    return NULL;
}

static fixture_t *consolidate_active;
static unsigned consolidate_reentries;

static void
consolidate_reentry(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)control; (void)progress;
    if (phase != 1) return;
    fixture_t *f = consolidate_active;
    wl_plan_op_t op = {.op = WL_PLAN_OP_CONSOLIDATE};
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f->frame,
        &f->session, f->operation, f->operation_id, &status,
        &f->record) == EBUSY);
    consolidate_sentinel(&status); ++consolidate_reentries;
}

static void
test_owned_consolidate_entry(void)
{
    wl_plan_op_t map = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &map, .op_count = 1};
    wl_plan_op_t op = {.op = WL_PLAN_OP_CONSOLIDATE};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 1);
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_filter_consolidate_attempt_start(&op, f.frame,
        &f.session, f.operation, f.operation_id, &status) == EBUSY);
    consolidate_sentinel(&status);
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &f.session, NULL, 0, &status, &f.record) == EINVAL);
    consolidate_sentinel(&status);
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &f.session, f.operation, f.operation_id + 1, &status,
        &f.record) == EINVAL);
    consolidate_sentinel(&status);
    int alien_owner = 0;
    wl_evaluation_control_t *alien = control_new(0, &alien_owner);
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &f.session, alien, wl_evaluation_control_active_attempt_id(alien),
        &status, &f.record) == EINVAL);
    consolidate_sentinel(&status);
    CHECK(!wl_evaluation_control_finish(alien, &alien_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(alien, &report) && !report.consumed);
    CHECK(!wl_evaluation_control_detach(alien, &alien_owner));
    wl_evaluation_control_release(alien);
    wl_columnar_eval_plan_numeric_attempt_t copy = f.record;
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &f.session, f.operation, f.operation_id, &status, &copy) == EBUSY);
    consolidate_sentinel(&status);
    wl_col_session_t foreign = {0};
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &foreign, f.operation, f.operation_id, &status, &f.record) == EINVAL);
    consolidate_sentinel(&status);
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f.session, &nested));
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &f.session, f.operation, f.operation_id, &status, &f.record) == EBUSY);
    consolidate_sentinel(&status);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, owned_consolidate_wrong_thread, &f));
    CHECK(!wl_thread_join(&thread));
    wl_columnar_filter_context_t *context = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &context,
        &f.record));
    CHECK(context->stage == WL_COLUMNAR_FILTER_STAGE_INERT
        && !context->pin_owned && !context->source_initialized);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        f.input);
    consolidate_active = &f; consolidate_reentries = 0;
    wl_columnar_filter_test_attempt_checkpoint = consolidate_reentry;
    CHECK(!owned_consolidate_action(&f, &op, &status, 0));
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    consolidate_active = NULL; CHECK(consolidate_reentries == 1);
    unsigned turns = 0;
    while (status.progress != WL_COLUMNAR_FILTER_PROGRESS_DONE) {
        CHECK(++turns < 10000);
        CHECK(!owned_consolidate_action(&f, &op, &status, 1));
    }
    CHECK(status.committed && !status.semantic_result);
    col_rel_t *result = wl_columnar_eval_stack_cleanup_stack(
        f.frame)->items[0].rel;
    CHECK(result != f.input && result->nrows == 7 && result->sorted_nrows == 7);
    for (uint32_t row = 0; row < 7; ++row)
        CHECK(result->columns[0][row] == row);
    CHECK(!owned_consolidate_action(&f, &op, &status, 2));
    run(&f); CHECK(!f.record.semantic_result && f.record.completed == 1);
    compare_raw(&f, &plan, 0, 1); fixture_close(&f);
}

static void
test_owned_consolidate_successor(void)
{
    wl_plan_op_t map = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &map, .op_count = 1};
    wl_plan_op_t op = {.op = WL_PLAN_OP_CONSOLIDATE};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 1);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && !report.consumed && report.attempt_id == f.operation_id);
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    uint64_t next = wl_evaluation_control_active_attempt_id(f.operation);
    wl_columnar_filter_status_t status = {.semantic_result = 119};
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &f.session, f.operation, next, &status, &f.record) == EINVAL);
    consolidate_sentinel(&status);
    CHECK(wl_columnar_filter_consolidate_attempt_start_owned(&op, f.frame,
        &f.session, f.operation, f.operation_id, &status,
        &f.record) == EINVAL);
    consolidate_sentinel(&status);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        f.input);
    run(&f); CHECK(f.record.semantic_result == EINVAL && !f.record.completed);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && !report.consumed && report.attempt_id == next);
    fixture_close(&f);
}

static void
test_intermediate_stop(void)
{
    wl_plan_op_t ops[3] = {{0}, {0}, {0}};
    for (unsigned i = 0; i < 3; ++i) {
        ops[i].op = WL_PLAN_OP_MAP; ops[i].project_count = 1;
    }
    wl_plan_relation_t plan = {.ops = ops, .op_count = 3};
    for (unsigned restart = 0; restart < 2; ++restart) {
        fixture_t f; fixture_init(&f, 1, 17, 0);
        fixture_begin(&f, &plan, 0, 3);
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, wrong_thread, &f));
        CHECK(!wl_thread_join(&thread));
        unsigned turns = 0;
        while (!(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_ADVANCE
            && f.record.completed == 1)) {
            CHECK(++turns < 10000);
            engine_dispatch(&f); ++f.witnesses;
        }
        col_rel_t *intermediate = wl_columnar_eval_stack_cleanup_stack(
            f.frame)->items[0].rel;
        CHECK(intermediate != f.input && intermediate->nrows == 17);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
        if (restart) {
            CHECK(!wl_evaluation_control_finish(f.operation,
                &f.operation_owner, 0, 0));
            CHECK(!wl_evaluation_control_begin(f.operation,
                &f.operation_owner));
        } else {
            wl_evaluation_control_request_cancel(f.operation);
        }
        run(&f);
        CHECK(f.record.completed == 1);
        CHECK(f.record.semantic_result == (restart ? EINVAL :
            WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel
            == intermediate);
        if (restart) {
            uint64_t successor_id = wl_evaluation_control_active_attempt_id(
                f.operation);
            wl_columnar_eval_plan_numeric_status_t status;
            CHECK(wl_columnar_eval_plan_numeric_attempt_status(&f.record,
                &status, f.operation,
                wl_evaluation_control_active_attempt_id(f.operation)) ==
                EINVAL);
            CHECK(!wl_evaluation_control_finish(f.operation,
                &f.operation_owner, 0, 0));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            CHECK(report.attempt_id == successor_id && report.consumed == 0);
        }
        fixture_close(&f);
    }
}

static void
test_retained_input(void)
{
    wl_plan_op_t ops[2] = {{.op = WL_PLAN_OP_MAP, .project_count = 1},
                           {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 2);
    unsigned turns = 0;
    while (!(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_ADVANCE
        && f.record.completed == 1)) {
        CHECK(++turns < 10000); engine_dispatch(&f); ++f.witnesses;
    }
    col_rel_t *held =
        wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(held, &reader));
    while (!(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        && f.record.child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED)) {
        CHECK(++turns < 20000); engine_dispatch(&f); ++f.witnesses;
    }
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    int poor_owner = 0;
    wl_evaluation_control_t *poor = control_new(383, &poor_owner);
    uint64_t poor_id = wl_evaluation_control_active_attempt_id(poor);
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f.record, poor, poor_id)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD &&
        f.record.child_status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    CHECK(wl_evaluation_control_finish(poor, &poor_owner, 0, 0) ==
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(poor, &poor_owner));
    wl_evaluation_control_release(poor);
    /* Two fresh paid retries remain retained; neither destroys the live input. */
    for (unsigned retry = 0; retry < 2; ++retry) {
        engine_dispatch(&f); ++f.witnesses;
        CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING);
        engine_dispatch(&f); ++f.witnesses;
        CHECK(f.record.child_status.progress ==
            WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    }
    CHECK(!col_rel_source_reader_release(&reader));
    run(&f); CHECK(!f.record.semantic_result && f.record.completed == 2);
    compare_raw(&f, &plan, 0, 2); fixture_close(&f);
}

/* An admitted child result remains pending when a different host cannot fund
 * consumption. Retrying with the original host consumes that result once. */
static void
test_pending_funding(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 1);
    engine_dispatch(&f); ++f.witnesses;
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_READY);
    engine_dispatch(&f); ++f.witnesses;
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING);
    int owner = 0;
    wl_evaluation_control_t *poor = control_new(383, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(poor);
    CHECK(wl_columnar_eval_plan_numeric_consume_result(&f.record,
        f.pending_protocol, poor, id) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING);
    CHECK(wl_evaluation_control_finish(poor, &owner, 0, 0) ==
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(poor, &owner));
    wl_evaluation_control_release(poor);
    run(&f);
    CHECK(!f.record.semantic_result && f.record.completed == 1);
    wl_columnar_eval_plan_numeric_status_t status;
    CHECK(!wl_columnar_eval_plan_numeric_attempt_status(&f.record, &status,
        f.funding, f.funding_id));
    CHECK(status.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE &&
        status.completed == 1 && !status.retained);
    fixture_close(&f);
}

static void
test_later_allocation_failure(void)
{
    wl_plan_op_t ops[2] = {{.op = WL_PLAN_OP_MAP, .project_count = 1},
                           {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    fixture_t f; fixture_init(&f, 1, 7, 0); fixture_begin(&f, &plan, 0, 2);
    unsigned turns = 0;
    while (!(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_ADVANCE &&
        f.record.completed == 1)) {
        CHECK(++turns < 10000); engine_dispatch(&f); ++f.witnesses;
    }
    col_rel_t *intermediate =
        wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
    wl_columnar_relation_test_constructor_fail_allocation = 1;
    run(&f);
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    CHECK(f.record.semantic_result == ENOMEM && f.record.completed == 1);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        intermediate);
    fixture_close(&f);
}

static void
test_begin_rollback(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    fixture_t f; fixture_init(&f, 1, 7, 0);
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f.session, &nested));
    CHECK(wl_columnar_eval_plan_numeric_attempt_begin(&f.record, &plan, 0, 1,
        f.frame, &f.session, f.operation, f.operation_id, f.funding,
        f.funding_id) == EBUSY);
    CHECK(!f.record.identity && !f.record.control && !f.record.frame &&
        f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(!eval_stack_push(stack, f.input, false));
    CHECK(wl_columnar_eval_plan_numeric_attempt_begin(&f.record, &plan, 0, 1,
        f.frame, &f.session, f.operation, f.operation_id, f.funding,
        f.funding_id) == EINVAL);
    CHECK(!f.record.identity && !f.record.control && !f.record.frame);
    CHECK(stack->top == 2 && stack->items[0].rel == f.input);
    stack->items[1] = (eval_entry_t){0}; stack->top = 1;
    fixture_begin(&f, &plan, 0, 1);
    wl_columnar_eval_plan_numeric_attempt_t competitor;
    wl_columnar_eval_plan_numeric_attempt_init(&competitor);
    CHECK(wl_columnar_eval_plan_numeric_attempt_begin(&competitor, &plan, 0, 1,
        f.frame, &f.session, f.operation, f.operation_id, f.funding,
        f.funding_id) == EBUSY);
    CHECK(!competitor.identity && !competitor.control && !competitor.frame);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    run(&f); CHECK(!f.record.semantic_result); fixture_close(&f);
}

static void
test_host_boundaries(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    for (uint64_t limit = 383; limit <= 384; ++limit) {
        fixture_t f; fixture_init(&f, 1, 7, 0);
        int owner = 0;
        wl_evaluation_control_t *host = control_new(limit, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(host);
        int rc = wl_columnar_eval_plan_numeric_attempt_begin(&f.record, &plan,
                0, 1, f.frame, &f.session, f.operation, f.operation_id, host,
                id);
        CHECK(rc == (limit == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        if (limit == 383) {
            CHECK(!f.record.identity && !f.record.control);
            fixture_begin(&f, &plan, 0, 1);
        } else {
            CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f.record,
                host, id) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PREFLIGHT);
        }
        CHECK(wl_evaluation_control_finish(host, &owner, 0, 0) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(host, &report));
        CHECK(report.attempt_id == id && report.consumed ==
            (limit == 383 ? 0 : 384));
        CHECK(!wl_evaluation_control_detach(host, &owner));
        wl_evaluation_control_release(host);
        run(&f); CHECK(!f.record.semantic_result); fixture_close(&f);
    }
}

/* Source-bound fragments use an empty original frame and a stable, unique-name
 * registry. Raw parity runs on deep-copied descriptors in a separate session. */
static void
source_fixture(fixture_t *f, uint32_t width, uint32_t rows, uint64_t limit)
{
    fixture_init(f, width, rows, limit);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(eval_stack_pop(stack).rel == f->input);
    memset(&stack->items[0], 0, sizeof(stack->items[0]));
}

static void
source_begin(fixture_t *f, const wl_plan_relation_t *plan,
    col_rel_t **registry, uint32_t count)
{
    f->session.rels = registry; f->session.nrels = count;
    f->session.rel_cap = count;
    CHECK(!wl_columnar_eval_plan_numeric_source_attempt_begin(&f->record,
        plan, 0, plan->op_count, f->frame, &f->session, f->operation,
        f->operation_id, f->funding, f->funding_id));
}

static void
until_phase(fixture_t *f, wl_columnar_eval_plan_numeric_phase_t phase)
{
    unsigned turns = 0;
    while (f->record.phase != phase) {
        CHECK(++turns < 100000);
        CHECK(f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        engine_dispatch(f); ++f->witnesses;
    }
}

static void
same_relation(const col_rel_t *a, const col_rel_t *b)
{
    CHECK(a && b && a->nrows == b->nrows && a->ncols == b->ncols);
    CHECK(a->has_graph_column == b->has_graph_column);
    CHECK(a->graph_col_idx == b->graph_col_idx);
    CHECK(a->declared_ncols == b->declared_ncols);
    CHECK((a->timestamps != NULL) == (b->timestamps != NULL));
    CHECK((a->column_types != NULL) == (b->column_types != NULL));
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
        if (a->column_types) CHECK(a->column_types[c] == b->column_types[c]);
        for (uint32_t r = 0; r < a->nrows; ++r)
            CHECK(col_rel_get(a, r, c) == col_rel_get(b, r, c));
    }
    for (uint32_t r = 0; a->timestamps && r < a->nrows; ++r) {
        CHECK(a->timestamps[r].stratum == b->timestamps[r].stratum);
        CHECK(a->timestamps[r].worker == b->timestamps[r].worker);
        CHECK(a->timestamps[r]._reserved == b->timestamps[r]._reserved);
        CHECK(a->timestamps[r].iteration == b->timestamps[r].iteration);
        CHECK(a->timestamps[r].multiplicity == b->timestamps[r].multiplicity);
    }
}

static void
source_raw_parity(fixture_t *f, const wl_plan_relation_t *plan)
{
    wl_col_session_t raw = {0};
    wl_mem_ledger_init(&raw.mem_ledger, 0);
    raw.delta_seeded = f->session.delta_seeded;
    raw.nrels = f->session.nrels; raw.rel_cap = raw.nrels;
    raw.rels = calloc(raw.nrels ? raw.nrels : 1, sizeof(*raw.rels));
    CHECK(raw.rels);
    for (uint32_t i = 0; i < raw.nrels; ++i)
        if (f->session.rels[i])
            CHECK(!col_rel_deep_copy(f->session.rels[i], &raw.rels[i], NULL));
    eval_stack_t stack = {0};
    for (uint32_t i = 0; i < plan->op_count; ++i) {
        const wl_plan_op_t *op = &plan->ops[i];
        int rc = op->op == WL_PLAN_OP_VARIABLE ? col_op_variable(op, &stack,
                &raw)
            : op->op == WL_PLAN_OP_MAP ? col_op_map(op, &stack, &raw)
            : wl_columnar_filter_op(op, &stack, &raw);
        CHECK(!rc);
    }
    eval_stack_t *actual = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(actual->top == 1 && stack.top == 1);
    same_relation(actual->items[0].rel, stack.items[0].rel);
    CHECK(actual->items[0].is_delta == stack.items[0].is_delta);
    CHECK(!eval_stack_drain(&stack));
    session_rel_free_hash(&raw);
    for (uint32_t i = 0; i < raw.nrels; ++i) col_rel_destroy(raw.rels[i]);
    free(raw.rels);
    CHECK(!f->session.rel_hash_nbuckets); /* No raw lookup/hash fallback. */
}

static void
test_source_generated(void)
{
    for (unsigned floating = 0; floating < 2; ++floating) {
        const char *text = floating
            ? ".decl input(x: float)\n.decl output(y: float)\n"
            "output(x + 1.25) :- input(x), x > -3.0.\n"
            : ".decl input(x: int64)\n.decl output(y: int64)\n"
            "output(x + 1) :- input(x), x > 2.\n";
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        const wl_plan_relation_t *rp = NULL;
        for (uint32_t s = 0; s < plan->stratum_count; ++s)
            for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
                if (!strcmp(plan->strata[s].relations[r].name, "output"))
                    rp = &plan->strata[s].relations[r];
        CHECK(rp && rp->ops[0].op == WL_PLAN_OP_VARIABLE);
        uint32_t end = 1;
        while (end < rp->op_count && (rp->ops[end].op == WL_PLAN_OP_FILTER
            || rp->ops[end].op == WL_PLAN_OP_MAP)) ++end;
        wl_plan_relation_t fragment = {.ops = rp->ops, .op_count = end};
        const uint32_t widths[] = {1, 17, 129};
        for (unsigned w = 0; w < 3; ++w)
            for (unsigned rows = 0; rows <= 133; rows += 133) {
                fixture_t f; source_fixture(&f, widths[w], rows, 0);
                if (floating) {
                    source_float(&f);
                    memset(&wl_columnar_eval_stack_cleanup_stack(
                            f.frame)->items[0],
                        0, sizeof(eval_entry_t));
                }
                CHECK(!col_rel_enable_timestamps(f.input));
                for (uint32_t r = 0; r < rows; ++r) {
                    f.input->timestamps[r].stratum = 3;
                    f.input->timestamps[r].worker = 2;
                    f.input->timestamps[r].iteration = r;
                    f.input->timestamps[r].multiplicity = r % 2 ? -3 : 5;
                }
                col_rel_t *registry[] = {NULL, f.input};
                source_begin(&f, &fragment, registry, 2); run(&f);
                CHECK(!f.record.semantic_result && f.record.source_installed);
                CHECK(f.record.completed == end - 1);
                source_raw_parity(&f, &fragment); fixture_close(&f);
            }
        /* Real generated numeric operands, in both three-operator orders. */
        const wl_plan_op_t *filter = NULL, *map = NULL;
        for (uint32_t i = 1; i < end; ++i) {
            if (rp->ops[i].op == WL_PLAN_OP_FILTER) filter = &rp->ops[i];
            if (rp->ops[i].op == WL_PLAN_OP_MAP) map = &rp->ops[i];
        }
        CHECK(filter && map);
        for (unsigned order = 0; order < 2; ++order) {
            wl_plan_op_t ops[] = {rp->ops[0], *filter, *map, *filter};
            if (order) {
                ops[1] = *map; ops[2] = *filter; ops[3] = *map;
            }
            wl_plan_relation_t chain = {.ops = ops, .op_count = 4};
            fixture_t f; source_fixture(&f, 1, 17, 0);
            if (floating) {
                source_float(&f);
                memset(&wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0],
                    0, sizeof(eval_entry_t));
            }
            col_rel_t *registry[] = {f.input};
            source_begin(&f, &chain, registry, 1); run(&f);
            CHECK(!f.record.semantic_result && f.record.completed == 3);
            source_raw_parity(&f, &chain); fixture_close(&f);
        }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

/* JOIN hash chains may enumerate a bag in a different order. Match complete
 * tuples, including their timestamps, once each; row-count parity alone would
 * miss a duplicate-key loss or a wrong intermediate column. */
static void
same_join_bag(const col_rel_t *a, const col_rel_t *b)
{
    CHECK(a && b && a->nrows == b->nrows && a->ncols == b->ncols);
    CHECK(a->has_graph_column == b->has_graph_column);
    CHECK(a->graph_col_idx == b->graph_col_idx);
    CHECK(a->declared_ncols == b->declared_ncols);
    CHECK((a->timestamps != NULL) == (b->timestamps != NULL));
    CHECK((a->column_types != NULL) == (b->column_types != NULL));
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
        if (a->column_types) CHECK(a->column_types[c] == b->column_types[c]);
    }
    bool *used = calloc(b->nrows ? b->nrows : 1, sizeof(*used));
    CHECK(used);
    for (uint32_t r = 0; r < a->nrows; ++r) {
        uint32_t match = b->nrows;
        for (uint32_t s = 0; s < b->nrows; ++s) {
            if (used[s]) continue;
            bool equal = true;
            for (uint32_t c = 0; c < a->ncols; ++c)
                equal = equal && wl_columnar_value_equal(a, c,
                        col_rel_get(a, r, c), b, c, col_rel_get(b, s, c));
            if (a->timestamps)
                equal = equal &&
                    a->timestamps[r].stratum == b->timestamps[s].stratum
                    && a->timestamps[r].worker == b->timestamps[s].worker
                    && a->timestamps[r]._reserved == b->timestamps[s]._reserved
                    && a->timestamps[r].iteration == b->timestamps[s].iteration
                    && a->timestamps[r].multiplicity ==
                    b->timestamps[s].multiplicity;
            if (equal) {
                match = s; break;
            }
        }
        CHECK(match < b->nrows); used[match] = true;
    }
    free(used);
}

static void
join_raw_parity(fixture_t *f, const wl_plan_relation_t *fragment, bool prebound)
{
    wl_col_session_t raw = {0};
    wl_mem_ledger_init(&raw.mem_ledger, 0);
    raw.delta_seeded = f->session.delta_seeded;
    raw.nrels = f->session.nrels; raw.rel_cap = raw.nrels;
    raw.rels = calloc(raw.nrels ? raw.nrels : 1, sizeof(*raw.rels));
    CHECK(raw.rels);
    for (uint32_t i = 0; i < raw.nrels; ++i)
        if (f->session.rels[i])
            CHECK(!col_rel_deep_copy(f->session.rels[i], &raw.rels[i], NULL));
    eval_stack_t stack = {0};
    if (prebound) CHECK(!eval_stack_push(&stack, raw.rels[0], false));
    wl_plan_relation_t tail = *fragment;
    if (prebound && tail.ops[0].op == WL_PLAN_OP_VARIABLE) {
        ++tail.ops; --tail.op_count;
    }
    CHECK(!col_eval_relation_plan(&tail, &stack, &raw));
    eval_stack_t *actual = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(actual->top == 1 && stack.top == 1);
    same_join_bag(actual->items[0].rel, stack.items[0].rel);
    CHECK(actual->items[0].is_delta == stack.items[0].is_delta);
    CHECK(!eval_stack_drain(&stack));
    /* Raw JOIN may cache an arrangement; it is session-owned, not output-owned. */
    for (uint32_t i = 0; i < raw.arr_count; ++i) {
        CHECK(!raw.arr_entries[i].pin_count);
        free(raw.arr_entries[i].rel_name); free(raw.arr_entries[i].key_cols);
        arr_free_contents(&raw.arr_entries[i].arr);
        col_arr_detach_memory_governor(&raw.arr_entries[i].arr);
    }
    free(raw.arr_entries); session_rel_free_hash(&raw);
    for (uint32_t i = 0; i < raw.nrels; ++i) col_rel_destroy(raw.rels[i]);
    free(raw.rels);
    CHECK(!f->session.rel_hash_nbuckets && !f->session.arr_count);
}

static col_rel_t *
join_float(col_rel_t *original)
{
    col_rel_t *rel = source_new(original->ncols, 0);
    CHECK(!wl_columnar_relation_rename_checked(rel, original->name));
    wirelog_column_type_t *types = calloc(rel->ncols, sizeof(*types));
    int64_t *values = calloc(rel->ncols, sizeof(*values));
    CHECK(types && values);
    for (uint32_t c = 0; c < rel->ncols; ++c) types[c] = WIRELOG_TYPE_FLOAT;
    CHECK(!col_rel_set_column_types(rel, types, rel->ncols));
    free(types);
    for (uint32_t r = 0; r < original->nrows; ++r) {
        for (uint32_t c = 0; c < rel->ncols; ++c)
            values[c] = wl_columnar_float_to_bits((double)(r + c));
        CHECK(!col_rel_append_row(rel, values));
    }
    free(values); col_rel_destroy(original);
    return rel;
}

static void
consolidated_values(fixture_t *f, uint32_t rows, int64_t first, int64_t step,
    bool floating)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack->top == 1 && stack->items[0].owned &&
        !stack->items[0].is_delta);
    col_rel_t *rel = stack->items[0].rel;
    CHECK(rel->ncols == 1 && rel->nrows == rows && !rel->timestamps);
    CHECK(rel->column_types && rel->column_types[0] ==
        (floating ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64));
    CHECK(rel->sorted_nrows == rows && rel->run_count == (rows != 0));
    if (rows) CHECK(rel->run_ends[0] == rows);
    for (uint32_t row = 0; row < rows; ++row) {
        int64_t expected = first + row * step;
        if (floating)
            CHECK(wl_columnar_float_from_bits(rel->columns[0][row]) ==
                (double)expected);
        else CHECK(rel->columns[0][row] == expected);
    }
}

static void
test_generated_join_fragments(bool complete)
{
    for (unsigned shape = 0; shape < 3; ++shape)
        for (unsigned floating = 0; floating < 2; ++floating) {
            const char *type = floating ? "float" : "int64";
            const char *threshold = floating ? "2.0" : "2";
            char text[1024];
            if (!shape)
                snprintf(text, sizeof(text),
                    ".decl input(x: %s)\n.decl right(y: %s)\n"
                    ".decl output(z: %s)\n"
                    "output(x+y) :- input(x), right(y), x+y>%s.\n", type, type,
                    type, threshold);
            else if (shape == 1)
                snprintf(text, sizeof(text),
                    ".decl input(k: %s,x: %s)\n.decl right(k: %s,y: %s)\n"
                    ".decl output(z: %s)\n"
                    "output(x+y) :- input(k,x), right(k,y), x+y>%s.\n",
                    type, type, type, type, type, threshold);
            else
                snprintf(text, sizeof(text),
                    ".decl input(k: %s,x: %s)\n.decl right(k: %s,y: %s)\n"
                    ".decl third(y: %s,z: %s)\n.decl output(z: %s)\n"
                    "output(x+y+z) :- input(k,x), right(k,y), third(y,z), x+y+z>%s.\n",
                    type, type, type, type, type, type, type, threshold);
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(text, &error);
            wl_plan_t *plan = NULL;
            CHECK(program && !wl_plan_from_program(program, &plan));
            const wl_plan_relation_t *rp = NULL;
            for (uint32_t s = 0; s < plan->stratum_count; ++s)
                for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
                    if (!strcmp(plan->strata[s].relations[r].name, "output"))
                        rp = &plan->strata[s].relations[r];
            CHECK(rp && rp->ops[0].op == WL_PLAN_OP_VARIABLE);
            uint32_t end = 1, joins = 0, filters = 0, maps = 0;
            while (end < rp->op_count) {
                wl_plan_op_type_t kind = rp->ops[end].op;
                if (kind == WL_PLAN_OP_JOIN) {
                    ++joins;
                    CHECK((rp->ops[end].key_count != 0) == (shape != 0));
                } else if (kind == WL_PLAN_OP_FILTER) ++filters;
                else if (kind == WL_PLAN_OP_MAP) ++maps;
                else break;
                ++end;
            }
            CHECK(joins == (shape == 2 ? 2u : 1u) && filters && maps);
            CHECK(end + 1 == rp->op_count &&
                rp->ops[end].op == WL_PLAN_OP_CONSOLIDATE);
            if (complete) ++end;
            wl_plan_relation_t fragment = *rp;
            fragment.op_count = end;
            for (unsigned prebound = 0; prebound < 2; ++prebound)
                for (unsigned empty = 0; empty < 3; ++empty) {
                    fixture_t f;
                    uint32_t width = shape ? 2 : 1;
                    if (prebound) fixture_init(&f, width, empty == 1 ? 0 : 7,
                            0);
                    else source_fixture(&f, width, empty == 1 ? 0 : 7, 0);
                    CHECK(!wl_columnar_relation_rename_checked(f.input,
                        rp->ops[0].relation_name));
                    col_rel_t *right = source_new(width, empty == 2 ? 0 : 5);
                    col_rel_t *third = source_new(width, 3);
                    CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                    CHECK(!wl_columnar_relation_rename_checked(third, "third"));
                    if (floating) {
                        f.input = join_float(f.input);
                        right = join_float(right); third = join_float(third);
                        if (prebound)
                            wl_columnar_eval_stack_cleanup_stack(
                                f.frame)->items[0].rel = f.input;
                    }
                    col_rel_t *registry[] = {f.input, right, third};
                    if (!complete) {
                        CHECK(!col_rel_enable_timestamps(f.input));
                        for (uint32_t r = 0; r < f.input->nrows; ++r) {
                            f.input->timestamps[r].iteration = r;
                            f.input->timestamps[r].multiplicity = r %
                                2 ? -3 : 5;
                        }
                    }
                    if (prebound) {
                        f.session.rels = registry; f.session.nrels = 3;
                        fixture_begin(&f, &fragment, 1, end);
                    } else source_begin(&f, &fragment, registry, 3);
                    run(&f);
                    CHECK(!f.record.semantic_result &&
                        f.record.completed == end - 1);
                    CHECK(f.record.source_installed == !prebound);
                    join_raw_parity(&f, &fragment, prebound != 0);
                    if (complete) {
                        const uint32_t counts[] = {8, 4, 2};
                        const int64_t first[] = {3, 4, 4}, step[] = {1, 2, 3};
                        consolidated_values(&f, empty ? 0 : counts[shape],
                            first[shape], step[shape], floating != 0);
                    }
                    if (empty) CHECK(!wl_columnar_eval_stack_cleanup_stack(
                                f.frame)->items[0].rel->nrows);
                    fixture_close(&f); col_rel_destroy(right);
                    col_rel_destroy(third);
                }
            wl_plan_free(plan); wirelog_program_free(program);
        }
}

static void
join_until_ready(fixture_t *f, uint32_t cursor)
{
    unsigned turns = 0;
    while (f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_READY
        || f->record.cursor != cursor) {
        CHECK(++turns < 100000);
        CHECK(f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        engine_dispatch(f); ++f->witnesses;
    }
}

typedef struct {
    col_rel_t *identity, *contents;
    bool owned, is_delta;
} join_snapshot_t;

static join_snapshot_t
join_snapshot(fixture_t *f)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack->top == 1);
    eval_entry_t *entry = &stack->items[0];
    CHECK(entry->kind == WL_COLUMNAR_EVAL_ENTRY_RELATION
        && !entry->continuation && !entry->seg_boundaries && !entry->seg_count);
    join_snapshot_t snapshot = {.identity = entry->rel, .owned = entry->owned,
                                .is_delta = entry->is_delta};
    CHECK(!col_rel_deep_copy(entry->rel, &snapshot.contents, NULL));
    return snapshot;
}

static void
join_snapshot_check(fixture_t *f, const join_snapshot_t *snapshot)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack->top == 1);
    eval_entry_t *entry = &stack->items[0];
    CHECK(entry->rel == snapshot->identity && entry->owned == snapshot->owned
        && entry->is_delta == snapshot->is_delta);
    CHECK(entry->kind == WL_COLUMNAR_EVAL_ENTRY_RELATION
        && !entry->continuation && !entry->seg_boundaries && !entry->seg_count);
    same_join_bag(entry->rel, snapshot->contents);
}

static wl_columnar_memory_governor_ref_t *
join_governor(fixture_t *f, uint64_t budget)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = budget, .usable_bytes = budget,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV, .status = WL_COLUMNAR_MEMORY_OK
    };
    f->session.memory_governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->session.memory_governor);
    return f->session.memory_governor;
}

/* All mutable observers and fault setup are outside engine_dispatch. The
 * driver still invokes exactly one child action/consume per funded turn. */
static fixture_t *join_fault_fixture;
static uint32_t join_fault_cursor;
static unsigned join_fault_calls;
static bool join_fault_restart;

static void
join_collision_stop(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    fixture_t *f = join_fault_fixture;
    if (phase != 30 || f->record.cursor != join_fault_cursor ||
        join_fault_calls)
        return;
    ++join_fault_calls;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, &ctx,
        &f->record));
    CHECK(ctx->output && !ctx->output->nrows && control == f->operation);
    if (join_fault_restart) {
        CHECK(!wl_evaluation_control_finish(control, &f->operation_owner, 0,
            0));
        CHECK(!wl_evaluation_control_begin(control, &f->operation_owner));
    } else wl_evaluation_control_request_cancel(control);
}

static int64_t
join_collision(col_rel_t *rel)
{
    uint32_t bucket = wl_columnar_hash_value(2166136261u, rel, 0, 0) & 15u;
    int64_t value = 1;
    while ((wl_columnar_hash_value(2166136261u, rel, 0, value) & 15u) != bucket)
        ++value;
    CHECK(value && (wl_columnar_hash_value(2166136261u, rel, 0,
        value) & 15u) == bucket);
    return value;
}

static void
test_join_no_output_stops(void)
{
    const char *key = "col0";
    for (unsigned target = 0; target < 2; ++target)
        for (unsigned restart = 0; restart < 2; ++restart) {
            fixture_t f; fixture_init(&f, 1, 3, 0);
            int64_t collision = join_collision(f.input);
            for (uint32_t r = 0; r < f.input->nrows; ++r)
                f.input->columns[0][r] = collision;
            col_rel_t *right = source_new(1, 1), *third = source_new(1, 5);
            CHECK(!wl_columnar_relation_rename_checked(right, "right"));
            CHECK(!wl_columnar_relation_rename_checked(third, "third"));
            for (uint32_t r = 0; r < third->nrows;
                ++r) third->columns[0][r] = 0;
            col_rel_t *registry[] = {right, third};
            f.session.rels = registry; f.session.nrels = 2;
            wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_JOIN,
                                   .right_relation = "right",
                                   .delta_mode = WL_DELTA_FORCE_FULL},
                                  {.op = WL_PLAN_OP_JOIN,
                                   .right_relation = "third",
                                   .delta_mode = WL_DELTA_FORCE_FULL,
                                   .key_count = 1,
                                   .left_keys = &key, .right_keys = &key},
                                  {.op = WL_PLAN_OP_MAP, .project_count = 1}};
            if (!target) ops[0] = ops[1];
            wl_plan_relation_t plan = {.ops = ops, .op_count = 3};
            fixture_begin(&f, &plan, 0, 3); join_until_ready(&f, target);
            join_snapshot_t snapshot = join_snapshot(&f);
            join_fault_fixture = &f; join_fault_cursor = target;
            join_fault_calls = 0; join_fault_restart = restart != 0;
            wl_columnar_join_test_attempt_checkpoint = join_collision_stop;
            run(&f); wl_columnar_join_test_attempt_checkpoint = NULL;
            CHECK(join_fault_calls == 1 && f.record.completed == target
                && f.record.cursor == target);
            CHECK(f.record.semantic_result ==
                (restart ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(!f.session.memory_budget_denied);
            join_snapshot_check(&f, &snapshot);
            col_rel_destroy(snapshot.contents);
            uint64_t active =
                wl_evaluation_control_active_attempt_id(f.operation);
            int result = restart ? 0 : f.record.semantic_result;
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                result, 0) == result);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            CHECK(report.attempt_id == active);
            if (restart) CHECK(!report.consumed && active != f.operation_id);
            fixture_close(&f); col_rel_destroy(right); col_rel_destroy(third);
        }
}

/* Independent published noTS typed width2 "$join" construction schedule,
 * also used by the standalone collision test. No measured hook fee oracle. */
static uint64_t
join_constructor_two(void)
{
    return 512u + 2u * 128u + 192u + 6u * 96u
           + 6u * 96u + 96u + 2u * 96u + 3u * 96u + 128u
           + 2u * (96u + 96u + 192u + 3u * 96u)
           + 96u + 160u + 112u + 224u * ((sizeof(col_rel_t) + 7u) / 8u)
           + 5u * 96u + 112u + 224u + 80u + 112u + 96u + 2u * 160u
           + 112u + 192u + 2u * 112u
           + 112u + 192u + 2u * (96u + 96u + 192u + 112u + 224u)
           + 112u + 2u * 96u + 96u + 112u + 224u + 112u + 192u
           + 2u * (128u + 112u + 96u + 224u + 96u + 96u + 192u + 112u + 224u)
           + 3u * 128u + 220u;
}

static void
join_no_output_probe(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase != 30) return;
    fixture_t *f = join_fault_fixture;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(control == f->operation);
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, &ctx,
        &f->record));
    CHECK(ctx->output && !ctx->output->nrows);
    ++join_fault_calls;
}

static void
test_join_literal_limits(void)
{
#ifndef _MSC_VER
    /* Two opcode preflights256 + READY128, then the published standalone
     * width1 left3/right5 forced-full collision schedule H. */
    const uint64_t prefix = 384u + 28476u + join_constructor_two();
    /* First candidate128 + comparison parent192 + first key128 ends at7296.
     * Next candidate adds128, then its comparison parent192. */
    const uint64_t boundaries[] = {7296, 7424, 7616};
    const uint64_t predecessors[] = {7168, 7296, 7424};
    const char *key = "col0";
    for (unsigned b = 0; b < 3; ++b)
        for (unsigned minus = 0; minus < 2; ++minus) {
            fixture_t f; fixture_init(&f, 1, 3, prefix + boundaries[b] - minus);
            CHECK(!wl_columnar_relation_rename_checked(f.input, "left"));
            col_rel_t *right = source_new(1, 5);
            CHECK(!wl_columnar_relation_rename_checked(right, "right"));
            int64_t collision = join_collision(f.input);
            for (uint32_t r = 0; r < f.input->nrows;
                ++r) f.input->columns[0][r] = collision;
            for (uint32_t r = 0; r < right->nrows;
                ++r) right->columns[0][r] = 0;
            col_rel_t *registry[] = {right}; f.session.rels = registry;
            f.session.nrels = 1;
            wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_JOIN,
                                   .right_relation = "right",
                                   .delta_mode = WL_DELTA_FORCE_FULL,
                                   .key_count = 1,
                                   .left_keys = &key, .right_keys = &key},
                                  {.op = WL_PLAN_OP_MAP, .project_count = 1}};
            wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
            fixture_begin(&f, &plan, 0, 2);
            join_fault_fixture = &f; join_fault_calls = 0;
            wl_columnar_join_test_attempt_checkpoint = join_no_output_probe;
            run(&f); wl_columnar_join_test_attempt_checkpoint = NULL;
            CHECK((join_fault_calls != 0) == !(b == 0 && minus));
            CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT
                && !f.record.completed && !f.record.cursor &&
                !f.session.memory_budget_denied);
            CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
                f.input);
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                f.record.semantic_result, 0) == f.record.semantic_result);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            CHECK(report.attempt_id == f.operation_id &&
                report.consumed == prefix
                + (minus ? predecessors[b] : boundaries[b]));
            fixture_close(&f); col_rel_destroy(right);
        }
#endif
}

static void
test_join_later_failures(void)
{
    const char *key = "col0";
    for (unsigned target = 0; target < 2; ++target)
        for (unsigned fault = 0; fault < 4; ++fault) {
            fixture_t f; fixture_init(&f, 1, 2, 0);
            col_rel_t *right = source_new(1, 1), *third = source_new(1, 3);
            CHECK(!wl_columnar_relation_rename_checked(right, "right"));
            CHECK(!wl_columnar_relation_rename_checked(third, "third"));
            col_rel_t *registry[] = {right, third}; f.session.rels = registry;
            f.session.nrels = 2;
            wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_JOIN,
                                   .right_relation = "right",
                                   .delta_mode = WL_DELTA_FORCE_FULL},
                                  {.op = WL_PLAN_OP_JOIN,
                                   .right_relation = "third",
                                   .delta_mode = WL_DELTA_FORCE_FULL},
                                  {.op = WL_PLAN_OP_MAP, .project_count = 1}};
            if (fault == 0) {
                ops[target].key_count = 1; ops[target].left_keys = &key;
                ops[target].right_keys = &key;
            }
            if (fault == 2) ops[target].materialized = true;
            if (fault == 3) f.session.join_output_limit = target ? 3 : 1;
            wl_columnar_memory_governor_ref_t *governor = fault ==
                1 ? join_governor(&f, 1u << 20) : NULL;
            wl_plan_relation_t plan = {.ops = ops, .op_count = 3};
            fixture_begin(&f, &plan, 0, 3); join_until_ready(&f, target);
            join_snapshot_t snapshot = join_snapshot(&f);
            wl_columnar_memory_reservation_t peer;
            wl_columnar_memory_reservation_init(&peer);
            uint64_t baseline =
                governor ?
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                        governor)) : 0;
            if (governor) {
                CHECK(baseline + 1 < (1u << 20));
                CHECK(wl_columnar_memory_reserve(
                        wl_columnar_memory_governor_ref_get(governor),
                        (1u << 20) - baseline - 1, &peer));
            }
            if (fault == 0) wl_columnar_join_test_attempt_fail_allocation = 0;
            engine_dispatch(&f); ++f.witnesses;
            wl_columnar_join_test_attempt_fail_allocation = -1;
            run(&f);
            const int errors[] = {ENOMEM, ENOSPC, ENOTSUP, EOVERFLOW};
            CHECK(f.record.semantic_result == errors[fault]
                && f.record.completed == target && f.record.cursor == target);
            CHECK(f.session.memory_budget_denied == (fault == 1));
            join_snapshot_check(&f, &snapshot);
            col_rel_destroy(snapshot.contents);
            if (governor) CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)) ==
                    (1u << 20) - 1);
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                errors[fault], 0) == errors[fault]);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation,
                &report) && report.attempt_id == f.operation_id);
            fixture_close(&f);
            if (governor) {
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)) ==
                    peer.bytes);
                CHECK(wl_columnar_memory_rollback(&peer));
                CHECK(!wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)));
                wl_columnar_memory_governor_ref_release(governor);
            }
            col_rel_destroy(right); col_rel_destroy(third);
        }
    fixture_t f; fixture_init(&f, 1, 3, 0);
    wl_plan_op_t join = {.op = WL_PLAN_OP_JOIN, .right_relation = "unused"};
    wl_plan_relation_t plan = {.ops = &join, .op_count = 1};
    fixture_begin(&f, &plan, 0, 1);
    CHECK(!wl_evaluation_control_charge(f.operation, UINT64_MAX - 64));
    run(&f);
    CHECK(f.record.semantic_result == EOVERFLOW && !f.record.completed &&
        !f.session.memory_budget_denied);
    CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
        EOVERFLOW, 0) == EOVERFLOW);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation,
        &report) && report.consumed == UINT64_MAX - 64);
    fixture_close(&f);
}

/* A refused host grant never consumes a pending child result or a retirement
 * step. A grant384 admits exactly one existing FIFO engine action. */
static void
join_host_boundary(fixture_t *f, wl_columnar_filter_context_t *ctx)
{
    wl_columnar_eval_plan_numeric_phase_t phase = f->record.phase;
    CHECK(phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING ||
        phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD);
    wl_columnar_filter_stage_t stage = ctx->stage;
    uint32_t cursor = ctx->join_retirement_cursor;
    wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[cursor];
    uint64_t bytes = scratch->credit.bytes;
    wl_columnar_memory_governor_ref_t *ref = scratch->governor;
    join_snapshot_t snapshot = join_snapshot(f);
    for (unsigned allowance = 383; allowance <= 384; ++allowance) {
        int owner = 0;
        wl_evaluation_control_t *host = control_new(allowance, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(host);
        if (allowance == 384) {
            wl_evaluation_control_t *saved = f->funding;
            uint64_t saved_id = f->funding_id;
            f->funding = host; f->funding_id = id;
            engine_dispatch(f); ++f->witnesses;
            f->funding = saved; f->funding_id = saved_id;
        }
        wl_columnar_eval_plan_numeric_phase_t before = f->record.phase;
        wl_columnar_filter_stage_t before_stage = ctx->stage;
        int rc = before == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
            ? wl_columnar_eval_plan_numeric_consume_result(&f->record,
                f->pending_protocol, host, id)
            : wl_columnar_eval_plan_numeric_attempt_step(&f->record, host, id);
        CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
            f->record.phase == before && ctx->stage == before_stage);
        if (allowance == 383)
            CHECK(f->record.phase == phase && ctx->stage == stage
                && ctx->join_retirement_cursor == cursor && scratch->admitted
                && scratch->committed && scratch->ref_held &&
                scratch->governor == ref
                && !scratch->physical && !scratch->allocated &&
                scratch->credit.bytes == bytes);
        join_snapshot_check(f, &snapshot);
        CHECK(wl_evaluation_control_finish(host, &owner, 0,
            0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(host,
            &report) && report.consumed == (allowance == 383 ? 0u : 384u));
        CHECK(!wl_evaluation_control_detach(host, &owner));
        wl_evaluation_control_release(host);
    }
    col_rel_destroy(snapshot.contents);
}

static void
test_join_retained_successor(wl_plan_op_type_t kind)
{
    fixture_t f; fixture_init(&f, 1, 2, 0);
    col_rel_t *right = source_new(1, 1), *third = source_new(1, 3);
    CHECK(!wl_columnar_relation_rename_checked(right, "right"));
    CHECK(!wl_columnar_relation_rename_checked(third, "third"));
    col_rel_t *registry[] = {right, third}; f.session.rels = registry;
    f.session.nrels = 2;
    wl_columnar_memory_governor_ref_t *governor = join_governor(&f, 1u << 20);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
            governor), 128, &peer));
    const char *key = "col0";
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_JOIN, .right_relation = "right",
                           .delta_mode = WL_DELTA_FORCE_FULL},
                          {.op = kind, .right_relation = "third",
                           .delta_mode = WL_DELTA_FORCE_FULL,
                           .key_count = 1, .left_keys = &key,
                           .right_keys = &key},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t plan = {.ops = ops, .op_count = 3};
    fixture_begin(&f, &plan, 0, 3); join_until_ready(&f, 1);
    join_snapshot_t snapshot = join_snapshot(&f);
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &ctx,
        &f.record));
    unsigned turns = 0;
    while (f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT ||
        ctx->join_retirement_cursor != 0){
        CHECK(++turns < 10000 &&
            f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        engine_dispatch(&f); ++f.witnesses;
    }
    wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[0];
    CHECK(scratch->initialized && scratch->admitted && scratch->committed &&
        scratch->ref_held
        && scratch->governor == governor && !scratch->physical &&
        !scratch->allocated);
    uint64_t bytes = scratch->credit.bytes;
    uint64_t reserved =
        wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor));
    CHECK(bytes && reserved > bytes + peer.bytes && ctx->pin_owned);
    wl_columnar_memory_governor_test_refuse_next_release();
    engine_dispatch(&f); ++f.witnesses;
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
        && f.record.child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED
        && ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT
        && scratch->admitted && scratch->ref_held &&
        scratch->credit.bytes == bytes);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == reserved);
    join_snapshot_check(&f, &snapshot);
    join_host_boundary(&f, ctx); /* pending consume; one grant reaches CHILD */
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD &&
        ctx->pin_owned);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    uint64_t next = wl_evaluation_control_active_attempt_id(f.operation);
    CHECK(next != f.operation_id);
    join_host_boundary(&f, ctx); /* genuine CREDIT retry, then park PENDING */
    CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_REFERENCE &&
        !scratch->admitted
        && scratch->ref_held && scratch->governor == governor &&
        ctx->pin_owned);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == reserved - bytes);
    run(&f);
    CHECK(f.record.semantic_result == EINVAL && f.record.completed == 1 &&
        f.record.cursor == 1
        && !ctx->join_initialized && !ctx->pin_owned &&
        !f.session.memory_budget_denied);
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i)
        CHECK(!ctx->join_scratch[i].physical && !ctx->join_scratch[i].allocated
            && !ctx->join_scratch[i].admitted &&
            !ctx->join_scratch[i].ref_held);
    join_snapshot_check(&f, &snapshot); col_rel_destroy(snapshot.contents);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation,
        &report) && report.attempt_id == next && !report.consumed);
    col_rel_t *stable = f.input;
    if (kind != WL_PLAN_OP_JOIN) f.input = NULL;
    fixture_close(&f);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == peer.bytes);
    CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
    if (kind != WL_PLAN_OP_JOIN) {
        fixture_t retry; fixture_init(&retry, 1, 0, 0);
        col_rel_destroy(retry.input); retry.input = stable;
        wl_columnar_eval_stack_cleanup_stack(retry.frame)->items[0].rel =
            stable;
        col_rel_t *retry_registry[] = {stable, right, third};
        retry.session.rels = retry_registry; retry.session.nrels = 3;
        fixture_begin(&retry, &plan, 0, 3); run(&retry);
        CHECK(!retry.record.semantic_result && retry.record.completed == 3);
        join_raw_parity(&retry, &plan, true);
        col_rel_t *out =
            wl_columnar_eval_stack_cleanup_stack(retry.frame)->items[0].rel;
        CHECK(out->ncols == 1 &&
            out->nrows == (kind == WL_PLAN_OP_SEMIJOIN ? 2u : 0u));
        for (uint32_t r = 0; r < out->nrows;
            ++r) CHECK(out->columns[0][r] == r);
        fixture_close(&retry);
    }
    col_rel_destroy(right); col_rel_destroy(third);
}

static void
test_join_chained_delta(void)
{
    uint32_t projection[] = {0};
    for (unsigned projected = 0; projected < 4; ++projected) {
        fixture_t f; source_fixture(&f, 1, 7, 0);
        col_rel_t *delta = source_new(1, 2), *right = source_new(1, 3),
            *rd = source_new(1, 1);
        col_rel_t *third = source_new(1, 4), *td = source_new(1, 1);
        CHECK(!wl_columnar_relation_rename_checked(delta, "$d$input"));
        CHECK(!wl_columnar_relation_rename_checked(right, "right"));
        CHECK(!wl_columnar_relation_rename_checked(rd, "$d$right"));
        CHECK(!wl_columnar_relation_rename_checked(third, "third"));
        CHECK(!wl_columnar_relation_rename_checked(td, "$d$third"));
        col_rel_t *registry[] = {f.input, delta, right, rd, third, td};
        wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                               .relation_name = "input"},
                              {.op = WL_PLAN_OP_JOIN,
                               .right_relation = "right"},
                              {.op = WL_PLAN_OP_JOIN,
                               .right_relation = "third"}};
        for (unsigned j = 0; j < 2; ++j)
            if (projected & (1u << j)) {
                ops[j + 1].project_count = 1;
                ops[j + 1].project_indices = projection;
            }
        wl_plan_relation_t fragment = {.ops = ops, .op_count = 3};
        source_begin(&f, &fragment, registry, 6); join_until_ready(&f, 2);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        CHECK(f.record.completed == 1 && f.record.source_selected == delta);
        CHECK(stack->items[0].rel->nrows == 6 &&
            stack->items[0].is_delta == !(projected & 1));
        run(&f);
        CHECK(!f.record.semantic_result && f.record.completed == 2);
        CHECK(stack->items[0].rel->nrows == (projected & 1 ? 6u : 24u));
        CHECK(stack->items[0].is_delta == !(projected & 2));
        join_raw_parity(&f, &fragment, false);
        fixture_close(&f); col_rel_destroy(delta); col_rel_destroy(right);
        col_rel_destroy(rd);
        col_rel_destroy(third); col_rel_destroy(td);
    }
    /* Explicit selected-right boundaries: override a delta left, force full,
     * absent/empty delta with and without seed, ordinary empty, missing full. */
    const struct { unsigned mode; bool seeded; unsigned full_rows, delta_kind;
                   uint32_t rows; bool is_delta; } cases[] = {
        {WL_DELTA_FORCE_DELTA, false, 3, 2, 2, true},
        {WL_DELTA_FORCE_FULL, false, 3, 2, 6, true},
        {WL_DELTA_FORCE_DELTA, false, 3, 0, 6, true},
        {WL_DELTA_FORCE_DELTA, true, 3, 0, 0, false},
        {WL_DELTA_FORCE_DELTA, false, 3, 1, 6, true},
        {WL_DELTA_FORCE_DELTA, true, 3, 1, 0, false},
        {WL_DELTA_FORCE_FULL, true, 0, 0, 0, true},
        {WL_DELTA_AUTO, false, 3, 3, 0, false}
    };
    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        fixture_t f; source_fixture(&f, 1, 7, 0);
        f.session.delta_seeded = cases[i].seeded;
        col_rel_t *delta = source_new(1, 2),
            *right = source_new(1, cases[i].full_rows);
        col_rel_t *rd = source_new(1, cases[i].delta_kind == 2 ? 1 : 0),
            *third = source_new(1, 4);
        CHECK(!wl_columnar_relation_rename_checked(delta, "$d$input"));
        CHECK(!wl_columnar_relation_rename_checked(right, "right"));
        CHECK(!wl_columnar_relation_rename_checked(rd, "$d$right"));
        CHECK(!wl_columnar_relation_rename_checked(third, "third"));
        col_rel_t *registry[] = {f.input, delta,
                                 cases[i].delta_kind == 3 ? NULL : right,
                                 cases[i].delta_kind ? rd : NULL, third};
        wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                               .relation_name = "input"},
                              {.op = WL_PLAN_OP_JOIN, .right_relation = "right",
                               .delta_mode = cases[i].mode},
                              {.op = WL_PLAN_OP_JOIN, .right_relation = "third",
                               .delta_mode = WL_DELTA_FORCE_FULL}};
        wl_plan_relation_t fragment = {.ops = ops, .op_count = 3};
        source_begin(&f, &fragment, registry, 5); join_until_ready(&f, 2);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        CHECK(stack->items[0].rel->nrows == cases[i].rows &&
            stack->items[0].is_delta == cases[i].is_delta);
        run(&f); CHECK(!f.record.semantic_result && f.record.completed == 2);
        CHECK(stack->items[0].rel->nrows == 4 * cases[i].rows &&
            stack->items[0].is_delta == cases[i].is_delta);
        join_raw_parity(&f, &fragment, false); fixture_close(&f);
        col_rel_destroy(delta); col_rel_destroy(right); col_rel_destroy(rd);
        col_rel_destroy(third);
    }
}

static void
test_source_delta_modes(void)
{
    const unsigned modes[] = {WL_DELTA_AUTO, WL_DELTA_FORCE_FULL,
                              WL_DELTA_FORCE_DELTA, WL_DELTA_FORCE_EMPTY,
                              WL_DELTA_FORCE_EMPTY_AFTER_SEED, 99};
    const uint32_t delta_rows[] = {0, 3, 7, 11};
    for (unsigned seeded = 0; seeded < 2; ++seeded)
        for (unsigned m = 0; m < 6; ++m)
            for (unsigned d = 0; d < 5; ++d) {
                fixture_t f; source_fixture(&f, 1, 7, 0);
                f.session.delta_seeded = seeded != 0;
                col_rel_t *delta = source_new(1, d ? delta_rows[d - 1] : 0);
                CHECK(!wl_columnar_relation_rename_checked(delta, "$d$input"));
                col_rel_t *registry[] = {f.input, d ? delta : NULL};
                wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                                       .relation_name = "input",
                                       .delta_mode = modes[m]},
                                      {.op = WL_PLAN_OP_MAP,
                                       .project_count = 1}};
                wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
                source_begin(&f, &rp, registry, 2); run(&f);
                CHECK(!f.record.semantic_result);
                bool chosen_delta = d && delta->nrows
                    && (modes[m] == WL_DELTA_FORCE_DELTA
                    || (modes[m] != WL_DELTA_FORCE_FULL
                    && modes[m] != WL_DELTA_FORCE_EMPTY && delta->nrows < 7));
                CHECK(f.record.source_selected ==
                    (chosen_delta ? delta : f.input));
                bool empty = modes[m] == WL_DELTA_FORCE_EMPTY
                    || (modes[m] == WL_DELTA_FORCE_DELTA
                    && !chosen_delta && seeded);
                CHECK(f.record.source_empty == empty);
                source_raw_parity(&f, &rp); fixture_close(&f);
                col_rel_destroy(delta);
            }
    fixture_t f; source_fixture(&f, 0, 3, 0);
    col_rel_t *registry[] = {f.input};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
                          {.op = WL_PLAN_OP_MAP, .project_count = 0}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    source_begin(&f, &rp, registry, 1); run(&f);
    CHECK(!f.record.semantic_result); source_raw_parity(&f, &rp);
    fixture_close(&f);
}

static void
test_source_long_names(void)
{
    const unsigned lengths[] = {251, 252, 253, 511};
    for (unsigned n = 0; n < 4; ++n) {
        char name[513], other[513], delta_name[256];
        memset(name, 'a', lengths[n]); name[lengths[n]] = '\0';
        memcpy(other, name, lengths[n] + 1); other[lengths[n] - 1] = 'b';
        snprintf(delta_name, sizeof(delta_name), "$d$%.252s", name);
        fixture_t f; source_fixture(&f, 1, 7, 0);
        CHECK(!wl_columnar_relation_rename_checked(f.input, name));
        col_rel_t *miss = source_new(1, 7), *delta = source_new(1, 3);
        CHECK(!wl_columnar_relation_rename_checked(miss, other));
        CHECK(!wl_columnar_relation_rename_checked(delta, delta_name));
        col_rel_t *registry[] = {miss, NULL, f.input, delta};
        wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                               .relation_name = name},
                              {.op = WL_PLAN_OP_MAP, .project_count = 1}};
        wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
        source_begin(&f, &rp, registry, 4); run(&f);
        CHECK(!f.record.semantic_result && f.record.source_full == f.input
            && f.record.source_selected == delta);
        source_raw_parity(&f, &rp); fixture_close(&f);
        col_rel_destroy(miss); col_rel_destroy(delta);
    }
}

static void
test_source_scan_stops(void)
{
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                           .relation_name = "missing"},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    for (unsigned mode = 0; mode < 3; ++mode) {
        fixture_t f; source_fixture(&f, 1, 7, 0);
        col_rel_t *registry[128];
        for (unsigned i = 0; i < 128; ++i) registry[i] = NULL;
        registry[127] = f.input;
        source_begin(&f, &rp, registry, 128);
        until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_FULL);
        for (unsigned i = 0; i < 17; ++i) {
            engine_dispatch(&f); ++f.witnesses;
        }
        CHECK(f.record.source_slot == 17 && !f.record.source_installed);
        if (mode == 0) wl_evaluation_control_request_cancel(f.operation);
        else if (mode == 1) {
            CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner,
                0, 0));
            CHECK(!wl_evaluation_control_begin(f.operation,
                &f.operation_owner));
        }
        run(&f);
        CHECK(f.record.semantic_result == (mode == 0
            ? WL_EVALUATION_CONTROL_CANCELLED : mode == 1 ? EINVAL : ENOENT));
        CHECK(!f.record.completed && !f.record.source_installed);
        CHECK(!wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
        if (mode == 1) {
            uint64_t next =
                wl_evaluation_control_active_attempt_id(f.operation);
            CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner,
                0, 0));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            CHECK(report.attempt_id == next && !report.consumed);
        }
        fixture_close(&f);
    }
}

static void
test_source_owned_handoff(void)
{
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                           .delta_mode = WL_DELTA_FORCE_EMPTY},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    for (unsigned restart = 0; restart < 2; ++restart) {
        fixture_t f; source_fixture(&f, 17, 7, 0);
        col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
        until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL);
        eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(f.frame);
        col_rel_t *empty = result->rel;
        CHECK(empty && result->owned && !empty->nrows &&
            !f.record.source_initialized);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
        if (restart) {
            CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner,
                0, 0));
            CHECK(!wl_evaluation_control_begin(f.operation,
                &f.operation_owner));
        } else wl_evaluation_control_request_cancel(f.operation);
        run(&f);
        CHECK(f.record.semantic_result ==
            (restart ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(!f.record.source_installed && result->rel == empty &&
            result->owned);
        CHECK(!wl_columnar_eval_plan_numeric_attempt_ack(&f.record, f.funding,
            f.funding_id));
        CHECK(result->rel == empty && result->owned);
        /* Real reader makes generic disposal transfer the frame to pending;
         * ACK did not destroy the result or grant a successor work. */
        wl_columnar_source_access_reader_t reader = {0};
        CHECK(!col_rel_source_reader_acquire(empty, &reader));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
        CHECK(!f.frame && f.session.cleanup_pending_count == 1);
        CHECK(!col_rel_source_reader_release(&reader));
        CHECK(!wl_columnar_eval_stack_cleanup_retry(&f.session));
        CHECK(!f.session.cleanup_pending_count &&
            !f.session.cleanup_reserved_bytes);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            restart ? 0 : WL_EVALUATION_CONTROL_CANCELLED, 0) ==
            (restart ? 0 : WL_EVALUATION_CONTROL_CANCELLED));
        if (restart) {
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation,
                &report) && !report.consumed);
        }
        CHECK(!wl_evaluation_control_finish(f.funding, &f.funding_owner, 0, 0));
        CHECK(!wl_evaluation_control_detach(f.operation, &f.operation_owner));
        CHECK(!wl_evaluation_control_detach(f.funding, &f.funding_owner));
        wl_evaluation_control_release(f.operation);
        wl_evaluation_control_release(f.funding);
        col_rel_destroy(f.input);
    }
}

static void
test_source_allocation_failures(void)
{
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                           .delta_mode = WL_DELTA_FORCE_EMPTY},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    bool completed = false;
    for (unsigned ordinal = 1; ordinal < 128; ++ordinal) {
        fixture_t f; source_fixture(&f, 1, 7, 0);
        col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
        until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW);
        wl_columnar_relation_test_constructor_fail_allocation = ordinal;
        engine_dispatch(&f); ++f.witnesses;
        int result = f.pending_protocol;
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        run(&f);
        CHECK(!f.session.memory_budget_denied);
        if (result) {
            CHECK(result == ENOMEM && f.record.semantic_result == ENOMEM);
            CHECK(!f.record.source_installed);
            CHECK(!wl_columnar_eval_stack_cleanup_result(f.frame)->rel);
        } else {
            CHECK(!f.record.semantic_result); completed = true;
        }
        fixture_close(&f);
        if (completed) break;
    }
    CHECK(completed);
}

static void
test_source_authority(void)
{
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                           .delta_mode = WL_DELTA_FORCE_EMPTY},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    fixture_t f; source_fixture(&f, 1, 7, 0);
    col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
    wl_columnar_eval_plan_numeric_attempt_t copy = f.record, other;
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&copy, f.funding,
        f.funding_id) == EINVAL);
    wl_thread_t thread; CHECK(!wl_thread_create(&thread, wrong_thread, &f));
    CHECK(!wl_thread_join(&thread));
    wl_columnar_eval_plan_numeric_attempt_init(&other);
    CHECK(wl_columnar_eval_plan_numeric_source_attempt_begin(&other, &rp, 0, 2,
        f.frame, &f.session, f.operation, f.operation_id, f.funding,
        f.funding_id) == EBUSY);
    CHECK(!other.identity && !other.control);
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f.session, &nested));
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f.record, f.funding,
        f.funding_id) == EBUSY);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SLOT);
    CHECK(!wl_columnar_eval_stack_numeric_source_output(f.frame, &f.session,
        &f.record));
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(f.frame);
    CHECK(result->owned && !result->rel);
    CHECK(wl_columnar_eval_stack_numeric_source_output(f.frame, &f.session,
        &f.record) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_attempt_ack(&f.record, f.funding,
        f.funding_id) == EBUSY);
    f.record.phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW;
    until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL);
    col_rel_t *empty = result->rel;
    CHECK(empty && result->owned);
    result->seg_count = 1;
    CHECK(wl_columnar_eval_stack_numeric_source_install(f.frame, &f.session,
        &f.record) == EBUSY);
    CHECK(result->rel == empty && !f.record.source_installed);
    result->seg_count = 0;
    CHECK(!wl_columnar_eval_stack_numeric_source_install(f.frame, &f.session,
        &f.record));
    CHECK(!result->rel && !result->owned && !result->is_delta
        && !result->seg_count && !result->seg_boundaries &&
        !result->continuation
        && result->kind == WL_COLUMNAR_EVAL_ENTRY_RELATION);
    CHECK(wl_columnar_eval_stack_numeric_source_install(f.frame, &f.session,
        &f.record) == EBUSY);
    f.record.cursor = f.record.begin;
    f.record.phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_PREFLIGHT;
    run(&f); CHECK(!f.record.semantic_result); fixture_close(&f);
}

static void
test_source_unsupported(void)
{
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    for (unsigned domain = 0; domain < 7; ++domain) {
        fixture_t f; source_fixture(&f, 1, 7, 0);
        if (domain == 0) f.session.current_iteration = 1;
        if (domain == 1) f.session.retraction_seeded = true;
        if (domain == 2) f.session.retraction_right_pass = true;
        if (domain == 3) f.session.tdd_input_run = (void *)&f;
        if (domain == 4) f.session.tdd_subpass_active = true;
        if (domain == 5) f.session.tdd_outbound_only_active = true;
        if (domain == 6) f.session.diff_operators_active = true;
        CHECK(wl_columnar_eval_plan_numeric_source_attempt_begin(&f.record,
            &rp, 0, 2, f.frame, &f.session, f.operation, f.operation_id,
            f.funding, f.funding_id) == ENOTSUP);
        CHECK(!f.record.identity && !f.record.control);
        CHECK(!wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
        f.session.current_iteration = 0; f.session.retraction_seeded = false;
        f.session.retraction_right_pass = false; f.session.tdd_input_run = NULL;
        f.session.tdd_subpass_active = false;
        f.session.tdd_outbound_only_active = false;
        f.session.diff_operators_active = false;
        col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
        run(&f); CHECK(!f.record.semantic_result); fixture_close(&f);
    }
    const wl_plan_op_type_t unsupported_kinds[] = {WL_PLAN_OP_CONCAT,
                                                   WL_PLAN_OP_LFTJ,
                                                   WL_PLAN_OP_K_FUSION};
    for (unsigned kind = 0; kind < 3; ++kind)
        for (unsigned bad = 0; bad < 3; ++bad) {
            wl_plan_op_t invalid[] = {ops[0], ops[1], ops[1]};
            invalid[bad].op = unsupported_kinds[kind];
            wl_plan_relation_t unsupported = {.ops = invalid, .op_count = 3};
            fixture_t f; source_fixture(&f, 1, 7, 0);
            col_rel_t *registry[] = {f.input};
            source_begin(&f, &unsupported, registry, 1);
            run(&f); CHECK(f.record.semantic_result == ENOTSUP);
            CHECK(!f.record.source_slot && !f.record.source_byte
                && !f.record.source_installed && !f.session.rel_hash_nbuckets);
            fixture_close(&f);
        }
    for (unsigned missing = 0; missing < 2; ++missing) {
        fixture_t f; source_fixture(&f, 1, 7, 0);
        ops[0].relation_name = missing ? NULL : "input";
        source_begin(&f, &rp, NULL, 0); run(&f);
        CHECK(f.record.semantic_result == ENOENT && !f.record.source_installed);
        fixture_close(&f);
    }
}

static bool source_rejected_checkpoint;
static void
source_cancel_rejected(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (phase == 12 && progress == 37) {
        source_rejected_checkpoint = true;
        wl_evaluation_control_request_cancel(control);
    }
}

static void
test_source_no_output(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl input(x: int64)\n.decl output(x: int64)\n"
        "output(x) :- input(x), x < 0.\n", &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "output"))
                rp = &plan->strata[s].relations[r];
    CHECK(rp && rp->ops[0].op == WL_PLAN_OP_VARIABLE);
    uint32_t end = 1;
    while (end < rp->op_count && (rp->ops[end].op == WL_PLAN_OP_FILTER
        || rp->ops[end].op == WL_PLAN_OP_MAP)) ++end;
    wl_plan_relation_t fragment = {.ops = rp->ops, .op_count = end};
    for (unsigned cancel = 0; cancel < 2; ++cancel) {
        fixture_t f; source_fixture(&f, 1, 133, 0);
        col_rel_t *registry[] = {f.input};
        source_begin(&f, &fragment, registry, 1);
        source_rejected_checkpoint = false;
        if (cancel) wl_columnar_filter_test_attempt_checkpoint =
                source_cancel_rejected;
        run(&f); wl_columnar_filter_test_attempt_checkpoint = NULL;
        if (cancel) {
            CHECK(source_rejected_checkpoint && f.record.semantic_result ==
                WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(!f.record.completed && !f.record.child_status.committed);
            CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
                f.input);
        } else {
            CHECK(!f.record.semantic_result);
            CHECK(!wl_columnar_eval_stack_cleanup_stack(
                    f.frame)->items[0].rel->nrows);
            source_raw_parity(&f, &fragment);
        }
        fixture_close(&f);
    }
    /* A generated FILTER-only tail preserves the raw empty-variable TS
     * policy (empty inputs drop TS even when the template had timestamps). */
    wl_plan_op_t empty_ops[2] = {rp->ops[0], rp->ops[1]};
    CHECK(empty_ops[1].op == WL_PLAN_OP_FILTER);
    empty_ops[0].delta_mode = WL_DELTA_FORCE_EMPTY;
    wl_plan_relation_t empty = {.ops = empty_ops, .op_count = 2};
    fixture_t f; source_fixture(&f, 2, 7, 0);
    f.input->has_graph_column = true;
    f.input->graph_col_idx = 1; f.input->declared_ncols = 1;
    CHECK(!col_rel_enable_timestamps(f.input));
    col_rel_t *registry[] = {f.input}; source_begin(&f, &empty, registry, 1);
    run(&f); CHECK(!f.record.semantic_result);
    CHECK(!wl_columnar_eval_stack_cleanup_stack(
            f.frame)->items[0].rel->timestamps);
    source_raw_parity(&f, &empty); fixture_close(&f);
    wl_plan_free(plan); wirelog_program_free(program);
}

static void
test_source_denial(void)
{
    fixture_t f; source_fixture(&f, 1, 7, 0);
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                           .delta_mode = WL_DELTA_FORCE_EMPTY},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1, .usable_bytes = 1,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV, .status = WL_COLUMNAR_MEMORY_OK
    };
    f.session.memory_governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.session.memory_governor);
    col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
    run(&f);
    CHECK(f.record.semantic_result == ENOSPC && f.session.memory_budget_denied);
    CHECK(!f.record.source_installed &&
        !wl_columnar_eval_stack_cleanup_result(f.frame)->rel);
    wl_columnar_memory_governor_ref_t *governor = f.session.memory_governor;
    fixture_close(&f);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

static void
test_source_pending_funding(void)
{
    fixture_t f; source_fixture(&f, 1, 7, 0);
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                           .delta_mode = WL_DELTA_FORCE_EMPTY},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
    until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW);
    engine_dispatch(&f); ++f.witnesses;
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING);
    col_rel_t *empty = wl_columnar_eval_stack_cleanup_result(f.frame)->rel;
    CHECK(empty && f.record.source_initialized);
    int poor_owner = 0;
    wl_evaluation_control_t *poor = control_new(383, &poor_owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(poor);
    CHECK(wl_columnar_eval_plan_numeric_consume_result(&f.record,
        f.pending_protocol, poor, id) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
        && wl_columnar_eval_stack_cleanup_result(f.frame)->rel == empty
        && f.record.source_initialized);
    CHECK(wl_evaluation_control_finish(poor, &poor_owner, 0, 0)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(poor, &poor_owner));
    wl_evaluation_control_release(poor);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    run(&f);
    CHECK(f.record.semantic_result == EINVAL && !f.record.source_initialized);
    CHECK(!f.record.source_installed &&
        wl_columnar_eval_stack_cleanup_result(f.frame)->rel == empty);
    uint64_t next = wl_evaluation_control_active_attempt_id(f.operation);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report));
    CHECK(report.attempt_id == next && !report.consumed);
    fixture_close(&f);
    CHECK(!wl_columnar_relation_scalar_source_end(&f.record.source, EINVAL));
}

static void
test_source_work_boundaries(void)
{
    /* Independently stated interface tariffs: two source opcode admissions
     * of128, then128 for each name byte. Limits383/384/385 bracket the first
     * actual name byte; no scalar-source/constructor admission is reached. */
    const uint64_t limits[] = {1, 127, 128, 255, 256, 383, 384, 385};
    const uint64_t expected[] = {0, 0, 128, 128, 256, 256, 384, 384};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    for (unsigned i = 0; i < 8; ++i) {
        fixture_t f; source_fixture(&f, 1, 7, limits[i]);
        col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
        run(&f);
        CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!f.record.source_initialized && !f.record.source_installed);
        CHECK(f.record.source_byte == (limits[i] >= 384 ? 1 : 0));
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            f.record.semantic_result, 0) == f.record.semantic_result);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.attempt_id == f.operation_id &&
            report.consumed == expected[i]);
        fixture_close(&f);
    }
    fixture_t f; source_fixture(&f, 1, 7, 0);
    CHECK(!wl_evaluation_control_charge(f.operation, UINT64_MAX - 64));
    col_rel_t *registry[] = {f.input}; source_begin(&f, &rp, registry, 1);
    run(&f);
    CHECK(f.record.semantic_result == EOVERFLOW && !f.record.source_installed);
    CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
        EOVERFLOW, 0) == EOVERFLOW);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report));
    CHECK(report.consumed == UINT64_MAX - 64 && !report.stop_reason);
    fixture_close(&f);
}

static void
test_source_host_boundaries(void)
{
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
                          {.op = WL_PLAN_OP_MAP, .project_count = 1}};
    wl_plan_relation_t rp = {.ops = ops, .op_count = 2};
    for (uint64_t limit = 383; limit <= 384; ++limit) {
        fixture_t f; source_fixture(&f, 1, 7, 0);
        col_rel_t *registry[] = {f.input};
        f.session.rels = registry; f.session.nrels = 1; f.session.rel_cap = 1;
        int owner = 0;
        wl_evaluation_control_t *host = control_new(limit, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(host);
        int rc = wl_columnar_eval_plan_numeric_source_attempt_begin(&f.record,
                &rp, 0, 2, f.frame, &f.session, f.operation, f.operation_id,
                host, id);
        CHECK(rc == (limit == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        if (limit == 383) {
            CHECK(!f.record.identity && !f.record.control);
            source_begin(&f, &rp, registry, 1);
        } else {
            CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f.record, host,
                id) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_PREFLIGHT);
        }
        CHECK(wl_evaluation_control_finish(host, &owner, 0, 0)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(host, &report));
        CHECK(report.consumed == (limit == 383 ? 0 : 384));
        CHECK(!wl_evaluation_control_detach(host, &owner));
        wl_evaluation_control_release(host);
        run(&f); CHECK(!f.record.semantic_result); fixture_close(&f);
    }
}

static void
test_complete_unary_plans(void)
{
    const uint32_t row_counts[] = {0, 1, 33};
    for (unsigned floating = 0; floating < 2; ++floating) {
        const char *text = floating
            ? ".decl input(x: float)\n.decl output(y: float)\n"
            "output(1.0) :- input(x), x >= 0.0.\n"
            : ".decl input(x: int64)\n.decl output(y: int64)\n"
            "output(1) :- input(x), x >= 0.\n";
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        const wl_plan_relation_t *rp = NULL;
        for (uint32_t s = 0; s < plan->stratum_count; ++s)
            for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
                if (!strcmp(plan->strata[s].relations[r].name, "output"))
                    rp = &plan->strata[s].relations[r];
        CHECK(rp && rp->op_count >= 4 && rp->ops[0].op == WL_PLAN_OP_VARIABLE
            && rp->ops[rp->op_count - 1].op == WL_PLAN_OP_CONSOLIDATE);
        unsigned filters = 0, maps = 0;
        for (uint32_t i = 1; i + 1 < rp->op_count; ++i) {
            filters += rp->ops[i].op == WL_PLAN_OP_FILTER;
            maps += rp->ops[i].op == WL_PLAN_OP_MAP;
        }
        CHECK(filters && maps);
        for (unsigned prebound = 0; prebound < 2; ++prebound)
            for (unsigned n = 0; n < 3; ++n)
                for (unsigned pattern = 0; pattern < 3; ++pattern) {
                    fixture_t f;
                    uint32_t rows = row_counts[n];
                    if (prebound) fixture_init(&f, 1, rows, 0);
                    else source_fixture(&f, 1, rows, 0);
                    CHECK(!wl_columnar_relation_rename_checked(f.input,
                        rp->ops[0].relation_name));
                    if (floating) {
                        f.input = join_float(f.input);
                        if (prebound) wl_columnar_eval_stack_cleanup_stack(
                                f.frame)->items[0].rel = f.input;
                    }
                    for (uint32_t row = 0; row < rows; ++row) {
                        int64_t value = pattern == 0 ? rows - row - 1
                            : pattern == 1 ? row % 7 : 0;
                        f.input->columns[0][row] = floating
                            ? wl_columnar_float_to_bits(value ? (double)value
                                : row % 2 ? -0.0 : 0.0) : value;
                    }
                    col_rel_t *registry[] = {f.input};
                    if (prebound) {
                        f.session.rels = registry; f.session.nrels = 1;
                        fixture_begin(&f, rp, 1, rp->op_count);
                    } else source_begin(&f, rp, registry, 1);
                    run(&f);
                    CHECK(!f.record.semantic_result &&
                        f.record.completed == rp->op_count - 1);
                    consolidated_values(&f, rows != 0, 1, 0, floating != 0);
                    join_raw_parity(&f, rp, prebound != 0);
                    fixture_close(&f);
                }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

static void
test_consolidate_composition(void)
{
    const uint32_t widths[] = {0, 1, 17, 129};
    for (unsigned w = 0; w < 4; ++w) {
        fixture_t f; fixture_init(&f, widths[w], 33, 0);
        for (uint32_t c = 0; c < widths[w]; ++c)
            for (uint32_t row = 0; row < 33; ++row)
                f.input->columns[c][row] = row % 7 + c;
        col_rel_t *registry[] = {f.input};
        f.session.rels = registry; f.session.nrels = 1;
        wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_CONSOLIDATE},
                              {.op = WL_PLAN_OP_MAP,
                               .project_count = widths[w]},
                              {.op = WL_PLAN_OP_CONSOLIDATE}};
        wl_plan_relation_t plan = {.ops = ops, .op_count = 3};
        fixture_begin(&f, &plan, 0, 3); run(&f);
        CHECK(!f.record.semantic_result && f.record.completed == 3);
        col_rel_t *result = wl_columnar_eval_stack_cleanup_stack(
            f.frame)->items[0].rel;
        uint32_t count = widths[w] ? 7 : 1;
        CHECK(result->ncols == widths[w] && result->nrows == count
            && result->sorted_nrows == count && result->run_count == 1
            && result->run_ends[0] == count);
        for (uint32_t c = 0; c < widths[w]; ++c)
            for (uint32_t row = 0; row < count; ++row)
                CHECK(result->columns[c][row] == row + c);
        join_raw_parity(&f, &plan, true); fixture_close(&f);
    }
}

static void
test_consolidate_literal_limits(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_CONSOLIDATE};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    /* Independent admission schedule, no runtime hook totals: preflight128,
     * READY128, ENTRY224, ten lifecycle192, INIT192. Refused admissions do not
     * debit; partial bootstrap cleanup does not charge the next attempt. */
    const uint64_t limits[] = {1, 127, 128, 255, 256, 479, 480, 671, 672,
                               2399, 2400, 2591, 2592};
    const uint64_t consumed[] = {0, 0, 128, 128, 256, 256, 480, 480, 672,
                                 2208, 2400, 2400, 2592};
    for (unsigned repeat = 0; repeat < 2; ++repeat)
        for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
            fixture_t f; fixture_init(&f, 1, 3, limits[i]);
            fixture_begin(&f, &plan, 0, 1); run(&f);
            CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT
                && !f.record.completed);
            CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel
                == f.input && !f.session.memory_budget_denied);
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                f.record.semantic_result, 0) == f.record.semantic_result);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report)
                && report.attempt_id == f.operation_id
                && report.consumed == consumed[i]);
            fixture_close(&f);
        }
}

static fixture_t *consolidate_stop_fixture;
static unsigned consolidate_stop_phase;
static bool consolidate_stop_hit, consolidate_stop_restart;

static void
consolidate_stop(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (consolidate_stop_hit || phase != consolidate_stop_phase
        || progress != (phase == 206 ? 1u : phase == 207 ? 7u : 0u)) return;
    fixture_t *f = consolidate_stop_fixture;
    CHECK(control == f->operation && f->record.cursor + 1 == f->record.end
        && f->record.completed == f->record.cursor - f->record.begin);
    consolidate_stop_hit = true;
    if (consolidate_stop_restart) {
        CHECK(!wl_evaluation_control_finish(control, &f->operation_owner, 0,
            0));
        CHECK(!wl_evaluation_control_begin(control, &f->operation_owner));
    } else wl_evaluation_control_request_cancel(control);
}

static void
test_consolidate_stops(void)
{
    /* Phase207 position7 has emitted just one tuple on this all-equal input.
     * Finite scan, comparisons and merge/index work need no output to poll. */
    const unsigned phases[] = {200, 201, 202, 203, 204, 205, 206, 207};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_MAP, .project_count = 1},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    for (unsigned restart = 0; restart < 2; ++restart)
        for (unsigned k = 0; k < sizeof(phases) / sizeof(phases[0]); ++k) {
            fixture_t f; fixture_init(&f, 1, 19, 0);
            for (uint32_t row = 0; row < 19; ++row)
                f.input->columns[0][row] = 0;
            fixture_begin(&f, &plan, 0, 2); join_until_ready(&f, 1);
            join_snapshot_t before = join_snapshot(&f);
            CHECK(before.owned && before.identity != f.input);
            consolidate_stop_fixture = &f; consolidate_stop_phase = phases[k];
            consolidate_stop_restart = restart != 0;
            consolidate_stop_hit = false;
            wl_columnar_filter_test_attempt_checkpoint = consolidate_stop;
            run(&f); wl_columnar_filter_test_attempt_checkpoint = NULL;
            consolidate_stop_fixture = NULL;
            CHECK(consolidate_stop_hit && f.record.completed == 1
                && f.record.cursor == 1 && f.record.semantic_result ==
                (restart ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED)
                && !f.session.memory_budget_denied);
            join_snapshot_check(&f, &before); col_rel_destroy(before.contents);
            if (restart) {
                uint64_t next =
                    wl_evaluation_control_active_attempt_id(f.operation);
                CHECK(next != f.operation_id);
                CHECK(!wl_evaluation_control_finish(f.operation,
                    &f.operation_owner, 0, 0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.operation, &report)
                    && report.attempt_id == next && !report.consumed);
            }
            fixture_close(&f);
        }
}

static void
test_complete_join_consolidate_stops(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl input(x: int64)\n.decl right(y: int64)\n"
        ".decl output(z: int64)\n"
        "output(x) :- input(x), right(y), y >= 0.\n", &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "output"))
                rp = &plan->strata[s].relations[r];
    CHECK(rp && rp->ops[0].op == WL_PLAN_OP_VARIABLE
        && rp->ops[rp->op_count - 1].op == WL_PLAN_OP_CONSOLIDATE);
    bool joined = false;
    for (uint32_t i = 1; i + 1 < rp->op_count; ++i)
        joined = joined || rp->ops[i].op == WL_PLAN_OP_JOIN;
    CHECK(joined);
    for (unsigned restart = 0; restart < 2; ++restart)
        for (unsigned duplicate = 0; duplicate < 2; ++duplicate) {
            fixture_t f; source_fixture(&f, 1, 19, 0);
            for (uint32_t row = 0; row < 19; ++row)
                f.input->columns[0][row] = 0;
            CHECK(!wl_columnar_relation_rename_checked(f.input,
                rp->ops[0].relation_name));
            col_rel_t *right = source_new(1, 3);
            CHECK(!wl_columnar_relation_rename_checked(right, "right"));
            col_rel_t *registry[] = {f.input, right};
            source_begin(&f, rp, registry, 2);
            join_until_ready(&f, rp->op_count - 1);
            join_snapshot_t before = join_snapshot(&f);
            CHECK(before.owned && before.identity->nrows == 57);
            consolidate_stop_fixture = &f;
            consolidate_stop_phase = duplicate ? 207 : 200;
            consolidate_stop_restart = restart != 0;
            consolidate_stop_hit = false;
            wl_columnar_filter_test_attempt_checkpoint = consolidate_stop;
            run(&f); wl_columnar_filter_test_attempt_checkpoint = NULL;
            consolidate_stop_fixture = NULL;
            CHECK(consolidate_stop_hit && f.record.completed == rp->op_count - 2
                && f.record.cursor == rp->op_count - 1
                && f.record.semantic_result == (restart ? EINVAL
                : WL_EVALUATION_CONTROL_CANCELLED));
            join_snapshot_check(&f, &before); col_rel_destroy(before.contents);
            if (restart) {
                uint64_t next =
                    wl_evaluation_control_active_attempt_id(f.operation);
                CHECK(!wl_evaluation_control_finish(f.operation,
                    &f.operation_owner, 0, 0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.operation, &report)
                    && report.attempt_id == next && !report.consumed);
            }
            fixture_close(&f); col_rel_destroy(right);
        }
    wl_plan_free(plan); wirelog_program_free(program);
}

/* Exact384 funds one real engine action. A refused383 grant or a second action
 * against the spent384 grant leaves the original pending/retirement unchanged. */
static void
consolidate_host_boundary(fixture_t *f, wl_columnar_filter_context_t *ctx)
{
    for (unsigned allowance = 383; allowance <= 384; ++allowance) {
        int owner = 0;
        wl_evaluation_control_t *host = control_new(allowance, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(host);
        if (allowance == 384) {
            wl_evaluation_control_t *saved = f->funding;
            uint64_t saved_id = f->funding_id;
            f->funding = host; f->funding_id = id;
            engine_dispatch(f); ++f->witnesses;
            f->funding = saved; f->funding_id = saved_id;
        }
        wl_columnar_eval_plan_numeric_phase_t phase = f->record.phase;
        wl_columnar_filter_stage_t stage = ctx->stage;
        uint64_t bytes = ctx->credit.bytes;
        bool admitted = ctx->admitted, held = ctx->ref_held;
        int result = f->record.semantic_result;
        int rc = phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
            ? wl_columnar_eval_plan_numeric_consume_result(&f->record,
                f->pending_protocol, host, id)
            : wl_columnar_eval_plan_numeric_attempt_step(&f->record, host, id);
        CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
            && f->record.phase == phase && ctx->stage == stage
            && ctx->credit.bytes == bytes && ctx->admitted == admitted
            && ctx->ref_held == held && f->record.semantic_result == result);
        CHECK(wl_evaluation_control_finish(host, &owner, 0, 0)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(host, &report)
            && report.consumed == (allowance == 383 ? 0u : 384u));
        CHECK(!wl_evaluation_control_detach(host, &owner));
        wl_evaluation_control_release(host);
    }
}

static void
test_consolidate_retained_successor(void)
{
    fixture_t f; fixture_init(&f, 1, 19, 0);
    for (uint32_t row = 0; row < 19; ++row) f.input->columns[0][row] = row % 7;
    wl_columnar_memory_governor_ref_t *governor = join_governor(&f, 1u << 20);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
            governor), 128, &peer));
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_MAP, .project_count = 1},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    fixture_begin(&f, &plan, 0, 2); join_until_ready(&f, 1);
    join_snapshot_t before = join_snapshot(&f);
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &ctx,
        &f.record));
    unsigned turns = 0;
    while (f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_CREDIT) {
        CHECK(++turns < 10000 &&
            f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        engine_dispatch(&f); ++f.witnesses;
    }
    CHECK(ctx->admitted && ctx->credit_committed && ctx->ref_held
        && ctx->governor == governor && !ctx->heap
        && ctx->values == ctx->inline_values && ctx->pin_owned);
    CHECK(ctx->output && ctx->output->nrows == 7
        && ctx->output->sorted_nrows == 7);
    uint64_t bytes = ctx->credit.bytes;
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(governor));
    CHECK(bytes && reserved > bytes + peer.bytes);
    wl_columnar_memory_governor_test_refuse_next_release();
    engine_dispatch(&f); ++f.witnesses;
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
        && f.record.child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED
        && ctx->stage == WL_COLUMNAR_FILTER_STAGE_CREDIT
        && ctx->admitted && ctx->credit_committed && ctx->ref_held
        && ctx->credit.bytes == bytes);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == reserved);
    join_snapshot_check(&f, &before);
    consolidate_host_boundary(&f, ctx);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    uint64_t next = wl_evaluation_control_active_attempt_id(f.operation);
    consolidate_host_boundary(&f, ctx);
    CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_REFERENCE && !ctx->admitted
        && ctx->ref_held && ctx->pin_owned);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == reserved - bytes);
    run(&f);
    CHECK(f.record.semantic_result == EINVAL && f.record.completed == 1
        && !ctx->pin_owned && !ctx->source_initialized
        && !ctx->batch_initialized && !f.session.memory_budget_denied);
    join_snapshot_check(&f, &before); col_rel_destroy(before.contents);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report)
        && report.attempt_id == next && !report.consumed);
    /* Keep the exact original borrowed source, then recover on a fresh record
     * after old ACK/cleanup. Peer credit is unrelated and survives disposal. */
    col_rel_t *stable = f.input; f.input = NULL; fixture_close(&f);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == peer.bytes);
    CHECK(wl_columnar_memory_rollback(&peer));
    fixture_t retry; fixture_init(&retry, 1, 0, 0);
    col_rel_destroy(retry.input); retry.input = stable;
    wl_columnar_eval_stack_cleanup_stack(retry.frame)->items[0].rel = stable;
    retry.session.memory_governor = governor;
    col_rel_t *registry[] = {stable}; retry.session.rels = registry;
    retry.session.nrels = 1;
    fixture_begin(&retry, &plan, 0, 2); run(&retry);
    CHECK(!retry.record.semantic_result && retry.record.completed == 2);
    consolidated_values(&retry, 7, 0, 1, false);
    join_raw_parity(&retry, &plan, true); fixture_close(&retry);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

static void
test_consolidate_retained_input(void)
{
    fixture_t f; fixture_init(&f, 1, 19, 0);
    for (uint32_t row = 0; row < 19; ++row) f.input->columns[0][row] = row % 7;
    col_rel_t *registry[] = {f.input}; f.session.rels = registry;
    f.session.nrels = 1;
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_MAP, .project_count = 1},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    fixture_begin(&f, &plan, 0, 2); join_until_ready(&f, 1);
    col_rel_t *held =
        wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
    CHECK(held != f.input && held->nrows == 19);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(held, &reader));
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &ctx,
        &f.record));
    unsigned turns = 0;
    while (f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || f.record.child_status.progress !=
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_FINISH) {
        CHECK(++turns < 10000 &&
            f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        engine_dispatch(&f); ++f.witnesses;
    }
    CHECK(f.record.child_status.committed && held->nrows == 19);
    consolidated_values(&f, 7, 0, 1, false);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    consolidate_host_boundary(&f, ctx);
    CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_FINISH
        && f.record.child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    CHECK(!col_rel_source_reader_release(&reader));
    run(&f); CHECK(!f.record.semantic_result && f.record.completed == 2);
    consolidated_values(&f, 7, 0, 1, false);
    join_raw_parity(&f, &plan, true); fixture_close(&f);
}

static void
test_consolidate_later_failures(void)
{
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_MAP, .project_count = 1},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    for (unsigned fault = 0; fault < 2; ++fault) {
        fixture_t f; fixture_init(&f, 1, 19, 0);
        wl_columnar_memory_governor_ref_t *governor = join_governor(&f,
                1u << 20);
        fixture_begin(&f, &plan, 0, 2); join_until_ready(&f, 1);
        join_snapshot_t before = join_snapshot(&f);
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor));
        CHECK(baseline && baseline + 1 < (1u << 20));
        if (fault) CHECK(wl_columnar_memory_reserve(
                    wl_columnar_memory_governor_ref_get(governor),
                    (1u << 20) - baseline - 1, &peer));
        else wl_columnar_filter_test_attempt_fail_allocation = 0;
        engine_dispatch(&f); ++f.witnesses;
        wl_columnar_filter_test_attempt_fail_allocation = -1;
        run(&f);
        CHECK(f.record.semantic_result == (fault ? ENOSPC : ENOMEM)
            && f.record.completed == 1 && f.record.cursor == 1
            && f.session.memory_budget_denied == (fault != 0));
        join_snapshot_check(&f, &before); col_rel_destroy(before.contents);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) == baseline + (fault ? peer.bytes : 0));
        fixture_close(&f);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) == (fault ? peer.bytes : 0));
        if (fault) CHECK(wl_columnar_memory_rollback(&peer));
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

static void
test_consolidate_shape_refusals(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_CONSOLIDATE};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    for (unsigned shape = 0; shape < 4; ++shape)
        for (uint32_t rows = shape < 2 ? 0 : 1; rows <= 1; ++rows) {
            fixture_t f; fixture_init(&f, 1, rows, 0);
            if (!shape) CHECK(!col_rel_enable_timestamps(f.input));
            if (shape == 1) wl_columnar_eval_stack_cleanup_stack(
                    f.frame)->items[0].is_delta = true;
            if (shape >= 2) {
                f.input = join_float(f.input);
                wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel =
                    f.input;
                f.input->columns[0][0] = wl_columnar_float_to_bits(
                    shape == 2 ? INFINITY : NAN);
            }
            uint64_t generation = f.input->storage_generation;
            int64_t bits = rows ? f.input->columns[0][0] : 0;
            fixture_begin(&f, &plan, 0, 1); run(&f);
            CHECK(f.record.semantic_result == (shape < 2 ? ENOTSUP : EINVAL)
                && !f.record.completed && !f.session.memory_budget_denied);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            CHECK(stack->top == 1 && stack->items[0].rel == f.input
                && stack->items[0].is_delta == (shape == 1)
                && f.input->storage_generation == generation);
            if (rows) CHECK(f.input->columns[0][0] == bits);
            fixture_close(&f);
        }
}

/* Full generated bodies, independent literal order/types and a separate raw
 * session. The last variant composes another FILTER/MAP after a generated MAP. */
static void
test_generated_map_float_schema(void)
{
    const char *queries[] = {
        ".decl input(x:float)\n.decl output(y:float)\noutput(x) :- input(x).\n",
        ".decl input(x:float)\n.decl output(y:float)\noutput(x+0.0) :- input(x).\n",
        ".decl input(k:int64,x:float)\n.decl right(k:int64,y:float)\n"
        ".decl output(z:float)\noutput(x+y) :- input(k,x),right(k,y).\n",
        ".decl input(x:float)\n.decl output(y:float,n:int64)\noutput(x+0.0,7) :- input(x).\n",
        ".decl input(x:float)\n.decl output(y:float)\noutput(x+0.0) :- input(x).\n",
    };
    const double inputs[] = {-1.0, -3.0, -3.0, -0.0, 0.0, 2.0};
    const double expected[] = {-3.0, -1.0, 0.0, 2.0};
    for (unsigned shape = 0; shape < 5; ++shape) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(queries[shape],
                &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        const wl_plan_relation_t *rp = NULL;
        for (uint32_t s = 0; s < plan->stratum_count; ++s)
            for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
                if (!strcmp(plan->strata[s].relations[r].name,
                    "output")) rp = &plan->strata[s].relations[r];
        CHECK(rp && rp->ops[0].op == WL_PLAN_OP_VARIABLE &&
            rp->ops[rp->op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
        uint8_t predicate[17] = {WL_PLAN_EXPR_VAR_FLOAT, 4, 0, 'c', 'o', 'l',
                                 '0', WL_PLAN_EXPR_CONST_FLOAT};
        uint8_t mapped[17]; memcpy(mapped, predicate, sizeof(mapped));
        int64_t threshold = wl_columnar_float_to_bits(-4.0),
            zero = wl_columnar_float_to_bits(0.0);
        memcpy(predicate + 8, &threshold, 8);
        predicate[16] = WL_PLAN_EXPR_CMP_FLOAT_GT;
        memcpy(mapped + 8, &zero, 8); mapped[16] = WL_PLAN_EXPR_ARITH_FLOAT_ADD;
        wl_plan_expr_buffer_t expression = {mapped, sizeof(mapped)};
        wl_plan_op_t ops[16]; wl_plan_relation_t composed = *rp;
        if (shape == 4) {
            CHECK(rp->op_count + 2 <= 16);
            for (uint32_t i = 0; i + 1 < rp->op_count; ++i) ops[i] = rp->ops[i];
            ops[rp->op_count-1] = (wl_plan_op_t){.op = WL_PLAN_OP_FILTER,
                                                 .filter_expr = {predicate,
                                                                 sizeof(
                                                                     predicate)}};
            ops[rp->op_count] = (wl_plan_op_t){.op = WL_PLAN_OP_MAP,
                                               .project_count = 1,
                                               .map_exprs = &expression,
                                               .map_expr_count = 1};
            ops[rp->op_count+1] = rp->ops[rp->op_count-1];
            composed.ops = ops; composed.op_count += 2; rp = &composed;
        }
        for (unsigned prebound = 0; prebound < 2; ++prebound)
            for (unsigned empty = 0; empty < 2; ++empty)
                for (unsigned governed = 0; governed < 2; ++governed) {
                    fixture_t f;
                    uint32_t width = shape == 2 ? 2 : 1;
                    if (prebound) fixture_init(&f, width, 0, 0);
                    else source_fixture(&f, width, 0, 0);
                    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                                     WIRELOG_TYPE_FLOAT};
                    if (width == 1) types[0] = WIRELOG_TYPE_FLOAT;
                    CHECK(!col_rel_set_column_types(f.input, types, width));
                    for (unsigned i = 0; !empty && i < 6; ++i) {
                        int64_t values[] = {1,
                                            wl_columnar_float_to_bits(
                                                inputs[i])};
                        if (width == 1) values[0] = values[1];
                        CHECK(!col_rel_append_row(f.input, values));
                    }
                    if (!empty) f.input->columns[width-
                            1][3] = wl_columnar_float_to_bits(-0.0);
                    col_rel_t *right = source_new(2, 0);
                    CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                    wirelog_column_type_t right_types[] = {WIRELOG_TYPE_INT64,
                                                           WIRELOG_TYPE_FLOAT};
                    CHECK(!col_rel_set_column_types(right, right_types, 2));
                    int64_t right_values[] = {1,
                                              wl_columnar_float_to_bits(0.0)};
                    CHECK(!col_rel_append_row(right, right_values));
                    col_rel_t *registry[] = {f.input, right};
                    wl_columnar_memory_governor_ref_t *governor =
                        governed ? join_governor(&f, 1u << 20) : NULL;
                    if (prebound) {
                        f.session.rels = registry; f.session.nrels = 2;
                        fixture_begin(&f, rp, 1, rp->op_count);
                    } else source_begin(&f, rp, registry, 2);
                    run(&f);
                    CHECK(!f.record.semantic_result &&
                        f.record.completed == rp->op_count - 1);
                    join_raw_parity(&f, rp, prebound != 0);
                    col_rel_t *result =
                        wl_columnar_eval_stack_cleanup_stack(
                        f.frame)->items[0].rel;
                    CHECK(result->nrows == (empty ? 0u : 4u) &&
                        result->column_types);
                    CHECK(result->ncols == (shape == 3 ? 2u : 1u));
                    CHECK(result->column_types[0] == WIRELOG_TYPE_FLOAT);
                    if (shape ==
                        3) CHECK(result->column_types[1] == WIRELOG_TYPE_INT64);
                    CHECK(result->sorted_nrows == result->nrows &&
                        result->run_count == (empty ? 0u : 1u));
                    for (uint32_t r = 0; r < result->nrows; ++r) {
                        CHECK(result->columns[0][r] ==
                            wl_columnar_float_to_bits(expected[r]));
                        if (shape == 3) CHECK(result->columns[1][r] == 7);
                    }
                    fixture_close(&f); col_rel_destroy(right);
                    if (governor) {
                        CHECK(!wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(governor)));
                        wl_columnar_memory_governor_ref_release(governor);
                    }
                }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

/* The actual lowered bodies precede backend LFTJ/exchange rewrites. Both
 * generators and their expression/key tables retain ownership of these ops. */
static wl_plan_relation_t
probe_generated_body(const wl_plan_t *plan)
{
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "output")) {
                wl_plan_relation_t body = plan->strata[s].relations[r];
                CHECK(plan->strata[s].bodies);
                body.ops = plan->strata[s].bodies[r].ops;
                body.op_count = plan->strata[s].bodies[r].op_count;
                return body;
            }
    CHECK(false);
    return (wl_plan_relation_t){0};
}

static void
probe_literal_result(fixture_t *f, const double *values, uint32_t count)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack->top == 1 && stack->items[0].owned &&
        !stack->items[0].is_delta);
    col_rel_t *out = stack->items[0].rel;
    CHECK(out->ncols == 1 && out->nrows == count && !out->timestamps &&
        !out->has_graph_column && out->column_types &&
        out->column_types[0] == WIRELOG_TYPE_FLOAT &&
        !strcmp(out->col_names[0], "col0"));
    CHECK(out->sorted_nrows == count && out->run_count == (count != 0));
    if (count) CHECK(out->run_ends[0] == count);
    for (uint32_t r = 0; r < count; ++r)
        CHECK(out->columns[0][r] == wl_columnar_float_to_bits(values[r]));
}

static void
test_generated_probe_plans(void)
{
    const char *queries[] = {
        ".decl input(k:int64,x:float)\n.decl mid(k:int64,y:float)\n"
        ".decl right(y:float,z:float)\n.decl output(v:float)\n"
        "output(x+z) :- input(k,x),mid(k,y),right(y,z).\n",
        ".decl input(k:float,n:int64,x:float)\n"
        ".decl right(k:float,n:int64)\n.decl output(v:float)\n"
        "output(x+0.0) :- input(k,n,x),!right(k,n).\n"
    };
    const double expected[2][2] = {{-5.0, 3.0}, {-1.0, 4.0}};
    for (unsigned anti = 0; anti < 2; ++anti) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(queries[anti],
                &error);
        CHECK(program);
        if (!anti) {
            wl_sip_stats_t stats = {0};
            CHECK(!wl_sip_apply(program,
                &stats) && stats.semijoins_inserted == 1);
        }
        wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        wl_plan_relation_t body = probe_generated_body(plan);
        CHECK(body.op_count > 3 && body.ops[0].op == WL_PLAN_OP_VARIABLE &&
            body.ops[body.op_count - 1].op == WL_PLAN_OP_CONSOLIDATE);
        unsigned probes = 0, maps = 0;
        for (uint32_t i = 0; i < body.op_count; ++i) {
            if (body.ops[i].op ==
                (anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN)){
                ++probes;
                CHECK(body.ops[i].key_count == (anti ? 2u : 1u) &&
                    !body.ops[i].right_filter_expr.size);
            }
            maps += body.ops[i].op == WL_PLAN_OP_MAP;
        }
        CHECK(probes == 1 && maps);
        for (unsigned prebound = 0; prebound < 2; ++prebound)
            for (unsigned empty = 0; empty < 3; ++empty)
                for (unsigned governed = 0; governed < 2; ++governed) {
                    fixture_t f;
                    uint32_t width = anti ? 3 : 2;
                    if (prebound) fixture_init(&f, width, 0, 0);
                    else source_fixture(&f, width, 0, 0);
                    CHECK(!wl_columnar_relation_rename_checked(f.input,
                        body.ops[0].relation_name));
                    wirelog_column_type_t input_types[] = {WIRELOG_TYPE_INT64,
                                                           WIRELOG_TYPE_FLOAT,
                                                           WIRELOG_TYPE_FLOAT};
                    if (anti) {
                        input_types[0] = WIRELOG_TYPE_FLOAT;
                        input_types[1] = WIRELOG_TYPE_INT64;
                    }
                    CHECK(!col_rel_set_column_types(f.input, input_types,
                        width));
                    const int64_t semi_keys[] = {1, 1, 2, 3};
                    const double payloads[] = {-3.0, -3.0, -1.0, 2.0, 4.0};
                    const double anti_keys[] = {-0.0, 0.0, 1.0, 2.0, 3.0};
                    for (uint32_t r = 0; empty != 1 && r < (anti ? 5u : 4u);
                        ++r){
                        int64_t values[] = {semi_keys[r < 4 ? r : 0],
                                            wl_columnar_float_to_bits(
                                                payloads[r]), 0};
                        if (anti) {
                            values[0] = wl_columnar_float_to_bits(anti_keys[r]);
                            values[1] = r < 3 ? 1 : (int64_t)r - 1;
                            values[2] = wl_columnar_float_to_bits(payloads[r]);
                        }
                        CHECK(!col_rel_append_row(f.input, values));
                    }
                    if (anti && f.input->nrows)
                        f.input->columns[0][0] =
                            wl_columnar_float_to_bits(-0.0);
                    col_rel_t *mid = source_new(2, 0),
                        *right = source_new(2, 0);
                    CHECK(!wl_columnar_relation_rename_checked(mid, "mid"));
                    CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                    wirelog_column_type_t mid_types[] = {WIRELOG_TYPE_INT64,
                                                         WIRELOG_TYPE_FLOAT};
                    wirelog_column_type_t right_types[] = {WIRELOG_TYPE_FLOAT,
                                                           anti ?
                                                           WIRELOG_TYPE_INT64 :
                                                           WIRELOG_TYPE_FLOAT};
                    CHECK(!col_rel_set_column_types(mid, mid_types, 2));
                    CHECK(!col_rel_set_column_types(right, right_types, 2));
                    for (uint32_t r = 0; r < 4; ++r) {
                        int64_t values[] = {semi_keys[r],
                                            wl_columnar_float_to_bits(r <
                                                2 ? 0.0 : (double)r - 1)};
                        CHECK(!col_rel_append_row(mid, values));
                    }
                    for (uint32_t r = 0; empty != 2 && r < 3; ++r) {
                        int64_t values[] = {wl_columnar_float_to_bits(r <
                                                2 ? 0.0 : 2.0),
                                            anti ? (r <
                                            2 ? 1 : 2) :
                                            wl_columnar_float_to_bits(r <
                                                2 ? -2.0 : 1.0)};
                        CHECK(!col_rel_append_row(right, values));
                    }
                    col_rel_t *registry[] = {f.input, mid, right};
                    wl_columnar_memory_governor_ref_t *governor = governed
                        ? join_governor(&f, 1u << 20) : NULL;
                    if (prebound) {
                        f.session.rels = registry; f.session.nrels = 3;
                        fixture_begin(&f, &body, 1, body.op_count);
                    } else source_begin(&f, &body, registry, 3);
                    run(&f);
                    CHECK(!f.record.semantic_result &&
                        f.record.completed == body.op_count - 1);
                    CHECK(f.record.source_installed == !prebound);
                    join_raw_parity(&f, &body, prebound != 0);
                    /* Empty right keeps ANTI's distinct input payloads. */
                    const double anti_all[] = {-3.0, -1.0, 2.0, 4.0};
                    probe_literal_result(&f,
                        anti && empty == 2 ? anti_all : expected[anti],
                        empty == 1 || (!anti && empty == 2) ? 0 : anti &&
                        empty == 2 ? 4 : 2);
                    fixture_close(&f); col_rel_destroy(mid);
                    col_rel_destroy(right);
                    if (governor) {
                        CHECK(!wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(governor)));
                        wl_columnar_memory_governor_ref_release(governor);
                    }
                }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

static void
test_composed_probe_plans(void)
{
    const char *key = "col0";
    uint32_t projection = 0;
    uint8_t add[17] = {WL_PLAN_EXPR_VAR_FLOAT, 4, 0, 'c', 'o', 'l', '0',
                       WL_PLAN_EXPR_CONST_FLOAT};
    int64_t one = wl_columnar_float_to_bits(1.0);
    memcpy(add + 8, &one, 8); add[16] = WL_PLAN_EXPR_ARITH_FLOAT_ADD;
    wl_plan_expr_buffer_t expression = {add, sizeof(add)};
    const double inputs[] = {-3.0, -3.0, -1.0, 0.0, 2.0, 4.0};
    for (unsigned first = 0; first < 2; ++first)
        for (unsigned second = 0; second < 2; ++second)
            for (unsigned prebound = 0; prebound < 2; ++prebound) {
                fixture_t f;
                if (prebound) fixture_init(&f, 1, 0, 0);
                else source_fixture(&f, 1, 0, 0);
                wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
                CHECK(!col_rel_set_column_types(f.input, &type, 1));
                for (unsigned r = 0; r < 6; ++r) {
                    int64_t value = wl_columnar_float_to_bits(inputs[r]);
                    CHECK(!col_rel_append_row(f.input, &value));
                }
                col_rel_t *right = source_new(1, 0), *third = source_new(1, 0);
                CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                CHECK(!wl_columnar_relation_rename_checked(third, "third"));
                CHECK(!col_rel_set_column_types(right, &type, 1));
                CHECK(!col_rel_set_column_types(third, &type, 1));
                for (unsigned r = 0; r < 2; ++r) {
                    int64_t a = wl_columnar_float_to_bits(r ? 0.0 : -3.0),
                        b = wl_columnar_float_to_bits(r ? 4.0 : 0.0);
                    CHECK(!col_rel_append_row(right, &a));
                    CHECK(!col_rel_append_row(third, &b));
                }
                wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                                       .relation_name = "input"},
                                      {.op = WL_PLAN_OP_MAP, .project_count = 1,
                                       .project_indices = &projection},
                                      {.op =
                                           first ? WL_PLAN_OP_ANTIJOIN :
                                           WL_PLAN_OP_SEMIJOIN,
                                       .right_relation = "right",
                                       .key_count = 1, .left_keys = &key,
                                       .right_keys = &key},
                                      {.op =
                                           second ? WL_PLAN_OP_ANTIJOIN :
                                           WL_PLAN_OP_SEMIJOIN,
                                       .right_relation = "third",
                                       .key_count = 1, .left_keys = &key,
                                       .right_keys = &key},
                                      {.op = WL_PLAN_OP_MAP, .project_count = 1,
                                       .map_exprs = &expression,
                                       .map_expr_count = 1},
                                      {.op = WL_PLAN_OP_CONSOLIDATE}};
                wl_plan_relation_t plan = {.ops = ops, .op_count = 6};
                col_rel_t *registry[] = {f.input, right, third};
                if (prebound) {
                    f.session.rels = registry; f.session.nrels = 3;
                    fixture_begin(&f, &plan, 1, 6);
                } else source_begin(&f, &plan, registry, 3);
                run(&f);
                CHECK(!f.record.semantic_result && f.record.completed == 5);
                join_raw_parity(&f, &plan, prebound != 0);
                const double expected[4][2] = {{1.0, 0.0}, {-2.0, 0.0},
                                               {5.0, 0.0}, {0.0, 3.0}};
                probe_literal_result(&f, expected[first * 2 + second],
                    first && second ? 2 : 1);
                fixture_close(&f); col_rel_destroy(right);
                col_rel_destroy(third);
            }
}

static uint32_t probe_plan_private_rows;

static void
probe_plan_stop(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    fixture_t *f = join_fault_fixture;
    if (phase != 30 || f->record.cursor != join_fault_cursor ||
        join_fault_calls)
        return;
    ++join_fault_calls;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, &ctx,
        &f->record));
    CHECK(ctx->output && ctx->output->nrows == probe_plan_private_rows &&
        control == f->operation);
    if (join_fault_restart) {
        CHECK(!wl_evaluation_control_finish(control, &f->operation_owner, 0,
            0));
        CHECK(!wl_evaluation_control_begin(control, &f->operation_owner));
    } else wl_evaluation_control_request_cancel(control);
}

static void
test_probe_plan_failures(void)
{
    const char *key = "col0";
    uint32_t projection = 0;
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned target = 1; target < 3; ++target)
            for (unsigned fault = 0; fault < 5; ++fault) {
                fixture_t f; fixture_init(&f, 1, 5, 0);
                col_rel_t *right = source_new(1, 3), *third = source_new(1, 2);
                CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                CHECK(!wl_columnar_relation_rename_checked(third, "third"));
                if (anti) for (uint32_t r = 0; r < 3;
                        ++r) right->columns[0][r] += 2;
                wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_MAP, .project_count = 1,
                                       .project_indices = &projection},
                                      {.op =
                                           anti ? WL_PLAN_OP_ANTIJOIN :
                                           WL_PLAN_OP_SEMIJOIN,
                                       .right_relation = "right",
                                       .key_count = 1, .left_keys = &key,
                                       .right_keys = &key},
                                      {.op =
                                           anti ? WL_PLAN_OP_ANTIJOIN :
                                           WL_PLAN_OP_SEMIJOIN,
                                       .right_relation = "third",
                                       .key_count = 1, .left_keys = &key,
                                       .right_keys = &key},
                                      {.op = WL_PLAN_OP_MAP, .project_count = 1,
                                       .project_indices = &projection},
                                      {.op = WL_PLAN_OP_CONSOLIDATE}};
                if (!fault) ops[target].right_relation = "missing";
                wl_plan_relation_t plan = {.ops = ops, .op_count = 5};
                col_rel_t *registry[] = {f.input, right, third};
                f.session.rels = registry; f.session.nrels = 3;
                wl_columnar_memory_governor_ref_t *governor = fault == 4
                    ? join_governor(&f, 1u << 20) : NULL;
                fixture_begin(&f, &plan, 0, 5); join_until_ready(&f, target);
                join_snapshot_t snapshot = join_snapshot(&f);
                CHECK(snapshot.owned);
                wl_columnar_memory_reservation_t peer;
                wl_columnar_memory_reservation_init(&peer);
                uint64_t baseline = governor ? wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)) : 0;
                if (governor) {
                    CHECK(baseline + 1 < (1u << 20));
                    CHECK(wl_columnar_memory_reserve(
                            wl_columnar_memory_governor_ref_get(governor),
                            (1u << 20) - baseline - 1, &peer));
                }
                if (fault ==
                    1) wl_columnar_join_test_attempt_fail_allocation = 0;
                if (fault == 2 || fault == 3) {
                    join_fault_fixture = &f; join_fault_cursor = target;
                    join_fault_calls = 0; join_fault_restart = fault == 3;
                    /* ANTI's first two unmatched rows have private output
                     * before its first matching candidate. No row is public. */
                    probe_plan_private_rows = anti && target == 1 ? 2 : 0;
                    wl_columnar_join_test_attempt_checkpoint = probe_plan_stop;
                }
                engine_dispatch(&f); ++f.witnesses;
                wl_columnar_join_test_attempt_fail_allocation = -1;
                run(&f); wl_columnar_join_test_attempt_checkpoint = NULL;
                const int errors[] = {ENOTSUP, ENOMEM,
                                      WL_EVALUATION_CONTROL_CANCELLED, EINVAL,
                                      ENOSPC};
                CHECK(f.record.semantic_result == errors[fault] &&
                    f.record.completed == target &&
                    f.record.cursor == target &&
                    f.session.memory_budget_denied == (fault == 4));
                if (fault == 2 || fault == 3) CHECK(join_fault_calls == 1);
                join_snapshot_check(&f, &snapshot);
                col_rel_destroy(snapshot.contents);
                if (fault == 3) {
                    CHECK(!wl_evaluation_control_finish(f.operation,
                        &f.operation_owner, 0, 0));
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.operation,
                        &report) && !report.consumed);
                }
                fixture_close(&f);
                if (governor) {
                    CHECK(wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(governor)) ==
                        peer.bytes);
                    CHECK(wl_columnar_memory_rollback(&peer));
                    wl_columnar_memory_governor_ref_release(governor);
                }
                col_rel_destroy(right); col_rel_destroy(third);
            }
}

static void
test_generated_reduce_plans(void)
{
    const char *queries[] = {
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:int64)\noutput(g,count(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(n:int64,g:int64)\noutput(count(v),g) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:int64)\noutput(g,count(v/0.0)) :- input(g,v).\n",
        ".decl input(g:int64,v:int64)\n.decl output(g:int64,n:int64)\noutput(g,sum(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:int64)\n.decl output(g:int64,n:int64)\noutput(g,min(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:int64)\n.decl output(g:int64,n:int64)\noutput(g,max(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:float)\noutput(g,sum(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:float)\noutput(g,min(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:float)\noutput(g,max(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:float)\noutput(g,average(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(n:int64)\noutput(count(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(n:float)\noutput(average(v)) :- input(g,v).\n"
    };
    const int64_t keys[] = {2, 1, 2, 1, 2, 3},
        operands[] = {-3, 10, 5, -4, -2, 0};
    const int64_t answers[][3] = {{2, 3, 1}, {2, 3, 1}, {2, 3, 1}, {6, 0, 0},
                                  {-4, -3, 0}, {10, 5, 0}, {6, 0, 0},
                                  {-4, -3, 0}, {10, 5, 0}, {3, 0, 0}};
    for (unsigned shape = 0; shape < sizeof(queries) / sizeof(queries[0]);
        ++shape){
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(queries[shape],
                &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        wl_plan_relation_t body = probe_generated_body(plan);
        CHECK(body.op_count >= 3 && body.ops[0].op == WL_PLAN_OP_VARIABLE &&
            body.ops[body.op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
        unsigned reductions = 0;
        for (uint32_t i = 0; i < body.op_count; ++i)
            if (body.ops[i].op == WL_PLAN_OP_REDUCE) {
                ++reductions;
                if (shape < 3 ||
                    shape ==
                    10) CHECK(body.ops[i].agg_fn == WIRELOG_AGG_COUNT &&
                        body.ops[i].agg_operand_type ==
                        WL_PLAN_AGG_OPERAND_FLOAT &&
                        body.ops[i].agg_result_type ==
                        WL_PLAN_AGG_OPERAND_SCALAR);
                else CHECK(body.ops[i].agg_result_type == (shape < 6
                        ? WL_PLAN_AGG_OPERAND_SCALAR :
                        WL_PLAN_AGG_OPERAND_FLOAT));
            }
        CHECK(reductions == 1);
        for (unsigned prebound = 0; prebound < 2; ++prebound)
            for (unsigned empty = 0; empty < 2; ++empty)
                for (unsigned governed = 0; governed < 2; ++governed) {
                    fixture_t f;
                    if (prebound) fixture_init(&f, 2, 0, 0);
                    else source_fixture(&f, 2, 0, 0);
                    bool fp = shape < 3 || shape >= 6;
                    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                                     fp ? WIRELOG_TYPE_FLOAT :
                                                     WIRELOG_TYPE_INT64};
                    CHECK(!col_rel_set_column_types(f.input, types, 2));
                    for (unsigned r = 0; !empty && r < 6; ++r) {
                        int64_t row[] = {keys[r], fp
                            ? wl_columnar_float_to_bits((double)operands[r]) :
                                         operands[r]};
                        CHECK(!col_rel_append_row(f.input, row));
                    }
                    if (!empty &&
                        shape <
                        3) f.input->columns[1][0] =
                            wl_columnar_float_to_bits(NAN);
                    col_rel_t *registry[] = {f.input};
                    wl_columnar_memory_governor_ref_t *governor =
                        governed ? join_governor(&f, 1u << 20) : NULL;
                    if (prebound) {
                        f.session.rels = registry; f.session.nrels = 1;
                        fixture_begin(&f, &body, 1, body.op_count);
                    } else source_begin(&f, &body, registry, 1);
                    run(&f);
                    CHECK(!f.record.semantic_result &&
                        f.record.completed == body.op_count-1 &&
                        f.record.source_installed == !prebound);
                    join_raw_parity(&f, &body, prebound != 0);
                    col_rel_t *out =
                        wl_columnar_eval_stack_cleanup_stack(
                        f.frame)->items[0].rel;
                    uint32_t rows = empty ? 0 : shape >= 10 ? 1 : 3;
                    CHECK(out->nrows == rows &&
                        out->ncols == (shape >= 10 ? 1u : 2u));
                    CHECK(out->sorted_nrows == rows &&
                        out->run_count == (rows != 0));
                    CHECK(!out->timestamps && !out->has_graph_column);
                    unsigned agg = shape == 1 || shape >= 10 ? 0 : 1;
                    CHECK(out->column_types[agg] == (shape >= 6 && shape != 10
                        ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64));
                    if (rows) CHECK(out->run_ends[0] == rows);
                    for (unsigned r = 0; r < rows; ++r) {
                        int64_t expected = shape < 10 ? answers[shape][r]
                            : shape == 10 ? 6 : 1;
                        if (shape == 1) {
                            const int64_t counts[] = {1, 2, 3},
                                groupkeys[] = {3, 1, 2};
                            expected = counts[r];
                            CHECK(out->columns[1][r] == groupkeys[r]);
                        } else if (shape <
                            10) CHECK(out->columns[0][r] == (int64_t)r+1);
                        if (shape >= 6 &&
                            shape !=
                            10) expected =
                                wl_columnar_float_to_bits((double)expected);
                        CHECK(out->columns[agg][r] == expected);
                    }
                    fixture_close(&f);
                    if (governor) {
                        CHECK(!wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(governor)));
                        wl_columnar_memory_governor_ref_release(governor);
                    }
                }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

static void
test_composed_reduce_plans(void)
{
    uint8_t yes[] = {WL_PLAN_EXPR_BOOL, 1};
    uint8_t aggregate[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'};
    uint32_t identity[] = {0, 1}, first[] = {0};
    const char *key = "col0";
    for (unsigned kind = 0; kind < 3; ++kind)
        for (unsigned prebound = 0; prebound < 2; ++prebound)
            for (unsigned empty = 0; empty < 2; ++empty) {
                fixture_t f;
                if (prebound) fixture_init(&f, 2, 0, 0);
                else source_fixture(&f, 2, 0, 0);
                const int64_t keys[] = {2, 1, 2, 1, 2, 3},
                    values[] = {-3, 10, 5, -4, -2, 0};
                for (unsigned r = 0; !empty && r < 6; ++r) {
                    int64_t row[] = {keys[r], values[r]};
                    CHECK(!col_rel_append_row(f.input, row));
                }
                col_rel_t *right = source_new(2, 0);
                CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                for (unsigned r = 0; r < 3; ++r) {
                    int64_t row[] = {r < 2 ? 1 : 2, 9};
                    CHECK(!col_rel_append_row(right, row));
                }
                col_rel_t *registry[] = {f.input, right};
                wl_plan_op_t ops[] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
                    {.op = WL_PLAN_OP_FILTER,
                     .filter_expr = {yes, sizeof(yes)}},
                    {.op = WL_PLAN_OP_MAP, .project_count = 2,
                     .project_indices = identity},
                    {.op = kind == 0 ? WL_PLAN_OP_JOIN : kind ==
                         1 ? WL_PLAN_OP_SEMIJOIN : WL_PLAN_OP_ANTIJOIN,
                     .right_relation = "right", .key_count = 1,
                     .left_keys = &key, .right_keys = &key},
                    {.op = WL_PLAN_OP_MAP, .project_count = 2,
                     .project_indices = identity},
                    {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                     .aggregate_index = 1,
                     .agg_fn = WIRELOG_AGG_SUM,
                     .agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR},
                    {.op = WL_PLAN_OP_FILTER,
                     .filter_expr = {yes, sizeof(yes)}},
                    {.op = WL_PLAN_OP_REDUCE, .aggregate_index = 0,
                     .agg_fn = WIRELOG_AGG_SUM,
                     .agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR,
                     .agg_expr = {aggregate, sizeof(aggregate)}},
                    {.op = WL_PLAN_OP_MAP, .project_count = 1,
                     .project_indices = first},
                    {.op = WL_PLAN_OP_CONSOLIDATE}
                };
                wl_plan_relation_t plan = {.ops = ops, .op_count = 10};
                if (prebound) {
                    f.session.rels = registry; f.session.nrels = 2;
                    fixture_begin(&f, &plan, 1, 10);
                } else source_begin(&f, &plan, registry, 2);
                run(&f);
                CHECK(!f.record.semantic_result && f.record.completed == 9);
                join_raw_parity(&f, &plan, prebound != 0);
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                CHECK(stack->top == 1 && stack->items[0].owned &&
                    !stack->items[0].is_delta);
                col_rel_t *out = stack->items[0].rel;
                CHECK(out->ncols == 1 && out->nrows == (empty ? 0u : 1u) &&
                    out->column_types[0] == WIRELOG_TYPE_INT64);
                if (!empty) CHECK(out->columns[0][0] ==
                        (kind == 0 ? 12 : kind == 1 ? 6 : 0));
                CHECK(out->sorted_nrows == out->nrows &&
                    out->run_count == (empty ? 0u : 1u));
                fixture_close(&f); col_rel_destroy(right);
            }
}

static fixture_t *reduce_plan_observer;
static col_rel_t *reduce_prior_entry;
static unsigned reduce_plan_fault;
static void
reduce_plan_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase == 1) {
        eval_stack_t *stack =
            wl_columnar_eval_stack_cleanup_stack(reduce_plan_observer->frame);
        CHECK(stack->top == 1 && stack->items[0].owned &&
            !stack->items[0].is_delta);
        reduce_prior_entry = stack->items[0].rel;
    }
    if (phase == 40 &&
        reduce_plan_fault == 2) wl_evaluation_control_request_cancel(control);
}

static void
test_reduce_later_failure(void)
{
    uint32_t identity[] = {0, 1};
    for (unsigned fault = 0; fault < 3; ++fault) {
        fixture_t f; fixture_init(&f, 2, fault == 2 ? 600 : 3, 0);
        for (uint32_t r = 0; r < f.input->nrows; ++r) {
            f.input->columns[0][r] = 0;
            f.input->columns[1][r] = fault ==
                0 ? (r == 0 ? INT64_MAX : r == 1 ? 1 : -1) : 1;
        }
        wl_plan_op_t ops[] = {
            {.op = WL_PLAN_OP_MAP, .project_count = 2,
             .project_indices = identity},
            {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1, .aggregate_index = 1,
             .agg_fn = WIRELOG_AGG_SUM,
             .agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR},
            {.op = WL_PLAN_OP_MAP, .project_count = 2,
             .project_indices = identity},
            {.op = WL_PLAN_OP_CONSOLIDATE}
        };
        wl_plan_relation_t plan = {.ops = ops, .op_count = 4};
        reduce_plan_observer = &f; reduce_prior_entry = NULL;
        reduce_plan_fault = fault;
        wl_columnar_ops_test_reduce_attempt_checkpoint = reduce_plan_checkpoint;
        if (fault == 1)wl_columnar_ops_test_reduce_attempt_fail_allocation = 0;
        fixture_begin(&f, &plan, 0, 4); run(&f);
        wl_columnar_ops_test_reduce_attempt_checkpoint = NULL;
        wl_columnar_ops_test_reduce_attempt_fail_allocation = -1;
        CHECK(f.record.semantic_result ==
            (fault == 0 ? ERANGE : fault ==
            1 ? ENOMEM : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(f.record.completed == 1 && f.record.cursor == 1 &&
            reduce_prior_entry);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        CHECK(stack->top == 1 && stack->items[0].owned &&
            stack->items[0].rel == reduce_prior_entry &&
            !stack->items[0].is_delta);
        CHECK(reduce_prior_entry->nrows == f.input->nrows &&
            reduce_prior_entry->ncols == 2);
        for (uint32_t r = 0; r < f.input->nrows; ++r) {
            CHECK(reduce_prior_entry->columns[0][r] == f.input->columns[0][r]);
            CHECK(reduce_prior_entry->columns[1][r] == f.input->columns[1][r]);
        }
        fixture_close(&f);
    }
    reduce_plan_observer = NULL; reduce_prior_entry = NULL;
}

static void
test_generated_reduce_join_bodies(void)
{
    const char *queries[] = {
        ".decl input(g:int64,v:int64)\n.decl right(g:int64,w:int64)\n.decl output(g:int64,n:int64)\noutput(g,sum(v+w)) :- input(g,v),right(g,w).\n",
        ".decl input(g:int64,v:int64)\n.decl right(g:int64)\n.decl output(g:int64,n:int64)\noutput(g,sum(v)) :- input(g,v),right(g).\n",
        ".decl input(g:int64,v:int64)\n.decl right(g:int64)\n.decl output(g:int64,n:int64)\noutput(g,sum(v)) :- input(g,v),!right(g).\n"
    };
    const int64_t keys[] = {2, 1, 2, 1, 2, 3},
        values[] = {-3, 10, 5, -4, -2, 0};
    for (unsigned shape = 0; shape < 3; ++shape) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(queries[shape],
                &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        wl_plan_relation_t body = probe_generated_body(plan);
        CHECK(body.ops[0].op == WL_PLAN_OP_VARIABLE &&
            body.ops[body.op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
        unsigned joins = 0, reductions = 0;
        for (uint32_t i = 0; i < body.op_count; ++i) {
            if (body.ops[i].op == WL_PLAN_OP_JOIN ||
                body.ops[i].op == WL_PLAN_OP_SEMIJOIN ||
                body.ops[i].op == WL_PLAN_OP_ANTIJOIN) ++joins;
            if (body.ops[i].op == WL_PLAN_OP_REDUCE) {
                ++reductions;
                CHECK(body.ops[i].agg_fn == WIRELOG_AGG_SUM &&
                    body.ops[i].agg_result_type == WL_PLAN_AGG_OPERAND_SCALAR);
            }
        }
        CHECK(joins && reductions == 1);
        for (unsigned prebound = 0; prebound < 2; ++prebound)
            for (unsigned empty = 0; empty < 3; ++empty)
                for (unsigned governed = 0; governed < 2; ++governed) {
                    fixture_t f;
                    if (prebound) fixture_init(&f, 2, 0, 0);
                    else source_fixture(&f, 2, 0, 0);
                    for (unsigned r = 0; empty != 1 && r < 6; ++r) {
                        int64_t row[] = {keys[r], values[r]};
                        CHECK(!col_rel_append_row(f.input, row));
                    }
                    col_rel_t *right = source_new(shape ? 1 : 2, 0);
                    CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                    for (unsigned r = 0; empty != 2 && r < 2; ++r) {
                        int64_t row[] = {r+1, r+1};
                        CHECK(!col_rel_append_row(right, row));
                    }
                    col_rel_t *registry[] = {f.input, right};
                    wl_columnar_memory_governor_ref_t *governor =
                        governed ? join_governor(&f, 1u<<20) : NULL;
                    if (prebound) {
                        f.session.rels = registry; f.session.nrels = 2;
                        fixture_begin(&f, &body, 1, body.op_count);
                    }else source_begin(&f, &body, registry, 2);
                    run(&f);
                    CHECK(!f.record.semantic_result &&
                        f.record.completed == body.op_count-1);
                    join_raw_parity(&f, &body, prebound != 0);
                    col_rel_t *out =
                        wl_columnar_eval_stack_cleanup_stack(
                        f.frame)->items[0].rel;
                    unsigned rows = empty == 1 ||
                        (empty == 2 &&
                        shape != 2) ? 0u : shape ==
                        2 ? (empty == 2 ? 3u : 1u) : 2u;
                    CHECK(out->ncols == 2 && out->nrows == rows &&
                        out->column_types[0] == WIRELOG_TYPE_INT64 &&
                        out->column_types[1] == WIRELOG_TYPE_INT64);
                    for (unsigned r = 0; r < rows; ++r) {
                        int64_t key = shape == 2 &&
                            empty != 2 ? 3 : (int64_t)r+1;
                        int64_t value = shape == 0 ? (r ? 6 : 8) : key ==
                            1 ? 6 : 0;
                        CHECK(out->columns[0][r] == key &&
                            out->columns[1][r] == value);
                    }
                    CHECK(out->sorted_nrows == rows &&
                        out->run_count == (rows != 0));
                    fixture_close(&f); col_rel_destroy(right);
                    if (governor) {
                        CHECK(!wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(governor)));
                        wl_columnar_memory_governor_ref_release(governor);
                    }
                }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

/* Whole UNION bodies use a separate host action. The external FIFO records
 * the protocol only after this function returned; no child-result loop or
 * assertion is hidden inside the live child caller suffix. */
static int
composed_host_turn(fixture_t *f, bool ack)
{
    /* This local grant stays exclusive and active until this action ends. */
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(0, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    int rc = ack ? wl_columnar_eval_plan_numeric_composed_attempt_ack(
        &f->record, funding, id)
        : f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        ? wl_columnar_eval_plan_numeric_composed_consume_result(&f->record,
            f->pending_protocol, funding, id)
        : wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record,
            funding, id);
    CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static col_rel_t *composed_source(const char *, uint32_t, const double *,
    uint32_t, bool);
static void composed_fixture(fixture_t *, col_rel_t *, uint64_t);
static void composed_action(fixture_t *);

static int
composed_abort_request(fixture_t *f, uint64_t funding_limit)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(funding_limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    int rc = wl_columnar_eval_plan_numeric_composed_abort_request(
        &f->record, funding, id);
    int finish = wl_evaluation_control_finish(funding, &owner, 0, 0);
    CHECK(finish == (rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? rc : 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report)
        && report.attempt_id == id && !report.cleanup_status
        && report.consumed == (rc == WL_EVALUATION_CONTROL_WORK_LIMIT
        ? 0 : rc ? 0 : 384)
        && (!report.execution_status
        || report.execution_status == WL_EVALUATION_CONTROL_WORK_LIMIT));
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static int
composed_cleanup_lftj_turn(fixture_t *f, uint64_t funding_limit,
    uint64_t funding_id_adjustment)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(funding_limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    bool eligible = (f->record.phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
        || f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE)
        && f->record.lftj.identity;
    int rc = wl_columnar_eval_plan_numeric_composed_cleanup_lftj(
        &f->record, funding, id + funding_id_adjustment);
    int finish = wl_evaluation_control_finish(funding, &owner,
            rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? rc : 0, 0);
    CHECK(finish == (rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? rc : 0));
    wl_evaluation_control_report_t report;
    int report_rc = wl_evaluation_control_report(funding, &report);
    uint64_t expected_consumed = funding_limit == 383
        || funding_id_adjustment || !eligible ? 0 : 384;
    CHECK(!report_rc
        && report.attempt_id == id && !report.cleanup_status
        && report.consumed == expected_consumed);
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static uint32_t
composed_lftj_reader_count(const wl_columnar_lftj_attempt_t *attempt)
{
    uint32_t count = 0;
    if (attempt->inputs)
        for (uint32_t i = 0; i < attempt->input_count; ++i)
            count += attempt->inputs[i].reader.owner != NULL;
    return count;
}

static void
composed_lftj_fixture(fixture_t *f, uint64_t limit,
    wl_plan_relation_t *body, wl_plan_op_t *op, wl_plan_op_lftj_t *metadata,
    char **names, uint32_t *key_cols, col_rel_t **relations)
{
    static const double values[] = {1, 10, 2, 20};
    names[0] = "a"; names[1] = "b"; names[2] = "c";
    key_cols[0] = key_cols[1] = key_cols[2] = 0;
    metadata->k = 3; metadata->rel_names = names;
    metadata->key_cols = key_cols;
    *op = (wl_plan_op_t){.op = WL_PLAN_OP_LFTJ, .opaque_data = metadata};
    *body = (wl_plan_relation_t){.ops = op, .op_count = 1};
    for (uint32_t i = 0; i < 3; ++i)
        relations[i] = composed_source(names[i], 2, values, 2, false);
    composed_fixture(f, source_new(2, 0), limit);
    f->session.base.evaluation_control = f->operation;
    f->session.rels = relations; f->session.nrels = 3; f->session.rel_cap = 3;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin(&f->record,
        body, 0, body->op_count, f->frame, &f->session, 0, f->operation,
        f->operation_id, f->funding, f->funding_id));
}

static int
composed_cleanup_source_turn(fixture_t *f, uint64_t funding_limit,
    uint64_t funding_id_adjustment)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(funding_limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    bool receipt_pending = f->record.phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
    bool aborting = f->record.phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT;
    int original_stop = aborting ? wl_evaluation_control_charge_attempt(
        f->operation, f->operation_id, 0) : 0;
    bool abort_stop = original_stop == WL_EVALUATION_CONTROL_CANCELLED
        || original_stop == WL_EVALUATION_CONTROL_WORK_LIMIT;
    int rc = wl_columnar_eval_plan_numeric_composed_cleanup_source(
        &f->record, funding, id + funding_id_adjustment);
    int finish = wl_evaluation_control_finish(funding, &owner,
            rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? rc : 0, 0);
    CHECK(finish == (rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? rc : 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report)
        && report.attempt_id == id && !report.cleanup_status
        && report.consumed == (funding_limit == 383
        || funding_id_adjustment || receipt_pending
        || (aborting && !abort_stop) ? 0 : 384));
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static const wl_plan_relation_t *composed_find_relation(const wl_plan_t *,
    const char *);
static void composed_begin(fixture_t *, const wl_plan_relation_t *,
    col_rel_t **, uint32_t, uint32_t);
static void composed_until_phase(fixture_t *,
    wl_columnar_eval_plan_numeric_phase_t);
static void composed_until(fixture_t *,
    wl_columnar_eval_plan_numeric_phase_t, uint32_t);
static wl_columnar_memory_governor_ref_t *composed_governor(fixture_t *);
static void composed_close(fixture_t *);

static void
composed_until_stopped_child(fixture_t *f,
    wl_columnar_eval_plan_numeric_phase_t child_phase,
    wl_columnar_eval_plan_numeric_phase_t start_phase)
{
    bool consumed_stop_receipt = false;
    for (unsigned turns = 0; f->record.phase != child_phase; ++turns) {
        CHECK(turns < 100000 && f->record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
        if (f->record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING) {
            composed_action(f);
            continue;
        }
        int receipt = f->pending_protocol;
        if (f->record.pending_action == start_phase) {
            CHECK(f->record.child_status.semantic_result ==
                WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(composed_abort_request(f, 0) == EBUSY);
            consumed_stop_receipt = true;
        }
        CHECK(!composed_host_turn(f, false));
        CHECK(f->record.protocol_result == receipt);
    }
    CHECK(consumed_stop_receipt);
}

static void
composed_lftj_finish_stop(fixture_t *f, int stop, int result)
{
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        && f->record.semantic_result == result);
    CHECK(!composed_host_turn(f, true));
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    CHECK(!f->session.cleanup_active_count
        && !f->session.cleanup_reserved_bytes);
    session_rel_free_hash(&f->session);
    CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner,
        result, 0) == result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->operation, &report)
        && report.stop_reason == stop && report.result == result);
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f->operation, &f->operation_owner));
    CHECK(!wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->operation);
    wl_evaluation_control_release(f->funding);
    col_rel_destroy(f->input);
}

static void
test_composed_source_stopped_cleanup(void)
{
    static const char source[] =
        ".decl input(x:int64)\n.decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    static const double values[] = {3, 1, 4};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *body = composed_find_relation(plan, "output");
    CHECK(body && body->ops[0].op == WL_PLAN_OP_VARIABLE);
    fixture_t f;
    composed_fixture(&f, composed_source("input", 1, values, 3, false), 0);
    col_rel_t *registry[] = {f.input};
    composed_begin(&f, body, registry, 1, 0);
    composed_until_phase(&f,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END);
    CHECK(f.record.source_initialized && !f.record.source_slot_borrowed);
    wl_columnar_eval_plan_numeric_attempt_t before = f.record;
    CHECK(composed_cleanup_source_turn(&f, 0, 0) == EBUSY);
    CHECK(f.record.phase == before.phase
        && f.record.cursor == before.cursor
        && f.record.completed == before.completed
        && !f.record.semantic_result
        && f.record.source.phase == before.source.phase);
    CHECK(composed_cleanup_source_turn(&f, 0, 1) == EINVAL);
    CHECK(f.record.phase == before.phase
        && f.record.cursor == before.cursor
        && f.record.completed == before.completed
        && !f.record.semantic_result
        && f.record.source.phase == before.source.phase);

    wl_evaluation_control_request_cancel(f.operation);
    CHECK(composed_cleanup_source_turn(&f, 383, 0)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == before.phase
        && f.record.cursor == before.cursor
        && f.record.completed == before.completed
        && !f.record.semantic_result
        && f.record.source.phase == before.source.phase);
    int rc = composed_cleanup_source_turn(&f, 0, 0);
    f.pending_protocol = rc;
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        && f.record.pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
        && f.record.source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED
        && f.record.source_initialized && f.record.cursor == before.cursor
        && f.record.completed == before.completed);
    wl_columnar_eval_plan_numeric_attempt_t pending = f.record;
    CHECK(composed_cleanup_source_turn(&f, 0, 0) == EBUSY);
    CHECK(f.record.phase == pending.phase
        && f.record.pending_action == pending.pending_action
        && f.record.source.phase == pending.source.phase
        && f.record.cursor == pending.cursor
        && f.record.completed == pending.completed);
    CHECK(!composed_host_turn(&f, false));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        && !f.record.source_initialized
        && f.record.source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED
        && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.cursor == before.cursor
        && f.record.completed == before.completed);
    CHECK(!composed_host_turn(&f, false));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && !wl_columnar_eval_stack_cleanup_stack(f.frame)->top
        && !session_find_rel(&f.session, "output"));
    composed_close(&f);
    session_rel_free_hash(&f.session);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_composed_source_cleanup_abort_retry(void)
{
    static const double a_values[] = {3, 1, 4};
    static const double b_values[] = {2, 6};
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "b",
         .delta_mode = WL_DELTA_FORCE_EMPTY},
        {.op = WL_PLAN_OP_CONCAT},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t body = {.ops = ops, .op_count = 4};
    col_rel_t *a = composed_source("a", 1, a_values, 3, false);
    col_rel_t *b = composed_source("b", 1, b_values, 2, false);
    fixture_t f;
    composed_fixture(&f, a, 0);
    col_rel_t *sources[] = {a, b};
    composed_begin(&f, &body, sources, 2, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL,
        1);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(f.frame);
    col_rel_t *candidate = result->rel;
    CHECK(candidate && result->owned && f.record.source_slot_borrowed
        && f.record.source_output == &result->rel
        && !f.record.source_initialized);

    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(candidate, &reader));
    /* The install phase must not create a session result without a stop; it
     * pays for the host action but leaves its borrowed candidate untouched. */
    CHECK(composed_cleanup_source_turn(&f, 0, 0) == EBUSY);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL
        && !f.record.semantic_result && result->rel == candidate
        && result->owned && f.record.source_slot_borrowed
        && f.record.source_output == &result->rel);

    /* Stage a source-abort owner with a live borrowed slot. Direct cleanup
     * without a sticky control stop is completely inert and uncharged. */
    f.record.phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT;
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
        && !f.record.semantic_result && result->rel == candidate
        && f.record.source_slot_borrowed);
    CHECK(composed_cleanup_source_turn(&f, 0, 0) == EBUSY);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
        && !f.record.semantic_result && result->rel == candidate
        && result->owned && f.record.source_slot_borrowed
        && f.record.source_output == &result->rel);
    f.record.phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL;

    wl_evaluation_control_request_cancel(f.operation);
    CHECK(composed_cleanup_source_turn(&f, 0, 1) == EINVAL);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL
        && !f.record.semantic_result && result->rel == candidate
        && result->owned && f.record.source_slot_borrowed
        && f.record.source_output == &result->rel);
    CHECK(composed_cleanup_source_turn(&f, 383, 0)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL
        && !f.record.semantic_result && result->rel == candidate
        && result->owned && f.record.source_slot_borrowed
        && f.record.source_output == &result->rel);
    CHECK(!composed_cleanup_source_turn(&f, 0, 0));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
        && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && result->rel == candidate && result->owned
        && f.record.source_slot_borrowed
        && f.record.source_output == &result->rel
        && !session_find_rel(&f.session, "output"));

    /* The funded disposal attempt is refused by the held reader, consuming
     * the host action while retaining every borrowed-slot ownership marker. */
    CHECK(composed_cleanup_source_turn(&f, 0, 0) == EBUSY);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
        && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && result->rel == candidate
        && result->owned && f.record.source_slot_borrowed
        && f.record.source_output == &result->rel);
    CHECK(!col_rel_source_reader_release(&reader));
    CHECK(!composed_cleanup_source_turn(&f, 0, 0));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        && !result->rel && !result->owned && !f.record.source_slot_borrowed
        && !f.record.source_output
        && !session_find_rel(&f.session, "output"));
    CHECK(!composed_host_turn(&f, false));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
    composed_close(&f);
    session_rel_free_hash(&f.session);
    col_rel_destroy(b);
}

static void
test_composed_source_cleanup_pre_action_phases(void)
{
    static const double values[] = {3, 1, 4};
    static const wl_columnar_eval_plan_numeric_phase_t phases[] = {
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_DELTA,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SELECT,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SLOT,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW
    };
    for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); ++i) {
        wl_plan_op_t op = {.op = WL_PLAN_OP_VARIABLE,
                           .relation_name = "input",
                           .delta_mode = WL_DELTA_FORCE_EMPTY};
        wl_plan_relation_t body = {.ops = &op, .op_count = 1};
        fixture_t f;
        composed_fixture(&f, composed_source("input", 1, values, 3, false),
            0);
        col_rel_t *registry[] = {f.input};
        composed_begin(&f, &body, registry, 1, 0);
        composed_until_phase(&f, phases[i]);
        CHECK(f.record.phase == phases[i]);
        uint32_t cursor = f.record.cursor, completed = f.record.completed;
        bool initialized = f.record.source_initialized;
        bool borrowed = f.record.source_slot_borrowed;
        col_rel_t *slot = wl_columnar_eval_stack_cleanup_result(f.frame)->rel;

        wl_evaluation_control_request_cancel(f.operation);
        CHECK(!composed_cleanup_source_turn(&f, 0, 0));
        wl_columnar_eval_plan_numeric_phase_t routed = initialized
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
            : borrowed
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
            : WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE;
        CHECK(f.record.phase == routed
            && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
            && f.record.cursor == cursor && f.record.completed == completed
            && f.record.source_initialized == initialized
            && f.record.source_slot_borrowed == borrowed
            && wl_columnar_eval_stack_cleanup_result(f.frame)->rel == slot
            && !session_find_rel(&f.session, "output"));

        if (routed == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END) {
            int protocol = composed_cleanup_source_turn(&f, 0, 0);
            f.pending_protocol = protocol;
            CHECK(protocol == WL_EVALUATION_CONTROL_CANCELLED
                && f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
                && f.record.pending_action ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END
                && f.record.cursor == cursor
                && f.record.completed == completed);
            CHECK(!composed_host_turn(&f, false));
            if (f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT)
                CHECK(!composed_cleanup_source_turn(&f, 0, 0));
        } else if (routed ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT) {
            CHECK(!composed_cleanup_source_turn(&f, 0, 0));
        }
        CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            && f.record.cursor == cursor && f.record.completed == completed
            && !session_find_rel(&f.session, "output"));
        CHECK(!composed_host_turn(&f, false));
        CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
            && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
            && !session_find_rel(&f.session, "output"));
        composed_close(&f);
        session_rel_free_hash(&f.session);
    }
}

static void
composed_lftj_until_reader(fixture_t *f)
{
    for (unsigned turns = 0; f->record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD; ++turns) {
        CHECK(turns < 1000 && f->record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
        composed_action(f);
    }
    for (unsigned turns = 0; !composed_lftj_reader_count(&f->record.lftj);
        ++turns) {
        CHECK(turns < 1000 && f->record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD);
        composed_action(f);
    }
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
        && f->record.lftj.phase != WL_COLUMNAR_LFTJ_ATTEMPT_DONE);
}

static void
composed_action(fixture_t *f)
{
    f->pending_protocol = composed_host_turn(f, false);
    CHECK(!f->pending_protocol ||
        f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
}

static void
composed_drain_stopped_child(fixture_t *f,
    wl_columnar_eval_plan_numeric_phase_t child_phase,
    wl_columnar_eval_plan_numeric_phase_t ack_phase)
{
    for (unsigned turns = 0; f->record.phase != ack_phase; ++turns) {
        CHECK(turns < 100000 && f->record.phase == child_phase
            && f->record.child_status.semantic_result ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        composed_action(f);
        CHECK(f->record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
            && f->record.pending_action == child_phase);
        int receipt = f->pending_protocol;
        CHECK(composed_abort_request(f, 0) == EBUSY);
        CHECK(!composed_host_turn(f, false));
        CHECK(f->record.protocol_result == receipt);
    }
    CHECK(f->record.child_status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE);
}

static void
composed_run(fixture_t *f)
{
    queue_t q = {{0}, 0, 0};
    enqueue(&q, 0); enqueue(&q, 1);
    unsigned turns = 0, calls = 0, witnesses = 0;
    while (q.count) {
        CHECK(++turns < 100000);
        unsigned job = q.entries[q.head];
        q.head = (q.head + 1) % 4; --q.count;
        if (!job) {
            if (f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
                continue;
            enqueue(&q, 0); ++calls;
            CHECK(calls == witnesses + 1);
            composed_action(f);
        } else {
            ++witnesses; CHECK(witnesses == calls);
            if (f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
                enqueue(&q, 1);
        }
    }
    CHECK(calls == witnesses && calls);
}

static void
composed_until(fixture_t *f, wl_columnar_eval_plan_numeric_phase_t phase,
    uint32_t cursor)
{
    for (unsigned turns = 0; f->record.phase != phase
        || f->record.cursor != cursor; ++turns) {
        CHECK(turns < 100000 && f->record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
        composed_action(f);
    }
}

static void
composed_until_phase(fixture_t *f,
    wl_columnar_eval_plan_numeric_phase_t phase)
{
    for (unsigned turns = 0; f->record.phase != phase; ++turns) {
        CHECK(turns < 100000 && f->record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        composed_action(f);
    }
}

static const wl_plan_relation_t *
composed_find_relation(const wl_plan_t *plan, const char *name)
{
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, name))
                return &plan->strata[s].relations[r];
    return NULL;
}

static col_rel_t *composed_source(const char *, uint32_t, const double *,
    uint32_t, bool);
static void composed_fixture(fixture_t *, col_rel_t *, uint64_t);
static void composed_begin(fixture_t *, const wl_plan_relation_t *,
    col_rel_t **, uint32_t, uint32_t);
static void composed_close(fixture_t *);
static void composed_retry_frame(fixture_t *);
static void composed_raw_parity(fixture_t *, const wl_plan_relation_t *,
    uint32_t);

static void
test_composed_abort_request(void)
{
    eval_stack_t *stack;
    static const char unary_text[] =
        ".decl input(x:int64)\n.decl output(y:int64)\n"
        "output(x + 1) :- input(x), x > 0.\n";
    static const char map_text[] =
        ".decl input(x:int64)\n.decl output(y:int64)\n"
        "output(x + 1) :- input(x).\n";
    static const char union_text[] =
        ".decl a(x:int64)\n.decl b(x:int64)\n.decl output(x:int64)\n"
        "output(x) :- a(x).\noutput(x) :- b(x).\n";
    static const char join_text[] =
        ".decl a(k:int64)\n.decl b(k:int64)\n.decl output(k:int64)\n"
        "output(k) :- a(k), b(k).\n";
    const double a_values[] = {3, 1, 3, -2};
    const double b_values[] = {1, 4, 0};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(unary_text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *unary = composed_find_relation(plan, "output");
    CHECK(unary && unary->ops[0].op == WL_PLAN_OP_VARIABLE
        && unary->ops[unary->op_count-1].op == WL_PLAN_OP_CONSOLIDATE);

    /* Abort at the first fully preflighted action. Refused funding must not
     * latch cancellation or move the original composed state. */
    fixture_t f;
    composed_fixture(&f, composed_source("input", 1, a_values, 4, false), 0);
    col_rel_t *input_registry[] = {f.input};
    composed_begin(&f, unary, input_registry, 1, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 0);
    CHECK(f.record.abort_supported && !f.record.abort_requested);
    wl_columnar_eval_plan_numeric_phase_t saved_phase = f.record.phase;
    uint32_t saved_cursor = f.record.cursor;
    CHECK(composed_abort_request(&f, 1) ==
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == saved_phase && f.record.cursor == saved_cursor
        && !f.record.abort_requested && !f.record.semantic_result);
    CHECK(!composed_abort_request(&f, 0)
        && !composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.completed == 0
        && !wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
    composed_retry_frame(&f);
    composed_begin(&f, unary, input_registry, 1, 0);
    composed_run(&f);
    CHECK(!f.record.semantic_result && f.record.completed == unary->op_count);
    composed_raw_parity(&f, unary, 0);
    composed_close(&f);

    /* An owner-level pending result cannot be bypassed by ABORT. Consume it
     * on the normal distinct grant first, then let the owner's cancellation
     * checkpoint run its existing finish and ACK path. */
    composed_fixture(&f, composed_source("input", 1, a_values, 4, false), 0);
    input_registry[0] = f.input;
    composed_begin(&f, unary, input_registry, 1, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD, 1);
    composed_action(&f);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        && f.record.pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD);
    wl_columnar_eval_plan_numeric_phase_t pending_phase = f.record.phase;
    wl_columnar_eval_plan_numeric_phase_t pending_action =
        f.record.pending_action;
    CHECK(composed_abort_request(&f, 0) == EBUSY
        && f.record.phase == pending_phase
        && f.record.pending_action == pending_action);
    composed_action(&f);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD);
    CHECK(!composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel == f.input
        && !stack->items[0].owned);
    composed_close(&f);

    /* Cancellation after the unary commit must retain its replacement and
     * use the ordinary ACK to dispose the retired input exactly once. */
    composed_fixture(&f, composed_source("input", 1, a_values, 4, false), 0);
    input_registry[0] = f.input;
    composed_begin(&f, unary, input_registry, 1, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK, 1);
    CHECK(f.record.child_status.committed);
    CHECK(!composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.completed == 1);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].owned
        && stack->items[0].rel != f.input);
    composed_close(&f);
    wl_plan_free(plan); wirelog_program_free(program);

    /* Generated MAP cancellation is independently covered on both sides of
     * its child commit, matching the supported closure's explicit MAP bit. */
    program = wirelog_parse_string(map_text, &error); plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *map = composed_find_relation(plan, "output");
    CHECK(map && map->ops[0].op == WL_PLAN_OP_VARIABLE
        && map->ops[1].op == WL_PLAN_OP_MAP
        && map->ops[map->op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
    composed_fixture(&f, composed_source("input", 1, a_values, 4, false), 0);
    input_registry[0] = f.input;
    composed_begin(&f, map, input_registry, 1, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD, 1);
    CHECK(!composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && !f.record.child_status.committed);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && !stack->items[0].owned
        && stack->items[0].rel == f.input
        && stack->items[0].rel->nrows == 4);
    for (uint32_t r = 0; r < 4; ++r)
        CHECK(stack->items[0].rel->columns[0][r] == (int64_t)a_values[r]);
    composed_retry_frame(&f);
    composed_begin(&f, map, input_registry, 1, 0);
    composed_run(&f);
    CHECK(!f.record.semantic_result && f.record.completed == map->op_count);
    composed_raw_parity(&f, map, 0);
    composed_close(&f);

    composed_fixture(&f, composed_source("input", 1, a_values, 4, false), 0);
    input_registry[0] = f.input;
    composed_begin(&f, map, input_registry, 1, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK, 1);
    CHECK(f.record.child_status.committed);
    CHECK(!composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.completed == 1);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].owned
        && stack->items[0].rel != f.input
        && stack->items[0].rel->nrows == 4);
    for (uint32_t r = 0; r < 4; ++r)
        CHECK(stack->items[0].rel->columns[0][r] == (int64_t)a_values[r]+1);
    composed_close(&f);
    wl_plan_free(plan); wirelog_program_free(program);

    /* The generated union reaches CONCAT's owner-specific cleanup. Abort
     * before its commit and after its commit to cover both sides of the
     * existing binary finish/ACK ownership boundary. */
    program = wirelog_parse_string(union_text, &error); plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *union_plan =
        composed_find_relation(plan, "output");
    CHECK(union_plan);
    col_rel_t *a = composed_source("a", 1, a_values, 4, false);
    col_rel_t *b = composed_source("b", 1, b_values, 3, false);
    col_rel_t *union_registry[] = {a, b};
    composed_fixture(&f, a, 0);
    composed_begin(&f, union_plan, union_registry, 2, 0);
    composed_until_phase(&f,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD);
    CHECK(f.record.abort_supported);
    CHECK(!composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && !f.record.child_status.committed);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 2 && stack->items[0].owned
        && stack->items[1].owned && stack->items[0].rel != a
        && stack->items[1].rel != b
        && stack->items[0].rel->nrows == 4
        && stack->items[1].rel->nrows == 3);
    for (uint32_t r = 0; r < 4; ++r)
        CHECK(stack->items[0].rel->columns[0][r] == (int64_t)a_values[r]);
    for (uint32_t r = 0; r < 3; ++r)
        CHECK(stack->items[1].rel->columns[0][r] == (int64_t)b_values[r]);
    composed_close(&f);
    col_rel_destroy(b);

    a = composed_source("a", 1, a_values, 4, false);
    b = composed_source("b", 1, b_values, 3, false);
    union_registry[0] = a; union_registry[1] = b;
    composed_fixture(&f, a, 0);
    composed_begin(&f, union_plan, union_registry, 2, 0);
    composed_until_phase(&f,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD);
    for (unsigned turns = 0; !f.record.child_status.committed; ++turns) {
        CHECK(turns < 100000);
        composed_action(&f);
        if (f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
            composed_action(&f);
    }
    if (f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
        composed_action(&f);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD
        || f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ACK);
    CHECK(!composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel
        && stack->items[0].rel != a);
    composed_close(&f);
    col_rel_destroy(b);
    wl_plan_free(plan); wirelog_program_free(program);

    /* Generated ordinary JOIN cancellation cleans up the composed source
     * frame and leaves the registered inputs available for a full retry. */
    program = wirelog_parse_string(join_text, &error); plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *join = composed_find_relation(plan, "output");
    CHECK(join);
    a = composed_source("a", 1, a_values, 4, false);
    b = composed_source("b", 1, b_values, 3, false);
    col_rel_t *join_registry[] = {a, b};
    composed_fixture(&f, a, 0);
    composed_begin(&f, join, join_registry, 2, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 0);
    CHECK(f.record.abort_supported && !f.record.abort_requested);
    CHECK(!composed_abort_request(&f, 0));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.completed == 0);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 0);
    composed_retry_frame(&f);
    composed_begin(&f, join, join_registry, 2, 0);
    composed_run(&f);
    CHECK(!f.record.semantic_result && f.record.completed == join->op_count);
    composed_raw_parity(&f, join, 0);
    composed_close(&f);
    col_rel_destroy(b);
    wl_plan_free(plan); wirelog_program_free(program);
}

static void
composed_begin(fixture_t *f, const wl_plan_relation_t *plan,
    col_rel_t **registry, uint32_t count, uint32_t depth)
{
    f->session.rels = registry; f->session.nrels = count;
    f->session.rel_cap = count;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin(&f->record,
        plan, 0, plan->op_count, f->frame, &f->session, depth, f->operation,
        f->operation_id, f->funding, f->funding_id));
}

static void
test_composed_private_input_binding(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    static const double private_values[] = {17, 29, 43};
    static const double public_values[] = {80, 90};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *output = composed_find_relation(plan, "output");
    CHECK(output && output->op_count >= 3
        && output->ops[0].op == WL_PLAN_OP_VARIABLE);

    col_rel_t *private_input = composed_source("private", 1, private_values,
            3, false);
    free(private_input->name);
    private_input->name = NULL;
    col_rel_t *public_input = composed_source("input", 1, public_values, 2,
            false);
    col_rel_t *registry[] = {public_input};
    fixture_t f;
    composed_fixture(&f, private_input, 0);
    f.session.base.evaluation_control = f.operation;
    static const char *const names[] = {"input"};
    col_rel_t *relations[] = {private_input};
    wl_columnar_eval_private_bindings_t bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
        output, f.operation, f.operation_id, names, relations, 1));
    f.session.rels = registry;
    f.session.nrels = 1;
    f.session.rel_cap = 1;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, output, 0, output->op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id, &bindings));
    composed_run(&f);
    CHECK(!f.record.semantic_result
        && f.record.completed == output->op_count);
    CHECK(session_find_rel(&f.session, "input") == public_input);
    session_rel_free_hash(&f.session);
    CHECK(private_input->name == NULL);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 3
        && stack->items[0].rel->ncols == 1);
    for (uint32_t row = 0; row < 3; ++row)
        CHECK(stack->items[0].rel->columns[0][row]
            == (int64_t)private_values[row]);
    composed_close(&f);
    col_rel_destroy(public_input);
    wl_plan_free(plan);
    wirelog_program_free(program);

    static const char join_source[] =
        ".decl a(k:int64)\n"
        ".decl b(k:int64)\n"
        ".decl output(k:int64)\n"
        "output(k) :- a(k), b(k).\n";
    static const double a_values[] = {1, 3};
    static const double b_private_values[] = {1, 2};
    static const double b_public_values[] = {3, 3};
    wirelog_program_t *join_program = wirelog_parse_string(join_source,
            &error);
    wl_plan_t *join_plan = NULL;
    CHECK(join_program && !wl_plan_from_program(join_program, &join_plan));
    const wl_plan_relation_t *join = composed_find_relation(join_plan,
            "output");
    CHECK(join && join->op_count >= 3
        && join->ops[0].op == WL_PLAN_OP_VARIABLE
        && join->ops[1].op == WL_PLAN_OP_JOIN
        && !strcmp(join->ops[1].right_relation, "b"));
    col_rel_t *private_a = composed_source("private-a", 1, a_values, 2,
            false);
    col_rel_t *private_b = composed_source("private-b", 1,
            b_private_values, 2, false);
    col_rel_t *public_b = composed_source("b", 1, b_public_values, 2,
            false);
    free(private_a->name);
    private_a->name = NULL;
    free(private_b->name);
    private_b->name = NULL;
    col_rel_t *join_registry[] = {public_b};
    composed_fixture(&f, private_a, 0);
    f.session.base.evaluation_control = f.operation;
    static const char *const join_names[] = {"a", "b"};
    col_rel_t *join_relations[] = {private_a, private_b};
    wl_columnar_eval_private_bindings_t join_bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&join_bindings, &f.session,
        join, f.operation, f.operation_id, join_names, join_relations, 2));
    f.session.rels = join_registry;
    f.session.nrels = 1;
    f.session.rel_cap = 1;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, join, 0, join->op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id,
            &join_bindings));
    composed_run(&f);
    CHECK(!f.record.semantic_result
        && f.record.completed == join->op_count);
    CHECK(session_find_rel(&f.session, "b") == public_b);
    session_rel_free_hash(&f.session);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 1
        && stack->items[0].rel->columns[0][0] == 1);
    composed_close(&f);
    col_rel_destroy(private_b);
    col_rel_destroy(public_b);

    static const double a_public_values[] = {2, 2};
    static const double b_public_mixed_values[] = {1, 2};
    col_rel_t *private_a_mixed = composed_source("private-a", 1, a_values,
            2, false);
    free(private_a_mixed->name);
    private_a_mixed->name = NULL;
    col_rel_t *public_a = composed_source("a", 1, a_public_values, 2,
            false);
    col_rel_t *public_b_mixed = composed_source("b", 1,
            b_public_mixed_values, 2, false);
    col_rel_t *mixed_registry[] = {public_a, public_b_mixed};
    composed_fixture(&f, private_a_mixed, 0);
    f.session.base.evaluation_control = f.operation;
    static const char *const mixed_names[] = {"a"};
    col_rel_t *mixed_relations[] = {private_a_mixed};
    wl_columnar_eval_private_bindings_t mixed_bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&mixed_bindings,
        &f.session, join, f.operation, f.operation_id, mixed_names,
        mixed_relations, 1));
    f.session.rels = mixed_registry;
    f.session.nrels = 2;
    f.session.rel_cap = 2;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, join, 0, join->op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id,
            &mixed_bindings));
    composed_run(&f);
    CHECK(!f.record.semantic_result
        && f.record.completed == join->op_count);
    CHECK(session_find_rel(&f.session, "a") == public_a);
    CHECK(session_find_rel(&f.session, "b") == public_b_mixed);
    session_rel_free_hash(&f.session);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 1
        && stack->items[0].rel->columns[0][0] == 1);
    composed_close(&f);
    col_rel_destroy(public_a);
    col_rel_destroy(public_b_mixed);

    static const double private_delta_a_values[] = {1};
    static const double private_delta_b_values[] = {1};
    static const double private_full_b_values[] = {2};
    static const double public_delta_values[] = {3};
    col_rel_t *private_full_a = composed_source("private-full-a", 1,
            a_values, 2, false);
    col_rel_t *private_delta_a = composed_source("private-delta-a", 1,
            private_delta_a_values, 1, false);
    col_rel_t *private_full_b = composed_source("private-full-b", 1,
            private_full_b_values, 1, false);
    col_rel_t *private_delta_b = composed_source("private-delta-b", 1,
            private_delta_b_values, 1, false);
    col_rel_t *public_full_a = composed_source("a", 1, a_values, 2, false);
    col_rel_t *public_full_b = composed_source("b", 1,
            b_private_values, 2, false);
    col_rel_t *public_a_delta = composed_source("$d$a", 1,
            public_delta_values, 1, false);
    col_rel_t *public_b_delta = composed_source("$d$b", 1,
            public_delta_values, 1, false);
    free(private_full_a->name);
    private_full_a->name = NULL;
    free(private_delta_a->name);
    private_delta_a->name = NULL;
    free(private_full_b->name);
    private_full_b->name = NULL;
    free(private_delta_b->name);
    private_delta_b->name = NULL;
    wl_plan_relation_t delta_plan = *join;
    wl_plan_op_t *delta_ops = malloc((size_t)join->op_count
            * sizeof(*delta_ops));
    CHECK(delta_ops);
    memcpy(delta_ops, join->ops,
        (size_t)join->op_count * sizeof(*delta_ops));
    delta_ops[0].delta_mode = WL_DELTA_FORCE_DELTA;
    delta_ops[1].delta_mode = WL_DELTA_FORCE_DELTA;
    delta_plan.ops = delta_ops;
    col_rel_t *delta_registry[] = {public_full_a, public_full_b,
                                   public_a_delta, public_b_delta};
    composed_fixture(&f, private_full_a, 0);
    f.session.base.evaluation_control = f.operation;
    f.session.delta_seeded = true;
    static const char *const delta_names[] = {"a", "$d$a", "b", "$d$b"};
    col_rel_t *delta_relations[] = {private_full_a, private_delta_a,
                                    private_full_b, private_delta_b};
    wl_columnar_eval_private_bindings_t delta_bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&delta_bindings,
        &f.session, &delta_plan, f.operation, f.operation_id, delta_names,
        delta_relations, 4));
    f.session.rels = delta_registry;
    f.session.nrels = 4;
    f.session.rel_cap = 4;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, &delta_plan, 0, delta_plan.op_count, f.frame,
            &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id,
            &delta_bindings));
    composed_run(&f);
    CHECK(!f.record.semantic_result
        && f.record.completed == join->op_count);
    CHECK(session_find_rel(&f.session, "$d$a") == public_a_delta);
    CHECK(session_find_rel(&f.session, "$d$b") == public_b_delta);
    session_rel_free_hash(&f.session);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 1
        && stack->items[0].rel->columns[0][0] == 1);
    composed_close(&f);
    col_rel_destroy(private_delta_a);
    col_rel_destroy(private_full_b);
    col_rel_destroy(private_delta_b);
    col_rel_destroy(public_full_a);
    col_rel_destroy(public_full_b);
    col_rel_destroy(public_a_delta);
    col_rel_destroy(public_b_delta);
    free(delta_ops);
    wl_plan_free(join_plan);
    wirelog_program_free(join_program);
}

static void cancel_after_terminal_exchange(wl_evaluation_control_t *, uint64_t);

static void
reject_composed_recursive_manifest(const wl_plan_relation_t *original,
    wl_plan_op_t *ops, const char *const *names, col_rel_t **relations)
{
    wl_plan_relation_t changed = *original;
    changed.ops = ops;
    fixture_t f;
    composed_fixture(&f, source_new(2, 0), 0);
    f.session.base.evaluation_control = f.operation;
    wl_columnar_eval_private_bindings_t bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
        &changed, f.operation, f.operation_id, names, relations, 2));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, &changed, 0, changed.op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id, &bindings));
    composed_run(&f);
    CHECK(f.record.semantic_result == ENOTSUP
        && !f.record.completed
        && !wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
    composed_close(&f);
}

static void
test_composed_recursive_terminal_exchange(void)
{
    static const char source[] =
        ".decl edge(x:int64, y:int64)\n"
        ".decl path(x:int64, y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,z) :- path(x,y), edge(y,z).\n";
    static const double edge_values[] = {1, 2, 2, 3, 2, 3};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *path = composed_find_relation(plan, "path");
    CHECK(path && path->op_count == 7
        && path->ops[5].op == WL_PLAN_OP_CONSOLIDATE
        && path->ops[6].op == WL_PLAN_OP_EXCHANGE);

    col_rel_t *edge = composed_source("private-edge", 2, edge_values, 3,
            false);
    col_rel_t *seed = composed_source("private-path", 2, NULL, 0, false);
    free(edge->name); edge->name = NULL;
    free(seed->name); seed->name = NULL;
    col_rel_t *public_edge = composed_source("edge", 2, NULL, 0, false);
    col_rel_t *registry[] = {public_edge};
    fixture_t f;
    composed_fixture(&f, source_new(2, 0), 0);
    f.session.base.evaluation_control = f.operation;
    f.session.rels = registry; f.session.nrels = 1; f.session.rel_cap = 1;
    static const char *const names[] = {"edge", "path"};
    col_rel_t *relations[] = {edge, seed};
    wl_columnar_eval_private_bindings_t bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
        path, f.operation, f.operation_id, names, relations, 2));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, path, 0, path->op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id, &bindings));
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 6);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1);
    col_rel_t *before_exchange = stack->items[0].rel;
    bool owned_before_exchange = stack->items[0].owned;
    composed_action(&f);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        && f.record.cursor == 7 && f.record.completed == 7
        && stack->top == 1 && stack->items[0].rel == before_exchange
        && stack->items[0].owned == owned_before_exchange);
    composed_run(&f);
    CHECK(!f.record.semantic_result && f.record.completed == path->op_count);
    CHECK(session_find_rel(&f.session, "edge") == public_edge);
    session_rel_free_hash(&f.session);
    CHECK(stack->top == 1 && stack->items[0].owned
        && stack->items[0].rel->nrows == 2
        && stack->items[0].rel->ncols == 2);
    CHECK(stack->items[0].rel->columns[0][0] == 1
        && stack->items[0].rel->columns[1][0] == 2
        && stack->items[0].rel->columns[0][1] == 2
        && stack->items[0].rel->columns[1][1] == 3);
    composed_close(&f);
    col_rel_destroy(edge); col_rel_destroy(seed); col_rel_destroy(public_edge);

    /* The same plan with an empty private EDB produces an empty candidate;
     * the terminal bridge still advances exactly once. */
    edge = composed_source("private-edge", 2, NULL, 0, false);
    seed = composed_source("private-path", 2, NULL, 0, false);
    free(edge->name); edge->name = NULL;
    free(seed->name); seed->name = NULL;
    composed_fixture(&f, source_new(2, 0), 0);
    f.session.base.evaluation_control = f.operation;
    relations[0] = edge; relations[1] = seed;
    memset(&bindings, 0, sizeof(bindings));
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
        path, f.operation, f.operation_id, names, relations, 2));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, path, 0, path->op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id, &bindings));
    composed_run(&f);
    CHECK(!f.record.semantic_result && f.record.completed == path->op_count);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 0);
    composed_close(&f);
    col_rel_destroy(edge); col_rel_destroy(seed);

    /* A stop arriving between the no-op and its post-action poll records the
     * completed node once while preserving the completed candidate stack. */
    edge = composed_source("private-edge", 2, edge_values, 3, false);
    seed = composed_source("private-path", 2, NULL, 0, false);
    free(edge->name); edge->name = NULL;
    free(seed->name); seed->name = NULL;
    relations[0] = edge; relations[1] = seed;
    composed_fixture(&f, source_new(2, 0), 0);
    f.session.base.evaluation_control = f.operation;
    memset(&bindings, 0, sizeof(bindings));
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
        path, f.operation, f.operation_id, names, relations, 2));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, path, 0, path->op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id, &bindings));
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 6);
    wl_columnar_eval_plan_test_after_terminal_exchange =
        cancel_after_terminal_exchange;
    composed_action(&f);
    wl_columnar_eval_plan_test_after_terminal_exchange = NULL;
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.completed == path->op_count && f.record.cursor == 7);
    composed_run(&f);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 2);
    composed_close(&f);
    col_rel_destroy(edge); col_rel_destroy(seed);

    /* EXCHANGE metadata drift is rejected during full-plan preflight, before
     * any input is consumed or installed in the public registry. */
    edge = composed_source("private-edge", 2, edge_values, 3, false);
    seed = composed_source("private-path", 2, NULL, 0, false);
    free(edge->name); edge->name = NULL;
    free(seed->name); seed->name = NULL;
    wl_plan_relation_t changed = *path;
    wl_plan_op_t changed_ops[7];
    memcpy(changed_ops, path->ops, sizeof(changed_ops));
    wl_plan_op_exchange_t changed_exchange =
        *(const wl_plan_op_exchange_t *)path->ops[6].opaque_data;
    changed_exchange.num_workers = 1;
    changed_ops[6].opaque_data = &changed_exchange;
    changed.ops = changed_ops;
    relations[0] = edge; relations[1] = seed;
    composed_fixture(&f, source_new(2, 0), 0);
    f.session.base.evaluation_control = f.operation;
    memset(&bindings, 0, sizeof(bindings));
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
        &changed, f.operation, f.operation_id, names, relations, 2));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, &changed, 0, changed.op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id, &bindings));
    composed_run(&f);
    CHECK(f.record.semantic_result == ENOTSUP
        && !wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
    composed_close(&f);

    /* Moving EXCHANGE ahead of terminal CONSOLIDATE is rejected during
    * preflight, before any operator completes or changes the stack. */
    memcpy(changed_ops, path->ops, sizeof(changed_ops));
    wl_plan_op_t misplaced_exchange = changed_ops[6];
    changed_ops[6] = changed_ops[5];
    changed_ops[5] = misplaced_exchange;
    reject_composed_recursive_manifest(path, changed_ops, names, relations);

    /* Every behavior-bearing operator field is pinned, not merely the final
     * EXCHANGE descriptor. Changed MAP and JOIN metadata reject preflight. */
    memcpy(changed_ops, path->ops, sizeof(changed_ops));
    changed_ops[1].materialized = true;
    reject_composed_recursive_manifest(path, changed_ops, names, relations);
    memcpy(changed_ops, path->ops, sizeof(changed_ops));
    static uint8_t right_filter = 1;
    changed_ops[3].right_filter_expr.data = &right_filter;
    changed_ops[3].right_filter_expr.size = sizeof(right_filter);
    reject_composed_recursive_manifest(path, changed_ops, names, relations);
    col_rel_destroy(edge); col_rel_destroy(seed);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
cancel_after_terminal_exchange(wl_evaluation_control_t *control,
    uint64_t expected_id)
{
    CHECK(control && wl_evaluation_control_active_attempt_id(control)
        == expected_id);
    wl_evaluation_control_request_cancel(control);
}

static void
composed_close(fixture_t *f)
{
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY);
    int result = f->record.semantic_result;
    CHECK(!composed_host_turn(f, true));
    CHECK(!f->record.composed_mode && !f->record.identity);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    CHECK(!f->session.cleanup_active_count &&
        !f->session.cleanup_reserved_bytes);
    uint64_t active = wl_evaluation_control_active_attempt_id(f->operation);
    if (active && active != f->operation_id) result = 0;
    if (active) CHECK(wl_evaluation_control_finish(f->operation,
            &f->operation_owner, result, 0) == result);
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f->operation, &f->operation_owner));
    CHECK(!wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->operation);
    wl_evaluation_control_release(f->funding);
    col_rel_destroy(f->input);
}

static wl_columnar_memory_governor_ref_t *
composed_governor(fixture_t *f)
{
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    wl_columnar_memory_governor_ref_t *governor = join_governor(f, 1u << 24);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f->session, &f->frame));
    return governor;
}

/* Retire the genuine original record before resetting its operation. The
 * immutable registry descriptors survive both frames and the canonical retry. */
static void
composed_retry_frame(fixture_t *f)
{
    int result = f->record.semantic_result;
    CHECK(!composed_host_turn(f, true));
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    uint64_t active = wl_evaluation_control_active_attempt_id(f->operation);
    if (active && active != f->operation_id) result = 0;
    if (active) CHECK(wl_evaluation_control_finish(f->operation,
            &f->operation_owner, result, 0) == result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->operation, &report));
    if (report.attempt_id != f->operation_id) CHECK(!report.consumed);
    CHECK(!wl_evaluation_control_reset(f->operation));
    CHECK(!wl_evaluation_control_begin(f->operation, &f->operation_owner));
    f->operation_id = wl_evaluation_control_active_attempt_id(f->operation);
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->funding);
    f->funding = control_new(0, &f->funding_owner);
    f->funding_id = wl_evaluation_control_active_attempt_id(f->funding);
    wl_columnar_eval_plan_numeric_attempt_init(&f->record);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f->session, &f->frame));
    f->pending_protocol = 0;
}

static col_rel_t *
composed_source(const char *name, uint32_t width, const double *values,
    uint32_t rows, bool floating)
{
    col_rel_t *rel = source_new(width, 0);
    CHECK(!wl_columnar_relation_rename_checked(rel, name));
    wirelog_column_type_t *types = calloc(width ? width : 1, sizeof(*types));
    int64_t *row = calloc(width ? width : 1, sizeof(*row));
    CHECK(types && row);
    for (uint32_t c = 0; c < width; ++c)
        types[c] = floating ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
    if (width) CHECK(!col_rel_set_column_types(rel, types, width));
    for (uint32_t r = 0; r < rows; ++r) {
        for (uint32_t c = 0; c < width; ++c)
            row[c] = floating ? wl_columnar_float_to_bits(values[r] + c)
                : (int64_t)values[r] + c;
        CHECK(!col_rel_append_row(rel, row));
    }
    if (floating && width && rows && signbit(values[0]) && values[0] == 0.0)
        rel->columns[0][0] = (int64_t)UINT64_C(0x8000000000000000);
    free(types); free(row);
    return rel;
}

static void
composed_fixture(fixture_t *f, col_rel_t *input, uint64_t limit)
{
    source_fixture(f, input->ncols, 0, limit);
    col_rel_destroy(f->input); f->input = input;
}

static void
composed_literal(fixture_t *f, const double *values, uint32_t rows,
    uint32_t width, bool floating)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack->top == 1 && stack->items[0].owned &&
        !stack->items[0].is_delta && !stack->items[0].seg_boundaries &&
        !stack->items[0].seg_count);
    col_rel_t *out = stack->items[0].rel;
    CHECK(out->nrows == rows && out->ncols == width && !out->timestamps);
    CHECK(out->sorted_nrows == rows && out->run_count == (rows != 0));
    if (rows) CHECK(out->run_ends[0] == rows);
    for (uint32_t c = 0; c < width; ++c) {
        CHECK(out->column_types && out->column_types[c] ==
            (floating ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64));
        char name[32]; snprintf(name, sizeof(name), "col%u", c);
        CHECK(!strcmp(out->col_names[c], name));
        for (uint32_t r = 0; r < rows; ++r)
            CHECK(out->columns[c][r] == (floating
                ? wl_columnar_float_to_bits(values[r] + c)
                : (int64_t)values[r] + c));
    }
}

/* Cloned raw session and full original body, including CONCAT and final
 * consolidation; never substitute individual per-rule relations. */
static void
composed_raw_parity(fixture_t *f, const wl_plan_relation_t *body,
    uint32_t initial_depth)
{
    wl_col_session_t raw = {0};
    wl_mem_ledger_init(&raw.mem_ledger, 0);
    raw.nrels = f->session.nrels; raw.rel_cap = raw.nrels;
    raw.rels = calloc(raw.nrels ? raw.nrels : 1, sizeof(*raw.rels));
    CHECK(raw.rels);
    for (uint32_t i = 0; i < raw.nrels; ++i)
        CHECK(!col_rel_deep_copy(f->session.rels[i], &raw.rels[i], NULL));
    eval_stack_t stack = {0};
    for (uint32_t i = 0; i < initial_depth; ++i)
        CHECK(!eval_stack_push(&stack, raw.rels[i], false));
    CHECK(!col_eval_relation_plan(body, &stack, &raw));
    eval_stack_t *actual = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(actual->top == 1 && stack.top == 1);
    same_relation(actual->items[0].rel, stack.items[0].rel);
    CHECK(actual->items[0].is_delta == stack.items[0].is_delta);
    /* Raw empty CONS may keep a one-run cache; controlled empty cache is
     * explicitly0 and checked against its literal contract above. */
    if (actual->items[0].rel->nrows) {
        CHECK(actual->items[0].rel->sorted_nrows ==
            stack.items[0].rel->sorted_nrows);
        CHECK(actual->items[0].rel->run_count == stack.items[0].rel->run_count);
    }
    CHECK(!eval_stack_drain(&stack));
    for (uint32_t i = 0; i < raw.arr_count; ++i) {
        CHECK(!raw.arr_entries[i].pin_count);
        free(raw.arr_entries[i].rel_name); free(raw.arr_entries[i].key_cols);
        arr_free_contents(&raw.arr_entries[i].arr);
        col_arr_detach_memory_governor(&raw.arr_entries[i].arr);
    }
    free(raw.arr_entries); session_rel_free_hash(&raw);
    for (uint32_t i = 0; i < raw.nrels; ++i) col_rel_destroy(raw.rels[i]);
    free(raw.rels);
    CHECK(!f->session.rel_hash_nbuckets && !f->session.arr_count);
}

static void
test_composed_generated_union(void)
{
    const double ints_a[] = {3, 1, 3, -2}, ints_b[] = {1, 4, 0};
    const double floats_a[] = {-0.0, -2.5, 1.5, 1.5};
    const double floats_b[] = {0.0, 4.5, -2.5}, floats_c[] = {7, 4.5, -3};
    const double int_values[] = {-2, 0, 1, 3, 4};
    const double float_values[] = {-3, -2.5, 0.0, 1.5, 4.5, 7};
    const unsigned int_masks[] = {1, 2, 3, 1, 2};
    const unsigned float_masks[] = {4, 3, 3, 1, 6, 4};
    for (unsigned floating = 0; floating < 2; ++floating) {
        const char *text = floating
            ? ".decl a(x:float)\n.decl b(x:float)\n.decl c(x:float)\n"
            ".decl output(x:float)\noutput(x):-a(x).\n"
            "output(x):-b(x).\noutput(x):-c(x).\n"
            : ".decl a(x:int64)\n.decl b(x:int64)\n.decl output(x:int64)\n"
            "output(x):-a(x).\noutput(x):-b(x).\n";
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        wl_plan_relation_t body = probe_generated_body(plan);
        unsigned variables = 0, concats = 0;
        for (uint32_t i = 0; i < body.op_count; ++i) {
            variables += body.ops[i].op == WL_PLAN_OP_VARIABLE;
            concats += body.ops[i].op == WL_PLAN_OP_CONCAT;
        }
        CHECK(variables == (floating ? 3u : 2u) && concats + 1 == variables);
        CHECK(body.ops[body.op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
        for (unsigned empty = 0; empty < (1u << variables); ++empty)
            for (unsigned governed = 0; governed < 2; ++governed)
                for (unsigned prebound = 0; prebound < 2; ++prebound) {
                    col_rel_t *a = composed_source("a", 1,
                            floating ? floats_a : ints_a, empty & 1 ? 0 : 4,
                            floating);
                    col_rel_t *b = composed_source("b", 1,
                            floating ? floats_b : ints_b, empty & 2 ? 0 : 3,
                            floating);
                    col_rel_t *c = composed_source("c", 1, floats_c,
                            empty & 4 ? 0 : 3, floating);
                    fixture_t f; composed_fixture(&f, a, 0);
                    wl_columnar_memory_governor_ref_t *governor = governed
                        ? composed_governor(&f) : NULL;
                    col_rel_t *registry[] = {a, b, c};
                    wl_plan_relation_t slice = body;
                    if (prebound) {
                        CHECK(body.ops[0].op == WL_PLAN_OP_VARIABLE);
                        CHECK(!eval_stack_push(
                                wl_columnar_eval_stack_cleanup_stack(
                                    f.frame), a, false));
                        ++slice.ops; --slice.op_count;
                    }
                    composed_begin(&f, &slice, registry, variables, prebound);
                    composed_run(&f);
                    CHECK(!f.record.semantic_result &&
                        f.record.completed == slice.op_count &&
                        f.record.runtime_depth == 1);
                    double expected[6]; uint32_t count = 0;
                    for (unsigned i = 0; i < (floating ? 6u : 5u); ++i)
                        if ((floating ? float_masks[i] : int_masks[i]) & ~empty)
                            expected[count++] =
                                floating ? float_values[i] : int_values[i];
                    composed_literal(&f, expected, count, 1, floating);
                    composed_raw_parity(&f, &slice, prebound);
                    composed_close(&f); col_rel_destroy(b); col_rel_destroy(c);
                    if (governor) {
                        CHECK(!wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(governor)));
                        wl_columnar_memory_governor_ref_release(governor);
                    }
                }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

static void
test_composed_lftj_stopped_cleanup(void)
{
    wl_plan_relation_t body;
    wl_plan_op_t op;
    wl_plan_op_lftj_t metadata;
    char *names[3];
    uint32_t key_cols[3];
    col_rel_t *relations[3];
    fixture_t f;

    /* A work-limit stop can arrive while LFTJ owns source readers, before its
     * next semantic action observes the stop. Cleanup is host-funded and
     * drains at most one child release action per call. */
    composed_lftj_fixture(&f, UINT64_C(1) << 20, &body, &op, &metadata,
        names, key_cols, relations);
    wl_columnar_eval_plan_numeric_attempt_t before_record = f.record;
    CHECK(composed_cleanup_lftj_turn(&f, 0, 0) == EBUSY);
    CHECK(!memcmp(&before_record, &f.record, sizeof(before_record)));
    composed_lftj_until_reader(&f);
    wl_columnar_lftj_attempt_t before_lftj = f.record.lftj;
    before_record = f.record;
    CHECK(composed_cleanup_lftj_turn(&f, 383, 0)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!memcmp(&before_lftj, &f.record.lftj, sizeof(before_lftj))
        && !memcmp(&before_record, &f.record, sizeof(before_record)));
    CHECK(composed_cleanup_lftj_turn(&f, 0, 1) == EINVAL);
    CHECK(!memcmp(&before_record, &f.record, sizeof(before_record)));
    CHECK(composed_cleanup_lftj_turn(&f, 0, 0) == EBUSY);
    CHECK(!memcmp(&before_record, &f.record, sizeof(before_record)));
    CHECK(wl_evaluation_control_charge_attempt(f.operation,
        f.operation_id, UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    for (unsigned turns = 0; f.record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE; ++turns) {
        CHECK(turns < 100 && (f.record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD
            || f.record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE));
        uint32_t readers_before = composed_lftj_reader_count(&f.record.lftj);
        CHECK(!composed_cleanup_lftj_turn(&f, 0, 0));
        uint32_t readers_after = composed_lftj_reader_count(&f.record.lftj);
        CHECK(readers_before >= readers_after
            && readers_before - readers_after <= 1);
    }
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && f.record.lftj.semantic_result ==
        WL_EVALUATION_CONTROL_WORK_LIMIT
        && !session_find_rel(&f.session, "result"));
    composed_lftj_finish_stop(&f, WL_EVALUATION_CONTROL_WORK_LIMIT,
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    for (uint32_t i = 0; i < 3; ++i) col_rel_destroy(relations[i]);

    /* Abort-request cancellation and direct cancellation both use the same
     * cleanup drain; the LFTJ child has not yet polled this stop. */
    composed_lftj_fixture(&f, 0, &body, &op, &metadata, names, key_cols,
        relations);
    composed_lftj_until_reader(&f);
    CHECK(!composed_abort_request(&f, 0));
    for (unsigned turns = 0; f.record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE; ++turns) {
        CHECK(turns < 100);
        CHECK(!composed_cleanup_lftj_turn(&f, 0, 0));
    }
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
    composed_lftj_finish_stop(&f, WL_EVALUATION_CONTROL_CANCELLED,
        WL_EVALUATION_CONTROL_CANCELLED);
    for (uint32_t i = 0; i < 3; ++i) col_rel_destroy(relations[i]);

    /* A direct cancellation is copied to the child before its first abort
     * action, even while that child still owns readers. */
    composed_lftj_fixture(&f, 0, &body, &op, &metadata, names, key_cols,
        relations);
    composed_lftj_until_reader(&f);
    wl_evaluation_control_request_cancel(f.operation);
    uint32_t readers_before = composed_lftj_reader_count(&f.record.lftj);
    CHECK(!composed_cleanup_lftj_turn(&f, 0, 0));
    CHECK(f.record.lftj.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && composed_lftj_reader_count(&f.record.lftj) < readers_before);
    for (unsigned turns = 0; f.record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE; ++turns) {
        CHECK(turns < 100);
        CHECK(!composed_cleanup_lftj_turn(&f, 0, 0));
    }
    composed_lftj_finish_stop(&f, WL_EVALUATION_CONTROL_CANCELLED,
        WL_EVALUATION_CONTROL_CANCELLED);
    for (uint32_t i = 0; i < 3; ++i) col_rel_destroy(relations[i]);

    /* A completed LFTJ result is still private to the cleanup frame. A stop
     * before the parent advances to DONE makes cleanup discard that result. */
    composed_lftj_fixture(&f, 0, &body, &op, &metadata, names, key_cols,
        relations);
    for (unsigned turns = 0; f.record.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE; ++turns) {
        CHECK(turns < 10000 && f.record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
        composed_action(&f);
    }
    CHECK(f.record.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].owned);
    CHECK(!session_find_rel(&f.session, "result"));
    f.record.semantic_result = EDOM;
    f.record.lftj.semantic_result = EINVAL;
    wl_evaluation_control_request_cancel(f.operation);
    CHECK(!composed_cleanup_lftj_turn(&f, 0, 0));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        && f.record.semantic_result == EDOM
        && f.record.lftj.semantic_result == EINVAL
        && stack->top == 1 && stack->items[0].owned);
    CHECK(!session_find_rel(&f.session, "result"));
    composed_lftj_finish_stop(&f, WL_EVALUATION_CONTROL_CANCELLED,
        EDOM);
    for (uint32_t i = 0; i < 3; ++i) col_rel_destroy(relations[i]);
}

static void
test_composed_nested_and_wide(void)
{
    const double av[] = {3, 1, 3, -2}, bv[] = {1, 4, 0}, cv[] = {7, 4, -3};
    const double expected[] = {-3, -2, 0, 1, 3, 4, 7};
    const uint32_t widths[] = {0, 1, 17, 129};
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned prebound = 0; prebound < 2; ++prebound)
            for (unsigned governed = 0; governed < 2; ++governed) {
                col_rel_t *a = composed_source("a", widths[w], av, 4, false);
                col_rel_t *b = composed_source("b", widths[w], bv, 3, false);
                col_rel_t *c = composed_source("c", widths[w], cv, 3, false);
                fixture_t f; composed_fixture(&f, a, 0);
                wl_columnar_memory_governor_ref_t *governor = governed
                    ? composed_governor(&f) : NULL;
                wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                                       .relation_name = "a"},
                                      {.op = WL_PLAN_OP_VARIABLE,
                                       .relation_name = "b"},
                                      {.op = WL_PLAN_OP_VARIABLE,
                                       .relation_name = "c"},
                                      {.op = WL_PLAN_OP_CONCAT},
                                      {.op = WL_PLAN_OP_CONCAT},
                                      {.op = WL_PLAN_OP_CONSOLIDATE},
                                      {.op = WL_PLAN_OP_CONSOLIDATE}};
                wl_plan_relation_t body = {.ops = ops + (prebound ? 3 : 0),
                                           .op_count = prebound ? 4 : 7};
                col_rel_t *registry[] = {a, b, c};
                if (prebound) {
                    eval_stack_t *stack =
                        wl_columnar_eval_stack_cleanup_stack(f.frame);
                    for (unsigned i = 0; i < 3; ++i)
                        CHECK(!eval_stack_push(stack, registry[i], false));
                }
                composed_begin(&f, &body, registry, 3, prebound ? 3 : 0);
                composed_run(&f);
                CHECK(!f.record.semantic_result &&
                    f.record.completed == body.op_count);
                composed_literal(&f, expected, widths[w] ? 7 : 1, widths[w],
                    false);
                composed_raw_parity(&f, &body, prebound ? 3 : 0);
                composed_close(&f); col_rel_destroy(b); col_rel_destroy(c);
                if (governor) {
                    CHECK(!wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(governor)));
                    wl_columnar_memory_governor_ref_release(governor);
                }
            }
}

static void
test_composed_branch_children(void)
{
    const char *rules[] = {
        "output(x+1):-a(x),x>=0.\noutput(x+1):-b(x),x>=0.\n",
        "output(x+y):-a(x),right(y).\noutput(x+y):-b(x),right(y).\n",
        "output(x):-a(x),right(x),mid(x).\n"
        "output(x):-b(x),right(x),mid(x).\n",
        "output(x):-a(x),!right(x).\noutput(x):-b(x),!right(x).\n",
        "output(sum(x)):-a(x).\noutput(sum(x)):-b(x).\n"
    };
    const double av[] = {3, 1, 3, -2}, bv[] = {1, 4, 0};
    const double literals[][10] = {{1, 2, 4, 5},
                                   {-2, -1, 0, 1, 2, 3, 4, 5, 7, 8},
                                   {0, 1, 4}, {-2, 3}, {5}};
    const unsigned counts[] = {4, 10, 3, 2, 1};
    for (unsigned shape = 0; shape < 5; ++shape) {
        char text[1024];
        snprintf(text, sizeof(text), ".decl a(x:int64)\n.decl b(x:int64)\n"
            ".decl right(y:int64)\n.decl mid(z:int64)\n"
            ".decl output(x:int64)\n%s", rules[shape]);
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        CHECK(program);
        if (shape == 2) {
            wl_sip_stats_t stats = {0}; CHECK(!wl_sip_apply(program, &stats));
            CHECK(stats.semijoins_inserted == 2);
        }
        wl_plan_t *plan = NULL; CHECK(!wl_plan_from_program(program, &plan));
        wl_plan_relation_t body = probe_generated_body(plan);
        unsigned variables = 0, concats = 0, selected = 0;
        for (uint32_t i = 0; i < body.op_count; ++i) {
            variables += body.ops[i].op == WL_PLAN_OP_VARIABLE;
            concats += body.ops[i].op == WL_PLAN_OP_CONCAT;
            selected += body.ops[i].op == (shape == 0 ? WL_PLAN_OP_FILTER
                : shape == 1 ? WL_PLAN_OP_JOIN : shape ==
                2 ? WL_PLAN_OP_SEMIJOIN
                : shape == 3 ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_REDUCE);
        }
        CHECK(variables == 2 && concats == 1 && selected == 2);
        CHECK(body.ops[body.op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
        for (unsigned governed = 0; governed < 2; ++governed) {
            col_rel_t *a = composed_source("a", 1, av, 4, false);
            col_rel_t *b = composed_source("b", 1, bv, 3, false);
            col_rel_t *right = composed_source("right", 1, bv, 3, false);
            col_rel_t *mid = composed_source("mid", 1, bv, 3, false);
            fixture_t f; composed_fixture(&f, a, 0);
            wl_columnar_memory_governor_ref_t *governor = governed
                ? composed_governor(&f) : NULL;
            col_rel_t *registry[] = {a, b, right, mid};
            composed_begin(&f, &body, registry, 4, 0); composed_run(&f);
            CHECK(!f.record.semantic_result &&
                f.record.completed == body.op_count);
            composed_literal(&f, literals[shape], counts[shape], 1, false);
            composed_raw_parity(&f, &body, 0);
            composed_close(&f); col_rel_destroy(b); col_rel_destroy(right);
            col_rel_destroy(mid);
            if (governor) {
                CHECK(!wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)));
                wl_columnar_memory_governor_ref_release(governor);
            }
        }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

static void
test_composed_grammar(void)
{
    const double av[] = {3, 1, 3, -2};
    for (unsigned shape = 0; shape < 4; ++shape) {
        wl_plan_op_t ops[33] = {0};
        unsigned count = shape == 3 ? 33 : 2;
        for (unsigned i = 0; i < count; ++i) {
            ops[i].op = WL_PLAN_OP_VARIABLE; ops[i].relation_name = "a";
        }
        if (!shape) ops[1].op = (wl_plan_op_type_t)999;
        if (shape == 1) ops[1].op = WL_PLAN_OP_CONCAT;
        wl_plan_relation_t body = {.ops = ops, .op_count = count};
        fixture_t f;
        composed_fixture(&f, composed_source("a", 1, av, 4, false), 0);
        col_rel_t *registry[] = {f.input};
        composed_begin(&f, &body, registry, 1, 0); composed_run(&f);
        CHECK(f.record.semantic_result == (!shape ? ENOTSUP
            : shape == 3 ? ENOBUFS : EINVAL));
        CHECK(!f.record.completed && !f.record.runtime_depth
            && !f.record.source_initialized && !f.record.source_full
            && !f.record.source_selected && !f.record.source_output);
        CHECK(!wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
        composed_close(&f);
    }
}

static void
test_composed_later_failure_and_retry(void)
{
    const double av[] = {3, 1, 3, -2}, bv[] = {1, 4, 0};
    wl_plan_op_t missing_ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                                   .relation_name = "a"},
                                  {.op = WL_PLAN_OP_MAP, .project_count = 1},
                                  {.op = WL_PLAN_OP_VARIABLE,
                                   .relation_name = "missing"},
                                  {.op = WL_PLAN_OP_CONCAT},
                                  {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t missing = {.ops = missing_ops, .op_count = 5};
    fixture_t f; composed_fixture(&f, composed_source("a", 1, av, 4, false), 0);
    col_rel_t *registry[] = {f.input};
    composed_begin(&f, &missing, registry, 1, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 2);
    join_snapshot_t snapshot = join_snapshot(&f);
    CHECK(snapshot.owned);
    composed_run(&f);
    CHECK(f.record.semantic_result == ENOENT && f.record.completed == 2);
    join_snapshot_check(&f, &snapshot); col_rel_destroy(snapshot.contents);
    composed_close(&f);

    /* Stop at a later, actually owned empty-source candidate; retain it
     * through refusal, then complete cleanup after cancellation with fresh
     * host grants. Original lower entry never becomes candidate cleanup. */
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
                          {.op = WL_PLAN_OP_VARIABLE, .relation_name = "b",
                           .delta_mode = WL_DELTA_FORCE_EMPTY},
                          {.op = WL_PLAN_OP_CONCAT},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t body = {.ops = ops, .op_count = 4};
    for (unsigned fault = 0; fault < 4; ++fault) {
        col_rel_t *a = composed_source("a", 1, av, 4, false);
        col_rel_t *b = composed_source("b", 1, bv, 3, false);
        composed_fixture(&f, a, 0);
        wl_columnar_memory_governor_ref_t *governor = composed_governor(&f);
        col_rel_t *sources[] = {a, b};
        composed_begin(&f, &body, sources, 2, 0);
        const wl_columnar_eval_plan_numeric_phase_t stops[] = {
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_FULL,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INIT,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL
        };
        composed_until(&f, stops[fault], 1);
        snapshot = join_snapshot(&f);
        wl_columnar_source_access_reader_t reader = {0};
        col_rel_t *candidate =
            wl_columnar_eval_stack_cleanup_result(f.frame)->rel;
        if (fault == 3) {
            CHECK(candidate && !col_rel_source_reader_acquire(candidate,
                &reader));
        }
        wl_evaluation_control_request_cancel(f.operation);
        if (fault == 3) {
            composed_until(&f,
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT, 1);
            composed_action(&f);
            CHECK(f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
                && f.record.protocol_result == EBUSY
                && wl_columnar_eval_stack_cleanup_result(f.frame)->rel ==
                candidate);
            CHECK(!col_rel_source_reader_release(&reader));
        }
        composed_run(&f);
        CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
            && f.record.completed == 1 && f.record.runtime_depth == 1);
        join_snapshot_check(&f, &snapshot); col_rel_destroy(snapshot.contents);
        CHECK(!wl_columnar_eval_stack_cleanup_result(f.frame)->rel
            && !f.record.source_initialized && !f.record.source_slot_borrowed);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            f.record.semantic_result, 0) == f.record.semantic_result);
        CHECK(!wl_evaluation_control_reset(f.operation));
        CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
        uint64_t successor =
            wl_evaluation_control_active_attempt_id(f.operation);
        /* ACK's funding exclusion applies to the same pointer's successor. */
        CHECK(wl_columnar_eval_plan_numeric_composed_attempt_ack(&f.record,
            f.operation, successor) == EINVAL);
        composed_retry_frame(&f);
        composed_begin(&f, &body, sources, 2, 0); composed_run(&f);
        const double expected[] = {-2, 1, 3};
        CHECK(!f.record.semantic_result && f.record.completed == 4);
        composed_literal(&f, expected, 3, 1, false);
        composed_raw_parity(&f, &body, 0);
        composed_close(&f); col_rel_destroy(b);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

static void
test_composed_source_allocation_failures(void)
{
    const double av[] = {3, 1, 3, -2}, bv[] = {1, 4, 0};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
                          {.op = WL_PLAN_OP_VARIABLE, .relation_name = "b",
                           .delta_mode = WL_DELTA_FORCE_EMPTY},
                          {.op = WL_PLAN_OP_CONCAT},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t body = {.ops = ops, .op_count = 4};
    uint32_t reached = 0;
    for (uint32_t ordinal = 0; !ordinal || ordinal <= reached; ++ordinal) {
        col_rel_t *a = composed_source("a", 17, av, 4, false);
        col_rel_t *b = composed_source("b", 17, bv, 3, false);
        fixture_t f; composed_fixture(&f, a, 0);
        wl_columnar_memory_governor_ref_t *governor = composed_governor(&f);
        col_rel_t *registry[] = {a, b};
        composed_begin(&f, &body, registry, 2, 0);
        composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW, 1);
        join_snapshot_t snapshot = join_snapshot(&f);
        wl_columnar_relation_test_constructor_fail_allocation =
            ordinal ? ordinal : UINT32_MAX;
        composed_action(&f);
        if (!ordinal) reached = UINT32_MAX -
                wl_columnar_relation_test_constructor_fail_allocation;
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        CHECK(reached);
        composed_run(&f);
        CHECK(f.record.semantic_result == (ordinal ? ENOMEM : 0));
        if (ordinal) {
            CHECK(f.record.completed == 1 && !f.session.memory_budget_denied);
            join_snapshot_check(&f, &snapshot);
            CHECK(!wl_columnar_eval_stack_cleanup_result(f.frame)->rel);
        }
        col_rel_destroy(snapshot.contents); composed_close(&f);
        col_rel_destroy(b);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

static void
test_composed_later_concat_failure(void)
{
    const double av[] = {-0.0, -2.5, 1.5, 1.5}, bv[] = {0, 4.5, -2.5};
    const double cv[] = {7, 4.5, -3};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
                          {.op = WL_PLAN_OP_VARIABLE, .relation_name = "b"},
                          {.op = WL_PLAN_OP_CONCAT},
                          {.op = WL_PLAN_OP_VARIABLE, .relation_name = "c"},
                          {.op = WL_PLAN_OP_CONCAT},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t body = {.ops = ops, .op_count = 6};
    for (unsigned invalid = 0; invalid < 2; ++invalid) {
        col_rel_t *a = composed_source("a", 1, av, 4, true);
        col_rel_t *b = composed_source("b", 1, bv, 3, true);
        col_rel_t *c = composed_source("c", 1, cv, 3, true);
        c->columns[0][1] = wl_columnar_float_to_bits(invalid ? NAN : INFINITY);
        fixture_t f; composed_fixture(&f, a, 0);
        wl_columnar_memory_governor_ref_t *governor = composed_governor(&f);
        col_rel_t *registry[] = {a, b, c};
        composed_begin(&f, &body, registry, 3, 0);
        composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 3);
        join_snapshot_t snapshot = join_snapshot(&f); CHECK(snapshot.owned);
        composed_run(&f);
        CHECK(f.record.semantic_result == EINVAL && f.record.completed == 4);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        CHECK(stack->top == 2 && stack->items[0].rel == snapshot.identity
            && stack->items[0].owned && stack->items[1].rel == c &&
            !stack->items[1].owned);
        same_relation(stack->items[0].rel, snapshot.contents);
        col_rel_destroy(snapshot.contents);
        /* Keep both borrowed registry descriptors alive across failed-frame
         * teardown and a genuine reset/new frame, repairing only bad c cell. */
        composed_retry_frame(&f);
        c->columns[0][1] = wl_columnar_float_to_bits(4.5);
        composed_begin(&f, &body, registry, 3, 0); composed_run(&f);
        const double expected[] = {-3, -2.5, 0, 1.5, 4.5, 7};
        CHECK(!f.record.semantic_result && f.record.completed == 6);
        composed_literal(&f, expected, 6, 1, true);
        composed_raw_parity(&f, &body, 0); composed_close(&f);
        col_rel_destroy(b); col_rel_destroy(c);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

static void
test_composed_retained_and_turnover(void)
{
    const double av[] = {3, 1, 3, -2}, bv[] = {1, 4, 0}, cv[] = {7, 4, -3};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_CONCAT}, {.op = WL_PLAN_OP_CONCAT},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t body = {.ops = ops, .op_count = 3};
    for (unsigned mask = 1; mask < 4; ++mask)
        for (unsigned turnover = 0; turnover < 2; ++turnover) {
            col_rel_t *a = composed_source("a", 1, av, 4, false);
            col_rel_t *b = composed_source("b", 1, bv, 3, false);
            col_rel_t *c = composed_source("c", 1, cv, 3, false);
            fixture_t f; composed_fixture(&f, a, 0);
            wl_columnar_memory_governor_ref_t *governor = composed_governor(&f);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            col_rel_t *registry[] = {a, b, c};
            for (unsigned i = 0; i < 3;
                ++i) CHECK(!eval_stack_push(stack, registry[i], true));
            composed_begin(&f, &body, NULL, 0, 3);
            wl_columnar_source_access_reader_t readers[2] = {{0}};
            for (unsigned i = 0; i < 2; ++i)
                if (mask & (1u <<
                    i)) CHECK(!col_rel_source_reader_acquire(registry[i+1],
                        &readers[i]));
            for (unsigned turns = 0; !(f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD &&
                f.record.child_status.progress ==
                WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
                ++turns){
                CHECK(turns < 100000); composed_action(&f);
            }
            CHECK(stack->top == 2 && stack->items[0].rel == a &&
                stack->items[0].owned);
            col_rel_t *published = stack->items[1].rel;
            CHECK(published && published->nrows == 6 &&
                f.record.child_status.committed);
            if (turnover) {
                CHECK(!wl_evaluation_control_finish(f.operation,
                    &f.operation_owner, 0, 0));
                CHECK(!wl_evaluation_control_reset(f.operation));
                CHECK(!wl_evaluation_control_begin(f.operation,
                    &f.operation_owner));
            }
            for (unsigned retry = 0; retry < 3; ++retry) {
                composed_action(&f); CHECK(stack->items[1].rel == published);
            }
            for (unsigned i = 0; i < 2; ++i)
                if (mask & (1u <<
                    i)) CHECK(!col_rel_source_reader_release(&readers[i]));
            composed_run(&f);
            CHECK(f.record.semantic_result == (turnover ? EINVAL : 0));
            CHECK(f.record.completed == (turnover ? 1u : 3u));
            if (turnover) {
                CHECK(stack->top == 2 && stack->items[0].rel == a &&
                    stack->items[1].rel == published);
                CHECK(!wl_evaluation_control_finish(f.operation,
                    &f.operation_owner, 0, 0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.operation, &report));
                CHECK(report.attempt_id != f.operation_id && !report.consumed);
            } else {
                const double expected[] = {-3, -2, 0, 1, 3, 4, 7};
                composed_literal(&f, expected, 7, 1, false);
            }
            f.input = NULL; composed_close(&f);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
        }
}

static void *
composed_wrong_thread(void *data)
{
    fixture_t *f = data;
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record,
        f->funding, f->funding_id) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_ack(&f->record,
        f->funding, f->funding_id) == EINVAL);
    return NULL;
}

static void
test_composed_authority_and_host_funding(void)
{
    const double av[] = {3, 1, 3, -2}, bv[] = {1, 4, 0};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
                          {.op = WL_PLAN_OP_VARIABLE, .relation_name = "b"},
                          {.op = WL_PLAN_OP_CONCAT},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t body = {.ops = ops, .op_count = 4};
    fixture_t f; composed_fixture(&f, composed_source("a", 1, av, 4, false), 0);
    col_rel_t *b = composed_source("b", 1, bv, 3, false);
    col_rel_t *registry[] = {f.input, b};
    composed_begin(&f, &body, registry, 2, 0);
    wl_columnar_eval_plan_numeric_attempt_t copied = f.record;
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&copied,
        f.funding, f.funding_id) == EINVAL);
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, composed_wrong_thread, &f));
    CHECK(!wl_thread_join(&thread));
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f.session, &nested));
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
        f.funding, f.funding_id) == EBUSY);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f.record,
        f.funding, f.funding_id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_consume_result(&f.record, 0,
        f.funding, f.funding_id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_attempt_ack(&f.record,
        f.funding, f.funding_id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
        f.operation, f.operation_id) == EINVAL);
    int wrong_owner = 0;
    wl_evaluation_control_t *wrong = control_new(0, &wrong_owner);
    uint64_t wrong_id = wl_evaluation_control_active_attempt_id(wrong);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
        wrong, wrong_id + 1) == EINVAL);
    CHECK(!wl_evaluation_control_finish(wrong, &wrong_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(wrong));
    CHECK(!wl_evaluation_control_begin(wrong, &wrong_owner));
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
        wrong, wrong_id) == EINVAL);
    uint64_t current_wrong_id = wl_evaluation_control_active_attempt_id(wrong);
    CHECK(!wl_evaluation_control_finish(wrong, &wrong_owner, 0, 0));
    wl_evaluation_control_report_t wrong_report;
    CHECK(!wl_evaluation_control_report(wrong, &wrong_report));
    CHECK(wrong_report.attempt_id == current_wrong_id &&
        !wrong_report.consumed);
    CHECK(!wl_evaluation_control_detach(wrong, &wrong_owner));
    wl_evaluation_control_release(wrong);
    wl_columnar_eval_plan_numeric_attempt_t before;
    memcpy(&before, &f.record, sizeof(before));
    int owner = 0; wl_evaluation_control_t *poor = control_new(383, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(poor);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record, poor,
        id)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!memcmp(&before, &f.record, sizeof(before)));
    CHECK(wl_evaluation_control_finish(poor, &owner, 0,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(poor, &report));
    CHECK(report.attempt_id == id && !report.consumed);
    CHECK(!wl_evaluation_control_detach(poor, &owner));
    wl_evaluation_control_release(poor);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD, 2);
    /* Reach real BINARY retirement naturally; no fabricated phase. */
    wl_columnar_merge_concat_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_concat_context_owned(f.frame, &ctx,
        &f.record));
    for (unsigned turns = 0; ctx->phase != 10 /* original BINARY phase, merge.c */
        || f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD;
        ++turns){
        CHECK(turns < 100000); composed_action(&f);
    }
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD);
    const uint64_t limits[] = {384, 767, 768, 1151, 1152};
    const uint64_t consumed[] = {384, 384, 768, 768, 1152};
    for (unsigned i = 0; i < 5; ++i) {
        poor = control_new(limits[i], &owner);
        id = wl_evaluation_control_active_attempt_id(poor);
        wl_columnar_filter_status_t status;
        memcpy(&status, &f.record.child_status, sizeof(status));
        int rc = wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
                poor, id);
        CHECK(rc == (i < 4 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
        if (i < 4) CHECK(!memcmp(&status, &f.record.child_status,
                sizeof(status)));
        CHECK(wl_evaluation_control_finish(poor, &owner, 0, 0) == rc);
        CHECK(!wl_evaluation_control_report(poor, &report));
        CHECK(report.attempt_id == id && report.consumed == consumed[i]);
        CHECK(!wl_evaluation_control_detach(poor, &owner));
        wl_evaluation_control_release(poor);
        f.pending_protocol = rc;
        composed_action(&f); /* Different fresh host action consumes protocol. */
        CHECK(f.record.runtime_depth == 2 && f.record.completed == 2);
    }
    composed_run(&f); CHECK(!f.record.semantic_result);
    CHECK(f.record.completed == 4);
    composed_close(&f); col_rel_destroy(b);
}

static void
test_composed_begin_limits(void)
{
    const uint64_t limits[] = {191, 192, 1535, 1536, 1791, 1792, 2047, 2048};
    const uint64_t consumed[] = {0, 192, 1344, 1536, 1536, 1792, 1792, 2048};
    const double av[] = {3, 1, 3, -2};
    wl_plan_op_t op = {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"};
    wl_plan_relation_t body = {.ops = &op, .op_count = 1};
    for (unsigned i = 0; i < 8; ++i) {
        fixture_t f;
        composed_fixture(&f, composed_source("a", 1, av, 4, false), limits[i]);
        col_rel_t *registry[] = {f.input}; f.session.rels = registry;
        f.session.nrels = 1;
        int rc = wl_columnar_eval_plan_numeric_composed_attempt_begin(&f.record,
                &body, 0, 1, f.frame, &f.session, 0, f.operation,
                f.operation_id,
                f.funding, f.funding_id);
        if (i == 7) {
            CHECK(!rc); composed_run(&f);
            CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT
                && !f.record.completed && !f.record.runtime_depth);
            CHECK(!composed_host_turn(&f, true));
        } else CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                !f.record.identity);
        CHECK(!wl_columnar_eval_stack_cleanup_finish(&f.frame));
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            WL_EVALUATION_CONTROL_WORK_LIMIT,
            0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.attempt_id == f.operation_id &&
            report.consumed == consumed[i]);
        CHECK(!wl_evaluation_control_finish(f.funding, &f.funding_owner, 0, 0));
        CHECK(!wl_evaluation_control_detach(f.operation, &f.operation_owner));
        CHECK(!wl_evaluation_control_detach(f.funding, &f.funding_owner));
        wl_evaluation_control_release(f.operation);
        wl_evaluation_control_release(f.funding);
        col_rel_destroy(f.input);
    }
}

static void
test_composed_domain_and_repeated_source(void)
{
    const double values[] = {3, 1, 3, -2};
    wl_plan_op_t ops[] = {{.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
                          {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
                          {.op = WL_PLAN_OP_CONCAT},
                          {.op = WL_PLAN_OP_CONSOLIDATE}};
    wl_plan_relation_t body = {.ops = ops, .op_count = 4};
    for (unsigned domain = 0; domain < 11; ++domain) {
        fixture_t f;
        composed_fixture(&f, composed_source("a", 1, values, 4, false), 0);
        col_rel_t *registry[] = {f.input}; f.session.rels = registry;
        f.session.nrels = 1;
        if (domain == 0) f.session.current_iteration = 1;
        if (domain == 1) f.session.retraction_seeded = true;
        if (domain == 2) f.session.retraction_right_pass = true;
        if (domain == 3) f.session.tdd_input_run = (void *)&f;
        if (domain == 4) f.session.tdd_subpass_active = true;
        if (domain == 5) f.session.tdd_outbound_only_active = true;
        if (domain == 6) f.session.diff_operators_active = true;
        if (domain == 7) f.session.join_batch_bytes = 1;
        atomic_uint_fast64_t shared_count = 0;
        if (domain == 8) f.session.join_output_shared_count = &shared_count;
        if (domain == 9) f.session.coordinator = (void *)&f;
        if (domain == 10) f.session.num_workers = 2;
        CHECK(wl_columnar_eval_plan_numeric_composed_attempt_begin(&f.record,
            &body, 0, 4, f.frame, &f.session, 0, f.operation, f.operation_id,
            f.funding, f.funding_id) == ENOTSUP);
        CHECK(!f.record.identity &&
            !wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
        CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0,
            0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.attempt_id == f.operation_id && !report.consumed);
        CHECK(!wl_evaluation_control_reset(f.operation));
        CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
        f.operation_id = wl_evaluation_control_active_attempt_id(f.operation);
        f.session.current_iteration = 0; f.session.retraction_seeded = false;
        f.session.retraction_right_pass = false; f.session.tdd_input_run = NULL;
        f.session.tdd_subpass_active = false;
        f.session.tdd_outbound_only_active = false;
        f.session.diff_operators_active = false; f.session.join_batch_bytes = 0;
        f.session.join_output_shared_count = 0; f.session.coordinator = NULL;
        f.session.num_workers = 0;
        composed_begin(&f, &body, registry, 1, 0); composed_run(&f);
        const double expected[] = {-2, 1, 3};
        CHECK(!f.record.semantic_result && f.record.completed == 4);
        composed_literal(&f, expected, 3, 1, false);
        composed_raw_parity(&f, &body, 0); composed_close(&f);
    }
    /* A full depth32 claim must release without addressing items[32] when
     * the complete grammar leaves depth32 instead of its required depth1. */
    fixture_t full;
    composed_fixture(&full, composed_source("a", 1, values, 4, false), 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(full.frame);
    for (unsigned i = 0; i < COL_STACK_MAX;
        ++i) CHECK(!eval_stack_push(stack, full.input, false));
    wl_plan_op_t map = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_plan_relation_t invalid = {.ops = &map, .op_count = 1};
    composed_begin(&full, &invalid, NULL, 0, COL_STACK_MAX);
    composed_run(&full);
    CHECK(full.record.semantic_result == EINVAL && !full.record.completed
        && stack->top == COL_STACK_MAX);
    for (unsigned i = 0; i < COL_STACK_MAX;
        ++i) CHECK(stack->items[i].rel == full.input && !stack->items[i].owned);
    composed_close(&full);

    for (unsigned prebound = 0; prebound < 2; ++prebound) {
        fixture_t f;
        composed_fixture(&f, composed_source("a", 1, values, 4, false), 0);
        CHECK(!col_rel_enable_timestamps(f.input));
        wl_plan_op_t ts_ops[] = {{.op = WL_PLAN_OP_VARIABLE,
                                  .relation_name = "a",
                                  .delta_mode = WL_DELTA_FORCE_EMPTY},
                                 {.op = WL_PLAN_OP_CONSOLIDATE}};
        wl_plan_relation_t ts_body = {.ops = ts_ops + prebound,
                                      .op_count = 2-prebound};
        col_rel_t *registry[] = {f.input};
        if (prebound) CHECK(!eval_stack_push(
                    wl_columnar_eval_stack_cleanup_stack(f.frame), f.input,
                    false));
        composed_begin(&f, &ts_body, registry, 1, prebound); composed_run(&f);
        CHECK(f.record.semantic_result == ENOTSUP && !f.record.completed
            && !f.record.source_slot_borrowed && !f.record.source_initialized);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->top == prebound);
        composed_close(&f);
    }
}

static void
test_composed_controlled_batch_fallback_owner(void)
{
    static const char program_text[] =
        ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
        ".decl output(k:int64,x:int64,k2:int64,y:int64)\n"
        "output(k,x,k,y) :- a(k,x), b(k,y), x < y.\n";
    const double left_values[] = {0, 4, 1, -4};
    const double right_values[] = {1, 6, 3};
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(program_text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *body = composed_find_relation(plan, "output");
    CHECK(body && body->op_count == 5
        && body->ops[0].op == WL_PLAN_OP_VARIABLE
        && body->ops[1].op == WL_PLAN_OP_JOIN
        && body->ops[2].op == WL_PLAN_OP_FILTER
        && body->ops[3].op == WL_PLAN_OP_MAP
        && body->ops[4].op == WL_PLAN_OP_CONSOLIDATE);

    col_rel_t *left = composed_source("a", 2, left_values, 4, false);
    col_rel_t *right = composed_source("b", 2, right_values, 3, false);
    col_rel_t *registry[] = {left, right};
    fixture_t f;
    composed_fixture(&f, left, 0);
    f.session.rels = registry; f.session.nrels = 2; f.session.rel_cap = 2;
    f.session.join_batch_bytes = 224;
    wl_plan_op_t rejected_ops[5];
    memcpy(rejected_ops, body->ops, sizeof(rejected_ops));
    rejected_ops[0].delta_mode = WL_DELTA_FORCE_DELTA;
    wl_plan_relation_t rejected_body = *body;
    rejected_body.ops = rejected_ops;
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback(
            &f.record, &rejected_body, 0, rejected_body.op_count, f.frame,
            &f.session, 0, f.operation, f.operation_id, f.funding, f.funding_id)
        == ENOTSUP);
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback(
            &f.record, body, 0, body->op_count, f.frame,
            &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id));
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 0);
    CHECK(wl_columnar_eval_plan_numeric_batch_fallback_authorized(
            &f.record, f.frame, &f.session, f.operation, f.operation_id));
    wl_columnar_eval_plan_numeric_attempt_t copied = f.record;
    CHECK(!wl_columnar_eval_plan_numeric_batch_fallback_authorized(
            &copied, f.frame, &f.session, f.operation, f.operation_id));
    CHECK(!wl_columnar_eval_plan_numeric_batch_fallback_authorized(
            &f.record, f.frame, &f.session, f.operation, f.operation_id + 1));
    composed_run(&f);
    CHECK(!f.record.semantic_result
        && f.record.completed == body->op_count);
    composed_raw_parity(&f, body, 0);
    composed_close(&f);
    CHECK(!f.record.controlled_materialized_batch_fallback);
    col_rel_destroy(right);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_composed_unary_work_limit_cleanup(void)
{
    static const char text[] =
        ".decl input(x:int64)\n.decl output(y:int64)\n"
        "output(x + 1) :- input(x), x > 0.\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *body = composed_find_relation(plan, "output");
    CHECK(body && body->ops[1].op == WL_PLAN_OP_FILTER);
    double values[256];
    for (unsigned i = 0; i < 256; ++i) values[i] = i + 1;
    fixture_t f;
    /* This cap passes composed preflight and exhausts during the owned FILTER
     * child's bounded setup/row work; its step path then drains the stop. */
    composed_fixture(&f, composed_source("input", 1, values, 256, false),
        30000);
    col_rel_t *registry[] = {f.input};
    composed_begin(&f, body, registry, 1, 0);
    composed_until_stopped_child(&f,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY);
    CHECK(f.record.cursor == 1 && f.record.completed == 1);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1);
    col_rel_t *candidate = stack->items[0].rel;
    bool owned = stack->items[0].owned;
    CHECK(f.record.child_status.semantic_result ==
        WL_EVALUATION_CONTROL_WORK_LIMIT
        && wl_evaluation_control_charge_attempt(f.operation, f.operation_id,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);

    composed_drain_stopped_child(&f,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK);
    CHECK(!f.record.child_status.committed
        && stack->top == 1 && stack->items[0].rel == candidate
        && stack->items[0].owned == owned
        && f.record.cursor == 1 && f.record.completed == 1);
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && f.record.cursor == 1 && f.record.completed == 1
        && !f.record.child_status.committed
        && stack->top == 1 && stack->items[0].rel == candidate
        && stack->items[0].owned == owned
        && !session_find_rel(&f.session, "output"));
    session_rel_free_hash(&f.session);
    composed_close(&f);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static void
test_composed_concat_work_limit_cleanup(void)
{
    static const double values[256] = {0};
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "a"},
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "b"},
        {.op = WL_PLAN_OP_CONCAT},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    wl_plan_relation_t body = {.ops = ops, .op_count = 4};
    col_rel_t *a = composed_source("a", 1, values, 256, false);
    col_rel_t *b = composed_source("b", 1, values, 256, false);
    fixture_t f;
    /* The separate parent and CONCAT grants fund the child action. This cap
     * stops the original operation during CONCAT's bounded owned start path. */
    composed_fixture(&f, a, 30000);
    col_rel_t *registry[] = {a, b};
    composed_begin(&f, &body, registry, 2, 0);
    composed_until_stopped_child(&f,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_START);
    CHECK(f.record.cursor == 2 && f.record.completed == 2);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 2);
    col_rel_t *left = stack->items[0].rel;
    col_rel_t *right = stack->items[1].rel;
    bool left_owned = stack->items[0].owned;
    bool right_owned = stack->items[1].owned;
    CHECK(wl_evaluation_control_charge_attempt(f.operation, f.operation_id,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);

    composed_drain_stopped_child(&f,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD,
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_ACK);
    CHECK(!f.record.child_status.committed
        && stack->top == 2 && stack->items[0].rel == left
        && stack->items[1].rel == right
        && stack->items[0].owned == left_owned
        && stack->items[1].owned == right_owned
        && f.record.cursor == 2 && f.record.completed == 2);
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && f.record.cursor == 2 && f.record.completed == 2
        && !f.record.child_status.committed
        && stack->top == 2 && stack->items[0].rel == left
        && stack->items[1].rel == right
        && stack->items[0].owned == left_owned
        && stack->items[1].owned == right_owned
        && !session_find_rel(&f.session, "output"));
    session_rel_free_hash(&f.session);
    composed_close(&f);
    col_rel_destroy(b);
}

int
main(void)
{
    test_composed_unary_work_limit_cleanup();
    test_composed_concat_work_limit_cleanup();
    test_composed_private_input_binding();
    test_composed_recursive_terminal_exchange();
    test_composed_lftj_stopped_cleanup();
    test_composed_source_stopped_cleanup();
    test_composed_source_cleanup_abort_retry();
    test_composed_source_cleanup_pre_action_phases();
    test_composed_domain_and_repeated_source();
    test_composed_controlled_batch_fallback_owner();
    test_composed_abort_request();
    test_composed_generated_union(); test_composed_nested_and_wide();
    test_composed_branch_children(); test_composed_grammar();
    test_composed_later_failure_and_retry();
    test_composed_source_allocation_failures();
    test_composed_later_concat_failure(); test_composed_retained_and_turnover();
    test_composed_authority_and_host_funding(); test_composed_begin_limits();
    test_generated_reduce_plans(); test_composed_reduce_plans();
    test_reduce_later_failure(); test_generated_reduce_join_bodies();
    test_generated_probe_plans(); test_composed_probe_plans();
    test_probe_plan_failures();
    test_generated_map_float_schema();
    test_complete_unary_plans(); test_consolidate_composition();
    test_consolidate_literal_limits();
    test_consolidate_stops(); test_consolidate_retained_successor();
    test_complete_join_consolidate_stops();
    test_consolidate_retained_input(); test_consolidate_later_failures();
    test_consolidate_shape_refusals();
    test_owned_consolidate_entry(); test_owned_consolidate_successor();
    test_owned_join_entry();
    test_owned_join_successor();
    test_generated_join_fragments(false);
    test_generated_join_fragments(true);
    test_join_no_output_stops();
    test_join_literal_limits();
    test_join_later_failures();
    test_join_retained_successor(WL_PLAN_OP_JOIN);
    test_join_retained_successor(WL_PLAN_OP_SEMIJOIN);
    test_join_retained_successor(WL_PLAN_OP_ANTIJOIN);
    test_join_chained_delta();
    test_generated(false); test_generated(true); test_boundaries();
    test_authority();
    test_intermediate_stop(); test_literal_prefixes(); test_retained_input();
    test_pending_funding(); test_later_allocation_failure();
    test_begin_rollback(); test_host_boundaries();
    test_source_generated(); test_source_delta_modes();
    test_source_long_names();
    test_source_scan_stops(); test_source_owned_handoff();
    test_source_allocation_failures(); test_source_authority();
    test_source_unsupported(); test_source_no_output(); test_source_denial();
    test_source_pending_funding(); test_source_work_boundaries();
    test_source_host_boundaries();
    puts("numeric plan tests passed");
    return 0;
}
