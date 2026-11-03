/* #1821 production numeric plan composition, original authority and host turns. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/mem_ledger_publication.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

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
            if (!shape) {
                CHECK(!f.record.semantic_result && f.record.completed == 1);
                eval_stack_t *result =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                CHECK(result->top == 1 && result->items[0].rel->timestamps
                    && result->items[0].rel->nrows == rows);
                CHECK(f.input->storage_generation == generation);
                fixture_close(&f); continue;
            }
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
                if (!fault) {
                    /* The missing-right probe is an exact no-op, including
                     * the owned entry and delta image, before the next op. */
                    join_until_ready(&f, target + 1);
                    join_snapshot_check(&f, &snapshot);
                    run(&f);
                    CHECK(!f.record.semantic_result && f.record.completed == 5);
                    join_raw_parity(&f, &plan, true);
                    col_rel_destroy(snapshot.contents);
                    fixture_close(&f);
                    col_rel_destroy(right); col_rel_destroy(third);
                    continue;
                }
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
    CHECK(!report_rc
        && report.attempt_id == id && !report.cleanup_status
        && (funding_limit == 383 || funding_id_adjustment || !eligible
        ? report.consumed == 0
        : report.consumed >= 384 && report.consumed <= 386));
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
    const unsigned char cleared_lftj[sizeof(f->record.lftj)] = {0};
    CHECK(!memcmp(&f->record.lftj, cleared_lftj, sizeof(cleared_lftj)));
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

typedef struct {
    fixture_t *fixture;
    wl_columnar_join_setup_result_t result, getter_result, handoff_result;
} composed_join_setup_peer_t;

static void *
composed_join_setup_wrong_thread(void *argument)
{
    composed_join_setup_peer_t *peer = argument;
    fixture_t *f = peer->fixture;
    int owner = 0;
    wl_evaluation_control_t *grant = control_new(256, &owner);
    peer->result = wl_columnar_join_op_attempt_start_funded(
        &f->record.plan->ops[f->record.cursor], f->frame, &f->session,
        f->operation, f->operation_id, &f->record.child_status, &f->record,
        grant, wl_evaluation_control_active_attempt_id(grant));
    const wl_columnar_eval_private_bindings_t *bindings = NULL;
    peer->getter_result =
        wl_columnar_eval_plan_numeric_private_bindings_get_funded(&f->record,
            f->frame, &f->session, f->operation, f->operation_id, grant,
            wl_evaluation_control_active_attempt_id(grant), &bindings);
    peer->handoff_result =
        wl_columnar_eval_plan_numeric_composed_join_setup_handoff(&f->record,
            f->frame, &f->session, f->operation, f->operation_id,
            &f->record.plan->ops[f->record.cursor]);
    CHECK(!bindings);
    CHECK(!wl_evaluation_control_finish(grant, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(grant, &owner));
    wl_evaluation_control_release(grant);
    return NULL;
}

/* Refuse at every genuine setup boundary on the same constructor-created
 * READY owner. Completed metadata is paid; the stack/context remain untouched
 * until a later fully funded call performs the original forward/child work. */
static void
composed_join_setup_refusals(fixture_t *f, bool static_four)
{
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY);
    CHECK(!wl_columnar_eval_stack_numeric_setup_nonstream(f->frame,
        &f->record));
    wl_columnar_eval_plan_numeric_attempt_t copied = f->record;
    CHECK(wl_columnar_eval_stack_numeric_setup_nonstream(f->frame,
        &copied) == EBUSY);
    wl_columnar_join_setup_result_t handoff =
        wl_columnar_eval_plan_numeric_composed_join_setup_handoff(&f->record,
            f->frame, &f->session, f->operation, f->operation_id,
            &f->record.plan->ops[f->record.cursor]);
    CHECK(handoff.protocol == EINVAL
        && handoff.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID
        && f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY);
    handoff = wl_columnar_eval_plan_numeric_composed_join_setup_handoff(&copied,
            f->frame, &f->session, f->operation, f->operation_id, NULL);
    CHECK(handoff.protocol == EBUSY
        && handoff.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID);
    int refused_owner = 0;
    wl_evaluation_control_t *refused_grant = control_new(127, &refused_owner);
    uint64_t refused_id =
        wl_evaluation_control_active_attempt_id(refused_grant);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&copied,
        refused_grant, refused_id) == EINVAL);
    uint64_t unchanged;
    CHECK(!wl_evaluation_control_test_active_consumed(refused_grant,
        refused_id, &unchanged) && !unchanged);
    wl_evaluation_control_request_cancel(refused_grant);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record,
        refused_grant, refused_id) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
        && !f->record.semantic_result);
    CHECK(wl_evaluation_control_finish(refused_grant, &refused_owner, 0, 0)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_evaluation_control_detach(refused_grant, &refused_owner));
    wl_evaluation_control_release(refused_grant);
    int turnover_owner = 0;
    wl_evaluation_control_t *turnover = control_new(0, &turnover_owner);
    uint64_t old_funding_id = wl_evaluation_control_active_attempt_id(turnover);
    CHECK(!wl_evaluation_control_finish(turnover, &turnover_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(turnover));
    CHECK(!wl_evaluation_control_begin(turnover, &turnover_owner));
    CHECK(wl_evaluation_control_active_attempt_id(turnover) != old_funding_id);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record,
        turnover, old_funding_id) == EINVAL);
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
        && !f->record.semantic_result);
    CHECK(!wl_evaluation_control_finish(turnover, &turnover_owner, 0, 0));
    wl_evaluation_control_report_t successor;
    CHECK(!wl_evaluation_control_report(turnover, &successor)
        && successor.attempt_id != old_funding_id && !successor.consumed);
    CHECK(!wl_evaluation_control_detach(turnover, &turnover_owner));
    wl_evaluation_control_release(turnover);
    const uint64_t plain[] = {384, 512, 704, 832, 896, 1024};
    const uint64_t bound[] = {384, 512, 704, 832, 896, 960, 992, 1056,
                              1088, 1120, 1184, 1216, 1248, 1280, 1344, 1472};
    const uint64_t *ends = static_four ? bound : plain;
    unsigned count = static_four ? sizeof(bound) / sizeof(bound[0])
        : sizeof(plain) / sizeof(plain[0]);
    eval_stack_t *stack = NULL;
    CHECK(!wl_columnar_eval_stack_operator_stack_owned(f->frame,
        &f->session, &stack, &f->record));
    eval_stack_t before = *stack;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, &ctx,
        &f->record));
    wl_columnar_filter_context_t inert = *ctx;
    wl_columnar_filter_status_t status = f->record.child_status;
    uint32_t cursor = f->record.cursor;
    uint64_t original_before, original_after;
    CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
        f->operation_id, &original_before));
    int mixed_owner = 0;
    wl_evaluation_control_t *mixed_original = control_new(0, &mixed_owner);
    uint64_t mixed_id = wl_evaluation_control_active_attempt_id(mixed_original);
    wl_columnar_join_setup_result_t mixed =
        wl_columnar_join_op_attempt_start_funded(
        &f->record.plan->ops[f->record.cursor], f->frame, &f->session,
        mixed_original, mixed_id, &f->record.child_status, &f->record,
        f->operation, f->operation_id);
    CHECK(mixed.protocol == EINVAL
        && mixed.outcome == WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED);
    const wl_columnar_eval_private_bindings_t *mixed_output =
        (const void *)&f->record;
    mixed = wl_columnar_eval_plan_numeric_private_bindings_get_funded(
        &f->record, f->frame, &f->session, mixed_original, mixed_id,
        f->operation, f->operation_id, &mixed_output);
    CHECK(mixed.protocol == EINVAL
        && mixed.outcome == WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED
        && mixed_output == (const void *)&f->record);
    CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
        f->operation_id, &original_after) && original_after == original_before);
    CHECK(!wl_evaluation_control_charge_attempt(mixed_original, mixed_id, 0));
    CHECK(!wl_evaluation_control_finish(mixed_original, &mixed_owner, 0, 0));
    wl_evaluation_control_report_t mixed_report;
    CHECK(!wl_evaluation_control_report(mixed_original, &mixed_report)
        && !mixed_report.consumed && !mixed_report.result);
    CHECK(!wl_evaluation_control_detach(mixed_original, &mixed_owner));
    wl_evaluation_control_release(mixed_original);

    for (unsigned i = 0; i < count; ++i) {
        int owner = 0;
        wl_evaluation_control_t *grant = control_new(ends[i] - 1, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(grant);
        CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record,
            grant, id) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
            && f->record.cursor == cursor && !f->record.composed_busy
            && !f->record.semantic_result
            && !memcmp(stack, &before, sizeof(before))
            && !memcmp(ctx, &inert, sizeof(inert))
            && !memcmp(&f->record.child_status, &status, sizeof(status)));
        CHECK(!wl_evaluation_control_charge_attempt(f->operation,
            f->operation_id, 0));
        CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
            f->operation_id, &original_after)
            && original_before == original_after);
        CHECK(wl_evaluation_control_finish(grant, &owner, 0, 0)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(grant, &report)
            && report.consumed == (i ? ends[i - 1] : 0));
        CHECK(!wl_evaluation_control_detach(grant, &owner));
        wl_evaluation_control_release(grant);
    }
    int owner = 0;
    wl_evaluation_control_t *grant = control_new(ends[count - 1], &owner);
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record,
        grant, wl_evaluation_control_active_attempt_id(grant)));
    CHECK(f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        && f->record.pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY);
    CHECK(!wl_evaluation_control_finish(grant, &owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant, &report)
        && report.consumed == ends[count - 1]);
    CHECK(!wl_evaluation_control_detach(grant, &owner));
    wl_evaluation_control_release(grant);
    f->pending_protocol = 0;
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
    /* The unbound route exercises HEADER/FINISH even without entries. */
    col_rel_t *plain_a = composed_source("a", 1, a_values, 2, false);
    col_rel_t *plain_b = composed_source("b", 1, b_private_values, 2, false);
    col_rel_t *plain_registry[] = {plain_a, plain_b};
    composed_fixture(&f, plain_a, 0);
    f.session.base.evaluation_control = f.operation;
    composed_begin(&f, join, plain_registry, 2, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 1);
    uint64_t ready_work;
    CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
        f.operation_id, &ready_work));
    composed_join_setup_refusals(&f, false);
    composed_run(&f);
    CHECK(!f.record.semantic_result);
    stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 1
        && stack->items[0].rel->columns[0][0] == 1);
    session_rel_free_hash(&f.session);
    composed_close(&f);
    col_rel_destroy(plain_b);

    /* Sticky original stops are semantic, unlike the funding-only refusals
    * above. Only parent and DISPATCH are paid; no positive original setup
    * or child action may be spent after the saved original has stopped. */
    for (unsigned limited = 0; limited < 2; ++limited) {
        plain_a = composed_source("a", 1, a_values, 2, false);
        plain_b = composed_source("b", 1, b_private_values, 2, false);
        col_rel_t *stopped_registry[] = {plain_a, plain_b};
        composed_fixture(&f, plain_a, limited ? ready_work : 0);
        f.session.base.evaluation_control = f.operation;
        composed_begin(&f, join, stopped_registry, 2, 0);
        composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 1);
        int stop = limited ? WL_EVALUATION_CONTROL_WORK_LIMIT
            : WL_EVALUATION_CONTROL_CANCELLED;
        if (limited) CHECK(wl_evaluation_control_charge_attempt(f.operation,
                f.operation_id, 1) == stop);
        else {
            CHECK(!composed_abort_request(&f, 0));
            /* The complete funded getter must establish owner authority
             * before interpreting even a stopped supplied control. */
            int getter_owner = 0;
            wl_evaluation_control_t *getter_funding = control_new(128,
                    &getter_owner);
            const wl_columnar_eval_private_bindings_t *getter_output =
                (const void *)&f.record;
            wl_columnar_join_setup_result_t getter_refused =
                wl_columnar_eval_plan_numeric_private_bindings_get_funded(
                NULL, f.frame, &f.session, f.operation, f.operation_id,
                getter_funding,
                wl_evaluation_control_active_attempt_id(getter_funding),
                &getter_output);
            CHECK(getter_refused.protocol == EINVAL
                && getter_refused.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID
                && getter_output == (const void *)&f.record);
            CHECK(!wl_evaluation_control_finish(getter_funding,
                &getter_owner, 0, 0));
            CHECK(!wl_evaluation_control_detach(getter_funding,
                &getter_owner));
            wl_evaluation_control_release(getter_funding);
            int stopped_foreign_owner = 0, checked_getter_owner = 0;
            wl_evaluation_control_t *stopped_foreign = control_new(0,
                    &stopped_foreign_owner);
            wl_evaluation_control_request_cancel(stopped_foreign);
            wl_evaluation_control_t *checked_getter = control_new(128,
                    &checked_getter_owner);
            getter_output = (const void *)&f.record;
            getter_refused =
                wl_columnar_eval_plan_numeric_private_bindings_get_funded(
                &f.record, f.frame, &f.session, stopped_foreign,
                wl_evaluation_control_active_attempt_id(stopped_foreign),
                checked_getter,
                wl_evaluation_control_active_attempt_id(checked_getter),
                &getter_output);
            CHECK(getter_refused.protocol == EINVAL
                && getter_refused.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID
                && getter_output == (const void *)&f.record);
            /* An invalid association must not latch the requested foreign
             * cancellation by probing it. Its real finish remains successful. */
            CHECK(!wl_evaluation_control_finish(stopped_foreign,
                &stopped_foreign_owner, 0, 0));
            wl_evaluation_control_report_t foreign_report;
            CHECK(!wl_evaluation_control_report(stopped_foreign,
                &foreign_report) && !foreign_report.result
                && !foreign_report.consumed);
            CHECK(!wl_evaluation_control_detach(stopped_foreign,
                &stopped_foreign_owner));
            CHECK(!wl_evaluation_control_finish(checked_getter,
                &checked_getter_owner, 0, 0));
            CHECK(!wl_evaluation_control_detach(checked_getter,
                &checked_getter_owner));
            wl_evaluation_control_release(stopped_foreign);
            wl_evaluation_control_release(checked_getter);
            composed_join_setup_peer_t peer = { .fixture = &f };
            wl_thread_t peer_thread;
            CHECK(!wl_thread_create(&peer_thread,
                composed_join_setup_wrong_thread, &peer));
            CHECK(!wl_thread_join(&peer_thread));
            CHECK(peer.result.protocol == EINVAL
                && peer.result.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID);
            CHECK(peer.getter_result.protocol == EINVAL
                && peer.getter_result.outcome ==
                WL_COLUMNAR_JOIN_SETUP_INVALID);
            CHECK(peer.handoff_result.protocol == EINVAL
                && peer.handoff_result.outcome ==
                WL_COLUMNAR_JOIN_SETUP_INVALID);
            /* A real abort request sets the genuine owner's abort intent.
             * Supplying another active control to the full checked sibling
             * must refuse before probing or cancelling that control. */
            int foreign_owner = 0, proof_owner = 0;
            wl_evaluation_control_t *foreign = control_new(0, &foreign_owner);
            wl_evaluation_control_t *proof = control_new(128, &proof_owner);
            uint64_t foreign_id =
                wl_evaluation_control_active_attempt_id(foreign);
            wl_columnar_join_setup_result_t refused =
                wl_columnar_join_op_attempt_start_funded(&join->ops[1],
                    f.frame, &f.session, foreign, foreign_id,
                    &f.record.child_status,
                    &f.record, proof,
                    wl_evaluation_control_active_attempt_id(proof));
            CHECK(!wl_evaluation_control_charge_attempt(foreign, foreign_id,
                0));
            CHECK(refused.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID
                && refused.protocol == EINVAL);
            CHECK(!wl_evaluation_control_finish(foreign, &foreign_owner, 0, 0));
            CHECK(!wl_evaluation_control_detach(foreign, &foreign_owner));
            CHECK(!wl_evaluation_control_finish(proof, &proof_owner, 0, 0));
            CHECK(!wl_evaluation_control_detach(proof, &proof_owner));
            wl_evaluation_control_release(foreign);
            wl_evaluation_control_release(proof);
        }
        int grant_owner = 0;
        wl_evaluation_control_t *grant = control_new(1024, &grant_owner);
        CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
            grant, wl_evaluation_control_active_attempt_id(grant)));
        uint64_t after;
        CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
            f.operation_id, &after) && after == ready_work);
        CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            && f.record.semantic_result == stop && !f.record.composed_busy);
        CHECK(!wl_evaluation_control_finish(grant, &grant_owner, 0, 0));
        wl_evaluation_control_report_t paid;
        CHECK(!wl_evaluation_control_report(grant, &paid)
            && paid.consumed == 512 && !paid.result);
        CHECK(!wl_evaluation_control_detach(grant, &grant_owner));
        wl_evaluation_control_release(grant);
        composed_run(&f);
        session_rel_free_hash(&f.session);
        composed_close(&f);
        col_rel_destroy(plain_b);
    }

    const uint64_t original_allowances[] = {127, 128, 319, 320};
    for (unsigned boundary = 0; boundary < 4; ++boundary) {
        bool forwarded = boundary != 0;
        bool exact = boundary == 3;
        plain_a = composed_source("a", 1, a_values, 2, false);
        plain_b = composed_source("b", 1, b_private_values, 2, false);
        col_rel_t *child_registry[] = {plain_a, plain_b};
        composed_fixture(&f, plain_a,
            ready_work + original_allowances[boundary]);
        f.session.base.evaluation_control = f.operation;
        composed_begin(&f, join, child_registry, 2, 0);
        composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 1);
        wl_columnar_filter_context_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &ctx,
            &f.record));
        CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_INERT && !ctx->pin_owned);
        int grant_owner = 0;
        wl_evaluation_control_t *grant = control_new(1024, &grant_owner);
        CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
            grant, wl_evaluation_control_active_attempt_id(grant)));
        if (forwarded) CHECK(f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
                && f.record.pending_action ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY);
        else CHECK(f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
                && f.record.semantic_result ==
                WL_EVALUATION_CONTROL_WORK_LIMIT);
        uint64_t admitted;
        CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
            f.operation_id, &admitted)
            && admitted == ready_work + (forwarded ? 128 : 0)
            + (exact ? 192 : 0));
        CHECK(ctx->join_initialized == (exact != 0)
            && ctx->pin_owned == (exact != 0));
        if (forwarded &&
            !exact) CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_INERT
                && f.record.child_status.progress ==
                WL_COLUMNAR_FILTER_PROGRESS_DONE
                && f.record.child_status.semantic_result ==
                WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!wl_evaluation_control_finish(grant, &grant_owner, 0, 0));
        wl_evaluation_control_report_t paid;
        CHECK(!wl_evaluation_control_report(grant, &paid)
            && paid.consumed == 1024);
        CHECK(!wl_evaluation_control_detach(grant, &grant_owner));
        wl_evaluation_control_release(grant);
        f.pending_protocol = 0;
        composed_run(&f);
        CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        session_rel_free_hash(&f.session);
        composed_close(&f);
        col_rel_destroy(plain_b);
    }

    plain_a = composed_source("a", 1, a_values, 2, false);
    plain_b = composed_source("b", 1, b_private_values, 2, false);
    col_rel_t *successor_registry[] = {plain_a, plain_b};
    composed_fixture(&f, plain_a, 0);
    f.session.base.evaluation_control = f.operation;
    composed_begin(&f, join, successor_registry, 2, 0);
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 1);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    uint64_t successor_id =
        wl_evaluation_control_active_attempt_id(f.operation);
    CHECK(successor_id != f.operation_id);
    int successor_grant_owner = 0;
    wl_evaluation_control_t *successor_grant = control_new(1024,
            &successor_grant_owner);
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
        successor_grant,
        wl_evaluation_control_active_attempt_id(successor_grant)));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        && f.record.semantic_result == EINVAL);
    CHECK(!wl_evaluation_control_finish(successor_grant,
        &successor_grant_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(successor_grant,
        &successor_grant_owner));
    wl_evaluation_control_release(successor_grant);
    composed_run(&f);
    CHECK(!composed_host_turn(&f, true));
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t successor_report;
    CHECK(!wl_evaluation_control_report(f.operation, &successor_report)
        && successor_report.attempt_id == successor_id
        && !successor_report.consumed && !successor_report.result);
    session_rel_free_hash(&f.session);
    composed_close(&f);
    col_rel_destroy(plain_b);

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
    /* Four genuine checked bindings exercise the largest static owner proof;
     * extra names may alias live sources and do not replace the public shadow. */
    static const char *const join_names[] = {"a", "b", "c", "d"};
    col_rel_t *join_relations[] = {private_a, private_b, private_a, private_b};
    wl_columnar_eval_private_bindings_t join_bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&join_bindings, &f.session,
        join, f.operation, f.operation_id, join_names, join_relations, 4));
    f.session.rels = join_registry;
    f.session.nrels = 1;
    f.session.rel_cap = 1;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f.record, join, 0, join->op_count, f.frame, &f.session, 0,
            f.operation, f.operation_id, f.funding, f.funding_id,
            &join_bindings));
    composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 1);
    composed_join_setup_refusals(&f, true);
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

/* The compiler currently emits LFTJ only for EDB chains. Exercise the
 * authenticated internal binding contract directly: a private image must
 * shadow a different public image even when empty or stale. */
static void
test_composed_lftj_private_shadow(void)
{
    static const double private_values[] = {1, 3};
    static const double public_values[] = {2};
    static const double other_values[] = {1, 2, 3};
    for (unsigned mode = 0; mode < 3; ++mode)
        for (unsigned governed = 0; governed < 2; ++governed) {
            char *names[] = {"a", "b", "c"};
            uint32_t keys[] = {0, 0, 0};
            wl_plan_op_lftj_t metadata = {.k = 3, .rel_names = names,
                                          .key_cols = keys};
            wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ,
                               .opaque_data = &metadata};
            wl_plan_relation_t body = {.ops = &op, .op_count = 1};
            col_rel_t *private_a = composed_source("private-a", 1,
                    private_values, mode == 1 ? 0 : 2, false);
            free(private_a->name); private_a->name = NULL;
            col_rel_t *registry[] = {
                composed_source("a", 1, public_values, 1, false),
                composed_source("b", 1, other_values, 3, false),
                composed_source("c", 1, other_values, 3, false)
            };
            col_relation_snapshot_t snapshots[3];
            for (unsigned i = 0; i < 3; ++i)
                snapshots[i] = wl_columnar_relation_snapshot(registry[i]);

            /* Establish that accidentally selecting the public source would
             * return the distinguishable tuple (2,2,2), including empty mode. */
            wl_col_session_t raw = {0};
            wl_mem_ledger_init(&raw.mem_ledger, 0);
            raw.rels = registry; raw.nrels = raw.rel_cap = 3;
            eval_stack_t raw_stack = {0};
            CHECK(!col_eval_relation_plan(&body, &raw_stack, &raw));
            CHECK(raw_stack.top == 1 && raw_stack.items[0].rel->nrows == 1);
            for (unsigned c = 0; c < 3; ++c)
                CHECK(raw_stack.items[0].rel->columns[c][0] == 2);
            CHECK(!eval_stack_drain(&raw_stack));
            col_session_free_sorted_arrangements(&raw);
            session_rel_free_hash(&raw);

            fixture_t f; composed_fixture(&f, private_a, 0);
            wl_columnar_memory_governor_ref_t *governor = governed
                ? composed_governor(&f) : NULL;
            f.session.base.evaluation_control = f.operation;
            f.session.rels = registry; f.session.nrels = f.session.rel_cap = 3;
            const char *binding_names[] = {"a"};
            col_rel_t *binding_relations[] = {private_a};
            wl_columnar_eval_private_bindings_t bindings = {0};
            CHECK(!wl_columnar_eval_private_bindings_begin(&bindings,
                &f.session, &body, f.operation, f.operation_id,
                binding_names, binding_relations, 1));
            if (mode == 2) {
                /* Real mutation after binding capture invalidates its source
                 * snapshot. Neither lookup nor admission may use the public
                 * head to repair that stale authority. */
                int64_t value = 99;
                CHECK(!col_rel_append_row(private_a, &value));
                const char *name = NULL;
                col_rel_t *relation = NULL;
                CHECK(wl_columnar_eval_private_binding_get(&bindings, 0,
                    &name, &relation) == EINVAL);
            }
            int begin =
                wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
                &f.record, &body, 0, 1, f.frame, &f.session, 0,
                f.operation, f.operation_id, f.funding, f.funding_id,
                &bindings);
            CHECK(begin == (mode == 2 ? EINVAL : 0));
            if (!begin) composed_run(&f);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            CHECK(!f.record.semantic_result);
            if (mode == 2) CHECK(!stack->top);
            else {
                CHECK(stack->top == 1 && stack->items[0].rel->ncols == 3
                    && stack->items[0].rel->nrows == (mode == 1 ? 0u : 2u));
                for (uint32_t row = 0; row < stack->items[0].rel->nrows; ++row)
                    for (unsigned c = 0; c < 3; ++c)
                        CHECK(stack->items[0].rel->columns[c][row]
                            == (int64_t)private_values[row]);
            }
            for (unsigned i = 0; i < 3; ++i)
                CHECK(wl_columnar_relation_snapshot_equal(snapshots[i],
                    wl_columnar_relation_snapshot(registry[i])));
            CHECK(session_find_rel(&f.session, "a") == registry[0]);
            session_rel_free_hash(&f.session);
            composed_close(&f);
            for (unsigned i = 0; i < 3; ++i) col_rel_destroy(registry[i]);
            if (governor) {
                CHECK(!wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)));
                wl_columnar_memory_governor_ref_release(governor);
            }
        }
}

static void
composed_close(fixture_t *f)
{
    bool acknowledged = !f->record.composed_mode && !f->record.identity;
    CHECK(acknowledged
        ? f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT
        : f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
    if (!acknowledged)
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY);
    int result = f->record.semantic_result;
    if (!acknowledged) CHECK(!composed_host_turn(f, true));
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
    col_session_free_delta_arrangements(&raw);
    CHECK(!raw.darr_entries && !raw.darr_count && !raw.darr_cap);
    for (uint32_t i = 0; i < raw.arr_count; ++i) {
        CHECK(!raw.arr_entries[i].pin_count);
        free(raw.arr_entries[i].rel_name); free(raw.arr_entries[i].key_cols);
        arr_free_contents(&raw.arr_entries[i].arr);
        col_arr_detach_memory_governor(&raw.arr_entries[i].arr);
    }
    free(raw.arr_entries); session_rel_free_hash(&raw);
    CHECK(!raw.sarr_active_pins);
    col_session_free_sorted_arrangements(&raw);
    CHECK(!raw.sarr_entries && !raw.sarr_count);
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
        int cleanup_rc = composed_cleanup_lftj_turn(&f, 0, 0);
        CHECK(!cleanup_rc);
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
    CHECK(f.record.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE
        && f.record.lftj.identity == &f.record.lftj);
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
        CHECK(!f.record.semantic_result && f.record.completed == 2 - prebound
            && !f.record.source_slot_borrowed && !f.record.source_initialized);
        eval_stack_t *result = wl_columnar_eval_stack_cleanup_stack(f.frame);
        CHECK(result->top == 1);
        if (!prebound) CHECK(!result->items[0].rel->nrows
                && !result->items[0].rel->timestamps);
        else CHECK(result->items[0].rel->timestamps);
        composed_close(&f);
    }
}

static fixture_t *pipeline_lookup_fixture;
static wl_columnar_eval_stack_cleanup_frame_t *pipeline_lookup_nested;
static unsigned pipeline_lookup_ordinal, pipeline_lookup_target,
    pipeline_lookup_phase, pipeline_lookup_hits;
static bool pipeline_lookup_cancel;

static void
pipeline_lookup_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t value)
{
    fixture_t *f = pipeline_lookup_fixture;
    CHECK(f && control == f->operation);
    if (phase == 5 && !value) ++pipeline_lookup_ordinal;
    if (pipeline_lookup_hits ||
        pipeline_lookup_ordinal != pipeline_lookup_target
        || phase != pipeline_lookup_phase) return;
    /* This genuine nested frame keeps the original frame/storage alive while
     * changing which frame is active. No ownership fields are assigned. */
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f->session,
        &pipeline_lookup_nested));
    CHECK(f->session.cleanup_active == pipeline_lookup_nested);
    ++pipeline_lookup_hits;
    if (pipeline_lookup_cancel) wl_evaluation_control_request_cancel(control);
}

static col_rel_t *
pipeline_lookup_right(wl_columnar_memory_governor_ref_t *governor,
    const char *name, int64_t value)
{
    col_rel_t *right = NULL;
    CHECK(!wl_columnar_relation_new_auto_governed(name, 2, 1, false,
        governor, &right));
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64};
    CHECK(!col_rel_set_column_types(right, types, 2));
    int64_t row[] = {1, value};
    CHECK(!col_rel_append_row(right, row));
    return right;
}

static void
test_composed_pipeline_lookup_lineage(void)
{
    static const char text[] =
        ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
        ".decl output(k:int64,x:int64,k2:int64,y:int64)\n"
        "output(k,x,k,y) :- a(k,x), b(k,y), x < y.\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *parsed = composed_find_relation(plan, "output");
    CHECK(parsed && parsed->op_count == 5);
    for (unsigned which = 1; which <= 2; ++which)
        for (unsigned seam = 0; seam < 3; ++seam)
            for (unsigned cancel = 0; cancel < (seam ? 2u : 1u); ++cancel) {
                wl_plan_op_t ops[5]; memcpy(ops, parsed->ops, sizeof(ops));
                ops[1].delta_mode = which == 1 ? WL_DELTA_FORCE_FULL
                    : WL_DELTA_FORCE_DELTA;
                wl_plan_relation_t body = *parsed; body.ops = ops;
                const double values[] = {1};
                col_rel_t *left = composed_source("a", 2, values, 1, false);
                fixture_t f; composed_fixture(&f, left, 0);
                wl_columnar_memory_resolution_t resolution = {
                    .budget_bytes = 1u << 24, .usable_bytes = 1u << 24,
                        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
                };
                wl_columnar_memory_governor_ref_t *governor =
                    wl_columnar_memory_governor_ref_create(&resolution);
                CHECK(governor);
                col_rel_t *right = pipeline_lookup_right(governor, "b", 3);
                col_rel_t *delta = pipeline_lookup_right(governor, "$d$b", 4);
                col_rel_t *registry[] = {left, right, delta};
                f.session.rels = registry;
                f.session.nrels = f.session.rel_cap = 3;
                f.session.join_batch_bytes = 224;
                CHECK(!f.session.memory_governor && !left->memory_governor);
                CHECK(
                    !
                    wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback
                    (
                        &f.record, &body, 0, body.op_count, f.frame, &f.session,
                        0,
                        f.operation, f.operation_id, f.funding, f.funding_id));
                for (unsigned turns = 0; f.record.phase !=
                    WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH; ++turns) {
                    CHECK(turns < 1000); composed_action(&f);
                }
                CHECK(!f.record.join_host && !f.record.private_bindings);
                uint64_t reserved = wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor));
                if (seam) {
                    pipeline_lookup_fixture = &f;
                    pipeline_lookup_nested = NULL;
                    pipeline_lookup_target = which;
                    pipeline_lookup_phase = seam == 1 ? 5 : 6;
                    pipeline_lookup_ordinal = pipeline_lookup_hits = 0;
                    pipeline_lookup_cancel = cancel != 0;
                    wl_columnar_join_test_attempt_checkpoint =
                        pipeline_lookup_checkpoint;
                    composed_action(&f);
                    wl_columnar_join_test_attempt_checkpoint = NULL;
                    CHECK(pipeline_lookup_hits == 1 && pipeline_lookup_nested
                        && f.record.join_host_terminal && !f.record.join_host
                        && !f.record.join_host_governor
                        && !f.record.join_host_admitted
                        && !f.record.join_host_committed);
                    CHECK(f.record.phase ==
                        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
                        && f.record.child_status.semantic_result == (cancel
                        ? WL_EVALUATION_CONTROL_CANCELLED : EINVAL));
                    CHECK(wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(governor)) ==
                        reserved);
                    CHECK(!wl_columnar_eval_stack_cleanup_finish(
                            &pipeline_lookup_nested));
                    pipeline_lookup_fixture = NULL;
                }
                composed_run(&f);
                if (seam) CHECK(f.record.semantic_result == (cancel
                        ? WL_EVALUATION_CONTROL_CANCELLED : EINVAL));
                else {
                    CHECK(!f.record.semantic_result);
                    composed_raw_parity(&f, &body, 0);
                    eval_stack_t *stack =
                        wl_columnar_eval_stack_cleanup_stack(f.frame);
                    col_rel_t *out = stack->items[0].rel;
                    CHECK(out->nrows == 1 && out->ncols == 4
                        && out->columns[0][0] == 1 && out->columns[1][0] == 2
                        && out->columns[2][0] == 1
                        && out->columns[3][0] == (which == 1 ? 3 : 4));
                }
                composed_close(&f);
                col_rel_destroy(right); col_rel_destroy(delta);
                CHECK(!wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)));
                wl_columnar_memory_governor_ref_release(governor);
            }
    wl_plan_free(plan); wirelog_program_free(program);
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
    /* General plan admission validates each selector in its charged
     * preflight before any child or source installation. */
    rejected_ops[0].delta_mode = (wl_delta_mode_t)99;
    wl_plan_relation_t rejected_body = *body;
    rejected_body.ops = rejected_ops;
    fixture_t rejected;
    composed_fixture(&rejected, composed_source("a", 2, left_values, 4, false),
        0);
    col_rel_t *rejected_registry[] = {rejected.input, right};
    rejected.session.rels = rejected_registry;
    rejected.session.nrels = rejected.session.rel_cap = 2;
    rejected.session.join_batch_bytes = 224;
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback(
            &rejected.record, &rejected_body, 0, rejected_body.op_count,
            rejected.frame, &rejected.session, 0, rejected.operation,
            rejected.operation_id, rejected.funding, rejected.funding_id));
    composed_run(&rejected);
    CHECK(rejected.record.semantic_result == ENOTSUP
        && !rejected.record.completed && !rejected.record.source_installed
        && !rejected.record.source_initialized && !rejected.record.join_host);
    composed_close(&rejected);
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

static void
test_composed_concat_stopped_retirement_retry(void)
{
    static const double a_values[] = {3, 1, 3, -2};
    static const double b_values[] = {1, 4, 0};
    wl_plan_op_t concat = {.op = WL_PLAN_OP_CONCAT};
    wl_plan_relation_t body = {.ops = &concat, .op_count = 1};
    col_rel_t *a = composed_source("a", 1, a_values, 4, false);
    col_rel_t *b = composed_source("b", 1, b_values, 3, false);
    fixture_t f;
    composed_fixture(&f, a, 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(!eval_stack_push(stack, a, true));
    CHECK(!eval_stack_push(stack, b, true));
    composed_begin(&f, &body, NULL, 0, 2);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(b, &reader));

    for (unsigned turns = 0;
        !(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD
        && f.record.child_status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED); ++turns) {
        CHECK(turns < 100000 && f.record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
        composed_action(&f);
    }
    CHECK(f.record.child_status.committed && stack->top == 1
        && stack->items[0].owned && stack->items[0].rel);
    col_rel_t *published = stack->items[0].rel;

    /* Stop while the committed private result is retained behind an input
     * reader. Every refusal must preserve that result until the reader goes. */
    CHECK(!composed_abort_request(&f, 0));
    for (unsigned retry = 0; retry < 3; ++retry) {
        composed_action(&f);
        CHECK(f.record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
            && f.record.pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD);
        int receipt = f.pending_protocol;
        CHECK(composed_abort_request(&f, 0) == EBUSY);
        CHECK(!composed_host_turn(&f, false));
        CHECK(f.record.protocol_result == receipt
            && f.record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD
            && f.record.child_status.progress ==
            WL_COLUMNAR_FILTER_PROGRESS_RETAINED
            && f.record.child_status.committed
            && stack->top == 1 && stack->items[0].rel == published
            && stack->items[0].owned);
    }

    CHECK(!col_rel_source_reader_release(&reader));
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && f.record.cursor == 0 && f.record.completed == 0
        && stack->top == 1 && stack->items[0].rel == published
        && stack->items[0].owned);
    CHECK(!composed_host_turn(&f, true));
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_INERT
        && !f.record.composed_mode && !f.record.identity
        && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
        && stack->top == 1 && stack->items[0].rel == published
        && stack->items[0].owned);
    f.input = NULL;
    composed_close(&f);
}

static void
test_numeric_funded_scalar_finish(void)
{
    const uint32_t widths[] = {1, 65, 257};
    for (unsigned w = 0; w < 3; ++w)
        for (unsigned owned = 0; owned < 2; ++owned)
            for (unsigned commit = 0; commit < 2; ++commit) {
                uint32_t width = widths[w];
                fixture_t f; fixture_init(&f, width, 3, 0);
                wl_columnar_memory_governor_ref_t *governor =
                    join_governor(&f, 16u * 1024u * 1024u);
                int64_t *codes = malloc((size_t)width * sizeof(*codes));
                int64_t *values = malloc((size_t)width * sizeof(*values));
                CHECK(codes && values);
                for (uint32_t c = 0; c < width; ++c)
                    codes[c] = WIRELOG_TYPE_INT64;
                col_rel_t *input = NULL;
                CHECK(
                    !
                    wl_columnar_relation_new_typed_codes_stamped_operator_attempt_controlled
                    (
                        "$old", width, 3, codes, w != 0, governor, &input,
                        f.operation, f.operation_id));
                for (unsigned r = 0; r < 3; ++r) {
                    for (uint32_t c = 0; c < width; ++c) values[c] = r + c;
                    CHECK(!col_rel_append_row(input, values));
                }
                col_rel_destroy(f.input); f.input = input;
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                stack->items[0].rel = input; stack->items[0].owned = owned;
                uint32_t projection = 0;
                wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1,
                                   .project_indices = &projection};
                wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
                fixture_begin(&f, &plan, 0, 1);
                CHECK(!wl_columnar_eval_stack_unary_begin_attempt_owned(f.frame,
                    f.operation, f.operation_id, &f.record));
                col_rel_t **slot = NULL;
                CHECK(!wl_columnar_eval_stack_unary_output_owned(f.frame, &slot,
                    &f.record));
                CHECK(
                    !
                    wl_columnar_relation_new_typed_codes_stamped_operator_attempt_controlled
                    (
                        "$new", width, 3, codes, w != 0, governor, slot,
                        f.operation, f.operation_id));
                col_rel_t *output = *slot;
                bool ready = false;
                for (unsigned turn = 0; !ready; ++turn) {
                    CHECK(turn < 100000);
                    CHECK(
                        !
                        wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned
                        (
                            f.frame, &f.record, &ready));
                }
                if (commit) {
                    bool committed = false;
                    CHECK(!wl_columnar_eval_stack_unary_commit_attempt_owned(
                            f.frame, &committed, &f.record) && committed);
                    CHECK(stack->items[0].rel == output);
                }
                int initiating = commit ? 0 : ERANGE;
                CHECK(wl_evaluation_control_finish(f.operation,
                    &f.operation_owner, initiating, 0) == initiating);
                wl_evaluation_control_report_t original, after;
                CHECK(!wl_evaluation_control_report(f.operation, &original));
                bool done = true; int semantic = 123;
                CHECK(wl_columnar_eval_stack_unary_finish_scalar_funded_owned(
                        f.frame, initiating, &f.record, f.operation,
                        f.operation_id,
                        &semantic,
                        &done) == EINVAL && !done && semantic == 123);
                CHECK(wl_columnar_eval_stack_unary_reset_owned(f.frame,
                    &f.record) == EBUSY);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
                done = false;
                for (unsigned turn = 0; !done; ++turn) {
                    CHECK(turn < 100000);
                    int funding_owner;
                    wl_evaluation_control_t *funding = control_new(384,
                            &funding_owner);
                    uint64_t id =
                        wl_evaluation_control_active_attempt_id(funding);
                    CHECK(
                        !wl_columnar_eval_stack_unary_finish_scalar_funded_owned
                        (
                            f.frame, turn ? EIO : initiating, &f.record,
                            funding, id, &semantic, &done));
                    CHECK(semantic == initiating);
                    CHECK(!wl_evaluation_control_finish(funding, &funding_owner,
                        0, 0));
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(funding, &report)
                        && report.consumed && report.consumed <= 384);
                    CHECK(!wl_evaluation_control_detach(funding,
                        &funding_owner));
                    wl_evaluation_control_release(funding);
                }
                CHECK(!wl_evaluation_control_report(f.operation, &after)
                    && after.attempt_id == original.attempt_id
                    && after.consumed == original.consumed
                    && after.result == original.result);
                CHECK(stack->items[0].rel == (commit ? output : input));
                if (!commit) CHECK(input->nrows == 3 &&
                        input->columns[0][2] == 2);
                CHECK(!wl_columnar_eval_stack_unary_reset_owned(f.frame,
                    &f.record));
                if (owned) f.input = NULL;
                run(&f); /* Inactive original ID fails closed; cleanup still works. */
                CHECK(f.record.semantic_result == EINVAL);
                fixture_close(&f);
                CHECK(!wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)));
                wl_columnar_memory_governor_ref_release(governor);
                free(codes); free(values);
            }
}

static void
test_numeric_funded_scalar_contention(void)
{
    fixture_t f; fixture_init(&f, 65, 3, 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    stack->items[0].owned = true;
    uint32_t projection = 0;
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1,
                       .project_indices = &projection};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    fixture_begin(&f, &plan, 0, 1);
    CHECK(!wl_columnar_eval_stack_unary_begin_attempt_owned(f.frame,
        f.operation, f.operation_id, &f.record));
    col_rel_t **slot = NULL;
    CHECK(!wl_columnar_eval_stack_unary_output_owned(f.frame, &slot,
        &f.record));
    *slot = source_new(1, 2);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(*slot, &reader));
    bool ready = true;
    CHECK(!wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned(
            f.frame, &f.record, &ready) && !ready);
    bool committed = true;
    CHECK(wl_columnar_eval_stack_unary_commit_attempt_owned(f.frame,
        &committed, &f.record) == EBUSY && !committed);
    wl_evaluation_control_request_cancel(f.operation);
    CHECK(wl_evaluation_control_charge_attempt(f.operation, f.operation_id, 0)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
        WL_EVALUATION_CONTROL_CANCELLED, 0) == WL_EVALUATION_CONTROL_CANCELLED);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(f.operation, &before));
    bool done = false, released = false; int semantic = 123;
    for (unsigned turn = 0; !done; ++turn) {
        CHECK(turn < 10000);
        int owner;
        wl_evaluation_control_t *funding = control_new(turn ? 384 : 1, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        int rc = wl_columnar_eval_stack_unary_finish_scalar_funded_owned(
            f.frame, WL_EVALUATION_CONTROL_CANCELLED, &f.record, funding, id,
            &semantic, &done);
        if (!turn) CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
                && semantic == 123 && !done);
        else {
            CHECK(semantic == WL_EVALUATION_CONTROL_CANCELLED);
            if (rc) {
                CHECK(rc == EBUSY && !released && !done);
                CHECK(f.input->nrows == 3 && stack->items[0].rel == f.input);
                CHECK(!col_rel_source_reader_release(&reader)); released = true;
            }
            if (!released && turn >= 5) {
                CHECK(!done && stack->items[0].rel == f.input
                    && f.input->nrows == 3);
                CHECK(!col_rel_source_reader_release(&reader)); released = true;
            }
        }
        CHECK(wl_evaluation_control_finish(funding, &owner, rc, 0) == rc);
        CHECK(!wl_evaluation_control_detach(funding, &owner));
        wl_evaluation_control_release(funding);
    }
    CHECK(released && !wl_evaluation_control_report(f.operation, &after)
        && before.consumed == after.consumed && before.result == after.result);
    CHECK(stack->items[0].rel == f.input && f.input->nrows == 3);
    CHECK(!wl_columnar_eval_stack_unary_reset_owned(f.frame, &f.record));
    f.input = NULL; run(&f); fixture_close(&f);
}

/* Real governed constructor image, distinct from initialized empty schema. */
static col_rel_t *
bare_source(const char *name)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1u << 20, .usable_bytes = 1u << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    col_rel_t *rel = NULL;
    CHECK(!wl_columnar_relation_alloc_governed(&rel, name, governor));
    wl_columnar_memory_governor_ref_release(governor);
    rel->declared_ncols = 2;
    CHECK(!rel->schema_ok && !rel->ncols && !rel->nrows
        && rel->storage_owner == rel && !rel->schema.release);
    return rel;
}

static void
test_bare_source_borrow(void)
{
    for (unsigned composed = 0; composed < 2; ++composed)
        for (unsigned full = 0; full < 2; ++full) {
            fixture_t f; source_fixture(&f, 2, 0, 0);
            col_rel_destroy(f.input); f.input = bare_source("input");
            f.input->has_graph_column = true; f.input->graph_col_idx = 1;
            wl_plan_op_t ops[] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                 .delta_mode = full ? WL_DELTA_FORCE_FULL : WL_DELTA_AUTO},
                {.op = WL_PLAN_OP_JOIN, .right_relation = "absent"}
            };
            wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
            col_rel_t *registry[] = {f.input};
            if (composed) {
                composed_begin(&f, &plan, registry, 1, 0);
                composed_until_phase(&f,
                    WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL);
                composed_action(&f);
            } else {
                source_begin(&f, &plan, registry, 1);
                until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL);
                engine_dispatch(&f); ++f.witnesses;
            }
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            CHECK(stack->top == 1 && stack->items[0].rel == f.input
                && !stack->items[0].owned && !stack->items[0].is_delta);
            CHECK(!f.input->schema_ok && !f.input->schema.release
                && !f.input->ncols && !f.input->nrows
                && f.input->declared_ncols == 2 && f.input->has_graph_column
                && f.input->graph_col_idx == 1);
            CHECK(!f.record.source_bare && !f.record.source_initialized
                && f.record.source_empty_reader.phase ==
                WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED);
            if (composed) {
                composed_run(&f); CHECK(!f.record.semantic_result);
                composed_close(&f);
            } else {
                run(&f); CHECK(!f.record.semantic_result); fixture_close(&f);
            }
        }
}

static void
test_bare_ordinary_join(void)
{
    static const char *keys[] = {"col0"};
    for (unsigned empties = 1; empties < 4; ++empties)
        for (unsigned shape = 0; shape < 3; ++shape) {
            fixture_t f; source_fixture(&f, 2, 3, 0);
            if (empties & 1) {
                col_rel_destroy(f.input); f.input = bare_source("input");
            }
            col_rel_t *right = empties & 2 ? bare_source("right")
                : source_new(2, 3);
            if (!(empties & 2)) CHECK(!wl_columnar_relation_rename_checked(
                        right, "right"));
            bool mismatch = shape == 2 && empties != 3;
            if (mismatch) {
                static const double values[] = {1, 2, 3};
                if (empties & 1) {
                    col_rel_destroy(right);
                    right = composed_source("right", 2, values, 3, true);
                } else {
                    col_rel_destroy(f.input);
                    f.input = composed_source("input", 2, values, 3, true);
                }
            }
            uint32_t projection[] = {1, 0, 3};
            wl_plan_op_t ops[] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
                {.op = WL_PLAN_OP_JOIN, .right_relation = "right",
                 .key_count = 1, .left_keys = keys, .right_keys = keys,
                 .project_count = shape == 1 ? 3 : 0,
                 .project_indices = shape == 1 ? projection : NULL}
            };
            wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
            col_rel_t *registry[] = {f.input, right};
            composed_begin(&f, &plan, registry, 2, 0);
            composed_run(&f);
            eval_stack_t raw_stack = {0};
            CHECK(!eval_stack_push(&raw_stack, f.input, false));
            wl_col_session_t raw = {0}; raw.rels = registry; raw.nrels = 2;
            int raw_result = col_op_join(&ops[1], &raw_stack, &raw);
            CHECK(f.record.semantic_result == raw_result
                && raw_result == (mismatch ? EINVAL : 0));
            eval_stack_t *actual =
                wl_columnar_eval_stack_cleanup_stack(f.frame);
            CHECK(actual->top == 1);
            if (!mismatch) {
                same_relation(actual->items[0].rel, raw_stack.items[0].rel);
                CHECK(actual->items[0].owned && !actual->items[0].is_delta
                    && !actual->items[0].rel->nrows
                    && actual->items[0].rel->ncols == (shape == 1 ? 3
                    : empties == 3 ? 0 : 2));
            } else CHECK(actual->items[0].rel == f.input
                    && !actual->items[0].owned);
            CHECK(!eval_stack_drain(&raw_stack));
            for (uint32_t i = 0; i < raw.arr_count; ++i) {
                CHECK(!raw.arr_entries[i].pin_count);
                free(raw.arr_entries[i].rel_name);
                free(raw.arr_entries[i].key_cols);
                arr_free_contents(&raw.arr_entries[i].arr);
                col_arr_detach_memory_governor(&raw.arr_entries[i].arr);
            }
            free(raw.arr_entries);
            session_rel_free_hash(&raw);
            CHECK(!f.record.source_initialized && !f.record.source_bare);
            composed_close(&f); col_rel_destroy(right);
        }
}

/* Source END consumes the actual independent grant before HOST384. A
 * refused/dispatched receipt cannot install, ACK, or replace the first stop. */
static void
test_bare_source_cleanup_funding(void)
{
    fixture_t f; source_fixture(&f, 2, 0, 0);
    col_rel_destroy(f.input); f.input = bare_source("input");
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
        {.op = WL_PLAN_OP_JOIN, .right_relation = "absent"}
    };
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    col_rel_t *registry[] = {f.input};
    /* Keep a completed report while the successor owns live cleanup. */
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    f.operation_id = wl_evaluation_control_active_attempt_id(f.operation);
    composed_begin(&f, &plan, registry, 1, 0);
    composed_until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END);
    CHECK(f.record.source_bare && f.record.source_initialized);
    CHECK(col_rel_destroy_checked(f.input) == EBUSY);
    const char *names[] = {"a", "b"};
    CHECK(col_rel_set_schema(f.input, 2, names) == EBUSY);
    wl_evaluation_control_request_cancel(f.operation);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(f.operation, &before));
    for (unsigned turn = 0; f.record.source_initialized; ++turn) {
        CHECK(turn < 12 && f.record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END);
        int owner = 0;
        wl_evaluation_control_t *grant = control_new(turn ? 384 : 1, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(grant);
        int receipt = wl_columnar_eval_plan_numeric_composed_cleanup_source(
            &f.record, grant, id);
        CHECK(receipt == (turn ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
        CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
        CHECK(wl_columnar_eval_plan_numeric_composed_cleanup_source(&f.record,
            f.funding, f.funding_id) == EBUSY);
        CHECK(wl_evaluation_control_finish(grant, &owner, 0, 0) == receipt);
        wl_evaluation_control_report_t paid;
        CHECK(!wl_evaluation_control_report(grant, &paid));
        CHECK(paid.consumed >= 1 && paid.consumed < 384);
        CHECK(!wl_evaluation_control_detach(grant, &owner));
        wl_evaluation_control_release(grant);
        CHECK(!wl_columnar_eval_plan_numeric_composed_consume_result(&f.record,
            receipt, f.funding, f.funding_id));
        CHECK(!wl_evaluation_control_report(f.operation, &after));
        CHECK(after.consumed == before.consumed);
        CHECK(!wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
    }
    CHECK(f.record.source_empty_reader.phase ==
        WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED && !f.record.source_bare);
    composed_run(&f);
    CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!col_rel_set_schema(f.input, 2, names));
    int64_t row[] = {1, 2}; CHECK(!col_rel_append_row(f.input, row));
    composed_close(&f);
}

static void
test_bare_join_cleanup_receipt(void)
{
    fixture_t f; source_fixture(&f, 2, 3, 0);
    col_rel_t *right = bare_source("right");
    static const char *keys[] = {"col0"};
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
        {.op = WL_PLAN_OP_JOIN, .right_relation = "right", .key_count = 1,
         .left_keys = keys, .right_keys = keys}
    };
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    col_rel_t *registry[] = {f.input, right};
    composed_begin(&f, &plan, registry, 2, 0);
    wl_columnar_filter_context_t *ctx = NULL;
    for (unsigned turn = 0; ; ++turn) {
        CHECK(turn < 1000);
        if (f.record.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) {
            CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &ctx,
                &f.record));
            if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_RIGHT_SOURCE)
                break;
        }
        composed_action(&f);
    }
    bool pending = false;
    CHECK(!wl_columnar_filter_empty_end_pending_owned(f.frame, &f.record,
        &pending) && pending && ctx->join_right_bare);
    CHECK(wl_columnar_filter_op_attempt_step_owned(f.frame,
        &f.record.child_status,
        &f.record) == EBUSY);
    CHECK(col_rel_destroy_checked(right) == EBUSY);
    int owner = 0;
    wl_evaluation_control_t *grant = control_new(1, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(grant);
    CHECK(!wl_evaluation_control_charge_attempt(grant, id, 1));
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
        grant, id) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        && ctx->join_right_bare && !ctx->execution_result);
    CHECK(wl_evaluation_control_finish(grant, &owner, 0, 0) ==
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(grant, &owner));
    wl_evaluation_control_release(grant);
    grant = control_new(1, &owner);
    id = wl_evaluation_control_active_attempt_id(grant);
    int receipt = wl_columnar_eval_plan_numeric_composed_attempt_step(&f.record,
            grant, id);
    CHECK(receipt == WL_EVALUATION_CONTROL_WORK_LIMIT
        && f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        && f.record.pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD);
    CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_RIGHT_SOURCE
        && ctx->join_right_bare && ctx->join_right_initialized
        && !ctx->execution_result && !ctx->committed);
    CHECK(wl_evaluation_control_finish(grant, &owner, 0, 0) == receipt);
    CHECK(!wl_evaluation_control_detach(grant, &owner));
    wl_evaluation_control_release(grant);
    CHECK(!wl_columnar_eval_plan_numeric_composed_consume_result(&f.record,
        receipt, f.funding, f.funding_id));
    CHECK(f.record.protocol_result == receipt && !f.record.semantic_result);
    composed_run(&f);
    CHECK(!f.record.semantic_result && f.record.completed == 2
        && !ctx->join_right_bare && !ctx->source_bare
        && ctx->join_empty_reader.phase ==
        WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED);
    const char *names[] = {"k", "v"};
    CHECK(!col_rel_set_schema(right, 2, names));
    int64_t row[] = {1, 2}; CHECK(!col_rel_append_row(right, row));
    composed_close(&f); col_rel_destroy(right);
}

static void
test_bare_source_partial_stop(void)
{
    for (unsigned fault = 0; fault < 3; ++fault) {
        fixture_t f; source_fixture(&f, 2, 0, fault == 1 ? 1000000 : 0);
        col_rel_destroy(f.input); f.input = bare_source("input");
        wl_plan_op_t ops[] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
            {.op = WL_PLAN_OP_JOIN, .right_relation = "absent"}
        };
        wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
        col_rel_t *registry[] = {f.input};
        composed_begin(&f, &plan, registry, 1, 0);
        composed_until_phase(&f,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD);
        if (fault < 2) {
            /* Acquire exactly the descriptor gate and return a real pending
             * receipt before redirecting the stopped source to END. */
            composed_action(&f); composed_action(&f);
            CHECK(f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_HOLD
                && f.record.source_empty_reader.reader.acquire_stage == 1
                && f.record.source_empty_reader.reader.owner ==
                &f.input->descriptor_access);
            if (!fault) wl_evaluation_control_request_cancel(f.operation);
            else {
                CHECK(wl_evaluation_control_charge(f.operation, UINT64_MAX) ==
                    WL_EVALUATION_CONTROL_WORK_LIMIT);
            }
            CHECK(!wl_columnar_eval_plan_numeric_composed_cleanup_source(
                    &f.record, f.funding, f.funding_id));
        } else {
            f.input->capacity = 1; /* Malformed bare image; no payload exists. */
        }
        composed_run(&f);
        CHECK(f.record.semantic_result ==
            (!fault ? WL_EVALUATION_CONTROL_CANCELLED
            : fault == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : EINVAL));
        CHECK(!wl_columnar_eval_stack_cleanup_stack(f.frame)->top
            && !f.record.source_initialized && !f.record.source_bare
            && f.record.source_empty_reader.phase ==
            WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED);
        if (fault == 2) f.input->capacity = 0;
        composed_close(&f);
    }
}

static unsigned empty_filter_compile, empty_filter_buffer, empty_filter_recipe;
static unsigned empty_filter_stop;
static void
empty_filter_expr_hook(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed)
{
    (void)control; (void)progress; (void)failed;
    if (phase == 14) ++empty_filter_compile;
}

static void
empty_filter_join_hook(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase == 46) ++empty_filter_buffer;
    if (phase == 47) ++empty_filter_recipe;
    if ((empty_filter_stop == phase && (phase == 46 || phase == 47))
        || (empty_filter_stop == 24 && phase == 24 && empty_filter_compile))
        wl_evaluation_control_request_cancel(control);
}

static void
empty_filter_raw_drain(wl_col_session_t *raw)
{
    for (uint32_t i = 0; i < raw->arr_count; ++i) {
        CHECK(!raw->arr_entries[i].pin_count);
        free(raw->arr_entries[i].rel_name); free(raw->arr_entries[i].key_cols);
        arr_free_contents(&raw->arr_entries[i].arr);
        col_arr_detach_memory_governor(&raw->arr_entries[i].arr);
    }
    free(raw->arr_entries);
    col_session_free_filt_arrangements(raw);
    CHECK(!raw->filt_cache_active_pins);
    for (uint32_t i = 0; i < raw->filt_cache_count; ++i) {
        col_filt_cache_entry_t *entry = &raw->filt_cache[i];
        free(entry->rel_name); free(entry->filter_data);
        if (entry->metadata_reservation) {
            CHECK(wl_columnar_memory_rollback(entry->metadata_reservation));
            free(entry->metadata_reservation);
        }
        if (entry->filtered) CHECK(!col_rel_destroy_checked(entry->filtered));
    }
    free(raw->filt_cache);
    if (raw->filt_cache_array_reservation) {
        CHECK(wl_columnar_memory_rollback(raw->filt_cache_array_reservation));
        free(raw->filt_cache_array_reservation);
    }
    session_rel_free_hash(raw);
}

static void
test_empty_right_filter_setup(void)
{
    static const char *keys[] = {"col0"};
    static const uint8_t constant[] = {WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0,
                                       0, 0};
    static const uint8_t simple[] = {WL_PLAN_EXPR_VAR, 12, 0,
                                     'c', 'o', 'l', '9', '9', '9', '9', '9',
                                     '9', '9', '9', '9',
                                     WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0,
                                     0, 0, WL_PLAN_EXPR_CMP_GT};
    static const uint8_t two_vars[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l',
                                       '0',
                                       WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l',
                                       '1', WL_PLAN_EXPR_CMP_EQ};
    static const uint8_t string_cmp[] = {WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0,
                                         0, 0, 0,
                                         WL_PLAN_EXPR_CONST_INT, 2, 0, 0, 0, 0,
                                         0, 0, 0, WL_PLAN_EXPR_CMP_STR_LT};
    static const uint8_t hash[] = {WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0, 0,
                                   0,
                                   WL_PLAN_EXPR_ARITH_HASH};
    static const uint8_t crc_eth[] = {WL_PLAN_EXPR_ARITH_CRC32_ETH};
    static const uint8_t crc_cast[] = {WL_PLAN_EXPR_ARITH_CRC32_CAST};
    static const uint8_t ten_digits[] = {WL_PLAN_EXPR_VAR, 13, 0,
                                         'c', 'o', 'l', '1', '0', '0', '0', '0',
                                         '0', '0', '0', '0', '0'};
    static const uint8_t embedded_nul[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o',
                                           'l', 0};
    static const uint8_t float_var[] = {WL_PLAN_EXPR_VAR_FLOAT, 4, 0, 'c', 'o',
                                        'l', '0',
                                        WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0,
                                        0, 0, 0, WL_PLAN_EXPR_CMP_GT};
    static const uint8_t trailing[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l',
                                       '0',
                                       WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l',
                                       '1', WL_PLAN_EXPR_CMP_EQ,
                                       WL_PLAN_EXPR_BOOL, 1};
    const struct { const uint8_t *bytes; uint32_t size; bool compiled;
    } cases[] = {
        {NULL, 1, false}, {constant, sizeof(constant), true},
        {simple, sizeof(simple), false}, {two_vars, sizeof(two_vars), false},
        {string_cmp, sizeof(string_cmp), true}, {hash, sizeof(hash), false},
        {constant, 1, false}, {ten_digits, sizeof(ten_digits), false},
        {embedded_nul, sizeof(embedded_nul), false},
        {crc_eth, sizeof(crc_eth), false}, {crc_cast, sizeof(crc_cast), false},
        {two_vars, 2, false},
        {float_var, sizeof(float_var), true}, {trailing, sizeof(trailing), true}
    };
    for (unsigned bare = 0; bare < 2; ++bare)
        for (unsigned kind = 0; kind < sizeof(cases)/sizeof(cases[0]); ++kind) {
            fixture_t f; source_fixture(&f, 2, 3, 0);
            col_rel_t *right = bare ? bare_source("right") : source_new(2, 0);
            if (!bare) CHECK(!wl_columnar_relation_rename_checked(right,
                    "right"));
            wl_plan_op_t ops[] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
                {.op = WL_PLAN_OP_JOIN, .right_relation = "right",
                 .key_count = 1,
                 .left_keys = keys, .right_keys = keys,
                 .right_filter_expr = {(uint8_t *)cases[kind].bytes,
                                       cases[kind].size}}
            };
            wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
            col_rel_t *registry[] = {f.input, right};
            composed_begin(&f, &plan, registry, 2, 0);
            empty_filter_compile = empty_filter_buffer = empty_filter_recipe =
                0;
            empty_filter_stop = 0;
            wl_columnar_expr_test_checkpoint = empty_filter_expr_hook;
            wl_columnar_join_test_attempt_checkpoint = empty_filter_join_hook;
            if (!cases[kind].compiled) wl_columnar_expr_test_fail_allocation =
                    0;
            composed_run(&f);
            if (!cases[kind].compiled) CHECK(
                    wl_columnar_expr_test_fail_allocation == 0);
            wl_columnar_expr_test_fail_allocation = -1;
            wl_columnar_expr_test_checkpoint = NULL;
            wl_columnar_join_test_attempt_checkpoint = NULL;
            CHECK(empty_filter_buffer == 1 &&
                empty_filter_recipe == (kind != 0));
            CHECK((empty_filter_compile != 0) == cases[kind].compiled);
            eval_stack_t raw_stack = {0};
            CHECK(!eval_stack_push(&raw_stack, f.input, false));
            wl_col_session_t raw = {0}; raw.rels = registry; raw.nrels = 2;
            int expected = col_op_join(&ops[1], &raw_stack, &raw);
            CHECK(expected == (kind ? 0 : EINVAL)
                && f.record.semantic_result == expected);
            eval_stack_t *actual =
                wl_columnar_eval_stack_cleanup_stack(f.frame);
            if (!expected) {
                CHECK(actual->top == 1 && actual->items[0].owned
                    && !actual->items[0].rel->nrows);
                same_relation(actual->items[0].rel, raw_stack.items[0].rel);
            } else CHECK(actual->top == 1 && actual->items[0].rel == f.input);
            CHECK(!eval_stack_drain(&raw_stack)); empty_filter_raw_drain(&raw);
            composed_close(&f); CHECK(!col_rel_destroy_checked(right));
        }
    /* Raw graph recipe errors precede compiler/key work; NULL data precedes
     * even the graph recipe. Valid initialized physical graph remains usable. */
    for (unsigned scenario = 0; scenario < 9; ++scenario) {
        fixture_t f; source_fixture(&f, 2, 3, 0);
        bool bare = scenario != 2;
        col_rel_t *right = bare ? bare_source("right") : source_new(2, 0);
        if (!bare) CHECK(!wl_columnar_relation_rename_checked(right, "right"));
        right->has_graph_column = true; right->graph_col_idx = 1;
        if (scenario == 5) right->has_graph_column = false;
        if (scenario == 6) {
            static const double values[] = {1, 2, 3};
            col_rel_destroy(f.input);
            f.input = composed_source("input", 2, values, 3, true);
            right->has_graph_column = false;
        }
        wl_plan_op_t ops[] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
            {.op = WL_PLAN_OP_JOIN, .right_relation = "right", .key_count = 1,
             .left_keys = keys, .right_keys = keys,
             .right_filter_expr = {(uint8_t *)(scenario == 1 ||
                                   scenario >= 7 ? NULL : constant), 9}}
        };
        wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
        col_rel_t *registry[] = {f.input, right};
        if (scenario == 8) {
            f.session.delta_seeded = true;
            ops[1].delta_mode = WL_DELTA_FORCE_DELTA;
        }
        composed_begin(&f, &plan, registry, scenario == 7 ? 1 : 2, 0);
        empty_filter_compile = empty_filter_buffer = empty_filter_recipe = 0;
        empty_filter_stop = scenario >= 3 &&
            scenario <= 5 ? scenario == 3 ? 46 : scenario == 4 ? 47 : 24 : 0;
        wl_columnar_expr_test_checkpoint = empty_filter_expr_hook;
        wl_columnar_join_test_attempt_checkpoint = empty_filter_join_hook;
        composed_run(&f);
        wl_columnar_expr_test_checkpoint = NULL;
        wl_columnar_join_test_attempt_checkpoint = NULL;
        int expected = scenario >= 3 &&
            scenario <= 5 ? WL_EVALUATION_CONTROL_CANCELLED : scenario == 2 ||
            scenario >= 7 ? 0 : EINVAL;
        CHECK(f.record.semantic_result == expected);
        if (scenario == 1) CHECK(empty_filter_buffer == 1 &&
                !empty_filter_recipe);
        if (scenario == 5) CHECK(empty_filter_compile);
        if (scenario == 6) CHECK(empty_filter_compile && empty_filter_recipe);
        if (scenario >= 7) CHECK(!empty_filter_buffer && !empty_filter_compile);
        if (scenario < 3 || scenario >= 6) {
            eval_stack_t stack = {0};
            CHECK(!eval_stack_push(&stack, f.input, false));
            wl_col_session_t raw = {0}; raw.rels = registry;
            raw.nrels = scenario == 7 ? 1 : 2;
            raw.delta_seeded = f.session.delta_seeded;
            CHECK(col_op_join(&ops[1], &stack, &raw) == expected);
            CHECK(!eval_stack_drain(&stack)); empty_filter_raw_drain(&raw);
        }
        composed_close(&f); CHECK(!col_rel_destroy_checked(right));
    }
    empty_filter_stop = 0;
}

static void
test_empty_filter_compiler_resources(void)
{
    const uint8_t expression[] = {WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0, 0,
                                  0,
                                  WL_PLAN_EXPR_CONST_INT, 2, 0, 0, 0, 0, 0, 0,
                                  0, WL_PLAN_EXPR_CMP_STR_EQ};
    for (unsigned denial = 0; denial < 2; ++denial) {
        fixture_t f; source_fixture(&f, 2, 3, 0);
        col_rel_t *right = bare_source("right");
        wl_columnar_memory_governor_ref_t *ref = right->memory_governor;
        wl_columnar_memory_governor_t *gov =
            wl_columnar_memory_governor_ref_get(ref);
        f.session.memory_governor = ref;
        wl_plan_op_t ops[] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input"},
            {.op = WL_PLAN_OP_JOIN, .right_relation = "right",
             .right_filter_expr = {(uint8_t *)expression, sizeof(expression)}}
        };
        wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
        col_rel_t *registry[] = {f.input, right};
        composed_begin(&f, &plan, registry, 2, 0);
        composed_until(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY, 1);
        uint64_t baseline = wl_columnar_memory_reserved(gov);
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        if (denial) CHECK(wl_columnar_memory_reserve(gov, (1u<<20)-baseline-1,
                &peer));
        else wl_columnar_expr_test_fail_allocation = 0;
        empty_filter_compile = 0;
        wl_columnar_expr_test_checkpoint = empty_filter_expr_hook;
        composed_run(&f);
        wl_columnar_expr_test_checkpoint = NULL;
        wl_columnar_expr_test_fail_allocation = -1;
        CHECK(f.record.semantic_result == (denial ? ENOSPC : ENOMEM)
            && f.session.memory_budget_denied == (denial != 0));
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.input);
        CHECK(wl_columnar_memory_reserved(gov) ==
            baseline + (denial ? peer.bytes : 0));
        if (denial) CHECK(wl_columnar_memory_rollback(&peer));
        f.session.memory_governor = NULL;
        composed_close(&f);
        CHECK(!col_rel_destroy_checked(right));
    }
}

static void
test_forced_bare_selectors(void)
{
    for (unsigned composed = 0; composed < 2; ++composed)
        for (unsigned selector = 0; selector < 3; ++selector)
            for (unsigned state = 0; state < 3; ++state) {
                fixture_t f; source_fixture(&f, 2, 0, 0);
                if (state < 2) {
                    col_rel_destroy(f.input); f.input = bare_source("input");
                }
                if (state) {
                    f.input->has_graph_column = true;
                    f.input->graph_col_idx = 1;
                }
                const wl_delta_mode_t modes[] = {WL_DELTA_FORCE_EMPTY,
                                                 WL_DELTA_FORCE_DELTA,
                                                 WL_DELTA_FORCE_EMPTY_AFTER_SEED};
                f.session.delta_seeded = selector == 1;
                wl_plan_op_t ops[] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                     .delta_mode = modes[selector]},
                    {.op = WL_PLAN_OP_JOIN, .right_relation = "absent"}
                };
                wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
                col_rel_t *registry[] = {f.input};
                wl_col_session_t raw = {0}; raw.rels = registry; raw.nrels = 1;
                raw.delta_seeded = f.session.delta_seeded;
                eval_stack_t raw_stack = {0};
                int expected = col_op_variable(&ops[0], &raw_stack, &raw);
                CHECK(expected == (state == 1 && selector < 2 ? EINVAL : 0));
                if (composed) composed_begin(&f, &plan, registry, 1, 0);
                else source_begin(&f, &plan, registry, 1);
                if (!expected) {
                    if (composed) {
                        composed_until_phase(&f,
                            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL);
                        composed_action(&f);
                    } else {
                        until_phase(&f,
                            WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL);
                        engine_dispatch(&f); ++f.witnesses;
                    }
                    eval_stack_t *stack =
                        wl_columnar_eval_stack_cleanup_stack(f.frame);
                    CHECK(stack->top == 1 && raw_stack.top == 1);
                    const eval_entry_t *entry = &stack->items[0];
                    same_relation(entry->rel, raw_stack.items[0].rel);
                    CHECK(entry->owned == (selector < 2)
                        && entry->is_delta == (selector == 1));
                    if (selector < 2) CHECK(entry->rel != f.input
                            && entry->rel->schema_ok &&
                            entry->rel->schema.release);
                    else CHECK(entry->rel == f.input);
                    if (state < 2) CHECK(!f.input->schema_ok &&
                            !f.input->schema.release
                            && !f.input->ncols && f.input->declared_ncols == 2
                            && !f.record.source_initialized &&
                            !f.record.source_bare
                            && f.record.source_empty_reader.phase ==
                            WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED);
                }
                CHECK(!eval_stack_drain(&raw_stack));
                session_rel_free_hash(&raw);
                if (composed) {
                    composed_run(&f);
                    CHECK(f.record.semantic_result == expected);
                    composed_close(&f);
                } else {
                    run(&f); CHECK(f.record.semantic_result == expected);
                    fixture_close(&f);
                }
            }
}

static bool forced_constructor_stopped, forced_constructor_work;
static void
forced_constructor_stop(wl_evaluation_control_t *control, unsigned phase)
{
    if (forced_constructor_stopped || phase) return;
    forced_constructor_stopped = true;
    if (forced_constructor_work) CHECK(wl_evaluation_control_charge(control,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
test_forced_bare_failures(void)
{
    for (unsigned composed = 0; composed < 2; ++composed)
        for (unsigned fault = 0; fault < 5; ++fault) {
            fixture_t f; source_fixture(&f, 2, 0, fault == 3 ? 1000000 : 0);
            col_rel_destroy(f.input); f.input = bare_source("input");
            wl_columnar_memory_governor_ref_t *ref = f.input->memory_governor;
            wl_columnar_memory_governor_ref_retain(ref);
            wl_columnar_memory_governor_t *gov =
                wl_columnar_memory_governor_ref_get(ref);
            col_relation_snapshot_t snapshot =
                wl_columnar_relation_snapshot(f.input);
            wl_plan_op_t ops[] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                 .delta_mode = WL_DELTA_FORCE_EMPTY},
                {.op = WL_PLAN_OP_JOIN, .right_relation = "absent"}
            };
            wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
            col_rel_t *registry[] = {f.input};
            if (composed) {
                composed_begin(&f, &plan, registry, 1, 0);
                composed_until_phase(&f,
                    WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_NEW);
            } else {
                source_begin(&f, &plan, registry, 1);
                until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_NEW);
            }
            CHECK(f.record.source_bare && f.record.source_initialized);
            wl_columnar_memory_reservation_t peer;
            wl_columnar_memory_reservation_init(&peer);
            uint64_t baseline = wl_columnar_memory_reserved(gov);
            if (fault ==
                0) wl_columnar_relation_test_constructor_fail_allocation = 1;
            if (fault == 1) CHECK(wl_columnar_memory_reserve(gov,
                    (1u<<20)-baseline-1, &peer));
            forced_constructor_stopped = false;
            forced_constructor_work = fault == 3;
            if (fault == 2 || fault == 3)
                wl_columnar_relation_test_constructor_committed =
                    forced_constructor_stop;
            if (fault == 4) {
                if (composed) composed_until_phase(&f,
                        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_END);
                else until_phase(&f, WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_END);
                CHECK(f.record.source_output && *f.record.source_output);
                wl_evaluation_control_request_cancel(f.operation);
            }
            if (composed) composed_run(&f); else run(&f);
            wl_columnar_relation_test_constructor_committed = NULL;
            wl_columnar_relation_test_constructor_fail_allocation = 0;
            const int errors[] = {ENOMEM, ENOSPC,
                                  WL_EVALUATION_CONTROL_CANCELLED,
                                  WL_EVALUATION_CONTROL_WORK_LIMIT,
                                  WL_EVALUATION_CONTROL_CANCELLED};
            CHECK(f.record.semantic_result == errors[fault]
                && !f.record.source_installed && !f.record.source_initialized
                && !f.record.source_bare
                && f.record.source_empty_reader.phase ==
                WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED
                && !wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
            CHECK(f.session.memory_budget_denied == (fault == 1));
            if (fault == 2 || fault == 3) CHECK(forced_constructor_stopped);
            CHECK(wl_columnar_relation_snapshot_equal(snapshot,
                wl_columnar_relation_snapshot(f.input)));
            if (fault == 1) CHECK(wl_columnar_memory_rollback(&peer));
            if (composed) composed_close(&f); else fixture_close(&f);
            CHECK(!wl_columnar_memory_reserved(gov));
            wl_columnar_memory_governor_ref_release(ref);
        }
}

static col_rel_t *anti_identity_input;
static uint32_t *anti_identity_segments;
static unsigned anti_identity_commits;
static bool anti_identity_owned;
static void
anti_identity_cutoff(const eval_entry_t *retired, const eval_entry_t *entry)
{
    CHECK(entry->rel == anti_identity_input &&
        entry->owned == anti_identity_owned
        && !entry->is_delta && !entry->seg_boundaries && !entry->seg_count);
    CHECK(!retired->rel && !retired->owned && !retired->is_delta
        && !retired->continuation &&
        retired->kind == WL_COLUMNAR_EVAL_ENTRY_RELATION
        && retired->seg_boundaries == anti_identity_segments);
    CHECK(++anti_identity_commits == 1);
}

static void
anti_until_decision(fixture_t *f, wl_columnar_filter_context_t **ctx)
{
    for (unsigned turn = 0; ; ++turn) {
        CHECK(turn < 2000 &&
            f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        if (f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD) {
            CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, ctx,
                &f->record));
            if ((*ctx)->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE) return;
        }
        engine_dispatch(f); ++f->witnesses;
    }
}

static void
test_empty_anti_identity(void)
{
    static const char *keys[] = {"col0"};
    for (unsigned owned = 0; owned < 2; ++owned)
        for (unsigned bare = 0; bare < 2; ++bare)
            for (unsigned segments = 0; segments < 4; ++segments) {
                fixture_t f; fixture_init(&f, 2, 3, 0);
                /* Build valid native and Arrow names through the schema API. */
                col_rel_destroy(f.input); f.input = NULL;
                CHECK(!col_rel_alloc(&f.input, "left"));
                const char *names[] = {"col0", "payload"};
                CHECK(!col_rel_set_schema(f.input, 2, names));
                for (int64_t r = 0; r < 3; ++r) {
                    int64_t row[] = {r, r+1};
                    CHECK(!col_rel_append_row(f.input, row));
                }
                wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel =
                    f.input;
                f.input->sorted_nrows = 3; f.input->run_count = 1;
                f.input->run_ends[0] = 3;
                /* ANTI accepts non-colN names and valid physical graph metadata. */
                f.input->has_graph_column = true; f.input->graph_col_idx = 1;
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                eval_entry_t *entry = &stack->items[0];
                entry->owned = owned != 0; entry->is_delta = true;
                entry->seg_count = segments == 2 ? 7 : segments == 3 ? 0 : 2;
                if (segments != 2) {
                    entry->seg_boundaries = malloc(2*sizeof(uint32_t));
                    CHECK(entry->seg_boundaries);
                    entry->seg_boundaries[0] = 0; entry->seg_boundaries[1] = 3;
                }
                col_rel_t *right = bare ? bare_source("right") : source_new(2,
                        0);
                if (!bare) CHECK(!wl_columnar_relation_rename_checked(right,
                        "right"));
                col_rel_t *registry[] = {f.input, right};
                f.session.rels = registry; f.session.nrels = 2;
                wl_plan_op_t op = {.op = WL_PLAN_OP_ANTIJOIN,
                                   .right_relation = "right",
                                   .key_count = 1, .left_keys = keys,
                                   .right_keys = keys};
                wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
                col_rel_t *raw_left = NULL;
                CHECK(!col_rel_deep_copy(f.input, &raw_left, NULL));
                raw_left->sorted_nrows = 3; raw_left->run_count = 1;
                raw_left->run_ends[0] = 3;
                wl_col_session_t raw = {0};
                col_rel_t *raw_registry[] = {raw_left, right};
                raw.rels = raw_registry; raw.nrels = 2;
                eval_stack_t expected = {0};
                CHECK(!eval_stack_push_delta(&expected, raw_left, owned != 0,
                    true));
                expected.items[0].seg_count = entry->seg_count;
                if (entry->seg_boundaries) {
                    expected.items[0].seg_boundaries = malloc(2*
                            sizeof(uint32_t));
                    CHECK(expected.items[0].seg_boundaries);
                    memcpy(expected.items[0].seg_boundaries,
                        entry->seg_boundaries, 2*sizeof(uint32_t));
                }
                CHECK(!col_op_antijoin(&op, &expected, &raw));
                CHECK(expected.top == 1 && expected.items[0].rel == raw_left
                    && expected.items[0].owned == (owned != 0) &&
                    !expected.items[0].is_delta
                    && !expected.items[0].seg_boundaries &&
                    !expected.items[0].seg_count);
                col_relation_snapshot_t source_image =
                    wl_columnar_relation_snapshot(f.input);
                fixture_begin(&f, &plan, 0, 1);
                anti_identity_input = f.input; anti_identity_owned = owned != 0;
                anti_identity_segments = entry->seg_boundaries;
                anti_identity_commits = 0;
                wl_columnar_eval_stack_test_anti_metadata_committed =
                    anti_identity_cutoff;
                run(&f);
                wl_columnar_eval_stack_test_anti_metadata_committed = NULL;
                CHECK(!f.record.semantic_result && f.record.completed == 1 &&
                    anti_identity_commits == 1);
                CHECK(entry->rel == f.input && entry->owned == (owned != 0)
                    && !entry->is_delta && !entry->seg_boundaries &&
                    !entry->seg_count
                    && f.input->sorted_nrows == 3 && f.input->run_count == 1
                    && f.input->has_graph_column &&
                    !strcmp(f.input->col_names[1], "payload"));
                same_relation(entry->rel, expected.items[0].rel);
                CHECK(wl_columnar_relation_snapshot_equal(source_image,
                    wl_columnar_relation_snapshot(f.input)));
                CHECK(!eval_stack_drain(&expected));
                if (!owned) col_rel_destroy(raw_left);
                empty_filter_raw_drain(&raw);
                /* Normal ACK preserves the live relation; eventual frame owner
                * frees an owned input once. No relation enters the receipt. */
                CHECK(!wl_columnar_eval_plan_numeric_attempt_ack(&f.record,
                    f.funding, f.funding_id));
                CHECK(entry->rel == anti_identity_input &&
                    entry->rel->columns[0][1] == 1);
                if (owned) f.input = NULL;
                composed_close(&f); col_rel_destroy(right);
            }
}

static void
test_missing_probe_exact_order(void)
{
    for (unsigned semi = 0; semi < 2; ++semi)
        for (unsigned bare = 0; bare < 2; ++bare) {
            fixture_t f; fixture_init(&f, 2, 3, 0);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            if (bare) {
                col_rel_destroy(f.input); f.input = bare_source("input");
                stack->items[0].rel = f.input;
            } else CHECK(!col_rel_enable_timestamps(f.input));
            stack->items[0].is_delta = true;
            stack->items[0].seg_boundaries = malloc(2*sizeof(uint32_t));
            CHECK(stack->items[0].seg_boundaries);
            stack->items[0].seg_boundaries[0] = 0;
            stack->items[0].seg_boundaries[1] = f.input->nrows;
            stack->items[0].seg_count = 2;
            eval_entry_t before = stack->items[0];
            static const char *irrelevant[] = {NULL};
            wl_plan_op_t op =
            {.op = semi ? WL_PLAN_OP_SEMIJOIN : WL_PLAN_OP_ANTIJOIN,
             .right_relation = "missing", .project_count = 99,
             .project_indices = NULL,
             .key_count = 1, .left_keys = irrelevant,
             .right_keys = irrelevant};
            wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
            fixture_begin(&f, &plan, 0, 1); run(&f);
            CHECK(!f.record.semantic_result && f.record.completed == 1);
            const eval_entry_t *after = &stack->items[0];
            CHECK(after->rel == before.rel && after->owned == before.owned
                && after->is_delta == before.is_delta &&
                after->seg_boundaries == before.seg_boundaries
                && after->seg_count == before.seg_count &&
                after->kind == before.kind
                && after->continuation == before.continuation);
            /* The raw no-op ignores the same invalid projection/keys and
             * does not free/alter the borrowed original segment allocation. */
            wl_col_session_t raw = {0}; eval_stack_t expected = {0};
            expected.items[0] = before; expected.top = 1;
            CHECK(!(semi ? col_op_semijoin(&op, &expected,
                &raw) : col_op_antijoin(&op, &expected, &raw)));
            CHECK(expected.items[0].rel == before.rel
                && expected.items[0].seg_boundaries == before.seg_boundaries
                && expected.items[0].is_delta);
            expected.items[0] = (eval_entry_t){0}; expected.top = 0;
            fixture_close(&f);
        }
}

static void
test_present_empty_probe_metadata(void)
{
    static const char *keys[] = {"col0"};
    for (unsigned semi = 0; semi < 2; ++semi)
        for (unsigned left_state = 0; left_state < 4; ++left_state)
            for (unsigned right_bare = 0; right_bare < 2; ++right_bare) {
                fixture_t f; fixture_init(&f, 2, left_state == 2 ? 0 : 3, 0);
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                if (left_state == 0) {
                    col_rel_destroy(f.input); f.input = bare_source("input");
                    stack->items[0].rel = f.input;
                }
                if (left_state == 1) CHECK(!col_rel_enable_timestamps(f.input));
                if (left_state == 3) {
                    free(f.input->col_names[1]); f.input->col_names[1] = NULL;
                }
                col_rel_t *right =
                    right_bare ? bare_source("right") : source_new(2, 0);
                if (!right_bare) CHECK(!wl_columnar_relation_rename_checked(
                            right, "right"));
                col_rel_t *registry[] = {f.input, right};
                f.session.rels = registry; f.session.nrels = 2;
                uint32_t projection[] = {1, 0, 19};
                wl_plan_op_t op =
                {.op = semi ? WL_PLAN_OP_SEMIJOIN : WL_PLAN_OP_ANTIJOIN,
                 .right_relation = "right", .key_count = 1,
                 .left_keys = keys, .right_keys = keys,
                 .project_count = semi ? 3 : 0,
                 .project_indices = semi ? projection : NULL};
                wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
                fixture_begin(&f, &plan, 0, 1); run(&f);
                CHECK(!f.record.semantic_result && f.record.completed == 1);
                const col_rel_t *actual = stack->items[0].rel;
                CHECK(stack->items[0].owned && !stack->items[0].is_delta &&
                    !actual->timestamps
                    && actual != f.input &&
                    actual->ncols == (semi ? 3 : f.input->ncols)
                    && actual->nrows == (semi || left_state == 0 ||
                    left_state == 2 ? 0 : 3));
                if (!semi && actual->nrows) for (uint32_t r = 0; r < 3; ++r)
                        for (uint32_t c = 0; c < 2;
                            ++c) CHECK(actual->columns[c][r] ==
                                f.input->columns[c][r]);
                eval_stack_t expected = {0};
                CHECK(!eval_stack_push(&expected, f.input, false));
                wl_col_session_t raw = {0}; raw.rels = registry; raw.nrels = 2;
                CHECK(!(semi ? col_op_semijoin(&op, &expected, &raw)
                    : col_op_antijoin(&op, &expected, &raw)));
                same_relation(actual, expected.items[0].rel);
                CHECK(expected.items[0].owned && !expected.items[0].is_delta);
                CHECK(!eval_stack_drain(&expected));
                empty_filter_raw_drain(&raw);
                fixture_close(&f); col_rel_destroy(right);
            }
}

static void
anti_cancel_cutoff(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase == WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_CUTOFF_PHASE)
        wl_evaluation_control_request_cancel(control);
}

static fixture_t *anti_reentry_fixture;
static wl_columnar_filter_context_t *anti_reentry_context;
static unsigned anti_reentry_checkpoints, anti_reentry_calls;
static bool anti_reentry_late_cancel;

static void
anti_reentry_refuse(void)
{
    fixture_t *f = anti_reentry_fixture;
    wl_columnar_filter_context_t *ctx = anti_reentry_context;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    eval_entry_t before = stack->items[0];
    bool committed = ctx->committed;
    unsigned checkpoints = anti_reentry_checkpoints;
    CHECK(wl_columnar_eval_stack_unary_commit_anti_keep_all_context_owned(
            f->frame, ctx, &ctx->committed, &f->record) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_commit_attempt_owned(
            f->frame, &ctx->committed, &f->record) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_commit_numeric_join_owned(
            f->frame, &ctx->committed, &f->record) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_commit_join_attempt_context_owned(
            f->frame, ctx, &ctx->committed, &f->record) == EBUSY);
    col_rel_t **output = NULL;
    CHECK(wl_columnar_eval_stack_unary_output_owned(f->frame, &output,
        &f->record) == EBUSY && !output);
    CHECK(wl_columnar_eval_stack_unary_reset_owned(f->frame,
        &f->record) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_finish(f->frame, 0) == EBUSY);
    CHECK(checkpoints == anti_reentry_checkpoints
        && ctx->committed == committed
        && !memcmp(&before, &stack->items[0], sizeof(before)));
    ++anti_reentry_calls;
}

static void
anti_reentry_tail(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)control; (void)progress;
    ++anti_reentry_checkpoints;
    if (phase == WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_CUTOFF_PHASE)
        anti_reentry_refuse();
}

static void
anti_reentry_committed(const eval_entry_t *retired, const eval_entry_t *entry)
{
    anti_identity_cutoff(retired, entry);
    anti_reentry_refuse();
    CHECK(retired->seg_boundaries == anti_identity_segments);
    if (anti_reentry_late_cancel)
        wl_evaluation_control_request_cancel(anti_reentry_fixture->operation);
}

static bool lftj_order_postcheck;

static void
lftj_same_effective_relation(const col_rel_t *actual, const col_rel_t *raw)
{
    CHECK(actual && raw && actual->ncols == raw->ncols &&
        actual->nrows == raw->nrows
        && actual->schema_ok == raw->schema_ok &&
        actual->declared_ncols == raw->declared_ncols
        && actual->has_graph_column == raw->has_graph_column
        && actual->graph_col_idx == raw->graph_col_idx
        && !actual->timestamps && !raw->timestamps);
    for (uint32_t c = 0; c < actual->ncols; ++c) {
        CHECK(!strcmp(actual->col_names[c], raw->col_names[c]));
        CHECK((actual->column_types ? actual->column_types[c] :
            WIRELOG_TYPE_INT64)
            == (raw->column_types ? raw->column_types[c] : WIRELOG_TYPE_INT64));
        if (actual->schema_ok)
            CHECK(!strcmp(actual->schema.children[c]->format,
                raw->schema.children[c]->format));
        for (uint32_t r = 0; r < actual->nrows; ++r)
            CHECK(actual->columns[c][r] == raw->columns[c][r]);
    }
}

/* Internal admitted whole plans, including the original FLOAT-empty red.
 * Input type facts drive LFTJ keys; only expression opcodes may establish a
 * typed successor result from its raw default-auto intermediate schema. */
static void
test_lftj_schema_continuations(void)
{
    static const wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_STRING
    };
    const char *join_keys[] = {"col0"}; uint32_t projection = 0;
    uint8_t add[17] = {WL_PLAN_EXPR_VAR_FLOAT, 4, 0, 'c', 'o', 'l', '0',
                       WL_PLAN_EXPR_CONST_FLOAT};
    int64_t one = wl_columnar_float_to_bits(1.0);
    memcpy(add+8, &one, 8); add[16] = WL_PLAN_EXPR_ARITH_FLOAT_ADD;
    wl_plan_expr_buffer_t expression = {add, sizeof(add)};
    for (unsigned domain = 0; domain < 4; ++domain)
        for (unsigned empty = 0; empty < 2; ++empty)
            for (unsigned context = 0; context < 6; ++context) {
                if (context == 5 && domain != 1) continue;
                fixture_t f; col_rel_t *relations[4];
                char *names[] = {"a", "b", "c", "d"};
                uint32_t keys[] = {0, 0, 0};
                col_relation_snapshot_t before[4];
                for (unsigned i = 0; i < 4; ++i) {
                    relations[i] = source_new(1, 0);
                    CHECK(!wl_columnar_relation_rename_checked(relations[i],
                        names[i]));
                    wirelog_column_type_t type = domain == 3
                        ? (i == 3 ? WIRELOG_TYPE_INT64 : WIRELOG_TYPE_STRING)
                        : types[domain];
                    CHECK(!col_rel_set_column_types(relations[i], &type, 1));
                    if (!empty) for (unsigned r = 0; r < 3; ++r) {
                            int64_t row = domain ==
                                1 ? wl_columnar_float_to_bits(r ? 2.5 : 1.5)
                            : (r ? 20 : 10);
                            CHECK(!col_rel_append_row(relations[i], &row));
                        }
                    before[i] = wl_columnar_relation_snapshot(relations[i]);
                }
                wl_plan_op_lftj_t metadata = {.k = 3, .rel_names = names,
                                              .key_cols = keys};
                wl_plan_op_t ops[3] = {{.op = WL_PLAN_OP_LFTJ,
                                        .opaque_data = &metadata}};
                uint32_t count = 1;
                if (context >= 1 && context <= 3) {
                    ops[count++] = (wl_plan_op_t){
                        .op = context == 1 ? WL_PLAN_OP_JOIN : context ==
                            2 ? WL_PLAN_OP_SEMIJOIN : WL_PLAN_OP_ANTIJOIN,
                        .right_relation = "d", .key_count = 1,
                        .left_keys = join_keys, .right_keys = join_keys
                    };
                } else if (context == 4) {
                    ops[count++] = (wl_plan_op_t){.op = WL_PLAN_OP_MAP,
                                                  .project_count = 1,
                                                  .project_indices =
                                                      &projection};
                } else if (context == 5) {
                    ops[count++] = (wl_plan_op_t){.op = WL_PLAN_OP_MAP,
                                                  .project_count = 1,
                                                  .map_exprs = &expression,
                                                  .map_expr_count = 1};
                }
                if (context) ops[count++] =
                        (wl_plan_op_t){.op = WL_PLAN_OP_CONSOLIDATE};
                wl_plan_relation_t body = {.ops = ops, .op_count = count};
                wl_col_session_t raw = {0}; raw.rels = relations; raw.nrels = 4;
                eval_stack_t expected = {0};
                int raw_rc = col_op_lftj(&ops[0], &expected, &raw);
                CHECK(!raw_rc && !expected.items[0].rel->column_types);
                for (uint32_t i = 1; !raw_rc && i < count; ++i) {
                    switch (ops[i].op) {
                    case WL_PLAN_OP_JOIN: raw_rc = col_op_join(&ops[i],
                                &expected, &raw); break;
                    case WL_PLAN_OP_SEMIJOIN: raw_rc = col_op_semijoin(&ops[i],
                                &expected, &raw); break;
                    case WL_PLAN_OP_ANTIJOIN: raw_rc = col_op_antijoin(&ops[i],
                                &expected, &raw); break;
                    case WL_PLAN_OP_MAP: raw_rc = col_op_map(&ops[i], &expected,
                                &raw); break;
                    case WL_PLAN_OP_CONSOLIDATE: raw_rc =
                            col_op_consolidate(&expected, &raw); break;
                    default: CHECK(false);
                    }
                }
                CHECK(raw_rc == (domain == 1 && context >= 1 &&
                    context <= 3 ? EINVAL : 0));
                composed_fixture(&f, source_new(1, 0), 0);
                f.session.base.evaluation_control = f.operation;
                f.session.rels = relations; f.session.nrels = 4;
                f.session.rel_cap = 4;
                CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin(
                        &f.record, &body,
                        0, count, f.frame, &f.session, 0, f.operation,
                        f.operation_id, f.funding, f.funding_id));
                composed_run(&f);
                CHECK(f.record.semantic_result == raw_rc &&
                    !session_find_rel(&f.session, "output"));
                eval_stack_t *actual =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                if (!raw_rc) {
                    CHECK(actual->top == 1 && expected.top == 1 &&
                        actual->items[0].owned);
                    lftj_same_effective_relation(actual->items[0].rel,
                        expected.items[0].rel);
                    if (!context) {
                        col_rel_t *out = actual->items[0].rel;
                        CHECK(!out->column_types &&
                            out->nrows == (empty ? 0u : 9u));
                        for (uint32_t r = 0; r < out->nrows; ++r)
                            for (uint32_t c = 0; c < out->ncols; ++c)
                                CHECK(out->columns[c][r] == (domain == 1
                                        ? wl_columnar_float_to_bits(r ? 2.5 :
                                    1.5) : (r ? 20 : 10)));
                    } else {
                        /* Non-FLOAT lanes compare their int64 key encoding.
                         * STRING IDs therefore match default-INT64 LFTJ
                         * output; domain3 retains the integer right canary. */
                        uint32_t rows = empty ? 0 : context == 3 ? 0 : 2;
                        CHECK(actual->items[0].rel->nrows == rows);
                    }
                    if (context == 5) {
                        col_rel_t *out = actual->items[0].rel;
                        CHECK(out->column_types &&
                            out->column_types[0] == WIRELOG_TYPE_FLOAT);
                        CHECK(out->nrows == (empty ? 0u : 2u));
                        if (!empty) CHECK(out->columns[0][0] ==
                                wl_columnar_float_to_bits(2.5)
                                && out->columns[0][1] ==
                                wl_columnar_float_to_bits(3.5));
                    }
                } else CHECK(f.record.completed == 1 && actual->top == 1
                        && actual->items[0].owned &&
                        !actual->items[0].rel->column_types);
                for (unsigned i = 0; i < 4; ++i)
                    CHECK(wl_columnar_relation_snapshot_equal(before[i],
                        wl_columnar_relation_snapshot(relations[i])));
                session_rel_free_hash(&f.session);
                composed_close(&f); CHECK(!eval_stack_drain(&expected));
                empty_filter_raw_drain(&raw);
                col_session_free_sorted_arrangements(&raw);
                for (unsigned i = 0; i < 4; ++i) col_rel_destroy(relations[i]);
            }
}
static void
lftj_order_checkpoint(wl_evaluation_control_t *control,
    const wl_columnar_lftj_attempt_t *a)
{
    (void)control;
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_ACQUIRE)
        lftj_order_postcheck = true;
}

/* Internal composed fixtures: the raw sizing pass precedes all type checks.
 * No fabricated large descriptors are used to provoke arithmetic overflow. */
static void
test_lftj_source_order(void)
{
    uint64_t checked;
    CHECK(!wl_columnar_memory_size_mul(UINT64_MAX, 2, &checked));
    CHECK(!wl_columnar_memory_size_add(UINT64_MAX, 1, &checked));
    CHECK(wl_columnar_memory_size_mul(UINT64_MAX/8, 8, &checked)
        && checked == UINT64_MAX-7);
    for (unsigned mode = 0; mode < 11; ++mode) {
        fixture_t f;
        col_rel_t *relations[3];
        char *names[] = {"a", "b", "c"}; uint32_t keys[] = {0, 0, 0};
        for (unsigned i = 0; i < 3; ++i) {
            relations[i] = source_new(1, 0);
            CHECK(!wl_columnar_relation_rename_checked(relations[i], names[i]));
        }
        if (mode < 2) {
            col_rel_destroy(relations[0]); relations[0] = bare_source("a");
            if (!mode) names[1] = "missing";
            else {
                names[0] = "missing"; names[1] = "a";
            }
        }
        if (mode == 2) {
            keys[1] = 1; names[2] = "missing";
        }
        if (mode == 3) {
            names[1] = "missing"; keys[2] = 1;
        }
        if (mode >= 4 && mode <= 6) {
            wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
            CHECK(!col_rel_set_column_types(relations[1], &type, 1));
            if (mode == 4) names[2] = "missing";
            if (mode == 5) keys[2] = 1;
        }
        if (mode >= 7) for (unsigned i = 0; i < 3; ++i) {
                wirelog_column_type_t type = mode == 8 ? WIRELOG_TYPE_FLOAT
                : mode == 9 ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64;
                CHECK(!col_rel_set_column_types(relations[i], &type, 1));
                if (mode == 10) {
                    relations[i]->has_graph_column = true;
                    relations[i]->graph_col_idx = 0;
                }
            }
        wl_plan_op_lftj_t metadata = {.k = 3, .rel_names = names,
                                      .key_cols = keys};
        wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ, .opaque_data = &metadata};
        wl_plan_relation_t body = {.ops = &op, .op_count = 1};
        wl_col_session_t raw = {0}; raw.rels = relations; raw.nrels = 3;
        eval_stack_t expected = {0};
        int raw_rc = col_op_lftj(&op, &expected, &raw);
        int error = mode >= 7 ? 0 : mode == 1 || mode == 3 ||
            mode == 4 ? ENOENT : EINVAL;
        CHECK(raw_rc == error);
        composed_fixture(&f, source_new(1, 0), 0);
        f.session.base.evaluation_control = f.operation;
        f.session.rels = relations; f.session.nrels = 3; f.session.rel_cap = 3;
        CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin(&f.record,
            &body, 0, 1, f.frame, &f.session, 0, f.operation, f.operation_id,
            f.funding, f.funding_id));
        lftj_order_postcheck = false;
        wl_columnar_lftj_test_attempt_checkpoint = lftj_order_checkpoint;
        composed_run(&f);
        wl_columnar_lftj_test_attempt_checkpoint = NULL;
        CHECK(f.record.semantic_result == raw_rc
            && lftj_order_postcheck == (mode >= 6));
        eval_stack_t *actual = wl_columnar_eval_stack_cleanup_stack(f.frame);
        CHECK(actual->top == expected.top);
        if (!error) {
            col_rel_t *out = actual->items[0].rel,
                *reference = expected.items[0].rel;
            CHECK(out->ncols == reference->ncols && !out->nrows &&
                !reference->nrows
                && actual->items[0].owned && !actual->items[0].is_delta
                && !out->timestamps && !reference->timestamps
                && out->declared_ncols == reference->declared_ncols
                && out->has_graph_column == reference->has_graph_column
                && out->graph_col_idx == reference->graph_col_idx
                && out->sorted_nrows == reference->sorted_nrows
                && out->run_count == reference->run_count);
            CHECK(!out->column_types && !reference->column_types
                && out->schema_ok == reference->schema_ok);
            for (uint32_t c = 0; c < out->ncols; ++c) {
                CHECK(!strcmp(out->col_names[c], reference->col_names[c]));
                wirelog_column_type_t effective = out->column_types
                    ? out->column_types[c] : WIRELOG_TYPE_INT64;
                CHECK(effective == (reference->column_types
                        ? reference->column_types[c] : WIRELOG_TYPE_INT64));
                if (out->schema_ok)
                    CHECK(!strcmp(out->schema.children[c]->format,
                        reference->schema.children[c]->format));
            }
        }
        composed_close(&f); CHECK(!eval_stack_drain(&expected));
        empty_filter_raw_drain(&raw);
        CHECK(!raw.sarr_active_pins);
        col_session_free_sorted_arrangements(&raw);
        CHECK(!raw.sarr_entries && !raw.sarr_count);
        for (unsigned i = 0; i < 3; ++i) col_rel_destroy(relations[i]);
    }
}

static void
test_lftj_source_order_stops(void)
{
    for (unsigned point = 0; point < 10; ++point) {
        fixture_t f; wl_plan_relation_t body; wl_plan_op_t op;
        wl_plan_op_lftj_t metadata; char *names[3]; uint32_t keys[3];
        col_rel_t *relations[3];
        composed_lftj_fixture(&f, 0, &body, &op, &metadata, names, keys,
            relations);
        bool reached = false;
        for (unsigned turn = 0; !reached; ++turn) {
            CHECK(turn < 2000 &&
                f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
            wl_columnar_lftj_attempt_t *a = &f.record.lftj;
            if (f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD && a->inputs) {
                wl_columnar_lftj_input_attempt_t *in = &a->inputs[0];
                reached = point == 0 ? in->reader.acquire_stage == 1
                    : point < 8 ? in->reader_held && a->cursor == 0
                    && in->source_phase ==
                    (wl_columnar_lftj_source_phase_t)(point+1)
                    : point == 8 ? a->cursor == 1 &&
                    a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_RESOLVE
                        : a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_ACQUIRE;
            }
            if (!reached) composed_action(&f);
        }
        wl_columnar_lftj_attempt_t before = f.record.lftj;
        CHECK(before.input_count >= 1 && before.inputs[0].reader.owner);
        CHECK(before.inputs[0].reader_held == (point != 0));
        uint64_t prefix = 3u*(sizeof(wl_lftj_input_t)
            + sizeof(col_sorted_arrangement_probe_t) + sizeof(col_rel_t *)
            + sizeof(wl_columnar_source_access_reader_t)
            + sizeof(bool) + 3u*sizeof(uint32_t));
        uint64_t rows = 2u*2u*sizeof(int64_t), columns = 2u*sizeof(int64_t);
        uint64_t contribution = point < 5 ? 0 : point < 7 ? rows
            : point < 9 ? rows+columns : 3u*(rows+columns);
        CHECK(before.raw_preflight_scratch_bytes == prefix+contribution);
        col_relation_snapshot_t snapshot =
            wl_columnar_relation_snapshot(relations[0]);
        int64_t row[] = {9, 90};
        CHECK(col_rel_append_row(relations[0], row) == EBUSY);
        CHECK(composed_cleanup_lftj_turn(&f, 383,
            0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!memcmp(&before, &f.record.lftj, sizeof(before)));
        wl_evaluation_control_request_cancel(f.operation);
        for (unsigned turn = 0;
            f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
            ++turn) {
            CHECK(turn < 100);
            CHECK(!composed_cleanup_lftj_turn(&f, 386, 0));
        }
        CHECK(f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
            && wl_columnar_relation_snapshot_equal(snapshot,
            wl_columnar_relation_snapshot(relations[0])));
        composed_close(&f);
        CHECK(!col_rel_append_row(relations[0], row));
        for (unsigned i = 0; i < 3; ++i) col_rel_destroy(relations[i]);
    }
}

static void
test_anti_reentry(void)
{
    uint64_t baseline = 0;
    for (unsigned mode = 0; mode < 3; ++mode) {
        fixture_t f; fixture_init(&f, 2, 3, 0);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        eval_entry_t *entry = &stack->items[0];
        entry->is_delta = true;
        entry->seg_boundaries = malloc(2*sizeof(uint32_t));
        CHECK(entry->seg_boundaries);
        entry->seg_boundaries[0] = 0; entry->seg_boundaries[1] = 3;
        entry->seg_count = 2;
        col_rel_t *right = bare_source("right");
        col_rel_t *registry[] = {right}; f.session.rels = registry;
        f.session.nrels = 1;
        wl_plan_op_t op = {.op = WL_PLAN_OP_ANTIJOIN,
                           .right_relation = "right"};
        wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
        fixture_begin(&f, &plan, 0, 1);
        anti_until_decision(&f, &anti_reentry_context);
        anti_identity_input = f.input; anti_identity_owned = false;
        anti_identity_segments = entry->seg_boundaries;
        anti_identity_commits = 0;
        anti_reentry_fixture = &f;
        anti_reentry_checkpoints = anti_reentry_calls = 0;
        anti_reentry_late_cancel = mode == 2;
        wl_columnar_eval_stack_test_checkpoint =
            mode ? anti_reentry_tail : NULL;
        wl_columnar_eval_stack_test_anti_metadata_committed = mode
            ? anti_reentry_committed : anti_identity_cutoff;
        run(&f);
        wl_columnar_eval_stack_test_checkpoint = NULL;
        wl_columnar_eval_stack_test_anti_metadata_committed = NULL;
        CHECK(!f.record.semantic_result && f.record.completed == 1
            && anti_reentry_calls == (mode ? 2u : 0u) &&
            anti_identity_commits == 1
            && !anti_reentry_context->join_metadata_busy
            && entry->rel == f.input && !entry->seg_boundaries &&
            !entry->is_delta);
        CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0,
            0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        if (!mode) baseline = report.consumed;
        else CHECK(report.consumed == baseline);
        fixture_close(&f); col_rel_destroy(right);
    }
    anti_reentry_fixture = NULL; anti_reentry_context = NULL;
}

static void
test_anti_cutoff_refusals(void)
{
    for (unsigned negative = 0; negative < 7; ++negative) {
        fixture_t f; fixture_init(&f, 2, 3, 0);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        stack->items[0].is_delta = true;
        stack->items[0].seg_boundaries = malloc(2*sizeof(uint32_t));
        CHECK(stack->items[0].seg_boundaries);
        stack->items[0].seg_boundaries[0] = 0;
        stack->items[0].seg_boundaries[1] = 3;
        stack->items[0].seg_count = 2;
        eval_entry_t before = stack->items[0];
        col_rel_t *right = bare_source("right");
        col_rel_t *registry[] = {f.input, right}; f.session.rels = registry;
        f.session.nrels = 2;
        wl_plan_op_t op = {.op = WL_PLAN_OP_ANTIJOIN,
                           .right_relation = "right"};
        wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
        fixture_begin(&f, &plan, 0, 1);
        wl_columnar_filter_context_t *ctx = NULL; anti_until_decision(&f, &ctx);
        CHECK(ctx->join_disposition ==
            WL_COLUMNAR_JOIN_DISPOSITION_ANTI_KEEP_ALL);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
        bool committed = false;
        if (negative < 6) {
            ctx->busy = true;
            col_relation_snapshot_t saved = ctx->join_input_snapshot;
            const wl_plan_op_t *saved_op = ctx->join_operation;
            if (negative == 0) ++ctx->join_input_snapshot.view_generation;
            if (negative == 1) ctx->join_operation = NULL;
            if (negative == 2) ctx->source_initialized = true;
            if (negative ==
                3) ctx->join_disposition = WL_COLUMNAR_JOIN_DISPOSITION_REPLACE;
            if (negative == 4) ctx->busy = false;
            wl_columnar_filter_context_t copy = *ctx;
            int rc =
                wl_columnar_eval_stack_unary_commit_anti_keep_all_context_owned(
                f.frame, negative == 5 ? &copy : ctx, &committed, &f.record);
            CHECK(rc && !committed && !ctx->join_metadata_busy
                && stack->items[0].rel == before.rel
                && stack->items[0].seg_boundaries == before.seg_boundaries
                && stack->items[0].seg_count == before.seg_count &&
                stack->items[0].is_delta);
            ctx->busy = false; ctx->source_initialized = false;
            ctx->join_input_snapshot = saved; ctx->join_operation = saved_op;
            ctx->join_disposition = WL_COLUMNAR_JOIN_DISPOSITION_ANTI_KEEP_ALL;
        }
        wl_columnar_eval_stack_test_checkpoint = anti_cancel_cutoff;
        run(&f);
        wl_columnar_eval_stack_test_checkpoint = NULL;
        CHECK(!ctx->join_metadata_busy
            && f.record.semantic_result == WL_EVALUATION_CONTROL_CANCELLED
            && !f.record.completed && stack->items[0].rel == before.rel
            && stack->items[0].owned == before.owned && stack->items[0].is_delta
            && stack->items[0].seg_boundaries == before.seg_boundaries
            && stack->items[0].seg_boundaries[1] == 3 &&
            stack->items[0].seg_count == 2);
        fixture_close(&f); col_rel_destroy(right);
    }
}

/* Empirical host admission pair, separate from existing literal primitive
 * tariff oracles. Only the source-derived final256 region may be refused. */
static uint64_t
anti_host_budget_case(uint64_t limit, uint64_t measured)
{
    fixture_t f; fixture_init(&f, 2, 3, limit);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    stack->items[0].is_delta = true;
    stack->items[0].seg_boundaries = malloc(2*sizeof(uint32_t));
    CHECK(stack->items[0].seg_boundaries);
    stack->items[0].seg_boundaries[0] = 0;
    stack->items[0].seg_boundaries[1] = 3;
    stack->items[0].seg_count = 2;
    uint32_t *segments = stack->items[0].seg_boundaries;
    col_rel_t *right = bare_source("right");
    col_rel_t *registry[] = {right}; f.session.rels = registry;
    f.session.nrels = 1;
    wl_plan_op_t op = {.op = WL_PLAN_OP_ANTIJOIN, .right_relation = "right"};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    fixture_begin(&f, &plan, 0, 1);
    anti_identity_input = f.input; anti_identity_owned = false;
    anti_identity_segments = segments; anti_identity_commits = 0;
    wl_columnar_eval_stack_test_anti_metadata_committed = anti_identity_cutoff;
    run(&f);
    wl_columnar_eval_stack_test_anti_metadata_committed = NULL;
    bool refused = limit && limit < measured;
    CHECK(f.record.semantic_result ==
        (refused ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
    CHECK(anti_identity_commits == (refused ? 0u : 1u));
    CHECK(stack->items[0].rel == f.input);
    if (refused) CHECK(stack->items[0].seg_boundaries == segments
            && stack->items[0].is_delta && stack->items[0].seg_count == 2);
    CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
        f.record.semantic_result, 0)
        == f.record.semantic_result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report));
    if (limit) CHECK(report.consumed == (refused ? measured-256 : measured));
    fixture_close(&f); col_rel_destroy(right);
    return report.consumed;
}

static void
test_anti_host_budget(void)
{
    uint64_t measured = anti_host_budget_case(0, 0);
    CHECK(measured > 256);
    CHECK(anti_host_budget_case(measured-1, measured) == measured-256);
    CHECK(anti_host_budget_case(measured, measured) == measured);
}

static fixture_t *scalar_init_fixture;
static unsigned scalar_init_stop_api, scalar_init_seen[5],
    scalar_init_reentries;
static void scalar_init_frame_checkpoint(
    wl_columnar_relation_scalar_retirement_initializer_t *i,
    wl_evaluation_control_t *control, unsigned api)
{
    fixture_t *f = scalar_init_fixture;
    CHECK(f && i->busy && api >= 1 && api <= 4);
    ++scalar_init_seen[api];
    bool ready = true, done = true, committed = true; int semantic = 123;
    wl_columnar_eval_stack_cleanup_frame_t *frame = f->frame;
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned(frame,
        &f->record, &ready) == EBUSY && ready);
    CHECK(wl_columnar_eval_stack_unary_finish_scalar_funded_owned(frame, EIO,
        &f->record, control, id, &semantic,
        &done) == EBUSY && done && semantic == 123);
    CHECK(wl_columnar_eval_stack_unary_commit_attempt_owned(frame, &committed,
        &f->record) == EBUSY && !committed);
    col_rel_t **slot = (col_rel_t **)(uintptr_t)1;
    CHECK(wl_columnar_eval_stack_unary_output_owned(frame, &slot,
        &f->record) == EBUSY
        && slot == (col_rel_t **)(uintptr_t)1);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_owned(frame, f->operation,
        f->operation_id, &f->record) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_reset_owned(frame, &f->record) == EBUSY);
    CHECK(!wl_columnar_eval_stack_cleanup_stack(frame)
        && !wl_columnar_eval_stack_cleanup_result(frame));
    eval_stack_t *stack = (eval_stack_t *)(uintptr_t)1;
    CHECK(wl_columnar_eval_stack_operator_stack_owned(frame, &f->session,
        &stack,
        &f->record) == EBUSY && stack == (eval_stack_t *)(uintptr_t)1);
    CHECK(!wl_columnar_eval_stack_numeric_owner(frame, &f->record));
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY &&
        f->frame == frame);
    ++scalar_init_reentries;
    if (control == f->operation && scalar_init_stop_api == api) {
        scalar_init_stop_api = 0; wl_evaluation_control_request_cancel(control);
    }
}

static void test_scalar_initialization_frame_stops(void)
{
    for (unsigned stop = 1; stop <= 5; ++stop) {
        fixture_t f; fixture_init(&f, 3, 3, 0);
        wl_columnar_memory_governor_ref_t *g = join_governor(&f, 1u << 24);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        stack->items[0].owned = true;
        col_rel_t *original = f.input;
        col_relation_snapshot_t snapshot =
            wl_columnar_relation_snapshot(original);
        uint32_t projection = 0;
        wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1,
                           .project_indices = &projection};
        wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
        fixture_begin(&f, &plan, 0, 1);
        CHECK(!wl_columnar_eval_stack_unary_begin_attempt_owned(f.frame,
            f.operation, f.operation_id, &f.record));
        col_rel_t **slot = NULL;
        CHECK(!wl_columnar_eval_stack_unary_output_owned(f.frame, &slot,
            &f.record));
        CHECK(!wl_columnar_relation_new_auto_governed_attempt_controlled(
                "private abort", 3,
                4, g, slot, f.operation, f.operation_id));
        CHECK(*slot &&
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        scalar_init_fixture = &f; scalar_init_stop_api = stop <= 4 ? stop : 0;
        scalar_init_reentries = 0;
        memset(scalar_init_seen, 0, sizeof(scalar_init_seen));
        wl_columnar_relation_scalar_retirement_init_test_checkpoint =
            scalar_init_frame_checkpoint;
        int rc = 0; bool ready = false;
        for (unsigned turn = 0; !ready && !rc; ++turn) {
            CHECK(turn < 1000);
            rc =
                wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned(
                f.frame, &f.record, &ready);
        }
        if (stop == 5) {
            CHECK(!rc && ready);
            wl_evaluation_control_request_cancel(f.operation);
            CHECK(wl_evaluation_control_charge_attempt(f.operation,
                f.operation_id, 0) == WL_EVALUATION_CONTROL_CANCELLED);
        } else CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && !ready &&
                !scalar_init_stop_api);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            WL_EVALUATION_CONTROL_CANCELLED,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_evaluation_control_report_t before, after;
        CHECK(!wl_evaluation_control_report(f.operation, &before));
        bool done = false; int semantic = 123; unsigned refused = 0;
        for (unsigned turn = 0; !done; ++turn) {
            CHECK(turn < 10000); int fund_owner = 0;
            /* Empirical whole-parent probes; primitive fee literals are tested
             * independently in evaluation_physical_retirement. */
            uint64_t limit = turn % 2 ? 384 : 1;
            wl_evaluation_control_t *fund = control_new(limit, &fund_owner);
            uint64_t id = wl_evaluation_control_active_attempt_id(fund);
            rc =
                wl_columnar_eval_stack_unary_finish_scalar_funded_owned(f.frame,
                    WL_EVALUATION_CONTROL_CANCELLED, &f.record, fund, id,
                    &semantic, &done);
            CHECK(!rc ||
                (limit == 1 && rc == WL_EVALUATION_CONTROL_WORK_LIMIT));
            if (rc) ++refused;
            CHECK(semantic == (turn ? WL_EVALUATION_CONTROL_CANCELLED : 123));
            CHECK(wl_evaluation_control_finish(fund, &fund_owner, rc, 0) == rc);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(fund,
                &report) && report.consumed <= limit);
            CHECK(!wl_evaluation_control_detach(fund, &fund_owner));
            wl_evaluation_control_release(fund);
            CHECK(!wl_evaluation_control_report(f.operation, &after)
                && before.attempt_id == after.attempt_id &&
                before.consumed == after.consumed
                && before.result == after.result);
            CHECK(wl_columnar_relation_snapshot_equal(snapshot,
                wl_columnar_relation_snapshot(original))
                && original->columns[2][2] == 4);
        }
        CHECK(refused && scalar_init_reentries && scalar_init_seen[1] &&
            scalar_init_seen[2]
            && scalar_init_seen[3] && scalar_init_seen[4]);
        wl_columnar_relation_scalar_retirement_init_test_checkpoint = NULL;
        scalar_init_fixture = NULL;
        CHECK(stack->items[0].rel == original && stack->items[0].owned);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        CHECK(!wl_columnar_eval_stack_unary_reset_owned(f.frame, &f.record));
        f.input = NULL;
        run(&f); CHECK(f.record.semantic_result == EINVAL); fixture_close(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        wl_columnar_memory_governor_ref_release(g);
    }
}

static int scalar_lftj_turn(fixture_t *f, bool direct, uint64_t limit,
    uint64_t *work)
{
    int owner = 0;
    wl_evaluation_control_t *fund = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(fund);
    int rc =
        direct ? wl_columnar_eval_plan_numeric_composed_cleanup_lftj(&f->record,
            fund, id)
        : wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record, fund,
            id);
    CHECK(wl_evaluation_control_finish(fund, &owner, rc, 0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(fund, &report));
    *work = report.consumed;
    CHECK(!wl_evaluation_control_detach(fund, &owner));
    wl_evaluation_control_release(fund);
    return rc;
}

static fixture_t *scalar_lftj_fixture;
static unsigned scalar_lftj_reentry_count;
static void scalar_lftj_checkpoint(
    wl_columnar_relation_scalar_retirement_initializer_t *i,
    wl_evaluation_control_t *fund, unsigned api)
{
    fixture_t *f = scalar_lftj_fixture;
    CHECK(f && i == &f->record.lftj.retirement_initializer);
    (void)api;
    wl_columnar_lftj_attempt_t *a = &f->record.lftj;
    bool done = true;
    uint64_t id = wl_evaluation_control_active_attempt_id(fund);
    CHECK(wl_columnar_ops_lftj_attempt_abort(a, &done, fund,
        id) == EBUSY && done);
    CHECK(wl_columnar_ops_lftj_attempt_step(a, &done, fund,
        id) == EBUSY && done);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(&f->record, fund,
        id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_cleanup_lftj(&f->record, fund,
        id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_consume_result(&f->record, 0,
        fund, id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_abort_request(&f->record, fund,
        id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_ack(&f->record, fund,
        id) == EBUSY);
    ++scalar_lftj_reentry_count;
}

static void test_lftj_scalar_cleanup_grants(void)
{
    for (unsigned direct = 0; direct < 2; ++direct) {
        fixture_t f; wl_plan_relation_t body; wl_plan_op_t op;
        wl_plan_op_lftj_t metadata; char *names[] = {"a", "b", "c"};
        uint32_t keys[] = {0, 0, 0}; col_rel_t *relations[3];
        const double values[] = {1, 10, 2, 20};
        for (unsigned n = 0; n < 3;
            ++n) relations[n] = composed_source(names[n], 2, values, 2, false);
        metadata = (wl_plan_op_lftj_t){.k = 3, .rel_names = names,
                                       .key_cols = keys};
        op = (wl_plan_op_t){.op = WL_PLAN_OP_LFTJ, .opaque_data = &metadata};
        body = (wl_plan_relation_t){.ops = &op, .op_count = 1};
        composed_fixture(&f, source_new(2, 0), 0);
        wl_columnar_memory_governor_ref_t *g = composed_governor(&f);
        uint64_t base_reserved =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
        wl_mem_ledger_alloc(&f.session.mem_ledger, WL_MEM_SUBSYS_TEMPORARY, 37);
        composed_begin(&f, &body, relations, 3, 0);
        for (unsigned turn = 0; !f.record.lftj.output_owned; ++turn) {
            CHECK(turn < 100000); composed_action(&f);
        }
        wl_columnar_lftj_attempt_t *a = &f.record.lftj;
        CHECK(a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_BEGIN && a->output
            && !a->writer_active && !a->output->mem_ledger);
        col_rel_t *target = a->output;
        col_relation_snapshot_t source[3];
        for (unsigned n = 0; n < 3;
            ++n) source[n] = wl_columnar_relation_snapshot(relations[n]);
        CHECK(!composed_abort_request(&f, 384));
        uint64_t work = 0;
        /* Direct request genuinely authenticates the terminal stop once. */
        CHECK(!scalar_lftj_turn(&f, true, 384, &work) && work == 256
            && a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE &&
            f.record.lftj_cleanup_authorized);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            WL_EVALUATION_CONTROL_CANCELLED,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_evaluation_control_report_t before, after;
        CHECK(!wl_evaluation_control_report(f.operation, &before));
        memset(&a->retirement, 0xa5, sizeof(a->retirement)); /* Genuinely inert; no scalar claim. */
        scalar_lftj_fixture = &f; scalar_lftj_reentry_count = 0;
        wl_columnar_relation_scalar_retirement_init_test_checkpoint =
            scalar_lftj_checkpoint;
        CHECK(!scalar_lftj_turn(&f, direct != 0, 384, &work) && work == 256
            && a->retirement_initializer.phase ==
            WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO);
        unsigned char historic[sizeof(a->retirement)];
        memcpy(historic, &a->retirement, sizeof(historic));
        CHECK(scalar_lftj_turn(&f, direct != 0, 383,
            &work) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && work == 128 && !memcmp(historic, &a->retirement,
            sizeof(historic))
            && a->retirement_initializer.phase ==
            WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO);
        /* General child stepping returns an exact pending receipt on refusal. */
        if (!direct) {
            CHECK(f.record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING);
            CHECK(!wl_columnar_eval_plan_numeric_composed_consume_result(
                    &f.record,
                    WL_EVALUATION_CONTROL_WORK_LIMIT, f.funding, f.funding_id));
        }
        CHECK(!scalar_lftj_turn(&f, direct != 0, 384, &work) && work == 384
            && a->retirement_initializer.phase ==
            WL_COLUMNAR_RELATION_SCALAR_INIT_BIND);
        CHECK(!scalar_lftj_turn(&f, direct != 0, 384, &work) && work == 320
            && a->retirement_started &&
            a->retirement.storage.relation == target);
        CHECK(scalar_lftj_turn(&f, direct != 0, 255,
            &work) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && work == 128 && a->retirement_started &&
            a->retirement_initializer.identity);
        if (!direct) CHECK(
                !wl_columnar_eval_plan_numeric_composed_consume_result(
                    &f.record,
                    WL_EVALUATION_CONTROL_WORK_LIMIT, f.funding, f.funding_id));
        CHECK(!scalar_lftj_turn(&f, direct != 0, 384, &work) && work == 256
            && !a->retirement_initializer.identity && a->retirement_started);
        bool saw_commit = false, saw_drained = false;
        for (unsigned turn = 0;
            f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE;
            ++turn) {
            CHECK(turn < 10000);
            bool scalar = a->output_owned || a->retirement_started ||
                a->retirement_drained;
            CHECK(!scalar_lftj_turn(&f, direct != 0, scalar ? 384 : 386,
                &work));
            CHECK(work <= (scalar ? 384u : 386u));
            if (a->retirement_committed) {
                saw_commit = true; CHECK(!a->output && !a->output_owned);
            }
            if (a->retirement_drained) saw_drained = true;
            CHECK(!wl_evaluation_control_report(f.operation, &after)
                && before.consumed == after.consumed &&
                before.attempt_id == after.attempt_id
                && before.result == after.result);
        }
        CHECK(saw_commit && saw_drained && scalar_lftj_reentry_count >= 4
            && !a->retirement_started && !a->output);
        wl_columnar_relation_scalar_retirement_init_test_checkpoint = NULL;
        scalar_lftj_fixture = NULL;
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)) == base_reserved);
        wl_mem_ledger_snapshot_t ledger;
        wl_mem_ledger_snapshot(&f.session.mem_ledger, &ledger);
        CHECK(ledger.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY] == 37);
        for (unsigned n = 0; n < 3;
            ++n) CHECK(wl_columnar_relation_snapshot_equal(source[n],
                wl_columnar_relation_snapshot(relations[n])));
        composed_close(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        wl_columnar_memory_governor_ref_release(g);
        for (unsigned n = 0; n < 3; ++n) col_rel_destroy(relations[n]);
    }
}

static void test_lftj_success_retains_funding_class(void)
{
    fixture_t f;
    char *names[] = {"a", "b", "c"};
    uint32_t keys[] = {0, 0, 0};
    const double values[] = {1, 10, 2, 20};
    col_rel_t *relations[3];
    for (unsigned n = 0; n < 3; ++n)
        relations[n] = composed_source(names[n], 2, values, 2, false);
    wl_plan_op_lftj_t metadata = {.k = 3, .rel_names = names, .key_cols = keys};
    wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ, .opaque_data = &metadata};
    wl_plan_relation_t body = {.ops = &op, .op_count = 1};
    composed_fixture(&f, source_new(2, 0), 0);
    composed_begin(&f, &body, relations, 3, 0);
    for (unsigned turn = 0;
        f.record.lftj.phase != WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE
        || f.record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD;
        ++turn) {
        CHECK(turn < 100000); composed_action(&f);
    }
    CHECK(f.record.lftj.output_owned && f.record.lftj.output
        && !f.record.lftj.semantic_result && !f.record.semantic_result);
    col_rel_t *output = f.record.lftj.output;
    uint64_t work = 0;
    CHECK(scalar_lftj_turn(&f, true, 383,
        &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work);
    CHECK(scalar_lftj_turn(&f, true, 384, &work) == EBUSY && work == 384);
    CHECK(!f.record.lftj_cleanup_authorized && !f.record.lftj.semantic_result
        && !f.record.semantic_result && f.record.lftj.output == output
        && f.record.lftj.output_owned &&
        !f.record.lftj.retirement_initializer.identity);
    composed_run(&f);
    CHECK(!f.record.semantic_result && f.record.lftj.registered);
    composed_close(&f);
    for (unsigned n = 0; n < 3; ++n) col_rel_destroy(relations[n]);
}

static bool metadata_reports_equal(const wl_evaluation_control_report_t *,
    const wl_evaluation_control_report_t *);
typedef struct {
    wl_columnar_eval_plan_numeric_attempt_t *record;
    int rc; bool done;
} metadata_target_peer_t;

static void *
metadata_target_peer(void *data)
{
    metadata_target_peer_t *p = data;
    p->rc = wl_columnar_lftj_attempt_begin_prevalidated(&p->record->lftj,
            p->record, &p->done);
    return NULL;
}

typedef struct {
    fixture_t *fixture;
    unsigned hits[WL_COLUMNAR_LFTJ_METADATA_CONSUMED + 1];
    uint32_t next_name, entries, pairs;
    bool native_checked;
    uint64_t consumed[WL_COLUMNAR_LFTJ_METADATA_CONSUMED + 1][64];
} metadata_trace_t;
static metadata_trace_t *metadata_trace;

static void
metadata_trace_checkpoint(wl_columnar_eval_plan_numeric_attempt_t *record)
{
    metadata_trace_t *t = metadata_trace;
    CHECK(t && record == &t->fixture->record && record->composed_busy &&
        !record->lftj_metadata_dispatch);
    wl_columnar_lftj_metadata_proof_t *p = &record->lftj_metadata;
    CHECK(p->phase > WL_COLUMNAR_LFTJ_METADATA_INERT
        && p->phase < WL_COLUMNAR_LFTJ_METADATA_CONSUMED);
    unsigned hit = t->hits[p->phase]++;
    CHECK(hit < 64);
    CHECK(!wl_evaluation_control_test_active_consumed(record->control,
        record->attempt_id, &t->consumed[p->phase][hit]));
    CHECK(!record->lftj.identity && !record->lftj.inputs &&
        !record->lftj.output);
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_NAMES) {
        CHECK(p->name_index == t->next_name++ && !record->preflight_depth);
    }
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY) {
        CHECK(p->entry == t->entries++);
    }
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR) {
        CHECK(p->pair < p->entry); ++t->pairs;
    }
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_INSTALL) CHECK(p->prefix_cleared);
    if (p->phase == WL_COLUMNAR_LFTJ_METADATA_PREFIX ||
        p->phase == WL_COLUMNAR_LFTJ_METADATA_INSTALL){
        unsigned stage = p->phase; bool installed = true;
        bool cleared = p->prefix_cleared;
        unsigned char historic[offsetof(wl_columnar_lftj_attempt_t,
            retirement)];
        memcpy(historic, &record->lftj, sizeof(historic));
        uint64_t before_work = t->consumed[stage][hit], after_work;
        wl_evaluation_control_report_t before_report = {0}, after_report = {0};
        int report_status = wl_evaluation_control_report(record->control,
                &before_report);
        int rc = wl_columnar_lftj_attempt_begin_prevalidated(&record->lftj,
                record, &installed);
        CHECK(!wl_evaluation_control_test_active_consumed(record->control,
            record->attempt_id, &after_work));
        if (rc != EBUSY) fprintf(stderr,
                "actual metadata target reentry: phase=%u rc=%d added=%llu\n",
                stage, rc, (unsigned long long)(after_work - before_work));
        CHECK(wl_evaluation_control_report(record->control,
            &after_report) == report_status);
        if (!report_status) CHECK(metadata_reports_equal(&before_report,
                &after_report));
        if (!t->native_checked) {
            metadata_target_peer_t peer = {.record = record, .done = true};
            wl_thread_t thread;
            CHECK(!wl_thread_create(&thread, metadata_target_peer,
                &peer) && !wl_thread_join(&thread));
            CHECK(peer.rc == EBUSY && peer.done); t->native_checked = true;
        }
        CHECK(rc == EBUSY && installed && stage == p->phase &&
            cleared == p->prefix_cleared
            && !record->lftj_metadata_dispatch && !memcmp(historic,
            &record->lftj, sizeof(historic))
            && after_work == before_work
            && !record->lftj.identity && !record->lftj.inputs &&
            !record->lftj.output);
    }
    /* Use genuine parent/address authority. Copies or another target never
     * obtain the cheap handoff, including while the real owner is busy. */
    wl_columnar_eval_plan_numeric_attempt_t copy = *record;
    copy.identity = &copy; copy.lftj_metadata_dispatch = true; /* Negative forgery only. */
    wl_columnar_lftj_begin_context_t context;
    CHECK(wl_columnar_eval_plan_numeric_lftj_begin_context_get(&copy,
        &copy.lftj, &context) != 0);
    wl_columnar_lftj_attempt_t other = {0}; bool done = false;
    CHECK(wl_columnar_lftj_attempt_begin_prevalidated(&other, record,
        &done) == EINVAL
        && !other.identity && !other.inputs && !other.output);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(record,
        t->fixture->funding, t->fixture->funding_id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_ack(record,
        t->fixture->funding, t->fixture->funding_id) == EBUSY);
}

static void
test_lftj_metadata_maximum(void)
{
    for (unsigned kind = 0; kind < 12; ++kind) {
        fixture_t f;
        char *names[64]; uint32_t keys[64] = {0};
        for (unsigned i = 0; i < 64; ++i) names[i] = "a";
        if (kind == 2 || kind == 4) names[63] = NULL;
        if (kind == 3) names[0] = NULL;
        if (kind == 4 || kind == 5) names[0] = "missing";
        if (kind == 6) names[0] = "";
        wl_plan_op_lftj_t metadata = {.k = kind == 7 ? 1 : kind == 8 ? 65 : 64,
                                      .rel_names = kind == 9 ? NULL : names,
                                      .key_cols = kind == 10 ? NULL : keys};
        wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ,
                           .opaque_data = kind == 11 ? NULL : &metadata};
        wl_plan_relation_t body = {.ops = &op, .op_count = 1};
        col_rel_t *source = source_new(1, 1);
        CHECK(!wl_columnar_relation_rename_checked(source, "a"));
        col_rel_t *relations[] = {source};
        composed_fixture(&f, source_new(1, 0), 0);
        f.session.base.evaluation_control = f.operation;
        f.session.rels = relations; f.session.nrels = f.session.rel_cap = 1;
        wl_columnar_eval_private_bindings_t bindings = {0};
        if (kind == 1) {
            const char *binding_names[] = {"a", "b", "c", "d"};
            col_rel_t *binding_relations[] = {source, source, source, source};
            CHECK(!wl_columnar_eval_private_bindings_begin(&bindings,
                &f.session,
                &body, f.operation, f.operation_id, binding_names,
                binding_relations, 4));
        }
        CHECK(!(kind ==
            1 ? wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
                &f.record, &body, 0, 1, f.frame, &f.session, 0, f.operation,
                f.operation_id,
                f.funding, f.funding_id, &bindings)
            : wl_columnar_eval_plan_numeric_composed_attempt_begin(&f.record,
            &body, 0, 1,
            f.frame, &f.session, 0, f.operation, f.operation_id, f.funding,
            f.funding_id)));
        metadata_trace_t trace = {.fixture = &f};
        metadata_trace = &trace;
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint =
            metadata_trace_checkpoint;
        composed_run(&f);
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
        metadata_trace = NULL;
        int expected = kind < 2 ? 0 : kind == 5 || kind == 6 ? ENOENT : EINVAL;
        CHECK(f.record.semantic_result == expected);
        CHECK(f.record.lftj_metadata.phase == WL_COLUMNAR_LFTJ_METADATA_INERT &&
            !f.record.lftj_metadata_dispatch);
        CHECK(trace.hits[WL_COLUMNAR_LFTJ_METADATA_HEADER] == 1);
        if (!expected) {
            CHECK(trace.next_name == 64
                && trace.hits[WL_COLUMNAR_LFTJ_METADATA_VALIDATED] == 1
                && trace.hits[WL_COLUMNAR_LFTJ_METADATA_PREFIX] == 1
                && trace.hits[WL_COLUMNAR_LFTJ_METADATA_INSTALL] == 1);
            CHECK(trace.entries == (kind ? 4u : 0u) &&
                trace.pairs == (kind ? 6u : 0u));
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            CHECK(stack->top == 1 && stack->items[0].owned
                && stack->items[0].rel->ncols == 64 &&
                stack->items[0].rel->nrows == 1);
            for (unsigned c = 0; c < 64;
                ++c) CHECK(stack->items[0].rel->columns[c][0] == 0);
            composed_raw_parity(&f, &body, 0);
        } else if (kind == 2 || kind == 4) {
            CHECK(trace.next_name == 64 &&
                !trace.hits[WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER]
                && !f.record.lftj.identity);
        } else if (kind == 3) CHECK(trace.next_name == 1 &&
                !f.record.lftj.identity);
        CHECK(source->nrows == 1 && source->columns[0][0] == 0
            && !f.session.arr_count && !f.session.rel_hash_nbuckets);
        composed_close(&f); col_rel_destroy(source);
    }
}

typedef struct {
    fixture_t fixture;
    char *names[64]; uint32_t keys[64];
    wl_plan_op_lftj_t metadata;
    wl_plan_op_t op;
    wl_plan_relation_t body;
    col_rel_t *relations[1];
    wl_columnar_eval_private_bindings_t bindings;
    metadata_trace_t trace;
    wl_columnar_memory_governor_ref_t *governor;
} metadata_fixture_t;

static void
metadata_fixture_begin_options(metadata_fixture_t *m, uint64_t limit,
    bool governed)
{
    memset(m, 0, sizeof(*m));
    for (unsigned i = 0; i < 64; ++i) m->names[i] = "a";
    m->metadata = (wl_plan_op_lftj_t){.k = 64, .rel_names = m->names,
                                      .key_cols = m->keys};
    m->op = (wl_plan_op_t){.op = WL_PLAN_OP_LFTJ, .opaque_data = &m->metadata};
    m->body = (wl_plan_relation_t){.ops = &m->op, .op_count = 1};
    m->relations[0] = source_new(1, 1);
    CHECK(!wl_columnar_relation_rename_checked(m->relations[0], "a"));
    fixture_t *f = &m->fixture;
    composed_fixture(f, source_new(1, 0), limit);
    if (governed) m->governor = composed_governor(f);
    f->session.base.evaluation_control = f->operation;
    f->session.rels = m->relations; f->session.nrels = f->session.rel_cap = 1;
    const char *binding_names[] = {"a", "b", "c", "d"};
    col_rel_t *binding_relations[] = {m->relations[0], m->relations[0],
                                      m->relations[0], m->relations[0]};
    CHECK(!wl_columnar_eval_private_bindings_begin(&m->bindings, &f->session,
        &m->body, f->operation, f->operation_id, binding_names,
        binding_relations, 4));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
            &f->record,
            &m->body, 0, 1, f->frame, &f->session, 0, f->operation,
            f->operation_id,
            f->funding, f->funding_id, &m->bindings));
    m->trace.fixture = f;
    metadata_trace = &m->trace;
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint =
        metadata_trace_checkpoint;
}

static void
metadata_fixture_begin(metadata_fixture_t *m, uint64_t limit)
{
    metadata_fixture_begin_options(m, limit, false);
}

static void
metadata_fixture_close(metadata_fixture_t *m)
{
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_trace = NULL;
    composed_close(&m->fixture);
    col_rel_destroy(m->relations[0]);
    if (m->governor) {
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                m->governor))
            && wl_columnar_memory_governor_ref_is_sole(m->governor));
        wl_columnar_memory_governor_ref_release(m->governor);
    }
}

static void
test_lftj_metadata_original_boundaries(void)
{
    const struct { wl_columnar_lftj_metadata_phase_t phase;
                   unsigned index, work; } targets[] = {
        {WL_COLUMNAR_LFTJ_METADATA_HEADER, 0, 128},
        {WL_COLUMNAR_LFTJ_METADATA_NAMES, 0, 128},
        {WL_COLUMNAR_LFTJ_METADATA_NAMES, 63, 128},
        {WL_COLUMNAR_LFTJ_METADATA_VALIDATED, 0, 128},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER, 0, 192},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY, 0, 192},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY, 3, 192},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR, 0, 128},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR, 5, 128},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL, 0, 192},
        {WL_COLUMNAR_LFTJ_METADATA_PREFIX, 0, 256},
        {WL_COLUMNAR_LFTJ_METADATA_INSTALL, 0, 192}
    };
    metadata_fixture_t baseline;
    metadata_fixture_begin(&baseline, 0);
    composed_run(&baseline.fixture);
    CHECK(!baseline.fixture.record.semantic_result);
    uint64_t before[sizeof(targets)/sizeof(targets[0])];
    for (unsigned i = 0; i < sizeof(targets)/sizeof(targets[0]); ++i) {
        CHECK(baseline.trace.hits[targets[i].phase] > targets[i].index);
        before[i] = baseline.trace.consumed[targets[i].phase][targets[i].index];
    }
    metadata_fixture_close(&baseline);
    for (unsigned i = 0; i < sizeof(targets)/sizeof(targets[0]); ++i)
        for (unsigned exact = 0; exact < 2; ++exact) {
            metadata_fixture_t m;
            metadata_fixture_begin(&m,
                before[i] + targets[i].work - (exact ? 0 : 1));
            composed_run(&m.fixture);
            CHECK(m.fixture.record.semantic_result ==
                WL_EVALUATION_CONTROL_WORK_LIMIT
                && m.trace.hits[targets[i].phase] > targets[i].index);
            uint64_t consumed;
            CHECK(!wl_evaluation_control_test_active_consumed(
                    m.fixture.operation,
                    m.fixture.operation_id, &consumed)
                && consumed == before[i] + (exact ? targets[i].work : 0));
            CHECK(m.fixture.record.lftj_metadata.phase ==
                WL_COLUMNAR_LFTJ_METADATA_INERT
                && m.relations[0]->nrows == 1 &&
                m.relations[0]->columns[0][0] == 0);
            if (targets[i].phase == WL_COLUMNAR_LFTJ_METADATA_PREFIX ||
                !exact){
                CHECK(!m.fixture.record.lftj.identity &&
                    !m.fixture.record.lftj.inputs
                    && !m.fixture.record.lftj.output);
            }
            metadata_fixture_close(&m);
        }
}

static metadata_fixture_t *metadata_ended_probe;
static unsigned metadata_ended_reentries;

static bool
metadata_reports_equal(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b)
{
    return a->attempt_id == b->attempt_id && a->limit == b->limit
           && a->consumed == b->consumed && a->result == b->result
           && a->stop_reason == b->stop_reason &&
           a->execution_status == b->execution_status
           && a->cleanup_status == b->cleanup_status;
}

static void
metadata_ended_checkpoint(wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding)
{
    CHECK(metadata_ended_probe && r == &metadata_ended_probe->fixture.record &&
        r->composed_busy);
    fixture_t *f = &metadata_ended_probe->fixture;
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(r->control, &before));
    uint64_t consumed, after_work,
        id = wl_evaluation_control_active_attempt_id(funding);
    CHECK(!wl_evaluation_control_test_active_consumed(funding, id, &consumed));
    wl_columnar_lftj_metadata_proof_t saved = r->lftj_metadata;
    int output = 12345;
    CHECK(wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(r, funding,
        id, &output) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_lftj_metadata_ack_ended(r, funding, id,
        &output) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(r, funding,
        id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_attempt_ack(r, funding,
        id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_composed_abort_request(r, funding,
        id) == EBUSY);
    CHECK(output == 12345 && !memcmp(&saved, &r->lftj_metadata, sizeof(saved)));
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY);
    CHECK(!wl_evaluation_control_report(r->control,
        &after) && metadata_reports_equal(&before, &after));
    CHECK(!wl_evaluation_control_test_active_consumed(funding, id,
        &after_work) && consumed == after_work);
    ++metadata_ended_reentries;
}

static int
metadata_ended_turn(metadata_fixture_t *m, bool ack, uint64_t limit,
    uint64_t adjustment,
    uint64_t *work, int *result)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    int rc = ack ? wl_columnar_eval_plan_numeric_lftj_metadata_ack_ended(
        &m->fixture.record, funding, id + adjustment, result)
        : wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(
        &m->fixture.record, funding, id + adjustment, result);
    CHECK(wl_evaluation_control_finish(funding, &owner, rc, 0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    *work = report.consumed;
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

typedef struct {
    fixture_t *fixture;
    int rc, result;
} metadata_ended_peer_t;

static void *
metadata_ended_peer(void *data)
{
    metadata_ended_peer_t *p = data;
    p->rc =
        wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(
        &p->fixture->record,
        p->fixture->funding, p->fixture->funding_id, &p->result);
    return NULL;
}

static void
test_lftj_metadata_ended_cleanup(void)
{
    const struct { wl_columnar_lftj_metadata_phase_t phase;
                   uint32_t entry, pair; } targets[] = {
        {WL_COLUMNAR_LFTJ_METADATA_HEADER, 0, 0},
        {WL_COLUMNAR_LFTJ_METADATA_NAMES, 0, 0},
        {WL_COLUMNAR_LFTJ_METADATA_NAMES, 63, 0},
        {WL_COLUMNAR_LFTJ_METADATA_VALIDATED, 0, 0},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_HEADER, 0, 0},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY, 0, 0},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY, 3, 0},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR, 3, 2},
        {WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL, 0, 0},
        {WL_COLUMNAR_LFTJ_METADATA_PREFIX, 0, 0},
        {WL_COLUMNAR_LFTJ_METADATA_INSTALL, 0, 0}
    };
    for (unsigned trial = 0; trial < sizeof(targets)/sizeof(targets[0]);
        ++trial)
        for (unsigned work_stop = 0; work_stop < 2; ++work_stop) {
            metadata_fixture_t m;
            metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
            fixture_t *f = &m.fixture;
            for (unsigned turn = 0;; ++turn) {
                CHECK(turn < 10000);
                const wl_columnar_lftj_metadata_proof_t *p =
                    &f->record.lftj_metadata;
                if (p->phase == targets[trial].phase
                    && (p->phase != WL_COLUMNAR_LFTJ_METADATA_NAMES ||
                    p->name_index == targets[trial].entry)
                    && (p->phase != WL_COLUMNAR_LFTJ_METADATA_BIND_ENTRY ||
                    p->entry == targets[trial].entry)
                    && (p->phase != WL_COLUMNAR_LFTJ_METADATA_BIND_PAIR
                    || (p->entry == targets[trial].entry &&
                    p->pair == targets[trial].pair))) break;
                composed_action(f);
            }
            wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
            metadata_trace = NULL;
            int output = 12345; uint64_t charge;
            /* Active original is not a genuinely completed-stop capability. */
            CHECK(metadata_ended_turn(&m, false, 0, 0, &charge,
                &output) == EINVAL
                && !charge && output == 12345);
            if (!work_stop) wl_evaluation_control_request_cancel(f->operation);
            int stop = work_stop ? wl_evaluation_control_charge(f->operation,
                    UINT64_MAX)
                : wl_evaluation_control_charge(f->operation, 0);
            CHECK(stop ==
                (work_stop ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(wl_evaluation_control_finish(f->operation,
                &f->operation_owner, 0, 0) == stop);
            CHECK(!wl_evaluation_control_reset(f->operation) &&
                !wl_evaluation_control_configure(f->operation, 0));
            wl_evaluation_control_report_t before, after;
            CHECK(!wl_evaluation_control_report(f->operation, &before));
            CHECK(wl_columnar_eval_plan_numeric_composed_attempt_step(
                    &f->record,
                    f->funding, f->funding_id) == EINVAL);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY);
            wl_columnar_eval_plan_numeric_attempt_t copy = f->record;
            CHECK(wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(
                    &copy,
                    f->funding, f->funding_id,
                    &output) == EBUSY && output == 12345);
            CHECK(wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(
                    &f->record,
                    f->funding, f->funding_id,
                    &f->record.semantic_result) == EINVAL);
            CHECK(wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(
                    &f->record,
                    f->operation, f->operation_id, &output) == EINVAL);
            metadata_ended_peer_t peer = {.fixture = f, .result = 12345};
            wl_thread_t thread;
            CHECK(!wl_thread_create(&thread, metadata_ended_peer,
                &peer) && !wl_thread_join(&thread));
            CHECK(peer.rc == EINVAL && peer.result == 12345);
            CHECK(metadata_ended_turn(&m, false, 0, 1, &charge,
                &output) == EINVAL
                && !charge && output == 12345);
            metadata_ended_probe = &m; metadata_ended_reentries = 0;
            wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint =
                metadata_ended_checkpoint;
            CHECK(metadata_ended_turn(&m, false, 191, 0, &charge, &output)
                == WL_EVALUATION_CONTROL_WORK_LIMIT && !charge &&
                output == 12345
                && !f->record.lftj_metadata_ended && !f->record.lftj.identity);
            CHECK(metadata_ended_turn(&m, false, 192, 0, &charge, &output) == 0
                && charge == 192 && output == stop &&
                f->record.lftj_metadata_ended
                && f->record.phase ==
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE);
            CHECK(metadata_ended_reentries == 2);
            wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint = NULL;
            metadata_ended_probe = NULL;
            CHECK(wl_columnar_eval_plan_numeric_composed_attempt_ack(&f->record,
                f->funding, f->funding_id) == EBUSY);
            output = 12345;
            CHECK(metadata_ended_turn(&m, true, 0, 1, &charge,
                &output) == EINVAL && !charge && output == 12345);
            CHECK(metadata_ended_turn(&m, true, 383, 0, &charge, &output)
                == WL_EVALUATION_CONTROL_WORK_LIMIT && !charge &&
                output == 12345
                && f->record.identity == &f->record &&
                f->record.lftj_metadata_ended);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&f->frame) == EBUSY);
            CHECK(metadata_ended_turn(&m, true, 384, 0, &charge, &output) == 0
                && charge == 384 && output == stop && !f->record.identity
                && !f->record.control && !f->record.frame &&
                !f->record.private_bindings);
            CHECK(!wl_evaluation_control_report(f->operation,
                &after) && metadata_reports_equal(&before, &after));
            CHECK(m.relations[0]->nrows == 1 &&
                m.relations[0]->columns[0][0] == 0);
            metadata_fixture_close(&m);
        }
}

static void
test_lftj_metadata_ended_last_reference(void)
{
    metadata_fixture_t m;
    metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
    fixture_t *f = &m.fixture;
    for (unsigned n = 0;
        f->record.lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_INSTALL;
        ++n){
        CHECK(n < 10000); composed_action(f);
    }
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_trace = NULL;
    wl_evaluation_control_request_cancel(f->operation);
    CHECK(wl_evaluation_control_charge(f->operation,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner, 0,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_evaluation_control_detach(f->operation, &f->operation_owner));
    wl_evaluation_control_release(f->operation); /* Only the real parent ownref remains. */
    int result = 12345; uint64_t work;
    CHECK(!metadata_ended_turn(&m, false, 192, 0, &work, &result)
        && work == 192 && result == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!metadata_ended_turn(&m, true, 384, 0, &work, &result)
        && work == 384 && result == WL_EVALUATION_CONTROL_CANCELLED);
    f->operation = NULL; /* ACK dropped its last actual ref; never query it. */
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0)
        && !wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->funding); col_rel_destroy(f->input);
    col_rel_destroy(m.relations[0]);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            m.governor))
        && wl_columnar_memory_governor_ref_is_sole(m.governor));
    wl_columnar_memory_governor_ref_release(m.governor);
}

static void
test_lftj_metadata_ended_frame_refusal(void)
{
    metadata_fixture_t m;
    metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
    fixture_t *f = &m.fixture;
    while (f->record.lftj_metadata.phase !=
        WL_COLUMNAR_LFTJ_METADATA_BIND_FINAL) composed_action(f);
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_trace = NULL;
    wl_evaluation_control_request_cancel(f->operation);
    CHECK(wl_evaluation_control_charge(f->operation,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner, 0,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    int result; uint64_t work;
    CHECK(!metadata_ended_turn(&m, false, 192, 0, &work, &result));
    wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f->session, &nested));
    result = 12345;
    CHECK(metadata_ended_turn(&m, true, 384, 0, &work,
        &result) == EINVAL && !work
        && result == 12345 && f->record.identity == &f->record &&
        f->record.lftj_metadata_ended);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested));
    CHECK(!metadata_ended_turn(&m, true, 384, 0, &work, &result)
        && result == WL_EVALUATION_CONTROL_CANCELLED && work == 384);
    metadata_fixture_close(&m);
}

static void
test_lftj_metadata_header_and_grammar(void)
{
    char *names[] = {"a", "a"}; uint32_t keys[] = {0, 0};
    wl_plan_op_lftj_t metadata = {.k = 2, .rel_names = names, .key_cols = keys};
    wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ, .opaque_data = &metadata};
    wl_col_session_t session = {0};
    CHECK(!wl_columnar_lftj_metadata_header(&op, &session));
    CHECK(wl_columnar_lftj_metadata_header(NULL, &session) == EINVAL);
    CHECK(wl_columnar_lftj_metadata_header(&op, NULL) == EINVAL);
    session.current_iteration = 1; metadata.k = 1;
    CHECK(wl_columnar_lftj_metadata_header(&op, &session) == ENOTSUP);
    session.current_iteration = 0; session.diff_operators_active = true;
    CHECK(wl_columnar_lftj_metadata_header(&op, &session) == ENOTSUP);
    op.opaque_data = NULL;
    CHECK(wl_columnar_lftj_metadata_header(&op, &session) == EINVAL);
    op.opaque_data = &metadata; session.diff_operators_active = false;
    CHECK(wl_columnar_lftj_metadata_header(&op, &session) == EINVAL);
    metadata.k = 2;
    fixture_t f; fixture_init(&f, 1, 1, 0);
    wl_plan_relation_t body = {.ops = &op, .op_count = 1};
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin(&f.record,
        &body, 0, 1,
        f.frame, &f.session, 1, f.operation, f.operation_id, f.funding,
        f.funding_id));
    composed_run(&f);
    CHECK(f.record.semantic_result == EINVAL
        && f.record.lftj_metadata.phase == WL_COLUMNAR_LFTJ_METADATA_INERT
        && !f.record.lftj.identity && !f.record.completed);
    composed_close(&f);
}

#ifndef _WIN32
static void
test_lftj_metadata_replaced_original_refusal(void)
{
    metadata_fixture_t m;
    metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
    fixture_t *f = &m.fixture;
    while (f->record.lftj_metadata.phase !=
        WL_COLUMNAR_LFTJ_METADATA_INSTALL) composed_action(f);
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_trace = NULL;
    wl_evaluation_control_request_cancel(f->operation);
    CHECK(wl_evaluation_control_charge(f->operation,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner, 0,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    /* A replacement completion intentionally invalidates the old stop proof.
     * Isolate that protocol violation; the parent retains its genuine proof
     * and completes normal resource checks after the child exits. */
    pid_t child = fork(); CHECK(child >= 0);
    if (!child) {
        wl_columnar_lftj_metadata_proof_t saved = f->record.lftj_metadata;
        CHECK(!wl_evaluation_control_reset(f->operation)
            && !wl_evaluation_control_begin(f->operation, &f->operation_owner));
        int result = 12345; uint64_t work;
        CHECK(metadata_ended_turn(&m, false, 0, 0, &work, &result) == EINVAL
            && !work && result == 12345);
        CHECK(!wl_evaluation_control_finish(f->operation, &f->operation_owner,
            0, 0));
        CHECK(metadata_ended_turn(&m, false, 0, 0, &work, &result) == EINVAL
            && !work && result == 12345
            && !memcmp(&saved, &f->record.lftj_metadata, sizeof(saved))
            && f->record.identity == &f->record);
        _exit(0);
    }
    int status;
    CHECK(waitpid(child, &status,
        0) == child && WIFEXITED(status) && !WEXITSTATUS(status));
    int result; uint64_t work;
    CHECK(!metadata_ended_turn(&m, false, 192, 0, &work, &result));
    CHECK(!metadata_ended_turn(&m, true, 384, 0, &work, &result));
    metadata_fixture_close(&m);
}
#endif

static void
test_lftj_metadata_binding_semantic_parity(void)
{
    fixture_t f; composed_fixture(&f, source_new(1, 0), 0);
    f.session.base.evaluation_control = f.operation;
    char *names[] = {"a", "b"}; uint32_t keys[] = {0, 0};
    wl_plan_op_lftj_t metadata = {.k = 2, .rel_names = names, .key_cols = keys};
    wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ, .opaque_data = &metadata};
    wl_plan_relation_t body = {.ops = &op, .op_count = 1};
    const char *binding_names[] = {"a", "b", "c", "d"};
    col_rel_t *sources[4];
    for (unsigned i = 0; i < 4; ++i) {
        sources[i] = source_new(1, 1);
        CHECK(!wl_columnar_relation_rename_checked(sources[i],
            binding_names[i]));
    }
    f.session.rels = sources; f.session.nrels = f.session.rel_cap = 4;
    wl_columnar_eval_private_bindings_t bindings = {0};
    for (unsigned i = 1; i < 4; ++i) for (unsigned j = 0; j < i; ++j) {
            const char *duplicate[] = {"a", "b", "c", "d"};
            duplicate[i] = duplicate[j];
            CHECK(wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
                &body,
                f.operation, f.operation_id, duplicate, sources, 4) == EINVAL
                && !bindings.identity && !f.record.identity);
        }
    char unterminated[256]; memset(unterminated, 'z', sizeof(unterminated));
    for (unsigned bad = 0; bad < 4; ++bad) {
        const char *invalid[] = {"a", "b", "c", "d"};
        col_rel_t *relations[] = {sources[0], sources[1], sources[2],
                                  sources[3]};
        if (bad < 3) invalid[3] = bad == 0 ? NULL : bad ==
                1 ? "" : unterminated;
        else relations[3] = NULL;
        CHECK(wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
            &body,
            f.operation, f.operation_id, invalid, relations,
            4) == EINVAL && !f.record.identity);
    }
    /* Staleness arises from real writes before any parent adopts the proof.
     * It is an early authentication failure, not a forged ENTRY error. */
    for (unsigned i = 0; i < 4; ++i) {
        /* No parent adopted the preceding borrowed recipe. */
        memset(&bindings, 0, sizeof(bindings));
        CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session,
            &body,
            f.operation, f.operation_id, binding_names, sources, 4));
        int64_t extra = 10 + i; CHECK(!col_rel_append_row(sources[i], &extra));
        CHECK(wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
                &f.record,
                &body, 0, 1, f.frame, &f.session, 0, f.operation,
                f.operation_id,
                f.funding, f.funding_id,
                &bindings) == EINVAL && !f.record.identity);
    }
    memset(&bindings, 0, sizeof(bindings));
    char longest[256]; memset(longest, 'z', sizeof(longest));
    longest[255] = '\0';
    const char *bounded_names[] = {"a", "b", "c", longest};
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session, &body,
        f.operation, f.operation_id, bounded_names, sources, 4));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(&f.record,
        &body, 0, 1, f.frame, &f.session, 0, f.operation, f.operation_id,
        f.funding, f.funding_id, &bindings));
    metadata_trace_t trace = {.fixture = &f}; metadata_trace = &trace;
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint =
        metadata_trace_checkpoint;
    composed_run(&f);
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_trace = NULL;
    CHECK(!f.record.semantic_result && trace.entries == 4 && trace.pairs == 6);
    composed_raw_parity(&f, &body, 0); composed_close(&f);
    for (unsigned i = 0; i < 4; ++i) col_rel_destroy(sources[i]);
}

static metadata_fixture_t *metadata_shape_fixture;
static bool metadata_shape_result;

static void
metadata_shape_add(metadata_fixture_t *m, bool result_slot)
{
    fixture_t *f = &m->fixture;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack && !stack->top);
    CHECK(!eval_stack_push_delta(stack, m->relations[0], false, true));
    if (result_slot) {
        eval_entry_t *slot = wl_columnar_eval_stack_cleanup_result(f->frame);
        CHECK(slot && !slot->rel);
        *slot = eval_stack_pop(stack);
    }
}

static void
metadata_shape_restore(metadata_fixture_t *m, bool result_slot)
{
    fixture_t *f = &m->fixture;
    if (result_slot) {
        eval_entry_t *slot = wl_columnar_eval_stack_cleanup_result(f->frame);
        CHECK(slot && slot->rel == m->relations[0] && !slot->owned &&
            slot->is_delta);
        *slot = (eval_entry_t){0};
    } else {
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
        CHECK(stack && stack->top == 1);
        eval_entry_t borrowed = eval_stack_pop(stack);
        CHECK(borrowed.rel == m->relations[0] && !borrowed.owned &&
            borrowed.is_delta);
    }
}

static void
metadata_shape_checkpoint(wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding)
{
    (void)funding;
    CHECK(metadata_shape_fixture && r == &metadata_shape_fixture->fixture.record
        && r->composed_busy);
    metadata_shape_add(metadata_shape_fixture, metadata_shape_result);
}

static void
test_lftj_metadata_real_frame_shape(void)
{
    for (unsigned kind = 0; kind < 5; ++kind) {
        metadata_fixture_t m;
        metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
        fixture_t *f = &m.fixture;
        while (f->record.lftj_metadata.phase !=
            WL_COLUMNAR_LFTJ_METADATA_INSTALL) composed_action(f);
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
        metadata_trace = NULL;
        wl_evaluation_control_request_cancel(f->operation);
        CHECK(wl_evaluation_control_charge(f->operation,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner, 0,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_evaluation_control_report_t before, after;
        CHECK(!wl_evaluation_control_report(f->operation, &before));
        int result = 12345; uint64_t work;
        bool slot = (kind & 1u) != 0;
        if (kind == 4) CHECK(!metadata_ended_turn(&m, false, 192, 0, &work,
                &result));
        if (kind < 2 || kind == 4) metadata_shape_add(&m, slot);
        else {
            metadata_shape_fixture = &m; metadata_shape_result = slot;
            wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint =
                metadata_shape_checkpoint;
        }
        result = 12345;
        int rc = metadata_ended_turn(&m, kind == 4, kind == 4 ? 384 : 192, 0,
                &work, &result);
        if (rc != EBUSY) fprintf(stderr,
                "metadata real frame shape: case=%u rc=%d consumed=%llu\n",
                kind, rc, (unsigned long long)work);
        CHECK(rc == EBUSY && result == 12345 && work == (kind == 4 ? 384u : 0u)
            && f->record.identity == &f->record &&
            f->record.control == f->operation
            && f->record.lftj_metadata_ended == (kind == 4));
        wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint = NULL;
        metadata_shape_fixture = NULL;
        CHECK(!wl_evaluation_control_report(f->operation,
            &after) && metadata_reports_equal(&before, &after));
        metadata_shape_restore(&m, slot);
        if (kind != 4) CHECK(!metadata_ended_turn(&m, false, 192, 0, &work,
                &result));
        CHECK(!metadata_ended_turn(&m, true, 384, 0, &work, &result));
        metadata_fixture_close(&m);
    }
}

static metadata_fixture_t *metadata_fault_fixture;
static unsigned metadata_fault_kind, metadata_fault_hits;
static wl_plan_op_lftj_t metadata_fault_replacement;
static wl_columnar_eval_stack_cleanup_frame_t *metadata_fault_nested;

static void
metadata_fault_checkpoint(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    wl_columnar_lftj_metadata_phase_t phase = metadata_fault_kind == 4
        ? WL_COLUMNAR_LFTJ_METADATA_INSTALL : WL_COLUMNAR_LFTJ_METADATA_PREFIX;
    if (r->lftj_metadata.phase != phase) return;
    CHECK(metadata_fault_fixture && r == &metadata_fault_fixture->fixture.record
        && r->composed_busy && !r->lftj_metadata_dispatch &&
        !metadata_fault_hits++);
    metadata_fixture_t *m = metadata_fault_fixture;
    if (metadata_fault_kind == 0) {
        m->op.opaque_data = NULL; m->fixture.session.current_iteration = 1;
    } else if (metadata_fault_kind ==
        1) m->fixture.session.current_iteration = 1;
    else if (metadata_fault_kind == 2) {
        metadata_fault_replacement = m->metadata;
        m->op.opaque_data = &metadata_fault_replacement;
    } else if (metadata_fault_kind == 5)
        CHECK(!wl_columnar_eval_stack_cleanup_begin(&m->fixture.session,
            &metadata_fault_nested));
    else wl_evaluation_control_request_cancel(r->control);
}

static void
test_lftj_metadata_checked_handoff_failures(void)
{
    for (unsigned kind = 0; kind < 6; ++kind) {
        metadata_fixture_t m;
        metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
        fixture_t *f = &m.fixture;
        wl_columnar_lftj_metadata_phase_t target = kind == 4
            ? WL_COLUMNAR_LFTJ_METADATA_INSTALL :
            WL_COLUMNAR_LFTJ_METADATA_PREFIX;
        while (f->record.lftj_metadata.phase != target) composed_action(f);
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
        metadata_trace = NULL;
        unsigned char prefix[offsetof(wl_columnar_lftj_attempt_t, retirement)];
        memcpy(prefix, &f->record.lftj, sizeof(prefix));
        uint64_t before, after;
        CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
            f->operation_id, &before));
        metadata_fault_fixture = &m; metadata_fault_kind = kind;
        metadata_fault_hits = 0;
        metadata_fault_nested = NULL;
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint =
            metadata_fault_checkpoint;
        composed_action(f);
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
        metadata_fault_fixture = NULL;
        CHECK(metadata_fault_hits == 1 && !f->record.lftj_metadata_dispatch
            && f->record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            && !memcmp(prefix, &f->record.lftj,
            sizeof(prefix)) && !f->record.lftj.identity);
        CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
            f->operation_id, &after) && before == after);
        int expected = kind == 1 ? ENOTSUP : kind == 3 ||
            kind == 4 ? WL_EVALUATION_CONTROL_CANCELLED : EINVAL;
        CHECK(f->record.semantic_result == expected);
        /* These are negative immutable-header fault injections. Restore only
         * outside the returned action, before independent cleanup; no positive
         * progress is claimed under changed metadata/session context. */
        m.op.opaque_data = &m.metadata; f->session.current_iteration = 0;
        if (metadata_fault_nested) CHECK(!wl_columnar_eval_stack_cleanup_finish(
                    &metadata_fault_nested));
        composed_run(f);
        CHECK(f->record.semantic_result == expected &&
            !f->record.lftj_metadata_dispatch
            && f->record.lftj_metadata.phase ==
            WL_COLUMNAR_LFTJ_METADATA_INERT);
        metadata_fixture_close(&m);
    }
}

static void
test_lftj_metadata_frame_output_alias(void)
{
    for (unsigned kind = 0; kind < 4; ++kind) {
        bool ack = (kind & 1u) != 0;
        metadata_fixture_t m;
        metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
        fixture_t *f = &m.fixture;
        while (f->record.lftj_metadata.phase !=
            WL_COLUMNAR_LFTJ_METADATA_INSTALL) composed_action(f);
        wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
        metadata_trace = NULL;
        wl_evaluation_control_request_cancel(f->operation);
        CHECK(wl_evaluation_control_charge(f->operation,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner, 0,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        int result = 12345; uint64_t work;
        if (ack) CHECK(!metadata_ended_turn(&m, false, 192, 0, &work, &result));
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
        CHECK(stack && !stack->top && sizeof(stack->top) == sizeof(int));
        /* This is an actual mutable frame loan. The API must not turn its
         * copied status result into a write to its own stack metadata. */
        eval_entry_t *entry = wl_columnar_eval_stack_cleanup_result(f->frame);
        CHECK(entry && !entry->seg_count &&
            sizeof(entry->seg_count) == sizeof(int));
        wl_evaluation_control_report_t before, after;
        CHECK(!wl_evaluation_control_report(f->operation, &before));
        int *alias = kind < 2 ? (int *)(void *)&stack->top
                             : (int *)(void *)&entry->seg_count;
        int rc = metadata_ended_turn(&m, ack, ack ? 384 : 192, 0, &work, alias);
        if (rc != EINVAL) fprintf(stderr,
                "metadata frame output alias: ack=%u rc=%d work=%llu top=%u\n",
                ack, rc, (unsigned long long)work, stack->top);
        CHECK(rc == EINVAL && !work && !stack->top && !entry->seg_count
            && f->record.identity == &f->record && f->record.frame == f->frame
            && f->record.lftj_metadata_ended == ack);
        CHECK(!wl_evaluation_control_report(f->operation, &after)
            && metadata_reports_equal(&before, &after));
        if (!ack) CHECK(!metadata_ended_turn(&m, false, 192, 0, &work,
                &result));
        CHECK(!metadata_ended_turn(&m, true, 384, 0, &work, &result));
        metadata_fixture_close(&m);
    }
}

static const void *metadata_funding_hook_owner;
static uint64_t metadata_funding_hook_id;
static unsigned metadata_funding_hook_hits;

static void
metadata_funding_generation_checkpoint(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding)
{
    CHECK(r->composed_busy && metadata_funding_hook_owner &&
        !metadata_funding_hook_hits++);
    CHECK(!wl_evaluation_control_finish(funding, metadata_funding_hook_owner, 0,
        0));
    CHECK(!wl_evaluation_control_reset(funding));
    CHECK(!wl_evaluation_control_begin(funding, metadata_funding_hook_owner));
    metadata_funding_hook_id = wl_evaluation_control_active_attempt_id(funding);
}

static void
test_lftj_metadata_funding_generation(void)
{
    metadata_fixture_t m;
    metadata_fixture_begin_options(&m, UINT64_C(1000000000), true);
    fixture_t *f = &m.fixture;
    while (f->record.lftj_metadata.phase !=
        WL_COLUMNAR_LFTJ_METADATA_INSTALL) composed_action(f);
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_trace = NULL;
    wl_evaluation_control_request_cancel(f->operation);
    CHECK(wl_evaluation_control_charge(f->operation,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner, 0,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(f->operation, &before));
    wl_columnar_lftj_metadata_proof_t proof = f->record.lftj_metadata;
    int owner = 0, result = 12345;
    wl_evaluation_control_t *funding = control_new(192, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding), work;
    metadata_funding_hook_owner = &owner; metadata_funding_hook_hits = 0;
    wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint =
        metadata_funding_generation_checkpoint;
    int rc =
        wl_columnar_eval_plan_numeric_lftj_metadata_cleanup_ended(&f->record,
            funding, id, &result);
    wl_columnar_eval_plan_test_lftj_metadata_cleanup_checkpoint = NULL;
    metadata_funding_hook_owner = NULL;
    CHECK(rc == EINVAL && metadata_funding_hook_hits == 1 &&
        metadata_funding_hook_id != id
        && result == 12345 && !f->record.lftj_metadata_ended &&
        !f->record.composed_busy
        && !memcmp(&proof, &f->record.lftj_metadata, sizeof(proof)));
    CHECK(!wl_evaluation_control_test_active_consumed(funding,
        metadata_funding_hook_id, &work) && !work);
    CHECK(!wl_evaluation_control_report(f->operation,
        &after) && metadata_reports_equal(&before, &after));
    CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    CHECK(!metadata_ended_turn(&m, false, 192, 0, &work, &result));
    CHECK(!metadata_ended_turn(&m, true, 384, 0, &work, &result));
    metadata_fixture_close(&m);
}

static void
prepared_until(wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_unary_prepared_phase_t target)
{
    for (unsigned turn = 0; ; ++turn) {
        wl_columnar_eval_stack_unary_prepared_status_t status;
        CHECK(!wl_columnar_eval_stack_unary_prepared_query(p, &status));
        if (status.phase == target) return;
        CHECK(turn < 1000);
        int rc = wl_columnar_eval_stack_unary_prepared_step(p);
        CHECK(!rc || rc == EAGAIN);
    }
}

static void
prepared_drain_ack(fixture_t *f, wl_columnar_eval_stack_unary_prepared_t *p)
{
    for (unsigned turn = 0; ; ++turn) {
        wl_columnar_eval_stack_unary_prepared_status_t status;
        CHECK(!wl_columnar_eval_stack_unary_prepared_query(p, &status));
        if (status.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY) break;
        CHECK(turn < 1000);
        int rc = wl_columnar_eval_stack_unary_prepared_drain(p, f->funding,
                f->funding_id);
        CHECK(!rc || rc == EAGAIN);
    }
    CHECK(!wl_columnar_eval_stack_unary_prepared_ack(p, f->funding,
        f->funding_id));
    wl_columnar_eval_stack_unary_prepared_status_t status;
    CHECK(!wl_columnar_eval_stack_unary_prepared_query(p, &status)
        && status.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
        && !status.old_held && !status.output_held);
}

static void
prepared_finish_existing(fixture_t *f, int initiating)
{
    bool done = false; int semantic = 12345;
    for (unsigned turn = 0; !done; ++turn) {
        CHECK(turn < 10000);
        int rc =
            wl_columnar_eval_stack_unary_finish_scalar_funded_owned(f->frame,
                initiating, &f->record, f->funding, f->funding_id, &semantic,
                &done);
        if (rc || semantic != initiating) fprintf(stderr,
                "prepared existing finish: turn=%u rc=%d semantic=%d expected=%d\n",
                turn, rc, semantic, initiating);
        CHECK(!rc && semantic == initiating);
        wl_columnar_eval_stack_test_unary_observation_t observed;
        CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame, &f->record,
            &observed));
        if (!done && observed.segments && !observed.owned) {
            const wl_columnar_eval_stack_test_unary_observation_t original =
                observed;
            for (unsigned stage = 0; stage < 3; ++stage) {
                int owner = 0;
                wl_evaluation_control_t *fund = control_new(stage ? 128 : 127,
                        &owner);
                uint64_t id = wl_evaluation_control_active_attempt_id(fund);
                rc =
                    wl_columnar_eval_stack_unary_finish_scalar_funded_owned(
                    f->frame,
                    initiating, &f->record, fund, id, &semantic, &done);
                CHECK(rc == (stage ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT) &&
                    semantic == initiating);
                CHECK(wl_evaluation_control_finish(fund, &owner, rc, 0) == rc);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(fund,
                    &report) && report.consumed == (stage ? 128u : 0u));
                CHECK(!wl_evaluation_control_detach(fund, &owner));
                wl_evaluation_control_release(fund);
                CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame,
                    &f->record, &observed));
                CHECK(observed.first_result == original.first_result &&
                    observed.committed == original.committed
                    && observed.result_saved &&
                    observed.kind == original.kind && !observed.continuation);
                if (!stage) CHECK(!done &&
                        observed.relation == original.relation
                        && observed.segments == original.segments &&
                        observed.segment_count == original.segment_count
                        && observed.owned == original.owned &&
                        observed.is_delta == original.is_delta);
                else if (stage == 1) CHECK(!done &&
                        observed.relation == original.relation
                        && !observed.segments && !observed.segment_count
                        && observed.owned == original.owned &&
                        observed.is_delta == original.is_delta);
                else CHECK(done && !observed.relation && !observed.segments &&
                        !observed.segment_count);
            }
        }
    }
    CHECK(!wl_columnar_eval_stack_unary_reset_owned(f->frame, &f->record));
}

static fixture_t *prepared_guard_fixture;
static unsigned prepared_guard_hits[WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED + 1];

static void
prepared_guard_checkpoint(wl_columnar_eval_stack_unary_prepared_t *p)
{
    fixture_t *f = prepared_guard_fixture;
    CHECK(f && p->frame == f->frame && p->numeric_owner == &f->record &&
        p->busy);
    uint64_t original, funding, after;
    CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
        f->operation_id, &original));
    CHECK(!wl_evaluation_control_test_active_consumed(f->funding, f->funding_id,
        &funding));
    wl_columnar_eval_plan_numeric_phase_t phase = f->record.phase;
    uint32_t cursor = f->record.cursor, count = f->session.cleanup_active_count;
    uint64_t reserved = f->session.cleanup_reserved_bytes;
    wl_columnar_eval_stack_unary_prepared_status_t status;
    memset(&status, 0x5a, sizeof(status));
    unsigned char saved_status[sizeof(status)];
    memcpy(saved_status, &status, sizeof(status));
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p, &status) == EBUSY
        && !memcmp(&status, saved_status, sizeof(status)));
    CHECK(wl_columnar_eval_stack_unary_prepared_step(p) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_prepared_cancel(p, ERANGE, f->funding,
        f->funding_id) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_prepared_drain(p, f->funding,
        f->funding_id) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_prepared_ack(p, f->funding,
        f->funding_id) == EBUSY);
    CHECK(!wl_columnar_eval_stack_numeric_owner(f->frame, &f->record));
    CHECK(wl_columnar_eval_stack_numeric_writable(f->frame,
        &f->record) == EBUSY);
    CHECK(!wl_columnar_eval_stack_cleanup_stack(f->frame) &&
        !wl_columnar_eval_stack_cleanup_result(f->frame));
    eval_stack_t *stack = NULL;
    CHECK(wl_columnar_eval_stack_operator_stack_owned(f->frame, &f->session,
        &stack, &f->record) == EBUSY && !stack);
    wl_columnar_filter_context_t *context = NULL;
    CHECK(wl_columnar_eval_stack_filter_context_owned(f->frame, &context,
        &f->record) == EBUSY && !context);
    wl_columnar_eval_serial_framed_attempt_t *framed = NULL;
    CHECK(wl_columnar_eval_stack_framed_context(f->frame, &f->session,
        &framed) == EBUSY && !framed);
    wl_columnar_merge_concat_context_t *concat = NULL;
    CHECK(wl_columnar_eval_stack_concat_context_owned(f->frame, &concat,
        &f->record) == EBUSY && !concat);
    const col_rel_t *input = NULL; col_rel_t **slot = NULL;
    CHECK(wl_columnar_eval_stack_unary_input_owned(f->frame, &input,
        &f->record) == EBUSY && !input);
    CHECK(wl_columnar_eval_stack_unary_output_owned(f->frame, &slot,
        &f->record) == EBUSY && !slot);
    bool committed = true, ready = true, done = true, retained = true,
        pending = true;
    int semantic = 12345;
    CHECK(wl_columnar_eval_stack_unary_commit_attempt_owned(f->frame,
        &committed, &f->record) == EBUSY && committed);
    CHECK(wl_columnar_eval_stack_unary_commit_numeric_join_owned(f->frame,
        &committed, &f->record) == EBUSY && committed);
    CHECK(wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned(f->frame,
        &f->record, &ready) == EBUSY && ready);
    CHECK(wl_columnar_eval_stack_unary_finish_scalar_funded_owned(f->frame,
        ERANGE, &f->record,
        f->funding, f->funding_id, &semantic,
        &done) == EBUSY && semantic == 12345 && done);
    CHECK(wl_columnar_eval_stack_filter_finish_owned(f->frame, ERANGE,
        &semantic, &retained,
        &f->record) == EBUSY && retained && semantic == 12345);
    CHECK(wl_columnar_filter_empty_end_pending_owned(f->frame, &f->record,
        &pending) == EBUSY && pending);
    CHECK(wl_columnar_eval_stack_unary_reset_owned(f->frame,
        &f->record) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_owned(f->frame,
        f->operation,
        f->operation_id, &f->record) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_attempt_step(&f->record, f->funding,
        f->funding_id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_consume_result(&f->record, 0,
        f->funding, f->funding_id) == EBUSY);
    CHECK(wl_columnar_eval_plan_numeric_attempt_ack(&f->record, f->funding,
        f->funding_id) == EBUSY);
    wl_columnar_eval_stack_cleanup_frame_t *copy = f->frame, *nested = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&copy) == EBUSY &&
        copy == f->frame);
    CHECK(wl_columnar_eval_stack_cleanup_begin(&f->session,
        &nested) == EBUSY && !nested);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(&f->session,
        &nested,
        f->operation, f->operation_id) == EBUSY && !nested);
    wl_columnar_eval_cleanup_receipt_key_t key;
    memset(&key, 0x6b, sizeof(key)); unsigned char key_before[sizeof(key)];
    memcpy(key_before, &key, sizeof(key));
    CHECK(wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            &f->session,
            &nested, f->operation, f->operation_id, 123,
            &key) == EBUSY && !nested
        && !memcmp(key_before, &key, sizeof(key)));
    CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
        f->operation_id, &after) && after == original);
    CHECK(!wl_evaluation_control_test_active_consumed(f->funding, f->funding_id,
        &after) && after == funding);
    CHECK(f->record.phase == phase && f->record.cursor == cursor &&
        !f->record.semantic_result
        && !f->record.protocol_result && f->session.cleanup_active == f->frame
        && f->session.cleanup_active_count == count &&
        f->session.cleanup_reserved_bytes == reserved);
    ++prepared_guard_hits[p->phase];
}

static void
test_prepared_unary_actual_lifecycle(void)
{
    for (unsigned owned = 0; owned < 2; ++owned)
        for (unsigned action = 0; action < 3; ++action) {
            fixture_t f; fixture_init(&f, 3, 3, 0);
            wl_columnar_memory_governor_ref_t *g = join_governor(&f, 1u << 24);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            stack->items[0].owned = owned != 0; stack->items[0].is_delta = true;
            if (!owned) {
                stack->items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
                CHECK(stack->items[0].seg_boundaries);
                stack->items[0].seg_boundaries[0] = 0;
                stack->items[0].seg_boundaries[1] = 3;
                stack->items[0].seg_count = 1;
            }
            uint32_t projection = 0;
            wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1,
                               .project_indices = &projection};
            wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
            fixture_begin(&f, &plan, 0, 1);
            CHECK(!wl_columnar_eval_stack_unary_begin_attempt_owned(f.frame,
                f.operation, f.operation_id, &f.record));
            col_rel_t **slot = NULL;
            CHECK(!wl_columnar_eval_stack_unary_output_owned(f.frame, &slot,
                &f.record));
            CHECK(!wl_columnar_relation_new_auto_governed_attempt_controlled(
                    "prepared output", 3, 8, g, slot, f.operation,
                    f.operation_id));
            const int64_t row[] = {40, 41, 42};
            CHECK(!col_rel_append_row(*slot, row)); col_rel_t *output = *slot;
            eval_entry_t original = stack->items[0];
            wl_columnar_eval_stack_unary_prepared_t p = {0};
            if (owned) {
                CHECK(wl_columnar_eval_stack_unary_prepared_begin(&p, f.frame,
                    &f.record) == EBUSY
                    && !p.identity);
                bool ready = false;
                for (unsigned turn = 0; !ready; ++turn) {
                    CHECK(turn < 1000);
                    CHECK(
                        !
                        wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned
                            (f.frame, &f.record, &ready));
                }
            }
            CHECK(!wl_columnar_eval_stack_unary_prepared_begin(&p, f.frame,
                &f.record));
            prepared_guard_fixture = &f;
            memset(prepared_guard_hits, 0, sizeof(prepared_guard_hits));
            wl_columnar_eval_stack_test_unary_prepared_checkpoint =
                prepared_guard_checkpoint;
            CHECK(!wl_columnar_eval_stack_cleanup_stack(f.frame)
                && !wl_columnar_eval_stack_cleanup_result(f.frame));
            prepared_until(&p,
                action == 2 ? WL_COLUMNAR_EVAL_STACK_PREPARED_READY
                : WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
            wl_columnar_eval_stack_unary_prepared_status_t status;
            if (action == 0) {
                uint64_t before, after;
                CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
                    f.operation_id, &before));
                wl_columnar_eval_stack_unary_commit_prepared_nofail(&p);
                wl_columnar_eval_stack_test_unary_observation_t retired;
                CHECK(!wl_columnar_eval_stack_test_unary_observe(f.frame,
                    &f.record, &retired)
                    && retired.relation == original.rel &&
                    retired.segments == original.seg_boundaries
                    && retired.segment_count == original.seg_count &&
                    retired.owned == original.owned
                    && retired.is_delta == original.is_delta &&
                    retired.kind == (unsigned)original.kind
                    && retired.continuation == original.continuation &&
                    retired.committed);
                CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
                    f.operation_id, &after) && before == after);
                CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &status)
                    && status.committed && status.result_saved &&
                    !status.first_result);
                CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&p, EIO,
                    f.funding, f.funding_id));
            } else CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&p,
                    action == 1 ? ERANGE : 0,
                    f.funding, f.funding_id));
            prepared_drain_ack(&f, &p);
            wl_columnar_eval_stack_test_unary_prepared_checkpoint = NULL;
            prepared_guard_fixture = NULL;
            CHECK(prepared_guard_hits[WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD]
                && prepared_guard_hits[
                    WL_COLUMNAR_EVAL_STACK_PREPARED_ALIAS_ROWS]
                && prepared_guard_hits[
                    WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OUTPUT]
                && prepared_guard_hits[
                    WL_COLUMNAR_EVAL_STACK_PREPARED_CLEAR_WORK]
                && prepared_guard_hits[WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY]);
            CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &status));
            CHECK(status.first_result == (action == 1 ? ERANGE : 0) &&
                status.result_saved);
            CHECK(stack->items[0].rel == (action ? original.rel : output));
            if (action) CHECK(stack->items[0].owned == original.owned
                    && stack->items[0].is_delta == original.is_delta
                    && stack->items[0].seg_boundaries == original.seg_boundaries
                    && stack->items[0].seg_count == original.seg_count);
            if (action == 2) {
                bool committed = false;
                CHECK(!wl_columnar_eval_stack_unary_commit_attempt_owned(
                        f.frame, &committed, &f.record) && committed);
            }
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                status.first_result, 0) == status.first_result);
            prepared_finish_existing(&f, status.first_result);
            if (owned) f.input = NULL;
            run(&f); CHECK(f.record.semantic_result == EINVAL);
            fixture_close(&f);
            /* ACKED history is safe after the real frame and control owners
             * have been freed; no saved external pointer is consulted. */
            CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &status)
                && status.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
}

typedef struct {
    fixture_t fixture;
    wl_columnar_eval_stack_unary_prepared_t prepared;
    wl_columnar_memory_governor_ref_t *governor;
    wl_plan_op_t op;
    wl_plan_relation_t plan;
    uint32_t projection;
    bool owned;
} prepared_case_t;

static void
prepared_case_init(prepared_case_t *m, bool owned, uint64_t limit)
{
    memset(m, 0, sizeof(*m)); m->owned = owned;
    fixture_t *f = &m->fixture;
    fixture_init(f, 3, 3, limit);
    m->governor = join_governor(f, 1u << 24);
    wl_columnar_eval_stack_cleanup_stack(f->frame)->items[0].owned = owned;
    m->op = (wl_plan_op_t){.op = WL_PLAN_OP_MAP, .project_count = 1,
                           .project_indices = &m->projection};
    m->plan = (wl_plan_relation_t){.ops = &m->op, .op_count = 1};
    fixture_begin(f, &m->plan, 0, 1);
    CHECK(!wl_columnar_eval_stack_unary_begin_attempt_owned(f->frame,
        f->operation, f->operation_id, &f->record));
    col_rel_t **slot = NULL;
    CHECK(!wl_columnar_eval_stack_unary_output_owned(f->frame, &slot,
        &f->record));
    CHECK(!wl_columnar_relation_new_auto_governed_attempt_controlled(
            "prepared case", 3, 8, m->governor, slot, f->operation,
            f->operation_id));
    const int64_t row[] = {10, 11, 12}; CHECK(!col_rel_append_row(*slot, row));
    if (owned) {
        bool ready = false;
        for (unsigned i = 0; !ready; ++i) {
            CHECK(i < 1000);
            CHECK(!wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned(
                    f->frame, &f->record, &ready));
        }
    }
}

static void
prepared_case_close(prepared_case_t *m, int error)
{
    fixture_t *f = &m->fixture;
    wl_columnar_eval_stack_unary_prepared_t *p = &m->prepared;
    uint64_t successor = wl_evaluation_control_active_attempt_id(f->operation);
    wl_evaluation_control_report_t before, after;
    bool retained = successor && successor != f->operation_id;
    if (retained) CHECK(!wl_evaluation_control_report(f->operation, &before));
    if (p->identity && p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED) {
        if (p->phase < WL_COLUMNAR_EVAL_STACK_PREPARED_COMMITTED_DRAIN)
            CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(p, error,
                f->funding, f->funding_id));
        prepared_drain_ack(f, p);
        wl_columnar_eval_stack_unary_prepared_status_t status;
        CHECK(!wl_columnar_eval_stack_unary_prepared_query(p, &status));
        error = status.first_result;
    }
    if (wl_evaluation_control_active_attempt_id(f->operation) ==
        f->operation_id)
        CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner,
            error, 0) == error);
    col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(f->input);
    prepared_finish_existing(f, error);
    CHECK(wl_columnar_relation_snapshot_equal(snapshot,
        wl_columnar_relation_snapshot(f->input))
        && f->input->nrows == 3 && f->input->columns[2][2] == 4);
    if (retained) CHECK(!wl_evaluation_control_report(f->operation,
            &after) && metadata_reports_equal(&before, &after));
    if (m->owned) f->input = NULL;
    run(f); CHECK(f->record.semantic_result == EINVAL);
    if (retained) {
        uint64_t work;
        CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
            successor, &work) && !work);
    }
    fixture_close(f);
    if (p->identity) {
        wl_columnar_eval_stack_unary_prepared_status_t status;
        CHECK(!wl_columnar_eval_stack_unary_prepared_query(p, &status)
            && status.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED);
    }
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            m->governor)));
    wl_columnar_memory_governor_ref_release(m->governor);
}

typedef struct {
    wl_columnar_eval_stack_unary_prepared_phase_t phase;
    unsigned stage;
    uint64_t fee;
} prepared_boundary_t;

static const prepared_boundary_t prepared_boundaries[] = {
    {WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD, 0, 192},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_ALIAS_FIXED, 0, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_ALIAS_ROWS, 0, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD, 0, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD, 1, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OUTPUT, 0, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OUTPUT, 1, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OLD, 0, 192},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OUTPUT, 0, 192},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_READY, 0, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_OLD, 0, 192},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_OUTPUT, 0, 192},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_LATCH, 0, 128},
    {WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_LATCH, 0, 256}
};

static void
prepared_reach(prepared_case_t *m, const prepared_boundary_t *target)
{
    wl_columnar_eval_stack_unary_prepared_t *p = &m->prepared;
    for (unsigned i = 0; ; ++i) {
        unsigned stage = target->phase ==
            WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD
            ? p->work.old_reader.acquire_stage :
            p->work.output_reader.acquire_stage;
        if (p->phase == target->phase &&
            (!target->stage || stage == target->stage)) return;
        CHECK(i < 1000 && !wl_columnar_eval_stack_unary_prepared_step(p));
    }
}

static void
test_prepared_unary_original_boundaries(void)
{
    for (unsigned owned = 0; owned < 2; ++owned) {
        prepared_case_t baseline; prepared_case_init(&baseline, owned, 0);
        uint64_t before;
        CHECK(!wl_evaluation_control_test_active_consumed(
                baseline.fixture.operation,
                baseline.fixture.operation_id, &before));
        prepared_case_close(&baseline, ERANGE);
        for (unsigned exact = 0; exact < 2; ++exact) {
            prepared_case_t m;
            prepared_case_init(&m, owned, before + 256 - !exact);
            fixture_t *f = &m.fixture;
            int rc = wl_columnar_eval_stack_unary_prepared_begin(&m.prepared,
                    f->frame, &f->record);
            CHECK(rc == (exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
            uint64_t work;
            CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
                f->operation_id, &work)
                && work == before + (exact ? 256u : 0u));
            if (exact) CHECK(wl_evaluation_control_charge_attempt(f->operation,
                    f->operation_id, 1)
                    == WL_EVALUATION_CONTROL_WORK_LIMIT);
            prepared_case_close(&m, WL_EVALUATION_CONTROL_WORK_LIMIT);
        }
        for (unsigned b = 0;
            b < sizeof(prepared_boundaries) / sizeof(prepared_boundaries[0]);
            ++b){
            const prepared_boundary_t *target = &prepared_boundaries[b];
            if (owned &&
                target->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD &&
                target->stage) continue;
            prepared_case_init(&baseline, owned, 0);
            CHECK(!wl_columnar_eval_stack_unary_prepared_begin(
                    &baseline.prepared, baseline.fixture.frame,
                    &baseline.fixture.record));
            prepared_reach(&baseline, target);
            CHECK(!wl_evaluation_control_test_active_consumed(
                    baseline.fixture.operation,
                    baseline.fixture.operation_id, &before));
            prepared_case_close(&baseline, ERANGE);
            for (unsigned exact = 0; exact < 2; ++exact) {
                prepared_case_t m;
                prepared_case_init(&m, owned, before + target->fee - !exact);
                fixture_t *f = &m.fixture;
                CHECK(!wl_columnar_eval_stack_unary_prepared_begin(&m.prepared,
                    f->frame, &f->record));
                prepared_reach(&m, target);
                int rc =
                    wl_columnar_eval_stack_unary_prepared_step(&m.prepared);
                bool latch_half = target->phase ==
                    WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_LATCH
                    && (target->fee == 128 || !exact);
                CHECK(rc == (exact &&
                    !latch_half ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
                uint64_t work, paid = exact && !latch_half ? target->fee
                    : target->phase ==
                    WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_LATCH &&
                    target->fee - !exact >= 128 ? 128 : 0;
                CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
                    f->operation_id, &work)
                    && work == before + paid);
                if (!rc) CHECK(wl_evaluation_control_charge_attempt(
                            f->operation, f->operation_id, 1)
                        == WL_EVALUATION_CONTROL_WORK_LIMIT);
                prepared_case_close(&m, WL_EVALUATION_CONTROL_WORK_LIMIT);
            }
        }
    }
}

static void
test_prepared_unary_cancelled_parent_cleanup(void)
{
    for (unsigned b = 0;
        b < sizeof(prepared_boundaries) / sizeof(prepared_boundaries[0]); ++b){
        const prepared_boundary_t *target = &prepared_boundaries[b];
        prepared_case_t m; prepared_case_init(&m, false, 0);
        fixture_t *f = &m.fixture;
        CHECK(!wl_columnar_eval_stack_unary_prepared_begin(&m.prepared,
            f->frame, &f->record));
        prepared_reach(&m, target);
        wl_evaluation_control_request_cancel(f->operation);
        CHECK(wl_evaluation_control_charge_attempt(f->operation,
            f->operation_id, 0) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner, 0,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!wl_evaluation_control_reset(f->operation));
        CHECK(!wl_evaluation_control_begin(f->operation, &f->operation_owner));
        prepared_case_close(&m, WL_EVALUATION_CONTROL_CANCELLED);
    }
}

static int
prepared_funding_turn(prepared_case_t *m, unsigned action, uint64_t limit,
    uint64_t adjustment, int result, uint64_t *spent)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    int rc = action ==
        0 ? wl_columnar_eval_stack_unary_prepared_cancel(&m->prepared, result,
            funding, id + adjustment)
        : action ==
        1 ? wl_columnar_eval_stack_unary_prepared_drain(&m->prepared, funding,
            id + adjustment)
        : wl_columnar_eval_stack_unary_prepared_ack(&m->prepared, funding,
            id + adjustment);
    (void)wl_evaluation_control_finish(funding, &owner, 0, 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    *spent = report.consumed;
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static void
test_prepared_unary_funded_cleanup(void)
{
    for (unsigned partial = 0; partial < 2; ++partial) {
        prepared_case_t m; prepared_case_init(&m, false, 0);
        fixture_t *f = &m.fixture;
        wl_columnar_eval_stack_unary_prepared_t *p = &m.prepared;
        CHECK(!wl_columnar_eval_stack_unary_prepared_begin(p, f->frame,
            &f->record));
        if (!partial) prepared_until(p, WL_COLUMNAR_EVAL_STACK_PREPARED_READY);
        wl_columnar_eval_stack_unary_prepared_t saved;
        memcpy(&saved, p, sizeof(saved));
        uint64_t spent;
        if (partial) {
            CHECK(prepared_funding_turn(&m, 0, 128, 0, 0,
                &spent) == EINVAL && !spent
                && !memcmp(&saved, p, sizeof(saved)));
        }
        CHECK(prepared_funding_turn(&m, 0, 128, 1, ERANGE,
            &spent) == EINVAL && !spent
            && !memcmp(&saved, p, sizeof(saved)));
        CHECK(prepared_funding_turn(&m, 0, 127, 0, ERANGE,
            &spent) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !spent && !memcmp(&saved, p, sizeof(saved)));
        CHECK(!prepared_funding_turn(&m, 0, 128, 0, ERANGE,
            &spent) && spent == 128);
        for (unsigned turn = 0;
            p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY; ++turn){
            CHECK(turn < 100);
            uint64_t fee = p->phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_CANCEL_SANITIZE ? 256 : 128;
            wl_columnar_eval_stack_unary_prepared_work_t work;
            memcpy(&work, &p->work, sizeof(work));
            wl_columnar_eval_stack_unary_prepared_phase_t phase = p->phase;
            CHECK(prepared_funding_turn(&m, 1, fee - 1, 0, 0,
                &spent) == WL_EVALUATION_CONTROL_WORK_LIMIT
                && !spent && p->phase == phase && !memcmp(&work, &p->work,
                sizeof(work))
                && p->result_saved && p->first_result == ERANGE);
            CHECK(!prepared_funding_turn(&m, 1, fee, 0, 0,
                &spent) && spent == fee);
        }
        memcpy(&saved, p, sizeof(saved));
        CHECK(prepared_funding_turn(&m, 2, 191, 0, 0,
            &spent) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !spent && !memcmp(&saved, p, sizeof(saved)));
        CHECK(!prepared_funding_turn(&m, 2, 192, 0, 0, &spent) && spent == 192);
        CHECK(prepared_funding_turn(&m, 2, 192, 0, 0,
            &spent) == EINVAL && !spent);
        /* Own leases are gone. A real descriptor+storage writer can now be
         * acquired/released without changing source payload or generations. */
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(!col_rel_source_writer_acquire(f->input, &writer));
        CHECK(!wl_columnar_source_access_writer_release(&writer));
        prepared_case_close(&m, ERANGE);
    }
}

typedef struct {
    prepared_case_t *fixture;
    int step, cancel, drain, ack, query, wrong_session_get;
    wl_columnar_eval_stack_unary_prepared_status_t out;
} prepared_peer_t;

static void *
prepared_peer(void *argument)
{
    prepared_peer_t *p = argument; prepared_case_t *m = p->fixture;
    fixture_t *f = &m->fixture;
    p->query = wl_columnar_eval_stack_unary_prepared_query(&m->prepared,
            &p->out);
    p->step = wl_columnar_eval_stack_unary_prepared_step(&m->prepared);
    p->cancel = wl_columnar_eval_stack_unary_prepared_cancel(&m->prepared,
            ERANGE, f->funding, f->funding_id);
    p->drain = wl_columnar_eval_stack_unary_prepared_drain(&m->prepared,
            f->funding, f->funding_id);
    p->ack = wl_columnar_eval_stack_unary_prepared_ack(&m->prepared, f->funding,
            f->funding_id);
    p->wrong_session_get = wl_columnar_eval_plan_numeric_private_bindings_get(
        &f->record, f->frame, NULL, f->operation, f->operation_id,
        (void *)&m->prepared);
    return NULL;
}

static void
test_prepared_unary_authority_and_query(void)
{
    prepared_case_t m; prepared_case_init(&m, false, 0);
    fixture_t *f = &m.fixture;
    wl_columnar_eval_stack_unary_prepared_t *p = &m.prepared;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    uint64_t init_before, init_after;
    CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
        f->operation_id, &init_before));
    CHECK(wl_columnar_eval_stack_unary_prepared_begin((void *)f->frame,
        f->frame, &f->record) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_begin((void *)&f->record,
        f->frame, &f->record) == EINVAL);
    CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
        f->operation_id, &init_after)
        && init_before == init_after &&
        wl_columnar_eval_stack_cleanup_stack(f->frame) == stack);
    CHECK(!wl_columnar_eval_stack_unary_prepared_begin(p, f->frame,
        &f->record));
    prepared_until(p, WL_COLUMNAR_EVAL_STACK_PREPARED_READY);
    CHECK(col_rel_destroy_checked(f->input) == EBUSY);
    CHECK(col_rel_destroy_checked(p->work.output.rel) == EBUSY);
    const int64_t row[] = {90, 91, 92};
    CHECK(col_rel_append_row(f->input,
        row) == EBUSY && col_rel_append_row(p->work.output.rel, row) == EBUSY);
    uint64_t before, after;
    CHECK(!wl_evaluation_control_test_active_consumed(f->funding, f->funding_id,
        &before));
    wl_columnar_eval_stack_unary_prepared_t copy;
    memcpy(&copy, p, sizeof(copy));
    wl_columnar_eval_stack_unary_prepared_status_t status;
    CHECK(wl_columnar_eval_stack_unary_prepared_query(&copy,
        &status) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_step(&copy) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_cancel(&copy, ERANGE,
        f->funding, f->funding_id) == EINVAL);
    copy.identity = &copy;
    CHECK(wl_columnar_eval_stack_unary_prepared_step(&copy) == EINVAL
        && wl_columnar_eval_stack_unary_prepared_ack(&copy, f->funding,
        f->funding_id) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p, (void *)p) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p,
        (void *)&stack->top) == EINVAL);
    /* The numeric parent is a separate genuine live protocol object. An
     * observation must not turn into mutation of its frozen phase/cursor. */
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p,
        (void *)&f->record.phase) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p, NULL) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p,
        (void *)(UINTPTR_MAX - 1)) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p,
        (void *)((unsigned char *)p + sizeof(*p) - sizeof(void *))) == EINVAL);
    wl_columnar_eval_plan_numeric_attempt_t parent_before;
    memcpy(&parent_before, &f->record, sizeof(parent_before));
    CHECK(wl_columnar_eval_plan_numeric_attempt_status(&f->record, (void *)p,
        f->funding, f->funding_id) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_attempt_status(&f->record,
        (void *)&f->record.phase, f->funding, f->funding_id) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(&f->record,
        f->frame,
        &f->session, f->operation, f->operation_id,
        &f->record.private_bindings) == EINVAL);
    const wl_columnar_eval_private_bindings_t *bindings = NULL;
    CHECK(!wl_columnar_eval_plan_numeric_private_bindings_get(&f->record,
        f->frame,
        &f->session, f->operation, f->operation_id, &bindings)
        && bindings == f->record.private_bindings);
    CHECK(!memcmp(&parent_before, &f->record, sizeof(parent_before)));
    wl_columnar_eval_stack_unary_prepared_t receipt_before;
    memcpy(&receipt_before, p, sizeof(receipt_before));
    wl_col_session_t *wrong_session = calloc(1, sizeof(*wrong_session));
    CHECK(wrong_session);
    for (unsigned wrong = 0; wrong < 2; ++wrong) {
        wl_col_session_t *supplied = wrong ? wrong_session : NULL;
        for (unsigned mismatch = 0; mismatch < 4; ++mismatch) {
            wl_columnar_eval_stack_cleanup_frame_t *supplied_frame = mismatch ==
                1 ? NULL : f->frame;
            wl_evaluation_control_t *supplied_control = mismatch ==
                2 ? NULL : f->operation;
            uint64_t supplied_id = f->operation_id + (mismatch == 3);
            CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(&f->record,
                supplied_frame, supplied, supplied_control, supplied_id,
                (void *)p) == EINVAL);
            CHECK(!memcmp(&receipt_before, p, sizeof(receipt_before))
                && !memcmp(&parent_before, &f->record, sizeof(parent_before)));
            bindings = (const void *)p;
            CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(&f->record,
                supplied_frame, supplied, supplied_control, supplied_id,
                &bindings) == EINVAL
                && bindings == (const void *)p);
        }
    }
    int getter_owner = 0;
    wl_evaluation_control_t *getter_grant = control_new(0, &getter_owner);
    uint64_t getter_id = wl_evaluation_control_active_attempt_id(getter_grant);
    wl_columnar_join_setup_result_t getter_result =
        wl_columnar_eval_plan_numeric_private_bindings_get_funded(&f->record,
            f->frame, &f->session, f->operation, f->operation_id, getter_grant,
            getter_id, &f->record.private_bindings);
    CHECK(getter_result.protocol == EINVAL
        && getter_result.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID);
    getter_result = wl_columnar_eval_plan_numeric_private_bindings_get_funded(
        &f->record, f->frame, wrong_session, f->operation, f->operation_id,
        getter_grant, getter_id, (void *)p);
    CHECK(getter_result.protocol == EINVAL
        && getter_result.outcome == WL_COLUMNAR_JOIN_SETUP_INVALID
        && !memcmp(&receipt_before, p, sizeof(receipt_before))
        && !memcmp(&parent_before, &f->record, sizeof(parent_before)));
    CHECK(!wl_evaluation_control_finish(getter_grant, &getter_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(getter_grant, &getter_owner));
    wl_evaluation_control_release(getter_grant);
    free(wrong_session);
    /* Invalid owner tuples refuse before output normalization, including
     * absent and nonself owners that cannot prove external output storage. */
    bindings = (const void *)p;
    CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(NULL, NULL, NULL,
        NULL, 0, &bindings) == EINVAL && bindings == (const void *)p);
    wl_columnar_eval_plan_numeric_attempt_t nonself;
    memcpy(&nonself, &f->record, sizeof(nonself));
    CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(&nonself, NULL,
        NULL, NULL, 0, &bindings) == EINVAL && bindings == (const void *)p);
    CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(NULL, NULL, NULL,
        NULL, 0, (void *)p) == EINVAL);
    CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(&nonself, NULL,
        NULL, NULL, 0, (void *)p) == EINVAL);
    CHECK(!memcmp(&receipt_before, p, sizeof(receipt_before)));
    prepared_peer_t peer = {.fixture = &m};
    memset(&peer.out, 0x3a, sizeof(peer.out));
    unsigned char poison[sizeof(peer.out)];
    memcpy(poison, &peer.out, sizeof(poison));
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, prepared_peer,
        &peer) && !wl_thread_join(&thread));
    CHECK(peer.step == EINVAL && peer.cancel == EINVAL && peer.drain == EINVAL
        && peer.ack == EINVAL && peer.query == EINVAL && !memcmp(&peer.out,
        poison, sizeof(poison)));
    CHECK(peer.wrong_session_get == EINVAL && !memcmp(&receipt_before, p,
        sizeof(receipt_before)));
    CHECK(!wl_evaluation_control_test_active_consumed(f->funding, f->funding_id,
        &after) && before == after);
    CHECK(p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_READY &&
        !p->result_saved);
    prepared_case_close(&m, ERANGE);
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p,
        &status) == 0 && status.first_result == ERANGE);
    memcpy(&copy, p, sizeof(copy));
    CHECK(wl_columnar_eval_stack_unary_prepared_query(&copy,
        &status) == EINVAL);
}

static void
prepared_abandon_ready(prepared_case_t *m)
{
    fixture_t *f = &m->fixture;
    CHECK(!wl_columnar_eval_stack_unary_prepared_begin(&m->prepared, f->frame,
        &f->record));
    prepared_until(&m->prepared, WL_COLUMNAR_EVAL_STACK_PREPARED_READY);
    CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&m->prepared, 0,
        f->funding, f->funding_id));
    prepared_drain_ack(f, &m->prepared);
}

static struct {
    wl_evaluation_control_t *control;
    const void *owner;
    uint64_t successor;
    unsigned hits;
} prepared_generation;

static void
prepared_generation_checkpoint(wl_columnar_eval_stack_unary_prepared_t *p)
{
    CHECK(p->busy && !prepared_generation.hits++);
    CHECK(!wl_evaluation_control_finish(prepared_generation.control,
        prepared_generation.owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(prepared_generation.control));
    CHECK(!wl_evaluation_control_begin(prepared_generation.control,
        prepared_generation.owner));
    prepared_generation.successor =
        wl_evaluation_control_active_attempt_id(prepared_generation.control);
}

static void
test_prepared_unary_generation_checkpoint(void)
{
    for (unsigned action = 0; action < 4; ++action) {
        prepared_case_t m; prepared_case_init(&m, false, 0);
        fixture_t *f = &m.fixture;
        wl_columnar_eval_stack_unary_prepared_t *p = &m.prepared;
        CHECK(!wl_columnar_eval_stack_unary_prepared_begin(p, f->frame,
            &f->record));
        if (action == 3) {
            prepared_until(p, WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD);
            CHECK(!wl_columnar_eval_stack_unary_prepared_step(p));
            CHECK(p->work.old_reader.acquire_stage == 1 && !p->old_held);
        } else {
            prepared_until(p, WL_COLUMNAR_EVAL_STACK_PREPARED_READY);
            if (action) CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(p,
                    ERANGE, f->funding, f->funding_id));
            if (action == 2)
                while (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY)
                    CHECK(!wl_columnar_eval_stack_unary_prepared_drain(p,
                        f->funding, f->funding_id));
        }
        wl_columnar_eval_stack_unary_prepared_t saved;
        memcpy(&saved, p, sizeof(saved));
        int owner = 0;
        wl_evaluation_control_t *funding = action ==
            3 ? f->operation : control_new(0, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        memset(&prepared_generation, 0, sizeof(prepared_generation));
        prepared_generation.control = funding;
        prepared_generation.owner = action ==
            3 ? (const void *)&f->operation_owner : &owner;
        wl_columnar_eval_stack_test_unary_prepared_checkpoint =
            prepared_generation_checkpoint;
        int rc = action == 0 ? wl_columnar_eval_stack_unary_prepared_cancel(p,
                ERANGE, funding, id)
            : action == 1 ? wl_columnar_eval_stack_unary_prepared_drain(p,
                funding, id)
            : action == 2 ? wl_columnar_eval_stack_unary_prepared_ack(p,
                funding, id)
            : wl_columnar_eval_stack_unary_prepared_step(p);
        wl_columnar_eval_stack_test_unary_prepared_checkpoint = NULL;
        CHECK(rc == EINVAL && prepared_generation.hits == 1 &&
            prepared_generation.successor != id);
        CHECK(p->phase == saved.phase && p->frame == saved.frame &&
            p->control_ref == saved.control_ref
            && p->old_held == saved.old_held &&
            p->output_held == saved.output_held
            && !memcmp(&p->work, &saved.work, sizeof(p->work)));
        if (action == 3) CHECK(p->result_saved && p->first_result == EINVAL);
        else CHECK(p->result_saved == saved.result_saved &&
                p->first_result == saved.first_result);
        uint64_t spent;
        CHECK(!wl_evaluation_control_test_active_consumed(funding,
            prepared_generation.successor, &spent) && !spent);
        if (action != 3) {
            CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
            CHECK(!wl_evaluation_control_detach(funding, &owner));
            wl_evaluation_control_release(funding);
        }
        prepared_case_close(&m, action == 3 ? EINVAL : ERANGE);
    }
}

static void
test_prepared_unary_entry_aliases(void)
{
    for (unsigned alias = 0; alias < 3; ++alias) {
        prepared_case_t m = {0};
        fixture_t *f = &m.fixture; fixture_init(f, 3, 3, 0);
        m.governor = join_governor(f, 1u << 24);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
        col_rel_t *lower = NULL, *result_relation = NULL;
        if (alias == 1) {
            eval_entry_t original = eval_stack_pop(stack);
            lower = source_new(3, 2);
            CHECK(!eval_stack_push(stack, lower, true));
            CHECK(!eval_stack_push(stack, original.rel, original.owned));
        }
        m.op = (wl_plan_op_t){.op = WL_PLAN_OP_MAP, .project_count = 1,
                              .project_indices = &m.projection};
        m.plan = (wl_plan_relation_t){.ops = &m.op, .op_count = 1};
        CHECK(!wl_columnar_eval_plan_numeric_stack_attempt_begin(&f->record,
            &m.plan, 0, 1, f->frame, &f->session, stack->top, f->operation,
            f->operation_id, f->funding, f->funding_id));
        if (alias == 2) {
            result_relation = source_new(3, 1);
            eval_entry_t *result =
                wl_columnar_eval_stack_cleanup_result(f->frame);
            CHECK(result); result->rel = result_relation; result->owned = true;
            result->kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
        }
        CHECK(!wl_columnar_eval_stack_unary_begin_attempt_owned(f->frame,
            f->operation, f->operation_id, &f->record));
        col_rel_t **slot = NULL;
        CHECK(!wl_columnar_eval_stack_unary_output_owned(f->frame, &slot,
            &f->record));
        *slot = alias == 0 ? f->input : alias == 1 ? lower : result_relation;
        CHECK(!wl_columnar_eval_stack_unary_prepared_begin(&m.prepared,
            f->frame, &f->record));
        int rc = 0;
        for (unsigned turn = 0; !rc; ++turn) {
            CHECK(turn < 10);
            rc = wl_columnar_eval_stack_unary_prepared_step(&m.prepared);
        }
        CHECK(rc == EINVAL && m.prepared.result_saved &&
            m.prepared.first_result == EINVAL
            && !m.prepared.old_held && !m.prepared.output_held);
        CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&m.prepared, EINVAL,
            f->funding, f->funding_id));
        prepared_drain_ack(f, &m.prepared);
        stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
        CHECK(stack && stack->items[stack->top - 1].rel == f->input);
        if (alias == 1) CHECK(stack->items[0].rel == lower &&
                stack->items[0].owned);
        if (alias == 2) {
            eval_entry_t *result =
                wl_columnar_eval_stack_cleanup_result(f->frame);
            CHECK(result && result->rel == result_relation && result->owned);
            CHECK(!eval_entry_dispose(result));
        }
        prepared_case_close(&m, EINVAL);
    }
}

static void
test_prepared_unary_descriptor_only_cleanup(void)
{
    prepared_case_t m; prepared_case_init(&m, false, 0);
    fixture_t *f = &m.fixture;
    wl_columnar_eval_stack_unary_prepared_t *p = &m.prepared;
    CHECK(!wl_columnar_eval_stack_unary_prepared_begin(p, f->frame,
        &f->record));
    prepared_until(p, WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD);
    CHECK(!wl_columnar_eval_stack_unary_prepared_step(p));
    CHECK(p->work.old_reader.acquire_stage == 1
        && p->work.old_reader.owner == &f->input->descriptor_access
        && !p->work.old_reader.secondary_owner && !p->old_held
        && wl_columnar_source_access_gate_busy(&f->input->descriptor_access)
        && !wl_columnar_source_access_gate_busy(&f->input->source_access));
    CHECK(col_rel_destroy_checked(f->input) == EBUSY);
    const int64_t row[] = {90, 91, 92};
    CHECK(col_rel_append_row(f->input, row) == EBUSY);
    CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(p, ERANGE, f->funding,
        f->funding_id));
    prepared_drain_ack(f, p);
    CHECK(!wl_columnar_source_access_gate_busy(&f->input->descriptor_access));
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(!col_rel_source_writer_acquire(f->input, &writer));
    CHECK(!wl_columnar_source_access_writer_release(&writer));
    prepared_case_close(&m, ERANGE);
}

static void
test_prepared_unary_static_bindings_status(void)
{
    fixture_t f; fixture_init(&f, 3, 3, 0);
    wl_columnar_memory_governor_ref_t *governor = join_governor(&f, 1u << 24);
    f.session.base.evaluation_control = f.operation;
    uint32_t projection = 0;
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1,
                       .project_indices = &projection};
    wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
    const char *names[] = {"a", "b", "c", "d"};
    col_rel_t *relations[] = {f.input, f.input, f.input, f.input};
    wl_columnar_eval_private_bindings_t bindings = {0};
    CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, &f.session, &plan,
        f.operation, f.operation_id, names, relations, 4));
    CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(&f.record,
        &plan, 0, 1, f.frame, &f.session, 1, f.operation, f.operation_id,
        f.funding, f.funding_id, &bindings));
    CHECK(!wl_columnar_eval_stack_unary_begin_attempt_owned(f.frame,
        f.operation, f.operation_id, &f.record));
    col_rel_t **slot = NULL;
    CHECK(!wl_columnar_eval_stack_unary_output_owned(f.frame, &slot,
        &f.record));
    CHECK(!wl_columnar_relation_new_auto_governed_attempt_controlled(
            "bound prepared output", 1, 8, governor, slot, f.operation,
            f.operation_id));
    int64_t row = 10; CHECK(!col_rel_append_row(*slot, &row));
    wl_columnar_eval_stack_unary_prepared_t prepared = {0};
    CHECK(!wl_columnar_eval_stack_unary_prepared_begin(&prepared, f.frame,
        &f.record));
    prepared_until(&prepared, WL_COLUMNAR_EVAL_STACK_PREPARED_READY);
    wl_columnar_eval_plan_numeric_attempt_t saved;
    memcpy(&saved, &f.record, sizeof(saved));
    CHECK(f.record.private_bindings == &bindings && bindings.count == 4);
    CHECK(wl_columnar_eval_plan_numeric_private_bindings_get(&f.record, f.frame,
        &f.session, f.operation, f.operation_id,
        &f.record.private_bindings) == EINVAL);
    const wl_columnar_eval_private_bindings_t *copied = NULL;
    CHECK(!wl_columnar_eval_plan_numeric_private_bindings_get(&f.record,
        f.frame,
        &f.session, f.operation, f.operation_id,
        &copied) && copied == &bindings);
    uint64_t before, after;
    CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
        f.operation_id, &before));
    for (unsigned exact = 0; exact < 2; ++exact) {
        int owner = 0;
        wl_evaluation_control_t *funding = control_new(exact ? 384 : 383,
                &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        wl_columnar_eval_plan_numeric_status_t status;
        memset(&status, 0x39, sizeof(status));
        unsigned char poison[sizeof(status)];
        memcpy(poison, &status, sizeof(poison));
        int rc =
            wl_columnar_eval_plan_numeric_composed_attempt_status(&f.record,
                &status, funding, id);
        CHECK(rc == (exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
        if (exact) CHECK(status.phase == saved.phase && !status.completed
                && !status.semantic_result && !status.protocol_result &&
                !status.retained);
        else CHECK(!memcmp(poison, &status, sizeof(poison)));
        (void)wl_evaluation_control_finish(funding, &owner, 0, 0);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(funding,
            &report) && report.consumed == (exact ? 384u : 0u));
        CHECK(!wl_evaluation_control_detach(funding, &owner));
        wl_evaluation_control_release(funding);
    }
    CHECK(!memcmp(&saved, &f.record, sizeof(saved)));
    CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
        f.operation_id, &after) && before == after);
    CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&prepared, 0, f.funding,
        f.funding_id));
    prepared_drain_ack(&f, &prepared);
    prepared_finish_existing(&f, 0);
    composed_run(&f); CHECK(!f.record.semantic_result);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack && stack->top == 1 && stack->items[0].rel->ncols == 1
        && stack->items[0].rel->nrows == 3);
    for (unsigned r = 0; r < 3;
        ++r) CHECK(stack->items[0].rel->columns[0][r] == (int64_t)r);
    composed_close(&f);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

static void
test_prepared_unary_acked_reuse(void)
{
    for (unsigned owned = 0; owned < 2; ++owned) {
        prepared_case_t base; prepared_case_init(&base, owned, 0);
        prepared_abandon_ready(&base);
        uint64_t before;
        CHECK(!wl_evaluation_control_test_active_consumed(
                base.fixture.operation, base.fixture.operation_id, &before));
        prepared_case_close(&base, ERANGE);
        for (unsigned exact = 0; exact < 2; ++exact) {
            prepared_case_t m;
            prepared_case_init(&m, owned, before + 256 - !exact);
            prepared_abandon_ready(&m);
            wl_columnar_eval_stack_unary_prepared_t saved;
            memcpy(&saved, &m.prepared, sizeof(saved));
            int rc = wl_columnar_eval_stack_unary_prepared_begin(&m.prepared,
                    m.fixture.frame, &m.fixture.record);
            CHECK(rc == (exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
            if (!exact) CHECK(!memcmp(&saved, &m.prepared, sizeof(saved)));
            else {
                CHECK(m.prepared.phase ==
                    WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD &&
                    !m.prepared.result_saved);
                CHECK(wl_evaluation_control_charge_attempt(m.fixture.operation,
                    m.fixture.operation_id, 1)
                    == WL_EVALUATION_CONTROL_WORK_LIMIT);
            }
            uint64_t work;
            CHECK(!wl_evaluation_control_test_active_consumed(
                    m.fixture.operation, m.fixture.operation_id, &work)
                && work == before + (exact ? 256u : 0u));
            prepared_case_close(&m, WL_EVALUATION_CONTROL_WORK_LIMIT);
        }
    }
}

static void
prepared_join_until_decision(fixture_t *f, wl_columnar_filter_context_t **ctx,
    uint32_t cursor, bool pending)
{
    for (unsigned turn = 0; ; ++turn) {
        CHECK(turn < 10000 &&
            f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        if (f->record.cursor == cursor && f->record.phase == (pending
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING :
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD)){
            CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, ctx,
                &f->record));
            if ((*ctx)->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE) return;
        }
        engine_dispatch(f); ++f->witnesses;
    }
}

static void
prepared_join_drain_ack(wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    for (unsigned turn = 0; ; ++turn) {
        wl_columnar_eval_stack_unary_prepared_status_t status;
        CHECK(!wl_columnar_eval_stack_unary_prepared_query(p, &status));
        if (status.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY) break;
        CHECK(turn < 1000);
        int rc = wl_columnar_eval_stack_unary_prepared_drain(p, funding, id);
        CHECK(!rc || rc == EAGAIN);
    }
    CHECK(!wl_columnar_eval_stack_unary_prepared_ack(p, funding, id));
}

static void
test_prepared_join_chained_delta(void)
{
    uint32_t projection[] = {0};
    for (unsigned owned = 0; owned < 2; ++owned)
        for (unsigned segmented = 0; segmented < 2; ++segmented)
            for (unsigned pending = 0; pending < 2; ++pending)
                for (unsigned projected = 0; projected < 4; ++projected)
                    for (unsigned action = 0; action < 3; ++action) {
                        fixture_t f; source_fixture(&f, 1, 7, 0);
                        wl_columnar_memory_governor_ref_t *g = join_governor(&f,
                                1u << 24);
                        col_rel_t *delta = source_new(1, 2),
                            *right = source_new(1, 3),
                            *rd = source_new(1, 1), *third = source_new(1, 4),
                            *td = source_new(1, 1),
                            *prefix = source_new(1, 1);
                        CHECK(!wl_columnar_relation_rename_checked(delta,
                            "$d$input"));
                        CHECK(!wl_columnar_relation_rename_checked(right,
                            "right"));
                        CHECK(!wl_columnar_relation_rename_checked(rd,
                            "$d$right"));
                        CHECK(!wl_columnar_relation_rename_checked(third,
                            "third"));
                        CHECK(!wl_columnar_relation_rename_checked(td,
                            "$d$third"));
                        CHECK(!wl_columnar_relation_rename_checked(prefix,
                            "prefix"));
                        col_rel_t *registry[] = {f.input, delta, right, rd,
                                                 third, td, prefix};
                        unsigned cursor = 1 + owned;
                        wl_plan_op_t ops[4] = {{.op = WL_PLAN_OP_VARIABLE,
                                                .relation_name = "input"}};
                        if (owned) ops[1] =
                                (wl_plan_op_t){.op = WL_PLAN_OP_JOIN,
                                               .right_relation = "prefix"};
                        ops[cursor] = (wl_plan_op_t){.op = WL_PLAN_OP_JOIN,
                                                     .right_relation = "right"};
                        ops[cursor + 1] = (wl_plan_op_t){.op = WL_PLAN_OP_JOIN,
                                                         .right_relation =
                                                             "third"};
                        for (unsigned j = 0; j < 2; ++j)
                            if (projected & (1u << j)) {
                                ops[cursor + j].project_count = 1;
                                ops[cursor + j].project_indices = projection;
                            }
                        wl_plan_relation_t plan = {.ops = ops,
                                                   .op_count = 3 + owned};
                        source_begin(&f, &plan, registry, 7);
                        if (segmented) {
                            /* Configure genuine input metadata before the next unary
                             * claims it. Ownership comes from the real source/prefix
                             * operator, never from editing its owned flag. */
                            join_until_ready(&f, cursor);
                            eval_stack_t *input =
                                wl_columnar_eval_stack_cleanup_stack(f.frame);
                            CHECK(input && input->top == 1 &&
                                !input->items[0].seg_boundaries);
                            uint32_t *boundaries = malloc(2 *
                                    sizeof(*boundaries)); CHECK(boundaries);
                            boundaries[0] = 0;
                            boundaries[1] = input->items[0].rel->nrows;
                            input->items[0].seg_boundaries = boundaries;
                            input->items[0].seg_count = 1;
                        }
                        wl_columnar_filter_context_t *ctx = NULL;
                        prepared_join_until_decision(&f, &ctx, cursor, pending);
                        CHECK(f.record.cursor == cursor &&
                            f.record.phase == (pending
                ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING :
                            WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD)
                            && ctx->join_operation == &ops[cursor] &&
                            !ctx->busy);
                        eval_stack_t *stack =
                            wl_columnar_eval_stack_cleanup_stack(f.frame);
                        eval_entry_t old = stack->items[0];
                        CHECK(old.rel != f.input && old.rel->nrows == 2 &&
                            old.owned == (owned != 0));
                        col_rel_t *output = ctx->private_output;
                        bool delta_result = !(projected & 1);
                        CHECK(output && ctx->output == output &&
                            output->nrows == 6
                            && ctx->join_result_is_delta == delta_result);
                        wl_columnar_eval_plan_numeric_attempt_t parent_before;
                        memcpy(&parent_before, &f.record,
                            sizeof(parent_before));
                        uint64_t pending_work_before, pending_work_after;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f.funding, f.funding_id, &pending_work_before));
                        int pending_result = f.pending_protocol;
                        int cleanup_owner = 0;
                        wl_evaluation_control_t *cleanup = control_new(0,
                                &cleanup_owner);
                        uint64_t cleanup_id =
                            wl_evaluation_control_active_attempt_id(cleanup);
                        wl_columnar_eval_stack_unary_prepared_t p = {0};
                        CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(
                                &p, f.frame, &f.record));
                        CHECK(!p.join_mode &&
                            p.phase ==
                            WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD);
                        CHECK(!wl_columnar_eval_stack_unary_prepared_step(&p)
                            && p.join_mode &&
                            p.phase ==
                            WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_STATE);
                        CHECK(!wl_columnar_eval_stack_unary_prepared_step(&p));
                        prepared_until(&p,
                            WL_COLUMNAR_EVAL_STACK_PREPARED_READY);
                        CHECK(p.join_mode &&
                            p.publication_is_delta == delta_result
                            && p.old_held && p.output_held && !p.scalar_ready
                            && !memcmp(&parent_before, &f.record,
                            sizeof(parent_before)));
                        if (!action) {
                            prepared_until(&p,
                                WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
                            wl_columnar_eval_stack_unary_commit_prepared_nofail(
                                &p);
                            CHECK(ctx->committed &&
                                ctx->stage == WL_COLUMNAR_FILTER_STAGE_CONTROL
                                && !ctx->execution_result &&
                                !ctx->semantic_result
                                && !ctx->join_decision_pending &&
                                !ctx->join_operation
                                && !ctx->join_result_is_delta &&
                                ctx->output == output
                                && ctx->private_output == output);
                            wl_columnar_eval_stack_test_unary_observation_t
                                retired;
                            CHECK(!wl_columnar_eval_stack_test_unary_observe(
                                    f.frame, &f.record, &retired));
                            CHECK(retired.committed &&
                                retired.relation == old.rel
                                && retired.owned == old.owned &&
                                retired.is_delta == old.is_delta
                                && retired.segments == old.seg_boundaries &&
                                retired.segment_count == old.seg_count
                                && retired.continuation == old.continuation &&
                                retired.kind == (unsigned)old.kind);
                            CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(
                                    &p, ERANGE, cleanup, cleanup_id));
                            CHECK(p.committed && !p.first_result);
                        } else {
                            CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(
                                    &p, action == 1 ? 0 : ERANGE,
                                    cleanup, cleanup_id));
                            CHECK(!ctx->committed &&
                                ctx->stage ==
                                WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
                                && ctx->join_decision_pending &&
                                ctx->join_operation == &ops[cursor]
                                && ctx->join_result_is_delta == delta_result);
                            CHECK(ctx->execution_result ==
                                (action == 1 ? 0 : ERANGE)
                                && ctx->semantic_result ==
                                (action == 1 ? 0 : ERANGE));
                        }
                        prepared_join_drain_ack(&p, cleanup, cleanup_id);
                        CHECK(!memcmp(&parent_before, &f.record,
                            sizeof(parent_before))
                            && f.pending_protocol == pending_result
                            && !wl_evaluation_control_test_active_consumed(
                                f.funding, f.funding_id, &pending_work_after)
                            && pending_work_before == pending_work_after);
                        CHECK(!wl_evaluation_control_finish(cleanup,
                            &cleanup_owner, 0, 0));
                        CHECK(!wl_evaluation_control_detach(cleanup,
                            &cleanup_owner));
                        wl_evaluation_control_release(cleanup);
                        if (pending) {
                            /* Consume the real prior YIELD, not a fabricated prepared
                             * status, before the parent dispatches its next child turn. */
                            engine_dispatch(&f); ++f.witnesses;
                            CHECK(f.record.phase ==
                                WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
                                && f.record.cursor == cursor);
                        }
                        run(&f);
                        stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
                        CHECK(stack && stack->top == 1);
                        if (action == 2) {
                            CHECK(f.record.semantic_result == ERANGE &&
                                f.record.completed == owned
                                && stack->items[0].rel == old.rel &&
                                stack->items[0].is_delta
                                && stack->items[0].seg_boundaries ==
                                old.seg_boundaries
                                && stack->items[0].seg_count == old.seg_count
                                && old.rel->nrows == 2 &&
                                old.rel->columns[0][0] == 0
                                && old.rel->columns[0][1] == 1);
                        } else {
                            CHECK(!f.record.semantic_result &&
                                f.record.completed == 2 + owned
                                && stack->items[0].rel->nrows ==
                                (projected & 1 ? 6u : 24u)
                                && stack->items[0].is_delta ==
                                !(projected & 2));
                            join_raw_parity(&f, &plan, false);
                        }
                        fixture_close(&f);
                        col_rel_destroy(delta); col_rel_destroy(right);
                        col_rel_destroy(rd);
                        col_rel_destroy(third); col_rel_destroy(td);
                        col_rel_destroy(prefix);
                        CHECK(!wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(g)));
                        wl_columnar_memory_governor_ref_release(g);
                        wl_columnar_eval_stack_unary_prepared_status_t history;
                        CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p,
                            &history)
                            && history.phase ==
                            WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED);
                    }
}

typedef struct {
    fixture_t f;
    wl_columnar_memory_governor_ref_t *governor;
    col_rel_t *right, *prefix, *registry[2];
    wl_plan_op_t ops[2];
    wl_plan_relation_t plan;
    wl_columnar_filter_context_t *context;
    unsigned cursor;
} prepared_join_case_t;

static void
prepared_join_case_init_options(prepared_join_case_t *m, bool owned,
    bool pending,
    uint64_t limit, unsigned flags)
{
    memset(m, 0, sizeof(*m));
    fixture_init(&m->f, flags & 8 ? 64 : 2, 3, limit);
    m->governor = join_governor(&m->f, 1u << 24);
    m->right = source_new(1, 2);
    CHECK(!wl_columnar_relation_rename_checked(m->right, "right"));
    m->registry[0] = m->right;
    if (owned) {
        m->prefix = source_new(1, 1);
        CHECK(!wl_columnar_relation_rename_checked(m->prefix, "prefix"));
        m->registry[1] = m->prefix;
        m->ops[0] = (wl_plan_op_t){.op = WL_PLAN_OP_JOIN,
                                   .right_relation = "prefix",
                                   .delta_mode = WL_DELTA_FORCE_FULL};
    }
    m->cursor = owned;
    m->ops[m->cursor] = (wl_plan_op_t){.op = WL_PLAN_OP_JOIN,
                                       .right_relation = "right",
                                       .delta_mode = WL_DELTA_FORCE_FULL};
    m->plan = (wl_plan_relation_t){.ops = m->ops, .op_count = 1 + owned};
    m->f.session.rels = m->registry;
    m->f.session.nrels = m->f.session.rel_cap = 1 + owned;
    fixture_begin(&m->f, &m->plan, 0, m->plan.op_count);
    if (flags & 7) {
        join_until_ready(&m->f, m->cursor);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(m->f.frame);
        CHECK(stack && stack->top == 1 && !stack->items[0].seg_boundaries);
        if (flags & 2) {
            col_rel_t *target = stack->items[0].rel;
            CHECK(stack->items[0].owned && !target->timestamps);
            CHECK(!col_rel_enable_timestamps(target));
            for (uint32_t r = 0; r < target->nrows; ++r) {
                target->timestamps[r].stratum = 2;
                target->timestamps[r].worker = 1;
                target->timestamps[r].iteration = r;
                target->timestamps[r].multiplicity = 1;
            }
        }
        if (flags & 1) {
            uint32_t *segments = malloc(2 * sizeof(*segments)); CHECK(segments);
            segments[0] = 0; segments[1] = stack->items[0].rel->nrows;
            stack->items[0].seg_boundaries = segments;
            stack->items[0].seg_count = 1;
        }
        if (flags & 4) {
            col_rel_t *target = stack->items[0].rel;
            CHECK(!target->mem_ledger);
            target->mem_ledger = &m->f.session.mem_ledger;
            wl_mem_ledger_alloc(target->mem_ledger, WL_MEM_SUBSYS_RELATION,
                col_rel_owned_ledger_bytes(target));
            target->ledger_ts_bytes = col_rel_timestamp_ledger_bytes(target);
            wl_mem_ledger_alloc(target->mem_ledger, WL_MEM_SUBSYS_TIMESTAMP,
                target->ledger_ts_bytes);
        }
    }
    prepared_join_until_decision(&m->f, &m->context, m->cursor, pending);
}

static void
prepared_join_case_init(prepared_join_case_t *m, bool owned, bool pending,
    uint64_t limit)
{
    prepared_join_case_init_options(m, owned, pending, limit, false);
}

static void
prepared_join_case_close(prepared_join_case_t *m)
{
    fixture_close(&m->f);
    col_rel_destroy(m->right);
    if (m->prefix) col_rel_destroy(m->prefix);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            m->governor)));
    wl_columnar_memory_governor_ref_release(m->governor);
}

static int
prepared_join_cancel_turn(wl_columnar_eval_stack_unary_prepared_t *p,
    uint64_t limit, int result, uint64_t *spent)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    int rc = wl_columnar_eval_stack_unary_prepared_cancel(p, result, funding,
            id);
    (void)wl_evaluation_control_finish(funding, &owner, 0, 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    *spent = report.consumed;
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static void
test_prepared_join_early_stop_turnover(void)
{
    for (unsigned owned = 0; owned < 2; ++owned)
        for (unsigned pending = 0; pending < 2; ++pending) {
            prepared_join_case_t baseline;
            prepared_join_case_init(&baseline, owned, pending, 0);
            uint64_t before_init;
            CHECK(!wl_evaluation_control_test_active_consumed(
                    baseline.f.operation,
                    baseline.f.operation_id, &before_init));
            run(&baseline.f); CHECK(!baseline.f.record.semantic_result);
            prepared_join_case_close(&baseline);
            for (unsigned work = 0; work < 2; ++work) {
                prepared_join_case_t m;
                prepared_join_case_init(&m, owned, pending,
                    work ? before_init + 256 + 191 : 0);
                fixture_t *f = &m.f;
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(f->frame);
                eval_entry_t old = stack->items[0];
                CHECK(old.owned == (owned != 0));
                col_relation_snapshot_t old_snapshot =
                    wl_columnar_relation_snapshot(old.rel);
                wl_columnar_filter_context_t context_before;
                wl_columnar_eval_plan_numeric_attempt_t parent_before;
                memcpy(&context_before, m.context, sizeof(context_before));
                memcpy(&parent_before, &f->record, sizeof(parent_before));
                int pending_result = f->pending_protocol;
                uint64_t pending_work;
                CHECK(!wl_evaluation_control_test_active_consumed(f->funding,
                    f->funding_id, &pending_work));
                wl_columnar_eval_stack_unary_prepared_t p = {0};
                CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(&p,
                    f->frame, &f->record));
                if (!work) wl_evaluation_control_request_cancel(f->operation);
                int stop =
                    work ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                    WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(wl_columnar_eval_stack_unary_prepared_step(&p) == stop
                    && !p.join_mode &&
                    p.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD
                    && p.result_saved && p.first_result == stop);
                CHECK(wl_evaluation_control_finish(f->operation,
                    &f->operation_owner, 0, 0) == stop);
                CHECK(!wl_evaluation_control_reset(f->operation) &&
                    !wl_evaluation_control_configure(f->operation, 0));
                CHECK(!wl_evaluation_control_begin(f->operation,
                    &f->operation_owner));
                uint64_t successor =
                    wl_evaluation_control_active_attempt_id(f->operation);
                CHECK(successor != f->operation_id);
                wl_evaluation_control_report_t original_before, original_after;
                CHECK(!wl_evaluation_control_report(f->operation,
                    &original_before));
                wl_columnar_eval_stack_unary_prepared_t saved;
                memcpy(&saved, &p, sizeof(saved));
                uint64_t spent;
                CHECK(prepared_join_cancel_turn(&p, 191, ERANGE,
                    &spent) == WL_EVALUATION_CONTROL_WORK_LIMIT
                    && !spent && !memcmp(&saved, &p, sizeof(saved))
                    && !memcmp(&context_before, m.context,
                    sizeof(context_before)));
                CHECK(!prepared_join_cancel_turn(&p, 192, ERANGE,
                    &spent) && spent == 192);
                CHECK(p.join_mode && p.first_result == stop
                    && m.context->execution_result == stop &&
                    m.context->semantic_result == stop
                    && m.context->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
                    && !m.context->committed &&
                    m.context->join_decision_pending);
                int cleanup_owner = 0;
                wl_evaluation_control_t *cleanup = control_new(0,
                        &cleanup_owner);
                uint64_t cleanup_id =
                    wl_evaluation_control_active_attempt_id(cleanup);
                prepared_join_drain_ack(&p, cleanup, cleanup_id);
                CHECK(!wl_evaluation_control_finish(cleanup, &cleanup_owner, 0,
                    0));
                CHECK(!wl_evaluation_control_detach(cleanup, &cleanup_owner));
                wl_evaluation_control_release(cleanup);
                uint64_t after;
                CHECK(!memcmp(&parent_before, &f->record, sizeof(parent_before))
                    && f->pending_protocol == pending_result
                    && !wl_evaluation_control_test_active_consumed(f->funding,
                    f->funding_id, &after)
                    && after == pending_work);
                run(f);
                CHECK(f->record.semantic_result == stop &&
                    f->record.completed == owned);
                stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
                CHECK(stack && stack->top == 1 && stack->items[0].rel == old.rel
                    && stack->items[0].owned == old.owned
                    && stack->items[0].is_delta == old.is_delta
                    && wl_columnar_relation_snapshot_equal(old_snapshot,
                    wl_columnar_relation_snapshot(old.rel))
                    && old.rel->nrows == 3 && old.rel->columns[0][2] == 2);
                CHECK(!wl_evaluation_control_test_active_consumed(f->operation,
                    successor, &after) && !after);
                CHECK(!wl_evaluation_control_report(f->operation,
                    &original_after)
                    && metadata_reports_equal(&original_before,
                    &original_after));
                prepared_join_case_close(&m);
                wl_columnar_eval_stack_unary_prepared_status_t history;
                CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &history)
                    && history.first_result == stop && !history.committed);
            }
        }
}

/* Literal contract fees, independent of the implementation fee table. */
static void
test_prepared_join_original_boundaries(void)
{
    static const prepared_boundary_t boundaries[] = {
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD, 0, 192},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_STATE, 0, 192},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH0, 0, 128},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH1, 0, 128},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH2, 0, 128},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_HEAD, 0, 192},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_STATE, 0, 192},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH0, 0, 128},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH1, 0, 128},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH2, 0, 128}
    };
    for (unsigned owned = 0; owned < 2; ++owned)
        for (unsigned pending = 0; pending < 2; ++pending)
            for (unsigned b =
                0; b <= sizeof(boundaries) / sizeof(boundaries[0]);
                ++b){
                bool init = b == sizeof(boundaries) / sizeof(boundaries[0]);
                uint64_t fee = init ? 256 : boundaries[b].fee;
                prepared_join_case_t baseline;
                prepared_join_case_init(&baseline, owned, pending, 0);
                wl_columnar_eval_stack_unary_prepared_t p = {0};
                if (!init) {
                    CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(&p,
                        baseline.f.frame, &baseline.f.record));
                    prepared_until(&p, boundaries[b].phase);
                }
                uint64_t before, spent;
                CHECK(!wl_evaluation_control_test_active_consumed(
                        baseline.f.operation,
                        baseline.f.operation_id, &before));
                if (!init) {
                    CHECK(!prepared_join_cancel_turn(&p, 0, ERANGE, &spent));
                    int owner = 0;
                    wl_evaluation_control_t *funding = control_new(0, &owner);
                    prepared_join_drain_ack(&p, funding,
                        wl_evaluation_control_active_attempt_id(funding));
                    CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
                    CHECK(!wl_evaluation_control_detach(funding, &owner));
                    wl_evaluation_control_release(funding);
                }
                run(&baseline.f);
                CHECK(baseline.f.record.semantic_result == (init ? 0 : ERANGE));
                prepared_join_case_close(&baseline);
                for (unsigned exact = 0; exact < 2; ++exact) {
                    prepared_join_case_t m;
                    prepared_join_case_init(&m, owned, pending,
                        before + fee - !exact);
                    fixture_t *f = &m.f;
                    wl_columnar_eval_stack_unary_prepared_t receipt = {0};
                    if (!init) {
                        CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(
                                &receipt, f->frame, &f->record));
                        prepared_until(&receipt, boundaries[b].phase);
                    }
                    int rc = init
                    ? wl_columnar_eval_stack_unary_prepared_join_begin(
                        &receipt, f->frame, &f->record)
                    : wl_columnar_eval_stack_unary_prepared_step(&receipt);
                    CHECK(rc == (exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f->operation,
                            f->operation_id,
                            &spent) && spent == before + (exact ? fee : 0));
                    if (exact) CHECK(wl_evaluation_control_charge_attempt(
                                f->operation,
                                f->operation_id,
                                1) == WL_EVALUATION_CONTROL_WORK_LIMIT);
                    if (receipt.identity) {
                        CHECK(!prepared_join_cancel_turn(&receipt, 0,
                            WL_EVALUATION_CONTROL_WORK_LIMIT, &spent));
                        int owner = 0;
                        wl_evaluation_control_t *funding = control_new(0,
                                &owner);
                        prepared_join_drain_ack(&receipt, funding,
                            wl_evaluation_control_active_attempt_id(funding));
                        CHECK(!wl_evaluation_control_finish(funding, &owner, 0,
                            0));
                        CHECK(!wl_evaluation_control_detach(funding, &owner));
                        wl_evaluation_control_release(funding);
                    }
                    run(f);
                    CHECK(f->record.semantic_result ==
                        WL_EVALUATION_CONTROL_WORK_LIMIT);
                    prepared_join_case_close(&m);
                }
            }
}

static void
test_prepared_join_unsupported_contexts(void)
{
    static const char *keys[] = {"col0"};
    for (unsigned missing = 0; missing < 2; ++missing)
        for (unsigned stopped = 0; stopped < 2; ++stopped) {
            int expected_semantic = 0;
            uint32_t expected_completed = 0, expected_segments = 0;
            bool expected_delta = false, expected_has_segments = false;
            for (unsigned attempt = 0; attempt < 2; ++attempt) {
                fixture_t f; fixture_init(&f, 2, 3, 0);
                wl_columnar_memory_governor_ref_t *g = join_governor(&f,
                        1u << 24);
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                stack->items[0].is_delta = true;
                uint32_t *segments = malloc(2 * sizeof(*segments));
                CHECK(segments);
                segments[0] = 0; segments[1] = 3;
                stack->items[0].seg_boundaries = segments;
                stack->items[0].seg_count = 1;
                col_rel_t *right = source_new(2, 0);
                CHECK(!wl_columnar_relation_rename_checked(right, "right"));
                col_rel_t *registry[] = {right};
                f.session.rels = registry;
                f.session.nrels = f.session.rel_cap = missing ? 0 : 1;
                wl_plan_op_t op =
                {.op = missing ? WL_PLAN_OP_SEMIJOIN : WL_PLAN_OP_ANTIJOIN,
                 .right_relation =
                     missing ? "missing" : "right",
                 .key_count = 1, .left_keys = keys,
                 .right_keys = keys};
                wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
                fixture_begin(&f, &plan, 0, 1);
                wl_columnar_filter_context_t *ctx = NULL;
                prepared_join_until_decision(&f, &ctx, 0, true);
                CHECK(ctx->unary_started && ctx->pin_owned && !ctx->busy
                    && ctx->join_disposition == (missing
                    ? WL_COLUMNAR_JOIN_DISPOSITION_MISSING_EXACT
                    : WL_COLUMNAR_JOIN_DISPOSITION_ANTI_KEEP_ALL));
                col_relation_snapshot_t source_image =
                    wl_columnar_relation_snapshot(f.input);
                wl_columnar_eval_stack_unary_prepared_t p = {0};
                wl_columnar_filter_context_t context_before;
                wl_columnar_eval_plan_numeric_attempt_t parent_before;
                memcpy(&context_before, ctx, sizeof(context_before));
                memcpy(&parent_before, &f.record, sizeof(parent_before));
                if (attempt) CHECK(
                        !wl_columnar_eval_stack_unary_prepared_join_begin(&p,
                        f.frame, &f.record));
                if (stopped) wl_evaluation_control_request_cancel(f.operation);
                if (attempt) {
                    int expected =
                        stopped ? WL_EVALUATION_CONTROL_CANCELLED : EINVAL;
                    CHECK(wl_columnar_eval_stack_unary_prepared_step(&p) ==
                        expected
                        && !p.join_mode && p.first_result == expected
                        && p.phase ==
                        WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD);
                    wl_columnar_eval_stack_unary_prepared_t saved;
                    memcpy(&saved, &p, sizeof(saved));
                    uint64_t spent, original_before, original_after,
                        pending_before, pending_after;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f.operation, f.operation_id, &original_before));
                    CHECK(!wl_evaluation_control_test_active_consumed(f.funding,
                        f.funding_id, &pending_before));
                    CHECK(prepared_join_cancel_turn(&p, 191, ERANGE, &spent)
                        == WL_EVALUATION_CONTROL_WORK_LIMIT && !spent
                        && !memcmp(&saved, &p, sizeof(saved)));
                    CHECK(!prepared_join_cancel_turn(&p, 192, ERANGE,
                        &spent) && spent == 192
                        && !p.join_mode && p.first_result == expected);
                    CHECK(!memcmp(&context_before, ctx,
                        sizeof(context_before)));
                    int owner = 0;
                    wl_evaluation_control_t *funding = control_new(0, &owner);
                    prepared_join_drain_ack(&p, funding,
                        wl_evaluation_control_active_attempt_id(funding));
                    CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
                    CHECK(!wl_evaluation_control_detach(funding, &owner));
                    wl_evaluation_control_release(funding);
                    CHECK(!memcmp(&context_before, ctx, sizeof(context_before))
                        && !memcmp(&parent_before, &f.record,
                        sizeof(parent_before)));
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f.operation, f.operation_id, &original_after)
                        && original_before == original_after);
                    CHECK(!wl_evaluation_control_test_active_consumed(f.funding,
                        f.funding_id, &pending_after)
                        && pending_before == pending_after);
                }
                run(&f);
                stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
                CHECK(stack && stack->top == 1 && stack->items[0].rel == f.input
                    && !stack->items[0].owned && f.input->nrows == 3
                    && f.input->columns[0][2] == 2 &&
                    f.input->columns[1][2] == 3
                    && wl_columnar_relation_snapshot_equal(source_image,
                    wl_columnar_relation_snapshot(f.input)));
                if (!attempt) {
                    expected_semantic = f.record.semantic_result;
                    expected_completed = f.record.completed;
                    expected_delta = stack->items[0].is_delta;
                    expected_segments = stack->items[0].seg_count;
                    expected_has_segments = stack->items[0].seg_boundaries !=
                        NULL;
                } else CHECK(f.record.semantic_result == expected_semantic
                        && f.record.completed == expected_completed
                        && stack->items[0].is_delta == expected_delta
                        && stack->items[0].seg_count == expected_segments
                        && (stack->items[0].seg_boundaries != NULL) ==
                        expected_has_segments);
                if (stack->items[0].seg_boundaries) CHECK(
                        stack->items[0].seg_boundaries == segments);
                int finish = wl_evaluation_control_finish(f.operation,
                        &f.operation_owner,
                        f.record.semantic_result, 0);
                /* Unwrapped MISSING_EXACT performs no further original poll;
                 * request_cancel alone is not a latched stop. The attempted
                 * proof genuinely polls once, so its own control report records
                 * CANCEL while the original no-op semantic/entry stays intact. */
                int expected_finish = stopped && (!missing || attempt)
                ? WL_EVALUATION_CONTROL_CANCELLED : 0;
                if (finish != expected_finish)
                    fprintf(stderr,
                        "unsupported context missing=%u stopped=%u attempt=%u semantic=%d completed=%u finish=%d\n",
                        missing, stopped, attempt, f.record.semantic_result,
                        f.record.completed, finish);
                CHECK(finish == expected_finish);
                fixture_close(&f); col_rel_destroy(right);
                CHECK(!wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(g)));
                wl_columnar_memory_governor_ref_release(g);
                if (attempt) {
                    wl_columnar_eval_stack_unary_prepared_status_t history;
                    CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p,
                        &history)
                        && history.phase ==
                        WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED);
                }
            }
        }
}

static void
test_prepared_join_empty_destinations(void)
{
    for (unsigned kind = 0; kind < 5; ++kind)
        for (unsigned pending = 0; pending < 2; ++pending) {
            fixture_t f; fixture_init(&f, 2, kind == 2 ? 0 : 3, 0);
            wl_columnar_memory_governor_ref_t *g = join_governor(&f, 1u << 24);
            if (kind == 3) {
                col_rel_destroy(f.input); f.input = bare_source("input");
                wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel =
                    f.input;
            }
            col_rel_t *right = kind == 3 ? bare_source("right") : source_new(1,
                    kind == 4 ? 0 : 2);
            CHECK(!wl_columnar_relation_rename_checked(right, "right"));
            col_rel_t *registry[] = {f.input, kind == 0 ? NULL : right};
            f.session.rels = registry; f.session.nrels = f.session.rel_cap = 2;
            f.session.delta_seeded = kind == 1;
            wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN,
                               .right_relation = kind ==
                                   0 ? "missing" : "right",
                               .delta_mode = kind ==
                                   1 ? WL_DELTA_FORCE_DELTA :
                                   WL_DELTA_FORCE_FULL};
            wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
            fixture_begin(&f, &plan, 0, 1);
            wl_columnar_filter_context_t *ctx = NULL;
            prepared_join_until_decision(&f, &ctx, 0, pending);
            CHECK(!ctx->execution_result && ctx->output &&
                ctx->output == ctx->private_output
                && !ctx->output->nrows &&
                ctx->join_disposition == WL_COLUMNAR_JOIN_DISPOSITION_REPLACE);
            wl_columnar_eval_stack_unary_prepared_t p = {0};
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(&p, f.frame,
                &f.record));
            prepared_until(&p, WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
            wl_columnar_eval_stack_unary_commit_prepared_nofail(&p);
            int owner = 0;
            wl_evaluation_control_t *funding = control_new(0, &owner);
            prepared_join_drain_ack(&p, funding,
                wl_evaluation_control_active_attempt_id(funding));
            CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
            CHECK(!wl_evaluation_control_detach(funding, &owner));
            wl_evaluation_control_release(funding);
            run(&f);
            CHECK(!f.record.semantic_result && f.record.completed == 1);
            eval_stack_t *actual =
                wl_columnar_eval_stack_cleanup_stack(f.frame);
            wl_col_session_t raw = {0}; wl_mem_ledger_init(&raw.mem_ledger, 0);
            raw.rels = registry; raw.nrels = raw.rel_cap = 2;
            raw.delta_seeded = f.session.delta_seeded;
            eval_stack_t expected = {0};
            CHECK(!eval_stack_push(&expected, f.input,
                false) && !col_op_join(&op, &expected, &raw));
            CHECK(actual && actual->top == 1 && expected.top == 1 &&
                actual->items[0].owned
                && !actual->items[0].rel->nrows &&
                actual->items[0].is_delta == expected.items[0].is_delta);
            same_relation(actual->items[0].rel, expected.items[0].rel);
            CHECK(!eval_stack_drain(&expected)); empty_filter_raw_drain(&raw);
            fixture_close(&f); col_rel_destroy(right);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
}

static void
prepared_join_until_finish(fixture_t *f, wl_columnar_filter_context_t **ctx,
    uint32_t cursor, bool pending)
{
    for (unsigned turn = 0; ; ++turn) {
        CHECK(turn < 100000 &&
            f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE);
        if (f->record.cursor == cursor && f->record.phase == (pending
            ? WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING :
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD)){
            CHECK(!wl_columnar_eval_stack_filter_context_owned(f->frame, ctx,
                &f->record));
            if ((*ctx)->stage == WL_COLUMNAR_FILTER_STAGE_FINISH) return;
        }
        engine_dispatch(f); ++f->witnesses;
    }
}

static void
prepared_join_finish_raw_parity(prepared_join_case_t *m)
{
    wl_col_session_t raw = {0};
    wl_mem_ledger_init(&raw.mem_ledger, 0);
    raw.nrels = raw.rel_cap = m->f.session.nrels;
    raw.rels = calloc(raw.nrels, sizeof(*raw.rels)); CHECK(raw.rels);
    for (uint32_t i = 0; i < raw.nrels; ++i)
        CHECK(!col_rel_deep_copy(m->f.session.rels[i], &raw.rels[i], NULL));
    col_rel_t *input = NULL;
    CHECK(!col_rel_deep_copy(m->f.input, &input, NULL));
    eval_stack_t reference = {0};
    CHECK(!eval_stack_push(&reference, input, true));
    CHECK(!col_eval_relation_plan(&m->plan, &reference, &raw));
    eval_stack_t *actual = wl_columnar_eval_stack_cleanup_stack(m->f.frame);
    CHECK(actual && actual->top == 1 && reference.top == 1
        && actual->items[0].rel->nrows == 6);
    same_join_bag(actual->items[0].rel, reference.items[0].rel);
    CHECK(actual->items[0].is_delta == reference.items[0].is_delta);
    CHECK(!eval_stack_drain(&reference));
    empty_filter_raw_drain(&raw);
    for (uint32_t i = 0; i < raw.nrels; ++i) col_rel_destroy(raw.rels[i]);
    free(raw.rels);
}

static void
test_prepared_join_context_finish(void)
{
    for (unsigned owned = 0; owned < 2; ++owned)
        for (unsigned pending = 0; pending < 2; ++pending)
            for (unsigned segmented = 0; segmented < 2; ++segmented)
                for (unsigned action = 0; action < 3; ++action)
                    for (unsigned ended = 0; ended < 2; ++ended) {
                        prepared_join_case_t m;
                        prepared_join_case_init_options(&m, owned, false, 0,
                            segmented);
                        fixture_t *f = &m.f;
                        eval_stack_t *initial =
                            wl_columnar_eval_stack_cleanup_stack(f->frame);
                        uint32_t *original_segments =
                            initial->items[0].seg_boundaries;
                        int cleanup_owner = 0;
                        wl_evaluation_control_t *cleanup = control_new(0,
                                &cleanup_owner);
                        uint64_t cleanup_id =
                            wl_evaluation_control_active_attempt_id(cleanup);
                        wl_columnar_eval_stack_unary_prepared_t p = {0};
                        CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(
                                &p, f->frame, &f->record));
                        prepared_until(&p,
                            WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
                        if (!action)
                            wl_columnar_eval_stack_unary_commit_prepared_nofail(
                                &p);
                        CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&p,
                            action == 1 ? ERANGE : 0, cleanup, cleanup_id));
                        prepared_join_drain_ack(&p, cleanup, cleanup_id);
                        prepared_join_until_finish(f, &m.context, m.cursor,
                            pending);
                        CHECK(!m.context->busy && !m.context->control &&
                            !m.context->pin_owned
                            && !m.context->output && m.context->unary_started);
                        wl_columnar_eval_plan_numeric_attempt_t parent;
                        memcpy(&parent, &f->record, sizeof(parent));
                        int pending_protocol = f->pending_protocol;
                        uint64_t original_id = f->operation_id;
                        if (ended) {
                            int semantic = action == 1 ? ERANGE : 0;
                            CHECK(wl_evaluation_control_finish(f->operation,
                                &f->operation_owner,
                                semantic, 0) == semantic
                                && !wl_evaluation_control_reset(f->operation)
                                && !wl_evaluation_control_begin(f->operation,
                                &f->operation_owner));
                            original_id =
                                wl_evaluation_control_active_attempt_id(
                                f->operation);
                            CHECK(original_id != f->operation_id);
                        }
                        uint64_t original_work, pending_work, used;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f->operation,
                                original_id, &original_work)
                            && !wl_evaluation_control_test_active_consumed(
                                f->funding,
                                f->funding_id, &pending_work));
                        CHECK(!ended || !original_work);
                        CHECK(
                            !
                            wl_columnar_eval_stack_unary_prepared_join_finish_begin
                                (&p,
                            f->frame, &f->record, cleanup, cleanup_id));
                        for (unsigned turn = 0; ; ++turn) {
                            CHECK(turn < 100000);
                            wl_columnar_eval_stack_unary_prepared_status_t
                                status;
                            CHECK(!wl_columnar_eval_stack_unary_prepared_query(
                                    &p, &status));
                            if (status.phase ==
                                WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY) break;
                            wl_columnar_eval_stack_unary_prepared_phase_t before
                                = p.phase;
                            int rc =
                                wl_columnar_eval_stack_unary_prepared_join_finish_step
                                (
                                &p, cleanup, cleanup_id);
                            if (before ==
                                WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SEGMENTS
                                && !rc){
                                wl_columnar_eval_stack_test_unary_observation_t
                                    observed;
                                CHECK(
                                    !wl_columnar_eval_stack_test_unary_observe(
                                        f->frame, &f->record, &observed));
                                CHECK(!observed.segments &&
                                    !observed.segment_count);
                                CHECK(observed.snapshot_segments ==
                                    (action == 1 ? original_segments : NULL)
                                    && observed.snapshot_segment_count ==
                                    (action == 1 && segmented ? 1u : 0u));
                            }
                            if (rc && rc != EAGAIN) fprintf(stderr,
                                    "finish owned=%u pending=%u segmented=%u action=%u phase=%u rc=%d\n",
                                    owned, pending, segmented, action,
                                    (unsigned)p.phase, rc);
                            CHECK(!rc || rc == EAGAIN);
                            if (p.join_mode) CHECK(p.result_saved &&
                                    p.first_result ==
                                    (action == 1 ? ERANGE : 0));
                            CHECK(!memcmp(&parent, &f->record, sizeof(parent))
                                && f->pending_protocol == pending_protocol);
                        }
                        CHECK(!wl_columnar_eval_stack_unary_prepared_ack(&p,
                            cleanup, cleanup_id));
                        CHECK(m.context->stage == WL_COLUMNAR_FILTER_STAGE_RESET
                            && m.context->semantic_result ==
                            (action == 1 ? ERANGE : 0)
                            && !m.context->private_output
                            && !wl_evaluation_control_test_active_consumed(
                                f->operation, original_id, &used)
                            && used == original_work
                            && !wl_evaluation_control_test_active_consumed(
                                f->funding, f->funding_id, &used)
                            && used == pending_work);
                        if (action == 1 && segmented) {
                            eval_stack_t *remaining =
                                wl_columnar_eval_stack_cleanup_stack(f->frame);
                            CHECK(remaining->items[0].seg_boundaries ==
                                original_segments
                                && remaining->items[0].seg_count == 1 &&
                                original_segments[1] == 3);
                        }
                        /* The real older child tuple is consumed before RESET/childACK. */
                        run(f);
                        CHECK(f->record.semantic_result ==
                            (action == 1 ? ERANGE : 0)
                            && f->record.completed ==
                            (action == 1 ? owned : owned + 1));
                        if (action != 1) prepared_join_finish_raw_parity(&m);
                        CHECK(!wl_evaluation_control_finish(cleanup,
                            &cleanup_owner, 0, 0)
                            && !wl_evaluation_control_detach(cleanup,
                            &cleanup_owner));
                        wl_evaluation_control_release(cleanup);
                        prepared_join_case_close(&m);
                        wl_columnar_eval_stack_unary_prepared_status_t history;
                        CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p,
                            &history)
                            && history.phase ==
                            WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
                            && history.first_result ==
                            (action == 1 ? ERANGE : 0));
                    }
}

static void
test_prepared_join_pool_decline(void)
{
    for (unsigned pending = 0; pending < 2; ++pending) {
        prepared_join_case_t m = {0};
        fixture_init(&m.f, 2, 3, 0);
        m.governor = join_governor(&m.f, 1u << 24);
        delta_pool_t *pool = delta_pool_create(4, sizeof(col_rel_t), 65536);
        CHECK(pool);
        col_rel_t *pooled = NULL;
        CHECK(!wl_columnar_relation_pool_new_like_governed_checked(&pooled,
            pool, "pooled", m.f.input,
            m.governor) && pooled && pooled->pool_owned);
        for (int64_t r = 0; r < 3; ++r) {
            int64_t row[2] = {r, r + 1};
            CHECK(!col_rel_append_row(pooled, row));
        }
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(m.f.frame);
        eval_entry_t initial = eval_stack_pop(stack);
        CHECK(initial.rel == m.f.input && !initial.owned
            && !eval_stack_push(stack, pooled, true));
        uint32_t *segments = malloc(2 * sizeof(*segments)); CHECK(segments);
        segments[0] = 0; segments[1] = 3;
        stack->items[0].seg_boundaries = segments;
        stack->items[0].seg_count = 1;
        m.right = source_new(1, 2);
        CHECK(!wl_columnar_relation_rename_checked(m.right, "right"));
        m.registry[0] = m.right; m.f.session.rels = m.registry;
        m.f.session.nrels = m.f.session.rel_cap = 1;
        m.ops[0] = (wl_plan_op_t){.op = WL_PLAN_OP_JOIN,
                                  .right_relation = "right",
                                  .delta_mode = WL_DELTA_FORCE_FULL};
        m.plan = (wl_plan_relation_t){.ops = m.ops, .op_count = 1};
        fixture_begin(&m.f, &m.plan, 0, 1);
        prepared_join_until_decision(&m.f, &m.context, 0, false);
        int owner = 0;
        wl_evaluation_control_t *funding = control_new(0, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        wl_columnar_eval_stack_unary_prepared_t p = {0};
        CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(
                &p, m.f.frame, &m.f.record));
        prepared_until(&p, WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
        wl_columnar_eval_stack_unary_commit_prepared_nofail(&p);
        CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&p, 0, funding,
            id));
        prepared_join_drain_ack(&p, funding, id);
        prepared_join_until_finish(&m.f, &m.context, 0, pending);
        col_relation_snapshot_t target_image =
            wl_columnar_relation_snapshot(pooled);
        CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_begin(
                &p, m.f.frame, &m.f.record, funding, id));
        int rc = 0;
        for (unsigned turn = 0; !rc || rc == EAGAIN; ++turn) {
            CHECK(turn < 10000);
            rc = wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                    funding, id);
        }
        CHECK(rc == ENOTSUP &&
            p.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE
            && p.cleanup_result == ENOTSUP && p.first_result == 0);
        wl_columnar_eval_stack_test_unary_observation_t observed;
        CHECK(!wl_columnar_eval_stack_test_unary_observe(m.f.frame, &m.f.record,
            &observed)
            && observed.relation == pooled && !observed.segments &&
            !observed.segment_count
            && observed.snapshot_relation == pooled &&
            !observed.snapshot_segments
            && !observed.snapshot_segment_count && !m.context->private_output
            && wl_columnar_relation_snapshot_equal(target_image,
            wl_columnar_relation_snapshot(pooled)));
        same_join_bag(pooled, m.f.input);
        CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_decline(&p,
            funding, id));
        for (unsigned turn = 0;
            p.phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY; ++turn){
            CHECK(turn < 10000);
            rc = wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                    funding, id);
            CHECK(!rc || rc == EAGAIN);
        }
        CHECK(!wl_columnar_eval_stack_test_unary_observe(m.f.frame, &m.f.record,
            &observed)
            && !observed.segments && !observed.snapshot_segments
            && !observed.scalar_started &&
            m.context->stage == WL_COLUMNAR_FILTER_STAGE_FINISH
            && m.context->private_output && p.cleanup_result == ENOTSUP);
        CHECK(!wl_columnar_eval_stack_unary_prepared_ack(&p, funding, id));
        /* This deliberately unsupported private pool target resumes its legal
         * generic owner. It is not certified as a bounded disposal route. */
        run(&m.f);
        CHECK(!m.f.record.semantic_result && m.f.record.completed == 1);
        prepared_join_finish_raw_parity(&m);
        CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0)
            && !wl_evaluation_control_detach(funding, &owner));
        wl_evaluation_control_release(funding);
        delta_pool_destroy(pool);
        prepared_join_case_close(&m);
        wl_columnar_eval_stack_unary_prepared_status_t history;
        CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &history)
            && history.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
            && !history.first_result && history.cleanup_result == ENOTSUP);
    }
}

/* Ordinary JOIN naturally produces segmentless heap output. This manual
 * internal ownership fixture uses a real checked compound-schema mutation and
 * transfers an independent array; it is not JOIN-produced compound coverage. */
static void
test_prepared_join_abort_distinct_segments_decline(void)
{
    for (unsigned owned = 0; owned < 2; ++owned)
        for (unsigned pending = 0; pending < 2; ++pending) {
            prepared_join_case_t m;
            prepared_join_case_init_options(&m, owned, false, 0, true);
            fixture_t *f = &m.f;
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(f->frame);
            col_rel_t *original = stack->items[0].rel;
            uint32_t *b = stack->items[0].seg_boundaries;
            col_relation_snapshot_t original_image =
                wl_columnar_relation_snapshot(original);
            CHECK(b && b[0] == 0 && b[1] == 3);
            int owner = 0;
            wl_evaluation_control_t *funding = control_new(0, &owner);
            uint64_t id = wl_evaluation_control_active_attempt_id(funding);
            wl_columnar_eval_stack_unary_prepared_t p = {0};
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(&p,
                f->frame, &f->record));
            prepared_until(&p, WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
            CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&p, ERANGE,
                funding, id));
            prepared_join_drain_ack(&p, funding, id);
            prepared_join_until_finish(f, &m.context, m.cursor, pending);
            col_rel_t *candidate = m.context->private_output;
            CHECK(candidate && candidate->ncols <= 4);
            col_rel_logical_col_t layout[4] = {0};
            layout[0] = (col_rel_logical_col_t){WIRELOG_COMPOUND_KIND_SIDE, 1,
                                                1};
            CHECK(!col_rel_apply_compound_schema(candidate, layout,
                candidate->ncols)
                && candidate->compound_kind == WIRELOG_COMPOUND_KIND_SIDE);
            uint32_t *a = malloc(2 * sizeof(*a)); CHECK(a && a != b);
            a[0] = 0; a[1] = candidate->nrows;
            CHECK(wl_columnar_eval_stack_test_unary_output_segments_owned(
                    f->frame, &f->record, a,
                    0) == EINVAL && a[0] == 0 && a[1] == candidate->nrows);
            CHECK(!wl_columnar_eval_stack_test_unary_output_segments_owned(
                    f->frame, &f->record, a, 1));
            wl_columnar_eval_plan_numeric_attempt_t parent;
            memcpy(&parent, &f->record, sizeof(parent));
            int pending_protocol = f->pending_protocol;
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_begin(
                    &p, f->frame, &f->record, funding, id));
            uint32_t *refused = malloc(2 * sizeof(*refused)); CHECK(refused);
            refused[0] = 0; refused[1] = candidate->nrows;
            CHECK(wl_columnar_eval_stack_test_unary_output_segments_owned(
                    f->frame, &f->record, refused, 1) == EBUSY);
            free(refused);
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                funding, id)
                && p.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SEGMENTS);
            int short_owner = 0;
            wl_evaluation_control_t *grant = control_new(127, &short_owner);
            uint64_t grant_id = wl_evaluation_control_active_attempt_id(grant);
            CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                grant, grant_id)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
            wl_columnar_eval_stack_test_unary_observation_t observed;
            CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame,
                &f->record, &observed)
                && observed.segments == a && observed.segment_count == 1
                && observed.snapshot_segments == b &&
                observed.snapshot_segment_count == 1);
            CHECK(wl_evaluation_control_finish(grant, &short_owner, 0, 0)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(grant,
                &report) && !report.consumed);
            CHECK(!wl_evaluation_control_detach(grant, &short_owner));
            wl_evaluation_control_release(grant);
            grant = control_new(128, &short_owner);
            grant_id = wl_evaluation_control_active_attempt_id(grant);
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                grant, grant_id));
            CHECK(!wl_evaluation_control_finish(grant, &short_owner, 0, 0)
                && !wl_evaluation_control_report(grant,
                &report) && report.consumed == 128
                && !wl_evaluation_control_detach(grant, &short_owner));
            wl_evaluation_control_release(grant);
            CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame,
                &f->record, &observed)
                && !observed.segments && !observed.segment_count
                && observed.snapshot_segments == b &&
                observed.snapshot_segment_count == 1
                && b[0] == 0 && b[1] == 3);
            int rc = 0;
            for (unsigned turn = 0; !rc || rc == EAGAIN; ++turn) {
                CHECK(turn < 10000);
                rc = wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                        funding, id);
            }
            CHECK(rc == ENOTSUP && p.first_result == ERANGE);
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_decline(&p,
                funding, id));
            for (unsigned turn = 0;
                p.phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY; ++turn){
                CHECK(turn < 10000);
                rc = wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                        funding, id);
                CHECK(!rc || rc == EAGAIN);
            }
            CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame,
                &f->record, &observed)
                && observed.relation == candidate && !observed.segments
                && observed.snapshot_relation == original &&
                observed.snapshot_segments == b
                && observed.snapshot_segment_count == 1 && b[0] == 0 &&
                b[1] == 3
                && !memcmp(&parent, &f->record, sizeof(parent))
                && f->pending_protocol == pending_protocol
                && wl_columnar_relation_snapshot_equal(original_image,
                wl_columnar_relation_snapshot(original))
                && m.context->private_output == candidate &&
                p.cleanup_result == ENOTSUP);
            CHECK(!wl_columnar_eval_stack_unary_prepared_ack(&p, funding, id));
            run(f);
            CHECK(f->record.semantic_result == ERANGE &&
                f->record.completed == owned);
            stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
            CHECK(stack && stack->top == 1 && stack->items[0].rel == original
                && stack->items[0].owned == (owned != 0)
                && stack->items[0].seg_boundaries == b &&
                stack->items[0].seg_count == 1
                && b[0] == 0 && b[1] == 3 && original->nrows == 3
                && original->columns[0][2] == 2);
            CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0)
                && !wl_evaluation_control_detach(funding, &owner));
            wl_evaluation_control_release(funding);
            prepared_join_case_close(&m);
            wl_columnar_eval_stack_unary_prepared_status_t history;
            CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &history)
                && history.first_result == ERANGE &&
                history.cleanup_result == ENOTSUP);
        }
}

typedef struct {
    uint64_t metadata, total, short_spent;
} prepared_finish_schedule_t;

/* Literal protocol admissions, independent of production fee selection and
 * measured counters. Resource state comes only from copied actual observation. */
static prepared_finish_schedule_t
prepared_finish_schedule(wl_columnar_eval_stack_unary_prepared_phase_t phase,
    const wl_columnar_eval_stack_test_unary_observation_t *o)
{
    switch (phase) {
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_COMMIT:
        return (prepared_finish_schedule_t){192, 192, 0};
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SEGMENTS:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SELECT:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_WORK:
        return (prepared_finish_schedule_t){128, 128, 0};
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_SCALAR:
        return (prepared_finish_schedule_t){256, 256, 0};
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT:
        switch (o->scalar_init_phase) {
        case WL_COLUMNAR_RELATION_SCALAR_INIT_INERT:
            return (prepared_finish_schedule_t){192, 192 + 128, 192};
        case WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO:
            return (prepared_finish_schedule_t){192, 192 + 256, 192};
        case WL_COLUMNAR_RELATION_SCALAR_INIT_BIND:
            return (prepared_finish_schedule_t){192, 192 + 192, 192};
        case WL_COLUMNAR_RELATION_SCALAR_INIT_DONE:
            return (prepared_finish_schedule_t){128, 128 + 128, 128};
        default: CHECK(false);
        }
        break;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE:
        if (!o->scalar_phase) return (prepared_finish_schedule_t){256, 256 + 1,
                                                                  256};
        if (o->scalar_phase == 1 || o->scalar_phase == 2)
            return (prepared_finish_schedule_t){192, 192 + 1, 192};
        if (o->scalar_phase == WL_COLUMNAR_RELATION_SCALAR_FINITE_PAYLOAD)
            return (prepared_finish_schedule_t){128, 128 + 1 + 192, 128 + 1};
        return (prepared_finish_schedule_t){128, 128 + 1, 128};
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DISPOSE:
        if (o->scalar_phase == 12) return o->scalar_ledger_pending
            ? (prepared_finish_schedule_t){128, 128 + 109 + 166 + 28,
                                           128 + 109 + 166}
            : (prepared_finish_schedule_t){128, 128, 0};
        if (o->scalar_phase == 13) {
            unsigned index = 4;
            switch (o->managed_phase) {
            case WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_RETIRE_DESCRIPTOR:
                index = 0; break;
            case WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_RETIRE_METADATA: index
                    = 1; break;
            case WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_RETIRE_DEDUP: index =
                    2; break;
            case WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_RETIRE_RETAINED: index
                    = 3; break;
            default: break;
            }
            if (index < 4 && !o->credit_empty[index] &&
                o->credit_started[index])
                return (prepared_finish_schedule_t){256, 256 + 64, 256};
        }
        return (prepared_finish_schedule_t){256, 256 + 1, 256};
    default: CHECK(false);
    }
    return (prepared_finish_schedule_t){0};
}

static int
prepared_finish_grant(wl_columnar_eval_stack_unary_prepared_t *p,
    fixture_t *f, uint64_t limit, bool begin, bool ack, uint64_t *spent)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    int rc = begin ? wl_columnar_eval_stack_unary_prepared_join_finish_begin(
        p, f->frame, &f->record, funding, id)
        : ack ? wl_columnar_eval_stack_unary_prepared_ack(p, funding, id)
        : wl_columnar_eval_stack_unary_prepared_join_finish_step(p, funding,
            id);
    CHECK(wl_evaluation_control_finish(funding, &owner, 0, 0)
        == (rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? rc : 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    *spent = report.consumed;
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static void
test_prepared_join_finish_boundaries(void)
{
    for (unsigned pending = 0; pending < 2; ++pending) {
        prepared_join_case_t m;
        /* A real preceding JOIN owns the target. The checked timestamp API
         * configures its real storage before the next unary claims it; this
         * does not claim ordinary JOIN naturally produces stamped output.
         * Its actual relation/timestamp ledger debt is then billed. */
        prepared_join_case_init_options(&m, true, false, 0, 7);
        fixture_t *f = &m.f;
        int owner = 0;
        wl_evaluation_control_t *setup = control_new(0, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(setup);
        wl_columnar_eval_stack_unary_prepared_t p = {0};
        CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(&p, f->frame,
            &f->record));
        prepared_until(&p, WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
        wl_columnar_eval_stack_unary_commit_prepared_nofail(&p);
        CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(&p, 0, setup, id));
        prepared_join_drain_ack(&p, setup, id);
        CHECK(!wl_evaluation_control_finish(setup, &owner, 0, 0)
            && !wl_evaluation_control_detach(setup, &owner));
        wl_evaluation_control_release(setup);
        prepared_join_until_finish(f, &m.context, m.cursor, pending);
        unsigned char old_receipt[sizeof(p)];
        memcpy(old_receipt, &p, sizeof(p));
        uint64_t spent;
        CHECK(prepared_finish_grant(&p, f, 255, true, false, &spent)
            == WL_EVALUATION_CONTROL_WORK_LIMIT && !spent
            && !memcmp(old_receipt, &p, sizeof(p))
            && !wl_columnar_eval_stack_frame_mutation_blocked(f->frame));
        CHECK(!prepared_finish_grant(&p, f, 256, true, false,
            &spent) && spent == 256);
        wl_columnar_eval_plan_numeric_attempt_t parent;
        memcpy(&parent, &f->record, sizeof(parent));
        bool saw_payload = false, saw_ledger = false, saw_credit = false;
        unsigned initializer_phases = 0;
        for (unsigned turn = 0;
            p.phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY; ++turn){
            CHECK(turn < 100000);
            wl_columnar_eval_stack_test_unary_observation_t before = {0},
                after = {0};
            CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame,
                &f->record, &before));
            wl_columnar_eval_stack_unary_prepared_phase_t phase = p.phase;
            prepared_finish_schedule_t schedule =
                prepared_finish_schedule(phase, &before);
            if (phase == WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT)
                initializer_phases |= 1u << before.scalar_init_phase;
            saw_payload |= phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE
                && before.scalar_phase ==
                WL_COLUMNAR_RELATION_SCALAR_FINITE_PAYLOAD;
            saw_ledger |= phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DISPOSE
                && before.scalar_phase == 12 && before.scalar_ledger_pending;
            saw_credit |= schedule.total == 256 + 64;
            unsigned char image[sizeof(p)];
            memcpy(image, &p, sizeof(p));
            int work_limit = WL_EVALUATION_CONTROL_WORK_LIMIT;
            memcpy(image + offsetof(wl_columnar_eval_stack_unary_prepared_t,
                cleanup_result),
                &work_limit, sizeof(work_limit));
            CHECK(prepared_finish_grant(&p, f, schedule.metadata - 1, false,
                false, &spent)
                == WL_EVALUATION_CONTROL_WORK_LIMIT && !spent &&
                p.phase == phase
                && !memcmp(image, &p, sizeof(p)));
            CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame,
                &f->record, &after)
                && !memcmp(&before, &after, sizeof(before)));
            if (schedule.total > schedule.metadata) {
                CHECK(prepared_finish_grant(&p, f, schedule.total - 1, false,
                    false, &spent)
                    == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    spent == schedule.short_spent
                    && p.phase == phase && !memcmp(image, &p, sizeof(p)));
                memset(&after, 0, sizeof(after));
                CHECK(!wl_columnar_eval_stack_test_unary_observe(f->frame,
                    &f->record, &after)
                    && !memcmp(&before, &after, sizeof(before)));
            }
            int rc = prepared_finish_grant(&p, f, schedule.total, false, false,
                    &spent);
            if ((rc && rc != EAGAIN) || spent != schedule.total) fprintf(stderr,
                    "finish boundary phase=%u scalar=%u managed=%u init=%u rc=%d spent=%llu expected=%llu\n",
                    (unsigned)phase, before.scalar_phase, before.managed_phase,
                    before.scalar_init_phase,
                    rc, (unsigned long long)spent,
                    (unsigned long long)schedule.total);
            CHECK((!rc || rc == EAGAIN) && spent == schedule.total
                && !memcmp(&parent, &f->record, sizeof(parent)));
        }
        if (initializer_phases != 15 || !saw_payload || !saw_ledger ||
            !saw_credit)
            fprintf(stderr,
                "finish milestones init=%u payload=%u ledger=%u credit=%u\n",
                initializer_phases, saw_payload, saw_ledger, saw_credit);
        CHECK(initializer_phases == 15 && saw_payload && saw_ledger &&
            saw_credit);
        CHECK(prepared_finish_grant(&p, f, 191, false, true, &spent)
            == WL_EVALUATION_CONTROL_WORK_LIMIT && !spent);
        CHECK(!prepared_finish_grant(&p, f, 192, false, true,
            &spent) && spent == 192);
        run(f);
        CHECK(!f->record.semantic_result && f->record.completed == 2);
        prepared_join_finish_raw_parity(&m);
        prepared_join_case_close(&m);
    }
}

static void
prepared_finish_fixture_start_options(prepared_join_case_t *m,
    wl_columnar_eval_stack_unary_prepared_t *p, bool pending,
    wl_evaluation_control_t *funding, uint64_t id, unsigned flags)
{
    prepared_join_case_init_options(m, true, false, 0, flags);
    CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(p, m->f.frame,
        &m->f.record));
    prepared_until(p, WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED);
    wl_columnar_eval_stack_unary_commit_prepared_nofail(p);
    CHECK(!wl_columnar_eval_stack_unary_prepared_cancel(p, 0, funding, id));
    prepared_join_drain_ack(p, funding, id);
    prepared_join_until_finish(&m->f, &m->context, m->cursor, pending);
    CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_begin(
            p, m->f.frame, &m->f.record, funding, id));
}

static void
prepared_finish_fixture_start(prepared_join_case_t *m,
    wl_columnar_eval_stack_unary_prepared_t *p, bool pending,
    wl_evaluation_control_t *funding, uint64_t id)
{
    prepared_finish_fixture_start_options(m, p, pending, funding, id, 7);
}

static void
prepared_finish_complete(prepared_join_case_t *m,
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    for (unsigned turn = 0;
        p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY; ++turn){
        CHECK(turn < 100000);
        int rc = wl_columnar_eval_stack_unary_prepared_join_finish_step(p,
                funding, id);
        CHECK(!rc || rc == EAGAIN);
    }
    CHECK(!wl_columnar_eval_stack_unary_prepared_ack(p, funding, id));
    run(&m->f);
    CHECK(!m->f.record.semantic_result && m->f.record.completed == 2);
    prepared_join_finish_raw_parity(m);
}

typedef struct {
    prepared_join_case_t *m;
    wl_columnar_eval_stack_unary_prepared_t *p;
    wl_evaluation_control_t *funding;
    uint64_t id;
} prepared_finish_foreign_t;

static void *
prepared_finish_foreign(void *arg)
{
    prepared_finish_foreign_t *t = arg;
    wl_columnar_eval_stack_unary_prepared_status_t status;
    unsigned char image[sizeof(status)];
    memset(&status, 0xa5, sizeof(status));
    memcpy(image, &status, sizeof(status));
    CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_begin(t->p,
        t->m->f.frame, &t->m->f.record, t->funding, t->id) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(t->p,
        t->funding, t->id) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_decline(t->p,
        t->funding, t->id) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_ack(t->p, t->funding,
        t->id) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_prepared_query(t->p, &status) == EINVAL
        && !memcmp(image, &status, sizeof(status)));
    return NULL;
}

static void
test_prepared_join_finish_authority(void)
{
    for (unsigned pending = 0; pending < 2; ++pending) {
        prepared_join_case_t m;
        wl_columnar_eval_stack_unary_prepared_t p = {0};
        int owner = 0;
        wl_evaluation_control_t *funding = control_new(0, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        prepared_finish_fixture_start(&m, &p, pending, funding, id);
        unsigned char image[sizeof(p)];
        memcpy(image, &p, sizeof(p));
        wl_columnar_eval_plan_numeric_attempt_t parent;
        memcpy(&parent, &m.f.record, sizeof(parent));
        uint64_t before, after, original_before, original_after;
        CHECK(!wl_evaluation_control_test_active_consumed(funding, id, &before)
            && !wl_evaluation_control_test_active_consumed(m.f.operation,
            m.f.operation_id, &original_before));
        CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(
                &p, m.f.operation, m.f.operation_id) == EINVAL);
        CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_decline(
                &p, m.f.operation, m.f.operation_id) == EINVAL);
        CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
            funding, id + 1) == EINVAL);
        wl_columnar_eval_stack_unary_prepared_t copy = p;
        CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(&copy,
            funding, id) == EINVAL);
        copy.identity = &copy;
        CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(&copy,
            funding, id) == EINVAL
            && wl_columnar_eval_stack_unary_prepared_join_finish_decline(&copy,
            funding, id) == EINVAL
            && wl_columnar_eval_stack_unary_prepared_ack(&copy, funding,
            id) == EINVAL);
        CHECK(wl_columnar_eval_stack_unary_prepared_query(&p,
            (wl_columnar_eval_stack_unary_prepared_status_t *)&p) == EINVAL);
        CHECK(wl_columnar_eval_stack_unary_prepared_query(&p,
            (wl_columnar_eval_stack_unary_prepared_status_t *)&m.f.record) ==
            EINVAL);
        CHECK(wl_columnar_eval_stack_unary_prepared_query(&p,
            (wl_columnar_eval_stack_unary_prepared_status_t *)m.f.frame) ==
            EINVAL);
        prepared_finish_foreign_t foreign = {&m, &p, funding, id};
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, prepared_finish_foreign, &foreign)
            && !wl_thread_join(&thread));
        CHECK(!memcmp(image, &p, sizeof(p)) && !memcmp(&parent, &m.f.record,
            sizeof(parent))
            && !wl_evaluation_control_test_active_consumed(funding, id,
            &after) && after == before
            && !wl_evaluation_control_test_active_consumed(m.f.operation,
            m.f.operation_id,
            &original_after) && original_after == original_before);
        prepared_finish_complete(&m, &p, funding, id);
        CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0)
            && !wl_evaluation_control_detach(funding, &owner));
        wl_evaluation_control_release(funding);
        prepared_join_case_close(&m);
    }
}

static wl_columnar_eval_stack_unary_prepared_t *finish_turnover_receipt;
static wl_evaluation_control_t *finish_turnover_funding;
static const void *finish_turnover_owner;
static unsigned finish_turnover_hits, finish_turnover_initializer_api;
static uint64_t finish_turnover_new_id, finish_turnover_reserved;
static wl_columnar_memory_governor_t *finish_turnover_governor;

static void
finish_turnover_now(wl_evaluation_control_t *funding)
{
    wl_columnar_eval_stack_unary_prepared_t *p = finish_turnover_receipt;
    CHECK(p && p->busy && funding == finish_turnover_funding &&
        !finish_turnover_hits++);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding), before,
        after;
    CHECK(!wl_evaluation_control_test_active_consumed(funding, id, &before));
    CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(p, funding,
        id) == EBUSY
        && wl_columnar_eval_stack_unary_prepared_join_finish_decline(p, funding,
        id) == EBUSY
        && wl_columnar_eval_stack_unary_prepared_ack(p, funding, id) == EBUSY);
    wl_columnar_eval_stack_unary_prepared_status_t status;
    unsigned char image[sizeof(status)];
    memset(&status, 0xa5, sizeof(status));
    memcpy(image, &status, sizeof(status));
    CHECK(wl_columnar_eval_stack_unary_prepared_query(p, &status) == EBUSY
        && !memcmp(image, &status, sizeof(status))
        && !wl_evaluation_control_test_active_consumed(funding, id,
        &after) && after == before);
    CHECK(!wl_evaluation_control_finish(funding, finish_turnover_owner, 0, 0)
        && !wl_evaluation_control_reset(funding)
        && !wl_evaluation_control_begin(funding, finish_turnover_owner));
    finish_turnover_new_id = wl_evaluation_control_active_attempt_id(funding);
    CHECK(finish_turnover_new_id != id);
}

static void
finish_outer_turnover(wl_columnar_eval_stack_unary_prepared_t *p)
{
    CHECK(p == finish_turnover_receipt);
    finish_turnover_now(finish_turnover_funding);
}

static void
finish_initializer_turnover(
    wl_columnar_relation_scalar_retirement_initializer_t *i,
    wl_evaluation_control_t *funding, unsigned api)
{
    (void)i;
    if (api == finish_turnover_initializer_api) finish_turnover_now(funding);
}

static void
finish_ledger_turnover(wl_evaluation_control_t *funding,
    wl_columnar_mem_ledger_publication_prepared_t *ledger, unsigned phase)
{
    if (phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PRECAS) {
        CHECK(ledger->phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY
            && !atomic_load_explicit(&ledger->ledger->writer_gate,
            memory_order_acquire));
        finish_turnover_now(funding);
    }
}

static void
finish_credit_turnover(wl_evaluation_control_t *funding,
    wl_columnar_memory_governor_t *governor, uint64_t old, bool overflow)
{
    CHECK(!overflow && wl_columnar_memory_reserved(governor) == old);
    finish_turnover_governor = governor; finish_turnover_reserved = old;
    finish_turnover_now(funding);
}

static void
test_prepared_join_finish_funding_turnover(void)
{
    for (unsigned pending = 0; pending < 2; ++pending)
        for (unsigned kind = 0; kind < 7; ++kind) {
            prepared_join_case_t m;
            wl_columnar_eval_stack_unary_prepared_t p = {0};
            int owner = 0;
            wl_evaluation_control_t *funding = control_new(0, &owner);
            uint64_t id = wl_evaluation_control_active_attempt_id(funding);
            prepared_finish_fixture_start(&m, &p, pending, funding, id);
            finish_turnover_receipt = &p; finish_turnover_funding = funding;
            finish_turnover_owner = &owner; finish_turnover_hits = 0;
            finish_turnover_governor = NULL;
            if (!kind) wl_columnar_eval_stack_test_unary_prepared_checkpoint =
                    finish_outer_turnover;
            else if (kind < 5) {
                finish_turnover_initializer_api = kind;
                wl_columnar_relation_scalar_retirement_init_test_checkpoint =
                    finish_initializer_turnover;
            } else if (kind == 5)
                wl_columnar_mem_ledger_publication_test_checkpoint =
                    finish_ledger_turnover;
            else wl_columnar_memory_governor_test_before_total_cas =
                    finish_credit_turnover;
            wl_columnar_eval_plan_numeric_attempt_t parent;
            memcpy(&parent, &m.f.record, sizeof(parent));
            int rc = 0;
            for (unsigned turn = 0; !finish_turnover_hits; ++turn) {
                CHECK(turn < 100000 &&
                    p.phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY);
                rc = wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                        funding, id);
                if (!finish_turnover_hits) CHECK(!rc || rc == EAGAIN);
            }
            wl_columnar_eval_stack_test_unary_prepared_checkpoint = NULL;
            wl_columnar_relation_scalar_retirement_init_test_checkpoint = NULL;
            wl_columnar_mem_ledger_publication_test_checkpoint = NULL;
            wl_columnar_memory_governor_test_before_total_cas = NULL;
            CHECK(rc == EINVAL && finish_turnover_hits == 1
                && !memcmp(&parent, &m.f.record, sizeof(parent))
                && !m.context->execution_result && !m.context->semantic_result
                && m.context->stage == WL_COLUMNAR_FILTER_STAGE_FINISH);
            uint64_t consumed;
            CHECK(!wl_evaluation_control_test_active_consumed(funding,
                finish_turnover_new_id,
                &consumed) && !consumed && !p.first_result);
            if (kind == 4) {
                wl_columnar_eval_stack_test_unary_observation_t observed;
                CHECK(!wl_columnar_eval_stack_test_unary_observe(m.f.frame,
                    &m.f.record, &observed)
                    && observed.scalar_started &&
                    observed.scalar_init_phase ==
                    WL_COLUMNAR_RELATION_SCALAR_INIT_DONE);
            }
            if (kind == 5) {
                wl_columnar_eval_stack_test_unary_observation_t observed;
                CHECK(!wl_columnar_eval_stack_test_unary_observe(m.f.frame,
                    &m.f.record, &observed)
                    && observed.scalar_phase == 12 &&
                    observed.scalar_ledger_pending
                    && !atomic_load_explicit(
                        &m.f.session.mem_ledger.writer_gate,
                        memory_order_acquire));
            }
            if (kind == 6) CHECK(finish_turnover_governor
                    && wl_columnar_memory_reserved(finish_turnover_governor) ==
                    finish_turnover_reserved);
            id = finish_turnover_new_id;
            finish_turnover_receipt = NULL; finish_turnover_funding = NULL;
            finish_turnover_owner = NULL;
            finish_turnover_governor = NULL;
            prepared_finish_complete(&m, &p, funding, id);
            CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0)
                && !wl_evaluation_control_detach(funding, &owner));
            wl_evaluation_control_release(funding);
            prepared_join_case_close(&m);
        }
}

static int
prepared_finish_decline_grant(wl_columnar_eval_stack_unary_prepared_t *p,
    uint64_t limit, uint64_t *spent)
{
    int owner = 0;
    wl_evaluation_control_t *funding = control_new(limit, &owner);
    int rc = wl_columnar_eval_stack_unary_prepared_join_finish_decline(p,
            funding,
            wl_evaluation_control_active_attempt_id(funding));
    CHECK(wl_evaluation_control_finish(funding, &owner, 0, 0)
        == (rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? rc : 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    *spent = report.consumed;
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static void
test_prepared_join_finish_partial_decline(void)
{
    static const struct {
        wl_columnar_eval_stack_unary_prepared_phase_t phase;
        int initializer, scalar;
        uint64_t partial_grant, partial_spent;
    } targets[] = {
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD, -1, -1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SEGMENTS, -1, -1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SELECT, -1, -1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT,
         WL_COLUMNAR_RELATION_SCALAR_INIT_INERT, -1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT,
         WL_COLUMNAR_RELATION_SCALAR_INIT_INERT, -1, 319, 192},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT,
         WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO, -1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT,
         WL_COLUMNAR_RELATION_SCALAR_INIT_BIND, -1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT,
         WL_COLUMNAR_RELATION_SCALAR_INIT_DONE, -1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT,
         WL_COLUMNAR_RELATION_SCALAR_INIT_DONE, -1, 255, 128},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE, -1, 0, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE, -1, 1, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE, -1, 16, 0, 0},
        {WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_COMMIT, -1, -1, 0, 0}
    };
    for (unsigned pending = 0; pending < 2; ++pending)
        for (unsigned target = 0; target < sizeof(targets) / sizeof(targets[0]);
            ++target){
            prepared_join_case_t m;
            wl_columnar_eval_stack_unary_prepared_t p = {0};
            int owner = 0;
            wl_evaluation_control_t *funding = control_new(0, &owner);
            uint64_t id = wl_evaluation_control_active_attempt_id(funding);
            prepared_finish_fixture_start(&m, &p, pending, funding, id);
            for (unsigned turn = 0; ; ++turn) {
                CHECK(turn < 100000);
                wl_columnar_eval_stack_test_unary_observation_t observed;
                CHECK(!wl_columnar_eval_stack_test_unary_observe(m.f.frame,
                    &m.f.record, &observed));
                if (p.phase == targets[target].phase
                    && (targets[target].initializer < 0
                    || observed.scalar_init_phase ==
                    (unsigned)targets[target].initializer)
                    && (targets[target].scalar < 0
                    || observed.scalar_phase ==
                    (unsigned)targets[target].scalar)) break;
                int rc =
                    wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                        funding, id);
                CHECK(!rc || rc == EAGAIN);
            }
            uint64_t spent;
            if (targets[target].partial_grant)
                CHECK(prepared_finish_grant(&p, &m.f,
                    targets[target].partial_grant,
                    false, false, &spent) == WL_EVALUATION_CONTROL_WORK_LIMIT
                    && spent == targets[target].partial_spent);
            wl_columnar_filter_context_t context;
            wl_columnar_eval_plan_numeric_attempt_t parent;
            memcpy(&context, m.context, sizeof(context));
            memcpy(&parent, &m.f.record, sizeof(parent));
            unsigned char image[sizeof(p)]; memcpy(image, &p, sizeof(p));
            wl_mem_ledger_snapshot_t ledger_before, ledger_after;
            wl_mem_ledger_snapshot(&m.f.session.mem_ledger, &ledger_before);
            uint64_t reserved =
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                        m.governor));
            CHECK(prepared_finish_decline_grant(&p, 127,
                &spent) == WL_EVALUATION_CONTROL_WORK_LIMIT
                && !spent && !memcmp(image, &p, sizeof(p)));
            CHECK(!prepared_finish_decline_grant(&p, 128,
                &spent) && spent == 128);
            for (unsigned turn = 0;
                p.phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY; ++turn){
                CHECK(turn < 100000);
                wl_columnar_eval_stack_test_unary_observation_t observed;
                CHECK(!wl_columnar_eval_stack_test_unary_observe(m.f.frame,
                    &m.f.record, &observed));
                prepared_finish_schedule_t schedule;
                switch (p.phase) {
                case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_INIT:
                    schedule = observed.scalar_init_phase ==
                        WL_COLUMNAR_RELATION_SCALAR_INIT_INERT
                    ? (prepared_finish_schedule_t){128, 128, 0}
                    : (prepared_finish_schedule_t){128, 128 + 128, 128};
                    break;
                case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLAIMS:
                    schedule = (prepared_finish_schedule_t){0, 256, 0}; break;
                case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLEAR:
                    schedule = (prepared_finish_schedule_t){256, 256, 0}; break;
                case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_RESTORE:
                case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_WORK:
                    schedule = (prepared_finish_schedule_t){128, 128, 0}; break;
                default: CHECK(false);
                    schedule = (prepared_finish_schedule_t){0};
                }
                if (schedule.metadata) CHECK(prepared_finish_grant(&p, &m.f,
                        schedule.metadata - 1,
                        false, false,
                        &spent) == WL_EVALUATION_CONTROL_WORK_LIMIT && !spent);
                if (schedule.total > schedule.metadata)
                    CHECK(prepared_finish_grant(&p, &m.f, schedule.total - 1,
                        false, false, &spent)
                        == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                        spent == schedule.short_spent);
                CHECK(!prepared_finish_grant(&p, &m.f, schedule.total, false,
                    false, &spent)
                    && spent == schedule.total
                    && !memcmp(&parent, &m.f.record, sizeof(parent)));
            }
            CHECK(m.context->stage == WL_COLUMNAR_FILTER_STAGE_FINISH
                && m.context->private_output && !m.context->semantic_result
                && !m.context->execution_result
                && wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(m.governor)) ==
                reserved);
            if (!target) CHECK(!memcmp(&context, m.context, sizeof(context)));
            wl_mem_ledger_snapshot(&m.f.session.mem_ledger, &ledger_after);
            CHECK(ledger_after.current_bytes == ledger_before.current_bytes
                && ledger_after.subsys_bytes[WL_MEM_SUBSYS_RELATION]
                == ledger_before.subsys_bytes[WL_MEM_SUBSYS_RELATION]
                && ledger_after.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]
                == ledger_before.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]);
            CHECK(!wl_columnar_eval_stack_unary_prepared_ack(&p, funding, id));
            run(&m.f);
            CHECK(!m.f.record.semantic_result && m.f.record.completed == 2);
            prepared_join_finish_raw_parity(&m);
            CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0)
                && !wl_evaluation_control_detach(funding, &owner));
            wl_evaluation_control_release(funding);
            prepared_join_case_close(&m);
            wl_columnar_eval_stack_unary_prepared_status_t history;
            CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &history)
                && !history.first_result &&
                history.cleanup_result == WL_EVALUATION_CONTROL_WORK_LIMIT
                && history.result_saved == (target != 0));
        }
}

static void
test_prepared_join_finish_foreign_context(void)
{
    static const char *keys[] = {"col0"};
    for (unsigned missing = 0; missing < 2; ++missing)
        for (unsigned pending = 0; pending < 2; ++pending) {
            fixture_t f; fixture_init(&f, 2, 3, 0);
            wl_columnar_memory_governor_ref_t *g = join_governor(&f, 1u << 24);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            eval_entry_t input = eval_stack_pop(stack);
            CHECK(input.rel == f.input && !input.owned
                && !eval_stack_push_delta(stack, f.input, false, true));
            uint32_t *segments = malloc(2 * sizeof(*segments)); CHECK(segments);
            segments[0] = 0; segments[1] = 3;
            stack->items[0].seg_boundaries = segments;
            stack->items[0].seg_count = 1;
            col_rel_t *right = source_new(2, 0);
            CHECK(!wl_columnar_relation_rename_checked(right, "right"));
            col_rel_t *registry[] = {right};
            f.session.rels = registry;
            f.session.nrels = f.session.rel_cap = missing ? 0 : 1;
            wl_plan_op_t op =
            {.op = missing ? WL_PLAN_OP_SEMIJOIN : WL_PLAN_OP_ANTIJOIN,
             .right_relation = missing ? "missing" : "right",
             .key_count = 1,
             .left_keys = keys, .right_keys = keys};
            wl_plan_relation_t plan = {.ops = &op, .op_count = 1};
            fixture_begin(&f, &plan, 0, 1);
            wl_columnar_filter_context_t *ctx = NULL;
            prepared_join_until_decision(&f, &ctx, 0, false);
            int owner = 0;
            wl_evaluation_control_t *funding = control_new(0, &owner);
            uint64_t id = wl_evaluation_control_active_attempt_id(funding);
            wl_columnar_eval_stack_unary_prepared_t p = {0};
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_begin(&p, f.frame,
                &f.record)
                && wl_columnar_eval_stack_unary_prepared_step(&p) == EINVAL &&
                !p.join_mode
                && !wl_columnar_eval_stack_unary_prepared_cancel(&p, EINVAL,
                funding, id));
            prepared_join_drain_ack(&p, funding, id);
            prepared_join_until_finish(&f, &ctx, 0, pending);
            wl_columnar_filter_context_t context;
            wl_columnar_eval_plan_numeric_attempt_t parent;
            memcpy(&context, ctx, sizeof(context));
            memcpy(&parent, &f.record, sizeof(parent));
            uint64_t original_work, pending_work, after;
            CHECK(!wl_evaluation_control_test_active_consumed(f.operation,
                f.operation_id, &original_work)
                && !wl_evaluation_control_test_active_consumed(f.funding,
                f.funding_id, &pending_work));
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_begin(
                    &p, f.frame, &f.record, funding, id));
            CHECK(wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                funding, id) == EINVAL
                && !p.join_mode && !p.result_saved && !memcmp(&context, ctx,
                sizeof(context))
                && !memcmp(&parent, &f.record, sizeof(parent)));
            CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_decline(&p,
                funding, id)
                && !wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
                funding, id)
                && p.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY
                && !wl_columnar_eval_stack_unary_prepared_ack(&p, funding, id)
                && !memcmp(&context, ctx, sizeof(context))
                && !memcmp(&parent, &f.record, sizeof(parent))
                && !wl_evaluation_control_test_active_consumed(f.operation,
                f.operation_id, &after)
                && after == original_work
                && !wl_evaluation_control_test_active_consumed(f.funding,
                f.funding_id, &after)
                && after == pending_work);
            wl_columnar_eval_stack_test_unary_observation_t observed;
            CHECK(!wl_columnar_eval_stack_test_unary_observe(f.frame, &f.record,
                &observed)
                && observed.segments == (missing ? NULL : segments)
                && observed.segment_count == (missing ? 0u : 1u)
                && observed.snapshot_segments == segments && segments[1] == 3);
            run(&f);
            stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
            /* Actual ANTI_KEEP_ALL already cleared delta and moved its array to
             * retired at JOIN_DECIDE; MISSING_EXACT leaves input metadata intact.
             * Refusing this different FINISH protocol must preserve both rules. */
            CHECK(!f.record.semantic_result && f.record.completed == 1
                && stack && stack->top == 1 && stack->items[0].rel == f.input
                && !stack->items[0].owned &&
                stack->items[0].is_delta == (missing != 0)
                && stack->items[0].seg_count == (missing ? 1u : 0u));
            if (missing) CHECK(stack->items[0].seg_boundaries == segments &&
                    segments[1] == 3);
            else CHECK(!stack->items[0].seg_boundaries);
            CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0)
                && !wl_evaluation_control_detach(funding, &owner));
            wl_evaluation_control_release(funding);
            fixture_close(&f); col_rel_destroy(right);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
            wl_columnar_eval_stack_unary_prepared_status_t history;
            CHECK(!wl_columnar_eval_stack_unary_prepared_query(&p, &history)
                && !history.first_result && !history.result_saved &&
                history.cleanup_result == EINVAL);
        }
}

static void
test_prepared_join_finish_wide(void)
{
    for (unsigned pending = 0; pending < 2; ++pending) {
        prepared_join_case_t m;
        wl_columnar_eval_stack_unary_prepared_t p = {0};
        int owner = 0;
        wl_evaluation_control_t *funding = control_new(0, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        prepared_finish_fixture_start_options(&m, &p, pending, funding, id, 15);
        /* The actual preceding JOIN owns65 columns; no truncation to the
         * former64-column setup shape or one bulk column destructor is used. */
        CHECK(!wl_columnar_eval_stack_unary_prepared_join_finish_step(&p,
            funding, id)
            && p.work.original.rel && p.work.original.rel->ncols == 65
            && p.work.original.owned);
        prepared_finish_complete(&m, &p, funding, id);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(m.f.frame);
        CHECK(stack && stack->top == 1 && stack->items[0].rel->ncols == 66);
        CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0)
            && !wl_evaluation_control_detach(funding, &owner));
        wl_evaluation_control_release(funding);
        prepared_join_case_close(&m);
    }
}

/* Standalone leaf behavior, not automatic FINISH or a containing-turn
 * source-bound oracle. The future bridge has its separately audited guards. */
static void
test_finite_scalar_retirement_leaves(void)
{
    for (unsigned wide = 0; wide < 2; ++wide)
        for (unsigned governed = 0; governed < 2; ++governed)
            for (unsigned stamps = 0; stamps < 2; ++stamps) {
                uint32_t width = wide ? 65 : 3;
                int64_t codes[65], row[65] = {0};
                static const int64_t types[] = {WIRELOG_TYPE_INT32,
                                                WIRELOG_TYPE_INT64,
                                                WIRELOG_TYPE_UINT32,
                                                WIRELOG_TYPE_UINT64,
                                                WIRELOG_TYPE_FLOAT,
                                                WIRELOG_TYPE_BOOL};
                for (uint32_t c = 0; c < width; ++c) codes[c] = types[c % 6];
                wl_columnar_memory_resolution_t resolution = {
                    .budget_bytes = 1u << 24, .usable_bytes = 1u << 24,
                        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
                        .status = WL_COLUMNAR_MEMORY_OK
                };
                wl_columnar_memory_governor_ref_t *g = governed
                ? wl_columnar_memory_governor_ref_create(&resolution) : NULL;
                CHECK(!governed || g);
                int owner = 0;
                wl_evaluation_control_t *funding = control_new(0, &owner);
                uint64_t id = wl_evaluation_control_active_attempt_id(funding);
                col_rel_t *target = NULL;
                CHECK(
                    !
                    wl_columnar_relation_new_typed_codes_stamped_operator_attempt_controlled
                    (
                        "finite", width, 3, codes, stamps, g, &target, funding,
                        id));
                CHECK(target && !col_rel_append_row(target, row));
                wl_mem_ledger_t ledger;
                wl_mem_ledger_init(&ledger, 0);
                /* Independent peer debt catches duplicate FREE events hidden by
                 * saturating counters at zero. The target never owns this debt. */
                wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 11);
                wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 13);
                target->mem_ledger = &ledger;
                wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
                    col_rel_owned_ledger_bytes(target));
                target->ledger_ts_bytes =
                    col_rel_timestamp_ledger_bytes(target);
                wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP,
                    target->ledger_ts_bytes);
                col_relation_snapshot_t snapshot =
                    wl_columnar_relation_snapshot(target);
                wl_columnar_relation_scalar_retirement_t scalar;
                memset(&scalar, 0xa5, sizeof(scalar));
                wl_columnar_relation_scalar_retirement_initializer_t initializer
                    = {0};
                CHECK(!wl_columnar_relation_scalar_retirement_init_begin(
                        &initializer, &owner,
                        &scalar, target, funding, id));
                bool begun = false;
                for (unsigned turn = 0; !begun; ++turn) {
                    CHECK(turn < 1000 &&
                        !wl_columnar_relation_scalar_retirement_init_step(
                            &initializer, &owner, &scalar, target, funding, id,
                            &begun));
                }
                CHECK(!scalar.ready && scalar.storage.relation == target
                    && !wl_columnar_relation_scalar_retirement_init_reset(
                        &initializer,
                        &owner, funding, id));
                unsigned token_phases = 0;
                bool payload = false;
                for (unsigned turn = 0; !scalar.ready; ++turn) {
                    CHECK(turn < 100000);
                    if (scalar.phase >= 16 && scalar.phase <= 20)
                        token_phases |= 1u << (scalar.phase - 16);
                    if (scalar.phase ==
                        WL_COLUMNAR_RELATION_SCALAR_FINITE_PAYLOAD){
                        CHECK(governed && stamps && !scalar.ready);
                        wl_columnar_relation_scalar_retirement_t before;
                        memcpy(&before, &scalar, sizeof(before));
                        col_rel_t image;
                        memcpy(&image, target, sizeof(image));
                        CHECK(!wl_evaluation_control_finish(funding, &owner, 0,
                            0)
                            && !wl_evaluation_control_reset(funding)
                            && !wl_evaluation_control_begin(funding, &owner));
                        uint64_t successor =
                            wl_evaluation_control_active_attempt_id(funding),
                            used;
                        CHECK(successor != id
                            &&
                            wl_columnar_relation_scalar_retirement_prepare_finite
                            (
                                &scalar, funding, id) == EINVAL
                            && !memcmp(&before, &scalar, sizeof(before))
                            && !wl_evaluation_control_test_active_consumed(
                                funding, successor, &used)
                            && !used);
                        id = successor;
                        for (unsigned exact = 0; exact < 2; ++exact) {
                            wl_evaluation_control_t *grant = control_new(1 +
                                    192 - !exact, &owner);
                            uint64_t grant_id =
                                wl_evaluation_control_active_attempt_id(grant);
                            int rc =
                                wl_columnar_relation_scalar_retirement_prepare_finite
                                (
                                &scalar, grant, grant_id);
                            CHECK(rc ==
                                (exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
                            if (!exact) CHECK(!memcmp(&before, &scalar,
                                    sizeof(before))
                                    && !memcmp(&image, target, sizeof(image)));
                            else CHECK(scalar.ready && scalar.phase == 2 &&
                                    !scalar.column);
                            CHECK(wl_evaluation_control_finish(grant, &owner, 0,
                                0)
                                == (exact ? 0 :
                                WL_EVALUATION_CONTROL_WORK_LIMIT));
                            wl_evaluation_control_report_t report;
                            CHECK(!wl_evaluation_control_report(grant, &report)
                                && report.consumed == (exact ? 193u : 1u));
                            CHECK(!wl_evaluation_control_detach(grant, &owner));
                            wl_evaluation_control_release(grant);
                        }
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                funding, id, &used)
                            && !used);
                        payload = true;
                        break;
                    }
                    /* This leaf-only driver pays its own fixed setup. Actual
                     * bridge source regions have a separate containing audit. */
                    CHECK(!wl_evaluation_control_charge_attempt(funding, id,
                        256));
                    int rc =
                        wl_columnar_relation_scalar_retirement_prepare_finite(
                        &scalar, funding, id);
                    CHECK(!rc || rc == EAGAIN);
                }
                CHECK(token_phases == 31 && payload == (governed && stamps)
                    && !wl_columnar_relation_scalar_retirement_ready_validate(
                        &scalar, target, snapshot)
                    && !wl_columnar_relation_scalar_retirement_commit(&scalar));
                bool contended = false;
                for (unsigned turn = 0; scalar.storage.phase !=
                    WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_DONE; ++turn) {
                    CHECK(turn < 100000);
                    int rc;
                    if (scalar.phase == 12) {
                        CHECK(scalar.ledger == &ledger && scalar.ledger_bytes);
                        uint64_t bytes = scalar.ledger_bytes,
                            ts = scalar.ledger_timestamp_bytes;
                        wl_columnar_mem_ledger_publication_prepared_t holder =
                        {0};
                        CHECK(
                            !
                            wl_columnar_mem_ledger_publication_prepare_controlled
                            (
                                &ledger, NULL, 0, &holder, funding));
                        CHECK(
                            !
                            wl_columnar_mem_ledger_publication_try_acquire_controlled
                                (&holder));
                        CHECK(!wl_evaluation_control_charge_attempt(funding, id,
                            128));
                        CHECK(
                            wl_columnar_relation_scalar_retirement_ledger_step_finite
                            (
                                &scalar, funding, id) == EAGAIN
                            && scalar.phase == 12 && scalar.ledger == &ledger
                            && scalar.ledger_bytes == bytes &&
                            scalar.ledger_timestamp_bytes == ts);
                        wl_mem_ledger_snapshot_t held;
                        wl_mem_ledger_snapshot(&ledger, &held);
                        CHECK(held.current_bytes == bytes + ts + 24);
                        CHECK(!wl_columnar_mem_ledger_publication_cancel(
                                &holder));
                        CHECK(!wl_evaluation_control_charge_attempt(funding, id,
                            128));
                        rc =
                            wl_columnar_relation_scalar_retirement_ledger_step_finite
                            (
                            &scalar, funding, id);
                        CHECK(rc == EAGAIN && scalar.phase == 10 &&
                            !scalar.ledger
                            && !scalar.ledger_bytes &&
                            !scalar.ledger_timestamp_bytes);
                        contended = true;
                    } else {
                        CHECK(!wl_evaluation_control_charge_attempt(funding, id,
                            256));
                        rc =
                            wl_columnar_relation_scalar_retirement_step(&scalar,
                                funding, id);
                    }
                    CHECK(!rc || rc == EAGAIN);
                }
                CHECK(contended && !scalar.storage.relation);
                wl_mem_ledger_snapshot_t final;
                wl_mem_ledger_snapshot(&ledger, &final);
                CHECK(final.current_bytes == 24 &&
                    final.subsys_bytes[WL_MEM_SUBSYS_RELATION] == 11
                    && final.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 13);
                wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_RELATION, 11);
                wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 13);
                wl_mem_ledger_snapshot(&ledger, &final);
                CHECK(!final.current_bytes);
                if (g) {
                    CHECK(!wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(g)));
                    wl_columnar_memory_governor_ref_release(g);
                }
                CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
                CHECK(!wl_evaluation_control_detach(funding, &owner));
                wl_evaluation_control_release(funding);
            }
}

int
main(void)
{
    test_prepared_join_chained_delta();
    test_prepared_join_early_stop_turnover();
    test_prepared_join_unsupported_contexts();
    test_prepared_join_empty_destinations();
    test_prepared_join_original_boundaries();
    test_finite_scalar_retirement_leaves();
    test_prepared_join_context_finish();
    test_prepared_join_pool_decline();
    test_prepared_join_abort_distinct_segments_decline();
    test_prepared_join_finish_boundaries();
    test_prepared_join_finish_authority();
    test_prepared_join_finish_funding_turnover();
    test_prepared_join_finish_partial_decline();
    test_prepared_join_finish_foreign_context();
    test_prepared_join_finish_wide();
    test_prepared_unary_actual_lifecycle();
    test_prepared_unary_original_boundaries();
    test_prepared_unary_cancelled_parent_cleanup();
    test_prepared_unary_funded_cleanup();
    test_prepared_unary_authority_and_query();
    test_prepared_unary_acked_reuse();
    test_prepared_unary_generation_checkpoint();
    test_prepared_unary_entry_aliases();
    test_prepared_unary_descriptor_only_cleanup();
    test_prepared_unary_static_bindings_status();
    test_lftj_metadata_maximum();
    test_lftj_metadata_original_boundaries();
    test_lftj_metadata_ended_cleanup();
    test_lftj_metadata_ended_last_reference();
    test_lftj_metadata_ended_frame_refusal();
    test_lftj_metadata_header_and_grammar();
    test_lftj_metadata_binding_semantic_parity();
    test_lftj_metadata_real_frame_shape();
    test_lftj_metadata_checked_handoff_failures();
    test_lftj_metadata_frame_output_alias();
    test_lftj_metadata_funding_generation();
#ifndef _WIN32
    test_lftj_metadata_replaced_original_refusal();
#endif
    test_scalar_initialization_frame_stops();
    test_lftj_scalar_cleanup_grants();
    test_lftj_success_retains_funding_class();
    test_lftj_schema_continuations();
    test_lftj_source_order_stops(); test_lftj_source_order();
    test_anti_reentry();
    test_anti_host_budget(); test_anti_cutoff_refusals();
    test_empty_anti_identity(); test_missing_probe_exact_order();
    test_present_empty_probe_metadata();
    test_forced_bare_selectors(); test_forced_bare_failures();
    test_empty_right_filter_setup(); test_empty_filter_compiler_resources();
    test_bare_source_borrow(); test_bare_ordinary_join();
    test_bare_source_cleanup_funding();
    test_bare_join_cleanup_receipt(); test_bare_source_partial_stop();
    test_numeric_funded_scalar_finish();
    test_numeric_funded_scalar_contention();
    test_composed_unary_work_limit_cleanup();
    test_composed_concat_work_limit_cleanup();
    test_composed_concat_stopped_retirement_retry();
    test_composed_private_input_binding();
    test_composed_lftj_private_shadow();
    test_composed_recursive_terminal_exchange();
    test_composed_lftj_stopped_cleanup();
    test_composed_source_stopped_cleanup();
    test_composed_source_cleanup_abort_retry();
    test_composed_source_cleanup_pre_action_phases();
    test_composed_domain_and_repeated_source();
    test_composed_controlled_batch_fallback_owner();
    test_composed_pipeline_lookup_lineage();
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
