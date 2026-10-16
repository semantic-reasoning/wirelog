/* #1821 production numeric plan composition, original authority and host turns. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/thread.h"
#include <errno.h>
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

static void
run(fixture_t *f)
{
    queue_t q = {{0}, 0, 0};
    enqueue(&q, 0); enqueue(&q, 1);
    unsigned turns = 0;
    while (q.count) {
        CHECK(++turns < 100000);
        unsigned job = q.entries[q.head];
        q.head = (q.head + 1) % 4; --q.count;
        if (job == 0) {
            engine_dispatch(f);
            if (f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE)
                enqueue(&q, 0);
        } else {
            ++f->witnesses;
            CHECK(f->witnesses == f->calls);
            if (f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE)
                enqueue(&q, 1);
        }
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
    for (uint32_t i = prebound ? 1 : 0; i < fragment->op_count; ++i) {
        const wl_plan_op_t *op = &fragment->ops[i];
        int rc;
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE: rc = col_op_variable(op, &stack, &raw); break;
        case WL_PLAN_OP_JOIN: rc = col_op_join(op, &stack, &raw); break;
        case WL_PLAN_OP_FILTER: rc = wl_columnar_filter_op(op, &stack, &raw);
            break;
        case WL_PLAN_OP_MAP: rc = col_op_map(op, &stack, &raw); break;
        default: abort();
        }
        CHECK(!rc);
    }
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
test_generated_join_fragments(void)
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
            /* Execute this contiguous prefix only. CONSOLIDATE belongs to the
             * remaining generated relation, outside this interface. */
            CHECK(end < rp->op_count &&
                rp->ops[end].op == WL_PLAN_OP_CONSOLIDATE);
            wl_plan_relation_t fragment = {.ops = rp->ops, .op_count = end};
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
                    CHECK(!col_rel_enable_timestamps(f.input));
                    for (uint32_t r = 0; r < f.input->nrows; ++r) {
                        f.input->timestamps[r].iteration = r;
                        f.input->timestamps[r].multiplicity = r % 2 ? -3 : 5;
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
test_join_retained_successor(void)
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
                          {.op = WL_PLAN_OP_JOIN, .right_relation = "third",
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
    fixture_close(&f);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == peer.bytes);
    CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
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
    for (unsigned bad = 0; bad < 3; ++bad) {
        wl_plan_op_t invalid[] = {ops[0], ops[1], ops[1]};
        invalid[bad].op = WL_PLAN_OP_CONCAT;
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

int
main(void)
{
    test_owned_consolidate_entry(); test_owned_consolidate_successor();
    test_owned_join_entry();
    test_owned_join_successor();
    test_generated_join_fragments();
    test_join_no_output_stops();
    test_join_literal_limits();
    test_join_later_failures();
    test_join_retained_successor();
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
