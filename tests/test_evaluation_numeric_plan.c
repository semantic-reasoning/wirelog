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
        CHECK(!f->pending_protocol);
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
        ops[bad].op = WL_PLAN_OP_JOIN;
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

int
main(void)
{
    test_generated(false); test_generated(true); test_boundaries();
    test_authority();
    test_intermediate_stop(); test_literal_prefixes(); test_retained_input();
    test_pending_funding(); test_later_allocation_failure();
    test_begin_rollback(); test_host_boundaries();
    puts("numeric plan tests passed");
    return 0;
}
