/* #1821 ordinary JOIN: raw parity, original ownership and paid host turns. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    (void)token; (void)phase;
}

typedef struct {
    wl_col_session_t session;
    col_rel_t *registry[2], *left;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_evaluation_control_t *operation, *funding;
    uint64_t operation_id, funding_id;
    int operation_owner, funding_owner;
    wl_plan_op_t op;
    wl_columnar_filter_status_t status;
    unsigned phase, turns, witnesses;
    int protocol;
    bool owns_left;
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
relation_new(const char *name, uint32_t width, uint32_t rows)
{
    col_rel_t *rel = NULL;
    CHECK(!col_rel_alloc(&rel, name));
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
fixture_init(fixture_t *f, uint32_t width, uint32_t left_rows,
    uint32_t right_rows, uint64_t limit)
{
    memset(f, 0, sizeof(*f));
    wl_mem_ledger_init(&f->session.mem_ledger, 0);
    f->left = relation_new("left", width, left_rows);
    f->registry[0] = relation_new("right", width, right_rows);
    f->session.rels = f->registry; f->session.nrels = 1;
    f->operation = control_new(limit, &f->operation_owner);
    f->funding = control_new(0, &f->funding_owner);
    f->operation_id = wl_evaluation_control_active_attempt_id(f->operation);
    f->funding_id = wl_evaluation_control_active_attempt_id(f->funding);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(&f->session, &f->frame));
    CHECK(!eval_stack_push(wl_columnar_eval_stack_cleanup_stack(f->frame),
        f->left, false));
    f->op.op = WL_PLAN_OP_JOIN; f->op.right_relation = "right";
}

/* A host dispatch obtains its own live funding grant. The caller parks on
 * refusal and never enters a child without funding. ACK is a separate turn.
 * Setup, CHECK assertions, FIFO/witness bookkeeping are outside this body. */
static int
dispatch(fixture_t *f)
{
    if (wl_evaluation_control_active_attempt_id(f->funding) != f->funding_id)
        return EINVAL;
    int rc = wl_evaluation_control_charge(f->funding, 384);
    if (rc) return rc;
    if (!f->phase) {
        rc = wl_columnar_join_op_attempt_start(&f->op, f->frame, &f->session,
                f->operation, f->operation_id, &f->status);
    } else if (f->phase == 1) {
        rc = wl_columnar_filter_op_attempt_step(f->frame, &f->status);
    } else {
        rc = wl_columnar_filter_op_attempt_ack(f->frame);
        if (!rc) f->phase = 3;
        return rc;
    }
    if (!rc) f->phase =
            f->status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE ? 2 : 1;
    return rc;
}

static void
run(fixture_t *f)
{
    /* Actual two-job FIFO, with the witness already queued at each yield. */
    unsigned queue[4] = {0, 1, 0, 0}, head = 0, count = 2;
    while (count) {
        unsigned job = queue[head]; head = (head + 1) % 4; --count;
        if (!job) {
            CHECK(f->turns == f->witnesses);
            CHECK(++f->turns < 10000);
            f->protocol = dispatch(f);
            CHECK(!f->protocol);
        } else {
            CHECK(++f->witnesses == f->turns);
        }
        if (f->phase != 3) {
            CHECK(count < 4);
            queue[(head + count) % 4] = job; ++count;
        }
    }
    CHECK(f->witnesses == f->turns && f->turns > 1);
}

static void
fixture_close(fixture_t *f)
{
    CHECK(f->phase == 3);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    CHECK(!f->session.cleanup_active_count &&
        !f->session.cleanup_reserved_bytes);
    int result = wl_evaluation_control_active_attempt_id(f->operation) ==
        f->operation_id ? f->status.semantic_result : 0;
    if (wl_evaluation_control_active_attempt_id(f->operation))
        CHECK(wl_evaluation_control_finish(f->operation, &f->operation_owner,
            result, 0) == result);
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f->operation, &f->operation_owner));
    CHECK(!wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->operation);
    wl_evaluation_control_release(f->funding);
    if (!f->owns_left) col_rel_destroy(f->left);
    for (unsigned i = 0; i < 2; ++i) col_rel_destroy(f->registry[i]);
    free(f->session.rel_hash_head); free(f->session.rel_hash_next);
}

static void
compare_raw(fixture_t *f)
{
    wl_col_session_t raw = {0};
    col_rel_t *left = NULL, *right[2] = {NULL, NULL};
    CHECK(!wl_columnar_relation_deep_copy_governed(f->left, &left, NULL));
    for (unsigned i = 0; i < 2; ++i)
        if (f->registry[i]) CHECK(!wl_columnar_relation_deep_copy_governed(
                    f->registry[i], &right[i], NULL));
    raw.rels = right; raw.nrels = f->session.nrels;
    raw.delta_seeded = f->session.delta_seeded;
    wl_mem_ledger_init(&raw.mem_ledger, 0);
    eval_stack_t stack = {0};
    eval_stack_t *actual = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(!eval_stack_push_delta(&stack, left, false,
        actual->items[0].is_delta));
    CHECK(!col_op_join(&f->op, &stack, &raw));
    run(f);
    CHECK(!f->status.semantic_result && f->status.committed);
    CHECK(actual->top == 1 && stack.top == 1);
    CHECK(actual->items[0].is_delta == stack.items[0].is_delta);
    col_rel_t *a = actual->items[0].rel, *b = stack.items[0].rel;
    CHECK(a->nrows == b->nrows && a->ncols == b->ncols);
    CHECK(!a->timestamps && !a->has_graph_column);
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK(a->column_types[c] == b->column_types[c]);
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
    }
    /* Raw JOIN may traverse hash chains in reverse physical order. Compare
     * complete tuples and duplicate multiplicities outside the action. */
    for (uint32_t r = 0; r < a->nrows; ++r) {
        uint32_t actual_count = 0, expected_count = 0;
        for (uint32_t other = 0; other < a->nrows; ++other) {
            bool actual_equal = true, expected_equal = true;
            for (uint32_t c = 0; c < a->ncols; ++c) {
                actual_equal &= a->columns[c][r] == a->columns[c][other];
                expected_equal &= a->columns[c][r] == b->columns[c][other];
            }
            actual_count += actual_equal; expected_count += expected_equal;
        }
        CHECK(actual_count == expected_count);
    }
    CHECK(stack.items[0].owned);
    col_rel_destroy(b); col_rel_destroy(left);
    for (unsigned i = 0; i < 2; ++i) col_rel_destroy(right[i]);
    free(raw.rel_hash_head); free(raw.rel_hash_next);
}

static void
parity_cases(void)
{
    const uint32_t widths[] = {0, 1, 17, 129};
    for (unsigned i = 0; i < 4; ++i) {
        fixture_t f; fixture_init(&f, widths[i], 3, 5, 0);
        compare_raw(&f); fixture_close(&f);
    }
    fixture_t growth; fixture_init(&growth, 17, 65, 2, 0);
    compare_raw(&growth); fixture_close(&growth);
    fixture_t typed;
    fixture_init(&typed, 7, 0, 0, 0);
    wirelog_column_type_t types[7];
    for (unsigned c = 0; c < 7; ++c) types[c] = (wirelog_column_type_t)c;
    CHECK(!col_rel_set_column_types(typed.left, types, 7));
    CHECK(!col_rel_set_column_types(typed.registry[0], types, 7));
    for (unsigned i = 0; i < 2; ++i) {
        col_rel_t *rel = i ? typed.registry[0] : typed.left;
        for (uint32_t r = 0; r < (i ? 5u : 3u); ++r) {
            int64_t values[7];
            for (unsigned c = 0; c < 7; ++c) values[c] = (int64_t)r + c;
            double value = (double)r + 0.5;
            memcpy(&values[WIRELOG_TYPE_FLOAT], &value, sizeof(value));
            CHECK(!col_rel_append_row(rel, values));
        }
    }
    compare_raw(&typed); fixture_close(&typed);
    for (unsigned mode = 0; mode < 6; ++mode)
        for (unsigned flags = 0; flags < 16; ++flags) {
            fixture_t f; fixture_init(&f, 2, flags & 1 ? 0 : 3, 5, 0);
            f.op.delta_mode = mode;
            f.session.delta_seeded = (flags & 2) != 0;
            wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].is_delta =
                (flags & 4) != 0;
            f.registry[1] = relation_new("$d$right", 2, flags & 8 ? 2 : 0);
            f.session.nrels = 2;
            compare_raw(&f); fixture_close(&f);
        }
    for (unsigned mode = 0; mode < 2; ++mode) {
        fixture_t f; fixture_init(&f, 2, 3, 5, 0);
        uint32_t project[] = {3, 0, UINT32_MAX};
        f.op.project_count = 3; f.op.project_indices = project;
        if (mode) f.op.right_relation = "missing";
        compare_raw(&f); fixture_close(&f);
    }
}

static void
generated_case(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl left(x: int64)\n.decl right(y: int64)\n"
        ".decl output(x: int64, y: int64)\n"
        "output(x, y) :- left(x), right(y).\n", &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    const wl_plan_op_t *join = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r) {
            const wl_plan_relation_t *rel = &plan->strata[s].relations[r];
            if (strcmp(rel->name, "output")) continue;
            for (uint32_t o = 0; o < rel->op_count; ++o)
                if (rel->ops[o].op == WL_PLAN_OP_JOIN) join = &rel->ops[o];
        }
    CHECK(join && !join->key_count);
    fixture_t f; fixture_init(&f, 1, 3, 5, 0);
    f.op = *join;
    if (strcmp(f.registry[0]->name, join->right_relation)) {
        col_rel_destroy(f.registry[0]);
        f.registry[0] = relation_new(join->right_relation, 1, 5);
    }
    compare_raw(&f); fixture_close(&f);
    wl_plan_free(plan); wirelog_program_free(program);
}

static void
limit_cases(void)
{
    const uint64_t limits[] = {1, 191, 192, 383, 384, 575, 576};
    const uint64_t used[] = {0, 0, 192, 192, 384, 384, 576};
    for (unsigned i = 0; i < 7; ++i) {
        fixture_t f; fixture_init(&f, 2, 3, 5, limits[i]);
        run(&f);
        CHECK(f.status.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!f.status.committed);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.left);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            f.status.semantic_result, 0) == f.status.semantic_result);
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.consumed == used[i]);
        fixture_close(&f);
    }
    fixture_t f; fixture_init(&f, 2, 3, 5, 0);
    f.session.join_output_limit = 4;
    run(&f);
    CHECK(f.status.semantic_result == EOVERFLOW && !f.status.committed);
    CHECK(f.session.join_output_peak == 4);
    fixture_close(&f);
}

static void
funding_case(void)
{
    fixture_t f; fixture_init(&f, 2, 3, 5, 0);
    CHECK(!wl_evaluation_control_finish(f.funding, &f.funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f.funding, &f.funding_owner));
    wl_evaluation_control_release(f.funding);
    f.funding = control_new(383, &f.funding_owner);
    f.funding_id = wl_evaluation_control_active_attempt_id(f.funding);
    CHECK(dispatch(&f) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!f.phase);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_report(f.operation, &report));
    CHECK(!report.consumed);
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    f.operation_id = wl_evaluation_control_active_attempt_id(f.operation);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        f.left);
    CHECK(wl_evaluation_control_finish(f.funding, &f.funding_owner,
        WL_EVALUATION_CONTROL_WORK_LIMIT,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(f.funding, &f.funding_owner));
    wl_evaluation_control_release(f.funding);
    f.funding = control_new(0, &f.funding_owner);
    f.funding_id = wl_evaluation_control_active_attempt_id(f.funding);
    compare_raw(&f); fixture_close(&f);
}

static void
retained_case(void)
{
    fixture_t f; fixture_init(&f, 2, 3, 5, 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    stack->items[0].owned = true; f.owns_left = true;
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(f.left, &reader));
    unsigned turns = 0;
    do {
        CHECK(++turns < 1000);
        CHECK(!dispatch(&f));
    } while (f.status.progress != WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    CHECK(f.status.committed && !f.status.execution_result);
    CHECK(f.status.cleanup_result == EBUSY &&
        f.status.semantic_result == EBUSY);
    col_rel_t *published = stack->items[0].rel;
    CHECK(published != f.left && published->nrows == 15);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
    for (unsigned retry = 0; retry < 2; ++retry) {
        CHECK(!dispatch(&f));
        CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
        CHECK(stack->items[0].rel == published && published->nrows == 15);
    }
    CHECK(!col_rel_source_reader_release(&reader));
    run(&f);
    CHECK(f.status.committed && !f.status.semantic_result);
    CHECK(stack->items[0].rel == published && published->nrows == 15);
    fixture_close(&f);
}

static void
governor_cases(void)
{
    const uint64_t budgets[] = {1, 1024u * 1024u};
    for (unsigned i = 0; i < 2; ++i) {
        fixture_t f; fixture_init(&f, 17, 65, 2, 0);
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = budgets[i], .usable_bytes = budgets[i],
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK
        };
        f.session.memory_governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(f.session.memory_governor);
        wl_columnar_memory_governor_ref_t *governor = f.session.memory_governor;
        if (i) compare_raw(&f);
        else {
            run(&f);
            CHECK(f.status.semantic_result == ENOSPC && !f.status.committed);
            CHECK(f.session.memory_budget_denied);
        }
        fixture_close(&f);
        CHECK(!wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

static void
publication_case(void)
{
    fixture_t f; fixture_init(&f, 2, 3, 5, 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    stack->items[0].is_delta = true;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context(f.frame, &ctx));
    unsigned turns = 0;
    do {
        CHECK(++turns < 1000); CHECK(!dispatch(&f));
    } while (ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE);
    CHECK(stack->items[0].rel == f.left && stack->items[0].is_delta);
    wl_columnar_filter_context_t copy = *ctx;
    bool committed = true;
    CHECK(wl_columnar_eval_stack_unary_commit_join_attempt_context_owned(
            f.frame, &copy, &committed, NULL) == EINVAL);
    CHECK(committed && stack->items[0].rel == f.left);
    CHECK(ctx->output && ctx->join_result_is_delta);
    ctx->busy = true;
    CHECK(wl_columnar_eval_stack_unary_commit_join_attempt_context_owned(
            f.frame, ctx, &committed, &f) == EBUSY);
    CHECK(committed && stack->items[0].rel == f.left);
    ctx->busy = false;
    run(&f);
    CHECK(f.status.committed && stack->items[0].is_delta);
    fixture_close(&f);

    fixture_t self; fixture_init(&self, 2, 3, 5, 0);
    col_rel_destroy(self.registry[0]);
    self.registry[0] = self.left; self.op.right_relation = "left";
    compare_raw(&self);
    self.registry[0] = NULL;
    fixture_close(&self);
}

static fixture_t *hook_fixture;
static unsigned hook_phase, hook_calls;
static bool hook_restart;

static void
checkpoint(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    (void)progress;
    if (phase != hook_phase || hook_calls) return;
    ++hook_calls;
    CHECK(control == hook_fixture->operation);
    if (hook_restart) {
        CHECK(!wl_evaluation_control_finish(control,
            &hook_fixture->operation_owner, 0, 0));
        CHECK(!wl_evaluation_control_begin(control,
            &hook_fixture->operation_owner));
    } else wl_evaluation_control_request_cancel(control);
}

static void
interrupt_cases(void)
{
    const unsigned phases[] = {1, 2, 3, 5, 6, 7, 8, 9, 11, 13, 15, 17, 18, 20};
    for (unsigned restart = 0; restart < 2; ++restart)
        for (unsigned i = 0; i < sizeof(phases) / sizeof(phases[0]); ++i) {
            fixture_t f; fixture_init(&f, 2, 3, 5, 0);
            hook_fixture = &f; hook_phase = phases[i]; hook_calls = 0;
            hook_restart = restart != 0;
            wl_columnar_join_test_attempt_checkpoint = checkpoint;
            run(&f);
            wl_columnar_join_test_attempt_checkpoint = NULL;
            CHECK(hook_calls == 1 && !f.status.committed);
            CHECK(f.status.semantic_result == (restart ? EINVAL :
                WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
                f.left);
            wl_evaluation_control_report_t report;
            int result = restart ? 0 : f.status.semantic_result;
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                result, 0) == result);
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            if (restart) CHECK(!report.consumed);
            fixture_close(&f);
        }
    fixture_t f; fixture_init(&f, 2, 3, 5, 0);
    wl_columnar_join_test_attempt_fail_allocation = 0;
    run(&f);
    wl_columnar_join_test_attempt_fail_allocation = -1;
    CHECK(f.status.semantic_result == ENOMEM && !f.status.committed);
    fixture_close(&f);
}

int
main(void)
{
    parity_cases(); generated_case(); limit_cases(); funding_case();
    retained_case(); governor_cases(); publication_case(); interrupt_cases();
    puts("ordinary cross JOIN raw parity and work-limit cleanup passed");
    return 0;
}
