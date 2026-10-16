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
    wl_columnar_eval_plan_numeric_attempt_t *claimed;
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
        if (f->op.op == WL_PLAN_OP_SEMIJOIN || f->op.op == WL_PLAN_OP_ANTIJOIN)
            rc = wl_columnar_join_probe_op_attempt_start_owned(&f->op,
                    f->frame, &f->session, f->operation, f->operation_id,
                    &f->status, f->claimed);
        else
            rc = wl_columnar_join_op_attempt_start(&f->op, f->frame,
                    &f->session,
                    f->operation, f->operation_id, &f->status);
    } else if (f->phase == 1) {
        rc = wl_columnar_filter_op_attempt_step_owned(f->frame, &f->status,
                f->claimed);
    } else {
        rc = wl_columnar_filter_op_attempt_ack_owned(f->frame, f->claimed);
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

/* Test teardown is an external, unbounded session-owner action. Raw JOIN may
 * cache primary arrangements; those caches do not belong to its output. */
static void
raw_registry_clear(wl_col_session_t *session)
{
    for (uint32_t i = 0; i < session->arr_count; ++i) {
        CHECK(!session->arr_entries[i].pin_count);
        free(session->arr_entries[i].rel_name);
        free(session->arr_entries[i].key_cols);
        arr_free_contents(&session->arr_entries[i].arr);
        col_arr_detach_memory_governor(&session->arr_entries[i].arr);
    }
    free(session->arr_entries);
    session->arr_entries = NULL; session->arr_count = 0; session->arr_cap = 0;
    session_rel_free_hash(session);
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
    raw_registry_clear(&f->session);
    if (!f->owns_left) col_rel_destroy(f->left);
    for (unsigned i = 0; i < 2; ++i) col_rel_destroy(f->registry[i]);
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
    raw_registry_clear(&raw);
    col_rel_destroy(b); col_rel_destroy(left);
    for (unsigned i = 0; i < 2; ++i) col_rel_destroy(right[i]);
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
generated_case(bool keyed)
{
    wirelog_error_t error;
    const char *text = keyed
        ? ".decl left(x: int64, y: int64)\n.decl right(x: int64, z: int64)\n"
        ".decl output(x: int64, y: int64, z: int64)\n"
        "output(x, y, z) :- left(x, y), right(x, z).\n"
        : ".decl left(x: int64)\n.decl right(y: int64)\n"
        ".decl output(x: int64, y: int64)\n"
        "output(x, y) :- left(x), right(y).\n";
    wirelog_program_t *program = wirelog_parse_string(text, &error);
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
    CHECK(join && (join->key_count != 0) == keyed);
    fixture_t f; fixture_init(&f, keyed ? 2 : 1, 3, 5, 0);
    f.op = *join;
    if (strcmp(f.registry[0]->name, join->right_relation)) {
        col_rel_destroy(f.registry[0]);
        f.registry[0] = relation_new(join->right_relation, keyed ? 2 : 1, 5);
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

static void
keyed_cases(void)
{
    /* Later keys must discriminate even when the complete hashes collide in
     * the same bucket. Swap key positions across the two input schemas. */
    fixture_t composite; fixture_init(&composite, 2, 3, 4, 0);
    const char *left_keys[] = {"col0", "col1"};
    const char *right_keys[] = {"col1", "col0"};
    uint32_t prefix = wl_columnar_hash_value(2166136261u, composite.left, 0, 1);
    uint32_t composite_bucket = wl_columnar_hash_value(prefix, composite.left,
            1, 0) & 7u;
    int64_t composite_collision = 1;
    while ((wl_columnar_hash_value(prefix, composite.left, 1,
        composite_collision) & 7u)
        != composite_bucket) ++composite_collision;
    for (uint32_t r = 0; r < 3; ++r) {
        composite.left->columns[0][r] = 1;
        composite.left->columns[1][r] = r ? composite_collision : 0;
    }
    for (uint32_t r = 0; r < 4; ++r) {
        composite.registry[0]->columns[0][r] = 0;
        composite.registry[0]->columns[1][r] = 1;
    }
    composite.op.key_count = 2; composite.op.left_keys = left_keys;
    composite.op.right_keys = right_keys;
    compare_raw(&composite);
    col_rel_t *matched = wl_columnar_eval_stack_cleanup_stack(
        composite.frame)->items[0].rel;
    CHECK(matched->nrows == 4);
    for (uint32_t r = 0; r < matched->nrows; ++r)
        CHECK(matched->columns[0][r] == 1 && matched->columns[1][r] == 0 &&
            matched->columns[2][r] == 0 && matched->columns[3][r] == 1);
    fixture_close(&composite);
    const unsigned counts[] = {1, 2, 17, 129};
    const char *keys[129];
    for (unsigned key = 0; key < 129;
        ++key) keys[key] = key & 1 ? "col16" : "col0";
    for (unsigned i = 0; i < 4; ++i) {
        fixture_t f; fixture_init(&f, 17, 3, 5, 0);
        f.op.key_count = counts[i]; f.op.left_keys = keys;
        f.op.right_keys = keys;
        compare_raw(&f); fixture_close(&f);
    }
    fixture_t duplicates; fixture_init(&duplicates, 2, 65, 2, 0);
    for (uint32_t r = 0; r < 65; ++r) duplicates.left->columns[0][r] = 1;
    for (uint32_t r = 0; r < 2; ++r) duplicates.registry[0]->columns[0][r] = 1;
    duplicates.op.key_count = 1;
    compare_raw(&duplicates); fixture_close(&duplicates);
    fixture_t projection; fixture_init(&projection, 17, 3, 5, 0);
    uint32_t indices[] = {33, 0, UINT32_MAX};
    projection.op.key_count = 1;
    projection.op.project_count = 3; projection.op.project_indices = indices;
    compare_raw(&projection); fixture_close(&projection);

    fixture_t no_match; fixture_init(&no_match, 2, 3, 5, 0);
    uint32_t bucket = wl_columnar_hash_value(2166136261u, no_match.left, 0,
            0) & 15u;
    int64_t collision = 1;
    while ((wl_columnar_hash_value(2166136261u, no_match.left, 0,
        collision) & 15u)
        != bucket) ++collision;
    for (uint32_t r = 0; r < 3; ++r) no_match.left->columns[0][r] = collision;
    for (uint32_t r = 0; r < 5; ++r) no_match.registry[0]->columns[0][r] = 0;
    no_match.op.key_count = 1;
    compare_raw(&no_match);
    CHECK(!wl_columnar_eval_stack_cleanup_stack(
            no_match.frame)->items[0].rel->nrows);
    fixture_close(&no_match);
}

static void
key_grammar_cases(void)
{
    const char *valid[] = {NULL, "unknown", "col", "col ", "col+", "col-7",
                           "col12x", "col12 ", "col0x10", "col0", "col+0",
                           "col-0",
                           "col  +12", "col\t12", "col00012",
                           "col999999999999999999999x",
                           "col-999999999999999999999", "col2147483647"};
    for (unsigned i = 0; i < sizeof(valid) / sizeof(valid[0]); ++i) {
        fixture_t f; fixture_init(&f, 17, 3, 5, 0);
        f.op.key_count = 1; f.op.left_keys = &valid[i];
        f.op.right_keys = &valid[i];
        compare_raw(&f); fixture_close(&f);
    }
    const char *unsupported[] = {"col2147483648", "col4294967297",
                                 "col999999999999999999999", "col\302\240"};
    for (unsigned i = 0; i < 4; ++i) {
        fixture_t f; fixture_init(&f, 2, 3, 5, 0);
        f.op.key_count = 1; f.op.left_keys = &unsupported[i];
        run(&f);
        CHECK(f.status.semantic_result == ENOTSUP && !f.status.committed);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.left);
        fixture_close(&f);
    }
    for (unsigned i = 1; i < 4; ++i) {
        fixture_t f; fixture_init(&f, 2, 3, 5, 0);
        const char *name = unsupported[i];
        for (unsigned side = 0; side < 2; ++side) {
            col_rel_t *rel = side ? f.registry[0] : f.left;
            free(rel->col_names[1]);
            rel->col_names[1] = malloc(strlen(name) + 1);
            CHECK(rel->col_names[1]);
            strcpy(rel->col_names[1], name);
        }
        f.op.key_count = 1; f.op.left_keys = &name; f.op.right_keys = &name;
        compare_raw(&f); fixture_close(&f);
    }
    fixture_t missing_names; fixture_init(&missing_names, 2, 3, 5, 0);
    const char *lk = "col1", *rk = "col0";
    for (unsigned side = 0; side < 2; ++side) {
        col_rel_t *rel = side ? missing_names.registry[0] : missing_names.left;
        for (uint32_t c = 0; c < rel->ncols; ++c) free(rel->col_names[c]);
        free(rel->col_names); rel->col_names = NULL;
    }
    missing_names.op.key_count = 1;
    missing_names.op.left_keys = &lk; missing_names.op.right_keys = &rk;
    run(&missing_names);
    CHECK(!missing_names.status.semantic_result &&
        missing_names.status.committed);
    col_rel_t *out =
        wl_columnar_eval_stack_cleanup_stack(missing_names.frame)->items[0].rel;
    CHECK(out->nrows == 3);
    for (uint32_t r = 0; r < out->nrows;
        ++r) CHECK(out->columns[0][r] == out->columns[2][r]);
    fixture_close(&missing_names);
}

static void
key_empty_and_type_cases(void)
{
    for (unsigned mode = 0; mode < 3; ++mode) {
        fixture_t f; fixture_init(&f, 0, 3, 0, 0);
        f.op.key_count = 1;
        if (mode == 1) f.op.right_relation = "missing";
        if (mode == 2) {
            f.op.delta_mode = WL_DELTA_FORCE_DELTA;
            f.session.delta_seeded = true;
        }
        if (mode) compare_raw(&f);
        else {
            run(&f);
            CHECK(f.status.semantic_result == EINVAL && !f.status.committed);
        }
        fixture_close(&f);
    }
    fixture_t mismatch; fixture_init(&mismatch, 1, 0, 0, 0);
    wirelog_column_type_t floating = WIRELOG_TYPE_FLOAT;
    CHECK(!col_rel_set_column_types(mismatch.left, &floating, 1));
    mismatch.op.key_count = 1;
    run(&mismatch);
    CHECK(mismatch.status.semantic_result == EINVAL &&
        !mismatch.status.committed);
    fixture_close(&mismatch);

    fixture_t integer; fixture_init(&integer, 1, 0, 0, 0);
    wirelog_column_type_t int32 = WIRELOG_TYPE_INT32;
    CHECK(!col_rel_set_column_types(integer.left, &int32, 1));
    int64_t value = 1;
    CHECK(!col_rel_append_row(integer.left, &value));
    CHECK(!col_rel_append_row(integer.registry[0], &value));
    integer.op.key_count = 1;
    compare_raw(&integer); fixture_close(&integer);

    fixture_t zero; fixture_init(&zero, 1, 0, 0, 0);
    CHECK(!col_rel_set_column_types(zero.left, &floating, 1));
    CHECK(!col_rel_set_column_types(zero.registry[0], &floating, 1));
    for (unsigned side = 0; side < 2; ++side) {
        col_rel_t *rel = side ? zero.registry[0] : zero.left;
        int64_t bits = 0;
        CHECK(!col_rel_append_row(rel, &bits));
        CHECK(!col_rel_append_row(rel, &bits));
        double minus_zero = -0.0;
        memcpy(&rel->columns[0][0], &minus_zero, sizeof(minus_zero));
    }
    zero.op.key_count = 1;
    compare_raw(&zero); fixture_close(&zero);
    for (unsigned side = 0; side < 2; ++side) {
        fixture_t invalid; fixture_init(&invalid, 1, 0, 0, 0);
        CHECK(!col_rel_set_column_types(invalid.left, &floating, 1));
        CHECK(!col_rel_set_column_types(invalid.registry[0], &floating, 1));
        int64_t bits = 0;
        CHECK(!col_rel_append_row(invalid.left, &bits));
        CHECK(!col_rel_append_row(invalid.registry[0], &bits));
        uint64_t infinity = UINT64_C(0x7ff0000000000000);
        col_rel_t *rel = side ? invalid.registry[0] : invalid.left;
        memcpy(&rel->columns[0][0], &infinity, sizeof(infinity));
        invalid.op.key_count = 1;
        run(&invalid);
        CHECK(invalid.status.semantic_result == EINVAL &&
            !invalid.status.committed);
        fixture_close(&invalid);
    }
}

static bool hash_image_sized;

static void
hash_init_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    /* Five right rows choose16 buckets and an84-byte image. Wait until size
     * calculation finishes, then stop after the second initialized slot. */
    if (phase != 26) return;
    if (progress == 84) {
        hash_image_sized = true; return;
    }
    if (hash_image_sized && progress == 1) checkpoint(control, phase, progress);
}

static void
key_governor_cases(void)
{
    const uint64_t budgets[] = {500, 1024u * 1024u};
    for (unsigned i = 0; i < 2; ++i) {
        fixture_t f; fixture_init(&f, 2, 3, 5, 0); f.op.key_count = 1;
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = budgets[i], .usable_bytes = budgets[i],
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor); f.session.memory_governor = governor;
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        CHECK(wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(governor), 128, &peer)
            == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        if (i) compare_raw(&f);
        else {
            run(&f);
            CHECK(f.status.semantic_result == ENOSPC && !f.status.committed);
        }
        fixture_close(&f);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) == 128);
        CHECK(wl_columnar_memory_rollback(&peer));
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

static void
key_literal_prefixes(void)
{
#ifndef _MSC_VER
    /* Independent ungoverned width1/FORCE_FULL recipe, no CAS contention:
    * source =256 INIT+256 BEGIN+2*(96 entry+144 strong CAS)+224 resolve
    * +224 validate+192 snapshot+224 one column chunk =1856 per side.
    * Before key sizing:192 ENTRY+84*192 retirement+10*192 parents
    * +544 unary child+2*1856 source+7*128 lookup =23392.
    * Then192 sizing/parent/init/alloc/key-parent;128 slot/first byte.
    * MSVC's permitted weak-CAS spuriosity has no fixed success prefix. */
    const uint64_t limits[] = {23391, 23392, 23583, 23584, 23775, 23776,
                               23967, 23968, 24159, 24160, 24351, 24352, 24479,
                               24480, 24607, 24608};
    const uint64_t consumed[] = {23200, 23392, 23392, 23584, 23584, 23776,
                                 23776, 23968, 23968, 24160, 24160, 24352,
                                 24352, 24480, 24480, 24608};
    const char *key = "col0";
    for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
        fixture_t f; fixture_init(&f, 1, 3, 5, limits[i]);
        f.op.delta_mode = WL_DELTA_FORCE_FULL; f.op.key_count = 1;
        f.op.left_keys = &key; f.op.right_keys = &key;
        run(&f);
        CHECK(f.status.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!f.status.committed);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.left);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            f.status.semantic_result, 0) == f.status.semantic_result);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.consumed == consumed[i]);
        fixture_close(&f);
    }
#endif
}

/* Independent fixed-width2, ungoverned, typed noTS "$join" constructor
 * schedule. These literals describe the published admission table, without
 * reading production enums or measuring a run. Descriptor bytes use this ABI. */
static uint64_t
typed_constructor_two(void)
{
    return 512u + 2u * 128u /* external typed wrapper */
           + 192u + 6u * 96u /* builder header and relation-name scan */
           + 6u * 96u + 96u + 2u * 96u + 3u * 96u + 128u /* checked sizes */
           + 2u * (96u + 96u + 192u + 3u * 96u) /* generated-name sizes */
           + 96u + 160u + 112u + 224u * ((sizeof(col_rel_t) + 7u) / 8u)
           + 5u * 96u + 112u + 224u + 80u + 112u + 96u + 2u * 160u
           + 112u + 192u + 2u * 112u /* columns and two payload allocations */
           + 112u + 192u + 2u * (96u + 96u + 192u + 112u + 224u) /* names */
           + 112u + 2u * 96u /* type array and two type lanes */
           + 96u + 112u + 224u /* Arrow root */
           + 112u + 192u /* Arrow child-pointer table */
           + 2u * (128u + 112u + 96u + 224u + 96u + 96u + 192u + 112u + 224u)
           + 3u * 128u + 220u; /* unused ungoverned commit calls and install */
}

static void
hash_literal_prefixes(void)
{
#ifndef _MSC_VER
    /* First key's compatibility ends at26208. Width192, scratch1 three192,
     * two type lanes128, constructor parent192 gives27424 before constructor.
     * Two batch parents192 and child170+single(94+19+39+39+45+52+45+21+144)
     * give H=28476+constructor. Five right rows:16 heads+5 next slots,
     * four doublings; each build row192+128 hash+128 link. All candidates
     * are deliberate collisions with unequal keys, so no output fees. */
    const uint64_t h = 28476u + typed_constructor_two();
    const uint64_t boundaries[] = {192, 320, 704, 896, 1088, 1280, 1472, 1600,
                                   4160, 4352, 4480, 4608, 6400, 6592, 6720,
                                   6848, 6976, 7168, 7296};
    const uint64_t predecessors[] = {0, 192, 576, 704, 896, 1088, 1280, 1472,
                                     4032, 4160, 4352, 4480, 6272, 6400, 6592,
                                     6720, 6848, 6976, 7168};
    const char *key = "col0";
    for (unsigned b = 0; b < sizeof(boundaries) / sizeof(boundaries[0]); ++b)
        for (unsigned minus = 0; minus < 2; ++minus) {
            fixture_t f; fixture_init(&f, 1, 3, 5, h + boundaries[b] - minus);
            f.op.delta_mode = WL_DELTA_FORCE_FULL; f.op.key_count = 1;
            f.op.left_keys = &key; f.op.right_keys = &key;
            uint32_t bucket = wl_columnar_hash_value(2166136261u, f.left, 0,
                    0) & 15u;
            int64_t collision = 1;
            while ((wl_columnar_hash_value(2166136261u, f.left, 0,
                collision) & 15u)
                != bucket) ++collision;
            for (uint32_t r = 0; r < 3; ++r) f.left->columns[0][r] = collision;
            for (uint32_t r = 0; r < 5; ++r) f.registry[0]->columns[0][r] = 0;
            run(&f);
            CHECK(f.status.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(!f.status.committed);
            CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
                f.left);
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                f.status.semantic_result, 0) == f.status.semantic_result);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            CHECK(report.consumed ==
                h + (minus ? predecessors[b] : boundaries[b]));
            fixture_close(&f);
        }
#endif
}

static void
key_interrupt_cases(void)
{
    const char *key = "col  +0";
    for (unsigned restart = 0; restart < 2; ++restart)
        for (unsigned phase = 21; phase <= 30; ++phase) {
            fixture_t f; fixture_init(&f, 2, 3, 5, 0);
            f.op.key_count = 1; f.op.left_keys = &key; f.op.right_keys = &key;
            if (phase == 30) {
                uint32_t bucket = wl_columnar_hash_value(2166136261u, f.left, 0,
                        0) & 15u;
                int64_t collision = 1;
                while ((wl_columnar_hash_value(2166136261u, f.left, 0,
                    collision) & 15u)
                    != bucket) ++collision;
                for (uint32_t r = 0; r < 3;
                    ++r) f.left->columns[0][r] = collision;
                for (uint32_t r = 0; r < 5;
                    ++r) f.registry[0]->columns[0][r] = 0;
            }
            hook_fixture = &f; hook_phase = phase; hook_calls = 0;
            hook_restart = restart;
            wl_columnar_join_test_attempt_checkpoint = checkpoint;
            run(&f); wl_columnar_join_test_attempt_checkpoint = NULL;
            CHECK(hook_calls == 1 && !f.status.committed);
            CHECK(f.status.semantic_result ==
                (restart ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
                f.left);
            int result = restart ? 0 : f.status.semantic_result;
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                result, 0) == result);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            if (restart) CHECK(!report.consumed);
            fixture_close(&f);
        }
    for (int ordinal = 0; ordinal < 3; ++ordinal) {
        fixture_t f; fixture_init(&f, 2, 3, 5, 0); f.op.key_count = 1;
        wl_columnar_join_test_attempt_fail_allocation = ordinal;
        run(&f); wl_columnar_join_test_attempt_fail_allocation = -1;
        CHECK(f.status.semantic_result == ENOMEM && !f.status.committed);
        fixture_close(&f);
    }
    fixture_t partial; fixture_init(&partial, 2, 3, 5, 0);
    partial.op.key_count = 1;
    hook_fixture = &partial; hook_phase = 26; hook_calls = 0;
    hook_restart = false;
    hash_image_sized = false;
    wl_columnar_join_test_attempt_checkpoint = hash_init_checkpoint;
    run(&partial); wl_columnar_join_test_attempt_checkpoint = NULL;
    CHECK(hash_image_sized && hook_calls == 1);
    CHECK(partial.status.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!partial.status.committed);
    fixture_close(&partial);
}

static void
standalone_raw_case(void)
{
    fixture_t f; fixture_init(&f, 2, 3, 5, 0);
    CHECK(!wl_columnar_join_op_attempt_start(&f.op, f.frame, &f.session,
        NULL, 0, &f.status));
    CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE);
    CHECK(!f.status.semantic_result && f.status.committed);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel->nrows == 15
        && stack->items[0].rel->ncols == 4);
    f.phase = 3;
    fixture_close(&f);
}

/* Existence operators have distinct literal semantics and raw dispatch.
 * Relation identity/name and retained sort caches of raw keep-all are not
 * logical metadata; following operators still see the same names/types/rows. */
static int
probe_raw(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *session)
{
    return op->op == WL_PLAN_OP_SEMIJOIN
        ? col_op_semijoin(op, stack, session)
        : col_op_antijoin(op, stack, session);
}

static void
probe_compare(fixture_t *f, uint32_t expected_rows)
{
    wl_col_session_t raw = {0};
    col_rel_t *left = NULL, *right[2] = {NULL, NULL};
    CHECK(!wl_columnar_relation_deep_copy_governed(f->left, &left, NULL));
    for (unsigned i = 0; i < 2; ++i)
        if (f->registry[i]) CHECK(!wl_columnar_relation_deep_copy_governed(
                    f->registry[i], &right[i], NULL));
    raw.rels = right; raw.nrels = f->session.nrels;
    raw.join_output_limit = f->session.join_output_limit;
    raw.delta_seeded = f->session.delta_seeded;
    wl_mem_ledger_init(&raw.mem_ledger, 0);
    eval_stack_t stack = {0};
    eval_stack_t *actual = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(!eval_stack_push_delta(&stack, left, false,
        actual->items[0].is_delta));
    CHECK(!probe_raw(&f->op, &stack, &raw));
    run(f);
    CHECK(!f->status.semantic_result && f->status.committed);
    CHECK(actual->top == 1 && stack.top == 1);
    CHECK(!actual->items[0].is_delta && !stack.items[0].is_delta);
    CHECK(!actual->items[0].seg_boundaries && !actual->items[0].seg_count);
    col_rel_t *a = actual->items[0].rel, *b = stack.items[0].rel;
    CHECK(a->nrows == expected_rows && b->nrows == expected_rows);
    CHECK(a->ncols == b->ncols && !a->timestamps && !b->timestamps);
    CHECK((a->column_types != NULL) == (b->column_types != NULL));
    CHECK(a->has_graph_column == b->has_graph_column &&
        a->graph_col_idx == b->graph_col_idx &&
        a->declared_ncols == b->declared_ncols);
    for (uint32_t c = 0; c < a->ncols; ++c) {
        if (a->column_types) CHECK(a->column_types[c] == b->column_types[c]);
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
        for (uint32_t r = 0; r < expected_rows; ++r)
            CHECK(a->columns[c][r] == b->columns[c][r]);
    }
    CHECK(!f->session.join_output_peak && !raw.join_output_peak);
    CHECK(!eval_stack_drain(&stack));
    col_rel_destroy(left);
    for (unsigned i = 0; i < 2; ++i) col_rel_destroy(right[i]);
    raw_registry_clear(&raw);
}

static void
probe_semantics(void)
{
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned typed = 0; typed < 2; ++typed)
            for (unsigned empty = 0; empty < 4; ++empty) {
                fixture_t f; fixture_init(&f, 2, empty & 1 ? 0 : 4,
                    empty & 2 ? 0 : 4, 0);
                f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
                f.op.key_count = 1;
                if (anti) f.op.project_count = 99; /* ANTI ignores even absent projection indices. */
                if (typed) {
                    f.owns_left = true;
                    wl_columnar_eval_stack_cleanup_stack(
                        f.frame)->items[0].owned = true;
                    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                                     WIRELOG_TYPE_INT32};
                    CHECK(!col_rel_set_column_types(f.left, types, 2));
                }
                const int64_t keys[] = {0, 1, 1, 3};
                for (uint32_t r = 0; r < f.left->nrows; ++r) {
                    f.left->columns[0][r] = keys[r];
                    f.left->columns[1][r] = 10 + keys[r];
                }
                for (uint32_t r = 0; r < f.registry[0]->nrows; ++r)
                    f.registry[0]->columns[0][r] = 1;
                f.session.join_output_limit = 1;
                f.session.delta_seeded = true;
                f.op.delta_mode = WL_DELTA_FORCE_DELTA;
                f.registry[1] = relation_new("$d$right", 2, 1);
                f.registry[1]->columns[0][0] = 3;
                f.session.nrels = 2;
                eval_stack_t *stack =
                    wl_columnar_eval_stack_cleanup_stack(f.frame);
                stack->items[0].is_delta = true;
                stack->items[0].seg_count = 1;
                stack->items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
                CHECK(stack->items[0].seg_boundaries);
                stack->items[0].seg_boundaries[0] = 0;
                stack->items[0].seg_boundaries[1] = f.left->nrows;
                uint32_t rows = empty & 1 ? 0 : empty & 2 ? anti ? 4 : 0 : 2;
                probe_compare(&f, rows);
                col_rel_t *out = stack->items[0].rel;
                CHECK(out->ncols == 2 && (out->column_types != NULL) ==
                    (!anti || typed));
                for (uint32_t r = 0; r < rows; ++r) {
                    int64_t key = empty & 2 ? keys[r] : anti ? r ? 3 : 0 : 1;
                    CHECK(out->columns[0][r] == key &&
                        out->columns[1][r] == 10 + key);
                }
                fixture_close(&f);
            }
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned width_case = 0; width_case < 3; ++width_case)
            for (unsigned right_empty = 0; right_empty < 2; ++right_empty) {
                uint32_t width = width_case == 0 ? 0 : width_case ==
                    1 ? 1 : 129;
                fixture_t f; fixture_init(&f, width, 3, right_empty ? 0 : 5, 0);
                f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
                probe_compare(&f, (anti == (right_empty != 0)) ? 3 : 0);
                col_rel_t *out =
                    wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
                CHECK(out->ncols == width &&
                    (out->column_types != NULL) == (!anti && width));
                fixture_close(&f);
            }
}

static void
probe_metadata(void)
{
    const wirelog_column_type_t types[] = {WIRELOG_TYPE_INT32,
                                           WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_UINT32,
                                           WIRELOG_TYPE_UINT64,
                                           WIRELOG_TYPE_FLOAT,
                                           WIRELOG_TYPE_STRING,
                                           WIRELOG_TYPE_BOOL};
    for (unsigned anti = 0; anti < 2; ++anti) {
        fixture_t f; fixture_init(&f, 7, 0, 1, 0);
        f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
        f.op.key_count = anti ? 1 : 0;
        CHECK(!col_rel_set_column_types(f.left, types, 7));
        for (uint32_t r = 0; r < 3; ++r) {
            int64_t values[] = {r + 1, r + 1, r + 2, r + 3,
                                wl_columnar_float_to_bits(-2.5 + r), r + 5,
                                r % 2};
            CHECK(!col_rel_append_row(f.left, values));
        }
        uint32_t projection[] = {4, 0, 6, 4, 99, 5, 2, 3, 1};
        f.op.project_count = 9; f.op.project_indices = projection;
        if (anti) {
            for (uint32_t c = 0; c < 7; ++c) {
                free(f.left->col_names[c]); f.left->col_names[c] = malloc(20);
                CHECK(f.left->col_names[c]);
                snprintf(f.left->col_names[c], 20, "field%u", c);
            }
            f.left->has_graph_column = true; f.left->graph_col_idx = 6;
            f.left->declared_ncols = 6;
        }
        probe_compare(&f, 3);
        col_rel_t *out =
            wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
        CHECK(out->ncols == (anti ? 7u : 9u));
        for (uint32_t c = 0; c < out->ncols; ++c) {
            uint32_t source = anti ? c : projection[c];
            CHECK(out->column_types[c] ==
                (source < 7 ? types[source] : WIRELOG_TYPE_INT64));
            char name[20];
            snprintf(name, sizeof(name), anti ? "field%u" : "col%u", c);
            CHECK(!strcmp(out->col_names[c], name));
            for (uint32_t r = 0; r < 3; ++r)
                CHECK(out->columns[c][r] ==
                    (source < 7 ? f.left->columns[source][r] : 0));
        }
        fixture_close(&f);
    }
    /* Canonical typed input reaches raw's keep-all shortcut; controlled
     * materialization must retain logical metadata and clear entry delta. */
    fixture_t f; fixture_init(&f, 2, 0, 1, 0);
    f.op.op = WL_PLAN_OP_SEMIJOIN;
    wirelog_column_type_t typed[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT};
    CHECK(!col_rel_set_column_types(f.left, typed, 2));
    for (uint32_t r = 0; r < 3; ++r) {
        int64_t values[] = {r, wl_columnar_float_to_bits(-3.0 + r)};
        CHECK(!col_rel_append_row(f.left, values));
    }
    probe_compare(&f, 3); fixture_close(&f);
}

static void
probe_float_keys(void)
{
    const int64_t integer[] = {1, 1, 2, 2};
    const double floating[] = {-0.0, 0.0, -2.5, 9.0};
    const char *keys[] = {"col0", "col1"};
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT,
                                     WIRELOG_TYPE_FLOAT};
    for (unsigned anti = 0; anti < 2; ++anti) {
        fixture_t f; fixture_init(&f, 3, 0, 0, 0);
        f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
        f.op.key_count = 2; f.op.left_keys = keys; f.op.right_keys = keys;
        CHECK(!col_rel_set_column_types(f.left, types, 3));
        CHECK(!col_rel_set_column_types(f.registry[0], types, 3));
        for (uint32_t r = 0; r < 4; ++r) {
            int64_t values[] = {integer[r],
                                wl_columnar_float_to_bits(floating[r]),
                                wl_columnar_float_to_bits(-3.0 + r)};
            CHECK(!col_rel_append_row(f.left, values));
        }
        for (uint32_t r = 0; r < 3; ++r) {
            int64_t values[] = {integer[r],
                                wl_columnar_float_to_bits(r < 2 ? 0.0 : -2.5),
                                wl_columnar_float_to_bits(0.0)};
            CHECK(!col_rel_append_row(f.registry[0], values));
        }
        /* Append canonicalizes zero; restore a valid negative-zero key to
        * exercise mixed signed-zero hashing/equality against right +0. */
        f.left->columns[1][0] = wl_columnar_float_to_bits(-0.0);
        probe_compare(&f, anti ? 1 : 3);
        col_rel_t *out =
            wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
        for (uint32_t r = 0; r < out->nrows; ++r) {
            uint32_t source = anti ? 3 : r;
            CHECK(out->columns[0][r] == integer[source]);
            CHECK(out->columns[1][r] ==
                wl_columnar_float_to_bits(source < 2 ? 0.0 : floating[source]));
            CHECK(out->columns[2][r] ==
                wl_columnar_float_to_bits(-3.0 + source));
            CHECK(out->column_types[1] == WIRELOG_TYPE_FLOAT);
        }
        fixture_close(&f);
    }
}

static void
probe_rejections(void)
{
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned mode = 0; mode < 11; ++mode) {
            fixture_t f; fixture_init(&f, mode == 1 ? 0 : 2, 3, 5, 0);
            f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
            if (mode == 0) f.op.right_relation = "missing";
            if (mode == 1) f.op.key_count = 1;
            if (mode == 2) CHECK(!col_rel_enable_timestamps(f.left));
            if (mode == 3) CHECK(!col_rel_enable_timestamps(f.registry[0]));
            if (mode == 4) f.op.materialized = true;
            uint8_t expression[9] = {WL_PLAN_EXPR_CONST_INT};
            if (mode ==
                5) f.op.right_filter_expr = (wl_plan_expr_buffer_t){expression,
                                                                    sizeof(
                                                                        expression)};
            if (mode == 6) f.session.diff_operators_active = true;
            if (mode == 7) f.session.tdd_subpass_active = true;
            if (mode == 8) f.session.join_batch_bytes = 1;
            if (mode == 9) f.session.num_workers = 2;
            if (mode == 10) f.left->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
            eval_entry_t before =
                wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0];
            run(&f);
            CHECK(f.status.semantic_result == (mode == 1 ? EINVAL : ENOTSUP));
            CHECK(!f.status.committed && !f.session.memory_budget_denied);
            const eval_entry_t *after =
                &wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0];
            CHECK(after->rel == before.rel && after->owned == before.owned &&
                after->is_delta == before.is_delta &&
                after->kind == before.kind &&
                after->continuation == before.continuation &&
                after->seg_boundaries == before.seg_boundaries &&
                after->seg_count == before.seg_count);
            if (mode == 10) f.left->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
            fixture_close(&f);
        }
    fixture_t malformed; fixture_init(&malformed, 2, 3, 5, 0);
    malformed.op.op = WL_PLAN_OP_SEMIJOIN; malformed.op.project_count = 1;
    run(&malformed);
    CHECK(malformed.status.semantic_result == ENOTSUP &&
        !malformed.status.committed);
    CHECK(wl_columnar_eval_stack_cleanup_stack(malformed.frame)->items[0].rel ==
        malformed.left);
    fixture_close(&malformed);
    for (unsigned anti = 0; anti < 2; ++anti) {
        fixture_t f; fixture_init(&f, 2, 0, 5, 0);
        f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
        f.op.key_count = 1;
        wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT,
                                         WIRELOG_TYPE_INT64};
        CHECK(!col_rel_set_column_types(f.left, types, 2));
        for (uint32_t r = 0; r < 3; ++r) {
            int64_t values[] = {wl_columnar_float_to_bits((double)r), r + 1};
            CHECK(!col_rel_append_row(f.left, values));
        }
        run(&f);
        CHECK(f.status.semantic_result == EINVAL && !f.status.committed);
        fixture_close(&f);
        fixture_init(&f, 2, 3, 5, 0);
        f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
        CHECK(!wl_evaluation_control_charge(f.operation, UINT64_MAX - 64));
        run(&f);
        CHECK(f.status.semantic_result == EOVERFLOW && !f.status.committed);
        fixture_close(&f);
        fixture_init(&f, 2, 3, 5, 0);
        f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
        f.op.right_relation = "missing";
        CHECK(!wl_columnar_join_probe_op_attempt_start(&f.op, f.frame,
            &f.session, NULL, 0, &f.status));
        CHECK(f.status.committed && !f.status.semantic_result &&
            wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.left);
        f.phase = 3; fixture_close(&f);
    }
}

static void
probe_interrupts(void)
{
    const unsigned phases[] = {1, 2, 3, 5, 6, 7, 8, 9, 11, 13, 15, 17, 18, 20,
                               21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
    const char *key = "col  +0";
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned restart = 0; restart < 2; ++restart)
            for (unsigned p = 0; p < sizeof(phases) / sizeof(phases[0]); ++p) {
                if (anti && phases[p] == 13) continue;
                fixture_t f; fixture_init(&f, 2, 3, 5, 0);
                f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
                f.op.key_count = 1; f.op.left_keys = &key;
                f.op.right_keys = &key;
                for (uint32_t r = 0; r < 5;
                    ++r) f.registry[0]->columns[0][r] += 2;
                hook_fixture = &f; hook_phase = phases[p]; hook_calls = 0;
                hook_restart = restart != 0;
                wl_columnar_join_test_attempt_checkpoint = checkpoint;
                run(&f); wl_columnar_join_test_attempt_checkpoint = NULL;
                CHECK(hook_calls == 1 && !f.status.committed);
                CHECK(f.status.semantic_result ==
                    (restart ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
                CHECK(wl_columnar_eval_stack_cleanup_stack(
                        f.frame)->items[0].rel == f.left);
                wl_evaluation_control_report_t report;
                int result = restart ? 0 : f.status.semantic_result;
                CHECK(wl_evaluation_control_finish(f.operation,
                    &f.operation_owner, result, 0) == result);
                CHECK(!wl_evaluation_control_report(f.operation, &report));
                if (restart) CHECK(!report.consumed);
                fixture_close(&f);
            }
}

static void
probe_allocations_and_quota(void)
{
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned family = 0; family < 2; ++family) {
            unsigned reached = 0;
            for (unsigned ordinal = 0; !ordinal || ordinal <= reached;
                ++ordinal){
                fixture_t f; fixture_init(&f, 2, 3, 5, 0);
                f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
                f.op.key_count = 1;
                if (!family) wl_columnar_join_test_attempt_fail_allocation =
                        ordinal ? (int)ordinal - 1 : 10000;
                else wl_columnar_relation_test_constructor_fail_allocation =
                        ordinal ? ordinal : UINT32_MAX;
                run(&f);
                if (!ordinal) {
                    reached = family ? UINT32_MAX -
                        wl_columnar_relation_test_constructor_fail_allocation
                        : (unsigned)(10000 -
                        wl_columnar_join_test_attempt_fail_allocation);
                    CHECK(reached && !f.status.semantic_result);
                } else CHECK(f.status.semantic_result == ENOMEM &&
                        !f.status.committed);
                wl_columnar_join_test_attempt_fail_allocation = -1;
                wl_columnar_relation_test_constructor_fail_allocation = 0;
                fixture_close(&f);
            }
        }
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned poor = 0; poor < 2; ++poor) {
            fixture_t f; fixture_init(&f, 17, 65, 3, 0);
            f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
            f.op.key_count = 1;
            wl_columnar_memory_resolution_t resolution =
            {.budget_bytes = poor ? 1 : 1u << 20,
                    .usable_bytes =
                    poor ? 1 : 1u << 20,
                    .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                    .source =
                    WL_COLUMNAR_MEMORY_SOURCE_ENV,
                    .status = WL_COLUMNAR_MEMORY_OK};
            wl_columnar_memory_governor_ref_t *governor =
                wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor); f.session.memory_governor = governor;
            if (!poor) probe_compare(&f, anti ? 62 : 3);
            else {
                run(&f);
                CHECK(f.status.semantic_result == ENOSPC &&
                    !f.status.committed && f.session.memory_budget_denied);
            }
            fixture_close(&f);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
        }
}

/* Written prefix schedule: original entry192 +84 lifecycle payments. These
 * boundaries do not obtain a limit from a measured operation total. */
static void
probe_literal_prefixes(void)
{
    const uint64_t limits[] = {191, 192, 383, 384, 16127, 16128, 16319, 16320};
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
            fixture_t f; fixture_init(&f, 2, 3, 5, limits[i]);
            f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
            run(&f);
            CHECK(f.status.semantic_result ==
                WL_EVALUATION_CONTROL_WORK_LIMIT && !f.status.committed);
            CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
                f.status.semantic_result, 0) == f.status.semantic_result);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.operation, &report));
            CHECK(report.consumed == limits[i] / 192u * 192u);
            fixture_close(&f);
        }
#ifndef _MSC_VER
    /* Two-lane probe output: SEMI's ten-byte name adds4*96 scan+224 copy.
     * ANTI additionally substitutes512 for typed wrapper768, selects supplied
     * five-byte names3times/lane (+6*192), and skips2type-fill128s.
     * Pre-hash H otherwise follows the written source/key/scratch/B2 prefix. */
    const uint64_t boundaries[] = {192, 320, 704, 896, 1088, 1280, 1472, 1600,
                                   4160, 4352, 4480, 4608, 6400, 6592, 6720,
                                   6848, 6976, 7168, 7296};
    const uint64_t predecessors[] = {0, 192, 576, 704, 896, 1088, 1280, 1472,
                                     4032, 4160, 4352, 4480, 6272, 6400, 6592,
                                     6720, 6848, 6976, 7168};
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned b = 0; b < sizeof(boundaries) / sizeof(boundaries[0]);
            ++b)
            for (unsigned minus = 0; minus < 2; ++minus) {
                uint64_t h = 28476u + typed_constructor_two() +
                    (anti ? 1248u : 608u);
                fixture_t f;
                fixture_init(&f, 2, 3, 5, h + boundaries[b] - minus);
                f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
                f.op.key_count = 1;
                const char *key = "col0"; f.op.left_keys = &key;
                f.op.right_keys = &key;
                wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                                 WIRELOG_TYPE_INT64};
                CHECK(!col_rel_set_column_types(f.left, types, 2));
                uint32_t bucket = wl_columnar_hash_value(2166136261u, f.left, 0,
                        0) & 15u;
                int64_t collision = 1;
                while ((wl_columnar_hash_value(2166136261u, f.left, 0,
                    collision) & 15u) != bucket) ++collision;
                for (uint32_t r = 0; r < 3;
                    ++r) f.left->columns[0][r] = collision;
                for (uint32_t r = 0; r < 5;
                    ++r) f.registry[0]->columns[0][r] = 0;
                if (anti) f.registry[0]->columns[0][0] = collision; /* last chain candidate matches */
                run(&f);
                CHECK(f.status.semantic_result ==
                    WL_EVALUATION_CONTROL_WORK_LIMIT && !f.status.committed);
                CHECK(wl_evaluation_control_finish(f.operation,
                    &f.operation_owner, f.status.semantic_result,
                    0) == f.status.semantic_result);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.operation, &report));
                CHECK(report.consumed ==
                    h + (minus ? predecessors[b] : boundaries[b]));
                fixture_close(&f);
            }
#endif
}

static unsigned probe_candidate_visits;
static bool probe_cancel_last;

static void
probe_no_output_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (phase != 17) return;
    ++probe_candidate_visits;
    if (probe_cancel_last && progress == 0) {
        wl_columnar_filter_context_t *ctx = NULL;
        CHECK(control == hook_fixture->operation);
        CHECK(!wl_columnar_eval_stack_filter_context(hook_fixture->frame,
            &ctx));
        CHECK(ctx->output && !ctx->output->nrows &&
            probe_candidate_visits == 257);
        wl_evaluation_control_request_cancel(control);
    }
}

static void
probe_no_output_work(void)
{
    for (unsigned anti = 0; anti < 2; ++anti)
        for (unsigned early = 0; early < 2; ++early) {
            fixture_t f; fixture_init(&f, 2, 3, 257, 0);
            f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
            f.op.key_count = 1;
            uint32_t bucket = wl_columnar_hash_value(2166136261u, f.left, 0,
                    0) & 1023u;
            int64_t collision = 1;
            while ((wl_columnar_hash_value(2166136261u, f.left, 0,
                collision) & 1023u) != bucket) ++collision;
            for (uint32_t r = 0; r < 3; ++r) f.left->columns[0][r] = collision;
            for (uint32_t r = 0; r < 257; ++r) f.registry[0]->columns[0][r] = 0;
            if (early) f.registry[0]->columns[0][256] = collision;
            else if (anti) f.registry[0]->columns[0][0] = collision;
            hook_fixture = &f; probe_candidate_visits = 0;
            probe_cancel_last = !early;
            wl_columnar_join_test_attempt_checkpoint =
                probe_no_output_checkpoint;
            run(&f);
            wl_columnar_join_test_attempt_checkpoint = NULL;
            if (!early) {
                CHECK(probe_candidate_visits == 257 && !f.status.committed &&
                    f.status.semantic_result ==
                    WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(wl_columnar_eval_stack_cleanup_stack(
                        f.frame)->items[0].rel == f.left);
                CHECK(wl_evaluation_control_finish(f.operation,
                    &f.operation_owner,
                    WL_EVALUATION_CONTROL_CANCELLED,
                    0) == WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(!wl_evaluation_control_reset(f.operation));
                CHECK(!wl_evaluation_control_begin(f.operation,
                    &f.operation_owner));
                f.operation_id =
                    wl_evaluation_control_active_attempt_id(f.operation);
                f.phase = 0; probe_cancel_last = false;
                probe_candidate_visits = 0;
                wl_columnar_join_test_attempt_checkpoint =
                    probe_no_output_checkpoint;
                probe_compare(&f, 0);
                wl_columnar_join_test_attempt_checkpoint = NULL;
                CHECK(probe_candidate_visits == 3u * 257u);
            } else {
                CHECK(f.status.committed && !f.status.semantic_result &&
                    probe_candidate_visits == 3);
                col_rel_t *out =
                    wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
                CHECK(out->nrows == (anti ? 0u : 3u));
                for (uint32_t r = 0; r < out->nrows; ++r)
                    CHECK(out->columns[0][r] == collision &&
                        out->columns[1][r] == (int64_t)r + 1);
            }
            fixture_close(&f);
        }
}

static void
probe_owned_authority(void)
{
    for (unsigned anti = 0; anti < 2; ++anti) {
        fixture_t f; fixture_init(&f, 2, 3, 1, 0);
        f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
        f.op.key_count = 1;
        wl_plan_relation_t plan = {.ops = &f.op, .op_count = 1};
        wl_columnar_eval_plan_numeric_attempt_t owner;
        wl_columnar_eval_plan_numeric_attempt_init(&owner);
        CHECK(!wl_columnar_eval_plan_numeric_attempt_begin(&owner, &plan, 0, 1,
            f.frame, &f.session, f.operation, f.operation_id, f.funding,
            f.funding_id));
        f.claimed = &owner;
        wl_columnar_filter_status_t sentinel;
        memset(&sentinel, 0x5a, sizeof(sentinel));
        wl_columnar_filter_status_t unchanged;
        memcpy(&unchanged, &sentinel, sizeof(unchanged));
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&f.op, f.frame,
            &f.session,
            NULL, 0, &sentinel, &owner) == EINVAL);
        CHECK(!memcmp(&sentinel, &unchanged, sizeof(sentinel)));
        wl_columnar_eval_plan_numeric_attempt_t copy = owner;
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&f.op, f.frame,
            &f.session,
            f.operation, f.operation_id, &sentinel, &copy) == EBUSY);
        CHECK(!memcmp(&sentinel, &unchanged, sizeof(sentinel)));
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&f.op, f.frame,
            &f.session,
            f.operation, f.operation_id + 1, &sentinel, &owner) == EINVAL);
        CHECK(!memcmp(&sentinel, &unchanged, sizeof(sentinel)));
        wl_col_session_t other_session = {0};
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&f.op, f.frame,
            &other_session, f.operation, f.operation_id, &sentinel,
            &owner) == EINVAL);
        CHECK(!memcmp(&sentinel, &unchanged, sizeof(sentinel)));
        run(&f); CHECK(f.status.committed && !f.status.semantic_result);
        CHECK(wl_columnar_eval_stack_cleanup_stack(
                f.frame)->items[0].rel->nrows == (anti ? 2u : 1u));
        /* Actual claim release through a real stopped numeric preflight, not
         * record mutation or an assertion that P1 admits complete plans. */
        wl_evaluation_control_request_cancel(f.operation);
        for (unsigned turns = 0;
            owner.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE; ++turns) {
            CHECK(turns < 10);
            CHECK(!wl_columnar_eval_plan_numeric_attempt_step(&owner, f.funding,
                f.funding_id));
        }
        CHECK(owner.semantic_result == WL_EVALUATION_CONTROL_CANCELLED &&
            !owner.completed);
        CHECK(!wl_columnar_eval_plan_numeric_attempt_ack(&owner, f.funding,
            f.funding_id));
        f.claimed = NULL;
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            WL_EVALUATION_CONTROL_CANCELLED,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        fixture_close(&f);
    }
}

static void
probe_retained_credit(void)
{
    for (unsigned anti = 0; anti < 2; ++anti) {
        fixture_t f; fixture_init(&f, 2, 3, 5, 0);
        f.op.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
        f.op.key_count = 1;
        wl_columnar_memory_resolution_t resolution = {.budget_bytes = 1u << 20,
                                                      .usable_bytes = 1u << 20,
                                                      .mode =
                                                          WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                                                      .source =
                                                          WL_COLUMNAR_MEMORY_SOURCE_ENV,
                                                      .status =
                                                          WL_COLUMNAR_MEMORY_OK};
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor); f.session.memory_governor = governor;
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
                governor), 128, &peer));
        wl_columnar_filter_context_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_filter_context(f.frame, &ctx));
        for (unsigned turns = 0;
            ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT ||
            ctx->join_retirement_cursor; ++turns) {
            CHECK(turns < 1000); CHECK(!dispatch(&f));
        }
        wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[0];
        CHECK(scratch->admitted && scratch->committed && scratch->ref_held &&
            !scratch->physical && !scratch->allocated);
        uint64_t reserved =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    governor));
        uint64_t bytes = scratch->credit.bytes;
        wl_columnar_memory_governor_test_refuse_next_release();
        CHECK(!dispatch(&f));
        CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
        CHECK(scratch->admitted && scratch->ref_held &&
            scratch->credit.bytes == bytes);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) == reserved);
        int host_owner; wl_evaluation_control_t *saved = f.funding;
        uint64_t saved_id = f.funding_id;
        for (unsigned allowance = 383; allowance <= 384; ++allowance) {
            wl_evaluation_control_t *host = control_new(allowance, &host_owner);
            f.funding = host;
            f.funding_id = wl_evaluation_control_active_attempt_id(host);
            int rc = dispatch(&f);
            CHECK(rc ==
                (allowance == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
            if (allowance == 383) CHECK(scratch->admitted &&
                    scratch->credit.bytes == bytes &&
                    ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT);
            CHECK(wl_evaluation_control_finish(host, &host_owner, 0,
                0) ==
                (allowance == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
            CHECK(!wl_evaluation_control_detach(host, &host_owner));
            wl_evaluation_control_release(host);
        }
        f.funding = saved; f.funding_id = saved_id;
        CHECK(!scratch->admitted && scratch->ref_held &&
            ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_REFERENCE);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) == reserved - bytes);
        CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0,
            0));
        CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
        run(&f);
        CHECK(f.status.semantic_result == EINVAL && !f.status.committed);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.left);
        CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0,
            0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation,
            &report) && !report.consumed);
        CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
        f.operation_id = wl_evaluation_control_active_attempt_id(f.operation);
        f.phase = 0;
        run(&f); CHECK(f.status.committed && !f.status.semantic_result);
        CHECK(wl_columnar_eval_stack_cleanup_stack(
                f.frame)->items[0].rel->nrows == (anti ? 0u : 3u));
        fixture_close(&f);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) == peer.bytes);
        CHECK(wl_columnar_memory_rollback(&peer));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

/* #1382: the controlled route is not a bounded JOIN, so while
 * WIRELOG_JOIN_BATCH_BYTES is set (join_batch_bytes != 0) it finishes with
 * ENOTSUP, commits nothing and leaves the left input on the stack.  It
 * records no fallback reason; recording one belongs to #1821. */
static void
batch_knob_refused_case(void)
{
    fixture_t f; fixture_init(&f, 2, 3, 5, 0);
    f.session.join_batch_bytes = 224;
    run(&f);
    CHECK(f.status.semantic_result == ENOTSUP && !f.status.committed);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
    CHECK(stack->top == 1 && stack->items[0].rel == f.left);
    CHECK(!f.session.join_batch_fallback_count);
    f.session.join_batch_bytes = 0;
    fixture_close(&f);
}

int
main(void)
{
    probe_semantics(); probe_metadata(); probe_float_keys(); probe_rejections();
    probe_interrupts();
    probe_allocations_and_quota(); probe_literal_prefixes();
    probe_no_output_work(); probe_owned_authority(); probe_retained_credit();
    standalone_raw_case();
    parity_cases(); generated_case(false); generated_case(true); limit_cases();
    funding_case();
    retained_case(); governor_cases(); publication_case(); interrupt_cases();
    keyed_cases(); key_grammar_cases(); key_empty_and_type_cases();
    key_interrupt_cases(); key_governor_cases(); key_literal_prefixes();
    hash_literal_prefixes(); batch_knob_refused_case();
    puts("ordinary cross/keyed JOIN raw parity and work-limit cleanup passed");
    return 0;
}
