/* Actual controlled session prefix schemas, FULL probes and truncation. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static wl_evaluation_control_t *active;
static wl_session_t *active_session;
static bool funding_test, short_grant, ordinary_decline, bare_gate_case;
static unsigned bare_gate_starts;
static uint64_t selected_limit, selected_id;
static unsigned selected_consumers, refused_grants, paid_grants, probe_starts;
static unsigned prepared, consumers, indexes, schemas, stop_schema, stop_event;
static bool stopped, work_stop;
static int allocation_failure = -1;
static bool deny_schema;
static wl_columnar_memory_reservation_t peer;
static unsigned first_probe_op, first_probe_mode, first_probe_followups,
    first_probe_acks;
static uint64_t first_probe_limit, first_probe_prefix, first_probe_paid;
static bool first_probe_found, first_probe_inert_start;

static void
first_probe_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_eval_plan_numeric_attempt_t *record)
{
    if (!first_probe_mode) return;
    if (first_probe_found) {
        if (first_probe_mode >= 2) {
            wl_columnar_filter_context_t *ctx = NULL;
            CHECK(!wl_columnar_eval_stack_filter_context_owned(record->frame,
                &ctx, record));
            CHECK(ctx->stage != WL_COLUMNAR_FILTER_STAGE_INERT
                && !ctx->program && !ctx->output && !ctx->private_output
                && !ctx->unary_started && !ctx->source_initialized
                && !ctx->batch_initialized && !ctx->join_right_initialized);
            ++first_probe_followups;
            if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_DONE)
                ++first_probe_acks;
        }
        return;
    }
    const col_rel_t *input;
    const wl_plan_op_t *op;
    uint64_t generation;
    bool schema;
    if (wl_columnar_join_pipeline_test_stream_view(record, &input, &generation,
        &schema, &op) || op->op != first_probe_op || schema || !input->nrows)
        return;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(record->frame, &ctx,
        record));
    CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_INERT && !ctx->control
        && !ctx->pin_owned && !ctx->program && !ctx->output);
    first_probe_found = first_probe_inert_start = true;
    CHECK(!wl_evaluation_control_test_active_consumed(control,
        wl_evaluation_control_active_attempt_id(control), &first_probe_prefix));
    if (first_probe_mode == 3) wl_evaluation_control_request_cancel(control);
}

static void
stop(void)
{
    if (stopped) return;
    stopped = true;
    if (work_stop) CHECK(wl_evaluation_control_charge(active, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(active);
}

static void
schema_checkpoint(const wl_columnar_join_schema_attempt_t *a)
{
    CHECK(a->control == active && !prepared && !indexes);
    ++schemas;
    if (deny_schema && schemas == 1) {
        wl_columnar_memory_governor_t *governor =
            wl_columnar_memory_governor_ref_get(a->governor);
        uint64_t available = (UINT64_C(512) <<
            20) - wl_columnar_memory_reserved(governor);
        CHECK(available > 1 && wl_columnar_memory_reserve(governor,
            available - 1, &peer));
    }
    if (stop_schema == schemas) stop();
}

static void
host_checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t producer_work)
{
    (void)generation; (void)extent; (void)producer_work;
    CHECK(control == active);
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED) ++prepared;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER) {
        ++consumers;
        wl_col_session_t *session = (wl_col_session_t *)active_session;
        wl_columnar_eval_serial_framed_attempt_t *root = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
            session, &root));
        const col_rel_t *batch = NULL; const wl_plan_op_t *op = NULL;
        uint64_t gen = 0; bool schema = false;
        if (!wl_columnar_join_pipeline_test_stream_view(&root->child, &batch,
            &gen, &schema, &op)
            && (op->op == WL_PLAN_OP_SEMIJOIN ||
            op->op == WL_PLAN_OP_ANTIJOIN)){
            eval_stack_t *stack = NULL;
            CHECK(!wl_columnar_eval_stack_consumer_stack_owned(
                    session->cleanup_active,
                    session, &stack, &root->child, op) && stack->top);
            const col_rel_t *input = stack->items[stack->top - 1].rel;
            col_rel_t *right = session_find_rel(session, op->right_relation);
            CHECK(right &&
                !wl_columnar_join_pipeline_probe_source_owned(&root->child,
                session->cleanup_active, op, input, right));
            col_rel_t *delta = session_find_rel(session, "$d$gate");
            CHECK(delta &&
                wl_columnar_join_pipeline_probe_source_owned(&root->child,
                session->cleanup_active, op, input, delta) == EINVAL);
            int64_t value = 123;
            CHECK(col_rel_append_row(right, &value) == EBUSY);
            ++probe_starts;
        }
        /* Include the existing real source-validation debit in calibration. */
        first_probe_checkpoint(control, &root->child);
    }
    if (event == stop_event) stop();
}

static void
join_checkpoint(wl_evaluation_control_t *control, unsigned phase, uint64_t row)
{
    (void)row;
    CHECK(control == active);
    if (bare_gate_case && phase == 1) {
        wl_col_session_t *session = (wl_col_session_t *)active_session;
        wl_columnar_eval_serial_framed_attempt_t *root = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
            session, &root));
        const wl_plan_op_t *op = &root->child.plan->ops[root->child.cursor];
        if (op->op == WL_PLAN_OP_SEMIJOIN) {
            eval_stack_t *stack = NULL;
            CHECK(!root->child.join_host && !strcmp(op->right_relation,
                "gate"));
            stack =
                wl_columnar_eval_stack_cleanup_stack(session->cleanup_active);
            CHECK(stack);
            CHECK(stack->top == 1 && stack->items[0].owned
                && stack->items[0].rel->nrows == 28 &&
                stack->items[0].rel->ncols == 4);
            ++bare_gate_starts;
        }
    }
    if (phase == 28) {
        CHECK(prepared || ordinary_decline); ++indexes;
    }
}

static uint64_t
funding_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t proposed)
{
    selected_limit = 0;
    if (action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE ||
        !session->cleanup_active)
        return proposed;
    wl_columnar_eval_serial_framed_attempt_t *root = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
        (wl_col_session_t *)session, &root));
    if (root->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD ||
        root->child.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH)
        return proposed;
    uint64_t work = 0;
    CHECK(!wl_columnar_join_pipeline_numeric_funding_work(&root->child, &work));
    if (work != 384) return proposed;
    CHECK(wl_columnar_session_controlled_batch_authorized(session, active,
        root->attempt_id, root->operation_identity));
    selected_id = root->attempt_id; selected_consumers = consumers;
    selected_limit = short_grant ? 1791 : 1792;
    return selected_limit;
}

static void
funding_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *funding, int protocol)
{
    (void)session;
    if (!selected_limit) return;
    CHECK(action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_active_attempt_id(funding)
        && !wl_evaluation_control_report(funding, &report)
        && report.limit == selected_limit && !report.execution_status
        && wl_evaluation_control_active_attempt_id(active) == selected_id);
    if (short_grant) {
        CHECK(protocol == WL_EVALUATION_CONTROL_WORK_LIMIT &&
            report.result == protocol
            && report.consumed == 1408 && consumers == selected_consumers);
        ++refused_grants;
    } else {
        CHECK(!protocol && !report.result && report.consumed == 1792
            && consumers == selected_consumers + 1);
        ++paid_grants;
    }
    short_grant = !short_grant;
}

static int
finish(wl_session_t *session)
{
    for (unsigned turn = 0; turn < 1000000; ++turn) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}

static bool
expected_tuple(unsigned kind, unsigned a, unsigned b)
{
    if (kind != 1 && kind != 2 && kind != 4 && a >= b) return false;
    switch (kind) {
    case 3: case 4: case 9: return true;
    case 5: case 8: case 11: case 17: return false;
    case 7: return b != 1 && b != 4;
    case 12: case 14: case 15: return a != 4;
    case 13: return a != 4 && (LONG_MAX > INT_MAX ? a == 1 : true);
    case 16: return a != 4 && (LONG_MAX > INT_MAX ? b == 1 : true);
    default: return (b == 2 || b == 3 || b == 5) && a != 1 && a != 4;
    }
}

static uint64_t
run(unsigned kind, unsigned schema_stop, unsigned event_stop, bool work)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
        ".decl gate(x:int64)\n.decl blocked(x:int64)\n"
        ".decl output(x:int64,y:int64)\n"
        "output(x,y):-a(k,x),b(k,y),x<y.\n", &error);
    if (kind == 7) {
        wirelog_program_free(program);
        program = wirelog_parse_string(
            ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
            ".decl gate(x:int64)\n.decl blocked(y:int64)\n"
            ".decl output(x:int64,y:int64)\n"
            "output(x,y):-a(k,x),b(k,y),!blocked(y),x<y.\n", &error);
    }
    if (kind == 9) {
        wirelog_program_free(program);
        program = wirelog_parse_string(
            ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
            ".decl gate(x:int64)\n.decl blocked(y:int64)\n"
            ".decl first(x:int64,y:int64)\n.decl output(x:int64,y:int64)\n"
            "first(x,y):-a(k,x),b(k,y),x<y.\n"
            "output(x,y):-first(x,y).\n", &error);
    }
    wl_plan_t *original = NULL;
    CHECK(program && !wl_plan_from_program(program, &original));
    const wl_plan_relation_t *old = &original->strata[0].relations[0];
    CHECK(old->ops[1].op == WL_PLAN_OP_JOIN &&
        (kind == 7 || old->op_count == 5));
    static const char *left[] = {"col3"}, *right[] = {"col0"};
    static const char *anti_left[] = {"col0"}, *null_left[] = {NULL};
    static const char *unknown_left[] = {"unresolved"};
    static const char *suffix_left[][1] = {{"col2147483648"}, {"col4294967297"},
                                           {"col\xc3\xa9"}, {"col4"},
                                           {"col4294967299"}};
    static uint32_t project[] = {1, 3}, identity[] = {0, 1};
    wl_plan_op_t ops[8]; unsigned n = 0;
    ops[n++] = old->ops[0]; ops[n++] = old->ops[1];
    bool filtered = kind != 1 && kind != 2 && kind != 4;
    if (filtered) ops[n++] = old->ops[2];
    bool missing = kind == 3 || kind == 4;
    ops[n++] = (wl_plan_op_t){.op = WL_PLAN_OP_SEMIJOIN,
                              .right_relation = missing ? "missing" : "gate",
                              .key_count = 1,
                              .left_keys = kind == 5 ? null_left : kind ==
                                  8 ? unknown_left : kind >=
                                  12 &&
                                  kind <= 16 ? suffix_left[kind - 12] : left,
                              .right_keys = right,
                              .project_count = 2, .project_indices = project};
    if (!missing && kind != 5 && kind != 8) ops[n++] = (wl_plan_op_t){
            .op = WL_PLAN_OP_ANTIJOIN, .right_relation = "blocked",
            .key_count = 1,
            .left_keys = anti_left, .right_keys = right
        };
    if (kind != 2) ops[n++] = missing ? old->ops[3] : (wl_plan_op_t){
            .op = WL_PLAN_OP_MAP, .project_count = 2,
            .project_indices = identity
        };
    ops[n++] = old->ops[4];
    wl_plan_relation_t relation = *old;
    if (kind != 7 && kind != 9) {
        relation.ops = ops; relation.op_count = n;
    }else if (kind ==
        7){
        bool probe = false;
        for (unsigned i = 2; i < relation.op_count; ++i)
            if (relation.ops[i].op == WL_PLAN_OP_ANTIJOIN) probe = true;
        CHECK(probe);
    }
    wl_plan_stratum_t stratum = original->strata[0];
    stratum.relations = &relation;
    wl_plan_t plan = *original;
    if (kind != 9) plan.strata = &stratum;
    else CHECK(plan.stratum_count > 1 || stratum.relation_count > 1);
    CHECK(!wl_evaluation_control_create(first_probe_mode == 2
        ? first_probe_limit : work ? UINT64_C(1000000000) : 0, &active));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = active; options.memory_governor = governor;
    wl_session_t *controlled = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &controlled));
    CHECK(!wl_session_create(wl_backend_columnar(), &plan, 1, &raw));
    int64_t data[16];
    for (unsigned i = 0; i < 8; ++i) {
        data[2*i] = 1; data[2*i+1] = i;
    }
    int64_t gate[4096] = {2, 3, 5}, blocked[] = {1, 4}, delta[] = {7};
    if (kind >= 12) {
        gate[0] = 1; blocked[0] = 4;
    }
    for (unsigned i = 3; i < 4096; ++i) gate[i] = 100 + i;
    wl_session_t *sessions[] = {controlled, raw};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(sessions[i], "a", data, 8, 2));
        CHECK(!wl_session_insert(sessions[i], "b", data, 8, 2));
        if (kind != 17) CHECK(!wl_session_insert(sessions[i], "gate", gate,
                kind == 6 ? 4096 : kind >= 12 ? 1 : 3, 1));
        else {
            const col_rel_t *full =
                session_find_rel((wl_col_session_t *)sessions[i], "gate");
            CHECK(full && !full->schema_ok && !full->ncols && !full->nrows);
        }
        if (kind == 11) CHECK(!wl_session_remove(sessions[i], "gate", gate, 3,
                1));
        CHECK(!wl_session_insert(sessions[i], "blocked", blocked,
            kind >= 12 ? 1 : 2, 1));
        col_rel_t *d = col_rel_new_auto("$d$gate", 1);
        CHECK(d && !col_rel_append_row(d, delta));
        CHECK(!session_add_rel((wl_col_session_t *)sessions[i], d));
        ((wl_col_session_t *)sessions[i])->join_batch_bytes = kind ==
            10 ? 1 : 64;
    }
    active_session = controlled; ordinary_decline = kind == 10;
    bare_gate_case = kind == 17; bare_gate_starts = 0;
    probe_starts = refused_grants = paid_grants = 0; short_grant = true;
    first_probe_found = first_probe_inert_start = false;
    first_probe_followups = first_probe_acks = 0;
    if (funding_test) {
        wl_columnar_session_test_framed_grant_limit = funding_limit;
        wl_columnar_session_test_framed_grant_finished = funding_finished;
    }
    prepared = consumers = indexes = schemas = 0;
    stopped = false; work_stop = work; stop_schema = schema_stop;
    stop_event = event_stop;
    wl_columnar_join_test_schema_checkpoint = schema_checkpoint;
    wl_columnar_join_pipeline_test_numeric_checkpoint = host_checkpoint;
    wl_columnar_join_test_attempt_checkpoint = join_checkpoint;
    wl_columnar_join_test_attempt_fail_allocation = allocation_failure;
    int rc = finish(controlled);
    wl_columnar_join_test_attempt_fail_allocation = -1;
    if (first_probe_mode >= 2) {
        CHECK(first_probe_found && first_probe_inert_start
            && (first_probe_paid ? first_probe_followups > 0 &&
            first_probe_acks == 1
                : !first_probe_followups && !first_probe_acks)
            && rc == (first_probe_mode == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED));
        wl_evaluation_control_report_t first_report;
        CHECK(!wl_evaluation_control_report(active, &first_report)
            && first_report.consumed == first_probe_prefix + first_probe_paid
            && !session_find_rel((wl_col_session_t *)controlled, "output")
            && !((wl_col_session_t *)controlled)->cleanup_active
            && !((wl_col_session_t *)controlled)->cleanup_pending);
        first_probe_mode = 0;
        wl_columnar_session_test_framed_grant_finished = NULL;
        CHECK(!wl_evaluation_control_reset(active));
        CHECK(!wl_evaluation_control_configure(active, 0));
        prepared = consumers = indexes = schemas = 0;
        rc = finish(controlled);
    }
    if (schema_stop || event_stop || allocation_failure >= 0 || deny_schema) {
        if ((schema_stop || event_stop) &&
            (!stopped ||
            rc !=
            (work ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED)))
            fprintf(stderr,
                "stop kind=%u schema=%u event=%u work=%u stopped=%u rc=%d schemas=%u\n",
                kind, schema_stop, event_stop, work, stopped, rc, schemas);
        if (schema_stop ||
            event_stop) CHECK(stopped &&
                rc == (work ? WL_EVALUATION_CONTROL_WORK_LIMIT
            : WL_EVALUATION_CONTROL_CANCELLED));
        else CHECK(rc == (deny_schema ? ENOSPC : ENOMEM));
        if (deny_schema) {
            CHECK(wl_columnar_memory_rollback(&peer)); deny_schema = false;
        }
        CHECK(!session_find_rel((wl_col_session_t *)controlled, "output"));
        stop_schema = stop_event = 0;
        CHECK(!wl_evaluation_control_reset(active));
        prepared = consumers = indexes = schemas = 0;
        rc = finish(controlled);
    }
    if (rc) fprintf(stderr,
            "probe kind=%u rc=%d prepared=%u consumers=%u schemas=%u indexes=%u\n",
            kind, rc, prepared, consumers, schemas, indexes);
    CHECK(!rc && prepared == (kind == 10 ? 0u : 1u));
    if (!missing && kind != 5 && kind != 9 && kind != 10 &&
        kind != 11 && kind != 17) CHECK(probe_starts);
    if (kind == 17) CHECK(bare_gate_starts == 1);
    if (funding_test) CHECK(refused_grants && refused_grants == paid_grants);
    wl_columnar_session_test_framed_grant_limit = NULL;
    wl_columnar_session_test_framed_grant_finished = NULL;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(active, &report) && !report.result);
    CHECK((kind == 9 || schemas) && (kind == 4 || kind == 10 || consumers));
    if (!missing && kind != 5 && kind != 9 && kind != 11 &&
        kind != 17) CHECK(indexes);
    wl_columnar_join_test_schema_checkpoint = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    wl_columnar_join_test_attempt_checkpoint = NULL;
    CHECK(!finish(raw));
    col_rel_t *out = session_find_rel((wl_col_session_t *)controlled, "output");
    col_rel_t *reference = session_find_rel((wl_col_session_t *)raw, "output");
    CHECK(out && reference && out->ncols == 2 && reference->ncols == 2);
    bool seen[8][8] = {{false}}; unsigned count = 0;
    for (unsigned a = 0; a < 8; ++a) for (unsigned b = 0; b < 8; ++b) {
            bool expected = expected_tuple(kind, a, b);
            if (expected) ++count;
        }
    if (out->nrows != count || reference->nrows != count)
        fprintf(stderr, "kind=%u expected=%u controlled=%u raw=%u\n", kind,
            count, out->nrows, reference->nrows);
    CHECK(out->nrows == count && reference->nrows == count);
    for (unsigned i = 0; i < 2; ++i) {
        col_rel_t *r = i ? reference : out;
        memset(seen, 0, sizeof(seen));
        for (unsigned row = 0; row < r->nrows; ++row) {
            int64_t a = r->columns[0][row], b = r->columns[1][row];
            CHECK(a >= 0 && a < 8 && b >= 0 && b < 8 && !seen[a][b]);
            CHECK(expected_tuple(kind, (unsigned)a, (unsigned)b));
            seen[a][b] = true;
        }
    }
    CHECK(!wl_session_try_destroy(&controlled));
    CHECK(!wl_session_try_destroy(&raw));
    wl_plan_free(original); wirelog_program_free(program);
    uint64_t remaining =
        wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor));
    if (remaining) fprintf(stderr, "kind=%u stop=%u event=%u remaining=%llu\n",
            kind, schema_stop, event_stop, (unsigned long long)remaining);
    CHECK(!remaining);
    wl_columnar_memory_governor_ref_release(governor);
    wl_evaluation_control_release(active); active = NULL; active_session = NULL;
    return report.consumed;
}

static void
bound_probe_turn(wl_columnar_eval_plan_numeric_attempt_t *record,
    wl_evaluation_control_t *funding, uint64_t id)
{
    CHECK(!(record->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        ? wl_columnar_eval_plan_numeric_composed_consume_result(record, 0,
        funding, id)
        : wl_columnar_eval_plan_numeric_composed_attempt_step(record,
        funding, id)));
}

static void *
bound_probe_foreign_thread(void *argument)
{
    wl_columnar_eval_plan_numeric_attempt_t *record = argument;
    wl_columnar_filter_status_t status = {0};
    CHECK(wl_columnar_join_probe_op_attempt_start_owned(
            &record->plan->ops[record->cursor], record->frame, record->session,
            record->control, record->attempt_id, &status, record) == EINVAL);
    return NULL;
}

static void
bound_probe_compatibility(void)
{
    for (unsigned anti = 0; anti < 2; ++anti) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(
            ".decl left(x:int64)\n.decl right(x:int64)\n"
            ".decl output(x:int64)\noutput(x):-left(x),right(x).\n", &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        const char *key = "col0";
        wl_plan_op_t ops[] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "left",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN,
             .right_relation = "right", .key_count = 1,
             .left_keys = &key, .right_keys = &key},
            {.op = WL_PLAN_OP_CONSOLIDATE}
        };
        wl_plan_relation_t body = plan->strata[0].relations[0];
        body.ops = ops; body.op_count = 3;
        wl_evaluation_control_t *original = NULL, *funding = NULL;
        CHECK(!wl_evaluation_control_create(0, &original));
        wl_session_options_t options; wl_session_options_init(&options);
        options.evaluation_control = original;
        wl_session_t *base = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &base));
        wl_col_session_t *session = (wl_col_session_t *)base;
        session->join_batch_bytes = 0; session->join_batch_strict = false;
        int64_t shadow = 99, left[] = {1, 2}, right = 2;
        const char *names[] = {"left", "right"};
        col_rel_t *private_relations[2], *public_relations[2];
        col_relation_snapshot_t snapshots[2];
        for (unsigned i = 0; i < 2; ++i) {
            CHECK(!wl_session_insert(base, names[i], &shadow, 1, 1));
            public_relations[i] = session_find_rel(session, names[i]);
            private_relations[i] = col_rel_new_like(names[i],
                    public_relations[i]);
            CHECK(private_relations[i]);
            CHECK(!col_rel_append_rows_atomic(private_relations[i],
                i ? &right : left, i ? 1 : 2, 1, NULL));
            snapshots[i] = wl_columnar_relation_snapshot(private_relations[i]);
        }
        CHECK(!wl_evaluation_control_begin(original,
            base->operation_admission));
        uint64_t id = wl_evaluation_control_active_attempt_id(original);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(!wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
            &frame, original, id));
        int funding_owner;
        CHECK(!wl_evaluation_control_create(0, &funding)
            && !wl_evaluation_control_attach(funding, &funding_owner)
            && !wl_evaluation_control_begin(funding, &funding_owner));
        uint64_t fid = wl_evaluation_control_active_attempt_id(funding);
        wl_columnar_eval_private_bindings_t bindings = {0};
        CHECK(!wl_columnar_eval_private_bindings_begin(&bindings, session,
            &body, original, id, names, private_relations, 2));
        wl_columnar_eval_plan_numeric_attempt_t record;
        wl_columnar_eval_plan_numeric_attempt_init(&record);
        CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
                &record, &body, 0, 3, frame, session, 0, original, id,
                funding, fid, &bindings));
        for (unsigned turn = 0; record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY || record.cursor != 1;
            ++turn) {
            CHECK(turn < 10000);
            bound_probe_turn(&record, funding, fid);
        }
        CHECK(record.private_bindings == &bindings);
        wl_columnar_filter_status_t status;
        memset(&status, 0, sizeof(status));
        unsigned char status_before[sizeof(status)];
        memcpy(status_before, &status, sizeof(status));
        uint64_t original_before, original_after;
        CHECK(!wl_evaluation_control_test_active_consumed(original, id,
            &original_before));
        wl_session_t *other = NULL;
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &other));
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&ops[1], frame,
            (wl_col_session_t *)other, original, id, &status,
            &record) == EINVAL);
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&ops[1], frame,
            session, funding, fid, &status, &record) == EINVAL);
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&ops[1], frame,
            session, original, id + 1, &status, &record) == EINVAL);
        wl_columnar_eval_plan_numeric_attempt_t copy = record;
        CHECK(wl_columnar_join_probe_op_attempt_start_owned(&ops[1], frame,
            session, original, id, &status, &copy) == EBUSY);
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, bound_probe_foreign_thread, &record)
            && !wl_thread_join(&thread));
        CHECK(!memcmp(status_before, &status, sizeof(status))
            && !wl_evaluation_control_test_active_consumed(original, id,
            &original_after) && original_after == original_before);
        wl_session_destroy(other);
        /* The normal funded parent bypasses this legacy wrapper. Exercise
         * its real nonNULL branch directly at the genuine READY boundary. */
        CHECK(!wl_columnar_join_probe_op_attempt_start_owned(&ops[1], frame,
            session, original, id, &status, &record));
        for (unsigned turn = 0; status.progress !=
            WL_COLUMNAR_FILTER_PROGRESS_DONE; ++turn) {
            CHECK(turn < 10000);
            CHECK(!wl_columnar_filter_op_attempt_step_owned(frame, &status,
                &record));
        }
        CHECK(!status.semantic_result);
        CHECK(!wl_columnar_filter_op_attempt_ack_owned(frame, &record));
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        CHECK(stack && stack->top == 1 && stack->items[0].rel->nrows == 1
            && col_rel_get(stack->items[0].rel, 0, 0) == (anti ? 1 : 2));
        /* SEMI/ANTI with the same RHS is idempotent; resume the actual parent
         * without modifying its cursor, phase or completed counters. */
        for (unsigned turn = 0; record.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE; ++turn) {
            CHECK(turn < 10000);
            bound_probe_turn(&record, funding, fid);
        }
        CHECK(!record.semantic_result && stack->top == 1
            && stack->items[0].rel->nrows == 1
            && col_rel_get(stack->items[0].rel, 0, 0) == (anti ? 1 : 2));
        CHECK(!wl_columnar_eval_plan_numeric_composed_attempt_ack(&record,
            funding, fid));
        CHECK(!wl_columnar_eval_stack_cleanup_finish(&frame));
        CHECK(!wl_evaluation_control_finish(original, base->operation_admission,
            0, 0));
        CHECK(!wl_evaluation_control_reset(original)
            && !wl_evaluation_control_begin(original,
            base->operation_admission));
        uint64_t successor = wl_evaluation_control_active_attempt_id(original);
        CHECK(successor != id);
        CHECK(!wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session,
            &frame, original, successor));
        uint64_t before, after;
        CHECK(!wl_evaluation_control_test_active_consumed(original, successor,
            &before));
        wl_columnar_eval_plan_numeric_attempt_init(&record);
        CHECK(wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
                &record, &body, 0, 3, frame, session, 0, original, successor,
                funding, fid, &bindings) == EINVAL);
        CHECK(!wl_evaluation_control_test_active_consumed(original, successor,
            &after) && after == before);
        CHECK(!wl_columnar_eval_stack_cleanup_finish(&frame));
        CHECK(!wl_evaluation_control_finish(original, base->operation_admission,
            0, 0));
        CHECK(!wl_evaluation_control_finish(funding, &funding_owner, 0, 0)
            && !wl_evaluation_control_detach(funding, &funding_owner));
        wl_evaluation_control_release(funding);
        for (unsigned i = 0; i < 2; ++i) {
            CHECK(wl_columnar_relation_snapshot_equal(snapshots[i],
                wl_columnar_relation_snapshot(private_relations[i]))
                && session_find_rel(session, names[i]) == public_relations[i]
                && public_relations[i]->nrows == 1
                && col_rel_get(public_relations[i], 0, 0) == shadow);
            col_rel_destroy(private_relations[i]);
        }
        wl_session_destroy(base);
        wl_evaluation_control_release(original);
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

int
main(void)
{
    bound_probe_compatibility();
    for (unsigned kind = 0; kind < 18; ++kind) run(kind, 0, 0, false);
    for (unsigned point = 1; point <= 7; ++point)
        for (unsigned work = 0; work < 2; ++work) run(0, point, 0, work);
    for (unsigned event = 1; event <= 20; ++event)
        for (unsigned work = 0; work < 2; ++work) run(0, 0, event, work);
    for (int fault = 0; fault < 13; ++fault) {
        allocation_failure = fault; run(0, 0, 0, false);
    }
    allocation_failure = -1;
    for (unsigned work = 0; work < 2; ++work)
        run(3, 0, WL_COLUMNAR_JOIN_PIPELINE_TEST_SCHEMA_FINAL, work);
    wl_columnar_memory_reservation_init(&peer);
    deny_schema = true; run(0, 0, 0, false);
    const unsigned first_ops[] = {WL_PLAN_OP_SEMIJOIN, WL_PLAN_OP_ANTIJOIN};
    for (unsigned i = 0; i < 2; ++i) {
        first_probe_op = first_ops[i];
        first_probe_mode = 1;
        run(0, 0, 0, false);
        CHECK(first_probe_found && first_probe_prefix);
        uint64_t prefix = first_probe_prefix;
        for (unsigned exact = 0; exact < 2; ++exact) {
            first_probe_mode = 2;
            first_probe_limit = prefix + (exact ? 192 : 191);
            first_probe_paid = exact ? 192 : 0;
            run(0, 0, 0, false);
            CHECK(first_probe_prefix == prefix);
        }
        first_probe_mode = 3;
        first_probe_paid = 0;
        run(0, 0, 0, false);
        CHECK(first_probe_prefix == prefix);
    }
    uint64_t measured = run(0, 0, 0, false);
    funding_test = true;
    CHECK(run(0, 0, 0, false) == measured);
    return 0;
}
