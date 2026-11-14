/* Actual controlled session prefix schemas, FULL probes and truncation. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/wirelog-optimizer.h"
#include "../wirelog/wirelog-internal.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdarg.h>
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
static bool reset_enabled, reset_fault, reset_after, reset_fired,
    reset_recovery_pending;
static unsigned reset_target;
static size_t reset_bytes;
static wl_session_t *reset_session;
static col_rel_t *reset_head;
static col_relation_snapshot_t reset_head_snapshot;
static unsigned *reset_event_calls;
static void reset_recover(wl_session_t *, int);
extern void (*wl_columnar_session_test_step_reset_phase)(wl_col_session_t *,
    unsigned, size_t, bool);

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
        root->attempt_id, root->operation_identity, root->plan,
        root->private_bindings));
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
        if (reset_recovery_pending && session == reset_session) {
            reset_recover(session, rc);
            continue;
        }
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

static wl_evaluation_control_t *
key_control(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(limit, &control)
        && !wl_evaluation_control_attach(control, owner)
        && !wl_evaluation_control_begin(control, owner));
    return control;
}

static uint64_t
key_source_open(wl_columnar_relation_scalar_source_t *source, col_rel_t *rel,
    wl_evaluation_control_t *control)
{
    bool initialized = false;
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(source,
        &initialized, control, id) && initialized
        && !wl_columnar_relation_scalar_source_begin_attempt_controlled(
            source, rel, false));
    /* Independent existing scalar-source constructor schedule. */
    uint64_t expected = 1632 + 224 * ((rel->ncols + 7u) / 8u), actual = 0;
    CHECK(!wl_evaluation_control_test_active_consumed(control, id, &actual)
        && actual == expected
        && source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD);
    return expected;
}

static void
key_control_release(wl_evaluation_control_t *control, const void *owner)
{
    CHECK(!wl_evaluation_control_detach(control, owner));
    wl_evaluation_control_release(control);
}

static void
key_literal(col_rel_t *rel, const char *name, uint64_t allowance,
    int expected_rc, uint64_t key_paid, uint32_t expected_out)
{
    int owner = 0;
    uint64_t setup = 1632 + 224 * ((rel->ncols + 7u) / 8u);
    wl_evaluation_control_t *control = key_control(setup + allowance, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    wl_columnar_relation_scalar_source_t source;
    CHECK(key_source_open(&source, rel, control) == setup);
    uint32_t out = UINT32_MAX;
    int rc = wl_columnar_join_pipeline_key_attempt(&source, name, &out,
            control, id);
    CHECK(rc == expected_rc && out == expected_out);
    if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT) {
        /* Sticky refusal cannot restart a free scan or normalize output. */
        uint32_t again = UINT32_MAX;
        CHECK(wl_columnar_join_pipeline_key_attempt(&source, name, &again,
            control, id) == rc && again == UINT32_MAX);
    }
    CHECK(wl_columnar_relation_scalar_source_end(&source, rc) == rc);
    CHECK(wl_evaluation_control_finish(control, &owner, rc > 0 ? rc : 0,
        0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.attempt_id == id && report.result == rc
        && report.consumed == setup + key_paid && !report.cleanup_status);
    key_control_release(control, &owner);
}

static void
test_key_literal_boundaries(void)
{
    col_rel_t *rel = col_rel_new_auto("wide keys", 257);
    CHECK(rel);
    key_literal(rel, "col256", 95, WL_EVALUATION_CONTROL_WORK_LIMIT, 0,
        UINT32_MAX);
    key_literal(rel, "col256", 96, WL_EVALUATION_CONTROL_WORK_LIMIT, 96, 0);
    /* Canonical comparisons through col0/col128/col256 have respectively
    * 0/560/1098 preceding bytes, including the first differing byte.
    * Each visited column and each compared byte owns32; entry owns96. */
    const struct {
        const char *name;
        uint32_t column;
        uint64_t before_column, exact;
    } cases[] = {{"col0", 0, 96, 288}, {"col128", 128, 22112, 22368},
                 {"col256", 256, 43424, 43680}};
    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        uint64_t prefix = cases[i].before_column;
        key_literal(rel, cases[i].name, prefix + 31,
            WL_EVALUATION_CONTROL_WORK_LIMIT, prefix, 0);
        key_literal(rel, cases[i].name, prefix + 32,
            WL_EVALUATION_CONTROL_WORK_LIMIT, prefix + 32, 0);
        key_literal(rel, cases[i].name, prefix + 63,
            WL_EVALUATION_CONTROL_WORK_LIMIT, prefix + 32, 0);
        key_literal(rel, cases[i].name, prefix + 64,
            WL_EVALUATION_CONTROL_WORK_LIMIT, prefix + 64, 0);
        key_literal(rel, cases[i].name, cases[i].exact - 1,
            WL_EVALUATION_CONTROL_WORK_LIMIT, cases[i].exact - 32, 0);
        key_literal(rel, cases[i].name, cases[i].exact, 0, cases[i].exact,
            cases[i].column);
    }
    key_literal(rel, NULL, 96, EINVAL, 96, 0);
    CHECK(!col_rel_destroy_checked(rel));
    rel = col_rel_new_auto("parser keys", 4);
    CHECK(rel);
    /* Four column misses consume16 compared bytes: entry96+20*32=736.
     * Three prefix bytes bring832. Decimal units, including NUL, own64. */
    for (unsigned prefix = 0; prefix < 3; ++prefix) {
        uint64_t paid = 736 + prefix * 32;
        key_literal(rel, "col+1", paid + 31,
            WL_EVALUATION_CONTROL_WORK_LIMIT, paid, 0);
        key_literal(rel, "col+1", paid + 32,
            WL_EVALUATION_CONTROL_WORK_LIMIT, paid + 32, 0);
    }
    for (unsigned digit = 0; digit < 3; ++digit) {
        uint64_t paid = 832 + digit * 64;
        key_literal(rel, "col+1", paid + 63,
            WL_EVALUATION_CONTROL_WORK_LIMIT, paid, 0);
        key_literal(rel, "col+1", paid + 64,
            digit == 2 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT,
            paid + 64, digit == 2 ? 1 : 0);
    }
    key_literal(rel, "col4294967297", 1535,
        WL_EVALUATION_CONTROL_WORK_LIMIT, 1472, 0);
    key_literal(rel, "col4294967297", 1536, 0, 1536,
        LONG_MAX > INT_MAX ? 1 : 0);
    CHECK(!col_rel_destroy_checked(rel));
}

static void
key_raw_parity(col_rel_t *rel, const char *name)
{
    int owner = 0;
    wl_evaluation_control_t *control = key_control(0, &owner);
    wl_columnar_relation_scalar_source_t source;
    key_source_open(&source, rel, control);
    int raw = col_rel_col_idx(rel, name);
    uint32_t out = UINT32_MAX;
    CHECK(!wl_columnar_join_pipeline_key_attempt(&source, name, &out,
        control, wl_evaluation_control_active_attempt_id(control))
        && out == (raw >= 0 ? (uint32_t)raw : 0));
    CHECK(!wl_columnar_relation_scalar_source_end(&source, 0)
        && !wl_evaluation_control_finish(control, &owner, 0, 0));
    key_control_release(control, &owner);
}

static void
test_key_name_semantics(void)
{
    const char *names[] = {"col2", "col2", "custom", ""};
    col_rel_t *rel = NULL;
    CHECK(!col_rel_alloc(&rel, "named keys")
        && !col_rel_set_schema(rel, 4, names));
    const char *queries[] = {"col2", "custom", "", "missing", "c", "co",
                             "col", "col+", "col-", "col+1", "col-0", "col-1",
                             "col \t\n\r\f\v+1", "col1junk", "col2147483648",
                             "col4294967297", "col4294967299",
                             "col9223372036854775807",
                             "col9999999999999999999999999999999999999999",
                             "col\xc3\xa9"};
    for (unsigned i = 0; i < sizeof(queries) / sizeof(queries[0]); ++i)
        key_raw_parity(rel, queries[i]);
    /* Partial name metadata is assembled privately before any source lease,
     * as in the existing scalar-source tests. Arrow names stay consistent. */
    free(rel->col_names[0]); rel->col_names[0] = NULL;
    CHECK(!ArrowSchemaSetName(rel->schema.children[0], ""));
    key_raw_parity(rel, "col2");
    for (unsigned c = 1; c < 4; ++c) {
        free(rel->col_names[c]); rel->col_names[c] = NULL;
        CHECK(!ArrowSchemaSetName(rel->schema.children[c], ""));
    }
    free(rel->col_names); rel->col_names = NULL;
    key_literal(rel, "col2", 96, 0, 96, 0);
    key_raw_parity(rel, "col2");
    CHECK(!col_rel_destroy_checked(rel));
}

static void
test_key_saved_attempt(void)
{
    for (unsigned mode = 0; mode < 5; ++mode) {
        col_rel_t *rel = col_rel_new_auto("old key source", 4);
        CHECK(rel);
        int owner = 0;
        wl_evaluation_control_t *control = key_control(mode ==
                1 ? UINT64_MAX - 1 : 0, &owner);
        uint64_t old = wl_evaluation_control_active_attempt_id(control);
        wl_columnar_relation_scalar_source_t source;
        uint64_t setup = key_source_open(&source, rel, control);
        int expected = EINVAL;
        if (!mode) {
            wl_evaluation_control_request_cancel(control);
            expected = WL_EVALUATION_CONTROL_CANCELLED;
        } else if (mode == 1) {
            CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
            expected = WL_EVALUATION_CONTROL_WORK_LIMIT;
        } else {
            CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
            if (mode >= 3) CHECK(!wl_evaluation_control_reset(control));
            if (mode == 4) CHECK(!wl_evaluation_control_begin(control, &owner));
        }
        uint32_t out = UINT32_MAX;
        CHECK(wl_columnar_join_pipeline_key_attempt(&source, "col3", &out,
            control, old) == expected && out == UINT32_MAX);
        CHECK(wl_columnar_relation_scalar_source_end(&source, expected)
            == expected);
        if (mode < 2)
            CHECK(wl_evaluation_control_finish(control, &owner, 0,
                0) == expected);
        if (mode == 4) {
            uint64_t successor =
                wl_evaluation_control_active_attempt_id(control);
            uint64_t consumed = UINT64_MAX;
            CHECK(successor && successor != old
                && !wl_evaluation_control_test_active_consumed(control,
                successor, &consumed) && !consumed
                && !wl_evaluation_control_finish(control, &owner, 0, 0));
        }
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(control, &report));
        if (mode == 4) CHECK(report.attempt_id != old && !report.consumed
                && !report.result);
        else CHECK(report.attempt_id == old && report.consumed == setup
                && report.result == (mode < 2 ? expected : 0));
        key_control_release(control, &owner);
        CHECK(!col_rel_destroy_checked(rel));
    }
}

static void
key_program_append(char **next, size_t *remaining, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    int n = vsnprintf(*next, *remaining, format, args);
    va_end(args);
    CHECK(n >= 0 && (size_t)n < *remaining);
    *next += n; *remaining -= (size_t)n;
}

enum { KEY_WIDE = 257, KEY_RESULT = 258 };
static wl_session_t *key_wide_session;
static unsigned key_wide_events, key_wide_stop_mode;

static void
key_wide_checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t work)
{
    (void)generation; (void)extent; (void)work;
    if (event != WL_COLUMNAR_JOIN_PIPELINE_TEST_PROBE_KEYS) return;
    CHECK(key_wide_session && control == key_wide_session->evaluation_control);
    ++key_wide_events;
    if (key_wide_events == 129 && key_wide_stop_mode) {
        if (key_wide_stop_mode ==
            1) wl_evaluation_control_request_cancel(control);
        else CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
}

static void
key_environment(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(!(value ? setenv(name, value, 1) : unsetenv(name)));
#endif
}

static void
test_key_generated_wide(unsigned stop_mode)
{
    char text[65536], *next = text;
    size_t remaining = sizeof(text);
    const char *decls[] = {"a", "p", "output"};
    for (unsigned d = 0; d < 3; ++d) {
        key_program_append(&next, &remaining, ".decl %s(", decls[d]);
        for (unsigned c = 0; c < KEY_WIDE; ++c)
            key_program_append(&next, &remaining, "%sx%u:int64", c ? "," : "",
                c);
        key_program_append(&next, &remaining, d == 2 ? ",t:int64)\n" : ")\n");
    }
    key_program_append(&next, &remaining, ".decl b(x0:int64,t:int64)\noutput(");
    for (unsigned c = 0; c < KEY_WIDE; ++c)
        key_program_append(&next, &remaining, "%sx%u", c ? "," : "", c);
    key_program_append(&next, &remaining, ",t) :- a(");
    for (unsigned c = 0; c < KEY_WIDE; ++c)
        key_program_append(&next, &remaining, "%sx%u", c ? "," : "", c);
    key_program_append(&next, &remaining, "),b(x0,t),!p(");
    for (unsigned c = 0; c < KEY_WIDE; ++c)
        key_program_append(&next, &remaining, "%sx%u", c ? "," : "", c);
    key_program_append(&next, &remaining, ").\n");
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    unsigned anti = 0, joins = 0;
    for (unsigned st = 0; st < plan->stratum_count; ++st)
        for (unsigned r = 0; r < plan->strata[st].relation_count; ++r) {
            const wl_plan_relation_t *relation = &plan->strata[st].relations[r];
            for (unsigned i = 0; i < relation->op_count; ++i) {
                const wl_plan_op_t *op = &relation->ops[i];
                if (op->op == WL_PLAN_OP_JOIN) ++joins;
                if (op->op == WL_PLAN_OP_ANTIJOIN) {
                    CHECK(op->key_count == KEY_WIDE && op->right_keys
                        && !strcmp(op->right_keys[KEY_WIDE - 1], "col256"));
                    ++anti;
                }
            }
        }
    CHECK(anti == 1 && joins == 1);
    const char *old_bytes = getenv("WIRELOG_JOIN_BATCH_BYTES");
    const char *old_strict = getenv("WIRELOG_JOIN_BATCH_STRICT");
    char *saved_bytes = old_bytes ? wl_strdup(old_bytes) : NULL;
    char *saved_strict = old_strict ? wl_strdup(old_strict) : NULL;
    CHECK((!old_bytes || saved_bytes) && (!old_strict || saved_strict));
    key_environment("WIRELOG_JOIN_BATCH_BYTES", "1048576");
    key_environment("WIRELOG_JOIN_BATCH_STRICT", "0");
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(stop_mode == 2 ? UINT64_MAX - 1 : 0,
        &control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control; options.memory_governor = governor;
    wl_session_t *session = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session)
        && !wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    CHECK(((wl_col_session_t *)session)->join_batch_bytes == 1048576
        && !((wl_col_session_t *)session)->join_batch_strict);
    int64_t a[2 * KEY_WIDE], b[] = {1, 99};
    for (unsigned c = 0; c < KEY_WIDE; ++c)
        a[c] = a[KEY_WIDE + c] = c ? c : 1;
    a[KEY_WIDE - 1] = 42; a[2 * KEY_WIDE - 1] = 43;
    wl_session_t *sessions[] = {session, raw};
    for (unsigned i = 0; i < 2; ++i)
        CHECK(!wl_session_insert(sessions[i], "a", a, 2, KEY_WIDE)
            && !wl_session_insert(sessions[i], "b", b, 1, 2)
            && !wl_session_insert(sessions[i], "p", a, 1, KEY_WIDE));
    key_wide_session = session; key_wide_stop_mode = stop_mode;
    key_wide_events = 0;
    wl_columnar_join_pipeline_test_numeric_checkpoint = key_wide_checkpoint;
    int rc = EAGAIN;
    for (unsigned turn = 0; rc == EAGAIN && turn < 1000000; ++turn)
        rc = wl_session_step(session);
    if (stop_mode) {
        CHECK(rc == (stop_mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : WL_EVALUATION_CONTROL_WORK_LIMIT)
            && key_wide_events == 129
            && !session_find_rel((wl_col_session_t *)session, "output"));
        CHECK(!wl_evaluation_control_reset(control)
            && !wl_evaluation_control_configure(control, 0));
        key_wide_stop_mode = 0; key_wide_events = 0;
        rc = EAGAIN;
        for (unsigned turn = 0; rc == EAGAIN && turn < 1000000; ++turn)
            rc = wl_session_step(session);
    }
    CHECK(!rc && key_wide_events == KEY_WIDE + 1);
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    key_wide_session = NULL;
    CHECK(!finish(raw));
    const col_rel_t *actual = session_find_rel((wl_col_session_t *)session,
            "output");
    const col_rel_t *reference = session_find_rel((wl_col_session_t *)raw,
            "output");
    CHECK(actual && reference && actual->nrows == 1 && reference->nrows == 1
        && actual->ncols == KEY_RESULT && reference->ncols == KEY_RESULT);
    for (unsigned c = 0; c < KEY_WIDE; ++c)
        CHECK(actual->columns[c][0] == a[KEY_WIDE + c]
            && reference->columns[c][0] == a[KEY_WIDE + c]);
    CHECK(actual->columns[KEY_WIDE][0] == 99
        && reference->columns[KEY_WIDE][0] == 99);
    CHECK(!wl_session_try_destroy(&session) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)));
    wl_columnar_memory_governor_ref_release(governor);
    key_environment("WIRELOG_JOIN_BATCH_BYTES", saved_bytes);
    key_environment("WIRELOG_JOIN_BATCH_STRICT", saved_strict);
    free(saved_bytes); free(saved_strict);
}

enum probe_family {
    PROBE_KEYS, PROBE_WIDE, PROBE_BINDINGS, PROBE_DOMAINS, PROBE_SCHEMA,
    PROBE_EVENTS, PROBE_ALLOCATIONS, PROBE_FINAL_DENIAL, PROBE_FIRST,
    PROBE_FUNDING
};

typedef struct {
    const char *name;
    enum probe_family family;
    unsigned group;
} probe_case_t;

static void
first_probe_history(unsigned probe_op)
{
    first_probe_op = probe_op;
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
    first_probe_op = first_probe_mode = 0;
    first_probe_limit = first_probe_prefix = first_probe_paid = 0;
}

static void
run_case(const probe_case_t *test)
{
    switch (test->family) {
    case PROBE_KEYS:
        test_key_literal_boundaries();
        test_key_name_semantics();
        test_key_saved_attempt();
        break;
    case PROBE_WIDE:
        test_key_generated_wide(test->group);
        break;
    case PROBE_BINDINGS:
        bound_probe_compatibility();
        break;
    case PROBE_DOMAINS:
        for (unsigned kind = test->group * 3; kind < (test->group + 1) * 3;
            ++kind) run(kind, 0, 0, false);
        break;
    case PROBE_SCHEMA: {
        unsigned first = test->group ? 5 : 1;
        unsigned end = test->group ? 8 : 5;
        for (unsigned point = first; point < end; ++point)
            for (unsigned work = 0; work < 2; ++work) run(0, point, 0, work);
        break;
    }
    case PROBE_EVENTS:
        for (unsigned event = 1 + test->group * 4;
            event < 5 + test->group * 4; ++event)
            for (unsigned work = 0; work < 2; ++work) run(0, 0, event, work);
        break;
    case PROBE_ALLOCATIONS: {
        unsigned end = test->group == 2 ? 13 : (test->group + 1) * 4;
        for (unsigned fault = test->group * 4; fault < end; ++fault) {
            allocation_failure = (int)fault; run(0, 0, 0, false);
        }
        allocation_failure = -1;
        break;
    }
    case PROBE_FINAL_DENIAL:
        for (unsigned work = 0; work < 2; ++work)
            run(3, 0, WL_COLUMNAR_JOIN_PIPELINE_TEST_SCHEMA_FINAL, work);
        wl_columnar_memory_reservation_init(&peer);
        deny_schema = true; run(0, 0, 0, false);
        CHECK(!deny_schema);
        break;
    case PROBE_FIRST:
        first_probe_history(test->group ? WL_PLAN_OP_ANTIJOIN
            : WL_PLAN_OP_SEMIJOIN);
        break;
    case PROBE_FUNDING: {
        uint64_t measured = run(0, 0, 0, false);
        funding_test = true;
        CHECK(run(0, 0, 0, false) == measured);
        funding_test = false;
        break;
    }
    }
}

/* Numeric right-filter probe qualification. SEMI is explicitly IR generated. */
static wirelog_ir_node_t *
filtered_probe_anti(wirelog_ir_node_t *node)
{
    if (!node || node->type == WIRELOG_IR_ANTIJOIN) return node;
    for (uint32_t i = 0; i < node->child_count; ++i) {
        wirelog_ir_node_t *found = filtered_probe_anti(node->children[i]);
        if (found) return found;
    }
    return NULL;
}

static unsigned filtered_phase, filtered_fault, filtered_rows,
    filtered_build_rows, filtered_probe_rows;
static bool filtered_fired, filtered_scalar_mode;
static unsigned filtered_scalar_owners;
static wl_columnar_memory_governor_ref_t *filtered_governor;
static wl_columnar_memory_reservation_t filtered_peer;

static void
filtered_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    CHECK(control == active);
    if (phase == 44) {
        ++filtered_rows;
        if (filtered_scalar_mode) {
            wl_col_session_t *col = (wl_col_session_t *)active_session;
            wl_columnar_eval_serial_framed_attempt_t *framed = NULL;
            wl_columnar_filter_context_t *context = NULL;
            CHECK(col && col->cleanup_active &&
                !wl_columnar_eval_stack_framed_context(col->cleanup_active, col,
                &framed)
                && !wl_columnar_eval_stack_filter_context_owned(
                    framed->child.frame,
                    &context, &framed->child)
                && context->scalar_program && !context->program);
            ++filtered_scalar_owners;
        }
    }
    /* Main keeps physical row chains:28 inserts selected right rows;
     * 17 visits a real candidate during membership probing. */
    if (phase == 28) ++filtered_build_rows;
    if (phase == 17) ++filtered_probe_rows;
    if (!filtered_fault || filtered_fired || phase != filtered_phase) return;
    filtered_fired = true;
    if (filtered_fault == 7) {
        wl_columnar_memory_governor_t *governor =
            wl_columnar_memory_governor_ref_get(filtered_governor);
        uint64_t available = (UINT64_C(128) << 20)
            - wl_columnar_memory_reserved(governor);
        CHECK(available > 1 && wl_columnar_memory_reserve(governor,
            available - 1, &filtered_peer));
    } else if (filtered_fault == 8)
        wl_columnar_join_test_attempt_fail_allocation = 0;
    else if (filtered_fault & 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

typedef struct {
    unsigned events, mask;
    bool semi;
} filtered_events_t;

static void
filtered_capture(const char *name, const int64_t *row, uint32_t columns,
    int32_t diff, void *payload)
{
    if (strcmp(name, "output")) return;
    filtered_events_t *events = payload;
    CHECK(columns == 1 && diff == 1);
    unsigned bit = row[0] == 64 ? 1 : row[0] == 17 ? 2 : row[0] == 99 ? 4 : 0;
    CHECK(bit && !(events->mask & bit)
        && (events->semi ? bit != 4 : bit == 4));
    events->mask |= bit;
    ++events->events;
}

static void
filtered_generated_probes(unsigned domain)
{
    for (unsigned semi = 0; semi < 2; ++semi)
        for (unsigned batched = 0; batched < 2; ++batched)
            for (unsigned fault = 0; fault < 9; ++fault) {
                wirelog_error_t error;
                const char *source = domain ?
                    ".decl left(k:int64)\n.decl right(k:int64,v:symbol)\n"
                    ".decl output(k:int64)\n"
                    "output(k):-left(k),!right(k,\"chosen\").\n" :
                    ".decl left(k:int64)\n.decl right(k:int64,v:int64)\n"
                    ".decl output(k:int64)\n"
                    "output(k):-left(k),!right(k,7).\n";
                wirelog_program_t *program = wirelog_parse_string(source,
                        &error);
                CHECK(program);
                if (semi) {
                    wirelog_ir_node_t *anti = NULL;
                    for (uint32_t r = 0; r < program->rule_count && !anti; ++r)
                        anti = filtered_probe_anti(program->rules[r].ir_root);
                    CHECK(anti);
                    /* Explicit IR membership variant of the natural ANTI plan. */
                    anti->type = WIRELOG_IR_SEMIJOIN;
                    CHECK(!wl_ir_program_rebuild_relation_irs(program));
                }
                wl_plan_t *plan = NULL;
                CHECK(!wl_plan_from_program(program, &plan));
                unsigned emitted = 0;
                for (uint32_t st = 0; st < plan->stratum_count; ++st)
                    for (uint32_t r = 0; r < plan->strata[st].relation_count;
                        ++r){
                        const wl_plan_relation_t *rel =
                            &plan->strata[st].relations[r];
                        for (uint32_t o = 0; o < rel->op_count; ++o) {
                            const wl_plan_op_t *op = &rel->ops[o];
                            if (op->op !=
                                (semi ? WL_PLAN_OP_SEMIJOIN :
                                WL_PLAN_OP_ANTIJOIN)) continue;
                            CHECK(op->right_filter_expr.size &&
                                op->right_filter_expr.data
                                && !op->materialized && op->key_count == 1
                                && !strcmp(op->right_relation, "right"));
                            ++emitted;
                        }
                    }
                CHECK(emitted == 1);
                wl_evaluation_control_t *control = NULL;
                CHECK(!wl_evaluation_control_create(UINT64_C(10000000),
                    &control));
                wl_columnar_memory_resolution_t resolution = {
                    .budget_bytes = UINT64_C(128) << 20,
                        .usable_bytes = UINT64_C(128) << 20,
                        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
                };
                filtered_governor =
                    wl_columnar_memory_governor_ref_create(&resolution);
                CHECK(filtered_governor);
                wl_session_options_t options; wl_session_options_init(&options);
                options.evaluation_control = control;
                options.memory_governor = filtered_governor;
                wl_session_t *sessions[2] = {NULL, NULL};
                CHECK(!wl_session_create_with_options(wl_backend_columnar(),
                    plan, 1,
                    &options, &sessions[0]));
                CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                    &sessions[1]));
                const int64_t left[] = {64, 17, 99};
                int64_t right[130];
                for (unsigned r = 0; r < 65; ++r) {
                    right[2*r] = 1000 + r; right[2*r+1] = 8;
                }
                right[0] = 64; /* Rejected duplicate cannot create membership. */
                right[2*3] = 64; right[2*3+1] = 7;
                right[2*64] = 17; right[2*64+1] = 7;
                if (domain) {
                    /* Real strings inserted after plan construction. */
                    int64_t yes = wl_intern_put(plan->intern, "chosen");
                    int64_t no = wl_intern_put(plan->intern, "rejected");
                    CHECK(yes >= 0 && no >= 0);
                    for (unsigned r = 0; r < 65; ++r)
                        right[2*r+1] = r == 3 || r == 64 ? yes : no;
                }
                for (unsigned i = 0; i < 2; ++i) {
                    filtered_events_t events = {.semi = semi != 0};
                    CHECK(!wl_session_insert(sessions[i], "left", left, 3, 1)
                        && !wl_session_insert(sessions[i], "right", right, 65,
                        2));
                    wl_session_set_delta_cb(sessions[i], filtered_capture,
                        &events);
                    wl_col_session_t *col = (wl_col_session_t *)sessions[i];
                    col->join_batch_bytes = batched ? 256 : 0;
                    if (!i) {
                        active = control; active_session = sessions[i];
                        filtered_scalar_mode = domain != 0;
                        filtered_scalar_owners = 0;
                        filtered_fault = fault; filtered_fired = false;
                        wl_columnar_memory_reservation_init(&filtered_peer);
                        filtered_rows = filtered_build_rows =
                            filtered_probe_rows = 0;
                        filtered_phase = fault >= 7 ? 43 : fault >=
                            5 ? 17 : fault >= 3 ? 28 : 44;
                        wl_columnar_join_test_attempt_checkpoint =
                            filtered_checkpoint;
                    }
                    int rc = finish(sessions[i]);
                    if (!i && fault) {
                        CHECK(filtered_fired && !events.events && !events.mask
                            && !session_find_rel(col, "output"));
                        CHECK(rc == (fault == 8 ? ENOMEM : fault == 7 ? ENOSPC
                            : fault &
                            1 ? WL_EVALUATION_CONTROL_CANCELLED :
                            WL_EVALUATION_CONTROL_WORK_LIMIT));
                        CHECK(wl_session_budget_denied(sessions[i]) ==
                            (fault == 7));
                        if (fault ==
                            7) CHECK(wl_columnar_memory_rollback(
                                    &filtered_peer));
                        wl_columnar_join_test_attempt_fail_allocation = -1;
                        wl_evaluation_control_report_t report;
                        CHECK(!wl_evaluation_control_report(control, &report)
                            && report.result == rc && report.consumed
                            && !wl_evaluation_control_active_attempt_id(
                                control));
                        CHECK(!wl_evaluation_control_reset(control)
                            && !wl_evaluation_control_configure(control, 0));
                        filtered_fault = 0;
                        filtered_scalar_owners = 0;
                        filtered_rows = filtered_build_rows =
                            filtered_probe_rows = 0;
                        rc = finish(sessions[i]);
                    }
                    CHECK(!rc && events.events == (semi ? 2u : 1u)
                        && events.mask == (semi ? 3u : 4u));
                    if (!i) {
                        CHECK(filtered_rows == 65 && filtered_build_rows == 2
                            && filtered_probe_rows >= 2
                            && filtered_scalar_owners == (domain ? 65u : 0u));
                        wl_columnar_join_test_attempt_checkpoint = NULL;
                        active = NULL; active_session = NULL;
                    }
                    const col_rel_t *out = session_find_rel(col, "output");
                    CHECK(out && out->ncols == 1 &&
                        out->nrows == events.events);
                    unsigned mask = 0;
                    for (uint32_t r = 0; r < out->nrows; ++r)
                        mask |= out->columns[0][r] ==
                            64 ? 1 : out->columns[0][r] ==
                            17 ? 2 : out->columns[0][r] == 99 ? 4 : 0;
                    CHECK(mask == events.mask && !col->cleanup_active
                        && !col->cleanup_pending &&
                        !col->cleanup_reserved_bytes);
                    wl_mem_ledger_snapshot_t before, after;
                    wl_mem_ledger_snapshot(&col->mem_ledger, &before);
                    CHECK(!finish(sessions[i]) &&
                        events.events == (semi ? 2u : 1u));
                    wl_mem_ledger_snapshot(&col->mem_ledger, &after);
                    CHECK(before.current_bytes == after.current_bytes);
                    CHECK(!wl_session_try_destroy(&sessions[i]) &&
                        !sessions[i]);
                }
                wl_evaluation_control_release(control); wl_plan_free(plan);
                wirelog_program_free(program);
                CHECK(!wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(
                            filtered_governor)));
                wl_columnar_memory_governor_ref_release(filtered_governor);
                filtered_governor = NULL;
            }
}

/* The public optimizer produces this SEMI; no compiled IR is rewritten. */
static wl_session_t *natural_session;
static wl_evaluation_control_t *natural_control;
static unsigned natural_fault, natural_starts, natural_builds,
    natural_rejected, natural_emits, natural_variant, natural_key_column;
static bool natural_fired, natural_resumed;
static uint64_t natural_stopped_id;
static uint32_t natural_row_baseline, natural_row;
static const wl_plan_op_t *natural_emitted, *natural_body;

typedef struct {
    unsigned positive, negative, calls;
} natural_events_t;

static unsigned
natural_bit(int64_t x, int64_t z)
{
    return x == 1 && z == 9 ? 1u : x == 3 && z == 7 ? 2u
        : x == 3 && z == 8 ? 4u : x == 2 && z == 6 ? 8u
        : x == 4 && z == 5 ? 16u : 0;
}

static void
natural_capture(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *data)
{
    if (strcmp(name, "output")) return;
    natural_events_t *events = data;
    CHECK(width == 2 && (diff == 1 || diff == -1));
    unsigned bit = natural_bit(row[0], row[1]);
    unsigned *mask = diff == 1 ? &events->positive : &events->negative;
    CHECK(bit && !(*mask & bit));
    *mask |= bit; ++events->calls;
}

static void
natural_snapshot_callback(const char *name, const int64_t *row, uint32_t width,
    void *data)
{
    (void)name; (void)row; (void)width;
    ++*(unsigned *)data;
}

static void
natural_content(wl_session_t *session, unsigned expected)
{
    const col_rel_t *out = session_find_rel((wl_col_session_t *)session,
            "output");
    CHECK(out && out->ncols == 2);
    unsigned mask = 0;
    for (uint32_t r = 0; r < out->nrows; ++r) {
        unsigned bit = natural_bit(out->columns[0][r], out->columns[1][r]);
        CHECK(bit && !(mask & bit)); mask |= bit;
    }
    CHECK(mask == expected);
}

static void
natural_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    wl_col_session_t *col = (wl_col_session_t *)natural_session;
    wl_columnar_eval_serial_framed_attempt_t *frame = NULL;
    if (!col || !col->cleanup_active
        || wl_columnar_eval_stack_framed_context(col->cleanup_active, col,
        &frame))
        return;
    wl_columnar_filter_context_t *ctx = NULL;
    if (wl_columnar_eval_stack_filter_context_owned(frame->child.frame,
        &ctx, &frame->child) || !ctx->join_operation
        || ctx->join_operation->op != WL_PLAN_OP_SEMIJOIN) return;
    const wl_plan_op_t *op = ctx->join_operation;
    CHECK(control == natural_control && ctx->control == control
        && ctx->attempt_id == wl_evaluation_control_active_attempt_id(control)
        && ctx->session == col && ctx->identity == ctx && ctx->thread_valid
        && op->key_count == 1 && !op->project_count && !op->materialized
        && !strcmp(op->right_relation, "c")
        && (op == natural_emitted || op == natural_body
        || (!strcmp(op->left_keys[0], natural_body->left_keys[0])
        && !strcmp(op->right_keys[0], natural_body->right_keys[0]))));
    if (phase == 1) {
        ++natural_starts;
        if (natural_stopped_id) {
            CHECK(ctx->attempt_id != natural_stopped_id);
            natural_resumed = true;
        }
    }
    if (phase == 28) ++natural_builds;
    if (phase == 29) {
        CHECK(ctx->output && progress < ctx->source.nrows);
        natural_row = (uint32_t)progress;
        natural_row_baseline = ctx->output->nrows;
    }
    bool rejected = false;
    if (phase == 31) {
        CHECK(progress < ctx->source.nrows && op->left_keys[0]);
        uint32_t key_column = UINT32_MAX;
        for (uint32_t c = 0; c < ctx->source.width; ++c)
            if (ctx->source.names && ctx->source.names[c]
                && !strcmp(op->left_keys[0],
                ctx->source.names[c])) key_column = c;
        if (key_column == UINT32_MAX) {
            unsigned encoded = UINT_MAX; char extra;
            CHECK(sscanf(op->left_keys[0], "col%u%c", &encoded, &extra) == 1);
            key_column = encoded;
        }
        CHECK(key_column < ctx->source.width);
        int64_t w = ctx->source.columns[key_column][progress];
        natural_key_column = key_column;
        bool selected = natural_variant == 3
            || ((natural_variant == 0 || natural_variant == 4)
            && (w == 100 || w == 300));
        rejected = !selected;
        if (rejected) {
            CHECK(natural_row == progress && ctx->output
                && ctx->output->nrows == natural_row_baseline);
            ++natural_rejected;
        }
    }
    if (phase == 20) {
        CHECK(ctx->output && ctx->output->nrows &&
            progress == ctx->output->nrows
            && natural_key_column < ctx->output->ncols);
        int64_t w =
            ctx->output->columns[natural_key_column][ctx->output->nrows - 1];
        CHECK(w == 100 || w == 300 ||
            (natural_variant == 3 && (w == 200 || w == 400)));
        ++natural_emits;
    }
    if (!natural_fault || natural_fired
        || (natural_fault <= 2 ? !rejected ||
        phase != 31 : phase != 20)) return;
    natural_fired = true;
    if (natural_fault & 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

static void
reset_checkpoint(wl_col_session_t *session, unsigned phase, size_t cursor,
    bool after)
{
    if (&session->base != reset_session) return;
    wl_columnar_session_step_reset_t *r = &session->controlled_step_reset;
    if (phase == WL_COLUMNAR_SESSION_STEP_RESET_CLEAR && after
        && r->phase == WL_COLUMNAR_SESSION_STEP_RESET_RESTORE) {
        if (!reset_bytes) reset_bytes = cursor;
        CHECK(cursor == reset_bytes);
    }
    if (!reset_fault || reset_fired || after != reset_after) return;
    bool selected = false;
    if (reset_target < 3 && phase == WL_COLUMNAR_SESSION_STEP_RESET_CLEAR) {
        CHECK(reset_target == 0 || reset_bytes);
        size_t byte = after ? cursor >= 64 ? cursor - 64 : 0 : cursor;
        selected = reset_target == 0 ? !byte : reset_target == 1
            ? byte == (reset_bytes / 128) * 64
            : after ? r->phase == WL_COLUMNAR_SESSION_STEP_RESET_RESTORE
            : byte + 64 >= reset_bytes;
    } else if (reset_target < 6
        && phase == WL_COLUMNAR_SESSION_STEP_RESET_RESTORE) {
        selected = cursor == reset_target - 3 + (after ? 1u : 0u);
    } else if (reset_target == 6
        && phase == WL_COLUMNAR_SESSION_STEP_RESET_READY) selected = true;
    if (!selected) return;
    reset_fired = reset_recovery_pending = true;
    CHECK(!*reset_event_calls);
    /* This seam arms the real observation's cancel RMW, not a simulated
     * action result. Before/after is relative to actual metadata progress. */
    wl_evaluation_control_test_source_observation_cancel_at = 4;
}

static void
reset_blocked_tuple(const char *relation, const int64_t *row, uint32_t width,
    void *payload)
{
    (void)relation; (void)row; (void)width; (void)payload;
    CHECK(false);
}

typedef struct {
    wl_session_t *session;
    int result;
} reset_migration_t;

static void *
reset_migrate(void *payload)
{
    reset_migration_t *call = payload;
    call->result = wl_session_step(call->session);
    CHECK(!wl_session_source_operation_scope_owned(call->session));
    return NULL;
}

static void
reset_recover(wl_session_t *session, int rc)
{
    CHECK(rc == EAGAIN && reset_fired && !*reset_event_calls);
    wl_col_session_t *col = COL_SESSION(session);
    wl_columnar_session_step_reset_t *r = &col->controlled_step_reset;
    CHECK((r->identity == r && r->target == col->controlled_step)
        || (reset_target == 6 && reset_after && !r->identity
        && r->phase == WL_COLUMNAR_SESSION_STEP_RESET_NONE));
    size_t byte = r->byte; unsigned restore = r->restore;
    wl_evaluation_control_report_t original, after, source;
    CHECK(!wl_evaluation_control_report(session->evaluation_control,
        &original));
    uint64_t id =
        wl_evaluation_control_active_attempt_id(session->evaluation_control);
    uint64_t used = 0;
    if (id) CHECK(!wl_evaluation_control_test_active_consumed(
                session->evaluation_control, id, &used));
    const int64_t blocked[] = {999, 999};
    CHECK(wl_session_insert(session, "c", blocked, 1, 2) == EBUSY
        && wl_session_remove(session, "c", blocked, 1, 2) == EBUSY
        && wl_session_snapshot(session, reset_blocked_tuple, NULL) == EBUSY);
    if (r->identity) {
        CHECK(session->backend->session_insert(session, "c", blocked, 1,
            2) == EBUSY
            && session->backend->session_insert(session, "c", blocked, 0,
            2) == EBUSY
            && session->backend->session_remove(session, "c", blocked, 1,
            2) == EBUSY
            && session->backend->session_remove(session, "c", blocked, 0,
            2) == EBUSY
            && col_session_insert_incremental(session, "c", blocked, 1,
            2) == EBUSY);
        wirelog_on_delta_fn callback = col->delta_cb;
        void *data = col->delta_data;
        session->backend->session_set_delta_cb(session, NULL, NULL);
        CHECK(col->delta_cb == callback && col->delta_data == data);
        wirelog_compound_arg_t argument = {WIRELOG_TYPE_INT64, 999};
        uint64_t handle = UINT64_MAX;
        CHECK(session->backend->session_make_compound(session, "blocked", 1,
            &argument, &handle) == EBUSY
            && handle == WIRELOG_COMPOUND_HANDLE_NULL);
        wl_col_session_t *copy = malloc(sizeof(*copy)); CHECK(copy);
        wl_columnar_session_copy_without_ledger(copy, col);
        CHECK(!copy->controlled_step && !copy->controlled_step_reset.identity
            && copy->controlled_step_reset.phase ==
            WL_COLUMNAR_SESSION_STEP_RESET_NONE
            && !copy->controlled_step_reset.busy
            && !copy->controlled_step_reset.thread_valid
            && !copy->destroy_prepared && !copy->destroy_controlled_prepared
            && !wl_columnar_session_controlled_batch_authorized(copy,
            session->evaluation_control, id, r->operation_identity, NULL,
            NULL));
        free(copy);
    }
    wl_session_t *slot = session;
    CHECK(wl_session_try_destroy(&slot) == EBUSY && slot == session);
    CHECK(session_find_rel(col, "output") == reset_head
        && wl_columnar_relation_snapshot_equal(reset_head_snapshot,
        wl_columnar_relation_snapshot(reset_head))
        && r->byte == byte && r->restore == restore && !*reset_event_calls);
    CHECK(!wl_session_test_source_unstarted_reset_cancel(session, &source)
        && source.result == WL_EVALUATION_CONTROL_CANCELLED
        && source.consumed == 10);
    CHECK(!wl_evaluation_control_report(session->evaluation_control, &after)
        && after.attempt_id == original.attempt_id
        && after.limit == original.limit && after.consumed == original.consumed
        && after.stop_reason == original.stop_reason
        && after.execution_status == original.execution_status
        && after.cleanup_status == original.cleanup_status
        && after.result == original.result
        && wl_evaluation_control_active_attempt_id(
            session->evaluation_control) == id);
    if (id) {
        uint64_t current = 0;
        CHECK(!wl_evaluation_control_test_active_consumed(
                session->evaluation_control, id, &current) && current == used);
    }
    reset_recovery_pending = false;
    if (r->identity && r->phase < WL_COLUMNAR_SESSION_STEP_RESET_READY) {
        /* No Native actor lease survives the arm proof. A different actual
         * thread may perform one metadata-only packet, retaining Original. */
        reset_migration_t call = {session, EIO};
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, reset_migrate, &call)
            && !wl_thread_join(&thread) && call.result == EAGAIN
            && !*reset_event_calls
            && wl_evaluation_control_active_attempt_id(
                session->evaluation_control) == id);
        if (id) {
            uint64_t current = 0;
            CHECK(!wl_evaluation_control_test_active_consumed(
                    session->evaluation_control, id,
                    &current) && current == used);
        }
    }
}

static void
natural_generated_semi(bool batched)
{
    for (unsigned variant = 0; variant < (reset_enabled ? 1u : 5u); ++variant)
        for (unsigned fault = 0;
            fault < (reset_enabled ? 1u : variant ? 1u : 5u); ++fault) {
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(
                ".decl a(x:int64,y:int64)\n.decl b(y:int64,w:int64)\n"
                ".decl c(w:int64,z:int64)\n.decl output(x:int64,z:int64)\n"
                "output(x,z):-a(x,y),b(y,w),c(w,z).\n", &error);
            CHECK(program);
            wirelog_opt_config_t config = {0};
            config.enable_semijoin = true; config.max_join_search_space = 1024;
            CHECK(wirelog_optimize_with_config(program, &config, &error));
            wl_plan_t *plan = NULL;
            CHECK(!wl_plan_from_program(program, &plan));
            natural_emitted = natural_body = NULL;
            for (uint32_t st = 0; st < plan->stratum_count; ++st)
                for (uint32_t r = 0; r < plan->strata[st].relation_count; ++r) {
                    const wl_plan_relation_t *rel =
                        &plan->strata[st].relations[r];
                    for (uint32_t o = 0; o < rel->op_count; ++o)
                        if (rel->ops[o].op == WL_PLAN_OP_SEMIJOIN) {
                            CHECK(!natural_emitted);
                            natural_emitted = &rel->ops[o];
                        }
                    if (plan->strata[st].bodies) {
                        const wl_plan_body_t *body =
                            &plan->strata[st].bodies[r];
                        for (uint32_t o = 0; o < body->op_count; ++o)
                            if (body->ops[o].op == WL_PLAN_OP_SEMIJOIN) {
                                CHECK(!natural_body);
                                natural_body = &body->ops[o];
                            }
                    }
                }
            CHECK(natural_emitted && natural_body
                && natural_emitted->key_count == 1 &&
                !natural_emitted->project_count
                && !natural_emitted->materialized
                && !strcmp(natural_emitted->right_relation, "c"));
            wl_evaluation_control_t *control = NULL;
            /* Generous host allowance; independent primitive exact/one-short
             * tariffs are tested separately and are not inferred from this. */
            CHECK(!wl_evaluation_control_create(UINT64_C(10000000), &control));
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = UINT64_C(128) << 20,
                    .usable_bytes = UINT64_C(128) << 20,
                    .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
            };
            wl_columnar_memory_governor_ref_t *governor =
                wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor);
            wl_session_options_t options; wl_session_options_init(&options);
            options.evaluation_control = control;
            options.memory_governor = governor;
            wl_session_t *sessions[2] = {NULL, NULL};
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1,
                &options, &sessions[0])
                && !wl_session_create(wl_backend_columnar(), plan, 1,
                &sessions[1]));
            const int64_t a[] = {1, 10, 2, 20, 3, 30, 4, 40};
            const int64_t b[] = {10, 100, 20, 200, 30, 300, 40, 400};
            const int64_t seed[] = {100, 9, 300, 7, 300, 8, 300, 7};
            const int64_t absent[] = {500, 1};
            const int64_t all[] = {100, 9, 200, 6, 300, 7, 400, 5};
            const int64_t *right = variant == 2 ? absent : variant ==
                3 ? all : seed;
            uint32_t count = variant == 1 ? 0 : variant == 2 ? 1
                : variant == 3 || variant == 4 ? 4 : 3;
            unsigned expected = variant == 1 || variant == 2 ? 0
                : variant == 3 ? 27 : 7;
            for (unsigned i = 0; i < 2; ++i) {
                wl_session_t *session = sessions[i];
                wl_col_session_t *col = (wl_col_session_t *)session;
                col->join_batch_bytes = batched ? 256 : 0;
                if (reset_enabled && !i) CHECK(session->source_c1_admission);
                CHECK(!wl_session_insert(session, "a", a, 4, 2)
                    && !wl_session_insert(session, "b", b, 4, 2)
                    && !wl_session_insert(session, "c", right, count, 2));
                natural_events_t events = {0};
                wl_session_set_delta_cb(session, natural_capture, &events);
                if (!i) {
                    natural_session = session; natural_control = control;
                    natural_fault = fault; natural_fired = false;
                    natural_variant = variant;
                    natural_stopped_id = 0; natural_resumed = false;
                    natural_starts = natural_builds = natural_rejected =
                        natural_emits = 0;
                    wl_columnar_join_test_attempt_checkpoint =
                        natural_checkpoint;
                }
                const char *inputs[] = {"a", "b", "c"};
                col_relation_snapshot_t input_before[3];
                for (unsigned input = 0; input < 3; ++input)
                    input_before[input] = wl_columnar_relation_snapshot(
                        session_find_rel(col, inputs[input]));
                uint64_t reserved_before = wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor));
                int rc = finish(session);
                if (!i && fault) {
                    wl_evaluation_control_report_t stopped_report;
                    CHECK(!wl_evaluation_control_report(control,
                        &stopped_report));
                    for (unsigned input = 0; input < 3; ++input)
                        CHECK(wl_columnar_relation_snapshot_equal(
                                input_before[input],
                                wl_columnar_relation_snapshot(session_find_rel(
                                    col,
                                    inputs[input]))));
                    CHECK(col->pending_input_change);
                    bool retained_work = batched && !(fault & 1);
                    if (retained_work) {
                        uint64_t owner[8], producer[6];
                        CHECK(!wl_columnar_session_test_retained_work_owner(col,
                            owner)
                            && owner[0] && owner[1] && owner[2] && owner[3] == 8
                            && owner[6] && owner[7]);
                        CHECK(!wl_columnar_session_test_producer_view(col,
                            producer)
                            && producer[2] && producer[0] == producer[1]);
                        CHECK(session->backend->session_controlled_step_state(
                                session)
                            == WL_CONTROLLED_STEP_PENDING);
                        const int64_t denied[] = {5, 50};
                        CHECK(wl_session_insert(session, "a", denied, 1,
                            2) == EBUSY
                            && wl_session_remove(session, "a", a, 1,
                            2) == EBUSY);
                        unsigned snapshot_calls = 0;
                        CHECK(wl_session_snapshot(session,
                            natural_snapshot_callback,
                            &snapshot_calls) == EBUSY && !snapshot_calls);
                        wl_session_t *same = session;
                        CHECK(wl_session_try_destroy(&same) == EBUSY &&
                            same == session);
                        /* The genuine parent owns its private image/metadata.
                         * No byte tolerance or refund is inferred from WORK. */
                    } else CHECK(wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(
                                    governor)) == reserved_before);
                    CHECK(natural_fired && !events.calls &&
                        !session_find_rel(col, "output")
                        && rc == (fault & 1 ? WL_EVALUATION_CONTROL_CANCELLED
                        : WL_EVALUATION_CONTROL_WORK_LIMIT));
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(control, &report)
                        && report.result == rc && report.consumed
                        && !wl_evaluation_control_active_attempt_id(control)
                        && !col->cleanup_active && !col->cleanup_pending
                        && !col->cleanup_reserved_bytes);
                    CHECK(report.attempt_id == stopped_report.attempt_id
                        && report.limit == stopped_report.limit
                        && report.consumed == stopped_report.consumed
                        && report.stop_reason == stopped_report.stop_reason
                        && report.execution_status ==
                        stopped_report.execution_status
                        && report.cleanup_status ==
                        stopped_report.cleanup_status
                        && report.result == stopped_report.result);
                    natural_stopped_id = report.attempt_id;
                    CHECK(!wl_evaluation_control_reset(control));
                    natural_fault = 0;
                    natural_starts = natural_builds = natural_rejected =
                        natural_emits = 0;
                    rc = finish(session);
                    CHECK(natural_resumed);
                }
                CHECK(!rc && events.positive == expected && !events.negative);
                if (!i) {
                    CHECK(natural_starts && (!count || natural_builds)
                        && (!expected || natural_emits));
                    if (!variant || variant == 4) CHECK(natural_rejected);
                    wl_columnar_join_test_attempt_checkpoint = NULL;
                    natural_session = NULL; natural_control = NULL;
                }
                natural_content(session, expected);
                wl_mem_ledger_snapshot_t before, after;
                wl_mem_ledger_snapshot(&col->mem_ledger, &before);
                unsigned old_calls = events.calls;
                uint64_t complete_reserved = wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor));
                CHECK(!finish(session) && events.calls == old_calls
                    && complete_reserved == wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)));
                wl_mem_ledger_snapshot(&col->mem_ledger, &after);
                CHECK(before.current_bytes == after.current_bytes);
                if (!variant) {
                    events = (natural_events_t){0};
                    const int64_t added[] = {200, 6}, removed[] = {300, 7};
                    if (reset_enabled && !i) {
                        reset_session = session;
                        reset_head = session_find_rel(col, "output");
                        CHECK(reset_head);
                        reset_head_snapshot =
                            wl_columnar_relation_snapshot(reset_head);
                        reset_event_calls = &events.calls;
                        reset_fired = reset_recovery_pending = false;
                        wl_columnar_session_test_step_reset_phase =
                            reset_checkpoint;
                    }
                    CHECK(!wl_session_insert(session, "c", added, 1, 2)
                        && !finish(session));
                    CHECK(events.positive == 8 && !events.negative &&
                        events.calls == 1);
                    natural_content(session, 15);
                    if (reset_enabled && !i) {
                        CHECK(!reset_recovery_pending &&
                            (reset_fired == reset_fault));
                        wl_columnar_session_test_step_reset_phase = NULL;
                        reset_session = NULL;
                    }
                    events = (natural_events_t){0};
                    CHECK(!wl_session_remove(session, "c", removed, 1, 2)
                        && !finish(session));
                    CHECK(!events.positive && events.negative == 2 &&
                        events.calls == 1);
                    natural_content(session, 13);
                }
                CHECK(!wl_session_try_destroy(&sessions[i]) && !sessions[i]);
            }
            wl_evaluation_control_release(control); wl_plan_free(plan);
            wirelog_program_free(program);
            natural_emitted = natural_body = NULL;
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
        }
}

int
main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "reset-envelope")) {
        reset_enabled = true;
        /* Select the real configured constructor route. Setting the batch
         * field after construction does not activate its Source owner. */
        const char *old_value = getenv("WIRELOG_JOIN_BATCH_BYTES");
        char *saved = NULL;
        if (old_value) {
            saved = malloc(strlen(old_value) + 1); CHECK(saved);
            memcpy(saved, old_value, strlen(old_value) + 1);
        }
#if defined(_WIN32)
        CHECK(!_putenv_s("WIRELOG_JOIN_BATCH_BYTES", "256"));
#else
        CHECK(!setenv("WIRELOG_JOIN_BATCH_BYTES", "256", 1));
#endif
        {
            reset_fault = false;
            natural_generated_semi(true);
            CHECK(reset_bytes >= 128);
            reset_fault = true;
            for (reset_target = 0; reset_target != 7; ++reset_target)
                for (unsigned after = 0; after != 2; ++after) {
                    reset_after = after != 0;
                    natural_generated_semi(true);
                }
        }
#if defined(_WIN32)
        CHECK(!_putenv_s("WIRELOG_JOIN_BATCH_BYTES", saved ? saved : ""));
#else
        CHECK(saved ? !setenv("WIRELOG_JOIN_BATCH_BYTES", saved, 1)
            : !unsetenv("WIRELOG_JOIN_BATCH_BYTES"));
#endif
        free(saved);
        return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "natural-semi")) {
        natural_generated_semi(false); return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "natural-semi-batch")) {
        natural_generated_semi(true); return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "filtered-numeric")) {
        filtered_generated_probes(0); return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "filtered-scalar")) {
        filtered_generated_probes(1); return 0;
    }
    static const probe_case_t cases[] = {
        {"keys", PROBE_KEYS, 0},
        {"keys-wide0", PROBE_WIDE, 0}, {"keys-wide1", PROBE_WIDE, 1},
        {"keys-wide2", PROBE_WIDE, 2}, {"bindings", PROBE_BINDINGS, 0},
        {"domains0", PROBE_DOMAINS, 0}, {"domains1", PROBE_DOMAINS, 1},
        {"domains2", PROBE_DOMAINS, 2}, {"domains3", PROBE_DOMAINS, 3},
        {"domains4", PROBE_DOMAINS, 4}, {"domains5", PROBE_DOMAINS, 5},
        {"schema0", PROBE_SCHEMA, 0}, {"schema1", PROBE_SCHEMA, 1},
        {"events0", PROBE_EVENTS, 0}, {"events1", PROBE_EVENTS, 1},
        {"events2", PROBE_EVENTS, 2}, {"events3", PROBE_EVENTS, 3},
        {"events4", PROBE_EVENTS, 4},
        {"allocations0", PROBE_ALLOCATIONS, 0},
        {"allocations1", PROBE_ALLOCATIONS, 1},
        {"allocations2", PROBE_ALLOCATIONS, 2},
        {"schema-final-denial", PROBE_FINAL_DENIAL, 0},
        {"first-semi", PROBE_FIRST, 0}, {"first-anti", PROBE_FIRST, 1},
        {"funding", PROBE_FUNDING, 0}
    };
    if (argc > 2) return 2;
    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        if (argc == 1) run_case(&cases[i]);
        else if (!strcmp(argv[1], cases[i].name)) {
            run_case(&cases[i]); return 0;
        }
    }
    return argc == 1 ? 0 : 2;
}
