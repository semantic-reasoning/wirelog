/* Production session selection, without manual prepared API calls. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static unsigned publication_heads, finish_heads, acknowledgments;
static unsigned funding_injections;

static uint64_t
grant_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t limit)
{
    if (action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE
        || !session->cleanup_active || funding_injections == 24
        || !wl_columnar_eval_stack_auto_present(session->cleanup_active))
        return limit;
    static const uint64_t short_limits[] = {383, 511, 703, 895, 1023, 1151};
    CHECK(!session_find_rel((wl_col_session_t *)session, "output"));
    return short_limits[funding_injections++ % 6];
}

typedef struct {
    int64_t rows[8][3];
    int differences[8];
    unsigned count;
} capture_t;

static void
on_delta(const char *name, const int64_t *row, uint32_t width, int difference,
    void *data)
{
    if (strcmp(name, "output")) return;
    capture_t *capture = data;
    CHECK(width == 3 && capture->count < 8);
    memcpy(capture->rows[capture->count], row, sizeof(capture->rows[0]));
    capture->differences[capture->count++] = difference;
}

static void
on_tuple(const char *name, const int64_t *row, uint32_t width, void *data)
{
    on_delta(name, row, width, 1, data);
}

static int
drive(wl_session_t *session, bool snapshot, capture_t *capture)
{
    int result = EAGAIN;
    for (unsigned turn = 0; result == EAGAIN && turn < 1000000; ++turn)
        result = snapshot ? wl_session_snapshot(session, on_tuple, capture)
            : wl_session_step(session);
    return result;
}

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    (void)token;
    (void)phase;
}

static void
prepared_checkpoint(wl_columnar_eval_stack_unary_prepared_t *p)
{
    if (p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD)
        ++publication_heads;
    if (p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD)
        ++finish_heads;
    if (p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY)
        ++acknowledgments;
}

static void
test_generated(bool snapshot, bool observed, bool empty)
{
    const char *text = ".decl a(k:int64,x:int64)\n"
        ".decl b(k:int64,t:int64,y:int64)\n"
        ".decl c(t:int64,z:int64)\n"
        ".decl output(x:int64,y:int64,z:int64)\n"
        "output(x,y,z) :- a(k,x), b(k,t,y), c(t,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    unsigned joins = 0;
    for (unsigned s = 0; s < plan->stratum_count; ++s)
        for (unsigned r = 0; r < plan->strata[s].relation_count; ++r)
            for (unsigned o = 0; o < plan->strata[s].relations[r].op_count;
                ++o){
                wl_plan_op_type_t op = plan->strata[s].relations[r].ops[o].op;
                CHECK(op != WL_PLAN_OP_LFTJ);
                if (op == WL_PLAN_OP_JOIN) ++joins;
            }
    CHECK(joins == 2);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    capture_t events = {0}, captured = {0};
    if (observed) wl_session_set_delta_cb(session, on_delta, &events);
    const int64_t a[] = {1, 10, 2, 20};
    const int64_t b[] = {1, 7, 100, 2, 8, 200};
    const int64_t c[] = {7, 1000, 8, 2000};
    wl_session_t *sessions[] = {session, raw};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(sessions[i], "a", a, 2, 2));
        CHECK(!wl_session_insert(sessions[i], "b", b, 2, 3));
        if (!empty) CHECK(!wl_session_insert(sessions[i], "c", c, 2, 2));
    }
    publication_heads = finish_heads = acknowledgments = 0;
    funding_injections = 0;
    wl_columnar_session_test_framed_grant_limit = grant_limit;
    wl_columnar_eval_stack_test_unary_prepared_checkpoint = prepared_checkpoint;
    int result = drive(session, snapshot, &captured);
    wl_columnar_session_test_framed_grant_limit = NULL;
    wl_columnar_eval_stack_test_unary_prepared_checkpoint = NULL;
    CHECK(!result && !drive(raw, false, NULL));
    col_rel_t *actual = session_find_rel((wl_col_session_t *)session, "output");
    col_rel_t *reference = session_find_rel((wl_col_session_t *)raw, "output");
    unsigned rows = empty ? 0u : 2u;
    CHECK(actual && reference && actual->nrows == rows && actual->ncols == 3
        && reference->nrows == rows && reference->ncols == 3
        && actual->schema_ok && reference->schema_ok);
    const int64_t literal[3][2] = {{10, 20}, {100, 200}, {1000, 2000}};
    for (unsigned column = 0; column < 3; ++column) {
        for (unsigned row = 0; row < rows; ++row) {
            CHECK(actual->columns[column][row] == literal[column][row]);
            CHECK(reference->columns[column][row] == literal[column][row]);
            if (snapshot) CHECK(captured.rows[row][column] ==
                    literal[column][row]);
            if (observed) CHECK(events.rows[row][column] ==
                    literal[column][row]);
        }
    }
    if (snapshot) CHECK(captured.count == rows);
    if (observed) CHECK(events.count == rows);
    for (unsigned row = 0; row < rows; ++row) {
        if (observed) CHECK(events.differences[row] == 1);
        if (snapshot) CHECK(captured.differences[row] == 1);
    }
    CHECK(publication_heads == 2 && finish_heads == 2 && acknowledgments == 4);
    CHECK(funding_injections == 24);
    unsigned delivered = events.count;
    CHECK(!drive(session, false, NULL) && events.count == delivered);
    if (!empty) {
        events.count = captured.count = 0;
        publication_heads = finish_heads = acknowledgments = 0;
        CHECK(!wl_session_remove(session, "c", c, 1, 2));
        CHECK(!wl_session_remove(raw, "c", c, 1, 2));
        wl_columnar_eval_stack_test_unary_prepared_checkpoint =
            prepared_checkpoint;
        CHECK(!drive(session, snapshot, &captured));
        wl_columnar_eval_stack_test_unary_prepared_checkpoint = NULL;
        CHECK(!drive(raw, false, NULL));
        actual = session_find_rel((wl_col_session_t *)session, "output");
        reference = session_find_rel((wl_col_session_t *)raw, "output");
        CHECK(actual && reference && actual->nrows == 1 &&
            reference->nrows == 1);
        for (unsigned column = 0; column < 3; ++column) {
            CHECK(actual->columns[column][0] == literal[column][1]);
            CHECK(reference->columns[column][0] == literal[column][1]);
        }
        if (observed) {
            CHECK(events.count == 1 && events.differences[0] == -1);
            for (unsigned column = 0; column < 3; ++column)
                CHECK(events.rows[0][column] == literal[column][0]);
        }
        if (snapshot) {
            CHECK(captured.count == 1 && captured.differences[0] == 1);
            for (unsigned column = 0; column < 3; ++column)
                CHECK(captured.rows[0][column] == literal[column][1]);
        }
        CHECK(publication_heads == 2 && finish_heads == 2 &&
            acknowledgments == 4);
        delivered = events.count;
        CHECK(!drive(session, false, NULL) && events.count == delivered);
    }
    CHECK(!wl_session_try_destroy(&session) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static unsigned abort_consume_stage;
static const void *abort_launch;
static uint64_t abort_launch_id;

static uint64_t
abort_consume_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t limit)
{
    if (!session->cleanup_active || abort_consume_stage == 3) return limit;
    wl_columnar_eval_stack_test_auto_observation_t observation;
    CHECK(!wl_columnar_eval_stack_test_auto_observe(session->cleanup_active,
        session, &observation));
    if (!observation.launch || observation.finish ||
        observation.receipt_phase !=
        WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED) return limit;
    if (abort_consume_stage == 0
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE) {
        abort_launch = observation.launch;
        abort_launch_id = observation.launch_id;
        wl_evaluation_control_request_cancel(session->base.evaluation_control);
        ++abort_consume_stage;
        return 895;
    }
    if (action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT) {
        CHECK(observation.launch == abort_launch
            && observation.launch_id == abort_launch_id);
        CHECK(!wl_evaluation_control_active_attempt_id(
                (wl_evaluation_control_t *)observation.launch));
        unsigned stage = abort_consume_stage++;
        return stage == 1 ? 895 : 896;
    }
    return limit;
}

static void
test_production_abort_consume(void)
{
    const char *text = ".decl edge(x:int64,y:int64)\n"
        ".decl output(x:int64,y:int64)\n"
        "output(x,y) :- edge(x,y).\n"
        "output(x,z) :- output(x,a), edge(a,b), edge(b,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    const int64_t edges[] = {1, 2, 2, 3, 3, 4};
    CHECK(!wl_session_insert(session, "edge", edges, 3, 2));
    abort_consume_stage = 0;
    abort_launch = NULL;
    wl_columnar_session_test_framed_grant_limit = abort_consume_limit;
    int result = EAGAIN;
    for (unsigned turn = 0; abort_consume_stage == 0; ++turn) {
        CHECK(turn < 100000 && result == EAGAIN);
        result = wl_session_step(session);
    }
    CHECK(result == EAGAIN && ((wl_col_session_t *)session)->cleanup_active);
    /* Invoke the actual production host classifier from a genuinely parked
     * session. Public checked destruction correctly refuses this live attempt. */
    CHECK(wl_session_try_destroy(&session) == EBUSY && session);
    CHECK(wl_columnar_session_test_host_abort((wl_col_session_t *)session)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_columnar_session_test_host_abort((wl_col_session_t *)session));
    wl_columnar_session_test_framed_grant_limit = NULL;
    CHECK(abort_consume_stage == 3);
    CHECK(drive(session, false, NULL) == WL_EVALUATION_CONTROL_CANCELLED);
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(!session_find_rel(columnar, "output") && !columnar->cleanup_active
        && !columnar->cleanup_pending && !columnar->cleanup_reserved_bytes);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == WL_EVALUATION_CONTROL_CANCELLED
        && !wl_evaluation_control_active_attempt_id(control));
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!drive(session, false, NULL));
    const col_rel_t *output = session_find_rel(columnar, "output");
    CHECK(output && output->nrows == 4 && output->ncols == 2);
    const int64_t literal[4][2] = {{1, 2}, {1, 4}, {2, 3}, {3, 4}};
    for (unsigned row = 0; row < 4; ++row)
        for (unsigned column = 0; column < 2; ++column)
            CHECK(output->columns[column][row] == literal[row][column]);
    CHECK(!wl_session_try_destroy(&session));
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static unsigned bound_failure_mode;
static bool bound_failure_injected;
static bool bound_expect_lftj, bound_saw_lftj;
static unsigned bound_turnover_mode;
static bool bound_turned_over;
static uint64_t bound_successor;
static uint64_t bound_metadata_limit;
static unsigned bound_metadata_reports, bound_metadata_factories;
static uint64_t bound_metadata_consumed;
static bool bound_factory_reentry;
static bool bound_factory_turned_over;
static unsigned bound_factory_reentries;
static bool bound_receipt_probe, bound_receipt_short_requested;
static unsigned bound_receipt_peer_count, bound_receipt_refusals,
    bound_receipt_acks;
static uint64_t bound_receipt_original;
static wl_evaluation_control_report_t bound_turnover_report;

/* Genuine earlier-head result, observed only while its actual owner is live. */
static bool bound_earlier_head;
static unsigned bound_earlier_candidates, bound_null_ledger_phase12;
static wl_col_session_t *bound_observed_session;
extern int wl_columnar_session_test_retirement_view(
    const wl_col_session_t *, uint64_t [16]);
extern void (*wl_columnar_session_test_retirement_checkpoint)(
    wl_evaluation_control_t *, unsigned, unsigned, uint32_t, const col_rel_t *);

static void
bound_earlier_checkpoint(wl_evaluation_control_t *control, unsigned role,
    unsigned action, uint32_t index, const col_rel_t *target)
{
    (void)control; (void)index;
    uint64_t view[16];
    CHECK(bound_observed_session &&
        !wl_columnar_session_test_retirement_view(bound_observed_session,
        view));
    if (role == 5 && action == 8 && view[14] == 1 && target
        && target->name && !strcmp(target->name, "first")) {
        CHECK(target->memory_governor && target->storage_owner == target
            && target->ncols == 2 && target->nrows == 2
            && target->retained_reserved_bytes > 0);
        ++bound_earlier_candidates;
    }
}

static uint64_t
bound_metadata_capacity(const wl_col_session_t *session,
    wl_columnar_session_metadata_purpose_t purpose, uint64_t limit)
{
    CHECK(purpose == WL_COLUMNAR_SESSION_METADATA_NATIVE_ERROR &&
        limit == 2304);
    ++bound_metadata_factories;
    if (bound_earlier_head) {
        uint64_t view[16];
        CHECK(!wl_columnar_session_test_retirement_view(session, view));
        if (view[11] == 5 && view[14] == 12) {
            /* Capacity is recorded even without a ledger pointer. */
            CHECK(!(view[12] & 2u) && (view[12] & 8u));
            ++bound_null_ledger_phase12;
        }
    }
    if (bound_turnover_mode == 5 && !bound_factory_turned_over
        && bound_metadata_limit == 2304) {
        /* Legal control turnover inside the capacity hook cannot refresh
         * the protected owner proved before this factory call. */
        wl_evaluation_control_t *control = session->base.evaluation_control;
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
            (wl_col_session_t *)session, &ctx));
        uint64_t old_id = ctx->attempt_id;
        CHECK(!wl_evaluation_control_active_attempt_id(control)
            && !wl_evaluation_control_reset(control)
            && !wl_evaluation_control_begin(control,
            session->base.operation_admission));
        bound_successor = wl_evaluation_control_active_attempt_id(control);
        CHECK(bound_successor && bound_successor != old_id
            && ctx->attempt_id == old_id
            && wl_columnar_session_test_native_error_phase(session)
            == WL_COLUMNAR_SESSION_NATIVE_ERROR_INERT);
        CHECK(!wl_evaluation_control_report(control, &bound_turnover_report));
        bound_factory_turned_over = true;
    }
    if (bound_factory_reentry) {
        bound_factory_reentry = false;
        wl_session_t *base = &((wl_col_session_t *)session)->base;
        wl_session_t *same = base;
        wl_columnar_session_native_error_phase_t phase =
            wl_columnar_session_test_native_error_phase(session);
        CHECK(wl_session_step(base) == EBUSY
            && wl_session_try_destroy(&same) == EBUSY && same == base
            && wl_columnar_session_test_native_error_phase(session) == phase);
        ++bound_factory_reentries;
    }
    return bound_metadata_limit ? bound_metadata_limit : limit;
}

static uint64_t
bound_failure_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t limit)
{
    if (bound_receipt_probe
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK) {
        const wl_columnar_eval_cleanup_receipt_t *receipt =
            &session->cleanup_receipts[bound_receipt_peer_count];
        CHECK(!session->cleanup_active
            && receipt->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
            && receipt->key.operation_attempt_id == bound_receipt_original);
        if (!bound_receipt_short_requested) {
            bound_receipt_short_requested = true;
            return 383;
        }
        return 384;
    }
    if (!session->cleanup_active) return limit;
    if (bound_failure_injected) {
        if (bound_turnover_mode && !bound_turned_over) {
            wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
            if (!wl_columnar_eval_stack_framed_context(session->cleanup_active,
                (wl_col_session_t *)session, &ctx)
                && wl_columnar_eval_stack_auto_error_drain(ctx)) {
                CHECK(wl_columnar_eval_stack_auto_cleanup_current(session));
                CHECK(!wl_columnar_eval_stack_auto_cleanup_shape(session, ctx));
                CHECK(!wl_columnar_eval_stack_auto_cleanup_ready(session)
                    && wl_columnar_eval_stack_auto_cleanup_ready(NULL) ==
                    EINVAL);
                wl_columnar_eval_serial_framed_attempt_t copy;
                memcpy(&copy, ctx, sizeof(copy));
                CHECK(wl_columnar_eval_stack_auto_cleanup_shape(session,
                    &copy) == EINVAL
                    && wl_columnar_eval_stack_auto_cleanup_shape(NULL,
                    ctx) == EINVAL
                    && !wl_columnar_eval_stack_auto_cleanup_current(NULL)
                    && !memcmp(ctx, &copy, sizeof(copy)));
                wl_evaluation_control_t *control =
                    session->base.evaluation_control;
                bound_receipt_original = ctx->attempt_id;
                int cause = bound_failure_mode == 2
                    ? WL_EVALUATION_CONTROL_CANCELLED
                    : WL_EVALUATION_CONTROL_WORK_LIMIT;
                CHECK(wl_evaluation_control_active_attempt_id(control) ==
                    ctx->attempt_id);
                CHECK(wl_evaluation_control_finish(control,
                    session->base.operation_admission, cause, 0) == cause);
                wl_evaluation_control_report_t ended;
                CHECK(!wl_evaluation_control_report(control, &ended)
                    && ended.attempt_id == ctx->attempt_id &&
                    ended.result == cause
                    && !ended.cleanup_status);
                if (bound_turnover_mode > 1 && bound_turnover_mode != 5)
                    CHECK(!wl_evaluation_control_reset(control));
                if (bound_turnover_mode > 2 && bound_turnover_mode != 5) {
                    CHECK(!wl_evaluation_control_begin(control,
                        session->base.operation_admission));
                    bound_successor =
                        wl_evaluation_control_active_attempt_id(control);
                    CHECK(bound_successor &&
                        bound_successor != ctx->attempt_id);
                    if (bound_turnover_mode == 3)
                        CHECK(!wl_evaluation_control_finish(control,
                            session->base.operation_admission, 0, 0));
                }
                CHECK(!wl_evaluation_control_report(control,
                    &bound_turnover_report));
                bound_turned_over = true;
            }
        }
        return limit;
    }
    wl_columnar_eval_stack_test_auto_observation_t observation;
    CHECK(!wl_columnar_eval_stack_test_auto_observe(session->cleanup_active,
        session, &observation));
    if (bound_failure_mode >= 2) {
        if (observation.finish || observation.receipt_phase !=
            WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD) return limit;
        CHECK(observation.saved_binding && observation.launch);
        if (bound_failure_mode == 2)
            wl_evaluation_control_request_cancel(
                session->base.evaluation_control);
        else CHECK(wl_evaluation_control_charge(
                    session->base.evaluation_control,
                    UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else {
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        if (wl_columnar_eval_stack_framed_context(session->cleanup_active,
            (wl_col_session_t *)session, &ctx) || !ctx->child_bound
            || ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
            || ctx->child.phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD)
            return limit;
        wl_columnar_filter_context_t *filter = NULL;
        CHECK(!wl_columnar_eval_stack_filter_context_owned(
                session->cleanup_active,
                &filter, &ctx->child));
        if (filter->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE) return limit;
        unsigned joins = 0;
        for (unsigned i = 0; i <= ctx->child.cursor; ++i)
            if (ctx->child.plan->ops[i].op == WL_PLAN_OP_JOIN) ++joins;
        if (joins != bound_failure_mode + 1) return limit;
        if (bound_expect_lftj) {
            CHECK(ctx->child.lftj.identity == &ctx->child.lftj
                && ctx->child.lftj.phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE
                && wl_columnar_eval_stack_auto_cleanup_shape(session,
                ctx) == EINVAL
                && wl_columnar_eval_stack_auto_cleanup_ready(session) ==
                EINVAL);
            bound_saw_lftj = true;
        } else CHECK(wl_columnar_eval_stack_auto_cleanup_shape(session,
                ctx) == EINVAL
                && wl_columnar_eval_stack_auto_cleanup_ready(session) ==
                EINVAL);
        /* The genuine live decision has no semantic error yet; structural
         * observations never turn it into an authorized cleanup destination. */
        CHECK(ctx->private_bindings && ctx->private_bindings->dynamic_entries
            && observation.saved_binding == ctx->private_bindings
            && observation.saved_binding_generation ==
            ctx->private_bindings->generation);
        if (bound_failure_mode == 1)
            CHECK(observation.receipt_phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED && observation.committed);
        uint64_t consumed;
        wl_evaluation_control_t *control = session->base.evaluation_control;
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        CHECK(!wl_evaluation_control_test_active_consumed(control, id,
            &consumed)
            && consumed < UINT64_MAX - 256);
        CHECK(!wl_evaluation_control_charge(control,
            UINT64_MAX - 1 - consumed - 255));
    }
    bound_failure_injected = true;
    return limit;
}

static void
bound_check_original(const wl_session_t *session)
{
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(session->evaluation_control, &report));
    CHECK(report.attempt_id == bound_turnover_report.attempt_id
        && report.result == bound_turnover_report.result
        && report.limit == bound_turnover_report.limit
        && report.consumed == bound_turnover_report.consumed
        && report.stop_reason == bound_turnover_report.stop_reason
        && report.execution_status == bound_turnover_report.execution_status
        && report.cleanup_status == bound_turnover_report.cleanup_status);
    if (bound_turnover_mode == 4
        || (bound_turnover_mode == 5 && bound_factory_turned_over)) {
        uint64_t work;
        CHECK(!wl_evaluation_control_test_active_consumed(
                session->evaluation_control,
                bound_successor, &work) && !work);
    } else CHECK(!wl_evaluation_control_active_attempt_id(
                session->evaluation_control));
}

static void
bound_metadata_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *grant, int protocol)
{
    (void)session; (void)protocol;
    if (bound_receipt_probe
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK) {
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(grant, &report));
        if (protocol == WL_EVALUATION_CONTROL_WORK_LIMIT) {
            CHECK(!report.consumed);
            ++bound_receipt_refusals;
        } else {
            CHECK(!protocol && report.consumed == 384);
            ++bound_receipt_acks;
        }
    }

}

/* The engine has already finished/detached/released private metadata.
* Only this seven-field value copy is available to instrumentation. */
static void
bound_metadata_report(const wl_col_session_t *session,
    wl_columnar_session_metadata_purpose_t purpose,
    wl_columnar_session_metadata_event_t event, int protocol,
    wl_evaluation_control_report_t report)
{
    (void)session; (void)protocol;
    CHECK(purpose == WL_COLUMNAR_SESSION_METADATA_NATIVE_ERROR
        && report.attempt_id == 1 && !report.execution_status
        && !report.cleanup_status);
    CHECK(report.limit == (bound_metadata_limit ? bound_metadata_limit : 2304));
    if (event == WL_COLUMNAR_SESSION_METADATA_SETUP_REFUSED) {
        CHECK(report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !report.consumed && protocol == EAGAIN);
    } else {
        CHECK(event == WL_COLUMNAR_SESSION_METADATA_BODY_RETURNED
            && !report.result && !report.stop_reason &&
            report.consumed == 2304);
    }
    bound_metadata_consumed = report.consumed;
    ++bound_metadata_reports;
}

typedef struct { wl_session_t *session; int result; } bound_peer_t;
static void *
bound_foreign_step(void *argument)
{
    bound_peer_t *peer = argument;
    CHECK(!wl_columnar_eval_stack_auto_cleanup_current(
            (wl_col_session_t *)peer->session));
    CHECK(wl_columnar_eval_stack_auto_cleanup_ready(
            (wl_col_session_t *)peer->session) == EBUSY);
    peer->result = wl_session_step(peer->session);
    return NULL;
}

static void
bound_foreign_refusal(wl_session_t *session)
{
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    wl_columnar_session_native_error_phase_t phase =
        wl_columnar_session_test_native_error_phase(columnar);
    wl_columnar_eval_stack_cleanup_frame_t *frame = columnar->cleanup_active;
    uint64_t bytes = columnar->cleanup_reserved_bytes;
    bound_peer_t peer = {.session = session};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, bound_foreign_step, &peer));
    CHECK(!wl_thread_join(&thread));
    if (wl_columnar_session_test_native_error_phase(columnar) != phase)
        fprintf(stderr, "foreign native cleanup result=%d phase=%u->%u\n",
            peer.result, (unsigned)phase,
            (unsigned)wl_columnar_session_test_native_error_phase(columnar));
    CHECK((peer.result == EBUSY || peer.result == EINVAL)
        && wl_columnar_session_test_native_error_phase(columnar) == phase
        && columnar->cleanup_active == frame &&
        columnar->cleanup_reserved_bytes == bytes);
    CHECK(session->backend->session_controlled_step_state(session)
        == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
    bound_check_original(session);
    wl_session_t *same = session;
    CHECK(wl_session_try_destroy(&same) == EBUSY && same == session);
}

static void
bound_metadata_refusal(wl_session_t *session, bool factory)
{
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    wl_columnar_session_native_error_phase_t phase =
        wl_columnar_session_test_native_error_phase(columnar);
    wl_columnar_eval_stack_cleanup_frame_t *frame = columnar->cleanup_active;
    uint64_t bytes = columnar->cleanup_reserved_bytes;
    unsigned reports = bound_metadata_reports;
    unsigned factories = bound_metadata_factories;
    uint64_t restore_identity = 0;
    if (factory) {
        /* A real create-error branch, outside the capacity hook. No other
         * creator runs in this quiescent fixture. The saved next identity
         * has never been issued; restore it before any retry/cleanup. */
        wl_evaluation_control_t *probe = NULL;
        CHECK(!wl_evaluation_control_create(0, &probe));
        uint64_t identity = wl_evaluation_control_identity(probe);
        CHECK(identity && identity < UINT64_MAX - 1);
        restore_identity = identity + 1;
        wl_evaluation_control_release(probe);
        CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX));
    } else bound_metadata_limit = 2303;
    int result = wl_session_step(session);
    if (factory)
        CHECK(!wl_evaluation_control_test_seed_next_identity(restore_identity));
    bound_metadata_limit = 0;
    CHECK(result == EAGAIN && bound_metadata_factories == factories + 1);
    if (factory) CHECK(bound_metadata_reports == reports);
    else CHECK(bound_metadata_reports == reports + 1 &&
            !bound_metadata_consumed);
    CHECK(wl_columnar_session_test_native_error_phase(columnar) == phase
        && columnar->cleanup_active == frame &&
        columnar->cleanup_reserved_bytes == bytes
        && session->backend->session_controlled_step_state(session)
        == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
    bound_check_original(session);
    wl_session_t *same = session;
    CHECK(wl_session_try_destroy(&same) == EBUSY && same == session);
    capture_t image = {0};
    CHECK(wl_session_snapshot(session, on_tuple, &image) == EBUSY
        && wl_session_snapshot(session, NULL, NULL) == EBUSY && !image.count);
    const int64_t duplicate[] = {1, 10};
    CHECK(wl_session_insert(session, "a", duplicate, 1, 2) == EBUSY);
    CHECK(wl_session_inspection_begin(session) ==
        WIRELOG_SOURCE_QUERY_UNSUPPORTED);
    /* v1 refuses controlled inspection before acquiring its reservation;
     * source_query requires that reservation and cannot be called here. */
    wirelog_on_delta_fn callback = columnar->delta_cb;
    void *userdata = columnar->delta_data;
    wl_session_set_delta_cb(session, NULL, NULL);
    CHECK(columnar->delta_cb == callback && columnar->delta_data == userdata);
}

static int
bound_ended_drive(wl_session_t *session)
{
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    bool first = false, registered = false, frame_freed = false, final = false;
    bool disposing = false;
    for (unsigned turns = 0; turns < 100000; ++turns) {
        if (bound_turned_over) {
            wl_columnar_session_native_error_phase_t phase =
                wl_columnar_session_test_native_error_phase(columnar);
            if (!first) {
                CHECK(phase == WL_COLUMNAR_SESSION_NATIVE_ERROR_INERT);
                bound_foreign_refusal(session);
                bound_metadata_refusal(session, true);
                bound_metadata_refusal(session, false);
                first = true;
            } else if (!registered &&
                phase == WL_COLUMNAR_SESSION_NATIVE_ERROR_CHILD) {
                bound_foreign_refusal(session);
                bound_metadata_refusal(session, false);
                registered = true;
            }
            if (!frame_freed && phase == WL_COLUMNAR_SESSION_NATIVE_ERROR_CHILD
                && !columnar->cleanup_active) {
                bound_metadata_refusal(session, false);
                frame_freed = true;
            }
            if (!disposing &&
                phase == WL_COLUMNAR_SESSION_NATIVE_ERROR_DISPOSE) {
                bound_metadata_refusal(session, false);
                disposing = true;
            }
            if (!final && phase == WL_COLUMNAR_SESSION_NATIVE_ERROR_FINAL) {
                CHECK(!columnar->cleanup_active && !columnar->cleanup_pending);
                bound_metadata_refusal(session, false);
                final = true;
            }
            bound_check_original(session);
        }
        bool metadata = bound_turned_over;
        unsigned reports = bound_metadata_reports;
        if (metadata) {
            bound_metadata_limit = 2304;
            if (bound_factory_reentries < 2) bound_factory_reentry = true;
        }
        int rc = wl_session_step(session);
        bound_metadata_limit = 0;
        if (metadata) CHECK(bound_metadata_reports == reports + 1
                && bound_metadata_consumed == 2304);
        if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT && bound_receipt_probe
            && bound_receipt_refusals == 1 && !bound_receipt_acks
            && wl_columnar_session_test_native_error_phase(columnar)
            == WL_COLUMNAR_SESSION_NATIVE_ERROR_CHILD) {
            CHECK(session->backend->session_controlled_step_state(session)
                == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
            bound_check_original(session);
            if (!bound_receipt_peer_count && !bound_failure_mode) {
                wl_columnar_eval_cleanup_receipt_t before, after;
                wl_columnar_eval_cleanup_receipt_key_t key =
                    columnar->cleanup_receipts[0].key;
                CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(columnar,
                    &key, &before));
                wl_columnar_eval_stack_cleanup_frame_t *nested = NULL;
                CHECK(!wl_columnar_eval_stack_cleanup_begin(columnar,
                    &nested));
                bound_receipt_probe = false;
                int refusal = wl_session_step(session);
                bound_receipt_probe = true;
                CHECK((refusal == EINVAL || refusal == EBUSY)
                    && columnar->cleanup_active == nested
                    && wl_columnar_session_test_native_error_phase(columnar)
                    == WL_COLUMNAR_SESSION_NATIVE_ERROR_CHILD
                    && session->backend->session_controlled_step_state(
                        session)
                    == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
                CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(columnar,
                    &key, &after) && !memcmp(&before, &after, sizeof(before)));
                bound_check_original(session);
                CHECK(!wl_columnar_eval_stack_cleanup_finish(&nested)
                    && !nested && !columnar->cleanup_active);
            }
            continue;
        }
        if (rc != EAGAIN) {
            CHECK(first && registered && frame_freed && disposing && final
                && bound_factory_reentries == 2
                && wl_columnar_session_test_native_error_phase(columnar)
                == WL_COLUMNAR_SESSION_NATIVE_ERROR_DONE
                && session->backend->session_controlled_step_state(session)
                == WL_CONTROLLED_STEP_IDLE);
            bound_check_original(session);
            return rc;
        }
    }
    CHECK(false);
    return EIO;
}

/* Unrelated backend EAGAIN without an attempt must retain its old EIO
 * disposition. This stub is only a negative framework case, no native owner. */
static int
bound_unrelated_yield(wl_session_t *session)
{
    (void)session;
    return EAGAIN;
}

static wl_controlled_step_state_t
bound_unrelated_idle(const wl_session_t *session)
{
    (void)session;
    return WL_CONTROLLED_STEP_IDLE;
}

static void
bound_unrelated_refusal(wl_session_t *session)
{
    const wl_compute_backend_t *backend = session->backend;
    wl_compute_backend_t unrelated = *backend;
    unrelated.session_step = bound_unrelated_yield;
    unrelated.session_controlled_step_state = bound_unrelated_idle;
    session->backend = &unrelated;
    CHECK(wl_session_step(session) == EIO);
    session->backend = backend;
    CHECK(!wl_evaluation_control_active_attempt_id(session->evaluation_control)
        && !((wl_col_session_t *)session)->controlled_step
        && !((wl_col_session_t *)session)->cleanup_active);
}

static void
test_dynamic_bound_failure(unsigned turnover)
{
    bound_turnover_mode = turnover;
    bound_turned_over = false;
    bound_expect_lftj = false;
    const char *text = ".decl a(k:int64,x:int64)\n"
        ".decl b(k:int64,t:int64,y:int64)\n.decl c(t:int64,z:int64)\n"
        ".decl output(x:int64,y:int64,z:int64)\n"
        "output(x,y,z) :- a(k,x), b(k,t,y), c(t,z).\n";
    if (bound_earlier_head)
        text = ".decl a(k:int64,x:int64)\n"
            ".decl b(k:int64,t:int64,y:int64)\n.decl c(t:int64,z:int64)\n"
            ".decl first(k:int64,x:int64)\n"
            ".decl output(x:int64,y:int64,z:int64)\n"
            "first(k,x) :- a(k,x).\n"
            "output(x,y,z) :- first(k,x), b(k,t,y), c(t,z).\n";
    for (unsigned mode = 0; mode < 4; ++mode) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(UINT64_MAX - 1, &control));
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_columnar_memory_governor_ref_t *governor = NULL;
        if (bound_receipt_probe || bound_earlier_head) {
            const wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = UINT64_C(512) << 20,
                    .usable_bytes = UINT64_C(512) << 20,
                    .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                    .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
                    .status = WL_COLUMNAR_MEMORY_OK
            };
            governor = wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor);
            options.memory_governor = governor;
        }
        wl_session_t *session = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        const int64_t a[] = {1, 10, 2, 20};
        const int64_t b[] = {1, 7, 100, 2, 8, 200};
        const int64_t c[] = {7, 1000, 8, 2000};
        CHECK(!wl_session_insert(session, "a", a, 2, 2));
        CHECK(!wl_session_insert(session, "b", b, 2, 3));
        CHECK(!wl_session_insert(session, "c", c, 2, 2));
        wl_columnar_eval_cleanup_receipt_key_t peers[
            WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES];
        uint64_t reserved_before = 0;
        if (bound_receipt_probe) {
            CHECK(bound_receipt_peer_count <
                WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES);
            /* These are real prior-attempt reservations. The generated
             * evaluation creates its own target in the next free slot. */
            CHECK(!wl_evaluation_control_begin(control,
                session->operation_admission));
            uint64_t peer_attempt =
                wl_evaluation_control_active_attempt_id(control);
            uint64_t peer_owner = wl_evaluation_control_identity(control);
            CHECK(peer_attempt && peer_owner);
            for (unsigned i = 0; i < bound_receipt_peer_count; ++i)
                CHECK(!wl_columnar_eval_stack_cleanup_receipt_reserve(
                        (wl_col_session_t *)session, peer_attempt, peer_owner,
                        &peers[i]));
            CHECK(!wl_evaluation_control_finish(control,
                session->operation_admission, 0, 0)
                && !wl_evaluation_control_reset(control));
            reserved_before = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor));
        }
        capture_t events = {0};
        wl_session_set_delta_cb(session, on_delta, &events);
        col_relation_snapshot_t before[3];
        const char *names[] = {"a", "b", "c"};
        for (unsigned i = 0; i < 3; ++i)
            before[i] = wl_columnar_relation_snapshot(
                session_find_rel((wl_col_session_t *)session, names[i]));
        if (turnover == 1 && !mode) bound_unrelated_refusal(session);
        bound_failure_mode = mode;
        bound_failure_injected = false;
        bound_turned_over = false;
        bound_successor = 0;
        bound_metadata_reports = bound_metadata_factories = 0;
        bound_factory_reentries = 0;
        bound_factory_reentry = false;
        bound_factory_turned_over = false;
        bound_receipt_short_requested = false;
        bound_receipt_refusals = bound_receipt_acks = 0;
        publication_heads = finish_heads = acknowledgments = 0;
        wl_columnar_session_test_framed_grant_limit = bound_failure_limit;
        wl_columnar_eval_stack_test_unary_prepared_checkpoint =
            prepared_checkpoint;
        int cause = mode == 2 ? WL_EVALUATION_CONTROL_CANCELLED
            : WL_EVALUATION_CONTROL_WORK_LIMIT;
        if (turnover) {
            wl_columnar_session_test_framed_grant_finished =
                bound_metadata_finished;
            wl_columnar_session_test_metadata_limit = bound_metadata_capacity;
            wl_columnar_session_test_metadata_finished = bound_metadata_report;
        }
        bound_earlier_candidates = bound_null_ledger_phase12 = 0;
        if (bound_earlier_head) {
            bound_observed_session = (wl_col_session_t *)session;
            wl_columnar_session_test_retirement_checkpoint =
                bound_earlier_checkpoint;
        }
        int result = turnover ? bound_ended_drive(session) : drive(session,
                false, NULL);
        if (bound_earlier_head) {
            wl_columnar_session_test_retirement_checkpoint = NULL;
            bound_observed_session = NULL;
            CHECK(bound_earlier_candidates && bound_null_ledger_phase12);
            CHECK(!session_find_rel((wl_col_session_t *)session, "first"));
        }
        wl_columnar_session_test_framed_grant_finished = NULL;
        wl_columnar_session_test_metadata_limit = NULL;
        wl_columnar_session_test_metadata_finished = NULL;
        if (result != cause)
            fprintf(stderr,
                "bound mode=%u turnover=%u ended=%u result=%d expected=%d\n",
                mode, turnover, bound_turned_over, result, cause);
        CHECK(result == cause && bound_failure_injected
            && (!turnover || bound_turned_over)
            && bound_factory_turned_over == (turnover == 5));
        wl_columnar_session_test_framed_grant_limit = NULL;
        wl_columnar_eval_stack_test_unary_prepared_checkpoint = NULL;
        wl_col_session_t *columnar = (wl_col_session_t *)session;
        CHECK(!events.count && !session_find_rel(columnar, "output")
            && !columnar->cleanup_active && !columnar->cleanup_pending
            && !columnar->cleanup_reserved_bytes);
        if (bound_receipt_probe) {
            CHECK(bound_receipt_short_requested && bound_receipt_refusals == 1
                && bound_receipt_acks == 1
                && wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)) ==
                reserved_before);
        }
        CHECK(finish_heads == (mode == 0 ? 0u : 1u));
        for (unsigned i = 0; i < 3; ++i)
            CHECK(wl_columnar_relation_snapshot_equal(before[i],
                wl_columnar_relation_snapshot(session_find_rel(columnar,
                names[i]))));
        wl_evaluation_control_report_t report;
        if (turnover) {
            bound_check_original(session);
            if (turnover >= 4)
                CHECK(!wl_evaluation_control_finish(control,
                    session->operation_admission, 0, 0));
        } else CHECK(!wl_evaluation_control_report(control, &report)
                && report.result == cause && !report.cleanup_status
                && !wl_evaluation_control_active_attempt_id(control));
        if (!turnover || !(turnover % 2)) {
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control, 0));
            CHECK(!drive(session, false, NULL));
            const col_rel_t *output = session_find_rel(columnar, "output");
            CHECK(output && output->nrows == 2 && output->ncols == 3
                && output->columns[0][0] == 10 && output->columns[1][0] == 100
                && output->columns[2][0] == 1000 && output->columns[0][1] == 20
                && output->columns[1][1] == 200 && output->columns[2][1] == 2000
                && events.count == 2);
        }
        if (bound_receipt_probe) {
            for (unsigned i = 0; i < bound_receipt_peer_count; ++i)
                CHECK(!wl_columnar_eval_stack_cleanup_receipt_release(columnar,
                    &peers[i]));
            for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES;
                ++i)
                CHECK(columnar->cleanup_receipts[i].state ==
                    WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE);
        }
        CHECK(!wl_session_try_destroy(&session));
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        /* The program's intern table can retain its governor reservation
         * beyond the session, so the final zero check follows this owner. */
        wirelog_program_free(program);
        if (governor) {
            uint64_t remaining = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor));
            if (remaining) fprintf(stderr,
                    "receipt pool governor mode=%u peers=%u remaining=%" PRIu64
                    "\n",
                    mode, bound_receipt_peer_count, remaining);
            CHECK(!remaining);
            wl_columnar_memory_governor_ref_release(governor);
        }
    }
}

static void
test_dynamic_lftj_then_join_failure(void)
{
    bound_turnover_mode = 0;
    const char *text = ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
        ".decl c(k:int64,z:int64)\n.decl d(z:int64,w:int64)\n"
        ".decl output(x:int64,y:int64,w:int64)\n"
        "output(x,y,w) :- a(k,x), b(k,y), c(k,z), d(z,w).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1
        && plan->strata[0].relations[0].ops[0].op == WL_PLAN_OP_LFTJ
        && plan->strata[0].relations[0].ops[1].op == WL_PLAN_OP_JOIN);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_MAX - 1, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    const int64_t a[] = {1, 10, 2, 20}, b[] = {1, 100, 2, 200};
    const int64_t c[] = {1, 7, 2, 8}, d[] = {7, 1000, 8, 2000};
    CHECK(!wl_session_insert(session, "a", a, 2, 2));
    CHECK(!wl_session_insert(session, "b", b, 2, 2));
    CHECK(!wl_session_insert(session, "c", c, 2, 2));
    CHECK(!wl_session_insert(session, "d", d, 2, 2));
    capture_t events = {0};
    wl_session_set_delta_cb(session, on_delta, &events);
    bound_failure_mode = 0;
    bound_failure_injected = bound_saw_lftj = false;
    bound_expect_lftj = true;
    wl_columnar_session_test_framed_grant_limit = bound_failure_limit;
    int result = drive(session, false, NULL);
    if (result != WL_EVALUATION_CONTROL_WORK_LIMIT) fprintf(stderr,
            "mixed LFTJ/JOIN cleanup result%d injected%d completed-LFTJ%d\n",
            result, bound_failure_injected, bound_saw_lftj);
    CHECK(result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && bound_failure_injected && bound_saw_lftj);
    wl_columnar_session_test_framed_grant_limit = NULL;
    bound_expect_lftj = false;
    wl_col_session_t *columnar = (wl_col_session_t *)session;
    CHECK(!events.count && !session_find_rel(columnar, "output")
        && !columnar->cleanup_active && !columnar->cleanup_pending
        && !columnar->cleanup_reserved_bytes);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 0));
    CHECK(!drive(session, false, NULL));
    const col_rel_t *output = session_find_rel(columnar, "output");
    CHECK(output && output->nrows == 2 && output->ncols == 3
        && output->columns[0][0] == 10 && output->columns[1][0] == 100
        && output->columns[2][0] == 1000 && output->columns[0][1] == 20
        && output->columns[1][1] == 200 && output->columns[2][1] == 2000
        && events.count == 2);
    CHECK(!wl_session_try_destroy(&session));
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

int
main(void)
{
    for (unsigned snapshot = 0; snapshot < 2; ++snapshot)
        for (unsigned observed = 0; observed < 2; ++observed)
            for (unsigned empty = 0; empty < 2; ++empty)
                test_generated(snapshot, observed, empty);
    test_production_abort_consume();
    test_dynamic_bound_failure(0);
    test_dynamic_bound_failure(1);
    test_dynamic_bound_failure(2);
    test_dynamic_bound_failure(3);
    test_dynamic_bound_failure(4);
    test_dynamic_bound_failure(5);
    bound_receipt_probe = true;
    const unsigned positions[] = {0, 15, 31};
    for (unsigned i = 0; i < sizeof(positions) / sizeof(positions[0]); ++i) {
        bound_receipt_peer_count = positions[i];
        test_dynamic_bound_failure(4);
    }
    bound_receipt_probe = false;
    bound_earlier_head = true;
    test_dynamic_bound_failure(4);
    bound_earlier_head = false;
    test_dynamic_lftj_then_join_failure();
    return 0;
}
