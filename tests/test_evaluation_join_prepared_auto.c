/* Production session selection, without manual prepared API calls. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
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

int
main(void)
{
    for (unsigned snapshot = 0; snapshot < 2; ++snapshot)
        for (unsigned observed = 0; observed < 2; ++observed)
            for (unsigned empty = 0; empty < 2; ++empty)
                test_generated(snapshot, observed, empty);
    test_production_abort_consume();
    return 0;
}
