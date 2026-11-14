/* An externally ended Original is historical output, never permission to
 * adopt its successor or to publish the dirty input as evaluated. */
#include "wirelog/columnar/internal.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #e); abort(); \
                      } } while (0)

extern void (*wl_columnar_session_test_step_reset_phase)(wl_col_session_t *,
    unsigned, size_t, bool);
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);

static unsigned target_phase, events;
static bool fired, open_successor;
static bool failed_retry_case, source_end_cancel;
static int ended_result;
static int32_t last_sign;
static uint64_t ended_id, successor_id;
static wl_evaluation_control_report_t ended_report;

static void
capture(const char *name, const int64_t *row, uint32_t width, int32_t sign,
    void *payload)
{
    (void)name; (void)row; (void)payload;
    CHECK(width == 2 && (sign == 1 || (failed_retry_case && sign == -1)));
    last_sign = sign;
    ++events;
}

static void
turnover(wl_col_session_t *col, unsigned phase, size_t cursor, bool after)
{
    (void)cursor;
    if (fired || !after || phase != target_phase) return;
    fired = true;
    wl_evaluation_control_t *control = col->base.evaluation_control;
    ended_id = wl_evaluation_control_active_attempt_id(control);
    CHECK(ended_id && col->controlled_step_reset.operation_id == ended_id);
    CHECK(wl_evaluation_control_finish(control, col->base.operation_admission,
        ended_result, 0) == ended_result);
    CHECK(!wl_evaluation_control_report(control, &ended_report)
        && ended_report.attempt_id == ended_id
        && ended_report.result == ended_result);
    if (open_successor) {
        CHECK(!wl_evaluation_control_begin(control,
            col->base.operation_admission));
        successor_id = wl_evaluation_control_active_attempt_id(control);
        CHECK(successor_id && successor_id != ended_id);
        CHECK(!wl_evaluation_control_charge_attempt(control, successor_id, 0));
    }
    if (source_end_cancel)
        wl_evaluation_control_test_source_observation_cancel_at = 5;
}

static unsigned cutoff_stops;
static void
stop_cutoff(wl_evaluation_control_t *control, unsigned phase, uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)index; (void)bindings;
    if (phase != 14) return;
    ++cutoff_stops;
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    CHECK(wl_evaluation_control_charge(control, UINT64_MAX) == EOVERFLOW);
    wl_evaluation_control_request_cancel(control);
}

static int
finish(wl_session_t *session)
{
    for (unsigned i = 0; i != 100000; ++i) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false);
    return EIO;
}

static void
same_report(wl_evaluation_control_t *control)
{
    wl_evaluation_control_report_t actual;
    CHECK(!wl_evaluation_control_report(control, &actual));
    CHECK(actual.attempt_id == ended_report.attempt_id
        && actual.limit == ended_report.limit
        && actual.consumed == ended_report.consumed
        && actual.stop_reason == ended_report.stop_reason
        && actual.execution_status == ended_report.execution_status
        && actual.cleanup_status == ended_report.cleanup_status
        && actual.result == ended_report.result);
}

static void
literal_content(wl_session_t *session, bool old_image)
{
    const col_rel_t *output = session_find_rel(COL_SESSION(session), "output");
    CHECK(output && output->ncols == 2);
    bool first = false, added = false;
    for (uint32_t row = 0; row != output->nrows; ++row) {
        int64_t x = output->columns[0][row], y = output->columns[1][row];
        if (x == 1 && y == 2) {
            CHECK(!first); first = true;
        }else {
            CHECK(x == 3 && y == 4 && !added); added = true;
        }
    }
    CHECK(first == (old_image || !failed_retry_case)
        && added == (!old_image || failed_retry_case));
}

static void
run_case(bool close_only)
{
    wirelog_error_t error;
    const char *text = failed_retry_case ?
        ".decl a(x:int64,y:int64)\n.decl output(x:int64,y:int64)\n"
        "output(x,y):-a(x,y),x>0.\n" :
        ".decl a(x:int64,y:int64)\n.decl output(x:int64,y:int64)\n"
        "output(x,y):-a(x,y).\n";
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(failed_retry_case ? 0 :
        UINT64_C(100000000), &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(128) << 20,
            .usable_bytes = UINT64_C(128) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor); options.memory_governor = governor;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) && session->source_c1_admission);
    wl_session_set_delta_cb(session, capture, NULL);
    const int64_t first[] = {1, 2}, added[] = {3, 4};
    CHECK(!wl_session_insert(session, "a", first, 1, 2) && !finish(session));
    events = 0; fired = false; successor_id = 0;
    CHECK(!wl_session_insert(session, "a", added, 1, 2));
    if (failed_retry_case) {
        CHECK(!finish(session)); events = 0;
        CHECK(!wl_session_remove(session, "a", first, 1, 2));
        cutoff_stops = 0;
        wl_columnar_session_test_observed_transaction_checkpoint = stop_cutoff;
        CHECK(finish(session) == EOVERFLOW
            && cutoff_stops == 1 && !events
            && !wl_columnar_session_test_observed_transaction_checkpoint);
        uint64_t disposition[6];
        CHECK(!wl_columnar_session_test_producer_disposition_view(COL_SESSION(
                session), disposition));
        if (!disposition[4] || !disposition[5]) fprintf(stderr,
                "failed retry fixture: registered=%llu kind=%llu tx=%llu phase=%llu paid=%llu ready=%llu active=%llu state=%u\n",
                (unsigned long long)disposition[0],
                (unsigned long long)disposition[1],
                (unsigned long long)disposition[2],
                (unsigned long long)disposition[3],
                (unsigned long long)disposition[4],
                (unsigned long long)disposition[5],
                (unsigned long long)wl_evaluation_control_active_attempt_id(
                    control),
                session->backend->session_controlled_step_state(session));
        CHECK(disposition[4] == 1 && disposition[5] == 1
            && !disposition[0] && !disposition[2] && !disposition[3]
            && !wl_evaluation_control_active_attempt_id(control)
            && session->backend->session_controlled_step_state(session) ==
            WL_CONTROLLED_STEP_RETRYABLE);
        CHECK(!wl_evaluation_control_reset(control));
    }
    wl_columnar_session_test_step_reset_phase = turnover;
    int rc = EIO;
    bool settled = false;
    for (unsigned i = 0; i != 10000; ++i) {
        rc = wl_session_step(session);
        if (fired && COL_SESSION(session)->controlled_step_reset.phase ==
            WL_COLUMNAR_SESSION_STEP_RESET_NONE) {
            settled = true; break;
        }
        CHECK(rc == EAGAIN);
    }
    wl_columnar_session_test_step_reset_phase = NULL;
    if (!fired || !settled || rc != ended_result)
        fprintf(stderr,
            "phase=%u result=%d successor=%u close=%u fired=%u settled=%u rc=%d\n",
            target_phase, ended_result, open_successor, close_only, fired,
            settled, rc);
    CHECK(fired && settled && rc == ended_result);
    CHECK(!events && COL_SESSION(session)->pending_input_change
        && session_find_rel(COL_SESSION(session), "output")->nrows ==
        (failed_retry_case ? 2u : 1u));
    CHECK(session->backend->session_controlled_step_state(session) ==
        (failed_retry_case ? WL_CONTROLLED_STEP_IDLE :
        WL_CONTROLLED_STEP_DIRTY));
    literal_content(session, true);
    same_report(control);
    if (failed_retry_case) {
        uint64_t disposition[6];
        CHECK(!wl_columnar_session_test_producer_disposition_view(COL_SESSION(
                session),
            disposition) && !disposition[4] && !disposition[5]);
    }
    if (source_end_cancel) {
        wl_evaluation_control_report_t source;
        CHECK(!wl_session_test_source_unstarted_reset_cancel(session, &source)
            && source.result == WL_EVALUATION_CONTROL_CANCELLED &&
            source.consumed == 10);
        same_report(control);
    }
    if (open_successor) {
        uint64_t consumed = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(control, successor_id,
            &consumed) && !consumed);
        wl_session_t *saved = session;
        CHECK(wl_session_try_destroy(&session) == EBUSY && session == saved);
        same_report(control);
        if (!close_only) {
            /* A foreign live owner cannot become this descriptor's Native
             * generation across repeated public admission. */
            for (unsigned attempt = 0; attempt != 3; ++attempt) {
                int refused = wl_session_step(session);
                CHECK(refused != EINVAL && refused != 0 && !events);
                CHECK(!wl_evaluation_control_test_active_consumed(control,
                    successor_id, &consumed) && !consumed);
                same_report(control);
            }
        }
        CHECK(!wl_evaluation_control_finish(control,
            session->operation_admission,
            0, 0));
    }
    if (!close_only) {
        if (failed_retry_case) {
            /* A genuine restart may end again before its first receiver.
             * The consumed maintenance192 receipt must stay consumed. */
            fired = false;
            wl_columnar_session_test_step_reset_phase = turnover;
            settled = false;
            for (unsigned i = 0; i != 10000; ++i) {
                rc = wl_session_step(session);
                if (fired &&
                    COL_SESSION(session)->controlled_step_reset.phase ==
                    WL_COLUMNAR_SESSION_STEP_RESET_NONE) {
                    settled = true; break;
                }
                CHECK(rc == EAGAIN);
            }
            wl_columnar_session_test_step_reset_phase = NULL;
            CHECK(fired && settled && rc == ended_result && !events
                && session->backend->session_controlled_step_state(session) ==
                WL_CONTROLLED_STEP_IDLE);
            literal_content(session, true); same_report(control);
            uint64_t disposition[6];
            CHECK(!wl_columnar_session_test_producer_disposition_view(
                    COL_SESSION(session),
                    disposition) && !disposition[4] && !disposition[5]);
            if (open_successor) {
                uint64_t consumed = UINT64_MAX;
                CHECK(!wl_evaluation_control_test_active_consumed(control,
                    successor_id,
                    &consumed) && !consumed);
                CHECK(!wl_evaluation_control_finish(control,
                    session->operation_admission, 0, 0));
            }
        }
        int resumed = finish(session);
        if (resumed || events != 1)
            fprintf(stderr,
                "future rc=%d events=%u reset=%u general=%u active=%u\n",
                resumed, events,
                COL_SESSION(session)->controlled_step_reset.phase,
                COL_SESSION(session)->controlled_step_reset.general,
                COL_SESSION(session)->controlled_step_reset.operation_active);
        CHECK(!resumed && events == 1 &&
            last_sign == (failed_retry_case ? -1 : 1)
            && session_find_rel(COL_SESSION(session), "output")->nrows ==
            (failed_retry_case ? 1u : 2u));
        literal_content(session, false);
        CHECK(!finish(session) && events == 1);
    }
    CHECK(!wl_session_try_destroy(&session) && !session);
    wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
    /* The parsed program's shared intern storage retains its genuine credit
     * after Session destruction; end that owner before expecting zero. */
    if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)))
        fprintf(stderr,
            "governor leftover phase=%u result=%d successor=%u close=%u failed=%u reserved=%llu\n",
            target_phase, ended_result, open_successor, close_only,
            failed_retry_case,
            (unsigned long long)wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor)));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

int
main(void)
{
    const char *prior = getenv("WIRELOG_JOIN_BATCH_BYTES");
    char *saved = NULL;
    if (prior) {
        saved = malloc(strlen(prior) + 1); CHECK(saved);
        memcpy(saved, prior, strlen(prior) + 1);
    }
#if defined(_WIN32)
    CHECK(!_putenv_s("WIRELOG_JOIN_BATCH_BYTES", "256"));
#else
    CHECK(!setenv("WIRELOG_JOIN_BATCH_BYTES", "256", 1));
#endif
    const unsigned phases[] = {WL_COLUMNAR_SESSION_STEP_RESET_CLEAR,
                               WL_COLUMNAR_SESSION_STEP_RESET_RESTORE,
                               WL_COLUMNAR_SESSION_STEP_RESET_READY};
    const int results[] = {7, 0, EAGAIN, WL_EVALUATION_CONTROL_CANCELLED,
                           WL_EVALUATION_CONTROL_WORK_LIMIT};
    for (unsigned p = 0; p != sizeof(phases) / sizeof(phases[0]); ++p)
        for (unsigned r = 0; r != sizeof(results) / sizeof(results[0]); ++r)
            for (unsigned successor = 0; successor != 2; ++successor)
                for (unsigned close = 0; close != 2; ++close) {
                    target_phase = phases[p]; ended_result = results[r];
                    open_successor = successor != 0; run_case(close != 0);
                }
    for (unsigned kind = 0; kind != 2; ++kind)
        for (unsigned r = 0; r != sizeof(results) / sizeof(results[0]); ++r)
            for (unsigned successor = 0; successor != 2; ++successor)
                for (unsigned close = 0; close != 2; ++close) {
                    failed_retry_case = kind != 0;
                    source_end_cancel = !failed_retry_case;
                    target_phase =
                        failed_retry_case ?
                        WL_COLUMNAR_SESSION_STEP_RESET_HANDOFF
                        : WL_COLUMNAR_SESSION_STEP_RESET_READY;
                    ended_result = results[r]; open_successor = successor != 0;
                    run_case(close != 0);
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
