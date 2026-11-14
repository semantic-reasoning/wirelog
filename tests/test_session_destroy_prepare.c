/* Actual CLOSE financial refusals precede slot loss and the Original seal. */
#include "wirelog/session.h"
#include "wirelog/thread.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #e); abort(); } } while (0)

typedef struct {
    wl_session_t base;
    bool prepared;
} fixture_t;

static unsigned actions, commits, before_seals, after_seals;
static bool stop_prepare, stop_retire, begin_before_seal;
static int expected_result = 7;
static uint64_t raced_id;

static int
create(const wl_plan_t *plan, uint32_t workers, wl_session_t **out)
{
    (void)plan; (void)workers;
    fixture_t *f = calloc(1, sizeof(*f));
    if (!f) return ENOMEM;
    *out = &f->base;
    return 0;
}

static int
create_options(const wl_plan_t *plan, uint32_t workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    (void)options;
    return create(plan, workers, out);
}

static int
prepare_action(void *payload)
{
    fixture_t *f = payload;
    CHECK(wl_session_source_checked_close_scope_owned(&f->base));
    wl_session_t copy = f->base;
    CHECK(!wl_session_source_checked_close_scope_owned(&copy));
    CHECK(!f->prepared);
    f->prepared = true;
    ++actions;
    return 0;
}

static void *
foreign_scope(void *payload)
{
    CHECK(!wl_session_source_checked_close_scope_owned(payload));
    return NULL;
}

static int
prepare(wl_session_t *session)
{
    fixture_t *f = (void *)session;
    bool checked = wl_session_source_checked_close_scope_owned(session);
    CHECK(checked == wl_session_source_close_scope_owned(session)
        && !wl_session_source_operation_scope_owned(session));
    /* A seal race can leave an already prepared backend after its Source
     * bank was actually destroyed. That retry has no financial CLOSE scope. */
    CHECK(checked || f->prepared);
    wl_thread_t peer;
    CHECK(!wl_thread_create(&peer, foreign_scope, session));
    CHECK(!wl_thread_join(&peer));
    if (f->prepared) return 0;
    if (stop_prepare) {
        stop_prepare = false;
        wl_evaluation_control_test_source_lexical_cancel_before_action = true;
    }
    wl_evaluation_control_source_phase_result_t phase =
        wl_session_source_snapshot_metadata(session, prepare_action, f);
    if (phase.financial_result) return phase.financial_result;
    if (!phase.action_called) return EINVAL;
    if (phase.action_result) return phase.action_result;
    if (stop_retire) {
        stop_retire = false;
        wl_evaluation_control_test_source_bank_destroy_cancel = true;
    }
    return 0;
}

static void
destroy(wl_session_t *session)
{
    fixture_t *f = (void *)session;
    wl_evaluation_control_report_t report;
    CHECK(f->prepared && !wl_evaluation_control_active_attempt_id(
            session->evaluation_control));
    /* The attachment still pins Original even with no external reference. */
    CHECK(!wl_evaluation_control_report(session->evaluation_control, &report)
        && report.result == expected_result);
    ++commits;
    free(f);
}

static int
step(wl_session_t *session)
{
    (void)session;
    CHECK(false); return EINVAL;
}

static wl_controlled_step_state_t
state(const wl_session_t *session)
{
    (void)session;
    return WL_CONTROLLED_STEP_IDLE;
}

static const wl_compute_backend_t backend = {
    .name = "checked-close-source-owner",
    .session_create = create,
    .session_create_with_options = create_options,
    .session_destroy_prepare = prepare,
    .session_destroy = destroy,
    .session_step = step,
    .session_controlled_step_state = state,
};

static wl_session_t *
new_fixture(wl_evaluation_control_t **control)
{
    CHECK(!wl_evaluation_control_create(64, control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = *control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(&backend, NULL, 1, &options,
        &session));
    CHECK(!wl_evaluation_control_begin(*control, session->operation_admission)
        && !wl_evaluation_control_charge(*control, 7));
    CHECK(wl_evaluation_control_finish(*control, session->operation_admission,
        7, 0) == 7);
    expected_result = 7;
    CHECK(!wl_session_source_checked_close_scope_owned(session));
    return session;
}

static void
before_seal(wl_session_t *session)
{
    CHECK(!wl_session_source_checked_close_scope_owned(session));
    ++before_seals;
    if (begin_before_seal) {
        begin_before_seal = false;
        CHECK(!wl_evaluation_control_begin(session->evaluation_control,
            session->operation_admission));
        raced_id = wl_evaluation_control_active_attempt_id(
            session->evaluation_control);
        CHECK(raced_id);
    }
}

static void
after_seal(wl_session_t *session)
{
    CHECK(!wl_session_source_checked_close_scope_owned(session));
    ++after_seals;
    CHECK(wl_evaluation_control_begin(session->evaluation_control,
        session->operation_admission) == EINVAL);
    CHECK(!wl_evaluation_control_active_attempt_id(
            session->evaluation_control));
}

static void
financial_refusal(bool retirement)
{
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = new_fixture(&control), *saved = session;
    unsigned old_actions = actions, old_commits = commits;
    unsigned old_seals = before_seals;
    stop_prepare = !retirement; stop_retire = retirement;
    CHECK(wl_session_try_destroy(&session) == EAGAIN && session == saved);
    CHECK(!wl_session_source_checked_close_scope_owned(session));
    CHECK(actions == old_actions + (retirement ? 1u : 0u)
        && commits == old_commits && before_seals == old_seals);
    for (unsigned retry = 0; retry < 3; ++retry) {
        CHECK(wl_session_step(session) == EBUSY);
        const int64_t value = 0;
        CHECK(wl_session_insert(session, "r", &value, 0, 1) == EBUSY);
        wl_session_exclusive_attempt_t exclusive = {0};
        CHECK(wl_session_exclusive_attempt_acquire(session,
            &exclusive) == EBUSY);
        CHECK(wl_session_try_destroy(&session) == EAGAIN && session == saved);
        CHECK(actions == old_actions + (retirement ? 1u : 0u)
            && commits == old_commits && before_seals == old_seals);
    }
    wl_evaluation_control_report_t source, original;
    CHECK(!wl_session_test_source_parked_close_reset_cancel(session, &source)
        && source.result == WL_EVALUATION_CONTROL_CANCELLED &&
        !source.consumed);
    CHECK(!wl_evaluation_control_report(control, &original)
        && original.result == 7 && original.consumed == 7);
    CHECK(!wl_session_try_destroy(&session) && !session);
    CHECK(actions == old_actions + 1 && commits == old_commits + 1);
    wl_evaluation_control_release(control);
}

int
main(void)
{
    wl_session_test_before_destroy_seal = before_seal;
    wl_session_test_after_destroy_seal = after_seal;
    financial_refusal(false);
    financial_refusal(true);

    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = new_fixture(&control), *saved = session;
    unsigned old_actions = actions, old_commits = commits;
    begin_before_seal = true;
    CHECK(wl_session_try_destroy(&session) == EBUSY && session == saved);
    CHECK(actions == old_actions + 1 && commits == old_commits);
    uint64_t used = UINT64_MAX;
    CHECK(!wl_evaluation_control_test_active_consumed(control, raced_id, &used)
        && !used);
    CHECK(!wl_evaluation_control_finish(control, session->operation_admission,
        0, 0));
    expected_result = 0;
    CHECK(!wl_session_try_destroy(&session) && !session);
    CHECK(actions == old_actions + 1 && commits == old_commits + 1);
    wl_evaluation_control_release(control);

    session = new_fixture(&control);
    wl_evaluation_control_release(control); control = NULL;
    CHECK(!wl_session_try_destroy(&session) && !session);
    CHECK(before_seals == 5 && after_seals == 4 && commits == 4);
    wl_session_test_before_destroy_seal = NULL;
    wl_session_test_after_destroy_seal = NULL;
    puts(
        "checked CLOSE: prepare/retirement refusal, both seal orders and last ref passed");
    return 0;
}
