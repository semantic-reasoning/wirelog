/* Reuse the real JOIN lifecycle fixture and its signed-result verifier. This
* translation unit adds no synthetic backend, recipe, phase or Core owner. */
#define main wl_session_retained_bridge_fixture_main
#include "test_evaluation_join_host_lifecycle.c"
#undef main
#ifndef _WIN32
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

static wl_backend_financial_offer_t retained_copy;
static unsigned retained_takes;

static const wl_backend_financial_offer_t *
retained_claim_copy(const wl_backend_financial_offer_t *offer)
{
    retained_copy = *offer;
    retained_copy.identity = &retained_copy;
    return &retained_copy;
}

static void
retained_taken(const wl_col_session_t *session, unsigned role, unsigned mask)
{
    (void)session; (void)role; (void)mask;
    ++retained_takes;
}

static void *
retained_foreign(void *payload)
{
    wl_session_t *session = payload;
    CHECK(wl_session_step(session) == EBUSY);
    wl_session_t *same = session;
    CHECK(wl_session_try_destroy(&same) == EBUSY && same == session);
    return NULL;
}

static void
retained_pin_guards(wl_session_t *session)
{
    CHECK(wl_session_source_retained_pending(session));
    CHECK(wl_session_insert(session, "a", (int64_t[]){1, 0}, 1, 2) == EBUSY);
    CHECK(wl_session_remove(session, "a", (int64_t[]){1, 0}, 1, 2) == EBUSY);
    capture_t rows = {0};
    CHECK(wl_session_snapshot(session, snapshot, &rows) == EBUSY);
    wl_session_exclusive_attempt_t attempt = {0};
    CHECK(wl_session_exclusive_attempt_acquire(session, &attempt) == EBUSY);
    wl_evaluation_control_report_t report;
    CHECK(wl_session_test_source_pending_reset_cancel(session,
        &report) == EBUSY);
    wl_session_t alias = *session;
    CHECK(wl_session_step(&alias) == EINVAL);
}

static void
retained_void_guard(wl_session_t *session)
{
#ifndef _WIN32
    pid_t child = fork();
    CHECK(child >= 0);
    if (!child) {
        wl_session_destroy(session); _exit(0);
    }
    int status;
    CHECK(waitpid(child, &status, 0) == child && WIFSIGNALED(status)
        && WTERMSIG(status) == SIGABRT);
#else
    (void)session;
#endif
}

static void
retained_view(wl_session_t *session, unsigned *kind,
    wl_evaluation_control_source_financial_view_t *view)
{
    CHECK(!wl_session_test_source_retained_view(session, kind, view));
}

/* Observe every genuine packet; Q only releases the pin. The callback state
 * and canonical receipt remain exactly as the real executor left them. */
static void
retained_drain(wl_session_t *session, bool checked_close,
    const wl_session_source_metadata_recipe_t *recipe, bool ready)
{
    unsigned kind;
    wl_evaluation_control_source_financial_view_t view;
    retained_view(session, &kind, &view);
    CHECK(kind);
    wl_evaluation_control_source_financial_state_t previous = view.state;
    for (unsigned call = 0; kind; ++call) {
        CHECK(call < 12);
        retained_pin_guards(session);
        wl_session_t *same = session;
        CHECK((checked_close ? wl_session_try_destroy(&same) :
            wl_session_step(session)) == EAGAIN && same == session);
        retained_view(session, &kind, &view);
        if (previous == WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY)
            CHECK(view.state == WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN);
        else if (previous == WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING)
            CHECK(view.state == WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED);
        else if (previous == WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED)
            CHECK(view.state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT &&
                !kind);
        else CHECK(view.state == previous + 1);
        previous = view.state;
        if (recipe) CHECK(recipe->result->ready == ready
                && !recipe->result->scheduled && !*recipe->busy &&
                !retained_takes);
    }
    CHECK(!wl_session_source_retained_pending(session));
}

static const wl_backend_financial_offer_t *
retained_maint_offer(fixture_t *f)
{
    wl_session_t *session = f->sessions[0];
    wl_columnar_eval_stack_test_fail_allocation = 0;
    const wl_backend_financial_offer_t *offer = NULL;
    wl_backend_pending_bind_t pending;
    for (unsigned call = 0; !offer; ++call) {
        CHECK(call < 100000 && wl_session_step(session) == EAGAIN);
        offer = session->backend->session_controlled_financial_offer(session,
                &pending);
    }
    wl_columnar_eval_stack_test_fail_allocation = -1;
    CHECK(offer->recipe->role == WL_SESSION_SOURCE_METADATA_MAINTENANCE
        && *offer->recipe->phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH);
    return offer;
}

static void
retained_role(unsigned stop)
{
    fprintf(stderr, "retained role stop=%u\n", stop);
    fixture_t f; init(&f, true);
    wl_session_t *session = f.sessions[0];
    wl_columnar_memory_governor_ref_t *governor =
        ((wl_col_session_t *)session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    if (stop >= 5) {
        native_fixture = &f; native_pending = native_ended = false;
        native_successor = stop == 6; native_successor_id = 0;
        wl_columnar_session_test_framed_grant_finished =
            native_bootstrap_finished;
    }
    const wl_backend_financial_offer_t *offer = retained_maint_offer(&f);
    wl_columnar_session_test_framed_grant_finished = NULL;
    if (stop >= 5) CHECK(native_ended);
    const wl_session_source_metadata_recipe_t *recipe = offer->recipe;
    wl_session_source_metadata_preparation_t prepared;
    prepared.financial_record = (void *)offer;
    CHECK(!recipe->resolve(recipe, WL_SESSION_SOURCE_METADATA_OFFER_PROVE,
        &prepared) && !*recipe->busy && !recipe->result->ready);
    retained_copy = *offer; retained_copy.identity = &retained_copy;
    prepared.financial_record = &retained_copy;
    CHECK(recipe->resolve(recipe, WL_SESSION_SOURCE_METADATA_OFFER_PROVE,
        &prepared) == EINVAL && !*recipe->busy && !recipe->result->ready);
    unsigned before = *recipe->phase;
    retained_takes = 0;
    wl_columnar_session_test_metadata_taken = retained_taken;
    if (stop == 4) wl_evaluation_control_test_source_observation_cancel_at = 1;
    int armed = wl_session_test_source_retained_arm(session);
    CHECK(armed == (stop == 4 ? WL_EVALUATION_CONTROL_CANCELLED : 0));
    wl_thread_t peer;
    CHECK(!wl_thread_create(&peer, retained_foreign, session));
    CHECK(!wl_thread_join(&peer));
    retained_pin_guards(session);
    if (stop == 0) wl_evaluation_control_test_source_metadata_cancel_at = 1;
    if (stop ==
        1) wl_evaluation_control_test_source_lexical_cancel_before_action =
            true;
    if (stop ==
        2) wl_evaluation_control_test_source_lexical_cancel_after_action = true;
    if (stop == 3) wl_session_test_retained_claim_offer = retained_claim_copy;
    if (stop !=
        4) CHECK(wl_session_step(session) == (stop == 3 ? EINVAL : EAGAIN));
    wl_session_test_retained_claim_offer = NULL;
    bool receipt = stop == 1 || stop == 2 || stop >= 5;
    CHECK(recipe->result->ready == receipt && !*recipe->busy);
    CHECK(*recipe->phase == ((stop == 2 ||
        stop >= 5) ? WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT : before));
    if (receipt) CHECK(recipe->result->financial_result ==
            (stop >= 5 ? 0 : WL_EVALUATION_CONTROL_CANCELLED)
            && recipe->result->outcome.action_called == (stop == 2 ||
            stop >= 5));
    retained_void_guard(session);
    retained_drain(session, (stop & 1) != 0, recipe, receipt);
    CHECK(*recipe->phase == ((stop == 2 ||
        stop >= 5) ? WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT : before));
    wl_evaluation_control_report_t report;
    CHECK(!wl_session_test_source_pending_report(session, &report));
    CHECK(report.result == ((stop <= 2 || stop == 4) ?
        WL_EVALUATION_CONTROL_CANCELLED : 0));
    if (report.result == WL_EVALUATION_CONTROL_CANCELLED)
        CHECK(!wl_session_test_source_pending_reset_cancel(session, &report));
    if (receipt) {
        CHECK(wl_session_step(session) == EAGAIN && retained_takes == 1
            && !recipe->result->ready);
    } else CHECK(!retained_takes);
    wl_columnar_session_test_metadata_taken = NULL;
    CHECK(finish(session) == ENOMEM);
    if (stop >= 5) {
        wl_evaluation_control_report_t actual;
        CHECK(!wl_evaluation_control_report(f.control, &actual));
        native_report_same(&native_report, &actual);
        if (stop == 6) {
            uint64_t used = UINT64_MAX;
            CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                native_successor_id, &used) && !used);
            wl_evaluation_control_attempt_finish_result_t end =
                wl_evaluation_control_finish_attempt(f.control,
                    session->operation_admission, native_successor_id, 0, 0);
            CHECK(!end.protocol && end.report_valid && !end.report.result
                && !end.report.consumed);
        }
    }
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!finish(session) && !finish(f.sessions[1]));
    verify(session, &f.state); verify(f.sessions[1], &f.state);
    state_t old = f.state; f.state.a[1] = false;
    for (unsigned i = 0; i != 2; ++i) {
        memset(&f.events[i], 0, sizeof(f.events[i]));
        CHECK(!wl_session_remove(f.sessions[i], "a", (int64_t[]){1, 0}, 1, 2)
            && !finish(f.sessions[i]));
        verify(f.sessions[i], &f.state);
        expect_events(&f.events[i], &old, &f.state);
    }
    destroy(&f);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

static void
retained_bind(unsigned cursor, unsigned stop)
{
    fprintf(stderr, "retained bind cursor=%u stop=%u\n", cursor, stop);
    fixture_t f; init(&f, true);
    wl_session_t *session = f.sessions[0];
    CHECK(wl_session_step(session) == EAGAIN);
    for (unsigned i = 0; i != cursor;
        ++i) CHECK(wl_session_step(session) == EAGAIN);
    wl_backend_pending_bind_t pending;
    CHECK(!session->backend->session_controlled_financial_offer(session,
        &pending)
        && pending.root && !pending.refusal);
    uint64_t before[10], after[10];
    CHECK(!wl_columnar_session_test_metadata_bind_view(
            (wl_col_session_t *)session, before)
        && before[0] == 1 && before[1] == cursor);
    CHECK(!wl_session_test_source_retained_arm(session));
    wl_thread_t peer;
    CHECK(!wl_thread_create(&peer, retained_foreign, session));
    CHECK(!wl_thread_join(&peer));
    if (stop) wl_evaluation_control_test_source_metadata_cancel_at = stop;
    CHECK(wl_session_step(session) == EAGAIN);
    CHECK(!wl_columnar_session_test_metadata_bind_view(
            (wl_col_session_t *)session, after));
    if (!stop &&
        cursor == 4) CHECK(after[0] == 2 && !session->source_financial_head);
    else CHECK(after[0] == 1 && after[1] == cursor + (!stop || stop == 4));
    retained_drain(session, cursor == 4, NULL, false);
    wl_evaluation_control_report_t report;
    CHECK(!wl_session_test_source_pending_report(session, &report));
    CHECK(report.result == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0));
    if (stop) CHECK(!wl_session_test_source_pending_reset_cancel(session,
            &report));
    CHECK(!finish(session) && !finish(f.sessions[1]));
    verify(session, &f.state); verify(f.sessions[1], &f.state);
    destroy(&f);
}

int
main(void)
{
    for (unsigned stop = 0; stop != 7; ++stop) retained_role(stop);
    for (unsigned cursor = 0; cursor != 5; ++cursor) {
        retained_bind(cursor, 0);
        for (unsigned stop = 1; stop <= 4; ++stop)
            if (cursor != 4 || stop != 4) retained_bind(cursor, stop);
    }
    puts(
        "Session retained bridge: canonical role/BIND packet histories passed");
    return 0;
}
