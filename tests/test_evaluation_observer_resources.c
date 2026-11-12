/* Authentic observed-parent cleanup, without fabricated Step/observer owners. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static unsigned stage, scenario, stop_kind, target_phase;
static bool injected, turned, footer_refused, short_refused, foreign_checked,
    alias_checked;
static unsigned grant_calls, kernel_actions, callbacks;
static unsigned finished_calls, close_grant, close_stage;
static bool child_close_injected, bank_close_injected;
static wl_evaluation_control_report_t child_close_report;
static uint64_t successor, event_mask;
static int expected;
static wl_session_t *active_session;
static wl_evaluation_control_report_t old_report;
static wl_columnar_source_access_reader_t reader_peer;
static wl_columnar_memory_reservation_t credit_peer;
static wl_columnar_memory_reservation_t *credit_target;
static uint64_t reader_observed, credit_observed;
static bool reader_peer_hit, reader_peer_held, credit_peer_hit,
    credit_peer_held,
    cas_footer_refused;
extern void (*wl_columnar_relation_test_before_reader_cas)(
    wl_evaluation_control_t *, wl_columnar_source_access_gate_t *, uint64_t);
extern void (*wl_columnar_memory_governor_test_before_finish_credit_cas)(
    wl_columnar_memory_reservation_t *, uint64_t);

static void
reader_cas_peer(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, uint64_t observed)
{
    if ((scenario != 6 && scenario != 8) || !injected || reader_peer_hit
        || c == active_session->evaluation_control) return;
    uint64_t v[12];
    CHECK(!wl_columnar_session_test_observer_resource_view(
            (wl_col_session_t *)active_session, v));
    if (!v[0] || !v[1] || v[5]) return;
    CHECK(observed && observed < WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1);
    CHECK(!wl_columnar_source_access_reader_acquire(gate, &reader_peer));
    reader_observed = observed;
    reader_peer_hit = reader_peer_held = true;
    if (scenario == 8) wl_evaluation_control_request_cancel(c);
}

static void
credit_cas_peer(wl_columnar_memory_reservation_t *token, uint64_t observed)
{
    if ((scenario != 7 && scenario != 9) || !injected ||
        credit_peer_hit) return;
    uint64_t v[12];
    CHECK(!wl_columnar_session_test_observer_resource_view(
            (wl_col_session_t *)active_session, v));
    if (!v[0] || v[3] || v[4] < WL_COLUMNAR_EVAL_DELTA_RESOURCE_STAGING
        || v[4] > WL_COLUMNAR_EVAL_DELTA_RESOURCE_BUILD_METADATA) return;
    CHECK(token->governor && token->release_bytes &&
        observed >= token->release_bytes);
    CHECK(wl_columnar_memory_reserve_checked(token->governor, 1, &credit_peer)
        == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    CHECK(wl_columnar_memory_commit(&credit_peer, &credit_peer));
    credit_target = token; credit_observed = observed;
    credit_peer_hit = credit_peer_held = true;
}

static void
close_peers(void)
{
    if (reader_peer_held) {
        bool done = false;
        for (unsigned i = 0; !done && i < 4; ++i)
            CHECK(!wl_columnar_source_access_reader_release_step(&reader_peer,
                &done));
        CHECK(done); reader_peer_held = false;
    }
    if (credit_peer_held) {
        bool done = false;
        for (unsigned i = 0; !done && i < 3; ++i)
            CHECK(!wl_columnar_memory_reservation_finish_step(&credit_peer,
                false, &done));
        CHECK(done); credit_peer_held = false; credit_target = NULL;
        wl_columnar_memory_reservation_init(&credit_peer);
    }
}

static bool
same_report(wl_evaluation_control_report_t a, wl_evaluation_control_report_t b)
{
    return a.attempt_id == b.attempt_id && a.limit == b.limit
           && a.consumed == b.consumed && a.stop_reason == b.stop_reason
           && a.execution_status == b.execution_status
           && a.cleanup_status == b.cleanup_status && a.result == b.result;
}

static void
check_original(void)
{
    if (!turned) return;
    wl_evaluation_control_t *c = active_session->evaluation_control;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report) && same_report(report,
        old_report));
    if (successor) {
        uint64_t consumed = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(c, successor,
            &consumed)
            && !consumed);
    } else CHECK(!wl_evaluation_control_active_attempt_id(c));
}

static void
turn_original(void)
{
    CHECK(injected && !turned);
    wl_evaluation_control_t *c = active_session->evaluation_control;
    CHECK(wl_evaluation_control_finish(c,
        active_session->operation_admission, expected, 0) == expected);
    if (scenario >= 2) {
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c,
            active_session->operation_admission));
        successor = wl_evaluation_control_active_attempt_id(c);
        CHECK(successor);
    }
    CHECK(!wl_evaluation_control_report(c, &old_report));
    turned = true;
}

static void
advanced(wl_col_session_t *s)
{
    uint64_t v[16];
    CHECK(!wl_columnar_eval_delta_test_observer_resource_view(s, v));
    bool match = stage == 0 ? v[2] && !v[3]
        : stage == 1 ? v[2] && v[3] >= 32
        : stage == 2 ? v[5] != 0
        : stage == 3 ? v[12] && !v[13]
        : stage == 4 ? v[13] != 0
        : stage == 5 ? v[7] != 0
        : stage == 6 ? v[14] != 0
        : stage == 7 ? v[9] != 0
        : v[1] && !v[0] && !v[6];
    if (!match) return;
    CHECK(!injected);
    injected = true;
    wl_columnar_eval_delta_test_observer_advanced = NULL;
    wl_evaluation_control_t *c = s->base.evaluation_control;
    if (!stop_kind) wl_evaluation_control_request_cancel(c);
    else CHECK(wl_evaluation_control_charge(c, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

static uint64_t
limit(const wl_col_session_t *s, uint64_t capacity)
{
    CHECK(injected && s == (wl_col_session_t *)active_session &&
        capacity == 384);
    ++grant_calls;
    /* Registration128 + owned proof128 precede the refused leaf128. */
    if (scenario == 3 && !short_refused) return 383;
    return capacity;
}

static void
finished(const wl_col_session_t *s, unsigned action,
    wl_evaluation_control_report_t report, int protocol)
{
    (void)s; (void)action;
    ++finished_calls;
    CHECK(close_stage == 5 && finished_calls == grant_calls);
    CHECK(same_report(report, child_close_report));
    CHECK(report.limit == 384 || report.limit == 383);
    CHECK(report.consumed <= report.limit && !report.cleanup_status);
    if (report.limit == 383) {
        CHECK(protocol == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.consumed == 256 && !short_refused);
        short_refused = true;
    }
    check_original();
}

static int
cancel_bank_action(void *payload)
{
    (void)payload;
    return 0;
}

static void
finance_phase(wl_col_session_t *s, wl_evaluation_control_t *funding,
    uint64_t id, unsigned phase, bool after, int financial)
{
    CHECK(s == (wl_col_session_t *)active_session && phase >= 1 && phase <= 5);
    if (!after) {
        if (phase == 1) {
            close_grant = grant_calls; close_stage = 1;
        } else CHECK(close_grant == grant_calls && phase == close_stage);
        CHECK((phase <= 3) == (funding != NULL));
        CHECK((phase <= 3) == (id != 0));
        if ((scenario == 10 || scenario == 11) && phase == 1
            && !child_close_injected) {
            child_close_injected = true;
            if (scenario == 10) {
                wl_evaluation_control_request_cancel(funding);
                CHECK(wl_evaluation_control_charge_attempt(funding, id, 0)
                    == WL_EVALUATION_CONTROL_CANCELLED);
            } else CHECK(!wl_evaluation_control_finish(funding,
                    s->controlled_step, 0, 0));
        }
        if (scenario >= 12 && scenario <= 15 && !bank_close_injected
            && phase == scenario - 11) {
            bank_close_injected = true;
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
        }
        if (scenario >= 16 && scenario <= 19 && !bank_close_injected
            && phase == scenario - 15) {
            bank_close_injected = true;
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
            wl_evaluation_control_source_phase_result_t stopped =
                wl_session_source_snapshot_metadata(active_session,
                    cancel_bank_action, NULL);
            CHECK(stopped.action_called && !stopped.action_result
                && stopped.financial_result == WL_EVALUATION_CONTROL_CANCELLED);
        }
    } else {
        CHECK(phase == close_stage + 1 || (phase == 5 && close_stage == 5)
            || (scenario >= 16 && scenario <= 19 && financial
            && phase == close_stage));
        close_stage = phase;
        if (phase == 2) {
            CHECK(funding && id);
            CHECK(!wl_evaluation_control_report(funding, &child_close_report)
                && child_close_report.attempt_id == id);
        }
        if (financial) {
            CHECK(financial == WL_EVALUATION_CONTROL_CANCELLED
                && bank_close_injected
                && !
                wl_evaluation_control_test_source_lexical_cancel_after_action);
            /* Reset the real quiescent BANK test generation, never Original. */
            CHECK(!wl_session_test_source_final_reset_cancel(active_session));
        }
        if (phase >= 4) CHECK(!funding && !id);
    }
    check_original();
}

static void *
foreign_step(void *argument)
{
    wl_session_t *session = argument;
    CHECK(wl_session_step(session) == EBUSY);
    CHECK(session->backend->session_controlled_step_state(session)
        == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
    return NULL;
}

static void
prefooter(wl_evaluation_control_t *funding, uint64_t id, unsigned point)
{
    CHECK(id == wl_evaluation_control_active_attempt_id(funding));
    ++kernel_actions;
    wl_col_session_t *s = (wl_col_session_t *)active_session;
    uint64_t v[12];
    CHECK(!wl_columnar_session_test_observer_resource_view(s, v));
    CHECK(v[0] && !v[3]);
    if (!alias_checked) {
        wl_columnar_eval_delta_observer_resource_t wrong = {0};
        CHECK(wl_columnar_eval_delta_observer_resource_step_owned(s,
            s->controlled_step, &wrong, active_session->evaluation_control,
            wl_evaluation_control_active_attempt_id(
                active_session->evaluation_control)).protocol
            == EINVAL);
        static int owner;
        wl_evaluation_control_t *other = NULL;
        CHECK(!wl_evaluation_control_create(128, &other)
            && !wl_evaluation_control_attach(other, &owner)
            && !wl_evaluation_control_begin(other, &owner));
        uint64_t other_id = wl_evaluation_control_active_attempt_id(other),
            consumed = 1;
        CHECK(wl_columnar_eval_delta_observer_resource_begin_owned(s,
            s->controlled_step, &wrong, other, other_id).protocol == EINVAL);
        CHECK(wl_columnar_eval_delta_observer_resource_step_owned(s,
            s->controlled_step, &wrong, other, other_id).protocol == EINVAL);
        CHECK(wl_columnar_eval_delta_observer_resource_ack_owned(s,
            s->controlled_step, &wrong, other, other_id).protocol == EINVAL);
        CHECK(wl_columnar_eval_delta_observer_resource_observe_owned(s,
            s->controlled_step, &wrong, other, other_id).protocol == EINVAL);
        CHECK(!wl_evaluation_control_test_active_consumed(other, other_id,
            &consumed)
            && !consumed && !wl_evaluation_control_finish(other, &owner, 0, 0)
            && !wl_evaluation_control_detach(other, &owner));
        wl_evaluation_control_release(other);
        alias_checked = true;
    }
    /* The raw mutators cannot consume the actual in-flight kernel's slots. */
    CHECK(wl_columnar_eval_delta_observer_discard(s) == EBUSY);
    CHECK(wl_columnar_eval_delta_observer_build_discard(s) == EBUSY);
    CHECK(wl_columnar_eval_delta_observer_prepare_discard(s) == EBUSY);
    wl_session_t *same = active_session;
    CHECK(wl_session_try_destroy(&same) == EBUSY && same == active_session);
    bool terminal = point == WL_COLUMNAR_EVAL_DELTA_TEST_RESOURCE_ACK && v[2];
    bool target = target_phase == 0 ? point ==
        WL_COLUMNAR_EVAL_DELTA_TEST_RESOURCE_BEGIN
        : target_phase == 1 ? v[4] ==
        WL_COLUMNAR_EVAL_DELTA_RESOURCE_READERS_DONE
        : target_phase == 2 ? point ==
        WL_COLUMNAR_EVAL_DELTA_TEST_RESOURCE_STEP_BASE
        + WL_COLUMNAR_EVAL_DELTA_RESOURCE_ROWS
        : target_phase == 3 ? terminal : false;
    if (scenario && !turned && target) turn_original();
    if (reader_peer_held) CHECK(atomic_load_explicit(&reader_peer.owner->state,
            memory_order_relaxed) == reader_observed + 1);
    if (credit_peer_held) CHECK(credit_target && credit_target->release_bytes
            && atomic_load_explicit(&credit_target->state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RELEASING
            && wl_columnar_memory_reserved(credit_target->governor) ==
            credit_observed + 1);
    if (!cas_footer_refused && ((scenario == 8 && reader_peer_held)
        || (scenario == 9 && credit_peer_held))) {
        cas_footer_refused = true;
        wl_evaluation_control_request_cancel(funding);
    }
    if (scenario == 4 && terminal && !footer_refused) {
        footer_refused = true;
        CHECK(v[2] && !v[11]);
        wl_evaluation_control_request_cancel(funding);
    }
    check_original();
}

static void
parent_free(wl_col_session_t *s)
{
    uint64_t v[12];
    CHECK(!wl_columnar_session_test_observer_resource_view(s, v));
    CHECK(v[0] && v[2] && v[3] && v[10]);
    if (scenario && target_phase == 5 && !turned) turn_original();
    check_original();
}

static void
delta(const char *name, const int64_t *row, uint32_t width, int difference,
    void *data)
{
    (void)data;
    if (strcmp(name, "output")) return;
    CHECK(width == 1 && row[0] >= 1 && row[0] <= 5);
    if (difference == 1) {
        uint64_t bit = UINT64_C(1) << row[0];
        CHECK(!(event_mask & bit)); event_mask |= bit;
    } else CHECK(difference == -1 && row[0] == 2);
    ++callbacks;
}

static int
run(wl_session_t *session)
{
    int rc = EAGAIN;
    for (unsigned n = 0; rc == EAGAIN && n < 100000; ++n) {
        rc = wl_session_step(session);
        close_peers();
        if (injected && !callbacks) {
            uint64_t v[12];
            CHECK(!wl_columnar_session_test_observer_resource_view(
                    (wl_col_session_t *)session, v));
            if (scenario && target_phase == 4 && v[3] && v[10] && !turned)
                turn_original();
            if (turned &&
                v[10]) CHECK(session->backend->session_controlled_step_state(
                        session)
                    == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
            check_original();
            if (turned && v[10] && v[9] && !foreign_checked) {
                wl_thread_t thread;
                CHECK(!wl_thread_create(&thread, foreign_step, session));
                wl_thread_join(&thread);
                foreign_checked = true;
                check_original();
            }
        }
    }
    CHECK(rc != EAGAIN);
    return rc;
}

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    (void)token; (void)phase;
}

static void
history(unsigned selected_stage, unsigned selected_scenario,
    unsigned selected_stop, unsigned selected_target)
{
    stage = selected_stage; scenario = selected_scenario;
    stop_kind = selected_stop; target_phase = selected_target;
    injected = turned = footer_refused = short_refused = false;
    foreign_checked = alias_checked = false;
    reader_peer_hit = reader_peer_held = credit_peer_hit = credit_peer_held =
        false;
    cas_footer_refused = false;
    child_close_injected = bank_close_injected = false;
    finished_calls = close_grant = close_stage = 0;
    CHECK(!reader_peer.owner && !reader_peer.identity);
    wl_columnar_memory_reservation_init(&credit_peer);
    grant_calls = kernel_actions = callbacks = 0; successor = event_mask = 0;
    expected =
        stop_kind ? WL_EVALUATION_CONTROL_WORK_LIMIT :
        WL_EVALUATION_CONTROL_CANCELLED;
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl input(x:int64)\n.decl link(x:int64,y:int64)\n"
        ".decl output(x:int64)\n.decl other(x:int64)\n"
        "output(x) :- input(x), x > 0.\n"
        "output(x) :- output(y), link(y,x).\n"
        "other(x) :- input(x), x > 1.\n", &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_MAX - 1, &c));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(governor);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve_checked(g, 17, &peer)
        == WL_COLUMNAR_MEMORY_ADMISSION_OK && wl_columnar_memory_commit(&peer,
        &peer));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = c;
    options.memory_governor = scenario == 5 ? NULL : governor;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    active_session = session;
    wl_columnar_memory_governor_ref_t *selected_governor =
        ((wl_col_session_t *)session)->memory_governor;
    CHECK(selected_governor && (scenario == 5
        ? selected_governor != governor : selected_governor == governor));
    /* NULL options resolve a real default governor; they do not disable it. */
    wl_columnar_memory_governor_ref_retain(selected_governor);
    const int64_t rows[] = {-1, 1, 2, 3};
    CHECK(!wl_session_insert(session, "input", rows, 4, 1));
    const int64_t link[] = {-2, -1};
    CHECK(!wl_session_insert(session, "link", link, 1, 2));
    wl_session_set_delta_cb(session, delta, NULL);
    CHECK(!run(session) && callbacks == 3 && event_mask == 14);
    callbacks = 0; event_mask = 0;
    CHECK(!wl_evaluation_control_reset(c));
    const int64_t four = 4;
    CHECK(!wl_session_insert(session, "input", &four, 1, 1));
    col_rel_t *input = session_find_rel((wl_col_session_t *)session, "input");
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(input);
    col_rel_t *old_head = session_find_rel((wl_col_session_t *)session,
            "output");
    CHECK(old_head && old_head->nrows == 3);
    col_relation_snapshot_t old_image = wl_columnar_relation_snapshot(old_head);
    wl_columnar_eval_delta_test_observer_advanced = advanced;
    wl_columnar_session_test_observer_resource_limit = limit;
    wl_columnar_session_test_observer_resource_finished = finished;
    wl_columnar_eval_delta_test_resource_prefooter = prefooter;
    wl_columnar_session_test_observer_parent_free = parent_free;
    wl_columnar_session_test_observer_finance_phase = finance_phase;
    wl_columnar_relation_test_before_reader_cas = reader_cas_peer;
    wl_columnar_memory_governor_test_before_finish_credit_cas = credit_cas_peer;
    int rc = run(session);
    if (!injected || rc != expected || !grant_calls ||
        !kernel_actions) fprintf(stderr,
            "observer history stage=%u scenario=%u stop=%u target=%u injected=%u rc=%d expected=%d\n",
            stage, scenario, stop_kind, target_phase, injected, rc, expected);
    CHECK(injected && rc == expected && !callbacks && grant_calls &&
        kernel_actions);
    CHECK(((scenario != 6 && scenario != 8) || reader_peer_hit)
        && ((scenario != 7 && scenario != 9) || credit_peer_hit)
        && ((scenario != 8 && scenario != 9) || cas_footer_refused));
    CHECK(alias_checked && (scenario == 0 || turned)
        && (scenario == 0 || target_phase == 5 || foreign_checked)
        && (scenario != 3 || short_refused)
        && (scenario != 4 || footer_refused));
    CHECK(finished_calls == grant_calls
        && ((scenario != 10 && scenario != 11) || child_close_injected)
        && ((scenario < 12 || scenario > 19) || bank_close_injected));
    CHECK(session_find_rel((wl_col_session_t *)session, "output") == old_head
        && wl_columnar_relation_snapshot_equal(old_image,
        wl_columnar_relation_snapshot(old_head))
        && wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(input)));
    check_original();
    wl_columnar_eval_delta_test_observer_advanced = NULL;
    wl_columnar_eval_delta_test_resource_prefooter = NULL;
    wl_columnar_session_test_observer_resource_limit = NULL;
    wl_columnar_session_test_observer_resource_finished = NULL;
    wl_columnar_session_test_observer_parent_free = NULL;
    wl_columnar_session_test_observer_finance_phase = NULL;
    wl_columnar_relation_test_before_reader_cas = NULL;
    wl_columnar_memory_governor_test_before_finish_credit_cas = NULL;
    if (successor) CHECK(!wl_evaluation_control_finish(c,
            session->operation_admission, 0, 0));
    CHECK(!wl_evaluation_control_reset(c));
    turned = false; injected = false;
    CHECK(!run(session) && callbacks == 1 && event_mask == 16);
    callbacks = 0; event_mask = 0;
    const int64_t five = 5, two = 2;
    CHECK(!wl_session_insert(session, "input", &five, 1, 1));
    CHECK(!run(session) && callbacks == 1 && event_mask == 32);
    callbacks = 0; event_mask = 0;
    CHECK(!wl_session_remove(session, "input", &two, 1, 1));
    CHECK(!run(session) && callbacks == 1 && !event_mask);
    CHECK(!wl_session_try_destroy(&session));
    wl_evaluation_control_release(c);
    wl_plan_free(plan); wirelog_program_free(program);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(selected_governor))
        == (selected_governor == governor ? 17u : 0u));
    wl_columnar_memory_governor_ref_release(selected_governor);
    CHECK(wl_columnar_memory_reserved(g) == 17
        && atomic_load_explicit(&peer.state, memory_order_relaxed)
        == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
    CHECK(wl_columnar_memory_release(&peer) && !wl_columnar_memory_reserved(g));
    wl_columnar_memory_governor_ref_release(governor);
    active_session = NULL;
}

int
main(void)
{
    unsigned count = 0;
    for (unsigned s = 0; s < 9; ++s)
        for (unsigned k = 0; k < 2; ++k)
            for (unsigned t = 0; t < 5; ++t) {
                history(s, t, k, t == 4 ? 3 : 0); ++count;
            }
    for (unsigned p = 1; p < 6; ++p)
        for (unsigned k = 0; k < 2; ++k) {
            history(7, 2, k, p); ++count;
        }
    for (unsigned s = 0; s < 9; ++s)
        for (unsigned k = 0; k < 2; ++k) {
            history(s, 5, k, 0); ++count;
        }
    for (unsigned k = 0; k < 2; ++k) {
        history(2, 6, k, 0); ++count;
        history(5, 6, k, 0); ++count;
        history(7, 7, k, 0); ++count;
        history(2, 8, k, 0); ++count;
        history(7, 9, k, 0); ++count;
        for (unsigned s = 10; s <= 19; ++s) {
            history(7, s, k, 0); ++count;
        }
    }
    printf("observer resources: %u authentic public histories passed\n", count);
    return 0;
}
