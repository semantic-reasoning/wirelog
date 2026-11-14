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
static bool child_close_injected, bank_close_injected, terminal_entry_cancelled;
static wl_evaluation_control_report_t child_close_report;
static bool terminal_enabled, terminal_destroying, terminal_injected,
    terminal_short, terminal_free_seen, terminal_move_seen,
    terminal_foreign_seen, terminal_fault_seen;
static unsigned terminal_mode, terminal_target, terminal_bank_phase,
    terminal_attempts, terminal_reports, terminal_refusals, terminal_actions,
    terminal_stopped_reports, terminal_setup_reports;
static uint64_t terminal_allocation_identity, terminal_generation;
static wl_evaluation_control_t *main_birth_control;
static uint64_t main_birth_id, main_birth_identity;
static bool main_birth_fault_injected;
static bool main_birth_reported;
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

static uint64_t producer_header_bytes;
extern bool wl_columnar_session_test_fail_frontier_map_allocation;
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);

static void
producer_checkpoint(wl_evaluation_control_t *control, unsigned point,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)bindings;
    if (control != active_session->evaluation_control || point != stage
        || injected) return;
    uint64_t v[6];
    CHECK(!wl_columnar_session_test_producer_view(
            (wl_col_session_t *)active_session, v));
    producer_header_bytes = v[1];
    if (point == 51) {
        uint64_t extent = (uint64_t)target_phase * 64;
        if (extent > v[1]) extent = v[1];
        if (progress != extent) return;
        CHECK(v[0] == progress && (v[2] == (v[0] == v[1])));
    } else if ((point == 52 || point == 53 || point == 54)
        && progress != target_phase)
        return;
    CHECK(point == 50 || point == 51 || v[2]);
    if (point == 50) CHECK(!v[0] && !v[2]);
    if (point == 52) CHECK(v[4] == progress);
    if (point == 53 && !v[5]) return; /* Genuine nonempty zero packet. */
    if (point == 54) CHECK(!v[3] && progress && !v[4]);
    injected = true;
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    if (scenario == 105) {
        CHECK(point == 42 && progress == 8 && v[2]);
        wl_columnar_session_test_fail_frontier_map_allocation = true;
        return; /* A real own-forward array allocation failure, no END. */
    }
    if (!stop_kind) wl_evaluation_control_request_cancel(control);
    else if (stop_kind == 1) CHECK(wl_evaluation_control_charge(control,
            UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    if (scenario == 104) return; /* Ordinary retained WORK, no external END. */
    int ended = stop_kind == 2 ? 7 : stop_kind == 3 ? 0 : expected;
    CHECK(wl_evaluation_control_finish(control,
        active_session->operation_admission, ended, 0) == ended);
    CHECK(!wl_evaluation_control_report(control, &old_report));
    if (scenario == 101 || scenario == 103) {
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!wl_evaluation_control_begin(control,
            active_session->operation_admission));
        successor = wl_evaluation_control_active_attempt_id(control);
        CHECK(successor && successor != old_report.attempt_id);
    }
    turned = true;
}

static bool
terminal_armed(void)
{
    return terminal_enabled && (injected || terminal_destroying);
}
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
    bool terminal = terminal_armed() &&
        (terminal_mode == 4 || terminal_mode == 5)
        && wl_columnar_session_test_terminal_credit(
        (wl_col_session_t *)active_session, token);
    if (!terminal && ((scenario != 7 && scenario != 9) || !injected)) return;
    if (credit_peer_hit) return;
    uint64_t v[12];
    CHECK(!wl_columnar_session_test_observer_resource_view(
            (wl_col_session_t *)active_session, v));
    if (!terminal &&
        (!v[0] || v[3] || v[4] < WL_COLUMNAR_EVAL_DELTA_RESOURCE_STAGING
        || v[4] > WL_COLUMNAR_EVAL_DELTA_RESOURCE_BUILD_METADATA)) return;
    CHECK(token->governor && token->release_bytes &&
        observed >= token->release_bytes);
    wl_columnar_memory_admission_status_t admitted =
        wl_columnar_memory_reserve_checked(token->governor, 1, &credit_peer);
    CHECK(admitted == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || admitted == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
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
        capacity == 256);
    ++grant_calls;
    /* Registration and every owned resource action are separate real MAIN
     * generations. A one-short grant refuses its actual128 action. */
    if (scenario == 3 && !short_refused) return 127;
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
    CHECK(report.limit == 256 || report.limit == 127);
    CHECK(report.consumed <= report.limit && !report.cleanup_status);
    if (report.limit == 127) {
        CHECK(protocol == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !report.consumed && !short_refused);
        short_refused = true;
    }
    check_original();
}

static void
finance_phase(wl_col_session_t *s, wl_evaluation_control_t *funding,
    uint64_t id, unsigned phase, bool after, int financial)
{
    CHECK(s == (wl_col_session_t *)active_session
        && (phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH
        || phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT
        || phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_OBSERVED));
    if (!after) {
        if (phase == 1) {
            close_grant = grant_calls; close_stage = 1;
        } else CHECK(close_grant == grant_calls && phase == close_stage);
        CHECK(funding && id); /* Retained MAIN is never detached per action. */
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
        unsigned selected_phase = ((scenario - 12) & 1u)
            ? WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT
            : WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH;
        if (!bank_close_injected && ((scenario >= 12 && scenario <= 15
            && phase == selected_phase) || (scenario == 46
            && phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT))) {
            bank_close_injected = true;
            if (scenario == 14 || scenario == 15)
                wl_evaluation_control_test_source_lexical_cancel_before_action =
                    true;
            else wl_evaluation_control_test_source_lexical_cancel_after_action =
                    true;
        }

    } else {
        wl_evaluation_control_report_t source;
        CHECK(!wl_session_test_source_early_report(active_session, &source)
            && source.result == financial
            && (financial ? (source.consumed == 0 || source.consumed == 10
            || source.consumed == 23) : source.consumed == 29));
        unsigned next = close_stage == 1
            ? WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT
            : WL_COLUMNAR_SESSION_FINANCE_CLOSE_OBSERVED;
        CHECK(phase == next || (((scenario >= 14 && scenario <= 19)
            || scenario == 47) && financial && phase == close_stage));
        close_stage = phase;
        if (phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_OBSERVED) {
            CHECK(funding && id);
            CHECK(!wl_evaluation_control_report(funding, &child_close_report)
                && child_close_report.attempt_id == id);
        }
        if (financial) {
            CHECK(financial == WL_EVALUATION_CONTROL_CANCELLED
                && bank_close_injected
                && !
                wl_evaluation_control_test_source_lexical_cancel_after_action
                && !
                wl_evaluation_control_test_source_lexical_cancel_before_action);
            /* Reset the real quiescent BANK test generation, never Original. */
            CHECK(!wl_session_test_source_final_reset_cancel(active_session));
        }
        CHECK(funding && id);
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

/* Values in the descriptive MAIN hook contract, not resource permissions. */
enum {
    MAIN_OBSERVER_REGISTER = 13,
    MAIN_OBSERVER = 14,
    MAIN_OBSERVER_FINISH = 15,
    MAIN_OBSERVER_RELEASE = 17
};

static bool
observer_recipient(unsigned recipient)
{
    return recipient == MAIN_OBSERVER_REGISTER || recipient == MAIN_OBSERVER
           || recipient == MAIN_OBSERVER_FINISH
           || recipient == MAIN_OBSERVER_RELEASE;
}

static uint64_t
main_observer_limit(const wl_col_session_t *s, unsigned recipient,
    unsigned action, uint64_t capacity)
{
    (void)action;
    return observer_recipient(recipient) && injected ? limit(s, capacity)
        : capacity;
}

static void
main_observer_finance(wl_col_session_t *s, wl_evaluation_control_t *funding,
    uint64_t id, unsigned recipient, unsigned action, unsigned phase,
    bool after, int financial)
{
    (void)action;
    if (observer_recipient(recipient) && injected)
        finance_phase(s, funding, id, phase, after, financial);
}

static void
main_observer_report(const wl_col_session_t *s, unsigned recipient,
    unsigned action, wl_evaluation_control_report_t report, int protocol)
{
    if (observer_recipient(recipient) && injected)
        finished(s, action, report, protocol);
}

static void
terminal_birth_peer(void)
{
    wl_evaluation_control_test_finance_birth_claim = NULL;
    wl_evaluation_control_t *peer = NULL;
    CHECK(!wl_evaluation_control_create(1, &peer));
    wl_evaluation_control_release(peer);
}

static void
main_birth(const wl_col_session_t *s, bool after,
    wl_evaluation_control_t *control, uint64_t id, uint64_t identity,
    int protocol)
{
    CHECK(s == (wl_col_session_t *)active_session);
    if (!after) {
        uint64_t terminal[16];
        CHECK(!control && !id && !identity && !protocol
            && !wl_columnar_session_test_terminal_view(s, terminal)
            && !terminal[2]); /* MAIN precedes the dependent Tx allocation. */
        if (terminal_mode == 8 && !main_birth_fault_injected) {
            main_birth_fault_injected = true;
            if (scenario == 40)
                wl_evaluation_control_test_finance_birth_claim =
                    terminal_birth_peer;
            else wl_evaluation_control_test_finance_birth_fail = scenario - 40;
        }
        return;
    }
    if (protocol) {
        CHECK(main_birth_fault_injected && !control && !id && !identity);
        return;
    }
    CHECK(control && id && identity);
    main_birth_control = control;
    main_birth_id = id; main_birth_identity = identity;
    main_birth_reported = false;
}

static void
main_report(const wl_col_session_t *s, wl_evaluation_control_t *control,
    uint64_t identity, wl_evaluation_control_report_t report)
{
    CHECK(s == (wl_col_session_t *)active_session
        && control == main_birth_control && identity == main_birth_identity);
    if (report.attempt_id != main_birth_id) return;
    CHECK(report.limit == 256 && !report.consumed && !report.result
        && !report.execution_status && !report.cleanup_status
        && !main_birth_reported);
    main_birth_reported = true;
    ++terminal_setup_reports;
}

static uint64_t
terminal_limit(const wl_col_session_t *s, uint64_t capacity)
{
    CHECK(capacity == 256 && s == (wl_col_session_t *)active_session);
    if (!terminal_armed()) return capacity;
    ++terminal_attempts;
    uint64_t v[16];
    CHECK(!wl_columnar_session_test_terminal_view(s, v) && v[0] && v[6]
        && v[7] == WL_COLUMNAR_SESSION_FINANCE_REBEGIN && v[8]);
    if (terminal_mode == 1 && v[0] == terminal_target && !terminal_short) {
        terminal_short = true; return 127;
    }
    return terminal_mode == 1 ? 128 : capacity;
}

static void
terminal_advanced(wl_col_session_t *s, wl_evaluation_control_t *funding,
    uint64_t id, unsigned before, unsigned after, int protocol)
{
    if (!terminal_armed()) return;
    uint64_t v[16], used = UINT64_MAX;
    CHECK(!wl_columnar_session_test_terminal_view(s, v)
        && !wl_evaluation_control_test_active_consumed(funding, id, &used));
    if (protocol == WL_EVALUATION_CONTROL_WORK_LIMIT) {
        CHECK(terminal_mode == 1 && terminal_short && before == after && !used);
        ++terminal_refusals;
        return;
    }
    CHECK(used == 128 && (protocol == 0 || protocol == EAGAIN));
    uint64_t allocation = wl_evaluation_control_identity(funding);
    CHECK(allocation && allocation == main_birth_identity);
    if (before == WL_COLUMNAR_SESSION_TERMINAL_REGISTER) {
        terminal_allocation_identity = allocation;
        terminal_generation = id;
    } else {
        CHECK(allocation == terminal_allocation_identity
            && id > terminal_generation);
        terminal_generation = id;
    }
    ++terminal_actions;
    if (before == WL_COLUMNAR_SESSION_TERMINAL_METADATA_MOVE) {
        CHECK(v[2] && !v[4] && v[3] && v[5] >= v[3]);
        terminal_move_seen = true;
    }
    if (before == WL_COLUMNAR_SESSION_TERMINAL_TX_FREE) {
        CHECK(!v[2] && v[3] && v[5] >= v[3]
            && after == WL_COLUMNAR_SESSION_TERMINAL_CREDIT);
        terminal_free_seen = true;
    }
    if (credit_peer_held && wl_columnar_session_test_terminal_credit(s,
        credit_target)) {
        CHECK(!v[2] && protocol == EAGAIN
            && atomic_load_explicit(&credit_target->state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RELEASING
            && wl_columnar_memory_reserved(credit_target->governor) ==
            credit_observed + 1);
        if (terminal_mode == 5 && !terminal_injected) {
            terminal_injected = true;
            wl_evaluation_control_request_cancel(funding);
        }
    }
    if (terminal_mode == 2 && before == terminal_target && !terminal_injected) {
        terminal_injected = true;
        wl_evaluation_control_request_cancel(funding);
    }
    check_original();
}

static void
terminal_finance_phase(wl_col_session_t *s, bool maintenance,
    wl_evaluation_control_t *funding, uint64_t id, unsigned phase,
    bool after, int financial)
{
    if (after) {
        wl_evaluation_control_report_t source;
        CHECK(!wl_session_test_source_early_report(active_session, &source)
            && source.result == financial
            && (financial ? (source.consumed == 0 || source.consumed == 10
            || source.consumed == 23) : source.consumed == 29));
    }
    if (!terminal_armed() || maintenance) return;
    uint64_t v[16];
    CHECK(!wl_columnar_session_test_terminal_view(s, v) && v[11]);
    if (!after && (v[0] == WL_COLUMNAR_SESSION_TERMINAL_CREDIT
        || (terminal_mode == 3 && terminal_bank_phase >= 2
        && v[0] == WL_COLUMNAR_SESSION_TERMINAL_HANDOFF))
        && !terminal_injected) {
        if (terminal_mode == 3 && phase == terminal_bank_phase) {
            terminal_injected = true;
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
        }
        if ((terminal_mode == 6 || terminal_mode == 7)
            && phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH) {
            terminal_injected = true;
            CHECK(funding && id);
            if (terminal_mode == 6) {
                wl_evaluation_control_request_cancel(funding);
                CHECK(wl_evaluation_control_charge_attempt(funding, id, 0)
                    == WL_EVALUATION_CONTROL_CANCELLED);
            } else CHECK(!wl_evaluation_control_finish(funding,
                    s->controlled_step, 0, 0));
        }
    }
    if (after && financial) {
        CHECK(terminal_mode == 3 && terminal_injected
            && financial == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!wl_session_test_source_final_reset_cancel(active_session));
    }
    if (phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_CONSUME
        || (phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_OBSERVED
        && v[0] == WL_COLUMNAR_SESSION_TERMINAL_HANDOFF))
        CHECK(!funding && !id);
    if (phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_OBSERVED
        && v[0] != WL_COLUMNAR_SESSION_TERMINAL_HANDOFF)
        CHECK(funding && id &&
            !wl_evaluation_control_active_attempt_id(funding));
    check_original();
}

static void
terminal_finished(const wl_col_session_t *s, unsigned action, bool maintenance,
    wl_evaluation_control_report_t report)
{
    if (!terminal_armed()) return;
    CHECK(s == (wl_col_session_t *)active_session);
    if (maintenance) {
        CHECK(!report.limit); return;
    }
    if (!action) {
        CHECK(report.attempt_id && report.limit == 256 && !report.consumed
            && !report.execution_status && !report.cleanup_status
            && !report.result);
        ++terminal_setup_reports;
        check_original();
        return;
    }
    ++terminal_reports;
    CHECK(report.attempt_id && !report.execution_status &&
        !report.cleanup_status);
    if (report.limit == 127) {
        CHECK(terminal_mode == 1 && action == terminal_target
            && !report.consumed &&
            report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else {
        CHECK((report.limit == 128 || report.limit == 256) &&
            report.consumed == 128);
        CHECK(!report.result || ((terminal_mode == 2 || terminal_mode == 5
            || terminal_mode == 6) && terminal_injected
            && report.result == WL_EVALUATION_CONTROL_CANCELLED));
        if (report.result) ++terminal_stopped_reports;
    }
    check_original();
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
    if (scenario && injected && target_phase == 5 && !turned) turn_original();
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

static unsigned metadata_retry_bind_stop;
static bool metadata_retry_bind_fired, metadata_retry_bind_seen;

static void
metadata_retry_bind(wl_col_session_t *sess, unsigned cursor, bool after,
    int financial, bool published, uint64_t operation_id, uint64_t payload_id,
    bool snapshot_owned)
{
    CHECK(&sess->base == active_session && published && !snapshot_owned
        && !operation_id && !payload_id);
    uint64_t view[10];
    CHECK(!wl_columnar_session_test_metadata_bind_view(sess, view)
        && view[0] == 1 && view[1] == cursor && view[3]
        && !view[4] && !view[5] && !view[6] && !view[7]);
    CHECK(!wl_evaluation_control_active_attempt_id(
            sess->base.evaluation_control));
    if (!after && !metadata_retry_bind_fired) {
        if (cursor != metadata_retry_bind_stop - 1) return;
        metadata_retry_bind_fired = true;
        wl_evaluation_control_test_source_metadata_cancel_at =
            cursor == 4 ? 2 : 4;
    }
    if (after && financial) {
        CHECK(financial == WL_EVALUATION_CONTROL_CANCELLED
            && cursor == (metadata_retry_bind_stop < 5 ?
            metadata_retry_bind_stop : 4));
        metadata_retry_bind_seen = true;
        CHECK(!wl_session_test_source_final_reset_cancel(&sess->base));
    }
}

static int
run(wl_session_t *session)
{
    int rc = EAGAIN;
    for (unsigned n = 0; rc == EAGAIN && n < 100000; ++n) {
        bool first_refusal = false;
        unsigned first_role = WL_SESSION_SOURCE_METADATA_MAIN;
        /* Retained MAIN removes per-action DETACH/RELEASE. Cover genuine
         * FINISH/REPORT both before Source entry and after its real group0
         * preparation instead of nesting the obsolete BANK issuer. */
        unsigned first_phase = scenario == 16 || scenario == 18 ?
            WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH :
            WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT;
        if (!bank_close_injected && ((scenario >= 16 && scenario <= 19)
            || scenario == 47)) {
            uint64_t candidate[5];
            int selected = wl_columnar_session_test_metadata_candidate(
                (wl_col_session_t *)session, candidate);
            uint64_t main[16];
            CHECK(!wl_columnar_session_test_main_view(
                    (wl_col_session_t *)session, main));
            if (injected && selected == WL_BACKEND_FINANCIAL_SELECTED
                && candidate[0] == WL_SESSION_SOURCE_METADATA_MAIN
                && observer_recipient((unsigned)main[2])
                && candidate[1] == first_phase) {
                CHECK(!candidate[2] && !candidate[3] && !candidate[4]);
                bank_close_injected = true;
                first_refusal = scenario != 18 && scenario != 19;
                wl_evaluation_control_test_source_metadata_cancel_at =
                    first_refusal ? 1 : 2;
            }
        }
        if (scenario == 20 && !terminal_entry_cancelled) {
            uint64_t candidate[5];
            if (wl_columnar_session_test_metadata_candidate(
                    (wl_col_session_t *)session, candidate) ==
                WL_BACKEND_FINANCIAL_SELECTED
                && candidate[0] == WL_SESSION_SOURCE_METADATA_MAIN) {
                CHECK(!candidate[2] && !candidate[3] && !candidate[4]);
                first_refusal = terminal_entry_cancelled = true;
                first_role = WL_SESSION_SOURCE_METADATA_MAIN;
                first_phase = (unsigned)candidate[1];
                wl_evaluation_control_test_source_metadata_cancel_at = 1;
                wl_thread_t peer;
                CHECK(!wl_thread_create(&peer, foreign_step, session));
                CHECK(!wl_thread_join(&peer)
                    && wl_evaluation_control_test_source_metadata_cancel_at ==
                    1);
            }
        }
        rc = wl_session_step(session);
        if (first_refusal) {
            uint64_t candidate[5];
            CHECK(rc == EAGAIN
                && wl_columnar_session_test_metadata_candidate(
                    (wl_col_session_t *)session, candidate) ==
                WL_BACKEND_FINANCIAL_SELECTED
                && candidate[0] == first_role
                && candidate[1] == first_phase && !candidate[2]
                && !candidate[3] && !candidate[4]);
            wl_evaluation_control_report_t report;
            CHECK(!wl_session_test_source_pending_reset_cancel(session, &report)
                && report.result == WL_EVALUATION_CONTROL_CANCELLED
                && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
                && !report.consumed && !report.execution_status
                && !report.cleanup_status);
            check_original();
        }
        if (terminal_armed()) {
            uint64_t t[16];
            CHECK(!wl_columnar_session_test_terminal_view(
                    (wl_col_session_t *)session, t));
            if (terminal_free_seen && t[11] && !terminal_foreign_seen) {
                wl_thread_t thread;
                CHECK(!wl_thread_create(&thread, foreign_step, session));
                wl_thread_join(&thread);
                terminal_foreign_seen = true;
                CHECK(wl_session_insert(session, "input", NULL, 0, 1) == EBUSY);
            }
        }
        close_peers();
        if (scenario >= 100 && turned && !foreign_checked && rc == EAGAIN) {
            wl_thread_t thread;
            CHECK(!wl_thread_create(&thread, foreign_step, session));
            CHECK(!wl_thread_join(&thread));
            CHECK(wl_session_insert(session, "input", NULL, 0, 1) == EBUSY);
            foreign_checked = true;
        }
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

typedef struct {
    wl_session_t *session;
    int result;
} migration_t;

static void *
migrated_run(void *argument)
{
    migration_t *p = argument;
    p->result = run(p->session);
    return NULL;
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
    child_close_injected = bank_close_injected = terminal_entry_cancelled =
        false;
    finished_calls = close_grant = close_stage = 0;
    terminal_enabled = scenario >= 20 && scenario <= 45;
    terminal_destroying = terminal_injected = terminal_short = false;
    terminal_free_seen = terminal_move_seen = terminal_foreign_seen = false;
    terminal_fault_seen = false;
    terminal_attempts = terminal_reports = terminal_refusals =
        terminal_actions = 0;
    terminal_stopped_reports = terminal_setup_reports = 0;
    terminal_allocation_identity = terminal_generation = 0;
    main_birth_control = NULL; main_birth_id = main_birth_identity = 0;
    main_birth_fault_injected = false;
    main_birth_reported = false;
    static const unsigned terminal_targets[] = {
        WL_COLUMNAR_SESSION_TERMINAL_ENTRIES,
        WL_COLUMNAR_SESSION_TERMINAL_METADATA_MOVE,
        WL_COLUMNAR_SESSION_TERMINAL_TX_FREE,
        WL_COLUMNAR_SESSION_TERMINAL_CREDIT
    };
    if (scenario >= 20 && scenario <= 31) {
        terminal_mode = (scenario - 20) / 4;
        terminal_target = terminal_targets[(scenario - 20) % 4];
    } else if (scenario >= 32 && scenario <= 35) {
        terminal_mode = 3; terminal_bank_phase = scenario - 31;
        terminal_target = WL_COLUMNAR_SESSION_TERMINAL_TX_FREE;
    } else if (scenario == 36 || scenario == 37) {
        terminal_mode = scenario - 32;
        terminal_target = WL_COLUMNAR_SESSION_TERMINAL_CREDIT;
    } else if (scenario == 38 || scenario == 39) {
        terminal_mode = scenario - 32;
        terminal_target = WL_COLUMNAR_SESSION_TERMINAL_TX_FREE;
    } else if (scenario >= 40 && scenario <= 42) {
        terminal_mode = 8;
        terminal_target = WL_COLUMNAR_SESSION_TERMINAL_METADATA_MOVE;
    } else if (scenario >= 43) {
        terminal_mode = scenario == 44 ? 1 : scenario == 45 ? 4 : 0;
        terminal_target = scenario == 44 ? WL_COLUMNAR_SESSION_TERMINAL_TX_FREE
            : WL_COLUMNAR_SESSION_TERMINAL_CREDIT;
    }
    CHECK(!reader_peer.owner && !reader_peer.identity);
    wl_columnar_memory_reservation_init(&credit_peer);
    grant_calls = kernel_actions = callbacks = 0; successor = event_mask = 0;
    expected = scenario == 105 ? ENOMEM : stop_kind == 2 ? 7
        : stop_kind == 3 ? EINVAL : stop_kind ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : WL_EVALUATION_CONTROL_CANCELLED;
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
    bool use_default = scenario == 5 || (scenario >= 43 && scenario <= 45)
        || scenario == 102 || scenario == 103;
    options.memory_governor = use_default ? NULL : governor;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    active_session = session;
    if (terminal_enabled) {
        wl_columnar_session_test_main_birth = main_birth;
        wl_columnar_session_test_main_report_finished = main_report;
    }
    CHECK(session->owns_evaluation_control &&
        session->owns_operation_admission);
    wl_columnar_memory_governor_ref_t *selected_governor =
        ((wl_col_session_t *)session)->memory_governor;
    CHECK(selected_governor && (use_default
        ? selected_governor != governor : selected_governor == governor));
    /* NULL options resolve a real default governor; they do not disable it. */
    wl_columnar_memory_governor_ref_retain(selected_governor);
    const int64_t rows[] = {-1, 1, 2, 3};
    CHECK(!wl_session_insert(session, "input", rows, 4, 1));
    const int64_t link[] = {-2, -1};
    CHECK(!wl_session_insert(session, "link", link, 1, 2));
    wl_session_set_delta_cb(session, delta, NULL);
    int seeded = run(session);
    if (terminal_enabled && terminal_mode == 8) {
        CHECK(main_birth_fault_injected
            && !wl_evaluation_control_test_finance_birth_fail
            && !wl_evaluation_control_test_finance_birth_claim);
        if (scenario != 40) {
            CHECK(seeded == ENOMEM && !callbacks && !event_mask
                && !session_find_rel((wl_col_session_t *)session, "output"));
            wl_evaluation_control_report_t failed;
            CHECK(!wl_evaluation_control_report(c, &failed)
                && failed.result == ENOMEM);
            CHECK(!wl_evaluation_control_reset(c));
            seeded = run(session);
        }
        terminal_fault_seen = true;
    }
    CHECK(!seeded && callbacks == 3 && event_mask == 14);
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
    if (scenario >= 100)
        wl_columnar_session_test_observed_transaction_checkpoint =
            producer_checkpoint;
    else wl_columnar_eval_delta_test_observer_advanced = advanced;
    wl_columnar_session_test_main_limit = main_observer_limit;
    wl_columnar_session_test_main_action_report = main_observer_report;
    wl_columnar_eval_delta_test_resource_prefooter = prefooter;
    wl_columnar_session_test_observer_parent_free = parent_free;
    wl_columnar_session_test_main_finance_phase = main_observer_finance;
    wl_columnar_relation_test_before_reader_cas = reader_cas_peer;
    wl_columnar_memory_governor_test_before_finish_credit_cas = credit_cas_peer;
    if (terminal_enabled) {
        wl_columnar_session_test_terminal_limit = terminal_limit;
        wl_columnar_session_test_terminal_advanced = terminal_advanced;
        wl_columnar_session_test_terminal_finance_phase =
            terminal_finance_phase;
        wl_columnar_session_test_terminal_finished = terminal_finished;
    }
    int rc = run(session);
    CHECK(scenario != 20 || terminal_entry_cancelled);
    if (terminal_mode == 8 && terminal_enabled) {
        CHECK(!wl_evaluation_control_test_finance_birth_fail
            && !wl_evaluation_control_test_finance_birth_claim
            && main_birth_fault_injected && terminal_fault_seen);
        terminal_injected = true;
    }
    if (!injected || rc != expected || (scenario < 100 && (!grant_calls ||
        !kernel_actions))) fprintf(stderr,
            "observer history stage=%u scenario=%u stop=%u target=%u injected=%u rc=%d expected=%d\n",
            stage, scenario, stop_kind, target_phase, injected, rc, expected);
    CHECK(injected && rc == expected && !callbacks);
    if (scenario >= 100) {
        CHECK(!grant_calls && !kernel_actions);
        CHECK(scenario == 104 || scenario == 105 || turned);
    } else {
        CHECK(grant_calls && kernel_actions);
        CHECK(((scenario != 6 && scenario != 8) || reader_peer_hit)
            && ((scenario != 7 && scenario != 9) || credit_peer_hit)
            && ((scenario != 8 && scenario != 9) || cas_footer_refused));
        CHECK(alias_checked && (scenario == 0 || turned)
            && (scenario == 0 || target_phase == 5 || foreign_checked)
            && (scenario != 3 || short_refused)
            && (scenario != 4 || footer_refused));
        CHECK(finished_calls == grant_calls
            && ((scenario != 10 && scenario != 11) || child_close_injected)
            && ((scenario < 12 || (scenario > 19 && scenario != 46
            && scenario != 47)) || bank_close_injected));
    }
    if (terminal_enabled) {
        CHECK(terminal_move_seen && terminal_free_seen && terminal_foreign_seen
            && terminal_actions && terminal_attempts
            && terminal_reports == terminal_attempts
            && terminal_setup_reports);
        CHECK((terminal_mode != 1 || (terminal_short && terminal_refusals == 1))
            && ((terminal_mode < 2 || terminal_mode == 4) || terminal_injected)
            && ((terminal_mode != 4 && terminal_mode != 5) || credit_peer_hit)
            && (terminal_mode != 8 || terminal_fault_seen));
        CHECK(terminal_stopped_reports == ((terminal_mode == 2
            || terminal_mode == 5 || terminal_mode == 6) ? 1u : 0u));
    }
    if (scenario >= 100 && scenario != 104) {
        uint64_t owner[6], terminal[16];
        CHECK(!wl_columnar_session_test_producer_disposition_view(
                (wl_col_session_t *)session, owner)
            && !wl_columnar_session_test_terminal_view(
                (wl_col_session_t *)session, terminal));
        CHECK(!owner[0] && !owner[1] && !owner[2] && !owner[3]
            && !terminal[6] && !terminal[7] && !terminal[9] && !terminal[12]);
        if (scenario == 105) {
            CHECK(!turned && owner[4] && owner[5]
                && !wl_columnar_session_test_fail_frontier_map_allocation);
        } else if (stop_kind >= 2) {
            CHECK(!owner[4] && !owner[5]);
            const int64_t row = 6;
            CHECK(wl_session_insert(session, "input", &row, 0, 1) == EBUSY);
        }
    }
    CHECK(session_find_rel((wl_col_session_t *)session, "output") == old_head
        && wl_columnar_relation_snapshot_equal(old_image,
        wl_columnar_relation_snapshot(old_head))
        && wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(input)));
    check_original();
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    wl_columnar_eval_delta_test_observer_advanced = NULL;
    wl_columnar_eval_delta_test_resource_prefooter = NULL;
    wl_columnar_session_test_main_limit = NULL;
    wl_columnar_session_test_main_action_report = NULL;
    wl_columnar_session_test_observer_parent_free = NULL;
    wl_columnar_session_test_main_finance_phase = NULL;
    wl_columnar_relation_test_before_reader_cas = NULL;
    wl_columnar_memory_governor_test_before_finish_credit_cas = NULL;
    if (successor) CHECK(!wl_evaluation_control_finish(c,
            session->operation_admission, 0, 0));
    CHECK(!wl_evaluation_control_reset(c));
    turned = false; injected = false;
    if (scenario == 105 && metadata_retry_bind_stop) {
        /* Real OWN_FORWARD_ERROR/ENOMEM has already produced the genuine
         * quiescent failed-retry receipt. This reset has no Original or Tx. */
        wl_evaluation_control_report_t saved, after;
        CHECK(!wl_evaluation_control_report(c, &saved)
            && saved.result == ENOMEM
            && !wl_evaluation_control_active_attempt_id(c));
        metadata_retry_bind_fired = metadata_retry_bind_seen = false;
        wl_columnar_session_test_metadata_bind_phase = metadata_retry_bind;
        int bind_rc;
        unsigned calls = 0;
        do {
            CHECK(++calls < 32);
            bind_rc = wl_session_step(session);
        } while (bind_rc == EAGAIN && !metadata_retry_bind_seen);
        CHECK(bind_rc == EAGAIN && metadata_retry_bind_fired
            && metadata_retry_bind_seen);
        wl_columnar_session_test_metadata_bind_phase = NULL;
        CHECK(session->backend->session_controlled_step_state(session)
            == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING
            && !wl_evaluation_control_active_attempt_id(c)
            && !wl_evaluation_control_report(c, &after)
            && same_report(saved, after));
        wl_session_t *same = session;
        CHECK(wl_session_try_destroy(&same) == EBUSY && same == session);
        const int64_t row = 6;
        CHECK(wl_session_insert(session, "input", &row, 0, 1) == EBUSY);
    }
    CHECK(!run(session) && callbacks == 1 && event_mask == 16);
    callbacks = 0; event_mask = 0;
    const int64_t five = 5, two = 2;
    CHECK(!wl_session_insert(session, "input", &five, 1, 1));
    CHECK(!run(session) && callbacks == 1 && event_mask == 32);
    callbacks = 0; event_mask = 0;
    CHECK(!wl_session_remove(session, "input", &two, 1, 1));
    if (terminal_enabled) {
        migration_t migrated = {session, EINVAL};
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, migrated_run, &migrated));
        CHECK(!wl_thread_join(&thread) && !migrated.result);
    } else CHECK(!run(session));
    CHECK(callbacks == 1 && !event_mask);
    wl_evaluation_control_report_t destroy_report, destroyed_report;
    CHECK(!wl_evaluation_control_report(c, &destroy_report));
    terminal_destroying = terminal_enabled;
    CHECK(!wl_session_try_destroy(&session));
    CHECK(!wl_evaluation_control_report(c, &destroyed_report)
        && same_report(destroy_report, destroyed_report));
    wl_columnar_session_test_terminal_limit = NULL;
    wl_columnar_session_test_terminal_advanced = NULL;
    wl_columnar_session_test_terminal_finance_phase = NULL;
    wl_columnar_session_test_terminal_finished = NULL;
    wl_columnar_session_test_main_birth = NULL;
    wl_columnar_session_test_main_report_finished = NULL;
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
        history(7, 46, k, 0); ++count;
        history(7, 47, k, 0); ++count;
        for (unsigned s = 20; s <= 45; ++s) {
            history(7, s, k, 0); ++count;
        }
    }
    for (unsigned s = 100; s <= 103; ++s)
        for (unsigned k = 0; k < 2; ++k) {
            history(50, s, k, 0); ++count;
            unsigned headers = (unsigned)((producer_header_bytes + 63) / 64);
            for (unsigned h = 1; h <= headers; ++h) {
                history(51, s, k, h); ++count;
            }
            for (unsigned a = 1; a <= 9; ++a) {
                history(52, s, k, a); ++count;
            }
            for (unsigned a = 0; a < 6; ++a) {
                history(53, s, k, a); ++count;
            }
        }
    for (unsigned s = 100; s <= 103; ++s)
        for (unsigned k = 2; k <= 3; ++k) {
            history(50, s, k, 0); ++count;
            history(51, s, k, 1); ++count;
            history(51, s, k, 9999); ++count;
            history(54, s, k, 1); ++count;
            for (unsigned a = 1; a <= 9; ++a) {
                history(52, s, k, a); ++count;
            }
            for (unsigned a = 0; a < 6; ++a) {
                history(53, s, k, a); ++count;
            }
        }
    for (unsigned stop = 1; stop <= 5; ++stop) {
        metadata_retry_bind_stop = stop;
        history(42, 105, 2, 8); ++count;
    }
    metadata_retry_bind_stop = 0;
    history(50, 104, 1, 0); ++count;
    history(51, 104, 1, 1); ++count;
    history(52, 104, 1, 4); ++count;
    history(53, 104, 1, 0); ++count;
    printf("observer resources: %u authentic public histories passed\n", count);
    return 0;
}
