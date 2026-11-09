/* Financial issuer component tests use real public Session OP/CLOSE scopes.
 * This backend performs no Native evaluation and claims no operator coverage. */
#include "wirelog/session.h"
#include "wirelog/thread.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #e); abort(); } } while (0)

static unsigned operation_calls, close_calls;

static void
report_same(wl_evaluation_control_report_t a, wl_evaluation_control_report_t b)
{
    CHECK(a.attempt_id == b.attempt_id && a.limit == b.limit &&
        a.consumed == b.consumed
        && a.stop_reason == b.stop_reason &&
        a.execution_status == b.execution_status
        && a.cleanup_status == b.cleanup_status && a.result == b.result);
}

typedef struct {
    wl_session_t *session;
    wl_session_source_finance_t *record;
    wl_session_source_close_scope_t *scope;
    wl_session_source_finance_close_result_t result;
} close_call_t;

static void *
foreign_close(void *payload)
{
    close_call_t *p = payload;
    p->result = wl_session_source_finance_close_owned(p->session, p->record,
            p->scope);
    return NULL;
}

static int
nested_close(void *payload)
{
    close_call_t *p = payload;
    p->result = wl_session_source_finance_close_owned(p->session, p->record,
            p->scope);
    return 0;
}

typedef struct {
    wl_session_t *session;
    wl_session_source_finance_t *record;
    wl_session_source_close_scope_t *scope;
    unsigned checks, calls;
    int metadata_error;
    wl_session_source_finance_pair_result_t result;
} pair_call_t;

static pair_call_t *observed_pair_call;

static void
pair_finance_checkpoint(void)
{
    CHECK(observed_pair_call && !observed_pair_call->calls);
    ++observed_pair_call->checks;
}

static int
closed_metadata(void *payload)
{
    pair_call_t *p = payload;
    CHECK(p->checks == 1 && !p->calls && !p->record->control && !p->record->id
        && !p->record->active && !p->record->attached && !p->scope->control
        && p->scope->result.closed && p->scope->result.applied &&
        !p->scope->pair_complete);
    ++p->calls;
    return p->metadata_error;
}

static int
nested_pair_close(void *payload)
{
    pair_call_t *p = payload;
    p->result = wl_session_source_finance_close_then_metadata(p->session,
            p->record, p->scope, closed_metadata, p);
    return 0;
}

static void
finance_pair_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 128,
                                                                       80};
    for (unsigned mode = 0; mode < 8; ++mode) {
        wl_session_source_finance_t record = {0};
        wl_session_source_close_scope_t scope;
        scope.identity = NULL;
        CHECK(!wl_session_source_finance_start_checked(session, &record,
            &policy, 672, &scope));
        CHECK(!wl_session_source_finance_charge(&record, 17));
        pair_call_t call = {session, &record, &scope, 0, 0, mode == 3 ? EIO : 0,
                            {0}};
        observed_pair_call = &call;
        wl_evaluation_control_test_source_pair_checkpoint =
            pair_finance_checkpoint;
        if (mode == 6) {
            wl_session_source_finance_close_result_t ordinary =
                wl_session_source_finance_close_owned(session, &record, &scope);
            CHECK(ordinary.closed && !ordinary.protocol &&
                !scope.pair_complete);
            call.result = wl_session_source_finance_close_then_metadata(session,
                    &record, &scope, closed_metadata, &call);
            CHECK(call.result.protocol == EINVAL && call.result.close.closed
                && !call.result.phase.first_called &&
                !call.result.phase.second_called
                && !scope.pair_complete && !call.calls && !call.checks &&
                !record.control);
            wl_evaluation_control_test_source_pair_checkpoint = NULL;
            observed_pair_call = NULL;
            continue;
        }
        if (mode == 1) {
            wl_evaluation_control_source_phase_result_t outer =
                wl_session_source_snapshot_metadata(session, nested_pair_close,
                    &call);
            CHECK(!outer.financial_result && outer.action_called &&
                !outer.action_result);
            CHECK(call.result.phase.financial_result == EBUSY
                && !call.result.phase.first_called &&
                !call.result.phase.second_called
                && !call.calls && !call.checks);
        } else {
            if (mode == 2) wl_evaluation_control_request_cancel(record.control);
            if (mode == 4) {
                /* Active financial completion really observes cancellation;
                 * inactive physical close must preserve that actual report. */
                wl_evaluation_control_request_cancel(record.control);
                CHECK(wl_session_source_finance_finish(&record) ==
                    WL_EVALUATION_CONTROL_CANCELLED);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(record.control, &report)
                    && report.result == WL_EVALUATION_CONTROL_CANCELLED
                    && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
                    && report.consumed == 17 && !record.active && !record.id);
            }
            if (mode == 7) {
                CHECK(!wl_session_source_finance_finish(&record));
                /* A different real history: cancel arrives only after the
                 * successful financial attempt is already inactive. */
                wl_evaluation_control_request_cancel(record.control);
            }
            if (mode == 5) {
                CHECK(!wl_evaluation_control_retain(record.control));
                wl_session_source_finance_pair_result_t refused =
                    wl_session_source_finance_close_then_metadata(session,
                        &record,
                        &scope, closed_metadata, &call);
                CHECK(refused.protocol == EBUSY && refused.phase.first_called
                    && refused.phase.first_result == EBUSY &&
                    !refused.phase.second_called
                    && !scope.result.closed && !scope.pair_complete &&
                    record.control && !call.calls);
                wl_evaluation_control_release(record.control);
                call.checks = 0; /* Observation only, no owner or permission. */
            }
            call.result = wl_session_source_finance_close_then_metadata(session,
                    &record, &scope, closed_metadata, &call);
            CHECK(!call.result.phase.financial_result &&
                call.result.phase.first_called
                && call.result.phase.first_result ==
                (mode == 2 || mode == 4 ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                && call.result.phase.second_called == (mode != 2 && mode != 4)
                && call.result.phase.second_result == call.metadata_error
                && call.checks == 1 &&
                call.calls == (mode == 2 || mode == 4 ? 0u : 1u));
        }
        CHECK(!call.result.protocol && call.result.close.closed &&
            call.result.close.applied
            && !record.control && !record.id && !scope.control &&
            scope.pair_complete);
        wl_session_source_finance_pair_result_t again =
            wl_session_source_finance_close_then_metadata(session, &record,
                &scope, closed_metadata, &call);
        CHECK(!again.protocol && again.close.closed
            && again.phase.financial_result ==
            call.result.phase.financial_result
            && again.phase.first_called == call.result.phase.first_called
            && again.phase.second_called == call.result.phase.second_called
            && again.phase.first_result == call.result.phase.first_result
            && again.phase.second_result == call.result.phase.second_result);
        wl_evaluation_control_test_source_pair_checkpoint = NULL;
        observed_pair_call = NULL;
    }
}

static void
finance_birth_negatives(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 128,
                                                                       80};
    wl_session_source_finance_t record = {0};
    /* Fully initialized storage is deliberate only for malformed geometry
     * fixtures. Real selected birth initializes its single marker instead. */
    wl_session_source_close_scope_t scope = {0};
    CHECK(wl_session_source_finance_start_checked(session,
        (wl_session_source_finance_t *)(void *)&scope, &policy, 672,
        &scope) == EINVAL);
    CHECK(wl_session_source_finance_start_checked(session,
        (wl_session_source_finance_t *)(void *)session, &policy, 672,
        &scope) == EINVAL);
    CHECK(wl_session_source_finance_start_checked(session, &record, &policy,
        672,
        (wl_session_source_close_scope_t *)(void *)session) == EINVAL);
    CHECK(!record.control && !record.id && !record.limit && !scope.identity);

    /* The policy is a real typed member, not an incompatible lvalue cast
     * into pointer fields. The union supplies deliberate overlapping spans;
     * rejection must precede following any record/scope contents. */
    union {
        wl_session_source_finance_t record;
        struct { void *prefix;
                 wl_evaluation_control_source_phase_policy_t policy; } view;
    } record_alias = {0};
    record_alias.view.policy = policy;
    unsigned char saved_record[sizeof(record_alias)];
    memcpy(saved_record, &record_alias, sizeof(saved_record));
    CHECK(wl_session_source_finance_start_checked(session, &record_alias.record,
        &record_alias.view.policy, 672, &scope) == EINVAL);
    CHECK(!memcmp(&record_alias, saved_record,
        sizeof(saved_record)) && !scope.identity);
    union {
        wl_session_source_close_scope_t scope;
        struct { void *prefix;
                 wl_evaluation_control_source_phase_policy_t policy; } view;
    } scope_alias = {0};
    scope_alias.view.policy = policy;
    unsigned char saved_scope[sizeof(scope_alias)];
    memcpy(saved_scope, &scope_alias, sizeof(saved_scope));
    CHECK(wl_session_source_finance_start_checked(session, &record,
        &scope_alias.view.policy, 672, &scope_alias.scope) == EINVAL);
    CHECK(!memcmp(&scope_alias, saved_scope,
        sizeof(saved_scope)) && !record.control && !record.id);
}

static uint64_t legacy_peer_identity;

static void
legacy_birth_peer(void)
{
    wl_evaluation_control_test_finance_birth_claim = NULL;
    wl_evaluation_control_t *peer = NULL;
    CHECK(!wl_evaluation_control_create(1, &peer));
    legacy_peer_identity = wl_evaluation_control_identity(peer);
    wl_evaluation_control_release(peer);
}

typedef struct {
    wl_session_t *session;
    wl_session_source_finance_t record;
    int result;
} legacy_call_t;

static void *
legacy_foreign_birth(void *payload)
{
    legacy_call_t *p = payload;
    static const wl_evaluation_control_source_phase_policy_t policy = {248, 88,
                                                                       104};
    p->result = wl_session_source_finance_start(p->session, &p->record, &policy,
            672);
    return NULL;
}

static void
legacy_finance_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {248, 88,
                                                                       104};
    static const uint64_t limits[] = {240, 672, 2304};
    for (unsigned i = 0; i < sizeof(limits) / sizeof(*limits); ++i) {
        wl_session_source_finance_t record = {0};
        wl_evaluation_control_test_finance_birth_claim = legacy_birth_peer;
        CHECK(wl_session_source_finance_start(session, &record, &policy,
            0) == EINVAL
            && wl_evaluation_control_test_finance_birth_claim && !record.limit);
        CHECK(wl_session_source_finance_start(session, &record, &policy,
            limits[i]) == EAGAIN
            && !wl_evaluation_control_test_finance_birth_claim &&
            !record.control && !record.id
            && !record.active && !record.attached && !record.charged &&
            record.limit == limits[i]);
        uint64_t next_identity = legacy_peer_identity + 1;
        for (unsigned fault = 0; fault < 3; ++fault) {
            if (fault) {
                wl_evaluation_control_test_finance_birth_fail = fault;
                CHECK(wl_session_source_finance_start(session, &record, &policy,
                    limits[i]) == ENOMEM
                    && !record.control && !record.id && !record.active &&
                    !record.attached
                    && !record.charged && record.limit == limits[i]
                    && !wl_evaluation_control_test_finance_birth_fail);
                ++next_identity;
            }
            CHECK(!wl_session_source_finance_start(session, &record, &policy,
                limits[i]));
            CHECK(record.control && record.id && record.active &&
                record.attached
                && record.limit == limits[i] && !record.charged
                && wl_evaluation_control_identity(record.control) ==
                next_identity++);
            uint64_t scope[8] = {0};
            wl_evaluation_control_lexical_finance_expectation_t e = {
                .owner = &record, .scope = scope,
                .owner_bytes = sizeof(record), .scope_bytes = sizeof(scope),
                .identity = wl_evaluation_control_identity(record.control),
                .limit = record.limit, .id = record.id, .active = true,
            };
            CHECK(!wl_evaluation_control_lexical_finance_prove_checked(
                    record.control, &e));
            wl_session_source_finance_t copy = record;
            wl_evaluation_control_test_finance_birth_claim = legacy_birth_peer;
            CHECK(wl_session_source_finance_start(session, &copy, &policy,
                limits[i]) == EINVAL
                && wl_evaluation_control_test_finance_birth_claim &&
                copy.control == record.control);
            wl_evaluation_control_test_finance_birth_claim = NULL; /* Never dispose a copy. */
            CHECK(!wl_session_source_finance_charge(&record, 17));
            uint64_t id = record.id;
            CHECK(!wl_session_source_finance_finish(&record));
            wl_evaluation_control_report_t saved, current;
            CHECK(!wl_evaluation_control_report(record.control, &saved)
                && saved.attempt_id == id && saved.limit == limits[i]
                && saved.consumed == 17 && !saved.result &&
                !saved.execution_status
                && !saved.cleanup_status && !saved.stop_reason);
            CHECK(!wl_session_source_finance_rebegin(&record) && record.id != id
                && !record.charged && record.active && record.attached);
            CHECK(!wl_evaluation_control_report(record.control, &current));
            report_same(saved, current);
            CHECK(!wl_session_source_finance_charge(&record, 1));
            CHECK(!wl_session_source_finance_dispose(&record) && !record.control
                && !record.id && !record.active && !record.attached);
            record = (wl_session_source_finance_t){0}; /* Genuine old owner fully closed. */
        }
    }
    legacy_call_t foreign = {session, {0}, 0};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, legacy_foreign_birth, &foreign));
    CHECK(!wl_thread_join(&thread));
    CHECK(foreign.result == EINVAL && !foreign.record.control &&
        !foreign.record.id
        && foreign.record.limit == 672 && !foreign.record.active &&
        !foreign.record.attached);
}

static void
legacy_finish_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {248, 88,
                                                                       104};
    for (unsigned mode = 0; mode < 4; ++mode) {
        wl_session_source_finance_t record = {0};
        CHECK(!wl_session_source_finance_start(session, &record, &policy, 32));
        CHECK(!wl_session_source_finance_charge(&record, mode == 2 ? 32 : 7));
        if (mode == 2) CHECK(wl_session_source_finance_charge(&record, 1)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
        if (mode == 1) wl_evaluation_control_request_cancel(record.control);
        uint64_t id = record.id;
        wl_session_source_finance_t copy = record;
        CHECK(wl_session_source_finance_finish(&copy) == EINVAL
            && copy.control == record.control && copy.id == id && copy.active &&
            copy.attached);
        CHECK(!wl_evaluation_control_retain(record.control));
        CHECK(wl_session_source_finance_finish(&record) == EBUSY
            && wl_session_source_finance_dispose(&record) == EBUSY
            && record.active && record.id == id && record.attached &&
            record.control);
        wl_evaluation_control_release(record.control);
        int expected = mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
            : mode == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
        CHECK(wl_session_source_finance_finish(&record) == expected
            && record.control && record.attached && !record.active &&
            !record.id);
        wl_evaluation_control_report_t saved, actual;
        CHECK(!wl_evaluation_control_report(record.control, &saved)
            && saved.attempt_id == id && saved.limit == 32
            && saved.consumed == (mode == 2 ? 32u : 7u)
            && saved.result == expected && (int)saved.stop_reason == expected
            && !saved.execution_status && !saved.cleanup_status);
        if (mode == 0) {
            CHECK(!wl_session_source_finance_rebegin(&record) &&
                record.id != id);
            CHECK(!wl_evaluation_control_report(record.control, &actual));
            report_same(saved, actual);
            wl_evaluation_control_lexical_finance_finish_result_t stale =
                wl_evaluation_control_lexical_finance_finish_owned(
                record.control,
                &record, sizeof(record), id, 32, 0);
            CHECK(stale.protocol == EINVAL && !stale.finished && record.active
                && wl_evaluation_control_active_attempt_id(record.control) ==
                record.id);
            CHECK(!wl_session_source_finance_charge(&record, 1)
                && !wl_session_source_finance_finish(&record));
            CHECK(!wl_evaluation_control_report(record.control,
                &saved) && saved.consumed == 1);
        } else if (mode ==
            3) wl_evaluation_control_request_cancel(record.control);
        /* The observer is acquired after FINISH; inactive disposal must not
        * observe a late cancel, and releases only the actual owner refs. */
        wl_evaluation_control_t *observer = record.control;
        CHECK(!wl_evaluation_control_retain(observer));
        CHECK(!wl_session_source_finance_dispose(&record)
            && !record.control && !record.active && !record.attached &&
            !record.id);
        CHECK(!wl_evaluation_control_report(observer, &actual));
        report_same(saved, actual);
        int new_owner = 0;
        CHECK(!wl_evaluation_control_attach(observer, &new_owner)
            && !wl_evaluation_control_detach(observer, &new_owner));
        wl_evaluation_control_release(observer);
    }
    /* Active implicit disposal completes and still retires both owner refs
     * on a legal financial stop; retain no observer until after its proof. */
    wl_session_source_finance_t stopped = {0};
    CHECK(!wl_session_source_finance_start(session, &stopped, &policy, 32));
    CHECK(!wl_session_source_finance_charge(&stopped, 7));
    wl_evaluation_control_request_cancel(stopped.control);
    CHECK(wl_session_source_finance_dispose(&stopped) ==
        WL_EVALUATION_CONTROL_CANCELLED
        && !stopped.control && !stopped.active && !stopped.attached &&
        !stopped.id);
}

static void
legacy_retire_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {248, 88,
                                                                       104};
    for (unsigned mode = 0; mode < 4; ++mode) {
        wl_session_source_finance_t record = {0};
        CHECK(!wl_session_source_finance_start(session, &record, &policy, 32));
        uint64_t used = mode == 2 ? 32 : 7;
        CHECK(!wl_session_source_finance_charge(&record, used));
        if (mode == 2) CHECK(wl_session_source_finance_charge(&record, 1)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
        if (mode == 1) wl_evaluation_control_request_cancel(record.control);
        wl_session_source_finance_t copy = record;
        CHECK(wl_session_source_finance_retire_active_owned(&copy) == EINVAL
            && copy.control == record.control && copy.id == record.id
            && copy.active && copy.attached);
        CHECK(!wl_evaluation_control_retain(record.control));
        CHECK(wl_session_source_finance_retire_active_owned(&record) == EBUSY
            && record.control && record.active && record.attached);
        wl_evaluation_control_release(record.control);
        if (mode == 3) {
            CHECK(!wl_session_source_finance_finish(&record));
            wl_evaluation_control_request_cancel(record.control);
            CHECK(wl_session_source_finance_retire_active_owned(&record) ==
                EINVAL
                && record.control && record.attached && !record.active &&
                !record.id);
            wl_evaluation_control_report_t saved;
            CHECK(!wl_evaluation_control_report(record.control, &saved)
                && !saved.result && !saved.stop_reason &&
                saved.consumed == used);
            CHECK(!wl_session_source_finance_dispose(&record));
        } else {
            int expected = mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : mode == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
            CHECK(wl_session_source_finance_retire_active_owned(&record) ==
                expected);
        }
        CHECK(!record.control && !record.active && !record.attached &&
            !record.id);
        CHECK(wl_session_source_finance_retire_active_owned(&record) == EINVAL);
    }
}

typedef struct {
    wl_session_t *session;
    wl_session_source_finance_t *record;
    wl_session_source_close_scope_t *scope;
    unsigned *journal;
    unsigned calls, acknowledgments;
    int error;
    wl_session_source_final_pair_result_t original;
    wl_evaluation_control_source_final_result_t retry;
} final_call_t;

static const void *expected_final_closed_payload;

static int
final_closed_metadata(void *payload)
{
    CHECK(!expected_final_closed_payload ||
        payload == expected_final_closed_payload);
    final_call_t *p = payload;
    CHECK(!p->record->control && !p->record->id && !p->record->active
        && !p->record->attached && !p->scope->control &&
        p->scope->result.closed);
    CHECK(!p->calls && !*p->journal);
    ++p->calls; ++*p->journal;
    return p->error;
}

static int
final_ack_metadata(void *payload)
{
    final_call_t *p = payload;
    CHECK(p->calls == 1 && *p->journal == 1 && !p->record->control
        && !p->record->active && p->scope->result.closed);
    ++p->acknowledgments;
    return 0;
}

static int
final_nested_pair(void *payload)
{
    final_call_t *p = payload;
    p->original =
        wl_session_source_finance_close_then_final_metadata(p->session,
            p->record, p->scope, final_closed_metadata, p, sizeof(*p),
            p->journal,
            sizeof(*p->journal));
    return 0;
}

static int
final_nested_publish(void *payload)
{
    final_call_t *p = payload;
    const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
    p->retry = wl_session_source_final_metadata(p->session, &policy,
            final_closed_metadata, p, sizeof(*p), p->journal,
            sizeof(*p->journal));
    return 0;
}

static void
finance_final_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t birth = {208, 128,
                                                                      80};
    for (unsigned mode = 0; mode < 9; ++mode) {
        wl_session_source_finance_t record = {0};
        wl_session_source_close_scope_t scope;
        scope.identity = NULL;
        CHECK(!wl_session_source_finance_start_checked(session, &record, &birth,
            672, &scope));
        CHECK(!wl_session_source_finance_charge(&record, 17));
        unsigned journal = 0;
        final_call_t call = {.session = session, .record = &record,
                             .scope = &scope,
                             .journal = &journal, .error = mode == 1 ? EIO : 0};
        if (mode == 2) {
            wl_evaluation_control_source_phase_result_t outer =
                wl_session_source_snapshot_metadata(session, final_nested_pair,
                    &call);
            CHECK(!outer.financial_result && outer.action_called &&
                !outer.action_result);
            CHECK(call.original.phase.financial_result == EBUSY
                && !call.original.phase.phase_started &&
                !call.original.phase.phase_finished
                && !call.original.phase.first_called &&
                !call.original.phase.second_called
                && call.original.mandatory_unwind_called &&
                call.original.mandatory_unwind.closed
                && call.original.mandatory_unwind.applied &&
                call.original.close.closed
                && !call.calls && !journal);
            unsigned char frozen[sizeof(call.original)];
            memcpy(frozen, &call.original, sizeof(frozen));
            for (unsigned refusal = 0; refusal < 2; ++refusal) {
                outer = wl_session_source_snapshot_metadata(session,
                        final_nested_publish, &call);
                CHECK(!outer.financial_result && outer.action_called &&
                    !outer.action_result
                    && call.retry.financial_result == EBUSY &&
                    !call.retry.action_called
                    && !call.retry.phase_started &&
                    !call.retry.phase_finished && !journal
                    && !memcmp(frozen, &call.original, sizeof(frozen)));
            }
            const wl_evaluation_control_source_phase_policy_t policy = {64, 64,
                                                                        64};
            call.retry = wl_session_source_final_metadata(session, &policy,
                    final_closed_metadata, &call, sizeof(call), &journal,
                    sizeof(journal));
            CHECK(!call.retry.financial_result && call.retry.action_called
                && !call.retry.action_result && call.retry.phase_started &&
                call.retry.phase_finished
                && call.calls == 1 && journal == 1
                && !memcmp(frozen, &call.original, sizeof(frozen)));
        } else if (mode == 3) {
            wl_session_source_close_scope_t copy = scope;
            call.original =
                wl_session_source_finance_close_then_final_metadata(session,
                    &record, &copy, final_closed_metadata, &call, sizeof(call),
                    &journal, sizeof(journal));
            CHECK(call.original.protocol == EINVAL &&
                !call.original.phase.phase_started
                && !call.original.mandatory_unwind_called && record.control &&
                record.active);
            CHECK(wl_session_source_finance_close_owned(session, &record,
                &scope).closed);
        } else if (mode == 4) {
            wl_evaluation_control_t *peer = record.control;
            CHECK(!wl_evaluation_control_retain(peer));
            call.original =
                wl_session_source_finance_close_then_final_metadata(session,
                    &record, &scope, final_closed_metadata, &call, sizeof(call),
                    &journal, sizeof(journal));
            CHECK(call.original.protocol == EBUSY &&
                call.original.phase.phase_started
                && call.original.phase.phase_finished &&
                !call.original.phase.first_called
                && !call.original.mandatory_unwind_called &&
                record.control == peer && record.active);
            wl_evaluation_control_release(peer);
            CHECK(wl_session_source_finance_close_owned(session, &record,
                &scope).closed);
        } else if (mode >= 6) {
            wl_evaluation_control_test_source_final_cancel_at =
                mode == 6 ? WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN
                : mode ==
                7 ? WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_FIRST
                : WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_SECOND;
            call.original =
                wl_session_source_finance_close_then_final_metadata(session,
                    &record, &scope, final_closed_metadata, &call, sizeof(call),
                    &journal, sizeof(journal));
            CHECK(!call.original.protocol && call.original.phase.phase_started
                && call.original.phase.phase_finished &&
                call.original.close.closed
                && call.original.phase.financial_result ==
                WL_EVALUATION_CONTROL_CANCELLED
                && call.original.phase.first_called == (mode != 6)
                && call.original.phase.second_called == (mode == 8)
                && call.original.mandatory_unwind_called == (mode == 6)
                && call.original.mandatory_unwind.closed == (mode == 6)
                && !wl_evaluation_control_test_source_final_cancel_at);
            unsigned char frozen[sizeof(call.original)];
            memcpy(frozen, &call.original, sizeof(frozen));
            const wl_evaluation_control_source_phase_policy_t policy = {64, 64,
                                                                        64};
            CHECK(!wl_session_test_source_final_reset_cancel(session));
            if (mode != 8) {
                CHECK(!journal && !call.calls);
                wl_evaluation_control_test_source_final_cancel_at =
                    WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION;
                call.retry = wl_session_source_final_metadata(session, &policy,
                        final_closed_metadata, &call, sizeof(call), &journal,
                        sizeof(journal));
                CHECK(call.retry.action_called && !call.retry.action_result
                    && call.retry.phase_started && call.retry.phase_finished
                    && call.retry.financial_result ==
                    WL_EVALUATION_CONTROL_CANCELLED
                    && call.calls == 1 && journal == 1 && !call.acknowledgments
                    && !wl_evaluation_control_test_source_final_cancel_at);
                CHECK(!wl_session_test_source_final_reset_cancel(session));
            }
            CHECK(call.calls == 1 && journal == 1);
            wl_evaluation_control_test_source_final_cancel_at =
                WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION;
            wl_evaluation_control_source_final_result_t failed_ack =
                wl_session_source_final_metadata(session, &policy,
                    final_ack_metadata,
                    &call, sizeof(call), &journal, sizeof(journal));
            CHECK(failed_ack.action_called && !failed_ack.action_result
                && failed_ack.phase_started && failed_ack.phase_finished
                && failed_ack.financial_result ==
                WL_EVALUATION_CONTROL_CANCELLED
                && call.acknowledgments == 1 && call.calls == 1 && journal == 1
                && !memcmp(frozen, &call.original, sizeof(frozen))
                && !wl_evaluation_control_test_source_final_cancel_at);
            CHECK(!wl_session_test_source_final_reset_cancel(session));
            call.retry = wl_session_source_final_metadata(session, &policy,
                    final_ack_metadata,
                    &call, sizeof(call), &journal, sizeof(journal));
            CHECK(call.retry.action_called && !call.retry.action_result
                && call.retry.phase_started && call.retry.phase_finished
                && !call.retry.financial_result && call.acknowledgments == 2
                && call.calls == 1 && journal == 1
                && !memcmp(frozen, &call.original, sizeof(frozen))
                && failed_ack.financial_result ==
                WL_EVALUATION_CONTROL_CANCELLED);
        } else {
            if (mode == 5) wl_evaluation_control_request_cancel(record.control);
            call.original =
                wl_session_source_finance_close_then_final_metadata(session,
                    &record, &scope, final_closed_metadata, &call, sizeof(call),
                    &journal, sizeof(journal));
            CHECK(!call.original.protocol && call.original.phase.phase_started
                && call.original.phase.phase_finished &&
                !call.original.phase.financial_result
                && call.original.phase.first_called &&
                call.original.close.closed
                && !call.original.mandatory_unwind_called);
            if (mode == 5) CHECK(!call.original.phase.second_called &&
                    !journal && !call.calls
                    && call.original.phase.first_result ==
                    WL_EVALUATION_CONTROL_CANCELLED
                    && call.original.close.source_result ==
                    WL_EVALUATION_CONTROL_CANCELLED);
            else CHECK(call.original.phase.second_called &&
                    call.original.phase.second_result == call.error
                    && call.calls == 1 && journal == 1);
        }
        CHECK(!record.control && !record.id && !record.active &&
            !record.attached && !scope.control);
        unsigned calls = call.calls;
        wl_session_source_final_pair_result_t repeated =
            wl_session_source_finance_close_then_final_metadata(session,
                &record, &scope,
                final_closed_metadata, &call, sizeof(call), &journal,
                sizeof(journal));
        CHECK(repeated.protocol == EALREADY && !repeated.phase.phase_started
            && !repeated.mandatory_unwind_called && call.calls == calls);
    }
}

static void *
final_foreign_close(void *payload)
{
    final_call_t *p = payload;
    p->original =
        wl_session_source_finance_close_then_final_metadata(p->session,
            p->record, p->scope, final_closed_metadata, p, sizeof(*p),
            p->journal,
            sizeof(*p->journal));
    return NULL;
}

static void
finance_final_range_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 128,
                                                                       80};
    for (unsigned mode = 0; mode < 13; ++mode) {
        wl_session_source_finance_t record = {0};
        wl_session_source_close_scope_t scope;
        scope.identity = NULL;
        CHECK(!wl_session_source_finance_start_checked(session, &record,
            &policy, 672, &scope));
        unsigned journal = 0;
        final_call_t call = {.session = session, .record = &record,
                             .scope = &scope, .journal = &journal};
        void *payload = &call, *target = &journal;
        size_t payload_bytes = sizeof(call), target_bytes = sizeof(journal);
        if (mode == 0) {
            payload = &record; payload_bytes = sizeof(record);
        }
        if (mode == 1) {
            payload = &scope; payload_bytes = sizeof(scope);
        }
        if (mode == 2) {
            target = &record; target_bytes = sizeof(record);
        }
        if (mode == 3) {
            target = &scope; target_bytes = sizeof(scope);
        }
        if (mode == 4) {
            payload = &journal; payload_bytes = sizeof(journal);
        }
        if (mode == 5) {
            payload = session; payload_bytes = sizeof(*session);
        }
        if (mode == 6) {
            target = session; target_bytes = sizeof(*session);
        }
        if (mode == 7) {
            payload = (unsigned char *)record.control + 1; payload_bytes = 1;
        }
        if (mode == 8) {
            target = (unsigned char *)record.control + 1; target_bytes = 1;
        }
        if (mode == 9 || mode == 10)
            wl_evaluation_control_test_source_final_geometry_fault = mode - 8;
        if (mode == 11) payload_bytes = SIZE_MAX;
        if (mode == 12) target_bytes = SIZE_MAX;
        expected_final_closed_payload = &call;
        wl_session_source_final_pair_result_t result =
            wl_session_source_finance_close_then_final_metadata(session,
                &record, &scope,
                final_closed_metadata, payload, payload_bytes, target,
                target_bytes);
        expected_final_closed_payload = NULL;
        CHECK(result.protocol == EINVAL && result.phase.phase_started &&
            result.phase.phase_finished
            && !result.phase.financial_result &&
            result.phase.first_result == EINVAL
            && !result.phase.first_called && !result.phase.second_called
            && result.mandatory_unwind_called && result.mandatory_unwind.closed
            && result.mandatory_unwind.applied && result.close.closed
            && !call.calls && !journal && !record.control && !record.active &&
            !record.attached
            && !scope.control &&
            !wl_evaluation_control_test_source_final_geometry_fault);
    }
    /* A genuine saved FINISH is supported; no active poll is fabricated for
     * the inactive Source. The pair still physically closes its actual refs. */
    wl_session_source_finance_t inactive = {0};
    wl_session_source_close_scope_t scope;
    scope.identity = NULL;
    CHECK(!wl_session_source_finance_start_checked(session, &inactive, &policy,
        672, &scope)
        && !wl_session_source_finance_charge(&inactive, 17)
        && !wl_session_source_finance_finish(&inactive));
    unsigned journal = 0;
    final_call_t call = {.session = session, .record = &inactive,
                         .scope = &scope, .journal = &journal};
    wl_session_source_final_pair_result_t result =
        wl_session_source_finance_close_then_final_metadata(session, &inactive,
            &scope,
            final_closed_metadata, &call, sizeof(call), &journal,
            sizeof(journal));
    CHECK(!result.protocol && result.phase.phase_started &&
        result.phase.phase_finished
        && !result.phase.financial_result && result.phase.first_called &&
        result.phase.second_called
        && !result.phase.first_result && !result.phase.second_result &&
        result.close.closed
        && !result.mandatory_unwind_called && call.calls == 1 && journal == 1 &&
        !inactive.control);
    wl_session_source_finance_t real = {0};
    wl_session_source_close_scope_t real_scope;
    real_scope.identity = NULL;
    CHECK(!wl_session_source_finance_start_checked(session, &real, &policy, 672,
        &real_scope));
    wl_session_source_finance_t invalid_copy = real;
    unsigned untouched = 0;
    final_call_t negative = {.session = session, .record = &real,
                             .scope = &real_scope,
                             .journal = &untouched};
    result = wl_session_source_finance_close_then_final_metadata(session,
            &invalid_copy, &real_scope,
            final_closed_metadata, &negative, sizeof(negative), &untouched,
            sizeof(untouched));
    CHECK(result.protocol == EINVAL && !result.phase.phase_started
        && !result.mandatory_unwind_called && real.control && real.active &&
        !untouched);
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, final_foreign_close, &negative));
    CHECK(!wl_thread_join(&thread) && negative.original.protocol == EINVAL
        && !negative.original.phase.phase_started &&
        !negative.original.mandatory_unwind_called
        && real.control && real.active && !negative.calls && !untouched);
    result = wl_session_source_finance_close_then_final_metadata(session, &real,
            &real_scope,
            final_closed_metadata, NULL, 0, &untouched, sizeof(untouched));
    CHECK(result.protocol == EINVAL && !result.phase.phase_started
        && !result.mandatory_unwind_called && real.control && real.active &&
        !untouched);
    CHECK(wl_session_source_finance_close_owned(session, &real,
        &real_scope).closed);
}

typedef struct {
    wl_session_t *session;
    wl_session_source_finance_t record;
    wl_session_source_close_scope_t scope;
    wl_session_source_checked_birth_outcome_t result;
} early_foreign_birth_t;

static void *
early_foreign_birth(void *payload)
{
    early_foreign_birth_t *p = payload;
    const wl_evaluation_control_source_phase_policy_t policy = {208, 128, 80};
    p->result = wl_session_source_finance_start_checked_early(p->session,
            &p->record, &policy, 672, &p->scope);
    return NULL;
}

static void
early_birth_rejections(wl_session_t *session)
{
    const wl_evaluation_control_source_phase_policy_t policy = {208, 128, 80};
    wl_session_source_finance_t record = {0};
    wl_session_source_close_scope_t scope;
    scope.identity = NULL;
    CHECK(wl_session_source_finance_start_checked_early(session,
        &record, &policy, 672, &scope).bound);
    unsigned char record_before[sizeof(record)], scope_before[sizeof(scope)];
    memcpy(record_before, &record, sizeof(record));
    memcpy(scope_before, &scope, sizeof(scope));
    wl_session_source_finance_t copied_record = record;
    wl_session_source_close_scope_t copied_scope = scope;
    for (unsigned mode = 0; mode < 4; ++mode) {
        wl_session_source_close_scope_t unbound;
        unbound.identity = NULL;
        wl_session_source_checked_birth_outcome_t result =
            wl_session_source_finance_start_checked_early(session,
                mode == 1 ? &copied_record : mode ==
                2 ? (void *)&scope : &record,
                &policy, 672,
                mode == 1 ? &copied_scope : mode == 3 ? &unbound : &scope);
        CHECK(!result.phase.protocol && !result.phase.financial_result
            && result.phase.phase_started && result.phase.phase_finished
            && result.phase.first_called && result.phase.first_result == EINVAL
            && result.birth_attempted && result.birth_result == EINVAL &&
            !result.bound
            && !memcmp(record_before, &record, sizeof(record))
            && !memcmp(scope_before, &scope, sizeof(scope)));
        if (mode == 3) CHECK(!unbound.identity);
    }
    wl_session_t copy = *session;
    wl_session_source_checked_birth_outcome_t invalid =
        wl_session_source_finance_start_checked_early(&copy, &record, &policy,
            672, &scope);
    CHECK(invalid.phase.protocol == EINVAL && !invalid.phase.phase_started
        && !invalid.birth_attempted && !invalid.bound
        && !memcmp(record_before, &record, sizeof(record))
        && !memcmp(scope_before, &scope, sizeof(scope)));
    early_foreign_birth_t foreign = {.session = session};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, early_foreign_birth,
        &foreign) && !wl_thread_join(&thread));
    CHECK(foreign.result.phase.protocol == EINVAL &&
        !foreign.result.phase.phase_started
        && !foreign.result.birth_attempted && !foreign.result.bound
        && !foreign.record.control && !foreign.scope.identity);
    wl_session_source_finance_close_result_t closed =
        wl_session_source_finance_close_owned(session, &record, &scope);
    CHECK(!closed.protocol && closed.closed && closed.applied &&
        !record.control);
}

static void
early_finance_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 128,
                                                                       80};
    for (unsigned mode = 0; mode < 6; ++mode) {
        wl_session_source_finance_t record = {0};
        wl_session_source_close_scope_t scope;
        scope.identity = NULL;
        if (mode ==
            1) wl_evaluation_control_test_source_early_cancel_once = true;
        if (mode == 2 ||
            mode == 3) wl_evaluation_control_test_finance_birth_fail = mode - 1;
        if (mode ==
            4) wl_evaluation_control_test_finance_birth_claim =
                legacy_birth_peer;
        if (mode == 5) wl_evaluation_control_test_source_final_cancel_at =
                WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION;
        wl_session_source_checked_birth_outcome_t result =
            wl_session_source_finance_start_checked_early(session, &record,
                &policy, 672, &scope);
        CHECK(!result.phase.protocol && !result.phase.begin_protocol
            && result.phase.phase_started && result.phase.phase_finished);
        wl_evaluation_control_report_t bank_report;
        CHECK(!wl_session_test_source_early_report(session, &bank_report)
            && bank_report.attempt_id == 1 &&
            bank_report.limit == 3 * WL_EVALUATION_CONTROL_QUANTUM
            && bank_report.consumed ==
            (mode == 1 ? 0u : mode == 5 ? 336u : 416u)
            && bank_report.stop_reason == ((mode == 1 ||
            mode == 5) ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && !bank_report.execution_status && !bank_report.cleanup_status
            && bank_report.result == ((mode == 1 ||
            mode == 5) ? WL_EVALUATION_CONTROL_CANCELLED : 0));
        if (mode == 1) {
            CHECK(result.phase.checkpoint_result ==
                WL_EVALUATION_CONTROL_CANCELLED
                && result.phase.financial_result ==
                WL_EVALUATION_CONTROL_CANCELLED
                && !result.phase.first_called && !result.birth_attempted &&
                !result.bound
                && !record.control && !scope.identity
                && !wl_evaluation_control_test_source_early_cancel_once);
            CHECK(!wl_session_test_source_final_reset_cancel(session));
        } else {
            CHECK(!result.phase.checkpoint_result && result.phase.first_called
                && result.birth_attempted);
            if (mode >= 2 && mode <= 4) {
                CHECK(result.birth_result == (mode == 4 ? EAGAIN : ENOMEM)
                    && result.phase.first_result == result.birth_result &&
                    !result.bound
                    && !record.control && !scope.identity &&
                    !result.phase.financial_result);
            } else {
                CHECK(!result.birth_result && !result.phase.first_result &&
                    result.bound
                    && record.control && record.active && record.attached &&
                    scope.identity == &scope);
                CHECK(result.phase.financial_result ==
                    (mode == 5 ? WL_EVALUATION_CONTROL_CANCELLED : 0));
                wl_session_source_finance_close_result_t closed =
                    wl_session_source_finance_close_owned(session, &record,
                        &scope);
                CHECK(!closed.protocol && closed.applied && closed.closed
                    && !record.control && !record.active && !record.attached);
                if (mode ==
                    5) CHECK(!wl_session_test_source_final_reset_cancel(
                            session));
            }
        }
        CHECK(!wl_evaluation_control_test_finance_birth_claim
            && !wl_evaluation_control_test_finance_birth_fail
            && !wl_evaluation_control_test_source_final_cancel_at);
    }
}

static void
finance_cases(wl_session_t *session)
{
    CHECK(wl_session_source_operation_scope_owned(session) ==
        (close_calls == 0));
    wl_session_t placement_copy = *session;
    CHECK(!wl_session_source_operation_scope_owned(&placement_copy));
    early_finance_cases(session);
    early_birth_rejections(session);
    finance_final_cases(session);
    finance_final_range_cases(session);
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 128,
                                                                       80};
    legacy_finance_cases(session);
    legacy_finish_cases(session);
    legacy_retire_cases(session);
    finance_birth_negatives(session);
    finance_pair_cases(session);
    for (unsigned mode = 0; mode < 7; ++mode) {
        wl_session_source_finance_t record = {0};
        wl_session_source_close_scope_t scope;
        scope.identity = NULL;
        if (mode < 2) {
            wl_evaluation_control_test_finance_birth_fail = mode + 1;
            CHECK(wl_session_source_finance_start_checked(session, &record,
                &policy,
                672, &scope) == ENOMEM && !scope.identity && !record.control
                && !record.id && !record.limit && !record.charged
                && !record.attached && !record.active
                && !wl_evaluation_control_test_finance_birth_fail);
        }
        CHECK(!wl_session_source_finance_start_checked(session, &record,
            &policy, 672, &scope));
        CHECK(scope.identity == &scope && record.active && record.attached &&
            record.id
            && record.control && !record.charged);
        CHECK(!wl_session_source_finance_charge(&record, 17));
        uint64_t id = record.id, used = UINT64_MAX;
        /* Strict internal misuse probes, not public-path Source exposure. */
        wl_session_source_finance_t copied_record = record;
        wl_session_source_close_scope_t copied_scope = scope;
        wl_session_source_finance_close_result_t invalid =
            wl_session_source_finance_close_owned(session, &copied_record,
                &scope);
        CHECK(invalid.protocol == EINVAL && !invalid.closed);
        invalid = wl_session_source_finance_close_owned(session, &record,
                &copied_scope);
        CHECK(invalid.protocol == EINVAL && !invalid.closed);
        invalid = wl_session_source_finance_close_owned(session,
                (wl_session_source_finance_t *)(void *)&scope, &scope);
        CHECK(invalid.protocol == EINVAL && !invalid.closed);
        invalid = wl_session_source_finance_close_owned(session, &record,
                (wl_session_source_close_scope_t *)(void *)session);
        CHECK(invalid.protocol == EINVAL && !invalid.closed);
        for (unsigned bad = 0; bad < 4; ++bad) {
            if (bad == 0) ++record.id;
            else if (bad == 1) ++record.charged;
            else if (bad == 2) ++record.limit;
            else ++scope.control_identity;
            invalid = wl_session_source_finance_close_owned(session, &record,
                    &scope);
            CHECK(invalid.protocol == EINVAL && !invalid.closed &&
                !scope.result.closed);
            if (bad == 0) --record.id;
            else if (bad == 1) --record.charged;
            else if (bad == 2) --record.limit;
            else --scope.control_identity;
        }
        CHECK(!wl_evaluation_control_retain(record.control));
        invalid = wl_session_source_finance_close_owned(session, &record,
                &scope);
        CHECK(invalid.protocol == EBUSY && !invalid.closed &&
            !scope.result.closed);
        wl_evaluation_control_release(record.control);
        close_call_t foreign = {session, &record, &scope, {0}};
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, foreign_close, &foreign));
        CHECK(!wl_thread_join(&thread) && foreign.result.protocol == EBUSY
            && !foreign.result.closed && !scope.result.closed);
        CHECK(wl_evaluation_control_active_attempt_id(record.control) == id
            && !wl_evaluation_control_test_active_consumed(record.control, id,
            &used) && used == 17);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(record.control, &report) == ENOENT);

        int expected = 0;
        if (mode == 3 || mode == 6) {
            wl_evaluation_control_request_cancel(record.control);
            expected = WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(wl_evaluation_control_charge_attempt(record.control,
                record.id, 0) == expected);
        } else if (mode == 4) {
            CHECK(!wl_session_source_finance_charge(&record, 256));
            CHECK(!wl_session_source_finance_charge(&record, 256));
            CHECK(!wl_session_source_finance_charge(&record, 143));
            expected = WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(wl_session_source_finance_charge(&record, 1) == expected);
            CHECK(record.charged == record.limit);
        } else if (mode == 5) {
            /* The authenticated financial close itself polls this request. */
            wl_evaluation_control_request_cancel(record.control);
            expected = WL_EVALUATION_CONTROL_CANCELLED;
        }
        wl_session_source_finance_close_result_t closed;
        if (mode == 1 || mode == 6) {
            /* Real nested BANK composition rejects before close's callback.
             * This internal placement negative exercises actual lexical
             * fallback, without a fabricated result or invalid actionNULL. */
            close_call_t call = {session, &record, &scope, {0}};
            wl_evaluation_control_source_phase_result_t outer =
                wl_session_source_snapshot_metadata(session, nested_close,
                    &call);
            CHECK(!outer.financial_result && outer.action_called &&
                !outer.action_result);
            closed = call.result;
            CHECK(closed.phase.financial_result == EBUSY &&
                !closed.phase.action_called);
        } else {
            if (mode == 2) CHECK(!wl_session_source_finance_finish(&record));
            closed = wl_session_source_finance_close_owned(session, &record,
                    &scope);
            CHECK(!closed.phase.financial_result && closed.phase.action_called
                && closed.phase.action_result == expected);
        }
        CHECK(closed.closed && closed.applied && !closed.protocol &&
            closed.source_result == expected
            && closed.checkpoint_result == expected && !record.control &&
            !record.active
            && !record.attached && !record.id && !scope.control &&
            scope.result.closed);
        wl_session_source_finance_close_result_t again =
            wl_session_source_finance_close_owned(session, &record, &scope);
        CHECK(again.closed && again.applied &&
            again.phase.financial_result == closed.phase.financial_result);
    }
}

static int
finance_create(const wl_plan_t *plan, uint32_t workers, wl_session_t **out)
{
    (void)plan; (void)workers;
    *out = calloc(1, sizeof(**out));
    return *out ? 0 : ENOMEM;
}

static int
finance_create_options(const wl_plan_t *plan, uint32_t workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    (void)options;
    return finance_create(plan, workers, out);
}

static wl_controlled_step_state_t
finance_state(const wl_session_t *session)
{
    (void)session;
    return WL_CONTROLLED_STEP_IDLE;
}

static int
finance_step(wl_session_t *session)
{
    ++operation_calls;
    wl_evaluation_control_t *original = session->evaluation_control;
    wl_evaluation_control_attempt_begin_result_t begin =
        wl_evaluation_control_begin_attempt(original,
            session->operation_admission);
    CHECK(!begin.protocol && begin.attempt_id);
    CHECK(!wl_evaluation_control_charge_attempt(original, begin.attempt_id, 2));
    wl_evaluation_control_request_cancel(original);
    finance_cases(session);
    /* Financial ownership tests must not poll the pending Original cancel. */
    wl_evaluation_control_attempt_finish_result_t end =
        wl_evaluation_control_finish_attempt(original,
            session->operation_admission, begin.attempt_id, 7, 0);
    CHECK(!end.protocol && end.report_valid && end.report.result == 7
        && end.report.execution_status == 7 && !end.report.stop_reason &&
        end.report.consumed == 2);
    wl_evaluation_control_report_t saved = end.report;
    CHECK(!wl_evaluation_control_reset(original));
    begin = wl_evaluation_control_begin_attempt(original,
            session->operation_admission);
    CHECK(!begin.protocol && begin.attempt_id != saved.attempt_id);
    finance_cases(session);
    uint64_t used = UINT64_MAX;
    wl_evaluation_control_report_t frozen;
    CHECK(!wl_evaluation_control_report(original, &frozen));
    report_same(saved, frozen);
    CHECK(!wl_evaluation_control_test_active_consumed(original,
        begin.attempt_id, &used) && !used);
    end = wl_evaluation_control_finish_attempt(original,
            session->operation_admission, begin.attempt_id, 7, 0);
    CHECK(!end.protocol && end.report_valid && end.report.result == 7
        && end.report.execution_status == 7 && !end.report.consumed);
    return 0;
}

static void
finance_destroy(wl_session_t *session)
{
    ++close_calls;
    wl_evaluation_control_report_t saved, current;
    CHECK(!wl_evaluation_control_active_attempt_id(session->evaluation_control)
        && !wl_evaluation_control_report(session->evaluation_control, &saved)
        && saved.result == 7);
    finance_cases(session);
    CHECK(!wl_evaluation_control_report(session->evaluation_control, &current));
    report_same(saved, current);
    free(session);
}

int
main(void)
{
    const wl_compute_backend_t backend = {
        .name = "financial-scope-only",
        .session_create = finance_create,
        .session_create_with_options = finance_create_options,
        .session_destroy = finance_destroy,
        .session_step = finance_step,
        .session_controlled_step_state = finance_state,
    };
    wl_evaluation_control_t *original = NULL;
    CHECK(!wl_evaluation_control_create(256, &original));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = original;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(&backend, NULL, 1, &options,
        &session));
    wl_session_source_finance_t outside = {0};
    wl_session_source_close_scope_t unbound;
    unbound.identity = NULL;
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 128,
                                                                       80};
    wl_session_source_checked_birth_outcome_t early_outside =
        wl_session_source_finance_start_checked_early(session, &outside,
            &policy, 672, &unbound);
    CHECK(early_outside.phase.protocol == EINVAL &&
        !early_outside.phase.phase_started
        && !early_outside.birth_attempted && !early_outside.bound
        && !outside.control && !unbound.identity);
    CHECK(wl_session_source_finance_start_checked(session, &outside, &policy,
        672, &unbound) == EINVAL
        && !outside.control && !unbound.identity);
    CHECK(wl_session_source_finance_start(session, &outside, &policy,
        672) == EINVAL
        && !outside.control && !outside.id && outside.limit == 672);
    CHECK(!wl_session_source_operation_scope_owned(session)
        && !wl_session_source_operation_scope_owned(NULL));
    CHECK(!wl_session_step(session) && operation_calls == 1);
    wl_session_destroy(session);
    CHECK(close_calls == 1);
    wl_evaluation_control_release(original);
    puts("evaluation_source_finance: OK");
    return 0;
}
