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
    for (unsigned mode = 0; mode < 7; ++mode) {
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
                /* A genuinely completed unlatched cancellation remains a
                 * completed report; inactive close must not poll it anew. */
                wl_evaluation_control_request_cancel(record.control);
                CHECK(!wl_session_source_finance_finish(&record));
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
                (mode == 2 ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                && call.result.phase.second_called == (mode != 2)
                && call.result.phase.second_result == call.metadata_error
                && call.checks == 1 && call.calls == (mode == 2 ? 0u : 1u));
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

static void
finance_cases(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 128,
                                                                       80};
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
            session->operation_admission, begin.attempt_id, 0, 0);
    CHECK(!end.protocol && end.report_valid && !end.report.result
        && !end.report.stop_reason && end.report.consumed == 2);
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
            session->operation_admission, begin.attempt_id, 0, 0);
    CHECK(!end.protocol && end.report_valid && !end.report.result &&
        !end.report.consumed);
    return 0;
}

static void
finance_destroy(wl_session_t *session)
{
    ++close_calls;
    finance_cases(session);
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
    CHECK(wl_session_source_finance_start_checked(session, &outside, &policy,
        672, &unbound) == EINVAL
        && !outside.control && !unbound.identity);
    CHECK(!wl_session_step(session) && operation_calls == 1);
    wl_session_destroy(session);
    CHECK(close_calls == 1);
    wl_evaluation_control_release(original);
    puts("evaluation_source_finance: OK");
    return 0;
}
