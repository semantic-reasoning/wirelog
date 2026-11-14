/* Read-only historical observations for real public STEP test drivers. */
#ifndef WL_TEST_CONTROLLED_STEP_OBSERVATION_H
#define WL_TEST_CONTROLLED_STEP_OBSERVATION_H

#include "../wirelog/session.h"

/* Historical metadata only: none of these fields permits Native work. */
typedef struct {
    uint64_t ticket, operation_identity, old_id;
    wl_backend_controlled_completion_domain_t domain;
    wl_backend_controlled_completion_kind_t kind;
    int result;
    wl_evaluation_control_report_t report;
    bool selected;
} wl_test_completed_step_t;

static inline bool
wl_test_completed_step_report_equal(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b)
{
    return a->attempt_id == b->attempt_id && a->limit == b->limit
           && a->consumed == b->consumed && a->stop_reason == b->stop_reason
           && a->execution_status == b->execution_status
           && a->cleanup_status == b->cleanup_status && a->result == b->result;
}

static inline bool
wl_test_completed_step_unchanged(
    const wl_test_completed_step_t *old,
    bool same_control, uint64_t current,
    const wl_evaluation_control_report_t *report)
{
    return old->selected && same_control && !current
           && wl_test_completed_step_report_equal(&old->report, report);
}

static inline bool
wl_test_completed_step_matches(const wl_test_completed_step_t *old,
    bool same_control, uint64_t current, uint64_t id, int observation,
    const wl_backend_controlled_completion_t *c,
    const wl_session_test_completed_pin_t *pin,
    const wl_evaluation_control_report_t *report)
{
    if (!same_control || current || !id || observation !=
        WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETED_OWNER_PENDING
        || !c->selected || c->domain !=
        WL_BACKEND_CONTROLLED_COMPLETION_POST_FINISH_TX
        || c->kind != WL_BACKEND_CONTROLLED_COMPLETION_STEP
        || c->state != WL_CONTROLLED_STEP_PENDING
        || !c->ticket || !c->operation_identity || c->completed_attempt_id != id
        || !pin->selected || pin->active_operations
        || pin->ticket != c->ticket
        || pin->operation_identity != c->operation_identity
        || pin->completed_attempt_id != id || pin->result != c->result
        || report->attempt_id != id || report->result != c->result)
        return false;
    return !old->selected || (old->domain == c->domain && old->kind == c->kind
           && old->ticket == c->ticket &&
           old->operation_identity == c->operation_identity
           && old->old_id == id && old->result == c->result
           && wl_test_completed_step_unchanged(old, same_control, current,
           report));
}

static inline bool
wl_test_completed_step_observe(wl_session_t *session,
    wl_evaluation_control_t *control, uint64_t current, uint64_t id,
    wl_test_completed_step_t *old)
{
    if (current || !id || session->evaluation_control != control
        || !session->backend->session_controlled_step_observe) return false;
    wl_backend_controlled_completion_t c = {0};
    int observed = session->backend->session_controlled_step_observe(session,
            WL_BACKEND_CONTROLLED_COMPLETION_STEP, true, &c);
    wl_session_test_completed_pin_t pin;
    wl_evaluation_control_report_t report;
    if (wl_session_test_completed_pin_view(session, &pin)
        || wl_evaluation_control_report(control, &report)
        || !wl_test_completed_step_matches(old, true, current, id, observed,
        &c, &pin, &report)) return false;
    if (!old->selected) {
        *old = (wl_test_completed_step_t){
            .ticket = c.ticket, .operation_identity = c.operation_identity,
            .old_id = id, .domain = c.domain, .kind = c.kind,
            .result = c.result,
            .report = report, .selected = true
        };
    }
    return true;
}

#endif
