/* Genuine financial cancellation before backend dispatch must release only
 * this invocation's admission, preserving prior continuation and TLS. */
#include "wirelog/session.h"
#include "wirelog/thread.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#if !defined(_WIN32)
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #e); abort(); } } while (0)

typedef struct {
    wl_session_t base;
    wl_controlled_step_state_t state;
    unsigned steps, snapshots, reads;
    bool retain, stop_on_read, stop_in_body, reentry, unselected;
    int result;
    bool request_stop, request_take, metadata_busy;
    unsigned metadata_phase, metadata_actions, metadata_afters;
    wl_session_source_metadata_result_t metadata;
    unsigned commits;
    unsigned issuer_stop;
} fixture_t;

static unsigned destroyed, stop_slot, hook_calls, callbacks;
static wl_session_t *target;
static wl_session_t *nested;
static wl_mutex_t barrier_mutex;
static wl_cond_t barrier_changed;
static bool closing_case, observation_entered, admission_closed;
static bool entry_stop;
static unsigned candidate_mode;
static int candidate_root;
static wl_session_t *binding_session, *binding_other;
static void snapshot_binding_body(wl_session_t *);

static int
metadata_action(void *payload)
{
    fixture_t *f = payload;
    ++f->metadata_actions;
    return 0;
}

static void
metadata_after(void *payload)
{
    fixture_t *f = payload;
    ++f->metadata_afters;
}

static int
metadata_bind(void *payload, unsigned role)
{
    (void)role;
    return metadata_action(payload);
}

static int
metadata_resolve(const wl_session_source_metadata_recipe_t *recipe,
    unsigned group, wl_session_source_metadata_preparation_t *prepared)
{
    (void)prepared;
    CHECK(group == WL_SESSION_SOURCE_METADATA_SELECT
        && recipe->identity == recipe && recipe->root);
    return 0;
}

static int
issuer_observation_stop(fixture_t *f, unsigned issuer)
{
    wl_session_t *session = &f->base;
    wl_session_source_metadata_recipe_t recipe = {
        .session = session, .root = f,
        .root_identity = (uint64_t)(uintptr_t)f,
        .role = WL_SESSION_SOURCE_METADATA_MAIN,
        .result = &f->metadata, .busy = &f->metadata_busy,
        .phase = &f->metadata_phase, .resolve = metadata_resolve
    };
    recipe.identity = &recipe;
    f->metadata_phase = 1; f->metadata_busy = true;
    wl_session_source_metadata_request_t request = {
        .owner = f, .action = metadata_action, .payload = f,
        .busy = &f->metadata_busy, .phase = &f->metadata_phase,
        .before_phase = 1, .operation_identity = (uint64_t)(uintptr_t)f,
        .operation_id = wl_evaluation_control_active_attempt_id(
            session->evaluation_control),
        .kind = WL_SESSION_SOURCE_METADATA_FINANCIER
    };
    wl_evaluation_control_test_source_observation_cancel_at = 5;
    wl_evaluation_control_source_phase_result_t phase;
    switch (issuer) {
    case 0:
        phase = wl_session_source_snapshot_metadata(session, metadata_action,
                f);
        break;
    case 1: {
        unsigned cursor = 0;
        int rc = wl_session_source_metadata_bind_roles(session, f, &cursor,
                metadata_bind);
        CHECK(!cursor);
        return rc;
    }
    case 2:
        return wl_session_source_metadata_observe_bound_roles(session);
    case 3: {
        wl_session_source_metadata_role_result_t role =
            wl_session_source_metadata_role_owned(session, &recipe);
        CHECK(!role.phase.action_called);
        return role.phase.financial_result;
    }
    case 4:
    case 5: {
        static const wl_evaluation_control_source_phase_policy_t policy = {
            208, 128, 80
        };
        wl_session_source_finance_t record = {0};
        wl_session_source_close_scope_t close;
        close.identity = NULL;
        int rc;
        if (issuer == 4)
            rc = wl_session_source_finance_start_checked(session, &record,
                    &policy, 256, &close);
        else {
            wl_session_source_checked_birth_outcome_t birth =
                wl_session_source_finance_start_checked_early(session, &record,
                    &policy, 256, &close);
            CHECK(!birth.birth_attempted && !birth.bound);
            rc = birth.phase.financial_result;
        }
        CHECK(!record.control && !record.id && !record.attached
            && !record.active && !close.identity);
        return rc;
    }
    case 6:
        phase = wl_session_source_metadata_immediate(session, &request);
        break;
    default:
        CHECK(issuer == 7);
        phase = wl_session_source_metadata_immediate_recipe(session, &recipe);
        break;
    }
    CHECK(!phase.action_called && !phase.action_result);
    return phase.financial_result;
}

static const wl_backend_financial_offer_t *
candidate_none(const wl_session_t *session, wl_backend_pending_bind_t *pending)
{
    (void)session;
    pending->root = candidate_mode >= 3 ? &candidate_root : NULL;
    pending->refusal = candidate_mode == 1 ? EBUSY : candidate_mode ==
        2 ? -1 : 0;
    if (candidate_mode == 4) {
        /* A real first-zero cancellation reaches the retained binder's
         * unlock; the continuation and its Root must remain unread. */
        wl_evaluation_control_test_source_metadata_cancel_at = 1;
        wl_session_testhook_fail_next_admission_unlock();
    }
    return NULL;
}

static int
candidate_continue(wl_session_t *session, const void *root)
{
    (void)session; (void)root;
    CHECK(false); return EINVAL;
}

static const wl_backend_financial_offer_t *
observation_candidate(const wl_session_t *session,
    wl_backend_pending_bind_t *pending)
{
    pending->root = NULL; pending->refusal = 0;
    if (session == target && entry_stop) {
        entry_stop = false;
        wl_evaluation_control_test_source_observation_cancel_at = stop_slot;
    }
    return NULL;
}

static int
create_fixture(const wl_plan_t *plan, uint32_t workers, wl_session_t **out)
{
    (void)plan; (void)workers;
    fixture_t *f = calloc(1, sizeof(*f));
    if (!f) return ENOMEM;
    f->base.source_c1_admission = true;
    *out = &f->base;
    return 0;
}

static int
create_options(const wl_plan_t *plan, uint32_t workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    (void)options;
    return create_fixture(plan, workers, out);
}

static void
destroy_fixture(wl_session_t *session)
{
    ++destroyed; free(session);
}

static wl_controlled_step_state_t
state(const wl_session_t *session)
{
    fixture_t *f = (void *)session;
    ++f->reads;
    if (f->stop_on_read) {
        f->stop_on_read = false;
        wl_evaluation_control_test_source_observation_cancel_at = 5;
    }
    return f->state;
}

static int
observe_completion(const wl_session_t *session,
    wl_backend_controlled_completion_kind_t kind, bool dispatched,
    wl_backend_controlled_completion_t *out)
{
    fixture_t *f = (void *)session;
    wl_controlled_step_state_t observed = state(session);
    if (!dispatched || observed != WL_CONTROLLED_STEP_COMPLETE)
        return observed;
    CHECK(kind == WL_BACKEND_CONTROLLED_COMPLETION_STEP);
    *out = (wl_backend_controlled_completion_t){
        .selected = true, .kind = kind, .state = observed,
        .result = f->result, .operation_identity = (uint64_t)(uintptr_t)f,
        .ticket = (uint64_t)f->commits + 1
    };
    return WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETION_READY;
}

static void
commit_completion(wl_session_t *session,
    const wl_backend_controlled_completion_t *completion)
{
    fixture_t *f = (void *)session;
    CHECK(!wl_session_source_operation_scope_owned(session)
        && completion->selected
        && completion->ticket == (uint64_t)f->commits + 1
        && completion->operation_identity == (uint64_t)(uintptr_t)f);
    ++f->commits;
}

static void
tuple(const char *relation, const int64_t *row, uint32_t width, void *data)
{
    (void)relation; (void)row; (void)width; (void)data;
    ++callbacks;
}

static int
step(wl_session_t *session)
{
    fixture_t *f = (void *)session;
    ++f->steps;
    if (f->issuer_stop) {
        unsigned issuer = f->issuer_stop - 1;
        f->issuer_stop = 0;
        int result = issuer_observation_stop(f, issuer);
        CHECK(result == WL_EVALUATION_CONTROL_CANCELLED);
        f->metadata_busy = false;
        return result;
    }
    if (f->reentry) {
        f->reentry = false;
        CHECK(wl_session_source_operation_scope_owned(session));
        CHECK(wl_session_step(session) == EBUSY);
        CHECK(wl_session_source_operation_scope_owned(session));
    }
    if (f->unselected)
        CHECK(!wl_session_source_step_checkpoint(session).selected);
    if (f->request_stop) {
        f->request_stop = false;
        f->metadata_busy = true; f->metadata_phase = 1;
        wl_session_source_metadata_request_t request = {
            .result = &f->metadata, .owner = f, .action = metadata_action,
            .payload = f, .busy = &f->metadata_busy,
            .phase = &f->metadata_phase,
            .operation_id = wl_evaluation_control_active_attempt_id(
                session->evaluation_control),
            .operation_identity = (uint64_t)(uintptr_t)f,
            .before_phase = 1, .kind = WL_SESSION_SOURCE_METADATA_FINANCIER,
            .after = metadata_after
        };
        CHECK(wl_session_source_step_metadata_select(session, &request) == 1);
        wl_evaluation_control_test_source_observation_cancel_at = 5;
        return EAGAIN;
    }
    if (f->request_take) {
        f->request_take = false;
        wl_evaluation_control_source_phase_result_t phase;
        unsigned before = 0;
        uint64_t id = wl_evaluation_control_active_attempt_id(
            session->evaluation_control);
        CHECK(wl_session_source_step_metadata_take(session, &f->metadata,
            f, id, (uint64_t)(uintptr_t)f,
            WL_SESSION_SOURCE_METADATA_FINANCIER, &phase, &before) == 1
            && phase.financial_result == WL_EVALUATION_CONTROL_CANCELLED
            && !phase.action_called && !phase.action_result && before == 1);
        CHECK(!wl_session_source_step_metadata_take(session, &f->metadata,
            f, id, (uint64_t)(uintptr_t)f,
            WL_SESSION_SOURCE_METADATA_FINANCIER, &phase, &before));
    }
    if (f->stop_in_body) {
        f->stop_in_body = false;
        wl_evaluation_control_test_source_observation_cancel_at = 4;
        wl_session_source_step_checkpoint_result_t observed =
            wl_session_source_step_checkpoint(session);
        CHECK(observed.selected
            && observed.financial_result == WL_EVALUATION_CONTROL_CANCELLED);
        return observed.financial_result;
    }
    if (nested) {
        CHECK(wl_session_source_operation_scope_owned(session));
        CHECK(wl_session_snapshot(nested, tuple, NULL)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_session_source_operation_scope_owned(session));
    }
    if (f->retain) {
        if (!wl_evaluation_control_active_attempt_id(
                session->evaluation_control)) {
            CHECK(!wl_evaluation_control_begin(session->evaluation_control,
                session->operation_admission));
            CHECK(!wl_evaluation_control_charge(session->evaluation_control,
                7));
        }
        f->state = WL_CONTROLLED_STEP_PENDING;
        return EAGAIN;
    }
    uint64_t id = wl_evaluation_control_active_attempt_id(
        session->evaluation_control);
    if (id) CHECK(wl_evaluation_control_finish(session->evaluation_control,
            session->operation_admission, f->result, 0) == f->result);
    f->state = WL_CONTROLLED_STEP_COMPLETE;
    return f->result;
}

static int
snapshot(wl_session_t *session, wirelog_on_tuple_fn callback, void *data)
{
    (void)callback; (void)data;
    fixture_t *f = (void *)session;
    if (session == binding_session) snapshot_binding_body(session);
    ++f->snapshots; f->state = WL_CONTROLLED_STEP_COMPLETE;
    return 0;
}

static const wl_compute_backend_t backend = {
    .name = "unstarted-financial-only",
    .session_create = create_fixture,
    .session_create_with_options = create_options,
    .session_destroy = destroy_fixture,
    .session_step = step,
    .session_snapshot = snapshot,
    .session_controlled_step_state = state,
    .session_controlled_financial_offer = observation_candidate,
    .session_controlled_snapshot_evaluates = true
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
    return session;
}

static void
before_observation(wl_session_t *session, bool is_snapshot)
{
    if (session != target) return;
    ++hook_calls;
    CHECK(wl_session_source_operation_scope_owned(session));
    if (closing_case) {
        CHECK(is_snapshot && !wl_mutex_lock(&barrier_mutex));
        observation_entered = true;
        CHECK(!wl_cond_broadcast(&barrier_changed));
        while (!admission_closed)
            CHECK(!wl_cond_wait(&barrier_changed, &barrier_mutex));
        CHECK(!wl_mutex_unlock(&barrier_mutex));
    }
    if (is_snapshot)
        wl_evaluation_control_test_source_metadata_cancel_at = stop_slot;
    else wl_evaluation_control_test_source_observation_cancel_at = 4;
}

static void
closed(wl_session_t *session)
{
    CHECK(session == target && !wl_mutex_lock(&barrier_mutex));
    admission_closed = true;
    CHECK(!wl_cond_broadcast(&barrier_changed));
    CHECK(!wl_mutex_unlock(&barrier_mutex));
}

static void *
destroy_waiter(void *payload)
{
    CHECK(!wl_mutex_lock(&barrier_mutex));
    while (!observation_entered)
        CHECK(!wl_cond_wait(&barrier_changed, &barrier_mutex));
    CHECK(!wl_mutex_unlock(&barrier_mutex));
    wl_session_destroy(payload);
    return NULL;
}

static void
refusal_cases(void)
{
    for (unsigned kind = 0; kind < 8; ++kind) {
        for (stop_slot = 1; stop_slot <= (kind < 5 ? 3u : 1u); ++stop_slot) {
            wl_evaluation_control_t *control = NULL;
            target = new_fixture(&control);
            fixture_t *f = (void *)target;
            bool snap = kind == 3 || kind == 4;
            unsigned logical = kind < 5 ? kind : kind - 5;
            if (logical == 1) f->retain = true;
            if (logical == 1 || logical == 2 || logical == 4)
                CHECK(wl_session_step(target) == (logical == 1 ? EAGAIN : 0));
            unsigned steps = f->steps, snapshots = f->snapshots,
                reads = f->reads;
            uint64_t id = wl_evaluation_control_active_attempt_id(control);
            uint64_t used = 0;
            if (id) CHECK(!wl_evaluation_control_test_active_consumed(control,
                    id, &used) && used == 7);
            hook_calls = 0;
            entry_stop = kind < 3;
            wl_session_test_before_source_observation = before_observation;
            CHECK((snap ? wl_session_snapshot(target, tuple, NULL) :
                wl_session_step(target)) == WL_EVALUATION_CONTROL_CANCELLED);
            wl_session_test_before_source_observation = NULL;
            CHECK(hook_calls == (kind < 3 ? 0u : 1u) && f->steps == steps
                && f->snapshots == snapshots && !callbacks
                && f->reads == reads + (snap ? 1u : 0u)
                && wl_evaluation_control_active_attempt_id(control) == id
                && !wl_session_source_operation_scope_owned(target));
            if (id) {
                uint64_t after = 0;
                CHECK(!wl_evaluation_control_test_active_consumed(control, id,
                    &after) && after == used);
            }
            wl_evaluation_control_report_t source;
            CHECK(!wl_session_test_source_unstarted_reset_cancel(target,
                &source) && source.result == WL_EVALUATION_CONTROL_CANCELLED
                && source.consumed == (kind >= 5 || stop_slot == 3 ? 10u : 0u)
                && !source.execution_status && !source.cleanup_status);
            wl_session_exclusive_attempt_t exclusive = {0};
            bool blocked = logical == 1 || logical == 2 || logical == 4;
            CHECK(wl_session_exclusive_attempt_acquire(target, &exclusive)
                == (blocked ? EBUSY : 0));
            if (!blocked)
                CHECK(!wl_session_exclusive_attempt_release(&exclusive));
            f->retain = false;
            CHECK(!wl_session_step(target));
            CHECK(!wl_session_try_destroy(&target));
            wl_evaluation_control_release(control);
        }
    }
}

static void
snapshot_binding_body(wl_session_t *session)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {
        248, 88, 104
    };
    wl_session_source_finance_t source = {0};
    CHECK(!wl_session_source_finance_start(session, &source, &policy, 240));
    wl_session_source_snapshot_observation_t binding = {0}, bad = {0};
    wl_session_source_finance_t copied = source;
    CHECK(wl_session_source_snapshot_observation_bind(session, &bad, &copied)
        == EINVAL && !bad.identity);
    wl_session_source_finance_t original_alias = {
        .control = session->evaluation_control, .id = 1, .limit = 240,
        .attached = true, .active = true
    };
    CHECK(wl_session_source_snapshot_observation_bind(session, &bad,
        &original_alias) == EINVAL && !bad.identity);
    CHECK(!wl_session_source_snapshot_observation_bind(session, &binding,
        &source));
    wl_session_source_snapshot_observation_t copy = binding;
    copy.identity = &copy;
    CHECK(wl_session_source_snapshot_observation_unbind(session,
        &copy) == EINVAL);
    CHECK(wl_session_source_snapshot_observation_unbind(binding_other,
        &binding) == EINVAL && binding.identity == &binding);
    CHECK(!wl_session_source_snapshot_checkpoint(binding_other).selected);
    CHECK(!wl_session_snapshot(binding_other, tuple, NULL));
    CHECK(wl_session_source_snapshot_checkpoint(session).selected);
    CHECK(wl_session_snapshot(session, tuple, NULL) == EBUSY);
    binding.record = &copied;
    wl_session_source_snapshot_checkpoint_result_t observed =
        wl_session_source_snapshot_checkpoint(session);
    CHECK(observed.selected && observed.financial_result == EINVAL);
    binding.record = &source;
    observed = wl_session_source_snapshot_checkpoint(session);
    CHECK(observed.selected && !observed.financial_result);
    /* This actual BANK initial-zero stop selects the separate real outer
    * observation producer. Subsequent observations do not renew BANK. */
    wl_evaluation_control_test_source_metadata_cancel_at = 1;
    CHECK(!wl_session_source_footer(session) && binding.outer);
    wl_evaluation_control_report_t bank, after;
    CHECK(!wl_session_test_source_early_report(session, &bank)
        && bank.result == WL_EVALUATION_CONTROL_CANCELLED && !bank.consumed);
    CHECK(!wl_session_source_footer(session));
    uint64_t old_id = source.id;
    CHECK(!wl_session_source_finance_finish(&source));
    observed = wl_session_source_snapshot_checkpoint(session);
    CHECK(observed.selected && observed.financial_result == EINVAL);
    CHECK(!wl_session_source_finance_rebegin(&source) && source.id != old_id);
    CHECK(!wl_session_source_footer(session));
    CHECK(!wl_session_test_source_early_report(session, &after)
        && bank.attempt_id == after.attempt_id && bank.limit == after.limit
        && bank.consumed == after.consumed &&
        bank.stop_reason == after.stop_reason
        && bank.execution_status == after.execution_status
        && bank.cleanup_status == after.cleanup_status &&
        bank.result == after.result);
    CHECK(!wl_session_source_snapshot_observation_unbind(session, &binding)
        && !binding.identity && !binding.scope && !binding.record
        && !binding.control && !binding.session);
    CHECK(!wl_evaluation_control_finish(source.control, &source, 0, 0));
    CHECK(!wl_evaluation_control_detach(source.control, &source));
    wl_evaluation_control_release(source.control);
    CHECK(!wl_session_source_snapshot_checkpoint(session).selected);
}

static void
snapshot_binding_contract(void)
{
    wl_evaluation_control_t *control = NULL, *other = NULL;
    binding_session = new_fixture(&control);
    binding_other = new_fixture(&other);
    CHECK(!wl_session_snapshot(binding_session, tuple, NULL));
    wl_evaluation_control_report_t bank;
    CHECK(!wl_session_test_source_unstarted_reset_cancel(binding_session, &bank)
        && bank.result == WL_EVALUATION_CONTROL_CANCELLED && !bank.consumed);
    CHECK(!wl_session_try_destroy(&binding_session)
        && !wl_session_try_destroy(&binding_other));
    wl_evaluation_control_release(other);
    wl_evaluation_control_release(control);
}

/* A Source stop is independent of the real Original result. Both the
 * retained Native generation and an already completed positive cause survive
 * final financial settlement without an extra BEGIN or semantic replay. */
static void
dispatched_observation_stops(void)
{
    for (unsigned kind = 0; kind != 4; ++kind) {
        wl_evaluation_control_t *control = NULL;
        wl_session_t *session = new_fixture(&control);
        fixture_t *f = (void *)session;
        if (kind) {
            f->retain = true;
            CHECK(wl_session_step(session) == EAGAIN);
        }
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        uint64_t used = 0;
        if (id) CHECK(!wl_evaluation_control_test_active_consumed(control,
                id, &used) && used == 7);
        f->retain = false;
        f->stop_in_body = kind < 2;
        f->stop_on_read = kind >= 2;
        if (kind == 2) f->retain = true;
        if (kind == 3) f->result = 9;
        unsigned before = f->steps;
        int result = wl_session_step(session);
        CHECK(result == (!kind ? WL_EVALUATION_CONTROL_CANCELLED
                : kind == 3 ? 9 : EAGAIN)
            && f->steps == before + 1
            && !wl_session_source_operation_scope_owned(session));
        uint64_t after = wl_evaluation_control_active_attempt_id(control);
        if (kind == 1 || kind == 2) {
            CHECK(after == id);
            CHECK(!wl_evaluation_control_test_active_consumed(control,
                id, &used) && used == 7);
        } else CHECK(!after);
        wl_evaluation_control_report_t report;
        CHECK(!wl_session_test_source_unstarted_reset_cancel(session, &report)
            && report.result == WL_EVALUATION_CONTROL_CANCELLED
            && report.consumed == 10);
        if (kind == 3) {
            CHECK(!wl_evaluation_control_report(control, &report)
                && report.attempt_id == id && report.consumed == 7
                && report.execution_status == 9 && !report.cleanup_status
                && report.result == 9);
        }
        f->retain = false; f->result = 0;
        CHECK(!wl_session_step(session));
        CHECK(!wl_session_try_destroy(&session));
        wl_evaluation_control_release(control);
    }
}

static void
deferred_observation_stop(void)
{
    wl_evaluation_control_t *control = NULL;
    wl_session_t *session = new_fixture(&control);
    fixture_t *f = (void *)session;
    f->retain = true; f->reentry = true;
    CHECK(wl_session_step(session) == EAGAIN);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    f->retain = false; f->request_stop = true;
    CHECK(wl_session_step(session) == EAGAIN
        && f->metadata.ready && !f->metadata.scheduled
        && !f->metadata.semantic_valid && !f->metadata_busy
        && !f->metadata.outcome.action_called
        && f->metadata.financial_result == WL_EVALUATION_CONTROL_CANCELLED
        && !f->metadata_actions && !f->metadata_afters
        && wl_evaluation_control_active_attempt_id(control) == id);
    uint64_t used = 0;
    CHECK(!wl_evaluation_control_test_active_consumed(control, id, &used)
        && used == 7);
    wl_evaluation_control_report_t report;
    CHECK(!wl_session_test_source_unstarted_reset_cancel(session, &report)
        && report.result == WL_EVALUATION_CONTROL_CANCELLED
        && report.consumed == 10);
    f->request_take = true;
    CHECK(!wl_session_step(session) && !f->metadata.ready
        && !f->metadata.identity && !f->metadata_actions
        && !f->metadata_afters);
    CHECK(!wl_session_try_destroy(&session));
    wl_evaluation_control_release(control);
}

static void
typed_completion_final_stop(void)
{
    wl_compute_backend_t custom = backend;
    custom.session_controlled_step_observe = observe_completion;
    custom.session_controlled_completion_commit = commit_completion;
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(64, &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(&custom, NULL, 1, &options,
        &session));
    fixture_t *f = (void *)session;
    f->retain = true;
    CHECK(wl_session_step(session) == EAGAIN && !f->commits);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    f->retain = false; f->result = 9; f->stop_on_read = true;
    CHECK(wl_session_step(session) == 9 && f->commits == 1
        && !wl_evaluation_control_active_attempt_id(control)
        && !wl_session_source_operation_scope_owned(session));
    wl_evaluation_control_report_t source, original;
    CHECK(!wl_session_test_source_unstarted_reset_cancel(session, &source)
        && source.result == WL_EVALUATION_CONTROL_CANCELLED
        && source.consumed == 10);
    CHECK(!wl_evaluation_control_report(control, &original)
        && original.attempt_id == id && original.consumed == 7
        && original.result == 9 && original.execution_status == 9
        && !original.cleanup_status);
    CHECK(!wl_session_try_destroy(&session));
    wl_evaluation_control_release(control);
}

static void
issuer_end_refusals(void)
{
    for (unsigned issuer = 0; issuer != 8; ++issuer) {
        wl_evaluation_control_t *control = NULL;
        wl_session_t *session = new_fixture(&control);
        fixture_t *f = (void *)session;
        f->retain = true;
        CHECK(wl_session_step(session) == EAGAIN);
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        f->retain = false; f->issuer_stop = issuer + 1;
        CHECK(wl_session_step(session) == EAGAIN
            && !f->metadata_actions && !f->metadata_afters
            && wl_evaluation_control_active_attempt_id(control) == id
            && !wl_session_source_operation_scope_owned(session));
        uint64_t used = 0;
        CHECK(!wl_evaluation_control_test_active_consumed(control, id, &used)
            && used == 7);
        wl_evaluation_control_report_t source;
        CHECK(!wl_session_test_source_unstarted_reset_cancel(session, &source)
            && source.consumed == 10
            && source.result == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!wl_session_step(session) && !wl_session_try_destroy(&session));
        wl_evaluation_control_release(control);
    }
}

static void
candidate_contract(void)
{
    wl_compute_backend_t custom = backend;
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(64, &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    custom.session_controlled_metadata_bind_continue = candidate_continue;
    custom.session_controlled_financial_offer = NULL;
    CHECK(wl_session_create_with_options(&custom, NULL, 1, &options,
        &session) == EINVAL && !session);
    custom.session_controlled_metadata_bind_continue = NULL;
    custom.session_controlled_financial_offer = candidate_none;
    for (candidate_mode = 0; candidate_mode < 4; ++candidate_mode) {
        CHECK(!wl_session_create_with_options(&custom, NULL, 1, &options,
            &session));
        int rc = wl_session_step(session);
        CHECK(rc == (!candidate_mode ? 0 : candidate_mode == 1 ? EBUSY : EINVAL)
            && ((fixture_t *)session)->steps == (!candidate_mode ? 1u : 0u)
            && !wl_evaluation_control_active_attempt_id(control));
        if (candidate_mode) {
            wl_session_exclusive_attempt_t exclusive = {0};
            CHECK(!wl_session_exclusive_attempt_acquire(session, &exclusive)
                && !wl_session_exclusive_attempt_release(&exclusive));
        }
        CHECK(!wl_session_try_destroy(&session));
    }
    wl_evaluation_control_release(control);
}

static void
binder_unlock_fail_stop(void)
{
#if !defined(_WIN32)
    pid_t child = fork(); CHECK(child >= 0);
    if (!child) {
        wl_compute_backend_t custom = backend;
        custom.session_controlled_financial_offer = candidate_none;
        custom.session_controlled_metadata_bind_continue = candidate_continue;
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(64, &control));
        wl_session_options_t options; wl_session_options_init(&options);
        options.evaluation_control = control;
        wl_session_t *session = NULL;
        CHECK(!wl_session_create_with_options(&custom, NULL, 1, &options,
            &session));
        candidate_mode = 4;
        (void)wl_session_step(session);
        _exit(5); /* An ignored unlock fault must fail the parent oracle. */
    }
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child && WIFSIGNALED(status)
        && WTERMSIG(status) == SIGABRT);
#endif
}

int
main(void)
{
    issuer_end_refusals();
    typed_completion_final_stop();
    deferred_observation_stop();
    dispatched_observation_stops();
    wl_evaluation_control_t *unselected_control = NULL;
    wl_session_t *unselected = new_fixture(&unselected_control);
    unselected->source_c1_admission = false;
    ((fixture_t *)unselected)->unselected = true;
    CHECK(!wl_session_step(unselected) && !wl_session_try_destroy(&unselected));
    wl_evaluation_control_release(unselected_control);
    binder_unlock_fail_stop();
    snapshot_binding_contract();
    candidate_contract();
    refusal_cases();
    wl_evaluation_control_t *outer_control = NULL, *inner_control = NULL;
    wl_session_t *outer = new_fixture(&outer_control);
    nested = new_fixture(&inner_control); target = nested; stop_slot = 3;
    wl_session_test_before_source_observation = before_observation;
    CHECK(!wl_session_step(outer));
    wl_session_test_before_source_observation = NULL;
    wl_evaluation_control_report_t report;
    CHECK(!wl_session_test_source_unstarted_reset_cancel(nested, &report)
        && report.consumed == 10);
    CHECK(!wl_session_try_destroy(&nested) && !wl_session_try_destroy(&outer));
    wl_evaluation_control_release(inner_control);
    wl_evaluation_control_release(outer_control);

    wl_evaluation_control_t *control = NULL;
    target = new_fixture(&control); stop_slot = 3;
    CHECK(!wl_mutex_init(&barrier_mutex) && !wl_cond_init(&barrier_changed));
    closing_case = true;
    wl_session_test_before_source_observation = before_observation;
    wl_session_testhook_set_admission_closed(closed);
    unsigned before = destroyed;
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, destroy_waiter, target));
    CHECK(wl_session_snapshot(target, tuple, NULL)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_thread_join(&thread) && destroyed == before + 1);
    target = NULL;
    wl_session_test_before_source_observation = NULL;
    wl_session_testhook_set_admission_closed(NULL);
    wl_cond_destroy(&barrier_changed); wl_mutex_destroy(&barrier_mutex);
    wl_evaluation_control_release(control);
    puts(
        "unstarted observations: 18 refusals, nested TLS and closing waiter passed");
    return 0;
}
