/* Focused regression for the private financial class and public birth lifetime.
 * Test prices exercise the finite kernel; they are not production source fees. */
#include "wirelog/evaluation_control.h"
#include "wirelog/thread.h"
#include "wirelog/session.h"
#include "wirelog/session_options.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); exit(1); \
                      } } while (0)

static const wl_evaluation_control_source_phase_policy_t test_policy = {
    WL_EVALUATION_CONTROL_QUANTUM, WL_EVALUATION_CONTROL_QUANTUM,
    WL_EVALUATION_CONTROL_QUANTUM
};

static int
count_action(void *payload)
{
    ++*(unsigned *)payload;
    return 0;
}

/* Intentional invalid callers: neither probe is a positive SOURCE issuer. */
static wl_evaluation_control_source_context_t *negative_reentry_context;

static int
reentry_action(void *payload)
{
    wl_evaluation_control_source_phase_result_t nested =
        wl_evaluation_control_source_bank_run(negative_reentry_context,
            &test_policy, count_action, payload);
    CHECK(nested.financial_result != 0 && !nested.action_called);
    return 0;
}

typedef struct {
    wl_evaluation_control_source_context_t *context;
    unsigned *calls;
    wl_evaluation_control_source_phase_result_t result;
    int end_result;
} foreign_probe_t;

static void *
foreign_context(void *payload)
{
    foreign_probe_t *p = payload;
    p->result = wl_evaluation_control_source_bank_run(p->context,
            &test_policy, count_action, p->calls);
    p->end_result = wl_evaluation_control_source_context_end(p->context);
    return NULL;
}

static int
real_failed_constructor(void *payload)
{
    wl_evaluation_control_t **out = payload;
    return wl_evaluation_control_create(1, out);
}

static void
same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b)
{
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit
        && a->consumed == b->consumed && a->stop_reason == b->stop_reason
        && a->execution_status == b->execution_status
        && a->cleanup_status == b->cleanup_status && a->result == b->result);
}

typedef struct {
    wl_evaluation_control_t *control;
    uint64_t id;
} lexical_birth_t;

static int
lexical_birth(void *payload)
{
    lexical_birth_t *birth = payload;
    int rc = wl_evaluation_control_create(0, &birth->control);
    if (rc) return rc;
    rc = wl_evaluation_control_attach(birth->control, birth);
    if (rc) {
        wl_evaluation_control_release(birth->control);
        birth->control = NULL;
        return rc;
    }
    rc = wl_evaluation_control_begin(birth->control, birth);
    if (rc) {
        CHECK(!wl_evaluation_control_detach(birth->control, birth));
        wl_evaluation_control_release(birth->control);
        birth->control = NULL;
        return rc;
    }
    birth->id = wl_evaluation_control_active_attempt_id(birth->control);
    return birth->id ? 0 : EINVAL;
}

/* Deliberately invalid reentrant SOURCE caller, never a production action. */
static wl_evaluation_control_source_bank_t *negative_lexical_bank;
static int
lexical_reentry(void *payload)
{
    wl_evaluation_control_source_phase_result_t nested =
        wl_evaluation_control_source_bank_run_lexical(negative_lexical_bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, payload);
    CHECK(nested.financial_result == EBUSY && !nested.action_called);
    return 0;
}

/* Invalid nested issuers prove the enforced sealed financial contract.
 * No Native Root/resource or synthetic permission is supplied by this test. */
typedef struct {
    wl_evaluation_control_source_bank_t *bank, *other;
    wl_evaluation_control_t *original;
    unsigned calls;
} sealed_probe_t;

static int
sealed_nested_action(void *payload)
{
    sealed_probe_t *p = payload;
    ++p->calls;
    wl_evaluation_control_source_metadata_outcome_t out = {37, false};
    CHECK(wl_evaluation_control_source_bank_run_metadata(p->bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy, count_action,
        &p->calls,
        &out) == EBUSY && !out.action_called && out.action_result == 37);
    wl_evaluation_control_source_phase_result_t nested =
        wl_evaluation_control_source_bank_run_lexical(p->other,
            WL_EVALUATION_CONTROL_SOURCE_CLOSE, &test_policy,
            count_action, &p->calls);
    CHECK(nested.financial_result == EBUSY && !nested.action_called);
    wl_evaluation_control_source_context_t issued = {0};
    CHECK(wl_evaluation_control_source_metadata_enter_issued(p->other, &issued)
        == -EBUSY && !issued.identity && !issued.bank && !issued.thread_cookie);
    wl_evaluation_control_source_context_t context = {0};
    CHECK(wl_evaluation_control_source_context_begin(p->other,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context) == EBUSY
        && !context.identity);
    CHECK(wl_evaluation_control_source_bank_destroy(&p->other) == EBUSY
        && p->other);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(p->original, &report)
        && report.limit == 9 && report.consumed == 3 && !report.result);
    return 0;
}

static void
sealed_metadata(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    sealed_probe_t p = {.bank = bank, .original = original};
    CHECK(!wl_evaluation_control_source_bank_create(&p.other));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    for (unsigned mode = 0; mode < 3; ++mode) {
        unsigned calls = p.calls;
        wl_evaluation_control_source_metadata_outcome_t out = {29, false};
        if (mode == 1)
            wl_evaluation_control_test_source_lexical_cancel_before_action =
                true;
        if (mode == 2)
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
        int rc = wl_evaluation_control_source_bank_run_metadata(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
                sealed_nested_action, &p, &out);
        CHECK(rc == (mode ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && out.action_called == (mode != 1)
            && out.action_result == (mode == 1 ? 29 : 0)
            && p.calls == calls + (mode == 1 ? 0u : 1u));
        if (mode) CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                    bank, WL_EVALUATION_CONTROL_SOURCE_OPERATION));
        /* Both BANKs are usable after actual callback return/TLS cleanup. */
        out = (wl_evaluation_control_source_metadata_outcome_t){0, false};
        CHECK(!wl_evaluation_control_source_bank_run_metadata(p.other,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, &p.calls, &out) && out.action_called);
        wl_evaluation_control_source_phase_result_t lexical =
            wl_evaluation_control_source_bank_run_lexical(bank,
                WL_EVALUATION_CONTROL_SOURCE_CLOSE, &test_policy,
                count_action, &p.calls);
        CHECK(!lexical.financial_result && lexical.action_called);
    }
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_source_bank_destroy(&p.other) && !p.other);
}

/* Financial-only kernel fixture: it proves real saved-generation checkpoint
 * refusal and sealed preparation, not any columnar Root or Native permission. */
typedef struct {
    sealed_probe_t sealed;
    unsigned groups, fail_group;
    bool omit_action, omit_publication;
} metadata_recipe_probe_t;

static int
metadata_recipe_prepare(const void *recipe, unsigned group,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    metadata_recipe_probe_t *p = (metadata_recipe_probe_t *)recipe;
    CHECK(group == p->groups && group < 3);
    ++p->groups;
    if (p->fail_group == group + 1) return EINVAL;
    /* The no-nested-BANK guard applies to preparation as well as action. */
    wl_evaluation_control_source_metadata_outcome_t nested = {17, false};
    CHECK(wl_evaluation_control_source_bank_run_metadata(p->sealed.other,
        WL_EVALUATION_CONTROL_SOURCE_CLOSE, &test_policy, count_action,
        &p->sealed.calls, &nested) == EBUSY
        && !nested.action_called && nested.action_result == 17);
    if (group == 2) {
        if (!p->omit_action) prepared->action = sealed_nested_action;
        prepared->payload = &p->sealed;
        if (!p->omit_publication) prepared->result_published = true;
    }
    return 0;
}

static void
sealed_metadata_recipe(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    metadata_recipe_probe_t p = {.sealed = {
                                     .bank = bank, .original = original
                                 }};
    CHECK(!wl_evaluation_control_source_bank_create(&p.sealed.other));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    /* Success, each real zero refusal, CURRENT refusal, and footer refusal. */
    for (unsigned mode = 0; mode < 7; ++mode) {
        p.groups = 0;
        unsigned calls = p.sealed.calls;
        wl_evaluation_control_source_metadata_preparation_t prepared = {0};
        if (mode && mode <= 4)
            wl_evaluation_control_test_source_metadata_cancel_at = mode;
        if (mode == 5)
            wl_evaluation_control_test_source_lexical_cancel_before_action =
                true;
        if (mode == 6)
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
        int rc = wl_evaluation_control_source_bank_run_metadata_recipe(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
                metadata_recipe_prepare, &p, &prepared);
        unsigned expected_groups = mode && mode <= 4 ? mode - 1 : 3;
        bool called = !mode || mode == 6;
        CHECK(rc == (mode ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && p.groups == expected_groups
            && prepared.groups_completed == expected_groups
            && !prepared.preparation_protocol && prepared.entry.begun
            && prepared.entry.settled
            && prepared.entry.checkpoint_result ==
            (mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && prepared.result_published == (expected_groups == 3)
            && prepared.outcome.action_called == called
            && !prepared.outcome.action_result
            && p.sealed.calls == calls + (called ? 1u : 0u)
            && !wl_evaluation_control_test_source_metadata_cancel_at);
        wl_evaluation_control_report_t actual;
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &actual));
        if (mode <= 1) {
            /* Genuine fresh Normal BEGIN/check/FINISH supplies the reference
             * semantics; no stopped fields are manufactured after BEGIN. */
            int owner = 0;
            wl_evaluation_control_t *reference = NULL;
            CHECK(!wl_evaluation_control_create(actual.limit, &reference)
                && !wl_evaluation_control_attach(reference, &owner)
                && !wl_evaluation_control_begin(reference, &owner));
            uint64_t id = wl_evaluation_control_active_attempt_id(reference);
            CHECK(id == actual.attempt_id && id == 1);
            if (mode) {
                wl_evaluation_control_request_cancel(reference);
                CHECK(wl_evaluation_control_charge_attempt(reference, id, 0)
                    == WL_EVALUATION_CONTROL_CANCELLED);
            } else {
                CHECK(!wl_evaluation_control_charge_attempt(reference, id,
                    test_policy.bootstrap_work)
                    && !wl_evaluation_control_charge_attempt(reference, id,
                    test_policy.current_work)
                    && !wl_evaluation_control_charge_attempt(reference, id,
                    test_policy.close_work));
            }
            CHECK(wl_evaluation_control_finish(reference, &owner, 0, 0)
                == actual.result);
            wl_evaluation_control_report_t expected;
            CHECK(!wl_evaluation_control_report(reference, &expected));
            same_report(&actual, &expected);
            CHECK(!wl_evaluation_control_detach(reference, &owner));
            wl_evaluation_control_release(reference);
        }
        if (mode) {
            CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                    bank, WL_EVALUATION_CONTROL_SOURCE_OPERATION));
            wl_evaluation_control_report_t frozen;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &frozen));
            same_report(&actual, &frozen);
        }
        /* Settlement really unbound the issuer, including early failure. */
        wl_evaluation_control_source_phase_result_t usable =
            wl_evaluation_control_source_bank_run_lexical(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
                count_action, &p.sealed.calls);
        CHECK(!usable.financial_result && usable.action_called);
    }
    for (unsigned fault = 1; fault <= 5; ++fault) {
        p.groups = 0;
        p.fail_group = fault <= 3 ? fault : 0;
        p.omit_action = fault == 4;
        p.omit_publication = fault == 5;
        unsigned calls = p.sealed.calls;
        wl_evaluation_control_source_metadata_preparation_t prepared = {0};
        CHECK(wl_evaluation_control_source_bank_run_metadata_recipe(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            metadata_recipe_prepare, &p, &prepared) == EINVAL
            && !prepared.outcome.action_called && p.sealed.calls == calls
            && prepared.entry.begun && prepared.entry.settled
            && !prepared.entry.checkpoint_result);
        CHECK(prepared.preparation_protocol == (fault <= 3 ? EINVAL : 0)
            && prepared.groups_completed == (fault <= 3 ? fault - 1 : 3)
            && p.groups == (fault <= 3 ? fault : 3));
    }
    wl_evaluation_control_source_metadata_preparation_t untouched = {0};
    untouched.outcome.action_result = 91;
    const wl_evaluation_control_source_phase_policy_t bad_policy = {0, 1, 1};
    CHECK(wl_evaluation_control_source_bank_run_metadata_recipe(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &bad_policy,
        metadata_recipe_prepare, &p, &untouched) == EINVAL
        && !untouched.groups_completed && !untouched.outcome.action_called
        && untouched.outcome.action_result == 91);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_source_bank_destroy(&p.sealed.other));
}

typedef struct {
    unsigned cursor, calls[4], fail_role;
    bool omit_advance;
} metadata_bind_probe_t;

static int
metadata_bind_role(void *root, unsigned role)
{
    metadata_bind_probe_t *p = root;
    CHECK(role < 4 && role == p->cursor);
    ++p->calls[role];
    if (p->fail_role == role + 1) return EINVAL;
    if (!p->omit_advance) p->cursor = role + 1;
    return 0;
}

static void
sealed_metadata_binding(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    for (unsigned stopped = 0; stopped <= 5; ++stopped) {
        metadata_bind_probe_t p = {0};
        wl_evaluation_control_test_source_metadata_cancel_at = stopped;
        int rc = wl_evaluation_control_source_bank_bind_metadata_roles(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
                metadata_bind_role, &p, &p.cursor);
        unsigned expected = stopped ? stopped - 1 : 4;
        CHECK(rc == (stopped ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && p.cursor == expected
            && !wl_evaluation_control_test_source_metadata_cancel_at);
        for (unsigned role = 0; role < 4; ++role)
            CHECK(p.calls[role] == (role < expected ? 1u : 0u));
        if (stopped) CHECK(
                !wl_evaluation_control_test_source_final_reset_cancel(
                    bank, WL_EVALUATION_CONTROL_SOURCE_OPERATION));
        /* Resume from the true cursor. Completed roles are never rebound,
         * including a refused zero after the fourth physical binding. */
        if (p.cursor == 4) {
            for (unsigned repeated = 0; repeated < 3; ++repeated) {
                wl_evaluation_control_source_metadata_entry_t entry = {0};
                if (repeated == 1)
                    wl_evaluation_control_test_source_metadata_cancel_at = 1;
                int observed =
                    wl_evaluation_control_source_bank_observe_metadata(
                    bank, WL_EVALUATION_CONTROL_SOURCE_OPERATION, &entry);
                CHECK(observed ==
                    (repeated == 1 ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                    && entry.begun && entry.settled
                    && entry.checkpoint_result == observed);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_test_source_early_report(bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report)
                    && !report.consumed && !report.execution_status
                    && !report.cleanup_status && report.stop_reason == observed
                    && report.result == observed);
                if (observed) CHECK(
                        !wl_evaluation_control_test_source_final_reset_cancel(
                            bank, WL_EVALUATION_CONTROL_SOURCE_OPERATION));
            }
        } else CHECK(!wl_evaluation_control_source_bank_bind_metadata_roles(
                    bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
                    metadata_bind_role, &p, &p.cursor) && p.cursor == 4);
        for (unsigned role = 0; role < 4; ++role) CHECK(p.calls[role] == 1);
    }
    for (unsigned role = 0; role < 4; ++role) {
        metadata_bind_probe_t p = {.fail_role = role + 1};
        CHECK(wl_evaluation_control_source_bank_bind_metadata_roles(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            metadata_bind_role, &p, &p.cursor) == EINVAL
            && p.cursor == role && p.calls[role] == 1);
        p.fail_role = 0;
        CHECK(!wl_evaluation_control_source_bank_bind_metadata_roles(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            metadata_bind_role, &p, &p.cursor) && p.cursor == 4);
        for (unsigned i = 0; i < 4; ++i)
            CHECK(p.calls[i] == (i == role ? 2u : 1u));
    }
    metadata_bind_probe_t invalid = {.cursor = 5};
    CHECK(wl_evaluation_control_source_bank_bind_metadata_roles(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
        metadata_bind_role, &invalid, &invalid.cursor) == EINVAL
        && invalid.cursor == 5 && !invalid.calls[0]);
    invalid.cursor = 0; invalid.omit_advance = true;
    CHECK(wl_evaluation_control_source_bank_bind_metadata_roles(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
        metadata_bind_role, &invalid, &invalid.cursor) == EINVAL
        && !invalid.cursor && invalid.calls[0] == 1 && !invalid.calls[1]);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
}

static void
sealed_metadata_start_rejections(void)
{
    for (unsigned variant = 0; variant < 21; ++variant) {
        unsigned fault = variant % 7 + 1;
        unsigned issued = variant / 7;
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        unsigned calls = 0;
        wl_evaluation_control_source_metadata_outcome_t outcome = {0};
        CHECK(!wl_evaluation_control_source_bank_run_metadata(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, &calls,
            &outcome) && outcome.action_called && calls == 1);
        wl_evaluation_control_t *foreign = NULL;
        int owner = 0;
        wl_evaluation_control_report_t frozen = {0};
        uint64_t successor = 0;
        if (fault == 5 || fault == 7) {
            CHECK(!wl_evaluation_control_create(17, &foreign)
                && !wl_evaluation_control_attach(foreign, &owner)
                && !wl_evaluation_control_begin(foreign, &owner)
                && !wl_evaluation_control_charge_attempt(foreign, 1, 7)
                && wl_evaluation_control_finish(foreign, &owner, 7, 0) == 7
                && !wl_evaluation_control_report(foreign, &frozen));
            if (fault == 5)
                CHECK(!wl_evaluation_control_test_seed_attempt_id(foreign,
                    &owner, UINT64_MAX));
            else {
                CHECK(!wl_evaluation_control_begin(foreign, &owner));
                successor = wl_evaluation_control_active_attempt_id(foreign);
                CHECK(successor == 2);
            }
        }
        wl_evaluation_control_source_metadata_probe_t probe = {0};
        CHECK(wl_evaluation_control_test_source_metadata_start_rejection(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, fault == 7 ? 5 : fault,
            foreign, issued, &probe) == (issued ? -EBUSY : EBUSY)
            && !probe.entry.begun && !probe.entry.settled
            && !probe.entry.checkpoint_result);
        CHECK(probe.before.counter == probe.after.counter
            && probe.before.active_id == probe.after.active_id
            && probe.before.consumed == probe.after.consumed
            && probe.before.lane_id == probe.after.lane_id
            && probe.before.charged == probe.after.charged
            && probe.before.verified == probe.after.verified);
        same_report(&probe.before.report, &probe.after.report);
        CHECK(probe.before.counter == (fault == 5 ? UINT64_MAX :
            fault == 3 ? 8 : fault == 7 ? 2 : 7));
        if (foreign) {
            wl_evaluation_control_report_t after;
            CHECK(!wl_evaluation_control_report(foreign, &after));
            same_report(&frozen, &after);
            if (successor) {
                uint64_t used = UINT64_MAX;
                CHECK(wl_evaluation_control_active_attempt_id(foreign) ==
                    successor
                    && !wl_evaluation_control_test_active_consumed(foreign,
                    successor, &used) && !used);
                CHECK(!wl_evaluation_control_finish(foreign, &owner, 0, 0));
            }
            CHECK(!wl_evaluation_control_detach(foreign, &owner));
            wl_evaluation_control_release(foreign);
        }
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
    }
}

typedef struct {
    wl_evaluation_control_source_context_t *context;
    metadata_recipe_probe_t *probe;
    int result;
} issued_foreign_t;

static void *
issued_foreign_continue(void *payload)
{
    issued_foreign_t *p = payload;
    /* A real other thread cannot begin over the still-live issued context.
     * Its refused output stays untouched and the owning thread can resume. */
    wl_evaluation_control_source_context_t competing = {0};
    bool close = p->context->lane == WL_EVALUATION_CONTROL_SOURCE_CLOSE;
    int entered = close ?
        wl_evaluation_control_source_metadata_enter_close_owned(
        p->context->bank,
        &competing) : wl_evaluation_control_source_metadata_enter_issued(
        p->context->bank, &competing);
    CHECK(entered == -EBUSY && !competing.identity && !competing.bank
        && !competing.thread_cookie);
    wl_evaluation_control_source_metadata_preparation_t prepared = {0};
    p->result = close ?
        wl_evaluation_control_source_metadata_continue_close_owned(
        p->context, &test_policy, metadata_recipe_prepare, p->probe,
        &prepared) :
        wl_evaluation_control_source_metadata_continue_issued(
        p->context, &test_policy, metadata_recipe_prepare, p->probe, &prepared);
    CHECK(!prepared.entry.begun && !prepared.entry.settled
        && !prepared.outcome.action_called);
    return NULL;
}

/* Isolated Core financial protocol, not an operator/Native issuer. Context
 * copies and foreign-thread attempts are intentional invalid borrowers. */
static void
sealed_issued_metadata(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original, bool close)
{
    wl_evaluation_control_source_lane_t lane = close ?
        WL_EVALUATION_CONTROL_SOURCE_CLOSE :
        WL_EVALUATION_CONTROL_SOURCE_OPERATION;
    int (*enter_owned)(wl_evaluation_control_source_bank_t *,
        wl_evaluation_control_source_context_t *) = close ?
        wl_evaluation_control_source_metadata_enter_close_owned :
        wl_evaluation_control_source_metadata_enter_issued;
    int (*continue_owned)(wl_evaluation_control_source_context_t *,
        const wl_evaluation_control_source_phase_policy_t *,
        wl_evaluation_control_source_metadata_prepare_fn, const void *,
        wl_evaluation_control_source_metadata_preparation_t *) = close ?
        wl_evaluation_control_source_metadata_continue_close_owned :
        wl_evaluation_control_source_metadata_continue_issued;
    metadata_recipe_probe_t p = {.sealed = {.bank = bank,
                                            .original = original}};
    CHECK(!wl_evaluation_control_source_bank_create(&p.sealed.other));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    for (unsigned mode = 0; mode < 7; ++mode) {
        p.groups = 0;
        unsigned calls = p.sealed.calls;
        wl_evaluation_control_source_context_t context = {0};
        wl_evaluation_control_source_metadata_preparation_t prepared = {0};
        if (mode == 1) wl_evaluation_control_test_source_metadata_cancel_at = 1;
        int entered = enter_owned(bank,
                &context);
        if (mode == 1) {
            CHECK(entered ==
                WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED
                && !context.identity && !context.bank && !context.thread_cookie
                && !prepared.outcome.action_called && !p.groups);
            /* Sticky actual cancellation, without a production reset. */
            wl_evaluation_control_source_context_t again = {0};
            CHECK(enter_owned(bank,
                &again)
                == WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED
                && !again.identity && !again.bank && !again.thread_cookie);
        } else {
            CHECK(entered == WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN
                && context.identity == &context && context.bank == bank
                && context.lane == lane
                && context.thread_cookie);
            int wrong_lane = close ?
                wl_evaluation_control_source_metadata_continue_issued(&context,
                    &test_policy, metadata_recipe_prepare, &p, &prepared) :
                wl_evaluation_control_source_metadata_continue_close_owned(
                &context, &test_policy, metadata_recipe_prepare, &p, &prepared);
            CHECK(wrong_lane == EINVAL && context.identity == &context
                && !prepared.entry.begun && !prepared.entry.settled &&
                !p.groups);
            wl_evaluation_control_source_context_t another = {0};
            CHECK(enter_owned(bank,
                &another)
                == -EBUSY && !another.identity && !another.bank);
            wl_evaluation_control_source_context_t copied = context;
            CHECK(continue_owned(&copied,
                &test_policy, metadata_recipe_prepare, &p,
                &prepared) == EINVAL);
            copied.identity = &copied;
            CHECK(continue_owned(&copied,
                &test_policy, metadata_recipe_prepare, &p, &prepared) == EBUSY);
            issued_foreign_t foreign = {&context, &p, 0}; wl_thread_t thread;
            CHECK(!wl_thread_create(&thread, issued_foreign_continue,
                &foreign));
            CHECK(!wl_thread_join(&thread) && foreign.result == EINVAL
                && context.identity == &context && !p.groups
                && p.sealed.calls == calls);
            if (mode >= 2 && mode <= 4)
                wl_evaluation_control_test_source_metadata_cancel_at = mode;
            if (mode == 5)
                wl_evaluation_control_test_source_lexical_cancel_before_action =
                    true;
            if (mode == 6)
                wl_evaluation_control_test_source_lexical_cancel_after_action =
                    true;
            int rc =
                continue_owned(&context,
                    &test_policy, metadata_recipe_prepare, &p, &prepared);
            unsigned groups = mode >= 2 && mode <= 4 ? mode - 1 : 3;
            CHECK(rc == (mode ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                && !context.identity && prepared.entry.begun
                && prepared.entry.settled && !prepared.entry.checkpoint_result
                && p.groups == groups && prepared.groups_completed == groups
                && prepared.outcome.action_called == (!mode || mode == 6)
                && p.sealed.calls == calls + (!mode || mode == 6 ? 1u : 0u));
            CHECK(continue_owned(
                    &context,
                    &test_policy, metadata_recipe_prepare, &p,
                    &prepared) == EINVAL);
        }
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            lane, &report));
        CHECK(report.stop_reason == (mode ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && report.result == (mode ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && !report.execution_status && !report.cleanup_status);
        if (mode <= 1) {
            int owner = 0; wl_evaluation_control_t *reference = NULL;
            CHECK(!wl_evaluation_control_create(report.limit, &reference)
                && !wl_evaluation_control_attach(reference, &owner)
                && !wl_evaluation_control_begin(reference, &owner));
            if (mode) {
                wl_evaluation_control_request_cancel(reference);
                CHECK(wl_evaluation_control_charge_attempt(reference, 1, 0)
                    == WL_EVALUATION_CONTROL_CANCELLED && !report.consumed);
            } else CHECK(!wl_evaluation_control_charge_attempt(reference, 1,
                    256)
                    && !wl_evaluation_control_charge_attempt(reference, 1, 256)
                    && !wl_evaluation_control_charge_attempt(reference, 1,
                    256));
            CHECK(wl_evaluation_control_finish(reference, &owner, 0,
                0) == report.result);
            wl_evaluation_control_report_t expected;
            CHECK(!wl_evaluation_control_report(reference, &expected));
            same_report(&report, &expected);
            CHECK(!wl_evaluation_control_detach(reference, &owner));
            wl_evaluation_control_release(reference);
        }
        if (mode) {
            CHECK(!wl_evaluation_control_test_source_final_reset_cancel(bank,
                lane));
            wl_evaluation_control_report_t frozen;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                lane, &frozen));
            same_report(&report, &frozen);
        }
    }
    for (unsigned fault = 0; fault < 6; ++fault) {
        p.groups = 0;
        wl_evaluation_control_source_context_t context = {0};
        CHECK(enter_owned(bank, &context)
            == WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN);
        wl_evaluation_control_source_metadata_preparation_t prepared = {0};
        wl_evaluation_control_source_phase_policy_t policy = test_policy;
        if (fault == 4) policy.bootstrap_work = 0;
        if (fault == 5) policy.current_work = 257;
        CHECK(continue_owned(&context,
            fault == 0 ? NULL : &policy,
            fault == 1 ? NULL : metadata_recipe_prepare,
            fault == 2 ? NULL : &p,
            fault == 3 ? NULL : &prepared) == EINVAL
            && !context.identity && !p.groups &&
            !prepared.outcome.action_called);
        if (fault != 3) CHECK(prepared.entry.begun && prepared.entry.settled);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            lane, &report)
            && !report.consumed && !report.stop_reason && !report.result);
    }
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_source_bank_destroy(&p.sealed.other));
}

typedef struct {
    unsigned cursor, groups, calls, fault;
} issued_bind_probe_t;

static int
issued_bind_prepare(void *root, unsigned group,
    wl_evaluation_control_source_metadata_bind_outcome_t *out)
{
    issued_bind_probe_t *p = root;
    CHECK(group == p->groups && group < 2);
    ++p->groups;
    if (p->fault == group + 1) return EINVAL;
    if (!group) {
        out->cursor = &p->cursor; out->before_cursor = p->cursor;
    }
    return 0;
}

static int
issued_bind_one(void *root, unsigned cursor)
{
    issued_bind_probe_t *p = root;
    CHECK(cursor == p->cursor && cursor < 4);
    ++p->calls; ++p->cursor;
    return 0;
}

static void
issued_bind_cases(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    for (unsigned cursor = 0; cursor <= 4; ++cursor) {
        for (unsigned stop = 0; stop <= 4; ++stop) {
            issued_bind_probe_t p = {.cursor = cursor};
            wl_evaluation_control_source_context_t context = {0};
            if (stop == 1)
                wl_evaluation_control_test_source_metadata_cancel_at = 1;
            int entered = wl_evaluation_control_source_metadata_enter_issued(
                bank, &context);
            bool called = cursor < 4 && (!stop || stop == 4);
            bool cancelled = stop && (cursor < 4 || stop < 4);
            if (stop == 1) {
                CHECK(entered ==
                    WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED
                    && !context.identity && !p.groups && !p.calls);
            } else {
                CHECK(entered == WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN);
                wl_evaluation_control_source_metadata_bind_outcome_t out = {0};
                wl_evaluation_control_source_context_t copy = context;
                CHECK(
                    wl_evaluation_control_source_metadata_bind_continue_issued(
                        &copy, issued_bind_prepare, issued_bind_one, &p, &out)
                    == EINVAL && !p.groups && !out.settled);
                copy.identity = &copy;
                CHECK(
                    wl_evaluation_control_source_metadata_bind_continue_issued(
                        &copy, issued_bind_prepare, issued_bind_one, &p, &out)
                    == EBUSY && !p.groups && !out.settled);
                if (stop > 1)
                    wl_evaluation_control_test_source_metadata_cancel_at = stop;
                int rc =
                    wl_evaluation_control_source_metadata_bind_continue_issued(
                    &context, issued_bind_prepare, issued_bind_one, &p, &out);
                CHECK(rc == (cancelled ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                    && !context.identity && out.settled
                    && out.role_called == called
                    && out.complete_selected == (cursor == 4 && !cancelled)
                    && p.calls == (called ? 1u : 0u)
                    && p.cursor == cursor + (called ? 1u : 0u));
                if (cursor == 4 && stop == 4)
                    wl_evaluation_control_test_source_metadata_cancel_at = 0;
            }
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report)
                && report.consumed == (called ? 10u : 0u)
                && report.result ==
                (cancelled ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                && !report.execution_status && !report.cleanup_status);
            if (cancelled)
                CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                        bank, WL_EVALUATION_CONTROL_SOURCE_OPERATION));
        }
    }
    for (unsigned fault = 0; fault < 6; ++fault) {
        issued_bind_probe_t p = {.fault = fault};
        wl_evaluation_control_source_context_t context;
        wl_evaluation_control_source_metadata_bind_outcome_t out;
        CHECK(!wl_evaluation_control_source_metadata_enter_issued(bank,
            &context));
        CHECK(wl_evaluation_control_source_metadata_bind_continue_issued(
                &context, fault == 3 ? NULL : issued_bind_prepare,
                fault == 4 ? NULL : issued_bind_one, fault == 5 ? NULL : &p,
                fault == 0 ? NULL : &out) == EINVAL && !context.identity
            && !p.calls);
        if (fault) CHECK(out.settled);
    }
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
}

/* Fixed observation is a financial component; it creates no operator/Native
 * owner and never calls a supplied callback. Stops use the real cancel flag. */
static void
fixed_observation(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t frozen, after;
    CHECK(!wl_evaluation_control_report(original, &frozen));
    for (unsigned stop = 0; stop <= 3; ++stop) {
        wl_evaluation_control_test_source_metadata_cancel_at = stop;
        CHECK(wl_evaluation_control_source_operation_observe_owned(bank)
            == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0));
        CHECK(!wl_evaluation_control_test_source_metadata_cancel_at);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report)
            && report.attempt_id == 1 && report.limit == 768
            && report.consumed == (stop == 1 || stop == 2 ? 0 : 10)
            && report.result == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && report.stop_reason == report.result
            && !report.execution_status && !report.cleanup_status);
        CHECK(!wl_evaluation_control_report(original, &after));
        same_report(&frozen, &after);
        if (stop) {
            CHECK(!wl_evaluation_control_test_source_final_reset_cancel(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION));
            wl_evaluation_control_report_t preserved;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &preserved));
            same_report(&report, &preserved);
        }
    }
    /* A real other owner is active: refusal neither closes nor replaces it. */
    wl_evaluation_control_source_context_t context;
    CHECK(wl_evaluation_control_source_metadata_enter_issued(bank, &context)
        == WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN);
    CHECK(wl_evaluation_control_source_operation_observe_owned(bank) == EBUSY
        && context.identity == &context);
    wl_evaluation_control_source_metadata_preparation_t prepared = {0};
    CHECK(wl_evaluation_control_source_metadata_continue_issued(&context,
        NULL, NULL, NULL, &prepared) == EINVAL
        && !context.identity && prepared.entry.settled);
    CHECK(!wl_evaluation_control_source_operation_observe_owned(bank));
}

typedef struct { unsigned groups, calls; } issued_count_t;
static int
issued_count_prepare(const void *recipe, unsigned group,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    issued_count_t *p = (issued_count_t *)recipe;
    CHECK(group == p->groups++ && group < 3);
    if (group == 2) {
        prepared->action = count_action; prepared->payload = &p->calls;
        prepared->result_published = true;
    }
    return 0;
}

static void
lexical_lifetime(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    unsigned calls = 0;
    wl_evaluation_control_source_context_t borrowed = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &borrowed));
    wl_evaluation_control_source_phase_result_t r =
        wl_evaluation_control_source_bank_run_lexical(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result == EBUSY && !r.action_called && !calls);
    CHECK(!wl_evaluation_control_source_context_end(&borrowed));
    const wl_evaluation_control_source_phase_policy_t invalid[] = {
        {0, 1, 1}, {1, 0, 1}, {1, 1, 0},
        {WL_EVALUATION_CONTROL_QUANTUM + 1, 1, 1},
        {1, WL_EVALUATION_CONTROL_QUANTUM + 1, 1},
        {1, 1, WL_EVALUATION_CONTROL_QUANTUM + 1}
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
        r = wl_evaluation_control_source_bank_run_lexical(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &invalid[i],
                count_action, &calls);
        CHECK(r.financial_result == EINVAL && !r.action_called && !calls);
    }
    negative_lexical_bank = bank;
    r = wl_evaluation_control_source_bank_run_lexical(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            lexical_reentry, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result && !calls);
    negative_lexical_bank = NULL;
    for (unsigned lane = 0; lane < 2; ++lane) {
        lexical_birth_t birth = {0};
        r = wl_evaluation_control_source_bank_run_lexical(bank,
                (wl_evaluation_control_source_lane_t)lane, &test_policy,
                lexical_birth, &birth);
        CHECK(!r.financial_result && r.action_called && !r.action_result
            && birth.control && birth.id);
        CHECK(!wl_evaluation_control_charge_attempt(birth.control, birth.id,
            1));
        CHECK(!wl_evaluation_control_finish(birth.control, &birth, 0, 0));
        CHECK(!wl_evaluation_control_detach(birth.control, &birth));
        wl_evaluation_control_release(birth.control);
        /* Each lexical call released its real lane association, so an ordinary
         * context can immediately acquire it and complete its own lifetime. */
        CHECK(!wl_evaluation_control_source_context_begin(bank,
            (wl_evaluation_control_source_lane_t)lane, &borrowed));
        CHECK(!wl_evaluation_control_source_context_end(&borrowed));
    }
    for (unsigned i = 0; i < 4096; ++i) {
        r = wl_evaluation_control_source_bank_run_lexical(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
                count_action, &calls);
        CHECK(!r.financial_result && r.action_called && !r.action_result);
    }
    CHECK(calls == 4096);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
}

static void
constructor_peer_claim(void)
{
    wl_evaluation_control_test_finance_birth_claim = NULL;
    wl_evaluation_control_t *peer = NULL;
    CHECK(!wl_evaluation_control_create(1, &peer) && peer);
    wl_evaluation_control_release(peer);
}

static int
bounded_constructor(void *payload)
{
    lexical_birth_t *birth = payload;
    wl_evaluation_control_lexical_finance_birth_result_t result =
        wl_evaluation_control_lexical_finance_birth_try(128, birth);
    if (result.protocol) return result.protocol;
    birth->control = result.control; birth->id = result.id;
    return 0;
}

/* Negative nested issuer: no BANK loan is part of the normal callback. */
static int
constructor_reentry(void *payload)
{
    wl_evaluation_control_source_phase_result_t result =
        wl_evaluation_control_source_bank_run_lexical_constructor(
        negative_lexical_bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
        count_action, payload);
    CHECK(result.financial_result == EBUSY && !result.action_called);
    return 0;
}

static void
constructor_lifetime(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t saved, after;
    CHECK(!wl_evaluation_control_report(original, &saved));
    unsigned calls = 0;
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    wl_evaluation_control_source_phase_result_t r =
        wl_evaluation_control_source_bank_run_lexical_constructor(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result == EBUSY && !r.action_called && !calls);
    CHECK(!wl_evaluation_control_source_context_end(&context));
    const wl_evaluation_control_source_phase_policy_t invalid[] = {
        {0, 1, 1}, {1, 0, 1}, {1, 1, 0},
        {WL_EVALUATION_CONTROL_QUANTUM + 1, 1, 1},
        {1, WL_EVALUATION_CONTROL_QUANTUM + 1, 1},
        {1, 1, WL_EVALUATION_CONTROL_QUANTUM + 1}
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
        r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &invalid[i],
                count_action, &calls);
        CHECK(r.financial_result == EINVAL && !r.action_called && !calls);
    }
    negative_lexical_bank = bank;
    r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            constructor_reentry, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result && !calls);
    negative_lexical_bank = NULL;
    for (unsigned lane = 0; lane < 2; ++lane) {
        for (unsigned fault = 0; fault < 4; ++fault) {
            lexical_birth_t birth = {0};
            if (fault == 1)
                wl_evaluation_control_test_finance_birth_claim =
                    constructor_peer_claim;
            else if (fault) wl_evaluation_control_test_finance_birth_fail =
                    fault - 1;
            r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
                    (wl_evaluation_control_source_lane_t)lane, &test_policy,
                    bounded_constructor, &birth);
            CHECK(!r.financial_result && r.action_called
                && r.action_result == (fault == 1 ? EAGAIN : fault ? ENOMEM : 0)
                && !wl_evaluation_control_test_finance_birth_claim
                && !wl_evaluation_control_test_finance_birth_fail);
            if (fault) CHECK(!birth.control && !birth.id);
            else {
                CHECK(birth.control && birth.id
                    && !wl_evaluation_control_charge_attempt(birth.control,
                    birth.id, 5));
                CHECK(!wl_evaluation_control_finish(birth.control, &birth, 0,
                    0));
                CHECK(!wl_evaluation_control_detach(birth.control, &birth));
                wl_evaluation_control_release(birth.control);
            }
            /* The constructor's actual completed financial history and
             * verified lane must permit the ordinary subsequent footer. */
            calls = 0;
            r = wl_evaluation_control_source_bank_run_lexical(bank,
                    (wl_evaluation_control_source_lane_t)lane, &test_policy,
                    count_action, &calls);
            CHECK(!r.financial_result && r.action_called && !r.action_result &&
                calls == 1);
            CHECK(!wl_evaluation_control_report(original, &after));
            same_report(&saved, &after);
        }
    }
}

static void
public_birth_and_partial_failure(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(9, &control));
    int owner;
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(!wl_evaluation_control_charge_attempt(control, 1, 3));
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(control, &before));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) && session);
    CHECK(!wl_session_try_destroy(&session) && !session);
    CHECK(!wl_evaluation_control_report(control, &after));
    same_report(&before, &after);

    /* The first real private lane is created, the second genuinely exhausts
     * the global constructor identity. No phase/token/owner is fabricated. */
    CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1));
    CHECK(wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) == EOVERFLOW && !session);
    CHECK(!wl_evaluation_control_report(control, &after));
    same_report(&before, &after);
    options.spill_quota_bytes = 1;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) == EINVAL && !session);
    CHECK(!wl_evaluation_control_report(control, &after));
    same_report(&before, &after);
    /* Both successful retirement and partial birth rollback released only
     * their genuine attachment. The original remains reusable and monotone. */
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(wl_evaluation_control_active_attempt_id(control) == 2);
    CHECK(!wl_evaluation_control_charge_attempt(control, 2, 2));
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

typedef struct {
    wl_evaluation_control_source_bank_t *bank;
    unsigned first, between, second;
    int first_error, second_error;
} pair_fixture_t;

static pair_fixture_t *observed_pair;

static void
pair_checkpoint(void)
{
    CHECK(observed_pair && observed_pair->first == 1 && !observed_pair->second);
    ++observed_pair->between;
}

static int
pair_second(void *payload)
{
    pair_fixture_t *p = payload;
    CHECK(p->first == 1 && p->between == 1 && !p->second);
    ++p->second;
    return p->second_error;
}

static int
pair_first(void *payload)
{
    pair_fixture_t *p = payload;
    CHECK(!p->first && !p->between && !p->second);
    ++p->first;
    const wl_evaluation_control_source_pair_policy_t minimal_policy = {1, 1, 1,
                                                                       1};
    wl_evaluation_control_source_pair_result_t nested =
        wl_evaluation_control_source_bank_run_lexical_pair(p->bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &minimal_policy,
            pair_first, p, pair_second, p);
    CHECK(nested.financial_result == EBUSY && !nested.first_called &&
        !nested.second_called);
    return p->first_error;
}

static void
lexical_pair(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    for (unsigned mode = 0; mode < 3; ++mode) {
        pair_fixture_t p = {bank, 0, 0, 0, mode == 1 ? EIO : 0,
                            mode == 2 ? ENOMEM : 0};
        observed_pair = &p;
        wl_evaluation_control_test_source_pair_checkpoint = pair_checkpoint;
        /* Actual four-payment finite phase, exactly the old 3Q capacity.
         * Test prices are not a source-distance or production tariff proof. */
        const wl_evaluation_control_source_pair_policy_t policy = {256, 256,
                                                                   255, 1};
        wl_evaluation_control_source_pair_result_t r =
            wl_evaluation_control_source_bank_run_lexical_pair(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                pair_first, &p, pair_second, &p);
        CHECK(!r.financial_result && r.first_called &&
            r.first_result == p.first_error
            && r.second_called == (mode != 1) &&
            r.second_result == (mode == 2 ? ENOMEM : 0)
            && p.first == 1 && p.between == 1 && p.second == (mode != 1));
        wl_evaluation_control_test_source_pair_checkpoint = NULL;
        observed_pair = NULL;
    }
    unsigned calls = 0;
    const wl_evaluation_control_source_pair_policy_t invalid[] = {
        {256, 256, 256, 256}, {0, 1, 1, 1}, {1, 0, 1, 1},
        {1, 1, 0, 1}, {1, 1, 1, 0}, {UINT64_MAX, 1, 1, 1},
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
        wl_evaluation_control_source_pair_result_t r =
            wl_evaluation_control_source_bank_run_lexical_pair(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &invalid[i],
                count_action, &calls, count_action, &calls);
        CHECK(r.financial_result == EINVAL && !r.first_called &&
            !r.second_called && !calls);
    }
    const wl_evaluation_control_source_pair_policy_t minimal_policy = {1, 1, 1,
                                                                       1};
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    wl_evaluation_control_source_pair_result_t busy =
        wl_evaluation_control_source_bank_run_lexical_pair(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &minimal_policy,
            count_action, &calls, count_action, &calls);
    CHECK(busy.financial_result == EBUSY && !busy.first_called &&
        !busy.second_called && !calls);
    CHECK(!wl_evaluation_control_source_context_end(&context));
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
}

typedef struct {
    unsigned *journal;
    unsigned calls;
    int error;
} final_metadata_t;

static const void *expected_final_metadata_payload;
static bool early_nested_enabled;

static void
early_nested_check(void)
{
    if (!early_nested_enabled) return;
    unsigned session = 0, payload = 0, journal = 0;
    wl_evaluation_control_source_bank_t *bank = negative_lexical_bank;
    CHECK(bank);
    const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
    const wl_evaluation_control_source_pair_policy_t pair_policy = {64, 64, 64,
                                                                    64};
    const wl_evaluation_control_source_final_geometry_t geometry = {
        {&session, sizeof(session)}, {&payload, sizeof(payload)},
        {&journal, sizeof(journal)}
    };
    const wl_evaluation_control_source_final_pair_geometry_t pair_geometry =
    {0};
    wl_evaluation_control_source_early_result_t outcomes[5] = {
        wl_evaluation_control_source_bank_run_lexical_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, count_action,
            &payload),
        wl_evaluation_control_source_bank_run_lexical_constructor_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, count_action,
            &payload),
        wl_evaluation_control_source_bank_run_lexical_pair_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &pair_policy,
            count_action, &payload, count_action, &journal),
        wl_evaluation_control_source_bank_run_lexical_final_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &geometry,
            count_action),
        wl_evaluation_control_source_bank_run_lexical_pair_final_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &pair_policy,
            &pair_geometry,
            count_action, count_action)
    };
    for (unsigned i = 0; i < 5; ++i)
        CHECK(outcomes[i].protocol == EBUSY &&
            outcomes[i].financial_result == EBUSY
            && !outcomes[i].phase_started && !outcomes[i].phase_finished
            && !outcomes[i].first_called && !outcomes[i].second_called);
    wl_evaluation_control_report_t report = {.attempt_id = 47, .result = EIO};
    unsigned char saved[sizeof(report)]; memcpy(saved, &report, sizeof(report));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report) == EBUSY
        && !memcmp(saved, &report,
        sizeof(report)) && !payload && !journal && !session);
}

static void
early_expected_report(wl_evaluation_control_source_bank_t *bank, uint64_t work,
    int stop)
{
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report));
    CHECK(report.attempt_id == 1 &&
        report.limit == 3 * WL_EVALUATION_CONTROL_QUANTUM
        && report.consumed == work && report.stop_reason == stop
        && !report.execution_status && !report.cleanup_status &&
        report.result == stop);
}

static int
final_metadata(void *payload)
{
    CHECK(!expected_final_metadata_payload ||
        payload == expected_final_metadata_payload);
    final_metadata_t *p = payload;
    early_nested_check();
    ++p->calls;
    ++*p->journal;
    return p->error;
}

static void
final_metadata_geometry(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    for (unsigned mode = 0; mode < 8; ++mode) {
        unsigned session = 0, journal = 0;
        final_metadata_t payload = {&journal, 0, mode == 1 ? EIO : 0};
        const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
        wl_evaluation_control_source_final_geometry_t geometry = {
            {&session, sizeof(session)}, {&payload, sizeof(payload)},
            {&journal, sizeof(journal)}
        };
        if (mode == 2) geometry.payload.base = &journal;
        if (mode == 3) geometry.payload.bytes = 0;
        if (mode == 4) geometry.payload.bytes = SIZE_MAX;
        if (mode == 5) geometry.payload.base = (const unsigned char *)bank + 1;
        if (mode == 6) geometry.journal = geometry.session;
        if (mode ==
            7) geometry.payload = (wl_evaluation_control_source_extent_t){
                &geometry, sizeof(geometry)
            };
        expected_final_metadata_payload = &payload;
        wl_evaluation_control_source_final_result_t result =
            wl_evaluation_control_source_bank_run_lexical_final(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &geometry,
                final_metadata);
        CHECK(result.phase_started && result.phase_finished &&
            !result.financial_result);
        if (mode <= 1)
            CHECK(result.action_called && result.action_result == payload.error
                && payload.calls == 1 && journal == 1);
        else CHECK(!result.action_called && result.action_result == EINVAL
                && !payload.calls && !journal && !session);
    }
    for (unsigned fault = 1; fault <= 2; ++fault) {
        unsigned session = 0, journal = 0;
        final_metadata_t payload = {&journal, 0, 0};
        const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
        const wl_evaluation_control_source_final_geometry_t geometry = {
            {&session, sizeof(session)}, {&payload, sizeof(payload)},
            {&journal, sizeof(journal)}
        };
        expected_final_metadata_payload = &payload;
        wl_evaluation_control_test_source_final_geometry_fault = fault;
        wl_evaluation_control_source_final_result_t result =
            wl_evaluation_control_source_bank_run_lexical_final(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &geometry,
                final_metadata);
        CHECK(result.phase_started && result.phase_finished &&
            !result.financial_result
            && !result.action_called && result.action_result == EINVAL &&
            !payload.calls
            && !journal && !session &&
            !wl_evaluation_control_test_source_final_geometry_fault);
    }
    struct {
        final_metadata_t payload;
        wl_evaluation_control_source_phase_policy_t policy;
    } embedded = {{0}, {64, 64, 64}};
    unsigned policy_session = 0, policy_journal = 0;
    embedded.payload.journal = &policy_journal;
    const wl_evaluation_control_source_final_geometry_t policy_geometry = {
        {&policy_session, sizeof(policy_session)},
        {&embedded, sizeof(embedded)},
        {&policy_journal, sizeof(policy_journal)}
    };
    expected_final_metadata_payload = &embedded.payload;
    wl_evaluation_control_source_final_result_t policy_result =
        wl_evaluation_control_source_bank_run_lexical_final(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &embedded.policy,
            &policy_geometry, final_metadata);
    CHECK(policy_result.phase_started && policy_result.phase_finished
        && !policy_result.financial_result && !policy_result.action_called
        && policy_result.action_result == EINVAL && !embedded.payload.calls &&
        !policy_journal);
    expected_final_metadata_payload = NULL;
    /* Adjoining actual objects are disjoint; no strict '<=' overlap test. */
    struct { unsigned session, journal; } adjacent = {0};
    final_metadata_t payload = {&adjacent.journal, 0, 0};
    const wl_evaluation_control_source_phase_policy_t policy = {1, 1, 1};
    const wl_evaluation_control_source_final_geometry_t geometry = {
        {&adjacent.session, sizeof(adjacent.session)},
        {&payload, sizeof(payload)},
        {&adjacent.journal, sizeof(adjacent.journal)}
    };
    wl_evaluation_control_source_final_result_t result =
        wl_evaluation_control_source_bank_run_lexical_final(bank,
            WL_EVALUATION_CONTROL_SOURCE_CLOSE, &policy, &geometry,
            final_metadata);
    CHECK(result.phase_started && result.phase_finished && result.action_called
        && !result.financial_result && !result.action_result &&
        payload.calls == 1
        && adjacent.journal == 1 && !adjacent.session);
    for (unsigned after = 0; after < 2; ++after) {
        unsigned session = 0, journal = 0;
        final_metadata_t p = {&journal, 0, 0};
        const wl_evaluation_control_source_final_geometry_t g = {
            {&session, sizeof(session)}, {&p, sizeof(p)},
            {&journal, sizeof(journal)}
        };
        wl_evaluation_control_test_source_final_cancel_at = after
            ? WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION
            : WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN;
        result = wl_evaluation_control_source_bank_run_lexical_final(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &g,
                final_metadata);
        CHECK(result.phase_started && result.phase_finished
            && result.financial_result == WL_EVALUATION_CONTROL_CANCELLED
            && result.action_called == (after != 0) && !result.action_result
            && p.calls == after && journal == after && !session
            && !wl_evaluation_control_test_source_final_cancel_at);
        CHECK(!wl_evaluation_control_test_source_final_reset_cancel(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION));
    }
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
}

typedef struct {
    wl_evaluation_control_lexical_finance_birth_result_t born;
    wl_evaluation_control_lexical_finance_expectation_t expected;
    unsigned calls;
    int error;
} early_pair_scope_t;

static int
early_pair_close(void *payload)
{
    early_pair_scope_t *scope = payload;
    early_nested_check();
    const wl_evaluation_control_lexical_finance_expectation_t expected =
        scope->expected;
    wl_evaluation_control_lexical_finance_result_t closed =
        wl_evaluation_control_lexical_finance_close_checked(scope->born.control,
            &expected);
    CHECK(!closed.protocol && closed.closed && closed.report_valid &&
        !closed.result);
    scope->born.control = NULL; ++scope->calls;
    return scope->error;
}

static void
early_phase_cases(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
    const wl_evaluation_control_source_pair_policy_t pair_policy = {64, 64, 64,
                                                                    64};
    for (unsigned mode = 0; mode < 5; ++mode)
        for (unsigned fault = 0; fault < 7; ++fault) {
            if ((fault == 3 && mode < 3) ||
                ((fault == 4 || fault == 5) && mode != 4)) continue;
            unsigned session = 0, journal = 0, second_calls = 0, record = 0;
            final_metadata_t payload = {&journal, 0, fault == 2 ? EIO : 0};
            expected_final_metadata_payload = &payload;
            wl_evaluation_control_source_final_geometry_t geometry = {
                {&session, sizeof(session)}, {&payload, sizeof(payload)},
                {&journal, sizeof(journal)}
            };
            early_pair_scope_t scope = {.error = fault == 5 ? EIO : 0};
            if (mode == 4) {
                scope.born =
                    wl_evaluation_control_lexical_finance_birth_try(672,
                        &record);
                CHECK(!scope.born.protocol && scope.born.control);
                scope.expected =
                    (wl_evaluation_control_lexical_finance_expectation_t){
                    .owner = &record, .scope = &scope,
                    .owner_bytes = sizeof(record), .scope_bytes = sizeof(scope),
                    .identity = scope.born.identity, .id = scope.born.id,
                    .limit = 672, .active = true
                };
            }
            wl_evaluation_control_source_final_pair_geometry_t pair_geometry = {
                {&session, sizeof(session)}, {&record, sizeof(record)},
                {&scope, sizeof(scope)}, {&payload, sizeof(payload)},
                {&journal, sizeof(journal)},
                scope.born.control, scope.expected
            };
            if (fault == 3) {
                geometry.payload = geometry.journal;
                pair_geometry.payload = pair_geometry.journal;
            }
            if (fault == 4) wl_evaluation_control_test_source_final_cancel_at =
                    WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_FIRST;
            if (fault ==
                1) wl_evaluation_control_test_source_early_cancel_once = true;
            negative_lexical_bank = bank; early_nested_enabled = true;
            wl_evaluation_control_report_t before_ref, after_ref;
            if (fault == 6) {
                unsigned prime = 0;
                CHECK(wl_evaluation_control_source_bank_run_lexical_early(bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                    count_action, &prime).first_called
                    && prime == 1);
                CHECK(!wl_evaluation_control_test_source_early_report(bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION, &before_ref));
                wl_evaluation_control_test_source_early_rearm_ref_once = true;
            }
            wl_evaluation_control_source_early_result_t result;
            for (unsigned retry = 0; retry < (fault == 6 ? 2u : 1u); ++retry) {
                if (!mode) result =
                        wl_evaluation_control_source_bank_run_lexical_early(
                        bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                        final_metadata, &payload);
                else if (mode ==
                    1) result =
                        wl_evaluation_control_source_bank_run_lexical_constructor_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                            final_metadata, &payload);
                else if (mode ==
                    2) result =
                        wl_evaluation_control_source_bank_run_lexical_pair_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION,
                            &pair_policy, final_metadata, &payload,
                            count_action, &second_calls);
                else if (mode ==
                    3) result =
                        wl_evaluation_control_source_bank_run_lexical_final_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                            &geometry, final_metadata);
                else result =
                        wl_evaluation_control_source_bank_run_lexical_pair_final_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION,
                            &pair_policy, &pair_geometry,
                            early_pair_close, final_metadata);
                if (fault == 6 && !retry) {
                    CHECK(result.protocol == EBUSY &&
                        result.financial_result == EBUSY
                        && !result.begin_protocol && !result.checkpoint_result
                        && !result.phase_started && !result.phase_finished
                        && !result.first_called && !result.second_called
                        && !payload.calls && !journal && !scope.calls &&
                        !second_calls
                        && !
                        wl_evaluation_control_test_source_early_rearm_ref_once);
                    CHECK(!wl_evaluation_control_test_source_early_report(bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after_ref));
                    same_report(&before_ref, &after_ref);
                }
            }
            early_nested_enabled = false; negative_lexical_bank = NULL;
            CHECK(!result.protocol && !result.begin_protocol
                && result.phase_started && result.phase_finished);
            early_expected_report(bank,
                fault == 1 ? 0 : fault == 5 ? 192 : (fault == 3 ||
                fault == 4) ? 128
                : mode == 4 || (mode == 2 && fault != 2) ? 256 : 192,
                fault == 1 || fault == 4 ? WL_EVALUATION_CONTROL_CANCELLED : 0);
            if (fault == 1) {
                CHECK(result.checkpoint_result ==
                    WL_EVALUATION_CONTROL_CANCELLED
                    && result.financial_result ==
                    WL_EVALUATION_CONTROL_CANCELLED
                    && !result.first_called && !result.second_called &&
                    !payload.calls
                    && !journal && !second_calls && !scope.calls);
                CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                        bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION));
            } else if (fault == 3) {
                CHECK(!result.checkpoint_result && !result.financial_result
                    && !result.first_called && !result.second_called
                    && result.first_result == EINVAL && !payload.calls &&
                    !journal && !scope.calls);
            } else if (fault == 4) {
                CHECK(!result.checkpoint_result &&
                    result.financial_result == WL_EVALUATION_CONTROL_CANCELLED
                    && result.first_called && !result.first_result &&
                    !result.second_called
                    && scope.calls == 1 && !scope.born.control &&
                    !payload.calls && !journal);
                CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                        bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION));
            } else if (fault == 5) {
                CHECK(!result.checkpoint_result && !result.financial_result &&
                    result.first_called
                    && result.first_result == EIO && !result.second_called
                    && scope.calls == 1 && !scope.born.control &&
                    !payload.calls && !journal);
            } else {
                CHECK(!result.checkpoint_result && !result.financial_result &&
                    result.first_called);
                if (mode == 4) CHECK(scope.calls == 1 && !scope.born.control
                        && result.second_called &&
                        result.second_result == payload.error
                        && payload.calls == 1 && journal == 1);
                else {
                    CHECK(result.first_result == payload.error &&
                        payload.calls == 1 && journal == 1);
                    if (mode == 2) CHECK(result.second_called == (fault != 2)
                            && second_calls == (fault != 2));
                    else CHECK(!result.second_called);
                }
            }
            if (scope.born.control) {
                const wl_evaluation_control_lexical_finance_expectation_t
                    expected = scope.expected;
                wl_evaluation_control_lexical_finance_result_t closed =
                    wl_evaluation_control_lexical_finance_close_checked(
                    scope.born.control, &expected);
                CHECK(!closed.protocol && closed.closed && closed.report_valid);
            }
            CHECK(!wl_evaluation_control_test_source_early_cancel_once);
        }
    expected_final_metadata_payload = NULL;
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
}

static wl_evaluation_control_source_early_result_t
early_invalid_call(unsigned mode, wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t lane,
    const wl_evaluation_control_source_phase_policy_t *policy,
    const wl_evaluation_control_source_pair_policy_t *pair_policy,
    void *payload, bool missing_geometry)
{
    wl_evaluation_control_source_final_geometry_t geometry = {0};
    wl_evaluation_control_source_final_pair_geometry_t pair_geometry = {0};
    if (!mode) return wl_evaluation_control_source_bank_run_lexical_early(bank,
                   lane,
                   policy, count_action, payload);
    if (mode ==
        1) return
            wl_evaluation_control_source_bank_run_lexical_constructor_early(
            bank, lane,
            policy, count_action, payload);
    if (mode ==
        2) return wl_evaluation_control_source_bank_run_lexical_pair_early(bank,
                   lane,
                   pair_policy, count_action, payload, count_action, payload);
    if (mode ==
        3) return wl_evaluation_control_source_bank_run_lexical_final_early(
            bank, lane,
            policy, missing_geometry ? NULL : &geometry, count_action);
    return wl_evaluation_control_source_bank_run_lexical_pair_final_early(bank,
               lane,
               pair_policy, missing_geometry ? NULL : &pair_geometry,
               count_action, count_action);
}

static void
early_invalid_cases(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
    const wl_evaluation_control_source_pair_policy_t pair_policy = {64, 64, 64,
                                                                    64};
    unsigned calls = 0;
    CHECK(wl_evaluation_control_source_bank_run_lexical_early(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, count_action,
        &calls).first_called && calls == 1);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &before));
    for (unsigned mode = 0; mode < 5; ++mode)
        for (unsigned fault = 0; fault < 6; ++fault) {
            wl_evaluation_control_source_phase_policy_t invalid = policy;
            wl_evaluation_control_source_pair_policy_t invalid_pair =
                pair_policy;
            if (fault == 2) {
                invalid.bootstrap_work = 0; invalid_pair.bootstrap_work = 0;
            }
            if (fault == 3) {
                invalid.current_work = 257; invalid_pair.first_work = 257;
            }
            if (fault == 4) {
                invalid.close_work = 0; invalid_pair.close_work = 0;
            }
            if (fault == 5 && (mode == 2 || mode == 4))
                invalid_pair = (wl_evaluation_control_source_pair_policy_t){256,
                                                                            256,
                                                                            256,
                                                                            256};
            wl_evaluation_control_source_early_result_t result =
                early_invalid_call(mode,
                    fault == 0 ? NULL : bank,
                    fault ==
                    1 ? (wl_evaluation_control_source_lane_t)2 :
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION,
                    &invalid, &invalid_pair,
                    fault == 5 && mode < 2 ? (void *)bank : &calls,
                    fault == 5 && mode == 3);
            CHECK(result.protocol == EINVAL && result.financial_result == EINVAL
                && !result.begin_protocol && !result.checkpoint_result
                && !result.phase_started && !result.phase_finished
                && !result.first_called && !result.second_called && calls == 1);
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
            same_report(&before, &after);
        }
    for (unsigned mode = 2; mode <= 4; mode += 2) {
        wl_evaluation_control_source_early_result_t invalid =
            early_invalid_call(mode, bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &pair_policy,
                mode == 2 ? (void *)bank : &calls, mode == 4);
        CHECK(invalid.protocol == EINVAL && invalid.financial_result == EINVAL
            && !invalid.phase_started && !invalid.phase_finished &&
            !invalid.first_called
            && !invalid.second_called && calls == 1);
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
        same_report(&before, &after);
    }
    unsigned char saved[sizeof(after)]; memcpy(saved, &after, sizeof(after));
    for (unsigned fault = 1; fault <= 3; ++fault) {
        wl_evaluation_control_test_source_early_report_fault = fault;
        CHECK(wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after) == EINVAL
            && !wl_evaluation_control_test_source_early_report_fault
            && !memcmp(saved, &after, sizeof(after)));
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
        same_report(&before, &after);
    }
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION,
        (wl_evaluation_control_report_t *)(uintptr_t)(UINTPTR_MAX - 1)) ==
        EINVAL);
    CHECK(wl_evaluation_control_test_source_early_report(NULL,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after) == EINVAL
        && !memcmp(saved, &after, sizeof(after)));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        (wl_evaluation_control_source_lane_t)2, &after) == EINVAL
        && !memcmp(saved, &after, sizeof(after)));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, NULL) == EINVAL);
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION,
        (wl_evaluation_control_report_t *)((unsigned char *)bank + 1)) ==
        EINVAL);
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after) == EBUSY
        && !memcmp(saved, &after, sizeof(after)));
    CHECK(!wl_evaluation_control_source_context_end(&context));
    CHECK(!wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
}

static void
fresh_source_birth_cases(void)
{
    int owner = 0, scope = 0;
    for (unsigned mode = 0; mode < 2; ++mode) {
        wl_evaluation_control_lexical_finance_birth_result_t born = mode
            ? wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner)
            : wl_evaluation_control_lexical_finance_birth_try(129, &owner);
        CHECK(!born.protocol && born.control && born.id == 1 && born.identity
            && wl_evaluation_control_identity(born.control) == born.identity
            && wl_evaluation_control_active_attempt_id(born.control) == 1);
        wl_evaluation_control_lexical_finance_expectation_t expected = {
            .owner = &owner, .scope = &scope,
            .owner_bytes = sizeof(owner), .scope_bytes = sizeof(scope),
            .identity = born.identity, .limit = 129, .id = born.id,
            .active = true
        };
        CHECK(!wl_evaluation_control_lexical_finance_prove_checked(born.control,
            &expected));
        wl_evaluation_control_report_t report, saved;
        CHECK(wl_evaluation_control_report(born.control, &report) == ENOENT);
        uint64_t used = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(born.control, 1,
            &used) && !used);
        CHECK(!wl_evaluation_control_retain(born.control));
        CHECK(wl_evaluation_control_lexical_finance_prove_checked(born.control,
            &expected) == EBUSY);
        wl_evaluation_control_release(born.control);
        CHECK(!wl_evaluation_control_charge_attempt(born.control, 1, 7));
        CHECK(!wl_evaluation_control_finish(born.control, &owner, 0, 0));
        CHECK(!wl_evaluation_control_report(born.control,
            &saved) && saved.attempt_id == 1
            && saved.limit == 129 && saved.consumed == 7 && !saved.stop_reason
            && !saved.execution_status && !saved.cleanup_status &&
            !saved.result);
        CHECK(!wl_evaluation_control_begin(born.control, &owner)
            && wl_evaluation_control_active_attempt_id(born.control) == 2);
        CHECK(!wl_evaluation_control_report(born.control, &report));
        same_report(&saved, &report);
        expected.charged = 7;
        wl_evaluation_control_lexical_finance_result_t stale =
            wl_evaluation_control_lexical_finance_close_checked(born.control,
                &expected);
        CHECK(stale.protocol == EINVAL && !stale.closed
            && !wl_evaluation_control_test_active_consumed(born.control, 2,
            &used) && !used
            && !wl_evaluation_control_report(born.control, &report));
        same_report(&saved, &report);
        expected.id = 2; expected.charged = 0;
        wl_evaluation_control_lexical_finance_result_t closed =
            wl_evaluation_control_lexical_finance_close_checked(born.control,
                &expected);
        CHECK(!closed.protocol && closed.closed && closed.report_valid
            && closed.report.attempt_id == 2 && !closed.report.consumed &&
            !closed.result);
    }
    wl_evaluation_control_test_finance_birth_claim = constructor_peer_claim;
    wl_evaluation_control_lexical_finance_birth_result_t invalid =
        wl_evaluation_control_lexical_finance_fresh_birth_try(0, &owner);
    CHECK(invalid.protocol == EINVAL && !invalid.control && !invalid.id &&
        !invalid.identity
        && wl_evaluation_control_test_finance_birth_claim);
    invalid = wl_evaluation_control_lexical_finance_fresh_birth_try(1, NULL);
    CHECK(invalid.protocol == EINVAL && !invalid.control && !invalid.id &&
        !invalid.identity
        && wl_evaluation_control_test_finance_birth_claim);
    invalid = wl_evaluation_control_lexical_finance_fresh_birth_try(129,
            &owner);
    CHECK(invalid.protocol == EAGAIN && !invalid.control && !invalid.id &&
        !invalid.identity
        && !wl_evaluation_control_test_finance_birth_claim);
    for (unsigned fault = 1; fault <= 2; ++fault) {
        wl_evaluation_control_lexical_finance_birth_result_t before =
            wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
        CHECK(!before.protocol && before.control);
        CHECK(!wl_evaluation_control_finish(before.control, &owner, 0, 0)
            && !wl_evaluation_control_detach(before.control, &owner));
        wl_evaluation_control_release(before.control);
        wl_evaluation_control_test_finance_birth_fail = fault;
        invalid = wl_evaluation_control_lexical_finance_fresh_birth_try(129,
                &owner);
        CHECK(invalid.protocol == ENOMEM && !invalid.control && !invalid.id &&
            !invalid.identity
            && !wl_evaluation_control_test_finance_birth_fail);
        wl_evaluation_control_lexical_finance_birth_result_t after =
            wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
        CHECK(!after.protocol && after.control &&
            after.identity == before.identity + 2);
        wl_evaluation_control_request_cancel(after.control);
        CHECK(wl_evaluation_control_charge_attempt(after.control, after.id, 0)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_evaluation_control_finish(after.control, &owner, 0,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_evaluation_control_report_t cancelled;
        CHECK(!wl_evaluation_control_report(after.control, &cancelled)
            && cancelled.attempt_id == 1 && !cancelled.consumed
            && cancelled.result == WL_EVALUATION_CONTROL_CANCELLED
            && !cancelled.execution_status && !cancelled.cleanup_status);
        CHECK(!wl_evaluation_control_detach(after.control, &owner));
        wl_evaluation_control_release(after.control);
    }
}

static void
fresh_source_birth_exhaustion(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    int owner = 0;
    CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1));
    wl_evaluation_control_lexical_finance_birth_result_t last =
        wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
    CHECK(!last.protocol && last.control && last.identity == UINT64_MAX - 1 &&
        last.id == 1);
    wl_evaluation_control_lexical_finance_birth_result_t exhausted =
        wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
    CHECK(exhausted.protocol == EOVERFLOW && !exhausted.control &&
        !exhausted.id && !exhausted.identity);
    CHECK(!wl_evaluation_control_finish(last.control, &owner, 0, 0)
        && !wl_evaluation_control_detach(last.control, &owner));
    wl_evaluation_control_release(last.control);
    wl_evaluation_control_source_context_t context = {0};
    CHECK(wl_evaluation_control_source_metadata_enter_issued(bank, &context)
        == WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN);
    issued_count_t payload = {0};
    wl_evaluation_control_source_metadata_preparation_t prepared = {0};
    CHECK(!wl_evaluation_control_source_metadata_continue_issued(&context,
        &test_policy, issued_count_prepare, &payload, &prepared)
        && payload.groups == 3 && payload.calls == 1
        && prepared.entry.begun && prepared.entry.settled && !context.identity);
    wl_evaluation_control_t *still_exhausted = NULL;
    CHECK(wl_evaluation_control_create(1, &still_exhausted) == EOVERFLOW
        && !still_exhausted);
    CHECK(!wl_evaluation_control_source_operation_observe_owned(bank));
    payload = (issued_count_t){0};
    prepared = (wl_evaluation_control_source_metadata_preparation_t){0};
    CHECK(!wl_evaluation_control_source_metadata_enter_close_owned(bank,
        &context));
    CHECK(!wl_evaluation_control_source_metadata_continue_close_owned(&context,
        &test_policy, issued_count_prepare, &payload, &prepared)
        && payload.groups == 3 && payload.calls == 1
        && prepared.entry.settled && !context.identity);
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
}

typedef struct {
    wl_evaluation_control_t *source;
    wl_evaluation_control_lexical_finance_expectation_t expected;
    unsigned calls;
} early_close_refusal_t;

static int
early_close_strict_refusal(void *payload)
{
    early_close_refusal_t *p = payload;
    ++p->calls;
    wl_evaluation_control_request_cancel(p->source);
    wl_evaluation_control_lexical_finance_expectation_t stale = p->expected;
    ++stale.id;
    wl_evaluation_control_lexical_finance_result_t refused =
        wl_evaluation_control_lexical_finance_close_checked(p->source, &stale);
    CHECK(refused.protocol == EINVAL && !refused.closed);
    return refused.protocol;
}

static void
early_close_refusal_cases(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 144,
                                                                       80};
    int owner = 0, scope = 0;
    for (unsigned mode = 0; mode < 3; ++mode) {
        wl_evaluation_control_lexical_finance_birth_result_t born =
            wl_evaluation_control_private_financier_birth_try(64, &owner);
        CHECK(!born.protocol);
        wl_evaluation_control_lexical_finance_checkpoint_result_t checked =
            wl_evaluation_control_lexical_finance_checkpoint_owned(born.control,
                &owner, sizeof(owner), born.identity, born.id, 64, 0);
        CHECK(checked.checked && !checked.protocol &&
            !checked.checkpoint_result);
        early_close_refusal_t p = {
            .source = born.control,
            .expected = {.owner = &owner, .scope = &scope,
                         .owner_bytes = sizeof(owner),
                         .scope_bytes = sizeof(scope),
                         .identity = born.identity, .limit = 64, .id = born.id,
                         .active = true}
        };
        wl_evaluation_control_source_close_borrow_t borrow = {
            born.control, mode == 0 ? 0 : born.id,
            mode == 1 ? born.identity + 1 : born.identity
        };
        if (mode < 2) wl_evaluation_control_request_cancel(born.control);
        wl_evaluation_control_source_close_early_result_t result =
            wl_evaluation_control_source_bank_run_lexical_close_early(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &borrow,
                early_close_strict_refusal, &p);
        if (mode < 2) CHECK(result.protocol == EINVAL && !p.calls
                && !result.phase.phase_started && !result.phase.first_called
                && !result.acquisition_called && !result.unwind_handoff_called);
        else CHECK(!result.protocol && p.calls == 1 &&
                result.phase.phase_started
                && result.phase.phase_finished && result.phase.first_called
                && result.phase.first_result == EINVAL &&
                !result.phase.financial_result
                && result.acquisition_called && !result.unwind_handoff_called);
        /* The callback refused; ownership is still live. In particular no
         * saved Source poll is permitted after entering A, even on refusal. */
        CHECK(wl_evaluation_control_active_attempt_id(born.control) == born.id);
        wl_evaluation_control_attempt_finish_result_t ended =
            wl_evaluation_control_finish_attempt(born.control, &owner, born.id,
                0, 0);
        CHECK(!ended.protocol && ended.report_valid && !ended.report.stop_reason
            && !ended.report.result);
        p.expected.active = false; p.expected.id = 0;
        wl_evaluation_control_lexical_finance_result_t closed =
            wl_evaluation_control_lexical_finance_close_checked(born.control,
                &p.expected);
        CHECK(!closed.protocol && closed.closed && !closed.result);
    }
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
}

typedef struct {
    wl_evaluation_control_source_context_t *context;
    int poll, end;
} observation_peer_t;

static void *
observation_foreign(void *payload)
{
    observation_peer_t *peer = payload;
    peer->poll = wl_evaluation_control_source_observation_poll(peer->context);
    peer->end = wl_evaluation_control_source_observation_end(peer->context);
    return NULL;
}

static void
scoped_observation(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    for (unsigned stop = 0; stop <= 5; ++stop) {
        wl_evaluation_control_source_context_t context;
        unsigned char untouched[sizeof(context)];
        memset(&context, 0x5a, sizeof(context));
        memcpy(untouched, &context, sizeof(context));
        wl_evaluation_control_test_source_observation_cancel_at = stop <=
            3 ? stop : 0;
        int opened = wl_evaluation_control_source_observation_open(bank,
                &context);
        if (stop && stop <= 3) {
            CHECK(opened ==
                WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED
                && !memcmp(untouched, &context, sizeof(context)));
        } else {
            CHECK(opened == WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN
                && context.identity == &context);
            wl_evaluation_control_source_context_t copy = context, busy;
            memset(&busy, 0x5a, sizeof(busy));
            copy.identity = &copy;
            CHECK(wl_evaluation_control_source_observation_poll(&copy) == EINVAL
                && wl_evaluation_control_source_observation_end(&copy) == EINVAL
                && context.identity == &context);
            CHECK(wl_evaluation_control_source_observation_open(bank, &busy)
                == -EBUSY && context.identity == &context);
            unsigned calls = 0;
            wl_evaluation_control_source_phase_result_t wrong =
                wl_evaluation_control_source_bank_run(&context, &test_policy,
                    count_action, &calls);
            CHECK(wrong.financial_result == EBUSY && !wrong.action_called &&
                !calls
                && wl_evaluation_control_source_context_end(&context) == EBUSY);
            wl_evaluation_control_source_metadata_preparation_t prepared = {0};
            CHECK(wl_evaluation_control_source_metadata_continue_issued(
                    &context,
                    NULL, NULL, NULL, &prepared) == EBUSY
                && !prepared.entry.begun && !prepared.entry.settled
                && context.identity == &context);
            wl_evaluation_control_source_metadata_bind_outcome_t bound = {0};
            CHECK(wl_evaluation_control_source_metadata_bind_continue_issued(
                    &context, NULL, NULL, NULL,
                    &bound) == EBUSY && !bound.settled
                && context.identity == &context);
            observation_peer_t peer = {&context, 0, 0};
            wl_thread_t thread;
            CHECK(!wl_thread_create(&thread, observation_foreign, &peer)
                && !wl_thread_join(&thread) && peer.poll == EBUSY &&
                peer.end == EBUSY
                && context.identity == &context);
            if (!stop) {
                for (unsigned poll = 0; poll < 4096; ++poll)
                    CHECK(!wl_evaluation_control_source_observation_poll(
                            &context));
                CHECK(!wl_evaluation_control_source_observation_end(&context));
            } else {
                wl_evaluation_control_test_source_observation_cancel_at = stop;
                int result = stop == 4
                    ? wl_evaluation_control_source_observation_poll(&context)
                    : wl_evaluation_control_source_observation_end(&context);
                CHECK(result == WL_EVALUATION_CONTROL_CANCELLED);
            }
            CHECK(!context.identity
                && wl_evaluation_control_source_observation_poll(&context) ==
                EINVAL
                && wl_evaluation_control_source_observation_end(&context) ==
                EINVAL);
        }
        wl_evaluation_control_report_t source;
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &source)
            && source.attempt_id == 1 && source.limit == 768
            && source.consumed == (stop == 1 || stop == 2 ? 0u : 10u)
            && source.result == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0)
            && source.stop_reason == source.result
            && !source.execution_status && !source.cleanup_status);
        if (stop) CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                    bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION));
        CHECK(!wl_evaluation_control_report(original, &after));
        same_report(&before, &after);
    }
}

static void
retained_legacy_writers_refused(wl_evaluation_control_source_bank_t *bank)
{
    wl_evaluation_control_source_context_t context = {0};
    unsigned calls = 0;
    CHECK(wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION,
        &context) == EBUSY && !context.identity);
    CHECK(wl_evaluation_control_source_metadata_enter_issued(bank,
        &context) == -EBUSY
        && !context.identity);
    CHECK(wl_evaluation_control_source_observation_open(bank,
        &context) == -EBUSY
        && !context.identity);
    wl_evaluation_control_source_phase_result_t run =
        wl_evaluation_control_source_bank_run_lexical(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy, count_action,
            &calls);
    CHECK(run.financial_result == EBUSY && !run.action_called && !calls);
    run = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy, count_action,
            &calls);
    CHECK(run.financial_result == EBUSY && !run.action_called && !calls);
    wl_evaluation_control_t *foreign = NULL;
    CHECK(!wl_evaluation_control_create(1, &foreign));
    wl_evaluation_control_source_metadata_probe_t probe;
    unsigned char unchanged[sizeof(probe)];
    memset(&probe, 0x5a, sizeof(probe));
    memcpy(unchanged, &probe, sizeof(probe));
    for (unsigned fault = 1; fault <= 6; ++fault)
        for (unsigned issued = 0; issued != 2; ++issued)
            CHECK(wl_evaluation_control_test_source_metadata_start_rejection(
                    bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION, fault, foreign,
                    issued != 0,
                    &probe) == EBUSY && !memcmp(unchanged, &probe,
                sizeof(probe)));
    wl_evaluation_control_release(foreign);
}

static void
retained_observation(void)
{
    for (unsigned stop = 0; stop <= 5; ++stop) {
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        wl_evaluation_control_source_bank_t *alias = bank;
        wl_evaluation_control_source_retained_output_t result;
        unsigned char unchanged[sizeof(result)];
        memset(&result, 0x5a, sizeof(result));
        memcpy(unchanged, &result, sizeof(result));
        CHECK(wl_evaluation_control_source_standby_prepare_owned(&alias,
            &result) == EINVAL
            && !memcmp(unchanged, &result, sizeof(result)));
        CHECK(wl_evaluation_control_source_standby_prepare_owned(&bank,
            NULL) == EINVAL);
        CHECK(wl_evaluation_control_source_standby_observe_owned(&bank,
            &result) == EINVAL
            && !memcmp(unchanged, &result, sizeof(result)));
        wl_evaluation_control_test_source_observation_cancel_at = stop ==
            1 ? 1 : 0;
        CHECK(!wl_evaluation_control_source_standby_prepare_owned(&bank,
            &result));
        CHECK(!result.report_verified
            && result.opened == (stop != 1));
        retained_legacy_writers_refused(bank);
        for (unsigned fault = 1; fault <= 6; ++fault) {
            memcpy(unchanged, &result, sizeof(result));
            int protocol = wl_evaluation_control_test_source_standby_rejection(
                &bank, fault, &result);
            CHECK(protocol == (fault == 4 || fault == 5 ? EINVAL : EBUSY)
                && !memcmp(unchanged, &result, sizeof(result)));
        }
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report) == EBUSY);
        wl_evaluation_control_source_context_t blocked = {0};
        CHECK(wl_evaluation_control_source_context_begin(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &blocked) == EBUSY
            && !blocked.identity);
        CHECK(wl_evaluation_control_source_bank_destroy(&bank) == EBUSY &&
            bank);
        uint64_t expected = stop <= 2 && stop ? 0 :
            WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK;
        if (stop != 1) {
            CHECK(!wl_evaluation_control_source_standby_observe_owned(&bank,
                &result));
            CHECK(result.opened && !result.financial_result);
            wl_evaluation_control_source_context_t context = {0};
            wl_evaluation_control_test_source_observation_cancel_at =
                stop == 2 || stop == 3 ? stop : 0;
            CHECK(!wl_evaluation_control_source_standby_attach_owned(&bank,
                &context, &result));
            CHECK(result.opened == (stop != 2 && stop != 3));
            if (result.opened) {
                wl_evaluation_control_source_context_t copy = context;
                memcpy(unchanged, &result, sizeof(result));
                CHECK(wl_evaluation_control_source_standby_observe_owned(&bank,
                    &result) == EBUSY
                    && !memcmp(unchanged, &result, sizeof(result)));
                CHECK(wl_evaluation_control_source_standby_attach_owned(&bank,
                    &copy, &result) == EINVAL
                    && !memcmp(unchanged, &result, sizeof(result)));
                CHECK(wl_evaluation_control_source_standby_attach_owned(&bank,
                    &context, &result) == EINVAL
                    && !memcmp(unchanged, &result,
                    sizeof(result)) && context.identity == &context);
                CHECK(wl_evaluation_control_source_observation_poll(&copy) ==
                    EINVAL);
                observation_peer_t peer = {&context, 0, 0};
                wl_thread_t thread;
                CHECK(!wl_thread_create(&thread, observation_foreign, &peer)
                    && !wl_thread_join(&thread)
                    && peer.poll == EBUSY && peer.end == EBUSY);
                wl_evaluation_control_test_source_observation_cancel_at = stop;
                int rc = stop == 4
                    ? wl_evaluation_control_source_observation_poll(&context)
                    : wl_evaluation_control_source_observation_end(&context);
                CHECK(rc == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                    && !context.identity);
            }
        }
        if (stop) {
            retained_legacy_writers_refused(bank);
            for (unsigned repeat = 0; repeat != 3; ++repeat) {
                CHECK(!wl_evaluation_control_source_standby_observe_owned(&bank,
                    &result));
                CHECK(!result.opened && !result.report_verified
                    && result.financial_result ==
                    WL_EVALUATION_CONTROL_CANCELLED
                    && result.state ==
                    WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY);
            }
            CHECK(wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report) == EBUSY);
            CHECK(!wl_evaluation_control_source_standby_handoff_owned(&bank,
                &result));
            CHECK(result.report_verified
                && result.state == WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING
                && result.financial_result == WL_EVALUATION_CONTROL_CANCELLED);
        }
        memcpy(unchanged, &result, sizeof(result));
        CHECK(wl_evaluation_control_source_standby_prepare_owned(&bank,
            &result) == EBUSY
            && !memcmp(unchanged, &result, sizeof(result)));
        CHECK(!wl_evaluation_control_source_standby_handoff_owned(&bank,
            &result));
        CHECK(result.report_verified
            && result.state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT);
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report)
            && report.consumed == expected && report.attempt_id == 1
            && report.result == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0));
        memcpy(unchanged, &result, sizeof(result));
        CHECK(wl_evaluation_control_source_standby_handoff_owned(&bank,
            &result) == EINVAL
            && !memcmp(unchanged, &result, sizeof(result)));
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
    }
}

static void
retained_metadata_ack(wl_evaluation_control_source_bank_t **bank,
    wl_evaluation_control_source_retained_output_t *out,
    uint64_t consumed, int financial)
{
    if (out->state == WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY)
        CHECK(!wl_evaluation_control_source_standby_handoff_owned(bank, out));
    CHECK(out->state == WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING
        && out->report_verified && !out->opened
        && out->financial_result == financial);
    retained_legacy_writers_refused(*bank);
    CHECK(!wl_evaluation_control_source_standby_handoff_owned(bank, out)
        && out->state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        && out->report_verified && out->financial_result == financial);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_test_source_early_report(*bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report)
        && report.attempt_id == 1 && report.consumed == consumed
        && report.limit == 3 * WL_EVALUATION_CONTROL_QUANTUM
        && report.result == financial && report.stop_reason == financial
        && !report.execution_status && !report.cleanup_status);
}

typedef struct {
    unsigned groups, calls, fault;
    int result;
} retained_metadata_probe_t;

static int
retained_metadata_action(void *payload)
{
    retained_metadata_probe_t *p = payload;
    ++p->calls;
    return p->result;
}

static int
retained_metadata_prepare(const void *recipe, unsigned group,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    retained_metadata_probe_t *p = (void *)recipe;
    CHECK(group == p->groups++ && group < 3);
    if (p->fault == group + 1) return ERANGE;
    if (group == 2) {
        prepared->action = retained_metadata_action; prepared->payload = p;
        prepared->result_published = true;
    }
    return 0;
}

typedef struct {
    wl_evaluation_control_source_context_t *context;
    retained_metadata_probe_t *probe;
    int protocol;
} retained_metadata_peer_t;

static void *
retained_metadata_foreign(void *payload)
{
    retained_metadata_peer_t *peer = payload;
    wl_evaluation_control_source_retained_output_t out;
    unsigned char unchanged[sizeof(out)];
    memset(&out, 0x5a, sizeof(out)); memcpy(unchanged, &out, sizeof(out));
    wl_evaluation_control_source_metadata_preparation_t prepared = {0};
    peer->protocol =
        wl_evaluation_control_source_metadata_continue_retained_owned(
        peer->context, &test_policy, retained_metadata_prepare, peer->probe,
        &prepared, &out);
    CHECK(peer->protocol == EBUSY && !prepared.entry.begun
        && !memcmp(unchanged, &out, sizeof(out)));
    return NULL;
}

static void
retained_metadata(wl_evaluation_control_t *original)
{
    const wl_evaluation_control_source_phase_policy_t policy = {10, 13, 6};
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    for (unsigned mode = 0; mode != 8; ++mode) {
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        wl_evaluation_control_source_context_t context = {0};
        wl_evaluation_control_source_retained_output_t out;
        memset(&out, 0x5a, sizeof(out));
        unsigned char unchanged[sizeof(out)];
        memcpy(unchanged, &out, sizeof(out));
        wl_evaluation_control_source_bank_t *alias = bank;
        CHECK(wl_evaluation_control_source_metadata_enter_retained_owned(
                &alias, &context, &out) == EINVAL
            && !memcmp(unchanged, &out, sizeof(out)) && !context.identity);
        CHECK(wl_evaluation_control_source_metadata_enter_retained_owned(
                &bank, &context, NULL) == EINVAL && !context.identity);
        wl_evaluation_control_test_source_metadata_cancel_at = mode >= 1 &&
            mode <= 4 ? mode : 0;
        CHECK(!wl_evaluation_control_source_metadata_enter_retained_owned(
                &bank, &context, &out));
        retained_legacy_writers_refused(bank);
        retained_metadata_probe_t probe = {.result = mode == 7 ? 7 : 0};
        wl_evaluation_control_source_metadata_preparation_t prepared = {0};
        if (mode == 1) {
            CHECK(!out.opened && !out.report_verified && !context.identity
                && out.state == WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY
                && out.financial_result == WL_EVALUATION_CONTROL_CANCELLED);
        } else {
            CHECK(out.opened && !out.report_verified && !out.financial_result
                && context.identity == &context
                && out.state == WL_EVALUATION_CONTROL_SOURCE_METADATA);
            CHECK(wl_evaluation_control_source_metadata_enter_retained_owned(
                    &bank, &context, &out) == EINVAL
                && context.identity == &context);
            if (!mode) {
                retained_metadata_peer_t peer = {&context, &probe, 0};
                wl_thread_t thread;
                CHECK(!wl_thread_create(&thread, retained_metadata_foreign,
                    &peer)
                    && !wl_thread_join(&thread) && peer.protocol == EBUSY
                    && context.identity == &context && !probe.calls &&
                    !probe.groups);
            }
            wl_evaluation_control_source_context_t copy = context;
            memcpy(unchanged, &out, sizeof(out));
            CHECK(wl_evaluation_control_source_metadata_continue_retained_owned(
                    &copy, &policy, retained_metadata_prepare, &probe,
                    &prepared,
                    &out) == EINVAL && !memcmp(unchanged, &out, sizeof(out)));
            copy.identity = &copy;
            CHECK(wl_evaluation_control_source_metadata_continue_retained_owned(
                    &copy, &policy, retained_metadata_prepare, &probe,
                    &prepared,
                    &out) == EBUSY && !memcmp(unchanged, &out, sizeof(out)));
            CHECK(wl_evaluation_control_source_metadata_continue_issued(
                    &context, &policy, retained_metadata_prepare, &probe,
                    &prepared) == EBUSY && context.identity == &context
                && !prepared.entry.begun && !probe.groups && !probe.calls);
            wl_evaluation_control_test_source_lexical_cancel_before_action =
                mode == 5;
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                mode == 6;
            CHECK(
                !wl_evaluation_control_source_metadata_continue_retained_owned(
                    &context, &policy, retained_metadata_prepare, &probe,
                    &prepared,
                    &out) && !context.identity && prepared.entry.begun
                && prepared.entry.settled && !prepared.preparation_protocol
                && prepared.outcome.action_called == (mode == 0 || mode >= 6)
                && prepared.outcome.action_result == probe.result
                && probe.calls == (mode == 0 || mode >= 6 ? 1u : 0u));
            CHECK(probe.groups == (mode >= 2 && mode <= 4 ? mode - 1 : 3));
        }
        int financial = mode >= 1 && mode <= 6 ?
            WL_EVALUATION_CONTROL_CANCELLED : 0;
        uint64_t charged = mode == 1 ? 0 : mode >= 2 && mode <= 5 ? 10 :
            mode == 6 ? 23 : 29;
        retained_metadata_ack(&bank, &out, charged, financial);
        if (financial) CHECK(
                !wl_evaluation_control_test_source_final_reset_cancel(
                    bank, WL_EVALUATION_CONTROL_SOURCE_OPERATION));
        CHECK(!wl_evaluation_control_source_standby_prepare_owned(&bank, &out)
            && out.opened && out.state == WL_EVALUATION_CONTROL_SOURCE_STANDBY);
        CHECK(!wl_evaluation_control_source_standby_handoff_owned(&bank, &out));
        retained_metadata_ack(&bank, &out, 0, 0);
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
    }
    for (unsigned fault = 0; fault != 10; ++fault) {
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        wl_evaluation_control_source_context_t context = {0};
        wl_evaluation_control_source_retained_output_t out;
        CHECK(!wl_evaluation_control_source_metadata_enter_retained_owned(
                &bank, &context, &out) && out.opened);
        retained_metadata_probe_t probe = {.fault = fault >= 7 ? fault - 6 : 0};
        wl_evaluation_control_source_metadata_preparation_t prepared = {0};
        wl_evaluation_control_source_phase_policy_t selected = policy;
        if (fault == 1) selected.current_work = 0;
        int rc = wl_evaluation_control_source_metadata_continue_retained_owned(
            &context, fault == 0 ? NULL : &selected,
            fault == 2 ? NULL : retained_metadata_prepare,
            fault == 3 ? NULL : fault == 4 ? (void *)bank : (void *)&probe,
            fault == 5 ? NULL : &prepared, fault == 6 ? NULL : &out);
        CHECK(rc == (fault >= 7 ? ERANGE : EINVAL) && !context.identity
            && !probe.calls);
        if (fault != 5) CHECK(prepared.entry.begun && prepared.entry.settled);
        if (fault == 6) {
            CHECK(!wl_evaluation_control_source_standby_handoff_owned(&bank,
                &out) && out.state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report)
                && !report.consumed && !report.result);
        } else retained_metadata_ack(&bank, &out, fault >= 7 ? 10 : 0, 0);
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
    }
    for (unsigned cursor = 0; cursor <= 4; ++cursor)
        for (unsigned stop = 0; stop != 5; ++stop) {
            wl_evaluation_control_source_bank_t *bank = NULL;
            CHECK(!wl_evaluation_control_source_bank_create(&bank));
            wl_evaluation_control_source_context_t context = {0};
            wl_evaluation_control_source_retained_output_t out;
            wl_evaluation_control_test_source_metadata_cancel_at = stop;
            CHECK(!wl_evaluation_control_source_metadata_enter_retained_owned(
                    &bank, &context, &out));
            issued_bind_probe_t p = {.cursor = cursor};
            wl_evaluation_control_source_metadata_bind_outcome_t bound;
            if (out.opened) {
                CHECK(
                    !
                    wl_evaluation_control_source_metadata_bind_continue_retained_owned
                    (
                        &context, issued_bind_prepare, issued_bind_one, &p,
                        &bound,
                        &out) && bound.settled && !context.identity
                    && bound.role_called == (cursor < 4 && (!stop || stop == 4))
                    && bound.complete_selected == (cursor == 4 &&
                    (!stop || stop == 4))
                    && p.cursor == cursor + (bound.role_called ? 1u : 0u));
            } else CHECK(stop == 1 && !context.identity);
            bool stopped = stop && (stop != 4 || cursor < 4);
            retained_metadata_ack(&bank, &out,
                cursor < 4 && (!stop || stop == 4) ? 10 : 0,
                stopped ? WL_EVALUATION_CONTROL_CANCELLED : 0);
            wl_evaluation_control_test_source_metadata_cancel_at = 0;
            CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
        }
    for (unsigned fault = 0; fault != 9; ++fault) {
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        wl_evaluation_control_source_context_t context = {0};
        wl_evaluation_control_source_retained_output_t out;
        CHECK(!wl_evaluation_control_source_metadata_enter_retained_owned(
                &bank, &context, &out) && out.opened);
        issued_bind_probe_t probe = {
            .cursor = fault == 8 ? 5 : 0,
            .fault = fault >= 6 && fault <= 7 ? fault - 5 : 0
        };
        wl_evaluation_control_source_metadata_bind_outcome_t bound;
        CHECK(
            wl_evaluation_control_source_metadata_bind_continue_retained_owned(
                &context, fault == 0 ? NULL : issued_bind_prepare,
                fault == 1 ? NULL : issued_bind_one,
                fault == 2 ? NULL : fault == 3 ? (void *)bank : (void *)&probe,
                fault == 4 ? NULL : &bound, fault == 5 ? NULL : &out) == EINVAL
            && !context.identity && !probe.calls);
        if (fault != 4) CHECK(bound.settled);
        if (fault == 5) {
            CHECK(!wl_evaluation_control_source_standby_handoff_owned(&bank,
                &out) && out.state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT);
        } else retained_metadata_ack(&bank, &out, 0, 0);
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
    }
    /* Legitimate metadata need not have observation's fixed ten-cell extent. */
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    wl_evaluation_control_source_context_t context = {0};
    wl_evaluation_control_source_retained_output_t out;
    CHECK(!wl_evaluation_control_source_metadata_enter_retained_owned(&bank,
        &context, &out));
    retained_metadata_probe_t probe = {0};
    wl_evaluation_control_source_metadata_preparation_t prepared = {0};
    CHECK(!wl_evaluation_control_source_metadata_continue_retained_owned(
            &context, &test_policy, retained_metadata_prepare, &probe,
            &prepared,
            &out) && prepared.outcome.action_called && probe.calls == 1);
    retained_metadata_ack(&bank, &out, 3 * WL_EVALUATION_CONTROL_QUANTUM, 0);
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
}

static void
retained_metadata_attach(wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    for (unsigned stop = 0; stop != 2; ++stop) {
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        wl_evaluation_control_source_retained_output_t out;
        wl_evaluation_control_source_context_t context = {0};
        CHECK(!wl_evaluation_control_source_standby_prepare_owned(&bank, &out)
            && out.opened && out.state == WL_EVALUATION_CONTROL_SOURCE_STANDBY);
        unsigned char unchanged[sizeof(out)];
        memcpy(unchanged, &out, sizeof(out));
        for (unsigned fault = 1; fault <= 6; ++fault) {
            CHECK(wl_evaluation_control_test_source_metadata_attach_rejection(
                    &bank, fault,
                    &out) == (fault == 4 || fault == 5 ? EINVAL : EBUSY)
                && !memcmp(unchanged, &out, sizeof(out)));
        }
        wl_evaluation_control_source_bank_t *alias = bank;
        CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                &alias, &context, &out) == EINVAL
            && !memcmp(unchanged, &out, sizeof(out)) && !context.identity);
        CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                &bank, &context, NULL) == EINVAL && !context.identity);
        wl_evaluation_control_test_source_metadata_cancel_at = stop;
        CHECK(!wl_evaluation_control_source_metadata_attach_retained_owned(
                &bank, &context, &out)
            && out.opened == !stop && !out.report_verified);
        retained_legacy_writers_refused(bank);
        if (stop) {
            CHECK(!context.identity && !context.bank && !context.thread_cookie
                && !context.lane &&
                out.financial_result == WL_EVALUATION_CONTROL_CANCELLED
                && out.state == WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY);
            memcpy(unchanged, &out, sizeof(out));
            CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                    &bank, &context, &out) == EBUSY
                && !memcmp(unchanged, &out, sizeof(out)));
            retained_metadata_ack(&bank, &out, 0,
                WL_EVALUATION_CONTROL_CANCELLED);
        } else {
            CHECK(context.identity == &context && context.bank == bank
                && out.state == WL_EVALUATION_CONTROL_SOURCE_METADATA
                && !out.financial_result);
            wl_evaluation_control_source_context_t copy = context;
            memcpy(unchanged, &out, sizeof(out));
            CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                    &bank, &copy, &out) == EINVAL
                && !memcmp(unchanged, &out, sizeof(out)));
            CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                    &bank, &context, &out) == EINVAL
                && !memcmp(unchanged, &out, sizeof(out)));
            wl_evaluation_control_source_context_t empty = {0};
            CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                    &bank, &empty, &out) == EBUSY && !empty.identity
                && !memcmp(unchanged, &out, sizeof(out)));
            retained_metadata_probe_t probe = {0};
            wl_evaluation_control_source_metadata_preparation_t prepared = {0};
            const wl_evaluation_control_source_phase_policy_t policy = {10, 13,
                                                                        6};
            CHECK(
                !wl_evaluation_control_source_metadata_continue_retained_owned(
                    &context, &policy, retained_metadata_prepare, &probe,
                    &prepared,
                    &out) && prepared.entry.settled &&
                prepared.outcome.action_called
                && probe.groups == 3 && probe.calls == 1);
            CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                    &bank, &empty, &out) == EBUSY && !empty.identity);
            retained_metadata_ack(&bank, &out, 29, 0);
        }
        context = (wl_evaluation_control_source_context_t){0};
        memcpy(unchanged, &out, sizeof(out));
        CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                &bank, &context, &out) == EBUSY && !context.identity
            && !memcmp(unchanged, &out, sizeof(out)));
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
    }
    for (unsigned stop = 0; stop != 2; ++stop) {
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        wl_evaluation_control_source_retained_output_t out;
        wl_evaluation_control_source_context_t frame = {0}, destination = {0};
        CHECK(!wl_evaluation_control_source_standby_prepare_owned(&bank, &out)
            && !wl_evaluation_control_source_standby_attach_owned(&bank, &frame,
            &out) && out.opened);
        unsigned char unchanged[sizeof(out)];
        memcpy(unchanged, &out, sizeof(out));
        CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                &bank, &destination, &out) == EBUSY
            && !memcmp(unchanged, &out, sizeof(out)) && !destination.identity
            && frame.identity == &frame);
        wl_evaluation_control_test_source_observation_cancel_at = stop ? 5 : 0;
        CHECK(wl_evaluation_control_source_observation_end(&frame) ==
            (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0) && !frame.identity);
        CHECK(wl_evaluation_control_source_metadata_attach_retained_owned(
                &bank, &destination, &out) == EBUSY && !destination.identity);
        if (stop) {
            CHECK(!wl_evaluation_control_source_standby_handoff_owned(&bank,
                &out));
            retained_metadata_ack(&bank, &out, 10,
                WL_EVALUATION_CONTROL_CANCELLED);
        } else {
            CHECK(!wl_evaluation_control_source_standby_handoff_owned(&bank,
                &out)
                && out.state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
                && out.report_verified && !out.financial_result);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report)
                && report.consumed == 10 && !report.result);
        }
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
    }
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
}

static void
financial_packets(wl_evaluation_control_t *original)
{
    const wl_evaluation_control_source_financial_state_t states[] = {
        WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN,
        WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_BUILT,
        WL_EVALUATION_CONTROL_SOURCE_PF_BODY_BUILT,
        WL_EVALUATION_CONTROL_SOURCE_PF_PUBLISHED,
        WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_CHECKED,
        WL_EVALUATION_CONTROL_SOURCE_PF_TOTALS_CHECKED,
        WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED,
        WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
    };
    wl_evaluation_control_report_t original_before, original_after;
    CHECK(!wl_evaluation_control_report(original, &original_before));
    const wl_evaluation_control_source_phase_policy_t policy = {10, 13, 6};
    for (unsigned frame = 0; frame != 2; ++frame)
        for (unsigned cancel_at = 0; cancel_at != 10; ++cancel_at) {
            wl_evaluation_control_source_bank_t *bank = NULL;
            CHECK(!wl_evaluation_control_source_bank_create(&bank));
            unsigned calls = 0;
            wl_evaluation_control_source_metadata_outcome_t outcome = {0};
            CHECK(!wl_evaluation_control_source_bank_run_metadata(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, count_action,
                &calls, &outcome) && outcome.action_called && calls == 1);
            wl_evaluation_control_report_t old;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &old)
                && old.consumed == 29 && !old.result);
            wl_evaluation_control_source_retained_output_t out;
            CHECK(!wl_evaluation_control_source_standby_prepare_owned(&bank,
                &out)
                && out.opened);
            if (frame) {
                wl_evaluation_control_source_context_t context = {0};
                CHECK(!wl_evaluation_control_source_standby_attach_owned(&bank,
                    &context, &out) && out.opened);
                wl_evaluation_control_test_source_observation_cancel_at = 5;
                CHECK(wl_evaluation_control_source_observation_end(&context) ==
                    WL_EVALUATION_CONTROL_CANCELLED && !context.identity);
            }
            if (cancel_at == 1)
                CHECK(!wl_evaluation_control_test_source_financial_cancel(
                        &bank));
            int expected = frame ||
                cancel_at == 1 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
            for (unsigned step = 0; step != 8; ++step) {
                unsigned char unchanged[sizeof(out)];
                memcpy(unchanged, &out, sizeof(out));
                wl_evaluation_control_source_bank_t *alias = bank;
                CHECK(wl_evaluation_control_source_financial_progress_owned(
                        &alias, &out) == EINVAL && !memcmp(unchanged, &out,
                    sizeof(out)));
                CHECK(wl_evaluation_control_source_financial_progress_owned(
                        &bank, NULL) == EINVAL);
                CHECK(!wl_evaluation_control_source_financial_progress_owned(
                        &bank, &out) && out.state == states[step] && !out.opened
                    && out.financial_result == expected
                    && out.report_verified == (step >= 6));
                if (cancel_at == step + 2)
                    CHECK(!wl_evaluation_control_test_source_financial_cancel(
                            &bank));
                wl_evaluation_control_source_financial_view_t view;
                CHECK(!wl_evaluation_control_test_source_financial_view(&bank,
                    &view)
                    && view.state == states[step] &&
                    view.active_id == (step < 3 ? 1u : 0u)
                    && view.charged == (frame ? 10u : 0u)
                    && view.verified == (step >= 6));
                if (step < 3) same_report(&old, &view.history);
                else CHECK(view.history.attempt_id == 1
                        && view.history.limit ==
                        3 * WL_EVALUATION_CONTROL_QUANTUM
                        && view.history.consumed == (frame ? 10u : 0u)
                        && view.history.stop_reason == expected &&
                        view.history.result == expected
                        && !view.history.execution_status &&
                        !view.history.cleanup_status);
                if (step != 7) {
                    retained_legacy_writers_refused(bank);
                    for (unsigned fault = 1; fault <= 6; ++fault) {
                        memcpy(unchanged, &out, sizeof(out));
                        CHECK(
                            wl_evaluation_control_test_source_financial_rejection
                            (
                                &bank, fault,
                                &out) == (fault == 4 ||
                            fault == 5 ? EINVAL : EBUSY)
                            && !memcmp(unchanged, &out, sizeof(out)));
                    }
                    memcpy(unchanged, &out, sizeof(out));
                    CHECK(wl_evaluation_control_source_standby_handoff_owned(
                            &bank, &out) == EBUSY && !memcmp(unchanged, &out,
                        sizeof(out)));
                    CHECK(wl_evaluation_control_source_bank_destroy(&bank) ==
                        EBUSY && bank);
                    CHECK(wl_evaluation_control_test_source_final_reset_cancel(
                            bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION) == EBUSY);
                    wl_evaluation_control_report_t report;
                    unsigned char unreported[sizeof(report)];
                    memset(&report, 0x5a, sizeof(report));
                    memcpy(unreported, &report, sizeof(report));
                    CHECK(wl_evaluation_control_test_source_early_report(bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION,
                        &report) == EBUSY
                        && !memcmp(unreported, &report, sizeof(report)));
                    if (step >= 3 && step <= 5) {
                        unsigned first = step == 3 ? 1 : step == 4 ? 3 : 5;
                        unsigned last = step == 3 ? 2 : step == 4 ? 4 : 7;
                        for (unsigned fault = first; fault <= last; ++fault) {
                            memcpy(unchanged, &out, sizeof(out));
                            CHECK(
                                wl_evaluation_control_test_source_financial_readback_rejection
                                (
                                    &bank, fault, &out) == EINVAL
                                && !memcmp(unchanged, &out, sizeof(out)));
                        }
                        wl_evaluation_control_source_financial_view_t restored;
                        CHECK(!wl_evaluation_control_test_source_financial_view(
                                &bank,
                                &restored) && restored.state == view.state
                            && !restored.verified && !restored.active_id);
                        same_report(&view.history, &restored.history);
                    }
                }
            }
            unsigned char unchanged[sizeof(out)];
            memcpy(unchanged, &out, sizeof(out));
            CHECK(wl_evaluation_control_source_financial_progress_owned(&bank,
                &out) == EINVAL && !memcmp(unchanged, &out, sizeof(out)));
            wl_evaluation_control_report_t completed;
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &completed)
                && completed.result == expected &&
                completed.consumed == (frame ? 10u : 0u));
            if (frame || cancel_at) {
                /* A post-freeze request did not rewrite that report. It is
                * genuinely observed by the following active generation. */
                CHECK(!wl_evaluation_control_source_standby_prepare_owned(&bank,
                    &out)
                    && !out.opened && !out.report_verified
                    && out.state == WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY
                    && out.financial_result == WL_EVALUATION_CONTROL_CANCELLED);
                for (unsigned step = 0; step != 8; ++step)
                    CHECK(
                        !wl_evaluation_control_source_financial_progress_owned(
                            &bank,
                            &out) && out.state == states[step]
                        && out.financial_result ==
                        WL_EVALUATION_CONTROL_CANCELLED);
            }
            CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
        }
    const uint64_t extents[] = {0, 10, 23, 29, 768};
    for (unsigned i = 0; i != 5; ++i) {
        wl_evaluation_control_source_bank_t *bank = NULL;
        CHECK(!wl_evaluation_control_source_bank_create(&bank));
        wl_evaluation_control_source_context_t context = {0};
        wl_evaluation_control_source_retained_output_t out;
        CHECK(!wl_evaluation_control_source_metadata_enter_retained_owned(&bank,
            &context, &out) && out.opened);
        retained_metadata_probe_t probe = {0};
        wl_evaluation_control_source_metadata_preparation_t prepared = {0};
        wl_evaluation_control_test_source_lexical_cancel_before_action = i == 1;
        wl_evaluation_control_test_source_lexical_cancel_after_action = i == 2;
        CHECK(wl_evaluation_control_source_metadata_continue_retained_owned(
                &context,
                i ? i == 4 ? &test_policy : &policy : NULL,
                retained_metadata_prepare, &probe, &prepared,
                &out) == (i ? 0 : EINVAL)
            && out.state == WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING &&
            out.report_verified);
        wl_evaluation_control_source_financial_view_t before, after;
        CHECK(!wl_evaluation_control_test_source_financial_view(&bank, &before)
            && before.history.consumed == extents[i]);
        CHECK(!wl_evaluation_control_source_financial_progress_owned(&bank,
            &out)
            && out.state == WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED &&
            out.report_verified);
        CHECK(wl_evaluation_control_source_standby_handoff_owned(&bank,
            &out) == EBUSY);
        CHECK(!wl_evaluation_control_test_source_financial_view(&bank, &after));
        same_report(&before.history, &after.history);
        CHECK(!wl_evaluation_control_source_financial_progress_owned(&bank,
            &out)
            && out.state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT &&
            out.report_verified);
        CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
    }
    CHECK(!wl_evaluation_control_report(original, &original_after));
    same_report(&original_before, &original_after);
}

int
main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--fresh-exhaustion")) {
        fresh_source_birth_exhaustion();
        return 0;
    }
    retained_observation();
    fresh_source_birth_cases();
    early_close_refusal_cases();
    early_phase_cases();
    early_invalid_cases();
    final_metadata_geometry();
    lexical_pair();
    int owner;
    wl_evaluation_control_t *original = NULL;
    CHECK(!wl_evaluation_control_create(9, &original));
    CHECK(!wl_evaluation_control_attach(original, &owner));
    CHECK(!wl_evaluation_control_begin(original, &owner));
    CHECK(!wl_evaluation_control_charge_attempt(original, 1, 3));
    CHECK(!wl_evaluation_control_finish(original, &owner, 0, 0));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));

    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank) && bank);
    sealed_metadata(bank, original);
    sealed_metadata_recipe(bank, original);
    sealed_metadata_binding(bank, original);
    sealed_metadata_start_rejections();
    retained_metadata(original);
    retained_metadata_attach(original);
    financial_packets(original);
    sealed_issued_metadata(bank, original, false);
    sealed_issued_metadata(bank, original, true);
    issued_bind_cases(bank, original);
    fixed_observation(bank, original);
    scoped_observation(bank, original);
    lexical_lifetime(bank, original);
    constructor_lifetime(bank, original);
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    unsigned calls = 0;
    wl_evaluation_control_source_context_t copied = context;
    wl_evaluation_control_source_phase_result_t r =
        wl_evaluation_control_source_bank_run(&copied, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 0);
    static const wl_evaluation_control_source_phase_policy_t zero_policy = {
        0, WL_EVALUATION_CONTROL_QUANTUM, WL_EVALUATION_CONTROL_QUANTUM
    };
    static const wl_evaluation_control_source_phase_policy_t oversized_policy =
    {
        WL_EVALUATION_CONTROL_QUANTUM,
        WL_EVALUATION_CONTROL_QUANTUM + 1, WL_EVALUATION_CONTROL_QUANTUM
    };
    r = wl_evaluation_control_source_bank_run(&context, &zero_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 0);
    r = wl_evaluation_control_source_bank_run(&context, &oversized_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 0);
    wl_evaluation_control_source_bank_t *saved = bank;
    CHECK(wl_evaluation_control_source_bank_create(&bank) != 0 &&
        bank == saved);
    wl_evaluation_control_source_bank_t *copied_handle = bank;
    CHECK(wl_evaluation_control_source_bank_destroy(&copied_handle) == EINVAL
        && copied_handle == bank);
    CHECK(wl_evaluation_control_source_bank_destroy(&context.bank) == EINVAL
        && context.bank == bank);
    CHECK(wl_evaluation_control_source_bank_destroy(
            (wl_evaluation_control_source_bank_t **)(void *)bank) == EINVAL);
    CHECK(wl_evaluation_control_source_bank_destroy(&bank) == EBUSY
        && bank == saved);

    negative_reentry_context = &context;
    r = wl_evaluation_control_source_bank_run(&context, &test_policy,
            reentry_action, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result
        && calls == 0);
    negative_reentry_context = NULL;
    foreign_probe_t foreign = {&context, &calls, {0}, 0};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign_context, &foreign));
    CHECK(!wl_thread_join(&thread));
    CHECK(foreign.result.financial_result != 0 && !foreign.result.action_called
        && foreign.end_result != 0 && calls == 0);

    for (unsigned i = 0; i < 4096; ++i) {
        r = wl_evaluation_control_source_bank_run(&context, &test_policy,
                count_action, &calls);
        CHECK(!r.financial_result && r.action_called && !r.action_result);
    }
    CHECK(calls == 4096);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_begin(original, &owner));
    CHECK(wl_evaluation_control_active_attempt_id(original) == 2);
    uint64_t successor_consumed = UINT64_MAX;
    CHECK(!wl_evaluation_control_test_active_consumed(original, 2,
        &successor_consumed) && successor_consumed == 0);

    wl_evaluation_control_source_context_t closer = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_CLOSE, &closer));
    r = wl_evaluation_control_source_bank_run(&closer, &test_policy,
            count_action, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result
        && calls == 4097);
    CHECK(!wl_evaluation_control_source_context_end(&closer));
    CHECK(!wl_evaluation_control_source_context_end(&context));
    r = wl_evaluation_control_source_bank_run(&context, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 4097);
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));

    /* Real global constructor exhaustion, not fabricated native ownership.
     * The preborn bank must still actually call the failed constructor and
     * close its own finite paid phase. Nothing charges the original. */
    public_birth_and_partial_failure();
    wl_evaluation_control_t *failed = NULL;
    CHECK(!wl_evaluation_control_report(original, &before));
    r = wl_evaluation_control_source_bank_run(&context, &test_policy,
            real_failed_constructor, &failed);
    CHECK(!r.financial_result && r.action_called
        && r.action_result == EOVERFLOW && !failed);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_source_context_end(&context));
    r = wl_evaluation_control_source_bank_run_lexical(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            real_failed_constructor, &failed);
    CHECK(!r.financial_result && r.action_called
        && r.action_result == EOVERFLOW && !failed);
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    CHECK(!wl_evaluation_control_source_context_end(&context));
    r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            real_failed_constructor, &failed);
    CHECK(!r.financial_result && r.action_called
        && r.action_result == EOVERFLOW && !failed);
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
    CHECK(wl_evaluation_control_active_attempt_id(original) == 2);
    successor_consumed = UINT64_MAX;
    CHECK(!wl_evaluation_control_test_active_consumed(original, 2,
        &successor_consumed) && successor_consumed == 0);
    CHECK(!wl_evaluation_control_finish(original, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(original, &owner));
    wl_evaluation_control_release(original);
    puts("evaluation_source_bank: OK");
    return 0;
}
