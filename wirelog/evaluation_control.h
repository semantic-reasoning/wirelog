/*
 * evaluation_control.h - internal retained evaluation-control foundation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * INTERNAL HEADER - not installed and not part of the public API.
 */
#ifndef WL_EVALUATION_CONTROL_H
#define WL_EVALUATION_CONTROL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct wl_evaluation_control wl_evaluation_control_t;

/* Dedicated dispositions: never aliases of errno-based fallback requests. */
typedef enum {
    WL_EVALUATION_CONTROL_NONE = 0,
    WL_EVALUATION_CONTROL_CANCELLED = -2,
    WL_EVALUATION_CONTROL_WORK_LIMIT = -3,
} wl_evaluation_control_stop_t;

/* Downstream engine integration contract, not existing checkpoint coverage. */
#define WL_EVALUATION_CONTROL_QUANTUM UINT64_C(256)

typedef struct {
    uint64_t attempt_id;
    uint64_t limit;
    /* Admitted logical work, not CPU time or necessarily completed work. */
    uint64_t consumed;
    wl_evaluation_control_stop_t stop_reason;
    int execution_status;
    int cleanup_status;
    int result;
} wl_evaluation_control_report_t;

/* One readonly, mutex-serialized generation view of a live allocation.
 * This is completion history, not permission to run or finish Native work.
 * Caller holds a live reference and serializes owner lifecycle transitions.
 * EINVAL leaves both IDs zero. With ENOENT, active_id is valid and there is
 * no completed generation. Existing reports and cancellation are untouched. */
typedef struct {
    uint64_t active_id, completed_id;
    int protocol;
} wl_evaluation_control_completion_generation_t;
wl_evaluation_control_completion_generation_t
wl_evaluation_control_completion_generation_observe(wl_evaluation_control_t *,
    uint64_t expected_allocation_identity);

/* Limit 0 is unlimited. On failure *out is NULL. */
int wl_evaluation_control_create(uint64_t limit, wl_evaluation_control_t **out);
int wl_evaluation_control_retain(wl_evaluation_control_t *control);
/* The caller holds a live allocation reference. One strong CAS: EAGAIN
 * acquires no reference. Success owns one additional lifetime reference;
 * neither result authorizes Native work or changes the attempt. */
int wl_evaluation_control_retain_try(wl_evaluation_control_t *control);
#ifdef WL_SESSION_TEST_HOOKS
extern void (*wl_evaluation_control_test_retain_claim)(
    wl_evaluation_control_t *control);
#endif
void wl_evaluation_control_release(wl_evaluation_control_t *control);
/* Relinquish exactly one owned reference while another independent live
 * attachment/reference pins this allocation through the operation. This never
 * performs final destruction. A count invariant violation is fatal after the
 * decrement, not a recoverable refusal. */
void wl_evaluation_control_release_nonfinal(wl_evaluation_control_t *);
/* Stable, never-reused process identity; create returns EOVERFLOW on
 * exhaustion. The caller holds a live reference. */
uint64_t wl_evaluation_control_identity(
    const wl_evaluation_control_t *control);

/*
 * Attach retains one reference and claims a non-NULL, stable logical owner.
 * Detach releases it. A second attach (even the same owner) returns EBUSY.
 * Transfer changes the owner key without changing the owning reference: a
 * future lazy facade can hand its claim to the backend without reattaching.
 * Owner transitions require quiescence. Workers borrow, never attach/detach.
 */
int wl_evaluation_control_attach(wl_evaluation_control_t *control,
    const void *owner);
int wl_evaluation_control_detach(wl_evaluation_control_t *control,
    const void *owner);
int wl_evaluation_control_transfer(wl_evaluation_control_t *control,
    const void *owner, const void *next_owner);

/*
 * Configure/reset only between attempts; EBUSY while active. These do not
 * erase the last completed report. All host calls except request_cancel must
 * be externally serialized with session operations. Independently retained
 * references may be released concurrently. Charge calls by active workers
 * are serialized internally; finish/detach require all workers drained.
 */
int wl_evaluation_control_configure(wl_evaluation_control_t *control,
    uint64_t limit);
int wl_evaluation_control_reset(wl_evaluation_control_t *control);
/* NULL-safe. Takes no mutex. Caller must hold a live reference. */
void wl_evaluation_control_request_cancel(wl_evaluation_control_t *control);

/*
 * Begin requires the attached owner and returns 0 even if pre-cancelled:
 * charge(0) observes that cancellation, and every successful begin needs a
 * finish. Attempt IDs start at 1; exhaustion returns EOVERFLOW before begin.
 */
int wl_evaluation_control_begin(wl_evaluation_control_t *control,
    const void *owner);
/*
 * NULL/inactive returns 0; every nonzero uint64_t, including UINT64_MAX, is
 * a valid active attempt ID. One atomic observation, no mutex or work charge.
 * Caller holds a live reference. This does not pin an attempt: host lifecycle
 * still requires external serialization and finish requires workers drained.
 */
uint64_t wl_evaluation_control_active_attempt_id(
    const wl_evaluation_control_t *control);
#ifdef WL_SESSION_TEST_HOOKS
/* Inactive attached owner only; advance the generator without changing report. */
int wl_evaluation_control_test_seed_attempt_id(wl_evaluation_control_t *control,
    const void *owner, uint64_t value);
/* Caller holds live control and disjoint out. Fixed copied observation only;
 * inactive/stale/overlapping output fails without a write or work charge. */
int wl_evaluation_control_test_active_consumed(wl_evaluation_control_t *control,
    uint64_t expected_attempt_id, uint64_t *out);
/* Isolated test process only; seeds the next process-wide control identity. */
int wl_evaluation_control_test_seed_next_identity(uint64_t value);
#endif
/*
 * Charge before work; charge(0) is a poll. Denial does not consume work.
 * Cancellation precedes a new budget denial; a latched disposition persists.
 * Unlimited accounting overflow is a sticky EOVERFLOW execution error.
 */
int wl_evaluation_control_charge(wl_evaluation_control_t *control,
    uint64_t work);
/* Charge only if expected_attempt_id is still active; stale calls mutate nothing. */
int wl_evaluation_control_charge_attempt(wl_evaluation_control_t *control,
    uint64_t expected_attempt_id, uint64_t work);
/*
 * Finish does not poll: the integration must make its final charge(0) check
 * before committing publication. Late cancellation remains sticky for the
 * next attempt. Genuine execution error > cleanup error > control stop.
 * Both errors and the independently latched stop remain in the report.
 */
int wl_evaluation_control_finish(wl_evaluation_control_t *control,
    const void *owner, int execution_status, int cleanup_status);
/* ENOENT before first finish; returns the prior completed report if active. */
int wl_evaluation_control_report(wl_evaluation_control_t *control,
    wl_evaluation_control_report_t *out);

/* Financial-only validation for an actual paired lexical Source birth.
 * The Session layer must first authenticate its original-address record,
 * birth scope, issuer and creator thread. These expected values alone do not
 * establish that provenance or confer Native permission. */
typedef struct {
    const void *owner, *scope;
    size_t owner_bytes, scope_bytes;
    uint64_t identity, limit, id, charged;
    bool active;
} wl_evaluation_control_lexical_finance_expectation_t;

typedef struct {
    int protocol, result, checkpoint_result;
    bool closed, report_valid;
    wl_evaluation_control_report_t report;
} wl_evaluation_control_lexical_finance_result_t;

typedef struct {
    int protocol;
    wl_evaluation_control_t *control;
    uint64_t id, identity;
} wl_evaluation_control_lexical_finance_birth_result_t;

/* Check only a genuine exclusively owned active financial pair. A checked
 * cancellation/work stop still owns both obligations and must be retired.
 * Wrong provenance/owner/ref/id refuses before the controller is polled. */
typedef struct {
    int protocol, checkpoint_result;
    bool checked;
} wl_evaluation_control_lexical_finance_checkpoint_result_t;

wl_evaluation_control_lexical_finance_checkpoint_result_t
    wl_evaluation_control_lexical_finance_checkpoint_owned(
    wl_evaluation_control_t *, const void *, size_t, uint64_t, uint64_t,
    uint64_t, uint64_t);

/* Financial completion keeps both real references and owner attachment.
 * Caller proves a genuine live financial record/issuer; these expected values
 * alone confer neither Source provenance nor Native permission. */
typedef struct {
    int protocol, result, checkpoint_result;
    bool finished;
    wl_evaluation_control_report_t report;
} wl_evaluation_control_lexical_finance_finish_result_t;
wl_evaluation_control_lexical_finance_finish_result_t
wl_evaluation_control_lexical_finance_finish_owned(
    wl_evaluation_control_t *control, const void *owner, size_t owner_bytes,
    uint64_t saved_id, uint64_t limit, uint64_t charged);

/* Final retirement of a private, unexposed creation/attachment pair. Caller
 * guarantees exclusive lifetime through both real releases, not merely a
 * momentary ref2 observation. The actual completed report survives free by
 * value; this financial-only API supplies no issuer or Native authority. */
wl_evaluation_control_lexical_finance_result_t
wl_evaluation_control_lexical_finance_retire_active_owned(
    wl_evaluation_control_t *control, const void *owner, size_t owner_bytes,
    uint64_t saved_id, uint64_t limit, uint64_t charged);

/* One immutable-identity claim, no retry loop. Establishes actual creation
 * and owner-attachment references on a fresh private NORMAL control, then
 * performs real BEGIN. The paired Session issuer supplies the empty record. */
wl_evaluation_control_lexical_finance_birth_result_t
wl_evaluation_control_lexical_finance_birth_try(uint64_t limit,
    const void *owner);

/* Same bounded private birth, allowing the legacy unlimited capacity (0).
 * This NORMAL controller is financial ownership, never Native permission.
 * Caller settles FINISH, detaches its actual owner and releases creation.
 * Limit 0 does not satisfy the positive-only lexical Source close contract. */
wl_evaluation_control_lexical_finance_birth_result_t
wl_evaluation_control_private_financier_birth_try(uint64_t limit,
    const void *owner);

/* Fresh unpublished positive Source only; initializes its actual first
 * generation before returning any pointer. Never resets an existing owner. */
wl_evaluation_control_lexical_finance_birth_result_t
    wl_evaluation_control_lexical_finance_fresh_birth_try(uint64_t,
    const void *);

#ifdef WL_SESSION_TEST_HOOKS
extern void (*wl_evaluation_control_test_finance_birth_claim)(void);
/* One-shot allocator/mutex-init boundary faults, only for birth_try. */
extern unsigned wl_evaluation_control_test_finance_birth_fail;
/* Seed only the actual next unpublished attached allocation's counter before
 * its genuine first BEGIN; no returned id/report/active state is fabricated. */
extern uint64_t wl_evaluation_control_test_finance_birth_attempt_id;

#endif

int wl_evaluation_control_lexical_finance_prove_checked(
    wl_evaluation_control_t *,
    const wl_evaluation_control_lexical_finance_expectation_t *);

/* Requires the same unexposed creation+attachment pair through the entire
 * synchronous operation. Wrong owner, aliases, stale generation or borrowed
 * references refuse before mutation. Returns a value, never writes through a
 * caller-supplied result pointer. Closed means both real refs were released
 * and the financial allocation is gone; do not follow the old pointer.
 * Authenticated active closure performs a real saved-generation financial
 * checkpoint under its mutex before FINISH, including pending cancellation.
 * Inactive closure verifies the prior report and never polls or finishes. */
wl_evaluation_control_lexical_finance_result_t
wl_evaluation_control_lexical_finance_close_checked(
    wl_evaluation_control_t *,
    const wl_evaluation_control_lexical_finance_expectation_t *);

/* Internal financial source class; never an engine/native permission. */
typedef struct wl_evaluation_control_source_bank
    wl_evaluation_control_source_bank_t;

typedef enum {
    WL_EVALUATION_CONTROL_SOURCE_OPERATION = 0,
    WL_EVALUATION_CONTROL_SOURCE_CLOSE = 1
} wl_evaluation_control_source_lane_t;

/* Initialize with {0}, pass by address, never copy or retain across public activations.
 * No underlying controller or saved native attempt ID is exported. */
typedef struct wl_evaluation_control_source_context {
    struct wl_evaluation_control_source_context *identity;
    wl_evaluation_control_source_bank_t *bank;
    unsigned lane;
    const void *thread_cookie;
} wl_evaluation_control_source_context_t;

typedef struct {
    uint64_t bootstrap_work;
    uint64_t current_work;
    uint64_t close_work;
} wl_evaluation_control_source_phase_policy_t;

typedef struct {
    int financial_result;
    int action_result;
    bool action_called;
} wl_evaluation_control_source_phase_result_t;

typedef int (*wl_evaluation_control_source_action_fn)(void *payload);

/* Actual financial-only metadata outcome, at a stable caller-owned address.
 * Initialize before entry; disjoint from BANK, its controllers and context.
 * An uncalled action leaves this record unchanged. It conveys no authority.
 * The sealed action runs with BANK locked: only the designated bounded Normal
 * financial/fixed-metadata bodies, no Native/user/diagnostic/nested issuer. */
typedef struct {
    int action_result;
    bool action_called;
} wl_evaluation_control_source_metadata_outcome_t;
int wl_evaluation_control_source_bank_run_metadata(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_phase_policy_t *,
    wl_evaluation_control_source_action_fn, void *,
    wl_evaluation_control_source_metadata_outcome_t *);

/* Trusted automatic preparation for one synchronous sealed recipe issuer.
 * Initialize before entry. None of this storage may escape the admitted call.
 * Groups authenticate the stable recipe, resolve its live financial tuple,
 * then publish the selected action. They receive no BANK/control/id loan.
 * A completed group is retained even if its following actual zero refuses;
 * only action_called proves execution. Prices remain caller policy. */
typedef struct {
    /* checkpoint_result is valid only after an actual begun phase. */
    int checkpoint_result;
    bool begun, settled;
} wl_evaluation_control_source_metadata_entry_t;

typedef struct {
    wl_evaluation_control_source_action_fn action;
    void *payload;
    wl_evaluation_control_source_metadata_outcome_t outcome;
    int preparation_protocol;
    unsigned groups_completed;
    bool result_published;
    wl_evaluation_control_source_metadata_entry_t entry;
} wl_evaluation_control_source_metadata_preparation_t;
typedef int (*wl_evaluation_control_source_metadata_prepare_fn)(
    const void *, unsigned,
    wl_evaluation_control_source_metadata_preparation_t *);
int wl_evaluation_control_source_bank_run_metadata_recipe(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_phase_policy_t *,
    wl_evaluation_control_source_metadata_prepare_fn, const void *,
    wl_evaluation_control_source_metadata_preparation_t *);

/* Separate bootstrap for the four stable role recipes: no recipe exists
 * yet. Only the admitted fresh/reset constructor supplies these no-owner
 * metadata packets. Each successful packet must advance the real cursor
 * exactly once, before the following refusing checkpoint. */
typedef int (*wl_evaluation_control_source_metadata_bind_fn)(void *, unsigned);
int wl_evaluation_control_source_bank_bind_metadata_roles(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_phase_policy_t *,
    wl_evaluation_control_source_metadata_bind_fn, void *, unsigned *);
/* Cursor4 completion consumes the caller's authentic initialization lineage.
* This protocol has a genuine saved check/FINISH, and no positive coupons. */
int wl_evaluation_control_source_bank_observe_metadata(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    wl_evaluation_control_source_metadata_entry_t *);
/* Private issued entry, exclusively from a live owning admission/frame. BANK
 * and fresh automatic context are accessible/disjoint, never copied or loaned
 * to Native/user code. Context is initialized ONLY on OPEN. No policy/recipe
 * is submitted before the first real check. Generic checked APIs stay strict.
 * +1 classifies a genuinely closed CANCEL: its actual report remains -2.
 * Negative errno means no new phase began; it never means a Source stop. */
enum {
    WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN = 0,
    WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED = 1
};
int wl_evaluation_control_source_metadata_enter_issued(
    wl_evaluation_control_source_bank_t *,
    wl_evaluation_control_source_context_t *);
int wl_evaluation_control_source_metadata_enter_close_owned(
    wl_evaluation_control_source_bank_t *,
    wl_evaluation_control_source_context_t *);
/* Source-only fixed OP observation, no callback/payload or Native loan.
 * Caller owns the live BANK/frame. One actual setup-derived coupon10; every
 * actual OPEN settles/unbinds before return. Non-OPEN never reads context. */
#define WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK UINT64_C(10)
int wl_evaluation_control_source_operation_observe_owned(
    wl_evaluation_control_source_bank_t *);

/* Private ordinary STEP observation lifetime. Fresh final-address context and
 * live owning BANK are accessible/disjoint. Only OPEN initializes context;
 * first0/fixed10/post10zero all precede publication. Poll/end have no positive
 * charge; a financial stop settles and invalidates its exact owner. Generic
 * context/metadata operations cannot consume this distinct active family. */
int wl_evaluation_control_source_observation_open(
    wl_evaluation_control_source_bank_t *,
    wl_evaluation_control_source_context_t *);
int wl_evaluation_control_source_observation_poll(
    wl_evaluation_control_source_context_t *);
int wl_evaluation_control_source_observation_end(
    wl_evaluation_control_source_context_t *);

/* Stable BANK-owned financial generations. No automatic context, controller
 * or Native generation escapes the owner. Only an authenticated attach may
 * publish an automatic frame. Financial refusal and protocol are distinct. */
typedef enum {
    WL_EVALUATION_CONTROL_SOURCE_QUIESCENT,
    WL_EVALUATION_CONTROL_SOURCE_STANDBY,
    WL_EVALUATION_CONTROL_SOURCE_FRAME,
    WL_EVALUATION_CONTROL_SOURCE_METADATA,
    WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY,
    WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING,
    WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN,
    WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_BUILT,
    WL_EVALUATION_CONTROL_SOURCE_PF_BODY_BUILT,
    WL_EVALUATION_CONTROL_SOURCE_PF_PUBLISHED,
    WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_CHECKED,
    WL_EVALUATION_CONTROL_SOURCE_PF_TOTALS_CHECKED,
    WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED
} wl_evaluation_control_source_financial_state_t;
typedef struct {
    int financial_result;
    wl_evaluation_control_source_financial_state_t state;
    bool opened, report_verified;
} wl_evaluation_control_source_retained_output_t;
/* Positive protocol errno leaves the exclusive accessible output unchanged.
 * On zero all four fields describe the real transition. Output storage is
 * disjoint from slot, BANK/private controllers and optional context; its
 * pointer never escapes this call. Financial stops remain output values. */
int
wl_evaluation_control_source_standby_prepare_owned(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_retained_output_t *);
int
wl_evaluation_control_source_standby_observe_owned(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_retained_output_t *);
int
wl_evaluation_control_source_standby_attach_owned(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_context_t *,
    wl_evaluation_control_source_retained_output_t *);
/* First call FINISHes and verifies the report; a second owner call ACKs that
 * exact report and reaches QUIESCENT. Neither call rearms a generation. */
int
wl_evaluation_control_source_standby_handoff_owned(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_retained_output_t *);
/* Advance exactly one context-free financial packet. Freeze's actual zero is
 * this Source-only FINISH's stop linearization; later cancellation stays sticky
 * for the next observation. BUILD preserves the old completed report. COMMIT
 * publishes inactive, and separate readback packets establish verification
 * before ACK. No positive charge, callback, context or rearm is performed.
 * Q is EINVAL; live FRAME/METADATA or a conflicting protocol is EBUSY. Positive
 * protocol errors leave valid output and actual owner unchanged, including
 * outNULL. Output/storage preconditions match the retained scalar APIs. */
int wl_evaluation_control_source_financial_progress_owned(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_retained_output_t *);

/* Retained OP metadata has its own authenticated family; legacy issued and
 * CLOSE continuations cannot consume it. Entry requires an empty context at
 * its final address and the actual BANK birth slot. Initial refusal retains
 * the active zero-work generation without publishing an automatic context.
 * Accessible/aligned/exclusive/disjoint storage is required as above. */
int wl_evaluation_control_source_metadata_enter_retained_owned(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_context_t *,
    wl_evaluation_control_source_retained_output_t *);
/* Convert only the genuine uncharged STANDBY generation, under its BANK lock
 * and real saved-generation zero. No BEGIN, FINISH or observation ten is paid.
 * Empty final-address context and output/storage rules match retained entry;
 * protocol rejection leaves both untouched. Later continuation still proves
 * the complete live tuple before actual metadata charges or actions. */
int wl_evaluation_control_source_metadata_attach_retained_owned(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_context_t *,
    wl_evaluation_control_source_retained_output_t *);
/* Unowned/copy rejection leaves valid output storage and the owner unchanged;
 * a foreign actor returns EBUSY without settling the rightful actor's context.
 * After authenticating an owned context, even bad parameters (including NULL
 * outputs) settle/unlink it and return positive protocol errno. Valid output
 * storage then records actual FINISH_PENDING/report state, despite that errno;
 * valid prepared/outcome storage records settled. No output may be inspected
 * after an unowned rejection. Preparation/action outcomes remain separate from
 * the real financial stop. Only a later owner handoff ACK reaches QUIESCENT. */
int wl_evaluation_control_source_metadata_continue_retained_owned(
    wl_evaluation_control_source_context_t *,
    const wl_evaluation_control_source_phase_policy_t *,
    wl_evaluation_control_source_metadata_prepare_fn, const void *,
    wl_evaluation_control_source_metadata_preparation_t *,
    wl_evaluation_control_source_retained_output_t *);

/* Continue only the exact owned automatic OPEN context, in the same public
 * frame. Every owned protocol/action/refusal exit settles and unbinds it.
 * A copied/foreign context cannot close its real owner's active phase. */
int wl_evaluation_control_source_metadata_continue_issued(
    wl_evaluation_control_source_context_t *,
    const wl_evaluation_control_source_phase_policy_t *,
    wl_evaluation_control_source_metadata_prepare_fn, const void *,
    wl_evaluation_control_source_metadata_preparation_t *);
int wl_evaluation_control_source_metadata_continue_close_owned(
    wl_evaluation_control_source_context_t *,
    const wl_evaluation_control_source_phase_policy_t *,
    wl_evaluation_control_source_metadata_prepare_fn, const void *,
    wl_evaluation_control_source_metadata_preparation_t *);

/* Bootstrap one unpublished canonical role in an owned OP phase. The
 * preparation establishes the real cursor and exact slot before any body.
 * Cursor4 observes completion without a positive charge or role call. */
typedef struct {
    unsigned *cursor;
    unsigned before_cursor, after_cursor;
    int preparation_protocol;
    bool role_called, complete_selected, settled;
} wl_evaluation_control_source_metadata_bind_outcome_t;
typedef int (*wl_evaluation_control_source_metadata_bind_prepare_fn)(
    void *, unsigned,
    wl_evaluation_control_source_metadata_bind_outcome_t *);
/* Same retained owned-error settlement and disjoint-storage contract above. */
int wl_evaluation_control_source_metadata_bind_continue_retained_owned(
    wl_evaluation_control_source_context_t *,
    wl_evaluation_control_source_metadata_bind_prepare_fn,
    wl_evaluation_control_source_metadata_bind_fn, void *,
    wl_evaluation_control_source_metadata_bind_outcome_t *,
    wl_evaluation_control_source_retained_output_t *);
int wl_evaluation_control_source_metadata_bind_continue_issued(
    wl_evaluation_control_source_context_t *,
    wl_evaluation_control_source_metadata_bind_prepare_fn,
    wl_evaluation_control_source_metadata_bind_fn, void *,
    wl_evaluation_control_source_metadata_bind_outcome_t *);

/* out is NULL-initialized exclusive external owning storage and must
 * outlive BANK. Its address is the immutable owner_slot; never move/copy
 * ownership to another slot. Borrowed opaque handles cannot destroy BANK. */
int wl_evaluation_control_source_bank_create(
    wl_evaluation_control_source_bank_t **out);
int wl_evaluation_control_source_bank_destroy(
    wl_evaluation_control_source_bank_t **bank);
/* context is a complete zeroed external object, disjoint from BANK and
 * its private controllers; genuine public callers use lexical stack storage.
 * Arbitrary cast/overlapping output storage is outside this internal contract. */
int wl_evaluation_control_source_context_begin(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t lane,
    wl_evaluation_control_source_context_t *context);
int wl_evaluation_control_source_context_end(
    wl_evaluation_control_source_context_t *context);
/* The complete bounded SOURCE-only action receives no BANK/context/ID.
 * All three positive fees must be <=QUANTUM; policy is private immutable.
 * Constructor action executes immediately after CURRENT and returns before
 * CLOSE/report/rearm. No arbitrary BODY/user callback is an action. */
wl_evaluation_control_source_phase_result_t
wl_evaluation_control_source_bank_run(
    wl_evaluation_control_source_context_t *context,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload);

/* Same SOURCE-only callback contract, with a context owned exclusively by
 * this synchronous activation. No stack-context borrow survives its return.
 * This avoids separate context-begin/run/end source prefixes; actual callers
 * still require their own immutable policy and complete containing proof. */
wl_evaluation_control_source_phase_result_t
wl_evaluation_control_source_bank_run_lexical(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t lane,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload);

/* Constructor-only SOURCE callback sibling. BANK/control/context remain
 * private to this synchronous financial activation; completion uses one
 * actual locked checkpoint and validated report publication. */
wl_evaluation_control_source_phase_result_t
wl_evaluation_control_source_bank_run_lexical_constructor(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t lane,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload);

typedef struct {
    uint64_t bootstrap_work, first_work, second_work, close_work;
} wl_evaluation_control_source_pair_policy_t;

typedef struct {
    int financial_result, first_result, second_result;
    bool first_called, second_called;
} wl_evaluation_control_source_pair_result_t;

/* Two synchronous SOURCE-only actions under one actual lexical lane. A real
 * saved-generation zero separates the actions, followed by the independent
 * second positive admission. A nonzero first action suppresses the second.
 * No BANK/context/control/ID is lent to either action. Actual caller source
 * and chronological promise accounting require their own qualification. */
wl_evaluation_control_source_pair_result_t
    wl_evaluation_control_source_bank_run_lexical_pair(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_pair_policy_t *,
    wl_evaluation_control_source_action_fn, void *,
    wl_evaluation_control_source_action_fn, void *);

/* Sealed FINAL financial siblings. Geometry authenticates only financial
 * storage; the typed caller proves the actual metadata journal and callback.
 * No Source/context/control/ID is lent to either synchronous action. */
typedef struct {
    int financial_result, first_result, second_result;
    bool first_called, second_called, phase_started, phase_finished;
} wl_evaluation_control_source_final_pair_result_t;

typedef struct {
    int financial_result, action_result;
    bool action_called, phase_started, phase_finished;
} wl_evaluation_control_source_final_result_t;

typedef struct {
    const void *base;
    size_t bytes;
} wl_evaluation_control_source_extent_t;

typedef struct {
    wl_evaluation_control_source_extent_t session, record, scope, payload,
        journal;
    wl_evaluation_control_t *source_control;
    wl_evaluation_control_lexical_finance_expectation_t source_expectation;
} wl_evaluation_control_source_final_pair_geometry_t;

typedef struct {
    wl_evaluation_control_source_extent_t session, payload, journal;
} wl_evaluation_control_source_final_geometry_t;

/* MSVC's default C mode has no _Static_assert. Keep the same layout
 * constraints using constant-expression array bounds. */
#if defined(_MSC_VER)
typedef char wl_evaluation_control_financial_scalar_layout_check_
    [(sizeof(int) == 4 && sizeof(bool) == 1) ? 1 : -1];
typedef char wl_evaluation_control_final_pair_layout_check_
    [(sizeof(wl_evaluation_control_source_final_pair_result_t) == 16) ? 1 : -1];
typedef char wl_evaluation_control_final_layout_check_
    [(sizeof(wl_evaluation_control_source_final_result_t) == 12) ? 1 : -1];
typedef char wl_evaluation_control_final_pair_offset_check_
    [(offsetof(wl_evaluation_control_source_final_pair_result_t,
        phase_finished) == 15) ? 1 : -1];
typedef char wl_evaluation_control_final_offset_check_
    [(offsetof(wl_evaluation_control_source_final_result_t,
        phase_finished) == 10) ? 1 : -1];
#else
_Static_assert(sizeof(int) == 4 && sizeof(bool) == 1,
    "financial scalar layout");
_Static_assert(sizeof(wl_evaluation_control_source_final_pair_result_t) == 16,
    "final pair result layout");
_Static_assert(sizeof(wl_evaluation_control_source_final_result_t) == 12,
    "final result layout");
_Static_assert(offsetof(wl_evaluation_control_source_final_pair_result_t,
    phase_finished) == 15,
    "final pair lifecycle offsets");
_Static_assert(offsetof(wl_evaluation_control_source_final_result_t,
    phase_finished) == 10,
    "final lifecycle offsets");
#endif

wl_evaluation_control_source_final_pair_result_t
    wl_evaluation_control_source_bank_run_lexical_pair_final(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_pair_policy_t *,
    const wl_evaluation_control_source_final_pair_geometry_t *,
    wl_evaluation_control_source_action_fn,
    wl_evaluation_control_source_action_fn);

wl_evaluation_control_source_final_result_t
    wl_evaluation_control_source_bank_run_lexical_final(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_phase_policy_t *,
    const wl_evaluation_control_source_final_geometry_t *,
    wl_evaluation_control_source_action_fn);

/* A real early saved-generation observation with separate ownership facts. */
typedef struct {
    int protocol, begin_protocol, checkpoint_result, financial_result;
    int first_result, second_result;
    bool phase_started, phase_finished, first_called, second_called;
} wl_evaluation_control_source_early_result_t;

#ifdef WL_SESSION_TEST_HOOKS
enum {
    WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_ACQUIRE = 1,
    WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_PREFINISH,
    WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_COMPLETION,
    WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_PHYSICAL
};
extern unsigned wl_evaluation_control_test_source_close_cancel_at;
#endif

/* Synchronous financial borrow only. The Session authorizer has proved the
 * original lexical pair, identity/owner/ref2/saved generation, and keeps it
 * unexposed and exclusive until return. Only cancellation may change. These
 * scalar values alone authenticate no arbitrary caller or Native operation. */
typedef struct {
    wl_evaluation_control_t *control;
    uint64_t id, identity;
} wl_evaluation_control_source_close_borrow_t;

typedef struct {
    wl_evaluation_control_source_early_result_t phase;
    int protocol, acquisition_result, unwind_handoff_result;
    bool acquisition_called, unwind_handoff_called;
} wl_evaluation_control_source_close_early_result_t;

wl_evaluation_control_source_close_early_result_t
    wl_evaluation_control_source_bank_run_lexical_close_early(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_phase_policy_t *,
    const wl_evaluation_control_source_close_borrow_t *,
    wl_evaluation_control_source_action_fn, void *);

wl_evaluation_control_source_early_result_t
wl_evaluation_control_source_bank_run_lexical_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t lane,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload);

wl_evaluation_control_source_early_result_t
wl_evaluation_control_source_bank_run_lexical_constructor_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t lane,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload);

wl_evaluation_control_source_early_result_t
    wl_evaluation_control_source_bank_run_lexical_pair_early(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_pair_policy_t *,
    wl_evaluation_control_source_action_fn, void *,
    wl_evaluation_control_source_action_fn, void *);

wl_evaluation_control_source_early_result_t
    wl_evaluation_control_source_bank_run_lexical_final_early(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_phase_policy_t *,
    const wl_evaluation_control_source_final_geometry_t *,
    wl_evaluation_control_source_action_fn);

wl_evaluation_control_source_early_result_t
    wl_evaluation_control_source_bank_run_lexical_pair_final_early(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    const wl_evaluation_control_source_pair_policy_t *,
    const wl_evaluation_control_source_final_pair_geometry_t *,
    wl_evaluation_control_source_action_fn,
    wl_evaluation_control_source_action_fn);

#ifdef WL_SESSION_TEST_HOOKS
extern bool wl_evaluation_control_test_source_early_cancel_once;
extern bool wl_evaluation_control_test_source_early_rearm_ref_once;
extern unsigned wl_evaluation_control_test_source_early_report_fault;
int wl_evaluation_control_test_source_early_report(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    wl_evaluation_control_report_t *);
#endif

#ifdef WL_SESSION_TEST_HOOKS
/* Observation after the real successful between-actions saved0; no loan. */
extern void (*wl_evaluation_control_test_source_pair_checkpoint)(void);
/* Isolated financial component injection: cancel the actual private BANK
 * generation at the named event, without exposing its controller or ID. */
enum {
    WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN = 1,
    WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_FIRST,
    WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_SECOND,
    WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION
};
extern unsigned wl_evaluation_control_test_source_final_cancel_at;
/*1/2 substitutes that real private lane's interior as invalid payload range. */
extern unsigned wl_evaluation_control_test_source_final_geometry_fault;
/* Cancel the actual generic lexical BANK after its next called action. */
extern bool wl_evaluation_control_test_source_lexical_cancel_after_action;
/* Cancel after actual bootstrap admission, before CURRENT observes its stop. */
extern bool wl_evaluation_control_test_source_lexical_cancel_before_action;
/* 1 initial saved zero, 2..4 after preparation groups0..2;
 * the role binder additionally uses5 after its fourth actual role packet. */
extern unsigned wl_evaluation_control_test_source_metadata_cancel_at;
/* Separate producer:1 first0,2 before10,3 after10,4 poll,5 final0. */
extern unsigned wl_evaluation_control_test_source_observation_cancel_at;
extern bool wl_evaluation_control_test_source_bank_destroy_cancel;
/* Deliberately invalid financial callers. Statistics are observations only;
 * no underlying BANK controller, context or usable funding pair is loaned. */
typedef struct {
    uint64_t counter, active_id, consumed, lane_id, charged;
    bool verified;
    wl_evaluation_control_report_t report;
} wl_evaluation_control_source_metadata_probe_state_t;
typedef struct {
    wl_evaluation_control_source_metadata_probe_state_t before, after;
    wl_evaluation_control_source_metadata_entry_t entry;
} wl_evaluation_control_source_metadata_probe_t;
int wl_evaluation_control_test_source_metadata_start_rejection(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t,
    unsigned fault, wl_evaluation_control_t *foreign, unsigned issuer,
    wl_evaluation_control_source_metadata_probe_t *);
/* Externally serialized test fixture for the observer's REQUIRED runtime
 * class/owner/ref/saved-id/current-id/control checks. It never invalidates
 * projected count/limit/context/report invariants and exports no control. */
int wl_evaluation_control_test_source_standby_rejection(
    wl_evaluation_control_source_bank_t **, unsigned,
    wl_evaluation_control_source_retained_output_t *);
int wl_evaluation_control_test_source_metadata_attach_rejection(
    wl_evaluation_control_source_bank_t **, unsigned,
    wl_evaluation_control_source_retained_output_t *);
/* Serialized diagnostics of prior/published bytes, never a current-report ACK
 * or funding loan. A false verified flag forbids interpreting history as the
 * current generation's completed report. */
typedef struct {
    wl_evaluation_control_source_financial_state_t state;
    uint64_t active_id, charged;
    size_t bank_bytes;
    bool verified;
    wl_evaluation_control_report_t history;
} wl_evaluation_control_source_financial_view_t;
int wl_evaluation_control_test_source_financial_view(
    wl_evaluation_control_source_bank_t **,
    wl_evaluation_control_source_financial_view_t *);
int wl_evaluation_control_test_source_financial_cancel(
    wl_evaluation_control_source_bank_t **);
int wl_evaluation_control_test_source_financial_readback_rejection(
    wl_evaluation_control_source_bank_t **, unsigned,
    wl_evaluation_control_source_retained_output_t *);
int wl_evaluation_control_test_source_financial_rejection(
    wl_evaluation_control_source_bank_t **, unsigned,
    wl_evaluation_control_source_retained_output_t *);

/* Genuine quiescent issuer reset after a completed injected CANCEL only. */
int wl_evaluation_control_test_source_final_reset_cancel(
    wl_evaluation_control_source_bank_t *, wl_evaluation_control_source_lane_t);
#endif

/* Atomic identity siblings for hosts holding a live controller reference and
 * its stable attached owner. Workers must be drained before FINISH. These
 * mutex-serialized transitions do not authorize concurrent session/native
 * mutation or supply native permission. Their complete bodies are ordinary
 * work, outside the exact-nine qualified interiors. */
typedef enum {
    WL_EVALUATION_CONTROL_ATTEMPT_FINISH_REJECTED = 0,
    WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED,
    WL_EVALUATION_CONTROL_ATTEMPT_FINISH_ALREADY_ENDED,
    WL_EVALUATION_CONTROL_ATTEMPT_FINISH_SUCCESSOR_MISMATCH
} wl_evaluation_control_attempt_finish_kind_t;

typedef struct {
    wl_evaluation_control_attempt_finish_kind_t kind;
    int protocol;
    bool report_valid;
    wl_evaluation_control_report_t report;
} wl_evaluation_control_attempt_finish_result_t;

/* protocol is structural status, separate from report.result. Read report
 * only when report_valid. A matching active generation finishes once; a
 * matching ended generation returns its original report despite new error
 * arguments. A different generation returns SUCCESSOR_MISMATCH without
 * polling, modifying state or exposing a report. This kind also covers an
 * invalid future expectation: it does not prove a successor is alive.
 * FINISH never polls cancellation, matching the generic finish contract. */
wl_evaluation_control_attempt_finish_result_t
wl_evaluation_control_finish_attempt(wl_evaluation_control_t *control,
    const void *owner, uint64_t expected_attempt_id, int execution_status,
    int cleanup_status);

/* Release only the matching, completed generation's attachment. A stale
 * expectation, including an already-ended successor, leaves ownership intact.
 * The caller independently holds the creation reference through this call. */
int wl_evaluation_control_detach_attempt(wl_evaluation_control_t *,
    const void *owner, uint64_t expected_attempt_id);

typedef struct {
    int protocol;
    uint64_t attempt_id;
} wl_evaluation_control_attempt_begin_result_t;

/* Success copies the genuine new ID before releasing the same BEGIN mutex.
 * Every failure returns ID0. Cancellation persistence, limit capture and
 * monotone exhaustion have the same semantics as generic BEGIN. */
wl_evaluation_control_attempt_begin_result_t
wl_evaluation_control_begin_attempt(wl_evaluation_control_t *control,
    const void *owner);

/* Reuse only an exclusively owned private NORMAL financial allocation's
 * exact completed generation. Configuration, cancel reset and next BEGIN are
 * one mutex transition; failures return ID0 and leave every field unchanged.
 * Caller independently proves all callbacks/borrowers have returned. This
 * supplies neither SOURCE provenance nor Native resource permission. */
wl_evaluation_control_attempt_begin_result_t
wl_evaluation_control_private_financier_rebegin(wl_evaluation_control_t *,
    const void *owner, uint64_t allocation_identity, uint64_t completed_id,
    uint64_t positive_limit);

/* Read-only financial proof of an actual inactive positive NORMAL generation
 * and its exact creation+attachment pair. Host independently owns the live
 * allocation and serializes lifecycle/all borrowers through the following
 * callback-free adoption. Success is not a retained permission or a SOURCE/
 * Native capability; no charge, cancel poll, reference or state changes. */
int wl_evaluation_control_private_financier_parked_owned(
    wl_evaluation_control_t *, const void *owner,
    uint64_t allocation_identity, uint64_t completed_id);

/* Additive financial-only transitions. Host serializes lifecycle and proves
 * all workers/borrowers have drained; none supplies Native/SOURCE authority.
 * Output storage is trusted internal caller-owned writable memory, disjoint
 * from the control allocation and all validated owner/id/phase fields. It is
 * written only on success; foreign generations leave it unchanged. */
int wl_evaluation_control_financier_finish_commit(wl_evaluation_control_t *,
    const void *owner, uint64_t expected_id);
int wl_evaluation_control_financier_report_owned(wl_evaluation_control_t *,
    const void *owner, uint64_t expected_id,
    wl_evaluation_control_report_t *out);
/* Positive no-Native construction errors retain generic FINISH precedence.
* SUCCESSOR_MISMATCH classifies only the local constructor result; it conveys
* no report or permission over that foreign active/ended generation.
* Returns strict positive OWN/ALREADY_ENDED/SUCCESSOR_MISMATCH classification;
* negative errno is protocol refusal. Zero is never a successful result. */
int wl_evaluation_control_construction_error_finish_commit(
    wl_evaluation_control_t *, const void *owner, uint64_t expected_id,
    int positive_error);

#endif /* WL_EVALUATION_CONTROL_H */
