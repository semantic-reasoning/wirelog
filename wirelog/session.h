/*
 * session.h - wirelog session management
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

#ifndef WIRELOG_SESSION_H
#define WIRELOG_SESSION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "backend.h"
#include "evaluation_control.h"
#include "wirelog/wirelog-extension.h"

typedef struct wl_session_admission wl_session_admission_t;

/* Persistent financial result only. The matching transient recipe lives in
 * one admitted STEP scope; neither record grants Native permission. */
typedef enum {
    WL_SESSION_SOURCE_METADATA_FINANCIER = 1,
    WL_SESSION_SOURCE_METADATA_CONSTRUCTION,
    WL_SESSION_SOURCE_METADATA_FINANCIER_COMPLETED,
    WL_SESSION_SOURCE_METADATA_FINANCIER_BEGIN_FAILURE,
    WL_SESSION_SOURCE_METADATA_FINANCIER_SNAPSHOT_PRIOR,
    WL_SESSION_SOURCE_METADATA_FINANCIER_PRE_RESOURCE
} wl_session_source_metadata_kind_t;
typedef struct wl_session_source_metadata_recipe
    wl_session_source_metadata_recipe_t;
typedef struct wl_session_source_metadata_result {
    const void *identity, *owner, *admission, *thread_cookie;
    wl_session_t *session;
    uint64_t operation_id, operation_identity;
    unsigned before_phase;
    wl_session_source_metadata_kind_t kind;
    wl_evaluation_control_source_metadata_outcome_t outcome;
    int financial_result, preparation_protocol;
    bool scheduled, ready;
    /* Permanent stable-slot binding. semantic_valid distinguishes an actual
     * prepared tuple from an unperformed early transport refusal. */
    const wl_session_source_metadata_recipe_t *recipe;
    bool semantic_valid;
} wl_session_source_metadata_result_t;
typedef struct {
    wl_session_source_metadata_result_t *result;
    const void *owner;
    wl_evaluation_control_source_action_fn action;
    void *payload;
    bool *busy;
    const unsigned *phase;
    uint64_t operation_id, operation_identity;
    unsigned before_phase;
    wl_session_source_metadata_kind_t kind;
#ifdef WL_SESSION_TEST_HOOKS
    void (*after)(void *);
#endif
} wl_session_source_metadata_request_t;
typedef enum {
    WL_SESSION_SOURCE_METADATA_MAIN,
    WL_SESSION_SOURCE_METADATA_MAINTENANCE,
    WL_SESSION_SOURCE_METADATA_OBSERVER,
    WL_SESSION_SOURCE_METADATA_CONSTRUCTOR,
    WL_SESSION_SOURCE_METADATA_ROLE_COUNT
} wl_session_source_metadata_role_t;
/* Groups0..2 are sealed Core preparation. These extra selectors never supply
 * financial/Native permission; AFTER is diagnostic-only outside BANK. */
enum {
    WL_SESSION_SOURCE_METADATA_SELECT = 3,
    WL_SESSION_SOURCE_METADATA_TAKE,
    WL_SESSION_SOURCE_METADATA_AFTER,
    WL_SESSION_SOURCE_METADATA_CLAIM,
    WL_SESSION_SOURCE_METADATA_BEFORE,
    WL_SESSION_SOURCE_METADATA_RESULT_STORED,
    WL_SESSION_SOURCE_METADATA_OBSERVATION_STOP,
    WL_SESSION_SOURCE_METADATA_PRESENTATION_TAKEN,
    WL_SESSION_SOURCE_METADATA_OFFER_PROVE
};
/* Automatic synchronous preparation, Core prefix first. Dynamic tuple
 * fields are written by group1 before group2 reads them; not zero-filled or
 * retained as a token. No controller/BANK/context/id loan is stored here. */
typedef struct wl_session_source_metadata_preparation {
    wl_evaluation_control_source_metadata_preparation_t source;
    void *financial_record;
    const void *owner;
    uint64_t operation_id, operation_identity;
    wl_session_source_metadata_kind_t kind;
} wl_session_source_metadata_preparation_t;
typedef enum {
    WL_SESSION_SOURCE_METADATA_ACCOUNTING_LEGACY = 0,
    WL_SESSION_SOURCE_METADATA_ACCOUNTING_COLUMNAR_ROLE
} wl_session_source_metadata_accounting_t;
typedef int (*wl_session_source_metadata_resolve_fn)(
    const wl_session_source_metadata_recipe_t *, unsigned,
    wl_session_source_metadata_preparation_t *);
struct wl_session_source_metadata_recipe {
    const wl_session_source_metadata_recipe_t *identity;
    wl_session_t *session;
    const void *root;
    uint64_t root_identity;
    wl_session_source_metadata_role_t role;
    wl_session_source_metadata_result_t *result;
    bool *busy;
    const unsigned *phase;
    wl_session_source_metadata_resolve_fn resolve;
    wl_session_source_metadata_accounting_t accounting_class;
};
/* True only in an ordinary admitted OP frame of an opted-in backend.
 * A live issued context, Snapshot and CLOSE cannot defer another issuer. */
bool wl_session_source_step_metadata_can_defer(const wl_session_t *);
int wl_session_source_step_metadata_select_recipe(wl_session_t *,
    const wl_session_source_metadata_recipe_t *);
/* On1, result.preparation_protocol is the consumed actual prepare refusal;
 * callers propagate it rather than retrying structural errors as BANK stops. */
int wl_session_source_step_metadata_take_recipe(wl_session_t *,
    const wl_session_source_metadata_recipe_t *,
    wl_evaluation_control_source_phase_result_t *, unsigned *);
wl_evaluation_control_source_phase_result_t
wl_session_source_metadata_immediate_recipe(wl_session_t *,
    const wl_session_source_metadata_recipe_t *);
/* Only canonical audited producers own this entry. Protocol belongs to this
 * invocation even when first0 refuses before preparation exists. */
typedef struct {
    wl_evaluation_control_source_phase_result_t phase;
    int preparation_protocol;
} wl_session_source_metadata_role_result_t;
wl_session_source_metadata_role_result_t
wl_session_source_metadata_role_owned(wl_session_t *,
    const wl_session_source_metadata_recipe_t *);
int wl_session_source_metadata_bind_roles(wl_session_t *, void *, unsigned *,
    wl_evaluation_control_source_metadata_bind_fn);
int wl_session_source_metadata_bind_issued(wl_session_t *, void *,
    wl_evaluation_control_source_metadata_bind_prepare_fn,
    wl_evaluation_control_source_metadata_bind_fn,
    wl_evaluation_control_source_metadata_bind_outcome_t *);
/* Caller authenticates its canonical pending initializer, cursor4, actual
 * prior outcome and all four published identity/result links before entry. */
int wl_session_source_metadata_observe_bound_roles(wl_session_t *);

/* 1 selected/taken, 0 no STEP scope/result, negative errno on refusal.
 * Host first proves its real canonical descriptor/disposition. All referenced
 * storage is stable, disjoint, and survives until result consumption. */
int wl_session_source_step_metadata_select(wl_session_t *,
    const wl_session_source_metadata_request_t *);
int wl_session_source_step_metadata_take(wl_session_t *,
    wl_session_source_metadata_result_t *, const void *owner,
    uint64_t operation_id, uint64_t operation_identity,
    wl_session_source_metadata_kind_t,
    wl_evaluation_control_source_phase_result_t *, unsigned *before_phase);
wl_evaluation_control_source_phase_result_t
wl_session_source_metadata_immediate(wl_session_t *,
    const wl_session_source_metadata_request_t *);

#ifdef WL_SESSION_TEST_HOOKS
/* Observe actual private Source construction without loaning its record,
 * BANK, controller or saved generation. */
extern void (*wl_session_test_source_birth_entered)(void);
#endif

/* Lexical SOURCE financial record. Zero-init at its final address, never copy,
* retain in a session/frame/receipt, or use as a BODY funding/native permit. */
typedef struct {
    wl_evaluation_control_t *control;
    uint64_t id, limit, charged;
    bool attached, active;
} wl_session_source_finance_t;

/* Lexical Snapshot observation owner; never copied or exposed to Native work.
 * Bind only the actual outer record, and unbind before its physical release. */
typedef struct wl_session_source_snapshot_observation {
    const struct wl_session_source_snapshot_observation *identity;
    wl_session_t *session;
    const void *scope, *thread_cookie;
    wl_session_source_finance_t *record;
    wl_evaluation_control_t *control;
    uint64_t control_identity;
    bool outer;
} wl_session_source_snapshot_observation_t;
int wl_session_source_snapshot_observation_bind(wl_session_t *,
    wl_session_source_snapshot_observation_t *, wl_session_source_finance_t *);
int wl_session_source_snapshot_observation_unbind(wl_session_t *,
    wl_session_source_snapshot_observation_t *);

typedef struct {
    bool selected;
    int financial_result;
} wl_session_source_snapshot_checkpoint_result_t;
/* Fixed Source-only Columnar families use the actual current outer owner.
 * No callback, financial record or Native permission crosses this interface. */
wl_session_source_snapshot_checkpoint_result_t
wl_session_source_snapshot_checkpoint(wl_session_t *);

typedef wl_session_source_snapshot_checkpoint_result_t
    wl_session_source_step_checkpoint_result_t;
/* Actual nonrecursive source_c1 ordinary Scope only. A nonzero result is
 * genuine refusal (or its owned stopped receipt), never a Native permit. */
wl_session_source_step_checkpoint_result_t
wl_session_source_step_checkpoint(wl_session_t *);

typedef struct {
    wl_evaluation_control_source_phase_result_t phase;
    int protocol, source_result, checkpoint_result;
    bool applied, closed;
} wl_session_source_finance_close_result_t;

typedef struct {
    wl_session_source_finance_close_result_t close;
    wl_evaluation_control_source_pair_result_t phase;
    int protocol;
} wl_session_source_finance_pair_result_t;

/* Paired lexical birth provenance. Before start_checked initialize ONLY
 * identity=NULL. Birth writes all other fields before publishing identity.
 * Never copy, retain, expose to Native work, or outlive the actual record. */
typedef struct wl_session_source_close_scope {
    struct wl_session_source_close_scope *identity;
    wl_session_source_finance_t *record;
    wl_session_t *session;
    const void *thread_cookie;
    wl_evaluation_control_t *control;
    uint64_t control_identity, limit;
    wl_evaluation_control_source_phase_result_t birth_phase;
    wl_session_source_finance_close_result_t result;
    wl_evaluation_control_source_pair_result_t pair_phase;
    bool pair_complete;
} wl_session_source_close_scope_t;

/* Internal persistent exclusion lease used by controlled attempts. Zero-init
 * before first acquire and retain at the same address through release. */
typedef struct wl_session_exclusive_attempt {
    struct wl_session *session;
    const void *owner_cookie;
    struct wl_session_exclusive_attempt *self;
    bool held;
    bool entered;
    bool exit_pending;
} wl_session_exclusive_attempt_t;

/*
 * Note: concrete session types embed wl_session_t as their first field,
 * or allocate a struct where the backend pointer is tracked. For our purposes,
 * wl_session_t just needs to house the backend pointer so wrapper functions
 * can dispatch.
 */
struct wl_session {
    const wl_compute_backend_t *backend;
    /* Owned reference; workers borrow this pointer and never release it. */
    wirelog_extension_snapshot_t *extension_snapshot;
    bool owns_extension_snapshot;
    /*
     * A failed streamed input load leaves the session unsuitable for
     * evaluation.  Keep this internal poison bit until the session is
     * destroyed; otherwise a later snapshot could evaluate truncated facts.
     */
    bool input_load_failed;
    /* Permanent backend input-admission policy, distinct from partial-load
     * poison. Set only on successful immutable sealing; no unseal operation.
     * Lifecycle is externally serialized. Generic loaders refuse before I/O. */
    bool input_sealed;
    /*
     * Heap-owned because columnar worker sessions begin as bitwise copies of
     * the coordinator.  Workers clear both fields and never close or destroy
     * the coordinator's admission state.
     */
    wl_session_admission_t *operation_admission;
    bool owns_operation_admission;
    /* Owner key is operation_admission; copied workers borrow the control. */
    wl_evaluation_control_t *evaluation_control;
    bool owns_evaluation_control;
    /* Selects SOURCE finance only; actual eligibility is always rechecked. */
    bool source_c1_admission;
    /* Descriptive canonical Step address only; presence selects the complete
     * provider callback. It never grants action or Native permission. */
    const void *source_financial_head;
};

/* Internal: lease protects the session between controlled attempt calls.
 * Zero-initialize and retain the record at the same address. Acquire through
 * ACK/release and every action must use the acquiring thread. After an action
 * completes, mark exit pending before leave; retrying leave never repeats the
 * action. The caller may release only after all attempt resources have
 * retired or moved; this gate cannot inspect columnar attempt ownership. */
int wl_session_exclusive_attempt_acquire(wl_session_t *session,
    wl_session_exclusive_attempt_t *attempt);
int wl_session_exclusive_attempt_enter(wl_session_exclusive_attempt_t *attempt);
int wl_session_exclusive_attempt_mark_exit_pending(
    wl_session_exclusive_attempt_t *attempt);
int wl_session_exclusive_attempt_leave(wl_session_exclusive_attempt_t *attempt);
int wl_session_exclusive_attempt_release(
    wl_session_exclusive_attempt_t *attempt);

/* Wrapper functions that delegate to the backend vtable */

/*
 * NOTE: This header is INTERNAL — it is not part of the installed
 * public surface (see install_headers in the top-level meson.build).
 * Public users must use <wirelog/wirelog-easy.h> (or, equivalently, the
 * umbrella <wirelog/wirelog.h>).  The wl_session_* primitives below
 * back the wirelog_easy_* facade and are exercised by in-tree tests; they
 * are not promised to external consumers and may change without notice.
 */

int wl_session_source_finance_start(wl_session_t *,
    wl_session_source_finance_t *,
    const wl_evaluation_control_source_phase_policy_t *, uint64_t);
int wl_session_source_finance_charge(wl_session_source_finance_t *, uint64_t);
int wl_session_source_finance_finish(wl_session_source_finance_t *);
/* Only final locally owned, unexposed active Source destruction. */
int wl_session_source_finance_retire_active_owned(
    wl_session_source_finance_t *);
int wl_session_source_finance_rebegin(wl_session_source_finance_t *);
int wl_session_source_finance_dispose(wl_session_source_finance_t *);
int wl_session_source_finance_start_checked(wl_session_t *,
    wl_session_source_finance_t *,
    const wl_evaluation_control_source_phase_policy_t *, uint64_t,
    wl_session_source_close_scope_t *);

typedef struct {
    wl_evaluation_control_source_early_result_t phase;
    int birth_result;
    bool birth_attempted, bound;
} wl_session_source_checked_birth_outcome_t;

wl_session_source_checked_birth_outcome_t
    wl_session_source_finance_start_checked_early(wl_session_t *,
    wl_session_source_finance_t *,
    const wl_evaluation_control_source_phase_policy_t *, uint64_t,
    wl_session_source_close_scope_t *);

/* Genuine own records are physically closed before return even when BANK
 * rejects before its action; that refusal path appends no footer or commit.
 * Strict malformed-input rejection leaves the real other owner untouched. */
wl_session_source_finance_close_result_t
wl_session_source_finance_close_owned(wl_session_t *,
    wl_session_source_finance_t *, wl_session_source_close_scope_t *);
/* Active-only additive close. The checked private pair remains exclusive
 * through this synchronous activation; no Source/controller is lent out.
 * An A-called strict refusal retains the caller's original lexical owner.
 * Keep that record/scope alive and retire through a correct owning path. */
typedef struct {
    wl_evaluation_control_source_close_early_result_t phase;
    wl_session_source_finance_close_result_t close;
    int protocol, authorizer_result, completion_result;
    bool owner_checked, completion_called, mandatory_unwind_called;
} wl_session_source_finance_close_early_result_t;

wl_session_source_finance_close_early_result_t
wl_session_source_finance_close_early_owned(wl_session_t *,
    wl_session_source_finance_t *, wl_session_source_close_scope_t *);

/* Same actual financial close, then synchronous SOURCE-only metadata under
 * a genuine intervening BANK check. Payload stays valid through return and
 * is disjoint from the retiring Source allocation and lexical pair. Never
 * pass user/native work or retain a BANK/context/controller/ID. Repeated
 * completed calls retrieve the actual cached pair outcome without replay. */
wl_session_source_finance_pair_result_t
    wl_session_source_finance_close_then_metadata(wl_session_t *,
    wl_session_source_finance_t *, wl_session_source_close_scope_t *,
    wl_evaluation_control_source_action_fn, void *);
/* FINAL close is one-shot. The typed caller stores the complete first
 * result before its automatic scope expires, then uses fresh metadata phases
 * for publication/acknowledgment retries. No lexical owner is retained. */
typedef struct {
    int protocol, source_result, checkpoint_result;
    bool applied, closed;
} wl_session_source_final_close_value_t;

typedef struct {
    wl_evaluation_control_source_final_pair_result_t phase;
    wl_session_source_final_close_value_t close, mandatory_unwind;
    int protocol;
    bool mandatory_unwind_called;
} wl_session_source_final_pair_result_t;

/* Preserve these checks in MSVC's default C mode as well as C11. */
#if defined(_MSC_VER)
typedef char wl_session_source_final_close_layout_check_
    [(sizeof(wl_session_source_final_close_value_t) == 16) ? 1 : -1];
typedef char wl_session_source_final_pair_layout_check_
    [(sizeof(wl_session_source_final_pair_result_t) == 56) ? 1 : -1];
#else
_Static_assert(sizeof(wl_session_source_final_close_value_t) == 16,
    "final close value layout");
_Static_assert(sizeof(wl_session_source_final_pair_result_t) == 56,
    "final Session result layout");
#endif

wl_session_source_final_pair_result_t
    wl_session_source_finance_close_then_final_metadata(wl_session_t *,
    wl_session_source_finance_t *, wl_session_source_close_scope_t *,
    wl_evaluation_control_source_action_fn, void *, size_t, void *, size_t);

#ifdef WL_SESSION_TEST_HOOKS
typedef enum {
    WL_SESSION_TEST_COMPLETED_BEFORE_SOURCE_END = 1,
    WL_SESSION_TEST_COMPLETED_ENDPOINT_ACTIVE,
    WL_SESSION_TEST_COMPLETED_ENDPOINT_REVOKED,
    WL_SESSION_TEST_COMPLETED_BEFORE_COMMIT
} wl_session_test_completed_event_t;
/* Receipt is borrowed only for the synchronous callback. No admission or
 * BANK lock is held, and the event supplies no Native or Source permit. */
extern void (*wl_session_test_completed_event)(
    wl_session_test_completed_event_t,
    const wl_session_t *, const wl_backend_controlled_completion_t *,
    wl_backend_completed_source_kind_t, int);
typedef struct {
    bool selected;
    unsigned active_operations;
    uint64_t ticket, operation_identity, completed_attempt_id;
    int result;
} wl_session_test_completed_pin_t;
int wl_session_test_completed_pin_view(const wl_session_t *,
    wl_session_test_completed_pin_t *);
int wl_columnar_session_test_retained_bind_prove(const wl_session_t *,
    const void *);
struct wl_backend_financial_offer;
extern const struct wl_backend_financial_offer *(*
wl_session_test_retained_claim_offer)(
    const struct wl_backend_financial_offer *);
int wl_session_test_source_retained_arm(wl_session_t *);
int wl_session_test_source_retained_view(wl_session_t *, unsigned *,
    wl_evaluation_control_source_financial_view_t *);
int wl_session_test_source_final_reset_cancel(wl_session_t *);
int wl_session_test_source_early_report(wl_session_t *,
    wl_evaluation_control_report_t *);
/* Externally serialized pending STEP observation/reset under admission. */
int wl_session_test_source_pending_reset_cancel(wl_session_t *,
    wl_evaluation_control_report_t *);
int wl_session_test_source_pending_report(wl_session_t *,
    wl_evaluation_control_report_t *);
extern void (*wl_session_test_before_source_observation)(wl_session_t *, bool);
/* Different producer from a BANK metadata action/report. */
extern void (*wl_session_test_metadata_observation_stop)(wl_session_t *, bool,
    int);
extern void (*wl_session_test_before_destroy_seal)(wl_session_t *);
extern void (*wl_session_test_after_destroy_seal)(wl_session_t *);
int wl_session_test_source_unstarted_reset_cancel(wl_session_t *,
    wl_evaluation_control_report_t *);
int wl_session_test_source_parked_close_reset_cancel(wl_session_t *,
    wl_evaluation_control_report_t *);
#endif

/* Caller supplies actual full typed payload/journal extents and a qualified
 * immutable policy. Geometry is financial validation, not Native authority. */
wl_evaluation_control_source_final_result_t
    wl_session_source_final_metadata(wl_session_t *,
    const wl_evaluation_control_source_phase_policy_t *,
    wl_evaluation_control_source_action_fn, void *, size_t, void *, size_t);

/* Financial placement observation only; conveys no Native authority. */
/* Locked Source lifetime pin inspection before any backend state reader. */
bool wl_session_source_retained_pending(const wl_session_t *);
bool wl_session_source_operation_scope_owned(const wl_session_t *session);
bool wl_session_source_snapshot_scope_owned(const wl_session_t *session);
/* Actual checked destroy_prepare financial scope only. Safe under BANK;
 * no admission lock, escaped scope pointer or Native resource permission. */
bool wl_session_source_checked_close_scope_owned(const wl_session_t *session);
typedef struct {
    int protocol;
    uint64_t ticket;
} wl_session_completed_owner_ticket_result_t;
/* Reserve before completed-Tx dependencies. Descriptive identity only. */
wl_session_completed_owner_ticket_result_t
wl_session_completed_owner_ticket_reserve(wl_session_t *,
    uint64_t operation_identity, uint64_t completed_attempt_id);
/* Actual transient Core endpoint only; copied receipts, other threads and
 * calls outside the real callback cannot authenticate this metadata event. */
bool wl_session_source_completed_endpoint_owned(const wl_session_t *,
    const wl_backend_controlled_completion_t *,
    wl_backend_completed_source_kind_t, int source_result);
/* Refusal-only guard for raw backend reentry during Core Source return. */
bool wl_session_completed_postcall_pending(const wl_session_t *);
/* Broader financial CLOSE placement, including the legacy destructor; it
 * confers neither a checked claim nor a foreign Native ownership transfer. */
bool wl_session_source_close_scope_owned(const wl_session_t *session);
int wl_session_source_footer(wl_session_t *);
/* Actual admitted SOURCE scope only. Read callbacks do at most 40 scalar
 * actions and never perform Native work; errors have no direct-read fallback. */
int wl_session_source_admission_read(wl_session_t *,
    wl_evaluation_control_source_action_fn, void *);
/* Actual admitted C1 state observation; separate from the 40-action class. */
int wl_session_source_admission_state_read(wl_session_t *,
    wl_controlled_step_state_t *);
/* Synchronous SOURCE bookkeeping only; no native or user callback work. */
wl_evaluation_control_source_phase_result_t
    wl_session_source_snapshot_metadata(wl_session_t *,
    wl_evaluation_control_source_action_fn, void *);

/* Observer financier close: one FINISH/report, saved-generation detach,
 * creation release or saved-value receipt action. No Native/BODY callback.
 * Also admits narrowly typed failed-constructor Original FINISH and fixed
 * no-owner metadata stages; none may destroy Native objects or call users.
 * Expanded callback/caller envelopes and containing carry are separate. */
wl_evaluation_control_source_phase_result_t
    wl_session_source_operator_close_metadata(wl_session_t *,
    wl_evaluation_control_source_action_fn, void *);

/**
 * wl_session_create:
 * @backend:      The compute backend to use for this session.
 * @plan:         The compiled FFI execution plan.
 * @num_workers:  Number of worker threads to spawn for execution.
 * @out:          (out) Pointer to the newly created session handle on success.
 *
 * Initialize a new execution session using the provided backend vtable.
 *
 * Returns:
 *    0 on success.
 *   -1 on allocation failure or backend initialization error.
 */
int
wl_session_create(const wl_compute_backend_t *backend, const wl_plan_t *plan,
    uint32_t num_workers, wl_session_t **out);

/**
 * wl_session_create_with_snapshot:
 * Create a session while retaining an optional scalar-addon snapshot.
 * The caller retains ownership of its input reference.
 */
int
wl_session_create_with_snapshot(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot, wl_session_t **out);

/* Internal host path.  @options is borrowed for the duration of creation. */
int
wl_session_create_with_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    const wl_session_options_t *options, wl_session_t **out);

int
wl_session_create_with_snapshot_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot,
    const wl_session_options_t *options, wl_session_t **out);

#ifdef WL_SESSION_TEST_HOOKS
/* One-shot actual read-action rejection; zero clears the test fault. */
void wl_session_testhook_refuse_source_read(unsigned after);
void wl_session_testhook_fail_next_admission_lock(void);
void wl_session_testhook_fail_next_admission_unlock(void);
/* Test-only, per-thread default options substituted when a creator passes
 * NULL options.  Borrowed for as long as it stays installed; install NULL
 * to clear.  Never compiled into libwirelog. */
void
wl_session_testhook_set_default_options(const wl_session_options_t *options);
const wl_session_options_t *
wl_session_testhook_default_options(void);

typedef void (*wl_session_testhook_fn)(wl_session_t *session);

void
wl_session_testhook_set_admission_closed(wl_session_testhook_fn fn);
void
wl_session_testhook_admission_closed(wl_session_t *session);
void
wl_session_testhook_set_before_workqueue_drain(wl_session_testhook_fn fn);
void
wl_session_testhook_before_workqueue_drain(wl_session_t *session);
void
wl_session_testhook_set_after_worker_lease_release(wl_session_testhook_fn fn);
void
wl_session_testhook_after_worker_lease_release(wl_session_t *session);
#endif

/**
 * wl_session_destroy:
 * @session:  The session to destroy (NULL-safe).
 *
 * Destroy an active execution session and release all associated resources.
 * Callers must not invoke this function from a session operation or from a
 * tuple/delta callback; such reentrant destruction is unsupported.  The
 * operation admission guard waits for admitted work to finish before backend
 * teardown and therefore cannot safely wait for the operation that called it.
 * Calling this with a parked internal exclusive attempt or controlled STEP
 * continuation is a fatal contract violation; use wl_session_try_destroy()
 * and retry after the continuation completes and is acknowledged.
 */
void
wl_session_destroy(wl_session_t *session);

/* Internal checked destruction for sessions with persistent attempts. The
 * caller must externally synchronize all call entry against destruction. */
int
wl_session_try_destroy(wl_session_t **session_slot);

/**
 * wl_session_insert:
 * @session:   The active execution session.
 * @relation:  The null-terminated name of the relation to receive facts.
 * @data:      Row-major array of int64_t values.
 * @num_rows:  Number of rows to insert.
 * @num_cols:  Number of columns per row.
 *
 * Insert initial input facts into an execution session.
 * Must be called before stepping or execution.
 *
 * Returns:
 *    0 on success.
 *   -1 if the backend or relation is invalid, or if an insertion error occurs.
 */
int
wl_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/* Internal: return the backend's shared admission governor, when available. */
wl_columnar_memory_governor_t *
wl_session_memory_governor(wl_session_t *session);

/**
 * wl_session_make_compound:
 * @session:    The active execution session.
 * @functor:    Compound functor name.
 * @arity:      Number of compound arguments.
 * @args:       Argument values and declared scalar types.
 * @handle_out: (out) Newly allocated session-local handle.
 *
 * Allocate a handle-backed side-tier compound and append its side-relation row.
 *
 * Returns:
 *    0 on success.
 *    ENOSPC if the compound arena is saturated.
 *    EBUSY if the compound arena is temporarily frozen, an observed or plain
 *    step is pending retry, or a delta callback is running.
 *    ENOMEM on allocation failure.
 *    EINVAL or another errno-style value for invalid arguments/backend errors.
 */
int
wl_session_make_compound(wl_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out);

/**
 * wl_session_remove:
 * @session:   The active execution session.
 * @relation:  The null-terminated name of the relation to revoke facts from.
 * @data:      Row-major array of int64_t values.
 * @num_rows:  Number of rows to remove.
 * @num_cols:  Number of columns per row.
 *
 * Remove/retract previously inserted input facts.
 * Note: Support depends on whether the underlying backend is incremental.
 *
 * Returns:
 *    0 on success.
 *   -1 on error or if the backend does not support retraction.
 */
int
wl_session_remove(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * wl_session_step:
 * @session:  The active execution session.
 *
 * Advance an incremental session, evaluating any newly inserted/removed facts.
 * Triggers the delta callback registered via wl_session_set_delta_cb.
 * The columnar backend delivers the net change from the start of the whole
 * step only after evaluation and event preparation succeed. A step that
 * fails evaluation, compaction or event preparation delivers nothing, including
 * for relations it already finished: the error can surface after the outputs
 * have moved, so a non-zero return is not evidence that the outputs are
 * unchanged. The baseline the net change is measured against survives such a
 * failure, so a later successful wl_session_step reports the change relative
 * to the state before the failed step -- retry rather than treat the failure
 * as a lost update. The net change is then measured from that first attempt,
 * not from the retry. Cancelling notifications before the retry completes is
 * the one case retrying cannot recover; see wl_session_set_delta_cb. Until an
 * observed step completes, or stops at its iteration cap, snapshot and nonzero
 * input mutation (including compound construction) return EBUSY. A step with no
 * callback or observer that retains a completion cursor refuses the same input,
 * but a snapshot resumes it. Zero-row insert/remove remain no-ops.
 * Reentrant step, snapshot and input mutation from delta callbacks return EBUSY.
 *
 * Returns:
 *    0 on success.
 *   -1 if the backend does not support incremental stepping or an error occurs.
 */
int
wl_session_step(wl_session_t *session);

/**
 * wl_session_set_delta_cb:
 * @session:    The active execution session.
 * @callback:   The delta callback function to invoke upon state changes.
 * @user_data:  Opaque user data pointer passed to the callback.
 *
 * Register a callback for receiving incremental updates (insertions/removals)
 * on output records as the session advances via wl_session_step.
 * Replacing the callback during delivery affects the remaining events. Setting
 * it to NULL cancels remaining notifications for the current step, even if a
 * callback is registered again before that step completes. Re-enabling then
 * observes subsequent steps. Cancellation does not cancel evaluation: a failed
 * step still requires retry, and once notifications are cancelled that retry
 * reports nothing at all -- neither the changes the cancelled step had already
 * applied nor any the retry newly derives. Re-read the outputs with
 * wl_session_snapshot after the retry completes to resynchronize. Cancelling
 * from inside delivery is different: that step still succeeds and only the
 * events after the cancelling one are dropped. A callback registered after a
 * plain step failed past its entry checks, or stopped at its iteration cap,
 * reports nothing for the step that finishes that evaluation, and observes from
 * the following step. The exception is a session's first evaluation that kept
 * no completion cursor: that step reports the whole model. The same holds for a
 * callback already registered when a snapshot that had to evaluate fails, or a
 * step stops at its iteration cap, on a session that has committed. In each
 * case the committed model a change would be measured from is gone. Read
 * wl_session_snapshot between the two to get a baseline. Callback row/name
 * pointers are valid only for the callback invocation. Destruction from a
 * callback remains unsupported.
 */
void
wl_session_set_delta_cb(wl_session_t *session, wirelog_on_delta_fn callback,
    void *user_data);

/**
 * wl_session_snapshot:
 * @session:    The active execution session.
 * @callback:   The callback function to invoke for each computed output tuple.
 * @user_data:  Opaque user data pointer passed to the callback.
 *
 * Evaluate the current state of the backend as a single batch and yield all
 * output tuples (IDB derivations) to the provided callback synchronously.
 * With attached evaluation control, fresh and normally dirty columnar sessions
 * evaluate and prepare all tuples under one attempt and cumulative allowance.
 * Before publication, a stop delivers no tuples or deltas and preserves the
 * preceding public heads and staged inputs. Reset/raise the control and retry
 * SNAPSHOT; an unrelated retained STEP must instead be completed with STEP.
 * Tuple delivery after the prepaid cutoff is indivisible with respect to
 * control stops; callback-triggered cancellation remains for the next attempt.
 * A snapshot never calls the delta callback, so a later step does not report
 * the changes it evaluated.
 *
 * Returns:
 *    0 on success.
 *   -1 on execution failure or invalid session.
 */
int
wl_session_snapshot(wl_session_t *session, wirelog_on_tuple_fn callback,
    void *user_data);

/* True when the most recent step/snapshot stopped on governed admission
 * denial.  Other ENOSPC and ENOMEM meanings remain untyped at this internal
 * boundary.  One can be reported as a denial when the governor refused anything
 * during the same call: another session sharing it, or an admission this call
 * recovered from.  With evaluation control it can be read while a STEP is
 * parked on EAGAIN or complete; a controlled STEP is typed when its attempt
 * returns something other than EAGAIN, and a new attempt clears it.  While a
 * controlled call is running, including from its own callbacks, it returns
 * false. */
bool
wl_session_budget_denied(wl_session_t *session);

/*
 * Host source inspection admission (Issue #2321).
 *
 * wl_session_inspection_begin() admits a read-only inspection only at a
 * quiescent boundary: it refuses a controlled session (UNSUPPORTED, before
 * touching the admission lock, so no control, fuel or attempt state moves),
 * a call made from a delta callback or while any other operation is active
 * (BUSY: every callback-producing call holds an operation reservation for
 * its whole length), a closing session or a retained controlled attempt
 * (BUSY), and a session whose input is sealed or whose streamed load failed
 * (UNSUPPORTED).  On OK the caller holds one operation reservation and must
 * release it with wl_session_inspection_end().
 *
 * wl_session_source_query() dispatches one request to the backend while the
 * reservation is held.  It never clears budget-denial diagnostics.
 */
wirelog_source_query_status_t
wl_session_inspection_begin(wl_session_t *session);

void
wl_session_inspection_end(wl_session_t *session);

wirelog_source_query_status_t
wl_session_source_query(wl_session_t *session, const wl_source_query_t *query,
    wl_source_query_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_SESSION_H */
