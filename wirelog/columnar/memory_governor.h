/*
 * columnar/memory_governor.h - managed session memory foundation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * INTERNAL HEADER - not installed, not part of the public API.
 */

#ifndef WL_COLUMNAR_MEMORY_GOVERNOR_H
#define WL_COLUMNAR_MEMORY_GOVERNOR_H

#include "columnar/mem_ledger.h"
#include "evaluation_control.h"
#include "thread.h"

#include <stdbool.h>
#include <stdint.h>

#define WL_COLUMNAR_MEMORY_MIN_BUDGET (UINT64_C(256) * 1024 * 1024)
#define WL_COLUMNAR_MEMORY_HEADROOM_CAP (UINT64_C(256) * 1024 * 1024)

typedef enum {
    WL_COLUMNAR_MEMORY_MODE_ADVISORY = 0,
    WL_COLUMNAR_MEMORY_MODE_ENFORCING = 1,
} wl_columnar_memory_mode_t;

typedef enum {
    WL_COLUMNAR_MEMORY_SOURCE_NONE = 0,
    WL_COLUMNAR_MEMORY_SOURCE_ENV,
    WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V2,
    WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V1,
    WL_COLUMNAR_MEMORY_SOURCE_RLIMIT_AS,
    WL_COLUMNAR_MEMORY_SOURCE_WINDOWS_JOB,
} wl_columnar_memory_source_t;

typedef enum {
    WL_COLUMNAR_MEMORY_OK = 0,
    WL_COLUMNAR_MEMORY_INVALID = 1,
    WL_COLUMNAR_MEMORY_UNAVAILABLE = 2,
    WL_COLUMNAR_MEMORY_OVERFLOW = 3,
} wl_columnar_memory_status_t;

/*
 * Testable automatic-limit inputs. An unavailable field means that source did
 * not yield a finite limit. Cgroup values are already the minimum finite
 * values found across that hierarchy; the runtime probe performs that
 * reduction before returning.
 */
typedef struct {
    bool cgroup_v2_available;
    uint64_t cgroup_v2_limit;
    bool cgroup_v1_available;
    uint64_t cgroup_v1_limit;
    bool rlimit_as_available;
    uint64_t rlimit_as_limit;
    bool windows_job_available;
    uint64_t windows_job_limit;
} wl_columnar_memory_sources_t;

typedef struct {
    uint64_t budget_bytes;
    uint64_t headroom_bytes;
    uint64_t usable_bytes;
    wl_columnar_memory_mode_t mode;
    wl_columnar_memory_source_t source;
    wl_columnar_memory_status_t status;
} wl_columnar_memory_resolution_t;

typedef struct {
    wl_atomic_u64 usable_bytes;
    wl_atomic_u64 reserved_bytes;
    uint64_t budget_bytes;
    uint64_t headroom_bytes;
    wl_columnar_memory_mode_t mode;
    wl_columnar_memory_source_t source;
} wl_columnar_memory_governor_t;

typedef struct wl_columnar_memory_governor_ref
    wl_columnar_memory_governor_ref_t;

/* A coordinator-owned lifetime wrapper shared by coordinator and workers. */
wl_columnar_memory_governor_ref_t *
wl_columnar_memory_governor_ref_create(
    const wl_columnar_memory_resolution_t *resolution);

void
wl_columnar_memory_governor_ref_retain(
    wl_columnar_memory_governor_ref_t *ref);

void
wl_columnar_memory_governor_ref_release(
    wl_columnar_memory_governor_ref_t *ref);

wl_columnar_memory_governor_t *
wl_columnar_memory_governor_ref_get(
    wl_columnar_memory_governor_ref_t *ref);

/* True when @ref is held by exactly one owner.  Used by the program-owned
 * intern table (Issue #1469): once the table is the only holder of a
 * session's governor, no live session, worker or result can retain it
 * again, so the table may rebind to the next managed session. */
bool
wl_columnar_memory_governor_ref_is_sole(
    const wl_columnar_memory_governor_ref_t *ref);

typedef enum {
    WL_COLUMNAR_MEMORY_RESERVATION_EMPTY = 0,
    WL_COLUMNAR_MEMORY_RESERVATION_RESERVED = 1,
    WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED = 2,
    WL_COLUMNAR_MEMORY_RESERVATION_RELEASED = 3,
    WL_COLUMNAR_MEMORY_RESERVATION_CLAIMED = 4,
    WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING = 5,
    WL_COLUMNAR_MEMORY_RESERVATION_TRANSFERRING = 6,
    WL_COLUMNAR_MEMORY_RESERVATION_DOWNSIZING = 7,
    WL_COLUMNAR_MEMORY_RESERVATION_RELEASING = 8,
    WL_COLUMNAR_MEMORY_RESERVATION_REPLACEMENT_ADMITTING = 9,
    WL_COLUMNAR_MEMORY_RESERVATION_REPLACING = 10,
    WL_COLUMNAR_MEMORY_RESERVATION_GROWTH_REFUNDING = 11,
    WL_COLUMNAR_MEMORY_RESERVATION_RELEASE_PREPARING = 12,
    WL_COLUMNAR_MEMORY_RESERVATION_RETIREMENT_CLAIMED = 13,
} wl_columnar_memory_reservation_state_t;

typedef enum {
    WL_COLUMNAR_MEMORY_TRANSITION_NONE = 0,
    WL_COLUMNAR_MEMORY_TRANSITION_REPLACEMENT = 1,
    WL_COLUMNAR_MEMORY_TRANSITION_GROWTH = 2,
} wl_columnar_memory_transition_kind_t;

typedef enum {
    WL_COLUMNAR_MEMORY_ADMISSION_OK = 0,
    WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY = 1,
    WL_COLUMNAR_MEMORY_ADMISSION_INVALID = 2,
    WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW = 3,
    WL_COLUMNAR_MEMORY_ADMISSION_DENIED = 4,
} wl_columnar_memory_admission_status_t;

typedef enum {
    WL_COLUMNAR_MEMORY_RELEASE_TRY_OK = 0,
    WL_COLUMNAR_MEMORY_RELEASE_TRY_RETRY,
    WL_COLUMNAR_MEMORY_RELEASE_TRY_INVALID,
    WL_COLUMNAR_MEMORY_RELEASE_TRY_OVERFLOW,
} wl_columnar_memory_release_try_status_t;

typedef struct {
    wl_columnar_memory_governor_t *governor;
    uint64_t bytes;
    uint64_t replacement_bytes;
    /* Private overlap discriminator; read only after claiming REPLACING. */
    uint8_t transition_kind;
    uint64_t release_bytes;
    uint8_t release_from_state;
    uint8_t release_mode;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t release_owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD release_owner_thread;
#else
    pthread_t release_owner_thread;
#endif
    bool release_thread_valid;
    wl_atomic_u64 owner_bits;
    wl_atomic_u64 state;
    /* Tokens are caller-owned and intentionally non-copyable.  A copied
     * token retains the original address and is rejected by every operation,
     * preventing a copied state from releasing the same reservation twice. */
    const void *identity;
} wl_columnar_memory_reservation_t;

#ifdef WL_COLUMNAR_MEMORY_TEST_HOOKS
/* Only compiled into direct-source internal test executables. */
enum {
    WL_COLUMNAR_MEMORY_TEST_RELEASE_BEFORE_CLAIM = 1,
    WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CLAIMED,
    WL_COLUMNAR_MEMORY_TEST_RELEASE_CLAIMED,
    WL_COLUMNAR_MEMORY_TEST_TRANSFER_CLAIMED,
    WL_COLUMNAR_MEMORY_TEST_RELEASE_CREDITED,
    WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CREDITED,
    WL_COLUMNAR_MEMORY_TEST_RELEASE_CREDIT_CAS,
    WL_COLUMNAR_MEMORY_TEST_FINISH_PREPARING,
    WL_COLUMNAR_MEMORY_TEST_FINISH_RELEASING,
};
void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *reservation,
    unsigned phase);
#endif

/* Controlled forward admission requires exclusive ownership of this token;
 * unrelated tokens may concurrently share the same governor. Outputs are valid
 * only when execution rc is zero. Callers propagate a nonzero returned execution
 * rc unchanged. A pending typed failure beats a later negative tail stop, but
 * positive control accounting error before return overrides the typed failure.
 * Interrupted admission restores token fields and refunds only its own added
 * credit; ordinary typed denial retains raw RELEASED payload semantics.
 * Commit polls before irreversible publication and never returns a negative
 * stop after COMMITTED. NULL control delegates to the corresponding raw API.
 * Mandatory refund/release retries are excluded from forward work. Impossible
 * refund failure aborts before restoring any fields or returning normally.
 * Source-region tariffs: checked reserve validation3, claim1 per attempt,
 * snapshot5, prepare4, total round1, publication1 (EMPTY15/RELEASED16).
 * Reserve growth adds validation4. Begin growth is2+1+5+3+round1+3=15.
 * Commit is1+1+2=4; growth commit is1+1+3+4=9. Total-round retries each add1.
 * First total observation follows admission; every region is at most256 units.
 * These are logical source regions, not whole-call latency guarantees. */
int wl_columnar_memory_governor_reserve_checked_controlled(
    wl_columnar_memory_governor_t *, uint64_t,
    wl_columnar_memory_reservation_t *,
    wl_columnar_memory_admission_status_t *, wl_evaluation_control_t *);
int wl_columnar_memory_governor_reserve_growth_controlled(
    wl_columnar_memory_governor_t *, uint64_t, uint64_t,
    wl_columnar_memory_reservation_t *, wl_columnar_memory_admission_status_t *,
    wl_evaluation_control_t *);
int wl_columnar_memory_governor_begin_growth_controlled(
    wl_columnar_memory_reservation_t *, uint64_t,
    wl_columnar_memory_admission_status_t *, wl_evaluation_control_t *);
int wl_columnar_memory_governor_commit_controlled(
    wl_columnar_memory_reservation_t *, const void *, bool *,
    wl_evaluation_control_t *);
int wl_columnar_memory_governor_commit_growth_controlled(
    wl_columnar_memory_reservation_t *, bool *, wl_evaluation_control_t *);

/* Opt-in caller-attempt binding. Requires live control, nonzero matching ID
 * and output; NULL control is EINVAL, with no raw delegation. The original
 * token and output remain exclusively caller-owned. Control lifecycle is
 * externally serialized; token, control and shared governor storage remain
 * live through cleanup, with stable governor configuration. Independent peer
 * reservations may concurrently change the governor's reserved total.
 * Output, token and control storage must not overlap. Each hook
 * checks the caller ID before another hook, poll, claim or admission.
 * Entry correlation is a bounded read-only exception: valid12 slots belong
 * to the first admission retrospectively; invalid/stale (at most13 slots)
 * or first admission refusal can execute with consumed0. No mutation,
 * allocation or pointer traversal precedes admission. It is not prepayment.
 * Outputs are authoritative only with rc0; positive errors beat typed/negative
 * outcomes. Commit has no callback or negative poll after COMMITTED.
 * Physical allocation follows successful admission, never occurs in a hook
 * borrowing the exclusive token. These APIs do not enable engine operators.
 */
int wl_columnar_memory_governor_reserve_checked_attempt_controlled(
    wl_columnar_memory_governor_t *, uint64_t,
    wl_columnar_memory_reservation_t *,
    wl_columnar_memory_admission_status_t *, wl_evaluation_control_t *,
    uint64_t);
int wl_columnar_memory_governor_commit_attempt_controlled(
    wl_columnar_memory_reservation_t *, const void *, bool *,
    wl_evaluation_control_t *, uint64_t);
int wl_columnar_memory_governor_begin_growth_attempt_controlled(
    wl_columnar_memory_reservation_t *, uint64_t,
    wl_columnar_memory_admission_status_t *, wl_evaluation_control_t *,
    uint64_t);

typedef enum {
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_INERT = 0,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CLAIMED,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_READY,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED,
} wl_columnar_memory_governor_growth_commit_phase_t;

/* Caller-owned original address, zero-initialized before first preparation.
 * Noncopyable; caller retains exclusive token and live control/governor until
 * consumed. Explicit reinitialization is legal only with no remaining claim.
 * Outputs/record/token must not overlap. This is a controlled growth-only
 * protocol; existing raw and controlled commit entry points remain available.
 */
typedef struct {
    const void *identity;
    wl_columnar_memory_reservation_t *token;
    wl_columnar_memory_governor_t *governor;
    uint64_t owner_bits;
    wl_evaluation_control_t *control;
    uint64_t active_attempt_id;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
    uint64_t prior_bytes;
    uint64_t additional_bytes;
    uint64_t final_bytes;
    uint64_t prepaid_work;
    wl_columnar_memory_governor_growth_commit_phase_t phase;
} wl_columnar_memory_governor_growth_commit_prepared_t;

/* Bounded rollback receipt for one admitted growth overlap. Each advance
 * performs at most one strong CAS. The receipt and token are noncopyable and
 * remain live until DONE; callers free incoming storage before beginning. */
typedef enum {
    WL_COLUMNAR_MEMORY_GROWTH_REFUND_INERT = 0,
    WL_COLUMNAR_MEMORY_GROWTH_REFUND_VALIDATE,
    WL_COLUMNAR_MEMORY_GROWTH_REFUND_CLAIM,
    WL_COLUMNAR_MEMORY_GROWTH_REFUND_CREDIT,
    WL_COLUMNAR_MEMORY_GROWTH_REFUND_DONE,
} wl_columnar_memory_growth_refund_phase_t;

typedef struct wl_columnar_memory_growth_refund_attempt {
    struct wl_columnar_memory_growth_refund_attempt *identity;
    wl_columnar_memory_reservation_t *token;
    wl_columnar_memory_governor_t *governor;
    const void *owner;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    uint64_t old_bytes;
    uint64_t overlap_bytes;
    wl_columnar_memory_growth_refund_phase_t phase;
    bool thread_valid;
} wl_columnar_memory_growth_refund_attempt_t;

int wl_columnar_memory_growth_refund_attempt_begin(
    wl_columnar_memory_reservation_t *token, const void *owner,
    wl_columnar_memory_growth_refund_attempt_t *attempt);
/* EAGAIN retains the same receipt phase after gate/token/aggregate contention.
 * Success is idempotent only after the receipt reaches DONE. */
int wl_columnar_memory_growth_refund_attempt_advance(
    wl_columnar_memory_growth_refund_attempt_t *attempt);

typedef enum {
    WL_COLUMNAR_MEMORY_RETIRE_ROLLBACK_RESERVED = 1,
    WL_COLUMNAR_MEMORY_RETIRE_RELEASE_COMMITTED = 2,
} wl_columnar_memory_retirement_operation_t;

typedef enum {
    WL_COLUMNAR_MEMORY_RETIREMENT_EMPTY = 0,
    WL_COLUMNAR_MEMORY_RETIREMENT_CLAIM = 1,
    WL_COLUMNAR_MEMORY_RETIREMENT_CAPTURE = 2,
    WL_COLUMNAR_MEMORY_RETIREMENT_CREDIT = 3,
    WL_COLUMNAR_MEMORY_RETIREMENT_DONE = 4,
    WL_COLUMNAR_MEMORY_RETIREMENT_FAILED = 5,
} wl_columnar_memory_retirement_phase_t;

enum {
    WL_COLUMNAR_MEMORY_RETIREMENT_STEP_WORK = 64,
};
typedef char wl_columnar_memory_retirement_step_quantum_bound[
        (WL_COLUMNAR_MEMORY_RETIREMENT_STEP_WORK
        <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

/* Zero-initialize this stable-address, thread-affine receipt. The caller keeps
 * the token and governor alive through acknowledgement and completes physical
 * storage retirement before beginning. Each funded step admits 64
 * conservative work units and advances at most one phase/CAS; the supplied
 * control is never kept. Failed aggregate CAS keeps CREDIT authority for a
 * later funded retry. */
typedef struct {
    const void *identity;
    wl_columnar_memory_reservation_t *token;
    wl_columnar_memory_governor_t *expected_governor;
    uint64_t expected_owner_bits;
    uint64_t captured_bytes;
    wl_columnar_memory_governor_t *captured_governor;
    uint64_t original_state;
    wl_columnar_memory_retirement_operation_t operation;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
    int terminal_status;
    wl_columnar_memory_retirement_phase_t phase;
} wl_columnar_memory_retirement_receipt_t;

/* Bounded post-retirement reduction of a committed reservation. Zero-init
 * the receipt. The caller must have physically retired exactly
 * old_bytes - target_bytes before begin; target_bytes must be positive and
 * smaller than old_bytes because zero-byte tokens use release. The first
 * advance claims the token before reading its mutable payload. Each advance
 * performs at most one strong CAS. The receipt is address/thread-bound; DONE
 * acknowledgment remains valid after the owner later reuses the token. */
typedef enum {
    WL_COLUMNAR_MEMORY_DOWNSIZE_INERT = 0,
    WL_COLUMNAR_MEMORY_DOWNSIZE_CLAIM,
    WL_COLUMNAR_MEMORY_DOWNSIZE_CREDIT,
    WL_COLUMNAR_MEMORY_DOWNSIZE_DONE,
    WL_COLUMNAR_MEMORY_DOWNSIZE_FAILED,
} wl_columnar_memory_downsize_phase_t;

typedef struct wl_columnar_memory_downsize_attempt {
    struct wl_columnar_memory_downsize_attempt *identity;
    wl_columnar_memory_reservation_t *token;
    wl_columnar_memory_governor_t *governor;
    const void *owner;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    uint64_t old_bytes;
    uint64_t target_bytes;
    wl_columnar_memory_downsize_phase_t phase;
    bool thread_valid;
} wl_columnar_memory_downsize_attempt_t;

int wl_columnar_memory_downsize_attempt_begin(
    wl_columnar_memory_reservation_t *token, const void *owner,
    uint64_t target_bytes, wl_columnar_memory_downsize_attempt_t *attempt);
/* EAGAIN advances the claim phase or retains DOWNSIZING after aggregate-credit
 * CAS contention. A pre-claim CAS loss is terminal EBUSY. */
int wl_columnar_memory_downsize_attempt_advance(
    wl_columnar_memory_downsize_attempt_t *attempt);

/* rc0 makes ready authoritative; shape/claim refusal yields false. Invalid
 * pointers/record/attempt yield EINVAL. Stops preserve original reservations.
 * Preparation admits a conservative228 units once (including finish44), then
 * claims COMMITTING reversibly. Early refusal/stop does not refund admission.
 * Invalid original record may consume228 without mutation. Work is admitted,
 * not exact executed operations; successful checkpoint instrumentation ceilings total180 and
 * exclude trailing READY4 + finish44. Largest interval63; trailing48 requires
 * caller's cutoff/composition proof for any additional work before finish.
 * Preclaim unclaimed error return is bounded15 (its final consume/return2
 * use the snapshot dispatch reserve); interval13 labels the success bucket.
 */
int wl_columnar_memory_governor_growth_commit_prepare_controlled(
    wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_governor_t *expected_governor,
    const void *expected_owner,
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared, bool *ready,
    wl_evaluation_control_t *control);
/* Caller-attempt-bound form. Every forward charge and checkpoint uses the
 * supplied ID so a stale receipt cannot debit a later control attempt. */
int wl_columnar_memory_governor_growth_commit_prepare_attempt_controlled(
    wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_governor_t *expected_governor,
    const void *expected_owner,
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared, bool *ready,
    wl_evaluation_control_t *control, uint64_t expected_attempt_id);
/* Mandatory cleanup, also legal after attempt end. Restores REPLACING only:
 * free incoming physical storage before the existing rollback_growth refund.
 */
int wl_columnar_memory_governor_growth_commit_cancel(
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared);
/* Caller establishes final cutoff before finish. Stale/invalid finish leaves
 * READY reachable for original cancellation. Successful finish is finite and
 * performs no charge, callback, mutex, allocation, CAS or governor refund.
 */
int wl_columnar_memory_governor_growth_commit_finish(
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared);

/* Private protocol phase IDs also compile when instrumentation is disabled. */
enum {
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_ARGUMENTS = 10,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_SNAPSHOT,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_PRECLAIM,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_CLAIMED,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_READY_CUTOFF,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_REFUSED,
};

/* Private phase IDs also compile without instrumentation. */
enum {
    WL_COLUMNAR_MEMORY_GOVERNOR_VALIDATED = 1,
    WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
    WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT,
    WL_COLUMNAR_MEMORY_GOVERNOR_PREPARED,
    WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
    WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED,
    WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
    WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF,
    WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT
};
#if defined(WL_SESSION_TEST_HOOKS) || defined(WL_COLUMNAR_MEMORY_TEST_HOOKS)
extern void (*wl_columnar_memory_governor_test_work_checkpoint)(
    wl_evaluation_control_t *, uint64_t);
extern void (*wl_columnar_memory_governor_test_checkpoint)(
    wl_evaluation_control_t *, wl_columnar_memory_reservation_t *, unsigned);
extern void (*wl_columnar_memory_governor_test_before_total_cas)(
    wl_evaluation_control_t *, wl_columnar_memory_governor_t *, uint64_t, bool);
extern void (*wl_columnar_memory_governor_test_after_total_cas)(
    wl_evaluation_control_t *, wl_columnar_memory_governor_t *, bool, bool);
/* Called after every admission that raises a governor's reserved total,
 * with the new total, on the admitting thread; tests use it to record a
 * high-water mark (#1370).  Set and clear it only while no other thread can
 * admit to any governor.  It runs after the total is published but before
 * the reservation's own state is, so the callback must not call back into
 * the governor. */
extern void (*wl_columnar_memory_governor_test_admitted)(
    wl_columnar_memory_governor_t *, uint64_t);
extern void (*wl_columnar_memory_growth_refund_test_before_credit_cas)(
    wl_columnar_memory_governor_t *);
extern void (*wl_columnar_memory_downsize_test_before_credit_cas)(
    wl_columnar_memory_governor_t *);
#endif

/* Initialize a caller-owned token before its first reserve or reuse.
 * Initialization and move require exclusive access to both token objects.
 * Direct reads of non-atomic token payload require external synchronization. */
void
wl_columnar_memory_reservation_init(
    wl_columnar_memory_reservation_t *reservation);

/* Move an active non-replacing token to another caller-owned storage
 * location. A replacement token must be committed or rolled back first. */
bool
wl_columnar_memory_reservation_move(
    wl_columnar_memory_reservation_t *destination,
    wl_columnar_memory_reservation_t *source);

const char *
wl_columnar_memory_source_name(wl_columnar_memory_source_t source);

/*
 * Fill sources from the current process limits. This function is best effort:
 * no finite supported source is a valid result and is represented by all
 * availability flags being false. It never treats physical RAM alone as an
 * enforcing limit.
 */
wl_columnar_memory_status_t
wl_columnar_memory_probe_sources(wl_columnar_memory_sources_t *sources);

/*
 * Query a host-owned Windows Job Object when session integration has a job
 * handle. The opaque handle keeps this internal header portable; NULL or a
 * platform without Job Objects returns unavailable.
 */
wl_columnar_memory_status_t
wl_columnar_memory_probe_windows_job(void *job_handle,
    wl_columnar_memory_sources_t *sources);

/*
 * Resolve a decimal WIRELOG_MEMORY_BUDGET value. A non-NULL env_value is an
 * explicit configuration, including an empty string, and is validated strictly.
 * A NULL value selects the effective minimum from sources and falls back to
 * advisory/unbounded mode when no source is finite.
 */
wl_columnar_memory_status_t
wl_columnar_memory_resolve(const char *env_value,
    const wl_columnar_memory_sources_t *sources,
    wl_columnar_memory_resolution_t *out);

wl_columnar_memory_status_t
wl_columnar_memory_governor_init(wl_columnar_memory_governor_t *governor,
    const wl_columnar_memory_resolution_t *resolution);

/*
 * Reservations are tokenized so release/rollback are idempotent and cannot
 * release another reservation's bytes. A token is caller-owned and must remain
 * alive until its terminal operation returns.
 */
bool
wl_columnar_memory_reserve(wl_columnar_memory_governor_t *governor,
    uint64_t bytes, wl_columnar_memory_reservation_t *reservation);

/* Checked admission result used by allocation sites. */
wl_columnar_memory_admission_status_t
wl_columnar_memory_reserve_checked(wl_columnar_memory_governor_t *governor,
    uint64_t bytes, wl_columnar_memory_reservation_t *reservation);

/* Reserve the new footprint while the old footprint is still live.
 * The output token must be distinct from the token that accounts old_bytes;
 * callers release the old token only after publishing the replacement. */
wl_columnar_memory_admission_status_t
wl_columnar_memory_reserve_growth(wl_columnar_memory_governor_t *governor,
    uint64_t old_bytes, uint64_t new_bytes,
    wl_columnar_memory_reservation_t *reservation);

/* Admit the replacement footprint while the committed footprint remains
 * live.  The token keeps the old logical size until commit or rollback. */
wl_columnar_memory_admission_status_t
wl_columnar_memory_begin_replacement(
    wl_columnar_memory_reservation_t *reservation, uint64_t new_bytes);

/* Publish or discard a replacement admitted by begin_replacement(). */
bool
wl_columnar_memory_commit_replacement(
    wl_columnar_memory_reservation_t *reservation);

bool
wl_columnar_memory_rollback_replacement(
    wl_columnar_memory_reservation_t *reservation);

/* Temporarily charge additional bytes on a committed token while its old
 * physical image remains live. Commit keeps O+N, rollback restores O. */
wl_columnar_memory_admission_status_t
wl_columnar_memory_begin_growth(
    wl_columnar_memory_reservation_t *reservation, uint64_t additional_bytes);

bool
wl_columnar_memory_commit_growth(
    wl_columnar_memory_reservation_t *reservation);

bool
wl_columnar_memory_rollback_growth(
    wl_columnar_memory_reservation_t *reservation);

bool
wl_columnar_memory_size_add(uint64_t left, uint64_t right, uint64_t *out);

bool
wl_columnar_memory_size_mul(uint64_t left, uint64_t right, uint64_t *out);

bool
wl_columnar_memory_commit(wl_columnar_memory_reservation_t *reservation,
    const void *owner);

bool
wl_columnar_memory_transfer(wl_columnar_memory_reservation_t *reservation,
    const void *owner);

/* Credit storage already released by the owner of a committed token.
 * Only positive sizes <= the current size are accepted; equality is a no-op.
 * Zero requires explicit release after freeing the remaining storage.
 * Serialized with release/transfer/downsize; busy or invalid tokens return
 * false without mutation. This does not establish storage-reader safety or
 * admit overlapping replacement allocations. Owner and identity are retained. */
bool
wl_columnar_memory_reservation_downsize(
    wl_columnar_memory_reservation_t *reservation, uint64_t bytes);

bool
wl_columnar_memory_rollback(
    wl_columnar_memory_reservation_t *reservation);

/* Release or roll back a reservation with at most one reservation-state or
 * governor-accounting CAS per call. The first call claims RELEASE_PREPARING,
 * snapshots a receipt, and publishes RELEASING without crediting the governor;
 * later calls perform one credit CAS each. The continuation is thread-affine
 * to the thread that claimed it, and callers must serialize calls for a token.
 * EAGAIN and done=false retain the token. */
int
wl_columnar_memory_reservation_finish_step(
    wl_columnar_memory_reservation_t *reservation, bool rollback,
    bool *done);

bool
wl_columnar_memory_release(wl_columnar_memory_reservation_t *reservation);

/* Begin reads only immutable token identity. A funded step claims the expected
 * state before reading mutable payload; one aggregate CAS contention retains
 * CREDIT authority for a later step. DONE must be acknowledged before receipt
 * reuse. */
int
wl_columnar_memory_retirement_begin(
    wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_governor_t *expected_governor,
    const void *expected_owner,
    wl_columnar_memory_retirement_operation_t operation,
    wl_columnar_memory_retirement_receipt_t *receipt);

int
wl_columnar_memory_retirement_step(
    wl_columnar_memory_retirement_receipt_t *receipt,
    wl_evaluation_control_t *control, uint64_t expected_attempt_id);

int
wl_columnar_memory_retirement_status(
    const wl_columnar_memory_retirement_receipt_t *receipt,
    wl_columnar_memory_retirement_phase_t *phase, bool *owns_claim,
    int *terminal_status);

int
wl_columnar_memory_retirement_acknowledge(
    wl_columnar_memory_retirement_receipt_t *receipt);

/* Non-spinning release for resumable cleanup. Makes at most one token-state
 * CAS and one aggregate-credit CAS. RETRY denotes contention; INVALID and
 * OVERFLOW denote terminal token/accounting shape errors. Supports RESERVED,
 * COMMITTED, and REPLACING tokens. RETRY leaves the original token live and
 * unchanged so a caller retaining exclusive token/governor lifetime can try
 * again later. Does not release physical storage. */
wl_columnar_memory_release_try_status_t
wl_columnar_memory_release_try(wl_columnar_memory_reservation_t *reservation);

uint64_t
wl_columnar_memory_reserved(const wl_columnar_memory_governor_t *governor);

#endif /* WL_COLUMNAR_MEMORY_GOVERNOR_H */
