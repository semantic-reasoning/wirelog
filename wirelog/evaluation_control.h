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

/* Limit 0 is unlimited. On failure *out is NULL. */
int wl_evaluation_control_create(uint64_t limit, wl_evaluation_control_t **out);
int wl_evaluation_control_retain(wl_evaluation_control_t *control);
void wl_evaluation_control_release(wl_evaluation_control_t *control);
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

#endif /* WL_EVALUATION_CONTROL_H */
