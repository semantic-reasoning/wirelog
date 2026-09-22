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
 * Charge before work; charge(0) is a poll. Denial does not consume work.
 * Cancellation precedes a new budget denial; a latched disposition persists.
 * Unlimited accounting overflow is a sticky EOVERFLOW execution error.
 */
int wl_evaluation_control_charge(wl_evaluation_control_t *control,
    uint64_t work);
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

#endif /* WL_EVALUATION_CONTROL_H */
