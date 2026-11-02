/*
 * columnar/tdd_channel.h - byte-bounded TDD batch channel (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL - not installed, not part of public API.
 *
 * One channel carries the batches W producer lanes publish during one TDD
 * round to a single consumer.  It never looks inside a payload: it moves
 * descriptors and counts the bytes each descriptor declares.
 *
 * Producer protocol (one lane, one thread at a time):
 *   wl_tdd_channel_batch_new()  -> descriptor funded under the governor
 *   wl_tdd_channel_reserve()    -> may block until the bytes fit
 *   wl_tdd_channel_publish()    -> attaches the payload; cannot fail
 * and, on any error path, wl_tdd_channel_reservation_cancel() and
 * wl_tdd_channel_batch_abandon().  The task wrapper always ends a lane with
 * wl_tdd_channel_close_lane(), which reclaims whatever the producer left.
 *
 * Consumer protocol (the coordinator): wl_tdd_channel_pump() until it
 * returns COMPLETE or CANCELLED, wl_tdd_channel_retire() for each batch it
 * is done with, and wl_tdd_channel_discard_all() after the producers have
 * been joined.
 *
 * Admission: a reservation is admitted in ticket order when the queued plus
 * reserved bytes stay within the capacity, or when no bytes are queued or
 * reserved at all, so one batch larger than the capacity still passes.  A
 * reservation of zero bytes is refused, so a batch always holds bytes.  No
 * lane has a fixed share of the capacity.
 *
 * Locking: one mutex guards every mutable field; the governor, the consumer
 * owner, the lane count and the lane array pointer never change after
 * create and are read without it.  No library function calls the governor,
 * frees memory or calls back into the caller while holding the mutex; the
 * test-only wl_tdd_channel_test_wait() runs its predicate under it.
 */
#ifndef WL_COLUMNAR_TDD_CHANNEL_H
#define WL_COLUMNAR_TDD_CHANNEL_H

#include "memory_governor.h"

#include <stdbool.h>
#include <stdint.h>

typedef struct wl_tdd_channel wl_tdd_channel_t;

typedef enum {
    WL_TDD_BATCH_RESIDENT = 0,
    WL_TDD_BATCH_SPILLED = 1,
} wl_tdd_batch_kind_t;

typedef enum {
    WL_TDD_BATCH_PRODUCER = 0, /* owned by its lane, no payload attached */
    WL_TDD_BATCH_QUEUED = 1,   /* in the FIFO, payload attached */
    WL_TDD_BATCH_CONSUMER = 2, /* taken by the consumer */
    WL_TDD_BATCH_RETIRED = 3,
} wl_tdd_batch_state_t;

/*
 * A batch descriptor.  It is heap allocated with a stable address and is its
 * own list node.  The channel sets worker, stratum, round, seq, charge and
 * state, batch_new sets kind to RESIDENT, the producer sets rel_idx and may
 * set flags before publish, and publish attaches payload and rows.  next
 * belongs to the channel from batch_new until the batch is taken (it links
 * the lane's unpublished descriptors, then the queue); once the consumer
 * has taken a batch, next is the consumer's (for its own lists), and it may
 * rewrite payload, kind, charge and flags (for example after spilling).
 */
typedef struct wl_tdd_batch {
    struct wl_tdd_batch *next;
    uint32_t worker;
    uint32_t rel_idx;
    uint32_t stratum;
    uint32_t round;
    uint64_t seq;            /* per-lane publication ordinal */
    wl_tdd_batch_kind_t kind;
    void *payload;           /* NULL until publish */
    uint64_t charge;         /* bytes counted against the channel */
    uint32_t rows;
    uint32_t flags;          /* WL_TDD_BATCH_FLAG_* */
    wl_tdd_batch_state_t state;
    wl_columnar_memory_reservation_t desc_token; /* funds this descriptor */
} wl_tdd_batch_t;

#define WL_TDD_BATCH_FLAG_UNSPILLABLE 0x1u

typedef enum {
    WL_TDD_PUMP_DATA = 0,      /* *out is a CONSUMER batch */
    WL_TDD_PUMP_COMPLETE = 1,  /* every lane closed, every batch taken */
    WL_TDD_PUMP_CANCELLED = 2, /* cancelled and every lane closed */
    WL_TDD_PUMP_ERROR = 3,     /* *err holds the protocol error */
} wl_tdd_pump_result_t;

typedef struct {
    uint64_t takes;
    uint64_t blocked_waits;
    uint64_t max_queued_bytes;
    uint64_t oversize_admits;
    uint64_t orphan_reclaims;
    uint64_t transfer_refused;
    uint64_t rejected_epoch;
    uint64_t admission_bound_violations;
    uint64_t max_takes_while_blocked;
    uint64_t published;
    uint64_t retired;
    uint32_t lanes_open;
    uint32_t lanes_closed;
    uint32_t space_waiters;
    uint64_t queued_bytes;
    uint64_t reserved_bytes;
    uint64_t queued_batches;
    bool cancelled;
    bool consumer_waiting;
    int channel_rc;
} wl_tdd_channel_stats_t;

/*
 * Create a channel for @lanes producer lanes.  @governor may be NULL; when
 * set, each descriptor's bytes are admitted under it.  @consumer_owner is
 * the owner a descriptor's governor token is transferred to when the
 * consumer takes it.  Returns NULL on invalid arguments or allocation
 * failure.
 */
wl_tdd_channel_t *
wl_tdd_channel_create(uint32_t lanes, uint64_t cap_bytes,
    wl_columnar_memory_governor_t *governor, const void *consumer_owner);

/* The bytes wl_tdd_channel_create() allocates for @lanes lanes: the channel
 * object and its lane array.  Returns 0, or EINVAL for zero lanes or a NULL
 * @bytes. */
int
wl_tdd_channel_footprint(uint32_t lanes, uint64_t *bytes);

/* Free the channel.  Returns EBUSY, and frees nothing, while a lane is open,
 * a batch is queued or reserved, or a taken batch has not been retired; 0
 * otherwise. */
int
wl_tdd_channel_destroy(wl_tdd_channel_t *ch);

/* Start a round: reopen every lane and stamp new descriptors with
 * (@stratum, @round).  Returns EBUSY unless every lane is closed and the
 * channel holds no batch or reservation. */
int
wl_tdd_channel_begin_round(wl_tdd_channel_t *ch, uint32_t stratum,
    uint32_t round);

/* Producer: allocate a descriptor for lane @lane, funded under the governor
 * and committed to @owner.  Returns 0, EINVAL, ENOSPC (denied), EOVERFLOW,
 * EBUSY (governor token state), ENOMEM, or EPROTO when the lane is closed. */
int
wl_tdd_channel_batch_new(wl_tdd_channel_t *ch, uint32_t lane,
    uint32_t rel_idx, const void *owner, wl_tdd_batch_t **out);

/* Producer: drop a PRODUCER-state descriptor, cancelling its reservation if
 * it holds one. */
void
wl_tdd_channel_batch_abandon(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch);

/* Producer: reserve @charge bytes for @batch, blocking while they do not
 * fit.  Returns 0, ECANCELED once the channel is cancelled, EINVAL for a
 * zero @charge, or EPROTO when the lane is closed, already holds a
 * reservation, or the descriptor belongs to another round.  Only the lane's
 * producer may reserve for it, one batch at a time, and the lane must not be
 * closed while it waits here. */
int
wl_tdd_channel_reserve(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch,
    uint64_t charge);

/* Producer: as reserve, but returns EAGAIN instead of waiting. */
int
wl_tdd_channel_try_reserve(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch,
    uint64_t charge);

/* Producer: give back the bytes reserved for @batch. */
void
wl_tdd_channel_reservation_cancel(wl_tdd_channel_t *ch,
    wl_tdd_batch_t *batch);

/* Producer: attach @payload and queue @batch.  The batch must hold a
 * reservation; the reserved bytes become its charge.  Cannot fail; a call
 * outside that contract aborts. */
void
wl_tdd_channel_publish(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch,
    void *payload, uint32_t rows);

/* Task wrapper: close lane @lane with @rc.  Reclaims an outstanding
 * reservation and every PRODUCER-state descriptor of the lane; if anything
 * was reclaimed while @rc is 0, the lane records EPROTO, and so does the
 * channel unless it already holds an error. */
void
wl_tdd_channel_close_lane(wl_tdd_channel_t *ch, uint32_t lane, int rc);

/* Coordinator: close lanes [@from, @to) whose tasks never started. */
void
wl_tdd_channel_close_unstarted(wl_tdd_channel_t *ch, uint32_t from,
    uint32_t to);

/* Any thread: wake every waiter; later reservations in this round return
 * ECANCELED.  The channel keeps its first error: EPROTO recorded by an
 * orphaning close_lane, or the @rc of the first cancel, with 0 recorded as
 * ECANCELED.  The next wl_tdd_channel_begin_round() clears both. */
void
wl_tdd_channel_cancel(wl_tdd_channel_t *ch, int rc);

/* Producer task wrapper: whether the channel has been cancelled. */
bool
wl_tdd_channel_cancelled(wl_tdd_channel_t *ch);

/* Consumer: wait for the next event.  On DATA, *out is the taken batch and
 * its governor token is relabelled to the consumer owner (a refused relabel
 * is only counted in the stats).  On ERROR, *err is set.  COMPLETE requires
 * every lane closed, no batch queued or reserved, and every lane's batches
 * all received. */
wl_tdd_pump_result_t
wl_tdd_channel_pump(wl_tdd_channel_t *ch, wl_tdd_batch_t **out, int *err);

/* Consumer: whether a lane closed with a non-zero rc. */
bool
wl_tdd_channel_failed_lanes(wl_tdd_channel_t *ch);

/* Consumer, after the producers are joined: take every queued batch,
 * relabel its token as pump does, and hand it to @sink, which owns it from
 * then on and must retire it, now or later; the channel cannot be destroyed
 * before it has been.  Allocates nothing. */
void
wl_tdd_channel_discard_all(wl_tdd_channel_t *ch,
    void (*sink)(void *ctx, wl_tdd_batch_t *batch), void *ctx);

/* Consumer: free a CONSUMER-state descriptor and release its token.  The
 * payload is the caller's. */
void
wl_tdd_channel_retire(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch);

void
wl_tdd_channel_stats(wl_tdd_channel_t *ch, wl_tdd_channel_stats_t *out);

#ifdef WL_TDD_CHANNEL_TEST_HOOKS
/* Wait until @pred holds over the channel's stats, or @timeout_ms passes
 * without a state change.  @pred runs under the channel mutex on a stats
 * snapshot and must not call into the channel.  Returns 0 or ETIMEDOUT. */
int
wl_tdd_channel_test_wait(wl_tdd_channel_t *ch,
    bool (*pred)(const wl_tdd_channel_stats_t *stats, void *ctx), void *ctx,
    unsigned timeout_ms);
#endif

#endif /* WL_COLUMNAR_TDD_CHANNEL_H */
