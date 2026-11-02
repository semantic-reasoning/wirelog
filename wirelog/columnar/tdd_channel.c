/*
 * columnar/tdd_channel.c - byte-bounded TDD batch channel (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * See tdd_channel.h for the protocol.  Every mutable field of the channel
 * is guarded by ch->mu; governor, consumer_owner, nlanes and the lanes
 * pointer never change after create and are read without it.  The library
 * functions reach the governor, free() and caller callbacks only with the
 * mutex released, so the mutex is a leaf in the lock order.  The test-only
 * wl_tdd_channel_test_wait() is the exception: it runs its predicate under
 * the mutex, on a stats snapshot.
 */
#include "tdd_channel.h"

#include "../thread.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_TDD_CHANNEL_TEST_HOOKS
#if defined(WL_HAVE_C11_THREADS) || (!defined(_WIN32) && !defined(_WIN64))
#include <time.h>
#endif
#endif

typedef struct {
    bool closed;
    int close_rc;
    uint64_t published;
    uint64_t received;
    bool waiting;                 /* the lane's producer waits in reserve */
    wl_tdd_batch_t *outstanding;  /* batch holding a reservation, if any */
    wl_tdd_batch_t *producer;     /* PRODUCER-state descriptors */
} wl_tdd_lane_t;

struct wl_tdd_channel {
    wl_mutex_t mu;
    wl_cond_t cond_space;  /* producers wait for space */
    wl_cond_t cond_state;  /* the consumer waits for data or lane closes */
#ifdef WL_TDD_CHANNEL_TEST_HOOKS
    wl_cond_t cond_test;   /* broadcast whenever the stats change */
#endif
    wl_columnar_memory_governor_t *governor;
    const void *consumer_owner;
    uint32_t nlanes;
    wl_tdd_lane_t *lanes;
    uint64_t cap_bytes;
    uint64_t queued_bytes;
    uint64_t reserved_bytes;
    uint64_t queued_batches;
    wl_tdd_batch_t *head;
    wl_tdd_batch_t *tail;
    uint64_t next_ticket;
    uint64_t serving_ticket;
    uint32_t space_waiters;
    uint32_t lanes_open;
    uint32_t stratum;
    uint32_t round;
    bool cancelled;
    bool consumer_waiting;
    int channel_rc;
    wl_tdd_channel_stats_t stats;
};

static void
channel_changed(wl_tdd_channel_t *ch)
{
#ifdef WL_TDD_CHANNEL_TEST_HOOKS
    wl_cond_broadcast(&ch->cond_test);
#else
    (void)ch;
#endif
}

static void
wake_consumer(wl_tdd_channel_t *ch)
{
    if (ch->consumer_waiting)
        wl_cond_signal(&ch->cond_state);
}

wl_tdd_channel_t *
wl_tdd_channel_create(uint32_t lanes, uint64_t cap_bytes,
    wl_columnar_memory_governor_t *governor, const void *consumer_owner)
{
    if (lanes == 0 || cap_bytes == 0)
        return NULL;
    wl_tdd_channel_t *ch = calloc(1, sizeof(*ch));
    if (!ch)
        return NULL;
    ch->lanes = calloc(lanes, sizeof(*ch->lanes));
    if (!ch->lanes) {
        free(ch);
        return NULL;
    }
    if (wl_mutex_init(&ch->mu) != 0)
        goto fail_lanes;
    if (wl_cond_init(&ch->cond_space) != 0)
        goto fail_mu;
    if (wl_cond_init(&ch->cond_state) != 0)
        goto fail_space;
#ifdef WL_TDD_CHANNEL_TEST_HOOKS
    if (wl_cond_init(&ch->cond_test) != 0)
        goto fail_state;
#endif
    ch->governor = governor;
    ch->consumer_owner = consumer_owner;
    ch->nlanes = lanes;
    ch->cap_bytes = cap_bytes;
    /* A new channel has no round: every lane counts as closed. */
    for (uint32_t i = 0; i < lanes; i++)
        ch->lanes[i].closed = true;
    return ch;

#ifdef WL_TDD_CHANNEL_TEST_HOOKS
fail_state:
    wl_cond_destroy(&ch->cond_state);
#endif
fail_space:
    wl_cond_destroy(&ch->cond_space);
fail_mu:
    wl_mutex_destroy(&ch->mu);
fail_lanes:
    free(ch->lanes);
    free(ch);
    return NULL;
}

int
wl_tdd_channel_destroy(wl_tdd_channel_t *ch)
{
    if (!ch)
        return 0;
    wl_mutex_lock(&ch->mu);
    /* A taken batch retires through this channel, so it must not outlive
     * it. */
    bool busy = ch->lanes_open != 0 || ch->head != NULL
        || ch->reserved_bytes != 0 || ch->stats.takes != ch->stats.retired;
    wl_mutex_unlock(&ch->mu);
    if (busy)
        return EBUSY;
#ifdef WL_TDD_CHANNEL_TEST_HOOKS
    wl_cond_destroy(&ch->cond_test);
#endif
    wl_cond_destroy(&ch->cond_state);
    wl_cond_destroy(&ch->cond_space);
    wl_mutex_destroy(&ch->mu);
    free(ch->lanes);
    free(ch);
    return 0;
}

int
wl_tdd_channel_begin_round(wl_tdd_channel_t *ch, uint32_t stratum,
    uint32_t round)
{
    if (!ch)
        return EINVAL;
    wl_mutex_lock(&ch->mu);
    if (ch->lanes_open != 0 || ch->head != NULL || ch->reserved_bytes != 0
        || ch->space_waiters != 0) {
        wl_mutex_unlock(&ch->mu);
        return EBUSY;
    }
    for (uint32_t i = 0; i < ch->nlanes; i++) {
        wl_tdd_lane_t *lane = &ch->lanes[i];
        lane->closed = false;
        lane->close_rc = 0;
        lane->published = 0;
        lane->received = 0;
        lane->waiting = false;
        lane->outstanding = NULL;
        lane->producer = NULL;
    }
    ch->lanes_open = ch->nlanes;
    ch->stratum = stratum;
    ch->round = round;
    ch->cancelled = false;
    ch->channel_rc = 0;
    ch->next_ticket = 0;
    ch->serving_ticket = 0;
    channel_changed(ch);
    wl_mutex_unlock(&ch->mu);
    return 0;
}

/* Release a descriptor's token and free it.  Called without ch->mu. */
static void
batch_free(wl_tdd_batch_t *batch)
{
    if (batch->desc_token.bytes
        && !wl_columnar_memory_release(&batch->desc_token))
        abort();
    free(batch);
}

int
wl_tdd_channel_batch_new(wl_tdd_channel_t *ch, uint32_t lane,
    uint32_t rel_idx, const void *owner, wl_tdd_batch_t **out)
{
    if (!ch || !out || lane >= ch->nlanes)
        return EINVAL;
    *out = NULL;
    wl_tdd_batch_t *batch = calloc(1, sizeof(*batch));
    if (!batch)
        return ENOMEM;
    wl_columnar_memory_reservation_init(&batch->desc_token);
    if (ch->governor) {
        /* Fund the descriptor before any wait, so the consumer never has to
         * allocate or admit anything while producers are blocked. */
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(ch->governor,
                sizeof(*batch), &batch->desc_token);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            free(batch);
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                return ENOSPC;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EBUSY;
        }
        if (!wl_columnar_memory_commit(&batch->desc_token, owner))
            abort();
    }
    batch->worker = lane;
    batch->rel_idx = rel_idx;
    batch->kind = WL_TDD_BATCH_RESIDENT;
    batch->state = WL_TDD_BATCH_PRODUCER;
    wl_mutex_lock(&ch->mu);
    if (ch->lanes[lane].closed) {
        wl_mutex_unlock(&ch->mu);
        batch_free(batch);
        return EPROTO;
    }
    batch->stratum = ch->stratum;
    batch->round = ch->round;
    batch->next = ch->lanes[lane].producer;
    ch->lanes[lane].producer = batch;
    wl_mutex_unlock(&ch->mu);
    *out = batch;
    return 0;
}

/* Unlink @batch from its lane's producer list.  Requires ch->mu. */
static void
lane_unlink_producer(wl_tdd_lane_t *lane, wl_tdd_batch_t *batch)
{
    wl_tdd_batch_t **link = &lane->producer;
    while (*link && *link != batch)
        link = &(*link)->next;
    if (*link)
        *link = batch->next;
    batch->next = NULL;
}

/* Give back @batch's reservation.  Requires ch->mu. */
static void
drop_reservation(wl_tdd_channel_t *ch, wl_tdd_lane_t *lane,
    wl_tdd_batch_t *batch)
{
    if (lane->outstanding != batch)
        return;
    ch->reserved_bytes -= batch->charge;
    batch->charge = 0;
    lane->outstanding = NULL;
    if (ch->space_waiters)
        wl_cond_broadcast(&ch->cond_space);
    wake_consumer(ch);
}

void
wl_tdd_channel_batch_abandon(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch)
{
    if (!ch || !batch)
        return;
    wl_mutex_lock(&ch->mu);
    if (batch->state != WL_TDD_BATCH_PRODUCER || batch->worker >= ch->nlanes)
        abort();
    wl_tdd_lane_t *lane = &ch->lanes[batch->worker];
    drop_reservation(ch, lane, batch);
    lane_unlink_producer(lane, batch);
    channel_changed(ch);
    wl_mutex_unlock(&ch->mu);
    batch_free(batch);
}

/* Whether @charge more bytes fit now.  Requires ch->mu. */
static bool
admissible(const wl_tdd_channel_t *ch, uint64_t charge)
{
    uint64_t used = ch->queued_bytes + ch->reserved_bytes;
    if (used == 0)
        return true; /* an empty channel admits any single batch */
    return used <= ch->cap_bytes && charge <= ch->cap_bytes - used;
}

static void
admit(wl_tdd_channel_t *ch, wl_tdd_lane_t *lane, wl_tdd_batch_t *batch,
    uint64_t charge)
{
    if (charge > ch->cap_bytes)
        ch->stats.oversize_admits++;
    ch->reserved_bytes += charge;
    batch->charge = charge;
    lane->outstanding = batch;
}

static int
reserve_locked(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch, uint64_t charge,
    bool wait)
{
    if (batch->worker >= ch->nlanes || batch->state != WL_TDD_BATCH_PRODUCER)
        return EPROTO;
    wl_tdd_lane_t *lane = &ch->lanes[batch->worker];
    if (ch->cancelled)
        return ECANCELED;
    if (charge == 0)
        return EINVAL;
    if (lane->closed || lane->outstanding)
        return EPROTO;
    if (batch->round != ch->round || batch->stratum != ch->stratum) {
        ch->stats.rejected_epoch++;
        channel_changed(ch);
        return EPROTO;
    }
    /* Nobody may pass a waiting reservation, or a large batch could starve
     * behind a stream of small ones. */
    if (ch->space_waiters == 0 && admissible(ch, charge)) {
        admit(ch, lane, batch, charge);
        channel_changed(ch);
        return 0;
    }
    if (!wait)
        return EAGAIN;

    uint64_t ticket = ch->next_ticket++;
    uint64_t takes_at_block = ch->stats.takes;
    uint64_t bound = ch->queued_batches + (uint64_t)(ch->nlanes - 1);
    ch->space_waiters++;
    ch->stats.blocked_waits++;
    lane->waiting = true;
    channel_changed(ch);
    while (!ch->cancelled
        && !(ticket == ch->serving_ticket && admissible(ch, charge)))
        wl_cond_wait(&ch->cond_space, &ch->mu);
    lane->waiting = false;
    ch->space_waiters--;
    if (ch->cancelled) {
        channel_changed(ch);
        return ECANCELED;
    }
    ch->serving_ticket++;
    uint64_t waited = ch->stats.takes - takes_at_block;
    if (waited > ch->stats.max_takes_while_blocked)
        ch->stats.max_takes_while_blocked = waited;
    if (waited > bound)
        ch->stats.admission_bound_violations++;
    admit(ch, lane, batch, charge);
    /* The next ticket may fit in what is left. */
    if (ch->space_waiters)
        wl_cond_broadcast(&ch->cond_space);
    channel_changed(ch);
    return 0;
}

int
wl_tdd_channel_reserve(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch,
    uint64_t charge)
{
    if (!ch || !batch)
        return EINVAL;
    wl_mutex_lock(&ch->mu);
    int rc = reserve_locked(ch, batch, charge, true);
    wl_mutex_unlock(&ch->mu);
    return rc;
}

int
wl_tdd_channel_try_reserve(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch,
    uint64_t charge)
{
    if (!ch || !batch)
        return EINVAL;
    wl_mutex_lock(&ch->mu);
    int rc = reserve_locked(ch, batch, charge, false);
    wl_mutex_unlock(&ch->mu);
    return rc;
}

void
wl_tdd_channel_reservation_cancel(wl_tdd_channel_t *ch,
    wl_tdd_batch_t *batch)
{
    if (!ch || !batch || batch->worker >= ch->nlanes)
        return;
    wl_mutex_lock(&ch->mu);
    drop_reservation(ch, &ch->lanes[batch->worker], batch);
    channel_changed(ch);
    wl_mutex_unlock(&ch->mu);
}

void
wl_tdd_channel_publish(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch,
    void *payload, uint32_t rows)
{
    if (!ch || !batch || batch->worker >= ch->nlanes)
        abort();
    wl_mutex_lock(&ch->mu);
    wl_tdd_lane_t *lane = &ch->lanes[batch->worker];
    if (batch->state != WL_TDD_BATCH_PRODUCER || lane->outstanding != batch)
        abort();
    lane_unlink_producer(lane, batch);
    lane->outstanding = NULL;
    batch->payload = payload;
    batch->rows = rows;
    batch->seq = lane->published++;
    batch->state = WL_TDD_BATCH_QUEUED;
    ch->reserved_bytes -= batch->charge;
    ch->queued_bytes += batch->charge;
    ch->queued_batches++;
    if (ch->queued_bytes > ch->stats.max_queued_bytes)
        ch->stats.max_queued_bytes = ch->queued_bytes;
    ch->stats.published++;
    batch->next = NULL;
    if (ch->tail)
        ch->tail->next = batch;
    else
        ch->head = batch;
    ch->tail = batch;
    wake_consumer(ch);
    channel_changed(ch);
    wl_mutex_unlock(&ch->mu);
}

/* Close @lane and detach what its producer left behind.  Requires ch->mu.
 * Returns the detached descriptors, which the caller frees unlocked. */
static wl_tdd_batch_t *
close_lane_locked(wl_tdd_channel_t *ch, uint32_t index, int rc)
{
    wl_tdd_lane_t *lane = &ch->lanes[index];
    if (lane->closed)
        return NULL;
    /* Only the lane's own task closes it, after its producer returned, or
     * the coordinator closes a lane whose task never started. */
    if (lane->waiting)
        abort();
    if (lane->outstanding)
        drop_reservation(ch, lane, lane->outstanding);
    wl_tdd_batch_t *orphans = lane->producer;
    lane->producer = NULL;
    uint64_t count = 0;
    for (wl_tdd_batch_t *b = orphans; b; b = b->next)
        count++;
    ch->stats.orphan_reclaims += count;
    if (rc == 0 && count != 0) {
        /* The producer returned success but left a batch unpublished: its
         * delta is lost, so the round must not report success. */
        rc = EPROTO;
        if (ch->channel_rc == 0)
            ch->channel_rc = EPROTO;
    }
    lane->closed = true;
    lane->close_rc = rc;
    ch->lanes_open--;
    ch->stats.lanes_closed++;
    wake_consumer(ch);
    channel_changed(ch);
    return orphans;
}

static void
free_list(wl_tdd_batch_t *list)
{
    while (list) {
        wl_tdd_batch_t *next = list->next;
        batch_free(list);
        list = next;
    }
}

void
wl_tdd_channel_close_lane(wl_tdd_channel_t *ch, uint32_t lane, int rc)
{
    if (!ch || lane >= ch->nlanes)
        return;
    wl_mutex_lock(&ch->mu);
    wl_tdd_batch_t *orphans = close_lane_locked(ch, lane, rc);
    wl_mutex_unlock(&ch->mu);
    free_list(orphans);
}

void
wl_tdd_channel_close_unstarted(wl_tdd_channel_t *ch, uint32_t from,
    uint32_t to)
{
    if (!ch)
        return;
    if (to > ch->nlanes)
        to = ch->nlanes;
    wl_mutex_lock(&ch->mu);
    wl_tdd_batch_t *orphans = NULL;
    for (uint32_t i = from; i < to; i++) {
        wl_tdd_batch_t *left = close_lane_locked(ch, i, ECANCELED);
        while (left) {
            wl_tdd_batch_t *next = left->next;
            left->next = orphans;
            orphans = left;
            left = next;
        }
    }
    wl_mutex_unlock(&ch->mu);
    free_list(orphans);
}

void
wl_tdd_channel_cancel(wl_tdd_channel_t *ch, int rc)
{
    if (!ch)
        return;
    wl_mutex_lock(&ch->mu);
    ch->cancelled = true;
    if (ch->channel_rc == 0)
        ch->channel_rc = rc ? rc : ECANCELED;
    wl_cond_broadcast(&ch->cond_space);
    wl_cond_broadcast(&ch->cond_state);
    channel_changed(ch);
    wl_mutex_unlock(&ch->mu);
}

bool
wl_tdd_channel_cancelled(wl_tdd_channel_t *ch)
{
    if (!ch)
        return true;
    wl_mutex_lock(&ch->mu);
    bool cancelled = ch->cancelled;
    wl_mutex_unlock(&ch->mu);
    return cancelled;
}

/* Pop the FIFO head as a CONSUMER batch.  Requires ch->mu. */
static wl_tdd_batch_t *
take_locked(wl_tdd_channel_t *ch)
{
    wl_tdd_batch_t *batch = ch->head;
    ch->head = batch->next;
    if (!ch->head)
        ch->tail = NULL;
    batch->next = NULL;
    batch->state = WL_TDD_BATCH_CONSUMER;
    ch->queued_bytes -= batch->charge;
    ch->queued_batches--;
    if (batch->worker < ch->nlanes)
        ch->lanes[batch->worker].received++;
    ch->stats.takes++;
    if (ch->space_waiters)
        wl_cond_broadcast(&ch->cond_space);
    channel_changed(ch);
    return batch;
}

wl_tdd_pump_result_t
wl_tdd_channel_pump(wl_tdd_channel_t *ch, wl_tdd_batch_t **out, int *err)
{
    if (out)
        *out = NULL;
    if (err)
        *err = 0;
    if (!ch || !out) {
        if (err)
            *err = EINVAL;
        return WL_TDD_PUMP_ERROR;
    }
    wl_mutex_lock(&ch->mu);
    for (;;) {
        if (ch->cancelled && ch->lanes_open == 0
            && ch->reserved_bytes == 0) {
            wl_mutex_unlock(&ch->mu);
            return WL_TDD_PUMP_CANCELLED;
        }
        if (ch->head) {
            wl_tdd_batch_t *batch = take_locked(ch);
            wl_mutex_unlock(&ch->mu);
            /* The token is relabelled outside the mutex; the label is for
             * attribution, so a refusal is only counted. */
            if (batch->desc_token.bytes
                && !wl_columnar_memory_transfer(&batch->desc_token,
                ch->consumer_owner)) {
                wl_mutex_lock(&ch->mu);
                ch->stats.transfer_refused++;
                channel_changed(ch);
                wl_mutex_unlock(&ch->mu);
            }
            *out = batch;
            return WL_TDD_PUMP_DATA;
        }
        if (ch->lanes_open == 0 && ch->reserved_bytes == 0) {
            /* Defensive: with the FIFO empty every published batch has been
            * taken, so the counts can only differ if this file is wrong. */
            bool balanced = true;
            for (uint32_t i = 0; i < ch->nlanes; i++)
                balanced &= ch->lanes[i].received == ch->lanes[i].published;
            channel_changed(ch);
            wl_mutex_unlock(&ch->mu);
            if (balanced)
                return WL_TDD_PUMP_COMPLETE;
            if (err)
                *err = EPROTO;
            return WL_TDD_PUMP_ERROR;
        }
        ch->consumer_waiting = true;
        channel_changed(ch);
        wl_cond_wait(&ch->cond_state, &ch->mu);
        ch->consumer_waiting = false;
        channel_changed(ch);
    }
}

bool
wl_tdd_channel_failed_lanes(wl_tdd_channel_t *ch)
{
    if (!ch)
        return false;
    bool failed = false;
    wl_mutex_lock(&ch->mu);
    for (uint32_t i = 0; i < ch->nlanes; i++)
        failed |= ch->lanes[i].closed && ch->lanes[i].close_rc != 0;
    wl_mutex_unlock(&ch->mu);
    return failed;
}

void
wl_tdd_channel_discard_all(wl_tdd_channel_t *ch,
    void (*sink)(void *ctx, wl_tdd_batch_t *batch), void *ctx)
{
    if (!ch || !sink)
        return;
    wl_mutex_lock(&ch->mu);
    wl_tdd_batch_t *list = NULL;
    wl_tdd_batch_t **tail = &list;
    while (ch->head) {
        wl_tdd_batch_t *batch = take_locked(ch);
        *tail = batch;
        tail = &batch->next;
    }
    wl_mutex_unlock(&ch->mu);
    while (list) {
        wl_tdd_batch_t *next = list->next;
        list->next = NULL;
        /* As in pump: the consumer now owns the descriptor. */
        if (list->desc_token.bytes
            && !wl_columnar_memory_transfer(&list->desc_token,
            ch->consumer_owner)) {
            wl_mutex_lock(&ch->mu);
            ch->stats.transfer_refused++;
            channel_changed(ch);
            wl_mutex_unlock(&ch->mu);
        }
        sink(ctx, list);
        list = next;
    }
}

void
wl_tdd_channel_retire(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch)
{
    if (!ch || !batch)
        return;
    if (batch->state != WL_TDD_BATCH_CONSUMER)
        abort();
    batch->state = WL_TDD_BATCH_RETIRED;
    wl_mutex_lock(&ch->mu);
    ch->stats.retired++;
    channel_changed(ch);
    wl_mutex_unlock(&ch->mu);
    batch_free(batch);
}

static void
stats_locked(const wl_tdd_channel_t *ch, wl_tdd_channel_stats_t *out)
{
    *out = ch->stats;
    out->lanes_open = ch->lanes_open;
    out->space_waiters = ch->space_waiters;
    out->queued_bytes = ch->queued_bytes;
    out->reserved_bytes = ch->reserved_bytes;
    out->queued_batches = ch->queued_batches;
    out->cancelled = ch->cancelled;
    out->consumer_waiting = ch->consumer_waiting;
    out->channel_rc = ch->channel_rc;
}

void
wl_tdd_channel_stats(wl_tdd_channel_t *ch, wl_tdd_channel_stats_t *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof(*out));
    if (!ch)
        return;
    wl_mutex_lock(&ch->mu);
    stats_locked(ch, out);
    wl_mutex_unlock(&ch->mu);
}

#ifdef WL_TDD_CHANNEL_TEST_HOOKS
static int
channel_timedwait(wl_cond_t *cond, wl_mutex_t *mutex, unsigned timeout_ms)
{
#if defined(WL_HAVE_C11_THREADS) || (!defined(_WIN32) && !defined(_WIN64))
    struct timespec deadline;
    if (timespec_get(&deadline, TIME_UTC) != TIME_UTC)
        deadline = (struct timespec){ 0, 0 };
    deadline.tv_sec += timeout_ms / 1000u;
    deadline.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
#endif
#if defined(WL_HAVE_C11_THREADS)
    return cnd_timedwait(&cond->c, &mutex->m, &deadline) == thrd_success
        ? 0 : -1;
#elif defined(_WIN32) || defined(_WIN64)
    return SleepConditionVariableCS(&cond->cv, &mutex->cs, timeout_ms)
        ? 0 : -1;
#else
    return pthread_cond_timedwait(&cond->c, &mutex->m, &deadline) == 0
        ? 0 : -1;
#endif
}

int
wl_tdd_channel_test_wait(wl_tdd_channel_t *ch,
    bool (*pred)(const wl_tdd_channel_stats_t *stats, void *ctx), void *ctx,
    unsigned timeout_ms)
{
    if (!ch || !pred)
        return EINVAL;
    /* Each wakeup re-arms the full timeout; a wait that never sees the
     * predicate change still ends after one quiet period. */
    wl_mutex_lock(&ch->mu);
    for (;;) {
        wl_tdd_channel_stats_t s;
        stats_locked(ch, &s);
        if (pred(&s, ctx)) {
            wl_mutex_unlock(&ch->mu);
            return 0;
        }
        if (channel_timedwait(&ch->cond_test, &ch->mu, timeout_ms) != 0) {
            stats_locked(ch, &s);
            bool ok = pred(&s, ctx);
            wl_mutex_unlock(&ch->mu);
            return ok ? 0 : ETIMEDOUT;
        }
    }
}
#endif
