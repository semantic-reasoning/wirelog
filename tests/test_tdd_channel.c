/*
 * test_tdd_channel.c - byte-bounded TDD batch channel (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Payloads are fake pointers; the channel never dereferences them.  Every
 * wait is condition based: helpers wait on channel stats with a deadline,
 * and an expired deadline fails the test instead of hanging it.
 */
#include "../wirelog/columnar/tdd_channel.h"
#include "../wirelog/thread.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEADLINE_MS 10000u

static unsigned failures;

static void
check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        failures++;
    }
}

#define REQUIRE(expr)                                                     \
        do {                                                              \
            if (!(expr)) {                                                \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr); \
                abort();                                                  \
            }                                                             \
        } while (0)

static int consumer_owner;
static int producer_owner;

static void
make_governor_mode(wl_columnar_memory_governor_t *governor, uint64_t budget,
    wl_columnar_memory_mode_t mode)
{
    wl_columnar_memory_resolution_t resolution;
    memset(&resolution, 0, sizeof(resolution));
    resolution.budget_bytes = budget;
    resolution.headroom_bytes = 0;
    resolution.usable_bytes = budget;
    resolution.mode = mode;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    REQUIRE(wl_columnar_memory_governor_init(governor, &resolution) == 0);
}

static void
make_governor(wl_columnar_memory_governor_t *governor, uint64_t budget)
{
    make_governor_mode(governor, budget, WL_COLUMNAR_MEMORY_MODE_ENFORCING);
}

static wl_tdd_channel_stats_t
stats_of(wl_tdd_channel_t *ch)
{
    wl_tdd_channel_stats_t s;
    wl_tdd_channel_stats(ch, &s);
    return s;
}

/* Publish one batch of @charge bytes on @lane; returns the reserve rc. */
static int
publish_one(wl_tdd_channel_t *ch, uint32_t lane, uint32_t rel,
    uint64_t charge, void *payload)
{
    wl_tdd_batch_t *batch = NULL;
    int rc = wl_tdd_channel_batch_new(ch, lane, rel, &producer_owner,
            &batch);
    if (rc)
        return rc;
    rc = wl_tdd_channel_reserve(ch, batch, charge);
    if (rc) {
        wl_tdd_channel_batch_abandon(ch, batch);
        return rc;
    }
    wl_tdd_channel_publish(ch, batch, payload, 1);
    return 0;
}

static uintptr_t
fake_payload(uint32_t lane, uint32_t i)
{
    return ((uintptr_t)lane << 16) | (uintptr_t)(i + 1);
}

/* Drain to the terminal result, retiring every batch.  Records lane order. */
typedef struct {
    uint32_t taken;
    uint64_t next_seq[16];
    bool order_ok;
    bool payload_ok;
} drain_t;

static wl_tdd_pump_result_t
drain(wl_tdd_channel_t *ch, drain_t *d)
{
    memset(d, 0, sizeof(*d));
    d->order_ok = true;
    d->payload_ok = true;
    for (;;) {
        wl_tdd_batch_t *batch = NULL;
        int err = 0;
        wl_tdd_pump_result_t r = wl_tdd_channel_pump(ch, &batch, &err);
        if (r != WL_TDD_PUMP_DATA)
            return r;
        REQUIRE(batch->state == WL_TDD_BATCH_CONSUMER);
        if (batch->worker < 16) {
            d->order_ok &= batch->seq == d->next_seq[batch->worker];
            d->payload_ok &= (uintptr_t)batch->payload == fake_payload(
                batch->worker, (uint32_t)batch->seq);
            d->next_seq[batch->worker]++;
        }
        d->taken++;
        wl_tdd_channel_retire(ch, batch);
    }
}

/* ------------------------------------------------------------------ */

static void
test_multi_batch_order(void)
{
    wl_columnar_memory_governor_t governor;
    make_governor(&governor, UINT64_C(1) << 30);
    wl_tdd_channel_t *ch = wl_tdd_channel_create(2, UINT64_MAX, &governor,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 3, 7) == 0);
    for (uint32_t i = 0; i < 3; i++)
        for (uint32_t lane = 0; lane < 2; lane++)
            REQUIRE(publish_one(ch, lane, 5, 10,
                (void *)fake_payload(lane, i)) == 0);
    check(wl_columnar_memory_reserved(&governor) > 0,
        "descriptors are admitted under the governor");
    wl_tdd_channel_close_lane(ch, 0, 0);
    wl_tdd_channel_close_lane(ch, 1, 0);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE, "multi-batch round completes");
    check(d.taken == 6, "every batch is delivered once");
    check(d.order_ok, "each lane's batches arrive in publication order");
    check(d.payload_ok, "payloads arrive with their batches");
    wl_tdd_channel_stats_t s = stats_of(ch);
    check(s.published == 6 && s.retired == 6 && s.takes == 6,
        "publish, take and retire counts match");
    check(s.transfer_refused == 0, "every token moves to the consumer");
    check(!wl_tdd_channel_failed_lanes(ch), "clean lanes do not fail");
    check(wl_columnar_memory_reserved(&governor) == 0,
        "retiring releases every descriptor token");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* ------------------------------------------------------------------ */

typedef struct {
    wl_tdd_channel_t *ch;
    uint32_t lane;
    uint32_t batches;
    uint64_t charge;
    int rc;
    bool close_after;
} producer_t;

static void *
producer_thread(void *arg)
{
    producer_t *p = arg;
    p->rc = 0;
    for (uint32_t i = 0; i < p->batches && p->rc == 0; i++)
        p->rc = publish_one(p->ch, p->lane, 0, p->charge,
                (void *)fake_payload(p->lane, i));
    if (p->close_after)
        wl_tdd_channel_close_lane(p->ch, p->lane, p->rc);
    return NULL;
}

static bool
consumer_waiting_pred(const wl_tdd_channel_stats_t *s, void *ctx)
{
    (void)ctx;
    return s->consumer_waiting && s->lanes_open == 1;
}

typedef struct {
    wl_tdd_channel_t *ch;
    bool closed_before_complete;
    int wait_rc;
} closer_t;

static void *
closer_thread(void *arg)
{
    closer_t *c = arg;
    c->wait_rc = wl_tdd_channel_test_wait(c->ch, consumer_waiting_pred, NULL,
            DEADLINE_MS);
    c->closed_before_complete = true;
    wl_tdd_channel_close_lane(c->ch, 0, 0);
    return NULL;
}

/* An empty FIFO with an open lane is not the end of the round. */
static void
test_empty_is_not_complete(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(1, 100, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    closer_t c = { .ch = ch };
    wl_thread_t t;
    REQUIRE(wl_thread_create(&t, closer_thread, &c) == 0);
    drain_t d;
    wl_tdd_pump_result_t r = drain(ch, &d);
    REQUIRE(wl_thread_join(&t) == 0);
    check(c.wait_rc == 0, "consumer reached its wait with the lane open");
    check(r == WL_TDD_PUMP_COMPLETE && c.closed_before_complete,
        "COMPLETE only after the last lane closes");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* A producer that reserves and returns without publishing must not hang the
 * round, and the loss must surface as EPROTO. */
static void
test_orphan_reclaim(void)
{
    wl_columnar_memory_governor_t governor;
    make_governor(&governor, UINT64_C(1) << 30);
    wl_tdd_channel_t *ch = wl_tdd_channel_create(2, 100, &governor,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    wl_tdd_batch_t *leaked = NULL;
    wl_tdd_batch_t *unreserved = NULL;
    REQUIRE(wl_tdd_channel_batch_new(ch, 0, 0, &producer_owner, &leaked) == 0);
    REQUIRE(wl_tdd_channel_reserve(ch, leaked, 40) == 0);
    REQUIRE(wl_tdd_channel_batch_new(ch, 0, 1, &producer_owner,
        &unreserved) == 0);
    REQUIRE(publish_one(ch, 1, 0, 10, (void *)fake_payload(1, 0)) == 0);
    wl_tdd_channel_close_lane(ch, 0, 0);
    wl_tdd_channel_close_lane(ch, 1, 0);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE,
        "orphaned reservation does not hang the round");
    check(d.taken == 1, "published batches are still delivered");
    wl_tdd_channel_stats_t s = stats_of(ch);
    check(s.orphan_reclaims == 2, "both orphaned descriptors are reclaimed");
    check(s.channel_rc == EPROTO, "an orphan on a clean lane is EPROTO");
    check(wl_tdd_channel_failed_lanes(ch), "the orphaning lane failed");
    check(s.reserved_bytes == 0, "orphaned reservation is returned");
    check(wl_columnar_memory_reserved(&governor) == 0,
        "orphaned descriptor tokens are released");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* ------------------------------------------------------------------ */

typedef struct {
    uint32_t want_waiters;
    uint64_t want_queued;
} blocked_t;

static bool
blocked_pred(const wl_tdd_channel_stats_t *s, void *ctx)
{
    const blocked_t *b = ctx;
    return s->space_waiters >= b->want_waiters
           && s->queued_bytes >= b->want_queued;
}

/* Fast producers, slow consumer: the channel stays within its capacity and
 * each blocked producer is admitted within the stated number of takes. */
static void
test_backpressure(uint32_t lanes)
{
    const uint64_t charge = 16;
    const uint32_t per_lane = 4;
    wl_tdd_channel_t *ch = wl_tdd_channel_create(lanes, 2 * charge, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    producer_t p[8];
    wl_thread_t t[8];
    for (uint32_t w = 0; w < lanes; w++) {
        p[w] = (producer_t){ .ch = ch, .lane = w, .batches = per_lane,
                             .charge = charge, .close_after = true };
        REQUIRE(wl_thread_create(&t[w], producer_thread, &p[w]) == 0);
    }
    /* The consumer starts only once the channel is full and every producer
     * waits: no producer can finish while nothing is taken. */
    blocked_t want = { .want_waiters = lanes, .want_queued = 2 * charge };
    check(wl_tdd_channel_test_wait(ch, blocked_pred, &want, DEADLINE_MS) == 0,
        "every producer blocks on a full channel");
    drain_t d;
    wl_tdd_pump_result_t r = drain(ch, &d);
    for (uint32_t w = 0; w < lanes; w++) {
        REQUIRE(wl_thread_join(&t[w]) == 0);
        check(p[w].rc == 0, "every producer publishes all its batches");
    }
    wl_tdd_channel_stats_t s = stats_of(ch);
    check(r == WL_TDD_PUMP_COMPLETE, "backpressured round completes");
    check(d.taken == lanes * per_lane, "no batch is lost under backpressure");
    check(d.order_ok, "lane order survives backpressure");
    check(s.max_queued_bytes <= 2 * charge, "queued bytes stay within cap");
    check(s.blocked_waits > 0, "backpressure was exercised");
    check(s.admission_bound_violations == 0,
        "each blocked producer is admitted within |FIFO| + W - 1 takes");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* One batch larger than the capacity passes only into an empty channel. */
static void
test_oversize_singleton(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(2, 10, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    REQUIRE(publish_one(ch, 0, 0, 100, (void *)fake_payload(0, 0)) == 0);
    wl_tdd_batch_t *small_batch = NULL;
    REQUIRE(wl_tdd_channel_batch_new(ch, 1, 0, &producer_owner,
        &small_batch) == 0);
    check(wl_tdd_channel_try_reserve(ch, small_batch, 5) == EAGAIN,
        "nothing joins an oversize batch in the channel");
    wl_tdd_batch_t *taken = NULL;
    int err = 0;
    REQUIRE(wl_tdd_channel_pump(ch, &taken, &err) == WL_TDD_PUMP_DATA);
    wl_tdd_channel_retire(ch, taken);
    check(wl_tdd_channel_try_reserve(ch, small_batch, 5) == 0,
        "space returns once the oversize batch is taken");
    wl_tdd_channel_publish(ch, small_batch, (void *)fake_payload(1, 0), 1);
    wl_tdd_channel_close_lane(ch, 0, 0);
    wl_tdd_channel_close_lane(ch, 1, 0);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE && d.taken == 1,
        "oversize round completes");
    wl_tdd_channel_stats_t s = stats_of(ch);
    check(s.oversize_admits == 1, "the oversize admission is counted");
    check(s.max_queued_bytes == 100, "only the singleton exceeded the cap");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* A later small reservation cannot pass a waiting large one. */
static void
test_no_starvation(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(3, 10, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    REQUIRE(publish_one(ch, 0, 0, 6, (void *)fake_payload(0, 0)) == 0);
    producer_t big = { .ch = ch, .lane = 1, .batches = 1, .charge = 8 };
    wl_thread_t t;
    REQUIRE(wl_thread_create(&t, producer_thread, &big) == 0);
    blocked_t want = { .want_waiters = 1, .want_queued = 6 };
    check(wl_tdd_channel_test_wait(ch, blocked_pred, &want, DEADLINE_MS) == 0,
        "the large reservation waits");
    wl_tdd_batch_t *small_batch = NULL;
    REQUIRE(wl_tdd_channel_batch_new(ch, 2, 0, &producer_owner,
        &small_batch) == 0);
    check(wl_tdd_channel_try_reserve(ch, small_batch, 1) == EAGAIN,
        "a small reservation that fits still queues behind a waiter");
    wl_tdd_channel_batch_abandon(ch, small_batch);
    wl_tdd_batch_t *taken = NULL;
    int err = 0;
    REQUIRE(wl_tdd_channel_pump(ch, &taken, &err) == WL_TDD_PUMP_DATA);
    wl_tdd_channel_retire(ch, taken);
    REQUIRE(wl_thread_join(&t) == 0);
    check(big.rc == 0, "the large reservation is admitted after one take");
    for (uint32_t w = 0; w < 3; w++)
        wl_tdd_channel_close_lane(ch, w, 0);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE && d.taken == 1,
        "starvation round completes");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* ------------------------------------------------------------------ */

static bool
all_closed_pred(const wl_tdd_channel_stats_t *s, void *ctx)
{
    (void)ctx;
    return s->lanes_open == 0 && s->space_waiters == 0;
}

static unsigned sunk;

static void
count_sink(void *ctx, wl_tdd_batch_t *batch)
{
    wl_tdd_channel_t *ch = ctx;
    sunk++;
    wl_tdd_channel_retire(ch, batch);
}

/* Cancelling wakes every blocked producer; queued batches are discarded
 * once, after the producers are joined. */
static void
test_cancel_blocked(void)
{
    wl_columnar_memory_governor_t governor;
    make_governor(&governor, UINT64_C(1) << 30);
    const uint32_t lanes = 4;
    wl_tdd_channel_t *ch = wl_tdd_channel_create(lanes, 16, &governor,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    producer_t p[4];
    wl_thread_t t[4];
    for (uint32_t w = 0; w < lanes; w++) {
        p[w] = (producer_t){ .ch = ch, .lane = w, .batches = 3, .charge = 8,
                             .close_after = true };
        REQUIRE(wl_thread_create(&t[w], producer_thread, &p[w]) == 0);
    }
    blocked_t want = { .want_waiters = lanes - 2, .want_queued = 16 };
    check(wl_tdd_channel_test_wait(ch, blocked_pred, &want, DEADLINE_MS) == 0,
        "producers block before the cancel");
    wl_tdd_channel_cancel(ch, ENOSPC);
    /* No take happens until every producer has returned: the cancel alone
     * must release the blocked reservations. */
    check(wl_tdd_channel_test_wait(ch, all_closed_pred, NULL, DEADLINE_MS) == 0,
        "cancel alone releases every blocked producer");
    wl_tdd_batch_t *batch = NULL;
    int err = 0;
    wl_tdd_pump_result_t r;
    unsigned taken = 0;
    while ((r = wl_tdd_channel_pump(ch, &batch, &err)) == WL_TDD_PUMP_DATA) {
        taken++;
        wl_tdd_channel_retire(ch, batch);
    }
    check(r == WL_TDD_PUMP_CANCELLED, "pump ends CANCELLED");
    check(taken == 0, "a cancelled round with every lane closed takes nothing");
    unsigned cancelled = 0;
    for (uint32_t w = 0; w < lanes; w++) {
        REQUIRE(wl_thread_join(&t[w]) == 0);
        cancelled += p[w].rc == ECANCELED;
    }
    check(cancelled >= lanes - 2, "blocked producers return ECANCELED");
    wl_tdd_channel_stats_t before = stats_of(ch);
    sunk = 0;
    wl_tdd_channel_discard_all(ch, count_sink, ch);
    check(before.queued_batches == 2, "the full channel is left to discard");
    check(sunk == before.queued_batches,
        "every queued batch is discarded once");
    wl_tdd_channel_stats_t s = stats_of(ch);
    check(s.channel_rc == ENOSPC, "the first cancel rc is kept");
    check(s.queued_batches == 0 && s.reserved_bytes == 0,
        "nothing is left queued or reserved");
    check(s.retired == s.published, "every published batch is retired");
    check(wl_columnar_memory_reserved(&governor) == 0,
        "cancelled round releases every token");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

static void
test_epoch_and_denial(void)
{
    wl_columnar_memory_governor_t governor;
    make_governor(&governor, 64);
    wl_tdd_channel_t *ch = wl_tdd_channel_create(1, 100, &governor,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 2, 4) == 0);
    wl_tdd_batch_t *batch = NULL;
    check(wl_tdd_channel_batch_new(ch, 0, 0, &producer_owner, &batch)
        == ENOSPC, "an unaffordable descriptor is denied");
    check(wl_columnar_memory_reserved(&governor) == 0,
        "a denied descriptor holds nothing");
    wl_tdd_channel_close_lane(ch, 0, 0);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE, "denied round completes");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);

    ch = wl_tdd_channel_create(1, 100, NULL, &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 2, 4) == 0);
    REQUIRE(wl_tdd_channel_batch_new(ch, 0, 0, &producer_owner, &batch) == 0);
    batch->round = 3; /* a descriptor from another round */
    check(wl_tdd_channel_reserve(ch, batch, 10) == EPROTO,
        "a stale-round descriptor is rejected at reserve");
    check(stats_of(ch).rejected_epoch == 1, "the rejection is recorded");
    wl_tdd_channel_batch_abandon(ch, batch);
    check(wl_tdd_channel_begin_round(ch, 2, 5) == EBUSY,
        "a round cannot start while a lane is open");
    wl_tdd_channel_close_lane(ch, 0, 0);
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE, "epoch round completes");
    check(wl_tdd_channel_begin_round(ch, 2, 5) == 0,
        "the next round starts after completion");
    wl_tdd_channel_close_lane(ch, 0, 0);
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE, "second round completes");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* Contract edges: a zero-byte reservation is refused, and the channel
 * outlives every batch taken from it. */
static void
test_contract_edges(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(1, 10, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    wl_tdd_batch_t *batch = NULL;
    REQUIRE(wl_tdd_channel_batch_new(ch, 0, 0, &producer_owner, &batch) == 0);
    check(wl_tdd_channel_reserve(ch, batch, 0) == EINVAL,
        "a zero-byte reservation is refused");
    check(stats_of(ch).reserved_bytes == 0, "the refusal reserves nothing");
    REQUIRE(wl_tdd_channel_reserve(ch, batch, 4) == 0);
    wl_tdd_channel_publish(ch, batch, (void *)fake_payload(0, 0), 1);
    wl_tdd_channel_close_lane(ch, 0, 0);
    wl_tdd_batch_t *taken = NULL;
    int err = 0;
    REQUIRE(wl_tdd_channel_pump(ch, &taken, &err) == WL_TDD_PUMP_DATA);
    check(wl_tdd_channel_destroy(ch) == EBUSY,
        "destroy refuses while a taken batch is unretired");
    wl_tdd_channel_retire(ch, taken);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE, "contract round completes");
    check(wl_tdd_channel_destroy(ch) == 0, "destroy succeeds after retire");
}

static void
test_unstarted_lanes(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(3, 100, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    REQUIRE(publish_one(ch, 0, 0, 10, (void *)fake_payload(0, 0)) == 0);
    wl_tdd_channel_close_lane(ch, 0, 0);
    wl_tdd_channel_close_unstarted(ch, 1, 3);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE && d.taken == 1,
        "unstarted lanes close the round");
    check(wl_tdd_channel_failed_lanes(ch), "unstarted lanes count as failed");
    check(wl_tdd_channel_destroy(ch) == 0, "closed channel is destroyed");
}

/* The channel keeps its first error, and 0 counts as ECANCELED. */
static void
test_cancel_rc(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(2, 10, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    wl_tdd_channel_cancel(ch, 0);
    wl_tdd_channel_cancel(ch, ENOSPC);
    check(stats_of(ch).channel_rc == ECANCELED,
        "cancel(0) records ECANCELED and keeps it");
    wl_tdd_channel_close_lane(ch, 0, ECANCELED);
    wl_tdd_channel_close_lane(ch, 1, ECANCELED);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_CANCELLED, "cancelled round ends");

    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 1) == 0);
    check(stats_of(ch).channel_rc == 0 && !stats_of(ch).cancelled,
        "begin_round clears the cancel and its error");
    wl_tdd_batch_t *left = NULL;
    REQUIRE(wl_tdd_channel_batch_new(ch, 0, 0, &producer_owner, &left) == 0);
    wl_tdd_channel_close_lane(ch, 0, 0);
    wl_tdd_channel_cancel(ch, ENOSPC);
    check(stats_of(ch).channel_rc == EPROTO,
        "an orphan's EPROTO is kept over a later cancel");
    wl_tdd_channel_close_lane(ch, 1, ECANCELED);
    check(drain(ch, &d) == WL_TDD_PUMP_CANCELLED, "orphan round ends");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* A producer blocked behind another lane's reservation wakes when that lane
 * closes and its reservation is reclaimed. */
static void
test_wake_on_reclaimed_reservation(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(2, 10, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    wl_tdd_batch_t *held = NULL;
    REQUIRE(wl_tdd_channel_batch_new(ch, 0, 0, &producer_owner, &held) == 0);
    REQUIRE(wl_tdd_channel_reserve(ch, held, 8) == 0);
    producer_t p = { .ch = ch, .lane = 1, .batches = 1, .charge = 8,
                     .close_after = true };
    wl_thread_t t;
    REQUIRE(wl_thread_create(&t, producer_thread, &p) == 0);
    blocked_t want = { .want_waiters = 1, .want_queued = 0 };
    check(wl_tdd_channel_test_wait(ch, blocked_pred, &want, DEADLINE_MS) == 0,
        "the second producer waits behind the reservation");
    wl_tdd_channel_close_lane(ch, 0, ECANCELED);
    drain_t d;
    wl_tdd_pump_result_t r = drain(ch, &d);
    REQUIRE(wl_thread_join(&t) == 0);
    check(p.rc == 0 && d.taken == 1,
        "the reclaimed reservation admits the waiting producer");
    check(r == WL_TDD_PUMP_COMPLETE, "reclaim round completes");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* A channel cancelled with a producer waiting serves the next round's
 * waiters again. */
static void
test_reuse_after_cancel(void)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(2, 10, NULL,
            &consumer_owner);
    REQUIRE(ch != NULL);
    for (uint32_t round = 0; round < 2; round++) {
        REQUIRE(wl_tdd_channel_begin_round(ch, 0, round) == 0);
        REQUIRE(publish_one(ch, 0, 0, 8, (void *)fake_payload(0, 0)) == 0);
        producer_t p = { .ch = ch, .lane = 1, .batches = 1, .charge = 8,
                         .close_after = true };
        wl_thread_t t;
        REQUIRE(wl_thread_create(&t, producer_thread, &p) == 0);
        blocked_t want = { .want_waiters = 1, .want_queued = 8 };
        check(wl_tdd_channel_test_wait(ch, blocked_pred, &want,
            DEADLINE_MS) == 0, "a producer waits on the full channel");
        wl_tdd_channel_close_lane(ch, 0, 0);
        drain_t d;
        if (round == 0) {
            wl_tdd_channel_cancel(ch, ECANCELED);
            REQUIRE(wl_thread_join(&t) == 0);
            check(p.rc == ECANCELED, "the waiter is cancelled");
            check(drain(ch, &d) == WL_TDD_PUMP_CANCELLED,
                "the cancelled round ends");
            sunk = 0;
            wl_tdd_channel_discard_all(ch, count_sink, ch);
            check(sunk == 1, "the queued batch is discarded");
        } else {
            wl_tdd_pump_result_t r = drain(ch, &d);
            REQUIRE(wl_thread_join(&t) == 0);
            check(p.rc == 0 && d.taken == 2 && r == WL_TDD_PUMP_COMPLETE,
                "the next round admits its waiter after a take");
        }
    }
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

/* An advisory governor admits descriptors with ADVISORY status. */
static void
test_advisory_governor(void)
{
    wl_columnar_memory_governor_t governor;
    make_governor_mode(&governor, UINT64_C(1) << 30,
        WL_COLUMNAR_MEMORY_MODE_ADVISORY);
    wl_tdd_channel_t *ch = wl_tdd_channel_create(1, 100, &governor,
            &consumer_owner);
    REQUIRE(ch != NULL);
    REQUIRE(wl_tdd_channel_begin_round(ch, 0, 0) == 0);
    check(publish_one(ch, 0, 0, 10, (void *)fake_payload(0, 0)) == 0,
        "an advisory admission funds the descriptor");
    wl_tdd_channel_close_lane(ch, 0, 0);
    drain_t d;
    check(drain(ch, &d) == WL_TDD_PUMP_COMPLETE && d.taken == 1,
        "advisory round completes");
    check(wl_columnar_memory_reserved(&governor) == 0,
        "the advisory token is released");
    REQUIRE(wl_tdd_channel_destroy(ch) == 0);
}

int
main(void)
{
    test_multi_batch_order();
    test_empty_is_not_complete();
    test_orphan_reclaim();
    test_backpressure(1);
    test_backpressure(2);
    test_backpressure(8);
    test_oversize_singleton();
    test_no_starvation();
    test_cancel_blocked();
    test_epoch_and_denial();
    test_unstarted_lanes();
    test_contract_edges();
    test_cancel_rc();
    test_wake_on_reclaimed_reservation();
    test_reuse_after_cancel();
    test_advisory_governor();
    fprintf(stderr, "tdd_channel: %u failure(s)\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
