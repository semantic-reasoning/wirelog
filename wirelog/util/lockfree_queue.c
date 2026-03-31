/*
 * lockfree_queue.c - wirelog Lock-Free MPMC Delta Queue
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL - not installed, not part of public API.
 *
 * Implementation: W independent SPSC ring buffers, one per producer thread.
 * The coordinator (sole consumer) polls all W queues round-robin.
 *
 * Memory ordering per SPSC queue:
 *   Enqueue: load head(acquire), write slot, store tail(release)
 *   Dequeue: load tail(acquire), read slot,  store head(release)
 *
 * No mutex, no CAS: 2 atomic loads + 1 atomic store per operation.
 */

#include "lockfree_queue.h"

#ifndef _MSC_VER
#include <stdatomic.h>
#else
#include <windows.h>
/*
 * MSVC C11 mode does not yet provide full <stdatomic.h> for all types.
 * Provide a minimal shim for _Atomic uint32_t using volatile + Interlocked.
 */
typedef volatile uint32_t wl_atomic_u32;
#define WL_ATOMIC_LOAD_RELAXED(p)    (*(p))
#define WL_ATOMIC_LOAD_ACQUIRE(p)    (*(p))
#define WL_ATOMIC_STORE_RELAXED(p, v) (*(p) = (v))
#define WL_ATOMIC_STORE_RELEASE(p, v) \
        do { _WriteBarrier(); *(p) = (v); } while (0)
#endif

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Atomic helpers (unified interface over C11 / MSVC shim)                   */
/* ======================================================================== */

#ifndef _MSC_VER
typedef _Atomic uint32_t wl_atomic_u32;
#define WL_ATOMIC_LOAD_RELAXED(p) \
        atomic_load_explicit((p), memory_order_relaxed)
#define WL_ATOMIC_LOAD_ACQUIRE(p) \
        atomic_load_explicit((p), memory_order_acquire)
#define WL_ATOMIC_STORE_RELAXED(p, v) \
        atomic_store_explicit((p), (v), memory_order_relaxed)
#define WL_ATOMIC_STORE_RELEASE(p, v) \
        atomic_store_explicit((p), (v), memory_order_release)
#endif

/* ======================================================================== */
/* SPSC Ring Buffer                                                           */
/* ======================================================================== */

typedef struct {
    wl_delta_msg_t *slots;    /* ring buffer of elements          */
    uint32_t capacity;        /* number of slots (power of 2)     */
    uint32_t mask;            /* capacity - 1, for index wrapping */

    /*
     * Pad to cache-line boundary (64B on x86-64).
     * Prevents false sharing between tail and head atomics.
     * With W=512 workers, each round-robin dequeue scans 512 tails.
     * Without padding, all tails share cache lines with heads,
     * causing cache-line ping-pong between producer and consumer.
     * Cost: 3 x 64B per ring; benefit: 2-10x throughput improvement.
     */
    uint32_t _pad_to_tail[12];

    /*
     * tail: producer write cursor (64B offset, isolated cache line).
     *   Only the producer increments it.
     *   Published to consumer via release store.
     */
    wl_atomic_u32 tail;
    uint32_t _pad_tail[15];    /* Pad to next cache line */

    /*
     * head: consumer read cursor (128B offset, isolated cache line).
     *   Only the consumer increments it.
     *   Published to producer via release store.
     */
    wl_atomic_u32 head;
    uint32_t _pad_head[15];    /* Pad to avoid next allocation boundary */
} wl_spsc_queue_t;

/* Round v up to the nearest power of 2 (minimum 2). */
static uint32_t
next_pow2(uint32_t v)
{
    if (v < 2)
        v = 2;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1u;
}

static int
spsc_init(wl_spsc_queue_t *q, uint32_t capacity)
{
    capacity = next_pow2(capacity);
    q->slots = (wl_delta_msg_t *)calloc(capacity, sizeof(wl_delta_msg_t));
    if (!q->slots)
        return -1;
    q->capacity = capacity;
    q->mask = capacity - 1u;
    WL_ATOMIC_STORE_RELAXED(&q->tail, 0u);
    WL_ATOMIC_STORE_RELAXED(&q->head, 0u);
    return 0;
}

static void
spsc_destroy(wl_spsc_queue_t *q)
{
    free(q->slots);
    q->slots = NULL;
}

/*
 * spsc_enqueue — called by the producer thread only.
 *
 * Returns 0 on success, -1 if the ring is full.
 */
static int
spsc_enqueue(wl_spsc_queue_t *q, wl_delta_msg_t item)
{
    /* Own cursor: relaxed load (no ordering needed for self-owned index). */
    uint32_t tail = WL_ATOMIC_LOAD_RELAXED(&q->tail);

    /* Peer cursor: acquire load to observe consumer's latest head update. */
    uint32_t head = WL_ATOMIC_LOAD_ACQUIRE(&q->head);

    /* Unsigned subtraction wraps correctly: full when distance >= capacity. */
    if ((tail - head) >= q->capacity)
        return -1;

    q->slots[tail & q->mask] = item;

    /* Release store: makes the written slot visible to the consumer. */
    WL_ATOMIC_STORE_RELEASE(&q->tail, tail + 1u);
    return 0;
}

/*
 * spsc_dequeue — called by the consumer (coordinator) thread only.
 *
 * Returns 1 if an item was dequeued into *out, 0 if empty.
 */
static int
spsc_dequeue(wl_spsc_queue_t *q, wl_delta_msg_t *out)
{
    /* Own cursor: relaxed load. */
    uint32_t head = WL_ATOMIC_LOAD_RELAXED(&q->head);

    /* Peer cursor: acquire load to observe producer's latest tail update. */
    uint32_t tail = WL_ATOMIC_LOAD_ACQUIRE(&q->tail);

    if (head == tail)
        return 0;

    *out = q->slots[head & q->mask];

    /* Release store: makes the freed slot visible to the producer. */
    WL_ATOMIC_STORE_RELEASE(&q->head, head + 1u);
    return 1;
}

/* Approximate size — not synchronized, for diagnostics only. */
static uint32_t
spsc_size(wl_spsc_queue_t *q)
{
    uint32_t tail = WL_ATOMIC_LOAD_ACQUIRE(&q->tail);
    uint32_t head = WL_ATOMIC_LOAD_ACQUIRE(&q->head);
    return tail - head;
}

/* ======================================================================== */
/* MPMC Queue (W SPSC queues + single coordinator consumer)                   */
/* ======================================================================== */

struct wl_mpmc_queue {
    wl_spsc_queue_t *workers;     /* per-worker SPSC rings   */
    uint32_t num_workers;
    uint32_t dequeue_idx;         /* round-robin scan cursor */
};

wl_mpmc_queue_t *
wl_mpmc_queue_create(uint32_t num_workers, uint32_t capacity)
{
    if (num_workers == 0 || capacity < 2)
        return NULL;

    wl_mpmc_queue_t *q = (wl_mpmc_queue_t *)calloc(1, sizeof(wl_mpmc_queue_t));
    if (!q)
        return NULL;

    q->workers = (wl_spsc_queue_t *)calloc(num_workers,
            sizeof(wl_spsc_queue_t));
    if (!q->workers) {
        free(q);
        return NULL;
    }

    q->num_workers = num_workers;
    q->dequeue_idx = 0;

    for (uint32_t i = 0; i < num_workers; i++) {
        if (spsc_init(&q->workers[i], capacity) != 0) {
            for (uint32_t j = 0; j < i; j++)
                spsc_destroy(&q->workers[j]);
            free(q->workers);
            free(q);
            return NULL;
        }
    }

    return q;
}

void
wl_mpmc_queue_destroy(wl_mpmc_queue_t *q)
{
    if (!q)
        return;
    for (uint32_t i = 0; i < q->num_workers; i++)
        spsc_destroy(&q->workers[i]);
    free(q->workers);
    free(q);
}

int
wl_mpmc_enqueue(wl_mpmc_queue_t *q, uint32_t worker_id,
    void *delta, uint32_t stratum)
{
    if (!q || worker_id >= q->num_workers)
        return -1;

    wl_delta_msg_t msg;
    msg.delta = delta;
    msg.stratum = stratum;
    msg.worker_id = worker_id;

    return spsc_enqueue(&q->workers[worker_id], msg);
}

int
wl_mpmc_dequeue(wl_mpmc_queue_t *q, wl_delta_msg_t *out)
{
    if (!q || !out)
        return 0;

    for (uint32_t i = 0; i < q->num_workers; i++) {
        uint32_t idx = (q->dequeue_idx + i) % q->num_workers;
        if (spsc_dequeue(&q->workers[idx], out)) {
            q->dequeue_idx = (idx + 1u) % q->num_workers;
            return 1;
        }
    }
    return 0;
}

uint32_t
wl_mpmc_dequeue_all(wl_mpmc_queue_t *q, wl_delta_msg_t *buf, uint32_t buf_len)
{
    if (!q || !buf || buf_len == 0)
        return 0;

    uint32_t count = 0;
    for (uint32_t w = 0; w < q->num_workers && count < buf_len; w++) {
        wl_delta_msg_t item;
        while (count < buf_len && spsc_dequeue(&q->workers[w], &item))
            buf[count++] = item;
    }
    return count;
}

uint32_t
wl_mpmc_size(wl_mpmc_queue_t *q)
{
    if (!q)
        return 0;
    uint32_t total = 0;
    for (uint32_t i = 0; i < q->num_workers; i++)
        total += spsc_size(&q->workers[i]);
    return total;
}
