/*
 * workqueue.c - wirelog Work Queue (Phase B-lite)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL - not installed, not part of public API.
 *
 * Implements the 5-function interface declared in workqueue.h:
 *   wl_workqueue_create, wl_workqueue_submit, wl_workqueue_wait_all,
 *   wl_workqueue_drain, wl_workqueue_destroy.
 *
 * Thread pool design:
 *   - Fixed-size pool of threads, created at create() time.
 *   - Ring buffer of work items protected by a single mutex.
 *   - Workers block on a condition variable until work is available.
 *   - wait_all() blocks until all submitted items complete (barrier).
 *   - drain() executes all pending items synchronously on the caller thread.
 */

#include "workqueue.h"

#include "thread.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Work Item                                                                 */
/* ======================================================================== */

typedef struct {
    void (*fn)(void *ctx);
    void *ctx;
} wl_work_item_t;

/* ======================================================================== */
/* Work Queue (opaque)                                                       */
/* ======================================================================== */

struct wl_work_queue {
    /* Ring buffer of pending work items (dynamically allocated) */
    wl_work_item_t *ring;  /* allocated in create(); size = capacity */
    uint32_t capacity;     /* ring buffer capacity (power of 2)       */
    uint32_t head;         /* next item to dequeue (consumer)         */
    uint32_t tail;         /* next slot to enqueue (producer)         */
    uint32_t count;        /* items currently in ring                 */

    /* Synchronisation */
    mutex_t mutex;
    cond_t work_avail; /* signalled when items are enqueued   */
    cond_t all_done;   /* signalled when in_flight reaches 0  */

    /* Barrier tracking */
    uint32_t submitted; /* total items submitted in current batch */
    uint32_t completed; /* total items completed in current batch */

    /* Shutdown flag */
    bool shutdown;

    /* Thread pool */
    thread_t *threads;
    uint32_t num_workers;
};

/* ======================================================================== */
/* Worker Thread                                                             */
/* ======================================================================== */

static void *
worker_thread(void *arg)
{
    wl_work_queue_t *wq = (wl_work_queue_t *)arg;

    for (;;) {
        wl_work_item_t item;

        mutex_lock(&wq->mutex);

        /* Wait for work or shutdown */
        while (wq->count == 0 && !wq->shutdown)
            cond_wait(&wq->work_avail, &wq->mutex);

        if (wq->shutdown && wq->count == 0) {
            mutex_unlock(&wq->mutex);
            return NULL;
        }

        /* Dequeue one item */
        item = wq->ring[wq->head];
        wq->head = (wq->head + 1) % wq->capacity;
        wq->count--;

        mutex_unlock(&wq->mutex);

        /* Execute outside the lock */
        item.fn(item.ctx);

        /* Signal completion */
        mutex_lock(&wq->mutex);
        wq->completed++;
        if (wq->completed == wq->submitted)
            cond_signal(&wq->all_done);
        mutex_unlock(&wq->mutex);
    }
}

/* ======================================================================== */
/* Public API                                                                */
/* ======================================================================== */

wl_work_queue_t *
wl_workqueue_create(uint32_t num_workers)
{
    if (num_workers == 0)
        return NULL;

    wl_work_queue_t *wq = (wl_work_queue_t *)calloc(1, sizeof(wl_work_queue_t));
    if (!wq)
        return NULL;

    if (mutex_init(&wq->mutex) != 0) {
        free(wq);
        return NULL;
    }
    if (cond_init(&wq->work_avail) != 0) {
        mutex_destroy(&wq->mutex);
        free(wq);
        return NULL;
    }
    if (cond_init(&wq->all_done) != 0) {
        cond_destroy(&wq->work_avail);
        mutex_destroy(&wq->mutex);
        free(wq);
        return NULL;
    }

    wq->threads = (thread_t *)calloc(num_workers, sizeof(thread_t));
    if (!wq->threads) {
        cond_destroy(&wq->all_done);
        cond_destroy(&wq->work_avail);
        mutex_destroy(&wq->mutex);
        free(wq);
        return NULL;
    }

    wq->num_workers = num_workers;

    /* Allocate ring buffer dynamically (Phase 0: support W=512+).
     * Capacity = num_workers * 2, rounded up to power of 2, minimum 256.
     * This ensures a full batch of W items always fits without back-pressure. */
    uint32_t min_cap = num_workers * 2u;
    if (min_cap < 256u)
        min_cap = 256u;

    /* Round up to next power of 2 */
    uint32_t cap = 1u;
    while (cap < min_cap)
        cap <<= 1u;

    wq->ring = (wl_work_item_t *)calloc(cap, sizeof(wl_work_item_t));
    if (!wq->ring) {
        free(wq->threads);
        cond_destroy(&wq->all_done);
        cond_destroy(&wq->work_avail);
        mutex_destroy(&wq->mutex);
        free(wq);
        return NULL;
    }
    wq->capacity = cap;

    for (uint32_t i = 0; i < num_workers; i++) {
        if (thread_create(&wq->threads[i], worker_thread, wq) != 0) {
            /* Partial creation: shut down already-created threads */
            wq->shutdown = true;
            cond_broadcast(&wq->work_avail);
            for (uint32_t j = 0; j < i; j++)
                thread_join(&wq->threads[j]);
            free(wq->ring);
            free(wq->threads);
            cond_destroy(&wq->all_done);
            cond_destroy(&wq->work_avail);
            mutex_destroy(&wq->mutex);
            free(wq);
            return NULL;
        }
    }

    return wq;
}

int
wl_workqueue_submit(wl_work_queue_t *wq, void (*work_fn)(void *ctx), void *ctx)
{
    if (!wq || !work_fn)
        return -1;

    mutex_lock(&wq->mutex);

    if (wq->count >= wq->capacity) {
        mutex_unlock(&wq->mutex);
        return -1; /* ring full */
    }

    wq->ring[wq->tail].fn = work_fn;
    wq->ring[wq->tail].ctx = ctx;
    wq->tail = (wq->tail + 1) % wq->capacity;
    wq->count++;
    wq->submitted++;

    /* Do NOT signal workers here — wl_workqueue_wait_all() broadcasts
     * once all items are queued.  This ensures that wl_workqueue_drain()
     * can dequeue every item on the calling thread without workers racing
     * to steal from the ring between submit() and drain(). */
    mutex_unlock(&wq->mutex);

    return 0;
}

int
wl_workqueue_wait_all(wl_work_queue_t *wq)
{
    if (!wq)
        return -1;

    mutex_lock(&wq->mutex);

    /* Wake all workers now that the batch is fully queued. */
    cond_broadcast(&wq->work_avail);

    while (wq->completed < wq->submitted)
        cond_wait(&wq->all_done, &wq->mutex);

    /* Reset counters for next batch */
    wq->submitted = 0;
    wq->completed = 0;

    mutex_unlock(&wq->mutex);

    return 0;
}

int
wl_workqueue_drain(wl_work_queue_t *wq)
{
    if (!wq)
        return -1;

    /*
     * Execute all pending items synchronously on the calling thread.
     * No lock needed for execution since drain bypasses the thread pool,
     * but we lock to dequeue safely.
     */
    for (;;) {
        wl_work_item_t item;

        mutex_lock(&wq->mutex);
        if (wq->count == 0) {
            /* Reset counters */
            wq->submitted = 0;
            wq->completed = 0;
            mutex_unlock(&wq->mutex);
            return 0;
        }

        item = wq->ring[wq->head];
        wq->head = (wq->head + 1) % wq->capacity;
        wq->count--;
        mutex_unlock(&wq->mutex);

        item.fn(item.ctx);
    }
}

void
wl_workqueue_destroy(wl_work_queue_t *wq)
{
    if (!wq)
        return;

    /* Signal shutdown to all workers */
    mutex_lock(&wq->mutex);
    wq->shutdown = true;
    cond_broadcast(&wq->work_avail);
    mutex_unlock(&wq->mutex);

    /* Join all worker threads */
    for (uint32_t i = 0; i < wq->num_workers; i++)
        thread_join(&wq->threads[i]);

    free(wq->ring);
    free(wq->threads);
    cond_destroy(&wq->all_done);
    cond_destroy(&wq->work_avail);
    mutex_destroy(&wq->mutex);
    free(wq);
}
