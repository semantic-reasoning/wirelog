/*
 * lockfree_queue.h - wirelog Lock-Free MPSC Delta Queue
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * ========================================================================
 * Overview
 * ========================================================================
 *
 * Lock-free multi-producer, single-consumer (MPSC) queue for delivering
 * delta messages from worker threads to a coordinator thread.
 *
 * Implemented as W independent SPSC ring buffers (one per worker thread).
 * The coordinator polls all W queues in round-robin order.
 *
 * ========================================================================
 * Thread Safety Contract
 * ========================================================================
 *
 *   wl_mpsc_enqueue(worker_id=i): call ONLY from the thread that owns i.
 *   wl_mpsc_dequeue / wl_mpsc_dequeue_all: call ONLY from the coordinator.
 *   wl_mpsc_size: approximate; safe from any thread for diagnostics only.
 *   wl_mpsc_queue_create / _destroy: call before spawning or after joining
 *     all threads (not concurrent with other queue operations).
 *
 * Violating these constraints produces data races (undefined behaviour).
 *
 * ========================================================================
 * Algorithm
 * ========================================================================
 *
 * Each SPSC ring buffer uses two monotonically-increasing unsigned cursors:
 *   tail  (producer write cursor) — only the producer updates it
 *   head  (consumer read cursor)  — only the consumer updates it
 *
 * Memory ordering:
 *   Enqueue: load head(acquire), write slot, store tail(release)
 *   Dequeue: load tail(acquire), read slot, store head(release)
 *
 * No CAS, no mutex — 2 atomic ops per enqueue, 2 per dequeue.
 * ABA-free: cursors are monotone integers; no pointer-value comparison.
 */

#ifndef WL_LOCKFREE_QUEUE_H
#define WL_LOCKFREE_QUEUE_H

#include <stdint.h>

/**
 * wl_delta_msg_t:
 *
 * Element type enqueued by a worker and dequeued by the coordinator.
 * The @delta pointer is caller-owned; the queue does not manage its lifetime.
 */
typedef struct {
    void    *delta;     /* delta relation pointer                  */
    uint32_t stratum;   /* stratum ID                              */
    uint32_t worker_id; /* originating worker thread               */
    uint32_t rel_idx;   /* relation index within stratum plan      */
    uint32_t epoch;     /* barrier epoch counter                   */
    uint16_t flags;     /* message status flags                    */
    int16_t rc;         /* return/result code                      */
} wl_delta_msg_t;

/**
 * wl_mpsc_queue_t:
 *
 * Opaque MPSC queue backed by num_workers independent SPSC ring buffers.
 * Created with wl_mpsc_queue_create(), destroyed with wl_mpsc_queue_destroy().
 */
typedef struct wl_mpsc_queue wl_mpsc_queue_t;

/**
 * wl_mpsc_queue_create:
 * @num_workers: Number of producer threads.  Must be >= 1.
 * @capacity:    Per-worker ring buffer capacity (rounded up to next power of
 *               2 internally).  Must be >= 2.
 *
 * Allocate and initialise the queue.  No threads are created.
 *
 * Returns:
 *   non-NULL: Opaque queue handle.  Caller must destroy with
 *             wl_mpsc_queue_destroy().
 *   NULL:     Allocation failure or invalid arguments.
 */
wl_mpsc_queue_t *
wl_mpsc_queue_create(uint32_t num_workers, uint32_t capacity);

/**
 * wl_mpsc_queue_destroy:
 * @q: (transfer full) Queue to destroy.  NULL-safe.
 *
 * Free all resources.  All threads must have stopped using the queue
 * before this is called.
 */
void
wl_mpsc_queue_destroy(wl_mpsc_queue_t *q);

/**
 * wl_mpsc_enqueue:
 * @q:         Queue handle.  Must not be NULL.
 * @worker_id: Caller's worker index (0 .. num_workers-1).  Must match the
 *             calling thread; only one thread may use each worker_id.
 * @delta:     Delta relation pointer.  May be NULL.
 * @stratum:   Stratum ID.
 * @rel_idx:   Relation index within the stratum plan.
 *
 * Enqueue one delta message into worker_id's dedicated SPSC ring buffer.
 * Non-blocking: returns -1 immediately if the ring is full.
 *
 * Thread safety: Call ONLY from the thread that owns @worker_id.
 *
 * Returns:
 *    0: Success.
 *   -1: Ring full or invalid arguments.
 */
int
wl_mpsc_enqueue(wl_mpsc_queue_t *q, uint32_t worker_id,
    void *delta, uint32_t stratum, uint32_t rel_idx);

/**
 * wl_mpsc_dequeue:
 * @q:   Queue handle.  Must not be NULL.
 * @out: (out) Filled with one dequeued message when an item is available.
 *
 * Dequeue one message from any worker's ring buffer (round-robin scan).
 * Non-blocking: returns 0 immediately if all rings are empty.
 *
 * Thread safety: Call ONLY from the coordinator thread.
 *
 * Returns:
 *   1: Item dequeued; *out is valid.
 *   0: All rings empty; *out is unmodified.
 */
int
wl_mpsc_dequeue(wl_mpsc_queue_t *q, wl_delta_msg_t *out);

/**
 * wl_mpsc_dequeue_all:
 * @q:       Queue handle.  Must not be NULL.
 * @buf:     Caller-supplied output buffer.
 * @buf_len: Capacity of @buf in elements.
 *
 * Drain all currently available messages from all worker rings into @buf.
 * Stops when @buf is full or all rings are empty.  Non-blocking.
 *
 * Thread safety: Call ONLY from the coordinator thread.
 *
 * Returns:
 *   Number of items written to @buf (0 if all rings were empty).
 */
uint32_t
wl_mpsc_dequeue_all(wl_mpsc_queue_t *q, wl_delta_msg_t *buf, uint32_t buf_len);

/**
 * wl_mpsc_size:
 * @q: Queue handle.  NULL-safe (returns 0).
 *
 * Approximate total number of live items across all worker rings.
 * Not synchronized — use only for diagnostics or capacity-planning estimates.
 *
 * Returns:
 *   Approximate item count (may be transiently inaccurate).
 */
uint32_t
wl_mpsc_size(wl_mpsc_queue_t *q);

#endif /* WL_LOCKFREE_QUEUE_H */
