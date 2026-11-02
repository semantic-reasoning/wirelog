/*
 * workqueue.h - wirelog Work Queue (Phase B-lite)
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
 * Minimal work-queue abstraction for parallelising non-recursive stratum
 * evaluation in the columnar backend.  Provides a task-submission
 * interface backed by a cross-platform thread pool (CPU backend).
 *
 * Design constraints:
 *   - Pure C11 + cross-platform thread abstraction (POSIX/MSVC compatible).
 *   - Synchronisation via mutex + condition variables only.
 *   - No async callbacks, no cancellation — thin enough for TSan/GDB.
 *   - Future FPGA backend can implement the same interface with
 *     DMA/Arrow IPC transport (Phase C).
 *
 * ========================================================================
 * Threading Model
 * ========================================================================
 *
 * The work queue owns a fixed-size pool of worker threads, created at
 * wl_workqueue_create() time and joined at wl_workqueue_destroy() time.
 *
 * Workers block on a condition variable until dispatch or wait_all opens
 * the batch.
 * Tasks are dispatched from a ring buffer protected by a single mutex.
 *
 * The caller submits work items via wl_workqueue_submit(), then calls
 * wl_workqueue_wait_all() to block until all submitted items complete
 * (barrier semantics).  After the barrier, results are available in
 * caller-owned buffers for sequential merge.
 *
 * A caller that must keep serving the running tasks calls
 * wl_workqueue_dispatch() first, and still ends the batch with wait_all or
 * drain.
 *
 * For single-threaded / embedded fallback, wl_workqueue_drain() executes
 * all pending tasks synchronously on the calling thread.
 * The coordinator serializes submit, dispatch, wait_all, drain, and destroy
 * calls; these control operations must not overlap with each other.
 *
 * ========================================================================
 * Per-Worker Arena Cloning
 * ========================================================================
 *
 * The columnar backend's wl_arena_t allocator is NOT thread-safe.
 * Each worker thread must own an independent arena.  The integration
 * pattern is:
 *
 *   // Before submitting work:
 *   for (uint32_t i = 0; i < num_workers; i++)
 *       worker_arenas[i] = wl_arena_create(capacity);
 *
 *   // Each work item's ctx embeds its own arena pointer:
 *   struct work_ctx { wl_arena_t *arena; ... };
 *
 *   // After wait_all, arenas are freed by the main thread.
 *
 * Arenas are NOT cloned (deep-copied) because they are bump allocators
 * with no meaningful pre-existing state to preserve.  Instead, each
 * worker receives a fresh, empty arena of the same capacity.
 *
 * ========================================================================
 * Collect-Then-Merge Pattern
 * ========================================================================
 *
 * Workers must NOT write to shared session state (sess->rels[],
 * sess->nrels, etc.).  Each worker produces an independent result
 * buffer via its ctx.  After the wait_all barrier, the main thread
 * merges results sequentially — this structurally prevents the three
 * shared-state hazards documented in shared-state-hazards.md.
 */

#ifndef WL_WORKQUEUE_H
#define WL_WORKQUEUE_H

#include <stdint.h>

/**
 * Work Queue Ring Buffer Capacity (Phase 0: Dynamic Allocation)
 *
 * Ring capacity is now dynamically allocated based on num_workers.
 * Each queue allocates ring_capacity = max(256, next_pow2(num_workers * 2)).
 * This scales the ring buffer automatically for W=1 to W=512+ without
 * compile-time configuration or back-pressure limits.
 *
 * OLD: Fixed compile-time macro WL_WQ_RING_CAP (now removed).
 */

/**
 * wl_work_queue_t:
 *
 * Opaque handle to a work queue with a cross-platform thread pool.
 * Created with wl_workqueue_create(), destroyed with wl_workqueue_destroy().
 */
typedef struct wl_work_queue wl_work_queue_t;

/**
 * wl_workqueue_create:
 * @num_workers: Number of worker threads to spawn.  Must be in the range
 *               1..2^30; larger values cannot be rounded to a representable
 *               power-of-two ring capacity.
 *
 * Create a work queue backed by a fixed-size thread pool.
 * Worker threads are created immediately and block until work is
 * submitted.
 *
 * Returns:
 *   non-NULL: Opaque queue handle (caller must destroy with
 *             wl_workqueue_destroy).
 *   NULL:     Allocation or thread creation failure.
 */
wl_work_queue_t *
wl_workqueue_create(uint32_t num_workers);

/**
 * wl_workqueue_submit:
 * @wq:      Work queue handle.  Must not be NULL.
 * @work_fn: Function to execute on a worker thread.  Must not be NULL.
 *           The function receives @ctx as its sole argument.
 * @ctx:     Opaque context pointer passed to @work_fn.
 *           Caller-owned; must remain valid until wl_workqueue_wait_all()
 *           or wl_workqueue_drain() returns.
 *
 * Enqueue a work item for execution by the thread pool.  The work
 * function is called exactly once on exactly one worker thread.
 *
 * Thread safety: safe to call from the main thread while workers are
 * executing prior tasks.  NOT safe to call from worker threads.
 *
 * Returns:
 *    0: Success.
 *   -1: Queue capacity exhausted (ENOMEM) or invalid arguments.
 */
int
wl_workqueue_submit(wl_work_queue_t *wq, void (*work_fn)(void *ctx), void *ctx);

/**
 * wl_workqueue_dispatch:
 * @wq: Work queue handle.  Must not be NULL.
 *
 * Open the dispatch gate so worker threads may take the submitted items,
 * and return without waiting for them, so the caller can serve callbacks
 * that wait on it.  An item submitted after this call, before the batch
 * ends, wakes an idle worker without another dispatch.  An item runs only
 * when a worker is free to take it; while every worker is busy (and a pool
 * may have fewer threads than requested), items stay queued.
 *
 * The batch still ends with wl_workqueue_wait_all() or wl_workqueue_drain(),
 * which close the dispatch gate again.  Contexts must stay valid until then.
 * Do not overlap with wait_all or drain.
 *
 * Returns:
 *    0: Dispatch opened.
 *   -1: @wq is NULL.
 */
int
wl_workqueue_dispatch(wl_work_queue_t *wq);

/**
 * wl_workqueue_wait_all:
 * @wq: Work queue handle.  Must not be NULL.
 *
 * Block the calling thread until all previously submitted work items
 * have completed execution (barrier semantics).
 *
 * After this call returns, all results written by workers into their
 * ctx buffers are visible to the caller (sequenced-after relationship
 * via mutex release in worker, mutex acquire in wait_all).
 *
 * The queue is ready for a new batch of submit/wait_all cycles after
 * this call returns.
 *
 * Returns:
 *    0: All tasks completed successfully.
 *   -1: Internal synchronisation error (should not occur in practice).
 */
int
wl_workqueue_wait_all(wl_work_queue_t *wq);

/**
 * wl_workqueue_drain:
 * @wq: Work queue handle.  Must not be NULL.
 *
 * Execute all pending work items synchronously on the calling thread.
 * Intended as a single-threaded fallback for embedded targets or
 * debugging. A submit-only batch executes entirely on the caller. If tasks
 * are already executing on workers, their completion is also awaited.
 *
 * Returns only after every submitted callback has finished, including any
 * callback already dequeued by a worker. Contexts may then be released and
 * the queue is ready for a new batch. Do not overlap with submit, dispatch or
 * wait_all.
 *
 * Returns:
 *    0: All tasks drained successfully.
 *   -1: Internal error.
 */
int
wl_workqueue_drain(wl_work_queue_t *wq);

/**
 * wl_workqueue_destroy:
 * @wq: (transfer full): Work queue to destroy.  NULL-safe.
 *
 * Signal all worker threads to shut down, join them, and free all
 * queue resources.  Workers run every task still queued before they exit,
 * whether or not the dispatch gate is open.
 *
 * The caller must ensure no concurrent submit/dispatch/wait_all/drain calls
 * are in progress when destroy is called.
 */
void
wl_workqueue_destroy(wl_work_queue_t *wq);

/**
 * wl_workqueue_capacity:
 * @wq: Work queue handle.  Must not be NULL.
 *
 * Return the ring buffer capacity of the work queue.
 * Useful for testing and diagnostics.
 *
 * This is an internal API (not part of the public contract).
 * The capacity is computed at create time via the formula:
 *   capacity = max(256, next_pow2(num_workers * 2))
 *
 * Returns:
 *   The ring buffer capacity (always a power of 2, >= 256).
 */
uint32_t
wl_workqueue_capacity(const wl_work_queue_t *wq);

#endif /* WL_WORKQUEUE_H */
