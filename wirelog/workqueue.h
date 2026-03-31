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
 *   - Future FPGA backend can implement the same 5-function interface
 *     with DMA/Arrow IPC transport (Phase C).
 *
 * ========================================================================
 * Threading Model
 * ========================================================================
 *
 * The work queue owns a fixed-size pool of worker threads, created at
 * wl_workqueue_create() time and joined at wl_workqueue_destroy() time.
 *
 * Workers block on a condition variable until tasks are submitted.
 * Tasks are dispatched from a ring buffer protected by a single mutex.
 *
 * The caller submits work items via wl_workqueue_submit(), then calls
 * wl_workqueue_wait_all() to block until all submitted items complete
 * (barrier semantics).  After the barrier, results are available in
 * caller-owned buffers for sequential merge.
 *
 * For single-threaded / embedded fallback, wl_workqueue_drain() executes
 * all pending tasks synchronously on the calling thread.
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
 * @num_workers: Number of worker threads to spawn.  Must be >= 1.
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
 * debugging (bypasses the thread pool entirely).
 *
 * Clears the task queue after execution.  The queue is ready for new
 * submit calls after this returns.
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
 * queue resources.  Any pending (un-executed) tasks are discarded.
 *
 * The caller must ensure no concurrent submit/wait_all/drain calls
 * are in progress when destroy is called.
 */
void
wl_workqueue_destroy(wl_work_queue_t *wq);

#endif /* WL_WORKQUEUE_H */
