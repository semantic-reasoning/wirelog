/*
 * test_workqueue_drain.c - deterministic drain regressions (Issue #1378)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Local interceptors observe waits and delay startup without production hooks.
 * Observer lock order: queue mutex -> observer mutex, never the reverse.
 */
#include "../wirelog/thread.h"
#include "../wirelog/workqueue.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#define REQUIRE(expr)                                                        \
        do {                                                                     \
            if (!(expr)) {                                                       \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr);         \
                abort();                                                         \
            }                                                                    \
        } while (0)

static int observed_cond_wait(wl_cond_t *cond, wl_mutex_t *mutex);
static int delayed_thread_create(wl_thread_t *thread, void *(*fn)(void *),
    void *arg);

#define wl_cond_wait observed_cond_wait
#define wl_thread_create delayed_thread_create
#include "../wirelog/workqueue.c"
#undef wl_thread_create
#undef wl_cond_wait

static struct {
    wl_mutex_t mutex;
    wl_cond_t changed;
    wl_work_queue_t *queue;
    void *(*worker_fn)(void *);
    void *worker_arg;
    bool start_worker;
    unsigned work_waits;
    unsigned calls;
    bool active_started;
    bool release_active;
    unsigned active_calls;
    bool drain_waiting;
    bool drain_gate_closed;
    bool drain_returned;
    int drain_result;
} observer;

/* Identify the draining thread without backend-specific thread handles. */
#ifdef _MSC_VER
static __declspec(thread) bool is_drain_thread;
#else
static _Thread_local bool is_drain_thread;
#endif
static unsigned failures;

static void
check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        failures++;
    }
}

static int
observed_cond_wait(wl_cond_t *cond, wl_mutex_t *mutex)
{
    /* Queue mutex is held, preventing lost wakes before the real wait. */
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    if (cond == &observer.queue->work_avail) {
        observer.work_waits++;
    } else if (cond == &observer.queue->all_done && is_drain_thread) {
        observer.drain_waiting = true;
        observer.drain_gate_closed = !observer.queue->dispatch_enabled;
    }
    REQUIRE(wl_cond_broadcast(&observer.changed) == 0);
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    int result = wl_cond_wait(cond, mutex);
    REQUIRE(result == 0);
    return result;
}

static void *
delayed_worker(void *unused)
{
    (void)unused;
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    while (!observer.start_worker)
        REQUIRE(wl_cond_wait(&observer.changed, &observer.mutex) == 0);
    void *(*fn)(void *) = observer.worker_fn;
    void *arg = observer.worker_arg;
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    return fn(arg);
}

static int
delayed_thread_create(wl_thread_t *thread, void *(*fn)(void *), void *arg)
{
    /* All cases use exactly one worker and one live queue. */
    observer.worker_fn = fn;
    observer.worker_arg = arg;
    return wl_thread_create(thread, delayed_worker, NULL);
}

static wl_work_queue_t *
new_queue(void)
{
    memset(&observer, 0, sizeof(observer));
    REQUIRE(wl_mutex_init(&observer.mutex) == 0);
    REQUIRE(wl_cond_init(&observer.changed) == 0);
    wl_work_queue_t *queue = wl_workqueue_create(1);
    REQUIRE(queue != NULL);
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    observer.queue = queue;
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    return queue;
}

static void
start_worker(void)
{
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    observer.start_worker = true;
    REQUIRE(wl_cond_broadcast(&observer.changed) == 0);
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
}

static void
delete_queue(wl_work_queue_t *queue)
{
    start_worker();
    wl_workqueue_destroy(queue);
    wl_cond_destroy(&observer.changed);
    wl_mutex_destroy(&observer.mutex);
}

typedef struct {
    unsigned calls;
    bool expect_drain;
    bool wrong_thread;
} item_t;

static void
count_item(void *arg)
{
    item_t *item = arg;
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    item->calls++;
    item->wrong_thread |= is_drain_thread != item->expect_drain;
    observer.calls++;
    REQUIRE(wl_cond_broadcast(&observer.changed) == 0);
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
}

static void
check_items(item_t *items, unsigned count)
{
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    for (unsigned i = 0; i < count; i++) {
        check(items[i].calls == 1, "callback executes exactly once");
        check(!items[i].wrong_thread, "callback executes on expected thread");
    }
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
}

static bool
batch_reset(wl_work_queue_t *queue)
{
    REQUIRE(wl_mutex_lock(&queue->mutex) == 0);
    bool reset = queue->count == 0 && queue->submitted == 0
        && queue->completed == 0 && !queue->dispatch_enabled;
    REQUIRE(wl_mutex_unlock(&queue->mutex) == 0);
    check(reset, "batch resets counters and closes dispatch");
    return reset;
}

static void
reuse_queue(wl_work_queue_t *queue)
{
    item_t worker_items[3] = { 0 };
    for (unsigned i = 0; i < 3; i++)
        REQUIRE(wl_workqueue_submit(queue, count_item, &worker_items[i]) == 0);
    REQUIRE(wl_workqueue_wait_all(queue) == 0);
    check_items(worker_items, 3);
    if (!batch_reset(queue))
        return;

    item_t drain_items[3] = { 0 };
    for (unsigned i = 0; i < 3; i++) {
        drain_items[i].expect_drain = true;
        REQUIRE(wl_workqueue_submit(queue, count_item, &drain_items[i]) == 0);
    }
    is_drain_thread = true;
    REQUIRE(wl_workqueue_drain(queue) == 0);
    is_drain_thread = false;
    check_items(drain_items, 3);
    (void)batch_reset(queue);
}

static void
test_disabled_dispatch(bool at_startup)
{
    wl_work_queue_t *queue = new_queue();
    item_t item = { .expect_drain = true };
    if (!at_startup) {
        start_worker();
        REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
        while (observer.work_waits == 0)
            REQUIRE(wl_cond_wait(&observer.changed, &observer.mutex) == 0);
        REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    }
    REQUIRE(wl_workqueue_submit(queue, count_item, &item) == 0);
    REQUIRE(wl_mutex_lock(&queue->mutex) == 0);
    check(!queue->dispatch_enabled, "submit leaves dispatch disabled");
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    unsigned before = observer.work_waits;
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    if (!at_startup)
        REQUIRE(wl_cond_broadcast(&queue->work_avail) == 0);
    REQUIRE(wl_mutex_unlock(&queue->mutex) == 0);
    start_worker();

    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    while (observer.work_waits == before && observer.calls == 0)
        REQUIRE(wl_cond_wait(&observer.changed, &observer.mutex) == 0);
    bool held = observer.calls == 0;
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    check(held, at_startup ? "startup respects disabled dispatch"
                          : "spurious wake rechecks disabled dispatch");
    if (held) {
        is_drain_thread = true;
        REQUIRE(wl_workqueue_drain(queue) == 0);
        is_drain_thread = false;
    }
    /* On RED, join the worker instead of using possibly broken counters. */
    delete_queue(queue);
    check(item.calls == 1, "disabled-dispatch case executes exactly once");
    check(!item.wrong_thread, "disabled-dispatch item belongs to drain caller");
}

static void
blocked_item(void *unused)
{
    (void)unused;
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    observer.active_calls++;
    observer.active_started = true;
    REQUIRE(wl_cond_broadcast(&observer.changed) == 0);
    while (!observer.release_active)
        REQUIRE(wl_cond_wait(&observer.changed, &observer.mutex) == 0);
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
}

static void *
drain_thread(void *arg)
{
    is_drain_thread = true;
    int result = wl_workqueue_drain(arg);
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    observer.drain_result = result;
    observer.drain_returned = true;
    REQUIRE(wl_cond_broadcast(&observer.changed) == 0);
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    return NULL;
}

static void
test_active_and_pending(unsigned pending)
{
    wl_work_queue_t *queue = new_queue();
    item_t *items = calloc(pending ? pending : 1, sizeof(*items));
    REQUIRE(items != NULL);
    REQUIRE(wl_workqueue_submit(queue, blocked_item, NULL) == 0);
    REQUIRE(wl_mutex_lock(&queue->mutex) == 0);
    queue->dispatch_enabled = true;
    REQUIRE(wl_cond_broadcast(&queue->work_avail) == 0);
    REQUIRE(wl_mutex_unlock(&queue->mutex) == 0);
    start_worker();
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    while (!observer.active_started)
        REQUIRE(wl_cond_wait(&observer.changed, &observer.mutex) == 0);
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);

    /* The sole worker is blocked and cannot steal pending items. */
    for (unsigned i = 0; i < pending; i++) {
        items[i].expect_drain = true;
        REQUIRE(wl_workqueue_submit(queue, count_item, &items[i]) == 0);
    }
    wl_thread_t drainer;
    REQUIRE(wl_thread_create(&drainer, drain_thread, queue) == 0);
    REQUIRE(wl_mutex_lock(&observer.mutex) == 0);
    while (!observer.drain_waiting && !observer.drain_returned)
        REQUIRE(wl_cond_wait(&observer.changed, &observer.mutex) == 0);
    bool early = observer.drain_returned;
    bool closed = observer.drain_gate_closed;
    /* Release even on RED. Old drain corrupts batch counters, so skip reuse
     * after early return: a subsequent wait_all could hang forever. */
    observer.release_active = true;
    REQUIRE(wl_cond_broadcast(&observer.changed) == 0);
    REQUIRE(wl_mutex_unlock(&observer.mutex) == 0);
    REQUIRE(wl_thread_join(&drainer) == 0);
    check(!early, "drain must wait for the active worker");
    check(closed, "drain closes dispatch before waiting for active work");
    check(observer.drain_result == 0, "drain succeeds");
    check_items(items, pending);
    if (!early && batch_reset(queue))
        reuse_queue(queue);
    delete_queue(queue);
    check(observer.active_calls == 1, "active callback executes exactly once");
    free(items);
}

static void
test_pending_and_shutdown(void)
{
    wl_work_queue_t *queue = new_queue();
    REQUIRE(wl_workqueue_drain(queue) == 0);
    check(batch_reset(queue), "empty drain resets batch");
    unsigned capacity = wl_workqueue_capacity(queue);
    item_t *items = calloc(capacity, sizeof(*items));
    REQUIRE(items != NULL);
    for (unsigned i = 0; i < capacity; i++) {
        items[i].expect_drain = true;
        REQUIRE(wl_workqueue_submit(queue, count_item, &items[i]) == 0);
    }
    REQUIRE(wl_workqueue_submit(queue, count_item, &items[0]) != 0);
    is_drain_thread = true;
    REQUIRE(wl_workqueue_drain(queue) == 0);
    is_drain_thread = false;
    check_items(items, capacity);
    check(batch_reset(queue), "full pending batch drained");
    start_worker();
    reuse_queue(queue);
    reuse_queue(queue); /* wait after drain, then drain after wait */
    item_t shutdown_item = { 0 };
    REQUIRE(wl_workqueue_submit(queue, count_item, &shutdown_item) == 0);
    delete_queue(queue);
    check(shutdown_item.calls == 1, "shutdown finishes queued work");
    check(!shutdown_item.wrong_thread, "shutdown work runs on worker");
    free(items);
}

int
main(void)
{
    test_disabled_dispatch(true);
    test_disabled_dispatch(false);
    test_active_and_pending(3);
    test_active_and_pending(0); /* All workers active; ring empty. */
    test_pending_and_shutdown();
    fprintf(stderr, "workqueue_drain: %u failure(s)\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
