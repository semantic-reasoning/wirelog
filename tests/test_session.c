/*
 * test_session.c - Persistent session delta tests via wl_session_* API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the persistent columnar session: create, insert, step, delta callback.
 * Uses the wl_session_* wrapper API (not raw FFI), exercising the full
 * backend vtable dispatch.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/thread.h"
#include "../wirelog/columnar/memory_governor.h"
#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/intern.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#if defined(WL_HAVE_C11_THREADS) || !defined(_WIN32)
#include <time.h>
#endif

#ifdef WL_TEST_ALLOC_WRAP
void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);
static bool fail_next_alloc;
static size_t fail_malloc_size;
static unsigned fail_malloc_matches_to_skip;
static unsigned fail_malloc_match_count;
static size_t fail_calloc_size;
static wl_columnar_source_access_reader_t *release_on_calloc_failure;
static int calloc_reader_release_rc;

void *
__wrap_malloc(size_t size)
{
    if (fail_malloc_size != 0 && size == fail_malloc_size) {
        fail_malloc_match_count++;
        if (fail_malloc_matches_to_skip != 0) {
            fail_malloc_matches_to_skip--;
        } else {
            fail_malloc_size = 0;
            return NULL;
        }
    }
    if (fail_next_alloc) {
        fail_next_alloc = false;
        return NULL;
    }
    return __real_malloc(size);
}

void *
__wrap_calloc(size_t count, size_t size)
{
    if (fail_calloc_size != 0 && count == 1 && size == fail_calloc_size) {
        fail_calloc_size = 0;
        if (release_on_calloc_failure) {
            calloc_reader_release_rc = col_rel_source_reader_release(
                release_on_calloc_failure);
            release_on_calloc_failure = NULL;
        }
        return NULL;
    }
    if (fail_next_alloc) {
        fail_next_alloc = false;
        return NULL;
    }
    return __real_calloc(count, size);
}

void *
__wrap_realloc(void *ptr, size_t size)
{
    if (fail_next_alloc) {
        fail_next_alloc = false;
        return NULL;
    }
    return __real_realloc(ptr, size);
}
#endif

#ifdef _WIN32
#include <windows.h>
#endif

/* ======================================================================== */
/* Portability Macros                                                       */
/* ======================================================================== */

/* MSVC does not support __attribute__((unused)); use a portable macro */
#ifdef _MSC_VER
#define UNUSED
#else
#define UNUSED __attribute__((unused))
#endif

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

static wl_plan_t *build_plan(const char *src);

#ifdef WL_SESSION_TEST_HOOKS
typedef struct {
    wl_mutex_t mutex;
    wl_cond_t changed;
    wl_mutex_t task_mutex;
    wl_cond_t task_changed;
    wl_session_t *session;
    col_rel_t *owner;
    col_rel_t *alias;
    bool admission_closed;
    bool release_admission;
    bool before_drain;
    bool task_started;
    bool release_task;
    bool task_finished;
    bool callback_entered;
    bool release_callback;
    bool snapshot_finished;
    int snapshot_rc;
    bool after_worker_lease_release;
    bool destroy_finished;
    uint64_t borrow_count_after_release;
} session_destroy_probe_t;

static session_destroy_probe_t *session_destroy_probe;

static void
probe_admission_closed(wl_session_t *session)
{
    session_destroy_probe_t *probe = session_destroy_probe;
    wl_mutex_lock(&probe->mutex);
    probe->session = session;
    probe->admission_closed = true;
    wl_cond_broadcast(&probe->changed);
    while (!probe->release_admission)
        wl_cond_wait(&probe->changed, &probe->mutex);
    wl_mutex_unlock(&probe->mutex);
}

static void
probe_before_drain(wl_session_t *session)
{
    session_destroy_probe_t *probe = session_destroy_probe;
    (void)session;
    wl_mutex_lock(&probe->mutex);
    probe->before_drain = true;
    wl_cond_broadcast(&probe->changed);
    wl_mutex_unlock(&probe->mutex);
}

static void
probe_after_worker_lease_release(wl_session_t *session)
{
    session_destroy_probe_t *probe = session_destroy_probe;
    (void)session;
    wl_mutex_lock(&probe->mutex);
    probe->borrow_count_after_release = probe->owner
        ? col_rel_storage_alias_borrow_count(probe->owner) : UINT64_MAX;
    probe->after_worker_lease_release = true;
    wl_cond_broadcast(&probe->changed);
    wl_mutex_unlock(&probe->mutex);
}

static void
probe_work_item(void *opaque)
{
    session_destroy_probe_t *probe = (session_destroy_probe_t *)opaque;
    wl_mutex_lock(&probe->task_mutex);
    probe->task_started = true;
    wl_cond_broadcast(&probe->task_changed);
    while (!probe->release_task)
        wl_cond_wait(&probe->task_changed, &probe->task_mutex);
    /* The worker alias remains valid until the workqueue has drained. */
    if (probe->alias && probe->alias->name)
        probe->task_finished = strcmp(probe->alias->name, "edge_alias") == 0;
    wl_cond_broadcast(&probe->task_changed);
    wl_mutex_unlock(&probe->task_mutex);
}

static void
probe_snapshot_callback(const char *relation, const int64_t *row,
    uint32_t ncols, void *opaque)
{
    session_destroy_probe_t *probe = (session_destroy_probe_t *)opaque;
    (void)relation;
    (void)row;
    (void)ncols;
    wl_mutex_lock(&probe->mutex);
    probe->callback_entered = true;
    wl_cond_broadcast(&probe->changed);
    while (!probe->release_callback)
        wl_cond_wait(&probe->changed, &probe->mutex);
    wl_mutex_unlock(&probe->mutex);
}

static void
probe_snapshot_noop(const char *relation, const int64_t *row,
    uint32_t ncols, void *opaque)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)opaque;
}

static void *
probe_snapshot_thread(void *opaque)
{
    session_destroy_probe_t *probe = (session_destroy_probe_t *)opaque;
    int rc = wl_session_snapshot(probe->session, probe_snapshot_callback,
            probe);
    wl_mutex_lock(&probe->mutex);
    probe->snapshot_rc = rc;
    probe->snapshot_finished = true;
    wl_cond_broadcast(&probe->changed);
    wl_mutex_unlock(&probe->mutex);
    return NULL;
}

static void *
probe_destroy_thread(void *opaque)
{
    session_destroy_probe_t *probe = (session_destroy_probe_t *)opaque;
    wl_session_destroy(probe->session);
    wl_mutex_lock(&probe->mutex);
    probe->destroy_finished = true;
    wl_cond_broadcast(&probe->changed);
    wl_mutex_unlock(&probe->mutex);
    return NULL;
}

static int
probe_cond_wait_for(wl_cond_t *cond, wl_mutex_t *mutex, unsigned timeout_ms)
{
#if defined(WL_HAVE_C11_THREADS)
    struct timespec now;
    struct timespec deadline;
    if (timespec_get(&now, TIME_UTC) != TIME_UTC)
        return ETIMEDOUT;
    deadline = now;
    deadline.tv_sec += timeout_ms / 1000u;
    deadline.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    return cnd_timedwait(&cond->c, &mutex->m, &deadline) == thrd_success
        ? 0 : ETIMEDOUT;
#elif defined(_WIN32) || defined(_WIN64)
    return SleepConditionVariableCS(&cond->cv, &mutex->cs, timeout_ms)
        ? 0 : ETIMEDOUT;
#else
    struct timespec now;
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &now) != 0)
        return ETIMEDOUT;
    deadline = now;
    deadline.tv_sec += timeout_ms / 1000u;
    deadline.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    return pthread_cond_timedwait(&cond->c, &mutex->m, &deadline) == 0
        ? 0 : ETIMEDOUT;
#endif
}

static bool
probe_wait(bool *condition, session_destroy_probe_t *probe,
    wl_cond_t *cond, wl_mutex_t *mutex)
{
    while (!*condition) {
        if (probe_cond_wait_for(cond, mutex, 5000) != 0)
            return false;
    }
    return true;
}

static void
test_session_destroy_orders_worker_retirement(void)
{
    session_destroy_probe_t probe = { 0 };
    wirelog_program_t *program = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    wl_col_session_t *coord;
    wl_thread_t destroy_thread;
    wl_thread_t snapshot_thread;
    col_rel_t *owner;
    col_rel_t *parts[1] = { NULL };
    int64_t edge[] = { 1 };
    int rc;
    bool destroy_started = false;
    bool snapshot_started = false;
    bool work_submitted = false;

    TEST("session destroy closes admission before draining worker aliases");
    if (wl_mutex_init(&probe.mutex) != 0) {
        FAIL("probe synchronization setup failed");
        return;
    }
    if (wl_cond_init(&probe.changed) != 0) {
        wl_mutex_destroy(&probe.mutex);
        FAIL("probe synchronization setup failed");
        return;
    }
    if (wl_mutex_init(&probe.task_mutex) != 0) {
        wl_cond_destroy(&probe.changed);
        wl_mutex_destroy(&probe.mutex);
        FAIL("probe synchronization setup failed");
        return;
    }
    if (wl_cond_init(&probe.task_changed) != 0) {
        wl_mutex_destroy(&probe.task_mutex);
        wl_cond_destroy(&probe.changed);
        wl_mutex_destroy(&probe.mutex);
        FAIL("probe synchronization setup failed");
        return;
    }
    plan = build_plan(".decl edge(x: int32)\n"
            ".decl out(x: int32)\n"
            "out(x) :- edge(x).\n");
    rc = plan ? wl_session_create(wl_backend_columnar(), plan, 2, &session)
              : ENOMEM;
    coord = session ? COL_SESSION(session) : NULL;
    owner = coord ? session_find_rel(coord, "edge") : NULL;
    if (rc == 0)
        rc = wl_session_insert(session, "edge", edge, 1, 1);
    if (rc == 0)
        rc = wl_session_snapshot(session, probe_snapshot_noop, NULL);
    if (rc == 0)
        rc = wl_columnar_session_ensure_tdd_worker_slots(coord, 1);
    if (rc == 0)
        parts[0] = col_rel_new_like("edge_alias", owner);
    if (rc == 0 && parts[0])
        rc = col_rel_install_shared_view(parts[0], owner);
    if (rc == 0 && parts[0])
        rc = col_worker_session_create(coord, 0, parts, 1,
                &coord->tdd_workers[0]);
    if (rc == 0)
        coord->tdd_workers_count = 1;
    if (rc == 0)
        rc = wl_columnar_session_ensure_workqueue(coord, 2);
    probe.session = session;
    probe.owner = owner;
    probe.alias = coord && coord->tdd_workers_count == 1
        ? coord->tdd_workers[0].rels[0] : NULL;
    if (rc == 0) {
        session_destroy_probe = &probe;
        wl_session_testhook_set_admission_closed(probe_admission_closed);
        wl_session_testhook_set_before_workqueue_drain(probe_before_drain);
        wl_session_testhook_set_after_worker_lease_release(
            probe_after_worker_lease_release);
        if (wl_thread_create(&snapshot_thread, probe_snapshot_thread, &probe)
            != 0)
            rc = EAGAIN;
        else
            snapshot_started = true;
    }
    if (rc == 0) {
        wl_mutex_lock(&probe.mutex);
        if (!probe_wait(&probe.callback_entered, &probe,
            &probe.changed, &probe.mutex))
            rc = ETIMEDOUT;
        wl_mutex_unlock(&probe.mutex);
    }
    if (rc == 0) {
        rc = wl_workqueue_submit(coord->wq, probe_work_item, &probe);
        if (rc == 0)
            work_submitted = true;
    }
    if (rc == 0) {
        if (wl_thread_create(&destroy_thread, probe_destroy_thread, &probe)
            != 0)
            rc = EAGAIN;
        else
            destroy_started = true;
    }
    if (rc == 0) {
        wl_mutex_lock(&probe.mutex);
        if (!probe_wait(&probe.admission_closed, &probe,
            &probe.changed, &probe.mutex))
            rc = ETIMEDOUT;
        if (wl_session_memory_governor(session) != NULL
            || probe.destroy_finished
            || probe.snapshot_finished
            || col_rel_storage_alias_borrow_count(owner) != 1)
            rc = EFAULT;
        probe.release_admission = true;
        wl_cond_broadcast(&probe.changed);
        wl_mutex_unlock(&probe.mutex);
    }
    if (rc == 0) {
        wl_mutex_lock(&probe.mutex);
        probe.release_callback = true;
        wl_cond_broadcast(&probe.changed);
        if (!probe_wait(&probe.snapshot_finished, &probe,
            &probe.changed, &probe.mutex))
            rc = ETIMEDOUT;
        if (probe.snapshot_rc != 0) {
            rc = EFAULT;
        }
        wl_mutex_unlock(&probe.mutex);
    }
    if (rc == 0) {
        wl_mutex_lock(&probe.mutex);
        if (!probe_wait(&probe.before_drain, &probe,
            &probe.changed, &probe.mutex))
            rc = ETIMEDOUT;
        wl_mutex_unlock(&probe.mutex);
        if (rc == 0) {
            wl_mutex_lock(&probe.task_mutex);
            if (!probe_wait(&probe.task_started, &probe,
                &probe.task_changed, &probe.task_mutex))
                rc = ETIMEDOUT;
            bool destroy_finished_before_release;
            bool task_finished_before_release = probe.task_finished;
            wl_mutex_unlock(&probe.task_mutex);
            wl_mutex_lock(&probe.mutex);
            destroy_finished_before_release = probe.destroy_finished;
            wl_mutex_unlock(&probe.mutex);
            if (destroy_finished_before_release || task_finished_before_release
                || col_rel_storage_alias_borrow_count(owner) != 1)
                rc = EFAULT;
            wl_mutex_lock(&probe.task_mutex);
            probe.release_task = true;
            wl_cond_broadcast(&probe.task_changed);
            wl_mutex_unlock(&probe.task_mutex);
        }
    }
    if (snapshot_started && rc != 0) {
        wl_mutex_lock(&probe.mutex);
        probe.release_callback = true;
        wl_cond_broadcast(&probe.changed);
        wl_mutex_unlock(&probe.mutex);
        wl_mutex_lock(&probe.task_mutex);
        probe.release_task = true;
        wl_cond_broadcast(&probe.task_changed);
        wl_mutex_unlock(&probe.task_mutex);
    }
    if (!destroy_started && (snapshot_started || work_submitted)) {
        wl_mutex_lock(&probe.mutex);
        probe.release_admission = true;
        probe.release_callback = true;
        wl_cond_broadcast(&probe.changed);
        wl_mutex_unlock(&probe.mutex);
        wl_mutex_lock(&probe.task_mutex);
        probe.release_task = true;
        wl_cond_broadcast(&probe.task_changed);
        wl_mutex_unlock(&probe.task_mutex);
    }
    if (destroy_started && rc != 0) {
        /* Never leave a started destroy thread behind when an assertion
         * above fails; both gates are owned by this test fixture. */
        wl_mutex_lock(&probe.mutex);
        probe.release_admission = true;
        wl_cond_broadcast(&probe.changed);
        wl_mutex_unlock(&probe.mutex);
        wl_mutex_lock(&probe.task_mutex);
        probe.release_task = true;
        wl_cond_broadcast(&probe.task_changed);
        wl_mutex_unlock(&probe.task_mutex);
    }
    if (destroy_started) {
        if (wl_thread_join(&destroy_thread) != 0) {
            fputs("session destroy probe: destroy thread join failed\n",
                stderr);
            exit(EXIT_FAILURE);
        }
    }
    if (snapshot_started) {
        if (wl_thread_join(&snapshot_thread) != 0) {
            fputs("session destroy probe: snapshot thread join failed\n",
                stderr);
            exit(EXIT_FAILURE);
        }
    }
    if (rc == 0) {
        if (!probe.after_worker_lease_release || !probe.task_finished
            || probe.borrow_count_after_release != 0)
            rc = EFAULT;
    }
    session_destroy_probe = NULL;
    wl_session_testhook_set_admission_closed(NULL);
    wl_session_testhook_set_before_workqueue_drain(NULL);
    wl_session_testhook_set_after_worker_lease_release(NULL);
    if (!destroy_started && session)
        wl_session_destroy(session);
    if (parts[0])
        col_rel_destroy(parts[0]);
    if (plan)
        wl_plan_free(plan);
    if (program)
        wirelog_program_free(program);
    wl_cond_destroy(&probe.changed);
    wl_mutex_destroy(&probe.mutex);
    wl_cond_destroy(&probe.task_changed);
    wl_mutex_destroy(&probe.task_mutex);
    if (rc != 0) {
        FAIL("session teardown did not preserve worker/lease ordering");
        return;
    }
    PASS();
}
#endif

static void
test_session_hash_overflow_rejected(void)
{
    TEST("session hash rejects overflowing relation counts");

    wl_col_session_t sess = { 0 };
    sess.nrels = UINT32_MAX / 2u + 2u;

    int rc = session_rel_build_hash(&sess);

    if (rc != ENOMEM || sess.rel_hash_nbuckets != 0
        || sess.rel_hash_head != NULL) {
        FAIL("overflowing relation count changed hash state");
        return;
    }
    PASS();
}

static void
test_retained_relation_admission(void)
{
    const uint64_t initial_bytes = 64u * sizeof(int64_t)
        + 64u * sizeof(col_delta_timestamp_t);
    const uint64_t grown_bytes = 128u * sizeof(int64_t)
        + 128u * sizeof(col_delta_timestamp_t);
    const uint64_t exact_budget = initial_bytes + grown_bytes;
    wl_columnar_memory_resolution_t resolution = { 0 };
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *rel = NULL;
    int64_t row = 7;
    int rc;

    TEST("retained relation growth admits peak and releases on destroy");
    resolution.budget_bytes = exact_budget;
    resolution.usable_bytes = exact_budget;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    if (!ref || col_rel_alloc(&rel, "retained") != 0
        || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (rel)
            col_rel_destroy(rel);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        FAIL("retained relation setup failed");
        return;
    }
    rc = col_rel_set_schema(rel, 1, NULL);
    if (rc == 0)
        rc = col_rel_enable_timestamps(rel);
    for (uint32_t i = 0; rc == 0 && i < 64u; i++)
        rc = col_rel_append_row(rel, &row);
    if (rc == 0)
        rc = col_rel_append_row(rel, &row);
    if (rc != 0 || rel->capacity != 128u || rel->nrows != 65u
        || rel->timestamps[0].iteration != 0
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != grown_bytes) {
        col_rel_destroy(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("exact-fit retained growth was not committed");
        return;
    }
    rel->nrows = 1;
    col_rel_compact(rel);
    if (rel->capacity != 64u || rel->nrows != 1u
        || rel->timestamps[0].iteration != 0
        || col_rel_transport_bytes(rel) != initial_bytes
        || rel->retained_reserved_bytes != initial_bytes
        || atomic_load_explicit(&rel->retained_reservation.state,
        memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != initial_bytes) {
        col_rel_destroy(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("governed non-empty compaction did not commit replacement");
        return;
    }
    rel->nrows = 0;
    col_rel_compact(rel);
    if (rel->capacity != 0u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0u
        || col_rel_append_row(rel, &row) != 0
        || rel->capacity != 64u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        != 64u * sizeof(int64_t)) {
        col_rel_destroy(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("empty compaction left a stale retained admission");
        return;
    }
    col_rel_destroy(rel);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("retained reservation leaked after destroy");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    TEST("retained relation denial preserves old rows and capacity");
    resolution.usable_bytes = exact_budget - 1u;
    resolution.budget_bytes = exact_budget - 1u;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    rel = NULL;
    if (!ref || col_rel_alloc(&rel, "retained-denied") != 0
        || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (rel)
            col_rel_destroy(rel);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        FAIL("denial setup failed");
        return;
    }
    rc = col_rel_set_schema(rel, 1, NULL);
    if (rc == 0)
        rc = col_rel_enable_timestamps(rel);
    for (uint32_t i = 0; rc == 0 && i < 64u; i++)
        rc = col_rel_append_row(rel, &row);
    rc = rc == 0 ? col_rel_append_row(rel, &row) : rc;
    if (rc != ENOMEM || rel->capacity != 64u || rel->nrows != 64u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != initial_bytes) {
        col_rel_destroy(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("denied retained growth changed relation state");
        return;
    }
    col_rel_destroy(rel);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("denied relation reservation leaked after destroy");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    TEST("batch compaction rolls back denied replacements");
    resolution.usable_bytes = 4096u;
    resolution.budget_bytes = 4096u;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    col_rel_t *first = NULL;
    col_rel_t *second = NULL;
    if (!ref || col_rel_alloc(&first, "batch-first") != 0
        || col_rel_alloc(&second, "batch-second") != 0
        || col_rel_attach_memory_governor(first, ref) != 0
        || col_rel_attach_memory_governor(second, ref) != 0
        || col_rel_set_schema(first, 1, NULL) != 0
        || col_rel_set_schema(second, 2, NULL) != 0) {
        col_rel_destroy(first);
        col_rel_destroy(second);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        FAIL("batch compaction setup failed");
        return;
    }
    int64_t first_row[1] = { 1 };
    int64_t second_row[2] = { 1, 2 };
    rc = 0;
    for (uint32_t i = 0; rc == 0 && i < 65u; i++) {
        rc = col_rel_append_row(first, first_row);
        if (rc == 0)
            rc = col_rel_append_row(second, second_row);
    }
    atomic_store_explicit(
        &wl_columnar_memory_governor_ref_get(ref)->usable_bytes, 3584u,
        memory_order_release);
    first->nrows = 1;
    second->nrows = 1;
    int batch_rc = col_rel_compact_many((col_rel_t *[]) { first, second }, 2);
    if (rc != 0 || first->capacity != 128u || second->capacity != 128u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 3072u
        || batch_rc != 0
        || first->capacity != 128u || second->capacity != 128u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 3072u) {
        col_rel_destroy(first);
        col_rel_destroy(second);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("batch replacement denial changed relation state");
        return;
    }
    col_rel_destroy(first);
    col_rel_destroy(second);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("batch replacement rollback leaked admission");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();
}

static void
test_governed_compaction_transaction(void)
{
    const uint64_t old_bytes = 128u * sizeof(int64_t)
        + 128u * sizeof(col_delta_timestamp_t);
    const uint64_t new_bytes = 64u * sizeof(int64_t)
        + 64u * sizeof(col_delta_timestamp_t);
    const uint64_t overlap_bytes = old_bytes + new_bytes;
    wl_columnar_memory_resolution_t resolution = { 0 };
    wl_columnar_memory_governor_ref_t *ref = NULL;
    wl_columnar_memory_governor_t *governor = NULL;
    wl_mem_ledger_t ledger;
    wl_mem_ledger_snapshot_t snapshot;
    col_rel_t *rel = NULL;
    int64_t row = 7;
    int64_t **old_columns = NULL;
    int64_t **failed_columns = NULL;
    const char *failure = NULL;
    int rc;

#define COMPACTION_CHECK(condition, message) \
        do { \
            if (!(condition)) { \
                failure = (message); \
                goto cleanup; \
            } \
        } while (0)

    TEST("governed compaction admits overlap and retries conservatively");
    resolution.budget_bytes = overlap_bytes;
    resolution.usable_bytes = overlap_bytes;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    wl_mem_ledger_init(&ledger, 0u);
    COMPACTION_CHECK(ref && col_rel_alloc(&rel, "compaction-transaction") == 0,
        "governed compaction setup");
    governor = wl_columnar_memory_governor_ref_get(ref);
    COMPACTION_CHECK(governor != NULL
        && col_rel_attach_memory_governor(rel, ref) == 0
        && col_rel_set_schema(rel, 1, NULL) == 0
        && col_rel_enable_timestamps(rel) == 0,
        "governed compaction admission setup");
    for (uint32_t i = 0; i < 65u; i++)
        COMPACTION_CHECK(col_rel_append_row(rel, &row) == 0,
            "governed compaction growth setup");
    rel->mem_ledger = &ledger;
    /* The direct relation fixture owns the ledger, so normalize the
     * baseline before checking the replacement accounting below. */
    col_rel_ledger_reconcile(rel, 0u);
    COMPACTION_CHECK(rel->capacity == 128u
        && rel->nrows == 65u
        && rel->retained_reserved_bytes == old_bytes
        && wl_columnar_memory_reserved(governor) == old_bytes,
        "governed compaction old admission");

    old_columns = rel->columns;
    rel->nrows = 1;
    atomic_store_explicit(&governor->usable_bytes, overlap_bytes - 1u,
        memory_order_release);
    rc = col_rel_compact(rel);
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    COMPACTION_CHECK(rc == 0 && rel->columns == old_columns
        && rel->capacity == 128u
        && atomic_load_explicit(&rel->retained_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && wl_columnar_memory_reserved(governor) == old_bytes
        && snapshot.current_bytes == old_bytes,
        "governed compaction denied overlap changed state");

    atomic_store_explicit(&governor->usable_bytes, overlap_bytes,
        memory_order_release);
    rc = col_rel_compact(rel);
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    COMPACTION_CHECK(rc == 0 && rel->columns != old_columns
        && rel->capacity == 64u && rel->nrows == 1u
        && rel->retained_reserved_bytes == new_bytes
        && atomic_load_explicit(&rel->retained_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && wl_columnar_memory_reserved(governor) == new_bytes
        && snapshot.current_bytes == new_bytes
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_RELATION]
        == 64u * sizeof(int64_t)
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]
        == 64u * sizeof(col_delta_timestamp_t),
        "governed compaction replacement accounting");

    for (uint32_t i = 0; i < 64u; i++)
        COMPACTION_CHECK(col_rel_append_row(rel, &row) == 0,
            "governed compaction retry growth");
    COMPACTION_CHECK(rel->capacity == 128u && rel->nrows == 65u
        && rel->retained_reserved_bytes == old_bytes
        && wl_columnar_memory_reserved(governor) == old_bytes,
        "governed compaction retry old admission");
    rel->nrows = 16;
    old_columns = rel->columns;
    atomic_store_explicit(&governor->usable_bytes, overlap_bytes,
        memory_order_release);
    COMPACTION_CHECK(
        wl_columnar_memory_begin_replacement(&rel->retained_reservation,
        new_bytes) == WL_COLUMNAR_MEMORY_ADMISSION_OK
        && wl_columnar_memory_reserved(governor) == overlap_bytes,
        "governed compaction failure admission");

    /* The direct counter change simulates a governor accounting failure
     * after overlap admission.  No production hook is needed: the token
     * state and relation storage are the contract under test. */
    atomic_store_explicit(&governor->reserved_bytes, 0u,
        memory_order_release);
    rc = col_rel_compact(rel);
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    failed_columns = rel->columns;
    COMPACTION_CHECK(rc == EAGAIN && failed_columns != old_columns
        && rel->capacity == 64u
        && rel->retained_reserved_bytes == old_bytes
        && atomic_load_explicit(&rel->retained_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING
        && rel->retained_reservation.replacement_bytes == new_bytes
        && snapshot.current_bytes == new_bytes,
        "governed compaction failure did not preserve retry state");

    atomic_store_explicit(&governor->reserved_bytes, overlap_bytes,
        memory_order_release);
    rc = col_rel_compact(rel);
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    COMPACTION_CHECK(rc == 0 && rel->columns == failed_columns
        && rel->retained_reserved_bytes == new_bytes
        && atomic_load_explicit(&rel->retained_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && wl_columnar_memory_reserved(governor) == new_bytes
        && snapshot.current_bytes == new_bytes,
        "governed compaction retry did not settle token");

    col_rel_destroy(rel);
    rel = NULL;
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    COMPACTION_CHECK(wl_columnar_memory_reserved(governor) == 0u
        && snapshot.current_bytes == 0u,
        "governed compaction final release consistency");
    wl_columnar_memory_governor_ref_release(ref);
    ref = NULL;
    PASS();
    goto done;

cleanup:
    if (rel && ref
        && atomic_load_explicit(&rel->retained_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING) {
        uint64_t replacement_bytes
            = rel->retained_reservation.replacement_bytes;
        atomic_store_explicit(&governor->reserved_bytes,
            rel->retained_reserved_bytes + replacement_bytes,
            memory_order_release);
        if (col_rel_transport_bytes(rel) == replacement_bytes)
            (void)col_rel_compact(rel);
        else
            (void)wl_columnar_memory_rollback_replacement(
                &rel->retained_reservation);
    }
    col_rel_destroy(rel);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    FAIL(failure ? failure : "governed compaction test failed");

done:
#undef COMPACTION_CHECK
    return;
}

/* ======================================================================== */
/* Delta Collector                                                          */
/* ======================================================================== */

#define MAX_DELTAS 256
#define MAX_COLS 16

typedef struct {
    int count;
    char relations[MAX_DELTAS][64];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
    int32_t diffs[MAX_DELTAS];
} delta_collector_t;

static void
collect_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    delta_collector_t *c = (delta_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols;
    c->diffs[idx] = diff;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

/* ======================================================================== */
/* Tuple Collector (snapshot - no diff)                                    */
/* ======================================================================== */

typedef struct {
    int count;
    char relations[MAX_DELTAS][64];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
} tuple_collector_t;

static void
collect_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    tuple_collector_t *c = (tuple_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

/* ======================================================================== */
/* Delta Query Helpers                                                      */
/* ======================================================================== */

static int
count_deltas(const delta_collector_t *c, const char *relation, int32_t diff)
{
    int n = 0;
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) == 0 && c->diffs[i] == diff)
            n++;
    }
    return n;
}

static bool
has_delta(const delta_collector_t *c, const char *relation,
    const int64_t *expected, uint32_t ncols, int32_t diff)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] != ncols || c->diffs[i] != diff)
            continue;
        bool match = true;
        for (uint32_t j = 0; j < ncols; j++) {
            if (c->rows[i][j] != expected[j]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

static bool
has_tuple(const tuple_collector_t *c, const char *relation,
    const int64_t *expected, uint32_t ncols)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] != ncols)
            continue;
        bool match = true;
        for (uint32_t j = 0; j < ncols; j++) {
            if (c->rows[i][j] != expected[j]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Helper: Build plan from Datalog source                                   */
/* ======================================================================== */

static wl_plan_t *
build_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }
    plan_fixture_hold(prog);
    return plan;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/* Issue #1431: the program's intern table is charged to the governor of the
 * first session created from its plan, a second session from the same plan
 * tolerates that the table is already owned (EBUSY), destroying either
 * session leaves the program-owned reservation in place, and only
 * wirelog_program_free() releases it. */
static void
test_intern_reservation_program_lifetime(void)
{
    TEST("session: intern reservation is program-owned and survives destroy");
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        "edge(1, 2).\n"
        "path(x, y) :- edge(x, y).\n", &err);
    wl_plan_t *plan = NULL;
    wl_session_t *first = NULL;
    wl_session_t *second = NULL;
    wl_columnar_memory_governor_ref_t *owner = NULL;
    uint64_t with_first;
    uint64_t retained;

    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    if (wl_session_create(wl_backend_columnar(), plan, 1, &first) != 0
        || !first) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("first session create failed");
        return;
    }
    owner = COL_SESSION(first)->memory_governor;
    wl_columnar_memory_governor_ref_retain(owner);
    with_first = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner));

    /* The second session gets its own governor; the intern stays with the
     * first one and is neither re-charged nor stolen. */
    if (wl_session_create(wl_backend_columnar(), plan, 1, &second) != 0
        || !second
        || COL_SESSION(second)->memory_governor == owner
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != with_first) {
        if (second)
            wl_session_destroy(second);
        wl_session_destroy(first);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("second session did not tolerate the owned intern table");
        return;
    }
    wl_session_destroy(second);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != with_first) {
        wl_session_destroy(first);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("destroying the second session changed the owner's reservation");
        return;
    }
    wl_session_destroy(first);
    retained = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner));
    wl_plan_free(plan);
    if (retained == 0 || retained > with_first
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != retained) {
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("session destroy released or kept the wrong intern bytes");
        return;
    }
    wirelog_program_free(prog);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != 0) {
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("wirelog_program_free did not release the intern reservation");
        return;
    }
    wl_columnar_memory_governor_ref_release(owner);
    PASS();
}

/* Issue #1469: once no live session, worker or result holds the first
 * session's governor, the next session created from the same plan rebinds
 * the program-owned intern table to its own governor with the exact
 * retained footprint, and wirelog_program_free() releases it from there.
 * On the previous code the second governor never owned the table, so its
 * reservation read 0 after the session was destroyed. */
static void
test_intern_rebinds_to_next_session(void)
{
    TEST("session: orphaned intern reservation rebinds to the next session");
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        "edge(1, 2).\n"
        "path(x, y) :- edge(x, y).\n", &err);
    wl_plan_t *plan = NULL;
    wl_session_t *first = NULL;
    wl_session_t *second = NULL;
    wl_columnar_memory_governor_ref_t *owner = NULL;
    wl_columnar_memory_governor_ref_t *next = NULL;
    uint64_t retained;
    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    if (wl_session_create(wl_backend_columnar(), plan, 1, &first) != 0
        || !first) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("first session create failed");
        return;
    }
    owner = COL_SESSION(first)->memory_governor;
    wl_columnar_memory_governor_ref_retain(owner);
    wl_session_destroy(first);
    retained = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner));
    /* Drop this test's reference so the table is the only holder.  The
     * owner must not be touched after this point: the rebind frees it. */
    wl_columnar_memory_governor_ref_release(owner);
    owner = NULL;
    if (retained == 0
        || wl_session_create(wl_backend_columnar(), plan, 1, &second) != 0
        || !second) {
        if (second)
            wl_session_destroy(second);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("second session create failed after the owner was released");
        return;
    }
    next = COL_SESSION(second)->memory_governor;
    wl_columnar_memory_governor_ref_retain(next);
    wl_session_destroy(second);
    wl_plan_free(plan);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(next)) != retained) {
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(next);
        FAIL("intern table was not rebound to the next session's governor");
        return;
    }
    wirelog_program_free(prog);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(next)) != 0) {
        wl_columnar_memory_governor_ref_release(next);
        FAIL("wirelog_program_free did not release the rebound reservation");
        return;
    }
    wl_columnar_memory_governor_ref_release(next);
    PASS();
}

/* Issue #1469: a governor reference that outlives its session, as a live
 * wirelog_result_t holds one, keeps the table with that governor: the next
 * session gets EBUSY and does not take the bytes; once that reference is
 * released the following session rebinds. */
static void
test_intern_rebind_waits_for_last_holder(void)
{
    TEST("session: a surviving governor holder keeps the intern table");
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        "edge(1, 2).\n"
        "path(x, y) :- edge(x, y).\n", &err);
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    wl_columnar_memory_governor_ref_t *holder = NULL;
    wl_columnar_memory_governor_ref_t *next = NULL;
    uint64_t retained;
    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0
        || !sess) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("first session create failed");
        return;
    }
    /* Stand-in for a result handle: retains the session's governor and
     * survives the session. */
    holder = COL_SESSION(sess)->memory_governor;
    wl_columnar_memory_governor_ref_retain(holder);
    wl_session_destroy(sess);
    sess = NULL;
    retained = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(holder));
    if (retained == 0
        || wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0
        || !sess) {
        if (sess)
            wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(holder);
        FAIL("second session create failed while the holder survived");
        return;
    }
    next = COL_SESSION(sess)->memory_governor;
    wl_columnar_memory_governor_ref_retain(next);
    wl_session_destroy(sess);
    sess = NULL;
    /* The held governor kept the table; the new one never took it. */
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(holder)) != retained
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(next)) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(holder);
        wl_columnar_memory_governor_ref_release(next);
        FAIL("a surviving holder did not keep the intern table");
        return;
    }
    wl_columnar_memory_governor_ref_release(next);
    next = NULL;
    /* The holder goes away: the following session rebinds. */
    wl_columnar_memory_governor_ref_release(holder);
    holder = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0
        || !sess) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("third session create failed after the holder was released");
        return;
    }
    next = COL_SESSION(sess)->memory_governor;
    wl_columnar_memory_governor_ref_retain(next);
    wl_session_destroy(sess);
    wl_plan_free(plan);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(next)) != retained) {
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(next);
        FAIL("the session after the last holder did not rebind the table");
        return;
    }
    wirelog_program_free(prog);
    wl_columnar_memory_governor_ref_release(next);
    PASS();
}

/*
 * Test: create a session and destroy it without crash.
 */
static void
test_session_create_destroy_impl(const wl_compute_backend_t *backend)
{
    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(backend, ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

static void
test_session_create_destroy(void)
{
    TEST("session(columnar): create and destroy");
    test_session_create_destroy_impl(wl_backend_columnar());
}

static void
test_session_create_destroy_columnar(void)
{
    TEST("session(columnar): create and destroy (duplicate check)");
    test_session_create_destroy_impl(wl_backend_columnar());
}

static void
test_session_create_with_options(void)
{
    TEST("session: internal host options are accepted");
    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    wl_session_options_t options;
    wl_session_t *session = NULL;
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }
    wl_session_options_init(&options);
    /* Invalid on every host, but safe: providers must fall back to advisory. */
    options.windows_job_handle = (void *)(uintptr_t)1;
    int rc = wl_session_create_with_options(wl_backend_columnar(), ffi, 1,
            &options, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session options creation failed");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* Issue #1473: injected governor and create-time admission                 */
/* ======================================================================== */

static wl_columnar_memory_governor_ref_t *
enforcing_governor(uint64_t usable)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = usable;
    resolution.usable_bytes = usable;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static uint64_t
reserved_on(wl_columnar_memory_governor_ref_t *ref)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
}

/* Bytes a session admits for the program's intern table when it attaches
 * the table (#1431), measured with a throwaway governor and detached again
 * so the table is free for the session under test. */
static int
probe_intern_bytes(wl_intern_t *intern, uint64_t *out)
{
    wl_columnar_memory_governor_ref_t *probe
        = enforcing_governor(UINT64_MAX / 4u);
    int rc;
    if (!probe)
        return -1;
    rc = wl_intern_attach_memory_governor(intern, probe);
    if (rc == 0) {
        *out = reserved_on(probe);
        rc = wl_intern_detach_memory_governor(intern, probe);
        if (rc == 0 && reserved_on(probe) != 0)
            rc = -1;
    }
    wl_columnar_memory_governor_ref_release(probe);
    return rc;
}

/* Bytes the session's fixed compound arena admits at creation, using the
 * same shape col_session_create_internal builds (seed, default_gen_cap and
 * the library-default epoch count; like the budget tests this assumes
 * WIRELOG_COMPOUND_MAX_EPOCHS is unset in the test environment). */
static int
probe_compound_bytes(uint64_t *out)
{
    wl_columnar_memory_governor_ref_t *probe
        = enforcing_governor(UINT64_MAX / 4u);
    wl_compound_arena_t *arena;
    int rc = 0;
    if (!probe)
        return -1;
    arena = wl_compound_arena_create_managed(0x53455353u, 4096u, 0u,
            wl_columnar_memory_governor_ref_get(probe));
    if (!arena) {
        rc = -1;
    } else {
        *out = reserved_on(probe);
        wl_compound_arena_free(arena);
        if (reserved_on(probe) != 0)
            rc = -1;
    }
    wl_columnar_memory_governor_ref_release(probe);
    return rc;
}

/* A fact-free program: nothing but the create-time floor (intern table plus
 * fixed compound arena; the delta pool and eval arena degrade to malloc) is
 * admitted at session creation. */
static const char *INJECTED_GOVERNOR_SRC
    = ".decl edge(x: int64, y: int64)\n"
    ".decl path(x: int64, y: int64)\n"
    "path(x, y) :- edge(x, y).\n";

static void
test_session_injected_governor_admission(void)
{
    wirelog_error_t err;
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    wl_session_options_t options;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    wl_columnar_memory_reservation_t token;
    wl_mem_ledger_snapshot_t ledger;
    uint64_t intern_bytes = 0;
    uint64_t compound_bytes = 0;
    int rc;

    TEST("session(#1473): injected governor denies below the intern floor");
    prog = wirelog_parse_string(INJECTED_GOVERNOR_SRC, &err);
    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    /* The overflow case below needs more than 16 intern bytes so that the
     * pre-reserved total overflows instead of being merely denied. */
    if (probe_intern_bytes(plan->intern, &intern_bytes) != 0
        || intern_bytes <= 16u || probe_compound_bytes(&compound_bytes) != 0
        || compound_bytes == 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("could not measure the create-time floor");
        return;
    }

    /* The whole intern table is admitted transactionally; one byte short
     * fails creation before anything else is charged and leaves the
     * caller's reference untouched (LSan proves the refcount). */
    ref = enforcing_governor(intern_bytes - 1u);
    wl_session_options_init(&options);
    options.memory_governor = ref;
    session = NULL;
    rc = ref ? wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session) : -1;
    if (!ref || rc != ENOMEM || session != NULL || reserved_on(ref) != 0) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("intern-floor denial did not fail creation with ENOMEM");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* The intern table fits but the compound arena does not: creation fails
     * and the oom path detaches the table it attached. */
    TEST("session(#1473): injected governor denies below the compound floor");
    ref = enforcing_governor(intern_bytes + compound_bytes - 1u);
    options.memory_governor = ref;
    session = NULL;
    rc = ref ? wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session) : -1;
    if (!ref || rc != ENOMEM || session != NULL || reserved_on(ref) != 0) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("compound-floor denial did not fail creation with ENOMEM");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* Exact fit: creation succeeds on the injected governor itself, charges
     * exactly the floor, and destroy releases everything but the
     * program-owned intern reservation. */
    TEST("session(#1473): injected governor admits the exact floor");
    ref = enforcing_governor(intern_bytes + compound_bytes);
    options.memory_governor = ref;
    session = NULL;
    rc = ref ? wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session) : -1;
    if (rc == 0 && session)
        wl_mem_ledger_snapshot(&COL_SESSION(session)->mem_ledger, &ledger);
    if (!ref || rc != 0 || !session
        || COL_SESSION(session)->memory_governor != ref
        || wl_session_memory_governor(session)
        != wl_columnar_memory_governor_ref_get(ref)
        || reserved_on(ref) != intern_bytes + compound_bytes
        || ledger.total_budget != intern_bytes + compound_bytes) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("exact-fit creation did not use the injected governor");
        return;
    }
    wl_session_destroy(session);
    if (reserved_on(ref) != intern_bytes) {
        wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("destroy did not leave exactly the intern reservation");
        return;
    }
    wl_plan_free(plan);
    wirelog_program_free(prog);
    prog = NULL;
    plan = NULL;
    if (reserved_on(ref) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("program free did not release the intern reservation");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* Arithmetic overflow of the governor total is EOVERFLOW, not ENOMEM. */
    TEST("session(#1473): injected governor overflow fails with EOVERFLOW");
    prog = wirelog_parse_string(INJECTED_GOVERNOR_SRC, &err);
    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    ref = enforcing_governor(UINT64_MAX);
    wl_columnar_memory_reservation_init(&token);
    if (!ref || wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(ref), UINT64_MAX - 16u,
            &token) != WL_COLUMNAR_MEMORY_ADMISSION_OK) {
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("overflow fixture setup failed");
        return;
    }
    options.memory_governor = ref;
    session = NULL;
    rc = wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session);
    if (rc != EOVERFLOW || session != NULL
        || reserved_on(ref) != UINT64_MAX - 16u) {
        if (session)
            wl_session_destroy(session);
        (void)wl_columnar_memory_release(&token);
        wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("governor overflow did not fail creation with EOVERFLOW");
        return;
    }
    (void)wl_columnar_memory_release(&token);
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* The test-only per-thread default stands in for options on the
     * option-less creators used by the public facades. */
    TEST("session(#1473): thread default options reach option-less create");
    ref = enforcing_governor(intern_bytes - 1u);
    options.memory_governor = ref;
    wl_session_testhook_set_default_options(&options);
    session = NULL;
    rc = ref ? wl_session_create(wl_backend_columnar(), plan, 1, &session)
        : -1;
    wl_session_testhook_set_default_options(NULL);
    if (!ref || rc != ENOMEM || session != NULL || reserved_on(ref) != 0
        || wl_session_testhook_default_options() != NULL) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("thread default options were not applied");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("clearing the thread default did not restore resolution");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

#ifdef _WIN32
/*
 * The provider and session plumbing must be tested with a real Job Object,
 * not only with injected resolver fields.  The session borrows the handle
 * during creation, so closing the caller's handle before destruction also
 * verifies that the session does not retain or close it.
 */
static void
test_session_windows_job_options(void)
{
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits;
    HANDLE job = NULL;
    char *saved_budget = NULL;
    size_t saved_budget_len = 0;
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    wl_session_options_t options;
    wl_columnar_memory_governor_t *governor;
    const uint64_t expected_budget = UINT64_C(512) * 1024 * 1024;
    int ok = 0;

    TEST("session: real Windows Job Object memory source");
    if (_dupenv_s(&saved_budget, &saved_budget_len,
        "WIRELOG_MEMORY_BUDGET") != 0)
        saved_budget = NULL;
    /* An explicit environment budget intentionally has precedence. */
    if (_putenv_s("WIRELOG_MEMORY_BUDGET", "") != 0) {
        FAIL("could not clear WIRELOG_MEMORY_BUDGET");
        goto cleanup;
    }

    job = CreateJobObjectW(NULL, NULL);
    if (!job) {
        FAIL("CreateJobObjectW failed");
        goto cleanup;
    }
    memset(&limits, 0, sizeof(limits));
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_PROCESS_MEMORY;
    limits.ProcessMemoryLimit = (SIZE_T)expected_budget;
    if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation,
        &limits, sizeof(limits))) {
        FAIL("SetInformationJobObject failed");
        goto cleanup;
    }

    plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate Job Object test plan");
        goto cleanup;
    }
    wl_session_options_init(&options);
    options.windows_job_handle = (void *)job;
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) != 0 || !session) {
        FAIL("session creation with Job Object failed");
        goto cleanup;
    }

    /* The session must have completed its synchronous probe by now. */
    CloseHandle(job);
    job = NULL;
    governor = wl_session_memory_governor(session);
    if (!governor
        || governor->source != WL_COLUMNAR_MEMORY_SOURCE_WINDOWS_JOB
        || governor->mode != WL_COLUMNAR_MEMORY_MODE_ENFORCING
        || governor->budget_bytes != expected_budget) {
        FAIL("session did not enforce the Job Object memory source");
        goto cleanup;
    }
    ok = 1;

cleanup:
    if (job)
        CloseHandle(job);
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    if (saved_budget)
        _putenv_s("WIRELOG_MEMORY_BUDGET", saved_budget);
    else
        _putenv_s("WIRELOG_MEMORY_BUDGET", "");
    free(saved_budget);
    if (ok)
        PASS();
}
#endif

/*
 * Test: num_workers > 1 is rejected.
 */
static void
test_session_create_multi_worker_rejected(void)
{
    TEST("session: multi-worker accepted");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 2, &session);
    wl_plan_free(ffi);

    if (rc == 0 && session) {
        wl_session_destroy(session);
        PASS();
    } else {
        FAIL("multi-worker session creation should succeed");
    }
}

/*
 * Test: initial step produces all results as diff=+1.
 * Datalog: r(X,Y,Z) :- a(X,Y), b(Y,Z).
 * EDB: a={(1,2)}, b={(2,3)}
 * Expected delta: r(1,2,3) with diff=+1
 */
static void
test_session_step_initial_delta(void)
{
    TEST("session: initial step produces diff=+1 deltas");

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, z: int32)\n"
            ".decl r(x: int32, y: int32, z: int32)\n"
            "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Set delta callback */
    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert EDB facts */
    int64_t a_data[] = { 1, 2 };
    rc = wl_session_insert(session, "a", a_data, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert a failed");
        return;
    }

    int64_t b_data[] = { 2, 3 };
    rc = wl_session_insert(session, "b", b_data, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert b failed");
        return;
    }

    /* Step: should produce delta r(1,2,3) with diff=+1 */
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("session_step failed");
        return;
    }

    int64_t expected[] = { 1, 2, 3 };
    if (!has_delta(&deltas, "r", expected, 3, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(1,2,3) diff=+1, got %d deltas total",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
            count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: incremental insert + step produces only new derivations.
 * After initial step (r(1,2,3)), insert a=(1,4), b=(4,5).
 * Second step should produce exactly r(1,4,5) with diff=+1.
 */
static void
test_session_step_incremental_delta(void)
{
    TEST("session: incremental step produces only new deltas");

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, z: int32)\n"
            ".decl r(x: int32, y: int32, z: int32)\n"
            "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Initial data */
    int64_t a1[] = { 1, 2 };
    int64_t b1[] = { 2, 3 };
    wl_session_insert(session, "a", a1, 1, 2);
    wl_session_insert(session, "b", b1, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset collector for second step */
    memset(&deltas, 0, sizeof(deltas));

    /* Insert new facts */
    int64_t a2[] = { 1, 4 };
    int64_t b2[] = { 4, 5 };
    wl_session_insert(session, "a", a2, 1, 2);
    wl_session_insert(session, "b", b2, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    /* Should have exactly r(1,4,5) with diff=+1 */
    int64_t expected[] = { 1, 4, 5 };
    if (!has_delta(&deltas, "r", expected, 3, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(1,4,5) diff=+1, got %d deltas total",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
            count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: step with no changes produces zero delta callbacks.
 */
static void
test_session_step_no_change(void)
{
    TEST("session: step with no changes produces no deltas");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert and step once */
    int64_t a_data[] = { 10 };
    wl_session_insert(session, "a", a_data, 1, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset collector, step again with no changes */
    memset(&deltas, 0, sizeof(deltas));
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    if (deltas.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 deltas, got %d", deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* RED Tests: remove and snapshot (require TASK 1-3 for GREEN)             */
/* ======================================================================== */

/*
 * Test: direct removal admits a writer before reading or compacting storage.
 */
static void
test_session_remove_admission(void)
{
    TEST("session: direct removal is admission-safe");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    int64_t value[] = { 1 };
    if (rc != 0 || !session
        || wl_session_insert(session, "a", value, 1, 1) != 0) {
        if (session)
            wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("session setup failed");
        return;
    }

    col_rel_t *source = NULL;
    for (uint32_t i = 0; i < COL_SESSION(session)->nrels; i++) {
        if (strcmp(COL_SESSION(session)->rels[i]->name, "a") == 0) {
            source = COL_SESSION(session)->rels[i];
            break;
        }
    }
    wl_columnar_source_access_reader_t reader = { 0 };
    uint64_t generation = source ? source->view_generation : 0;
    uint32_t rows = source ? source->nrows : 0;
    if (!source
        || wl_columnar_source_access_reader_acquire(
            &source->source_access, &reader) != 0
        || wl_session_remove(session, "a", value, 1, 1) != EBUSY
        || source->nrows != rows
        || source->view_generation != generation
        || wl_columnar_source_access_reader_release(&reader) != 0
        || wl_session_remove(session, "a", value, 1, 1) != 0
        || source->nrows != 0) {
        if (reader.owner)
            (void)wl_columnar_source_access_reader_release(&reader);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("direct removal was not admission-safe");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: removal is refused while a live storage alias borrows the source.
 *
 * A shared view borrows the source column buffers directly.  Removal
 * rewrites those buffers in place, so it must be refused with EBUSY while
 * any alias is outstanding, and must leave the source untouched.  The
 * check runs for both the direct and the delta-seeded removal path.
 */
static void
test_session_remove_rejects_live_alias(bool with_delta_cb)
{
    TEST(with_delta_cb
        ? "session: incremental removal refuses a live storage alias"
        : "session: direct removal refuses a live storage alias");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    if (rc == 0 && session && with_delta_cb)
        wl_session_set_delta_cb(session, collect_delta, &deltas);
    int64_t value[] = { 1 };
    if (rc != 0 || !session
        || wl_session_insert(session, "a", value, 1, 1) != 0) {
        if (session)
            wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("session setup failed");
        return;
    }

    col_rel_t *source = NULL;
    for (uint32_t i = 0; i < COL_SESSION(session)->nrels; i++) {
        if (strcmp(COL_SESSION(session)->rels[i]->name, "a") == 0) {
            source = COL_SESSION(session)->rels[i];
            break;
        }
    }
    col_rel_t *view = col_rel_new_auto("alias_view", 1);
    if (!source || !view) {
        col_rel_destroy(view);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("alias setup failed");
        return;
    }

    uint64_t generation = source->storage_generation;
    uint32_t rows = source->nrows;
    if (col_rel_install_shared_view(view, source) != 0
        || source->storage_alias_borrows != 1
        || wl_session_remove(session, "a", value, 1, 1) != EBUSY
        || source->nrows != rows
        || source->storage_generation != generation
        || source->storage_alias_borrows != 1) {
        col_rel_destroy(view);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("removal ignored a live storage alias");
        return;
    }

    /* Releasing the alias re-admits the removal. */
    if (col_rel_storage_alias_release(view) != 0
        || source->storage_alias_borrows != 0
        || wl_session_remove(session, "a", value, 1, 1) != 0
        || source->nrows != 0) {
        col_rel_destroy(view);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("removal stayed blocked after the alias was released");
        return;
    }
    col_rel_destroy(view);
    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: remove a tuple and step produces diff=-1 delta.
 *
 * RED: currently fails because dd_session_remove returns -1 (not implemented).
 * GREEN: after TASK 2+3 wire wl_dd_session_remove through the vtable.
 */
static void
test_session_remove_single_delta(void)
{
    TEST("session: remove tuple produces diff=-1 delta");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a=(1) and step: establishes r(1) */
    int64_t a_data[] = { 1 };
    rc = wl_session_insert(session, "a", a_data, 1, 1);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert failed");
        return;
    }
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset and remove a=(1) */
    memset(&deltas, 0, sizeof(deltas));
    col_rel_t *source = NULL;
    for (uint32_t i = 0; i < COL_SESSION(session)->nrels; i++) {
        if (strcmp(COL_SESSION(session)->rels[i]->name, "a") == 0) {
            source = COL_SESSION(session)->rels[i];
            break;
        }
    }
    wl_columnar_source_access_reader_t reader = { 0 };
    uint32_t rows_before = source ? source->nrows : 0;
    if (!source
        || wl_columnar_source_access_reader_acquire(
            &source->source_access, &reader) != 0
        || wl_session_remove(session, "a", a_data, 1, 1) != EBUSY
        || source->nrows != rows_before
        || wl_columnar_source_access_reader_release(&reader) != 0) {
        if (reader.owner)
            (void)wl_columnar_source_access_reader_release(&reader);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("incremental removal was not admission-safe");
        return;
    }
    rc = wl_session_remove(session, "a", a_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    /* Step: should produce r(1) diff=-1 */
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    int64_t expected[] = { 1 };
    if (!has_delta(&deltas, "r", expected, 1, -1)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected r(1) diff=-1, got %d total deltas",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", -1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 retraction, got %d",
            count_deltas(&deltas, "r", -1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: remove of non-existent tuple is a no-op (no crash, rc=0).
 *
 * RED: currently fails because dd_session_remove returns -1.
 * GREEN: after TASK 2+3 remove returns 0 and produces no phantom deltas.
 */
static void
test_session_remove_nonexistent(void)
{
    TEST("session: remove non-existent tuple is no-op");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Remove a=(99) which was never inserted */
    int64_t a_data[] = { 99 };
    rc = wl_session_remove(session, "a", a_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

static col_rel_t *
test_session_relation(wl_session_t *session, const char *name)
{
    wl_col_session_t *sess = COL_SESSION(session);
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && sess->rels[i]->name
            && strcmp(sess->rels[i]->name, name) == 0)
            return sess->rels[i];
    }
    return NULL;
}

static bool
test_session_remove_alias_borrow_case(bool incremental)
{
    const int64_t rows[] = { 1, 2, 3 };
    const int64_t remove_row[] = { 2 };
    wl_plan_t *ffi = NULL;
    wl_session_t *session = NULL;
    col_rel_t *alias = NULL;
    col_rel_t *input = NULL;
    col_rel_t *derived = NULL;
    wl_col_session_t *sess = NULL;
    delta_collector_t deltas = { 0 };
    uint32_t before_nrels = 0;
    uint32_t before_outer_epoch = 0;
    uint32_t before_input_rows = 0;
    uint32_t before_derived_rows = 0;
    uint32_t before_input_base = 0;
    uint32_t before_delta_count = 0;
    uint64_t before_input_view = 0;
    uint64_t before_input_storage = 0;
    uint64_t before_derived_view = 0;
    uint64_t before_derived_storage = 0;
    const char *before_last_inserted = NULL;
    const char *before_last_removed = NULL;
    bool before_pending = false;
    bool before_pending_full = false;
    bool before_snapshot_stable = false;
    bool before_delta_seeded = false;
    bool before_retraction_seeded = false;
    bool before_rdelta_present = false;
    bool before_d_delta_present = false;
    bool ok = false;
    int rc;

    ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi)
        goto cleanup;
    rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session)
        goto cleanup;
    if (incremental)
        wl_session_set_delta_cb(session, collect_delta, &deltas);
    rc = wl_session_insert(session, "a", rows, 3, 1);
    if (rc == 0)
        rc = wl_session_step(session);
    if (rc != 0)
        goto cleanup;

    sess = COL_SESSION(session);
    input = test_session_relation(session, "a");
    derived = test_session_relation(session, "r");
    if (!input || !derived || input->nrows != 3 || derived->nrows != 3)
        goto cleanup;
    alias = col_rel_new_auto("test_alias_a", input->ncols);
    if (!alias || col_rel_install_shared_view(alias, input) != 0)
        goto cleanup;

    before_nrels = sess->nrels;
    before_outer_epoch = sess->outer_epoch;
    before_input_rows = input->nrows;
    before_derived_rows = derived->nrows;
    before_input_base = input->base_nrows;
    before_input_view = input->view_generation;
    before_input_storage = input->storage_generation;
    before_derived_view = derived->view_generation;
    before_derived_storage = derived->storage_generation;
    before_last_inserted = sess->last_inserted_relation;
    before_last_removed = sess->last_removed_relation;
    before_pending = sess->pending_input_change;
    before_pending_full = sess->pending_full_input_eval;
    before_snapshot_stable = sess->snapshot_stable_valid;
    before_delta_seeded = sess->delta_seeded;
    before_retraction_seeded = sess->retraction_seeded;
    before_delta_count = (uint32_t)deltas.count;
    before_rdelta_present = test_session_relation(session, "$r$a") != NULL;
    before_d_delta_present = test_session_relation(session, "$d$a") != NULL;

    rc = wl_session_remove(session, "a", remove_row, 1, 1);
    if (rc != EBUSY || input->storage_alias_borrows != 1
        || input->nrows != before_input_rows
        || input->columns[0][0] != rows[0]
        || input->columns[0][1] != rows[1]
        || input->columns[0][2] != rows[2]
        || derived->nrows != before_derived_rows
        || derived->columns[0][0] != rows[0]
        || derived->columns[0][1] != rows[1]
        || derived->columns[0][2] != rows[2]
        || input->base_nrows != before_input_base
        || input->view_generation != before_input_view
        || input->storage_generation != before_input_storage
        || derived->view_generation != before_derived_view
        || derived->storage_generation != before_derived_storage
        || sess->nrels != before_nrels
        || sess->outer_epoch != before_outer_epoch
        || sess->last_inserted_relation != before_last_inserted
        || sess->last_removed_relation != before_last_removed
        || sess->pending_input_change != before_pending
        || sess->pending_full_input_eval != before_pending_full
        || sess->snapshot_stable_valid != before_snapshot_stable
        || sess->delta_seeded != before_delta_seeded
        || sess->retraction_seeded != before_retraction_seeded
        || (uint32_t)deltas.count != before_delta_count
        || (test_session_relation(session, "$r$a") != NULL)
        != before_rdelta_present
        || (test_session_relation(session, "$d$a") != NULL)
        != before_d_delta_present)
        goto cleanup;

    col_rel_destroy(alias);
    alias = NULL;
    if (input->storage_alias_borrows != 0)
        goto cleanup;
    rc = wl_session_remove(session, "a", remove_row, 1, 1);
    if (rc != 0 || input->nrows != 2 || input->columns[0][0] != rows[0]
        || input->columns[0][1] != rows[2])
        goto cleanup;
    if (incremental) {
        col_rel_t *rdelta = test_session_relation(session, "$r$a");
        if (!rdelta || rdelta->nrows != 1
            || rdelta->columns[0][0] != remove_row[0]
            || sess->nrels != before_nrels + 1u
            || sess->last_removed_relation != input->name)
            goto cleanup;
    } else if (sess->nrels != before_nrels
        || test_session_relation(session, "$r$a") != NULL) {
        goto cleanup;
    }
    ok = true;

cleanup:
    if (alias)
        col_rel_destroy(alias);
    if (session)
        wl_session_destroy(session);
    if (ffi)
        wl_plan_free(ffi);
    return ok;
}

static void
test_session_remove_live_alias(void)
{
    TEST(
        "session: direct removal rejects a live storage alias transactionally");
    if (!test_session_remove_alias_borrow_case(false)) {
        FAIL("direct alias-blocked removal changed state or retry failed");
        return;
    }
    PASS();

    TEST(
        "session: incremental removal rejects a live storage alias transactionally");
    if (!test_session_remove_alias_borrow_case(true)) {
        FAIL("incremental alias-blocked removal changed state or retry failed");
        return;
    }
    PASS();
}

static void
test_session_remove_wide_alias_before_allocation(void)
{
    TEST("session: direct removal rejects wide live aliases before allocation");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n");
    wl_session_t *session = NULL;
    col_rel_t *wide = NULL;
    col_rel_t *alias = NULL;
    int64_t remove_row[COL_STACK_MAX + 1u] = { 0 };
    int rc = ffi ? wl_session_create(wl_backend_columnar(), ffi, 1, &session)
                 : ENOMEM;
    if (rc != 0 || !session)
        goto fail;

    wide = col_rel_new_auto("wide", COL_STACK_MAX + 1u);
    alias = col_rel_new_auto("wide_alias", COL_STACK_MAX + 1u);
    if (!wide || !alias || session_add_rel(COL_SESSION(session), wide) != 0)
        goto fail;
    wide = NULL;
    if (col_rel_install_shared_view(alias,
        test_session_relation(session, "wide")) != 0)
        goto fail;

#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = true;
#endif
    rc = wl_session_remove(session, "wide", remove_row, 1,
            COL_STACK_MAX + 1u);
#ifdef WL_TEST_ALLOC_WRAP
    bool allocation_was_not_attempted = fail_next_alloc;
    fail_next_alloc = false;
    if (rc != EBUSY || !allocation_was_not_attempted)
        goto fail;
#else
    if (rc != EBUSY)
        goto fail;
#endif

    col_rel_destroy(alias);
    alias = NULL;
    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
    return;

fail:
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (alias)
        col_rel_destroy(alias);
    if (wide)
        col_rel_destroy(wide);
    if (session)
        wl_session_destroy(session);
    if (ffi)
        wl_plan_free(ffi);
    FAIL("wide alias removal attempted allocation or did not return EBUSY");
}

static void
test_session_remove_reader_exclusion(void)
{
    TEST("session: removal is excluded while a source reader is active");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }
    int64_t a_data[] = { 1, 2, 3 };
    rc = wl_session_insert(session, "a", a_data, 3, 1);
    if (rc == 0)
        rc = wl_session_step(session);
    col_rel_t *rel = test_session_relation(session, "a");
    if (rc != 0 || !rel) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert or relation lookup failed");
        return;
    }
    uint32_t before_rows = rel->nrows;
    uint64_t before_view = rel->view_generation;
    int64_t before_value = rel->columns[0][1];
    wl_columnar_source_access_reader_t reader = { 0 };
    if (col_rel_source_reader_acquire(rel, &reader) != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("reader acquisition failed");
        return;
    }
    rc = wl_session_remove(session, "a", &a_data[1], 1, 1);
    int release_rc = col_rel_source_reader_release(&reader);
    if (rc != EBUSY || release_rc != 0
        || rel->nrows != before_rows
        || rel->view_generation != before_view
        || rel->columns[0][1] != before_value) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("reader-blocked plain removal was not transactional");
        return;
    }
    rc = wl_session_remove(session, "a", &a_data[1], 1, 1);
    if (rc != 0 || rel->nrows != before_rows - 1u
        || rel->columns[0][1] != 3) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("plain removal retry failed");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

static void
test_session_remove_incremental_reader_exclusion(void)
{
    TEST(
        "session: incremental removal is excluded while a source reader is active");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }
    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    int64_t value = 7;
    rc = wl_session_insert(session, "a", &value, 1, 1);
    col_rel_t *rel = test_session_relation(session, "a");
    if (rc != 0 || !rel) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert or relation lookup failed");
        return;
    }
    uint32_t before_rows = rel->nrows;
    uint32_t before_nrels = COL_SESSION(session)->nrels;
    wl_columnar_source_access_reader_t reader = { 0 };
    if (col_rel_source_reader_acquire(rel, &reader) != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("reader acquisition failed");
        return;
    }
    rc = wl_session_remove(session, "a", &value, 1, 1);
    int release_rc = col_rel_source_reader_release(&reader);
    if (rc != EBUSY || release_rc != 0 || rel->nrows != before_rows
        || COL_SESSION(session)->nrels != before_nrels
        || test_session_relation(session, "$r$a") != NULL) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("reader-blocked incremental removal changed session state");
        return;
    }
    rc = wl_session_remove(session, "a", &value, 1, 1);
    if (rc != 0 || rel->nrows != 0
        || test_session_relation(session, "$r$a") == NULL) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("incremental removal retry failed");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

static void
test_session_full_idb_clear_reader_exclusion(void)
{
    TEST("session: full IDB clear is transactional across source owners");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl b(x: int32)\n"
            ".decl r(x: int32)\n"
            ".decl s(x: int32)\n"
            "r(x) :- a(x).\n"
            "s(x) :- b(x).\n");
    wl_session_t *session = NULL;
    tuple_collector_t tuples = { 0 };
    tuple_collector_t retry_tuples = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    bool reader_active = false;
    col_rel_t *reader_owner = NULL;
    col_rel_t *r = NULL;
    col_rel_t *s = NULL;
    col_rel_t *a = NULL;
    col_rel_t *b = NULL;
    wl_col_session_t *sess = NULL;
    uint32_t before_r_rows = 0;
    uint32_t before_s_rows = 0;
    uint32_t before_a_rows = 0;
    uint32_t before_b_rows = 0;
    int64_t before_r_value = 0;
    int64_t before_s_value = 0;
    int64_t before_a_first = 0;
    int64_t before_a_last = 0;
    int64_t before_b_first = 0;
    int64_t before_b_last = 0;
    uint64_t before_r_view_generation = 0;
    uint64_t before_s_view_generation = 0;
    uint64_t before_r_storage_generation = 0;
    uint64_t before_s_storage_generation = 0;
    uint64_t before_a_view_generation = 0;
    uint64_t before_b_view_generation = 0;
    uint64_t before_a_storage_generation = 0;
    uint64_t before_b_storage_generation = 0;
    uint32_t before_tuple_count = 0;
    uint32_t before_outer_epoch = 0;
    const char *before_last_inserted = NULL;
    const char *before_last_removed = NULL;
    bool before_pending_input_change = false;
    bool before_pending_full_input_eval = false;
    bool before_snapshot_stable_valid = false;
    bool before_delta_seeded = false;
    bool before_retraction_seeded = false;
    int rc = 0;
    const char *failure = NULL;

    if (!ffi) {
        failure = "could not generate FFI plan";
        goto cleanup;
    }
    rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        failure = "session_create failed";
        goto cleanup;
    }

    int64_t a_initial = 1;
    int64_t b_initial = 2;
    rc = wl_session_insert(session, "a", &a_initial, 1, 1);
    if (rc == 0)
        rc = wl_session_insert(session, "b", &b_initial, 1, 1);
    if (rc == 0)
        rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        failure = "initial IDB snapshot failed";
        goto cleanup;
    }

    r = test_session_relation(session, "r");
    s = test_session_relation(session, "s");
    a = test_session_relation(session, "a");
    b = test_session_relation(session, "b");
    sess = COL_SESSION(session);
    col_rel_t *r_owner = NULL;
    col_rel_t *s_owner = NULL;
    uint32_t r_plan_pos = UINT32_MAX;
    uint32_t s_plan_pos = UINT32_MAX;
    if (!a || !b || !r || !s
        || col_rel_storage_owner_resolve(r, &r_owner) != 0
        || col_rel_storage_owner_resolve(s, &s_owner) != 0
        || r_owner == s_owner) {
        failure = "could not resolve source and distinct IDB owners";
        goto cleanup;
    }

    uint32_t plan_pos = 0;
    for (uint32_t si = 0; si < sess->plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &sess->plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++, plan_pos++) {
            if (strcmp(sp->relations[ri].name, "r") == 0)
                r_plan_pos = plan_pos;
            if (strcmp(sp->relations[ri].name, "s") == 0)
                s_plan_pos = plan_pos;
        }
    }
    if (r_plan_pos >= s_plan_pos) {
        failure = "test plan does not place r before s";
        goto cleanup;
    }

    if (r->nrows != 1u || s->nrows != 1u
        || a->nrows != 1u || b->nrows != 1u
        || r->ncols != 1u || s->ncols != 1u
        || a->ncols != 1u || b->ncols != 1u) {
        failure = "initial source or IDB targets were not as expected";
        goto cleanup;
    }
    before_r_rows = r->nrows;
    before_s_rows = s->nrows;
    before_a_rows = a->nrows;
    before_b_rows = b->nrows;
    before_r_value = r->columns[0][0];
    before_s_value = s->columns[0][0];
    before_a_first = a->columns[0][0];
    before_a_last = before_a_first;
    before_b_first = b->columns[0][0];
    before_b_last = before_b_first;
    before_r_view_generation = r->view_generation;
    before_s_view_generation = s->view_generation;
    before_r_storage_generation = r->storage_generation;
    before_s_storage_generation = s->storage_generation;
    before_a_view_generation = a->view_generation;
    before_b_view_generation = b->view_generation;
    before_a_storage_generation = a->storage_generation;
    before_b_storage_generation = b->storage_generation;

    /* The plan visits s after r.  Holding s's reader ensures a clear that
     * mutates targets in plan order cannot leave r changed before it notices
     * the later owner is busy. */
    reader_owner = s_owner;
    rc = col_rel_source_reader_acquire(reader_owner, &reader);
    if (rc != 0) {
        failure = "reader acquisition on an IDB owner failed";
        goto cleanup;
    }
    reader_active = true;

    int64_t a_next = 3;
    int64_t b_next = 4;
    rc = wl_session_insert(session, "a", &a_next, 1, 1);
    if (rc == 0)
        rc = wl_session_insert(session, "b", &b_next, 1, 1);
    if (rc != 0) {
        failure = "source updates for full IDB clear setup failed";
        goto cleanup;
    }
    before_a_rows = a->nrows;
    before_b_rows = b->nrows;
    before_a_first = a->columns[0][0];
    before_b_first = b->columns[0][0];
    before_a_view_generation = a->view_generation;
    before_b_view_generation = b->view_generation;
    before_a_storage_generation = a->storage_generation;
    before_b_storage_generation = b->storage_generation;
    before_tuple_count = (uint32_t)tuples.count;
    before_outer_epoch = sess->outer_epoch;
    before_last_inserted = sess->last_inserted_relation;
    before_last_removed = sess->last_removed_relation;
    before_pending_input_change = sess->pending_input_change;
    before_pending_full_input_eval = sess->pending_full_input_eval;
    before_snapshot_stable_valid = sess->snapshot_stable_valid;
    before_delta_seeded = sess->delta_seeded;
    before_retraction_seeded = sess->retraction_seeded;
    before_a_last = a->columns[0][a->nrows - 1u];
    before_b_last = b->columns[0][b->nrows - 1u];
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != EBUSY) {
        failure =
            "full IDB clear did not return EBUSY for a later owner reader";
        goto cleanup;
    }
    if ((uint32_t)tuples.count != before_tuple_count
        || a->nrows != before_a_rows
        || b->nrows != before_b_rows
        || a->columns[0][0] != before_a_first
        || a->columns[0][a->nrows - 1u] != before_a_last
        || b->columns[0][0] != before_b_first
        || b->columns[0][b->nrows - 1u] != before_b_last
        || sess->outer_epoch != before_outer_epoch
        || sess->last_inserted_relation != before_last_inserted
        || sess->last_removed_relation != before_last_removed
        || sess->pending_input_change != before_pending_input_change
        || sess->pending_full_input_eval != before_pending_full_input_eval
        || sess->snapshot_stable_valid != before_snapshot_stable_valid
        || sess->delta_seeded != before_delta_seeded
        || sess->retraction_seeded != before_retraction_seeded) {
        failure = "reader-blocked full clear changed input state";
        goto cleanup;
    }
    if (r->nrows != before_r_rows || s->nrows != before_s_rows
        || r->columns[0][0] != before_r_value
        || s->columns[0][0] != before_s_value
        || r->view_generation != before_r_view_generation
        || s->view_generation != before_s_view_generation
        || r->storage_generation != before_r_storage_generation
        || s->storage_generation != before_s_storage_generation
        || a->view_generation != before_a_view_generation
        || b->view_generation != before_b_view_generation
        || a->storage_generation != before_a_storage_generation
        || b->storage_generation != before_b_storage_generation
        || r_owner->storage_generation != before_r_storage_generation
        || s_owner->storage_generation != before_s_storage_generation) {
        failure = "reader-blocked full clear changed an IDB relation";
        goto cleanup;
    }

cleanup:
    if (reader_active) {
        int release_rc = col_rel_source_reader_release(&reader);
        reader_active = false;
        if (!failure && release_rc != 0)
            failure = "source reader release failed";
    }
    if (!failure && session) {
        rc = wl_session_snapshot(session, collect_tuple, &retry_tuples);
        if (rc != 0 || retry_tuples.count != 4
            || !has_tuple(&retry_tuples, "r", (int64_t[]){ 1 }, 1)
            || !has_tuple(&retry_tuples, "r", (int64_t[]){ 3 }, 1)
            || !has_tuple(&retry_tuples, "s", (int64_t[]){ 2 }, 1)
            || !has_tuple(&retry_tuples, "s", (int64_t[]){ 4 }, 1))
            failure = "full IDB snapshot retry failed after reader release";
    }
    if (session)
        wl_session_destroy(session);
    if (ffi)
        wl_plan_free(ffi);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
}

/*
 * Test: snapshot on empty session returns 0 tuples with rc=0.
 */
static void
test_session_snapshot_empty(void)
{
    TEST("session: snapshot of empty session returns 0 tuples");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    tuple_collector_t tuples;
    memset(&tuples, 0, sizeof(tuples));
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (tuples.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 tuples, got %d", tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: snapshot after insert+step reflects current derived state.
 *
 * RED: currently fails because dd_session_snapshot uses empty batch worker.
 * GREEN: after TASK 2+3 snapshot reads from persistent session state.
 */
static void
test_session_snapshot_after_insert(void)
{
    TEST("session: snapshot reflects current derived state");

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, z: int32)\n"
            ".decl r(x: int32, y: int32, z: int32)\n"
            "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Insert a=(1,2), b=(2,3) and step */
    int64_t a_data[] = { 1, 2 };
    int64_t b_data[] = { 2, 3 };
    wl_session_insert(session, "a", a_data, 1, 2);
    wl_session_insert(session, "b", b_data, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("step failed");
        return;
    }

    /* Snapshot: should contain r(1,2,3) */
    tuple_collector_t tuples;
    memset(&tuples, 0, sizeof(tuples));
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    int64_t expected[] = { 1, 2, 3 };
    if (!has_tuple(&tuples, "r", expected, 3)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(1,2,3) in snapshot, got %d tuples total",
            tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (tuples.count != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected exactly 1 tuple, got %d",
            tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: duplicate insert of the same tuple produces exactly one positive delta.
 * DD set semantics via consolidate should not double-count.
 */
static void
test_session_duplicate_insert(void)
{
    TEST("session: duplicate insert produces single positive delta");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a=(5) twice before stepping */
    int64_t a_data[] = { 5 };
    wl_session_insert(session, "a", a_data, 1, 1);
    wl_session_insert(session, "a", a_data, 1, 1);

    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("step failed");
        return;
    }

    int64_t expected[] = { 5 };
    if (!has_delta(&deltas, "r", expected, 1, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected r(5) diff=+1, got %d deltas total",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected exactly 1 positive delta (set semantics), got %d",
            count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* TEST: Transitive Closure Insert (RED phase - will FAIL)                 */
/* ======================================================================== */

static void
test_session_tc_insert(void)
{
    TEST("session: transitive closure insert test (recursive iterate)");

    /* Program:
       tc(X, Y) :- edge(X, Y).
       tc(X, Z) :- edge(X, Y), tc(Y, Z).
     */
    wl_plan_t *ffi = build_plan(".decl edge(x: int32, y: int32)\n"
            ".decl tc(x: int32, y: int32)\n"
            "tc(X, Y) :- edge(X, Y).\n"
            "tc(X, Z) :- edge(X, Y), tc(Y, Z).\n");

    if (!ffi) {
        FAIL("failed to parse TC program");
        return;
    }

    /* Create persistent session */
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0) {
        wl_plan_free(ffi);
        FAIL("session creation failed");
        return;
    }

    /* Setup delta collector */
    delta_collector_t deltas = { 0 };
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Step 1: Insert edge(1,2), edge(2,3) */
    int64_t edge_12[] = { 1, 2 };
    int64_t edge_23[] = { 2, 3 };
    wl_session_insert(session, "edge", edge_12, 1, 2);
    wl_session_insert(session, "edge", edge_23, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Should have TC: (1,2), (2,3), (1,3) */
    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 2 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,2) with diff=+1");
        return;
    }
    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 3 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,3) with diff=+1 (recursive)");
        return;
    }

    /* Reset for step 2 */
    memset(&deltas, 0, sizeof(deltas));

    /* Step 2: Insert edge(3,4) - should derive tc(1,4), tc(2,4), tc(3,4) */
    int64_t edge_34[] = { 3, 4 };
    wl_session_insert(session, "edge", edge_34, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    /* Verify exactly 3 new TC tuples */
    int new_tc_count = count_deltas(&deltas, "tc", +1);
    if (new_tc_count != 3) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 3 new TC deltas, got %d",
            new_tc_count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 4 }, 2, +1)
        || !has_delta(&deltas, "tc", (int64_t[]){ 2, 4 }, 2, +1)
        || !has_delta(&deltas, "tc", (int64_t[]){ 3, 4 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,4), tc(2,4), tc(3,4) deltas");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

static void
test_snapshot_error_preserves_delta_reader(void)
{
    TEST("snapshot: error cleanup preserves reader-busy delta owners");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE,
                        .relation_name = "missing" };
    wl_plan_relation_t relation = { .name = "output", .ops = &op,
                                    .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    col_rel_t *held = NULL;
    col_rel_t *unregistered = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    bool reader_active = false;
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
    int64_t value = 42;
#define SNAP_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    SNAP_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &session)
        == 0, "create");
    sess = COL_SESSION(session);
    SNAP_CHECK(wl_session_insert(session, "input", &value, 1, 1) == 0,
        "insert input");
    const char *names[] = { "$d$free", "$d$held", "survivor" };
    for (uint32_t i = 0; i < 3; i++) {
        unregistered = col_rel_new_auto(names[i], 1);
        SNAP_CHECK(unregistered
            && col_rel_append_row(unregistered, &value) == 0, "delta fixture");
        SNAP_CHECK(session_add_rel(sess, unregistered) == 0, "registration");
        if (i == 1)
            held = unregistered;
        unregistered = NULL;
    }
    SNAP_CHECK(session_rel_build_hash(sess) == 0 && sess->rel_hash_head,
        "lookup hash fixture");
    SNAP_CHECK(col_rel_source_reader_acquire(held, &reader) == 0, "reader");
    reader_active = true;
    uint64_t view = held->view_generation;
    uint64_t storage = held->storage_generation;
    int64_t **columns = held->columns;
    int failed_rc = wl_session_snapshot(session, collect_tuple, &tuples);
    SNAP_CHECK(session_find_rel(sess, "$d$held") == held
        && !session_find_rel(sess, "$d$free")
        && session_find_rel(sess, "survivor") != NULL
        && session_find_rel(sess, "input") != NULL && tuples.count == 0,
        "ownership or compacted lookup lost");
    SNAP_CHECK(failed_rc == EBUSY, "cleanup refusal must take precedence");
    uint32_t nrels = sess->nrels;
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(sess->memory_governor));
    SNAP_CHECK(session_rel_build_hash(sess) == 0, "rebuild lookup hash");
    SNAP_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == EBUSY,
        "repeated refusal");
    SNAP_CHECK(session_find_rel(sess, "$d$held") == held
        && held->columns == columns && held->nrows == 1
        && held->columns[0][0] == value && held->view_generation == view
        && held->storage_generation == storage && sess->nrels == nrels
        && tuples.count == 0
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            sess->memory_governor)) == reserved,
        "refusal changed retained state");
    SNAP_CHECK(col_rel_source_reader_release(&reader) == 0, "reader release");
    reader_active = false;
    held = NULL; /* The session owns the relation until cleanup consumes it. */
    SNAP_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == ENOENT,
        "original evaluation error after cleanup");
    SNAP_CHECK(!session_find_rel(sess, "$d$held") && tuples.count == 0,
        "delta not removed after release");
    op.relation_name = "input";
    SNAP_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0,
        "valid plan retry");
    SNAP_CHECK(tuples.count == 1 && strcmp(tuples.relations[0], "output") == 0
        && tuples.ncols[0] == 1 && tuples.rows[0][0] == value,
        "retry differs from exact oracle");
cleanup:
    if (reader_active)
        (void)col_rel_source_reader_release(&reader);
    /* The red baseline drops the registry owner; clean that fixture too. */
    if (held && session_find_rel(sess, "$d$held") != held)
        col_rel_destroy(held);
    col_rel_destroy(unregistered);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef SNAP_CHECK
}

static void
test_snapshot_preseed_failure(int fault)
{
    TEST(fault == 0 ? "snapshot: preseed replacement preserves a busy owner"
        : fault == 1 ? "snapshot: preseed construction failure is retryable"
        : "snapshot: preseed append growth failure is retryable");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relation = { .name = "output", .ops = &op,
                                    .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "prefix", "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 2 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    col_rel_t *held = NULL;
    col_rel_t *unregistered = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    bool reader_active = false;
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
    int64_t values[COL_REL_INIT_CAP + 1];
    for (uint32_t i = 0; i < COL_REL_INIT_CAP + 1; i++)
        values[i] = (int64_t)i + 1;
#define SEED_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    SEED_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "create");
    sess = COL_SESSION(session);
    int64_t base = 0;
    SEED_CHECK(wl_session_insert(session, "prefix", &base, 1, 1) == 0
        && wl_session_insert(session, "input", &base, 1, 1) == 0
        && wl_session_snapshot(session, collect_tuple, &tuples) == 0,
        "baseline snapshot");
    SEED_CHECK(tuples.count == 1 && tuples.rows[0][0] == base,
        "baseline oracle");
    unregistered = col_rel_new_auto("$d$input", 1);
    SEED_CHECK(unregistered && col_rel_append_row(unregistered, &base) == 0,
        "old delta fixture");
    SEED_CHECK(session_add_rel(sess, unregistered) == 0, "old delta register");
    held = unregistered;
    unregistered = NULL;
    /* Two public input inserts request full evaluation. Construct the first
     * source's pending suffix internally to exercise partial preseed progress. */
    if (fault == 0)
        SEED_CHECK(col_rel_append_row(session_find_rel(sess, "prefix"),
            values) == 0,
            "prefix suffix fixture");
    uint32_t count = fault == 2 ? COL_REL_INIT_CAP + 1 : 1;
    delta_collector_t deltas = { 0 };
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    SEED_CHECK(wl_session_insert(session, "input", values, count, 1) == 0
        && !sess->pending_full_input_eval, "incremental insert");
    wl_session_set_delta_cb(session, NULL, NULL);
    if (fault == 0) {
        SEED_CHECK(col_rel_source_reader_acquire(held, &reader) == 0, "reader");
        reader_active = true;
    }
    uint64_t view = held->view_generation;
    uint64_t storage = held->storage_generation;
    int64_t **columns = held->columns;
    uint64_t reserved = 0;
    uint32_t nrels = 0;
    for (int attempt = 0; attempt < 2; attempt++) {
        tuples.count = 0;
#ifdef WL_TEST_ALLOC_WRAP
        fail_next_alloc = fault == 1;
        fail_malloc_size = fault == 2
            ? 2 * COL_REL_INIT_CAP * sizeof(int64_t) : 0;
#endif
        int rc = wl_session_snapshot(session, collect_tuple, &tuples);
#ifdef WL_TEST_ALLOC_WRAP
        bool fault_fired = !fail_next_alloc && fail_malloc_size == 0;
        fail_next_alloc = false;
        fail_malloc_size = 0;
        SEED_CHECK(fault_fired, "intended allocation fault did not fire");
#endif
        SEED_CHECK(rc == (fault == 0 ? EBUSY : ENOMEM) && tuples.count == 0,
            "preseed failure was not propagated");
        SEED_CHECK(session_find_rel(sess, "$d$input") == held
            && held->columns == columns && held->nrows == 1
            && held->columns[0][0] == base && held->view_generation == view
            && held->storage_generation == storage
            && session_find_rel(sess, "input")->base_nrows == 1,
            "preseed failure changed old owner or input boundary");
        if (fault == 0) {
            col_rel_t *prefix = session_find_rel(sess, "$d$prefix");
            SEED_CHECK(prefix && prefix->nrows == 1 &&
                prefix->columns[0][0] == 1,
                "successful prefix was lost");
        }
        uint64_t now = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(sess->memory_governor));
        if (attempt == 0) {
            reserved = now;
            nrels = sess->nrels;
        } else {
            SEED_CHECK(now == reserved && sess->nrels == nrels,
                "repeated failure leaked accounting or registrations");
        }
    }
    if (reader_active) {
        SEED_CHECK(col_rel_source_reader_release(&reader) == 0, "release");
        reader_active = false;
    }
    held = NULL;
    SEED_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0,
        "retry snapshot");
    SEED_CHECK(tuples.count == (int)count + 1, "retry tuple count");
    for (uint32_t i = 0; i <= count; i++) {
        int64_t expected = i;
        SEED_CHECK(has_tuple(&tuples, "output", &expected, 1),
            "retry exact oracle");
    }
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
    fail_malloc_size = 0;
#endif
    if (reader_active)
        (void)col_rel_source_reader_release(&reader);
    if (held && session_find_rel(sess, "$d$input") != held && fault == 0)
        col_rel_destroy(held);
    col_rel_destroy(unregistered);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef SEED_CHECK
}

static void
test_recursive_delta_publication_failure(bool allocation_failure)
{
    TEST(allocation_failure
        ? "recursive: delta promotion allocation failure is propagated"
        : "recursive: delta publication preserves reader-busy owner");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relations[] = {
        { .name = "prefix", .delta_name = "$d$prefix", .ops = &op,
          .op_count = 1 },
        { .name = "output", .delta_name = "$d$output", .ops = &op,
          .op_count = 1 }
    };
    wl_plan_stratum_t stratum = { .relations = allocation_failure
        ? &relations[1] : relations,
                                  .relation_count = allocation_failure ? 1 : 2,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    col_rel_t *held = NULL;
    col_rel_t *unregistered = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    bool reader_active = false;
    const char *failure = NULL;
#define PUB_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    PUB_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &session) == 0,
        "create");
    sess = COL_SESSION(session);
    if (!allocation_failure) {
        unregistered = col_rel_new_auto("prefix", 1);
        PUB_CHECK(unregistered && session_add_rel(sess, unregistered) == 0,
            "precreate prefix output");
        unregistered = NULL;
    }
    unregistered = col_rel_new_auto("output", 1);
    PUB_CHECK(unregistered && session_add_rel(sess, unregistered) == 0,
        "precreate output");
    unregistered = NULL;
    int64_t marker = 99;
    unregistered = col_rel_new_auto("$d$output", 1);
    PUB_CHECK(unregistered && col_rel_append_row(unregistered, &marker) == 0
        && col_rel_enable_timestamps(unregistered) == 0, "old delta fixture");
    unregistered->timestamps[0].multiplicity = 1;
    PUB_CHECK(session_add_rel(sess, unregistered) == 0, "register old delta");
    held = unregistered;
    unregistered = NULL;
    PUB_CHECK(col_rel_source_reader_acquire(held, &reader) == 0, "reader");
    reader_active = true;
    uint64_t view = held->view_generation;
    uint64_t storage = held->storage_generation;
    int64_t **columns = held->columns;
    col_delta_timestamp_t *timestamps = held->timestamps;
    bool saved_diff = sess->diff_operators_active;
    for (int attempt = 1; attempt <= 2; attempt++) {
        int64_t input = attempt;
        PUB_CHECK(wl_session_insert(session, "input", &input, 1, 1) == 0,
            "insert next input");
#ifdef WL_TEST_ALLOC_WRAP
        /* Output and old delta descriptors already exist. The generated delta
         * uses a pool slot; this allocation is its iteration-1 promotion. */
        if (allocation_failure) {
            if (!reader_active) {
                PUB_CHECK(col_rel_source_reader_acquire(held, &reader) == 0,
                    "reacquire before next promotion");
                reader_active = true;
            }
            release_on_calloc_failure = &reader;
            calloc_reader_release_rc = EINVAL;
        }
        fail_calloc_size = allocation_failure ? sizeof(col_rel_t) : 0;
#endif
        int rc = col_eval_stratum(&stratum, sess, 0);
#ifdef WL_TEST_ALLOC_WRAP
        bool fault_fired = fail_calloc_size == 0;
        fail_calloc_size = 0;
        if (allocation_failure && release_on_calloc_failure == NULL
            && calloc_reader_release_rc == 0)
            reader_active = false;
        PUB_CHECK(fault_fired, "promotion allocation fault did not fire");
        PUB_CHECK(!allocation_failure || (!reader_active
            && calloc_reader_release_rc == 0),
            "promotion reader release failed");
#endif
        PUB_CHECK(rc == (allocation_failure ? ENOMEM : EBUSY)
            && sess->current_iteration == 1,
            "publication failure not propagated");
        PUB_CHECK(session_find_rel(sess, "$d$output") == held
            && held->columns == columns && held->timestamps == timestamps
            && held->nrows == 1 && held->columns[0][0] == marker
            && held->timestamps[0].multiplicity == 1
            && held->view_generation == view &&
            held->storage_generation == storage
            && sess->diff_operators_active == saved_diff,
            "retained owner changed");
        PUB_CHECK(!allocation_failure
            || !wl_columnar_source_access_gate_busy(&held->source_access),
            "old owner is still pinned after allocation failure");
        col_rel_t *output = session_find_rel(sess, "output");
        PUB_CHECK(output && output->nrows == (uint32_t)attempt,
            "partial output count");
        for (int row = 0; row < attempt; row++)
            PUB_CHECK(output->columns[0][row] == row + 1,
                "partial output oracle");
        if (!allocation_failure) {
            col_rel_t *prefix = session_find_rel(sess, "$d$prefix");
            PUB_CHECK(prefix && prefix->nrows == 1
                && prefix->columns[0][0] == attempt,
                "successfully published prefix lost");
        }
        /* The failed delta follows any prefix slot. Resetting the pool does
        * not free its heap payload; destruction must clear that payload. */
        col_rel_t *retired = (col_rel_t *)(sess->delta_pool->slab
            + (allocation_failure ? 0 : sess->delta_pool->slot_size));
        PUB_CHECK(retired->name == NULL && retired->columns == NULL
            && retired->timestamps == NULL, "unpublished delta payload leaked");
    }
    if (reader_active) {
        PUB_CHECK(col_rel_source_reader_release(&reader) == 0, "release");
        reader_active = false;
    }
    held = NULL;
    int64_t input = 3;
    PUB_CHECK(wl_session_insert(session, "input", &input, 1, 1) == 0
        && col_eval_stratum(&stratum, sess, 0) == 0, "retry evaluation");
    col_rel_t *output = session_find_rel(sess, "output");
    PUB_CHECK(output && output->nrows == 3
        && output->columns[0][0] == 1 && output->columns[0][1] == 2
        && output->columns[0][2] == 3 && !session_find_rel(sess, "$d$output")
        && sess->diff_operators_active == saved_diff, "retry exact oracle");
    if (!allocation_failure) {
        col_rel_t *prefix = session_find_rel(sess, "prefix");
        PUB_CHECK(prefix && prefix->nrows == 3 && prefix->columns[0][0] == 1
            && prefix->columns[0][1] == 2 && prefix->columns[0][2] == 3
            && !session_find_rel(sess, "$d$prefix"),
            "prefix retry exact oracle");
    }
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    fail_calloc_size = 0;
    release_on_calloc_failure = NULL;
#endif
    if (reader_active)
        (void)col_rel_source_reader_release(&reader);
    col_rel_destroy(unregistered);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef PUB_CHECK
}

static void
test_pending_cleanup_operation_guards(unsigned storage, bool in_worker)
{
    TEST(in_worker ? "session: retained worker cleanup gates operations"
        : storage == 0 ? "session: retained heap cleanup gates operations"
        : storage == 1 ? "session: retained pool cleanup gates operations"
        : "session: retained arena cleanup gates operations");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relation = { .name = "output", .ops = &op,
                                    .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL, *owner = NULL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    bool reader_active = false;
    col_rel_t *unowned = NULL;
    col_rel_t *held = NULL;
    tuple_collector_t tuples = { 0 };
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t value = 42, extra = 43;
#define GUARD_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    GUARD_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0
        && wl_session_insert(session, "input", &value, 1, 1) == 0, "fixture");
    sess = COL_SESSION(session);
    owner = sess;
    if (in_worker) {
        GUARD_CHECK(wl_columnar_session_ensure_tdd_worker_slots(sess, 1) == 0,
            "worker slots");
        owner = &sess->tdd_workers[0];
        sess->tdd_workers_count = 1;
        GUARD_CHECK(col_worker_session_create(sess, 0, NULL, 0, owner) == 0,
            "worker create");
    }
    unowned = storage == 0 ? col_rel_new_auto("held", 1)
        : col_rel_pool_new_auto(owner->delta_pool,
            storage == 2 ? owner->eval_arena : NULL, "held", 1);
    GUARD_CHECK(unowned && col_rel_append_row(unowned, &value) == 0,
        "held relation");
    held = unowned;
    GUARD_CHECK(col_rel_source_reader_acquire(held, &reader) == 0, "reader");
    reader_active = true;
    GUARD_CHECK(wl_columnar_eval_stack_cleanup_begin(owner, &frame) == 0,
        "begin frame");
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    result->rel = unowned;
    result->owned = true;
    unowned = NULL;
    result->seg_boundaries = malloc(2 * sizeof(uint32_t));
    GUARD_CHECK(result->seg_boundaries, "result segments");
    result->seg_boundaries[0] = 0;
    result->seg_boundaries[1] = 1;
    result->seg_count = 1;
    unowned = col_rel_new_auto("lower", 1);
    GUARD_CHECK(unowned && eval_stack_push(
            wl_columnar_eval_stack_cleanup_stack(frame), unowned, true) == 0,
        "lower stack owner");
    unowned = NULL;
    GUARD_CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY,
        "retain frame");
    uint64_t bytes = owner->cleanup_reserved_bytes;
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(sess->memory_governor));
    uint32_t epoch = sess->outer_epoch;
    uint32_t nrels = sess->nrels;
    uint32_t slots = owner->delta_pool->slot_used;
    size_t arena_used = owner->eval_arena->used;
    int64_t **columns = held->columns;
    uint64_t view = held->view_generation,
        generation = held->storage_generation;
    for (int attempt = 0; attempt < 2; attempt++) {
        GUARD_CHECK(wl_session_insert(session, "input", &extra, 0, 1) == 0,
            "zero insert is no-op");
        GUARD_CHECK(wl_session_insert(session, "input", &extra, 1, 1) == EBUSY,
            "insert bypassed cleanup");
        GUARD_CHECK(wl_session_remove(session, "input", &value, 1, 1) == EBUSY,
            "remove bypassed cleanup");
        wl_session_set_delta_cb(session, collect_delta, &deltas);
        GUARD_CHECK(wl_session_insert(session, "input", &extra, 0, 1) == 0,
            "zero incremental insert is no-op");
        GUARD_CHECK(wl_session_insert(session, "input", &extra, 1, 1) == EBUSY
            && wl_session_remove(session, "input", &value, 1, 1) == EBUSY,
            "incremental update bypassed cleanup");
        GUARD_CHECK(wl_session_step(session) == EBUSY, "step bypassed cleanup");
        wl_session_set_delta_cb(session, NULL, NULL);
        GUARD_CHECK(wl_session_snapshot(session, collect_tuple,
            &tuples) == EBUSY,
            "snapshot bypassed cleanup");
        if (!in_worker) {
            GUARD_CHECK(col_eval_stratum(&stratum, sess, 0) == EBUSY,
                "serial direct entry bypassed cleanup");
            GUARD_CHECK(col_stratum_step_with_delta(&stratum, sess, 0) == EBUSY,
                "delta direct entry bypassed cleanup");
        } else {
            GUARD_CHECK(col_worker_session_destroy(owner) == EBUSY
                && !owner->teardown_started, "worker teardown mutated state");
        }
        GUARD_CHECK(held->columns == columns && held->columns[0][0] == value
            && held->view_generation == view &&
            held->storage_generation == generation
            && owner->cleanup_pending_count == 1 &&
            owner->cleanup_reserved_bytes == bytes
            && owner->delta_pool->slot_used == slots
            && owner->eval_arena->used == arena_used
            && sess->outer_epoch == epoch && sess->nrels == nrels
            && session_find_rel(sess, "input")->nrows == 1
            && tuples.count == 0 && deltas.count == 0
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                sess->memory_governor)) == reserved,
            "refusal changed owned state");
    }
    GUARD_CHECK(col_rel_source_reader_release(&reader) == 0, "release");
    reader_active = false;
    held = NULL;
    GUARD_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
        && tuples.count == 1 && tuples.rows[0][0] == value
        && strcmp(tuples.relations[0], "output") == 0,
        "released cleanup retry exact oracle");
    GUARD_CHECK(!owner->cleanup_pending && owner->cleanup_reserved_bytes == 0,
        "cleanup accounting not released");
cleanup:
    if (reader_active)
        (void)col_rel_source_reader_release(&reader);
    if (frame)
        (void)wl_columnar_eval_stack_cleanup_finish(&frame);
    if (owner)
        (void)wl_columnar_eval_stack_cleanup_retry(owner);
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef GUARD_CHECK
}

static void
test_pending_cleanup_synchronous_destroy(void)
{
    TEST("session: synchronous destroy retries released cleanup");
    wl_plan_t plan = { 0 };
    wl_session_t *session = NULL;
    if (wl_session_create(wl_backend_columnar(), &plan, 1, &session) != 0) {
        FAIL("create");
        return;
    }
    wl_col_session_t *sess = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    col_rel_t *held = col_rel_new_auto("destroy-held", 1);
    wl_columnar_source_access_reader_t reader = { 0 };
    if (!held || col_rel_source_reader_acquire(held, &reader) != 0
        || wl_columnar_eval_stack_cleanup_begin(sess, &frame) != 0) {
        if (reader.owner)
            (void)col_rel_source_reader_release(&reader);
        col_rel_destroy(held);
        wl_session_destroy(session);
        wl_columnar_memory_governor_ref_release(governor);
        FAIL("frame fixture");
        return;
    }
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    result->rel = held;
    result->owned = true;
    int finish_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    int release_rc = col_rel_source_reader_release(&reader);
    wl_session_destroy(session);
    uint64_t remaining = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(governor));
    wl_columnar_memory_governor_ref_release(governor);
    if (finish_rc != EBUSY || release_rc != 0 || remaining != 0) {
        FAIL("synchronous destroy abandoned cleanup ownership");
        return;
    }
    PASS();
}

#ifdef WL_TEST_ALLOC_WRAP
static void
test_delta_snapshot_capture_failure(unsigned skip)
{
    TEST(skip ==
        0 ? "delta step: first snapshot allocation preserves prior state"
        : "delta step: later snapshot allocation preserves all prior state");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relations[] = {
        { .name = "a", .ops = &op, .op_count = 1 },
        { .name = "empty" },
        { .name = "b", .ops = &op, .op_count = 1 }
    };
    wl_plan_stratum_t stratum = { .relations = relations, .relation_count = 3 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    col_rel_t *unowned = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define CAPTURE_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    CAPTURE_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "create");
    sess = COL_SESSION(session);
    unowned = col_rel_new_auto("empty", 1);
    CAPTURE_CHECK(unowned && session_add_rel(sess, unowned) == 0,
        "empty sibling");
    unowned = NULL;
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    CAPTURE_CHECK(wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0 && deltas.count == 2
        && has_delta(&deltas, "a", &one, 1, +1)
        && has_delta(&deltas, "b", &one, 1, +1), "baseline oracle");
    deltas.count = 0;
    CAPTURE_CHECK(wl_session_insert(session, "input", &two, 1, 1) == 0,
        "next input");
    struct {
        col_rel_t *rel;
        int64_t **columns;
        uint32_t nrows, ncols, capacity, sorted_nrows;
        uint64_t identity, view, storage;
    } prior[3];
    for (uint32_t i = 0; i < 3; i++) {
        col_rel_t *r = session_find_rel(sess, relations[i].name);
        CAPTURE_CHECK(r, "prior relation missing");
        prior[i].rel = r;
        prior[i].columns = r->columns;
        prior[i].nrows = r->nrows;
        prior[i].ncols = r->ncols;
        prior[i].capacity = r->capacity;
        prior[i].sorted_nrows = r->sorted_nrows;
        prior[i].identity = r->relation_identity;
        prior[i].view = r->view_generation;
        prior[i].storage = r->storage_generation;
    }
    uint32_t epoch = sess->outer_epoch;
    const char *inserted = sess->last_inserted_relation;
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(sess->memory_governor));
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        /* Both populated IDBs have one int64 cell. The empty sibling has no
         * flat allocation. Skipping one match proves capture of a succeeded
         * before allocation of b was rejected. */
        fail_malloc_size = sizeof(int64_t);
        fail_malloc_matches_to_skip = skip;
        fail_malloc_match_count = 0;
        int rc = wl_session_step(session);
        bool fault_fired = fail_malloc_size == 0
            && fail_malloc_matches_to_skip == 0
            && fail_malloc_match_count == skip + 1;
        fail_malloc_size = 0;
        fail_malloc_matches_to_skip = 0;
        CAPTURE_CHECK(fault_fired, "intended snapshot allocation did not fail");
        CAPTURE_CHECK(rc == ENOMEM, "snapshot allocation failure ignored");
        for (uint32_t i = 0; i < 3; i++) {
            col_rel_t *r = session_find_rel(sess, relations[i].name);
            CAPTURE_CHECK(r == prior[i].rel && r->columns == prior[i].columns
                && r->nrows == prior[i].nrows && r->ncols == prior[i].ncols
                && r->capacity == prior[i].capacity
                && r->sorted_nrows == prior[i].sorted_nrows
                && r->relation_identity == prior[i].identity
                && r->view_generation == prior[i].view
                && r->storage_generation == prior[i].storage
                && (i == 1 || r->columns[0][0] == one),
                "failed capture changed prior relation");
        }
        CAPTURE_CHECK(deltas.count == 0 && !sess->delta_event_transaction
            && sess->delta_event_count == 0 && sess->pending_input_change
            && sess->last_inserted_relation == inserted &&
            sess->outer_epoch == epoch
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                sess->memory_governor)) == reserved,
            "failed capture published or consumed pending state");
    }
    CAPTURE_CHECK(wl_session_step(session) == 0 && deltas.count == 2
        && has_delta(&deltas, "a", &two, 1, +1)
        && has_delta(&deltas, "b", &two, 1, +1), "retry delta oracle");
    for (uint32_t i = 0; i < 3; i++) {
        col_rel_t *r = session_find_rel(sess, relations[i].name);
        CAPTURE_CHECK(r && (i == 1 ? r->nrows == 0
            : r->nrows == 2 && r->columns[0][0] == one
            && r->columns[0][1] == two), "retry relation oracle");
    }
    deltas.count = 0;
    CAPTURE_CHECK(wl_session_step(session) == 0 && deltas.count == 0,
        "unchanged step repeated callbacks");
cleanup:
    fail_malloc_size = 0;
    fail_malloc_matches_to_skip = 0;
    fail_malloc_match_count = 0;
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef CAPTURE_CHECK
}
#endif

int
main(void)
{
    printf("test_session: persistent columnar session delta tests\n");

    test_session_hash_overflow_rejected();
    test_retained_relation_admission();
    test_governed_compaction_transaction();
    test_intern_reservation_program_lifetime();
    test_intern_rebinds_to_next_session();
    test_intern_rebind_waits_for_last_holder();
    test_session_create_destroy();
    test_session_create_destroy_columnar();
    test_session_create_with_options();
    test_session_injected_governor_admission();
#ifdef _WIN32
    test_session_windows_job_options();
#endif
    test_session_create_multi_worker_rejected();
    test_session_step_initial_delta();
    test_session_step_incremental_delta();
    test_session_remove_admission();
    test_session_remove_rejects_live_alias(false);
    test_session_remove_rejects_live_alias(true);
    test_session_step_no_change();

    /* GREEN: diff=-1 retraction deltas now implemented */
    test_session_remove_single_delta();
    test_session_remove_nonexistent();
    test_session_remove_live_alias();
    test_session_remove_wide_alias_before_allocation();
    test_session_remove_reader_exclusion();
    test_session_remove_incremental_reader_exclusion();
#ifdef WL_SESSION_TEST_HOOKS
    test_session_destroy_orders_worker_retirement();
#endif
    test_session_full_idb_clear_reader_exclusion();
#ifdef WL_TEST_ALLOC_WRAP
    test_delta_snapshot_capture_failure(0);
    test_delta_snapshot_capture_failure(1);
#endif
    test_pending_cleanup_synchronous_destroy();
    test_pending_cleanup_operation_guards(0, false);
    test_pending_cleanup_operation_guards(1, false);
    test_pending_cleanup_operation_guards(2, false);
    test_pending_cleanup_operation_guards(2, true);
    test_snapshot_error_preserves_delta_reader();
    test_recursive_delta_publication_failure(false);
#ifdef WL_TEST_ALLOC_WRAP
    test_recursive_delta_publication_failure(true);
#endif
    test_snapshot_preseed_failure(0);
#ifdef WL_TEST_ALLOC_WRAP
    test_snapshot_preseed_failure(1);
    test_snapshot_preseed_failure(2);
#endif
    /* GREEN: col_session_remove returns 0 for uninitialized schema */
    /* test_session_snapshot_empty(); */
    /* test_session_snapshot_after_insert(); */
    /* test_session_duplicate_insert(); */

    /* RED: the following tests require TASK 4 (Variable collections + iterate) */
    /* Disabled until wl_session_iterate is implemented in columnar backend */
    /* test_session_tc_insert(); */

    printf("\n  %d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
