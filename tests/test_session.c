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

#ifndef _WIN32
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#endif

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
static size_t fail_realloc_size;
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
    if (fail_calloc_size != 0 && size != 0
        && count <= SIZE_MAX / size
        && count * size == fail_calloc_size) {
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
    if (fail_realloc_size != 0 && size == fail_realloc_size) {
        fail_realloc_size = 0;
        return NULL;
    }
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

static void
count_tuple_only(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (*(uint64_t *)user_data)++;
}

static void
count_delta_tuple_only(const char *relation, const int64_t *row, uint32_t ncols,
    int diff, void *user_data)
{
    (void)diff;
    count_tuple_only(relation, row, ncols, user_data);
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

static bool
compound_cleanup_refusal_unchanged(wl_session_t *session)
{
    wl_col_session_t *sess = COL_SESSION(session);
    wl_compound_arena_t *arena = sess->compound_arena;
    uint32_t epoch = sess->outer_epoch, nrels = sess->nrels;
    uint32_t arena_epoch = arena->current_epoch;
    uint64_t live = arena->live_handles;
    wl_compound_gen_t generation = arena->gens[arena_epoch];
    bool pending = sess->pending_input_change;
    const char *last = sess->last_inserted_relation;
    wirelog_compound_arg_t arg = { WIRELOG_TYPE_INT64, 42 };
    uint64_t handle = 123;
    int rc = wl_session_make_compound(session, "cleanup_guard", 1, &arg,
            &handle);
    return rc == EBUSY && handle == WIRELOG_COMPOUND_HANDLE_NULL
           && sess->outer_epoch == epoch && sess->nrels == nrels
           && sess->pending_input_change == pending &&
           sess->last_inserted_relation == last
           && arena->current_epoch == arena_epoch && arena->live_handles == live
           && arena->gens[arena_epoch].base == generation.base
           && arena->gens[arena_epoch].capacity == generation.capacity
           && arena->gens[arena_epoch].used == generation.used
           && arena->gens[arena_epoch].entry_count == generation.entry_count;
}

static bool
compound_cleanup_retry_once(wl_session_t *session)
{
    wl_col_session_t *sess = COL_SESSION(session);
    uint64_t live = sess->compound_arena->live_handles;
    uint32_t epoch = sess->outer_epoch;
    wirelog_compound_arg_t arg = { WIRELOG_TYPE_INT64, 42 };
    uint64_t handle = WIRELOG_COMPOUND_HANDLE_NULL;
    return wl_session_make_compound(session, "cleanup_guard", 1, &arg,
               &handle) == 0
           && handle != WIRELOG_COMPOUND_HANDLE_NULL
           && sess->compound_arena->live_handles == live + 1
           && sess->outer_epoch == epoch + 1;
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
        GUARD_CHECK(compound_cleanup_refusal_unchanged(session),
            "compound mutation bypassed cleanup");
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
    if (in_worker) {
        GUARD_CHECK(col_worker_session_destroy(owner) == 0,
            "release manually retained worker arena borrow");
        sess->tdd_workers_count = 0;
    }
    GUARD_CHECK(compound_cleanup_retry_once(session), "compound release retry");
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

extern void (*wl_columnar_eval_serial_test_after_plan)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *);
static wl_columnar_source_access_reader_t serial_cleanup_reader;
static col_rel_t *serial_cleanup_held;
static unsigned serial_cleanup_storage;
static bool serial_cleanup_lower;
static int serial_cleanup_hook_rc;
static uint32_t serial_cleanup_iteration;
static uint32_t serial_cleanup_expected_rows = 2;
static col_rel_t *serial_cleanup_delta;
static wl_columnar_source_access_reader_t serial_cleanup_delta_reader;

static void
inject_serial_cleanup_reader(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    if (sess->current_iteration != serial_cleanup_iteration
        || !result->owned || !result->rel || result->seg_count != 2)
        return;
    wl_columnar_eval_serial_test_after_plan = NULL;
    serial_cleanup_hook_rc = EINVAL;
    if (!result->seg_boundaries ||
        result->seg_boundaries[2] != serial_cleanup_expected_rows)
        return;
    if (serial_cleanup_iteration != 0) {
        serial_cleanup_delta = session_find_rel(sess, "$d$output");
        if (!serial_cleanup_delta
            || eval_stack_push(stack, serial_cleanup_delta, false) != 0
            || col_rel_source_reader_acquire(serial_cleanup_delta,
            &serial_cleanup_delta_reader) != 0)
            return;
    }
    serial_cleanup_held = result->rel;
    if (serial_cleanup_lower) {
        col_rel_t *lower = serial_cleanup_storage == 0
            ? col_rel_new_auto("lower", 1)
            : col_rel_pool_new_auto(sess->delta_pool,
                serial_cleanup_storage == 2 ? sess->eval_arena : NULL,
                "lower", 1);
        int64_t value = 42;
        if (!lower || col_rel_append_row(lower, &value) != 0
            || eval_stack_push(stack, lower, true) != 0) {
            col_rel_destroy(lower);
            return;
        }
        serial_cleanup_held = lower;
    }
    serial_cleanup_hook_rc = col_rel_source_reader_acquire(
        serial_cleanup_held, &serial_cleanup_reader);
}

static int
serial_cleanup_run(wl_session_t *session, tuple_collector_t *tuples,
    delta_collector_t *deltas, bool delta_step)
{
    if (!delta_step)
        return wl_session_snapshot(session, collect_tuple, tuples);
    int rc = wl_session_step(session);
    if (rc != 0)
        return rc;
    for (int i = 0; i < deltas->count; i++) {
        if (deltas->diffs[i] != 1)
            return EINVAL;
        collect_tuple(deltas->relations[i], deltas->rows[i], deltas->ncols[i],
            tuples);
    }
    return 0;
}

static void
test_serial_cleanup_caller(unsigned storage, bool lower, bool existing,
    unsigned recursive_mode, uint32_t workers)
{
    TEST(lower ? "serial caller: lower owner refusal retains production frame"
        : "serial caller: CONCAT result refusal retains production frame");
    bool delta_step = recursive_mode >= 5;
    if (delta_step)
        recursive_mode -= 4;
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT }
    };
    if (recursive_mode == 2) {
        ops[0].delta_mode = WL_DELTA_FORCE_FULL;
        ops[1].delta_mode = WL_DELTA_FORCE_FULL;
    }
    if (recursive_mode == 4) {
        ops[0].delta_mode = WL_DELTA_FORCE_EMPTY;
        ops[1].delta_mode = WL_DELTA_FORCE_EMPTY;
    }
    wl_plan_op_t prefix_op = { .op = WL_PLAN_OP_VARIABLE,
                               .relation_name = "input" };
    wl_plan_relation_t relations[] = {
        { .name = "prefix", .delta_name = "$d$prefix", .ops = &prefix_op,
          .op_count = 1 },
        { .name = "output", .delta_name = "$d$output", .ops = ops,
          .op_count = 3 }
    };
    wl_plan_stratum_t stratum = {
        .relations = recursive_mode == 3 ? relations : &relations[1],
        .relation_count = recursive_mode == 3 ? 2 : 1,
        .is_recursive = recursive_mode != 0
    };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    delta_pool_t *saved_pool = NULL;
    const char *failure = NULL;
    tuple_collector_t tuples = { 0 };
    delta_collector_t deltas = { 0 };
    int64_t value = 42;
    uint64_t target_generation = 0;
#define SERIAL_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    memset(&serial_cleanup_reader, 0, sizeof(serial_cleanup_reader));
    serial_cleanup_held = NULL;
    serial_cleanup_delta = NULL;
    memset(&serial_cleanup_delta_reader, 0,
        sizeof(serial_cleanup_delta_reader));
    serial_cleanup_iteration = recursive_mode == 2 ? 1 : 0;
    serial_cleanup_expected_rows = recursive_mode == 4 ? 0 : 2;
    SERIAL_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0
        && wl_session_insert(session, "input", &value, 1, 1) == 0, "fixture");
    sess = COL_SESSION(session);
    SERIAL_CHECK(sess->num_workers == workers, "requested worker count");
    if (workers > 1 && !recursive_mode) {
        uint32_t registered = sess->nrels;
        SERIAL_CHECK(wl_columnar_eval_nonrec_relation_parallel(&relations[1],
            sess) == EAGAIN && sess->nrels == registered
            && sess->tdd_workers_count == 0, "real parallel fallback boundary");
    }
    if (delta_step)
        wl_session_set_delta_cb(session, collect_delta, &deltas);
    if (existing) {
        col_rel_t *target = col_rel_new_auto("output", 1);
        SERIAL_CHECK(target, "existing target allocation");
        int add_rc = session_add_rel(sess, target);
        if (add_rc != 0)
            col_rel_destroy(target);
        SERIAL_CHECK(add_rc == 0, "existing target registration");
        target_generation = target->view_generation;
    }
    if (!lower && storage == 0) {
        saved_pool = sess->delta_pool;
        sess->delta_pool = NULL;
    }
    serial_cleanup_storage = storage;
    serial_cleanup_lower = lower;
    wl_columnar_eval_serial_test_after_plan = inject_serial_cleanup_reader;
    int rc = serial_cleanup_run(session, &tuples, &deltas, delta_step);
    if (saved_pool) {
        sess->delta_pool = saved_pool;
        saved_pool = NULL;
    }
    SERIAL_CHECK(rc == EBUSY && serial_cleanup_hook_rc == 0
        && sess->cleanup_pending_count == 1 && !sess->cleanup_active,
        "caller did not retain refusal");
    SERIAL_CHECK(workers == 1 || sess->tdd_workers_count == 0,
        "fallback unexpectedly dispatched workers");
    SERIAL_CHECK(serial_cleanup_held->pool_owned == (storage != 0)
        && serial_cleanup_held->arena_owned == (storage == 2), "storage");
    SERIAL_CHECK(!existing || (session_find_rel(sess, "output")->nrows == 0
        && session_find_rel(sess,
        "output")->view_generation == target_generation),
        "cleanup refusal published rows before retry");
    if (recursive_mode == 2)
        SERIAL_CHECK(serial_cleanup_delta
            && session_find_rel(sess, "$d$output") == serial_cleanup_delta
            && serial_cleanup_delta->nrows == 1,
            "next-subpass delta owner lost on unwind");
    char *name = serial_cleanup_held->name;
    int64_t **columns = serial_cleanup_held->columns;
    uint32_t slots = sess->delta_pool->slot_used;
    size_t used = sess->eval_arena->used;
    uint32_t nrels = sess->nrels;
    uint64_t bytes = sess->cleanup_reserved_bytes;
    SERIAL_CHECK(strcmp(name, lower ? "lower" : "$concat") == 0,
        "rename modified reader-held relation");
    for (unsigned retry = 0; retry < 2; retry++) {
        SERIAL_CHECK(serial_cleanup_run(session, &tuples, &deltas, delta_step)
            == EBUSY, "pending caller did not refuse");
        SERIAL_CHECK(serial_cleanup_held->name == name
            && serial_cleanup_held->columns == columns
            && (recursive_mode == 4 ? serial_cleanup_held->nrows == 0
                : col_rel_get(serial_cleanup_held, 0, 0) == value)
            && sess->delta_pool->slot_used == slots
            && sess->eval_arena->used == used && sess->nrels == nrels
            && sess->cleanup_reserved_bytes == bytes && bytes != 0
            && tuples.count == 0 && deltas.count == 0
            && (recursive_mode != 2 || (session_find_rel(sess, "$d$output")
            == serial_cleanup_delta && col_rel_get(serial_cleanup_delta, 0,
            0) == value))
            && (!existing || (session_find_rel(sess, "output")->nrows == 0
            && session_find_rel(sess,
            "output")->view_generation == target_generation)),
            "pending state changed");
    }
    SERIAL_CHECK(col_rel_source_reader_release(&serial_cleanup_reader) == 0,
        "reader release");
    serial_cleanup_held = NULL;
    if (serial_cleanup_delta_reader.owner)
        SERIAL_CHECK(col_rel_source_reader_release(
                &serial_cleanup_delta_reader) == 0,
            "delta reader release");
    serial_cleanup_delta = NULL;
    SERIAL_CHECK(serial_cleanup_run(session, &tuples, &deltas, delta_step) == 0
        && tuples.count ==
        (recursive_mode == 4 ? 0 : recursive_mode ==
        3 ? 2 : recursive_mode ? 1 : 2)
        && (tuples.count == 0 || tuples.rows[0][0] == value)
        && (tuples.count <= 1 || tuples.rows[1][0] == value)
        && (recursive_mode == 4 || (recursive_mode == 3
            ? ((strcmp(tuples.relations[0], "prefix") == 0
        && strcmp(tuples.relations[1], "output") == 0)
        || (strcmp(tuples.relations[1], "prefix") == 0
        && strcmp(tuples.relations[0], "output") == 0))
            : strcmp(tuples.relations[0], "output") == 0))
        && !sess->cleanup_pending && sess->cleanup_reserved_bytes == 0,
        "same-session exact retry");
    if (delta_step) {
        deltas.count = 0;
        SERIAL_CHECK(wl_session_step(session) == 0 && deltas.count == 0,
            "completed delta step duplicated notifications");
    }
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (serial_cleanup_reader.owner)
        (void)col_rel_source_reader_release(&serial_cleanup_reader);
    if (serial_cleanup_delta_reader.owner)
        (void)col_rel_source_reader_release(&serial_cleanup_delta_reader);
    serial_cleanup_iteration = 0;
    serial_cleanup_expected_rows = 2;
    if (saved_pool)
        sess->delta_pool = saved_pool;
    if (sess)
        (void)wl_columnar_eval_stack_cleanup_retry(sess);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef SERIAL_CHECK
}

#ifdef WL_TEST_ALLOC_WRAP
static bool serial_fail_promotion;
static bool serial_deny_copy;
static void
inject_serial_publication_failure(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    (void)result;
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (serial_deny_copy) {
        wl_columnar_memory_governor_t *budget =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
        atomic_store_explicit(&budget->usable_bytes,
            wl_columnar_memory_reserved(budget), memory_order_relaxed);
    } else if (serial_fail_promotion)
        fail_calloc_size = sizeof(col_rel_t);
    else
        fail_next_alloc = true;
}
#endif

static void
test_serial_cleanup_admission_and_failures(bool recursive, uint32_t workers)
{
    TEST("serial caller: admission and publication failures release ownership");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_relation_t relation = { .name = "output", .delta_name = "$d$output",
                                    .ops = ops,
                                    .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1,
                                  .is_recursive = recursive };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    const char *failure = NULL;
    tuple_collector_t tuples = { 0 };
    int64_t value = 42;
#define SERIAL_FAIL_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    SERIAL_FAIL_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0
        && wl_session_insert(session, "input", &value, 1, 1) == 0, "fixture");
    sess = COL_SESSION(session);
    SERIAL_FAIL_CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0,
        "parent frame");
    uint32_t slots = sess->delta_pool->slot_used;
    SERIAL_FAIL_CHECK(col_eval_stratum(&stratum, sess, 0) == EBUSY
        && sess->cleanup_active_count == 1
        && sess->delta_pool->slot_used == slots, "active parent not excluded");
    SERIAL_FAIL_CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0,
        "finish parent");
    wl_columnar_memory_governor_t *budget = wl_columnar_memory_governor_ref_get(
        sess->memory_governor);
    uint64_t saved_limit = atomic_load_explicit(&budget->usable_bytes,
            memory_order_relaxed);
    wl_columnar_memory_mode_t saved_mode = budget->mode;
    budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    atomic_store_explicit(&budget->usable_bytes,
        wl_columnar_memory_reserved(budget), memory_order_relaxed);
    int rc = col_eval_stratum(&stratum, sess, 0);
    budget->mode = saved_mode;
    atomic_store_explicit(&budget->usable_bytes, saved_limit,
        memory_order_relaxed);
    SERIAL_FAIL_CHECK(rc == ENOSPC && !sess->cleanup_active
        && !sess->cleanup_pending && sess->cleanup_reserved_bytes == 0
        && sess->delta_pool->slot_used == slots
        && (!recursive ? !session_find_rel(sess, "output")
            : session_find_rel(sess, "output")->nrows == 0),
        "admission ran operators");
#ifdef WL_TEST_ALLOC_WRAP
    for (unsigned mode = 0; mode < 2; mode++) {
        serial_fail_promotion = mode != 0;
        wl_columnar_eval_serial_test_after_plan =
            inject_serial_publication_failure;
        rc = col_eval_stratum(&stratum, sess, 0);
        SERIAL_FAIL_CHECK(rc == ENOMEM && !fail_next_alloc
            && fail_calloc_size == 0 && !sess->cleanup_active
            && !sess->cleanup_pending && sess->cleanup_reserved_bytes == 0
            && (!recursive ? !session_find_rel(sess, "output")
            : session_find_rel(sess, "output")->nrows == 0),
            "publication failure lost ownership");
    }
    col_rel_t *target = col_rel_new_auto("output", 1);
    SERIAL_FAIL_CHECK(target, "existing target allocation");
    rc = session_add_rel(sess, target);
    if (rc != 0)
        col_rel_destroy(target);
    SERIAL_FAIL_CHECK(rc == 0, "existing target registration");
    uint64_t target_generation = target->view_generation;
    uint64_t reserved_before = wl_columnar_memory_reserved(budget);
    for (unsigned mode = 0; mode < 2; mode++) {
        serial_fail_promotion = false;
        serial_deny_copy = mode != 0;
        wl_columnar_eval_serial_test_after_plan =
            inject_serial_publication_failure;
        rc = col_eval_stratum(&stratum, sess, 0);
        budget->mode = saved_mode;
        atomic_store_explicit(&budget->usable_bytes, saved_limit,
            memory_order_relaxed);
        serial_deny_copy = false;
        SERIAL_FAIL_CHECK(rc == ENOMEM
            && !fail_next_alloc && target->nrows == 0
            && target->view_generation == target_generation
            && wl_columnar_memory_reserved(budget) == reserved_before
            && !sess->cleanup_pending && sess->cleanup_reserved_bytes == 0,
            "private copy failure changed target or accounting");
    }
#endif
    SERIAL_FAIL_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
        && tuples.count == (recursive ? 1 : 2) && tuples.rows[0][0] == value
        && (recursive || tuples.rows[1][0] == value) &&
        sess->cleanup_reserved_bytes == 0,
        "publication retry oracle");
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
    fail_calloc_size = 0;
    serial_deny_copy = false;
#endif
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (frame)
        (void)wl_columnar_eval_stack_cleanup_finish(&frame);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef SERIAL_FAIL_CHECK
}

static int serial_metadata_rc;
static void
inject_serial_metadata(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    wl_columnar_eval_serial_test_after_plan = NULL;
    serial_metadata_rc = EINVAL;
    col_rel_t *source = result->rel;
    if (!source || source->nrows != 2 || col_rel_enable_timestamps(source) != 0)
        return;
    source->timestamps[0] = (col_delta_timestamp_t){
        .iteration = 7, .stratum = 3, .worker = 2, .multiplicity = -4
    };
    source->timestamps[1] = (col_delta_timestamp_t){
        .iteration = 8, .stratum = 4, .worker = 1, .multiplicity = 6
    };
    source->compound_arity_map = malloc(sizeof(uint32_t));
    if (!source->compound_arity_map)
        return;
    source->compound_arity_map[0] = 1;
    source->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    source->compound_count = 1;
    col_rel_t *copy = wl_columnar_relation_new_like_governed("metadata", source,
            sess->memory_governor);
    if (!copy)
        return;
    bool preserved = copy->compound_kind == source->compound_kind
        && copy->compound_count == 1 && copy->compound_arity_map
        && copy->compound_arity_map != source->compound_arity_map
        && copy->compound_arity_map[0] == 1 && copy->timestamps;
    col_rel_destroy(copy);
    if (!preserved)
        return;
#ifdef WL_TEST_ALLOC_WRAP
    uint64_t before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(sess->memory_governor));
    fail_malloc_size = sizeof(uint32_t);
    copy = wl_columnar_relation_new_like_governed("metadata", source,
            sess->memory_governor);
    bool refused = !copy && fail_malloc_size == 0;
    fail_malloc_size = 0;
    col_rel_destroy(copy);
    if (!refused || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(sess->memory_governor)) !=
        before)
        return;
#endif
    serial_metadata_rc = 0;
}

static void
test_serial_staging_metadata(void)
{
    TEST(
        "serial caller: governed staging preserves timestamp and compound metadata");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_relation_t relation = { .name = "output", .ops = ops,
                                    .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *target = NULL;
    const char *failure = NULL;
    int64_t value = 42;
#define META_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    META_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0
        && wl_session_insert(session, "input", &value, 1, 1) == 0, "fixture");
    wl_col_session_t *sess = COL_SESSION(session);
    target = col_rel_new_auto("output", 1);
    META_CHECK(target && col_rel_enable_timestamps(target) == 0, "target");
    col_rel_t *registered = target;
    META_CHECK(session_add_rel(sess, target) == 0, "register");
    target = NULL;
    wl_columnar_eval_serial_test_after_plan = inject_serial_metadata;
    META_CHECK(col_eval_stratum(&stratum, sess,
        0) == 0 && serial_metadata_rc == 0,
        "metadata staging");
    META_CHECK(registered->nrows == 2 && registered->timestamps
        && registered->timestamps[0].iteration == 7
        && registered->timestamps[0].stratum == 3
        && registered->timestamps[0].worker == 2
        && registered->timestamps[0].multiplicity == -4
        && registered->timestamps[1].iteration == 8
        && registered->timestamps[1].stratum == 4
        && registered->timestamps[1].worker == 1
        && registered->timestamps[1].multiplicity == 6
        && !sess->cleanup_pending && sess->cleanup_reserved_bytes == 0,
        "staging lost provenance or multiplicity");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    col_rel_destroy(target);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef META_CHECK
}

static void
test_recursive_delta_operator_error(void)
{
    TEST(
        "recursive delta: operator error releases CONCAT metadata before retry");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "missing" }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define ERROR_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    ERROR_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    ERROR_CHECK(wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0 && deltas.count == 1, "baseline");
    deltas.count = 0;
    ERROR_CHECK(wl_session_insert(session, "input", &two, 1, 1) == 0,
        "new input");
    output.op_count = 4;
    uint64_t retained = 0;
    for (unsigned retry = 0; retry < 3; retry++) {
        ERROR_CHECK(wl_session_step(session) == ENOENT && deltas.count == 0
            && sess->delta_observer && !sess->cleanup_pending
            && !sess->cleanup_active && sess->cleanup_reserved_bytes == 0,
            "operator error lost observer or leaked frame");
        uint64_t now = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(sess->memory_governor));
        ERROR_CHECK(retry == 0 || now == retained, "retry accounting growth");
        retained = now;
    }
    output.op_count = 3;
    ERROR_CHECK(wl_session_step(session) == 0 && deltas.count == 1
        && has_delta(&deltas, "output", &two, 1, +1)
        && session_find_rel(sess, "output")->nrows == 2,
        "exact successful retry");
    deltas.count = 0;
    ERROR_CHECK(wl_session_step(session) == 0 && deltas.count == 0,
        "duplicate notification");
cleanup:
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef ERROR_CHECK
}

static unsigned serial_scope_hook_calls;
static void
count_serial_scope_hook(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)sess;
    (void)stack;
    (void)result;
    serial_scope_hook_calls++;
}

static void
test_coordinator_specialized_parallel(void)
{
    TEST("coordinator: safe parallel evaluation still bypasses serial frames");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t output = { .name = "output", .ops = &op, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    int64_t *values = NULL;
    const char *failure = NULL;
    const uint32_t count = 65536;
#define PARALLEL_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    PARALLEL_CHECK(wl_session_create(wl_backend_columnar(), &plan, 2,
        &session) == 0, "create");
    values = malloc((size_t)count * sizeof(*values));
    PARALLEL_CHECK(values, "allocate input");
    for (uint32_t row = 0; row < count; row++)
        values[row] = row;
    PARALLEL_CHECK(wl_session_insert(session, "input", values, count, 1) == 0,
        "insert");
    serial_scope_hook_calls = 0;
    wl_columnar_eval_serial_test_after_plan = count_serial_scope_hook;
    wl_col_session_t *sess = COL_SESSION(session);
    PARALLEL_CHECK(col_eval_stratum(&stratum, sess, 0) == 0
        && sess->tdd_workers_cap >= 2 && sess->tdd_workers_count == 0
        && serial_scope_hook_calls == 0 && !sess->cleanup_pending,
        "specialized worker path did not complete");
    col_rel_t *result = session_find_rel(sess, "output");
    PARALLEL_CHECK(result && result->nrows == count, "parallel row count");
    for (uint32_t row = 0; row < count; row++)
        PARALLEL_CHECK(col_rel_get(result, row, 0) == (int64_t)row,
            "parallel exact rows");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef PARALLEL_CHECK
}

extern bool wl_columnar_session_test_enable_correctness_replay;
extern void (*wl_columnar_session_test_before_correctness_replay)(
    wl_col_session_t *);
static bool correctness_replay_entered, correctness_replay_lower;
static wl_plan_op_t *correctness_replay_fault_op;
static wl_plan_relation_t *correctness_replay_plan;
static wl_plan_op_t *correctness_replay_ops;
static int correctness_replay_hook_rc;
static wl_columnar_source_access_reader_t correctness_replay_reader;
static col_rel_t *correctness_replay_held;
static col_rel_t *correctness_replay_target;
static uint64_t correctness_replay_target_view;
static uint32_t correctness_replay_target_rows;
static int64_t **correctness_replay_target_columns;

static void
correctness_replay_before(wl_col_session_t *sess)
{
    correctness_replay_entered = sess->tdd_executed_strata > 0
        && sess->tdd_workers_cap >= 2 && sess->tdd_workers_count == 0;
    correctness_replay_plan->ops = correctness_replay_ops;
    correctness_replay_plan->op_count = 4;
    if (correctness_replay_fault_op)
        correctness_replay_fault_op->relation_name = "missing_replay_input";
}

static void
correctness_replay_hold(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    if (sess->coordinator || !correctness_replay_entered || !result->owned
        || !result->rel || result->seg_count != 2)
        return;
    wl_columnar_eval_serial_test_after_plan = NULL;
    correctness_replay_hook_rc = EINVAL;
    correctness_replay_held = result->rel;
    if (correctness_replay_lower) {
        col_rel_t *lower = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "replay_lower", 1);
        int64_t value = 42;
        if (!lower || col_rel_append_row(lower, &value) != 0
            || eval_stack_push(stack, lower, true) != 0) {
            col_rel_destroy(lower);
            return;
        }
        correctness_replay_held = lower;
    }
    correctness_replay_target = session_find_rel(sess, "output");
    if (!correctness_replay_target)
        return;
    correctness_replay_target_view = correctness_replay_target->view_generation;
    correctness_replay_target_rows = correctness_replay_target->nrows;
    correctness_replay_target_columns = correctness_replay_target->columns;
    correctness_replay_hook_rc = col_rel_source_reader_acquire(
        correctness_replay_held, &correctness_replay_reader);
}

static void
test_correctness_replay_refusal(unsigned mode)
{
    TEST(
        "TDD correctness replay: refusal preserves live owners before restoration");
    uint32_t key = 0;
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    /* Worker CONCAT metadata is tracked separately by #1686. Run real TDD
     * with VARIABLE+EXCHANGE, then substitute a set-equivalent duplicate
     * CONCAT only after workers drain, at the production replay boundary. */
    wl_plan_op_t worker_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = worker_ops, .op_count = 2 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    int64_t *values = NULL;
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
    const uint32_t count = 8192;
#define REPLAY_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    correctness_replay_plan = &output;
    correctness_replay_ops = ops;
    correctness_replay_entered = false;
    correctness_replay_lower = mode == 1;
    correctness_replay_fault_op = mode == 2 ? &ops[0] : NULL;
    correctness_replay_hook_rc = EINVAL;
    correctness_replay_held = NULL;
    memset(&correctness_replay_reader, 0, sizeof(correctness_replay_reader));
    REPLAY_CHECK(wl_session_create(wl_backend_columnar(), &plan, 2,
        &session) == 0, "create");
    values = malloc((size_t)count * sizeof(*values));
    REPLAY_CHECK(values, "allocate input");
    for (uint32_t row = 0; row < count; row++)
        values[row] = 42;
    REPLAY_CHECK(wl_session_insert(session, "input", values, count, 1) == 0,
        "insert");
    wl_columnar_session_test_enable_correctness_replay = true;
    wl_columnar_session_test_before_correctness_replay =
        correctness_replay_before;
    wl_columnar_eval_serial_test_after_plan = correctness_replay_hold;
    int rc = wl_session_snapshot(session, collect_tuple, &tuples);
    wl_col_session_t *sess = COL_SESSION(session);
    if (mode == 2) {
        REPLAY_CHECK(rc == ENOENT && correctness_replay_entered
            && !sess->cleanup_pending && sess->cleanup_reserved_bytes == 0
            && tuples.count == 0, "replay operator error not propagated");
        ops[0].relation_name = "input";
        correctness_replay_fault_op = NULL;
        goto retry;
    }
    REPLAY_CHECK(rc == EBUSY && correctness_replay_entered
        && correctness_replay_hook_rc == 0 && sess->cleanup_pending_count == 1
        && tuples.count == 0, "actual replay refusal not propagated");
    REPLAY_CHECK(correctness_replay_target->columns ==
        correctness_replay_target_columns
        && correctness_replay_target->nrows == correctness_replay_target_rows
        && correctness_replay_target->view_generation ==
        correctness_replay_target_view,
        "TDD restoration changed pending replay target");
    size_t arena_used = sess->eval_arena->used;
    uint32_t pool_slots = sess->delta_pool->slot_used;
    uint64_t bytes = sess->cleanup_reserved_bytes;
    int64_t **held_columns = correctness_replay_held->columns;
    for (unsigned retry = 0; retry < 2; retry++) {
        output.ops = worker_ops;
        output.op_count = 2;
        REPLAY_CHECK(wl_session_snapshot(session, collect_tuple,
            &tuples) == EBUSY
            && tuples.count == 0 && sess->cleanup_reserved_bytes == bytes
            && sess->eval_arena->used == arena_used
            && sess->delta_pool->slot_used == pool_slots
            && correctness_replay_held->columns == held_columns
            && col_rel_get(correctness_replay_held, 0, 0) == 42,
            "retry reset retained replay storage");
    }
    REPLAY_CHECK(col_rel_source_reader_release(&correctness_replay_reader) == 0,
        "release");
retry:
    output.ops = worker_ops;
    output.op_count = 2;
    wl_columnar_eval_serial_test_after_plan = NULL;
    int retry_rc = wl_session_snapshot(session, collect_tuple, &tuples);
    REPLAY_CHECK(retry_rc == 0
        && tuples.count == 1 && tuples.rows[0][0] == 42
        && strcmp(tuples.relations[0], "output") == 0
        && !sess->cleanup_pending && sess->cleanup_reserved_bytes == 0,
        "exact snapshot retry");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    wl_columnar_session_test_before_correctness_replay = NULL;
    wl_columnar_session_test_enable_correctness_replay = false;
    correctness_replay_fault_op = NULL;
    correctness_replay_plan = NULL;
    correctness_replay_ops = NULL;
    if (correctness_replay_reader.owner)
        (void)col_rel_source_reader_release(&correctness_replay_reader);
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef REPLAY_CHECK
}

extern void (*wl_columnar_eval_serial_test_before_aggregate)(
    wl_col_session_t *, col_rel_t *);

static void
test_aggregate_publication_guard(unsigned mode, bool maximum)
{
    TEST("aggregate publication: refuse dependencies and preserve provenance");
    wl_plan_relation_t output = {
        .name = "output",
        .recursive_agg = { .has_spec = true,
                           .fn = maximum ? WIRELOG_AGG_MAX : WIRELOG_AGG_MIN,
                           .operand_type = WL_PLAN_AGG_OPERAND_SCALAR,
                           .group_by_count = 1, .aggregate_index = 1 }
    };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = true };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL, *alias = NULL, *root = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    const char *failure = NULL;
#define AGG_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    AGG_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0, "aggregate session");
    wl_col_session_t *sess = COL_SESSION(session);
    unowned = col_rel_new_auto("output", 2);
    AGG_CHECK(unowned && col_rel_enable_timestamps(unowned) == 0,
        "aggregate relation");
    int64_t rows[][2] = { { 42, 30 }, { 42, 10 }, { 43, 20 } };
    for (unsigned i = 0; i < 3; i++) {
        AGG_CHECK(col_rel_append_row(unowned, rows[i]) == 0, "aggregate rows");
        unowned->timestamps[i].iteration = 10 + i;
        unowned->timestamps[i].multiplicity = (int32_t)i + 1;
    }
    AGG_CHECK(session_add_rel(sess, unowned) == 0, "aggregate register");
    root = unowned;
    unowned = NULL;
    uint32_t key = 0;
    AGG_CHECK(col_session_get_arrangement(session, "output", &key, 1),
        "aggregate arrangement");
    col_arr_entry_t arrangement = sess->arr_entries[0];
    uint64_t view = root->view_generation, storage = root->storage_generation;
    col_delta_timestamp_t timestamps[3];
    memcpy(timestamps, root->timestamps, sizeof(timestamps));
    int64_t **columns = root->columns;
    if (mode >= 1 && mode <= 3) {
        alias = col_rel_new_like("alias", root);
        AGG_CHECK(alias && col_rel_install_shared_view(alias, root) == 0,
            "aggregate shared alias");
    }
    if (mode <= 1)
        AGG_CHECK(col_rel_source_reader_acquire(mode ? alias : root,
            &reader) == 0, "aggregate reader");
    if (mode == 3) {
        AGG_CHECK(session_add_rel(sess, alias) == 0, "alias destination");
        alias = NULL;
        output.name = "alias";
    }
    for (unsigned attempt = 0; attempt < 2; attempt++) {
#ifdef WL_TEST_ALLOC_WRAP
        if (mode == 4)
            fail_next_alloc = true;
#endif
        if (mode == 5)
            output.recursive_agg.group_by_count = 2;
        int rc = wl_columnar_eval_serial_canonicalize_aggregates(&stratum,
                sess);
        AGG_CHECK(rc == (mode == 4 ? ENOMEM : mode == 5 ? EINVAL : EBUSY),
            "aggregate refusal code");
        AGG_CHECK(root->nrows == 3 && root->columns == columns
            && root->view_generation == view &&
            root->storage_generation == storage
            && memcmp(timestamps, root->timestamps, sizeof(timestamps)) == 0
            && memcmp(&arrangement, &sess->arr_entries[0],
            sizeof(arrangement)) == 0,
            "refusal mutated relation or arrangement");
        for (unsigned row = 0; row < 3; row++)
            AGG_CHECK(root->columns[0][row] == rows[row][0]
                && root->columns[1][row] == rows[row][1],
                "refusal changed rows");
        if (mode >= 4) {
            wl_columnar_source_access_writer_t writer = { 0 };
            AGG_CHECK(col_rel_source_writer_acquire(root, &writer) == 0
                && wl_columnar_source_access_writer_release(&writer) == 0,
                "failed aggregate retained writer");
        }
    }
    if (reader.owner)
        AGG_CHECK(col_rel_source_reader_release(&reader) == 0,
            "release reader");
    if (mode == 3) {
        AGG_CHECK(session_remove_rel(sess, "alias") == 0, "retire alias");
        output.name = "output";
    }
    col_rel_destroy(alias); alias = NULL;
    output.recursive_agg.group_by_count = 1;
    AGG_CHECK(wl_columnar_eval_serial_canonicalize_aggregates(&stratum,
        sess) == 0
        && root->nrows == 2 && root->columns[0][0] == 42
        && root->columns[1][0] == (maximum ? 30 : 10)
        && root->timestamps[0].iteration == (maximum ? 10u : 11u)
        && root->timestamps[0].multiplicity == (maximum ? 1 : 2)
        && root->columns[0][1] == 43 && root->columns[1][1] == 20
        && root->timestamps[1].iteration == 12,
        "exact aggregate/provenance retry");
    view = root->view_generation;
    AGG_CHECK(wl_columnar_eval_serial_canonicalize_aggregates(&stratum,
        sess) == 0
        && root->nrows == 2 && root->view_generation == view,
        "aggregate retry not idempotent");
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (reader.owner)
        (void)col_rel_source_reader_release(&reader);
    col_rel_destroy(alias);
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef AGG_CHECK
}

/* Use the repository's 64-bit CAS abstraction: MSVC has no C11 atomic
 * qualifier, atomic_uint, or generic atomic_fetch_or shim. */
static void
record_actual_worker(wl_atomic_u64 *workers, uint32_t worker_id)
{
    uint64_t expected = atomic_load_explicit(workers, memory_order_relaxed);
    uint64_t bit = UINT64_C(1) << worker_id;
    while (!atomic_compare_exchange_weak_explicit(workers, &expected,
        expected | bit, memory_order_relaxed, memory_order_relaxed)) {
    }
}

static wl_columnar_source_access_reader_t aggregate_public_reader;
static col_rel_t *aggregate_public_target;
static col_rel_t *aggregate_public_alias;
static bool aggregate_public_hold_alias;
static int aggregate_public_hook_rc;
static wl_atomic_u64 aggregate_public_workers;
extern void (*wl_columnar_eval_test_outbound_after_plan)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *);

static void
observe_aggregate_worker(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    (void)result;
    if (worker->coordinator)
        record_actual_worker(&aggregate_public_workers, worker->worker_id);
}

static void
hold_aggregate_publication(wl_col_session_t *sess, col_rel_t *rel)
{
    if (sess->coordinator || aggregate_public_target || !rel || rel->nrows < 2)
        return;
    aggregate_public_target = rel;
    if (aggregate_public_hold_alias) {
        aggregate_public_alias = col_rel_new_like("aggregate_alias", rel);
        if (!aggregate_public_alias) {
            aggregate_public_hook_rc = ENOMEM;
            return;
        }
        aggregate_public_hook_rc = col_rel_install_shared_view(
            aggregate_public_alias, rel);
        if (aggregate_public_hook_rc != 0)
            return;
    }
    aggregate_public_hook_rc = col_rel_source_reader_acquire(
        aggregate_public_hold_alias ? aggregate_public_alias : rel,
        &aggregate_public_reader);
}

static void
test_aggregate_public_retry(uint32_t workers, bool alias_reader)
{
    TEST("public aggregate: final publication refusal and exact retry");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    uint32_t key = 0;
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t output = {
        .name = "output", .delta_name = "$d$output", .ops = ops, .op_count = 3,
        .recursive_agg = { .has_spec = true, .fn = WIRELOG_AGG_MIN,
                           .operand_type = WL_PLAN_AGG_OPERAND_SCALAR,
                           .group_by_count = 1, .aggregate_index = 1 }
    };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    int64_t *values = NULL;
    const char *failure = NULL;
    tuple_collector_t tuples = { 0 };
    memset(&aggregate_public_reader, 0, sizeof(aggregate_public_reader));
    aggregate_public_target = NULL;
    aggregate_public_alias = NULL;
    aggregate_public_hold_alias = alias_reader;
    aggregate_public_hook_rc = EINVAL;
#define PUBLIC_AGG_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    PUBLIC_AGG_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "public aggregate session");
    wl_col_session_t *sess = COL_SESSION(session);
    values = malloc(65536 * 2 * sizeof(*values));
    PUBLIC_AGG_CHECK(values, "public aggregate input");
    for (unsigned i = 0; i < 65536; i++) {
        values[2 * i] = 42;
        values[2 * i + 1] = i % 2 ? 10 : 30;
    }
    PUBLIC_AGG_CHECK(wl_session_insert(session, "input", values, 65536, 2) == 0,
        "public aggregate insert");
    atomic_store_explicit(&aggregate_public_workers, 0, memory_order_relaxed);
    wl_columnar_eval_test_outbound_after_plan = observe_aggregate_worker;
    wl_columnar_eval_serial_test_before_aggregate = hold_aggregate_publication;
    int rc = wl_session_snapshot(session, collect_tuple, &tuples);
    wl_columnar_eval_serial_test_before_aggregate = NULL;
    wl_columnar_eval_test_outbound_after_plan = NULL;
    PUBLIC_AGG_CHECK(rc == EBUSY && aggregate_public_hook_rc == 0
        && aggregate_public_target && aggregate_public_target->nrows == 2
        && tuples.count == 0, "public final aggregate refusal");
    if (workers > 1)
        PUBLIC_AGG_CHECK(sess->tdd_executed_strata == 1
            && atomic_load_explicit(&aggregate_public_workers,
            memory_order_relaxed) == (1u << workers) - 1u,
            "actual aggregate workers");
    PUBLIC_AGG_CHECK(col_session_get_arrangement(session, "output", &key, 1),
        "held target arrangement");
    uint32_t arrangement_index = 0;
    while (arrangement_index < sess->arr_count &&
        strcmp(sess->arr_entries[arrangement_index].rel_name, "output") != 0)
        arrangement_index++;
    PUBLIC_AGG_CHECK(arrangement_index < sess->arr_count,
        "held arrangement registration");
    col_arr_entry_t held_arrangement = sess->arr_entries[arrangement_index];
    uint64_t view = aggregate_public_target->view_generation;
    uint64_t storage = aggregate_public_target->storage_generation;
    uint32_t capacity = aggregate_public_target->capacity;
    int64_t **columns = aggregate_public_target->columns;
    /* Repeated public evaluation must also preserve this held target
     * through the earlier final relation normalization boundary. */
    col_delta_timestamp_t saved_timestamps[2];
    bool timestamped = aggregate_public_target->timestamps != NULL;
    if (timestamped)
        memcpy(saved_timestamps, aggregate_public_target->timestamps,
            sizeof(saved_timestamps));
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        int retry_rc = wl_session_snapshot(session, collect_tuple, &tuples);
        PUBLIC_AGG_CHECK(retry_rc == EBUSY && tuples.count == 0
            && aggregate_public_target->nrows == 2
            && aggregate_public_target->view_generation == view
            && aggregate_public_target->columns == columns
            && aggregate_public_target->storage_generation == storage
            && aggregate_public_target->capacity == capacity
            && memcmp(&held_arrangement, &sess->arr_entries[arrangement_index],
            sizeof(held_arrangement)) == 0
            && (!timestamped || memcmp(saved_timestamps,
            aggregate_public_target->timestamps,
            sizeof(saved_timestamps)) == 0),
            "repeated aggregate boundary refusal");
    }
    PUBLIC_AGG_CHECK(col_rel_source_reader_release(&aggregate_public_reader) ==
        0,
        "public aggregate release");
    col_rel_destroy(aggregate_public_alias);
    aggregate_public_alias = NULL;
    PUBLIC_AGG_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
        && tuples.count == 1 && tuples.rows[0][0] == 42
        && tuples.rows[0][1] == 10
        && strcmp(tuples.relations[0], "output") == 0,
        "public exact aggregate retry");
cleanup:
    wl_columnar_eval_serial_test_before_aggregate = NULL;
    wl_columnar_eval_test_outbound_after_plan = NULL;
    if (aggregate_public_reader.owner)
        (void)col_rel_source_reader_release(&aggregate_public_reader);
    col_rel_destroy(aggregate_public_alias);
    aggregate_public_alias = NULL;
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef PUBLIC_AGG_CHECK
}

static wl_atomic_u64 schema_parity_workers;

static void
observe_schema_parity_worker(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    (void)result;
    if (worker->coordinator)
        record_actual_worker(&schema_parity_workers, worker->worker_id);
}

static void
test_parallel_schema_parity(uint32_t workers, bool typed, bool existing)
{
    TEST("parallel schema parity: clean step and exact snapshot");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL;
    int64_t *values = NULL;
    const char *failure = NULL;
    tuple_collector_t tuples = { 0 };
#define PARITY_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    PARITY_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create parity session");
    wl_col_session_t *coord = COL_SESSION(session);
    values = malloc(65536 * sizeof(*values));
    PARITY_CHECK(values, "parity rows");
    for (unsigned i = 0; i < 65536; i++)
        values[i] = 42;
    PARITY_CHECK(wl_session_insert(session, "input", values, 65536, 1) == 0,
        "parity input");
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    if (typed)
        PARITY_CHECK(col_rel_set_column_types(session_find_rel(coord, "input"),
            &type, 1) == 0, "typed input");
    if (existing) {
        unowned = col_rel_new_like("output", session_find_rel(coord, "input"));
        PARITY_CHECK(unowned && session_add_rel(coord, unowned) == 0,
            "empty existing output");
        unowned = NULL;
    }
    atomic_store_explicit(&schema_parity_workers, 0, memory_order_relaxed);
    wl_columnar_eval_serial_test_after_plan = observe_schema_parity_worker;
    int step_rc = wl_session_step(session);
    wl_columnar_eval_serial_test_after_plan = NULL;
    PARITY_CHECK(step_rc == 0
        && atomic_load_explicit(&schema_parity_workers,
        memory_order_relaxed) == (1u << workers) - 1u,
        "clean actual parallel step");
    col_rel_t *result = session_find_rel(coord, "output");
    PARITY_CHECK(result && result->nrows == 1
        && col_rel_get(result, 0, 0) == 42
        && (result->column_types != NULL) == typed,
        "parallel step exact output schema");
    PARITY_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
        && tuples.count == 1 && tuples.rows[0][0] == 42
        && strcmp(tuples.relations[0], "output") == 0,
        "parallel exact snapshot");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef PARITY_CHECK
}

static void
test_worker_serial_segments(bool recursive, unsigned mode)
{
    TEST("worker serial CONCAT: consumed metadata, empty and error retry");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, mode == 1 ? 0 : 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "missing" }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = mode == 2 ? 6 : 5 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = recursive };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *worker = NULL;
    col_rel_t *partition = NULL;
    const char *failure = NULL;
    bool initialized = false;
#define SEG_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    SEG_CHECK(wl_session_create(wl_backend_columnar(), &plan, 2,
        &session) == 0, "create");
    worker = calloc(1, sizeof(*worker));
    SEG_CHECK(worker && col_worker_session_create(COL_SESSION(session), 0,
        NULL, 0, worker) == 0, "worker create");
    initialized = true;
    partition = col_rel_new_auto("input", 1);
    int64_t value = 42;
    SEG_CHECK(partition && col_rel_append_row(partition, &value) == 0,
        "worker input");
    SEG_CHECK(session_add_rel(worker, partition) == 0, "register input");
    partition = NULL;
    if (mode == 2) {
        SEG_CHECK(col_eval_stratum(&stratum, worker, 0) == ENOENT,
            "post-CONCAT operator error");
        output.op_count = 5;
    }
    SEG_CHECK(col_eval_stratum(&stratum, worker, 0) == 0,
        "worker evaluation/retry");
    col_rel_t *result = session_find_rel(worker, "output");
    uint32_t expected = mode == 1 ? 0 : recursive ? 1 : 2;
    SEG_CHECK(result && result->nrows == expected
        && !worker->cleanup_active && !worker->cleanup_pending,
        "legacy worker exact count or accidental frame activation");
    for (uint32_t row = 0; row < expected; row++)
        SEG_CHECK(col_rel_get(result, row, 0) == 42, "exact worker rows");
cleanup:
    col_rel_destroy(partition);
    if (initialized && col_worker_session_destroy(worker) != 0)
        failure = "worker cleanup";
    free(worker);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef SEG_CHECK
}

static unsigned worker_frame_storage;
static uint32_t worker_frame_target;
static const char *worker_frame_delta_name = "$d$output";
static col_rel_t *worker_frame_delta_owner;
static uint64_t worker_frame_delta_view, worker_frame_owner_view,
    worker_frame_owner_storage;

static uint32_t worker_frame_iteration;
static bool worker_frame_injected;
static int worker_frame_hook_rc;
static eval_entry_t *worker_frame_entry;
static col_rel_t *worker_frame_delta;
static wl_columnar_source_access_reader_t worker_frame_reader;
static wl_columnar_source_access_reader_t worker_frame_delta_reader;

/* Configuration and hook pointer remain fixed until the dispatch barrier.
 * Only the selected worker writes these captures; the caller reads them after join. */
static void
hold_worker_frame(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result)
{
    if (!worker->coordinator || worker->worker_id != worker_frame_target)
        return;
    if (worker_frame_injected ||
        worker->current_iteration != worker_frame_iteration
        || !result->owned || !result->rel || result->seg_count != 2)
        return;
    worker_frame_injected = true;
    worker_frame_hook_rc = EINVAL;
    if (worker_frame_iteration != 0) {
        worker_frame_delta = session_find_rel(worker, worker_frame_delta_name);
        if (!worker_frame_delta
            || eval_stack_push(stack, worker_frame_delta, false) != 0
            || col_rel_source_reader_acquire_transferable(worker_frame_delta,
            &worker_frame_delta_reader) != 0)
            return;
        if (col_rel_storage_owner_resolve(worker_frame_delta,
            &worker_frame_delta_owner) != 0)
            return;
        worker_frame_delta_view = worker_frame_delta->view_generation;
        worker_frame_owner_view = worker_frame_delta_owner->view_generation;
        worker_frame_owner_storage =
            worker_frame_delta_owner->storage_generation;
    }
    worker_frame_entry = result;
    if (worker_frame_storage >= 1 && worker_frame_storage <= 3) {
        col_rel_t *lower = worker_frame_storage == 1
            ? col_rel_new_auto("worker_lower", 1)
            : col_rel_pool_new_auto(worker->delta_pool,
                worker_frame_storage == 3 ? worker->eval_arena : NULL,
                "worker_lower", 1);
        int64_t value = 42;
        if (!lower || col_rel_append_row(lower, &value) != 0
            || eval_stack_push(stack, lower, true) != 0) {
            col_rel_destroy(lower);
            return;
        }
        worker_frame_entry = &stack->items[stack->top - 1];
        worker_frame_entry->seg_boundaries = malloc(3 * sizeof(uint32_t));
        if (!worker_frame_entry->seg_boundaries)
            return;
        worker_frame_entry->seg_boundaries[0] = 0;
        worker_frame_entry->seg_boundaries[1] = 1;
        worker_frame_entry->seg_boundaries[2] = 1;
        worker_frame_entry->seg_count = 2;
    } else if (worker_frame_storage == 4) {
        col_rel_t *heap = NULL;
        if (col_rel_deep_copy(result->rel, &heap, NULL) != 0)
            return;
        col_rel_destroy(result->rel);
        result->rel = heap;
    }
    worker_frame_hook_rc = col_rel_source_reader_acquire_transferable(
        worker_frame_entry->rel, &worker_frame_reader);
}

static void
test_worker_frame_retention(uint32_t workers, unsigned storage,
    bool public_step, bool recursive, uint32_t iteration)
{
    TEST(
        "worker frame: actual dispatch retains result, stack and coordinator owners");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
          .delta_mode = WL_DELTA_FORCE_FULL },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
          .delta_mode = WL_DELTA_FORCE_FULL },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = recursive };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *external = NULL;
    uint32_t initialized = 0;
    col_rel_t *unowned = NULL, *cache_left = NULL, *cache_right = NULL;
    col_rel_t *cache_result = NULL, *marker = NULL;
    col_mat_entry_t cache_before = { 0 };
    int64_t *values = NULL;
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
    worker_frame_target = 0;
    worker_frame_delta_name = "$d$output";
    worker_frame_storage = storage;
    worker_frame_iteration = iteration;
    worker_frame_injected = false;
    worker_frame_hook_rc = EINVAL;
    worker_frame_entry = NULL;
    worker_frame_delta = NULL;
    memset(&worker_frame_reader, 0, sizeof(worker_frame_reader));
    memset(&worker_frame_delta_reader, 0, sizeof(worker_frame_delta_reader));
#define FRAME_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    FRAME_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    uint32_t count = public_step ? 65536 : 1;
    values = malloc((size_t)count * sizeof(*values));
    FRAME_CHECK(values, "input allocation");
    for (uint32_t row = 0; row < count; row++)
        values[row] = 42;
    FRAME_CHECK(wl_session_insert(session, "input", values, count, 1) == 0,
        "insert");
    if (public_step) {
        /* Existing untyped IDB isolates cleanup from schema-clone defects
         * tracked in #1698 (empty/pool targets and typed partitions). */
        unowned = col_rel_new_auto("output", 1);
        FRAME_CHECK(unowned && col_rel_append_row(unowned, values) == 0
            && session_add_rel(sess, unowned) == 0, "existing output");
        unowned = NULL;
        unowned = col_rel_new_auto("$d$guard", 1);
        FRAME_CHECK(unowned && col_rel_append_row(unowned, values) == 0
            && session_add_rel(sess, unowned) == 0, "registry marker");
        marker = unowned;
        unowned = NULL;
        cache_left = col_rel_new_auto("cache_left", 1);
        cache_right = col_rel_new_auto("cache_right", 1);
        unowned = col_rel_new_auto("cache_result", 1);
        FRAME_CHECK(cache_left && cache_right && unowned
            && col_rel_append_row(cache_left, values) == 0
            && col_rel_append_row(cache_right, values) == 0
            && col_rel_append_row(unowned, values) == 0
            && col_mat_cache_insert(&sess->mat_cache, cache_left, cache_right,
            unowned) == 0, "cache fixture");
        cache_result = unowned;
        unowned = NULL;
        FRAME_CHECK(col_mat_cache_lookup(&sess->mat_cache, cache_left,
            cache_right) == cache_result, "cache epoch pin");
        col_mat_cache_clear(&sess->mat_cache);
        FRAME_CHECK(sess->mat_cache.count == 1
            && sess->mat_cache.entries[0].eviction_deferred
            && sess->mat_cache.active_pins == 1, "deferred cache owner");
        cache_before = sess->mat_cache.entries[0];
    } else {
        FRAME_CHECK(wl_columnar_session_ensure_workqueue(sess, workers) == 0,
            "workqueue");
        external = calloc(workers, sizeof(*external));
        FRAME_CHECK(external, "worker array");
        for (uint32_t w = 0; w < workers; w++) {
            FRAME_CHECK(col_worker_session_create(sess, w, NULL, 0,
                &external[w]) == 0, "worker create");
            initialized++;
            external[w].frontier_ops->reset_rule_frontier(&external[w], 0,
                external[w].outer_epoch);
            unowned = col_rel_new_auto("input", 1);
            FRAME_CHECK(unowned && col_rel_append_row(unowned, values) == 0
                && col_rel_enable_timestamps(unowned) == 0
                && session_add_rel(&external[w], unowned) == 0,
                "worker input");
            unowned->timestamps[0].multiplicity = 1;
            unowned = NULL;
        }
    }
    wl_columnar_eval_serial_test_after_plan = hold_worker_frame;
    int rc = public_step
        ? wl_session_step(session)
        : col_eval_stratum_multiworker(&stratum, sess, 0, external, workers);
    FRAME_CHECK(rc == EBUSY && worker_frame_injected
        && worker_frame_hook_rc == 0 && tuples.count == 0,
        "actual worker cleanup refusal");
    wl_col_session_t *worker =
        public_step ? &sess->tdd_workers[0] : external;
    FRAME_CHECK(worker->cleanup_pending_count == 1 && !worker->cleanup_active
        && worker_frame_entry && worker_frame_entry->rel
        && worker_frame_entry->seg_boundaries &&
        worker_frame_entry->seg_count == 2,
        "retained worker frame and segments");
    col_rel_t *held = worker_frame_entry->rel;
    int64_t **columns = held->columns;
    uint32_t *segments = worker_frame_entry->seg_boundaries;
    uint64_t bytes = worker->cleanup_reserved_bytes;
    size_t arena_used = worker->eval_arena->used;
    uint32_t slots = worker->delta_pool->slot_used;
    if (public_step)
        FRAME_CHECK(sess->tdd_workers_count == workers
            && sess->tdd_workers_cap >= workers &&
            worker->coordinator == sess,
            "actual public TDD worker cohort");
    for (unsigned retry = 0; retry < 2; retry++) {
        int retry_rc = public_step
            ? wl_session_snapshot(session, collect_tuple, &tuples)
            : col_eval_stratum(&stratum, worker, 0);
        FRAME_CHECK(retry_rc == EBUSY && tuples.count == 0
            && worker->cleanup_pending_count == 1
            && worker->cleanup_reserved_bytes == bytes
            && worker->eval_arena->used == arena_used
            && worker->delta_pool->slot_used == slots
            && worker_frame_entry->rel == held && held->columns == columns
            && worker_frame_entry->seg_boundaries == segments
            && col_rel_get(held, 0, 0) == 42,
            "retry changed retained worker storage");
        if (iteration != 0)
            FRAME_CHECK(session_find_rel(worker,
                "$d$output") == worker_frame_delta
                && worker_frame_delta->nrows == 1,
                "next-subpass registered delta reclaimed");
        if (public_step) {
            FRAME_CHECK(compound_cleanup_refusal_unchanged(session),
                "compound mutation bypassed worker cleanup");
            FRAME_CHECK(wl_session_step(session) == EBUSY,
                "step bypassed worker readiness");
            FRAME_CHECK(wl_session_insert(session, "input", values, 1,
                1) == EBUSY
                && wl_session_remove(session, "input", values, 1, 1) == EBUSY,
                "mutation bypassed worker readiness");
            col_mat_entry_t *entry = &sess->mat_cache.entries[0];
            FRAME_CHECK(session_find_rel(sess, "$d$guard") == marker
                && col_rel_get(marker, 0, 0) == 42
                && sess->mat_cache.count == 1 &&
                sess->mat_cache.active_pins == 1
                && entry->result == cache_result && entry->owner_alive
                && entry->identity == cache_before.identity
                && entry->generation == cache_before.generation
                && entry->pin_epoch == cache_before.pin_epoch
                && entry->epoch_pin_count == cache_before.epoch_pin_count
                && entry->pin_count == cache_before.pin_count
                && entry->ledger_bytes == cache_before.ledger_bytes
                && col_rel_get(cache_result, 0, 0) == 42,
                "coordinator reclaimed retained worker dependencies");
        } else {
            FRAME_CHECK(col_worker_session_destroy(worker) == EBUSY
                && worker->cleanup_pending_count == 1,
                "caller-owned worker destroyed live frame");
        }
    }
    FRAME_CHECK(col_rel_source_reader_release(&worker_frame_reader) == 0,
        "release result/lower reader");
    if (worker_frame_delta_reader.owner)
        FRAME_CHECK(col_rel_source_reader_release(&worker_frame_delta_reader) ==
            0,
            "release registered delta reader");
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (public_step) {
        rc = wl_session_step(session);
        FRAME_CHECK(rc == 0, "public step retry");
        FRAME_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
            && tuples.count == 1 && tuples.rows[0][0] == 42
            && strcmp(tuples.relations[0], "output") == 0
            && sess->tdd_workers_count == 0,
            "exact public snapshot retry");
        FRAME_CHECK(compound_cleanup_retry_once(session),
            "compound retry after actual worker cleanup");
    } else {
        /* The external caller owns the entire cohort. Recreate all workers
         * before a whole-evaluation retry so earlier nonrecursive publications
         * cannot be appended a second time. */
        for (uint32_t w = 0; w < initialized; w++)
            FRAME_CHECK(col_worker_session_destroy(&external[w]) == 0,
                "checked caller-owned cleanup");
        initialized = 0;
        memset(external, 0, (size_t)workers * sizeof(*external));
        for (uint32_t w = 0; w < workers; w++) {
            FRAME_CHECK(col_worker_session_create(sess, w, NULL, 0,
                &external[w]) == 0, "retry worker create");
            initialized++;
            external[w].frontier_ops->reset_rule_frontier(&external[w], 0,
                external[w].outer_epoch);
            unowned = col_rel_new_auto("input", 1);
            FRAME_CHECK(unowned && col_rel_append_row(unowned, values) == 0
                && col_rel_enable_timestamps(unowned) == 0
                && session_add_rel(&external[w], unowned) == 0, "retry input");
            unowned->timestamps[0].multiplicity = 1;
            unowned = NULL;
        }
        FRAME_CHECK(col_eval_stratum_multiworker(&stratum, sess, 0, external,
            workers) == 0, "caller-owned cohort retry");
        for (uint32_t w = 0; w < workers; w++) {
            col_rel_t *r = session_find_rel(&external[w], "output");
            FRAME_CHECK(r && r->nrows == (recursive ? 1u : 2u)
                && col_rel_get(r, 0, 0) == 42
                && !external[w].cleanup_pending
                && external[w].cleanup_reserved_bytes == 0,
                "exact recreated worker result");
        }
    }
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (worker_frame_reader.owner)
        (void)col_rel_source_reader_release(&worker_frame_reader);
    if (worker_frame_delta_reader.owner)
        (void)col_rel_source_reader_release(&worker_frame_delta_reader);
    for (uint32_t w = 0; w < initialized; w++)
        (void)col_worker_session_destroy(&external[w]);
    free(external);
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    col_rel_destroy(cache_left);
    col_rel_destroy(cache_right);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef FRAME_CHECK
}

extern void (*wl_columnar_eval_test_outbound_after_plan)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *);
static bool global_read_frame_flags;
static void
hold_global_read_frame(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result)
{
    if (worker->worker_id == worker_frame_target)
        global_read_frame_flags = worker->tdd_outbound_only_active &&
            worker->diff_operators_active;
    hold_worker_frame(worker, stack, result);
}

static void
test_tdd_recursive_frame_retention(uint32_t workers, unsigned storage,
    uint32_t held_worker, bool global_read)
{
    TEST(
        "recursive TDD frames: retained dependencies survive public readiness");
    uint32_t width = global_read ? 2 : 1;
    uint32_t key = 0, projection[] = { 0, 1 };
    const char *keys[] = { global_read ? "col1" : "col0" };
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t output_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "output" },
        { .op = WL_PLAN_OP_JOIN,
          .right_relation = global_read ? "input" : "output", .left_keys = keys,
          .right_keys = keys, .key_count = 1, .project_indices = projection,
          .project_count = width },
        { .op = WL_PLAN_OP_VARIABLE,
          .relation_name = global_read ? "input" : "relay" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_op_t relay_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t relations[] = {
        { .name = "output", .delta_name = "$d$output", .ops = output_ops,
          .op_count = 5 },
        { .name = "relay", .delta_name = "$d$relay", .ops = relay_ops,
          .op_count = 3 }
    };
    wl_plan_stratum_t stratum = { .relations = relations, .relation_count = 2,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL, *cache_left = NULL, *cache_right = NULL;
    col_rel_t *cache_result = NULL, *marker = NULL;
    int64_t *values = NULL;
    const char *failure = NULL;
    tuple_collector_t tuples = { 0 };
    global_read_frame_flags = false;
    worker_frame_target = held_worker;
    worker_frame_storage = storage;
    worker_frame_iteration = global_read ? 0 : 1;
    worker_frame_delta_name = "$d$relay";
    worker_frame_injected = false;
    worker_frame_hook_rc = EINVAL;
    worker_frame_entry = NULL;
    worker_frame_delta = worker_frame_delta_owner = NULL;
    memset(&worker_frame_reader, 0, sizeof(worker_frame_reader));
    memset(&worker_frame_delta_reader, 0, sizeof(worker_frame_delta_reader));
#define TDD_FRAME_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    TDD_FRAME_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    values = malloc((size_t)65536 * width * sizeof(*values));
    TDD_FRAME_CHECK(values, "input allocation");
    for (uint32_t i = 0; i < 65536 * width; i++) values[i] = 42;
    TDD_FRAME_CHECK(wl_session_insert(session, "input", values, 65536,
        width) == 0,
        "input");
    wirelog_column_type_t types[] = { WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64 };
    for (unsigned i = 0; i < 2; i++) {
        unowned = col_rel_new_auto(relations[i].name, width);
        TDD_FRAME_CHECK(unowned && col_rel_set_column_types(unowned, types,
            width) == 0
            && session_add_rel(sess, unowned) == 0, "typed empty IDB");
        unowned = NULL;
    }
    unowned = col_rel_new_auto("$d$guard", 1);
    TDD_FRAME_CHECK(unowned && col_rel_append_row(unowned, values) == 0
        && session_add_rel(sess, unowned) == 0, "coordinator delta owner");
    marker = unowned;
    unowned = NULL;
    cache_left = col_rel_new_auto("cache_left", 1);
    cache_right = col_rel_new_auto("cache_right", 1);
    unowned = col_rel_new_auto("cache_result", 1);
    TDD_FRAME_CHECK(cache_left && cache_right && unowned
        && col_rel_append_row(cache_left, values) == 0
        && col_rel_append_row(cache_right, values) == 0
        && col_rel_append_row(unowned, values) == 0
        && col_mat_cache_insert(&sess->mat_cache, cache_left, cache_right,
        unowned) == 0, "cache owner");
    cache_result = unowned;
    unowned = NULL;
    TDD_FRAME_CHECK(col_mat_cache_lookup(&sess->mat_cache, cache_left,
        cache_right)
        == cache_result, "cache epoch pin");
    col_mat_cache_clear(&sess->mat_cache);
    col_mat_entry_t cache_before = sess->mat_cache.entries[0];
    if (global_read)
        wl_columnar_eval_test_outbound_after_plan = hold_global_read_frame;
    else
        wl_columnar_eval_serial_test_after_plan = hold_worker_frame;
    int rc = wl_session_snapshot(session, collect_tuple, &tuples);
    TDD_FRAME_CHECK(rc == EBUSY && (!global_read || global_read_frame_flags) &&
        worker_frame_injected
        && worker_frame_hook_rc == 0 && tuples.count == 0
        && sess->tdd_executed_strata == 1 && sess->tdd_workers_count == workers,
        "actual recursive worker refusal");
    wl_col_session_t *worker = &sess->tdd_workers[held_worker];
    TDD_FRAME_CHECK(worker->cleanup_pending_count == 1 &&
        !worker->cleanup_active
        && (global_read || (worker_frame_delta && worker_frame_delta_owner
        && (held_worker == 0 ? worker_frame_delta == worker_frame_delta_owner
            : worker_frame_delta != worker_frame_delta_owner))),
        "natural shared root or alias dependency");
    col_rel_t *held = worker_frame_entry->rel;
    uint32_t *segments = worker_frame_entry->seg_boundaries;
    int64_t **columns = held->columns;
    uint64_t bytes = worker->cleanup_reserved_bytes;
    uint32_t slots = worker->delta_pool->slot_used;
    size_t used = worker->eval_arena->used;
    uint32_t epoch = sess->outer_epoch;
    uint32_t root_rows = global_read ? 0 : worker_frame_delta_owner->nrows;
    col_delta_timestamp_t *timestamps =
        global_read ? NULL : worker_frame_delta_owner->timestamps;
    TDD_FRAME_CHECK(segments && worker_frame_entry->seg_count == 2 && bytes > 0,
        "retained entry metadata");
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        TDD_FRAME_CHECK(wl_session_snapshot(session, collect_tuple,
            &tuples) == EBUSY
            && wl_session_step(session) == EBUSY
            && wl_session_insert(session, "input", values, 1, 1) == EBUSY
            && wl_session_remove(session, "input", values, 1, 1) == EBUSY
            && compound_cleanup_refusal_unchanged(session), "public readiness");
        TDD_FRAME_CHECK(tuples.count == 0 && sess->outer_epoch == epoch
            && worker->cleanup_pending_count == 1
            && worker->cleanup_reserved_bytes == bytes
            && worker->delta_pool->slot_used == slots &&
            worker->eval_arena->used == used
            && worker_frame_entry->rel == held && held->columns == columns
            && worker_frame_entry->seg_boundaries == segments
            && worker_frame_entry->seg_count == 2 && col_rel_get(held, 0,
            0) == 42,
            "retained entry or allocator changed");
        if (!global_read)
            TDD_FRAME_CHECK(session_find_rel(worker,
                "$d$relay") == worker_frame_delta
                && worker_frame_delta->view_generation ==
                worker_frame_delta_view
                && worker_frame_delta->nrows == 1
                && worker_frame_delta_owner->nrows == root_rows &&
                root_rows == 1
                && worker_frame_delta_owner->view_generation ==
                worker_frame_owner_view
                && worker_frame_delta_owner->storage_generation ==
                worker_frame_owner_storage
                && worker_frame_delta_owner->timestamps == timestamps
                && col_rel_get(worker_frame_delta_owner, 0, 0) == 42,
                "shared prior delta reclaimed or invalidated");
        col_mat_entry_t *cache = &sess->mat_cache.entries[0];
        TDD_FRAME_CHECK(session_find_rel(sess, "$d$guard") == marker
            && col_rel_get(marker, 0, 0) == 42 && sess->mat_cache.count == 1
            && sess->mat_cache.active_pins == 1 && cache->result == cache_result
            && cache->identity == cache_before.identity
            && cache->generation == cache_before.generation
            && cache->pin_count == cache_before.pin_count
            && cache->epoch_pin_count == cache_before.epoch_pin_count
            && cache->ledger_bytes == cache_before.ledger_bytes
            && col_rel_get(cache_result, 0, 0) == 42,
            "coordinator dependencies reclaimed");
    }
    TDD_FRAME_CHECK(col_rel_source_reader_release(&worker_frame_reader) == 0
        && (global_read ||
        col_rel_source_reader_release(&worker_frame_delta_reader) == 0),
        "release readers");
    wl_columnar_eval_serial_test_after_plan = NULL;
    wl_columnar_eval_test_outbound_after_plan = NULL;
    TDD_FRAME_CHECK(wl_session_snapshot(session, collect_tuple,
        &tuples) == 0
        && tuples.count == 2 && tuples.rows[0][0] == 42 &&
        tuples.rows[1][0] == 42
        && strcmp(tuples.relations[0], "output") == 0
        && strcmp(tuples.relations[1],
        "relay") == 0 && sess->tdd_workers_count == 0,
        "exact public snapshot retry");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    wl_columnar_eval_test_outbound_after_plan = NULL;
    worker_frame_target = 0;
    worker_frame_delta_name = "$d$output";
    if (worker_frame_reader.owner)
        (void)col_rel_source_reader_release(&worker_frame_reader);
    if (worker_frame_delta_reader.owner)
        (void)col_rel_source_reader_release(&worker_frame_delta_reader);
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    col_rel_destroy(cache_left);
    col_rel_destroy(cache_right);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef TDD_FRAME_CHECK
}

extern void (*wl_columnar_eval_test_outbound_after_plan)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *);
extern void (*wl_columnar_eval_test_outbound_before_publish)(wl_col_session_t *,
    eval_entry_t *);
static col_delta_timestamp_t *outbound_frame_timestamps;
static uint64_t outbound_frame_storage_generation;
static uint32_t outbound_frame_rows;
static bool outbound_frame_prefix;

static void
hold_outbound_frame(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result)
{
    if (worker->worker_id == worker_frame_target && result->seg_count == 2) {
        wl_mem_ledger_snapshot_t ledger;
        wl_mem_ledger_snapshot(&worker->coordinator->mem_ledger, &ledger);
        outbound_frame_prefix = ledger.subsys_bytes[WL_MEM_SUBSYS_CHANNEL]
            > worker->coordinator->mem_channel_ring_bytes;
    }
    if (worker_frame_storage < 5) {
        hold_worker_frame(worker, stack, result);
        return;
    }
    if (worker->worker_id == worker_frame_target && result->rel
        && result->seg_count == 2 && worker_frame_storage == 5)
        worker_frame_hook_rc = col_rel_enable_timestamps(result->rel);
}

static void
hold_outbound_candidate(wl_col_session_t *worker, eval_entry_t *result)
{
    if (worker_frame_storage < 5 || worker->worker_id != worker_frame_target
        || worker_frame_injected || !result->rel
        || worker->current_iteration != worker_frame_iteration
        || strcmp(result->rel->name, "$d$output") != 0)
        return;
    worker_frame_injected = true;
    worker_frame_entry = result;
    outbound_frame_timestamps = result->rel->timestamps;
    outbound_frame_storage_generation = result->rel->storage_generation;
    outbound_frame_rows = result->rel->nrows;
    worker_frame_hook_rc = col_rel_source_reader_acquire_transferable(
        result->rel, &worker_frame_reader);
}

static void
test_tdd_outbound_frame_retention(uint32_t workers, unsigned storage,
    uint32_t held_worker, bool later)
{
    TEST(
        "outbound TDD frames: queue prefix and retained owners survive public retry");
    uint32_t key = 0, projection[] = { 0 };
    const char *keys[] = { "col0" };
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t output_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "output" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "output", .left_keys = keys,
          .right_keys = keys, .key_count = 1, .project_indices = projection,
          .project_count = 1 },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "relay" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_op_t relay_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t relations[] = {
        { .name = "output", .delta_name = "$d$output", .ops = output_ops,
          .op_count = 5 },
        { .name = "relay", .delta_name = "$d$relay", .ops = relay_ops,
          .op_count = 3 }
    };
    output_ops[0].relation_name = "input";
    output_ops[1] = (wl_plan_op_t){ .op = WL_PLAN_OP_FILTER,
                                    .filter_expr = { predicate, 2 } };
    output_ops[2].relation_name = "input";
    output_ops[0].delta_mode = WL_DELTA_FORCE_FULL;
    output_ops[2].delta_mode = WL_DELTA_FORCE_FULL;
    wl_plan_relation_t tmp = relations[0];
    relations[0] = relations[1];
    relations[1] = tmp;
    wl_plan_stratum_t stratum = { .relations = relations, .relation_count = 2,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL, *cache_left = NULL, *cache_right = NULL;
    col_rel_t *cache_result = NULL, *marker = NULL;
    int64_t *values = NULL;
    const char *failure = NULL;
    tuple_collector_t tuples = { 0 };
    outbound_frame_prefix = false;
    worker_frame_target = held_worker;
    worker_frame_storage = storage;
    worker_frame_iteration = later ? 1 : 0;
    worker_frame_delta_name = "$d$relay";
    worker_frame_injected = false;
    worker_frame_hook_rc = EINVAL;
    worker_frame_entry = NULL;
    worker_frame_delta = worker_frame_delta_owner = NULL;
    memset(&worker_frame_reader, 0, sizeof(worker_frame_reader));
    memset(&worker_frame_delta_reader, 0, sizeof(worker_frame_delta_reader));
#define TDD_FRAME_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    TDD_FRAME_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    values = malloc(65536 * sizeof(*values));
    TDD_FRAME_CHECK(values, "input allocation");
    for (uint32_t i = 0; i < 65536; i++) values[i] = 42;
    TDD_FRAME_CHECK(wl_session_insert(session, "input", values, 65536, 1) == 0,
        "input");
    unowned = col_rel_new_auto("$d$guard", 1);
    TDD_FRAME_CHECK(unowned && col_rel_append_row(unowned, values) == 0
        && session_add_rel(sess, unowned) == 0, "coordinator delta owner");
    marker = unowned;
    unowned = NULL;
    cache_left = col_rel_new_auto("cache_left", 1);
    cache_right = col_rel_new_auto("cache_right", 1);
    unowned = col_rel_new_auto("cache_result", 1);
    TDD_FRAME_CHECK(cache_left && cache_right && unowned
        && col_rel_append_row(cache_left, values) == 0
        && col_rel_append_row(cache_right, values) == 0
        && col_rel_append_row(unowned, values) == 0
        && col_mat_cache_insert(&sess->mat_cache, cache_left, cache_right,
        unowned) == 0, "cache owner");
    cache_result = unowned;
    unowned = NULL;
    TDD_FRAME_CHECK(col_mat_cache_lookup(&sess->mat_cache, cache_left,
        cache_right)
        == cache_result, "cache epoch pin");
    col_mat_cache_clear(&sess->mat_cache);
    col_mat_entry_t cache_before = sess->mat_cache.entries[0];
    wl_columnar_eval_test_outbound_after_plan = hold_outbound_frame;
    wl_columnar_eval_test_outbound_before_publish = hold_outbound_candidate;
    int rc = wl_session_snapshot(session, collect_tuple, &tuples);
    TDD_FRAME_CHECK(rc == EBUSY && worker_frame_injected
        && worker_frame_hook_rc == 0 && tuples.count == 0
        && sess->tdd_executed_strata == 1 && sess->tdd_workers_count == workers,
        "actual outbound worker refusal");
    TDD_FRAME_CHECK(later || outbound_frame_prefix,
        "earlier rule did not publish a queue prefix");
    wl_col_session_t *worker = &sess->tdd_workers[held_worker];
    TDD_FRAME_CHECK(worker->cleanup_pending_count == 1 &&
        !worker->cleanup_active,
        "retained outbound frame");
    if (later && storage < 5)
        TDD_FRAME_CHECK(worker_frame_delta && worker_frame_delta_owner,
            "later-round registered delta dependency");
    col_rel_t *held = worker_frame_entry->rel;
    if (later && storage >= 5)
        TDD_FRAME_CHECK(held->nrows == 0, "empty prepared candidate");
    uint32_t *segments = worker_frame_entry->seg_boundaries;
    int64_t **columns = held->columns;
    uint64_t bytes = worker->cleanup_reserved_bytes;
    uint32_t slots = worker->delta_pool->slot_used;
    size_t used = worker->eval_arena->used;
    uint32_t epoch = sess->outer_epoch;
    TDD_FRAME_CHECK((storage >= 5 ||
        (segments && worker_frame_entry->seg_count == 2)) && bytes > 0,
        "retained entry metadata");
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        TDD_FRAME_CHECK(wl_session_snapshot(session, collect_tuple,
            &tuples) == EBUSY
            && wl_session_step(session) == EBUSY
            && wl_session_insert(session, "input", values, 1, 1) == EBUSY
            && wl_session_remove(session, "input", values, 1, 1) == EBUSY
            && compound_cleanup_refusal_unchanged(session), "public readiness");
        TDD_FRAME_CHECK(tuples.count == 0 && sess->outer_epoch == epoch
            && worker->cleanup_pending_count == 1
            && worker->cleanup_reserved_bytes == bytes
            && worker->delta_pool->slot_used == slots &&
            worker->eval_arena->used == used
            && worker_frame_entry->rel == held && held->columns == columns
            && worker_frame_entry->seg_boundaries == segments
            && (storage >= 5 || worker_frame_entry->seg_count == 2) &&
            (held->nrows == 0 || col_rel_get(held, 0, 0) == 42),
            "retained entry or allocator changed");
        if (storage >= 5)
            TDD_FRAME_CHECK(held->timestamps == outbound_frame_timestamps
                && held->storage_generation == outbound_frame_storage_generation
                && held->nrows == outbound_frame_rows
                && ((held->timestamps != NULL) == (storage == 5)),
                "late candidate mutated before reader release");
        wl_mem_ledger_snapshot_t queue_ledger;
        wl_mem_ledger_snapshot(&sess->mem_ledger, &queue_ledger);
        TDD_FRAME_CHECK(queue_ledger.subsys_bytes[WL_MEM_SUBSYS_CHANNEL] == 0
            && !sess->delta_queue && sess->mem_channel_ring_bytes == 0,
            "earlier queue prefix not reclaimed");
        if (later && storage < 5)
            TDD_FRAME_CHECK(session_find_rel(worker,
                "$d$relay") == worker_frame_delta
                && worker_frame_delta->view_generation ==
                worker_frame_delta_view
                && worker_frame_delta_owner->view_generation ==
                worker_frame_owner_view
                && worker_frame_delta_owner->storage_generation ==
                worker_frame_owner_storage,
                "prior-round dependency invalidated");
        col_mat_entry_t *cache = &sess->mat_cache.entries[0];
        TDD_FRAME_CHECK(session_find_rel(sess, "$d$guard") == marker
            && col_rel_get(marker, 0, 0) == 42 && sess->mat_cache.count == 1
            && sess->mat_cache.active_pins == 1 && cache->result == cache_result
            && cache->identity == cache_before.identity
            && cache->generation == cache_before.generation
            && cache->pin_count == cache_before.pin_count
            && cache->epoch_pin_count == cache_before.epoch_pin_count
            && cache->ledger_bytes == cache_before.ledger_bytes
            && col_rel_get(cache_result, 0, 0) == 42,
            "coordinator dependencies reclaimed");
    }
    TDD_FRAME_CHECK(col_rel_source_reader_release(&worker_frame_reader) == 0,
        "release readers");
    if (later && storage < 5)
        TDD_FRAME_CHECK(col_rel_source_reader_release(
                &worker_frame_delta_reader) == 0,
            "release prior delta reader");
    wl_columnar_eval_test_outbound_after_plan = NULL;
    wl_columnar_eval_test_outbound_before_publish = NULL;
    TDD_FRAME_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
        && tuples.count == 2 && tuples.rows[0][0] == 42 &&
        tuples.rows[1][0] == 42
        && strcmp(tuples.relations[0], "relay") == 0
        && strcmp(tuples.relations[1],
        "output") == 0 && sess->tdd_workers_count == 0,
        "exact public snapshot retry");
cleanup:
    wl_columnar_eval_test_outbound_after_plan = NULL;
    wl_columnar_eval_test_outbound_before_publish = NULL;
    worker_frame_target = 0;
    worker_frame_delta_name = "$d$output";
    if (worker_frame_reader.owner)
        (void)col_rel_source_reader_release(&worker_frame_reader);
    if (worker_frame_delta_reader.owner)
        (void)col_rel_source_reader_release(&worker_frame_delta_reader);
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    col_rel_destroy(cache_left);
    col_rel_destroy(cache_right);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef TDD_FRAME_CHECK
}

extern void (*wl_columnar_eval_test_subpass_boundary)(wl_col_session_t *,
    uint32_t,
    bool);
static unsigned ordinary_gate_mode, ordinary_gate_calls;
static uint32_t ordinary_gate_workers, ordinary_gate_slots;
static size_t ordinary_gate_used;
static bool ordinary_gate_verified, ordinary_gate_budget_changed;
static int ordinary_gate_rc;
static wl_columnar_eval_stack_cleanup_frame_t *ordinary_gate_frame;
static wl_columnar_memory_governor_t *ordinary_gate_budget;
static wl_columnar_memory_mode_t ordinary_gate_saved_mode;
static uint64_t ordinary_gate_saved_limit;

static void
count_ordinary_worker_plan(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    (void)result;
    if (sess->coordinator && sess->worker_id == 0)
        ordinary_gate_calls++;
}

static void
ordinary_worker_gate_boundary(wl_col_session_t *coord, uint32_t iteration,
    bool before)
{
    if (iteration != 0)
        return;
    wl_col_session_t *worker = &coord->tdd_workers[0];
    if (before) {
        ordinary_gate_workers = coord->tdd_workers_count;
        ordinary_gate_slots = worker->delta_pool->slot_used;
        ordinary_gate_used = worker->eval_arena->used;
        if (ordinary_gate_mode == 0 || ordinary_gate_mode == 3) {
            worker->current_iteration = 7;
            ordinary_gate_rc = wl_columnar_eval_stack_cleanup_begin(worker,
                    &ordinary_gate_frame);
        } else if (ordinary_gate_mode == 1 || ordinary_gate_mode == 4) {
            ordinary_gate_budget =
                wl_columnar_memory_governor_ref_get(coord->memory_governor);
            ordinary_gate_saved_mode = ordinary_gate_budget->mode;
            ordinary_gate_saved_limit =
                atomic_load_explicit(&ordinary_gate_budget->usable_bytes,
                    memory_order_relaxed);
            ordinary_gate_budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
            atomic_store_explicit(&ordinary_gate_budget->usable_bytes, 0,
                memory_order_relaxed);
            ordinary_gate_budget_changed = true;
        }
        return;
    }
    if (ordinary_gate_budget_changed) {
        ordinary_gate_budget->mode = ordinary_gate_saved_mode;
        atomic_store_explicit(&ordinary_gate_budget->usable_bytes,
            ordinary_gate_saved_limit, memory_order_relaxed);
        ordinary_gate_budget_changed = false;
    }
    if (ordinary_gate_mode == 0 || ordinary_gate_mode == 3)
        ordinary_gate_verified = ordinary_gate_rc == 0 && ordinary_gate_frame
            && worker->cleanup_active_count == 1 &&
            worker->current_iteration == 7
            && !worker->tdd_subpass_active && !worker->tdd_outbound_only_active
            && worker->delta_pool->slot_used == ordinary_gate_slots
            && worker->eval_arena->used == ordinary_gate_used;
    else if (ordinary_gate_mode == 1 || ordinary_gate_mode == 4) {
        ordinary_gate_verified = worker->delta_pool->slot_used ==
            ordinary_gate_slots
            && worker->eval_arena->used == ordinary_gate_used;
        for (uint32_t w = 0; w < coord->tdd_workers_count; w++)
            ordinary_gate_verified &= !coord->tdd_workers[w].cleanup_active
                && !coord->tdd_workers[w].cleanup_pending
                && coord->tdd_workers[w].cleanup_reserved_bytes == 0;
    } else
        ordinary_gate_verified = true;
}

static void
test_tdd_ordinary_frame_gates(uint32_t workers, unsigned mode)
{
    TEST("ordinary TDD frames: entry guards, admission and outbound exclusion");
    uint32_t key = 0, projection[] = { 0 };
    const char *keys[] = { "col0" };
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "output" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "output", .left_keys = keys,
          .right_keys = keys, .key_count = 1, .project_indices = projection,
          .project_count = 1 },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    if (mode >= 2) {
        ops[0].relation_name = "input";
        ops[1] = (wl_plan_op_t){ .op = WL_PLAN_OP_FILTER,
                                 .filter_expr = { predicate, 2 } };
    }
    wl_plan_relation_t relation = { .name = "output", .delta_name = "$d$output",
                                    .ops = ops, .op_count = 6 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL;
    int64_t *values = NULL;
    const char *failure = NULL;
    tuple_collector_t tuples = { 0 };
    ordinary_gate_mode = mode;
    ordinary_gate_calls = ordinary_gate_workers = 0;
    ordinary_gate_verified = ordinary_gate_budget_changed = false;
    ordinary_gate_rc = 0;
    ordinary_gate_frame = NULL;
#define TDD_GATE_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    TDD_GATE_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    values = malloc(65536 * sizeof(*values));
    TDD_GATE_CHECK(values, "input allocation");
    for (uint32_t i = 0; i < 65536; i++) values[i] = 42;
    TDD_GATE_CHECK(wl_session_insert(session, "input", values, 65536, 1) == 0,
        "input");
    if (mode < 2) {
        wirelog_column_type_t type = WIRELOG_TYPE_INT64;
        unowned = col_rel_new_auto("output", 1);
        TDD_GATE_CHECK(unowned && col_rel_set_column_types(unowned, &type,
            1) == 0
            && session_add_rel(sess, unowned) == 0, "typed empty IDB");
        unowned = NULL;
    }
    wl_columnar_eval_serial_test_after_plan = count_ordinary_worker_plan;
    wl_columnar_eval_test_subpass_boundary = ordinary_worker_gate_boundary;
    int rc = wl_session_snapshot(session, collect_tuple, &tuples);
    wl_columnar_eval_test_subpass_boundary = NULL;
    wl_columnar_eval_serial_test_after_plan = NULL;
    TDD_GATE_CHECK(ordinary_gate_verified && ordinary_gate_workers == workers
        && ordinary_gate_calls == 0 && sess->tdd_executed_strata == 1,
        "actual dispatch bypassed gate or outbound exclusion");
    if (mode != 2) {
        TDD_GATE_CHECK(rc == ((mode == 0 || mode == 3) ? EBUSY : ENOSPC) &&
            tuples.count == 0,
            "failure not propagated before output");
        if (ordinary_gate_frame)
            TDD_GATE_CHECK(wl_columnar_eval_stack_cleanup_finish(
                    &ordinary_gate_frame) == 0,
                "release active frame");
        rc = wl_session_snapshot(session, collect_tuple, &tuples);
    }
    TDD_GATE_CHECK(rc == 0 && tuples.count == 1 && tuples.rows[0][0] == 42
        && strcmp(tuples.relations[0],
        "output") == 0 && sess->tdd_workers_count == 0,
        "exact admitted snapshot retry");
cleanup:
    wl_columnar_eval_test_subpass_boundary = NULL;
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (ordinary_gate_budget_changed) {
        ordinary_gate_budget->mode = ordinary_gate_saved_mode;
        atomic_store_explicit(&ordinary_gate_budget->usable_bytes,
            ordinary_gate_saved_limit, memory_order_relaxed);
        ordinary_gate_budget_changed = false;
    }
    if (ordinary_gate_frame)
        (void)wl_columnar_eval_stack_cleanup_finish(&ordinary_gate_frame);
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef TDD_GATE_CHECK
}

static void
test_outbound_publisher_ownership(unsigned mode)
{
    TEST("outbound publisher: refusal retains owner and retry transfers once");
    wl_plan_t plan = { 0 };
    wl_session_t *session = NULL;
    wl_col_session_t worker = { 0 };
    bool initialized = false, budget_changed = false;
    col_rel_t *candidate = NULL, *alias = NULL;
    col_rel_t *slots[1] = { NULL };
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_memory_governor_t *budget = NULL;
    wl_columnar_memory_mode_t saved_mode = 0;
    uint64_t saved_limit = 0;
    const char *failure = NULL;
    int64_t value = 42;
#define PUB_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    PUB_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &session) == 0,
        "create");
    wl_col_session_t *coord = COL_SESSION(session);
    PUB_CHECK(col_worker_session_create(coord, 0, NULL, 0, &worker) == 0,
        "worker");
    initialized = true;
    col_eval_tdd_worker_ctx_t ctx = { .worker_sess = &worker,
                                      .delta_rels = slots, .stratum_idx = 3 };
    candidate = col_rel_new_auto("$d$output", 1);
    PUB_CHECK(candidate && col_rel_attach_memory_governor(candidate,
        coord->memory_governor) == 0, "candidate governor");
    if (mode != 0)
        PUB_CHECK(col_rel_append_row(candidate, &value) == 0, "candidate row");
    if (mode == 2 || mode == 3)
        PUB_CHECK(col_rel_enable_timestamps(candidate) == 0, "timestamps");
    col_rel_t *original = candidate;
    col_delta_timestamp_t *timestamps = candidate->timestamps;
    uint64_t generation = candidate->storage_generation;
    if (mode <= 2)
        PUB_CHECK(col_rel_source_reader_acquire(candidate, &reader) == 0,
            "held reader");
    if (mode == 3) {
        alias = col_rel_new_auto("alias", 1);
        PUB_CHECK(alias && col_rel_install_shared_view(alias, candidate) == 0,
            "alias");
    }
    if (mode == 4) {
        coord->delta_queue = wl_mpsc_queue_create(1, 2);
        PUB_CHECK(coord->delta_queue, "queue");
        for (unsigned i = 0; i < 2; i++) {
            col_rel_t *prefix = col_rel_new_auto("prefix", 1);
            PUB_CHECK(prefix && col_rel_append_row(prefix, &value) == 0,
                "prefix");
            int rc = wl_columnar_eval_tdd_queue_publish_delta(&ctx, &worker,
                    &prefix, 0, 4);
            if (rc != 0) col_rel_destroy(prefix);
            PUB_CHECK(rc == 0 && !prefix, "prefix transfer");
        }
    }
#ifdef WL_TEST_ALLOC_WRAP
    if (mode == 5)
        fail_next_alloc = true;
#endif
    if (mode == 6) {
        budget = wl_columnar_memory_governor_ref_get(coord->memory_governor);
        saved_mode = budget->mode;
        saved_limit = atomic_load_explicit(&budget->usable_bytes,
                memory_order_relaxed);
        budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
        atomic_store_explicit(&budget->usable_bytes, 0, memory_order_relaxed);
        budget_changed = true;
    }
    int rc = wl_columnar_eval_tdd_queue_publish_delta(&ctx, &worker,
            &candidate, 0, 7);
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (budget_changed) {
        budget->mode = saved_mode;
        atomic_store_explicit(&budget->usable_bytes, saved_limit,
            memory_order_relaxed);
        budget_changed = false;
    }
    PUB_CHECK(rc != 0 && candidate == original && !slots[0],
        "refusal lost candidate ownership");
    if (mode <= 3)
        PUB_CHECK(rc == EBUSY && candidate->timestamps == timestamps
            && candidate->storage_generation == generation,
            "reader/alias refusal mutated candidate");
    if (reader.owner)
        PUB_CHECK(col_rel_source_reader_release(&reader) == 0, "release");
    col_rel_destroy(alias);
    alias = NULL;
    if (coord->delta_queue) {
        wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(
            coord->delta_queue,
            &coord->mem_ledger);
        wl_mpsc_queue_destroy(coord->delta_queue);
        coord->delta_queue = NULL;
        wl_mem_ledger_snapshot_t ledger;
        wl_mem_ledger_snapshot(&coord->mem_ledger, &ledger);
        PUB_CHECK(ledger.subsys_bytes[WL_MEM_SUBSYS_CHANNEL] == 0,
            "prefix accounting leaked");
    }
    PUB_CHECK(wl_columnar_eval_tdd_queue_publish_delta(&ctx, &worker,
        &candidate, 0, 7) == 0 && !candidate, "retry transfer");
    PUB_CHECK(mode == 0 ? slots[0] == NULL : slots[0] == original,
        "matrix ownership");
    if (mode != 0)
        PUB_CHECK(slots[0]->nrows == 1 && col_rel_get(slots[0], 0, 0) == 42
            && slots[0]->timestamps[0].iteration == 7
            && slots[0]->timestamps[0].stratum == 3
            && slots[0]->timestamps[0].multiplicity == 1,
            "provenance or exact row");
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (budget_changed) {
        budget->mode = saved_mode;
        atomic_store_explicit(&budget->usable_bytes, saved_limit,
            memory_order_relaxed);
    }
    if (reader.owner) (void)col_rel_source_reader_release(&reader);
    col_rel_destroy(alias);
    col_rel_destroy(candidate);
    col_rel_destroy(slots[0]);
    if (session && COL_SESSION(session)->delta_queue) {
        wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(
            COL_SESSION(session)->delta_queue,
            &COL_SESSION(session)->mem_ledger);
        wl_mpsc_queue_destroy(COL_SESSION(session)->delta_queue);
        COL_SESSION(session)->delta_queue = NULL;
    }
    if (initialized && col_worker_session_destroy(&worker) != 0)
        failure = "worker teardown";
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef PUB_CHECK
}

static void
test_worker_frame_admission(bool recursive)
{
    TEST("worker frame: active parent and admission gate operator execution");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_relation_t relation = { .name = "output", .delta_name = "$d$output",
                                    .ops = ops, .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1,
                                  .is_recursive = recursive };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *worker = NULL;
    bool initialized = false;
    col_rel_t *unowned = NULL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    const char *failure = NULL;
#define ADMISSION_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    ADMISSION_CHECK(wl_session_create(wl_backend_columnar(), &plan, 2,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(sess->memory_governor);
    uint64_t baseline = wl_columnar_memory_reserved(budget);
    worker = calloc(1, sizeof(*worker));
    ADMISSION_CHECK(worker && col_worker_session_create(sess, 0, NULL, 0,
        worker) == 0, "worker create");
    initialized = true;
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    int64_t value = 42;
    unowned = col_rel_new_auto("input", 1);
    ADMISSION_CHECK(unowned && col_rel_set_column_types(unowned, &type, 1) == 0
        && col_rel_append_row(unowned, &value) == 0
        && session_add_rel(worker, unowned) == 0, "input");
    unowned = col_rel_new_auto("output", 1);
    ADMISSION_CHECK(unowned && col_rel_set_column_types(unowned, &type, 1) == 0
        && session_add_rel(worker, unowned) == 0, "output");
    col_rel_t *output = unowned;
    unowned = NULL;
    uint64_t generation = output->view_generation;
    worker->current_iteration = 7;
    worker->frontier_ops->reset_rule_frontier(worker, 0, worker->outer_epoch);
    ADMISSION_CHECK(wl_columnar_eval_stack_cleanup_begin(worker, &frame) == 0,
        "active frame");
    uint32_t slots = worker->delta_pool->slot_used;
    size_t used = worker->eval_arena->used;
    serial_scope_hook_calls = 0;
    wl_columnar_eval_serial_test_after_plan = count_serial_scope_hook;
    ADMISSION_CHECK(col_eval_stratum(&stratum, worker, 0) == EBUSY
        && worker->cleanup_active_count == 1 && worker->current_iteration == 7
        && worker->delta_pool->slot_used == slots &&
        worker->eval_arena->used == used
        && output->nrows == 0 && output->view_generation == generation
        && serial_scope_hook_calls == 0, "active frame did not gate entry");
    ADMISSION_CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0,
        "finish active frame");
    uint64_t saved_limit = atomic_load_explicit(&budget->usable_bytes,
            memory_order_relaxed);
    wl_columnar_memory_mode_t saved_mode = budget->mode;
    budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    atomic_store_explicit(&budget->usable_bytes,
        wl_columnar_memory_reserved(budget), memory_order_relaxed);
    int rc = col_eval_stratum(&stratum, worker, 0);
    budget->mode = saved_mode;
    atomic_store_explicit(&budget->usable_bytes, saved_limit,
        memory_order_relaxed);
    ADMISSION_CHECK(rc == ENOSPC && serial_scope_hook_calls == 0
        && !worker->cleanup_active && !worker->cleanup_pending
        && worker->cleanup_reserved_bytes == 0 &&
        worker->delta_pool->slot_used == slots
        && worker->eval_arena->used == used && output->nrows == 0
        && output->view_generation == generation,
        "admission ran plan or changed owner");
    ADMISSION_CHECK(col_eval_stratum(&stratum, worker, 0) == 0
        && serial_scope_hook_calls > 0 && output->nrows == (recursive ? 1u : 2u)
        && col_rel_get(output, 0, 0) == value && !worker->cleanup_pending
        && !worker->cleanup_active && worker->cleanup_reserved_bytes == 0,
        "exact admitted retry");
    ADMISSION_CHECK(col_worker_session_destroy(worker) == 0, "worker destroy");
    initialized = false;
    ADMISSION_CHECK(wl_columnar_memory_reserved(budget) == baseline,
        "worker admission leaked reservation");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (frame)
        (void)wl_columnar_eval_stack_cleanup_finish(&frame);
    col_rel_destroy(unowned);
    if (initialized)
        (void)col_worker_session_destroy(worker);
    free(worker);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef ADMISSION_CHECK
}

static void
test_recursive_cleanup_scope(unsigned mode)
{
    TEST(
        "recursive frame scope includes coordinator fallback and workers");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relation = { .name = "output", .delta_name = "$d$output",
                                    .ops = &op, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *worker = NULL;
    bool worker_initialized = false;
    const char *failure = NULL;
    delta_collector_t deltas = { 0 };
    int64_t value = 42;
#define SCOPE_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    SCOPE_CHECK(wl_session_create(wl_backend_columnar(), &plan,
        mode == 2 ? 2 : 1, &session) == 0
        && wl_session_insert(session, "input", &value, 1, 1) == 0, "fixture");
    wl_col_session_t *sess = COL_SESSION(session);
    wl_col_session_t *owner = sess;
    if (mode == 1) {
        worker = calloc(1, sizeof(*worker));
        SCOPE_CHECK(worker && col_worker_session_create(sess, 0, NULL, 0,
            worker) == 0, "worker create");
        worker_initialized = true;
        col_rel_t *partition = col_rel_new_auto("input", 1);
        SCOPE_CHECK(partition, "worker partition allocation");
        int partition_rc = col_rel_append_row(partition, &value);
        if (partition_rc == 0)
            partition_rc = session_add_rel(worker, partition);
        if (partition_rc != 0)
            col_rel_destroy(partition);
        SCOPE_CHECK(partition_rc == 0, "worker partition");
        owner = worker;
    }
    serial_scope_hook_calls = 0;
    wl_columnar_eval_serial_test_after_plan = count_serial_scope_hook;
    int rc;
    if (mode == 0) {
        wl_session_set_delta_cb(session, collect_delta, &deltas);
        rc = wl_session_step(session);
    } else {
        rc = col_eval_stratum(&stratum, owner, 0);
    }
    wl_columnar_eval_serial_test_after_plan = NULL;
    SCOPE_CHECK(rc == 0 && serial_scope_hook_calls > 0
        && !owner->cleanup_active && !owner->cleanup_pending,
        "caller did not complete framed evaluation");
    col_rel_t *output = session_find_rel(owner, "output");
    SCOPE_CHECK(output && output->nrows == 1
        && col_rel_get(output, 0, 0) == value,
        "framed path result changed");
    SCOPE_CHECK(mode != 0 || (deltas.count == 1
        && has_delta(&deltas, "output", &value, 1, +1)), "delta callback");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (worker_initialized && col_worker_session_destroy(worker) != 0)
        failure = "worker destroy";
    free(worker);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef SCOPE_CHECK
}

extern void (*wl_columnar_eval_delta_test_observer_boundary)(wl_col_session_t *,
    unsigned);
static unsigned observer_seen_plans;
static wl_columnar_source_access_reader_t observer_result_reader;

static void
observer_hold_second_result(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    /* A recursive stratum may execute several plans before the next stratum.
     * Stop at the next first-pass rule, not A's second subpass. */
    if (++observer_seen_plans < 2 || sess->current_iteration != 0)
        return;
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (result->owned && result->rel)
        (void)col_rel_source_reader_acquire(result->rel,
            &observer_result_reader);
}

static void
test_observer_retry(bool separate_strata, bool recursive, unsigned cancel,
    bool destroy_pending)
{
    TEST(
        "observer: whole-step retry preserves signed notifications and ownership");
    wl_plan_op_t a_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_op_t b_ops[3];
    memcpy(b_ops, a_ops, sizeof(b_ops));
    wl_plan_relation_t relations[] = {
        { .name = "A", .delta_name = "$d$A", .ops = a_ops,
          .op_count = 3 },
        { .name = "B", .delta_name = "$d$B", .ops = b_ops,
          .op_count = 3 }
    };
    wl_plan_stratum_t strata[] = {
        { .relations = relations, .relation_count = separate_strata ? 1 : 2,
          .is_recursive = recursive },
        { .relations = &relations[1], .relation_count = 1,
          .is_recursive = recursive }
    };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = strata,
                       .stratum_count = separate_strata ? 2 : 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    delta_collector_t deltas = { 0 }, replacement = { 0 };
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
    int64_t initial[] = { 300, -2, 1 }, add = 2, remove = 300, later = 9;
#define OBS_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    memset(&observer_result_reader, 0, sizeof(observer_result_reader));
    OBS_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &session) == 0,
        "create");
    sess = COL_SESSION(session);
    governor = sess->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    OBS_CHECK(wl_session_insert(session, "input", initial, 3, 1) == 0
        && wl_session_step(session) == 0 && deltas.count == 6, "baseline");
    deltas.count = 0;
    OBS_CHECK(wl_session_insert(session, "input", &add, 1, 1) == 0
        && wl_session_remove(session, "input", &remove, 1, 1) == 0,
        "mixed input changes");
    observer_seen_plans = 0;
    wl_columnar_eval_serial_test_after_plan = observer_hold_second_result;
    int expected = EBUSY;
    OBS_CHECK(wl_session_step(session) == expected && deltas.count == 0
        && sess->delta_observer &&
        !wl_columnar_eval_delta_observer_active(sess),
        "failed step lost observer baseline");
    OBS_CHECK(observer_result_reader.owner && sess->cleanup_pending_count == 1,
        "actual result refusal missing");
    col_rel_t *prefix = session_find_rel(sess, "A");
    col_rel_t *pending = session_find_rel(sess, "B");
    bool prefix_added = false, prefix_removed = false, pending_added = false;
    OBS_CHECK(prefix && pending && (!separate_strata || prefix->nrows == 3),
        "completed prefix stratum");
    for (uint32_t row = 0; row < prefix->nrows; row++) {
        prefix_added |= col_rel_get(prefix, row, 0) == add;
        prefix_removed |= col_rel_get(prefix, row, 0) == remove;
    }
    for (uint32_t row = 0; row < pending->nrows; row++)
        pending_added |= col_rel_get(pending, row, 0) == add;
    OBS_CHECK(prefix_added && !prefix_removed && !pending_added,
        "refusal did not occur after completed prefix output");
    uint64_t observer_bytes = sess->delta_observer_reserved_bytes;
    uint32_t epoch = sess->outer_epoch, nrels = sess->nrels;
    OBS_CHECK(wl_session_insert(session, "input", &later, 1, 1) == EBUSY
        && wl_session_remove(session, "input", &add, 1, 1) == EBUSY
        && wl_session_snapshot(session, collect_tuple, &tuples) == EBUSY
        && wl_session_insert(session, "input", &later, 0, 1) == 0
        && wl_session_remove(session, "input", &later, 0, 1) == 0,
        "pending input/snapshot guards");
    wirelog_compound_arg_t arg = { WIRELOG_TYPE_INT64, 1 };
    uint64_t handle = 123;
    OBS_CHECK(wl_session_make_compound(session, "blocked", 1, &arg, &handle)
        == EBUSY && handle == WIRELOG_COMPOUND_HANDLE_NULL
        && sess->outer_epoch == epoch && sess->nrels == nrels,
        "compound input bypassed observer guard");
    for (unsigned retry = 0; retry < 2; retry++)
        OBS_CHECK(wl_session_step(session) == expected && deltas.count == 0
            && sess->delta_observer_reserved_bytes == observer_bytes,
            "repeated refusal changed baseline");
    if (cancel) {
        wl_session_set_delta_cb(session, NULL, NULL);
        if (cancel == 1)
            wl_session_set_delta_cb(session, collect_delta, &replacement);
    }
    if (observer_result_reader.owner)
        OBS_CHECK(col_rel_source_reader_release(&observer_result_reader) == 0,
            "reader release");
    b_ops[1].relation_name = "input";
    if (destroy_pending)
        goto cleanup;
    OBS_CHECK(wl_session_step(session) == 0 && !sess->delta_observer
        && !sess->delta_rollback && !sess->cleanup_pending,
        "retry completion");
    for (unsigned i = 0; i < 2; i++) {
        col_rel_t *rel = session_find_rel(sess, relations[i].name);
        OBS_CHECK(rel && rel->nrows == 3, "exact final row count");
        if (!cancel)
            OBS_CHECK(has_delta(&deltas, relations[i].name, &add, 1, +1)
                && has_delta(&deltas, relations[i].name, &remove, 1, -1),
                "missing signed notification after retry");
    }
    OBS_CHECK(deltas.count == (cancel ? 0 : 4) && replacement.count == 0,
        "duplicate or cancelled notifications");
    OBS_CHECK(wl_session_step(session) == 0
        && deltas.count == (cancel ? 0 : 4) && replacement.count == 0,
        "no-change retry duplicated events");
    if (cancel) {
        wl_session_set_delta_cb(session, collect_delta, &replacement);
        OBS_CHECK(wl_session_insert(session, "input", &later, 1, 1) == 0
            && wl_session_step(session) == 0 && replacement.count == 2
            && has_delta(&replacement, "A", &later, 1, +1)
            && has_delta(&replacement, "B", &later, 1, +1),
            "replacement did not observe subsequent transaction");
    }
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (observer_result_reader.owner)
        (void)col_rel_source_reader_release(&observer_result_reader);
    wl_session_destroy(session);
    if (governor) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) != 0)
            failure = "observer teardown reservation leak";
        wl_columnar_memory_governor_ref_release(governor);
    }
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef OBS_CHECK
}

extern void (*wl_columnar_eval_delta_test_consolidation_boundary)(
    wl_col_session_t *, col_rel_t *, col_rel_t *, unsigned);
static unsigned idb_failure_mode;
static bool idb_timestamped;
static int idb_hook_rc;
static wl_columnar_source_access_reader_t idb_reader;
static col_rel_t *idb_held;
static uint64_t idb_saved_limit;
static wl_columnar_memory_mode_t idb_saved_mode;
static bool idb_budget_changed;
static int64_t **idb_target_columns;
static uint64_t idb_target_view, idb_target_storage;
static col_delta_timestamp_t idb_timestamp(uint32_t row)
{
    return (col_delta_timestamp_t){ .iteration = row + 10, .stratum = row + 20,
                                    .worker = row + 30,
                                    .multiplicity = row %
                                        2 ? -(int64_t)(row + 1)
        : (int64_t)(row + 1) };
}

static void
idb_set_budget(wl_col_session_t *sess)
{
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(sess->memory_governor);
    idb_saved_limit = atomic_load_explicit(&budget->usable_bytes,
            memory_order_relaxed);
    idb_saved_mode = budget->mode;
    budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    atomic_store_explicit(&budget->usable_bytes,
        wl_columnar_memory_reserved(budget), memory_order_relaxed);
    idb_budget_changed = true;
}

static void
idb_restore_budget(wl_col_session_t *sess)
{
    if (!idb_budget_changed)
        return;
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(sess->memory_governor);
    budget->mode = idb_saved_mode;
    atomic_store_explicit(&budget->usable_bytes, idb_saved_limit,
        memory_order_relaxed);
    idb_budget_changed = false;
}

static void
idb_after_eval(wl_col_session_t *sess)
{
    col_rel_t *target = session_find_rel(sess, "output");
    idb_hook_rc = EINVAL;
    if (!target || target->nrows != 4)
        return;
    if (idb_timestamped) {
        if (col_rel_enable_timestamps(target) != 0)
            return;
        for (uint32_t row = 0; row < target->nrows; row++)
            target->timestamps[row] = idb_timestamp(row);
        if (!target->compound_arity_map) {
            target->compound_arity_map = malloc(sizeof(uint32_t));
            if (!target->compound_arity_map)
                return;
        }
        target->compound_arity_map[0] = 1;
        target->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        target->compound_count = 1;
    }
    idb_target_columns = target->columns;
    idb_target_view = target->view_generation;
    idb_target_storage = target->storage_generation;
    if (idb_failure_mode == 7)
        idb_set_budget(sess);
#ifdef WL_TEST_ALLOC_WRAP
    if (idb_failure_mode == 6)
        fail_calloc_size = sizeof(col_rel_t);
#endif
    idb_hook_rc = 0;
}

static void
idb_consolidation_boundary(wl_col_session_t *sess, col_rel_t *target,
    col_rel_t *candidate, unsigned boundary)
{
    if ((idb_failure_mode == 0 && boundary == 2)
        || (idb_failure_mode == 1 && boundary == 3)
        || (idb_failure_mode == 10 && boundary == 1)) {
        idb_held = idb_failure_mode == 0 ? target : candidate;
        idb_hook_rc = col_rel_source_reader_acquire(idb_held, &idb_reader);
    } else if (idb_failure_mode == 2 && boundary == 2) {
        wl_columnar_relation_touch_replacement(target);
        idb_target_view = target->view_generation;
        idb_target_storage = target->storage_generation;
    } else if (idb_failure_mode == 3 && boundary == 1) {
        idb_set_budget(sess);
#ifdef WL_TEST_ALLOC_WRAP
    } else if ((idb_failure_mode == 4 && boundary == 1)
        || (idb_failure_mode == 5 && boundary == 2)) {
        fail_next_alloc = true;
#endif
    } else {
        return;
    }
    wl_columnar_eval_delta_test_consolidation_boundary = NULL;
}

extern void (*wl_columnar_eval_test_before_final_normalize)(
    wl_col_session_t *, col_rel_t *);

static unsigned final_normalize_mode;
static bool final_normalize_hit;
static int final_normalize_hook_rc;
static col_rel_t *final_normalize_target, *final_normalize_alias;
static wl_columnar_source_access_reader_t final_normalize_reader;
static uint64_t final_normalize_view, final_normalize_storage;
static int64_t **final_normalize_columns;
static col_delta_timestamp_t final_normalize_timestamps[3];
static int64_t final_normalize_values[3];
static col_frontier_2d_t final_normalize_frontier, final_normalize_rule;
static uint32_t final_normalize_iterations;

static bool later_completion_hit;
static col_rel_t *later_completion_target;
static wl_columnar_source_access_reader_t later_completion_reader;

static void
inject_later_completion_refusal(wl_col_session_t *coord, col_rel_t *target)
{
    if (later_completion_hit || strcmp(target->name, "later") != 0)
        return;
    later_completion_hit = true;
    later_completion_target = target;
    wl_columnar_eval_test_before_final_normalize = NULL;
    (void)col_rel_source_reader_acquire(target, &later_completion_reader);
    (void)coord;
}

static void
test_later_stratum_completion_retry(uint32_t workers)
{
    TEST("plain STEP later-stratum completion resumes without replay");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    uint32_t key = 0;
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t first_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_CONSOLIDATE },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_op_t later_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_CONSOLIDATE },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t first = { .name = "output", .delta_name = "$d$output",
                                 .ops = first_ops, .op_count = 4 };
    wl_plan_relation_t later = { .name = "later", .delta_name = "$d$later",
                                 .ops = later_ops, .op_count = 4 };
    wl_plan_stratum_t strata[] = {
        { .relations = &first, .relation_count = 1, .is_recursive = false },
        { .relations = &later, .relation_count = 1, .is_recursive = false }
    };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = strata, .stratum_count = 2,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    int64_t *values = NULL;
    uint64_t tuple_count = 0;
    uint64_t delta_count = 0;
    wl_columnar_source_access_reader_t compaction_reader = { 0 };
    const char *failure = NULL;
#define LATER_CHECK(c, m) do { if (!(c)) { failure = (m); goto later_cleanup; \
                               } } while (0)
    later_completion_hit = false;
    later_completion_target = NULL;
    memset(&later_completion_reader, 0, sizeof(later_completion_reader));
    LATER_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "later session");
    values = malloc(65536 * sizeof(*values));
    LATER_CHECK(values, "later values");
    for (uint32_t i = 0; i < 65536; i++)
        values[i] = i & 1 ? 42 : 7;
    LATER_CHECK(wl_session_insert(session, "input", values, 65536, 1) == 0,
        "later input");
    wl_columnar_eval_test_before_final_normalize =
        inject_later_completion_refusal;
    int later_rc = wl_session_step(session);
    LATER_CHECK(later_rc == EBUSY && later_completion_hit, "later refusal");
    int64_t blocked = 9;
    LATER_CHECK(wl_session_insert(session, "input", &blocked, 1, 1) == EBUSY,
        "later mutation guard");
    LATER_CHECK(wl_session_snapshot(session, count_tuple_only, &tuple_count)
        == EBUSY && tuple_count == 0, "later repeated refusal");
    LATER_CHECK(col_rel_source_reader_release(&later_completion_reader) == 0,
        "later release");
    wl_col_session_t *pre_retry_coord = COL_SESSION(session);
    col_rel_t *pre_retry_output = session_find_rel(pre_retry_coord, "output");
    LATER_CHECK(pre_retry_output
        && col_rel_source_reader_acquire(pre_retry_output,
        &compaction_reader) == 0, "later compaction refusal setup");
    tuple_count = 0;
    wl_session_set_delta_cb(session, count_delta_tuple_only, &delta_count);
    LATER_CHECK(wl_session_snapshot(session, count_tuple_only, &tuple_count)
        == EBUSY && tuple_count == 0,
        "later compaction refusal");
    LATER_CHECK(COL_SESSION(session)->plain_step_completion_pending
        && !COL_SESSION(session)->plain_step_completion_active,
        "later compaction retry remains bounded");
    LATER_CHECK(col_rel_source_reader_release(&compaction_reader) == 0,
        "later compaction release");
    tuple_count = 0;
    LATER_CHECK(wl_session_snapshot(session, count_tuple_only, &tuple_count)
        == 0,
        "later callback-enabled retry");
    LATER_CHECK(!COL_SESSION(session)->plain_step_completion_pending
        && !COL_SESSION(session)->plain_step_completion_active,
        "later callback retry cleared completion");
    LATER_CHECK(tuple_count == 4, "later exact callback count");
    LATER_CHECK(delta_count == 0, "later callback emitted no deferred deltas");
    wl_session_set_delta_cb(session, NULL, NULL);
    tuple_count = 0;
    wl_col_session_t *later_coord = COL_SESSION(session);
    col_rel_t *later_output = session_find_rel(later_coord, "output");
    col_rel_t *later_relation = session_find_rel(later_coord, "later");
    LATER_CHECK(later_output && later_relation
        && later_output->nrows == 2 && later_relation->nrows == 2,
        "later exact stored rows");
    LATER_CHECK(later_output->columns[0][0] == 7
        && later_output->columns[0][1] == 42
        && later_relation->columns[0][0] == 7
        && later_relation->columns[0][1] == 42,
        "later exact stored values");
later_cleanup:
    wl_columnar_eval_test_before_final_normalize = NULL;
    if (later_completion_reader.owner)
        (void)col_rel_source_reader_release(&later_completion_reader);
    if (compaction_reader.owner)
        (void)col_rel_source_reader_release(&compaction_reader);
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef LATER_CHECK
}

static void
inject_final_normalization(wl_col_session_t *coord, col_rel_t *target)
{
    if (final_normalize_hit || coord->coordinator ||
        strcmp(target->name, "output") != 0)
        return;
    final_normalize_hit = true;
    wl_columnar_eval_test_before_final_normalize = NULL;
    final_normalize_hook_rc = EINVAL;
    final_normalize_target = target;
    if (coord->tdd_workers_count != 0 || target->nrows < 2)
        return;
    /* Set a controlled duplicate/provenance oracle at the publication
     * boundary, after internal worker dependencies have retired. */
    if (col_rel_enable_timestamps(target) != 0)
        return;
    int64_t low = 7, high = 42;
    if (target->nrows > 2) {
        /* Specialized merging can still have one pair per worker. */
        if (wl_columnar_eval_delta_consolidate(target, coord) != 0)
            return;
    }
    if (target->nrows != 2 || col_rel_set(target, 0, 0, low) != 0 ||
        col_rel_set(target, 1, 0, high) != 0)
        return;
    target->timestamps[0] = idb_timestamp(0);
    target->timestamps[1] = idb_timestamp(1);
    if (col_rel_append_row(target, &low) != 0)
        return;
    target->timestamps[2] = idb_timestamp(2);
    final_normalize_columns = target->columns;
    final_normalize_view = target->view_generation;
    final_normalize_storage = target->storage_generation;
    final_normalize_frontier = coord->frontiers[0];
    final_normalize_rule = coord->rule_frontiers[0];
    final_normalize_iterations = coord->total_iterations;
    memcpy(final_normalize_timestamps, target->timestamps,
        sizeof(final_normalize_timestamps));
    for (unsigned row = 0; row < 3; row++)
        final_normalize_values[row] = target->columns[0][row];
    if (final_normalize_mode == 1 || final_normalize_mode == 2) {
        final_normalize_alias = col_rel_new_like("external_alias", target);
        if (!final_normalize_alias ||
            col_rel_install_shared_view(final_normalize_alias, target) != 0)
            return;
    }
    if (final_normalize_mode <= 1)
        final_normalize_hook_rc = col_rel_source_reader_acquire(
            final_normalize_mode ? final_normalize_alias : target,
            &final_normalize_reader);
    else
        final_normalize_hook_rc = 0;
#ifdef WL_TEST_ALLOC_WRAP
    if (final_normalize_mode == 3)
        fail_next_alloc = true;
#endif
    if (final_normalize_mode == 4)
        idb_set_budget(coord);
}

extern int (*wl_columnar_eval_test_global_publication)(wl_col_session_t *,
    uint32_t, unsigned);
static unsigned global_publication_mode, global_publication_relation;
static bool global_publication_hit;
static int global_publication_hook_rc;
static col_rel_t *global_publication_held, *global_publication_alias;
static wl_columnar_source_access_reader_t global_publication_reader;
static uint64_t global_publication_view, global_publication_storage;
static uint32_t global_publication_rows;
static wl_atomic_u64 global_publication_workers;

static void
global_publication_worker(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    (void)result;
    if (worker->tdd_outbound_only_active && worker->diff_operators_active)
        record_actual_worker(&global_publication_workers, worker->worker_id);
}

static int
global_publication_boundary(wl_col_session_t *coord, uint32_t ri,
    unsigned phase)
{
    if (global_publication_hit || ri != global_publication_relation)
        return 0;
    unsigned wanted = global_publication_mode == 9 ? 6
        : global_publication_mode == 4 ? 0
        : global_publication_mode == 5 ? 1
        : global_publication_mode == 6 ? 3
        : global_publication_mode == 7 ? 4 : 1;
    if (phase != wanted)
        return 0;
    global_publication_hit = true;
    global_publication_hook_rc = 0;
    if (global_publication_mode == 0)
        return 0;
    if (global_publication_mode == 8) {
        idb_set_budget(coord);
        return 0;
    }
    if (global_publication_mode == 6 || global_publication_mode == 7)
        return ENOMEM;
#ifdef WL_TEST_ALLOC_WRAP
    if (global_publication_mode == 4 || global_publication_mode == 5
        || global_publication_mode == 9) {
        fail_next_alloc = true;
        return 0;
    }
#endif
    const char *name = ri == 0 ? "output" : "relay";
    col_rel_t *target = session_find_rel(coord, name);
    if (global_publication_mode == 3)
        target = session_find_rel(&coord->tdd_workers[coord->tdd_workers_count -
                1],
                name);
    if (global_publication_mode == 2) {
        global_publication_alias = col_rel_new_like("external", target);
        if (!global_publication_alias) return ENOMEM;
        int rc = col_rel_install_shared_view(global_publication_alias, target);
        if (rc != 0) return rc;
        target = global_publication_alias;
    }
    global_publication_held = target;
    global_publication_rows = target->nrows;
    global_publication_view = target->view_generation;
    global_publication_storage = target->storage_generation;
    global_publication_hook_rc = col_rel_source_reader_acquire(target,
            &global_publication_reader);
    return global_publication_hook_rc;
}

static void
test_global_read_publication(uint32_t workers, unsigned initial,
    unsigned mode, uint32_t target_relation, bool later)
{
    TEST("global-read publication: ownership, prefix failure and exact retry");
    uint32_t key = 0, projection[] = { 0, 1 };
    const char *keys[] = { "col1" };
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t output_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "output" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "input", .left_keys = keys,
          .right_keys = keys, .key_count = 1, .project_indices = projection,
          .project_count = 2 },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_op_t relay_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE,
          .relation_name = later ? "output" : "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t relations[] = {
        { .name = "output", .delta_name = "$d$output", .ops = output_ops,
          .op_count = 5 },
        { .name = "relay", .delta_name = "$d$relay", .ops = relay_ops,
          .op_count = 3 }
    };
    wl_plan_stratum_t stratum = { .relations = relations, .relation_count = 2,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    int64_t *values = NULL;
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
    global_publication_mode = mode;
    global_publication_relation = target_relation;
    global_publication_hit = false;
    global_publication_held = global_publication_alias = NULL;
    memset(&global_publication_reader, 0, sizeof(global_publication_reader));
    atomic_store_explicit(&global_publication_workers, 0, memory_order_relaxed);
#define GLOBAL_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    GLOBAL_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "global session");
    wl_col_session_t *coord = COL_SESSION(session);
    governor = coord->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    uint32_t input_rows = mode == 0 && initial == 0 ? 65536 : workers * 4096;
    values = malloc((size_t)input_rows * 2 * sizeof(*values));
    GLOBAL_CHECK(values, "global input allocation");
    for (uint32_t i = 0; i < input_rows * 2; i++) values[i] = 42;
    GLOBAL_CHECK(wl_session_insert(session, "input", values, input_rows,
        2) == 0,
        "global input");
    wirelog_column_type_t types[] = { WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64 };
    if (initial != 0) {
        for (uint32_t ri = 0; ri < 2; ri++) {
            unowned = col_rel_new_auto(relations[ri].name, 2);
            GLOBAL_CHECK(unowned && col_rel_set_column_types(unowned, types,
                2) == 0
                && col_rel_enable_timestamps(unowned) == 0,
                "global typed target");
            if (initial == 2) {
                int64_t seed[] = { 7, 7 };
                GLOBAL_CHECK(col_rel_append_row(unowned, seed) == 0,
                    "global populated seed");
                unowned->timestamps[0] = idb_timestamp(0);
            }
            GLOBAL_CHECK(session_add_rel(coord, unowned) == 0, "global target");
            unowned = NULL;
        }
    }
    wl_columnar_eval_test_outbound_after_plan = global_publication_worker;
    wl_columnar_eval_test_global_publication = global_publication_boundary;
    int rc = wl_session_snapshot(session, collect_tuple, &tuples);
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    idb_restore_budget(coord);
    GLOBAL_CHECK(global_publication_hit && global_publication_hook_rc == 0
        && atomic_load_explicit(&global_publication_workers,
        memory_order_relaxed) == (1u << workers) - 1,
        "actual global-read boundary and workers");
    if (mode != 0) {
        GLOBAL_CHECK(rc == (mode <= 3 ? EBUSY : mode == 8 ? ENOSPC : ENOMEM) &&
            tuples.count == 0,
            "global publication failure status or callbacks");
        GLOBAL_CHECK(coord->mem_channel_ring_bytes == 0 &&
            coord->delta_queue == NULL,
            "global failed channel ownership");
        if (mode <= 3) {
            for (unsigned retry = 0; retry < 2; retry++) {
                GLOBAL_CHECK(global_publication_held->nrows ==
                    global_publication_rows
                    && global_publication_held->view_generation ==
                    global_publication_view
                    && global_publication_held->storage_generation ==
                    global_publication_storage,
                    "held global relation changed");
                GLOBAL_CHECK(wl_session_snapshot(session, collect_tuple,
                    &tuples) == EBUSY
                    && tuples.count == 0, "repeated global refusal");
            }
            GLOBAL_CHECK(col_rel_source_reader_release(
                    &global_publication_reader) == 0,
                "global reader release");
            col_rel_destroy(global_publication_alias);
            global_publication_alias = NULL;
        }
        wl_columnar_eval_test_global_publication = NULL;
        GLOBAL_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0,
            "global exact public retry");
    } else GLOBAL_CHECK(rc == 0, "global healthy snapshot");
    unsigned expected = initial == 2 ? 4 : 2;
    GLOBAL_CHECK(tuples.count == expected && coord->tdd_workers_count == 0,
        "global exact tuple count and teardown");
    for (uint32_t ri = 0; ri < 2; ri++) {
        col_rel_t *r = session_find_rel(coord, relations[ri].name);
        GLOBAL_CHECK(r && r->nrows == expected / 2 && r->ncols == 2
            && col_rel_get(r, r->nrows - 1, 0) == 42
            && col_rel_get(r, r->nrows - 1, 1) == 42,
            "global exact stored set");
        if (initial != 0)
            GLOBAL_CHECK(r->column_types &&
                r->column_types[0] == WIRELOG_TYPE_INT64
                && r->timestamps, "global metadata and timestamps");
        if (initial == 2)
            GLOBAL_CHECK(col_rel_get(r, 0, 0) == 7 && col_rel_get(r, 0, 1) == 7
                && r->timestamps[0].iteration == idb_timestamp(0).iteration,
                "global seed provenance");
    }
cleanup:
    wl_columnar_eval_test_global_publication = NULL;
    wl_columnar_eval_test_outbound_after_plan = NULL;
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (session) idb_restore_budget(COL_SESSION(session));
    if (global_publication_reader.owner)
        (void)col_rel_source_reader_release(&global_publication_reader);
    col_rel_destroy(global_publication_alias);
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    if (governor) {
        if (reserved_on(governor) != 0 &&
            !failure) failure = "global retained reservation leak";
        wl_columnar_memory_governor_ref_release(governor);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef GLOBAL_CHECK
}

extern void (*wl_columnar_eval_test_nonrec_worker)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *, int *);
extern int (*wl_columnar_eval_test_nonrec_boundary)(wl_col_session_t *,
    unsigned, uint32_t, col_rel_t *);
static unsigned specialized_mode, specialized_worker;
static wl_atomic_u64 specialized_workers;
static bool specialized_injected;
static int specialized_hook_rc;
static wl_columnar_source_access_reader_t specialized_reader;
static col_rel_t *specialized_alias, *specialized_held;
static eval_entry_t *specialized_entry;
static col_arr_entry_t specialized_arrangement;
static bool specialized_arrangement_captured;
static wl_columnar_eval_stack_cleanup_frame_t *specialized_active;

static void
specialized_worker_boundary(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result, int *rc)
{
    record_actual_worker(&specialized_workers, worker->worker_id);
    /* FILTER loses upstream timestamps (#1715). Stamp this owned post-plan
     * fixture explicitly to test only the new staging/publication boundary.
     * VARIABLE-only cases below preserve real input timestamps end to end. */
    if (*rc == 0 && result->owned && result->rel && specialized_mode != 16) {
        *rc = col_rel_enable_timestamps(result->rel);
        if (*rc == 0)
            for (uint32_t row = 0; row < result->rel->nrows; row++)
                result->rel->timestamps[row] = idb_timestamp(worker->worker_id);


    }

    if (worker->worker_id != specialized_worker || specialized_injected
        || specialized_mode < 3 ||
        (specialized_mode > 8 && specialized_mode < 21))
        return;
    specialized_injected = true;
    specialized_hook_rc = 0;
    if (specialized_mode == 8) {
        *rc = ENOMEM; return;
    }
    if (!result->rel) {
        specialized_hook_rc = EINVAL; return;
    }
    if (specialized_mode == 3) {
        col_rel_t *heap = NULL;
        specialized_hook_rc = col_rel_deep_copy(result->rel, &heap, NULL);
        if (specialized_hook_rc != 0) return;
        specialized_hook_rc = eval_entry_dispose(result);
        if (specialized_hook_rc != 0) {
            col_rel_destroy(heap); return;
        }
        result->rel = heap; result->owned = true;
    } else if (specialized_mode == 22) {
        col_rel_t *arena = col_rel_pool_new_auto(worker->delta_pool,
                worker->eval_arena, "arena_result", result->rel->ncols);
        if (!arena) {
            specialized_hook_rc = ENOMEM; return;
        }
        specialized_hook_rc = col_rel_enable_timestamps(arena);
        if (specialized_hook_rc == 0)
            specialized_hook_rc = col_rel_append_all(arena, result->rel,
                    worker->eval_arena);
        if (specialized_hook_rc == 0)
            specialized_hook_rc = eval_entry_dispose(result);
        if (specialized_hook_rc != 0) {
            col_rel_destroy(arena); return;
        }
        result->rel = arena; result->owned = true;
    } else if (specialized_mode >= 4 && specialized_mode <= 6) {
        col_rel_t *lower = specialized_mode == 4 ? col_rel_new_auto("lower", 1)
            : col_rel_pool_new_auto(worker->delta_pool,
                specialized_mode == 6 ? worker->eval_arena : NULL, "lower", 1);
        int64_t value = 42;
        if (!lower || col_rel_append_row(lower, &value) != 0
            || eval_stack_push(stack, lower, true) != 0) {
            col_rel_destroy(lower); specialized_hook_rc = ENOMEM; return;
        }
        result = &stack->items[stack->top - 1];
    } else if (specialized_mode == 7) {
        specialized_hook_rc = eval_entry_dispose(result);
        if (specialized_hook_rc != 0) return;
        result->rel = session_find_rel(worker, "$nonrec$input");
        result->owned = false;
    }
    /* The specialized whitelist excludes CONCAT. These synthetic boundaries
     * exercise entry metadata ownership without claiming a native CONCAT path. */
    result->seg_boundaries = malloc(3 * sizeof(uint32_t));
    if (!result->seg_boundaries) {
        specialized_hook_rc = ENOMEM; return;
    }
    result->seg_count = 2;
    result->seg_boundaries[0] = 0;
    result->seg_boundaries[1] = result->rel->nrows;
    result->seg_boundaries[2] = result->rel->nrows;
    specialized_entry = result;
    specialized_held = result->rel;
    specialized_hook_rc =
        col_rel_source_reader_acquire_transferable(result->rel,
            &specialized_reader);
}

static int
specialized_publication_boundary(wl_col_session_t *coord, unsigned phase,
    uint32_t worker, col_rel_t *candidate)
{
    if (specialized_injected) return 0;
    unsigned wanted = specialized_mode == 9 || specialized_mode == 13 ? 0
        : specialized_mode == 10 ? 3
        : specialized_mode == 11 || specialized_mode == 15 ? 4
        : specialized_mode == 14 || specialized_mode == 19 ? 1 : 5;
    if (phase != wanted) return 0;
    if (specialized_mode == 0 ||
        (specialized_mode >= 3 && specialized_mode <= 8)
        || specialized_mode == 16 || specialized_mode == 17 ||
        specialized_mode == 18)
        return 0;
    if (specialized_mode == 14 && worker != 1) return 0;
    if (specialized_mode == 19 && worker != specialized_worker) return 0;
    specialized_injected = true;
    specialized_hook_rc = 0;
    if (specialized_mode == 14) return ENOMEM;
    if (specialized_mode == 19)
        return wl_columnar_eval_stack_cleanup_begin(&coord->tdd_workers[worker],
                   &specialized_active);
    if (specialized_mode == 13 || specialized_mode == 15) {
        idb_set_budget(coord); return 0;
    }
#ifdef WL_TEST_ALLOC_WRAP
    if (specialized_mode >= 9 && specialized_mode <= 12) {
        fail_next_alloc = true; return 0;
    }
#endif
    col_rel_t *target = specialized_mode == 20 ? candidate
        : session_find_rel(coord, "output");
    if (specialized_mode == 1 || specialized_mode == 2) {
        uint32_t key = 0;
        if (!col_session_get_arrangement(&coord->base, "output", &key, 1))
            return ENOMEM;
        specialized_arrangement = coord->arr_entries[0];
        specialized_arrangement_captured = true;
    }

    if (specialized_mode == 2) {
        specialized_alias = col_rel_new_like("external", target);
        if (!specialized_alias) return ENOMEM;
        int rc = col_rel_install_shared_view(specialized_alias, target);
        if (rc != 0) return rc;
        target = specialized_alias;
    }
    specialized_held = target;
    specialized_hook_rc = col_rel_source_reader_acquire(target,
            &specialized_reader);
    return specialized_hook_rc;
}

static void
specialized_set_threshold(const char *value)
{
#ifdef _WIN32
    (void)_putenv_s("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER",
        value ? value : "");
#else
    if (value)
        (void)setenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER", value, 1);
    else
        (void)unsetenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");
#endif
}

static void
test_specialized_publication(uint32_t workers, unsigned initial, unsigned mode,
    bool baseline)
{
    TEST("specialized nonrecursive: persistent owners and atomic publication");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, mode == 17 ? 0 : 1 };
    if (mode == 18) predicate[0] = WL_PLAN_EXPR_EXTENSION_CALL;
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_CONSOLIDATE }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = mode == 16 ? 1 : 3 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    col_rel_t *unowned = NULL, *target = NULL;
    tuple_collector_t tuples = { 0 };
    int64_t *values = NULL;
    const char *failure = NULL;
    const char *old_env = getenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");
    char *saved_env = old_env ? malloc(strlen(old_env) + 1) : NULL;
    if (saved_env) memcpy(saved_env, old_env, strlen(old_env) + 1);
    specialized_set_threshold(baseline ? "32768" : "64");
    specialized_mode = mode;
    specialized_worker = workers - 1;
    specialized_injected = false;
    specialized_hook_rc = 0;
    specialized_alias = specialized_held = NULL;
    specialized_entry = NULL;
    specialized_arrangement_captured = false;
    specialized_active = NULL;
    memset(&specialized_reader, 0, sizeof(specialized_reader));
    atomic_store_explicit(&specialized_workers, 0, memory_order_relaxed);
#define SPECIAL_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    SPECIAL_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "specialized session");
    wl_col_session_t *coord = COL_SESSION(session);
    governor = coord->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    uint32_t count = workers * (baseline ? 32768 : 64);
    values = malloc((size_t)count * sizeof(*values));
    SPECIAL_CHECK(values, "specialized input allocation");
    for (uint32_t row = 0; row < count; row++) values[row] = 42;
    SPECIAL_CHECK(wl_session_insert(session, "input", values, count, 1) == 0,
        "specialized input");
    col_rel_t *input = session_find_rel(coord, "input");
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    SPECIAL_CHECK(col_rel_set_column_types(input, &type, 1) == 0
        && col_rel_enable_timestamps(input) == 0,
        "specialized source metadata");
    for (uint32_t row = 0; row < count;
        row++) input->timestamps[row] = idb_timestamp(row);
    if (initial) {
        unowned = col_rel_new_like("output", input);
        SPECIAL_CHECK(unowned && col_rel_enable_timestamps(unowned) == 0,
            "specialized initial target");
        if (initial == 2) {
            int64_t seed = 7;
            SPECIAL_CHECK(col_rel_append_row(unowned, &seed) == 0,
                "specialized seed");
            unowned->timestamps[0] = idb_timestamp(0);
        }
        SPECIAL_CHECK(session_add_rel(coord, unowned) == 0,
            "specialized register");
        target = unowned; unowned = NULL;
    }
    uint64_t view = target ? target->view_generation : 0;
    uint64_t storage = target ? target->storage_generation : 0;
    int64_t **columns = target ? target->columns : NULL;
    wl_columnar_eval_test_nonrec_worker = specialized_worker_boundary;
    wl_columnar_eval_test_nonrec_boundary = specialized_publication_boundary;
    int rc = wl_session_snapshot(session, collect_tuple, &tuples);
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    idb_restore_budget(coord);
    bool healthy = mode == 0 || mode == 16 || mode == 17;
    if (mode != 9 && mode != 13 && mode != 14 && mode != 19)
        SPECIAL_CHECK(atomic_load_explicit(&specialized_workers,
            memory_order_relaxed) == (1u << workers) - 1,
            "actual specialized workers");
    SPECIAL_CHECK(specialized_hook_rc == 0, "specialized test seam");
    if (!healthy) {
        int expected = (mode <= 7 || mode == 19 || mode == 20 ||
            mode >= 21) ? EBUSY
            : mode == 13 || mode == 15 ? ENOSPC : ENOMEM;
        bool expected_status = mode == 18
            ? coord->extension_expr_status
            == WL_COLUMNAR_EXPR_EXTENSION_MALFORMED
            : rc == expected;
        SPECIAL_CHECK(rc != 0 && expected_status && tuples.count == 0,
            "specialized refusal code or callbacks");
        if (target)
            SPECIAL_CHECK(target == session_find_rel(coord, "output")
                && target->nrows == (initial == 2 ? 1u : 0u)
                && target->columns == columns && target->view_generation == view
                && target->storage_generation == storage,
                "specialized refused target changed");
        if (mode <= 7 || mode == 19 || mode == 20 || mode >= 21) {
            for (unsigned retry = 0; retry < 2; retry++) {
                SPECIAL_CHECK(wl_session_snapshot(session, collect_tuple,
                    &tuples) == EBUSY
                    && tuples.count == 0, "specialized repeated refusal");
                if (specialized_arrangement_captured)
                    SPECIAL_CHECK(memcmp(&specialized_arrangement,
                        &coord->arr_entries[0],
                        sizeof(specialized_arrangement)) == 0,
                        "specialized refused arrangement changed");
                if ((mode >= 3 && mode <= 6) || mode >= 21)
                    SPECIAL_CHECK(specialized_entry->rel == specialized_held
                        && specialized_entry->seg_count == 2
                        && specialized_entry->seg_boundaries
                        && col_rel_get(specialized_held, 0, 0) == 42,
                        "specialized retained descriptor or segments");
            }
        }
        if (specialized_reader.owner)
            SPECIAL_CHECK(col_rel_source_reader_release(&specialized_reader) ==
                0,
                "specialized release reader");
        if (specialized_active)
            SPECIAL_CHECK(wl_columnar_eval_stack_cleanup_finish(
                    &specialized_active) == 0,
                "specialized active guard release");
        col_rel_destroy(specialized_alias); specialized_alias = NULL;
        wl_columnar_eval_test_nonrec_boundary = NULL;
        predicate[0] = WL_PLAN_EXPR_BOOL;
        SPECIAL_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0,
            "specialized public retry");
    } else SPECIAL_CHECK(rc == 0, "specialized healthy evaluation");
    target = session_find_rel(coord, "output");
    uint32_t expected_rows = mode == 16 ? count + (initial == 2)
        : mode == 17 ? (initial == 2) : 1 + (initial == 2);
    SPECIAL_CHECK(target && target->nrows == expected_rows &&
        coord->tdd_workers_count == 0
        && !coord->cleanup_pending && coord->cleanup_reserved_bytes == 0,
        "specialized exact result or ownership");
    if (expected_rows && mode != 16 && mode != 17) {
        SPECIAL_CHECK(target->column_types && target->timestamps
            && col_rel_get(target, target->nrows - 1, 0) == 42
            && target->timestamps[target->nrows - 1].iteration == 10,
            "specialized metadata or provenance");
    }
    if (mode == 16) {
        SPECIAL_CHECK(target->timestamps, "VARIABLE timestamp mode");
        for (uint32_t row = 0; row < count; row++) {
            col_delta_timestamp_t expected = idb_timestamp(row);
            SPECIAL_CHECK(memcmp(&target->timestamps[row + (initial == 2)],
                &expected, sizeof(expected)) == 0,
                "VARIABLE complete signed row provenance");
        }
    }
    if (mode != 16) {
        memset(&tuples, 0, sizeof(tuples));
        SPECIAL_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
            && tuples.count == expected_rows && target->nrows == expected_rows,
            "specialized repeated exact snapshot");
    }
cleanup:
    wl_columnar_eval_test_nonrec_worker = NULL;
    wl_columnar_eval_test_nonrec_boundary = NULL;
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (session) idb_restore_budget(COL_SESSION(session));
    if (specialized_reader.owner)
        (void)col_rel_source_reader_release(&specialized_reader);
    if (specialized_active)
        (void)wl_columnar_eval_stack_cleanup_finish(&specialized_active);
    col_rel_destroy(specialized_alias);
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    if (governor) {
        if (reserved_on(governor) != 0 &&
            !failure) failure = "specialized reservation leak";
        wl_columnar_memory_governor_ref_release(governor);
    }
    if (saved_env) {
        specialized_set_threshold(saved_env);
        free(saved_env);
    }else specialized_set_threshold(NULL);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef SPECIAL_CHECK
}

static void
test_final_normalization(uint32_t workers, unsigned route, unsigned mode)
{
    TEST("TDD final normalization: checked ownership, provenance and retry");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    uint32_t key = 0;
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = route == 0 ? WL_PLAN_OP_EXCHANGE : WL_PLAN_OP_CONSOLIDATE,
          .opaque_data = route == 0 ? &exchange : NULL },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = route == 1 ? 4 : 3 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = route == 0 };
    uint8_t later_predicate[] = { WL_PLAN_EXPR_BOOL, 0 };
    wl_plan_op_t later_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "output" },
        { .op = WL_PLAN_OP_FILTER,
          .filter_expr = { later_predicate, sizeof(later_predicate) } },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t later = { .name = "later", .delta_name = "$d$later",
                                 .ops = later_ops, .op_count = 3 };
    wl_plan_stratum_t later_stratum = { .relations = &later,
                                        .relation_count = 1,
                                        .is_recursive = false };
    wl_plan_stratum_t strata[] = { stratum, later_stratum };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = strata,
                       .stratum_count = route == 1 ? 2 : 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    int64_t *values = NULL;
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
    final_normalize_mode = mode;
    final_normalize_hit = false;
    final_normalize_target = final_normalize_alias = NULL;
    idb_budget_changed = false;
    memset(&final_normalize_reader, 0, sizeof(final_normalize_reader));
#define FINAL_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    FINAL_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "final session");
    wl_col_session_t *coord = COL_SESSION(session);
    governor = coord->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    uint32_t count = route == 2 ? workers * 32768 : 65536;
    values = malloc((size_t)count * sizeof(*values));
    FINAL_CHECK(values, "final input");
    for (uint32_t row = 0; row < count; row++)
        values[row] = row % 2 ? 42 : 7;
    FINAL_CHECK(wl_session_insert(session, "input", values, count, 1) == 0,
        "final insert");
    wl_columnar_eval_test_before_final_normalize = inject_final_normalization;
    int rc = route == 0 ? wl_session_snapshot(session, collect_tuple, &tuples)
        : route == 1 ? wl_session_step(session)
        : wl_columnar_eval_nonrec_relation_parallel(&output, coord);
    idb_restore_budget(coord);
    FINAL_CHECK(final_normalize_hit && final_normalize_hook_rc == 0
        && coord->tdd_workers_cap >= workers && coord->tdd_workers_count == 0,
        "final route or worker retirement");
    if (mode != 5) {
        FINAL_CHECK(rc == (mode <= 2 ? EBUSY : mode == 3 ? ENOMEM : ENOSPC)
            && tuples.count == 0 && final_normalize_target->nrows == 3
            && final_normalize_target->columns == final_normalize_columns
            && final_normalize_target->view_generation == final_normalize_view
            && final_normalize_target->storage_generation ==
            final_normalize_storage
            && memcmp(final_normalize_target->timestamps,
            final_normalize_timestamps, sizeof(final_normalize_timestamps)) == 0
            && memcmp(final_normalize_target->columns[0],
            final_normalize_values, sizeof(final_normalize_values)) == 0,
            "failed finalization changed target");
        FINAL_CHECK(coord->total_iterations == final_normalize_iterations
            && memcmp(&coord->frontiers[0], &final_normalize_frontier,
            sizeof(final_normalize_frontier)) == 0
            && memcmp(&coord->rule_frontiers[0], &final_normalize_rule,
            sizeof(final_normalize_rule)) == 0,
            "failed finalization recorded convergence");
        if (mode <= 2) {
            for (unsigned attempt = 0; attempt < 2; attempt++)
                FINAL_CHECK(wl_columnar_eval_delta_consolidate(
                        final_normalize_target, coord) == EBUSY
                    && final_normalize_target->view_generation ==
                    final_normalize_view
                    && final_normalize_target->nrows == 3,
                    "repeated normalization refusal");
            if (route == 1) {
                int64_t blocked_input = 7;
                FINAL_CHECK(wl_session_insert(session, "input",
                    &blocked_input, 1, 1) == EBUSY,
                    "pending completion blocks input mutation");
                delta_collector_t blocked_deltas = { 0 };
                wl_session_set_delta_cb(session, collect_delta,
                    &blocked_deltas);
                FINAL_CHECK(wl_session_remove(session, "input",
                    &blocked_input, 1, 1) == EBUSY,
                    "pending completion blocks incremental removal");
                wl_session_set_delta_cb(session, NULL, NULL);
            }
            if (final_normalize_reader.owner)
                FINAL_CHECK(col_rel_source_reader_release(
                        &final_normalize_reader)
                    == 0, "final release reader");
            col_rel_destroy(final_normalize_alias);
            final_normalize_alias = NULL;
        }
        if (route == 1 && mode <= 2) {
            memset(&tuples, 0, sizeof(tuples));
            FINAL_CHECK(wl_session_snapshot(session, collect_tuple, &tuples)
                == 0 && tuples.count == 2
                && has_tuple(&tuples, "output", (int64_t[]){ 7 }, 1)
                && has_tuple(&tuples, "output", (int64_t[]){ 42 }, 1),
                "mixed STEP to SNAPSHOT recovery");
        } else {
            FINAL_CHECK(wl_columnar_eval_delta_consolidate(
                    final_normalize_target,
                    coord) == 0, "final direct recovery");
        }
    } else {
        FINAL_CHECK(rc == 0, "final successful normalization");
    }
    FINAL_CHECK(final_normalize_target->nrows == 2
        && final_normalize_target->columns[0][0] == 7
        && final_normalize_target->columns[0][1] == 42
        && memcmp(final_normalize_target->timestamps,
        final_normalize_timestamps,
        2 * sizeof(*final_normalize_timestamps)) == 0,
        "duplicate normalization lost row provenance");
    if (route == 1 && mode != 5)
        FINAL_CHECK(wl_session_step(session) == 0,
            "nonrecursive final step retry");
    /* #1711 tracks earlier specialized merge publication with existing
     * outputs; this route validates its final normalization boundary. */
    if (route != 2 && (mode != 5 || route != 0)) {
        memset(&tuples, 0, sizeof(tuples));
        int retry_rc = wl_session_snapshot(session, collect_tuple, &tuples);
        FINAL_CHECK(retry_rc == 0 && tuples.count == 2
            && has_tuple(&tuples, "output", (int64_t[]){ 7 }, 1)
            && has_tuple(&tuples, "output", (int64_t[]){ 42 }, 1),
            "final exact public retry");
    }
cleanup:
    wl_columnar_eval_test_before_final_normalize = NULL;
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (session)
        idb_restore_budget(COL_SESSION(session));
    if (final_normalize_reader.owner)
        (void)col_rel_source_reader_release(&final_normalize_reader);
    col_rel_destroy(final_normalize_alias);
    final_normalize_alias = NULL;
    free(values);
    wl_session_destroy(session);
    if (governor) {
        if (wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor)) != 0)
            failure = "finalization leaked governor reservation";
        wl_columnar_memory_governor_ref_release(governor);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef FINAL_CHECK
}

static void
test_idb_consolidation(unsigned mode, bool timestamped)
{
    TEST("IDB consolidation: checked failure, provenance and exact retry");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t output = { .name = "output", .ops = &op, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t values[] = { 300, -2, 300, 1 };
    int64_t sorted[] = { -2, 1, 300 };
    uint32_t representatives[] = { 1, 3, 0 };
    if (mode == 9) {
        double floats[] = { -0.0, -2.5, 0.0, 1.5 };
        double expected[] = { -2.5, 0.0, 1.5 };
        memcpy(values, floats, sizeof(values));
        memcpy(sorted, expected, sizeof(sorted));
        representatives[1] = 0;
        representatives[2] = 3;
    }
#define IDB_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    idb_failure_mode = mode;
    idb_timestamped = timestamped;
    idb_hook_rc = EINVAL;
    idb_budget_changed = false;
    idb_held = NULL;
    memset(&idb_reader, 0, sizeof(idb_reader));
    IDB_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &session) == 0,
        "create");
    sess = COL_SESSION(session);
    governor = sess->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    if (mode == 9) {
        wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
        col_rel_t *input = session_find_rel(sess, "input");
        IDB_CHECK(col_rel_set_schema(input, 1, NULL) == 0
            && col_rel_set_column_types(input, &type, 1) == 0,
            "float input type");
    }
    IDB_CHECK(wl_session_insert(session, "input", values, 4, 1) == 0, "insert");
    wl_columnar_eval_delta_test_after_eval = idb_after_eval;
    wl_columnar_eval_delta_test_consolidation_boundary =
        idb_consolidation_boundary;
    int rc = wl_session_step(session);
    idb_restore_budget(sess);
    col_rel_t *target = session_find_rel(sess, "output");
    if (mode < 8 || mode == 10) {
        int expected = mode <= 2 ||
            mode == 10 ? EBUSY : (mode == 3 || mode == 7)
            ? ENOSPC : ENOMEM;
        IDB_CHECK(rc == expected && idb_hook_rc == 0 && deltas.count == 0
            && target && target->nrows == 4
            && target->columns == idb_target_columns
            && target->view_generation == idb_target_view
            && target->storage_generation == idb_target_storage,
            "failure changed target or hid error");
        for (uint32_t row = 0; row < 4; row++) {
            IDB_CHECK(col_rel_get(target, row, 0) == values[row],
                "target rows changed on refusal");
            if (timestamped) {
                col_delta_timestamp_t expected_ts = idb_timestamp(row);
                IDB_CHECK(memcmp(target->timestamps + row, &expected_ts,
                    sizeof(expected_ts)) == 0, "failure timestamp change");
            }
        }
        if (mode <= 1 || mode == 10) {
            IDB_CHECK(idb_reader.owner && (mode == 0
                || sess->cleanup_pending_count == 1), "retained owner");
            if (mode == 10) {
                IDB_CHECK(idb_held->nrows == 4, "held candidate geometry");
                for (uint32_t row = 0; row < 4; row++) {
                    col_delta_timestamp_t expected_ts = idb_timestamp(row);
                    IDB_CHECK(col_rel_get(idb_held, row, 0) == values[row]
                        && memcmp(idb_held->timestamps + row, &expected_ts,
                        sizeof(expected_ts)) == 0,
                        "reader observed normalization mutation");
                }
            }
            uint64_t retained = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor));
            for (unsigned retry = 0; retry < 2; retry++)
                IDB_CHECK(wl_session_step(session) == EBUSY
                    && deltas.count == 0 &&
                    target->columns == idb_target_columns
                    && wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(governor)) ==
                    retained,
                    "repeated refusal changed ownership");
            IDB_CHECK(col_rel_source_reader_release(&idb_reader) == 0,
                "release held reader");
        }
        idb_failure_mode = 8;
        wl_columnar_eval_delta_test_consolidation_boundary = NULL;
        rc = wl_session_step(session);
    }
    IDB_CHECK(rc == 0 && idb_hook_rc == 0 && target->nrows == 3
        && deltas.count == 3 && !sess->cleanup_pending && !sess->delta_observer,
        "exact completion");
    for (uint32_t row = 0; row < 3; row++) {
        int64_t value = sorted[row];
        IDB_CHECK(col_rel_get(target, row, 0) == value
            && has_delta(&deltas, "output", &value, 1, +1), "sorted set");
        if (timestamped) {
            col_delta_timestamp_t expected_ts =
                idb_timestamp(representatives[row]);
            IDB_CHECK(target->timestamps && memcmp(target->timestamps + row,
                &expected_ts, sizeof(expected_ts)) == 0,
                "row provenance lost or duplicate multiplicity summed");
        }
    }
    IDB_CHECK(!timestamped ||
        (target->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
        && target->compound_count == 1 && target->compound_arity_map
        && target->compound_arity_map[0] == 1), "compound metadata lost");
    deltas.count = 0;
    IDB_CHECK(wl_session_step(session) == 0 && deltas.count == 0,
        "duplicate notifications");
cleanup:
    wl_columnar_eval_delta_test_after_eval = NULL;
    wl_columnar_eval_delta_test_consolidation_boundary = NULL;
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
    fail_calloc_size = 0;
#endif
    if (idb_reader.owner)
        (void)col_rel_source_reader_release(&idb_reader);
    if (sess)
        idb_restore_budget(sess);
    wl_session_destroy(session);
    if (governor) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) != 0)
            failure = "consolidation reservation leak";
        wl_columnar_memory_governor_ref_release(governor);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef IDB_CHECK
}

static bool idb_small_invalid;
static int idb_small_hook_rc;
static void
idb_small_after_eval(wl_col_session_t *sess)
{
    col_rel_t *target = session_find_rel(sess, "output");
    idb_small_hook_rc = EINVAL;
    if (!target || target->nrows > 1)
        return;
    target->sorted_nrows = 0;
    target->run_count = 0;
    if (idb_small_invalid) {
        wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
        if (target->nrows != 1 || col_rel_set_column_types(target, NULL, 1) != 0
            || col_rel_set_column_types(target, &type, 1) != 0)
            return;
        target->columns[0][0] = (int64_t)UINT64_C(0x7ff0000000000000);
    }
    idb_small_hook_rc = col_rel_source_reader_acquire(target, &idb_reader);
}

static void
test_idb_small_input(bool empty, bool invalid)
{
    TEST(
        "IDB consolidation: zero/one-row validation does not mutate held metadata");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t output = { .name = "output", .ops = &op, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    const char *failure = NULL;
    delta_collector_t deltas = { 0 };
    int64_t value = 42;
#define SMALL_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    memset(&idb_reader, 0, sizeof(idb_reader));
    idb_small_invalid = invalid;
    idb_small_hook_rc = EINVAL;
    SMALL_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0, "create");
    if (!empty)
        SMALL_CHECK(wl_session_insert(session, "input", &value, 1, 1) == 0,
            "insert");
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    wl_columnar_eval_delta_test_after_eval = idb_small_after_eval;
    int rc = col_stratum_step_with_delta(&stratum, COL_SESSION(session), 0);
    col_rel_t *target = session_find_rel(COL_SESSION(session), "output");
    SMALL_CHECK(rc == (invalid ? EINVAL : 0) && idb_small_hook_rc == 0
        && target && target->sorted_nrows == 0 && target->run_count == 0
        && deltas.count == (invalid || empty ? 0 : 1),
        "small input skipped validation or mutated held metadata");
cleanup:
    wl_columnar_eval_delta_test_after_eval = NULL;
    if (idb_reader.owner)
        (void)col_rel_source_reader_release(&idb_reader);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef SMALL_CHECK
}

static unsigned observer_boundary_mode, observer_boundary_calls;
static wl_columnar_source_access_reader_t observer_compaction_reader;

static void
observer_inject_boundary(wl_col_session_t *sess, unsigned boundary)
{
    if (boundary != (observer_boundary_mode == 0 ? 1u : 2u))
        return;
    wl_columnar_eval_delta_test_observer_boundary = NULL;
    observer_boundary_calls++;
    if (observer_boundary_mode == 0) {
        (void)col_rel_source_reader_acquire(session_find_rel(sess, "input"),
            &observer_compaction_reader);
    }
#ifdef WL_TEST_ALLOC_WRAP
    else if (observer_boundary_mode == 1) {
        fail_next_alloc = true;
    }
#endif
    else if (observer_boundary_mode == 2) {
        wl_columnar_memory_governor_t *budget =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
        atomic_store_explicit(&budget->usable_bytes,
            wl_columnar_memory_reserved(budget), memory_order_relaxed);
    }
}

static void
test_observer_completion_retry(unsigned mode, bool cancel)
{
    TEST(
        "observer: completion retry does not repeat evaluation or lose baseline");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relation = { .name = "A", .ops = &op, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define COMPLETE_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    memset(&observer_compaction_reader, 0, sizeof(observer_compaction_reader));
    COMPLETE_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "create");
    sess = COL_SESSION(session);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    COMPLETE_CHECK(wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0, "baseline");
    deltas.count = 0;
    COMPLETE_CHECK(wl_session_insert(session, "input", &two, 1, 1) == 0,
        "insert");
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(sess->memory_governor);
    uint64_t saved_limit = atomic_load_explicit(&budget->usable_bytes,
            memory_order_relaxed);
    wl_columnar_memory_mode_t saved_mode = budget->mode;
    observer_boundary_mode = mode;
    observer_boundary_calls = 0;
    wl_columnar_eval_delta_test_observer_boundary = observer_inject_boundary;
    int rc = wl_session_step(session);
    budget->mode = saved_mode;
    atomic_store_explicit(&budget->usable_bytes, saved_limit,
        memory_order_relaxed);
    COMPLETE_CHECK(rc == (mode == 0 ? EBUSY : mode == 1 ? ENOMEM : ENOSPC)
        && observer_boundary_calls == 1 && sess->delta_observer
        && wl_columnar_eval_delta_observer_evaluated(sess)
        && !wl_columnar_eval_delta_observer_active(sess) && deltas.count == 0,
        "completion failure lost evaluated phase");
    uint64_t bytes = sess->delta_observer_reserved_bytes;
    if (mode == 0)
        COMPLETE_CHECK(wl_session_step(session) == EBUSY
            && sess->delta_observer_reserved_bytes == bytes &&
            deltas.count == 0,
            "repeated compaction refusal lost phase");
    if (cancel)
        wl_session_set_delta_cb(session, NULL, NULL);
    if (observer_compaction_reader.owner)
        COMPLETE_CHECK(col_rel_source_reader_release(
                &observer_compaction_reader) == 0,
            "compaction reader release");
    serial_scope_hook_calls = 0;
    wl_columnar_eval_serial_test_after_plan = count_serial_scope_hook;
    COMPLETE_CHECK(wl_session_step(session) == 0 && serial_scope_hook_calls == 0
        && !sess->delta_observer && sess->delta_observer_reserved_bytes == 0
        && session_find_rel(sess, "A")->nrows == 2
        && deltas.count == (cancel ? 0 : 1)
        && (cancel || has_delta(&deltas, "A", &two, 1, +1)),
        "completion retry reevaluated or changed delivery");
cleanup:
    wl_columnar_eval_delta_test_observer_boundary = NULL;
    wl_columnar_eval_serial_test_after_plan = NULL;
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (observer_compaction_reader.owner)
        (void)col_rel_source_reader_release(&observer_compaction_reader);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef COMPLETE_CHECK
}

#ifdef WL_TEST_ALLOC_WRAP
static void
test_observer_capture_failure(unsigned mode)
{
    TEST("observer: baseline allocation failure leaves evaluation unstarted");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relations[] = {
        { .name = "A", .ops = &op, .op_count = 1 },
        { .name = "B", .ops = &op, .op_count = 1 }
    };
    wl_plan_stratum_t stratum = { .relations = relations, .relation_count = 2 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define BASE_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    BASE_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "create");
    sess = COL_SESSION(session);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    BASE_CHECK(wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0
        && wl_session_insert(session, "input", &two, 1, 1) == 0, "baseline");
    deltas.count = 0;
    col_rel_t *a = session_find_rel(sess, "A"),
        *b = session_find_rel(sess, "B");
    int64_t **a_columns = a->columns, **b_columns = b->columns;
    uint64_t a_generation = a->view_generation,
        b_generation = b->view_generation;
    uint64_t before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(sess->memory_governor));
    for (unsigned retry = 0; retry < 2; retry++) {
        if (mode == 0)
            fail_next_alloc = true;
        else {
            fail_malloc_size = sizeof(int64_t);
            fail_malloc_matches_to_skip = mode - 1;
        }
        int rc = wl_session_step(session);
        BASE_CHECK(rc == ENOMEM && !fail_next_alloc && fail_malloc_size == 0
            && !sess->delta_observer && !sess->delta_rollback
            && a->columns == a_columns && b->columns == b_columns
            && a->view_generation == a_generation &&
            b->view_generation == b_generation
            && a->nrows == 1 && b->nrows == 1 && deltas.count == 0
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                sess->memory_governor)) == before,
            "capture failure changed baseline or admission");
    }
    BASE_CHECK(wl_session_step(session) == 0 && deltas.count == 2
        && has_delta(&deltas, "A", &two, 1, +1)
        && has_delta(&deltas, "B", &two, 1, +1), "capture retry oracle");
cleanup:
    fail_next_alloc = false;
    fail_malloc_size = 0;
    fail_malloc_matches_to_skip = 0;
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef BASE_CHECK
}
#endif

typedef struct {
    wl_session_t *session;
    delta_collector_t first, remaining;
    unsigned mode;
    bool guards_ok;
} observer_callback_context_t;

static void
observer_mutating_callback(const char *name, const int64_t *row,
    uint32_t ncols, int32_t diff, void *opaque)
{
    observer_callback_context_t *context = opaque;
    collect_delta(name, row, ncols, diff, &context->first);
    wl_col_session_t *sess = COL_SESSION(context->session);
    uint32_t epoch = sess->outer_epoch, nrels = sess->nrels;
    int64_t value = 42;
    tuple_collector_t tuples = { 0 };
    wirelog_compound_arg_t arg = { WIRELOG_TYPE_INT64, 1 };
    uint64_t handle = 123;
    context->guards_ok = wl_session_step(context->session) == EBUSY
        && wl_session_snapshot(context->session, collect_tuple,
            &tuples) == EBUSY
        && wl_session_insert(context->session, "input", &value, 1, 1) == EBUSY
        && wl_session_remove(context->session, "input", &value, 1, 1) == EBUSY
        && wl_session_make_compound(context->session, "callback", 1, &arg,
            &handle) == EBUSY && handle == WIRELOG_COMPOUND_HANDLE_NULL
        && sess->outer_epoch == epoch && sess->nrels == nrels;
    if (context->mode != 0)
        wl_session_set_delta_cb(context->session, NULL, NULL);
    if (context->mode != 1)
        wl_session_set_delta_cb(context->session, collect_delta,
            &context->remaining);
}

static void
test_observer_callback_binding(unsigned mode, bool direct)
{
    TEST(
        "observer: callback replacement/cancellation protects active batch storage");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relation = { .name = "A", .ops = &op, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    observer_callback_context_t context = { .mode = mode };
    const char *failure = NULL;
    int64_t values[] = { 1, 2, 3 }, later = 4;
#define CALLBACK_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    CALLBACK_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "create");
    context.session = session;
    wl_session_set_delta_cb(session, observer_mutating_callback, &context);
    CALLBACK_CHECK(wl_session_insert(session, "input", values, 3, 1) == 0,
        "insert");
    int rc = direct ? col_stratum_step_with_delta(&stratum,
            COL_SESSION(session), 0)
        : wl_session_step(session);
    CALLBACK_CHECK(rc == 0 && context.guards_ok && context.first.count == 1
        && context.remaining.count == (mode == 0 ? 2 : 0)
        && !COL_SESSION(session)->delta_publish_active
        && !COL_SESSION(session)->delta_observer,
        "binding or reentry contract");
    if (!direct) {
        context.remaining.count = 0;
        wl_session_set_delta_cb(session, collect_delta, &context.remaining);
        CALLBACK_CHECK(wl_session_insert(session, "input", &later, 1, 1) == 0
            && wl_session_step(session) == 0 && context.remaining.count == 1
            && has_delta(&context.remaining, "A", &later, 1, +1),
            "next transaction binding");
    }
cleanup:
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef CALLBACK_CHECK
}

static void
test_observer_logical_identity(unsigned mode)
{
    TEST(
        "observer: logical names survive absence, replacement and arity changes");
    wl_plan_relation_t outputs[] = { { .name = "A" }, { .name = "A" },
                                     { .name = "B" } };
    wl_plan_stratum_t strata[] = {
        { .relations = outputs, .relation_count = 1 },
        { .relations = outputs + 1, .relation_count = 2 }
    };
    wl_plan_t plan = { .strata = strata, .stratum_count = 2 };
    wl_session_t *session = NULL;
    col_rel_t *owned = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t old = 7, replacement[] = { 9, 11 };
#define IDENTITY_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    IDENTITY_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    col_rel_t *a = session_find_rel(sess, "A");
    if (a)
        IDENTITY_CHECK(session_remove_rel(sess, "A") == 0,
            "remove placeholder");
    owned = col_rel_new_auto("A", 1);
    IDENTITY_CHECK(owned && col_rel_append_row(owned, &old) == 0
        && session_add_rel(sess, owned) == 0, "old output");
    owned = NULL;
    IDENTITY_CHECK(wl_columnar_eval_delta_observer_begin(sess, 3) == 0,
        "baseline capture");
    IDENTITY_CHECK(session_remove_rel(sess, "A") == 0, "replace descriptor");
    if (mode != 0) {
        owned = col_rel_new_auto("A", mode == 2 ? 2 : 1);
        IDENTITY_CHECK(owned && col_rel_append_row(owned, replacement) == 0
            && session_add_rel(sess, owned) == 0, "new output");
        owned = NULL;
    }
    /* B was absent or empty at capture and becomes populated afterwards. */
    if (session_find_rel(sess, "B"))
        IDENTITY_CHECK(session_remove_rel(sess, "B") == 0, "remove empty B");
    owned = col_rel_new_auto("B", 1);
    IDENTITY_CHECK(owned && col_rel_append_row(owned, &old) == 0
        && session_add_rel(sess, owned) == 0, "new B");
    owned = NULL;
    IDENTITY_CHECK(wl_columnar_eval_delta_observer_prepare(sess) == 0,
        "prepare difference");
    wl_columnar_eval_delta_observer_publish(sess);
    IDENTITY_CHECK(deltas.count == (mode ? 3 : 2)
        && has_delta(&deltas, "A", &old, 1, -1)
        && has_delta(&deltas, "B", &old, 1, +1)
        && (!mode || has_delta(&deltas, "A", replacement,
        mode == 2 ? 2 : 1, +1)), "logical signed set oracle");
    IDENTITY_CHECK(wl_columnar_eval_delta_observer_finish(sess) == 0,
        "finish");
cleanup:
    if (owned)
        col_rel_destroy(owned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef IDENTITY_CHECK
}

typedef struct {
    wl_compound_arena_t *arena;
    uint64_t handle;
    delta_collector_t deltas;
    bool live;
} observer_compound_context_t;

static void
observer_compound_callback(const char *name, const int64_t *row,
    uint32_t ncols, int32_t diff, void *opaque)
{
    observer_compound_context_t *context = opaque;
    if (diff < 0 && ncols == 1 && (uint64_t)row[0] == context->handle)
        context->live = wl_compound_arena_lookup(context->arena,
                context->handle, NULL) != NULL;
    collect_delta(name, row, ncols, diff, &context->deltas);
}

static void
test_observer_compound_removal(void)
{
    TEST("observer: removed compound handle survives failed step and delivery");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT }
    };
    wl_plan_relation_t outputs[] = {
        { .name = "A", .ops = ops, .op_count = 3 },
        { .name = "B", .ops = ops, .op_count = 3 }
    };
    wl_plan_stratum_t stratum = { .relations = outputs, .relation_count = 2 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    observer_compound_context_t context = { 0 };
    const char *failure = NULL;
#define HANDLE_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    HANDLE_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    context.arena = sess->compound_arena;
    context.handle = wl_compound_arena_alloc(context.arena, 16);
    HANDLE_CHECK(context.handle != 0, "compound allocate");
    wl_compound_arena_freeze(context.arena);
    int64_t initial[] = { (int64_t)context.handle, 42 };
    wl_session_set_delta_cb(session, observer_compound_callback, &context);
    HANDLE_CHECK(wl_session_insert(session, "input", initial, 2, 1) == 0
        && wl_session_step(session) == 0, "initial step");
    wl_compound_arena_unfreeze(context.arena);
    context.deltas.count = 0;
    HANDLE_CHECK(wl_session_remove(session, "input", initial, 1, 1) == 0,
        "remove handle row");
    observer_seen_plans = 0;
    wl_columnar_eval_serial_test_after_plan = observer_hold_second_result;
    HANDLE_CHECK(wl_session_step(session) == EBUSY && sess->delta_observer
        && context.deltas.count == 0, "failed step");
    HANDLE_CHECK(wl_compound_arena_retain(context.arena, context.handle, -1)
        == 0, "drop multiplicity");
    uint32_t epoch = context.arena->current_epoch;
    (void)wl_compound_arena_gc_epoch_boundary(context.arena);
    HANDLE_CHECK(context.arena->current_epoch == epoch
        && wl_compound_arena_lookup(context.arena, context.handle, NULL)
        && wl_session_step(session) == EBUSY, "hold through retry");
    HANDLE_CHECK(col_rel_source_reader_release(&observer_result_reader) == 0,
        "release reader");
    HANDLE_CHECK(wl_session_step(session) == 0 && context.live
        && context.deltas.count == 2
        && has_delta(&context.deltas, "A", initial, 1, -1)
        && has_delta(&context.deltas, "B", initial, 1, -1),
        "removed handle live through delivery");
    (void)wl_compound_arena_gc_epoch_boundary(context.arena);
    HANDLE_CHECK(context.arena->current_epoch > epoch, "GC resumes");
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (observer_result_reader.owner)
        (void)col_rel_source_reader_release(&observer_result_reader);
    if (context.arena)
        wl_compound_arena_unfreeze(context.arena);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef HANDLE_CHECK
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

#ifdef WL_SESSION_TEST_HOOKS
static wl_columnar_source_access_reader_t rollback_frame_reader;
static wl_columnar_source_access_reader_t rollback_idb_reader;
static int rollback_hook_rc;
static bool rollback_hook_replace;
static bool rollback_hook_shared;

static void
inject_rollback_refusal(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_test_after_eval = NULL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    col_rel_t *held = col_rel_new_auto("rollback-held", 1);
    col_rel_t *target = session_find_rel(sess, "a");
    rollback_hook_rc = EINVAL;
    sess->rotation_ops->gc_epoch_boundary(sess);
    if (!held || !target)
        goto fail;
    if (rollback_hook_replace) {
        int64_t replacement_row = 99;
        col_rel_t *replacement = col_rel_new_auto("a", 1);
        if (!replacement || col_rel_append_row(replacement,
            &replacement_row) != 0
            || session_add_rel(sess, replacement) != 0) {
            col_rel_destroy(replacement);
            goto fail;
        }
        target = replacement;
    }
    if (rollback_hook_shared
        && wl_columnar_session_install_shared_view(sess, target,
        session_find_rel(sess, "input")) != 0)
        goto fail;
    rollback_hook_rc = wl_columnar_eval_stack_cleanup_begin(sess, &frame);
    if (rollback_hook_rc != 0)
        goto fail;
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    result->rel = held;
    result->owned = true;
    held = NULL;
    if (col_rel_source_reader_acquire(result->rel, &rollback_frame_reader) != 0
        || col_rel_source_reader_acquire(target, &rollback_idb_reader) != 0)
        goto fail;
    rollback_hook_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    return;
fail:
    if (frame)
        (void)wl_columnar_eval_stack_cleanup_finish(&frame);
    col_rel_destroy(held);
}

static void
test_persistent_delta_rollback(bool destroy_pending)
{
    TEST(destroy_pending ?
        "delta rollback: pending teardown needs no allocation"
        : "delta rollback: new frame and restore refusal retain prior state");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "missing" }
    };
    wl_plan_relation_t relation = { .name = "a", .ops = ops, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define ROLLBACK_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    memset(&rollback_frame_reader, 0, sizeof(rollback_frame_reader));
    memset(&rollback_idb_reader, 0, sizeof(rollback_idb_reader));
    ROLLBACK_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0, "create");
    sess = COL_SESSION(session);
    governor = sess->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    if (!sess->compound_arena)
        sess->compound_arena = wl_compound_arena_create(7, 16, 4);
    ROLLBACK_CHECK(sess->compound_arena, "compound arena");
    uint64_t handle = wl_compound_arena_alloc(sess->compound_arena, 8);
    ROLLBACK_CHECK(handle != 0, "compound handle");
    one = (int64_t)handle;
    wl_compound_arena_freeze(sess->compound_arena);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    ROLLBACK_CHECK(wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0 && deltas.count == 1, "baseline");
    wl_compound_arena_unfreeze(sess->compound_arena);
    col_rel_t *target = session_find_rel(sess, "a");
    uint64_t identity = target->relation_identity;
    ROLLBACK_CHECK(wl_session_insert(session, "input", &two, 1, 1) == 0,
        "pending insertion");
    deltas.count = 0;
    relation.op_count = 2;
    wl_columnar_eval_delta_test_after_eval = inject_rollback_refusal;
    int step_rc = wl_session_step(session);
    ROLLBACK_CHECK(step_rc == EBUSY && rollback_hook_rc == EBUSY,
        "new evaluator cleanup refusal");
    ROLLBACK_CHECK(sess->delta_rollback &&
        sess->delta_rollback_reserved_bytes > 0
        && sess->cleanup_pending && target->columns == NULL
        && target->nrows == 0 && target->relation_identity == identity
        && deltas.count == 0 && sess->pending_input_change,
        "rollback ownership or callback state");
    uint64_t retained = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(governor));
    ROLLBACK_CHECK(wl_session_step(session) == EBUSY
        && wl_session_step(session) == EBUSY
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == retained,
        "repeated frame refusal");
    ROLLBACK_CHECK(col_rel_source_reader_release(&rollback_frame_reader) == 0,
        "release frame reader");
    ROLLBACK_CHECK(wl_session_step(session) == EBUSY && !sess->cleanup_pending
        && sess->delta_rollback && !target->columns,
        "restore must respect descriptor reader");
    uint32_t epoch = sess->compound_arena->current_epoch;
    ROLLBACK_CHECK(wl_compound_arena_retain(sess->compound_arena, handle,
        -1) == 0
        && wl_compound_arena_alloc(sess->compound_arena, 8192) != 0,
        "compound mutation while rollback retained");
    (void)wl_compound_arena_gc_epoch_boundary(sess->compound_arena);
    ROLLBACK_CHECK(sess->compound_arena->current_epoch == epoch
        && wl_compound_arena_lookup(sess->compound_arena, handle, NULL),
        "compound backup handle reclaimed");
    ROLLBACK_CHECK(col_rel_source_reader_release(&rollback_idb_reader) == 0,
        "release IDB reader");
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = true;
    if (!destroy_pending) {
        ROLLBACK_CHECK(wl_session_step(session) == ENOMEM && !fail_next_alloc
            && sess->delta_rollback && !target->columns && deltas.count == 0,
            "restoration allocation failure lost backup");
    }
#endif
    if (!destroy_pending) {
        relation.op_count = 1;
        ROLLBACK_CHECK(wl_session_step(session) == 0 && !sess->delta_rollback
            && sess->delta_rollback_reserved_bytes == 0 && target->nrows == 2
            && deltas.count == 1 && has_delta(&deltas, "a", &two, 1, +1),
            "exact retry after retained restore");
        deltas.count = 0;
        ROLLBACK_CHECK(wl_session_step(session) == 0 && deltas.count == 0,
            "duplicate callbacks after retry");
    }
    wl_session_destroy(session);
    session = NULL;
#ifdef WL_TEST_ALLOC_WRAP
    ROLLBACK_CHECK(!destroy_pending || fail_next_alloc,
        "pending teardown attempted restoration allocation");
    fail_next_alloc = false;
#endif
    ROLLBACK_CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)) == 0,
        "reservation leak");
cleanup:
    wl_columnar_eval_delta_test_after_eval = NULL;
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (rollback_frame_reader.owner)
        (void)col_rel_source_reader_release(&rollback_frame_reader);
    if (rollback_idb_reader.owner)
        (void)col_rel_source_reader_release(&rollback_idb_reader);
    wl_session_destroy(session);
    if (governor)
        wl_columnar_memory_governor_ref_release(governor);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef ROLLBACK_CHECK
}
#endif

static void
test_delta_alias_rollback(bool hold_second)
{
    TEST(hold_second ? "delta rollback: alias prefix recovers on reader refusal"
        : "delta rollback: sibling aliases detach without owner write exclusion");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t relations[] = {
        { .name = "a", .ops = &op, .op_count = 1 },
        { .name = "b", .ops = &op, .op_count = 1 }
    };
    wl_plan_stratum_t stratum = { .relations = relations, .relation_count = 2 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    col_rel_t *unowned = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define ALIAS_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    ALIAS_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "create");
    sess = COL_SESSION(session);
    uint32_t no_request_epoch = sess->compound_arena->current_epoch;
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    ALIAS_CHECK(wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0 && deltas.count == 2, "baseline");
    ALIAS_CHECK(wl_session_insert(session, "input", &two, 1, 1) == 0,
        "pending insertion");
    wl_columnar_memory_governor_t *budget
        = wl_columnar_memory_governor_ref_get(sess->memory_governor);
    uint64_t saved_limit = atomic_load_explicit(&budget->usable_bytes,
            memory_order_relaxed);
    wl_columnar_memory_mode_t saved_mode = budget->mode;
    uint64_t before_reserved = wl_columnar_memory_reserved(budget);
    int64_t **before_a = session_find_rel(sess, "a")->columns;
    int64_t **before_b = session_find_rel(sess, "b")->columns;
    deltas.count = 0;
    wl_columnar_eval_stack_cleanup_frame_t *outer = NULL;
    int begin_rc = wl_columnar_eval_stack_cleanup_begin(sess, &outer);
    int nested_rc = begin_rc == 0 ? wl_session_step(session) : begin_rc;
    int finish_rc =
        outer ? wl_columnar_eval_stack_cleanup_finish(&outer) : EINVAL;
    ALIAS_CHECK(begin_rc == 0 && nested_rc == EBUSY && finish_rc == 0
        && !sess->delta_rollback && deltas.count == 0
        && session_find_rel(sess, "a")->columns == before_a
        && session_find_rel(sess, "b")->columns == before_b,
        "active-frame delta entry published or mutated");
    budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    atomic_store_explicit(&budget->usable_bytes, before_reserved,
        memory_order_relaxed);
    int denial_rc = wl_session_step(session);
    int repeat_denial_rc = wl_session_step(session);
    atomic_store_explicit(&budget->usable_bytes, saved_limit,
        memory_order_relaxed);
    budget->mode = saved_mode;
    ALIAS_CHECK(denial_rc == ENOSPC && repeat_denial_rc == ENOSPC
        && !sess->delta_rollback && deltas.count == 0
        && wl_columnar_memory_reserved(budget) == before_reserved
        && session_find_rel(sess, "a")->columns == before_a
        && session_find_rel(sess, "b")->columns == before_b,
        "capture budget denial changed prior state");
    unowned = col_rel_new_auto("alias-owner", 1);
    ALIAS_CHECK(unowned && col_rel_append_row(unowned, &one) == 0
        && session_add_rel(sess, unowned) == 0, "alias owner");
    col_rel_t *owner = unowned;
    unowned = NULL;
    col_rel_t *a = session_find_rel(sess, "a");
    col_rel_t *b = session_find_rel(sess, "b");
    ALIAS_CHECK(wl_columnar_session_install_shared_view(sess, a, owner) == 0
        && wl_columnar_session_install_shared_view(sess, b, owner) == 0,
        "sibling aliases");
    int64_t **owner_columns = owner->columns;
    deltas.count = 0;
    if (hold_second) {
        int64_t **b_columns = b->columns;
        uint64_t b_generation = b->view_generation;
        ALIAS_CHECK(col_rel_source_reader_acquire(b, &reader) == 0,
            "alias reader");
        ALIAS_CHECK(wl_session_step(session) == EBUSY && deltas.count == 0
            && sess->pending_input_change && b->columns == b_columns
            && b->view_generation == b_generation && b->storage_owner == owner
            && a->storage_owner == a && a->nrows == 1 && col_rel_get(a, 0,
            0) == one
            && owner->columns == owner_columns && owner->nrows == 1
            && col_rel_storage_alias_borrow_count(owner) == 1,
            "prefix restoration or reader stability");
        ALIAS_CHECK(col_rel_source_reader_release(&reader) == 0,
            "release alias reader");
    }
    int alias_rc = wl_session_step(session);
    ALIAS_CHECK(alias_rc == 0 && deltas.count == 2
        && has_delta(&deltas, "a", &two, 1, +1)
        && has_delta(&deltas, "b", &two, 1, +1)
        && a->storage_owner == a && b->storage_owner == b
        && a->nrows == 2 && b->nrows == 2
        && col_rel_storage_alias_borrow_count(owner) == 0
        && owner->nrows == 1 && col_rel_get(owner, 0, 0) == one
        && !sess->delta_rollback && !sess->source_leases,
        "alias exact retry or owner corruption");
    ALIAS_CHECK(sess->compound_arena->current_epoch == no_request_epoch,
        "nonrecursive evaluation invented a frontier request");
cleanup:
    if (reader.owner)
        (void)col_rel_source_reader_release(&reader);
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef ALIAS_CHECK
}

#ifdef WL_SESSION_TEST_HOOKS
static void
test_delta_rollback_preserves_progress(bool replace, bool shared)
{
    TEST(shared ?
        "delta rollback: populated shared-view progress retains its lease"
        : replace ? "delta rollback: replacement identity is never overwritten"
        : "delta rollback: populated partial results are preserved");
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "missing" }
    };
    wl_plan_relation_t relation = { .name = "a", .ops = ops, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define PROGRESS_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    PROGRESS_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "create");
    wl_col_session_t *sess = COL_SESSION(session);
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    PROGRESS_CHECK(wl_session_insert(session, "input", &one, 1, 1) == 0
        && wl_session_step(session) == 0, "baseline");
    uint64_t old_identity = session_find_rel(sess, "a")->relation_identity;
    PROGRESS_CHECK(wl_session_insert(session, "input", &two, 1, 1) == 0,
        "pending input");
    deltas.count = 0;
    relation.op_count = replace ? 2 : 1;
    rollback_hook_replace = replace;
    rollback_hook_shared = shared;
    wl_columnar_eval_delta_test_after_eval = inject_rollback_refusal;
    PROGRESS_CHECK(wl_session_step(session) == EBUSY &&
        rollback_hook_rc == EBUSY
        && deltas.count == 0 && sess->delta_rollback, "pending boundary");
    col_rel_t *target = session_find_rel(sess, "a");
    int64_t **columns = target->columns;
    uint64_t generation = target->view_generation;
    PROGRESS_CHECK(target->nrows == (replace ? 1u : 2u)
        && (target->relation_identity != old_identity) == replace,
        "fixture progress or replacement");
    PROGRESS_CHECK(col_rel_source_reader_release(&rollback_frame_reader) == 0
        && col_rel_source_reader_release(&rollback_idb_reader) == 0,
        "release readers");
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = true;
#endif
    PROGRESS_CHECK(wl_columnar_session_cleanup_ready(sess) == 0
        && !sess->delta_rollback && !sess->cleanup_pending
        && target->columns == columns && target->view_generation == generation
        && target->nrows == (replace ? 1u : 2u)
        && col_rel_get(target, 0, 0) == (replace ? 99 : one)
        && deltas.count == 0 && sess->pending_input_change,
        "rollback overwrote newer state");
    PROGRESS_CHECK(!shared ||
        (target->storage_owner != target && sess->source_leases),
        "populated shared-view lease retired");
#ifdef WL_TEST_ALLOC_WRAP
    PROGRESS_CHECK(fail_next_alloc, "unnecessary restoration allocation");
#endif
cleanup:
    wl_columnar_eval_delta_test_after_eval = NULL;
    rollback_hook_replace = false;
    rollback_hook_shared = false;
#ifdef WL_TEST_ALLOC_WRAP
    fail_next_alloc = false;
#endif
    if (rollback_frame_reader.owner)
        (void)col_rel_source_reader_release(&rollback_frame_reader);
    if (rollback_idb_reader.owner)
        (void)col_rel_source_reader_release(&rollback_idb_reader);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef PROGRESS_CHECK
}
#endif

static void
test_delta_rollback_timestamp_charge(void)
{
    TEST("delta rollback: retained timestamp charge follows physical lifetime");
    wl_mem_ledger_t ledger;
    wl_mem_ledger_snapshot_t snapshot;
    wl_mem_ledger_init(&ledger, 0);
    col_rel_t *rel = col_rel_new_auto("timestamp-rollback", 1);
    const char *failure = NULL;
    int64_t one = 1, two = 2;
#define TIMESTAMP_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    TIMESTAMP_CHECK(rel && col_rel_append_row(rel, &one) == 0
        && col_rel_enable_timestamps(rel) == 0, "fixture");
    rel->mem_ledger = &ledger;
    col_rel_ledger_reconcile(rel, 0);
    uint64_t timestamp_bytes = rel->ledger_ts_bytes;
    col_delta_timestamp_t *timestamps = rel->timestamps;
    TIMESTAMP_CHECK(timestamp_bytes > sizeof(col_delta_timestamp_t),
        "spare capacity");
    TIMESTAMP_CHECK(wl_columnar_relation_delta_detach(rel,
        rel->relation_identity) == 0,
        "detach");
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    TIMESTAMP_CHECK(rel->timestamps == timestamps &&
        rel->ledger_ts_bytes == timestamp_bytes
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == timestamp_bytes
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_RELATION] == 0,
        "detachment prematurely credited timestamp storage");
    TIMESTAMP_CHECK(wl_columnar_relation_delta_restore_flat(rel,
        rel->relation_identity, &one, 1, 1) == 0, "restore");
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    TIMESTAMP_CHECK(rel->timestamps == timestamps &&
        rel->ledger_ts_bytes == timestamp_bytes
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == timestamp_bytes
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_RELATION] == sizeof(int64_t),
        "restore charged logical timestamp capacity");
    TIMESTAMP_CHECK(col_rel_append_row(rel, &two) == 0,
        "timestamp replacement growth");
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    TIMESTAMP_CHECK(snapshot.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]
        == (uint64_t)rel->capacity * sizeof(col_delta_timestamp_t),
        "replacement timestamp charge");
cleanup:
    col_rel_destroy(rel);
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    if (!failure && snapshot.current_bytes != 0)
        failure = "ledger leak";
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef TIMESTAMP_CHECK
}

static void
test_delta_rollback_frontier_gc(bool pinned)
{
    TEST(pinned ?
        "delta rollback: pinned strategy resumes requested frontier GC"
        : "delta rollback: standard strategy resumes requested frontier GC");
    wl_plan_t *plan = build_plan(
        ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n"
        "path(x, z) :- path(x, y), edge(y, z).\n");
    wl_session_t *session = NULL;
    wl_col_session_t *sess = NULL;
    wl_arena_compound_arena_gc_hold_t external = { 0 };
    delta_collector_t deltas = { 0 };
    const char *failure = NULL;
#define GC_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    GC_CHECK(plan && wl_session_create(wl_backend_columnar(), plan, 1,
        &session) == 0,
        "create");
    sess = COL_SESSION(session);
    sess->rotation_ops =
        pinned ? &col_rotation_pinned_ops : &col_rotation_standard_ops;
    wl_session_set_delta_cb(session, collect_delta, &deltas);
    for (int64_t i = 0; i < 5; i++) {
        int64_t edge[] = { i, i + 1 };
        uint32_t epoch = sess->compound_arena->current_epoch;
        if (i == 2)
            wl_compound_arena_freeze(sess->compound_arena);
        if (i == 3)
            GC_CHECK(wl_arena_compound_arena_gc_hold_acquire(
                    sess->compound_arena,
                    &external) == 0, "external hold");
        deltas.count = 0;
        GC_CHECK(wl_session_insert(session, "edge", edge, 1, 2) == 0
            && wl_session_step(session) == 0 &&
            deltas.count == (uint32_t)(i + 1),
            "recursive exact delta");
        GC_CHECK(sess->compound_arena->current_epoch
            == epoch + ((i == 2 || i == 3) ? 0u : 1u),
            "deferred frontier progress or skip");
        if (i == 2)
            wl_compound_arena_unfreeze(sess->compound_arena);
        if (external.arena) {
            GC_CHECK(wl_arena_compound_arena_gc_hold_release(&external) == 0
                && sess->compound_arena->current_epoch == epoch,
                "external release implicitly collected");
        }
    }
cleanup:
    if (external.arena)
        (void)wl_arena_compound_arena_gc_hold_release(&external);
    if (sess)
        wl_compound_arena_unfreeze(sess->compound_arena);
    wl_session_destroy(session);
    wl_plan_free(plan);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef GC_CHECK
}

#ifdef WL_TEST_ALLOC_WRAP
static void
test_filter_timestamp_allocation_failure_retry(void)
{
    TEST("FILTER timestamp allocation failure retains held input for retry");
    col_rel_t *input = col_rel_new_auto("filter-fault", 1);
    delta_pool_t *pool = NULL;
    wl_col_session_t sess = { 0 };
    eval_stack_t stack;
    wl_plan_op_t op = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    bool reader_active = false;
    bool input_owned = true;
    const char *failure = NULL;
    int64_t value = 7;
    eval_stack_init(&stack);

    if (!input || col_rel_enable_timestamps(input) != 0
        || col_rel_append_row(input, &value) != 0) {
        failure = "fixture setup";
        goto cleanup;
    }
    input->timestamps[0] = (col_delta_timestamp_t){
        .iteration = 71, .stratum = 72, .worker = 73, .multiplicity = -5
    };
    if (col_rel_source_reader_acquire(input, &reader) != 0) {
        failure = "reader acquire";
        goto cleanup;
    }
    reader_active = true;
    pool = delta_pool_create(4, sizeof(col_rel_t), 4096);
    if (!pool) {
        failure = "pool setup";
        goto cleanup;
    }
    sess.delta_pool = pool;
    if (eval_stack_push(&stack, input, true) != 0) {
        failure = "stack push";
        goto cleanup;
    }
    input_owned = false;
    fail_calloc_size = input->capacity * sizeof(col_delta_timestamp_t);
    int rc = wl_columnar_filter_op(&op, &stack, &sess);
    if (fail_calloc_size != 0 || rc != EBUSY || stack.top != 1) {
        failure = "timestamp fault did not yield EBUSY/retention";
        goto cleanup;
    }
    eval_entry_t retained = eval_stack_pop(&stack);
    if (retained.rel != input || !retained.owned
        || eval_stack_push(&stack, retained.rel, retained.owned) != 0) {
        failure = "retained input ownership";
        goto cleanup;
    }
    if (col_rel_source_reader_release(&reader) != 0) {
        failure = "reader release";
        goto cleanup;
    }
    reader_active = false;
    rc = wl_columnar_filter_op(&op, &stack, &sess);
    if (rc != 0 || stack.top != 1) {
        failure = "retry execution";
        goto cleanup;
    }
    eval_entry_t result = eval_stack_pop(&stack);
    if (!result.rel->timestamps || result.rel->nrows != 1
        || result.rel->timestamps[0].iteration != 71
        || result.rel->timestamps[0].stratum != 72
        || result.rel->timestamps[0].worker != 73
        || result.rel->timestamps[0].multiplicity != -5)
        failure = "retry timestamp provenance";
    if (result.owned)
        col_rel_destroy(result.rel);
cleanup:
    fail_next_alloc = false;
    fail_calloc_size = 0;
    if (reader_active)
        (void)col_rel_source_reader_release(&reader);
    while (stack.top > 0) {
        eval_entry_t leftover = eval_stack_pop(&stack);
        if (leftover.owned)
            col_rel_destroy(leftover.rel);
    }
    if (input_owned)
        col_rel_destroy(input);
    if (pool)
        delta_pool_destroy(pool);
    if (failure) {
        FAIL(failure ? failure :
            "FILTER allocation failure did not retain and retry input");
        return;
    }
    PASS();
}

static void
test_filter_timestamp_allocation_failure_without_reader(void)
{
    TEST("FILTER timestamp allocation failure returns ENOMEM without a reader");
    col_rel_t *input = col_rel_new_auto("filter-fault-clean", 1);
    delta_pool_t *pool = NULL;
    wl_col_session_t sess = { 0 };
    eval_stack_t stack;
    wl_plan_op_t op = { 0 };
    const char *failure = NULL;
    bool input_owned = true;
    int64_t value = 7;
    eval_stack_init(&stack);
    if (!input || col_rel_enable_timestamps(input) != 0
        || col_rel_append_row(input, &value) != 0) {
        failure = "fixture setup";
        goto cleanup;
    }
    pool = delta_pool_create(4, sizeof(col_rel_t), 4096);
    if (!pool || eval_stack_push(&stack, input, true) != 0) {
        failure = "pool setup";
        goto cleanup;
    }
    sess.delta_pool = pool;
    fail_calloc_size = input->capacity * sizeof(col_delta_timestamp_t);
    input_owned = false;
    int rc = wl_columnar_filter_op(&op, &stack, &sess);
    if (rc != ENOMEM || fail_calloc_size != 0 || stack.top != 0)
        failure = "unheld allocation denial did not dispose input";
cleanup:
    fail_next_alloc = false;
    fail_calloc_size = 0;
    while (stack.top > 0) {
        eval_entry_t leftover = eval_stack_pop(&stack);
        if (leftover.owned)
            col_rel_destroy(leftover.rel);
    }
    if (input_owned)
        col_rel_destroy(input);
    if (pool)
        delta_pool_destroy(pool);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
}
#endif

static void
test_filter_timestamp_pool_and_heap_routes(void)
{
    TEST("FILTER timestamp pool and heap routes preserve records");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_t op = { .filter_expr = { predicate, sizeof(predicate) } };
    int64_t values[] = { 7, 42 };
    col_rel_t *input = col_rel_new_auto("filter-routes", 1);
    delta_pool_t *pool = NULL;
    wl_col_session_t sess = { 0 };
    eval_stack_t stack;
    const char *failure = NULL;
    eval_stack_init(&stack);
    if (!input || col_rel_enable_timestamps(input) != 0) {
        failure = "route fixture";
        goto cleanup;
    }
    for (uint32_t i = 0; i < 2; i++) {
        if (col_rel_append_row(input, &values[i]) != 0) {
            failure = "route input";
            goto cleanup;
        }
        input->timestamps[i] = (col_delta_timestamp_t){
            .iteration = 10 + i, .stratum = 20 + i,
            .worker = 30 + i, .multiplicity = i ? -2 : 3
        };
    }
    pool = delta_pool_create(4, sizeof(col_rel_t), 4096);
    if (!pool) {
        failure = "route pool";
        goto cleanup;
    }
    sess.delta_pool = pool;
    if (eval_stack_push(&stack, input, false) != 0
        || wl_columnar_filter_op(&op, &stack, &sess) != 0
        || stack.top != 1) {
        failure = "pool route";
        goto cleanup;
    }
    eval_entry_t pooled = eval_stack_pop(&stack);
    if (!pooled.rel || !pooled.rel->pool_owned || pooled.rel->nrows != 2
        || !pooled.rel->timestamps
        || pooled.rel->columns[0][0] != 7
        || pooled.rel->columns[0][1] != 42
        || memcmp(pooled.rel->timestamps, input->timestamps,
        2 * sizeof(*input->timestamps)) != 0) {
        failure = "pool provenance";
        if (pooled.owned) col_rel_destroy(pooled.rel);
        goto cleanup;
    }
    col_rel_destroy(pooled.rel);
    while (pool->slot_used < pool->slot_cap)
        if (!delta_pool_alloc_slot(pool)) {
            failure = "pool exhaustion setup";
            goto cleanup;
        }
    if (eval_stack_push(&stack, input, false) != 0
        || wl_columnar_filter_op(&op, &stack, &sess) != 0
        || stack.top != 1) {
        failure = "heap route";
        goto cleanup;
    }
    eval_entry_t heap = eval_stack_pop(&stack);
    if (!heap.rel || heap.rel->pool_owned || heap.rel->nrows != 2
        || !heap.rel->timestamps
        || heap.rel->columns[0][0] != 7
        || heap.rel->columns[0][1] != 42
        || memcmp(heap.rel->timestamps, input->timestamps,
        2 * sizeof(*input->timestamps)) != 0){
        failure = "heap provenance";
    }
    if (heap.owned) col_rel_destroy(heap.rel);
cleanup:
    while (stack.top > 0) {
        eval_entry_t leftover = eval_stack_pop(&stack);
        if (leftover.owned) col_rel_destroy(leftover.rel);
    }
    if (pool) delta_pool_destroy(pool);
    col_rel_destroy(input);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
}

/* Capture producer results before serial publication. The parent consumes only
 * these independent buffers after the real workqueue barrier (#1736). */
typedef struct {
    unsigned calls;
    bool valid;
    uint32_t count;
    int64_t keys[16];
    int64_t selected[16];
    col_delta_timestamp_t timestamps[16];
} filter_worker_capture_t;
static filter_worker_capture_t filter_worker_capture[8];
static wl_col_session_t *filter_worker_coordinator;
static uint32_t filter_worker_count;
static wl_atomic_u64 filter_worker_bits;

static col_delta_timestamp_t
filter_worker_timestamp(uint32_t key)
{
    return (col_delta_timestamp_t){ .iteration = 100 + key,
                                    .stratum = 200 + key, .worker = 300 + key,
                                    .multiplicity = key % 4 == 1 ? -3 : 5 };
}

static void
capture_filter_worker(wl_col_session_t *worker, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    if (worker->coordinator != filter_worker_coordinator
        || worker->worker_id >= filter_worker_count)
        return;
    uint32_t w = worker->worker_id;
    filter_worker_capture_t *capture = &filter_worker_capture[w];
    capture->calls++;
    record_actual_worker(&filter_worker_bits, w);
    col_rel_t *input = session_find_rel(worker, "input");
    capture->valid = result->rel && result->owned && input
        && result->rel != input && result->rel->ncols == 2
        && result->rel->nrows == 16 && result->rel->timestamps
        && strcmp(result->rel->name, "$filter") == 0;
    if (!capture->valid)
        return;
    capture->count = result->rel->nrows;
    for (uint32_t r = 0; r < capture->count; r++) {
        capture->keys[r] = col_rel_get(result->rel, r, 0);
        capture->selected[r] = col_rel_get(result->rel, r, 1);
        capture->timestamps[r] = result->rel->timestamps[r];
    }
}

static void
test_filter_worker_provenance(uint32_t count)
{
    TEST("actual W2/W8 FILTER producers preserve complete signed provenance");
    uint8_t expr[32];
    uint32_t size = 0;
    expr[size++] = WL_PLAN_EXPR_VAR;
    expr[size++] = 4; expr[size++] = 0;
    memcpy(expr + size, "col1", 4); size += 4;
    expr[size++] = WL_PLAN_EXPR_CONST_INT;
    int64_t zero = 0;
    memcpy(expr + size, &zero, sizeof(zero)); size += sizeof(zero);
    expr[size++] = WL_PLAN_EXPR_CMP_GT;
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { expr, size } }
    };
    wl_plan_relation_t relation = { .name = "output", .ops = ops,
                                    .op_count = 2 };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_col_session_t *workers = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    col_rel_t *unowned = NULL;
    uint32_t initialized = 0;
    const char *failure = NULL;
#define FILTER_WORKER_CHECK(c, m) do { if (!(c)) { failure = (m); goto cleanup; \
                                       } } while (0)
    memset(filter_worker_capture, 0, sizeof(filter_worker_capture));
    atomic_store_explicit(&filter_worker_bits, 0, memory_order_relaxed);
    FILTER_WORKER_CHECK(wl_session_create(wl_backend_columnar(), &plan, count,
        &session) == 0, "worker provenance session");
    wl_col_session_t *coord = COL_SESSION(session);
    governor = coord->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    FILTER_WORKER_CHECK(wl_columnar_session_ensure_workqueue(coord, count) == 0,
        "worker provenance workqueue");
    workers = calloc(count, sizeof(*workers));
    FILTER_WORKER_CHECK(workers, "worker provenance cohort");
    for (uint32_t w = 0; w < count; w++) {
        FILTER_WORKER_CHECK(col_worker_session_create(coord, w, NULL, 0,
            &workers[w]) == 0, "worker provenance create");
        initialized++;
        workers[w].frontier_ops->reset_rule_frontier(&workers[w], 0,
            workers[w].outer_epoch);
        unowned = col_rel_new_auto("input", 2);
        FILTER_WORKER_CHECK(unowned && col_rel_enable_timestamps(unowned) == 0,
            "worker provenance input");
        for (uint32_t r = 0; r < 32; r++) {
            uint32_t key = w * 100 + r;
            int64_t row[] = { key, r % 2 ? 1 : -1 };
            FILTER_WORKER_CHECK(col_rel_append_row(unowned, row) == 0,
                "worker provenance append");
            unowned->timestamps[r] = filter_worker_timestamp(key);
        }
        FILTER_WORKER_CHECK(session_add_rel(&workers[w], unowned) == 0,
            "worker provenance registration");
        unowned = NULL;
    }
    filter_worker_coordinator = coord;
    filter_worker_count = count;
    wl_columnar_eval_serial_test_after_plan = capture_filter_worker;
    int rc = col_eval_stratum_multiworker(&stratum, coord, 0, workers, count);
    wl_columnar_eval_serial_test_after_plan = NULL;
    FILTER_WORKER_CHECK(rc == 0, "worker provenance evaluation");
    FILTER_WORKER_CHECK(atomic_load_explicit(&filter_worker_bits,
        memory_order_relaxed) == (UINT64_C(1) << count) - 1,
        "every actual worker dispatched");
    for (uint32_t w = 0; w < count; w++) {
        filter_worker_capture_t *capture = &filter_worker_capture[w];
        FILTER_WORKER_CHECK(capture->valid && capture->calls == 1
            && capture->count == 16, "one FILTER producer per worker");
        for (uint32_t r = 0; r < 16; r++) {
            uint32_t key = w * 100 + 2 * r + 1;
            col_delta_timestamp_t expected = filter_worker_timestamp(key);
            col_delta_timestamp_t actual = capture->timestamps[r];
            FILTER_WORKER_CHECK(capture->keys[r] == key
                && capture->selected[r] == 1
                && actual.iteration == expected.iteration
                && actual.stratum == expected.stratum
                && actual.worker == expected.worker
                && actual.multiplicity == expected.multiplicity,
                "consumer exact row and complete signed provenance");
        }
        FILTER_WORKER_CHECK(!workers[w].cleanup_active
            && !workers[w].cleanup_pending
            && workers[w].cleanup_reserved_bytes == 0,
            "worker frame balance");
    }
cleanup:
    wl_columnar_eval_serial_test_after_plan = NULL;
    filter_worker_coordinator = NULL;
    for (uint32_t w = 0; w < initialized; w++)
        if (col_worker_session_destroy(&workers[w]) != 0 && !failure)
            failure = "worker provenance checked teardown";
    free(workers);
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (governor) {
        if (reserved_on(governor) != 0 && !failure)
            failure = "worker provenance reservation leak";
        wl_columnar_memory_governor_ref_release(governor);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef FILTER_WORKER_CHECK
}

#ifdef WL_SESSION_TEST_HOOKS
static void
test_kfusion_retained_pool_entry_retry(void)
{
    TEST("K-Fusion retains a busy pool entry for retry");
    const char *failure = NULL;
#define KRETAIN_CHECK(c, m) \
        do { if (!(c)) { failure = (m); goto kfusion_retained_cleanup; \
             } } while (0)
    wl_col_session_t sess;
    memset(&sess, 0, sizeof(sess));
    sess.delta_pool = delta_pool_create(8, sizeof(col_rel_t), 4096);
    KRETAIN_CHECK(sess.delta_pool != NULL, "pool allocation failed");

    col_rel_t *rel = col_rel_pool_new_auto(sess.delta_pool, NULL,
            "kfusion-held", 1);
    KRETAIN_CHECK(rel != NULL, "pool relation allocation failed");
    KRETAIN_CHECK(col_rel_append_row(rel, (int64_t[]){ 7 }) == 0,
        "pool relation append failed");
    wl_columnar_source_access_reader_t reader = { 0 };
    KRETAIN_CHECK(col_rel_source_reader_acquire(rel, &reader) == 0,
        "pool reader acquire failed");

    eval_entry_t entry = {
        .rel = rel,
        .owned = true,
        .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION,
    };
    KRETAIN_CHECK(wl_columnar_session_retain_eval_entry(&sess, &entry) == 0,
        "retaining pool entry failed");
    KRETAIN_CHECK(entry.rel == NULL && sess.retained_eval_entry_count == 1,
        "retained entry ownership was not transferred");
    KRETAIN_CHECK(wl_columnar_session_retry_retained_eval_entries(&sess) ==
        EBUSY,
        "held pool entry should refuse cleanup");
    KRETAIN_CHECK(sess.retained_eval_entry_count == 1,
        "busy entry was lost during retry");
    KRETAIN_CHECK(col_rel_source_reader_release(&reader) == 0,
        "pool reader release failed");
    KRETAIN_CHECK(wl_columnar_session_retry_retained_eval_entries(&sess) == 0,
        "released pool entry did not retry");
    KRETAIN_CHECK(sess.retained_eval_entry_count == 0,
        "released pool entry remained retained");

kfusion_retained_cleanup:
    if (reader.owner)
        (void)col_rel_source_reader_release(&reader);
    (void)wl_columnar_session_retry_retained_eval_entries(&sess);
    delta_pool_destroy(sess.delta_pool);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef KRETAIN_CHECK
}

static bool map_dispose_hook_hit;
static wl_columnar_source_access_reader_t map_dispose_reader;

static void
map_dispose_refusal_hook(eval_stack_t *stack, eval_entry_t *entry)
{
    col_rel_t *heap = NULL;
    (void)stack;
    if (map_dispose_hook_hit || !entry->owned || !entry->rel)
        return;
    if (col_rel_deep_copy(entry->rel, &heap, NULL) != 0)
        return;
    col_rel_destroy(entry->rel);
    entry->rel = heap;
    map_dispose_hook_hit = true;
    (void)col_rel_source_reader_acquire(entry->rel, &map_dispose_reader);
}

static void
test_map_input_cleanup_retry(void)
{
    TEST("MAP retains reader-busy input for exact retry");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    uint32_t project[] = { 0 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_MAP, .project_count = 1,
          .project_indices = project }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    int64_t values[] = { 7, 42 };
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
#define MAP_CHECK(c, m) do { if (!(c)) { failure = (m); goto map_cleanup; \
                             } } while (0)
    map_dispose_hook_hit = false;
    memset(&map_dispose_reader, 0, sizeof(map_dispose_reader));
#ifdef WL_SESSION_TEST_HOOKS
    wl_columnar_ops_test_before_map_dispose = map_dispose_refusal_hook;
#endif
    MAP_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &session) == 0,
        "MAP session");
    MAP_CHECK(wl_session_insert(session, "input", values, 2, 1) == 0,
        "MAP input");
    MAP_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == EBUSY
        && tuples.count == 0 && map_dispose_hook_hit,
        "MAP refusal");
    MAP_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == EBUSY
        && tuples.count == 0, "MAP repeated refusal");
    MAP_CHECK(col_rel_source_reader_release(&map_dispose_reader) == 0,
        "MAP reader release");
    memset(&tuples, 0, sizeof(tuples));
    MAP_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
        && tuples.count == 2
        && has_tuple(&tuples, "output", (int64_t[]){ 7 }, 1)
        && has_tuple(&tuples, "output", (int64_t[]){ 42 }, 1),
        "MAP retry");
map_cleanup:
#ifdef WL_SESSION_TEST_HOOKS
    wl_columnar_ops_test_before_map_dispose = NULL;
#endif
    if (map_dispose_reader.owner)
        (void)col_rel_source_reader_release(&map_dispose_reader);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef MAP_CHECK
}
#endif

static void
test_map_entry_storage_modes(void)
{
    TEST("MAP preserves heap/pool/arena entries and lower metadata");
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output" };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    wl_plan_op_t map_op = { .op = WL_PLAN_OP_MAP, .project_count = 1 };
    wl_session_t *session = NULL;
    const char *failure = NULL;
    eval_stack_t stack;
    eval_stack_init(&stack);
    wl_columnar_source_access_reader_t active_reader = { 0 };
    col_rel_t *unstacked_input = NULL;
    col_rel_t *unowned_cleanup = NULL;
#define STORAGE_CHECK(c, m) \
        do { if (!(c)) { failure = (m); goto map_storage_cleanup; } } while (0)
    STORAGE_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "MAP storage session");
    wl_col_session_t *coord = COL_SESSION(session);
    for (unsigned mode = 0; mode < 3; mode++) {
        eval_stack_init(&stack);
        memset(&active_reader, 0, sizeof(active_reader));
        unstacked_input = NULL;
        col_rel_t *input = mode == 0 ? col_rel_new_auto("heap", 1)
            : col_rel_pool_new_auto(coord->delta_pool,
                mode == 2 ? coord->eval_arena : NULL, "owned", 1);
        STORAGE_CHECK(input, "MAP storage relation");
        unstacked_input = input;
        int64_t value = 42;
        STORAGE_CHECK(col_rel_append_row(input, &value) == 0,
            "MAP storage rows");
        for (unsigned lower_idx = 0; lower_idx < COL_STACK_MAX - 1;
            lower_idx++) {
            col_rel_t *lower = col_rel_new_auto("lower", 1);
            int64_t lower_value = (int64_t)lower_idx;
            STORAGE_CHECK(lower, "MAP storage lower relation");
            if (col_rel_append_row(lower, &lower_value) != 0
                || eval_stack_push(&stack, lower, true) != 0) {
                col_rel_destroy(lower);
                failure = "MAP storage stack";
                goto map_storage_cleanup;
            }
            if (lower_idx == 0) {
                stack.items[0].seg_boundaries =
                    malloc(2 * sizeof(uint32_t));
                STORAGE_CHECK(stack.items[0].seg_boundaries,
                    "MAP lower metadata");
                stack.items[0].seg_count = 1;
                stack.items[0].seg_boundaries[0] = 0;
                stack.items[0].seg_boundaries[1] = 1;
            }
        }
        STORAGE_CHECK(eval_stack_push(&stack, input, true) == 0,
            "MAP storage input stack");
        unstacked_input = NULL;
        stack.items[COL_STACK_MAX - 1].seg_boundaries =
            malloc(3 * sizeof(uint32_t));
        STORAGE_CHECK(stack.items[COL_STACK_MAX - 1].seg_boundaries,
            "MAP input metadata");
        stack.items[COL_STACK_MAX - 1].seg_count = 2;
        stack.items[COL_STACK_MAX - 1].seg_boundaries[0] = 0;
        stack.items[COL_STACK_MAX - 1].seg_boundaries[1] = 1;
        stack.items[COL_STACK_MAX - 1].seg_boundaries[2] = 1;
        uint32_t *input_segments =
            stack.items[COL_STACK_MAX - 1].seg_boundaries;
        uint32_t *lower_segments = stack.items[0].seg_boundaries;
        STORAGE_CHECK(col_rel_source_reader_acquire(input, &active_reader) == 0,
            "MAP storage reader");
        col_rel_t *held = input;
        STORAGE_CHECK(col_op_map(&map_op, &stack, coord) == EBUSY
            && stack.top == COL_STACK_MAX &&
            stack.items[COL_STACK_MAX - 1].rel == held
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == input_segments
            && stack.items[COL_STACK_MAX - 1].seg_count == 2
            && stack.items[COL_STACK_MAX - 1].seg_boundaries[1] == 1
            && stack.items[0].seg_boundaries == lower_segments,
            "MAP storage refusal");
        STORAGE_CHECK(col_op_map(&map_op, &stack, coord) == EBUSY
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel == held
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == input_segments,
            "MAP storage repeated refusal");
        STORAGE_CHECK(col_rel_source_reader_release(&active_reader) == 0,
            "MAP storage release");
        STORAGE_CHECK(col_op_map(&map_op, &stack, coord) == 0
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel != held
            && stack.items[COL_STACK_MAX - 1].rel->nrows == 1
            && col_rel_get(stack.items[COL_STACK_MAX - 1].rel, 0, 0) == 42,
            "MAP storage retry");
        STORAGE_CHECK(eval_stack_drain(&stack) == 0, "MAP storage drain");
    }
    eval_stack_init(&stack);
    col_rel_t *borrowed = col_rel_new_auto("borrowed", 1);
    STORAGE_CHECK(borrowed, "MAP borrowed relation");
    unowned_cleanup = borrowed;
    int64_t borrowed_value = 99;
    STORAGE_CHECK(col_rel_append_row(borrowed, &borrowed_value) == 0
        && eval_stack_push(&stack, borrowed, false) == 0,
        "MAP borrowed stack");
    stack.items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
    STORAGE_CHECK(stack.items[0].seg_boundaries, "MAP borrowed metadata");
    stack.items[0].seg_count = 1;
    stack.items[0].seg_boundaries[0] = 0;
    stack.items[0].seg_boundaries[1] = 1;
    STORAGE_CHECK(col_rel_source_reader_acquire(borrowed, &active_reader) == 0,
        "MAP borrowed reader");
    STORAGE_CHECK(col_op_map(&map_op, &stack, coord) == 0
        && stack.top == 1 && stack.items[0].rel->nrows == 1
        && col_rel_get(stack.items[0].rel, 0, 0) == 99,
        "MAP borrowed cleanup");
    STORAGE_CHECK(col_rel_source_reader_release(&active_reader) == 0,
        "MAP borrowed release");
    col_rel_destroy(borrowed);
    unowned_cleanup = NULL;
    STORAGE_CHECK(eval_stack_drain(&stack) == 0, "MAP borrowed drain");
    eval_stack_init(&stack);
    col_rel_t *error_input = col_rel_new_auto("error_input", 1);
    STORAGE_CHECK(error_input, "MAP error relation");
    unstacked_input = error_input;
    int64_t error_value = 11;
    STORAGE_CHECK(col_rel_append_row(error_input, &error_value) == 0
        && eval_stack_push(&stack, error_input, true) == 0,
        "MAP error stack");
    unstacked_input = NULL;
    uint8_t bad_expr[] = { WL_PLAN_EXPR_EXTENSION_CALL, 0 };
    wl_plan_expr_buffer_t map_expr = { bad_expr, sizeof(bad_expr) };
    wl_plan_op_t error_op = { .op = WL_PLAN_OP_MAP, .project_count = 1,
                              .map_exprs = &map_expr, .map_expr_count = 1 };
    STORAGE_CHECK(col_rel_source_reader_acquire(error_input,
        &active_reader) == 0,
        "MAP error reader");
    STORAGE_CHECK(col_op_map(&error_op, &stack, coord) == EBUSY
        && stack.top == 1 && stack.items[0].rel == error_input,
        "MAP error refusal precedence");
    STORAGE_CHECK(col_rel_source_reader_release(&active_reader) == 0,
        "MAP error release");
    STORAGE_CHECK(col_op_map(&error_op, &stack, coord)
        == WL_COLUMNAR_EXPR_EXTENSION_MALFORMED && stack.top == 0,
        "MAP error retry");
#ifdef WL_SESSION_TEST_HOOKS
    eval_stack_init(&stack);
    col_rel_t *alloc_input = col_rel_new_auto("alloc_input", 1);
    STORAGE_CHECK(alloc_input, "MAP allocation relation");
    unstacked_input = alloc_input;
    STORAGE_CHECK(col_rel_append_row(alloc_input, &error_value) == 0
        && eval_stack_push(&stack, alloc_input, true) == 0,
        "MAP allocation stack");
    unstacked_input = NULL;
    STORAGE_CHECK(col_rel_source_reader_acquire(alloc_input,
        &active_reader) == 0, "MAP allocation reader");
    wl_columnar_ops_test_map_fail_output_alloc = true;
    wl_plan_op_t alloc_op = { .op = WL_PLAN_OP_MAP, .project_count = 1 };
    STORAGE_CHECK(col_op_map(&alloc_op, &stack, coord) == EBUSY
        && stack.top == 1 && stack.items[0].rel == alloc_input,
        "MAP allocation refusal precedence");
    STORAGE_CHECK(col_rel_source_reader_release(&active_reader) == 0,
        "MAP allocation release");
    STORAGE_CHECK(col_op_map(&alloc_op, &stack, coord) == ENOMEM
        && stack.top == 0, "MAP allocation retry");
    wl_columnar_ops_test_map_fail_output_alloc = false;
#endif
map_storage_cleanup:
#ifdef WL_SESSION_TEST_HOOKS
    wl_columnar_ops_test_map_fail_output_alloc = false;
#endif
    if (active_reader.owner)
        (void)col_rel_source_reader_release(&active_reader);
    (void)eval_stack_drain(&stack);
    if (unstacked_input)
        col_rel_destroy(unstacked_input);
    if (unowned_cleanup)
        col_rel_destroy(unowned_cleanup);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef STORAGE_CHECK
}

#ifdef WL_SESSION_TEST_HOOKS
static bool reduce_dispose_hook_hit;
static wl_columnar_source_access_reader_t reduce_dispose_reader;

static void
reduce_dispose_refusal_hook(eval_stack_t *stack, eval_entry_t *entry)
{
    col_rel_t *heap = NULL;
    (void)stack;
    if (reduce_dispose_hook_hit || !entry->owned || !entry->rel)
        return;
    if (col_rel_deep_copy(entry->rel, &heap, NULL) != 0)
        return;
    col_rel_destroy(entry->rel);
    entry->rel = heap;
    reduce_dispose_hook_hit = true;
    (void)col_rel_source_reader_acquire(entry->rel, &reduce_dispose_reader);
}

static void
test_reduce_input_cleanup_retry(void)
{
    TEST("REDUCE retains reader-busy input for exact retry");
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_REDUCE, .aggregate_index = 0,
          .agg_fn = WIRELOG_AGG_COUNT }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = 3 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    int64_t values[] = { 7, 42 };
    tuple_collector_t tuples = { 0 };
    const char *failure = NULL;
#define REDUCE_CHECK(c, m) \
        do { if (!(c)) { failure = (m); goto reduce_cleanup; } } while (0)
    reduce_dispose_hook_hit = false;
    memset(&reduce_dispose_reader, 0, sizeof(reduce_dispose_reader));
    wl_columnar_ops_test_before_reduce_dispose = reduce_dispose_refusal_hook;
    REDUCE_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0,
        "REDUCE session");
    REDUCE_CHECK(wl_session_insert(session, "input", values, 2, 1) == 0,
        "REDUCE input");
    REDUCE_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == EBUSY
        && tuples.count == 0 && reduce_dispose_hook_hit,
        "REDUCE refusal");
    REDUCE_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == EBUSY
        && tuples.count == 0, "REDUCE repeated refusal");
    REDUCE_CHECK(col_rel_source_reader_release(&reduce_dispose_reader) == 0,
        "REDUCE reader release");
    memset(&tuples, 0, sizeof(tuples));
    REDUCE_CHECK(wl_session_snapshot(session, collect_tuple, &tuples) == 0
        && tuples.count == 1 && has_tuple(&tuples, "output",
        (int64_t[]){ 2 }, 1), "REDUCE retry");
reduce_cleanup:
    wl_columnar_ops_test_before_reduce_dispose = NULL;
    if (reduce_dispose_reader.owner)
        (void)col_rel_source_reader_release(&reduce_dispose_reader);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef REDUCE_CHECK
}

static void
test_reduce_entry_storage_modes(void)
{
    TEST("REDUCE preserves heap/pool/arena entries and error precedence");
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output" };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    wl_plan_op_t reduce_op = { .op = WL_PLAN_OP_REDUCE,
                               .aggregate_index = 0,
                               .agg_fn = WIRELOG_AGG_COUNT };
    wl_session_t *session = NULL;
    const char *failure = NULL;
    eval_stack_t stack;
    eval_stack_init(&stack);
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *unstacked = NULL, *unowned = NULL;
#define REDUCE_STORAGE_CHECK(c, m) \
        do { if (!(c)) { failure = (m); goto reduce_storage_cleanup; \
             } } while (0)
    REDUCE_STORAGE_CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
        &session) == 0, "REDUCE storage session");
    wl_col_session_t *coord = COL_SESSION(session);
    for (unsigned mode = 0; mode < 3; mode++) {
        eval_stack_init(&stack);
        memset(&reader, 0, sizeof(reader));
        unstacked = NULL;
        col_rel_t *input = mode == 0 ? col_rel_new_auto("heap", 1)
            : col_rel_pool_new_auto(coord->delta_pool,
                mode == 2 ? coord->eval_arena : NULL, "owned", 1);
        REDUCE_STORAGE_CHECK(input, "REDUCE storage relation");
        unstacked = input;
        int64_t value = 42;
        REDUCE_STORAGE_CHECK(col_rel_append_row(input, &value) == 0,
            "REDUCE storage row");
        for (unsigned lower_idx = 0; lower_idx < COL_STACK_MAX - 1;
            lower_idx++) {
            col_rel_t *lower = col_rel_new_auto("lower", 1);
            int64_t lower_value = (int64_t)lower_idx;
            REDUCE_STORAGE_CHECK(lower, "REDUCE lower relation");
            if (col_rel_append_row(lower, &lower_value) != 0
                || eval_stack_push(&stack, lower, true) != 0) {
                col_rel_destroy(lower);
                failure = "REDUCE lower stack";
                goto reduce_storage_cleanup;
            }
            if (lower_idx == 0) {
                stack.items[0].seg_boundaries =
                    malloc(2 * sizeof(uint32_t));
                REDUCE_STORAGE_CHECK(stack.items[0].seg_boundaries,
                    "REDUCE lower metadata");
                stack.items[0].seg_count = 1;
                stack.items[0].seg_boundaries[0] = 0;
                stack.items[0].seg_boundaries[1] = 1;
            }
        }
        REDUCE_STORAGE_CHECK(eval_stack_push(&stack, input, true) == 0,
            "REDUCE input stack");
        unstacked = NULL;
        stack.items[COL_STACK_MAX - 1].seg_boundaries =
            malloc(3 * sizeof(uint32_t));
        REDUCE_STORAGE_CHECK(stack.items[COL_STACK_MAX - 1].seg_boundaries,
            "REDUCE input metadata");
        stack.items[COL_STACK_MAX - 1].seg_count = 2;
        stack.items[COL_STACK_MAX - 1].seg_boundaries[0] = 0;
        stack.items[COL_STACK_MAX - 1].seg_boundaries[1] = 1;
        stack.items[COL_STACK_MAX - 1].seg_boundaries[2] = 1;
        uint32_t *input_segments = stack.items[COL_STACK_MAX -
                1].seg_boundaries;
        REDUCE_STORAGE_CHECK(col_rel_source_reader_acquire(input, &reader) == 0,
            "REDUCE storage reader");
        col_rel_t *held = input;
        REDUCE_STORAGE_CHECK(col_op_reduce(&reduce_op, &stack, coord) == EBUSY
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel == held
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == input_segments,
            "REDUCE storage refusal");
        REDUCE_STORAGE_CHECK(col_op_reduce(&reduce_op, &stack, coord) == EBUSY
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel == held,
            "REDUCE storage repeated refusal");
        REDUCE_STORAGE_CHECK(col_rel_source_reader_release(&reader) == 0,
            "REDUCE storage release");
        REDUCE_STORAGE_CHECK(col_op_reduce(&reduce_op, &stack, coord) == 0
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel != held
            && col_rel_get(stack.items[COL_STACK_MAX - 1].rel, 0, 0) == 1,
            "REDUCE storage retry");
        REDUCE_STORAGE_CHECK(eval_stack_drain(&stack) == 0,
            "REDUCE storage drain");
    }
    eval_stack_init(&stack);
    col_rel_t *borrowed = col_rel_new_auto("borrowed", 1);
    REDUCE_STORAGE_CHECK(borrowed, "REDUCE borrowed relation");
    unowned = borrowed;
    int64_t borrowed_value = 99;
    REDUCE_STORAGE_CHECK(col_rel_append_row(borrowed, &borrowed_value) == 0
        && eval_stack_push(&stack, borrowed, false) == 0,
        "REDUCE borrowed stack");
    stack.items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
    REDUCE_STORAGE_CHECK(stack.items[0].seg_boundaries,
        "REDUCE borrowed metadata");
    stack.items[0].seg_count = 1;
    stack.items[0].seg_boundaries[0] = 0;
    stack.items[0].seg_boundaries[1] = 1;
    REDUCE_STORAGE_CHECK(col_rel_source_reader_acquire(borrowed, &reader) == 0,
        "REDUCE borrowed reader");
    REDUCE_STORAGE_CHECK(col_op_reduce(&reduce_op, &stack, coord) == 0
        && stack.top == 1 && col_rel_get(stack.items[0].rel, 0, 0) == 1,
        "REDUCE borrowed cleanup");
    REDUCE_STORAGE_CHECK(col_rel_source_reader_release(&reader) == 0,
        "REDUCE borrowed release");
    col_rel_destroy(borrowed);
    unowned = NULL;
    REDUCE_STORAGE_CHECK(eval_stack_drain(&stack) == 0,
        "REDUCE borrowed drain");
    eval_stack_init(&stack);
    col_rel_t *error_input = col_rel_new_auto("error", 1);
    REDUCE_STORAGE_CHECK(error_input, "REDUCE error relation");
    unstacked = error_input;
    REDUCE_STORAGE_CHECK(col_rel_append_row(error_input, &borrowed_value) == 0
        && eval_stack_push(&stack, error_input, true) == 0,
        "REDUCE error stack");
    unstacked = NULL;
    uint8_t bad_expr[] = { WL_PLAN_EXPR_EXTENSION_CALL, 0 };
    wl_plan_expr_buffer_t expr = { bad_expr, sizeof(bad_expr) };
    wl_plan_op_t error_op = reduce_op;
    error_op.agg_fn = WIRELOG_AGG_SUM;
    error_op.agg_expr = expr;
    REDUCE_STORAGE_CHECK(col_rel_source_reader_acquire(error_input,
        &reader) == 0,
        "REDUCE error reader");
    REDUCE_STORAGE_CHECK(col_op_reduce(&error_op, &stack, coord) == EBUSY
        && stack.top == 1, "REDUCE error refusal");
    REDUCE_STORAGE_CHECK(col_rel_source_reader_release(&reader) == 0,
        "REDUCE error release");
    int reduce_error_rc = col_op_reduce(&error_op, &stack, coord);
    REDUCE_STORAGE_CHECK(reduce_error_rc
        == ERANGE && stack.top == 0,
        "REDUCE error retry");
#ifdef WL_SESSION_TEST_HOOKS
    eval_stack_init(&stack);
    col_rel_t *alloc_input = col_rel_new_auto("alloc", 1);
    REDUCE_STORAGE_CHECK(alloc_input, "REDUCE allocation relation");
    unstacked = alloc_input;
    REDUCE_STORAGE_CHECK(col_rel_append_row(alloc_input, &borrowed_value) == 0
        && eval_stack_push(&stack, alloc_input, true) == 0,
        "REDUCE allocation stack");
    unstacked = NULL;
    REDUCE_STORAGE_CHECK(col_rel_source_reader_acquire(alloc_input,
        &reader) == 0,
        "REDUCE allocation reader");
    wl_columnar_ops_test_reduce_fail_output_alloc = true;
    REDUCE_STORAGE_CHECK(col_op_reduce(&reduce_op, &stack, coord) == EBUSY
        && stack.top == 1, "REDUCE allocation refusal");
    REDUCE_STORAGE_CHECK(col_rel_source_reader_release(&reader) == 0,
        "REDUCE allocation release");
    REDUCE_STORAGE_CHECK(col_op_reduce(&reduce_op, &stack, coord) == ENOMEM
        && stack.top == 0, "REDUCE allocation retry");
#endif
reduce_storage_cleanup:
#ifdef WL_SESSION_TEST_HOOKS
    wl_columnar_ops_test_reduce_fail_output_alloc = false;
#endif
    if (reader.owner)
        (void)col_rel_source_reader_release(&reader);
    (void)eval_stack_drain(&stack);
    if (unstacked)
        col_rel_destroy(unstacked);
    if (unowned)
        col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef REDUCE_STORAGE_CHECK
}
#endif

static wl_atomic_u64 promotion_worker_mask;
static wl_atomic_u64 promotion_worker_error;

/* Observe the actual worker result before publication, and attach a governed
 * timestamp allocation to model the managed FILTER caller migration. */
static void
make_worker_filter_result_governed(wl_col_session_t *sess,
    eval_stack_t *stack, eval_entry_t *result)
{
    (void)stack;
    if (!sess->coordinator || !result->rel || !result->owned)
        return;
    if (!result->rel->pool_owned
        || col_rel_attach_memory_governor(result->rel,
        sess->memory_governor) != 0
        || col_rel_enable_timestamps(result->rel) != 0
        || result->rel->retained_reserved_bytes == 0)
        atomic_store_explicit(&promotion_worker_error, 1, memory_order_release);
    record_actual_worker(&promotion_worker_mask, sess->worker_id);
}

static void
test_governed_worker_filter_publication(uint32_t workers)
{
    atomic_store_explicit(&promotion_worker_mask, 0, memory_order_release);
    atomic_store_explicit(&promotion_worker_error, 0, memory_order_release);
    wl_columnar_eval_serial_test_after_plan =
        make_worker_filter_result_governed;
    test_final_normalization(workers, 1, 5);
    wl_columnar_eval_serial_test_after_plan = NULL;
    TEST("actual governed worker FILTER publication route");
    if (atomic_load_explicit(&promotion_worker_error, memory_order_acquire) != 0
        || atomic_load_explicit(&promotion_worker_mask, memory_order_acquire)
        != ((UINT64_C(1) << workers) - 1)) {
        FAIL("governed worker publication witness");
        return;
    }
    PASS();
}

static void
test_governed_pool_publication(unsigned mode)
{
    TEST("governed pool publication transfers and restores token ownership");
    const char *failure = NULL;
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor(1u << 20);
    delta_pool_t *pool = NULL;
    col_rel_t *source = col_rel_new_auto("source", 1), *candidate = NULL;
    col_rel_t *old = NULL;
    wl_col_session_t sess = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    bool held = false, published = false;
#define PROMOTE_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; \
                                 } } while (0)
    PROMOTE_CHECK(ref && source, "setup");
    pool = delta_pool_create_managed(2, sizeof(col_rel_t), 64,
            wl_columnar_memory_governor_ref_get(ref));
    PROMOTE_CHECK(pool, "managed pool");
    candidate = wl_columnar_relation_pool_new_like_governed(pool, "output",
            source, ref);
    int64_t row = 42;
    PROMOTE_CHECK(candidate && candidate->pool_owned
        && col_rel_append_row(candidate, &row) == 0
        && col_rel_enable_timestamps(candidate) == 0, "governed result");
    candidate->timestamps[0] = (col_delta_timestamp_t){
        .iteration = 11, .stratum = 12, .worker = 13, .multiplicity = -3
    };
    uint64_t bytes = candidate->retained_reserved_bytes;
    uint64_t reserved = reserved_on(ref);
    uint64_t identity = candidate->relation_identity;
    int64_t **columns = candidate->columns;
    col_delta_timestamp_t *timestamps = candidate->timestamps;
    if (mode == 1) {
        old = col_rel_new_auto("output", 1);
        PROMOTE_CHECK(old && session_add_rel(&sess, old) == 0,
            "old registration");
        PROMOTE_CHECK(col_rel_source_reader_acquire(old, &reader) == 0,
            "old reader");
        held = true;
    }
#ifdef WL_TEST_ALLOC_WRAP
    if (mode == 2) fail_calloc_size = sizeof(col_rel_t);
    if (mode == 3) fail_realloc_size = 16 * sizeof(col_rel_t *);
#endif
    if (mode == 4)
        atomic_store_explicit(&candidate->retained_reservation.state,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING, memory_order_release);
    int rc = session_add_rel(&sess, candidate);
    if (mode == 4)
        atomic_store_explicit(&candidate->retained_reservation.state,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    if (mode != 0) {
        PROMOTE_CHECK(rc == ((mode == 1 || mode == 4) ? EBUSY : ENOMEM),
            "publication failure");
#ifdef WL_TEST_ALLOC_WRAP
        PROMOTE_CHECK(fail_calloc_size == 0 && fail_realloc_size == 0,
            "fault hit");
#endif
        PROMOTE_CHECK(candidate->pool_owned && candidate->columns == columns
            && candidate->timestamps == timestamps
            && candidate->relation_identity == identity
            && candidate->retained_reserved_bytes == bytes
            && candidate->retained_reservation.identity ==
            &candidate->retained_reservation
            && atomic_load_explicit(&candidate->retained_reservation.owner_bits,
            memory_order_acquire) == (uintptr_t)candidate
            && candidate->retained_reservation.bytes == bytes
            && reserved_on(ref) == reserved,
            "rollback retains owner and charge");
        if (held) {
            PROMOTE_CHECK(session_add_rel(&sess, candidate) == EBUSY
                && reserved_on(ref) == reserved, "repeated refusal");
            PROMOTE_CHECK(col_rel_source_reader_release(&reader) == 0,
                "release");
            held = false;
        }
        rc = session_add_rel(&sess, candidate);
    }
    PROMOTE_CHECK(rc == 0, "publication retry");
    published = true;
    col_rel_t *result = session_find_rel(&sess, "output");
    PROMOTE_CHECK(result && result != candidate && !result->pool_owned
        && result->memory_governor == ref && result->columns == columns
        && result->timestamps == timestamps &&
        result->relation_identity == identity
        && result->retained_reservation.identity ==
        &result->retained_reservation
        && atomic_load_explicit(&result->retained_reservation.owner_bits,
        memory_order_acquire) == (uintptr_t)result
        && result->retained_reserved_bytes == bytes
        && reserved_on(ref) == reserved
        && candidate->memory_governor == NULL
        && candidate->retained_reserved_bytes == 0
        && result->timestamps[0].multiplicity == -3,
        "heap owns unchanged payload and token");
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    fail_calloc_size = fail_realloc_size = 0;
#endif
    if (held) (void)col_rel_source_reader_release(&reader);
    if (!published) col_rel_destroy(candidate);
    for (uint32_t i = 0; i < sess.nrels; i++) col_rel_destroy(sess.rels[i]);
    free(sess.rels);
    free(sess.rel_hash_head);
    free(sess.rel_hash_next);
    col_rel_destroy(source);
    delta_pool_destroy(pool);
    if (ref) {
        if (reserved_on(ref) != 0) failure = "reservation leak";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef PROMOTE_CHECK
}

static void
test_filter_governed_admission(unsigned route, bool timestamped,
    unsigned phase, bool session_governor)
{
    TEST("FILTER managed allocation and held-input retry");
    const char *failure = NULL;
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor(1u << 20);
    wl_columnar_memory_governor_ref_t *other = enforcing_governor(1u << 20);
    wl_col_session_t sess = { 0 };
    col_rel_t *input = col_rel_new_auto("filter-managed", 1);
    col_rel_t *occupied = NULL;
    delta_pool_t *pool = NULL;
    eval_stack_t stack;
    wl_columnar_source_access_reader_t reader = { 0 };
    bool held = false, local_owner = true;
    eval_stack_init(&stack);
#define FILTER_CHECK(c, msg) do { if (!(c)) { failure = msg; goto cleanup; \
                                  } } while (0)
    FILTER_CHECK(ref && other && input, "setup");
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    for (uint32_t i = 0; i < COL_REL_INIT_CAP + 1; i++) {
        int64_t value = i + 1;
        FILTER_CHECK(col_rel_append_row(input, &value) == 0, "input rows");
    }
    if (timestamped) {
        FILTER_CHECK(col_rel_enable_timestamps(input) == 0, "input timestamps");
        for (uint32_t i = 0; i < input->nrows; i++)
            input->timestamps[i] = (col_delta_timestamp_t){
                .iteration = i + 10, .stratum = i + 20, .worker = i + 30,
                .multiplicity = i % 2 ? -3 : 5
            };
    }
    FILTER_CHECK(col_rel_attach_memory_governor(input,
        session_governor ? other : ref) == 0, "source governor");
    sess.memory_governor = session_governor ? ref : NULL;
    if (route) {
        pool = delta_pool_create_managed(8, sizeof(col_rel_t), 64, g);
        FILTER_CHECK(pool, "managed pool");
        if (route == 2) {
            /* Consume real slab slots, retaining only the last descriptor:
             * earlier empty descriptors are destroyed before the next alloc. */
            while (pool->slot_used < pool->slot_cap) {
                col_rel_destroy(occupied);
                occupied = col_rel_pool_new_like(pool, "occupied", input);
                FILTER_CHECK(occupied && occupied->pool_owned,
                    "pool exhaustion");
            }
        }
    }
    sess.delta_pool = pool;
    uint64_t baseline = reserved_on(ref);
    uint64_t payload = (uint64_t)COL_REL_INIT_CAP * sizeof(int64_t);
    uint64_t ts = (uint64_t)COL_REL_INIT_CAP * sizeof(col_delta_timestamp_t);
    uint64_t allowance = phase == 0 ? 0 : payload;
    if (phase >= 2 && timestamped) allowance += ts;
    if (phase == 3) allowance = 1u << 19;
    atomic_store_explicit(&g->usable_bytes, baseline + allowance,
        memory_order_release);
    FILTER_CHECK(col_rel_source_reader_acquire(input, &reader) == 0, "reader");
    held = true;
    FILTER_CHECK(eval_stack_push(&stack, input, true) == 0, "push");
    local_owner = false;
    stack.items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
    FILTER_CHECK(stack.items[0].seg_boundaries, "segments");
    stack.items[0].seg_boundaries[0] = 0;
    stack.items[0].seg_boundaries[1] = input->nrows;
    stack.items[0].seg_count = 1;
    uint32_t *segments = stack.items[0].seg_boundaries;
    uint64_t generation = input->view_generation;
    uint8_t expr[] = { WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0',
                       WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0, 0, 0,
                       WL_PLAN_EXPR_CMP_GT };
    wl_plan_op_t op = { .op = WL_PLAN_OP_FILTER,
                        .filter_expr = { expr, sizeof(expr) } };
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        FILTER_CHECK(wl_columnar_filter_op(&op, &stack, &sess) == EBUSY,
            "held input cleanup must refuse");
        FILTER_CHECK(stack.top == 1 && stack.items[0].rel == input
            && stack.items[0].owned && stack.items[0].seg_boundaries == segments
            && input->view_generation == generation,
            "refusal preserves input and segments");
        FILTER_CHECK(reserved_on(ref) == baseline, "private output released");
    }
    FILTER_CHECK(col_rel_source_reader_release(&reader) == 0, "release");
    held = false;
    atomic_store_explicit(&g->usable_bytes, 1u << 20, memory_order_release);
    FILTER_CHECK(wl_columnar_filter_op(&op, &stack, &sess) == 0,
        "retry succeeds");
    input = NULL;
    FILTER_CHECK(stack.top == 1 && stack.items[0].rel->memory_governor == ref,
        "effective governor on output");
    col_rel_t *out = stack.items[0].rel;
    FILTER_CHECK(out->pool_owned == (route == 1)
        && out->nrows == COL_REL_INIT_CAP + 1, "route and row count");
    for (uint32_t i = 0; i < out->nrows; i++) {
        FILTER_CHECK(out->columns[0][i] == i + 1, "exact rows");
        if (timestamped)
            FILTER_CHECK(out->timestamps &&
                out->timestamps[i].iteration == i + 10
                && out->timestamps[i].stratum == i + 20
                && out->timestamps[i].worker == i + 30
                && out->timestamps[i].multiplicity == (i % 2 ? -3 : 5),
                "exact timestamp record");
    }
cleanup:
    if (held) (void)col_rel_source_reader_release(&reader);
    (void)eval_stack_drain(&stack);
    if (local_owner) col_rel_destroy(input);
    col_rel_destroy(occupied);
    delta_pool_destroy(pool);
    if (ref) {
        if (reserved_on(ref) != 0) failure = "reservation leak";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (other) {
        if (reserved_on(other) != 0) failure = "source governor leak";
        wl_columnar_memory_governor_ref_release(other);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef FILTER_CHECK
}

static unsigned serial_clone_denial;
static wl_atomic_u64 serial_clone_workers;
static wl_atomic_u64 serial_clone_bad_result;

static void
observe_serial_borrowed_clone(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack;
    if (!result->rel || result->owned
        || result->rel != session_find_rel(sess, "input"))
        atomic_store_explicit(&serial_clone_bad_result, 1,
            memory_order_release);
    if (sess->coordinator) {
        record_actual_worker(&serial_clone_workers, sess->worker_id);
        return;
    }
    if (serial_clone_denial) {
        idb_set_budget(sess); /* Frame is already admitted; target clone is next. */
        if (serial_clone_denial == 2) {
            wl_columnar_memory_governor_t *g =
                wl_columnar_memory_governor_ref_get(sess->memory_governor);
            atomic_store_explicit(&g->usable_bytes,
                reserved_on(sess->memory_governor)
                + (uint64_t)COL_REL_INIT_CAP * sizeof(int64_t),
                memory_order_release);
        }
    }
}

static void
test_serial_borrowed_clone_admission(uint32_t workers, unsigned denial,
    unsigned pool_route, bool source_fallback)
{
    TEST("serial borrowed publication retains governor and retries admission");
    wl_plan_op_t op = { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = &op, .op_count = 1 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1 };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    int64_t *rows = NULL;
    wl_columnar_memory_governor_ref_t *source_ref = NULL;
    col_rel_t *unowned = NULL;
    delta_pool_t *saved_pool = NULL;
    bool pool_detached = false;
    wl_columnar_source_access_writer_t writer = { 0 };
    bool writer_held = false;
    const char *failure = NULL;
#define CLONE_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; \
                               } } while (0)
    CLONE_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "session");
    wl_col_session_t *sess = COL_SESSION(session);
    ref = sess->memory_governor;
    wl_columnar_memory_governor_ref_retain(ref);
    uint32_t count = workers == 1 ? COL_REL_INIT_CAP + 1 : 65536;
    rows = malloc((size_t)count * sizeof(*rows));
    CLONE_CHECK(rows, "rows");
    for (uint32_t i = 0; i < count; i++) rows[i] = 42;
    if (workers == 1) {
        source_ref = enforcing_governor(1u << 20);
        unowned = col_rel_new_auto("input", 1);
        CLONE_CHECK(source_ref && unowned, "independent source governor");
        for (uint32_t i = 0; i < count; i++)
            CLONE_CHECK(col_rel_append_row(unowned, &rows[i]) == 0,
                "source rows");
        CLONE_CHECK(col_rel_attach_memory_governor(unowned, source_ref) == 0
            && session_add_rel(sess, unowned) == 0, "source registration");
        unowned = NULL;
    } else
        CLONE_CHECK(wl_session_insert(session, "input", rows, count, 1) == 0,
            "input");

    col_rel_t *input = session_find_rel(sess, "input");
    int64_t **columns = input->columns;
    uint64_t generation = input->view_generation;
    if (pool_route == 1) {
        saved_pool = sess->delta_pool;
        sess->delta_pool = NULL;
        pool_detached = true;
    } else if (pool_route == 2) {
        CLONE_CHECK(sess->delta_pool, "pool exists");
        while (sess->delta_pool->slot_used < sess->delta_pool->slot_cap) {
            col_rel_t *filler = col_rel_pool_new_like(sess->delta_pool, "fill",
                    input);
            bool pooled = filler && filler->pool_owned;
            col_rel_destroy(filler);
            CLONE_CHECK(pooled, "real pool exhaustion");
        }
    }
    uint64_t baseline = reserved_on(ref);

    serial_clone_denial = denial;
    atomic_store_explicit(&serial_clone_workers, 0, memory_order_release);
    atomic_store_explicit(&serial_clone_bad_result, 0, memory_order_release);
    wl_columnar_eval_serial_test_after_plan = observe_serial_borrowed_clone;
    if (denial) {
        for (unsigned attempt = 0; attempt < 2; attempt++) {
            int rc = wl_columnar_eval_serial_framed_relation(&output, sess,
                    false);
            idb_restore_budget(sess);
            CLONE_CHECK(rc == ENOMEM && !session_find_rel(sess, "output")
                && input->columns == columns &&
                input->view_generation == generation
                && input->nrows == count && !sess->cleanup_active
                && !sess->cleanup_pending && reserved_on(ref) == baseline,
                "clone or growth refusal preserves source and frame ownership");
        }
    }
    serial_clone_denial = 0;
    if (workers == 1) {
        CLONE_CHECK(col_rel_source_writer_acquire(input, &writer) == 0,
            "source writer");
        writer_held = true;
        CLONE_CHECK(wl_columnar_eval_serial_framed_relation(&output, sess,
            false) == EBUSY
            && !session_find_rel(sess,
            "output") && reserved_on(ref) == baseline,
            "source writer refuses metadata snapshot");
        CLONE_CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
            "writer release");
        writer_held = false;
        if (source_fallback) sess->memory_governor = NULL;
    }
    int rc
        = workers == 1
        ? wl_columnar_eval_serial_framed_relation(&output, sess, false)
        : wl_session_step(session);
    wl_columnar_eval_serial_test_after_plan = NULL;
    CLONE_CHECK(rc == 0 && atomic_load_explicit(&serial_clone_bad_result,
        memory_order_acquire) == 0, "borrowed route and retry");
    col_rel_t *published = session_find_rel(sess, "output");
    CLONE_CHECK(published && published->nrows == (workers == 1 ? count : 1)
        && published->columns[0][0] == 42, "exact publication rows");
    if (workers == 1)
        CLONE_CHECK(published->memory_governor ==
            (source_fallback ? source_ref : ref)
            && published->retained_reserved_bytes > 0,
            "published clone remains governed");
    else
        CLONE_CHECK(atomic_load_explicit(&serial_clone_workers,
            memory_order_acquire) == ((UINT64_C(1) << workers) - 1),
            "actual worker borrowed-plan dispatch");
    if (workers == 1)
        for (uint32_t i = 0; i < count; i++)
            CLONE_CHECK(published->columns[0][i] == rows[i],
                "all published rows");
cleanup:
    if (writer_held) (void)wl_columnar_source_access_writer_release(&writer);
    if (session) {
        COL_SESSION(session)->memory_governor = ref;
        if (pool_detached) COL_SESSION(session)->delta_pool = saved_pool;
    }
    col_rel_destroy(unowned);
    wl_columnar_eval_serial_test_after_plan = NULL;
    serial_clone_denial = 0;
    if (session) idb_restore_budget(COL_SESSION(session));
    free(rows);
    wl_session_destroy(session);
    if (ref) {
        if (reserved_on(ref) != 0) failure = "clone reservation leak";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (source_ref) {
        if (reserved_on(source_ref) !=
            0) failure = "source governor reservation leak";
        wl_columnar_memory_governor_ref_release(source_ref);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef CLONE_CHECK
}

int
main(void)
{
    for (unsigned route = 0; route < 3; route++)
        for (unsigned fallback = 0; fallback < 2; fallback++)
            test_serial_borrowed_clone_admission(1, 0, route, fallback != 0);
    test_serial_borrowed_clone_admission(1, 1, 0, false);
    test_serial_borrowed_clone_admission(1, 2, 0, false);
    test_serial_borrowed_clone_admission(2, 0, 0, false);
    test_serial_borrowed_clone_admission(8, 0, 0, false);
    for (unsigned route = 0; route < 3; route++)
        for (unsigned ts = 0; ts < 2; ts++)
            for (unsigned phase = 0; phase < 4; phase++)
                for (unsigned sg = 0; sg < 2; sg++)
                    test_filter_governed_admission(route, ts != 0, phase,
                        sg != 0);
    test_governed_worker_filter_publication(2);
    test_governed_worker_filter_publication(8);
    test_governed_pool_publication(0);
    test_governed_pool_publication(1);
    test_governed_pool_publication(4);
#ifdef WL_TEST_ALLOC_WRAP
    test_governed_pool_publication(2);
    test_governed_pool_publication(3);
#endif
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
    test_kfusion_retained_pool_entry_retry();
    test_map_input_cleanup_retry();
#endif
    test_map_entry_storage_modes();
#ifdef WL_SESSION_TEST_HOOKS
    test_reduce_entry_storage_modes();
#endif
#ifdef WL_SESSION_TEST_HOOKS
    test_session_destroy_orders_worker_retirement();
    test_reduce_input_cleanup_retry();
#endif
    test_session_full_idb_clear_reader_exclusion();
#ifdef WL_TEST_ALLOC_WRAP
    test_delta_snapshot_capture_failure(0);
    test_delta_snapshot_capture_failure(1);
#endif
#ifdef WL_SESSION_TEST_HOOKS
    test_persistent_delta_rollback(false);
    test_persistent_delta_rollback(true);
    test_delta_rollback_preserves_progress(false, false);
    test_delta_rollback_preserves_progress(false, true);
    test_delta_rollback_preserves_progress(true, false);
#endif
    test_delta_rollback_frontier_gc(false);
    test_delta_rollback_frontier_gc(true);
    test_delta_rollback_timestamp_charge();
    test_delta_alias_rollback(false);
    test_delta_alias_rollback(true);
    test_pending_cleanup_synchronous_destroy();
    test_serial_cleanup_admission_and_failures(false, 1);
    test_serial_cleanup_admission_and_failures(false, 2);
    test_serial_cleanup_admission_and_failures(true, 1);
    test_serial_cleanup_admission_and_failures(true, 8);
    test_serial_staging_metadata();
    test_serial_cleanup_caller(0, false, false, 0, 1);
    test_serial_cleanup_caller(1, false, false, 0, 1);
    test_serial_cleanup_caller(0, true, false, 0, 1);
    test_serial_cleanup_caller(1, true, false, 0, 1);
    test_serial_cleanup_caller(2, true, false, 0, 1);
    test_serial_cleanup_caller(1, false, true, 0, 1);
    test_serial_cleanup_caller(0, false, false, 1, 1);
    test_serial_cleanup_caller(1, false, false, 1, 1);
    test_serial_cleanup_caller(0, true, false, 1, 1);
    test_serial_cleanup_caller(1, true, false, 1, 1);
    test_serial_cleanup_caller(2, true, false, 1, 1);
    test_serial_cleanup_caller(1, false, false, 2, 1);
    test_serial_cleanup_caller(1, false, false, 3, 1);
    test_serial_cleanup_caller(1, false, false, 4, 1);
    test_serial_cleanup_caller(0, false, false, 5, 1);
    test_serial_cleanup_caller(1, false, false, 5, 1);
    test_serial_cleanup_caller(0, true, false, 5, 1);
    test_serial_cleanup_caller(1, true, false, 5, 1);
    test_serial_cleanup_caller(2, true, false, 5, 1);
    test_serial_cleanup_caller(1, false, false, 6, 1);
    test_serial_cleanup_caller(1, false, false, 7, 1);
    test_serial_cleanup_caller(1, false, false, 8, 1);
    test_serial_cleanup_caller(1, false, false, 0, 2);
    test_serial_cleanup_caller(0, true, false, 0, 2);
    test_serial_cleanup_caller(1, true, false, 0, 8);
    test_serial_cleanup_caller(2, true, false, 0, 8);
    test_serial_cleanup_caller(1, false, false, 1, 2);
    test_serial_cleanup_caller(1, false, false, 2, 2);
    test_serial_cleanup_caller(1, false, false, 3, 8);
    test_serial_cleanup_caller(1, false, false, 4, 8);
    test_serial_cleanup_caller(1, false, false, 5, 2);
    test_serial_cleanup_caller(1, false, false, 6, 2);
    test_serial_cleanup_caller(1, false, false, 7, 8);
    test_serial_cleanup_caller(1, false, false, 8, 8);
    test_recursive_delta_operator_error();
    for (uint32_t workers = 2; workers <= 8; workers += 6)
        for (unsigned typed = 0; typed < 2; typed++)
            for (unsigned existing = 0; existing < 2; existing++)
                test_parallel_schema_parity(workers, typed != 0, existing != 0);


    for (unsigned mode = 0; mode < 6; mode++) {
#ifndef WL_TEST_ALLOC_WRAP
        if (mode == 4)
            continue;
#endif
        test_aggregate_publication_guard(mode, false);
        test_aggregate_publication_guard(mode, true);
    }
    test_aggregate_public_retry(1, false);
    test_aggregate_public_retry(2, false);
    test_aggregate_public_retry(2, true);
    test_aggregate_public_retry(8, false);
    test_aggregate_public_retry(8, true);
    test_coordinator_specialized_parallel();
    test_filter_worker_provenance(2);
    test_filter_worker_provenance(8);
    test_correctness_replay_refusal(0);
    test_correctness_replay_refusal(1);
    test_correctness_replay_refusal(2);
    for (unsigned mode = 0; mode < 3; mode++) {
        test_worker_serial_segments(false, mode);
        test_worker_serial_segments(true, mode);
    }
    for (unsigned storage = 0; storage < 5; storage++) {
        test_worker_frame_retention(2, storage, true, false, 0);
        test_worker_frame_retention(2, storage, false, true, 0);
    }
    for (unsigned storage = 0; storage < 5; storage++)
        test_tdd_recursive_frame_retention(2, storage, 0, false);
    test_tdd_recursive_frame_retention(8, 0, 1, false);
    test_tdd_recursive_frame_retention(8, 3, 1, false);
    for (unsigned storage = 0; storage < 7; storage++)
        test_tdd_outbound_frame_retention(2, storage, 0, false);
    test_tdd_outbound_frame_retention(8, 0, 1, false);
    test_tdd_outbound_frame_retention(8, 3, 1, false);
    test_tdd_outbound_frame_retention(8, 5, 1, false);
    test_tdd_outbound_frame_retention(8, 6, 1, false);
    test_tdd_outbound_frame_retention(2, 0, 0, true);
    test_tdd_outbound_frame_retention(8, 3, 0, true);
    test_tdd_outbound_frame_retention(2, 5, 0, true);
    test_tdd_outbound_frame_retention(8, 6, 1, true);
    for (uint32_t workers = 2; workers <= 8; workers *= 4) {
        for (unsigned initial = 0; initial < 3; initial++)
            test_global_read_publication(workers, initial, 0, 0, false);
        for (unsigned mode = 1; mode <= 8; mode++) {
#ifndef WL_TEST_ALLOC_WRAP
            if (mode == 4 || mode == 5)
                continue;
#endif
            test_global_read_publication(workers, 1, mode, 0, false);
            test_global_read_publication(workers, 1, mode, 1, false);
        }
#ifdef WL_TEST_ALLOC_WRAP
        test_global_read_publication(workers, 0, 9, 0, false);
        test_global_read_publication(workers, 0, 9, 1, false);
#endif
        test_global_read_publication(workers, 1, 0, 1, true);
        test_global_read_publication(workers, 1, 1, 1, true);
        test_global_read_publication(workers, 1, 3, 1, true);
    }
    for (uint32_t workers = 2; workers <= 8; workers *= 4) {
        for (unsigned initial = 0; initial < 3; initial++)
            test_specialized_publication(workers, initial, 0, initial == 2);
        for (unsigned mode = 1; mode <= 22; mode++) {
#ifndef WL_TEST_ALLOC_WRAP
            if (mode >= 9 && mode <= 12)
                continue;
#endif
            test_specialized_publication(workers, 2, mode, false);
        }
        test_specialized_publication(workers, 0, 20, false);
    }
    test_tdd_recursive_frame_retention(2, 0, 0, true);
    test_tdd_recursive_frame_retention(8, 3, 1, true);
    for (unsigned mode = 0; mode < 7; mode++) {
#ifndef WL_TEST_ALLOC_WRAP
        if (mode == 5) continue;
#endif
        test_outbound_publisher_ownership(mode);
    }
    for (unsigned mode = 0; mode < 5; mode++) {
        test_tdd_ordinary_frame_gates(2, mode);
        test_tdd_ordinary_frame_gates(8, mode);
    }
    test_worker_frame_admission(false);
    test_worker_frame_admission(true);
    test_worker_frame_retention(8, 0, true, false, 0);
    test_worker_frame_retention(8, 3, true, false, 0);
    test_worker_frame_retention(8, 0, false, true, 1);
    test_worker_frame_retention(8, 3, false, true, 1);
    test_worker_frame_retention(2, 0, false, false, 0);
    test_worker_frame_retention(8, 3, false, false, 0);
    test_recursive_cleanup_scope(0);
    test_recursive_cleanup_scope(1);
    test_recursive_cleanup_scope(2);
    test_observer_retry(false, false, false, false);
    test_observer_retry(true, false, false, false);
    test_observer_retry(false, true, false, false);
    test_observer_retry(true, true, false, false);
    test_observer_retry(false, false, true, false);
    test_observer_retry(false, false, false, true);
    test_observer_retry(false, false, 2, false);
    test_observer_retry(false, false, 2, true);
    test_observer_logical_identity(0);
    test_observer_logical_identity(1);
    test_observer_logical_identity(2);
    test_observer_compound_removal();
    test_observer_callback_binding(0, false);
    test_observer_callback_binding(1, false);
    test_observer_callback_binding(2, false);
    test_observer_callback_binding(0, true);
    test_observer_callback_binding(1, true);
    test_observer_callback_binding(2, true);
    test_idb_small_input(true, false);
    test_idb_small_input(false, false);
    test_idb_small_input(false, true);
    for (uint32_t workers = 2; workers <= 8; workers += 6) {
        test_later_stratum_completion_retry(workers);
        for (unsigned mode = 0; mode < 6; mode++) {
#ifndef WL_TEST_ALLOC_WRAP
            if (mode == 3)
                continue;
#endif
            test_final_normalization(workers, 0, mode);
        }
        for (unsigned route = 1; route <= 1; route++) {
            test_final_normalization(workers, route, 0);
            test_final_normalization(workers, route, 5);
        }
    }
    test_idb_consolidation(0, false);
    test_idb_consolidation(1, false);
    test_idb_consolidation(0, true);
    test_idb_consolidation(1, true);
    test_idb_consolidation(2, true);
    test_idb_consolidation(3, true);
    test_idb_consolidation(7, false);
    test_idb_consolidation(8, true);
    test_idb_consolidation(9, true);
    test_idb_consolidation(10, true);
#ifdef WL_TEST_ALLOC_WRAP
    test_idb_consolidation(4, false);
    test_idb_consolidation(4, true);
    test_idb_consolidation(5, false);
    test_idb_consolidation(6, false);
#endif
    test_observer_completion_retry(0, false);
    test_observer_completion_retry(0, true);
    test_observer_completion_retry(2, false);
#ifdef WL_TEST_ALLOC_WRAP
    test_observer_completion_retry(1, false);
    test_observer_capture_failure(0);
    test_observer_capture_failure(1);
    test_observer_capture_failure(2);
#endif
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
#ifdef WL_TEST_ALLOC_WRAP
    test_filter_timestamp_allocation_failure_without_reader();
    test_filter_timestamp_allocation_failure_retry();
#endif
    test_filter_timestamp_pool_and_heap_routes();
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
