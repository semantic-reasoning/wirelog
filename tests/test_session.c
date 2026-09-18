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

#ifdef WL_SESSION_TEST_HOOKS
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
    test_session_remove_reader_exclusion();
    test_session_remove_incremental_reader_exclusion();
#ifdef WL_SESSION_TEST_HOOKS
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
