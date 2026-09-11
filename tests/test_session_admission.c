/* Internal session operation admission and teardown concurrency regressions. */

#include "wirelog/session.h"
#include "wirelog/thread.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

typedef enum {
    TEST_OP_INSERT,
    TEST_OP_MAKE_COMPOUND,
    TEST_OP_REMOVE,
    TEST_OP_STEP,
    TEST_OP_SET_DELTA_CB,
    TEST_OP_SNAPSHOT,
    TEST_OP_MEMORY_GOVERNOR,
    TEST_OP_COUNT,
} test_operation_t;

typedef struct {
    wl_mutex_t mutex;
    wl_cond_t changed;
    test_operation_t blocked_operation;
    bool operation_entered;
    bool release_operation;
    bool backend_destroy_entered;
    bool destroy_returned;
    unsigned calls[TEST_OP_COUNT];
} test_sync_t;

typedef struct {
    wl_session_t base;
    test_sync_t *sync;
} fake_session_t;

typedef struct {
    wl_session_t *session;
    test_sync_t *sync;
    test_operation_t operation;
    int rc;
} thread_context_t;

static test_sync_t *creation_sync;

static void
record_operation(fake_session_t *session, test_operation_t operation)
{
    test_sync_t *sync = session->sync;

    wl_mutex_lock(&sync->mutex);
    sync->calls[operation]++;
    if (sync->blocked_operation == operation) {
        sync->operation_entered = true;
        wl_cond_broadcast(&sync->changed);
        while (!sync->release_operation)
            wl_cond_wait(&sync->changed, &sync->mutex);
    }
    wl_mutex_unlock(&sync->mutex);
}

static int
fake_create(const wl_plan_t *plan, uint32_t num_workers, wl_session_t **out)
{
    fake_session_t *session;

    (void)plan;
    (void)num_workers;
    session = (fake_session_t *)calloc(1, sizeof(*session));
    if (!session)
        return ENOMEM;
    session->sync = creation_sync;
    *out = &session->base;
    return 0;
}

static void
fake_destroy(wl_session_t *base)
{
    fake_session_t *session = (fake_session_t *)base;
    test_sync_t *sync = session->sync;

    wl_mutex_lock(&sync->mutex);
    sync->backend_destroy_entered = true;
    wl_cond_broadcast(&sync->changed);
    wl_mutex_unlock(&sync->mutex);
    free(session);
}

static int
fake_insert(wl_session_t *base, const char *relation, const int64_t *data,
    uint32_t num_rows, uint32_t num_cols)
{
    (void)relation;
    (void)data;
    (void)num_rows;
    (void)num_cols;
    record_operation((fake_session_t *)base, TEST_OP_INSERT);
    return 0;
}

static int
fake_make_compound(wl_session_t *base, const char *functor, uint32_t arity,
    const wirelog_compound_arg_t *args, uint64_t *handle_out)
{
    (void)functor;
    (void)arity;
    (void)args;
    record_operation((fake_session_t *)base, TEST_OP_MAKE_COMPOUND);
    if (handle_out)
        *handle_out = 1;
    return 0;
}

static int
fake_remove(wl_session_t *base, const char *relation, const int64_t *data,
    uint32_t num_rows, uint32_t num_cols)
{
    (void)relation;
    (void)data;
    (void)num_rows;
    (void)num_cols;
    record_operation((fake_session_t *)base, TEST_OP_REMOVE);
    return 0;
}

static int
fake_step(wl_session_t *base)
{
    record_operation((fake_session_t *)base, TEST_OP_STEP);
    return 0;
}

static void
fake_set_delta_cb(wl_session_t *base, wirelog_on_delta_fn callback,
    void *user_data)
{
    (void)callback;
    (void)user_data;
    record_operation((fake_session_t *)base, TEST_OP_SET_DELTA_CB);
}

static int
fake_snapshot(wl_session_t *base, wirelog_on_tuple_fn callback,
    void *user_data)
{
    (void)callback;
    (void)user_data;
    record_operation((fake_session_t *)base, TEST_OP_SNAPSHOT);
    return 0;
}

static wl_columnar_memory_governor_t *
fake_memory_governor(wl_session_t *base)
{
    record_operation((fake_session_t *)base, TEST_OP_MEMORY_GOVERNOR);
    return (wl_columnar_memory_governor_t *)base;
}

static const wl_compute_backend_t fake_backend = {
    .name = "admission-test",
    .session_create = fake_create,
    .session_destroy = fake_destroy,
    .session_insert = fake_insert,
    .session_make_compound = fake_make_compound,
    .session_remove = fake_remove,
    .session_step = fake_step,
    .session_set_delta_cb = fake_set_delta_cb,
    .session_snapshot = fake_snapshot,
    .session_memory_governor = fake_memory_governor,
};

static void *
operation_thread(void *opaque)
{
    thread_context_t *ctx = (thread_context_t *)opaque;

    if (ctx->operation == TEST_OP_STEP)
        ctx->rc = wl_session_step(ctx->session);
    else
        ctx->rc = wl_session_snapshot(ctx->session, NULL, NULL);
    return NULL;
}

static void *
destroy_thread(void *opaque)
{
    thread_context_t *ctx = (thread_context_t *)opaque;

    wl_session_destroy(ctx->session);
    wl_mutex_lock(&ctx->sync->mutex);
    ctx->sync->destroy_returned = true;
    wl_cond_broadcast(&ctx->sync->changed);
    wl_mutex_unlock(&ctx->sync->mutex);
    return NULL;
}

static void
noop_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)diff;
    (void)user_data;
}

static int
expect(bool condition, const char *message)
{
    if (!condition)
        fprintf(stderr, "FAIL: %s\n", message);
    return condition ? 0 : 1;
}

static int
run_destroy_race(test_operation_t blocked_operation)
{
    test_sync_t sync = { 0 };
    thread_context_t operation_ctx = { 0 };
    thread_context_t destroy_ctx = { 0 };
    wl_thread_t operation_tid;
    wl_thread_t destroy_tid;
    wl_session_t *session = NULL;
    unsigned before[TEST_OP_COUNT];
    int64_t row = 1;
    wirelog_compound_arg_t arg = { WIRELOG_TYPE_INT64, 1 };
    uint64_t handle = 0;
    int denied_rc = 0;
    int failures = 0;

    if (wl_mutex_init(&sync.mutex) != 0) {
        fprintf(stderr, "FAIL: synchronization initialization\n");
        return 1;
    }
    if (wl_cond_init(&sync.changed) != 0) {
        wl_mutex_destroy(&sync.mutex);
        fprintf(stderr, "FAIL: synchronization initialization\n");
        return 1;
    }
    sync.blocked_operation = blocked_operation;
    creation_sync = &sync;
    if (wl_session_create(&fake_backend, NULL, 1, &session) != 0
        || !session) {
        wl_cond_destroy(&sync.changed);
        wl_mutex_destroy(&sync.mutex);
        fprintf(stderr, "FAIL: fake session creation\n");
        return 1;
    }

    operation_ctx.session = session;
    operation_ctx.sync = &sync;
    operation_ctx.operation = blocked_operation;
    if (wl_thread_create(&operation_tid, operation_thread,
        &operation_ctx) != 0) {
        wl_session_destroy(session);
        wl_cond_destroy(&sync.changed);
        wl_mutex_destroy(&sync.mutex);
        fprintf(stderr, "FAIL: operation thread creation\n");
        return 1;
    }

    wl_mutex_lock(&sync.mutex);
    while (!sync.operation_entered)
        wl_cond_wait(&sync.changed, &sync.mutex);
    wl_mutex_unlock(&sync.mutex);

    destroy_ctx.session = session;
    destroy_ctx.sync = &sync;
    if (wl_thread_create(&destroy_tid, destroy_thread, &destroy_ctx) != 0) {
        wl_mutex_lock(&sync.mutex);
        sync.release_operation = true;
        wl_cond_broadcast(&sync.changed);
        wl_mutex_unlock(&sync.mutex);
        wl_thread_join(&operation_tid);
        wl_session_destroy(session);
        wl_cond_destroy(&sync.changed);
        wl_mutex_destroy(&sync.mutex);
        fprintf(stderr, "FAIL: destroy thread creation\n");
        return 1;
    }

    /* The active backend call proves destroy cannot complete.  Probe the
     * other quick operation until destroy has closed admission. */
    for (unsigned attempt = 0; attempt < 1000000; attempt++) {
        denied_rc = blocked_operation == TEST_OP_SNAPSHOT
            ? wl_session_step(session)
            : wl_session_snapshot(session, NULL, NULL);
        if (denied_rc == EBUSY || denied_rc != 0)
            break;
    }
    failures += expect(denied_rc == EBUSY,
            "new operation denied while destroy waits");

    wl_mutex_lock(&sync.mutex);
    for (unsigned i = 0; i < TEST_OP_COUNT; i++)
        before[i] = sync.calls[i];
    failures += expect(!sync.backend_destroy_entered,
            "backend destroy waits for active operation");
    wl_mutex_unlock(&sync.mutex);

    failures += expect(wl_session_insert(session, "r", &row, 1, 1) == EBUSY,
            "insert denied after admission closes");
    failures += expect(wl_session_make_compound(session, "f", 1, &arg,
            &handle) == EBUSY,
            "compound creation denied after admission closes");
    failures += expect(wl_session_remove(session, "r", &row, 1, 1) == EBUSY,
            "remove denied after admission closes");
    failures += expect(wl_session_step(session) == EBUSY,
            "step denied after admission closes");
    wl_session_set_delta_cb(session, noop_delta, NULL);
    failures += expect(wl_session_snapshot(session, NULL, NULL) == EBUSY,
            "snapshot denied after admission closes");
    failures += expect(wl_session_memory_governor(session) == NULL,
            "memory governor access denied after admission closes");

    wl_mutex_lock(&sync.mutex);
    for (unsigned i = 0; i < TEST_OP_COUNT; i++)
        failures += expect(sync.calls[i] == before[i],
                "denied operation did not enter backend");
    sync.release_operation = true;
    wl_cond_broadcast(&sync.changed);
    wl_mutex_unlock(&sync.mutex);

    wl_thread_join(&operation_tid);
    wl_thread_join(&destroy_tid);
    failures += expect(operation_ctx.rc == 0,
            "admitted operation completes successfully");
    wl_mutex_lock(&sync.mutex);
    failures += expect(sync.backend_destroy_entered,
            "backend destroy runs after operation completes");
    failures += expect(sync.destroy_returned,
            "session destroy returns synchronously");
    wl_mutex_unlock(&sync.mutex);

    wl_cond_destroy(&sync.changed);
    wl_mutex_destroy(&sync.mutex);
    return failures;
}

int
main(void)
{
    int failures = 0;

    failures += run_destroy_race(TEST_OP_SNAPSHOT);
    failures += run_destroy_race(TEST_OP_STEP);
    if (failures)
        fprintf(stderr, "session_admission: %d failure(s)\n", failures);
    return failures != 0;
}
