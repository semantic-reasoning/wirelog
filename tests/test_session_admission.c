/* Internal session operation admission and teardown concurrency regressions. */

#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "wirelog/session.h"
#include "wirelog/thread.h"

#include <errno.h>
#include <stdbool.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(_WIN32)
#include <windows.h>
#else
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

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
    bool release_destroy;
    bool backend_destroy_entered;
    bool destroy_returned;
    unsigned calls[TEST_OP_COUNT];
} test_sync_t;

typedef struct {
    wl_session_t base;
    test_sync_t *sync;
    bool controlled_step_complete;
    bool controlled_step_retryable;
    bool controlled_insert_allowed;
    bool fail_next_insert;
} fake_session_t;

typedef struct {
    wl_session_t *session;
    test_sync_t *sync;
    test_operation_t operation;
    int rc;
} thread_context_t;

typedef struct {
    wl_session_exclusive_attempt_t *attempt;
    int enter_rc;
    int release_rc;
} attempt_thread_context_t;

static test_sync_t *creation_sync;
static bool fail_unlock_after_insert;

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

static int
fake_create_with_options(const wl_plan_t *plan, uint32_t workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    (void)options;
    return fake_create(plan, workers, out);
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
    fake_session_t *session = (fake_session_t *)base;
    if (session->fail_next_insert) {
        session->fail_next_insert = false;
        return ENOMEM;
    }
    if (session->controlled_step_complete && relation
        && strcmp(relation, "edge") == 0)
        session->controlled_step_retryable = true;
    if (fail_unlock_after_insert) {
        fail_unlock_after_insert = false;
        wl_session_testhook_fail_next_admission_unlock();
    }
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
    fake_session_t *session = (fake_session_t *)base;
    record_operation(session, TEST_OP_STEP);
    if (base->evaluation_control) {
        uint64_t id = wl_evaluation_control_active_attempt_id(
            base->evaluation_control);
        if (!id) {
            int rc = wl_evaluation_control_begin(base->evaluation_control,
                    base->operation_admission);
            return rc ? rc : EAGAIN;
        }
        int rc = wl_evaluation_control_finish(base->evaluation_control,
                base->operation_admission, 0, 0);
        if (!rc) {
            session->controlled_step_complete = true;
            session->controlled_step_retryable = false;
        }
        return rc;
    }
    return 0;
}

static wl_controlled_step_state_t
fake_controlled_step_state(const wl_session_t *base)
{
    const fake_session_t *session = (const fake_session_t *)base;
    if (base->evaluation_control
        && wl_evaluation_control_active_attempt_id(
            base->evaluation_control) != 0)
        return WL_CONTROLLED_STEP_PENDING;
    if (!session->controlled_step_complete)
        return WL_CONTROLLED_STEP_IDLE;
    return session->controlled_step_retryable
        ? WL_CONTROLLED_STEP_RETRYABLE : WL_CONTROLLED_STEP_COMPLETE;
}

static bool
fake_authorize_controlled_insert(const wl_session_t *base,
    const char *relation, uint32_t num_rows, uint32_t num_cols)
{
    const fake_session_t *session = (const fake_session_t *)base;
    return session->controlled_step_complete
           && session->controlled_insert_allowed
           && relation && strcmp(relation, "edge") == 0
           && num_rows > 0 && num_cols == 2;
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
    .session_create_with_options = fake_create_with_options,
    .session_destroy = fake_destroy,
    .session_insert = fake_insert,
    .session_make_compound = fake_make_compound,
    .session_remove = fake_remove,
    .session_step = fake_step,
    .session_set_delta_cb = fake_set_delta_cb,
    .session_snapshot = fake_snapshot,
    .session_memory_governor = fake_memory_governor,
    .session_controlled_step_state = fake_controlled_step_state,
    .session_controlled_step_authorize_insert =
        fake_authorize_controlled_insert,
};

static void noop_tuple(const char *relation, const int64_t *row,
    uint32_t ncols, void *user_data);

static void *
operation_thread(void *opaque)
{
    thread_context_t *ctx = (thread_context_t *)opaque;

    if (ctx->operation == TEST_OP_INSERT) {
        int64_t row = 1;
        ctx->rc = wl_session_insert(ctx->session, "r", &row, 1, 1);
    } else if (ctx->operation == TEST_OP_STEP)
        ctx->rc = wl_session_step(ctx->session);
    else
        ctx->rc = wl_session_snapshot(ctx->session, noop_tuple, NULL);
    return NULL;
}

static void *
attempt_wrong_thread(void *opaque)
{
    attempt_thread_context_t *ctx = (attempt_thread_context_t *)opaque;
    ctx->enter_rc = wl_session_exclusive_attempt_enter(ctx->attempt);
    ctx->release_rc = wl_session_exclusive_attempt_release(ctx->attempt);
    return NULL;
}

static void *
destroy_thread(void *opaque)
{
    thread_context_t *ctx = (thread_context_t *)opaque;

    wl_mutex_lock(&ctx->sync->mutex);
    while (!ctx->sync->release_destroy)
        wl_cond_wait(&ctx->sync->changed, &ctx->sync->mutex);
    wl_mutex_unlock(&ctx->sync->mutex);
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

static void
noop_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
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
admission_now_ms(uint64_t *now)
{
#if defined(_WIN32)
    *now = (uint64_t)GetTickCount64();
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return errno;
    *now = (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
#endif
    return 0;
}

static int
wait_for_admission_close(wl_session_t *session,
    test_operation_t blocked_operation, bool controlled, uint64_t timeout_ms)
{
    uint64_t start, now;
    int rc = admission_now_ms(&start);
    if (rc != 0)
        return rc;
    for (;;) {
        /* Only probe an operation that cannot enter the blocked backend call.
         * Controlled step/snapshot can fail before checking admission. */
        rc = controlled ? wl_session_remove(session, "r", NULL, 0, 0)
            : blocked_operation == TEST_OP_SNAPSHOT
            ? wl_session_step(session)
            : wl_session_snapshot(session, NULL, NULL);
        if (rc != 0)
            return rc;
        rc = admission_now_ms(&now);
        if (rc != 0)
            return rc;
        if (now - start >= timeout_ms)
            return ETIMEDOUT;
#if defined(_WIN32)
        Sleep(1);
#else
        const struct timespec pause = { 0, 1000000 };
        if (nanosleep(&pause, NULL) != 0 && errno != EINTR)
            return errno;
#endif
    }
}

static int
run_destroy_race(test_operation_t blocked_operation, bool controlled,
    bool force_timeout)
{
    test_sync_t sync = { 0 };
    thread_context_t operation_ctx = { 0 };
    thread_context_t destroy_ctx = { 0 };
    wl_thread_t operation_tid;
    wl_thread_t destroy_tid;
    wl_session_t *session = NULL;
    wl_evaluation_control_t *control = NULL;
    wl_session_options_t options;
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
    sync.release_destroy = !force_timeout;
    creation_sync = &sync;
    wl_session_options_init(&options);
    if (controlled && wl_evaluation_control_create(0, &control) != 0) {
        wl_cond_destroy(&sync.changed);
        wl_mutex_destroy(&sync.mutex);
        return 1;
    }
    options.evaluation_control = control;
    if (wl_session_create_with_options(&fake_backend, NULL, 1, &options,
        &session) != 0
        || !session) {
        wl_evaluation_control_release(control);
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
        wl_evaluation_control_release(control);
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
        wl_evaluation_control_release(control);
        wl_cond_destroy(&sync.changed);
        wl_mutex_destroy(&sync.mutex);
        fprintf(stderr, "FAIL: destroy thread creation\n");
        return 1;
    }

    /* The active backend call keeps the session alive throughout the wait.
     * Holding destroy behind its gate deterministically exercises timeout. */
    denied_rc = wait_for_admission_close(session, blocked_operation, controlled,
            force_timeout ? 10 : 5000);
    failures += expect(denied_rc == (force_timeout ? ETIMEDOUT : EBUSY),
            force_timeout ? "held destroy thread causes admission wait timeout"
            : "new operation denied while destroy waits");
    if (denied_rc != EBUSY) {
        if (!force_timeout || denied_rc != ETIMEDOUT)
            fprintf(stderr,
                "admission wait failed: rc=%d, operation=%d, controlled=%d\n",
                denied_rc, (int)blocked_operation, (int)controlled);
        goto release_threads;
    }

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
    wl_mutex_unlock(&sync.mutex);

release_threads:
    /* Cancel before releasing the admitted call, including on wait failure.
     * Never access session after release: destroy may immediately free it. */
    if (control)
        wl_evaluation_control_request_cancel(control);
    wl_mutex_lock(&sync.mutex);
    sync.release_operation = true;
    sync.release_destroy = true;
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

    if (control) {
        int owner;
        failures += expect(wl_evaluation_control_attach(control, &owner) == 0,
                "destroy releases logical owner");
        failures += expect(wl_evaluation_control_begin(control, &owner) == 0,
                "retained caller reference survives destroy");
        failures += expect(wl_evaluation_control_charge(control, 0)
                == WL_EVALUATION_CONTROL_CANCELLED,
                "request during admission drain remains sticky");
        (void)wl_evaluation_control_finish(control, &owner, 0, 0);
        (void)wl_evaluation_control_detach(control, &owner);
        wl_evaluation_control_release(control);
    }
    wl_cond_destroy(&sync.changed);
    wl_mutex_destroy(&sync.mutex);
    return failures;
}

static int
run_controlled_step_gate(void)
{
    int failures = 0;
    test_sync_t sync = {0};
    wl_mutex_init(&sync.mutex);
    wl_cond_init(&sync.changed);
    sync.blocked_operation = TEST_OP_INSERT;
    creation_sync = &sync;

    wl_evaluation_control_t *control = NULL;
    failures += expect(wl_evaluation_control_create(0, &control) == 0,
            "create controlled admission control");
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    failures += expect(wl_session_create_with_options(&fake_backend, NULL, 1,
            &options, &session) == 0, "create controlled fake session");
    if (!session) goto done;

    failures += expect(wl_session_snapshot(session, noop_tuple, NULL)
            == ENOTSUP,
            "snapshot is unavailable before controlled STEP completes");

    thread_context_t operation_ctx = {
        .session = session, .sync = &sync, .operation = TEST_OP_INSERT
    };
    wl_thread_t operation_tid;
    int thread_rc = wl_thread_create(&operation_tid, operation_thread,
            &operation_ctx);
    failures += expect(thread_rc == 0, "start admitted insert");
    if (thread_rc != 0) goto destroy_session;
    wl_mutex_lock(&sync.mutex);
    while (!sync.operation_entered)
        wl_cond_wait(&sync.changed, &sync.mutex);
    wl_mutex_unlock(&sync.mutex);
    failures += expect(wl_session_step(session) == EBUSY,
            "controlled step rejects an in-flight session operation");
    wl_mutex_lock(&sync.mutex);
    sync.release_operation = true;
    wl_cond_broadcast(&sync.changed);
    wl_mutex_unlock(&sync.mutex);
    wl_thread_join(&operation_tid);
    failures += expect(operation_ctx.rc == 0,
            "in-flight insert completes before controlled step");

    sync.blocked_operation = TEST_OP_COUNT;
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_step(session) == EIO,
            "controlled STEP admission lock failure fails closed");
    failures += expect(wl_session_step(session) == EAGAIN,
            "controlled step exposes its retained continuation");
    wl_session_t *pending_session = session;
    failures += expect(wl_session_try_destroy(&session) == EBUSY
            && session == pending_session,
            "checked destroy preserves a parked controlled STEP session");
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    failures += expect(id != 0, "pending step keeps its original attempt");
    int64_t row = 1;
    wirelog_compound_arg_t arg = {.type = WIRELOG_TYPE_INT64, .value = 1};
    uint64_t handle = 0;
    failures += expect(wl_session_insert(session, "r", &row, 1, 1) == EBUSY,
            "pending step blocks insert");
    failures += expect(wl_session_make_compound(session, "f", 1, &arg,
            &handle) == EBUSY, "pending step blocks compound creation");
    failures += expect(wl_session_remove(session, "r", &row, 1, 1) == EBUSY,
            "pending step blocks remove");
    unsigned set_delta_calls = sync.calls[TEST_OP_SET_DELTA_CB];
    wl_session_set_delta_cb(session, noop_delta, NULL);
    failures += expect(sync.calls[TEST_OP_SET_DELTA_CB] == set_delta_calls,
            "pending step blocks callback changes");
    failures += expect(wl_session_snapshot(session, NULL, NULL) == EBUSY,
            "pending step blocks snapshot");
    unsigned governor_calls = sync.calls[TEST_OP_MEMORY_GOVERNOR];
    failures += expect(wl_session_memory_governor(session) == NULL,
            "pending step blocks governor observation");
    failures += expect(sync.calls[TEST_OP_MEMORY_GOVERNOR] == governor_calls,
            "pending step does not enter backend governor hook");
    failures += expect(wl_evaluation_control_reset(control) == EBUSY
            && wl_evaluation_control_configure(control, 100) == EBUSY,
            "pending step blocks control reconfiguration");

    failures += expect(wl_session_step(session) == 0,
            "resume is the sole operation admitted through the gate");
    failures += expect(!wl_evaluation_control_active_attempt_id(control),
            "resume finishes the retained attempt");

    sync.blocked_operation = TEST_OP_SNAPSHOT;
    sync.operation_entered = false;
    sync.release_operation = false;
    operation_ctx.operation = TEST_OP_SNAPSHOT;
    operation_ctx.rc = -1;
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_snapshot(session, noop_tuple, NULL) == EIO,
            "controlled snapshot admission lock failure fails closed");
    thread_rc = wl_thread_create(&operation_tid, operation_thread,
            &operation_ctx);
    failures += expect(thread_rc == 0,
            "start controlled completed-step snapshot");
    if (thread_rc != 0) goto destroy_session;
    wl_mutex_lock(&sync.mutex);
    while (!sync.operation_entered)
        wl_cond_wait(&sync.changed, &sync.mutex);
    wl_mutex_unlock(&sync.mutex);
    failures += expect(wl_session_insert(session, "r", &row, 1, 1) == EBUSY,
            "completed controlled STEP still blocks mutation during snapshot");
    failures += expect(wl_session_step(session) == EBUSY,
            "snapshot excludes a concurrent controlled STEP");
    failures += expect(wl_session_snapshot(session, noop_tuple, NULL) == EBUSY,
            "snapshot admission is exclusive");
    wl_mutex_lock(&sync.mutex);
    sync.release_operation = true;
    wl_cond_broadcast(&sync.changed);
    wl_mutex_unlock(&sync.mutex);
    wl_thread_join(&operation_tid);
    failures += expect(operation_ctx.rc == 0,
            "completed controlled-step snapshot finishes");
    failures += expect(wl_session_insert(session, "r", &row, 1, 1) == EBUSY,
            "mutations remain blocked after snapshot completion");
destroy_session:
    wl_session_destroy(session);
done:
    if (control) wl_evaluation_control_release(control);
    creation_sync = NULL;
    wl_cond_destroy(&sync.changed);
    wl_mutex_destroy(&sync.mutex);
    return failures;
}

static int
run_controlled_retryable_insert_gate(void)
{
    int failures = 0;
    test_sync_t sync = {0};
    wl_mutex_init(&sync.mutex);
    wl_cond_init(&sync.changed);
    sync.blocked_operation = TEST_OP_COUNT;
    creation_sync = &sync;

    wl_evaluation_control_t *control = NULL;
    failures += expect(wl_evaluation_control_create(0, &control) == 0,
            "create retryable admission control");
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    failures += expect(wl_session_create_with_options(&fake_backend, NULL, 1,
            &options, &session) == 0,
            "create retryable fake session");
    if (!session) goto done;

    failures += expect(wl_session_step(session) == EAGAIN
            && wl_session_step(session) == 0,
            "initial controlled STEP reaches completed state");
    fake_session_t *fake = (fake_session_t *)session;
    fake->controlled_insert_allowed = true;
    int64_t edge[] = { 1, 2 };
    int64_t other[] = { 4, 5 };
    failures += expect(wl_session_insert(session, "edge", edge, 1, 2) == 0
            && fake_controlled_step_state(session)
            == WL_CONTROLLED_STEP_RETRYABLE,
            "successful edge append keeps restricted retryable admission");

    unsigned insert_calls = sync.calls[TEST_OP_INSERT];
    failures += expect(wl_session_insert(session, "other", other, 1, 2)
            == EBUSY
            && sync.calls[TEST_OP_INSERT] == insert_calls,
            "non-edge insert is rejected before backend mutation");
    failures += expect(wl_session_remove(session, "edge", edge, 1, 2)
            == EBUSY
            && sync.calls[TEST_OP_REMOVE] == 0,
            "remove is rejected before backend mutation in retryable state");
    wirelog_compound_arg_t arg = { .type = WIRELOG_TYPE_INT64, .value = 1 };
    uint64_t handle = 0;
    failures += expect(wl_session_make_compound(session, "f", 1, &arg,
            &handle) == EBUSY
            && sync.calls[TEST_OP_MAKE_COMPOUND] == 0,
            "compound creation is rejected before backend mutation");
    failures += expect(wl_session_snapshot(session, noop_tuple, NULL) == EBUSY,
            "stale recursive output is not snapshotted while retryable");

    fake->fail_next_insert = true;
    failures += expect(wl_session_insert(session, "edge", edge, 1, 2)
            == ENOMEM
            && fake_controlled_step_state(session)
            == WL_CONTROLLED_STEP_RETRYABLE,
            "failed authorized insert leaves retryable backend state unchanged");
    failures += expect(wl_session_insert(session, "edge", edge, 1, 2) == 0,
            "another edge append remains eligible after an insert failure");

    failures += expect(wl_session_step(session) == EAGAIN,
            "successor STEP starts from restricted retryable state");
    insert_calls = sync.calls[TEST_OP_INSERT];
    failures += expect(wl_session_insert(session, "edge", edge, 1, 2)
            == EBUSY
            && sync.calls[TEST_OP_INSERT] == insert_calls
            && wl_session_remove(session, "edge", edge, 1, 2) == EBUSY
            && wl_session_make_compound(session, "f", 1, &arg, &handle)
            == EBUSY,
            "pending successor STEP blocks all mutations");
    failures += expect(wl_session_step(session) == 0
            && fake_controlled_step_state(session)
            == WL_CONTROLLED_STEP_COMPLETE,
            "completed successor STEP restores controlled output state");

    wl_session_destroy(session);
done:
    if (control) wl_evaluation_control_release(control);
    creation_sync = NULL;
    wl_cond_destroy(&sync.changed);
    wl_mutex_destroy(&sync.mutex);
    return failures;
}

static int
run_exclusive_attempt_gate(void)
{
    test_sync_t sync = { 0 };
    wl_session_options_t options;
    wl_session_t *session = NULL;
    wl_session_exclusive_attempt_t attempt = { 0 };
    wl_session_exclusive_attempt_t competing_attempt = { 0 };
    wl_session_exclusive_attempt_t copied_attempt;
    attempt_thread_context_t thread_ctx = { 0 };
    wl_session_t borrowed = { 0 };
    wl_session_t *borrowed_slot = &borrowed;
    int failures = 0;
    int64_t row = 1;

    if (wl_mutex_init(&sync.mutex) != 0 || wl_cond_init(&sync.changed) != 0)
        return 1;
    creation_sync = &sync;
    failures += expect(wl_session_try_destroy(&borrowed_slot) == EINVAL
            && borrowed_slot == &borrowed,
            "checked destroy rejects borrowed session handles");
    wl_session_options_init(&options);
    failures += expect(wl_session_create_with_options(&fake_backend, NULL, 1,
            &options, &session) == 0 && session != NULL,
            "create fake session for exclusive attempt");
    if (!session) goto done;
    unsigned insert_calls = sync.calls[TEST_OP_INSERT];
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_insert(session, "r", &row, 1, 1) == EIO
            && sync.calls[TEST_OP_INSERT] == insert_calls,
            "admission lock failure refuses before backend mutation");
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_exclusive_attempt_acquire(session,
            &attempt) == EIO && !attempt.held,
            "lease acquisition lock failure leaves record inert");
    failures += expect(wl_session_exclusive_attempt_acquire(session, &attempt)
            == 0, "acquire persistent exclusive attempt");
    failures += expect(wl_session_insert(session, "r", &row, 1, 1) == EBUSY,
            "exclusive attempt blocks ordinary mutation while parked");
    failures += expect(wl_session_step(session) == EBUSY,
            "exclusive attempt blocks STEP while parked");
    failures += expect(wl_session_snapshot(session, noop_tuple, NULL) == EBUSY,
            "exclusive attempt blocks snapshot while parked");
    unsigned callback_calls = sync.calls[TEST_OP_SET_DELTA_CB];
    wl_session_set_delta_cb(session, noop_delta, NULL);
    failures += expect(sync.calls[TEST_OP_SET_DELTA_CB] == callback_calls,
            "exclusive attempt blocks callback replacement while parked");
    wl_session_t *same_session = session;
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_try_destroy(&session) == EIO
            && session == same_session,
            "checked destroy lock failure preserves session slot");
    failures += expect(wl_session_try_destroy(&session) == EBUSY
            && session == same_session,
            "checked destroy preserves session while attempt is parked");
    copied_attempt = attempt;
    failures += expect(wl_session_exclusive_attempt_enter(&copied_attempt)
            == EINVAL, "copied lease cannot enter");
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_exclusive_attempt_enter(&attempt) == EIO
            && !attempt.entered,
            "lease enter lock failure leaves action unentered");
    failures += expect(wl_session_exclusive_attempt_enter(&attempt) == 0,
            "enter exclusive attempt action");
    failures += expect(wl_session_exclusive_attempt_acquire(session,
            &competing_attempt) == EBUSY,
            "another lease cannot acquire during an admitted action");
    failures += expect(wl_session_exclusive_attempt_enter(&attempt) == EINVAL,
            "nested lease entry is refused");
    failures += expect(wl_session_exclusive_attempt_release(&attempt) == EINVAL,
            "entered lease cannot release");
    failures += expect(wl_session_exclusive_attempt_mark_exit_pending(&attempt)
            == 0, "mark exclusive action before leave");
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_exclusive_attempt_leave(&attempt) == EIO
            && attempt.entered && attempt.exit_pending,
            "leave lock failure preserves EXIT_PENDING without replay");
    failures += expect(wl_session_exclusive_attempt_leave(&attempt) == 0,
            "retry leave without rerunning the action");
    thread_ctx.attempt = &attempt;
    wl_thread_t attempt_tid;
    int attempt_thread_rc = wl_thread_create(&attempt_tid,
            attempt_wrong_thread, &thread_ctx);
    failures += expect(attempt_thread_rc == 0,
            "start wrong-thread lease probes");
    if (attempt_thread_rc == 0)
        wl_thread_join(&attempt_tid);
    failures += expect(thread_ctx.enter_rc == EINVAL
            && thread_ctx.release_rc == EINVAL,
            "wrong thread cannot enter or release a parked lease");
    wl_session_testhook_fail_next_admission_lock();
    failures += expect(wl_session_exclusive_attempt_release(&attempt) == EIO
            && attempt.held,
            "release lock failure preserves parked lease");
    failures += expect(wl_session_exclusive_attempt_release(&attempt) == 0,
            "release lease after draining attempt ownership");
    failures += expect(wl_session_try_destroy(&session) == 0 && session == NULL,
            "checked destroy consumes quiescent session slot");
    failures += expect(wl_session_try_destroy(&session) == 0,
            "checked destroy accepts an empty slot");
done:
    creation_sync = NULL;
    wl_cond_destroy(&sync.changed);
    wl_mutex_destroy(&sync.mutex);
    return failures;
}

#if !defined(_WIN32)
static int
run_admission_unlock_fail_stop(void)
{
    pid_t child = fork();
    if (child < 0)
        return 1;
    if (child == 0) {
        test_sync_t sync = { 0 };
        wl_session_options_t options;
        wl_session_t *session = NULL;
        int64_t row = 1;
        if (wl_mutex_init(&sync.mutex) != 0
            || wl_cond_init(&sync.changed) != 0)
            _exit(3);
        sync.blocked_operation = TEST_OP_COUNT;
        sync.release_operation = true;
        creation_sync = &sync;
        wl_session_options_init(&options);
        if (wl_session_create_with_options(&fake_backend, NULL, 1,
            &options, &session) != 0 || !session)
            _exit(4);
        fail_unlock_after_insert = true;
        (void)wl_session_insert(session, "r", &row, 1, 1);
        _exit(5);
    }
    int status = 0;
    if (waitpid(child, &status, 0) != child)
        return 1;
    return expect(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT,
               "admission unlock failure after backend action fail-stops");
}

static int
run_destroy_lock_fail_stop(void)
{
    pid_t child = fork();
    if (child < 0)
        return 1;
    if (child == 0) {
        test_sync_t sync = { 0 };
        wl_session_options_t options;
        wl_session_t *session = NULL;
        if (wl_mutex_init(&sync.mutex) != 0
            || wl_cond_init(&sync.changed) != 0)
            _exit(3);
        sync.blocked_operation = TEST_OP_COUNT;
        sync.release_operation = true;
        creation_sync = &sync;
        wl_session_options_init(&options);
        if (wl_session_create_with_options(&fake_backend, NULL, 1,
            &options, &session) != 0 || !session)
            _exit(4);
        wl_session_testhook_fail_next_admission_lock();
        wl_session_destroy(session);
        _exit(5);
    }
    int status = 0;
    if (waitpid(child, &status, 0) != child)
        return 1;
    return expect(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT,
               "destroy admission lock failure fail-stops before teardown");
}
#endif

int
main(void)
{
    int failures = 0;

    failures += run_destroy_race(TEST_OP_SNAPSHOT, false, false);
    failures += run_destroy_race(TEST_OP_STEP, false, false);
    failures += run_destroy_race(TEST_OP_INSERT, true, false);
    failures += run_destroy_race(TEST_OP_SNAPSHOT, false, true);
    failures += run_destroy_race(TEST_OP_STEP, false, true);
    failures += run_destroy_race(TEST_OP_INSERT, true, true);
    failures += run_controlled_step_gate();
    failures += run_controlled_retryable_insert_gate();
    failures += run_exclusive_attempt_gate();
#if !defined(_WIN32)
    failures += run_admission_unlock_fail_stop();
    failures += run_destroy_lock_fail_stop();
#endif
    if (failures)
        fprintf(stderr, "session_admission: %d failure(s)\n", failures);
    return failures != 0;
}
