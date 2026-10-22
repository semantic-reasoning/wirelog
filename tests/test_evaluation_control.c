/* Retained evaluation-control foundation: no evaluator support is implied. */
#include "wirelog/evaluation_control.h"
#include "wirelog/thread.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(expr) do { if (!(expr)) { \
                             fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                                 #expr); exit(1); \
                         } } while (0)

static void
boundaries(void)
{
    int owner, other;
    wl_evaluation_control_t *c = NULL;
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_create(3, &c) == 0);
    uint64_t identity = wl_evaluation_control_identity(c);
    CHECK(identity != 0);
    CHECK(wl_evaluation_control_report(c, &report) == ENOENT);
    CHECK(wl_evaluation_control_begin(c, &owner) == EINVAL);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == EBUSY);
    CHECK(wl_evaluation_control_attach(c, &other) == EBUSY);
    CHECK(wl_evaluation_control_begin(c, &other) == EINVAL);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_identity(c) == identity);
    CHECK(wl_evaluation_control_begin(c, &owner) == EBUSY);
    CHECK(wl_evaluation_control_detach(c, &owner) == EBUSY);
    CHECK(wl_evaluation_control_transfer(c, &owner, &other) == EBUSY);
    CHECK(wl_evaluation_control_configure(c, 100) == EBUSY);
    CHECK(wl_evaluation_control_reset(c) == EBUSY);
    CHECK(wl_evaluation_control_charge(c, 3) == 0);
    CHECK(wl_evaluation_control_charge(c, 0) == 0);
    CHECK(wl_evaluation_control_finish(c, &other, 0, 0) == EINVAL);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == 1 && report.limit == 3 && report.consumed == 3);
    CHECK(report.stop_reason == WL_EVALUATION_CONTROL_NONE);

    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == 1 && report.consumed == 3);
    CHECK(wl_evaluation_control_charge(c, 3) == 0);
    CHECK(wl_evaluation_control_charge(c,
        1) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_evaluation_control_charge(c,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_evaluation_control_finish(c, &owner, 0,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == 2 && report.consumed == 3);
    CHECK(report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);

    CHECK(wl_evaluation_control_configure(c, 4) == 0);
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_identity(c) == identity);
    CHECK(wl_evaluation_control_report(c,
        &report) == 0 && report.attempt_id == 2);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_charge(c, 4) == 0);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == 3 && report.limit == 4 && report.consumed == 4);
    CHECK(wl_evaluation_control_detach(c, &other) == EINVAL);
    CHECK(wl_evaluation_control_transfer(c, &owner, &other) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == EINVAL);
    /* The owning reference keeps c alive after the caller lets go. */
    wl_evaluation_control_release(c);
    CHECK(wl_evaluation_control_begin(c, &other) == 0);
    CHECK(wl_evaluation_control_finish(c, &other, 0, 0) == 0);
    CHECK(wl_evaluation_control_detach(c, &other) == 0);
}

static void
limits_and_errors(void)
{
    int owner;
    wl_evaluation_control_t *c;
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_create(UINT64_MAX, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == 0);
    CHECK(wl_evaluation_control_charge(c,
        1) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_evaluation_control_finish(c, &owner, 0,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.consumed == UINT64_MAX);
    CHECK(wl_evaluation_control_configure(c, 0) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == 0);
    CHECK(wl_evaluation_control_charge(c, 1) == EOVERFLOW);
    CHECK(wl_evaluation_control_charge(c, 0) == EOVERFLOW);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == EOVERFLOW);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.consumed == UINT64_MAX && report.limit == 0);
    CHECK(report.execution_status == EOVERFLOW && report.stop_reason == 0);

    wl_evaluation_control_request_cancel(c);
    CHECK(wl_evaluation_control_configure(c, 1) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_charge(c,
        2) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(c, &owner, EIO, EBUSY) == EIO);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(report.execution_status == EIO && report.cleanup_status == EBUSY);
    CHECK(report.consumed == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_charge(c,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(c, &owner,
        WL_EVALUATION_CONTROL_CANCELLED,
        EBUSY) == EBUSY);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(report.execution_status == 0 && report.cleanup_status == EBUSY);

    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_charge(c, 0) == 0); /* publication cutoff */
    wl_evaluation_control_request_cancel(c); /* late request */
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_charge(c,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(c, &owner, 0,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_detach(c, &owner) == 0);
    wl_evaluation_control_release(c);
}

static void
active_identity(void)
{
    int owner, other;
    wl_evaluation_control_t *c;
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_active_attempt_id(NULL) == 0);
    CHECK(wl_evaluation_control_create(7, &c) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 0);
    CHECK(wl_evaluation_control_test_seed_attempt_id(c, NULL, 10) == EINVAL);
    CHECK(wl_evaluation_control_test_seed_attempt_id(c, &owner, 10) == EINVAL);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_begin(c, &other) == EINVAL);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 1);
    CHECK(wl_evaluation_control_report(c, &report) == ENOENT);
    CHECK(wl_evaluation_control_begin(c, &owner) == EBUSY);
    CHECK(wl_evaluation_control_reset(c) == EBUSY);
    CHECK(wl_evaluation_control_configure(c, 10) == EBUSY);
    CHECK(wl_evaluation_control_test_seed_attempt_id(c, &owner, 10) == EBUSY);
    CHECK(wl_evaluation_control_finish(c, &other, 0, 0) == EINVAL);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 1);
    CHECK(wl_evaluation_control_finish(c, &owner, EIO, 0) == EIO);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 2);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == 1 && report.result == EIO);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);

    CHECK(wl_evaluation_control_test_seed_attempt_id(c, &other,
        UINT64_MAX) == EINVAL);
    CHECK(wl_evaluation_control_test_seed_attempt_id(c, &owner, 1) == EINVAL);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 3);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_test_seed_attempt_id(c, &owner,
        (uint64_t)INT64_MAX - 1) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == 3 && report.limit == 7 && report.consumed == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) == (uint64_t)INT64_MAX);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c)
        == (uint64_t)INT64_MAX + 1);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_test_seed_attempt_id(c, &owner,
        UINT64_MAX - 1) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) == UINT64_MAX);
    CHECK(wl_evaluation_control_begin(c, &owner) == EBUSY);
    CHECK(wl_evaluation_control_active_attempt_id(c) == UINT64_MAX);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == UINT64_MAX && report.limit == 7);
    CHECK(wl_evaluation_control_begin(c, &owner) == EOVERFLOW);
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_configure(c, 20) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == EOVERFLOW);
    CHECK(wl_evaluation_control_active_attempt_id(c) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == UINT64_MAX && report.limit == 7);
    CHECK(wl_evaluation_control_detach(c, &owner) == 0);
    wl_evaluation_control_release(c);
}

static void
attempt_scoped_charge(void)
{
    int owner;
    wl_evaluation_control_t *c;
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_create(100, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    uint64_t first = wl_evaluation_control_active_attempt_id(c);
    CHECK(first != 0);
    CHECK(wl_evaluation_control_charge_attempt(c, first + 1, 7) == EINVAL);
    CHECK(wl_evaluation_control_charge_attempt(c, first, 7) == 0);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    uint64_t second = wl_evaluation_control_active_attempt_id(c);
    CHECK(second != first);
    CHECK(wl_evaluation_control_charge_attempt(c, first, 9) == EINVAL);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == second && report.consumed == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    uint64_t third = wl_evaluation_control_active_attempt_id(c);
    CHECK(third == second + 1);
    CHECK(wl_evaluation_control_charge_attempt(c, third, 11) == 0);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == third && report.consumed == 11);
    CHECK(wl_evaluation_control_detach(c, &owner) == 0);
    wl_evaluation_control_release(c);
}

typedef struct {
    wl_mutex_t mutex;
    wl_cond_t changed;
    bool ready;
    bool go;
    wl_evaluation_control_t *control;
} cancel_context_t;

static void *
cancel_thread(void *opaque)
{
    cancel_context_t *ctx = opaque;
    wl_mutex_lock(&ctx->mutex);
    ctx->ready = true;
    wl_cond_signal(&ctx->changed);
    while (!ctx->go)
        wl_cond_wait(&ctx->changed, &ctx->mutex);
    wl_mutex_unlock(&ctx->mutex);
    CHECK(wl_evaluation_control_active_attempt_id(ctx->control) == 1);
    wl_evaluation_control_request_cancel(ctx->control);
    wl_evaluation_control_release(ctx->control);
    return NULL;
}

static void
cross_thread_cancel(void)
{
    int owner;
    cancel_context_t ctx = { 0 };
    wl_thread_t thread;
    CHECK(wl_mutex_init(&ctx.mutex) == 0);
    CHECK(wl_cond_init(&ctx.changed) == 0);
    CHECK(wl_evaluation_control_create(10, &ctx.control) == 0);
    CHECK(wl_evaluation_control_attach(ctx.control, &owner) == 0);
    CHECK(wl_evaluation_control_begin(ctx.control, &owner) == 0);
    CHECK(wl_evaluation_control_retain(ctx.control) == 0);
    CHECK(wl_thread_create(&thread, cancel_thread, &ctx) == 0);
    wl_mutex_lock(&ctx.mutex);
    while (!ctx.ready)
        wl_cond_wait(&ctx.changed, &ctx.mutex);
    CHECK(wl_evaluation_control_charge(ctx.control, 1) == 0);
    ctx.go = true;
    wl_cond_signal(&ctx.changed);
    wl_mutex_unlock(&ctx.mutex);
    CHECK(wl_thread_join(&thread) == 0);
    CHECK(wl_evaluation_control_charge(ctx.control,
        1) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(ctx.control, &owner, 0,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_detach(ctx.control, &owner) == 0);
    wl_evaluation_control_release(ctx.control);
    wl_cond_destroy(&ctx.changed);
    wl_mutex_destroy(&ctx.mutex);
}

typedef struct {
    wl_evaluation_control_t *control;
    uint64_t admitted;
    int result;
} charge_context_t;

static void *
charge_thread(void *opaque)
{
    charge_context_t *ctx = opaque;
    while ((ctx->result = wl_evaluation_control_charge(ctx->control, 1)) == 0)
        ctx->admitted++;
    return NULL;
}

static void
shared_allowance(void)
{
    int owner;
    wl_evaluation_control_t *c;
    wl_evaluation_control_report_t report;
    wl_thread_t threads[8];
    charge_context_t contexts[8] = { 0 };
    CHECK(wl_evaluation_control_create(10003, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    for (unsigned i = 0; i < 8; i++) {
        contexts[i].control = c;
        CHECK(wl_thread_create(&threads[i], charge_thread, &contexts[i]) == 0);
    }
    uint64_t sum = 0;
    for (unsigned i = 0; i < 8; i++) {
        CHECK(wl_thread_join(&threads[i]) == 0);
        CHECK(contexts[i].result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        sum += contexts[i].admitted;
    }
    CHECK(sum == 10003);
    CHECK(wl_evaluation_control_finish(c, &owner, 0,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_evaluation_control_report(c,
        &report) == 0 && report.consumed == sum);
    CHECK(wl_evaluation_control_detach(c, &owner) == 0);
    wl_evaluation_control_release(c);
}

typedef struct {
    uint64_t identity;
    int result;
} identity_context_t;

static void *
create_identity(void *opaque)
{
    identity_context_t *ctx = opaque;
    wl_evaluation_control_t *control = NULL;
    ctx->result = wl_evaluation_control_create(0, &control);
    if (ctx->result == 0) {
        ctx->identity = wl_evaluation_control_identity(control);
        wl_evaluation_control_release(control);
    }
    return NULL;
}

static void
identity_generation(void)
{
    wl_thread_t threads[8];
    identity_context_t contexts[8] = { 0 };
    for (unsigned i = 0; i < 8; i++)
        CHECK(wl_thread_create(&threads[i], create_identity,
            &contexts[i]) == 0);
    for (unsigned i = 0; i < 8; i++) {
        CHECK(wl_thread_join(&threads[i]) == 0);
        CHECK(contexts[i].result == 0 && contexts[i].identity != 0);
        for (unsigned j = 0; j < i; j++)
            CHECK(contexts[i].identity != contexts[j].identity);
    }
}

int
main(void)
{
    boundaries();
    limits_and_errors();
    active_identity();
    attempt_scoped_charge();
    cross_thread_cancel();
    shared_allowance();
    identity_generation();
    CHECK(wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1) == 0);
    wl_evaluation_control_t *last = NULL;
    CHECK(wl_evaluation_control_create(0, &last) == 0);
    CHECK(wl_evaluation_control_identity(last) == UINT64_MAX - 1);
    wl_evaluation_control_release(last);
    wl_evaluation_control_t *exhausted = NULL;
    CHECK(wl_evaluation_control_create(0, &exhausted) == EOVERFLOW);
    CHECK(exhausted == NULL);
    puts("evaluation_control: OK");
    return 0;
}
