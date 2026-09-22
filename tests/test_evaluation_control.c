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
    CHECK(wl_evaluation_control_report(c, &report) == ENOENT);
    CHECK(wl_evaluation_control_begin(c, &owner) == EINVAL);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == EBUSY);
    CHECK(wl_evaluation_control_attach(c, &other) == EBUSY);
    CHECK(wl_evaluation_control_begin(c, &other) == EINVAL);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
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

int
main(void)
{
    boundaries();
    limits_and_errors();
    cross_thread_cancel();
    shared_allowance();
    puts("evaluation_control: OK");
    return 0;
}
