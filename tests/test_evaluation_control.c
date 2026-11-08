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
active_consumed_observation(void)
{
    int owner;
    wl_evaluation_control_t *control = NULL;
    uint64_t value = UINT64_MAX;
    CHECK(!wl_evaluation_control_create(3, &control));
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(wl_evaluation_control_test_active_consumed(control, 1,
        &value) == EINVAL
        && value == UINT64_MAX);
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(!wl_evaluation_control_charge(control, 1));
    CHECK(!wl_evaluation_control_test_active_consumed(control, 1,
        &value) && value == 1);
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(!wl_evaluation_control_charge(control, 2));
    CHECK(!wl_evaluation_control_test_active_consumed(control, 2,
        &value) && value == 2);
    wl_evaluation_control_report_t prior;
    CHECK(!wl_evaluation_control_report(control, &prior)
        && prior.attempt_id == 1 && prior.consumed == 1);
    value = UINT64_MAX;
    CHECK(wl_evaluation_control_test_active_consumed(control, 1,
        &value) == EINVAL && value == UINT64_MAX);
    CHECK(wl_evaluation_control_test_active_consumed(NULL, 2,
        &value) == EINVAL && value == UINT64_MAX);
    CHECK(wl_evaluation_control_test_active_consumed(control, 0,
        &value) == EINVAL && value == UINT64_MAX);
    CHECK(wl_evaluation_control_test_active_consumed(control, 2,
        NULL) == EINVAL);
    CHECK(wl_evaluation_control_test_active_consumed(control, 2,
        (uint64_t *)(void *)control) == EINVAL);
    CHECK(wl_evaluation_control_test_active_consumed(control, 2,
        (uint64_t *)(uintptr_t)UINTPTR_MAX) == EINVAL);
    CHECK(wl_evaluation_control_charge(control,
        2) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_test_active_consumed(control, 2,
        &value) && value == 2);
    CHECK(wl_evaluation_control_charge(control,
        1) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_test_active_consumed(control, 2,
        &value) && value == 2);
    CHECK(!wl_evaluation_control_report(control, &prior)
        && prior.attempt_id == 1 && prior.consumed == 1);
    CHECK(wl_evaluation_control_finish(control, &owner, 0,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    value = UINT64_MAX;
    CHECK(wl_evaluation_control_test_active_consumed(control, 2,
        &value) == EINVAL && value == UINT64_MAX);
    CHECK(!wl_evaluation_control_report(control, &prior)
        && prior.attempt_id == 2 && prior.consumed == 2);
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
}

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

/* Core component checks. Actual paired lexical Session issuance remains a
* separate caller proof; the expected fields are never a Native permit. */
static void
lexical_finance_close(void)
{
    for (unsigned mode = 0; mode < 6; ++mode) {
        uint64_t owner[8] = {0}, scope[8] = {0}, foreign[8] = {0};
        wl_evaluation_control_t *c = NULL;
        CHECK(!wl_evaluation_control_create(3, &c));
        CHECK(!wl_evaluation_control_attach(c, owner));
        CHECK(!wl_evaluation_control_begin(c, owner));
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        CHECK(!wl_evaluation_control_charge_attempt(c, id, 1));
        wl_evaluation_control_lexical_finance_expectation_t e = {
            .owner = owner, .scope = scope,
            .owner_bytes = sizeof(owner), .scope_bytes = sizeof(scope),
            .identity = wl_evaluation_control_identity(c), .limit = 3,
            .id = id, .charged = 1, .active = true,
        };
        /* Readonly/refused queries leave this request unlatched. Mode4's
         * valid close polls it; mode5 independently FINISHes first to prove
         * the prior rejected/proof calls did not poll. */
        if (mode == 4 || mode == 5) wl_evaluation_control_request_cancel(c);
        CHECK(!wl_evaluation_control_lexical_finance_prove_checked(c, &e));
        CHECK(wl_evaluation_control_lexical_finance_prove_checked(c,
            (const wl_evaluation_control_lexical_finance_expectation_t *)(void *)
            c) == EINVAL);
        for (unsigned bad = 0; bad < 11; ++bad) {
            wl_evaluation_control_lexical_finance_expectation_t wrong = e;
            switch (bad) {
            case 0: wrong.owner = foreign; break;
            case 1: ++wrong.identity; break;
            case 2: ++wrong.limit; break;
            case 3: ++wrong.id; break;
            case 4: ++wrong.charged; break;
            case 5: wrong.scope = owner; break;
            case 6: wrong.owner = c; break;
            case 7: wrong.scope = c; break;
            case 8: wrong.scope = (void *)(uintptr_t)UINTPTR_MAX; break;
            case 9: wrong.owner = &wrong; break;
            default: wrong.scope = &wrong; break;
            }
            CHECK(wl_evaluation_control_lexical_finance_prove_checked(c,
                &wrong) == EINVAL);
            wl_evaluation_control_lexical_finance_result_t refused =
                wl_evaluation_control_lexical_finance_close_checked(c, &wrong);
            CHECK(refused.protocol == EINVAL && !refused.closed &&
                !refused.report_valid);
            uint64_t used = UINT64_MAX;
            CHECK(wl_evaluation_control_active_attempt_id(c) == id
                && !wl_evaluation_control_test_active_consumed(c, id,
                &used) && used == 1);
            wl_evaluation_control_report_t report;
            CHECK(wl_evaluation_control_report(c, &report) == ENOENT);
        }
        CHECK(!wl_evaluation_control_retain(c));
        CHECK(wl_evaluation_control_lexical_finance_prove_checked(c,
            &e) == EBUSY);
        wl_evaluation_control_lexical_finance_result_t borrowed =
            wl_evaluation_control_lexical_finance_close_checked(c, &e);
        CHECK(borrowed.protocol == EBUSY && !borrowed.closed &&
            !borrowed.report_valid);
        wl_evaluation_control_release(c);
        int expected = 0;
        if (mode == 1) {
            wl_evaluation_control_request_cancel(c);
            /* A readonly owner proof cannot latch cancellation or charge. */
            CHECK(!wl_evaluation_control_lexical_finance_prove_checked(c, &e));
            CHECK(wl_evaluation_control_charge_attempt(c, id,
                0) == WL_EVALUATION_CONTROL_CANCELLED);
            expected = WL_EVALUATION_CONTROL_CANCELLED;
        } else if (mode == 2) {
            CHECK(wl_evaluation_control_charge_attempt(c, id,
                3) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            expected = WL_EVALUATION_CONTROL_WORK_LIMIT;
        } else if (mode == 3 || mode == 5) {
            CHECK(!wl_evaluation_control_finish(c, owner, 0, 0));
            e.active = false; e.id = 0;
            CHECK(!wl_evaluation_control_lexical_finance_prove_checked(c, &e));
        } else if (mode == 4) expected = WL_EVALUATION_CONTROL_CANCELLED;
        wl_evaluation_control_lexical_finance_result_t closed =
            wl_evaluation_control_lexical_finance_close_checked(c, &e);
        /* The actual allocation and both refs are gone. Only value results
         * remain readable; never query the old c afterward. */
        CHECK(!closed.protocol && closed.closed && closed.report_valid
            && closed.result == expected && closed.report.result == expected
            && closed.checkpoint_result == expected
            && closed.report.attempt_id == id && closed.report.limit == 3
            && closed.report.consumed == 1 && !closed.report.execution_status
            && !closed.report.cleanup_status &&
            (int)closed.report.stop_reason == expected);
    }
}

static uint64_t finance_peer_identity;

static void
finance_birth_peer(void)
{
    wl_evaluation_control_test_finance_birth_claim = NULL;
    wl_evaluation_control_t *peer = NULL;
    CHECK(!wl_evaluation_control_create(1, &peer));
    finance_peer_identity = wl_evaluation_control_identity(peer);
    wl_evaluation_control_release(peer);
}

static void
lexical_finance_birth(void)
{
    uint64_t owner[8] = {0}, scope[8] = {0};
    wl_evaluation_control_test_finance_birth_claim = finance_birth_peer;
    wl_evaluation_control_lexical_finance_birth_result_t born =
        wl_evaluation_control_lexical_finance_birth_try(3, owner);
    /* A genuine peer claimed the observed identity: this constructor loses
     * its single strong CAS before allocation, and does not retry here. */
    CHECK(born.protocol == EAGAIN && !born.control && !born.id && !born.identity
        && finance_peer_identity &&
        !wl_evaluation_control_test_finance_birth_claim);
    uint64_t next_identity = finance_peer_identity + 1;
    for (unsigned fault = 0; fault < 3; ++fault) {
        if (fault) {
            wl_evaluation_control_test_finance_birth_fail = fault;
            born = wl_evaluation_control_lexical_finance_birth_try(3, owner);
            CHECK(born.protocol == ENOMEM && !born.control && !born.id &&
                !born.identity
                && !wl_evaluation_control_test_finance_birth_fail);
            ++next_identity; /* A claimed failed identity is never reused. */
        }
        born = wl_evaluation_control_lexical_finance_birth_try(3, owner);
        CHECK(!born.protocol && born.control && born.id &&
            born.identity == next_identity++);
        CHECK(wl_evaluation_control_active_attempt_id(born.control) == born.id);
        wl_evaluation_control_lexical_finance_expectation_t e = {
            .owner = owner, .scope = scope,
            .owner_bytes = sizeof(owner), .scope_bytes = sizeof(scope),
            .identity = born.identity, .limit = 3, .id = born.id,
            .active = true,
        };
        CHECK(!wl_evaluation_control_lexical_finance_prove_checked(born.control,
            &e));
        wl_evaluation_control_lexical_finance_result_t closed =
            wl_evaluation_control_lexical_finance_close_checked(born.control,
                &e);
        CHECK(!closed.protocol && !closed.result && !closed.checkpoint_result
            && closed.closed && closed.report_valid &&
            closed.report.attempt_id == born.id
            && closed.report.limit == 3 && !closed.report.consumed);
    }
    born = wl_evaluation_control_lexical_finance_birth_try(0, owner);
    CHECK(born.protocol == EINVAL && !born.control && !born.id &&
        !born.identity);
    born = wl_evaluation_control_lexical_finance_birth_try(3, NULL);
    CHECK(born.protocol == EINVAL && !born.control && !born.id &&
        !born.identity);
}

static void retain_peer(wl_evaluation_control_t *control) {
    wl_evaluation_control_test_retain_claim = NULL;
    CHECK(!wl_evaluation_control_retain(control));
}

static void bounded_references(void) {
    uint64_t owner[8] = {0}, scope[8] = {0};
    CHECK(wl_evaluation_control_retain_try(NULL) == EINVAL);
    wl_evaluation_control_lexical_finance_birth_result_t born =
        wl_evaluation_control_lexical_finance_birth_try(3, owner);
    CHECK(!born.protocol);
    wl_evaluation_control_lexical_finance_expectation_t e = {
        .owner = owner,
        .scope = scope,
        .owner_bytes = sizeof(owner),
        .scope_bytes = sizeof(scope),
        .identity = born.identity,
        .limit = 3,
        .id = born.id,
        .active = true,
    };
    CHECK(!wl_evaluation_control_charge(born.control, 1));
    e.charged = 1;
    wl_evaluation_control_request_cancel(born.control);
    wl_evaluation_control_test_retain_claim = retain_peer;
    CHECK(wl_evaluation_control_retain_try(born.control) == EAGAIN);
    CHECK(!wl_evaluation_control_test_retain_claim);
    CHECK(wl_evaluation_control_lexical_finance_prove_checked(born.control,
        &e) ==
        EBUSY);
    wl_evaluation_control_release(born.control); /* Real peer's reference. */
    CHECK(!wl_evaluation_control_lexical_finance_prove_checked(
            born.control,
            &e)); /* Exactly two refs: failed try acquired none. No cancel poll. */
    CHECK(!wl_evaluation_control_retain_try(born.control));
    CHECK(wl_evaluation_control_lexical_finance_prove_checked(born.control,
        &e) ==
        EBUSY);
    wl_evaluation_control_release(born.control); /* Successful try's ref. */
    CHECK(!wl_evaluation_control_lexical_finance_prove_checked(born.control,
        &e));
    wl_evaluation_control_lexical_finance_result_t closed =
        wl_evaluation_control_lexical_finance_close_checked(born.control, &e);
    CHECK(!closed.protocol && closed.closed && closed.report_valid &&
        closed.checkpoint_result == WL_EVALUATION_CONTROL_CANCELLED &&
        closed.report.consumed == 1);
}

static void private_financier_birth(void) {
    int owner;
    for (uint64_t limit = 0; limit < 2; ++limit) {
        wl_evaluation_control_test_finance_birth_claim = finance_birth_peer;
        wl_evaluation_control_lexical_finance_birth_result_t born =
            wl_evaluation_control_private_financier_birth_try(limit, &owner);
        CHECK(born.protocol == EAGAIN && !born.control && !born.id &&
            !born.identity && !wl_evaluation_control_test_finance_birth_claim);
        uint64_t next_identity = finance_peer_identity + 1;
        for (unsigned fault = 0; fault < 3; ++fault) {
            if (fault) {
                wl_evaluation_control_test_finance_birth_fail = fault;
                born = wl_evaluation_control_private_financier_birth_try(limit,
                        &owner);
                CHECK(born.protocol == ENOMEM && !born.control && !born.id &&
                    !born.identity &&
                    !wl_evaluation_control_test_finance_birth_fail);
                ++next_identity;
            }
            born = wl_evaluation_control_private_financier_birth_try(limit,
                    &owner);
            CHECK(!born.protocol && born.control && born.id &&
                born.identity == next_identity++);
            CHECK(wl_evaluation_control_active_attempt_id(born.control) ==
                born.id);
            CHECK(!wl_evaluation_control_charge(born.control, 1));
            CHECK(wl_evaluation_control_finish(born.control, &owner, 7,
                0) == 7);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(born.control, &report));
            CHECK(report.attempt_id == born.id && report.limit == limit &&
                report.consumed == 1 && report.execution_status == 7 &&
                report.result == 7 && !report.cleanup_status &&
                report.stop_reason == WL_EVALUATION_CONTROL_NONE);
            CHECK(!wl_evaluation_control_retain_try(born.control));
            wl_evaluation_control_release(born.control);
            CHECK(!wl_evaluation_control_begin(born.control, &owner));
            CHECK(wl_evaluation_control_active_attempt_id(born.control) !=
                report.attempt_id);
            wl_evaluation_control_report_t unchanged;
            CHECK(!wl_evaluation_control_report(born.control, &unchanged));
            CHECK(unchanged.attempt_id == report.attempt_id &&
                unchanged.result == 7 && unchanged.consumed == 1);
            CHECK(!wl_evaluation_control_finish(born.control, &owner, 0, 0));
            CHECK(!wl_evaluation_control_detach(born.control, &owner));
            wl_evaluation_control_release(born.control);
        }
    }
    wl_evaluation_control_lexical_finance_birth_result_t invalid =
        wl_evaluation_control_private_financier_birth_try(0, NULL);
    CHECK(invalid.protocol == EINVAL && !invalid.control && !invalid.id &&
        !invalid.identity);
}

int
main(void)
{
    active_consumed_observation();
    boundaries();
    limits_and_errors();
    active_identity();
    attempt_scoped_charge();
    cross_thread_cancel();
    shared_allowance();
    identity_generation();
    lexical_finance_close();
    lexical_finance_birth();
    bounded_references();
    private_financier_birth();
    CHECK(wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1) == 0);
    wl_evaluation_control_t *last = NULL;
    CHECK(wl_evaluation_control_create(0, &last) == 0);
    CHECK(wl_evaluation_control_identity(last) == UINT64_MAX - 1);
    wl_evaluation_control_release(last);
    wl_evaluation_control_t *exhausted = NULL;
    CHECK(wl_evaluation_control_create(0, &exhausted) == EOVERFLOW);
    CHECK(exhausted == NULL);
    int exhausted_owner;
    wl_evaluation_control_lexical_finance_birth_result_t no_identity =
        wl_evaluation_control_lexical_finance_birth_try(1, &exhausted_owner);
    CHECK(no_identity.protocol == EOVERFLOW && !no_identity.control
        && !no_identity.id && !no_identity.identity);
    no_identity = wl_evaluation_control_private_financier_birth_try(0,
            &exhausted_owner);
    CHECK(no_identity.protocol == EOVERFLOW && !no_identity.control
        && !no_identity.id && !no_identity.identity);
    puts("evaluation_control: OK");
    return 0;
}
