/* Genuine controller generations; no SOURCE tariff or public host claim. */
#include "wirelog/evaluation_control.h"
#include "wirelog/thread.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); exit(1); \
                      } } while (0)

static wl_evaluation_control_t *
create_control(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(limit, &control));
    CHECK(!wl_evaluation_control_attach(control, owner));
    return control;
}

static void
destroy_control(wl_evaluation_control_t *control, const void *owner)
{
    CHECK(!wl_evaluation_control_detach(control, owner));
    wl_evaluation_control_release(control);
}

static void
expect_report(const wl_evaluation_control_report_t *report, uint64_t id,
    uint64_t limit, uint64_t consumed, wl_evaluation_control_stop_t stop,
    int execution, int cleanup, int result)
{
    CHECK(report->attempt_id == id);
    CHECK(report->limit == limit);
    CHECK(report->consumed == consumed);
    CHECK(report->stop_reason == stop);
    CHECK(report->execution_status == execution);
    CHECK(report->cleanup_status == cleanup);
    CHECK(report->result == result);
}

static void
same_report(const wl_evaluation_control_report_t *left,
    const wl_evaluation_control_report_t *right)
{
    expect_report(left, right->attempt_id, right->limit, right->consumed,
        right->stop_reason, right->execution_status, right->cleanup_status,
        right->result);
}

static void
expect_consumed(wl_evaluation_control_t *control, uint64_t id, uint64_t amount)
{
    uint64_t consumed = UINT64_MAX;
    CHECK(wl_evaluation_control_active_attempt_id(control) == id);
    CHECK(!wl_evaluation_control_test_active_consumed(control, id, &consumed));
    CHECK(consumed == amount);
}

static void
argument_report_and_successor(void)
{
    int owner, foreign;
    wl_evaluation_control_t *control = create_control(7, &owner);
    wl_evaluation_control_attempt_begin_result_t begin =
        wl_evaluation_control_begin_attempt(NULL, &owner);
    CHECK(begin.protocol == EINVAL && !begin.attempt_id);
    begin = wl_evaluation_control_begin_attempt(control, NULL);
    CHECK(begin.protocol == EINVAL && !begin.attempt_id);
    begin = wl_evaluation_control_begin_attempt(control, &foreign);
    CHECK(begin.protocol == EINVAL && !begin.attempt_id);
    begin = wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(!begin.protocol && begin.attempt_id == 1);
    wl_evaluation_control_attempt_begin_result_t busy =
        wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(busy.protocol == EBUSY && !busy.attempt_id);
    expect_consumed(control, 1, 0);
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(NULL, &owner, 1, 0, 0);
    CHECK(finish.protocol == EINVAL && !finish.report_valid
        && finish.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_REJECTED);
    finish = wl_evaluation_control_finish_attempt(control, NULL, 1, 0, 0);
    CHECK(finish.protocol == EINVAL && !finish.report_valid);
    finish = wl_evaluation_control_finish_attempt(control, &foreign, 1, 0, 0);
    CHECK(finish.protocol == EINVAL && !finish.report_valid);
    finish = wl_evaluation_control_finish_attempt(control, &owner, 0, 0, 0);
    CHECK(finish.protocol == EINVAL && !finish.report_valid);
    /* A differing future expectation is a mismatch, not proof of a successor. */
    finish = wl_evaluation_control_finish_attempt(control, &owner, 2, EIO, EIO);
    CHECK(!finish.protocol && !finish.report_valid
        && finish.kind ==
        WL_EVALUATION_CONTROL_ATTEMPT_FINISH_SUCCESSOR_MISMATCH);
    expect_consumed(control, 1, 0);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control, &report) == ENOENT);
    CHECK(!wl_evaluation_control_charge_attempt(control, 1, 5));
    finish = wl_evaluation_control_finish_attempt(control, &owner, 1, 0, 0);
    CHECK(!finish.protocol && finish.report_valid
        && finish.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED);
    expect_report(&finish.report, 1, 7, 5, WL_EVALUATION_CONTROL_NONE, 0, 0, 0);
    wl_evaluation_control_report_t original = finish.report;
    wl_evaluation_control_request_cancel(control);
    finish = wl_evaluation_control_finish_attempt(control, &owner, 1, EIO,
            ENOMEM);
    CHECK(!finish.protocol && finish.report_valid
        && finish.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_ALREADY_ENDED);
    same_report(&finish.report, &original);
    CHECK(!wl_evaluation_control_report(control, &report));
    same_report(&report, &original);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 9));
    begin = wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(!begin.protocol && begin.attempt_id == 2);
    CHECK(!wl_evaluation_control_charge_attempt(control, 2, 2));
    CHECK(!wl_evaluation_control_charge_attempt(control, 2, 0));
    wl_evaluation_control_request_cancel(control);
    busy = wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(busy.protocol == EBUSY && !busy.attempt_id);
    finish = wl_evaluation_control_finish_attempt(control, &foreign, 2, EIO,
            ENOMEM);
    CHECK(finish.protocol == EINVAL && !finish.report_valid
        && finish.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_REJECTED);
    finish = wl_evaluation_control_finish_attempt(control, &owner, 0, EIO,
            ENOMEM);
    CHECK(finish.protocol == EINVAL && !finish.report_valid
        && finish.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_REJECTED);
    expect_consumed(control, 2, 2);
    CHECK(!wl_evaluation_control_report(control, &report));
    same_report(&report, &original);
    finish = wl_evaluation_control_finish_attempt(control, &owner, 1,
            WL_EVALUATION_CONTROL_CANCELLED, ENOMEM);
    CHECK(!finish.protocol && !finish.report_valid
        && finish.kind ==
        WL_EVALUATION_CONTROL_ATTEMPT_FINISH_SUCCESSOR_MISMATCH);
    expect_consumed(control, 2, 2);
    CHECK(!wl_evaluation_control_report(control, &report));
    same_report(&report, &original);
    finish = wl_evaluation_control_finish_attempt(control, &owner, 2, 0, 0);
    CHECK(!finish.protocol && finish.report_valid
        && finish.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED);
    /* Neither the stale footer nor FINISH polls post-cutoff cancellation. */
    expect_report(&finish.report, 2, 9, 2, WL_EVALUATION_CONTROL_NONE, 0, 0, 0);
    begin = wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(!begin.protocol && begin.attempt_id == 3);
    CHECK(wl_evaluation_control_charge_attempt(control, 3, 0)
        == WL_EVALUATION_CONTROL_CANCELLED);
    finish = wl_evaluation_control_finish_attempt(control, &owner, 3, ENOSPC,
            EIO);
    CHECK(!finish.protocol && finish.report_valid);
    expect_report(&finish.report, 3, 9, 0, WL_EVALUATION_CONTROL_CANCELLED,
        ENOSPC, EIO, ENOSPC);
    destroy_control(control, &owner);

    /* Unattached controls cannot claim either transition. */
    CHECK(!wl_evaluation_control_create(1, &control));
    begin = wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(begin.protocol == EINVAL && !begin.attempt_id);
    finish = wl_evaluation_control_finish_attempt(control, &owner, 1, 0, 0);
    CHECK(finish.protocol == EINVAL && !finish.report_valid);
    wl_evaluation_control_release(control);
}

static void
generation_bound_detach(void)
{
    int owner = 0, foreign = 0;
    wl_evaluation_control_t *control = create_control(7, &owner);
    CHECK(wl_evaluation_control_detach_attempt(NULL, &owner, 1) == EINVAL);
    CHECK(wl_evaluation_control_detach_attempt(control, NULL, 1) == EINVAL);
    CHECK(wl_evaluation_control_detach_attempt(control, &owner, 0) == EINVAL);
    CHECK(wl_evaluation_control_detach_attempt(control, &foreign, 1) == EINVAL);
    wl_evaluation_control_attempt_begin_result_t begin =
        wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(!begin.protocol && begin.attempt_id == 1);
    CHECK(wl_evaluation_control_detach_attempt(control, &owner, 1) == EBUSY);
    CHECK(!wl_evaluation_control_charge_attempt(control, 1, 3));
    wl_evaluation_control_attempt_finish_result_t ended =
        wl_evaluation_control_finish_attempt(control, &owner, 1, 0, 0);
    CHECK(!ended.protocol && ended.report_valid);
    begin = wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(!begin.protocol && begin.attempt_id == 2);
    CHECK(wl_evaluation_control_detach_attempt(control, &owner, 1) == EBUSY);
    expect_consumed(control, 2, 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report));
    same_report(&report, &ended.report);
    ended = wl_evaluation_control_finish_attempt(control, &owner, 2, ENOSPC, 0);
    CHECK(!ended.protocol && ended.report_valid &&
        ended.report.result == ENOSPC);
    /* An ended successor is still foreign to the old close receipt. */
    CHECK(wl_evaluation_control_detach_attempt(control, &owner, 1) == EBUSY);
    CHECK(!wl_evaluation_control_report(control, &report));
    same_report(&report, &ended.report);
    CHECK(!wl_evaluation_control_detach_attempt(control, &owner, 2));
    CHECK(wl_evaluation_control_detach_attempt(control, &owner, 2) == EINVAL);
    wl_evaluation_control_release(control);
}

typedef enum { TEST_HISTORY_WORK, TEST_HISTORY_CANCEL_ERROR,
               TEST_HISTORY_CLEANUP, TEST_HISTORY_DISPOSITION,
               TEST_HISTORY_OVERFLOW,
               TEST_HISTORY_OVERFLOW_ERROR, TEST_HISTORY_FIRST_STOP } history_t;

static wl_evaluation_control_report_t
run_history(history_t history, bool typed)
{
    int owner;
    uint64_t limit = history == TEST_HISTORY_OVERFLOW ||
        history == TEST_HISTORY_OVERFLOW_ERROR ? 0 : 2;
    wl_evaluation_control_t *control = create_control(limit, &owner);
    if (typed) {
        wl_evaluation_control_attempt_begin_result_t begin =
            wl_evaluation_control_begin_attempt(control, &owner);
        CHECK(!begin.protocol && begin.attempt_id == 1);
    } else CHECK(!wl_evaluation_control_begin(control, &owner));
    int execution = 0, cleanup = 0;
    uint64_t consumed = 0;
    wl_evaluation_control_stop_t stop = WL_EVALUATION_CONTROL_NONE;
    int result = 0, reported_execution = 0;
    switch (history) {
    case TEST_HISTORY_WORK:
    case TEST_HISTORY_CLEANUP:
    case TEST_HISTORY_FIRST_STOP:
        CHECK(wl_evaluation_control_charge_attempt(control, 1, 3)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_request_cancel(control);
        CHECK(wl_evaluation_control_charge_attempt(control, 1, 0)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        stop = WL_EVALUATION_CONTROL_WORK_LIMIT;
        cleanup = history == TEST_HISTORY_CLEANUP ? EFAULT : 0;
        execution = history ==
            TEST_HISTORY_FIRST_STOP ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        result = cleanup ? cleanup : (int)stop;
        break;
    case TEST_HISTORY_CANCEL_ERROR:
        wl_evaluation_control_request_cancel(control);
        CHECK(wl_evaluation_control_charge_attempt(control, 1, 1)
            == WL_EVALUATION_CONTROL_CANCELLED);
        stop = WL_EVALUATION_CONTROL_CANCELLED;
        execution = reported_execution = result = EIO;
        cleanup = ENOMEM;
        break;
    case TEST_HISTORY_DISPOSITION:
        execution = WL_EVALUATION_CONTROL_CANCELLED;
        stop = WL_EVALUATION_CONTROL_CANCELLED;
        result = (int)stop;
        break;
    case TEST_HISTORY_OVERFLOW:
    case TEST_HISTORY_OVERFLOW_ERROR:
        CHECK(!wl_evaluation_control_charge_attempt(control, 1, UINT64_MAX));
        CHECK(wl_evaluation_control_charge_attempt(control, 1, 1) == EOVERFLOW);
        consumed = UINT64_MAX;
        cleanup = ENOMEM;
        execution = history == TEST_HISTORY_OVERFLOW_ERROR ? EIO : 0;
        reported_execution = result = execution ? execution : EOVERFLOW;
        break;
    }
    wl_evaluation_control_report_t report;
    if (typed) {
        wl_evaluation_control_attempt_finish_result_t finish =
            wl_evaluation_control_finish_attempt(control, &owner, 1, execution,
                cleanup);
        CHECK(!finish.protocol && finish.report_valid
            && finish.kind ==
            WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED);
        report = finish.report;
    } else {
        CHECK(wl_evaluation_control_finish(control, &owner, execution, cleanup)
            == result);
        CHECK(!wl_evaluation_control_report(control, &report));
    }
    expect_report(&report, 1, limit, consumed, stop, reported_execution,
        cleanup,
        result);
    wl_evaluation_control_report_t stored;
    CHECK(!wl_evaluation_control_report(control, &stored));
    same_report(&stored, &report);
    destroy_control(control, &owner);
    return report;
}

typedef struct {
    wl_evaluation_control_t *control;
    const void *owner;
    wl_mutex_t mutex;
    wl_cond_t condition;
    unsigned stage;
    unsigned history;
    wl_evaluation_control_attempt_begin_result_t successor;
} threaded_history_t;

static void
set_stage(threaded_history_t *history, unsigned stage)
{
    CHECK(!wl_mutex_lock(&history->mutex));
    history->stage = stage;
    CHECK(!wl_cond_broadcast(&history->condition));
    CHECK(!wl_mutex_unlock(&history->mutex));
}

static void
await_stage(threaded_history_t *history, unsigned stage)
{
    CHECK(!wl_mutex_lock(&history->mutex));
    while (history->stage < stage)
        CHECK(!wl_cond_wait(&history->condition, &history->mutex));
    CHECK(!wl_mutex_unlock(&history->mutex));
}

static void *
end_then_begin(void *payload)
{
    threaded_history_t *history = payload;
    await_stage(history, 1);
    int finished = wl_evaluation_control_finish(history->control,
            history->owner, 0, 0);
    CHECK(finished == (history->history == 2 ? EINVAL : 0));
    set_stage(history, 2);
    if (history->history == 1) await_stage(history, 3);
    history->successor = wl_evaluation_control_begin_attempt(history->control,
            history->owner);
    CHECK(!history->successor.protocol && history->successor.attempt_id == 2);
    set_stage(history, 4);
    return NULL;
}

static void
threaded_generations(void)
{
    for (unsigned selector = 0; selector < 3; ++selector) {
        int owner;
        threaded_history_t history = {0};
        history.control = create_control(9, &owner);
        history.owner = &owner;
        history.history = selector;
        CHECK(!wl_mutex_init(&history.mutex));
        CHECK(!wl_cond_init(&history.condition));
        wl_evaluation_control_attempt_begin_result_t original =
            wl_evaluation_control_begin_attempt(history.control, &owner);
        CHECK(!original.protocol && original.attempt_id == 1);
        CHECK(!wl_evaluation_control_charge_attempt(history.control, 1, 3));
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, end_then_begin, &history));
        wl_evaluation_control_attempt_finish_result_t finish = {0};
        if (selector == 2) {
            finish = wl_evaluation_control_finish_attempt(history.control,
                    &owner, original.attempt_id, 0, 0);
            CHECK(finish.kind ==
                WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED
                && !finish.protocol && finish.report_valid);
            expect_report(&finish.report, 1, 9, 3, WL_EVALUATION_CONTROL_NONE,
                0, 0, 0);
        }
        set_stage(&history, 1);
        if (selector == 1) {
            await_stage(&history, 2);
            finish = wl_evaluation_control_finish_attempt(history.control,
                    &owner, original.attempt_id, EIO, EIO);
            CHECK(!finish.protocol && finish.report_valid
                && finish.kind ==
                WL_EVALUATION_CONTROL_ATTEMPT_FINISH_ALREADY_ENDED);
            expect_report(&finish.report, 1, 9, 3, WL_EVALUATION_CONTROL_NONE,
                0, 0, 0);
            set_stage(&history, 3);
        }
        await_stage(&history, 4);
        CHECK(!wl_thread_join(&thread));
        if (selector == 0) {
            finish = wl_evaluation_control_finish_attempt(history.control,
                    &owner, original.attempt_id, EIO, EIO);
            CHECK(!finish.protocol && !finish.report_valid
                && finish.kind ==
                WL_EVALUATION_CONTROL_ATTEMPT_FINISH_SUCCESSOR_MISMATCH);
        }
        expect_consumed(history.control, 2, 0);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(history.control, &report));
        expect_report(&report, 1, 9, 3, WL_EVALUATION_CONTROL_NONE, 0, 0, 0);
        CHECK(!wl_evaluation_control_charge_attempt(history.control, 2, 1));
        finish = wl_evaluation_control_finish_attempt(history.control, &owner,
                history.successor.attempt_id, 0, 0);
        CHECK(!finish.protocol && finish.report_valid
            && finish.kind ==
            WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED);
        expect_report(&finish.report, 2, 9, 1, WL_EVALUATION_CONTROL_NONE, 0, 0,
            0);
        wl_cond_destroy(&history.condition);
        wl_mutex_destroy(&history.mutex);
        destroy_control(history.control, &owner);
    }
}

static void
full_width_exhaustion(void)
{
    int owner;
    wl_evaluation_control_t *control = create_control(9, &owner);
    CHECK(!wl_evaluation_control_test_seed_attempt_id(control, &owner,
        UINT64_MAX - 1));
    /* Advancing a generator is not evidence of a completed real attempt. */
    wl_evaluation_control_attempt_finish_result_t seeded =
        wl_evaluation_control_finish_attempt(control, &owner, UINT64_MAX - 1,
            0, 0);
    CHECK(seeded.protocol == EINVAL && !seeded.report_valid
        && seeded.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_REJECTED);
    wl_evaluation_control_attempt_begin_result_t begin =
        wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(!begin.protocol && begin.attempt_id == UINT64_MAX);
    CHECK(!wl_evaluation_control_charge_attempt(control, begin.attempt_id, 1));
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(control, &owner, begin.attempt_id,
            0, 0);
    CHECK(!finish.protocol && finish.report_valid);
    expect_report(&finish.report, UINT64_MAX, 9, 1, WL_EVALUATION_CONTROL_NONE,
        0, 0, 0);
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_configure(control, 10));
    begin = wl_evaluation_control_begin_attempt(control, &owner);
    CHECK(begin.protocol == EOVERFLOW && !begin.attempt_id);
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report));
    same_report(&report, &finish.report);
    destroy_control(control, &owner);
}

int
main(void)
{
    argument_report_and_successor();
    generation_bound_detach();
    for (history_t history = TEST_HISTORY_WORK;
        history <= TEST_HISTORY_FIRST_STOP; ++history) {
        wl_evaluation_control_report_t generic = run_history(history, false);
        wl_evaluation_control_report_t typed = run_history(history, true);
        same_report(&generic, &typed);
    }
    threaded_generations();
    full_width_exhaustion();
    puts("evaluation_attempt_identity: OK");
    return 0;
}
