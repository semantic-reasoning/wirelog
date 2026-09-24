/*
 * test_evaluation_attempt_owner.c - the evaluation attempt boundary must use
 * the owner key the control was attached with, and must observe a cancellation
 * that is already pending before it runs anything.
 *
 * Issue #1821, repairing two defects in the #1820 remainder rather than all of
 * it: #1820's checkpoints, its retry-equivalence test and its MAX_ITERATIONS
 * audit are still absent.
 *
 * wirelog/session.c attaches the control to the session's operation admission
 * and wirelog/session.h documents that admission as the owner key, but both
 * columnar attempt wrappers -- step and snapshot -- passed the session itself.
 * wl_evaluation_control_begin rejects a mismatched owner with EINVAL, so every
 * controlled attempt failed before it started.  Fixing only that would have
 * made things worse: begin returns 0 when cancellation is already pending, and
 * neither begin nor finish reads that flag, so a cancelled attempt would have
 * run in full and been reported as a clean success.  charge(0) is the only
 * reader, so the wrappers poll it before doing any work.
 *
 * Both defects were invisible because wl_session_step and wl_session_snapshot
 * short-circuit to ENOTSUP whenever a control is attached, so nothing reached
 * the wrappers.  This test reaches them through the backend vtable, which is
 * deliberate and single-threaded only: a direct vtable call skips
 * wl_session_operation_begin/_end, so it is invisible to the admission drain
 * and must not be copied into a threaded fixture.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/evaluation_control.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
#include <stdio.h>

static wl_plan_t *
build_plan(void)
{
    const char *src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(x, y) :- edge(x, y).\n"
        "reach(x, z) :- reach(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return NULL;
    }
    plan_fixture_hold(prog);
    return plan;
}

struct emitted {
    unsigned count;
};

static void
count_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    ((struct emitted *)user_data)->count++;
}

static int
expect(const char *name, int ok)
{
    printf("%s ... %s\n", name, ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}

int
main(void)
{
    int failed = 0;
    wl_plan_t *plan = build_plan();
    failed += expect("plan builds", plan != NULL);
    if (!plan)
        return 1;

    wl_evaluation_control_t *control = NULL;
    int rc = wl_evaluation_control_create(0, &control); /* 0 = unlimited */
    failed += expect("control creates", rc == 0 && control != NULL);
    if (rc != 0 || !control) {
        wl_plan_free(plan);
        return 1;
    }

    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;

    wl_session_t *base = NULL;
    rc = wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &base);
    failed += expect("controlled columnar session creates",
            rc == 0 && base != NULL);
    if (rc != 0 || !base) {
        wl_evaluation_control_release(control);
        wl_plan_free(plan);
        return 1;
    }

    /* The owner key the control was actually attached with. */
    failed += expect("session holds the admission the control is keyed to",
            base->operation_admission != NULL);

    /*
     * Both public entry points refuse while a control is attached
     * (wirelog/session.c, "#1820-#1822 replace after complete enforcement").
     * These are tripwires, not false-pass guards: when later work lifts the
     * gate they fail, which is the signal to re-route this test through the
     * public API instead of the vtable.
     */
    failed += expect("public step stays gated while a control is attached",
            wl_session_step(base) == ENOTSUP);
    failed += expect("public snapshot stays gated while a control is attached",
            wl_session_snapshot(base, NULL, NULL) == ENOTSUP);

    /* No attempt has completed yet, so there is no report to read. */
    wl_evaluation_control_report_t report;
    failed += expect("no attempt is recorded before the wrapper runs",
            wl_evaluation_control_report(control, &report) == ENOENT);

    /*
     * Facts, so a snapshot below emits something and "emitted nothing" can
     * fail.  Without them every emission count would be zero either way.
     */
    static const int64_t edges[] = { 1, 2, 2, 3 };
    failed += expect("edge facts insert",
            wl_session_insert(base, "edge", edges, 2, 2) == 0);

    /*
     * Reaching the backend vtable directly is the only route to the wrappers
     * while the gate stands.  What is under test is the boundary, not whatever
     * this plan returns, so the assertions read the control, not the rc.
     */
    (void)base->backend->session_step(base);
    int report_rc = wl_evaluation_control_report(control, &report);
    failed += expect("the step boundary accepted its own owner key",
            report_rc == 0);
    failed += expect("exactly one attempt completed",
            report_rc == 0 && report.attempt_id == 1);

    /* The snapshot wrapper carries the same fix and needs its own coverage. */
    struct emitted live = { 0 };
    (void)base->backend->session_snapshot(base, count_tuple, &live);
    report_rc = wl_evaluation_control_report(control, &report);
    failed += expect("the snapshot boundary accepted its own owner key",
            report_rc == 0 && report.attempt_id == 2);
    failed += expect("an uncancelled snapshot emits tuples",
            live.count > 0);

    /*
     * Pre-cancellation.  begin still returns 0, so only the charge(0) poll can
     * catch this.  A wrapper without the poll runs the whole operation and
     * reports success, which is what these three assertions exist to catch.
     */
    wl_evaluation_control_request_cancel(control);
    struct emitted after_cancel = { 0 };
    int snap_rc = base->backend->session_snapshot(base, count_tuple,
            &after_cancel);
    failed += expect("a pre-cancelled snapshot reports cancellation",
            snap_rc == WL_EVALUATION_CONTROL_CANCELLED);
    failed += expect("a pre-cancelled snapshot emits nothing",
            after_cancel.count == 0);
    report_rc = wl_evaluation_control_report(control, &report);
    failed += expect("the cancelled attempt is recorded as cancelled",
            report_rc == 0
            && report.attempt_id == 3
            && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
            && report.result == WL_EVALUATION_CONTROL_CANCELLED);

    /* Cancellation stays latched for the next attempt. */
    failed += expect("a step after cancellation is refused too",
            base->backend->session_step(base)
            == WL_EVALUATION_CONTROL_CANCELLED);

    wl_session_destroy(base);
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    return failed == 0 ? 0 : 1;
}
