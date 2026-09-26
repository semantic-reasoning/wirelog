/*
 * test_evaluation_retry.c - the publication cutoff, and what a stop there
 * must leave behind.
 *
 * Issue #1953 item 1 (successor to #1820).  wl_evaluation_control_finish does
 * not poll: "the integration must make its final charge(0) check before
 * committing publication".  col_session_step_impl had no such check, so a
 * cancellation arriving after the wrapper's entry poll was observed only by
 * the NEXT attempt -- the current one published its events and reported
 * success.
 *
 * The cutoff sits after compaction and before the delta observer is prepared.
 * A stop there must publish nothing AND must leave the attempt retryable: the
 * bookkeeping that tells the next attempt "this step already succeeded" must
 * not be committed.
 *
 * These cases inject the cancellation through the existing test hooks rather
 * than through a budget, because a budget that only stops at entry proves
 * nothing about mid-evaluation recovery (#1820 says so explicitly).  The hooks
 * live under WL_SESSION_TEST_HOOKS, which testlib_prod -- the library this test
 * links -- defines; the installed library carries no hook symbol.
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

extern void (*wl_columnar_eval_delta_test_observer_boundary)(wl_col_session_t *,
    unsigned);
extern void (*wl_columnar_eval_serial_test_after_plan)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *);

static int failed;
static unsigned delta_events;
static unsigned cancel_at_boundary;      /* 1 = before the cutoff, 2 = after */
static unsigned boundary_hits;
static unsigned boundary2_hits;
static unsigned plan_hits;
static bool cancel_on_plan;

static int
expect(const char *name, int ok)
{
    printf("%s ... %s\n", name, ok ? "PASS" : "FAIL");
    if (!ok)
        failed++;
    return ok;
}

static void
count_delta(const char *relation, const int64_t *row, uint32_t ncols, int diff,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)diff;
    (void)user_data;
    delta_events++;
}

static void
on_boundary(wl_col_session_t *sess, unsigned boundary)
{
    boundary_hits++;
    if (boundary == 2)
        boundary2_hits++;
    if (boundary == cancel_at_boundary && sess->base.evaluation_control)
        wl_evaluation_control_request_cancel(sess->base.evaluation_control);
}

static void
on_plan(wl_col_session_t *sess, eval_stack_t *stack, eval_entry_t *result)
{
    (void)stack;
    (void)result;
    plan_hits++;
    if (cancel_on_plan && sess->base.evaluation_control)
        wl_evaluation_control_request_cancel(sess->base.evaluation_control);
}

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

/* A controlled session reached through the backend vtable: wl_session_step
 * still short-circuits to ENOTSUP while a control is attached. */
static wl_session_t *
make_controlled(wl_plan_t *plan, wl_evaluation_control_t *control, bool with_cb)
{
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *base = NULL;
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1, &options,
        &base) != 0)
        return NULL;
    if (with_cb)
        wl_session_set_delta_cb(base, count_delta, NULL);
    return base;
}

static const int64_t edges[] = { 1, 2, 2, 3 };

int
main(void)
{
    wl_plan_t *plan = build_plan();
    if (!expect("plan builds", plan != NULL))
        return 1;

    /*
     * Anti-vacuity reference.  Without this an "emitted nothing" assertion
     * below would hold for a plan that emits nothing anyway.
     */
    unsigned reference_events;
    {
        wl_session_t *ref = NULL;
        if (!expect("reference session creates",
            wl_session_create(wl_backend_columnar(), plan, 1, &ref) == 0))
            return 1;
        wl_session_set_delta_cb(ref, count_delta, NULL);
        delta_events = 0;
        expect("reference insert",
            wl_session_insert(ref, "edge", edges, 2, 2) == 0);
        expect("reference step succeeds", wl_session_step(ref) == 0);
        reference_events = delta_events;
        expect("the reference step emits events", reference_events > 0);
        wl_session_destroy(ref);
    }

    /* --- a stop at the cutoff publishes nothing and stays retryable ------ */
    {
        wl_evaluation_control_t *control = NULL;
        expect("control creates",
            wl_evaluation_control_create(0, &control) == 0);
        wl_session_t *base = make_controlled(plan, control, true);
        if (!expect("controlled session creates", base != NULL))
            return 1;
        wl_col_session_t *sess = (wl_col_session_t *)base;

        expect("public step stays gated", wl_session_step(base) == ENOTSUP);
        expect("insert", wl_session_insert(base, "edge", edges, 2, 2) == 0);

        col_rel_t *reach = session_find_rel(sess, "reach");
        expect("the derived relation does not exist before the first step",
            reach == NULL);
        uint32_t base_before = reach ? reach->base_nrows : 0u;

        delta_events = 0;
        boundary_hits = 0;
        cancel_at_boundary = 1;              /* before the cutoff */
        wl_columnar_eval_delta_test_observer_boundary = on_boundary;
        int rc = base->backend->session_step(base);
        wl_columnar_eval_delta_test_observer_boundary = NULL;

        expect("the boundary hook actually fired", boundary_hits > 0);
        expect("a stop at the cutoff is reported as cancellation",
            rc == WL_EVALUATION_CONTROL_CANCELLED);
        expect("a stopped attempt publishes no events", delta_events == 0);

        wl_evaluation_control_report_t report;
        int report_rc = wl_evaluation_control_report(control, &report);
        expect("the attempt is recorded as cancelled",
            report_rc == 0
            && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
            && report.execution_status == 0);

        /* The state that makes the retry possible. */
        expect("the observer survives the stop", sess->delta_observer != NULL);
        expect("the observer stays evaluated and inactive",
            wl_columnar_eval_delta_observer_evaluated(sess)
            && !wl_columnar_eval_delta_observer_active(sess));
        expect("the input is still pending", sess->pending_input_change);
        expect("the stable snapshot is still invalid",
            !sess->snapshot_stable_valid);
        reach = session_find_rel(sess, "reach");
        expect("the commit block's baseline advance did not run",
            reach != NULL && reach->nrows > 0
            && reach->base_nrows == base_before);

        /* --- the retry delivers exactly what the reference delivered ---- */
        expect("cancellation clears between attempts",
            wl_evaluation_control_reset(control) == 0);
        delta_events = 0;
        plan_hits = 0;
        wl_columnar_eval_serial_test_after_plan = on_plan;
        rc = base->backend->session_step(base);
        wl_columnar_eval_serial_test_after_plan = NULL;
        expect("the retry succeeds", rc == 0);
        expect("the retry delivers the reference events",
            delta_events == reference_events);
        expect("the retry does not re-evaluate", plan_hits == 0);

        wl_session_destroy(base);
        wl_evaluation_control_release(control);
    }

    /* --- a stop AFTER the cutoff is deferred, not returned -------------- */
    {
        wl_evaluation_control_t *control = NULL;
        expect("control creates (deferral)",
            wl_evaluation_control_create(0, &control) == 0);
        wl_session_t *base = make_controlled(plan, control, true);
        if (!expect("controlled session creates (deferral)", base != NULL))
            return 1;

        expect("insert (deferral)",
            wl_session_insert(base, "edge", edges, 2, 2) == 0);
        delta_events = 0;
        boundary_hits = 0;
        boundary2_hits = 0;
        cancel_at_boundary = 2;              /* after the cutoff */
        wl_columnar_eval_delta_test_observer_boundary = on_boundary;
        int rc = base->backend->session_step(base);
        wl_columnar_eval_delta_test_observer_boundary = NULL;

        expect("the post-cutoff boundary fired", boundary2_hits > 0);
        expect("a stop after the cutoff does not stop this attempt", rc == 0);
        expect("this attempt still delivers its events",
            delta_events == reference_events);
        expect("the next attempt is refused instead",
            base->backend->session_step(base)
            == WL_EVALUATION_CONTROL_CANCELLED);

        wl_session_destroy(base);
        wl_evaluation_control_release(control);
    }

    /* --- the plain step has no observer, and still has a cutoff --------- */
    {
        wl_evaluation_control_t *control = NULL;
        expect("control creates (plain)",
            wl_evaluation_control_create(0, &control) == 0);
        wl_session_t *base = make_controlled(plan, control, false);
        if (!expect("controlled session creates (plain)", base != NULL))
            return 1;
        wl_col_session_t *sess = (wl_col_session_t *)base;

        expect("insert (plain)",
            wl_session_insert(base, "edge", edges, 2, 2) == 0);
        plan_hits = 0;
        cancel_on_plan = true;
        wl_columnar_eval_serial_test_after_plan = on_plan;
        int rc = base->backend->session_step(base);
        wl_columnar_eval_serial_test_after_plan = NULL;
        cancel_on_plan = false;

        expect("the plain-step lever fired", plan_hits > 0);
        expect("no observer exists on the plain step",
            sess->delta_observer == NULL);
        expect("a plain step stops at the cutoff too",
            rc == WL_EVALUATION_CONTROL_CANCELLED);
        expect("the plain step's input is still pending",
            sess->pending_input_change);

        /*
         * Reset first.  With the cancellation still latched the wrapper's
         * entry poll refuses the next call before col_session_step_impl runs,
         * so the completion latch below would never be exercised -- the stop
         * would mask a session bricked into EBUSY.
         */
        expect("cancellation clears between attempts (plain)",
            wl_evaluation_control_reset(control) == 0);
        int retry_rc = base->backend->session_step(base);
        expect("the stop did not leave the completion latch set",
            retry_rc != EBUSY);
        expect("the plain step retries successfully", retry_rc == 0);

        wl_session_destroy(base);
        wl_evaluation_control_release(control);
    }

    wl_plan_free(plan);
    return failed == 0 ? 0 : 1;
}
