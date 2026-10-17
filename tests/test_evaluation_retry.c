/* #1821: raw observer reference and fail-closed whole controlled STEP.
 * Mid-relation cutoff/retry is now exercised through the real aware entry in
 * evaluation_serial. Whole controlled delta-observer delivery remains deferred. */
#include "evaluation_guard_fixture.h"
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

/* Real attached-control session for public and backend refusal checks. */
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
    if (!expect("plan builds", plan != NULL)) return 1;
    wl_session_t *ref = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &ref)) return 1;
    wl_session_set_delta_cb(ref, count_delta, NULL);
    expect("reference input inserts",
        !wl_session_insert(ref, "edge", edges, 2, 2));
    delta_events = boundary_hits = boundary2_hits = plan_hits = 0;
    wl_columnar_eval_delta_test_observer_boundary = on_boundary;
    wl_columnar_eval_serial_test_after_plan = on_plan;
    expect("raw reference step", !wl_session_step(ref));
    wl_columnar_eval_delta_test_observer_boundary = NULL;
    wl_columnar_eval_serial_test_after_plan = NULL;
    expect("raw observer emits real events", delta_events > 0);
    expect("raw observer visits both sides of its cutoff",
        boundary_hits > 0 && boundary2_hits > 0);
    expect("raw plan actually executes", plan_hits > 0);
    wl_session_destroy(ref);

    for (unsigned callback = 0; callback < 2; ++callback) {
        wl_evaluation_control_t *control = NULL;
        if (wl_evaluation_control_create(0, &control)) return 1;
        wl_session_t *base = make_controlled(plan, control, callback != 0);
        if (!base) return 1;
        expect("controlled input inserts",
            !wl_session_insert(base, "edge", edges, 2, 2));
        wl_col_session_t *sess = (wl_col_session_t *)base;
        delta_events = boundary_hits = boundary2_hits = plan_hits = 0;
        cancel_at_boundary = 1; cancel_on_plan = true;
        wl_columnar_eval_delta_test_observer_boundary = on_boundary;
        wl_columnar_eval_serial_test_after_plan = on_plan;
        for (unsigned cancellation = 0; cancellation < 2; ++cancellation) {
            if (cancellation) wl_evaluation_control_request_cancel(control);
            uint64_t before = wl_test_evaluation_guard_state(sess);
            expect("public controlled step gated",
                wl_session_step(base) == ENOTSUP);
            expect("backend controlled step gated",
                base->backend->session_step(base) == ENOTSUP);
            expect(
                "gate preserves registry, observer, pending, profile and report",
                before == wl_test_evaluation_guard_state(sess));
            expect("gate enters no plan or observer hook",
                !plan_hits && !boundary_hits && !boundary2_hits);
            expect("gate delivers no delta", !delta_events);
            expect("gate creates no partial head",
                !session_rel_hash_lookup(sess, "reach"));
        }
        wl_columnar_eval_delta_test_observer_boundary = NULL;
        wl_columnar_eval_serial_test_after_plan = NULL;
        cancel_on_plan = false; cancel_at_boundary = 0;
        expect("reset pending cancellation",
            !wl_evaluation_control_reset(control));
        expect("whole controlled retry stays explicitly unsupported",
            base->backend->session_step(base) == ENOTSUP);
        wl_evaluation_control_report_t report;
        expect("no original attempt began or finished",
            wl_evaluation_control_report(control, &report) == ENOENT);
        wl_session_destroy(base); wl_evaluation_control_release(control);
    }
    wl_plan_free(plan);
    return failed ? 1 : 0;
}
