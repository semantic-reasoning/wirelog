/* #1821: whole-session gates plus real admission-key control ownership.
 * Actual controlled relation publication is covered by evaluation_serial. */
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
    if (!plan) return 1;
    static const int64_t edges[] = {1, 2, 2, 3};
    wl_session_t *raw = NULL;
    failed += expect("raw session creates",
            !wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    if (!raw) return 1;
    failed += expect("raw facts insert",
            !wl_session_insert(raw, "edge", edges, 2, 2));
    struct emitted live = {0};
    failed += expect("raw snapshot succeeds",
            !wl_session_snapshot(raw, count_tuple, &live));
    failed += expect("raw snapshot is nonempty", live.count > 0);
    wl_session_destroy(raw);

    wl_evaluation_control_t *control = NULL;
    if (wl_evaluation_control_create(0, &control)) return 1;
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *base = NULL;
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1, &options,
        &base)) return 1;
    failed += expect("controlled facts insert",
            !wl_session_insert(base, "edge", edges, 2, 2));
    wl_col_session_t *sess = (wl_col_session_t *)base;
    const void *owner = base->operation_admission;
    failed += expect("real admission owner exists", owner != NULL);
    failed += expect("wrong owner cannot begin",
            wl_evaluation_control_begin(control, base) == EINVAL);
    for (unsigned phase = 0; phase < 3; ++phase) {
        if (phase == 1) {
            failed += expect("admission owner begins",
                    !wl_evaluation_control_begin(control, owner));
            wl_evaluation_control_request_cancel(control);
        }
        if (phase == 2) {
            failed += expect("gates did not poll pending cancellation",
                    !wl_evaluation_control_finish(control, owner, 0, 0));
            wl_evaluation_control_report_t untouched;
            failed += expect("gates consumed no original work",
                    !wl_evaluation_control_report(control, &untouched)
                    && untouched.attempt_id == 1 && !untouched.consumed
                    && untouched.stop_reason == WL_EVALUATION_CONTROL_NONE &&
                    !untouched.result);
            failed +=
                expect("next original begins without clearing cancellation",
                    !wl_evaluation_control_begin(control, owner));
            failed += expect("charge observes cancellation",
                    wl_evaluation_control_charge(control,
                    0) == WL_EVALUATION_CONTROL_CANCELLED);
            failed += expect("wrong owner cannot finish",
                    wl_evaluation_control_finish(control, base, 0,
                    0) == EINVAL);
            failed += expect("admission owner finishes",
                    wl_evaluation_control_finish(control, owner, 0,
                    0) == WL_EVALUATION_CONTROL_CANCELLED);
        }
        uint64_t before = wl_test_evaluation_guard_state(sess);
        struct emitted blocked = {0};
        failed += expect("public step gated", wl_session_step(base) == ENOTSUP);
        failed += expect("public snapshot gated",
                wl_session_snapshot(base, count_tuple, &blocked) == ENOTSUP);
        failed += expect("backend step gated",
                base->backend->session_step(base) == ENOTSUP);
        failed += expect("backend snapshot gated",
                base->backend->session_snapshot(base, count_tuple,
                &blocked) == ENOTSUP);
        failed += expect("gate preserves all observed state and report",
                before == wl_test_evaluation_guard_state(sess));
        failed += expect("gate emits no tuples", !blocked.count);
    }
    wl_evaluation_control_report_t report;
    failed += expect("exact lower-control owner/cancel report",
            !wl_evaluation_control_report(control, &report)
            && report.attempt_id == 2 && report.consumed == 0
            && !report.execution_status && !report.cleanup_status
            && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
            && report.result == WL_EVALUATION_CONTROL_CANCELLED);
    wl_session_destroy(base); wl_evaluation_control_release(control);
    wl_plan_free(plan);
    return failed ? 1 : 0;
}
