/* #1821: raw evaluating/stable snapshots and explicit whole controlled gate.
 * Actual one-relation cancellation/retry lives in evaluation_serial. Whole
 * controlled snapshot stable/base_nrows/observer delivery is deferred. */
#include "evaluation_guard_fixture.h"
#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/evaluation_control.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/columnar/mem_ledger.h"
#include "../wirelog/columnar/source_access.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
#ifndef _MSC_VER
#include <stdatomic.h>
#endif
#include <stdio.h>
#include <string.h>

extern void (*wl_columnar_eval_serial_test_after_plan)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *);
/* Proves that the raw stable path ran. Controlled calls must never reach
 * this hook while the whole-session route is unsupported. */
extern void (*wl_columnar_session_test_before_stable_emit)(wl_col_session_t *);

static int failed;
static unsigned tuples;
static unsigned plan_hits;
static unsigned stable_hits;
static bool cancel_on_plan;
static bool cancel_before_stable_emit;
static unsigned reclaim_hits;

static int
expect(const char *name, int ok)
{
    printf("%s: %s\n", ok ? "ok" : "FAIL", name);
    if (!ok)
        failed = 1;
    return ok;
}

static void
on_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
    tuples++;
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

static void
on_stable_emit(wl_col_session_t *sess)
{
    stable_hits++;
    if (cancel_before_stable_emit && sess->base.evaluation_control)
        wl_evaluation_control_request_cancel(sess->base.evaluation_control);
}

static wl_mem_reclaim_result_t
count_reclaim(void *owner)
{
    (void)owner;
    reclaim_hits++;
    return (wl_mem_reclaim_result_t){ 0 };
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

/* Both public and backend whole snapshots must refuse this real attached
 * control. The raw reference separately proves evaluating and stable paths. */
static wl_session_t *
make_controlled(wl_plan_t *plan, wl_evaluation_control_t *control)
{
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *base = NULL;
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1, &options,
        &base) != 0)
        return NULL;
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
    expect("reference input inserts",
        !wl_session_insert(ref, "edge", edges, 2, 2));
    tuples = plan_hits = 0;
    wl_columnar_eval_serial_test_after_plan = on_plan;
    expect("raw evaluating snapshot",
        !wl_session_snapshot(ref, on_tuple, NULL));
    wl_columnar_eval_serial_test_after_plan = NULL;
    unsigned reference = tuples;
    expect("raw evaluating path emits real tuples and executes plan",
        reference > 0 && plan_hits > 0);
    tuples = stable_hits = plan_hits = 0;
    wl_columnar_eval_serial_test_after_plan = on_plan;
    wl_columnar_session_test_before_stable_emit = on_stable_emit;
    expect("raw stable snapshot", !wl_session_snapshot(ref, on_tuple, NULL));
    wl_columnar_session_test_before_stable_emit = NULL;
    wl_columnar_eval_serial_test_after_plan = NULL;
    expect("raw stable path preserves tuples without evaluation",
        tuples == reference && stable_hits > 0 && !plan_hits);
    wl_session_destroy(ref);

    wl_evaluation_control_t *control = NULL;
    if (wl_evaluation_control_create(0, &control)) return 1;
    wl_session_t *base = make_controlled(plan, control);
    if (!base) return 1;
    wl_col_session_t *sess = (wl_col_session_t *)base;
    expect("controlled input inserts",
        !wl_session_insert(base, "edge", edges, 2, 2));
    wl_mem_reclaimer_handle_t handle = 0;
    expect("counting reclaimer registers",
        !wl_mem_ledger_register_reclaimer(&sess->mem_ledger, count_reclaim,
        sess, &handle));
    uint64_t budget = atomic_load_explicit(&sess->mem_ledger.total_budget,
            memory_order_relaxed);
    atomic_store_explicit(&sess->mem_ledger.total_budget, 1,
        memory_order_relaxed);
    expect("real ledger is over budget",
        wl_mem_ledger_over_budget(&sess->mem_ledger));
    tuples = plan_hits = stable_hits = reclaim_hits = 0;
    cancel_on_plan = cancel_before_stable_emit = true;
    wl_columnar_eval_serial_test_after_plan = on_plan;
    wl_columnar_session_test_before_stable_emit = on_stable_emit;
    for (unsigned state = 0; state < 3; ++state) {
        if (state == 1) {
            expect("real admission owner starts operation",
                !wl_evaluation_control_begin(control,
                base->operation_admission));
            wl_evaluation_control_request_cancel(control);
        }
        if (state == 2) {
            expect("snapshot gate did not poll cancellation",
                !wl_evaluation_control_finish(control,
                base->operation_admission, 0, 0));
            wl_evaluation_control_report_t untouched;
            expect("snapshot gate consumed no original work",
                !wl_evaluation_control_report(control, &untouched)
                && untouched.attempt_id == 1 && !untouched.consumed
                && untouched.stop_reason == WL_EVALUATION_CONTROL_NONE &&
                !untouched.result);
            expect("new original retains sticky cancellation",
                !wl_evaluation_control_begin(control,
                base->operation_admission));
            expect("explicit lower poll observes cancellation",
                wl_evaluation_control_charge(control,
                0) == WL_EVALUATION_CONTROL_CANCELLED);
            expect("explicit lower finish records it",
                wl_evaluation_control_finish(control, base->operation_admission,
                0, 0) == WL_EVALUATION_CONTROL_CANCELLED);
        }
        uint64_t before = wl_test_evaluation_guard_state(sess);
        expect("public snapshot controlled gate",
            wl_session_snapshot(base, on_tuple, NULL) == ENOTSUP);
        expect("backend snapshot controlled gate",
            base->backend->session_snapshot(base, on_tuple, NULL) == ENOTSUP);
        expect(
            "gate preserves actual registry/stable/pending/profile/report state",
            before == wl_test_evaluation_guard_state(sess));
        expect("gate runs no evaluation, stable hook, callback or reclaim",
            !tuples && !plan_hits && !stable_hits && !reclaim_hits);
    }
    wl_columnar_eval_serial_test_after_plan = NULL;
    wl_columnar_session_test_before_stable_emit = NULL;
    cancel_on_plan = cancel_before_stable_emit = false;
    /* This fixture is quiescent: no original operation or cleanup is active.
     * Invoke the ledger's internal API directly; the unsupported snapshot
     * wrapper must not invoke its registered reclaimer as a side effect. */
    expect("fixture is quiescent before direct ledger reclaim",
        !wl_evaluation_control_active_attempt_id(control)
        && !sess->cleanup_active && !sess->cleanup_pending);
    (void)wl_mem_ledger_reclaim(&sess->mem_ledger);
    expect("lower ledger API invokes the real reclaimer",
        reclaim_hits > 0);
    atomic_store_explicit(&sess->mem_ledger.total_budget, budget,
        memory_order_relaxed);
    wl_mem_ledger_unregister_reclaimer(&sess->mem_ledger, handle);
    wl_session_destroy(base); wl_evaluation_control_release(control);
    wl_plan_free(plan);
    return failed;
}
