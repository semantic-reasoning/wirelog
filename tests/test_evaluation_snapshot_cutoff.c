/*
 * The snapshot path's publication cutoff (#1953).
 *
 * col_session_snapshot_impl commits before it delivers, so a cancellation that
 * arrives after the wrapper's entry poll is seen by nothing: finish() derives
 * its result from control->stop, and only charge() converts the sticky
 * cancelled word into a stop.  Without a poll of its own the path publishes the
 * whole model and reports success.
 *
 * There are two emits to cover, and they need separate cases.  The evaluating
 * path emits after compaction and the bookkeeping; the stable fast path emits
 * without evaluating at all, so a cutoff placed for the first is not on its
 * route.  Case B and Case D are those two.
 */

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
/* Fired where the stable fast path would publish.  A cancellation arriving
 * there is the hole this unit closes, and no single-threaded test can place one
 * between the wrapper's entry poll and that emit without a lever. */
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

/* Reached through the backend vtable: wl_session_snapshot short-circuits to
 * ENOTSUP while a control is attached, so that it can never silently evaluate
 * one.  num_workers stays 1 so the serial lever is on the route. */
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
    if (!expect("plan builds", plan != NULL))
        return 1;

    /*
     * Case A, anti-vacuity.  col_session_emit_snapshot returns 0 whether or not
     * it emits anything, so "published nothing" and "succeeded" both hold
     * trivially against an empty model.  Every count below is compared to this.
     */
    unsigned reference_tuples;
    {
        wl_session_t *ref = NULL;
        if (!expect("reference session creates",
            wl_session_create(wl_backend_columnar(), plan, 1, &ref) == 0))
            return 1;
        expect("reference insert",
            wl_session_insert(ref, "edge", edges, 2, 2) == 0);
        tuples = 0;
        expect("reference snapshot succeeds",
            wl_session_snapshot(ref, on_tuple, NULL) == 0);
        reference_tuples = tuples;
        expect("the reference snapshot emits tuples", reference_tuples > 0);
        wl_session_destroy(ref);
    }

    /* --- Case B: the evaluating path ------------------------------------- */
    {
        wl_evaluation_control_t *control = NULL;
        expect("control creates",
            wl_evaluation_control_create(0, &control) == 0);
        wl_session_t *base = make_controlled(plan, control);
        if (!expect("controlled session creates", base != NULL))
            return 1;
        wl_col_session_t *sess = (wl_col_session_t *)base;

        expect("the public snapshot stays gated",
            wl_session_snapshot(base, on_tuple, NULL) == ENOTSUP);
        expect("insert", wl_session_insert(base, "edge", edges, 2, 2) == 0);

        col_rel_t *reach = session_find_rel(sess, "reach");
        expect("the derived relation does not exist before the first snapshot",
            reach == NULL);
        uint32_t base_before = reach ? reach->base_nrows : 0u;

        tuples = 0;
        plan_hits = 0;
        cancel_on_plan = true;
        wl_columnar_eval_serial_test_after_plan = on_plan;
        int rc = base->backend->session_snapshot(base, on_tuple, NULL);
        wl_columnar_eval_serial_test_after_plan = NULL;
        cancel_on_plan = false;

        expect("the serial lever actually fired", plan_hits > 0);
        expect("a stop at the cutoff is reported as cancellation",
            rc == WL_EVALUATION_CONTROL_CANCELLED);
        expect("a stopped attempt publishes no tuples", tuples == 0);

        wl_evaluation_control_report_t report;
        expect("the attempt is recorded as cancelled",
            wl_evaluation_control_report(control, &report) == 0
            && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
            && report.execution_status == 0);

        /*
         * The bookkeeping block, field by field.  These are what pin the cutoff
         * above the bookkeeping rather than merely above the emit: a cutoff
         * placed lower would return CANCELLED and emit nothing while leaving
         * the session claiming this pass succeeded.
         */
        expect("the insertion is still pending",
            sess->last_inserted_relation != NULL);
        expect("the input change is still pending", sess->pending_input_change);
        expect("the session has not recorded an evaluation",
            !sess->has_evaluated);
        expect("the stable snapshot is still invalid",
            !sess->snapshot_stable_valid);
        reach = session_find_rel(sess, "reach");
        expect("the baseline advance did not run",
            reach != NULL && reach->nrows > 0
            && reach->base_nrows == base_before);

        /*
         * Why the unwind does not sample the memory ledger, recorded here
         * because it is not what it looks like.  A first draft asserted that a
         * stopped attempt publishes no STORED high-water mark, on the grounds
         * that WL_MEM_SUBSYS_STORED is gauged in exactly one place in the tree.
         * It is -- but that place is inside col_session_mem_sample, which
         * evaluation calls from eval_serial.c and eval.c, above the cutoff.  So
         * the mark is already raised when a stop arrives, as this assertion
         * states, and omitting the sample from the unwind is redundancy rather
         * than a safeguard.
         */
        wl_mem_ledger_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        wl_mem_ledger_snapshot(&sess->mem_ledger, &snap);
        expect("evaluation has already raised the STORED mark",
            snap.subsys_peak[WL_MEM_SUBSYS_STORED] > 0);

        /*
         * The retry.  Reset first: cancelled is sticky and is cleared only
         * here, so without it the wrapper's entry poll refuses before the impl
         * runs and a bricked session would read as a pass.
         *
         * The retry is not the same evaluation again, though not for the reason
         * a first draft of this comment gave.  The pre-seed does not run: it is
         * guarded on has_evaluated, which the assertion above shows is still
         * false, and it would skip `reach` anyway because the loop continues on
         * base_nrows == 0.  What differs is that `reach` now exists, populated,
         * with base_nrows still 0 -- so the retry re-derives over a non-empty
         * IDB rather than from nothing.  That is why the assertion is output
         * equivalence against the reference and not rc == 0 alone.
         */
        expect("cancellation clears between attempts",
            wl_evaluation_control_reset(control) == 0);
        tuples = 0;
        plan_hits = 0;
        wl_columnar_eval_serial_test_after_plan = on_plan;   /* count only */
        rc = base->backend->session_snapshot(base, on_tuple, NULL);
        wl_columnar_eval_serial_test_after_plan = NULL;
        expect("the retry succeeds", rc == 0);
        expect("the retry delivers exactly the reference tuples",
            tuples == reference_tuples);
        expect("the retry re-evaluates, having no resume token",
            plan_hits > 0);
        expect("the retry commits the bookkeeping",
            sess->has_evaluated && sess->snapshot_stable_valid);

        wl_session_destroy(base);
        wl_evaluation_control_release(control);
    }

    /* --- Case D: the stable fast path ------------------------------------ */
    {
        wl_evaluation_control_t *control = NULL;
        expect("control creates for the fast path",
            wl_evaluation_control_create(0, &control) == 0);
        wl_session_t *base = make_controlled(plan, control);
        if (!expect("fast-path session creates", base != NULL))
            return 1;
        wl_col_session_t *sess = (wl_col_session_t *)base;

        expect("insert", wl_session_insert(base, "edge", edges, 2, 2) == 0);
        tuples = 0;
        expect("the first snapshot succeeds",
            base->backend->session_snapshot(base, on_tuple, NULL) == 0);
        expect("the first snapshot emits the reference tuples",
            tuples == reference_tuples);
        if (!expect("the model is now stable", sess->snapshot_stable_valid))
            return 1;

        uint64_t pin_epoch_before = sess->mat_cache.pin_epoch;
        wl_mem_reclaimer_handle_t fast_handle = 0;
        expect("a counting reclaimer registers for the fast path",
            wl_mem_ledger_register_reclaimer(&sess->mem_ledger, count_reclaim,
            sess, &fast_handle) == 0);
        uint64_t saved_budget = atomic_load_explicit(
            &sess->mem_ledger.total_budget, memory_order_relaxed);
        atomic_store_explicit(&sess->mem_ledger.total_budget, 1,
            memory_order_relaxed);
        if (!expect("the ledger reports over budget for the fast path",
            wl_mem_ledger_over_budget(&sess->mem_ledger)))
            return 1;

        tuples = 0;
        plan_hits = 0;
        stable_hits = 0;
        reclaim_hits = 0;
        cancel_before_stable_emit = true;
        wl_columnar_eval_serial_test_after_plan = on_plan;
        wl_columnar_session_test_before_stable_emit = on_stable_emit;
        int rc = base->backend->session_snapshot(base, on_tuple, NULL);
        wl_columnar_session_test_before_stable_emit = NULL;
        wl_columnar_eval_serial_test_after_plan = NULL;
        cancel_before_stable_emit = false;

        unsigned fast_reclaim_hits = reclaim_hits;
        atomic_store_explicit(&sess->mem_ledger.total_budget, saved_budget,
            memory_order_relaxed);
        wl_mem_ledger_unregister_reclaimer(&sess->mem_ledger, fast_handle);

        expect("the fast path was the route taken", stable_hits > 0);
        expect("the fast path did not evaluate", plan_hits == 0);
        expect("a stop before the stable emit is reported as cancellation",
            rc == WL_EVALUATION_CONTROL_CANCELLED);
        expect("the stable fast path publishes no tuples on a stop",
            tuples == 0);
        /* Refusing to publish does not invalidate a stable model; clearing this
         * would force the retry into a full re-evaluation. */
        expect("the stop leaves the stable model stable",
            sess->snapshot_stable_valid);
        /* The fast path has its own copy of the unwind, and Case E's budget
         * poke drives the evaluating path only, so without this the fast-path
         * col_session_reclaim_quiescent could be deleted unnoticed. */
        expect("the fast-path unwind reached the quiescent boundary",
            fast_reclaim_hits > 0);
        /*
         * Attributable here and only here: the fast path does not evaluate, so
         * the unwind's col_session_reclaim_quiescent is the only thing on this
         * route that can have advanced the pin epoch.  What this pins that the
         * reclaimer count does not is the pin release inside that shared helper
         * -- deleting the call itself already fails the assertion above.
         */
        expect("the fast-path unwind released mat-cache pins",
            sess->mat_cache.pin_epoch != pin_epoch_before);

        expect("cancellation clears between attempts",
            wl_evaluation_control_reset(control) == 0);
        tuples = 0;
        stable_hits = 0;
        wl_columnar_session_test_before_stable_emit = on_stable_emit;
        rc = base->backend->session_snapshot(base, on_tuple, NULL);
        wl_columnar_session_test_before_stable_emit = NULL;
        expect("the fast-path retry succeeds", rc == 0);
        expect("the fast-path retry delivers the reference tuples",
            tuples == reference_tuples);
        expect("the fast-path retry took the fast path again", stable_hits > 0);

        wl_session_destroy(base);
        wl_evaluation_control_release(control);
    }

    /* --- Case E: the unwind's only action ------------------------------- */
    /*
     * Removing col_session_reclaim_quiescent from both unwinds passed every
     * assertion above.  Not for the reason a first draft of this comment gave:
     * the function is not inert without a budget -- it releases mat-cache pins
     * without regard to the budget, and only wl_mem_ledger_reclaim is
     * budget-gated.  Both legs still sit under the retained-cleanup early
     * return, so neither is unconditional.  What
     * made the omission invisible is that the pin release is shared with
     * evaluation, which calls it from eval_serial.c and eval.c, so no
     * end-to-end assertion on the evaluating route can attribute it to the
     * unwind.  The reclaim leg can be attributed, and this case trips the
     * budget to reach it.  The pin-release leg is attributable only on the fast
     * path, which does not evaluate; Case D asserts it there.
     */
    {
        wl_evaluation_control_t *control = NULL;
        expect("control creates for the reclaim case",
            wl_evaluation_control_create(0, &control) == 0);
        wl_session_t *base = make_controlled(plan, control);
        if (!expect("reclaim-case session creates", base != NULL))
            return 1;
        wl_col_session_t *sess = (wl_col_session_t *)base;

        expect("insert", wl_session_insert(base, "edge", edges, 2, 2) == 0);

        wl_mem_reclaimer_handle_t handle = 0;
        expect("a counting reclaimer registers",
            wl_mem_ledger_register_reclaimer(&sess->mem_ledger, count_reclaim,
            sess, &handle) == 0);
        /*
         * There is no budget setter -- wl_mem_ledger_init would zero the
         * counters this case is observing -- so the budget is poked directly.
         * The assertion below refuses to proceed if the poke did not take, so
         * the case cannot pass vacuously.
         */
        uint64_t saved_budget = atomic_load_explicit(
            &sess->mem_ledger.total_budget, memory_order_relaxed);
        atomic_store_explicit(&sess->mem_ledger.total_budget, 1,
            memory_order_relaxed);
        if (!expect("the ledger now reports over budget",
            wl_mem_ledger_over_budget(&sess->mem_ledger)))
            return 1;

        reclaim_hits = 0;
        tuples = 0;
        cancel_on_plan = true;
        wl_columnar_eval_serial_test_after_plan = on_plan;
        int rc = base->backend->session_snapshot(base, on_tuple, NULL);
        wl_columnar_eval_serial_test_after_plan = NULL;
        cancel_on_plan = false;

        expect("the stop is still reported as cancellation",
            rc == WL_EVALUATION_CONTROL_CANCELLED);
        expect("the stopped attempt published nothing", tuples == 0);
        expect("the unwind reached the quiescent boundary", reclaim_hits > 0);

        atomic_store_explicit(&sess->mem_ledger.total_budget, saved_budget,
            memory_order_relaxed);
        wl_mem_ledger_unregister_reclaimer(&sess->mem_ledger, handle);
        wl_session_destroy(base);
        wl_evaluation_control_release(control);
    }

    /* --- Case F: why the cutoff sits BEFORE compaction ------------------- */
    /*
     * Moving the cutoff below col_rel_compact_many passes every assertion above,
     * because compaction succeeds in this fixture and the bookkeeping fields
     * read the same either side of it.  The one scenario that separates the two
     * placements is compaction failing while a cancellation is pending: finish()
     * prefers a non-zero execution_status over control->stop, so a cutoff below
     * compaction would report that failure and lose the cancellation the caller
     * asked for.
     *
     * Holding a reader on a relation's source_access makes the writer acquire
     * inside col_rel_compact_many fail, which is how the failure is arranged
     * without a fault-injection hook.
     */
    {
        wl_evaluation_control_t *control = NULL;
        expect("control creates for the compaction case",
            wl_evaluation_control_create(0, &control) == 0);
        wl_session_t *base = make_controlled(plan, control);
        if (!expect("compaction-case session creates", base != NULL))
            return 1;
        wl_col_session_t *sess = (wl_col_session_t *)base;

        expect("insert", wl_session_insert(base, "edge", edges, 2, 2) == 0);
        col_rel_t *edge = session_find_rel(sess, "edge");
        if (!expect("the inserted relation is registered", edge != NULL))
            return 1;

        wl_columnar_source_access_reader_t reader;
        memset(&reader, 0, sizeof(reader));
        if (!expect("a reader can be held on the source",
            wl_columnar_source_access_reader_acquire(&edge->source_access,
            &reader) == 0))
            return 1;
        /*
         * Guard the arrangement, not just its consequence.  The assertion below
         * is satisfied whether compaction fails or succeeds, so without this the
         * case keeps passing if col_rel_compact_many ever stops taking a writer
         * on this owner -- and then it tests nothing while still reading as the
         * placement's proof.
         */
        if (!expect("the held reader really makes compaction fail",
            col_rel_compact_many(sess->rels, sess->nrels) == EBUSY))
            return 1;

        tuples = 0;
        plan_hits = 0;
        cancel_on_plan = true;
        wl_columnar_eval_serial_test_after_plan = on_plan;
        int rc = base->backend->session_snapshot(base, on_tuple, NULL);
        wl_columnar_eval_serial_test_after_plan = NULL;
        cancel_on_plan = false;
        wl_columnar_source_access_reader_release(&reader);

        expect("the serial lever fired", plan_hits > 0);
        /*
         * The whole content of this case.  With the cutoff above compaction the
         * cancellation wins; with it below, compaction's EBUSY would be reported
         * instead and this assertion fails.
         */
        expect("cancellation outranks a compaction failure",
            rc == WL_EVALUATION_CONTROL_CANCELLED);
        expect("nothing was published", tuples == 0);

        wl_evaluation_control_report_t report;
        expect("the attempt is recorded as cancelled, not as busy",
            wl_evaluation_control_report(control, &report) == 0
            && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
            && report.execution_status == 0);

        wl_session_destroy(base);
        wl_evaluation_control_release(control);
    }

    wl_plan_free(plan);
    printf("%s\n", failed ? "FAILED" : "PASSED");
    return failed;
}
