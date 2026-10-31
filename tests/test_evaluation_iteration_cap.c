/* Real fixed-point boundaries with small test-only limits. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

enum { LENGTH = 13 };
static const char *program_text =
    ".decl seed(x:int64)\n.decl edge(x:int64,y:int64)\n"
    ".decl a(x:int64)\n.decl b(x:int64)\n"
    "a(x) :- seed(x).\na(y) :- b(x), edge(x,y).\nb(x) :- a(x).\n";
static unsigned rounds, stop_mode, stop_at;
static bool stop_hit;
static wl_columnar_memory_governor_ref_t *governor;

static void
round_checkpoint(wl_evaluation_control_t *control, uint32_t round, bool changed)
{
    if (round > rounds) rounds = round;
    if (!stop_mode || stop_hit || round != stop_at || !changed) return;
    stop_hit = true;
    if (stop_mode == 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

typedef struct { unsigned seen[2][LENGTH + 1], count; } capture_t;
static void
tuple(const char *name, const int64_t *values, uint32_t width, void *data)
{
    if (strcmp(name, "a") && strcmp(name, "b")) return;
    capture_t *c = data;
    CHECK(width == 1 && values[0] >= 0 && values[0] <= LENGTH);
    CHECK(!c->seen[name[0] == 'b'][values[0]]++);
    c->count++;
}
static void
delta(const char *name, const int64_t *values, uint32_t width, int diff,
    void *data)
{
    CHECK(diff == 1);
    tuple(name, values, width, data);
}
static int
step(wl_session_t *s)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(s);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}
static void
check_model(wl_session_t *s)
{
    for (unsigned h = 0; h < 2; ++h) {
        col_rel_t *r = session_find_rel((wl_col_session_t *)s, h ? "b" : "a");
        CHECK(r && r->nrows == LENGTH + 1 && r->ncols == 1);
        bool seen[LENGTH + 1] = {0};
        for (uint32_t i = 0; i < r->nrows; ++i) {
            int64_t v = r->columns[0][i];
            CHECK(v >= 0 && v <= LENGTH && !seen[v]); seen[v] = true;
        }
    }
}
static wl_session_t *
create(wl_plan_t *plan, wl_evaluation_control_t *control)
{
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    options.memory_governor = governor;
    wl_session_t *s = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &s));
    int64_t seed = 0, edges[LENGTH * 2];
    for (unsigned i = 0; i < LENGTH; ++i) {
        edges[2 * i] = i; edges[2 * i + 1] = i + 1;
    }
    CHECK(!wl_session_insert(s, "seed", &seed, 1, 1));
    CHECK(!wl_session_insert(s, "edge", edges, LENGTH, 2));
    return s;
}

static unsigned
controlled(wl_plan_t *plan, unsigned limit, unsigned stop, bool snapshot,
    bool observed, bool should_cap)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &control));
    wl_session_t *s = create(plan, control);
    wl_col_session_t *cs = (wl_col_session_t *)s;
    capture_t events = {0}, image = {0};
    if (observed) wl_session_set_delta_cb(s, delta, &events);
    col_rel_t *edge = session_find_rel(cs, "edge");
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(edge);
    rounds = 0; stop_mode = stop; stop_at = limit; stop_hit = false;
    wl_columnar_session_test_iteration_limit = limit;
    wl_columnar_session_test_iteration_checkpoint = round_checkpoint;
    int rc = snapshot ? wl_session_snapshot(s, tuple, &image) : step(s);
    wl_columnar_session_test_iteration_checkpoint = NULL;
    unsigned completed_rounds = rounds;
    int expected = stop == 1 ? WL_EVALUATION_CONTROL_CANCELLED
        : stop == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : should_cap ? EOVERFLOW : 0;
    if (rc != expected) fprintf(stderr,
            "limit=%u stop=%u snapshot=%u observed=%u rounds=%u rc=%d expected=%d\n",
            limit, stop, snapshot, observed, rounds, rc, expected);
    CHECK(rc == expected && (!stop || stop_hit));
    CHECK(wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(edge)));
    wl_evaluation_control_report_t report = {0}, frozen = {0};
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.result == rc && !report.cleanup_status);
    CHECK(report.execution_status == (stop ? 0 : rc));
    CHECK(report.stop_reason == (stop == 1 ? WL_EVALUATION_CONTROL_CANCELLED
        : stop ==
        2 ? WL_EVALUATION_CONTROL_WORK_LIMIT : WL_EVALUATION_CONTROL_NONE));
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    CHECK(!wl_evaluation_control_report(control, &frozen));
    CHECK(!memcmp(&report, &frozen, sizeof(report)));
    if (rc) {
        CHECK(!events.count && !image.count && cs->pending_input_change);
        for (unsigned h = 0; h < 2; ++h) {
            col_rel_t *head = session_find_rel(cs, h ? "b" : "a");
            CHECK(!head || !head->nrows);
        }
        CHECK(!wl_evaluation_control_reset(control));
        wl_columnar_session_test_iteration_limit = 64;
        int retry_rc = snapshot ? wl_session_snapshot(s, tuple,
                &image) : step(s);
        if (retry_rc) fprintf(stderr,
                "retry limit=%u stop=%u snapshot=%u observed=%u rc=%d\n",
                limit, stop, snapshot, observed, retry_rc);
        CHECK(!retry_rc);
    }
    check_model(s);
    if (observed) CHECK(events.count == 2 * (LENGTH + 1));
    if (!rc && snapshot) CHECK(image.count == 2 * (LENGTH + 1));
    CHECK(!step(s));
    if (observed) CHECK(events.count == 2 * (LENGTH + 1));
    CHECK(!wl_session_try_destroy(&s));
    wl_evaluation_control_release(control);
    /* The program owns its intern table beyond the session lifetime. */
    CHECK(!wl_intern_detach_memory_governor(plan->intern, governor));
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)));
    wl_columnar_session_test_iteration_limit = 0;
    stop_mode = 0;
    return completed_rounds;
}

static void
raw_retry(wl_plan_t *plan, bool snapshot, bool observed)
{
    wl_session_t *s = create(plan, NULL);
    wl_col_session_t *cs = (wl_col_session_t *)s;
    capture_t events = {0}, image = {0};
    if (observed) wl_session_set_delta_cb(s, delta, &events);
    wl_columnar_eval_serial_test_iteration_limit = 1;
    CHECK(!(snapshot ? wl_session_snapshot(s, tuple, &image) : step(s)));
    CHECK(cs->eval_stratum_iteration_capped && !cs->eval_stratum_heads_final);
    CHECK(cs->pending_input_change && cs->pending_full_input_eval
        && !cs->snapshot_stable_valid && !cs->has_evaluated);
    CHECK(!cs->plain_step_completion_pending && !events.count);
    wl_columnar_eval_serial_test_iteration_limit = 0;
    memset(&image, 0, sizeof(image));
    CHECK(!(snapshot ? wl_session_snapshot(s, tuple, &image) : step(s)));
    CHECK(!cs->eval_stratum_iteration_capped && cs->snapshot_stable_valid
        && cs->has_evaluated && !cs->pending_input_change);
    check_model(s);
    if (observed) CHECK(events.count == 2 * (LENGTH + 1));
    CHECK(!wl_session_try_destroy(&s));
    CHECK(!wl_intern_detach_memory_governor(plan->intern, governor));
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)));
}

int main(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(program_text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    /* Four outer strides allow32 effective passes. The generated closure
     * needs29 rounds: interpreting the outer limit as rounds would fail. */
    wl_columnar_eval_serial_test_iteration_limit = 4;
    wl_session_t *parity = create(plan, NULL);
    CHECK(!step(parity)); check_model(parity);
    CHECK(!((wl_col_session_t *)parity)->eval_stratum_iteration_capped);
    CHECK(!wl_session_try_destroy(&parity));
    CHECK(!wl_intern_detach_memory_governor(plan->intern, governor));
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)));
    CHECK(controlled(plan, 0, 0, false, false, false) > 4);
    wl_columnar_eval_serial_test_iteration_limit = 0;
    unsigned bound = controlled(plan, 64, 0, false, false, false);
    CHECK(bound > 3 && bound < 64);
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned snapshot = 0; snapshot < 2; ++snapshot) {
            controlled(plan, bound, 0, snapshot, observed, false);
            controlled(plan, bound - 1, 0, snapshot, observed, true);
            for (unsigned stop = 0; stop < 3; ++stop)
                controlled(plan, 3, stop, snapshot, observed, true);
        }
    raw_retry(plan, false, false);
    raw_retry(plan, false, true);
    raw_retry(plan, true, false);
    /* Direct raw serial/TDD entry refuses attached control before dispatch. */
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_t *s = create(plan, control);
    for (unsigned i = 0; i < plan->stratum_count; ++i) {
        CHECK(col_eval_stratum(&plan->strata[i], (wl_col_session_t *)s, i)
            == ENOTSUP);
        CHECK(col_eval_stratum_tdd(&plan->strata[i], (wl_col_session_t *)s, i)
            == ENOTSUP);
    }
    CHECK(!wl_session_try_destroy(&s)); wl_evaluation_control_release(control);
    CHECK(!wl_intern_detach_memory_governor(plan->intern, governor));
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)));
    wl_columnar_memory_governor_ref_release(governor);
    wl_plan_free(plan); wirelog_program_free(program);
    puts("iteration caps: real convergence, precedence and retry passed");
    return 0;
}
