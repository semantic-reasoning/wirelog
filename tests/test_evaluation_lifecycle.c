/* Controlled host lifecycle integration: actual generated session routes. */
#include "evaluation_guard_fixture.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); \
                          abort(); \
                      } } while (0)

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    (void)token; (void)phase;
}

typedef struct {
    char name[32];
    int64_t row[4];
    uint32_t width;
    int diff;
} event_t;

typedef struct {
    event_t events[128];
    unsigned count;
    wl_evaluation_control_t *cancel_control;
} capture_t;

static void
capture_delta(const char *name, const int64_t *row, uint32_t width,
    int diff, void *data)
{
    capture_t *capture = data;
    CHECK(capture->count < 128 && width <= 4 && strlen(name) < 32);
    event_t *event = &capture->events[capture->count++];
    memset(event, 0, sizeof(*event));
    strcpy(event->name, name);
    if (width) memcpy(event->row, row, width * sizeof(*row));
    event->width = width; event->diff = diff;
    if (capture->count == 1 && capture->cancel_control)
        wl_evaluation_control_request_cancel(capture->cancel_control);
}

static void
capture_tuple(const char *name, const int64_t *row, uint32_t width, void *data)
{
    capture_delta(name, row, width, 1, data);
}

static int
order_event(const void *a, const void *b)
{
    const event_t *left = a, *right = b;
    int order = strcmp(left->name, right->name);
    if (order) return order;
    if (left->width != right->width) return left->width < right->width ? -1 : 1;
    for (uint32_t i = 0; i < left->width; ++i)
        if (left->row[i] != right->row[i]) return left->row[i] <
                   right->row[i] ? -1 : 1;
    return (left->diff > right->diff) - (left->diff < right->diff);
}

static void
check_capture(capture_t *actual, capture_t *expected)
{
    CHECK(actual->count == expected->count);
    qsort(actual->events, actual->count, sizeof(event_t), order_event);
    qsort(expected->events, expected->count, sizeof(event_t), order_event);
    CHECK(!memcmp(actual->events, expected->events,
        actual->count * sizeof(event_t)));
}

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session, *reference;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    capture_t actual, expected;
} fixture_t;

static void
fixture_create(fixture_t *f, const char *source)
{
    memset(f, 0, sizeof(*f));
    wirelog_error_t error;
    f->program = wirelog_parse_string(source, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    CHECK(!wl_evaluation_control_create(0, &f->control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan,
        1, &options, &f->session));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1, &f->reference));
}

static void
fixture_init(fixture_t *f, bool multi)
{
    const char *source = multi
        ? ".decl input(x:int64)\n.decl first(x:int64)\n.decl second(x:int64)\n"
        ".decl rejected(x:int64)\nfirst(x) :- input(x).\n"
        "second(x) :- first(x).\nrejected(x) :- second(x), x < 0.\n"
        : ".decl input(x:int64)\n.decl first(x:int64)\nfirst(x) :- input(x).\n";
    fixture_create(f, source);
    int64_t rows[] = {7, 3, 7, 11};
    CHECK(!wl_session_insert(f->session, "input", rows, 4, 1));
    CHECK(!wl_session_insert(f->reference, "input", rows, 4, 1));
}

static int
step(wl_session_t *session)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false);
    return EIO;
}

static void
fixture_finish(fixture_t *f)
{
    CHECK(!wl_session_try_destroy(&f->session) && !f->session);

    wl_evaluation_control_release(f->control);
    wl_session_destroy(f->reference);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
}

static void
check_results(fixture_t *f)
{
    capture_t actual = {0}, expected = {0};
    CHECK(!wl_session_snapshot(f->session, capture_tuple, &actual));
    CHECK(!wl_session_snapshot(f->reference, capture_tuple, &expected));
    check_capture(&actual, &expected);
    CHECK(actual.count > 0);
}

extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
static unsigned stop_phase, stop_index, stop_hits;
static bool stop_work;

static void
stop_transaction(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    if (phase != stop_phase || (stop_index != UINT32_MAX && index != stop_index)
        || (phase == 8 && !bindings)) return;
    ++stop_hits;
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    if (stop_work)
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else
        wl_evaluation_control_request_cancel(control);
}

static void
test_step_successors(void)
{
    for (unsigned multi = 0; multi < 2; ++multi) {
        fixture_t f;
        fixture_init(&f, multi != 0);
        CHECK(!step(f.session) && !step(f.reference));
        check_results(&f);
        int64_t value = 13;
        CHECK(wl_session_insert(f.session, "first", &value, 1, 1) == ENOTSUP);
        CHECK(wl_session_insert(f.session, "input", &value, 1, 2) == ENOTSUP);
        CHECK(!wl_session_insert(f.session, "input", &value, 0, 1));
        CHECK(!step(f.session));
        for (unsigned cycle = 0; cycle < 6; ++cycle) {
            /* Install, clear and re-enable only at legal completed boundaries. */
            bool observed = cycle == 2 || cycle >= 4;
            wl_session_set_delta_cb(f.session, observed ? capture_delta : NULL,
                &f.actual);
            wl_session_set_delta_cb(f.reference,
                observed ? capture_delta : NULL,
                &f.expected);
            CHECK(((wl_col_session_t *)f.session)->delta_cb
                == (observed ? capture_delta : NULL));
            f.actual.count = f.expected.count = 0;
            value = cycle == 0 ? 13 : cycle == 1 ? 99 : cycle == 2 ? 17
                : cycle == 3 ? 7 : cycle == 4 ? 19 : 17;
            int rc, reference_rc;
            if (cycle == 1 || cycle == 3 || cycle == 5) {
                rc = wl_session_remove(f.session, "input", &value, 1, 1);
                reference_rc = wl_session_remove(f.reference, "input", &value,
                        1, 1);
            } else {
                int64_t duplicate[] = {value, value};
                rc = wl_session_insert(f.session, "input", duplicate, 2, 1);
                reference_rc = wl_session_insert(f.reference, "input",
                        duplicate, 2, 1);
                if (cycle == 0) {
                    int64_t next = 23, missing = 99;
                    CHECK(!wl_session_insert(f.session, "input", &next, 1, 1));
                    CHECK(!wl_session_insert(f.reference, "input", &next, 1,
                        1));
                    CHECK(!wl_session_remove(f.session, "input", &missing, 1,
                        1));
                    CHECK(!wl_session_remove(f.reference, "input", &missing, 1,
                        1));
                }
            }
            CHECK(!rc && !reference_rc);
            CHECK(!step(f.session) && !step(f.reference));
            check_capture(&f.actual, &f.expected);
            if (cycle == 2 || cycle == 4) CHECK(f.actual.count > 0);
            check_results(&f);
            unsigned events = f.actual.count;
            CHECK(!step(f.session) && events == f.actual.count);
        }
        fixture_finish(&f);
    }
}

static void
test_successor_stop_retry(void)
{
    for (unsigned mode = 0; mode < 4; ++mode) {
        fixture_t f;
        fixture_init(&f, true);
        CHECK(!step(f.session) && !step(f.reference));
        int64_t value = 17;
        CHECK(!wl_session_insert(f.session, "input", &value, 1, 1));
        CHECK(!wl_session_insert(f.reference, "input", &value, 1, 1));
        wl_col_session_t *s = (wl_col_session_t *)f.session;
        col_rel_t *first = session_find_rel(s, "first");
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(first);
        col_rel_t *input = session_find_rel(s, "input");
        col_relation_snapshot_t input_before =
            wl_columnar_relation_snapshot(input);
        col_rel_t **registry = s->rels;
        CHECK(!wl_evaluation_control_configure(f.control,
            UINT64_C(1000000000)));
        stop_phase = mode < 2 ? 7 : 8;
        stop_index = mode < 2 ? 0 : 1;
        stop_work = mode % 2 != 0; stop_hits = 0;
        wl_columnar_session_test_observed_transaction_checkpoint =
            stop_transaction;
        CHECK(step(f.session) == (stop_work ? WL_EVALUATION_CONTROL_WORK_LIMIT
            : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(stop_hits == 1 && s->rels == registry
            && session_find_rel(s, "first") == first
            && wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(first))
            && wl_columnar_relation_snapshot_equal(input_before,
            wl_columnar_relation_snapshot(input)) && s->pending_input_change);
        CHECK(wl_session_insert(f.session, "input", &value, 1, 1) == EBUSY);
        wl_session_set_delta_cb(f.session, capture_delta, &f.actual);
        CHECK(!s->delta_cb); /* A stopped baseline remains protected. */
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!wl_evaluation_control_configure(f.control, 0));
        CHECK(!step(f.session) && !step(f.reference));
        check_results(&f);
        fixture_finish(&f);
    }
}

extern void (*wl_columnar_session_test_snapshot_checkpoint)(
    wl_evaluation_control_t *, unsigned, size_t);
extern int wl_columnar_session_test_snapshot_allocation;
static uint64_t evaluation_id, preparation_id;
static unsigned evaluation_hits, preparation_hits, prepare_stop;

static void
snapshot_evaluation(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    if (phase == 8 && bindings) {
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        CHECK(id && (!evaluation_id || evaluation_id == id));
        evaluation_id = id; ++evaluation_hits;
    }
    stop_transaction(control, phase, index, bindings);
}

static void
snapshot_preparation(wl_evaluation_control_t *control, unsigned phase,
    size_t index)
{
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(id && evaluation_id == id);
    preparation_id = id; ++preparation_hits;
    if (phase != prepare_stop || (phase == 2 && index != 1)) return;
    ++stop_hits;
    wl_columnar_session_test_snapshot_checkpoint = NULL;
    if (stop_work)
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
reset_snapshot_hooks(void)
{
    evaluation_id = preparation_id = 0;
    evaluation_hits = preparation_hits = 0;
    stop_phase = stop_index = prepare_stop = UINT32_MAX;
    stop_hits = 0;
    wl_columnar_session_test_observed_transaction_checkpoint =
        snapshot_evaluation;
    wl_columnar_session_test_snapshot_checkpoint = snapshot_preparation;
}

static void
clear_snapshot_hooks(void)
{
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    wl_columnar_session_test_snapshot_checkpoint = NULL;
    wl_columnar_session_test_snapshot_allocation = -1;
}

static void
test_evaluating_snapshots(void)
{
    for (unsigned dirty = 0; dirty < 2; ++dirty) {
        for (unsigned observed = 0; observed < 2; ++observed) {
            fixture_t f;
            fixture_init(&f, true);
            if (observed) {
                wl_session_set_delta_cb(f.session, capture_delta, &f.actual);
                wl_session_set_delta_cb(f.reference, capture_delta,
                    &f.expected);
            }
            if (dirty) {
                CHECK(!step(f.session) && !step(f.reference));
                f.actual.count = f.expected.count = 0;
                int64_t next = 17;
                CHECK(!wl_session_insert(f.session, "input", &next, 1, 1));
                CHECK(!wl_session_insert(f.reference, "input", &next, 1, 1));
            }
            CHECK(wl_session_snapshot(f.session, NULL, NULL) == EINVAL);
            capture_t actual = {0}, expected = {0};
            reset_snapshot_hooks();
            CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
            clear_snapshot_hooks();
            CHECK(evaluation_hits && preparation_hits &&
                evaluation_id == preparation_id);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && report.attempt_id == evaluation_id && report.consumed > 0
                && report.result == 0 &&
                !wl_evaluation_control_active_attempt_id(f.control));
            CHECK(!step(f.reference));
            CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
            check_capture(&actual, &expected);
            CHECK(actual.count > 0);
            check_capture(&f.actual, &f.expected);
            unsigned events = f.actual.count;
            CHECK(!step(f.session) && events == f.actual.count);
            check_results(&f);
            fixture_finish(&f);
        }
    }
}

static void
test_snapshot_stop_retry(void)
{
    for (unsigned dirty = 0; dirty < 2; ++dirty) {
        for (unsigned mode = 0; mode < 8; ++mode) {
            for (unsigned work = 0; work < (mode == 7 ? 1u : 2u); ++work) {
                fixture_t f;
                fixture_init(&f, true);
                wl_session_set_delta_cb(f.session, capture_delta, &f.actual);
                wl_session_set_delta_cb(f.reference, capture_delta,
                    &f.expected);
                if (dirty) {
                    CHECK(!step(f.session) && !step(f.reference));
                    f.actual.count = f.expected.count = 0;
                    int64_t next = 17;
                    CHECK(!wl_session_insert(f.session, "input", &next, 1, 1));
                    CHECK(!wl_session_insert(f.reference, "input", &next, 1,
                        1));
                }
                wl_col_session_t *s = (wl_col_session_t *)f.session;
                col_rel_t *old = session_find_rel(s, "first");
                col_relation_snapshot_t before =
                    wl_columnar_relation_snapshot(old);
                col_rel_t **registry = s->rels;
                col_rel_t *input = session_find_rel(s, "input");
                col_relation_snapshot_t input_before =
                    wl_columnar_relation_snapshot(input);
                CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_C(1000000000)));
                reset_snapshot_hooks(); stop_work = work != 0;
                if (mode < 2) {
                    stop_phase = mode ? 8 : 7; stop_index = mode ? 1 : 0;
                } else if (mode < 5) prepare_stop = mode == 2 ? 1 : mode ==
                        3 ? 2 : 3;
                else if (mode < 7) {
                    stop_phase = mode == 5 ? 13 : 14; stop_index = UINT32_MAX;
                }else wl_columnar_session_test_snapshot_allocation = 0;
                capture_t actual = {0};
                int rc = wl_session_snapshot(f.session, capture_tuple, &actual);
                clear_snapshot_hooks();
                CHECK(rc == (mode == 7 ? ENOMEM : work
                    ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                    WL_EVALUATION_CONTROL_CANCELLED));
                CHECK((mode == 7 || stop_hits == 1) && !actual.count &&
                    !f.actual.count);
                CHECK(s->rels == registry && session_find_rel(s, "first") == old
                    && (!old || wl_columnar_relation_snapshot_equal(before,
                    wl_columnar_relation_snapshot(old)))
                    && wl_columnar_relation_snapshot_equal(input_before,
                    wl_columnar_relation_snapshot(input))
                    && s->pending_input_change &&
                    !wl_evaluation_control_active_attempt_id(f.control));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control,
                    &report) && report.result == rc);
                if (mode >= 2 &&
                    mode <= 6) CHECK(evaluation_hits && preparation_hits
                        && report.attempt_id == evaluation_id &&
                        preparation_id == evaluation_id);
                int64_t denied = 29;
                CHECK(wl_session_insert(f.session, "input", &denied, 1,
                    1) == EBUSY);
                CHECK(wl_session_step(f.session) == EBUSY);
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(!wl_evaluation_control_configure(f.control, 0));
                /* A different stack context must replace the failed call's payload. */
                capture_t retry = {0}, expected = {0};
                CHECK(!wl_session_snapshot(f.session, capture_tuple, &retry));
                CHECK(!step(f.reference));
                CHECK(!wl_session_snapshot(f.reference, capture_tuple,
                    &expected));
                check_capture(&retry, &expected);
                check_capture(&f.actual, &f.expected);
                CHECK(retry.count > 0);
                fixture_finish(&f);
            }
        }
    }
}

static void
test_snapshot_cutoff_and_allowance(void)
{
    for (unsigned late_delta = 0; late_delta < 2; ++late_delta) {
        fixture_t f;
        fixture_init(&f, true);
        wl_session_set_delta_cb(f.session, capture_delta, &f.actual);
        wl_session_set_delta_cb(f.reference, capture_delta, &f.expected);
        capture_t actual = {0}, expected = {0};
        if (late_delta) f.actual.cancel_control = f.control;
        else actual.cancel_control = f.control;
        CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
        CHECK(!step(f.reference));
        CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
        check_capture(&actual, &expected);
        check_capture(&f.actual, &f.expected);
        CHECK(actual.count > 1 && f.actual.count > 1);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report)
            && !report.result &&
            report.stop_reason == WL_EVALUATION_CONTROL_NONE);
        capture_t stopped = {0};
        CHECK(wl_session_snapshot(f.session, capture_tuple, &stopped)
            == WL_EVALUATION_CONTROL_CANCELLED && !stopped.count);
        CHECK(!wl_evaluation_control_reset(f.control));
        f.actual.cancel_control = NULL;
        check_results(&f);
        fixture_finish(&f);
    }
    fixture_t reference;
    fixture_init(&reference, true);
    capture_t expected = {0};
    CHECK(!wl_session_snapshot(reference.session, capture_tuple, &expected));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(reference.control, &report));
    uint64_t measured = report.consumed;
    CHECK(measured > 1 && report.attempt_id == 1);
    fixture_finish(&reference);
    fixture_t f;
    fixture_init(&f, true);
    CHECK(!wl_evaluation_control_configure(f.control, measured - 1));
    reset_snapshot_hooks();
    capture_t actual = {0};
    CHECK(wl_session_snapshot(f.session, capture_tuple, &actual)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    clear_snapshot_hooks();
    CHECK(!actual.count && evaluation_hits && preparation_hits
        && evaluation_id == preparation_id
        && !session_find_rel((wl_col_session_t *)f.session, "first"));
    CHECK(!wl_evaluation_control_report(f.control, &report)
        && report.attempt_id == evaluation_id && report.attempt_id == 1
        && report.consumed <= measured - 1);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!wl_evaluation_control_configure(f.control, 0));
    CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
    check_capture(&actual, &expected);
    fixture_finish(&f);
}

/* Allocation and partial zeroing stop before ownership-bearing fields exist. */
static void
test_snapshot_bootstrap_limits(void)
{
    const uint64_t limits[] = {1, 63, 64, 127, 128, 191, 192, 255, 256, 511,
                               1024};
    for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
        fixture_t f;
        fixture_init(&f, false);
        CHECK(!wl_evaluation_control_configure(f.control, limits[i]));
        capture_t actual = {0}, expected = {0};
        CHECK(wl_session_snapshot(f.session, capture_tuple, &actual)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!actual.count &&
            !wl_evaluation_control_active_attempt_id(f.control)
            && !session_find_rel((wl_col_session_t *)f.session, "first"));
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!wl_evaluation_control_configure(f.control, 0));
        CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
        CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
        check_capture(&actual, &expected);
        CHECK(actual.count > 0);
        fixture_finish(&f);
    }
}

static void
test_recursive_dirty_snapshot(void)
{
    const char *source =
        ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        "path(x,y) :- edge(x,y).\npath(x,z) :- path(x,y), edge(y,z).\n";
    for (unsigned mode = 0; mode < 3; ++mode) {
        fixture_t f;
        fixture_create(&f, source);
        ((wl_col_session_t *)f.session)->diff_enabled = false;
        ((wl_col_session_t *)f.reference)->diff_enabled = false;
        int64_t rows[] = {1, 2, 2, 3}, next[] = {3, 4};
        CHECK(!wl_session_insert(f.session, "edge", rows, 2, 2));
        CHECK(!wl_session_insert(f.reference, "edge", rows, 2, 2));
        CHECK(!step(f.session) && !step(f.reference));
        CHECK(!wl_session_insert(f.session, "edge", next, 1, 2));
        CHECK(!wl_session_insert(f.reference, "edge", next, 1, 2));
        CHECK(f.session->backend->session_controlled_step_state(f.session)
            == WL_CONTROLLED_STEP_DIRTY);
        col_rel_t *old = session_find_rel((wl_col_session_t *)f.session,
                "path");
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(old);
        capture_t actual = {0}, expected = {0};
        if (mode) {
            CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_C(1000000000)));
            reset_snapshot_hooks();
            prepare_stop = 2; stop_work = mode == 2;
            CHECK(wl_session_snapshot(f.session, capture_tuple, &actual)
                == (mode == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                    : WL_EVALUATION_CONTROL_CANCELLED));
            clear_snapshot_hooks();
            CHECK(stop_hits == 1 && !actual.count
                && session_find_rel((wl_col_session_t *)f.session,
                "path") == old
                && wl_columnar_relation_snapshot_equal(before,
                wl_columnar_relation_snapshot(old)));
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!wl_evaluation_control_configure(f.control, 0));
        }
        CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
        CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
        check_capture(&actual, &expected);
        CHECK(actual.count == 6);
        next[0] = 4; next[1] = 5;
        CHECK(!wl_session_insert(f.session, "edge", next, 1, 2));
        CHECK(!wl_session_insert(f.reference, "edge", next, 1, 2));
        CHECK(!step(f.session) && !step(f.reference));
        check_results(&f);
        fixture_finish(&f);
    }
}

static void
test_mixed_multiinput_lifecycle(void)
{
    const char *source =
        ".decl input(x:int64)\n.decl mask(x:int64)\n"
        ".decl first(x:int64)\n.decl second(x:int64)\n"
        "first(x) :- input(x), mask(x).\nsecond(x) :- first(x).\n";
    fixture_t f;
    fixture_create(&f, source);
    const int64_t rows[] = {1, 2, 3}, masks[] = {1, 3};
    CHECK(!wl_session_insert(f.session, "input", rows, 3, 1));
    CHECK(!wl_session_insert(f.reference, "input", rows, 3, 1));
    CHECK(!wl_session_insert(f.session, "mask", masks, 2, 1));
    CHECK(!wl_session_insert(f.reference, "mask", masks, 2, 1));
    CHECK(!step(f.session) && !step(f.reference));
    for (unsigned cycle = 0; cycle < 6; ++cycle) {
        bool observed = cycle == 1 || cycle >= 3;
        wl_session_set_delta_cb(f.session, observed ? capture_delta : NULL,
            &f.actual);
        wl_session_set_delta_cb(f.reference, observed ? capture_delta : NULL,
            &f.expected);
        f.actual.count = f.expected.count = 0;
        int64_t value = cycle < 2 ? 4 + cycle : cycle == 2 ? 1
            : cycle == 3 ? 6 : cycle == 4 ? 3 : 7;
        int (*mutate)(wl_session_t *, const char *, const int64_t *, uint32_t,
            uint32_t)
            = cycle == 2 || cycle == 4 ? wl_session_remove : wl_session_insert;
        CHECK(!mutate(f.session, "input", &value, 1, 1));
        CHECK(!mutate(f.reference, "input", &value, 1, 1));
        int mask_rc = mutate(f.session, "mask", &value, 1, 1);
        int reference_mask_rc = mutate(f.reference, "mask", &value, 1, 1);
        /* Compaction cannot rewrite a source backing a live output alias.
         * Preserve the raw session's ownership refusal, including pending
         * changes already admitted on the other input. */
        CHECK(mask_rc == reference_mask_rc);
        if (cycle == 4) CHECK(mask_rc == 0 || mask_rc == EBUSY);
        else CHECK(!mask_rc);
        if (cycle % 2) {
            capture_t actual = {0}, expected = {0};
            CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
            CHECK(!step(f.reference));
            CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
            check_capture(&actual, &expected);
        } else CHECK(!step(f.session) && !step(f.reference));
        check_capture(&f.actual, &f.expected);
        CHECK(!observed || f.actual.count == 2);
        check_results(&f);
        unsigned events = f.actual.count;
        CHECK(!step(f.session) && f.actual.count == events);
    }
    fixture_finish(&f);
}

static fixture_t *live_fixture;
static unsigned live_hits;
static void
check_live_snapshot(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)index;
    if (phase != 8 || !bindings || live_hits) return;
    ++live_hits;
    fixture_t *f = live_fixture;
    capture_t nested = {0};
    int64_t row = 99;
    CHECK(wl_evaluation_control_active_attempt_id(control));
    CHECK(wl_session_step(f->session) == EBUSY);
    CHECK(wl_session_snapshot(f->session, capture_tuple, &nested) == EBUSY);
    CHECK(f->session->backend->session_snapshot(f->session, capture_tuple,
        &nested) == EBUSY);
    CHECK(wl_session_insert(f->session, "input", &row, 1, 1) == EBUSY);
    CHECK(wl_session_remove(f->session, "input", &row, 1, 1) == EBUSY);
    wl_session_t *destroy = f->session;
    CHECK(wl_session_try_destroy(&destroy) == EBUSY && destroy == f->session);
    CHECK(wl_evaluation_control_reset(control) == EBUSY);
    CHECK(wl_evaluation_control_configure(control, 1) == EBUSY);
    wl_session_set_delta_cb(f->session, NULL, NULL);
    CHECK(((wl_col_session_t *)f->session)->delta_cb == capture_delta);
    CHECK(!nested.count);
}

static void
test_snapshot_admission(void)
{
    fixture_t f;
    fixture_init(&f, true);
    wl_session_set_delta_cb(f.session, capture_delta, &f.actual);
    wl_session_set_delta_cb(f.reference, capture_delta, &f.expected);
    const void *owner = f.session->operation_admission;
    CHECK(!wl_evaluation_control_begin(f.control, owner));
    capture_t actual = {0}, expected = {0};
    CHECK(wl_session_snapshot(f.session, capture_tuple, &actual) == EBUSY);
    CHECK(f.session->backend->session_snapshot(f.session, capture_tuple,
        &actual) == EBUSY);
    CHECK(!wl_evaluation_control_finish(f.control, owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control, &report)
        && report.attempt_id == 1 && !report.consumed);
    CHECK(wl_session_snapshot(f.session, NULL, NULL) == EINVAL);
    CHECK(!wl_evaluation_control_report(f.control,
        &report) && report.attempt_id == 1);
    live_fixture = &f; live_hits = 0;
    wl_columnar_session_test_observed_transaction_checkpoint =
        check_live_snapshot;
    CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
    clear_snapshot_hooks(); live_fixture = NULL;
    CHECK(live_hits == 1);
    CHECK(!wl_evaluation_control_report(f.control, &report)
        && report.attempt_id == 2 && !report.result);
    CHECK(!step(f.reference));
    CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
    check_capture(&actual, &expected);
    check_capture(&f.actual, &f.expected);
    fixture_finish(&f);

    fixture_init(&f, true);
    wl_columnar_session_test_fail_framed_funding_before_stop = 1;
    CHECK(wl_session_step(f.session) == EAGAIN);
    CHECK(!wl_columnar_session_test_fail_framed_funding_before_stop);
    CHECK(wl_evaluation_control_active_attempt_id(f.control));
    actual = (capture_t){0};
    CHECK(wl_session_snapshot(f.session, capture_tuple,
        &actual) == EBUSY && !actual.count);
    CHECK(!step(f.session) && !step(f.reference));
    check_results(&f);
    fixture_finish(&f);
}

static void
test_typed_nullary_and_empty_snapshots(void)
{
    const char *typed =
        ".decl input(a:float,b:float)\n.decl first(a:float,b:float)\n"
        "first(a,b) :- input(a,b).\n";
    fixture_t f;
    fixture_create(&f, typed);
    int64_t rows[] = {wl_columnar_float_to_bits(1.5),
                      wl_columnar_float_to_bits(10.5),
                      wl_columnar_float_to_bits(2.5),
                      wl_columnar_float_to_bits(20.5)};
    int64_t next[] = {wl_columnar_float_to_bits(-7.5),
                      wl_columnar_float_to_bits(30.5)};
    CHECK(!wl_session_insert(f.session, "input", rows, 2, 2));
    CHECK(!wl_session_insert(f.reference, "input", rows, 2, 2));
    capture_t actual = {0}, expected = {0};
    CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
    CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
    check_capture(&actual, &expected);
    col_rel_t *first = session_find_rel((wl_col_session_t *)f.session, "first");
    CHECK(actual.count == 2 && first->column_types
        && first->column_types[0] == WIRELOG_TYPE_FLOAT
        && first->column_types[1] == WIRELOG_TYPE_FLOAT);
    CHECK(!wl_session_insert(f.session, "input", next, 1, 2));
    CHECK(!wl_session_insert(f.reference, "input", next, 1, 2));
    CHECK(!step(f.session) && !step(f.reference));
    check_results(&f);
    fixture_finish(&f);

    const char *nullary =
        ".decl input(x:int64)\n.decl first()\nfirst() :- input(x).\n";
    fixture_create(&f, nullary);
    int64_t integers[] = {1, 2};
    CHECK(!wl_session_insert(f.session, "input", integers, 2, 1));
    CHECK(!wl_session_insert(f.reference, "input", integers, 2, 1));
    actual = expected = (capture_t){0};
    CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
    CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
    check_capture(&actual, &expected);
    CHECK(actual.count == 1 && !actual.events[0].width);
    fixture_finish(&f);

    const char *filtered =
        ".decl input(x:int64)\n.decl first(x:int64)\nfirst(x) :- input(x), x < 0.\n";
    fixture_create(&f, filtered);
    for (unsigned trial = 0; trial < 3; ++trial) {
        if (trial) {
            int64_t value = trial == 1 ? 5 : -1;
            CHECK(!wl_session_insert(f.session, "input", &value, 1, 1));
            CHECK(!wl_session_insert(f.reference, "input", &value, 1, 1));
        }
        actual = expected = (capture_t){0};
        reset_snapshot_hooks();
        CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
        clear_snapshot_hooks();
        CHECK(evaluation_hits && preparation_hits &&
            evaluation_id == preparation_id);
        CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
        check_capture(&actual, &expected);
        CHECK(actual.count == (trial == 2 ? 1u : 0u));
        if (trial == 2) CHECK(actual.events[0].row[0] == -1);
    }
    fixture_finish(&f);
}

static wl_columnar_memory_governor_t *pressure_governor;
static wl_columnar_memory_reservation_t *pressure_credit;
static unsigned pressure_phase, pressure_hits;

static void
snapshot_pressure(wl_evaluation_control_t *control, unsigned phase,
    size_t index)
{
    (void)control; (void)index;
    if (phase != pressure_phase || pressure_hits) return;
    ++pressure_hits;
    uint64_t reserved = wl_columnar_memory_reserved(pressure_governor);
    CHECK(reserved < pressure_governor->budget_bytes);
    CHECK(wl_columnar_memory_reserve_checked(pressure_governor,
        pressure_governor->budget_bytes - reserved, pressure_credit)
        == WL_COLUMNAR_MEMORY_ADMISSION_OK);
}

static void
test_snapshot_resource_failures(void)
{
    for (unsigned dirty = 0; dirty < 2; ++dirty) {
        for (unsigned mode = 0; mode < 3; ++mode) {
            fixture_t f;
            fixture_init(&f, true);
            wl_session_set_delta_cb(f.session, capture_delta, &f.actual);
            wl_session_set_delta_cb(f.reference, capture_delta, &f.expected);
            if (dirty) {
                CHECK(!step(f.session) && !step(f.reference));
                f.actual.count = f.expected.count = 0;
                int64_t value = 17;
                CHECK(!wl_session_insert(f.session, "input", &value, 1, 1));
                CHECK(!wl_session_insert(f.reference, "input", &value, 1, 1));
            }
            wl_col_session_t *session = (wl_col_session_t *)f.session;
            col_rel_t *old = session_find_rel(session, "first");
            wl_columnar_memory_reservation_t pressure;
            wl_columnar_memory_reservation_init(&pressure);
            pressure_hits = 0; pressure_phase = mode == 0 ? 1 : 5;
            pressure_governor = wl_columnar_memory_governor_ref_get(f.governor);
            pressure_credit = &pressure;
            if (mode <
                2) wl_columnar_session_test_snapshot_checkpoint =
                    snapshot_pressure;
            else wl_columnar_session_test_snapshot_allocation = 1;
            capture_t actual = {0}, expected = {0};
            int rc = wl_session_snapshot(f.session, capture_tuple, &actual);
            clear_snapshot_hooks();
            CHECK(rc == (mode < 2 ? ENOSPC : ENOMEM));
            CHECK(!actual.count && !f.actual.count
                && session_find_rel(session, "first") == old
                && session->memory_budget_denied == (mode < 2));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && report.result == rc && !report.cleanup_status);
            if (mode < 2) CHECK(pressure_hits == 1 &&
                    wl_columnar_memory_rollback(&pressure));
            pressure_credit = NULL; pressure_governor = NULL;
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!wl_session_snapshot(f.session, capture_tuple, &actual));
            CHECK(!session->memory_budget_denied);
            CHECK(!step(f.reference));
            CHECK(!wl_session_snapshot(f.reference, capture_tuple, &expected));
            check_capture(&actual, &expected);
            check_capture(&f.actual, &f.expected);
            CHECK(actual.count > 0);
            fixture_finish(&f);
        }
    }
}

int
main(void)
{
    test_step_successors();
    test_successor_stop_retry();
    test_evaluating_snapshots();
    test_snapshot_stop_retry();
    test_snapshot_cutoff_and_allowance();
    test_snapshot_bootstrap_limits();
    test_recursive_dirty_snapshot();
    test_mixed_multiinput_lifecycle();
    test_snapshot_admission();
    test_typed_nullary_and_empty_snapshots();
    test_snapshot_resource_failures();
    puts("controlled lifecycle: ok");
    return 0;
}
