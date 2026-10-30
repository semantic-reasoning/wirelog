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
fixture_init(fixture_t *f, bool multi)
{
    memset(f, 0, sizeof(*f));
    const char *source = multi
        ? ".decl input(x:int64)\n.decl first(x:int64)\n.decl second(x:int64)\n"
        ".decl rejected(x:int64)\nfirst(x) :- input(x).\n"
        "second(x) :- first(x).\nrejected(x) :- second(x), x < 0.\n"
        : ".decl input(x:int64)\n.decl first(x:int64)\nfirst(x) :- input(x).\n";
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
    if (phase != stop_phase || index != stop_index
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

int
main(void)
{
    test_step_successors();
    test_successor_stop_retry();
    puts("controlled lifecycle: ok");
    return 0;
}
