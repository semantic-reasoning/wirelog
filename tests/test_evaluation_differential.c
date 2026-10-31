/* Scalar selector contracts use internal plans; lifecycle tests below use
 * unchanged compiler plans. Neither fixture injects an active raw evaluator. */
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
                              #x); abort(); \
                      } } while (0)

static int
finish_step(wl_session_t *session)
{
    uint64_t attempt = 0;
    for (unsigned turn = 0; turn < 1000000; ++turn) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
        if (session->evaluation_control) {
            uint64_t active = wl_evaluation_control_active_attempt_id(
                session->evaluation_control);
            CHECK(active && (!attempt || attempt == active));
            attempt = active;
        }
    }
    CHECK(false);
    return EIO;
}

static void
add_delta(wl_session_t *base, const char *name, const char *full_name,
    const int64_t *rows, uint32_t count)
{
    wl_col_session_t *session = (wl_col_session_t *)base;
    col_rel_t *full = session_find_rel(session, full_name);
    CHECK(full);
    col_rel_t *delta = col_rel_new_like(name, full);
    CHECK(delta);
    delta->nrows = delta->base_nrows = delta->sorted_nrows = 0;
    delta->run_count = 0;
    bool denied = false;
    CHECK(!col_rel_append_rows_atomic(delta, rows, count, 2, &denied));
    CHECK(!session_add_rel(session, delta));
}

typedef struct { int64_t cells[4]; } tuple_t;

static int
tuple_order(const void *a, const void *b)
{
    const tuple_t *left = a, *right = b;
    for (unsigned c = 0; c < 4; ++c) {
        if (left->cells[c] != right->cells[c])
            return left->cells[c] < right->cells[c] ? -1 : 1;
    }
    return 0;
}

static void
check_output(wl_session_t *base, const tuple_t *expected, unsigned count)
{
    col_rel_t *output = session_find_rel((wl_col_session_t *)base, "output");
    CHECK(output && output->ncols == 4 && output->nrows == count);
    tuple_t actual[16];
    CHECK(count <= 16);
    for (unsigned r = 0; r < count; ++r)
        for (unsigned c = 0; c < 4; ++c)
            actual[r].cells[c] = output->columns[c][r];
    qsort(actual, count, sizeof(*actual), tuple_order);
    CHECK(!memcmp(actual, expected, count * sizeof(*actual)));
}

/* Independent iteration-zero scalar oracle. The delta flags are source
 * metadata, not an injected session->delta_seeded/current_iteration state. */
static bool batch_stop_work, batch_stop_hit, batch_selected_delta;

static void
stop_selected_batch(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (phase != 7 || batch_stop_hit) return;
    batch_stop_hit = true; batch_selected_delta = progress != 0;
    if (batch_stop_work)
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
test_batch_selectors(void)
{
    static const char source[] =
        ".decl a(k:int64,x:int64)\n"
        ".decl b(k:int64,y:int64)\n"
        ".decl output(k:int64,x:int64,k2:int64,y:int64)\n"
        "output(k,x,k,y) :- a(k,x), b(k,y), x < y.\n";
    const int64_t left_full[] = {1, 1, 1, 2};
    const int64_t right_full[] = {1, 5, 1, 6};
    const int64_t left_delta[][6] = {{0}, {0}, {1, 2},
                                     {1, 3, 1, 4}, {1, 3, 1, 4, 1, 9}};
    const int64_t right_delta[][6] = {{0}, {0}, {1, 6},
                                      {1, 7, 1, 8}, {1, 7, 1, 8, 1, 10}};
    const uint32_t sizes[] = {0, 0, 1, 2, 3};
    const wl_delta_mode_t left_modes[] = {WL_DELTA_AUTO, WL_DELTA_FORCE_FULL,
                                          WL_DELTA_FORCE_DELTA,
                                          WL_DELTA_FORCE_EMPTY,
                                          WL_DELTA_FORCE_EMPTY_AFTER_SEED};
    const wl_delta_mode_t right_modes[] = {WL_DELTA_AUTO, WL_DELTA_FORCE_FULL,
                                           WL_DELTA_FORCE_DELTA};
    for (unsigned lm = 0; lm < 5; ++lm)
        for (unsigned rm = 0; rm < 3; ++rm)
            for (unsigned ld = 0; ld < 5; ++ld)
                for (unsigned rd = 0; rd < 5; ++rd) {
                    wirelog_error_t error;
                    wirelog_program_t *program = wirelog_parse_string(source,
                            &error);
                    wl_plan_t *plan = NULL;
                    CHECK(program && !wl_plan_from_program(program, &plan));
                    CHECK(plan->stratum_count == 1
                        && plan->strata[0].relation_count == 1);
                    wl_plan_relation_t *output =
                        (wl_plan_relation_t *)&plan->strata[0].relations[0];
                    CHECK(output->op_count == 5);
                    wl_plan_op_t *ops = (wl_plan_op_t *)output->ops;
                    CHECK(ops[0].op == WL_PLAN_OP_VARIABLE
                        && ops[1].op == WL_PLAN_OP_JOIN);
                    ops[0].delta_mode = left_modes[lm];
                    ops[1].delta_mode = right_modes[rm];
                    wl_evaluation_control_t *control = NULL;
                    CHECK(!wl_evaluation_control_create(UINT64_C(1000000),
                        &control));
                    wl_session_options_t options;
                    wl_session_options_init(&options);
                    options.evaluation_control = control;
                    wl_session_t *controlled = NULL, *reference = NULL;
                    CHECK(!wl_session_create_with_options(wl_backend_columnar(),
                        plan, 1, &options, &controlled));
                    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                        &reference));
                    ((wl_col_session_t *)controlled)->join_batch_bytes = 224;
                    wl_session_t *sessions[] = {controlled, reference};
                    for (unsigned s = 0; s < 2; ++s) {
                        CHECK(!wl_session_insert(sessions[s], "a", left_full,
                            2, 2));
                        CHECK(!wl_session_insert(sessions[s], "b", right_full,
                            2, 2));
                        if (ld) add_delta(sessions[s], "$d$a", "a",
                                left_delta[ld], sizes[ld]);
                        if (rd) add_delta(sessions[s], "$d$b", "b",
                                right_delta[rd], sizes[rd]);
                    }
                    bool left_is_delta = sizes[ld] &&
                        (lm == 2 || ((lm == 0 || lm == 4) && sizes[ld] < 2));
                    bool right_is_delta = sizes[rd] &&
                        (rm == 2 || (rm == 0 && !left_is_delta
                        && sizes[rd] < 2));
                    const int64_t *left = left_is_delta
                        ? left_delta[ld] : left_full;
                    const int64_t *right = right_is_delta
                        ? right_delta[rd] : right_full;
                    unsigned left_count = lm == 3 ? 0
                        : left_is_delta ? sizes[ld] : 2;
                    unsigned right_count = right_is_delta ? sizes[rd] : 2;
                    tuple_t expected[16]; unsigned count = 0;
                    for (unsigned l = 0; l < left_count; ++l)
                        for (unsigned r = 0; r < right_count; ++r)
                            if (left[2 * l] == right[2 * r]
                                && left[2 * l + 1] < right[2 * r + 1]) {
                                tuple_t row = {{left[2 * l], left[2 * l + 1],
                                                right[2 * r],
                                                right[2 * r + 1]}};
                                expected[count++] = row;
                            }
                    qsort(expected, count, sizeof(*expected), tuple_order);
                    CHECK(!finish_step(reference));
                    check_output(reference, expected, count);
                    if (lm == 0 && (rm == 0 || rm == 2) && ld == 2 && rd == 2) {
                        col_rel_t *input = session_find_rel(
                            (wl_col_session_t *)controlled, "a");
                        col_relation_snapshot_t before =
                            wl_columnar_relation_snapshot(input);
                        batch_stop_work = rm == 2; batch_stop_hit = false;
                        wl_columnar_join_test_attempt_checkpoint =
                            stop_selected_batch;
                        int stopped = finish_step(controlled);
                        wl_columnar_join_test_attempt_checkpoint = NULL;
                        CHECK(stopped == (batch_stop_work
                            ? WL_EVALUATION_CONTROL_WORK_LIMIT
                            : WL_EVALUATION_CONTROL_CANCELLED));
                        CHECK(batch_stop_hit &&
                            batch_selected_delta == (rm == 2));
                        CHECK(!session_find_rel((wl_col_session_t *)controlled,
                            "output"));
                        CHECK(!((wl_col_session_t *)controlled)
                            ->controlled_materialized_batch_published_count);
                        CHECK(wl_columnar_relation_snapshot_equal(before,
                            wl_columnar_relation_snapshot(input)));
                        CHECK(!wl_evaluation_control_active_attempt_id(
                                control));
                        CHECK(!wl_evaluation_control_reset(control));
                        CHECK(!wl_evaluation_control_configure(control,
                            UINT64_C(1000000)));
                    }
                    CHECK(!finish_step(controlled));
                    check_output(controlled, expected, count);
                    CHECK(((wl_col_session_t *)controlled)
                        ->controlled_materialized_batch_published_count == 1);
                    CHECK(!wl_evaluation_control_active_attempt_id(control));
                    CHECK(!wl_session_try_destroy(&controlled));
                    wl_session_destroy(reference);
                    wl_evaluation_control_release(control);
                    wl_plan_free(plan); wirelog_program_free(program);
                }
}

typedef struct {
    char name[32];
    int64_t row[4];
    uint32_t width;
    int diff;
} event_t;

typedef struct { event_t events[512]; unsigned count; } capture_t;

static void
capture_delta(const char *name, const int64_t *row, uint32_t width,
    int diff, void *data)
{
    capture_t *capture = data;
    CHECK(capture->count < 512 && width <= 4 && strlen(name) < 32);
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
event_order(const void *a, const void *b)
{
    const event_t *left = a, *right = b;
    int order = strcmp(left->name, right->name);
    if (order) return order;
    if (left->width != right->width) return left->width < right->width ? -1 : 1;
    for (unsigned c = 0; c < left->width; ++c)
        if (left->row[c] != right->row[c])
            return left->row[c] < right->row[c] ? -1 : 1;
    return (left->diff > right->diff) - (left->diff < right->diff);
}

static void
check_capture(capture_t *actual, capture_t *expected)
{
    if (actual->count != expected->count)
        fprintf(stderr, "capture actual=%u expected=%u\n", actual->count,
            expected->count);
    CHECK(actual->count == expected->count);
    qsort(actual->events, actual->count, sizeof(event_t), event_order);
    qsort(expected->events, expected->count, sizeof(event_t), event_order);
    CHECK(!memcmp(actual->events, expected->events,
        actual->count * sizeof(event_t)));
}

static void
check_path(wl_session_t *base, const int64_t *edges, unsigned edge_count)
{
    bool reach[6][6] = {{false}};
    for (unsigned r = 0; r < edge_count; ++r) {
        CHECK(edges[2 * r] < 6 && edges[2 * r + 1] < 6);
        reach[edges[2 * r]][edges[2 * r + 1]] = true;
    }
    for (unsigned k = 0; k < 6; ++k)
        for (unsigned i = 0; i < 6; ++i)
            for (unsigned j = 0; j < 6; ++j)
                reach[i][j] = reach[i][j] || (reach[i][k] && reach[k][j]);
    capture_t expected = {0}, actual = {0};
    for (unsigned i = 0; i < 6; ++i)
        for (unsigned j = 0; j < 6; ++j)
            if (reach[i][j]) {
                int64_t row[] = {i, j};
                capture_tuple("path", row, 2, &expected);
            }
    CHECK(!wl_session_snapshot(base, capture_tuple, &actual));
    check_capture(&actual, &expected);
}

static void
test_recursive_removal_successor(void)
{
    const char *source =
        ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,z) :- path(x,y), edge(y,z).\n";
    for (unsigned mode = 0; mode < 5; ++mode) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options;
        wl_session_options_init(&options); options.evaluation_control = control;
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor);
        options.memory_governor = governor;
        wl_session_t *session = NULL, *reference = NULL;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session));
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &reference));
        const int64_t initial[] = {1, 2, 2, 3, 3, 4};
        const int64_t removed[] = {2, 3};
        CHECK(!wl_session_insert(session, "edge", initial, 3, 2));
        CHECK(!wl_session_insert(reference, "edge", initial, 3, 2));
        int initial_rc = finish_step(session);
        if (initial_rc) fprintf(stderr, "recursive initial rc=%d\n",
                initial_rc);
        CHECK(!initial_rc && !finish_step(reference));
        check_path(session, initial, 3);
        /* A zero request must leave the qualified fast path usable. */
        CHECK(!wl_session_remove(session, "edge", removed, 0, 2));
        CHECK(session->backend->session_controlled_step_state(session)
            == WL_CONTROLLED_STEP_COMPLETE);
        CHECK(wl_session_remove(session, "path", removed, 1, 2) == ENOTSUP);
        capture_t actual_events = {0}, reference_events = {0};
        if (mode == 2) {
            wl_session_set_delta_cb(session, capture_delta, &actual_events);
            wl_session_set_delta_cb(reference, capture_delta,
                &reference_events);
        }
        const int64_t extension[] = {4, 5};
        if (mode == 4) {
            CHECK(!wl_session_insert(session, "edge", extension, 1, 2));
            CHECK(!wl_session_insert(reference, "edge", extension, 1, 2));
        }
        CHECK(!wl_session_remove(session, "edge", removed, 1, 2));
        CHECK(!wl_session_remove(reference, "edge", removed, 1, 2));
        CHECK(session->backend->session_controlled_step_state(session)
            == WL_CONTROLLED_STEP_DIRTY);
        const int64_t remaining[] = {1, 2, 3, 4, 4, 5};
        unsigned remaining_count = mode == 4 ? 3 : 2;
        if (mode == 1) {
            check_path(session, remaining, remaining_count);
            check_path(reference, remaining, remaining_count);
        } else {
            CHECK(!finish_step(session) && !finish_step(reference));
            check_capture(&actual_events, &reference_events);
            if (mode == 2) {
                CHECK(actual_events.count == 4);
                for (unsigned i = 0; i < actual_events.count; ++i)
                    CHECK(actual_events.events[i].diff == -1);
            }
            check_path(session, remaining, remaining_count);
        }
        if (mode == 3) {
            /* Callback changes use the completed boundary, not dirty-input
             * admission (where the public setter deliberately refuses). */
            wl_session_set_delta_cb(session, capture_delta, &actual_events);
            wl_session_set_delta_cb(reference, capture_delta,
                &reference_events);
        }
        /* Repeated removals and an insertion before a single successor must
         * rebuild from the final EDB, never the old recursive closure. */
        const int64_t append[] = {2, 4};
        actual_events.count = reference_events.count = 0;
        wl_session_t *sessions[] = {session, reference};
        for (unsigned s = 0; s < 2; ++s) {
            CHECK(!wl_session_remove(sessions[s], "edge", remaining, 1, 2));
            CHECK(!wl_session_remove(sessions[s], "edge", remaining + 2, 1, 2));
            if (mode == 4)
                CHECK(!wl_session_remove(sessions[s], "edge", extension, 1, 2));
            CHECK(!wl_session_insert(sessions[s], "edge", append, 1, 2));
        }
        CHECK(!finish_step(session) && !finish_step(reference));
        check_capture(&actual_events, &reference_events);
        if (mode == 2 || mode == 3) CHECK(actual_events.count == 3);
        check_path(session, append, 1);
        CHECK(!wl_session_try_destroy(&session));
        wl_session_destroy(reference); wl_evaluation_control_release(control);
        wl_plan_free(plan); wirelog_program_free(program);
        CHECK(!wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
}

extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
static unsigned private_reads, append_reads;

static void
mutation_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)control; (void)index;
    if (phase == 8 && bindings && private_reads++ == 0) {
        wl_columnar_eval_private_bindings_t forged = *bindings;
        col_diff_arrangement_t *index = (col_diff_arrangement_t *)(uintptr_t)1;
        bool build = true;
        CHECK(wl_columnar_session_diff_index(bindings->session, &forged,
            NULL, "r", NULL, 0, &index, &build) == EINVAL);
        CHECK(!index && !build);
    }
    if (phase == 44) ++append_reads;
}

static void
test_generated_differential_mutations(void)
{
    const char *source =
        ".decl l(x:int64)\n.decl r(x:int64,y:int64)\n"
        ".decl mask(x:int64)\n.decl base(x:int64)\n"
        ".decl output(x:int64)\n"
        "base(x) :- l(x), r(x,y).\n"
        "output(x) :- base(x), !mask(x).\n";
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned snapshot = 0; snapshot < 2; ++snapshot) {
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(source, &error);
            wl_plan_t *plan = NULL;
            CHECK(program && !wl_plan_from_program(program, &plan));
            CHECK(plan->stratum_count > 1);
            wl_evaluation_control_t *control = NULL;
            CHECK(!wl_evaluation_control_create(0, &control));
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = control;
            wl_session_t *session = NULL, *reference = NULL;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1, &options, &session));
            CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                &reference));
            capture_t actual_events = {0}, reference_events = {0};
            if (observed) {
                wl_session_set_delta_cb(session, capture_delta, &actual_events);
                wl_session_set_delta_cb(reference, capture_delta,
                    &reference_events);
            }
            wl_session_t *sessions[] = {session, reference};
            for (unsigned s = 0; s < 2; ++s) {
                const int64_t left[] = {1, 2}, right[] = {1, 10, 2, 20};
                const int64_t initial_mask = 99;
                CHECK(!wl_session_insert(sessions[s], "l", left, 2, 1));
                CHECK(!wl_session_insert(sessions[s], "r", right, 2, 2));
                CHECK(!wl_session_insert(sessions[s], "mask", &initial_mask,
                    1, 1));
            }
            for (unsigned cycle = 0; cycle < 9; ++cycle) {
                actual_events.count = reference_events.count = 0;
                for (unsigned s = 0; s < 2; ++s) {
                    const int64_t one = 1, two = 2, three = 3, absent = 88;
                    const int64_t next[] = {3, 30};
                    if (cycle == 1) {
                        CHECK(!wl_session_insert(sessions[s], "l", &three, 1,
                            1));
                        CHECK(!wl_session_insert(sessions[s], "r", next, 1, 2));
                    } else if (cycle == 2)
                        CHECK(!wl_session_insert(sessions[s], "mask", &one, 1,
                            1));
                    else if (cycle == 3)
                        CHECK(!wl_session_remove(sessions[s], "mask", &one, 1,
                            1));
                    else if (cycle == 4) {
                        CHECK(!wl_session_remove(sessions[s], "l", &two, 1, 1));
                        const int64_t extra[] = {3, 31};
                        CHECK(!wl_session_insert(sessions[s], "r", extra, 1,
                            2));
                    } else if (cycle == 5) {
                        CHECK(!wl_session_remove(sessions[s], "mask", &absent,
                            1, 1));
                        CHECK(!wl_session_insert(sessions[s], "l", &one, 1, 1));
                    } else if (cycle == 6) {
                        const int64_t ones[] = {1, 1};
                        CHECK(!wl_session_remove(sessions[s], "l", ones, 2, 1));
                        CHECK(!wl_session_remove(sessions[s], "l", &three, 1,
                            1));
                    } else if (cycle == 7)
                        CHECK(!wl_session_insert(sessions[s], "l", &three, 1,
                            1));
                    else if (cycle == 8) {
                        const int64_t removed_right[] = {3, 30, 3, 31};
                        CHECK(!wl_session_remove(sessions[s], "r",
                            removed_right,
                            2, 2));
                    }
                }
                capture_t actual = {0}, expected = {0};
                private_reads = append_reads = 0;
                wl_columnar_session_test_observed_transaction_checkpoint =
                    mutation_checkpoint;
                if (snapshot && cycle)
                    CHECK(!wl_session_snapshot(session, capture_tuple,
                        &actual));
                else {
                    int execution_rc = finish_step(session);
                    if (execution_rc) fprintf(stderr,
                            "mutation observed=%u snapshot=%u cycle=%u rc=%d\n",
                            observed, snapshot, cycle, execution_rc);
                    CHECK(!execution_rc);
                }
                wl_columnar_session_test_observed_transaction_checkpoint = NULL;
                if (cycle && (observed || snapshot || cycle >= 3))
                    CHECK(private_reads > 0);
                if (cycle == 3 || cycle == 4 || cycle == 6 || cycle == 8)
                    CHECK(!append_reads);
                if (snapshot && cycle)
                    CHECK(!wl_session_snapshot(reference, capture_tuple,
                        &expected));
                else {
                    CHECK(!finish_step(reference));
                    /* Witness natural scheduler selection before snapshot
                     * recomputation clears the flag. No internal flag writes. */
                    if (cycle == 2 && observed)
                        CHECK(
                            ((wl_col_session_t *)reference)->
                            diff_operators_active);
                    check_capture(&actual_events, &reference_events);
                    CHECK(!wl_session_snapshot(session, capture_tuple,
                        &actual));
                    CHECK(!wl_session_snapshot(reference, capture_tuple,
                        &expected));
                }
                check_capture(&actual, &expected);
                unsigned expected_rows[] = {4, 6, 5, 6, 4, 4, 0, 2, 0};
                CHECK(actual.count == expected_rows[cycle]);
                if (cycle) CHECK(
                        !((wl_col_session_t *)session)->pending_input_change
                        && !((wl_col_session_t *)session)->last_removed_relation);
            }
            CHECK(!wl_session_try_destroy(&session));
            wl_session_destroy(reference);
            wl_evaluation_control_release(control);
            wl_plan_free(plan); wirelog_program_free(program);
        }
}

static unsigned recovery_phase;
static bool recovery_work, recovery_hit, recovery_join;

static void
request_recovery_stop(wl_evaluation_control_t *control)
{
    recovery_hit = true;
    if (recovery_work)
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
stop_reconstruction(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)index;
    if (recovery_join || recovery_hit || phase != recovery_phase
        || (phase == 8 && !bindings)) return;
    request_recovery_stop(control);
}

static void
stop_join(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    (void)progress;
    if (!recovery_join || recovery_hit || phase != recovery_phase) return;
    request_recovery_stop(control);
}

static void
test_recursive_removal_recovery(void)
{
    const char *source =
        ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,z) :- path(x,y), edge(y,z).\n";
    const unsigned phases[] = {3, 6, 8, 13, 14, 15, 28, 29, 0};
    for (unsigned trial = 0; trial < 9; ++trial)
        for (unsigned work = 0; work < 2; ++work) {
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(source, &error);
            wl_plan_t *plan = NULL;
            CHECK(program && !wl_plan_from_program(program, &plan));
            wl_evaluation_control_t *control = NULL;
            CHECK(!wl_evaluation_control_create(0, &control));
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
            };
            wl_columnar_memory_governor_ref_t *governor =
                wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor);
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = control;
            options.memory_governor = governor;
            wl_session_t *session = NULL;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1, &options, &session));
            const int64_t initial[] = {1, 2, 2, 3, 3, 4}, removed[] = {2, 3};
            CHECK(!wl_session_insert(session, "edge", initial, 3, 2));
            CHECK(!finish_step(session));
            wl_col_session_t *columnar = (wl_col_session_t *)session;
            col_rel_t *edge = session_find_rel(columnar, "edge");
            col_rel_t *old_path = session_find_rel(columnar, "path");
            col_relation_snapshot_t old_snapshot =
                wl_columnar_relation_snapshot(old_path);
            wl_evaluation_control_report_t before = {0}, after = {0};
            CHECK(!wl_evaluation_control_report(control, &before));
            /* A real foreign reader still protects destructive mutation.
             * Refusal must not promote the completed recursive host. */
            wl_columnar_source_access_reader_t reader = {0};
            CHECK(!col_rel_source_reader_acquire(edge, &reader));
            CHECK(wl_session_remove(session, "edge", removed, 1, 2) == EBUSY);
            CHECK(!col_rel_source_reader_release(&reader));
            CHECK(!wl_evaluation_control_report(control, &after));
            CHECK(!memcmp(&before, &after, sizeof(before)));
            CHECK(session->backend->session_controlled_step_state(session)
                == WL_CONTROLLED_STEP_COMPLETE);
            capture_t actual = {0}, expected = {0};
            wl_session_set_delta_cb(session, capture_delta, &actual);
            CHECK(!wl_session_remove(session, "edge", removed, 1, 2));
            col_relation_snapshot_t input_snapshot =
                wl_columnar_relation_snapshot(edge);
            CHECK(!wl_evaluation_control_configure(control,
                UINT64_C(1000000000)));
            recovery_phase = phases[trial]; recovery_work = work != 0;
            recovery_hit = false; recovery_join = trial == 6 || trial == 7;
            wl_columnar_session_test_observed_transaction_checkpoint =
                stop_reconstruction;
            wl_columnar_join_test_attempt_checkpoint = stop_join;
            if (trial == 8) {
                recovery_hit = true;
                if (work) CHECK(!wl_evaluation_control_configure(control, 1));
                else wl_evaluation_control_request_cancel(control);
            }
            int rc = finish_step(session);
            wl_columnar_session_test_observed_transaction_checkpoint = NULL;
            wl_columnar_join_test_attempt_checkpoint = NULL;
            if (!recovery_hit) fprintf(stderr,
                    "unreached recovery trial=%u phase=%u\n",
                    trial, recovery_phase);
            CHECK(recovery_hit &&
                !wl_evaluation_control_active_attempt_id(control));
            bool committed = trial == 5;
            int stopped = work ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(rc == (committed && !work ? 0 : stopped));
            CHECK(wl_columnar_relation_snapshot_equal(input_snapshot,
                wl_columnar_relation_snapshot(edge)));
            if (!committed) {
                CHECK(!actual.count && columnar->pending_input_change);
                CHECK(session_find_rel(columnar, "path") == old_path);
                CHECK(wl_columnar_relation_snapshot_equal(old_snapshot,
                    wl_columnar_relation_snapshot(old_path)));
                CHECK(!col_rel_source_reader_acquire(old_path, &reader));
                CHECK(!col_rel_source_reader_release(&reader));
            } else CHECK(actual.count == 4 && !columnar->pending_input_change);
            memset(&before, 0, sizeof(before));
            memset(&after, 0, sizeof(after));
            CHECK(!wl_evaluation_control_report(control, &before)
                && before.result == rc && !before.cleanup_status);
            CHECK(!wl_evaluation_control_report(control, &after));
            CHECK(!memcmp(&before, &after, sizeof(before)));
            CHECK(!wl_evaluation_control_reset(control));
            CHECK(!wl_evaluation_control_configure(control, 0));
            CHECK(!finish_step(session));
            if (!committed) {
                CHECK(!wl_evaluation_control_report(control, &after));
                CHECK(after.attempt_id != before.attempt_id && !after.result);
            }
            const int64_t lost[][2] = {{1, 3}, {1, 4}, {2, 3}, {2, 4}};
            for (unsigned i = 0; i < 4; ++i)
                capture_delta("path", lost[i], 2, -1, &expected);
            check_capture(&actual, &expected);
            const int64_t remaining[] = {1, 2, 3, 4};
            check_path(session, remaining, 2);
            unsigned delivered = actual.count;
            CHECK(!finish_step(session) && actual.count == delivered);
            const int64_t next[] = {1, 3};
            CHECK(!wl_session_insert(session, "edge", next, 1, 2));
            CHECK(!finish_step(session));
            capture_delta("path", lost[0], 2, 1, &expected);
            capture_delta("path", lost[1], 2, 1, &expected);
            check_capture(&actual, &expected);
            const int64_t final_edges[] = {1, 2, 3, 4, 1, 3};
            check_path(session, final_edges, 3);
            CHECK(!wl_session_try_destroy(&session));
            wl_evaluation_control_release(control);
            wl_plan_free(plan); wirelog_program_free(program);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
        }
}

int
main(void)
{
    test_batch_selectors();
    test_recursive_removal_successor();
    test_generated_differential_mutations();
    test_recursive_removal_recovery();
    puts("evaluation differential tests passed");
    return 0;
}
