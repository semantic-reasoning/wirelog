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
    for (unsigned turn = 0; turn < 1000000; ++turn) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
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

int
main(void)
{
    test_batch_selectors();
    puts("evaluation differential tests passed");
    return 0;
}
