/*
 * test_diff_bridge.c - Differential bridge translation tests (Issue #262)
 *
 * Tests bidirectional translation between col_frontier_2d_t (epoch model)
 * and col_diff_trace_t (lattice model) via the diff_bridge inline functions.
 */

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#include "../wirelog/columnar/diff_bridge.h"

/* ========================================================================
 * TEST HARNESS MACROS
 * ======================================================================== */

#define TEST(name)                       \
        do {                                 \
            printf("  [TEST] %-60s ", name); \
            fflush(stdout);                  \
        } while (0)

#define PASS              \
        do {                  \
            printf("PASS\n"); \
            tests_passed++;   \
        } while (0)

#define FAIL(msg)                  \
        do {                           \
            printf("FAIL: %s\n", msg); \
            tests_failed++;            \
        } while (0)

#define ASSERT_TRUE(cond, msg) \
        do {                       \
            if (!(cond)) {         \
                FAIL(msg);         \
                return;            \
            }                      \
        } while (0)

static int tests_passed = 0;
static int tests_failed = 0;

/* ========================================================================
 * TEST SUITE: epoch_to_trace
 * ======================================================================== */

static void
test_epoch_to_trace_basic(void)
{
    TEST(
        "epoch_to_trace: (5,10) -> trace with epoch=5,iter=10,worker=0,reserved=0");
    col_frontier_2d_t f2d = { .outer_epoch = 5, .iteration = 10 };
    col_diff_trace_t t = col_diff_bridge_epoch_to_trace(&f2d);
    ASSERT_TRUE(t.outer_epoch == 5,  "outer_epoch mismatch");
    ASSERT_TRUE(t.iteration == 10, "iteration mismatch");
    ASSERT_TRUE(t.worker == 0,  "worker should be 0");
    ASSERT_TRUE(t._reserved == 0,  "reserved should be 0");
    PASS;
}

/* ========================================================================
 * TEST SUITE: trace_to_epoch
 * ======================================================================== */

static void
test_trace_to_epoch_basic(void)
{
    TEST("trace_to_epoch: trace(5,10,3,0) -> (5,10) (worker dropped)");
    col_diff_trace_t t = col_diff_trace_init(5, 10, 3);
    col_frontier_2d_t f2d = col_diff_bridge_trace_to_epoch(&t);
    ASSERT_TRUE(f2d.outer_epoch == 5,  "outer_epoch mismatch");
    ASSERT_TRUE(f2d.iteration == 10, "iteration mismatch");
    PASS;
}

/* ========================================================================
 * TEST SUITE: Round-trip identity
 * ======================================================================== */

static void
test_round_trip_identity(void)
{
    TEST("round_trip: epoch->trace->epoch == original for various values");
    uint32_t epochs[] = { 0, 1, 7, 100, 65535 };
    uint32_t iters[] = { 0, 1, 3, 255, 65535 };
    for (int i = 0; i < 5; i++) {
        col_frontier_2d_t orig = { .outer_epoch = epochs[i],
                                   .iteration = iters[i] };
        col_diff_trace_t t = col_diff_bridge_epoch_to_trace(&orig);
        col_frontier_2d_t back = col_diff_bridge_trace_to_epoch(&t);
        ASSERT_TRUE(back.outer_epoch == orig.outer_epoch,
            "outer_epoch round-trip failed");
        ASSERT_TRUE(back.iteration == orig.iteration,
            "iteration round-trip failed");
    }
    PASS;
}

static void
test_round_trip_uint32_max(void)
{
    TEST("round_trip: (UINT32_MAX, UINT32_MAX) round-trip");
    col_frontier_2d_t orig = { .outer_epoch = UINT32_MAX,
                               .iteration = UINT32_MAX };
    col_diff_trace_t t = col_diff_bridge_epoch_to_trace(&orig);
    col_frontier_2d_t back = col_diff_bridge_trace_to_epoch(&t);
    ASSERT_TRUE(back.outer_epoch == UINT32_MAX, "outer_epoch UINT32_MAX lost");
    ASSERT_TRUE(back.iteration == UINT32_MAX, "iteration UINT32_MAX lost");
    PASS;
}

static void
test_round_trip_zero(void)
{
    TEST("round_trip: (0, 0) round-trip");
    col_frontier_2d_t orig = { .outer_epoch = 0, .iteration = 0 };
    col_diff_trace_t t = col_diff_bridge_epoch_to_trace(&orig);
    col_frontier_2d_t back = col_diff_bridge_trace_to_epoch(&t);
    ASSERT_TRUE(back.outer_epoch == 0, "outer_epoch zero lost");
    ASSERT_TRUE(back.iteration == 0, "iteration zero lost");
    PASS;
}

/* ========================================================================
 * TEST SUITE: Ordering preservation
 * ======================================================================== */

static void
test_ordering_preserved_less(void)
{
    TEST("ordering: a < b in epoch -> a < b in trace");
    col_frontier_2d_t a = { .outer_epoch = 1, .iteration = 0 };
    col_frontier_2d_t b = { .outer_epoch = 2, .iteration = 0 };
    col_diff_trace_t ta = col_diff_bridge_epoch_to_trace(&a);
    col_diff_trace_t tb = col_diff_bridge_epoch_to_trace(&b);
    ASSERT_TRUE(col_diff_trace_compare(&ta, &tb) < 0,
        "trace ordering should be less");
    PASS;
}

static void
test_ordering_preserved_equal(void)
{
    TEST("ordering: a == b in epoch -> a == b in trace");
    col_frontier_2d_t a = { .outer_epoch = 3, .iteration = 7 };
    col_frontier_2d_t b = { .outer_epoch = 3, .iteration = 7 };
    col_diff_trace_t ta = col_diff_bridge_epoch_to_trace(&a);
    col_diff_trace_t tb = col_diff_bridge_epoch_to_trace(&b);
    ASSERT_TRUE(col_diff_trace_compare(&ta, &tb) == 0,
        "trace ordering should be equal");
    PASS;
}

static void
test_ordering_preserved_greater(void)
{
    TEST("ordering: a > b in epoch -> a > b in trace");
    col_frontier_2d_t a = { .outer_epoch = 5, .iteration = 10 };
    col_frontier_2d_t b = { .outer_epoch = 3, .iteration = 99 };
    col_diff_trace_t ta = col_diff_bridge_epoch_to_trace(&a);
    col_diff_trace_t tb = col_diff_bridge_epoch_to_trace(&b);
    ASSERT_TRUE(col_diff_trace_compare(&ta, &tb) > 0,
        "trace ordering should be greater");
    PASS;
}

static void
test_ordering_preserved_same_epoch_diff_iter(void)
{
    TEST("ordering: (1,2) < (1,5) preserved across translation");
    col_frontier_2d_t a = { .outer_epoch = 1, .iteration = 2 };
    col_frontier_2d_t b = { .outer_epoch = 1, .iteration = 5 };
    col_diff_trace_t ta = col_diff_bridge_epoch_to_trace(&a);
    col_diff_trace_t tb = col_diff_bridge_epoch_to_trace(&b);
    ASSERT_TRUE(col_diff_trace_compare(&ta, &tb) < 0,
        "same epoch, lower iter should be less in trace");
    PASS;
}

/* ========================================================================
 * TEST SUITE: ordering_preserved helper function
 * ======================================================================== */

static void
test_ordering_function(void)
{
    TEST(
        "ordering_function: col_diff_bridge_ordering_preserved true for ordered pairs");
    /* less */
    col_frontier_2d_t a1 = { .outer_epoch = 1, .iteration = 0 };
    col_frontier_2d_t b1 = { .outer_epoch = 2, .iteration = 0 };
    ASSERT_TRUE(col_diff_bridge_ordering_preserved(&a1, &b1),
        "less-than pair should return true");
    /* equal */
    col_frontier_2d_t a2 = { .outer_epoch = 4, .iteration = 4 };
    col_frontier_2d_t b2 = { .outer_epoch = 4, .iteration = 4 };
    ASSERT_TRUE(col_diff_bridge_ordering_preserved(&a2, &b2),
        "equal pair should return true");
    /* greater */
    col_frontier_2d_t a3 = { .outer_epoch = 9, .iteration = 1 };
    col_frontier_2d_t b3 = { .outer_epoch = 2, .iteration = 1 };
    ASSERT_TRUE(col_diff_bridge_ordering_preserved(&a3, &b3),
        "greater-than pair should return true");
    PASS;
}

/* ========================================================================
 * TEST SUITE: worker field semantics
 * ======================================================================== */

static void
test_trace_to_epoch_drops_worker(void)
{
    TEST("trace_to_epoch: different worker IDs produce same epoch result");
    col_diff_trace_t t0 = col_diff_trace_init(7, 42, 0);
    col_diff_trace_t t1 = col_diff_trace_init(7, 42, 1);
    col_diff_trace_t t9 = col_diff_trace_init(7, 42, 9);
    col_frontier_2d_t f0 = col_diff_bridge_trace_to_epoch(&t0);
    col_frontier_2d_t f1 = col_diff_bridge_trace_to_epoch(&t1);
    col_frontier_2d_t f9 = col_diff_bridge_trace_to_epoch(&t9);
    ASSERT_TRUE(f0.outer_epoch == f1.outer_epoch &&
        f1.outer_epoch == f9.outer_epoch,
        "outer_epoch should be same regardless of worker");
    ASSERT_TRUE(f0.iteration == f1.iteration && f1.iteration == f9.iteration,
        "iteration should be same regardless of worker");
    PASS;
}

/* ========================================================================
 * TEST SUITE: Sentinel round-trip
 * ======================================================================== */

static void
test_sentinel_round_trip(void)
{
    TEST("sentinel: UINT32_MAX iteration sentinel round-trips correctly");
    col_frontier_2d_t orig = { .outer_epoch = 1, .iteration = UINT32_MAX };
    col_diff_trace_t t = col_diff_bridge_epoch_to_trace(&orig);
    col_frontier_2d_t back = col_diff_bridge_trace_to_epoch(&t);
    ASSERT_TRUE(back.outer_epoch == 1,          "epoch should be 1");
    ASSERT_TRUE(back.iteration == UINT32_MAX,
        "UINT32_MAX sentinel must survive round-trip");
    PASS;
}

/* ========================================================================
 * MAIN
 * ======================================================================== */

int
main(void)
{
    printf("=== Differential Bridge Tests (Issue #262) ===\n\n");

    /* epoch_to_trace */
    test_epoch_to_trace_basic();

    /* trace_to_epoch */
    test_trace_to_epoch_basic();

    /* round-trip identity */
    test_round_trip_identity();
    test_round_trip_uint32_max();
    test_round_trip_zero();

    /* ordering preservation */
    test_ordering_preserved_less();
    test_ordering_preserved_equal();
    test_ordering_preserved_greater();
    test_ordering_preserved_same_epoch_diff_iter();

    /* ordering helper function */
    test_ordering_function();

    /* worker field semantics */
    test_trace_to_epoch_drops_worker();

    /* sentinel round-trip */
    test_sentinel_round_trip();

    printf("\n=== Results: %d passed, %d failed ===\n",
        tests_passed, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
