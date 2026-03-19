/*
 * test_frontier_vtable.c - Frontier vtable interface tests (Issue #261)
 *
 * Tests the col_frontier_ops_t vtable abstraction and the epoch-based
 * implementation (col_frontier_epoch_ops). Verifies each vtable method
 * produces correct results and that the epoch-based model matches
 * the pre-refactor direct-access behavior.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "../wirelog/columnar/internal.h"

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
 * Helper: create a zeroed mock session with vtable wired
 * ======================================================================== */

static wl_col_session_t *
make_mock_session(void)
{
    wl_col_session_t *s = calloc(1, sizeof(*s));
    s->frontier_ops = &col_frontier_epoch_ops;
    return s;
}

/* ========================================================================
 * TEST SUITE: Vtable Wiring
 * ======================================================================== */

static void
test_vtable_not_null(void)
{
    TEST("epoch vtable has all methods non-NULL");
    const col_frontier_ops_t *ops = &col_frontier_epoch_ops;
    ASSERT_TRUE(ops->should_skip_iteration != NULL,
        "should_skip_iteration NULL");
    ASSERT_TRUE(ops->should_skip_rule != NULL, "should_skip_rule NULL");
    ASSERT_TRUE(ops->record_stratum_convergence != NULL,
        "record_stratum_convergence NULL");
    ASSERT_TRUE(ops->record_rule_convergence != NULL,
        "record_rule_convergence NULL");
    ASSERT_TRUE(ops->reset_stratum_frontier != NULL,
        "reset_stratum_frontier NULL");
    ASSERT_TRUE(ops->reset_rule_frontier != NULL, "reset_rule_frontier NULL");
    ASSERT_TRUE(ops->init_stratum != NULL, "init_stratum NULL");
    PASS;
}

/* ========================================================================
 * TEST SUITE: should_skip_iteration
 * ======================================================================== */

static void
test_skip_iteration_same_epoch_beyond(void)
{
    TEST("skip_iteration: same epoch, beyond convergence -> true");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 5;
    s->frontiers[0].outer_epoch = 5;
    s->frontiers[0].iteration = 3;
    bool result = s->frontier_ops->should_skip_iteration(s, 0, 4);
    ASSERT_TRUE(result == true, "expected skip");
    PASS;
    free(s);
}

static void
test_skip_iteration_same_epoch_at_convergence(void)
{
    TEST("skip_iteration: same epoch, at convergence -> false");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 5;
    s->frontiers[0].outer_epoch = 5;
    s->frontiers[0].iteration = 3;
    bool result = s->frontier_ops->should_skip_iteration(s, 0, 3);
    ASSERT_TRUE(result == false, "should not skip at convergence point");
    PASS;
    free(s);
}

static void
test_skip_iteration_different_epoch(void)
{
    TEST("skip_iteration: different epoch -> false");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 6;
    s->frontiers[0].outer_epoch = 5;
    s->frontiers[0].iteration = 3;
    bool result = s->frontier_ops->should_skip_iteration(s, 0, 4);
    ASSERT_TRUE(result == false, "different epoch must not skip");
    PASS;
    free(s);
}

static void
test_skip_iteration_out_of_bounds(void)
{
    TEST("skip_iteration: stratum >= MAX_STRATA -> false");
    wl_col_session_t *s = make_mock_session();
    bool result = s->frontier_ops->should_skip_iteration(s, MAX_STRATA, 0);
    ASSERT_TRUE(result == false, "out-of-bounds must return false");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: should_skip_rule
 * ======================================================================== */

static void
test_skip_rule_same_epoch_beyond(void)
{
    TEST("skip_rule: same epoch, beyond convergence -> true");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 2;
    s->rule_frontiers[7].outer_epoch = 2;
    s->rule_frontiers[7].iteration = 10;
    bool result = s->frontier_ops->should_skip_rule(s, 7, 11);
    ASSERT_TRUE(result == true, "expected skip");
    PASS;
    free(s);
}

static void
test_skip_rule_different_epoch(void)
{
    TEST("skip_rule: different epoch -> false");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 3;
    s->rule_frontiers[7].outer_epoch = 2;
    s->rule_frontiers[7].iteration = 10;
    bool result = s->frontier_ops->should_skip_rule(s, 7, 11);
    ASSERT_TRUE(result == false, "different epoch must not skip");
    PASS;
    free(s);
}

static void
test_skip_rule_out_of_bounds(void)
{
    TEST("skip_rule: rule >= MAX_RULES -> false");
    wl_col_session_t *s = make_mock_session();
    bool result = s->frontier_ops->should_skip_rule(s, MAX_RULES, 0);
    ASSERT_TRUE(result == false, "out-of-bounds must return false");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: record_stratum_convergence
 * ======================================================================== */

static void
test_record_stratum_convergence(void)
{
    TEST("record_stratum_convergence sets epoch and iteration");
    wl_col_session_t *s = make_mock_session();
    s->frontier_ops->record_stratum_convergence(s, 2, 5, 42);
    ASSERT_TRUE(s->frontiers[2].outer_epoch == 5, "epoch mismatch");
    ASSERT_TRUE(s->frontiers[2].iteration == 42, "iteration mismatch");
    PASS;
    free(s);
}

static void
test_record_stratum_convergence_oob(void)
{
    TEST("record_stratum_convergence: out-of-bounds is no-op");
    wl_col_session_t *s = make_mock_session();
    s->frontier_ops->record_stratum_convergence(s, MAX_STRATA, 1, 1);
    /* Should not crash */
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: record_rule_convergence
 * ======================================================================== */

static void
test_record_rule_convergence(void)
{
    TEST("record_rule_convergence sets epoch and iteration");
    wl_col_session_t *s = make_mock_session();
    s->frontier_ops->record_rule_convergence(s, 10, 3, 99);
    ASSERT_TRUE(s->rule_frontiers[10].outer_epoch == 3, "epoch mismatch");
    ASSERT_TRUE(s->rule_frontiers[10].iteration == 99, "iteration mismatch");
    PASS;
    free(s);
}

static void
test_record_rule_convergence_oob(void)
{
    TEST("record_rule_convergence: out-of-bounds is no-op");
    wl_col_session_t *s = make_mock_session();
    s->frontier_ops->record_rule_convergence(s, MAX_RULES, 1, 1);
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: reset_stratum_frontier
 * ======================================================================== */

static void
test_reset_stratum_frontier(void)
{
    TEST("reset_stratum_frontier sets epoch and UINT32_MAX iteration");
    wl_col_session_t *s = make_mock_session();
    s->frontiers[1].outer_epoch = 0;
    s->frontiers[1].iteration = 5;
    s->frontier_ops->reset_stratum_frontier(s, 1, 7);
    ASSERT_TRUE(s->frontiers[1].outer_epoch == 7, "epoch mismatch");
    ASSERT_TRUE(s->frontiers[1].iteration == UINT32_MAX,
        "iteration should be UINT32_MAX");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: reset_rule_frontier
 * ======================================================================== */

static void
test_reset_rule_frontier(void)
{
    TEST("reset_rule_frontier sets epoch and UINT32_MAX iteration");
    wl_col_session_t *s = make_mock_session();
    s->rule_frontiers[3].outer_epoch = 0;
    s->rule_frontiers[3].iteration = 5;
    s->frontier_ops->reset_rule_frontier(s, 3, 4);
    ASSERT_TRUE(s->rule_frontiers[3].outer_epoch == 4, "epoch mismatch");
    ASSERT_TRUE(s->rule_frontiers[3].iteration == UINT32_MAX,
        "iteration should be UINT32_MAX");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: init_stratum
 * ======================================================================== */

static void
test_init_stratum_from_zero(void)
{
    TEST("init_stratum: iteration==0 -> set to UINT32_MAX");
    wl_col_session_t *s = make_mock_session();
    /* calloc zeroes everything, so frontiers[0].iteration == 0 */
    s->frontier_ops->init_stratum(s, 0);
    ASSERT_TRUE(s->frontiers[0].iteration == UINT32_MAX,
        "should set UINT32_MAX");
    PASS;
    free(s);
}

static void
test_init_stratum_preserves_nonzero(void)
{
    TEST("init_stratum: iteration!=0 -> preserve value");
    wl_col_session_t *s = make_mock_session();
    s->frontiers[0].iteration = 42;
    s->frontier_ops->init_stratum(s, 0);
    ASSERT_TRUE(s->frontiers[0].iteration == 42,
        "should preserve existing value");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: Round-trip integration
 * ======================================================================== */

static void
test_record_then_skip(void)
{
    TEST("record convergence then skip fires correctly");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 1;
    s->frontier_ops->record_stratum_convergence(s, 0, 1, 5);
    /* Same epoch, eff_iter=6 > 5 -> should skip */
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 6) == true,
        "skip after record");
    /* eff_iter=5 == 5 -> should NOT skip */
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 5) == false,
        "no skip at convergence");
    PASS;
    free(s);
}

static void
test_reset_clears_skip(void)
{
    TEST("reset frontier clears skip condition");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 1;
    s->frontier_ops->record_stratum_convergence(s, 0, 1, 5);
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 6) == true,
        "skip before reset");
    /* Reset sets iteration to UINT32_MAX -> nothing can be beyond it */
    s->frontier_ops->reset_stratum_frontier(s, 0, 1);
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 6) == false,
        "no skip after reset");
    PASS;
    free(s);
}

static void
test_epoch_change_clears_skip(void)
{
    TEST("epoch change clears skip condition");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 1;
    s->frontier_ops->record_stratum_convergence(s, 0, 1, 5);
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 6) == true,
        "skip same epoch");
    /* New epoch -> frontier epoch doesn't match -> no skip */
    s->outer_epoch = 2;
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 6) == false,
        "no skip new epoch");
    PASS;
    free(s);
}

/* ========================================================================
 * MAIN
 * ======================================================================== */

int
main(void)
{
    printf("=== Frontier Vtable Tests (Issue #261) ===\n\n");

    /* Vtable wiring */
    test_vtable_not_null();

    /* should_skip_iteration */
    test_skip_iteration_same_epoch_beyond();
    test_skip_iteration_same_epoch_at_convergence();
    test_skip_iteration_different_epoch();
    test_skip_iteration_out_of_bounds();

    /* should_skip_rule */
    test_skip_rule_same_epoch_beyond();
    test_skip_rule_different_epoch();
    test_skip_rule_out_of_bounds();

    /* record convergence */
    test_record_stratum_convergence();
    test_record_stratum_convergence_oob();
    test_record_rule_convergence();
    test_record_rule_convergence_oob();

    /* reset */
    test_reset_stratum_frontier();
    test_reset_rule_frontier();

    /* init_stratum */
    test_init_stratum_from_zero();
    test_init_stratum_preserves_nonzero();

    /* round-trip integration */
    test_record_then_skip();
    test_reset_clears_skip();
    test_epoch_change_clears_skip();

    printf("\n=== Results: %d passed, %d failed ===\n",
        tests_passed, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
