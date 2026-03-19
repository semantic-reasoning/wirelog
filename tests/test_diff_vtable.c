/*
 * test_diff_vtable.c - Differential frontier vtable tests (Issue #262)
 *
 * Tests the col_frontier_diff_ops vtable implementation. Key behavioral
 * differences from the epoch vtable:
 *   - skip functions are epoch-independent (no outer_epoch check)
 *   - reset preserves iteration (does not set UINT32_MAX)
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
 * Helper: create a zeroed mock session with diff vtable wired
 * ======================================================================== */

static wl_col_session_t *
make_mock_session(void)
{
    wl_col_session_t *s = calloc(1, sizeof(*s));
    s->frontier_ops = &col_frontier_diff_ops;
    return s;
}

/* ========================================================================
 * TEST SUITE: Vtable Wiring
 * ======================================================================== */

static void
test_diff_vtable_not_null(void)
{
    TEST("diff vtable has all 7 methods non-NULL");
    const col_frontier_ops_t *ops = &col_frontier_diff_ops;
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
test_diff_skip_iteration_valid_beyond(void)
{
    TEST("skip_iteration: valid frontier, eff_iter beyond -> true");
    wl_col_session_t *s = make_mock_session();
    s->frontiers[0].iteration = 5;
    bool result = s->frontier_ops->should_skip_iteration(s, 0, 6);
    ASSERT_TRUE(result == true, "expected skip");
    PASS;
    free(s);
}

static void
test_diff_skip_iteration_at_convergence(void)
{
    TEST("skip_iteration: eff_iter == frontier.iteration -> false");
    wl_col_session_t *s = make_mock_session();
    s->frontiers[0].iteration = 5;
    bool result = s->frontier_ops->should_skip_iteration(s, 0, 5);
    ASSERT_TRUE(result == false, "should not skip at convergence point");
    PASS;
    free(s);
}

static void
test_diff_skip_iteration_invalid(void)
{
    TEST("skip_iteration: iteration==UINT32_MAX -> false (not set)");
    wl_col_session_t *s = make_mock_session();
    /* calloc gives 0; init_stratum converts 0 -> UINT32_MAX */
    s->frontiers[0].iteration = UINT32_MAX;
    bool result = s->frontier_ops->should_skip_iteration(s, 0, 4);
    ASSERT_TRUE(result == false, "UINT32_MAX iteration must not skip");
    PASS;
    free(s);
}

static void
test_diff_skip_iteration_oob(void)
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
test_diff_skip_rule_valid_beyond(void)
{
    TEST("skip_rule: valid frontier, eff_iter beyond -> true");
    wl_col_session_t *s = make_mock_session();
    s->rule_frontiers[7].iteration = 10;
    bool result = s->frontier_ops->should_skip_rule(s, 7, 11);
    ASSERT_TRUE(result == true, "expected skip");
    PASS;
    free(s);
}

static void
test_diff_skip_rule_invalid(void)
{
    TEST("skip_rule: iteration==UINT32_MAX -> false (not set)");
    wl_col_session_t *s = make_mock_session();
    s->rule_frontiers[7].iteration = UINT32_MAX;
    bool result = s->frontier_ops->should_skip_rule(s, 7, 5);
    ASSERT_TRUE(result == false, "UINT32_MAX iteration must not skip");
    PASS;
    free(s);
}

static void
test_diff_skip_rule_oob(void)
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
test_diff_record_stratum_convergence(void)
{
    TEST("record_stratum_convergence sets both epoch and iteration");
    wl_col_session_t *s = make_mock_session();
    s->frontier_ops->record_stratum_convergence(s, 2, 5, 42);
    ASSERT_TRUE(s->frontiers[2].outer_epoch == 5, "epoch mismatch");
    ASSERT_TRUE(s->frontiers[2].iteration == 42, "iteration mismatch");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: reset_stratum_frontier (KEY: preserves iteration)
 * ======================================================================== */

static void
test_diff_reset_stratum_preserves_iteration(void)
{
    TEST("reset_stratum_frontier: sets epoch but preserves iteration");
    wl_col_session_t *s = make_mock_session();
    s->frontiers[1].outer_epoch = 0;
    s->frontiers[1].iteration = 7;
    s->frontier_ops->reset_stratum_frontier(s, 1, 3);
    ASSERT_TRUE(s->frontiers[1].outer_epoch == 3, "epoch not updated");
    ASSERT_TRUE(s->frontiers[1].iteration == 7,
        "iteration must be preserved (diff semantics)");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: reset_rule_frontier (KEY: preserves iteration)
 * ======================================================================== */

static void
test_diff_reset_rule_preserves_iteration(void)
{
    TEST("reset_rule_frontier: sets epoch but preserves iteration");
    wl_col_session_t *s = make_mock_session();
    s->rule_frontiers[3].outer_epoch = 0;
    s->rule_frontiers[3].iteration = 99;
    s->frontier_ops->reset_rule_frontier(s, 3, 4);
    ASSERT_TRUE(s->rule_frontiers[3].outer_epoch == 4, "epoch not updated");
    ASSERT_TRUE(s->rule_frontiers[3].iteration == 99,
        "iteration must be preserved (diff semantics)");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: init_stratum
 * ======================================================================== */

static void
test_diff_init_stratum_from_zero(void)
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
test_diff_init_stratum_preserves_nonzero(void)
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
 * TEST SUITE: Cross-epoch skip (epoch-independent behavior)
 * ======================================================================== */

static void
test_diff_cross_epoch_skip(void)
{
    TEST("skip fires across epoch boundary (epoch-independent)");
    wl_col_session_t *s = make_mock_session();
    s->outer_epoch = 1;
    /* Record convergence at epoch=1, iter=5 */
    s->frontier_ops->record_stratum_convergence(s, 0, 1, 5);
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 6) == true,
        "skip same epoch");
    /* Advance to epoch=2 — diff vtable has no epoch guard, skip persists */
    s->outer_epoch = 2;
    ASSERT_TRUE(s->frontier_ops->should_skip_iteration(s, 0, 6) == true,
        "skip must persist across epoch (diff is epoch-independent)");
    PASS;
    free(s);
}

/* ========================================================================
 * TEST SUITE: Diff vs epoch reset behavior comparison
 * ======================================================================== */

static void
test_diff_vs_epoch_reset_behavior(void)
{
    TEST("diff reset preserves skip; epoch reset destroys it");
    wl_col_session_t *epoch_s = calloc(1, sizeof(*epoch_s));
    epoch_s->frontier_ops = &col_frontier_epoch_ops;

    wl_col_session_t *diff_s = make_mock_session();

    /* Set identical frontier state on both */
    epoch_s->outer_epoch = 1;
    epoch_s->frontier_ops->record_stratum_convergence(epoch_s, 0, 1, 5);

    diff_s->outer_epoch = 1;
    diff_s->frontier_ops->record_stratum_convergence(diff_s, 0, 1, 5);

    /* Both skip before reset */
    ASSERT_TRUE(epoch_s->frontier_ops->should_skip_iteration(epoch_s, 0, 6),
        "epoch: skip before reset");
    ASSERT_TRUE(diff_s->frontier_ops->should_skip_iteration(diff_s, 0, 6),
        "diff: skip before reset");

    /* Reset both with same new epoch */
    epoch_s->frontier_ops->reset_stratum_frontier(epoch_s, 0, 1);
    diff_s->frontier_ops->reset_stratum_frontier(diff_s, 0, 1);

    /* Epoch vtable: reset sets iteration=UINT32_MAX -> skip clears */
    ASSERT_TRUE(
        epoch_s->frontier_ops->should_skip_iteration(epoch_s, 0, 6) == false,
        "epoch: reset must clear skip (UINT32_MAX sentinel)");

    /* Diff vtable: reset preserves iteration -> skip persists */
    ASSERT_TRUE(
        diff_s->frontier_ops->should_skip_iteration(diff_s, 0, 6) == true,
        "diff: reset must preserve skip (iteration retained)");

    PASS;
    free(epoch_s);
    free(diff_s);
}

/* ========================================================================
 * MAIN
 * ======================================================================== */

int
main(void)
{
    printf("=== Differential Frontier Vtable Tests (Issue #262) ===\n\n");

    /* Vtable wiring */
    test_diff_vtable_not_null();

    /* should_skip_iteration */
    test_diff_skip_iteration_valid_beyond();
    test_diff_skip_iteration_at_convergence();
    test_diff_skip_iteration_invalid();
    test_diff_skip_iteration_oob();

    /* should_skip_rule */
    test_diff_skip_rule_valid_beyond();
    test_diff_skip_rule_invalid();
    test_diff_skip_rule_oob();

    /* record_stratum_convergence */
    test_diff_record_stratum_convergence();

    /* reset (KEY: preserves iteration) */
    test_diff_reset_stratum_preserves_iteration();
    test_diff_reset_rule_preserves_iteration();

    /* init_stratum */
    test_diff_init_stratum_from_zero();
    test_diff_init_stratum_preserves_nonzero();

    /* cross-epoch and comparison */
    test_diff_cross_epoch_skip();
    test_diff_vs_epoch_reset_behavior();

    printf("\n=== Results: %d passed, %d failed ===\n",
        tests_passed, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
