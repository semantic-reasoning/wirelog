/*
 * test_stratify_scc_bounds.c - SCC detection against out-of-range edges
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Issue #1083.  wl_ir_stratify_scc_detect() builds its adjacency list in two
 * passes.  The counting pass reserved a slot for every edge whose `from` was
 * in range; the fill pass only stored edges whose `from` AND `to` were both
 * in range.  An edge with an in-range `from` and an out-of-range `to`
 * therefore reserved a slot that was never written, while adj_start[] still
 * spanned it -- so the traversal read an indeterminate neighbour index and
 * used it to subscript disc[], low[], on_stack[], stack[], adj_start[] and
 * result->scc_id[].
 *
 * wl_ir_stratify_dep_graph_build() has never emitted an out-of-range
 * endpoint, so the defect is latent through the in-tree producer.  These
 * tests build the graph by hand instead; the documented contract of
 * wl_ir_stratify_scc_detect() (stratify.h) is what licenses that, not the
 * fact that other tests happen to call the function directly.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "../wirelog/ir/stratify.h"

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
        do {                                \
            tests_run++;                    \
            printf("  [TEST] %-55s", name); \
            fflush(stdout);                 \
        } while (0)

#define PASS()             \
        do {                   \
            tests_passed++;    \
            printf(" PASS\n"); \
        } while (0)

#define FAIL(msg)                   \
        do {                            \
            tests_failed++;             \
            printf(" FAIL: %s\n", msg); \
        } while (0)

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * Chain 0 -> 1 -> 2 plus one edge whose `to` is out of range.
 *
 * The out-of-range edge is deliberately hung off node 2, the LAST node of
 * the chain, and MUST NOT be moved to node 0.  The unwritten slot reads back
 * as 0 (the adjacency array is zero-filled), i.e. a phantom edge to node 0.
 * On node 2 that phantom closes the cycle 0 -> 1 -> 2 -> 0 and collapses
 * three components into one, which this test detects.  On node 0 it would be
 * a self-loop, which changes no SCC boundary, and the test would pass on
 * broken code.  Same for any node that already reaches 0.
 */
static void
test_scc_out_of_range_to_is_ignored(void)
{
    TEST("scc: edge with out-of-range 'to' does not join components");

    wl_ir_stratify_dep_edge_t edges[] = {
        { 0, 1, WL_IR_STRATIFY_DEP_POSITIVE },
        { 1, 2, WL_IR_STRATIFY_DEP_POSITIVE },
        { 2, 9, WL_IR_STRATIFY_DEP_POSITIVE }, /* 9 >= relation_count */
    };
    wl_ir_stratify_dep_graph_t graph = {
        .relation_count = 3,
        .relation_map = NULL,
        .edges = edges,
        .edge_count = 3,
        .edge_capacity = 3,
    };

    wl_ir_stratify_scc_result_t *scc = wl_ir_stratify_scc_detect(&graph);
    if (!scc) {
        FAIL("scc_detect returned NULL");
        return;
    }

    if (scc->scc_count != 3) {
        FAIL("expected 3 singleton SCCs");
        wl_ir_stratify_scc_free(scc);
        return;
    }

    if (scc->scc_id[0] == scc->scc_id[1] || scc->scc_id[1] == scc->scc_id[2]
        || scc->scc_id[0] == scc->scc_id[2]) {
        FAIL("nodes were merged into a shared SCC");
        wl_ir_stratify_scc_free(scc);
        return;
    }

    wl_ir_stratify_scc_free(scc);
    PASS();
}

/*
 * Every edge has an out-of-range `from`, so no adjacency slot is reserved at
 * all and the adjacency array has length zero.
 *
 * This case carries NO regression power: it passes both before and after the
 * #1083 fix.  It is kept as an executable witness that the two
 * clang-analyzer-core.NullDereference reports at stratify.c:303 and :338 were
 * false positives -- the total_adj == 0 path really is taken, really does
 * leave the fill loop and the traversal with nothing to dereference, and
 * really does return a well-formed result.
 */
static void
test_scc_out_of_range_from_yields_empty_adjacency(void)
{
    TEST("scc: all edges out of range -> empty adjacency, no crash");

    wl_ir_stratify_dep_edge_t edges[] = {
        { 9, 0, WL_IR_STRATIFY_DEP_POSITIVE }, /* 9 >= relation_count */
    };
    wl_ir_stratify_dep_graph_t graph = {
        .relation_count = 2,
        .relation_map = NULL,
        .edges = edges,
        .edge_count = 1,
        .edge_capacity = 1,
    };

    wl_ir_stratify_scc_result_t *scc = wl_ir_stratify_scc_detect(&graph);
    if (!scc) {
        FAIL("scc_detect returned NULL");
        return;
    }

    if (scc->scc_count != 2) {
        FAIL("expected 2 singleton SCCs");
        wl_ir_stratify_scc_free(scc);
        return;
    }

    if (scc->scc_id[0] == scc->scc_id[1]) {
        FAIL("nodes were merged into a shared SCC");
        wl_ir_stratify_scc_free(scc);
        return;
    }

    wl_ir_stratify_scc_free(scc);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog SCC Edge-Bounds Tests ===\n\n");

    test_scc_out_of_range_to_is_ignored();
    test_scc_out_of_range_from_yields_empty_adjacency();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
        tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
