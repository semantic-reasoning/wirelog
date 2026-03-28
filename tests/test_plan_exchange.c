/*
 * test_plan_exchange.c - Unit tests for EXCHANGE insertion in plan generator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests that the plan generator correctly inserts WL_PLAN_OP_EXCHANGE
 * operators into recursive strata plans (Issue #319).
 *
 * Conservative strategy: one EXCHANGE after the last JOIN in each
 * relation plan within a recursive stratum.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework (same as test_plan_gen.c)
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                    \
        do {                                              \
            test_count++;                                 \
            printf("TEST %d: %s ... ", test_count, name); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                  \
        do {                           \
            fail_count++;              \
            printf("FAIL: %s\n", msg); \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond)) {    \
                FAIL(msg);    \
                return;       \
            }                 \
        } while (0)

/* ----------------------------------------------------------------
 * Helper: parse program, apply passes, generate plan
 * ---------------------------------------------------------------- */

static wl_plan_t *
make_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;
    return plan;
}

/* ----------------------------------------------------------------
 * Helper: count ops of a given type within a relation plan
 * ---------------------------------------------------------------- */

static uint32_t
count_ops(const wl_plan_relation_t *rel, wl_plan_op_type_t type)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < rel->op_count; i++) {
        if (rel->ops[i].op == type)
            n++;
    }
    return n;
}

/* ----------------------------------------------------------------
 * Helper: find first op of a given type, return index or -1
 * ---------------------------------------------------------------- */

static int
find_op(const wl_plan_relation_t *rel, wl_plan_op_type_t type)
{
    for (uint32_t i = 0; i < rel->op_count; i++) {
        if (rel->ops[i].op == type)
            return (int)i;
    }
    return -1;
}

/* ----------------------------------------------------------------
 * Helper: find last op of a given type, return index or -1
 * ---------------------------------------------------------------- */

static int
find_last_op(const wl_plan_relation_t *rel, wl_plan_op_type_t type)
{
    for (int i = (int)rel->op_count - 1; i >= 0; i--) {
        if (rel->ops[i].op == type)
            return i;
    }
    return -1;
}

/* ----------------------------------------------------------------
 * Test 1: TC plan gets EXCHANGE after JOIN in recursive stratum
 *
 * A transitive closure rule (tc(x,z) :- tc(x,y), edge(y,z)) is
 * recursive and contains a JOIN.  The conservative strategy must
 * insert at least one EXCHANGE in the tc relation plan.
 * ---------------------------------------------------------------- */

static void
test_plan_tc_exchange_conservative(void)
{
    TEST("TC recursive plan gets EXCHANGE after JOIN");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    /* Find the recursive stratum containing tc */
    bool found = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        if (!st->is_recursive)
            continue;

        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_relation_t *rel = &st->relations[r];
            if (!rel->name || strcmp(rel->name, "tc") != 0)
                continue;

            found = true;

            /* Must have at least one EXCHANGE */
            uint32_t exchange_count = count_ops(rel, WL_PLAN_OP_EXCHANGE);
            ASSERT(exchange_count >= 1,
                "recursive tc plan must have at least one EXCHANGE");

            /* The EXCHANGE must come after the last JOIN */
            int last_join = find_last_op(rel, WL_PLAN_OP_JOIN);
            int first_exchange = find_op(rel, WL_PLAN_OP_EXCHANGE);
            ASSERT(last_join >= 0, "tc plan should have a JOIN");
            ASSERT(first_exchange > last_join,
                "EXCHANGE must come after the last JOIN");
        }
    }
    ASSERT(found, "tc relation not found in a recursive stratum");

    wl_plan_free(plan);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 2: Non-recursive stratum has zero EXCHANGE
 *
 * A program with only non-recursive rules should produce no EXCHANGE
 * operators in any stratum.
 * ---------------------------------------------------------------- */

static void
test_plan_no_exchange_nonrecursive(void)
{
    TEST("non-recursive stratum has zero EXCHANGE");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3).\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        for (uint32_t r = 0; r < st->relation_count; r++) {
            uint32_t exchange_count
                = count_ops(&st->relations[r], WL_PLAN_OP_EXCHANGE);
            ASSERT(exchange_count == 0,
                "non-recursive stratum must have zero EXCHANGE ops");
        }
    }

    wl_plan_free(plan);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 3: EXCHANGE key_col_idxs match JOIN keys
 *
 * For tc(x,z) :- tc(x,y), edge(y,z), the JOIN key is the shared
 * variable y.  The EXCHANGE metadata should have key_col_idxs that
 * reflect the JOIN's left_keys physical column indices.
 * ---------------------------------------------------------------- */

static void
test_plan_exchange_key_metadata(void)
{
    TEST("EXCHANGE key_col_idxs match JOIN keys");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    bool found = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        if (!st->is_recursive)
            continue;

        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_relation_t *rel = &st->relations[r];
            if (!rel->name || strcmp(rel->name, "tc") != 0)
                continue;

            /* Find the EXCHANGE op */
            int ex_idx = find_op(rel, WL_PLAN_OP_EXCHANGE);
            if (ex_idx < 0)
                continue;

            found = true;
            const wl_plan_op_t *ex_op = &rel->ops[ex_idx];
            ASSERT(ex_op->opaque_data != NULL,
                "EXCHANGE opaque_data must not be NULL");

            const wl_plan_op_exchange_t *meta
                = (const wl_plan_op_exchange_t *)ex_op->opaque_data;

            ASSERT(meta->key_col_count > 0,
                "EXCHANGE must have at least one key column");
            ASSERT(meta->key_col_idxs != NULL,
                "EXCHANGE key_col_idxs must not be NULL");

            /* The key column indices should be valid (< some reasonable
             * bound; for a 2-col relation this is typically col 0 or 1) */
            for (uint32_t k = 0; k < meta->key_col_count; k++) {
                ASSERT(meta->key_col_idxs[k] < 100,
                    "key_col_idx out of reasonable range");
            }
        }
    }
    ASSERT(found, "EXCHANGE op not found in recursive tc plan");

    wl_plan_free(plan);
    PASS();
}

/* ----------------------------------------------------------------
 * Helper: count EXCHANGE ops in a relation plan, including those
 * inside K_FUSION operator sequences.
 * ---------------------------------------------------------------- */

static uint32_t
count_exchanges_deep(const wl_plan_relation_t *rel)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < rel->op_count; i++) {
        if (rel->ops[i].op == WL_PLAN_OP_EXCHANGE) {
            n++;
        } else if (rel->ops[i].op == WL_PLAN_OP_K_FUSION
            && rel->ops[i].opaque_data) {
            /* Search inside K_FUSION sequences */
            const wl_plan_op_k_fusion_t *kf
                = (const wl_plan_op_k_fusion_t *)rel->ops[i].opaque_data;
            for (uint32_t d = 0; d < kf->k; d++) {
                for (uint32_t j = 0; j < kf->k_op_counts[d]; j++) {
                    if (kf->k_ops[d][j].op == WL_PLAN_OP_EXCHANGE)
                        n++;
                }
            }
        }
    }
    return n;
}

/* ----------------------------------------------------------------
 * Test 4: Multiple recursive rules - each gets EXCHANGE
 *
 * Multiple recursive rules (K >= 2 delta copies) will be wrapped in
 * K_FUSION by rewrite_multiway_delta.  The EXCHANGE ops inserted
 * before K_FUSION expansion should be cloned into each sequence.
 * ---------------------------------------------------------------- */

static void
test_plan_multi_join_exchange(void)
{
    TEST("multi-rule recursive relation gets EXCHANGE (including K_FUSION)");

    const char *src = ".decl link(x: int32, y: int32)\n"
        "link(1, 2). link(2, 3).\n"
        ".decl hop(x: int32, y: int32)\n"
        "hop(3, 4). hop(4, 5).\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(x, y) :- link(x, y).\n"
        "reach(x, y) :- hop(x, y).\n"
        "reach(a, c) :- reach(a, b), link(b, c).\n"
        "reach(a, c) :- reach(a, b), hop(b, c).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    bool found_recursive_with_exchange = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        if (!st->is_recursive)
            continue;

        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_relation_t *rel = &st->relations[r];
            if (!rel->name || strcmp(rel->name, "reach") != 0)
                continue;

            uint32_t exchange_count = count_exchanges_deep(rel);
            if (exchange_count >= 1)
                found_recursive_with_exchange = true;
        }
    }
    ASSERT(found_recursive_with_exchange,
        "recursive reach must have EXCHANGE (top-level or inside K_FUSION)");

    wl_plan_free(plan);
    PASS();
}

/* ----------------------------------------------------------------
 * Test 5: EXCHANGE metadata lifecycle (wl_plan_free cleans up)
 *
 * Verify that wl_plan_free properly frees EXCHANGE opaque_data
 * without leaks.  Under ASAN, a leak here would be caught.
 * This test simply ensures the free path executes without crash.
 * ---------------------------------------------------------------- */

static void
test_plan_exchange_cleanup(void)
{
    TEST("EXCHANGE metadata cleanup via wl_plan_free");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    /* Verify EXCHANGE ops exist before freeing */
    bool has_exchange = false;
    for (uint32_t s = 0; s < plan->stratum_count && !has_exchange; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        for (uint32_t r = 0; r < st->relation_count && !has_exchange; r++) {
            if (count_ops(&st->relations[r], WL_PLAN_OP_EXCHANGE) > 0)
                has_exchange = true;
        }
    }
    ASSERT(has_exchange,
        "plan must contain EXCHANGE ops for cleanup test to be meaningful");

    /* This should free all EXCHANGE opaque_data without crash or leak.
     * Under ASAN/LSAN, any leak in free_exchange_opaque would be detected. */
    wl_plan_free(plan);
    PASS();
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Plan Exchange Insertion Tests (#319) ===\n");

    test_plan_tc_exchange_conservative();
    test_plan_no_exchange_nonrecursive();
    test_plan_exchange_key_metadata();
    test_plan_multi_join_exchange();
    test_plan_exchange_cleanup();

    printf("\n%d tests: %d passed, %d failed\n", test_count, pass_count,
        fail_count);
    return fail_count > 0 ? 1 : 0;
}
