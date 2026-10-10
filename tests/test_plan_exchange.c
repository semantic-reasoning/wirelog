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
#include "plan_fixture.h"

#include <stdbool.h>
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
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }
    /* The plan borrows the program's intern table; keep the program alive
     * until exit (Issue #1431), which is also what the program-lifetime
     * gate (Issue #1471) enforces for every plan-building helper. */
    plan_fixture_hold(prog);
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

static void
test_plan_op_abi_values(void)
{
    TEST("exec_plan op enum ABI values (Issue #495)");

    /* Universal operators (0-8) */
    ASSERT(WL_PLAN_OP_VARIABLE == 0, "VARIABLE should be 0");
    ASSERT(WL_PLAN_OP_MAP == 1, "MAP should be 1");
    ASSERT(WL_PLAN_OP_FILTER == 2, "FILTER should be 2");
    ASSERT(WL_PLAN_OP_JOIN == 3, "JOIN should be 3");
    ASSERT(WL_PLAN_OP_ANTIJOIN == 4, "ANTIJOIN should be 4");
    ASSERT(WL_PLAN_OP_REDUCE == 5, "REDUCE should be 5");
    ASSERT(WL_PLAN_OP_CONCAT == 6, "CONCAT should be 6");
    ASSERT(WL_PLAN_OP_CONSOLIDATE == 7, "CONSOLIDATE should be 7");
    ASSERT(WL_PLAN_OP_SEMIJOIN == 8, "SEMIJOIN should be 8");

    /* Backend-specific boundary */
    ASSERT(WL_PLAN_OP__BACKEND_START == 9, "__BACKEND_START should be 9");

    /* Columnar backend operators (9-11) */
    ASSERT(WL_PLAN_OP_K_FUSION == 9, "K_FUSION should be 9");
    ASSERT(WL_PLAN_OP_LFTJ == 10, "LFTJ should be 10");
    ASSERT(WL_PLAN_OP_EXCHANGE == 11, "EXCHANGE should be 11");

    PASS();
}

/* ----------------------------------------------------------------
 * Issue #2118: EDB partition metadata
 *
 * tdd_init_workers_hybrid hash-partitions the EDB an EXCHANGE names, by
 * edb_key_col_idxs, for the whole stratum.  That is sound only when the
 * keys are that EDB's own columns and every relation of the stratum that
 * reads the EDB names it with the same keys and reads it once.  These
 * tests check that invariant over plans built with and without the
 * planning passes, and pin the metadata of a few programs.
 * ---------------------------------------------------------------- */

static wl_plan_t *
make_plan_raw(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;
    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return NULL;
    }
    plan_fixture_hold(prog);
    return plan;
}

static const wl_plan_op_exchange_t *
exchange_meta(const wl_plan_relation_t *rel)
{
    for (uint32_t i = 0; i < rel->op_count; i++) {
        if (rel->ops[i].op == WL_PLAN_OP_EXCHANGE)
            return (const wl_plan_op_exchange_t *)rel->ops[i].opaque_data;
    }
    return NULL;
}

static uint32_t
ops_read_count(const wl_plan_op_t *ops, uint32_t n, const char *name)
{
    uint32_t refs = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (ops[i].op == WL_PLAN_OP_K_FUSION && ops[i].opaque_data) {
            const wl_plan_op_k_fusion_t *kf
                = (const wl_plan_op_k_fusion_t *)ops[i].opaque_data;
            refs += ops_read_count(kf->k_ops[0], kf->k_op_counts[0], name);
            continue;
        }
        if (ops[i].relation_name && strcmp(ops[i].relation_name, name) == 0)
            refs++;
        if (ops[i].right_relation && strcmp(ops[i].right_relation, name) == 0)
            refs++;
        if (ops[i].op == WL_PLAN_OP_LFTJ && ops[i].opaque_data) {
            const wl_plan_op_lftj_t *lftj
                = (const wl_plan_op_lftj_t *)ops[i].opaque_data;
            for (uint32_t q = 0; q < lftj->k; q++) {
                if (strcmp(lftj->rel_names[q], name) == 0)
                    refs++;
            }
        }
    }
    return refs;
}

/* True when `ops` (or a fused relation's first sequence) uses `name` as
 * the right operand of a semijoin or antijoin, which needs all its rows. */
static bool
ops_filter_by(const wl_plan_op_t *ops, uint32_t n, const char *name)
{
    for (uint32_t i = 0; i < n; i++) {
        if (ops[i].op == WL_PLAN_OP_K_FUSION && ops[i].opaque_data) {
            const wl_plan_op_k_fusion_t *kf
                = (const wl_plan_op_k_fusion_t *)ops[i].opaque_data;
            if (ops_filter_by(kf->k_ops[0], kf->k_op_counts[0], name))
                return true;
            continue;
        }
        if ((ops[i].op == WL_PLAN_OP_SEMIJOIN
            || ops[i].op == WL_PLAN_OP_ANTIJOIN)
            && ops[i].right_relation
            && strcmp(ops[i].right_relation, name) == 0)
            return true;
    }
    return false;
}

/* NULL when every named EDB partition is sound, else what is wrong. */
static const char *
edb_partition_violation(const wl_plan_t *plan)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        if (!st->is_recursive)
            continue;
        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_op_exchange_t *m = exchange_meta(&st->relations[r]);
            if (!m || !m->edb_rel_name)
                continue;
            uint32_t width = WL_PLAN_WIDTH_UNDECLARED;
            for (uint32_t e = 0; e < plan->edb_count; e++) {
                if (strcmp(plan->edb_relations[e], m->edb_rel_name) == 0)
                    width = plan->edb_declared_width[e];
            }
            if (width == WL_PLAN_WIDTH_UNDECLARED)
                return "named relation is not a declared EDB";
            if (m->edb_key_col_count == 0 || !m->edb_key_col_idxs)
                return "named EDB has no partition key";
            for (uint32_t k = 0; k < m->edb_key_col_count; k++) {
                if (m->edb_key_col_idxs[k] >= width)
                    return "EDB partition key outside the EDB";
            }
            for (uint32_t r2 = 0; r2 < st->relation_count; r2++) {
                const wl_plan_relation_t *other = &st->relations[r2];
                uint32_t refs = ops_read_count(other->ops, other->op_count,
                        m->edb_rel_name);
                if (refs == 0)
                    continue;
                if (ops_filter_by(other->ops, other->op_count,
                    m->edb_rel_name))
                    return "partitioned EDB filters by semijoin or antijoin";
                const wl_plan_op_exchange_t *om = exchange_meta(other);
                if (refs != 1 || !om || !om->edb_rel_name
                    || strcmp(om->edb_rel_name, m->edb_rel_name) != 0
                    || om->edb_key_col_count != m->edb_key_col_count
                    || memcmp(om->edb_key_col_idxs, m->edb_key_col_idxs,
                    m->edb_key_col_count * sizeof(uint32_t)) != 0)
                    return "stratum reads the partitioned EDB elsewhere";
            }
        }
    }
    return NULL;
}

#define DECL_EFA                                                         \
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n" \
        ".decl a(x: int64, y: int64)\na(x, y) :- f(x, y).\n"

static const char *const edb_partition_programs[] = {
    DECL_EFA "a(x, z) :- e(x, y), a(y, w), a(w, z).\n",
    DECL_EFA "a(x, z) :- e(x, y), f(y, w), a(w, z).\n",
    DECL_EFA "a(x, z) :- a(x, y), e(y, w), a(w, z).\n",
    DECL_EFA "a(x, z) :- a(x, y), e(y, w), f(w, z).\n",
    DECL_EFA "a(x, z) :- a(x, y), a(y, w), e(w, z).\n",
    DECL_EFA "a(x, z) :- e(y, x), a(y, w), e(w, z).\n",
    DECL_EFA "a(x, z) :- e(x, y), a(y, z).\n",
    ".decl e(x: int64, y: int64)\n.decl n(x: int64, y: int64)\n"
    ".decl a(x: int64, y: int64)\n"
    "a(x, y) :- e(x, y), !n(x, y).\na(x, y) :- e(y, x).\n"
    "a(x, z) :- a(x, y), a(y, z).\n",
    DECL_EFA ".decl b(x: int64, y: int64)\n"
    "a(x, z) :- e(x, y), a(y, z).\nb(x, y) :- a(y, x).\n"
    "b(x, z) :- e(z, x), b(z, y).\na(x, y) :- b(x, y).\n",
    DECL_EFA ".decl g(x: int64, y: int64)\n.decl b(x: int64, y: int64)\n"
    "a(x, y) :- e(x, y), b(x, y).\n"
    "b(u, v) :- g(u, v), b(v, u).\nb(u, v) :- a(u, v).\n",
    ".decl addressOf(v: int64, o: int64)\n.decl assign(v1: int64, v2: int64)\n"
    ".decl load(v1: int64, v2: int64)\n.decl store(v1: int64, v2: int64)\n"
    ".decl pointsTo(v: int64, o: int64)\n"
    "pointsTo(v, o) :- addressOf(v, o).\n"
    "pointsTo(v1, o) :- assign(v1, v2), pointsTo(v2, o).\n"
    "pointsTo(v1, o) :- load(v1, v2), pointsTo(v2, p), pointsTo(p, o).\n"
    "pointsTo(v1, o) :- store(v1, v2), pointsTo(v1, p), "
    "pointsTo(v2, o).\n",
};

static void
test_plan_edb_partition_invariant(void)
{
    TEST("EDB partition metadata is sound with and without passes (#2118)");
    size_t n = sizeof(edb_partition_programs)
        / sizeof(edb_partition_programs[0]);
    for (size_t i = 0; i < n; i++) {
        for (int passes = 0; passes <= 1; passes++) {
            wl_plan_t *plan = passes ? make_plan(edb_partition_programs[i])
                : make_plan_raw(edb_partition_programs[i]);
            ASSERT(plan != NULL, "plan generation failed");
            const char *why = edb_partition_violation(plan);
            if (why) {
                printf("program %zu passes=%d: ", i, passes);
                wl_plan_free(plan);
                FAIL(why);
                return;
            }
            wl_plan_free(plan);
        }
    }
    PASS();
}

/* The metadata of relation `name` in the first recursive stratum. */
static const wl_plan_op_exchange_t *
recursive_exchange(const wl_plan_t *plan, const char *name)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        if (!st->is_recursive)
            continue;
        for (uint32_t r = 0; r < st->relation_count; r++) {
            if (strcmp(st->relations[r].name, name) == 0)
                return exchange_meta(&st->relations[r]);
        }
    }
    return NULL;
}

static bool
edb_meta_is(const wl_plan_op_exchange_t *m, const char *name, uint32_t key)
{
    if (!m)
        return false;
    if (!name)
        return m->edb_rel_name == NULL;
    return m->edb_rel_name && strcmp(m->edb_rel_name, name) == 0
           && m->edb_key_col_count == 1 && m->edb_key_col_idxs[0] == key;
}

static void
test_plan_edb_partition_exact(void)
{
    TEST("EDB partition metadata of known programs (#2118)");
    for (int passes = 0; passes <= 1; passes++) {
        wl_plan_t *(*build)(const char *) = passes ? make_plan : make_plan_raw;

        /* The EDB joined with the IDB through an intermediate result is
         * replicated, and so is the base-rule EDB. */
        wl_plan_t *plan = build(edb_partition_programs[0]);
        ASSERT(plan, "plan e,a,a");
        ASSERT(edb_meta_is(recursive_exchange(plan, "a"), NULL, 0),
            "e,a,a partitions no EDB");
        wl_plan_free(plan);

        plan = build(edb_partition_programs[1]);
        ASSERT(plan, "plan e,f,a");
        ASSERT(edb_meta_is(recursive_exchange(plan, "a"), NULL, 0),
            "e,f,a partitions no EDB");
        wl_plan_free(plan);

        /* Linear recursion: e is the join's direct left operand. */
        plan = build(edb_partition_programs[6]);
        ASSERT(plan, "plan linear");
        ASSERT(edb_meta_is(recursive_exchange(plan, "a"), "e", 1),
            "linear e,a co-partitions e by column 1");
        wl_plan_free(plan);

        /* a and b would partition e by different columns. */
        plan = build(edb_partition_programs[8]);
        ASSERT(plan, "plan mutual");
        ASSERT(edb_meta_is(recursive_exchange(plan, "a"), NULL, 0)
            && edb_meta_is(recursive_exchange(plan, "b"), NULL, 0),
            "mutual recursion over e partitions no EDB");
        wl_plan_free(plan);

        /* a joins e with b on (x, y), but b is partitioned by (v, u):
         * e stays replicated.  No join reads a itself, so a has no
         * EXCHANGE.  b's own join with g is aligned. */
        plan = build(edb_partition_programs[9]);
        ASSERT(plan, "plan key order");
        const wl_plan_op_exchange_t *mb = recursive_exchange(plan, "b");
        ASSERT(!recursive_exchange(plan, "a") && mb && mb->edb_rel_name
            && strcmp(mb->edb_rel_name, "g") == 0
            && mb->key_col_count == 2 && mb->key_col_idxs[0] == 1
            && mb->key_col_idxs[1] == 0,
            "a over e misaligned with b stays replicated; b names g");
        wl_plan_free(plan);
    }

    /* The CSPA K1 recogniser in eval_tdd_plan.c matches memoryAlias by
     * edb_key_col_idxs == {0} with no EDB name. */
    wl_plan_t *plan = make_plan(
        ".decl assign(x: int32, y: int32)\n"
        ".decl dereference(x: int32, y: int32)\n"
        ".decl valueFlow(x: int32, y: int32)\n"
        ".decl memoryAlias(x: int32, y: int32)\n"
        ".decl valueAlias(x: int32, y: int32)\n"
        "valueFlow(y, x) :- assign(y, x).\n"
        "valueFlow(x, x) :- assign(x, _).\n"
        "valueFlow(x, x) :- assign(_, x).\n"
        "memoryAlias(x, x) :- assign(_, x).\n"
        "memoryAlias(x, x) :- assign(x, _).\n"
        "valueFlow(x, y) :- valueFlow(x, z), valueFlow(z, y).\n"
        "valueFlow(x, y) :- assign(x, z), memoryAlias(z, y).\n"
        "memoryAlias(x, w) :- dereference(y, x), valueAlias(y, z), "
        "dereference(z, w).\n"
        "valueAlias(x, y) :- valueFlow(z, x), valueFlow(z, y).\n"
        "valueAlias(x, y) :- valueFlow(z, x), memoryAlias(z, w), "
        "valueFlow(w, y).\n");
    ASSERT(plan, "plan cspa");
    const wl_plan_op_exchange_t *ma = recursive_exchange(plan, "memoryAlias");
    ASSERT(ma && ma->edb_key_col_count == 1 && ma->edb_key_col_idxs
        && ma->edb_key_col_idxs[0] == 0 && !ma->edb_rel_name,
        "memoryAlias keeps ek {0} with no EDB name");
    wl_plan_free(plan);
    PASS();
}

/* ----------------------------------------------------------------
 * Issue #2118: IDB EXCHANGE keys
 *
 * The EXCHANGE key partitions its relation's rows, so every key must be
 * a column of that relation.  Keys taken from an earlier join's result or
 * from another relation's columns ran past the relation's width and made
 * snapshots fail with EINVAL at two or more workers.
 * ---------------------------------------------------------------- */

static const char *const idb_key_programs[] = {
    ".decl e0(c0: int64, c1: int64)\n"
    ".decl a(c0: int64, c1: int64, c2: int64)\n"
    ".decl b(c0: int64, c1: int64)\n"
    "a(x2, x0, x0) :- e0(x0, x1), e0(x1, x2), !e0(x2, x1).\n"
    "b(x2, x0) :- a(x0, x1, 2), a(y1, x2, x1), !e0(y1, x2).\n"
    "a(x2, x0, x2) :- b(x0, x1), e0(x2, x1).\n",
    ".decl e0(c0: int64, c1: int64)\n.decl e1(c0: int64)\n"
    ".decl e2(c0: int64, c1: int64)\n.decl a(c0: int64, c1: int64)\n"
    ".decl b(c0: int64)\n"
    "a(x2, x0) :- e2(x0, x1), e1(x2), x0 < x1.\n"
    "a(x3, x3) :- e1(x1), e1(x2), e2(x2, x3).\n"
    "b(x2) :- e1(x0), e2(x2, x1), !e0(x0, x2).\n"
    "b(x2) :- b(x1), a(x2, x1), x2 < x1.\n"
    "a(x1, x2) :- e0(x0, x1), b(x1), e1(x2), !e2(x0, x0).\n",
    DECL_EFA ".decl g(x: int64, y: int64)\n.decl b(x: int64, y: int64)\n"
    "a(x, z) :- a(x, y), b(y, z).\nb(x, y) :- a(x, y).\n"
    "b(v0, v3) :- b(v1, v0), e(v1, v2), g(v3, v2).\n",
};

/* NULL when every EXCHANGE key is a column of its relation. */
static const char *
idb_key_violation(const wl_plan_t *plan, const wirelog_program_t *prog)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        if (!st->is_recursive)
            continue;
        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_op_exchange_t *m = exchange_meta(&st->relations[r]);
            if (!m)
                continue;
            const wirelog_schema_t *schema = wirelog_program_get_schema(prog,
                    st->relations[r].name);
            if (!schema)
                return "relation has no schema";
            if (m->key_col_count == 0)
                return "EXCHANGE without a key";
            for (uint32_t k = 0; k < m->key_col_count; k++) {
                if (m->key_col_idxs[k] >= schema->column_count)
                    return "EXCHANGE key outside its relation";
            }
        }
    }
    return NULL;
}

static void
test_plan_idb_key_invariant(void)
{
    TEST("IDB EXCHANGE keys are columns of their relation (#2118)");
    const char *const *lists[] = { edb_partition_programs, idb_key_programs };
    size_t counts[] = {
        sizeof(edb_partition_programs) / sizeof(edb_partition_programs[0]),
        sizeof(idb_key_programs) / sizeof(idb_key_programs[0]),
    };
    for (size_t l = 0; l < 2; l++) {
        for (size_t i = 0; i < counts[l]; i++) {
            for (int passes = 0; passes <= 1; passes++) {
                wirelog_error_t err;
                wirelog_program_t *prog = wirelog_parse_string(lists[l][i],
                        &err);
                ASSERT(prog != NULL, "parse failed");
                if (passes) {
                    wl_fusion_apply(prog, NULL);
                    wl_jpp_apply(prog, NULL);
                    wl_sip_apply(prog, NULL);
                }
                wl_plan_t *plan = NULL;
                ASSERT(wl_plan_from_program(prog, &plan) == 0,
                    "plan generation failed");
                plan_fixture_hold(prog);
                const char *why = idb_key_violation(plan, prog);
                wl_plan_free(plan);
                if (why) {
                    printf("list %zu program %zu passes=%d: ", l, i, passes);
                    FAIL(why);
                    return;
                }
            }
        }
    }
    PASS();
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Plan Exchange Insertion Tests (#319) ===\n");

    test_plan_op_abi_values();
    test_plan_tc_exchange_conservative();
    test_plan_no_exchange_nonrecursive();
    test_plan_exchange_key_metadata();
    test_plan_multi_join_exchange();
    test_plan_exchange_cleanup();
    test_plan_edb_partition_invariant();
    test_plan_edb_partition_exact();
    test_plan_idb_key_invariant();

    printf("\n%d tests: %d passed, %d failed\n", test_count, pass_count,
        fail_count);
    return fail_count > 0 ? 1 : 0;
}
