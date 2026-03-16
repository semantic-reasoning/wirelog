/*
 * test_sip.c - Tests for Semijoin Information Passing Optimization
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/wirelog-parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
    do {                                      \
        tests_run++;                          \
        printf("  [%d] %s", tests_run, name); \
    } while (0)

#define PASS()                 \
    do {                       \
        tests_passed++;        \
        printf(" ... PASS\n"); \
    } while (0)

#define FAIL(msg)                         \
    do {                                  \
        tests_failed++;                   \
        printf(" ... FAIL: %s\n", (msg)); \
    } while (0)

/* ======================================================================== */
/* Helper: find relation IR by name                                         */
/* ======================================================================== */

static wirelog_ir_node_t *
find_relation_ir(struct wirelog_program *prog, const char *name)
{
    if (!prog || !prog->relation_irs)
        return NULL;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, name) == 0) {
            return prog->relation_irs[i];
        }
    }
    return NULL;
}

/* ======================================================================== */
/* Helper: count nodes of a specific type in an IR tree                     */
/* ======================================================================== */

static uint32_t
count_type_in_tree(const wirelog_ir_node_t *node, wirelog_ir_node_type_t type)
{
    if (!node)
        return 0;
    uint32_t count = (node->type == type) ? 1 : 0;
    for (uint32_t i = 0; i < node->child_count; i++) {
        count += count_type_in_tree(node->children[i], type);
    }
    return count;
}

/* ======================================================================== */
/* Helper: find the first SEMIJOIN in a tree                                */
/* ======================================================================== */

static wirelog_ir_node_t *
find_first_semijoin(wirelog_ir_node_t *node)
{
    if (!node)
        return NULL;
    if (node->type == WIRELOG_IR_SEMIJOIN)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *found = find_first_semijoin(node->children[i]);
        if (found)
            return found;
    }
    return NULL;
}

/* ======================================================================== */
/* Error Handling Tests                                                     */
/* ======================================================================== */

static void
test_sip_null_program(void)
{
    TEST("sip: NULL program returns -2");

    wl_sip_stats_t stats = { 0, 0 };
    int rc = wl_sip_apply(NULL, &stats);

    if (rc != -2) {
        FAIL("expected -2");
        return;
    }
    PASS();
}

static void
test_sip_null_stats(void)
{
    TEST("sip: NULL stats works (no crash)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(x: int32, y: int32)\n"
                               "edge(1, 2).\n",
                               &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    int rc = wl_sip_apply(prog, NULL);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_sip_no_ir_trees(void)
{
    TEST("sip: program with only EDB returns 0 with zero stats");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(x: int32, y: int32)\n"
                               "edge(1, 2).\n",
                               &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_sip_stats_t stats = { 99, 99 };
    int rc = wl_sip_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.semijoins_inserted != 0 || stats.chains_examined != 0) {
        FAIL("expected all stats = 0 for EDB-only program");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* No-op Tests                                                              */
/* ======================================================================== */

static void
test_sip_two_atom_noop(void)
{
    TEST("sip: non-recursive 2-atom rule gets no SEMIJOIN");

    /*
     * A plain 2-atom rule with no self-recursion: neither standard SIP
     * (depth < 2) nor demand-driven filtering (no recursive relation)
     * should fire.
     */
    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, z: int32)\n"
                               ".decl out(x: int32, z: int32)\n"
                               "out(x, z) :- a(x, y), b(y, z).\n",
                               &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_sip_stats_t stats = { 0, 0, 0 };
    int rc = wl_sip_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.semijoins_inserted != 0) {
        FAIL("non-recursive 2-atom rule should not have standard semijoins");
        wirelog_program_free(prog);
        return;
    }

    if (stats.demand_semijoins_inserted != 0) {
        FAIL("non-recursive 2-atom rule should not have demand semijoins");
        wirelog_program_free(prog);
        return;
    }

    /* Verify no SEMIJOIN nodes exist */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "out");
    if (!ir) {
        FAIL("no IR for 'out'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t sj_count = count_type_in_tree(ir, WIRELOG_IR_SEMIJOIN);
    if (sj_count != 0) {
        FAIL("expected 0 SEMIJOINs for non-recursive 2-atom rule");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Core SIP Tests                                                           */
/* ======================================================================== */

static void
test_sip_three_atom_semijoin(void)
{
    TEST("sip: 3-atom chain gets 1 SEMIJOIN inserted");

    /*
     * path(x, z) :- a(x, y), b(y, w), c(w, z).
     *
     * Before SIP:
     *   PROJECT
     *     JOIN(key=w)
     *       JOIN(key=y)
     *         SCAN(a)
     *         SCAN(b)
     *       SCAN(c)
     *
     * After SIP:
     *   PROJECT
     *     JOIN(key=w)
     *       SEMIJOIN(key=w, right=c)
     *         JOIN(key=y)
     *           SCAN(a)
     *           SCAN(b)
     *         SCAN(c) [clone]
     *       SCAN(c)
     */
    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, w: int32)\n"
                               ".decl c(w: int32, z: int32)\n"
                               ".decl path(x: int32, z: int32)\n"
                               "path(x, z) :- a(x, y), b(y, w), c(w, z).\n",
                               &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_sip_stats_t stats = { 0, 0 };
    int rc = wl_sip_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    /* Should have inserted 1 SEMIJOIN */
    if (stats.semijoins_inserted != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected semijoins_inserted=1, got %u",
                 stats.semijoins_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    if (stats.chains_examined != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected chains_examined=1, got %u",
                 stats.chains_examined);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify IR has exactly 1 SEMIJOIN node */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    if (!ir) {
        FAIL("no IR for 'path'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t sj_count = count_type_in_tree(ir, WIRELOG_IR_SEMIJOIN);
    if (sj_count != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 1 SEMIJOIN in tree, got %u",
                 sj_count);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify the SEMIJOIN has the right structure:
     * - child[0] should be a JOIN
     * - child[1] should be a SCAN
     * - join_key_count should be > 0
     */
    wirelog_ir_node_t *sj = find_first_semijoin(ir);
    if (!sj) {
        FAIL("SEMIJOIN not found in tree");
        wirelog_program_free(prog);
        return;
    }

    if (sj->child_count != 2) {
        FAIL("SEMIJOIN should have 2 children");
        wirelog_program_free(prog);
        return;
    }

    if (sj->children[0]->type != WIRELOG_IR_JOIN) {
        FAIL("SEMIJOIN child[0] should be JOIN");
        wirelog_program_free(prog);
        return;
    }

    if (sj->children[1]->type != WIRELOG_IR_SCAN) {
        FAIL("SEMIJOIN child[1] should be SCAN");
        wirelog_program_free(prog);
        return;
    }

    if (sj->join_key_count == 0) {
        FAIL("SEMIJOIN should have join keys");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_sip_four_atom_semijoin(void)
{
    TEST("sip: 4-atom chain gets 2 SEMIJOINs inserted");

    /*
     * out(x, v) :- a(x, y), b(y, w), c(w, u), d(u, v).
     *
     * After SIP (2 SEMIJOINs for 3 JOINs):
     *   JOIN(key=u)
     *     SEMIJOIN(key=u, right=d)
     *       JOIN(key=w)
     *         SEMIJOIN(key=w, right=c)
     *           JOIN(key=y)
     *             SCAN(a)
     *             SCAN(b)
     *           SCAN(c) [clone]
     *         SCAN(c)
     *       SCAN(d) [clone]
     *     SCAN(d)
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, w: int32)\n"
        ".decl c(w: int32, u: int32)\n"
        ".decl d(u: int32, v: int32)\n"
        ".decl out(x: int32, v: int32)\n"
        "out(x, v) :- a(x, y), b(y, w), c(w, u), d(u, v).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_sip_stats_t stats = { 0, 0 };
    int rc = wl_sip_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.semijoins_inserted != 2) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected semijoins_inserted=2, got %u",
                 stats.semijoins_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify IR has exactly 2 SEMIJOIN nodes */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "out");
    if (!ir) {
        FAIL("no IR for 'out'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t sj_count = count_type_in_tree(ir, WIRELOG_IR_SEMIJOIN);
    if (sj_count != 2) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 2 SEMIJOINs in tree, got %u",
                 sj_count);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* JOINs should still be preserved (3 total) */
    uint32_t join_count = count_type_in_tree(ir, WIRELOG_IR_JOIN);
    if (join_count != 3) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 3 JOINs preserved, got %u",
                 join_count);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* UNION and ANTIJOIN Tests                                                 */
/* ======================================================================== */

static void
test_sip_union_recurse(void)
{
    TEST("sip: recurses into UNION children");

    /*
     * Two 3-atom rules for 'path':
     *   path(x, z) :- a(x, y), b(y, w), c(w, z).
     *   path(x, z) :- a(x, y), b(y, w), c(w, z).
     *
     * The relation IR is UNION(rule1, rule2).
     * Each UNION child should get a SEMIJOIN inserted.
     */
    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, w: int32)\n"
                               ".decl c(w: int32, z: int32)\n"
                               ".decl d(w: int32, z: int32)\n"
                               ".decl path(x: int32, z: int32)\n"
                               "path(x, z) :- a(x, y), b(y, w), c(w, z).\n"
                               "path(x, z) :- a(x, y), b(y, w), d(w, z).\n",
                               &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* Verify UNION structure */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    if (!ir || ir->type != WIRELOG_IR_UNION) {
        FAIL("expected UNION IR for 'path' with two rules");
        wirelog_program_free(prog);
        return;
    }

    wl_sip_stats_t stats = { 0, 0 };
    int rc = wl_sip_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.semijoins_inserted != 2) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected semijoins_inserted=2, got %u",
                 stats.semijoins_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    if (stats.chains_examined != 2) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected chains_examined=2, got %u",
                 stats.chains_examined);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify each UNION child has 1 SEMIJOIN */
    ir = find_relation_ir(prog, "path");
    for (uint32_t i = 0; i < ir->child_count; i++) {
        uint32_t sj_count
            = count_type_in_tree(ir->children[i], WIRELOG_IR_SEMIJOIN);
        if (sj_count != 1) {
            char buf[80];
            snprintf(buf, sizeof(buf),
                     "expected 1 SEMIJOIN in UNION child %u, got %u", i,
                     sj_count);
            FAIL(buf);
            wirelog_program_free(prog);
            return;
        }
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_sip_antijoin_preserved(void)
{
    TEST("sip: ANTIJOIN wrapper preserved, SEMIJOIN inserted in chain");

    /*
     * path(x, z) :- a(x, y), b(y, w), c(w, z), !neg(x, z).
     *
     * IR structure (before SIP):
     *   PROJECT(ANTIJOIN(JOIN(JOIN(SCAN(a), SCAN(b)), SCAN(c)), SCAN(neg)))
     *
     * After SIP:
     *   - ANTIJOIN preserved
     *   - 1 SEMIJOIN inserted in the 3-atom join chain
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, w: int32)\n"
        ".decl c(w: int32, z: int32)\n"
        ".decl neg(x: int32, z: int32)\n"
        ".decl path(x: int32, z: int32)\n"
        "path(x, z) :- a(x, y), b(y, w), c(w, z), !neg(x, z).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_sip_stats_t stats = { 0, 0 };
    int rc = wl_sip_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.semijoins_inserted != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected semijoins_inserted=1, got %u",
                 stats.semijoins_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify ANTIJOIN is still present */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    if (!ir) {
        FAIL("no IR for 'path'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t aj_count = count_type_in_tree(ir, WIRELOG_IR_ANTIJOIN);
    if (aj_count != 1) {
        FAIL("expected 1 ANTIJOIN preserved");
        wirelog_program_free(prog);
        return;
    }

    /* Verify 1 SEMIJOIN inserted */
    uint32_t sj_count = count_type_in_tree(ir, WIRELOG_IR_SEMIJOIN);
    if (sj_count != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 1 SEMIJOIN in tree, got %u",
                 sj_count);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Idempotency Tests                                                        */
/* ======================================================================== */

static void
test_sip_idempotent(void)
{
    TEST("sip: second pass is idempotent (no additional SEMIJOINs)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, w: int32)\n"
                               ".decl c(w: int32, z: int32)\n"
                               ".decl path(x: int32, z: int32)\n"
                               "path(x, z) :- a(x, y), b(y, w), c(w, z).\n",
                               &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* First pass: should insert 1 SEMIJOIN */
    wl_sip_stats_t stats1 = { 0, 0 };
    int rc = wl_sip_apply(prog, &stats1);
    if (rc != 0) {
        FAIL("first pass failed");
        wirelog_program_free(prog);
        return;
    }
    if (stats1.semijoins_inserted != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "first pass: expected semijoins_inserted=1, got %u",
                 stats1.semijoins_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Second pass: should be no-op */
    wl_sip_stats_t stats2 = { 0, 0 };
    rc = wl_sip_apply(prog, &stats2);
    if (rc != 0) {
        FAIL("second pass failed");
        wirelog_program_free(prog);
        return;
    }
    if (stats2.semijoins_inserted != 0) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "second pass: expected semijoins_inserted=0, got %u",
                 stats2.semijoins_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Total SEMIJOIN count should still be 1 */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    uint32_t sj_count = count_type_in_tree(ir, WIRELOG_IR_SEMIJOIN);
    if (sj_count != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "expected 1 total SEMIJOIN after two passes, got %u",
                 sj_count);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* JPP interaction tests                                                    */
/* ======================================================================== */

static void
test_sip_jpp_project_chain(void)
{
    TEST("sip: 3-atom chain gets SEMIJOIN after JPP inserts intermediate "
         "PROJECT");

    /*
     * path(x, z) :- a(x, y), b(y, w), c(w, z).
     *
     * After JPP, the intermediate variable y is projected away between
     * the inner and outer JOIN.  The chain becomes:
     *
     *   JOIN(key=w)
     *     PROJECT(cols=[x,w])    <- JPP intermediate projection
     *       JOIN(key=y)
     *         SCAN(a)
     *         SCAN(b)
     *     SCAN(c)
     *
     * SIP must skip through the PROJECT to recognise the 3-atom chain
     * and insert 1 SEMIJOIN.
     */
    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, w: int32)\n"
                               ".decl c(w: int32, z: int32)\n"
                               ".decl path(x: int32, z: int32)\n"
                               "path(x, z) :- a(x, y), b(y, w), c(w, z).\n",
                               &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* Apply JPP first — inserts intermediate PROJECT nodes */
    wl_jpp_apply(prog, NULL);

    /* Now apply SIP — must still insert SEMIJOINs despite PROJECT nodes */
    wl_sip_stats_t stats = { 0, 0, 0 };
    int rc = wl_sip_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.semijoins_inserted != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "expected semijoins_inserted=1 after JPP, got %u",
                 stats.semijoins_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify exactly 1 SEMIJOIN in tree */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    if (!ir) {
        FAIL("no IR for 'path'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t sj_count = count_type_in_tree(ir, WIRELOG_IR_SEMIJOIN);
    if (sj_count != 1) {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 1 SEMIJOIN after JPP, got %u",
                 sj_count);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_sip ===\n");

    /* Error handling */
    test_sip_null_program();
    test_sip_null_stats();
    test_sip_no_ir_trees();

    /* No-op cases */
    test_sip_two_atom_noop();

    /* Core SIP */
    test_sip_three_atom_semijoin();
    test_sip_four_atom_semijoin();

    /* UNION and ANTIJOIN */
    test_sip_union_recurse();
    test_sip_antijoin_preserved();

    /* Idempotency */
    test_sip_idempotent();

    /* JPP interaction (Issue #192) */
    test_sip_jpp_project_chain();

    printf("\n  Total: %d  Passed: %d  Failed: %d\n\n", tests_run, tests_passed,
           tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
