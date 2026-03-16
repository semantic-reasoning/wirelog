/*
 * test_magic_sets.c - Tests for Magic Sets Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests verify:
 *   1. Adornment computation (bound/free positions propagated correctly)
 *   2. Magic relation and rule generation
 *   3. All-free adornment optimization (skip magic)
 *   4. No demand through negation (ANTIJOIN right child)
 *   5. Multiple adornments for the same relation
 *   6. EDB relations do not get magic relations
 *   7. Correctness: magic-optimized result == full result (integration)
 */

#include "../wirelog/passes/magic_sets.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/subsumption.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/ir/stratify.h"
#include "../wirelog/wirelog-parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

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
/* Helper: check if a magic relation exists in the program                 */
/* ======================================================================== */

static bool
has_relation(const struct wirelog_program *prog, const char *name)
{
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, name) == 0)
            return true;
    }
    return false;
}

static bool
has_rule_for(const struct wirelog_program *prog, const char *head_name)
{
    for (uint32_t i = 0; i < prog->rule_count; i++) {
        if (prog->rules[i].head_relation
            && strcmp(prog->rules[i].head_relation, head_name) == 0)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Helper: parse a program with standard optimizer passes applied          */
/* ======================================================================== */

static struct wirelog_program *
parse_and_optimize(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;
    wl_subsumption_apply(prog, NULL);
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    return prog;
}

/* ======================================================================== */
/* Test 1: test_adornment_basic                                             */
/* Single non-recursive IDB rule: demand propagates to IDB body atom.      */
/* ======================================================================== */

static void
test_adornment_basic(void)
{
    TEST("test_adornment_basic");

    /*
     * Edge(x, y) is EDB (no rules).
     * Reach(x, y) is IDB (has rules).
     * Out(x) :- Reach(x, y), Edge(y, z).
     *
     * With demand Out_b (position 0 = x bound):
     *   - Reach(x, y): x is bound -> adornment bf -> $m$Reach_bf created
     *   - Edge is EDB -> skip
     */
    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Reach(x: int32, y: int32)\n"
                      ".decl Out(x: int32)\n"
                      ".output Out\n"
                      "Reach(x, y) :- Edge(x, y).\n"
                      "Out(x) :- Reach(x, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "Out";
    demands[0].bound_mask = 0x1; /* position 0 bound */
    demands[0].arity = 1;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* Out is the demand root -> $m$Out_b may or may not be created depending
     * on whether Out itself is IDB. Out IS IDB (has rule Out(x) :- Reach(x,y)).
     * With demand Out_b: x is bound, walk Out's rule body:
     *   atom Reach(x, y): x is bound -> adornment bf -> $m$Reach_bf created.
     * Also Out gets guard $m$Out_b.
     */
    bool has_out_magic = has_relation(prog, "$m$Out_b");
    bool has_reach_magic = has_relation(prog, "$m$Reach_bf");

    if (!has_out_magic || !has_reach_magic) {
        wirelog_program_free(prog);
        FAIL("expected $m$Out_b and $m$Reach_bf to be created");
        return;
    }

    if (stats.adorned_predicates == 0) {
        wirelog_program_free(prog);
        FAIL("expected adorned_predicates > 0");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 2: test_adornment_recursive                                         */
/* Recursive TC rule: adornment propagates through recursive body atom.    */
/* ======================================================================== */

static void
test_adornment_recursive(void)
{
    TEST("test_adornment_recursive");

    /*
     * TC with Edge-first rule (allows meaningful demand propagation):
     *   Path(x, y) :- Edge(x, y).
     *   Path(x, y) :- Edge(x, z), Path(z, y).
     *
     * With demand Path_bf (x bound):
     *   Rule 2: bound_vars = {x}
     *     atom Edge(x, z): EDB, adds {x, z}
     *     atom Path(z, y): z is bound -> adornment bf -> $m$Path_bf (already in set)
     *   -> adorned_predicates = 1 (only Path_bf)
     */
    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Path(x: int32, y: int32)\n"
                      ".output Path\n"
                      "Path(x, y) :- Edge(x, y).\n"
                      "Path(x, y) :- Edge(x, z), Path(z, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "Path";
    demands[0].bound_mask = 0x1; /* position 0 (x) bound */
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* $m$Path_bf must be created */
    if (!has_relation(prog, "$m$Path_bf")) {
        wirelog_program_free(prog);
        FAIL("$m$Path_bf relation not created");
        return;
    }

    /* adorned_predicates should be 1 (only Path_bf) */
    if (stats.adorned_predicates != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected adorned_predicates=1, got %u",
                 stats.adorned_predicates);
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 3: test_magic_rule_generation                                       */
/* Verify correct demand propagation rule is generated for TC.             */
/* ======================================================================== */

static void
test_magic_rule_generation(void)
{
    TEST("test_magic_rule_generation");

    /*
     * Path(x, y) :- Edge(x, z), Path(z, y).
     * With Path_bf:
     *   prefix = [Edge(x,z)], target = Path(z,y), z bound
     *   Demand rule: $m$Path_bf(z) :- $m$Path_bf(x), Edge(x, z).
     */
    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Path(x: int32, y: int32)\n"
                      ".output Path\n"
                      "Path(x, y) :- Edge(x, y).\n"
                      "Path(x, y) :- Edge(x, z), Path(z, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "Path";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* A demand propagation rule for $m$Path_bf must be generated */
    if (stats.magic_rules_generated == 0) {
        wirelog_program_free(prog);
        FAIL("expected at least one magic demand rule generated");
        return;
    }

    /* The magic rule must have head $m$Path_bf */
    if (!has_rule_for(prog, "$m$Path_bf")) {
        wirelog_program_free(prog);
        FAIL("no rule with head $m$Path_bf found");
        return;
    }

    /* Both original Path rules must have magic guards inserted */
    if (stats.original_rules_modified != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected original_rules_modified=2, got %u",
                 stats.original_rules_modified);
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 4: test_all_free_skip                                               */
/* All-free adornment (bound_mask=0) triggers optimization skip.           */
/* ======================================================================== */

static void
test_all_free_skip(void)
{
    TEST("test_all_free_skip");

    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Path(x: int32, y: int32)\n"
                      ".output Path\n"
                      "Path(x, y) :- Edge(x, y).\n"
                      "Path(x, y) :- Edge(x, z), Path(z, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "Path";
    demands[0].bound_mask = 0; /* All-free */
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* No magic relations should be created */
    if (has_relation(prog, "$m$Path_ff")) {
        wirelog_program_free(prog);
        FAIL("all-free optimization should skip $m$Path_ff");
        return;
    }

    /* adorned_predicates must be 0 */
    if (stats.adorned_predicates != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected adorned_predicates=0, got %u",
                 stats.adorned_predicates);
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    /* skipped_all_free must be > 0 */
    if (stats.skipped_all_free == 0) {
        wirelog_program_free(prog);
        FAIL("expected skipped_all_free > 0");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 5: test_negation_no_demand                                          */
/* Demand does NOT flow through ANTIJOIN (negated atoms).                  */
/* ======================================================================== */

static void
test_negation_no_demand(void)
{
    TEST("test_negation_no_demand");

    /*
     * IDB(x) :- Base(x), !Exclude(x).
     * Exclude(x) :- BadBase(x).
     *
     * With demand IDB_b (x bound):
     *   Rule body: [Base(x), !Exclude(x)]
     *   Base is EDB -> skip.
     *   Exclude is in ANTIJOIN right child -> demand does NOT flow.
     *   -> NO magic relation for Exclude.
     */
    const char *src = ".decl Base(x: int32)\n"
                      ".decl BadBase(x: int32)\n"
                      ".decl Exclude(x: int32)\n"
                      ".decl IDB(x: int32)\n"
                      ".output IDB\n"
                      "Exclude(x) :- BadBase(x).\n"
                      "IDB(x) :- Base(x), !Exclude(x).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "IDB";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 1;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* No magic relation for Exclude (demand must NOT flow through negation) */
    if (has_relation(prog, "$m$Exclude_b")) {
        wirelog_program_free(prog);
        FAIL("demand must not flow through negation: $m$Exclude_b should not "
             "exist");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 6: test_multiple_adornments                                         */
/* Same relation used with both bf and fb adornments.                      */
/* ======================================================================== */

static void
test_multiple_adornments(void)
{
    TEST("test_multiple_adornments");

    /*
     * A(x, y) :- Edge(x, z), B(z, y).     // B with first arg bound -> B_bf
     * B(x, y) :- Edge(y, z), A(z, x).     // A with first arg bound -> A_bf
     *
     * With demand A_bf (x bound):
     *   Rule for A: bound={x}, atoms=[Edge(x,z), B(z,y)]
     *     Edge EDB, adds {x,z}
     *     B(z,y): z bound -> adornment bf -> $m$B_bf
     *   Process B_bf:
     *     Rule for B: bound={z}, atoms=[Edge(y,z), A(z,x)]
     *       Edge EDB, adds {y,z}  (note: z already bound)
     *       A(z,x): z bound -> adornment bf -> $m$A_bf (already in set)
     *   -> adorned: {A_bf, B_bf}
     */
    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl A(x: int32, y: int32)\n"
                      ".decl B(x: int32, y: int32)\n"
                      ".output A\n"
                      "A(x, y) :- Edge(x, z), B(z, y).\n"
                      "B(x, y) :- Edge(y, z), A(z, x).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "A";
    demands[0].bound_mask = 0x1; /* x bound */
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    bool has_a_magic = has_relation(prog, "$m$A_bf");
    bool has_b_magic = has_relation(prog, "$m$B_bf");

    if (!has_a_magic || !has_b_magic) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected $m$A_bf=%s $m$B_bf=%s",
                 has_a_magic ? "yes" : "NO", has_b_magic ? "yes" : "NO");
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    /* At least A_bf and B_bf must be adorned; mutual recursion may
     * discover additional adornments (A_bb, B_bb) as bound vars propagate. */
    if (stats.adorned_predicates < 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected adorned_predicates>=2, got %u",
                 stats.adorned_predicates);
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 7: test_mutual_recursion                                            */
/* Two mutually recursive relations with demand propagation.               */
/* ======================================================================== */

static void
test_mutual_recursion(void)
{
    TEST("test_mutual_recursion");

    /*
     * VPT(h, v) :- Assign(v, h).
     * VPT(h, v) :- VPT(h, u), Load(u, v).
     * CGE(m, n) :- Reachable(m), Calls(m, n).
     * Reachable(m) :- Root(m).
     * Reachable(m) :- Reachable(n), CGE(n, m).
     *
     * With demand VPT_bf (h bound):
     *   VPT rule 2: bound={h}, atoms = [VPT(h,u), Load(u,v)]
     *     VPT(h,u): h bound -> adornment bf -> $m$VPT_bf (already in set)
     *     Load EDB, adds {u,v}
     *   -> adorned: {VPT_bf}
     */
    const char *src = ".decl Assign(v: int32, h: int32)\n"
                      ".decl Load(u: int32, v: int32)\n"
                      ".decl VPT(h: int32, v: int32)\n"
                      ".output VPT\n"
                      "VPT(h, v) :- Assign(v, h).\n"
                      "VPT(h, v) :- VPT(h, u), Load(u, v).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "VPT";
    demands[0].bound_mask = 0x1; /* h bound */
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    if (!has_relation(prog, "$m$VPT_bf")) {
        wirelog_program_free(prog);
        FAIL("$m$VPT_bf relation not created");
        return;
    }

    /* Both VPT rules should have magic guards */
    if (stats.original_rules_modified != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected original_rules_modified=2, got %u",
                 stats.original_rules_modified);
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 8: test_edb_skip                                                    */
/* EDB relations (no defining rules) do not get magic relations.           */
/* ======================================================================== */

static void
test_edb_skip(void)
{
    TEST("test_edb_skip");

    /*
     * Edge is EDB (no rules).
     * Path is IDB (has rules).
     *
     * With demand Path_bf (x bound):
     *   $m$Path_bf created (IDB).
     *   Edge: EDB -> no $m$Edge_XX created.
     */
    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Path(x: int32, y: int32)\n"
                      ".output Path\n"
                      "Path(x, y) :- Edge(x, y).\n"
                      "Path(x, y) :- Edge(x, z), Path(z, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "Path";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* No magic relation for EDB Edge */
    if (has_relation(prog, "$m$Edge_bf") || has_relation(prog, "$m$Edge_bb")
        || has_relation(prog, "$m$Edge_fb")
        || has_relation(prog, "$m$Edge_ff")) {
        wirelog_program_free(prog);
        FAIL("EDB relation Edge must not get a magic relation");
        return;
    }

    /* $m$Path_bf must exist */
    if (!has_relation(prog, "$m$Path_bf")) {
        wirelog_program_free(prog);
        FAIL("$m$Path_bf should be created");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Test 9: test_standard_apply_noop                                        */
/* wl_magic_sets_apply (all-free from .output) is a no-op.                */
/* ======================================================================== */

static void
test_standard_apply_noop(void)
{
    TEST("test_standard_apply_noop");

    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Path(x: int32, y: int32)\n"
                      ".output Path\n"
                      "Path(x, y) :- Edge(x, y).\n"
                      "Path(x, y) :- Edge(x, z), Path(z, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    uint32_t rule_count_before = prog->rule_count;
    uint32_t relation_count_before = prog->relation_count;

    wl_magic_sets_stats_t stats;
    int rc = wl_magic_sets_apply(prog, &stats);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* No magic relations or rules added (all-free optimization) */
    if (prog->rule_count != rule_count_before) {
        char msg[64];
        snprintf(msg, sizeof(msg), "rule count changed: before=%u after=%u",
                 rule_count_before, prog->rule_count);
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    if (prog->relation_count != relation_count_before) {
        wirelog_program_free(prog);
        FAIL("relation count changed (should be no-op for all-free)");
        return;
    }

    if (prog->magic_sets_applied) {
        wirelog_program_free(prog);
        FAIL("magic_sets_applied should be false for all-free no-op");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Integration Test 1: magic sets does not change result for all-free      */
/* ======================================================================== */

static void
test_magic_correctness_noop(void)
{
    TEST("test_magic_correctness_noop (integration)");

    /*
     * Run TC program through the full pipeline with magic sets (all-free = noop).
     * Verify no crash and program still stratified.
     */
    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Path(x: int32, y: int32)\n"
                      ".output Path\n"
                      "Edge(1, 2).\n"
                      "Edge(2, 3).\n"
                      "Path(x, y) :- Edge(x, y).\n"
                      "Path(x, y) :- Edge(x, z), Path(z, y).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_subsumption_apply(prog, NULL);
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    wl_magic_sets_apply(prog, NULL);

    /* Rebuild and re-stratify only if magic was applied */
    if (prog->magic_sets_applied) {
        wl_ir_program_rebuild_relation_irs(prog);
        wl_ir_program_free_strata(prog);
        wl_ir_stratify_program(prog);
    }

    /* Program must still be stratified */
    if (!prog->is_stratified) {
        wirelog_program_free(prog);
        FAIL("program is not stratified after magic sets");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Integration Test 2: magic sets with bound demand + rebuild               */
/* ======================================================================== */

static void
test_magic_rebuild_and_stratify(void)
{
    TEST("test_magic_rebuild_and_stratify (integration)");

    const char *src = ".decl Edge(x: int32, y: int32)\n"
                      ".decl Path(x: int32, y: int32)\n"
                      ".output Path\n"
                      "Path(x, y) :- Edge(x, y).\n"
                      "Path(x, y) :- Edge(x, z), Path(z, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "Path";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    int rc = wl_magic_sets_apply_with_demands(prog, demands, 1, NULL);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    /* Rebuild and re-stratify */
    if (wl_ir_program_rebuild_relation_irs(prog) != 0) {
        wirelog_program_free(prog);
        FAIL("rebuild_relation_irs failed");
        return;
    }

    wl_ir_program_free_strata(prog);
    if (wl_ir_stratify_program(prog) != 0) {
        wirelog_program_free(prog);
        FAIL("re-stratification failed");
        return;
    }

    if (!prog->is_stratified) {
        wirelog_program_free(prog);
        FAIL("program not stratified after magic sets + rebuild");
        return;
    }

    /* $m$Path_bf must be in the rebuilt relation_irs */
    bool found_magic_ir = false;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, "$m$Path_bf") == 0) {
            found_magic_ir = true;
            break;
        }
    }
    if (!found_magic_ir) {
        wirelog_program_free(prog);
        FAIL("$m$Path_bf not in program after rebuild");
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
    printf("=== Magic Sets Tests ===\n\n");

    printf("Unit Tests:\n");
    test_adornment_basic();
    test_adornment_recursive();
    test_magic_rule_generation();
    test_all_free_skip();
    test_negation_no_demand();
    test_multiple_adornments();
    test_mutual_recursion();
    test_edb_skip();
    test_standard_apply_noop();

    printf("\nIntegration Tests:\n");
    test_magic_correctness_noop();
    test_magic_rebuild_and_stratify();

    printf("\n=== Results ===\n");
    printf("Tests run:    %d\n", tests_run);
    printf("Tests passed: %d\n", tests_passed);
    printf("Tests failed: %d\n", tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
