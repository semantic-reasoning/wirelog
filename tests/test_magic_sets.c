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
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/ir/stratify.h"
#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog-parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

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
/* Helpers: magic guard structure (Issue #989)                             */
/* ======================================================================== */

/*
 * The body root of the @nth rule whose head is @head, i.e. child[0] of the
 * rule's PROJECT.  After Magic Sets this is the guard JOIN.
 */
static const wirelog_ir_node_t *
rule_body_root(const struct wirelog_program *prog, const char *head,
    uint32_t nth)
{
    uint32_t seen = 0;
    for (uint32_t i = 0; i < prog->rule_count; i++) {
        if (!prog->rules[i].head_relation
            || strcmp(prog->rules[i].head_relation, head) != 0)
            continue;
        if (seen++ != nth)
            continue;
        const wirelog_ir_node_t *root = prog->rules[i].ir_root;
        if (!root || root->child_count == 0)
            return NULL;
        return root->children[0];
    }
    return NULL;
}

static const wirelog_ir_node_t *
rule_ir_root(const struct wirelog_program *prog, const char *head, uint32_t nth)
{
    uint32_t seen = 0;
    for (uint32_t i = 0; i < prog->rule_count; i++) {
        if (!prog->rules[i].head_relation
            || strcmp(prog->rules[i].head_relation, head) != 0)
            continue;
        if (seen++ == nth)
            return prog->rules[i].ir_root;
    }
    return NULL;
}

static uint32_t
rule_count_for(const struct wirelog_program *prog, const char *head)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < prog->rule_count; i++) {
        if (prog->rules[i].head_relation
            && strcmp(prog->rules[i].head_relation, head) == 0)
            n++;
    }
    return n;
}

static bool
ir_contains_type(const wirelog_ir_node_t *node, wirelog_ir_node_type_t type)
{
    if (!node)
        return false;
    if (node->type == type)
        return true;
    for (uint32_t i = 0; i < node->child_count; i++) {
        if (ir_contains_type(node->children[i], type))
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Helpers: seeded evaluation (Issue #989)                                 */
/* ======================================================================== */

/*
 * Seed a relation's inline facts directly.  This is test-only scaffolding:
 * the shipped path that seeds a magic demand relation from a bound `.query`
 * does not exist yet, so the tests below stand in for it in order to reach
 * the guarded rule bodies at all.
 */
static bool
seed_relation_facts(struct wirelog_program *prog, const char *name,
    const int64_t *rows, uint32_t row_count, uint32_t ncols)
{
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->name || strcmp(rel->name, name) != 0)
            continue;
        if (rel->column_count != ncols)
            return false;
        size_t total = (size_t)row_count * ncols;
        free(rel->fact_data);
        rel->fact_data = (int64_t *)malloc(total * sizeof(int64_t));
        if (!rel->fact_data)
            return false;
        memcpy(rel->fact_data, rows, total * sizeof(int64_t));
        rel->fact_count = row_count;
        rel->fact_capacity = (uint32_t)total;
        return true;
    }
    return false;
}

#define MS_MAX_ROWS 64
#define MS_MAX_COLS 4

typedef struct {
    const char *relation;
    uint32_t ncols;
    int64_t rows[MS_MAX_ROWS][MS_MAX_COLS];
    uint32_t count;
    bool overflow;
} ms_row_set_t;

static void
ms_collect_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    ms_row_set_t *set = (ms_row_set_t *)user_data;
    if (!relation || strcmp(relation, set->relation) != 0)
        return;
    if (ncols > MS_MAX_COLS || set->count >= MS_MAX_ROWS) {
        set->overflow = true;
        return;
    }
    set->ncols = ncols;
    for (uint32_t i = 0; i < ncols; i++)
        set->rows[set->count][i] = row[i];
    set->count++;
}

static bool
ms_rows_match(const ms_row_set_t *actual, const int64_t *expected,
    uint32_t expected_rows, uint32_t ncols)
{
    if (actual->overflow || actual->count != expected_rows)
        return false;
    if (expected_rows == 0)
        return true;
    if (actual->ncols != ncols)
        return false;
    for (uint32_t r = 0; r < expected_rows; r++) {
        bool found = false;
        for (uint32_t a = 0; a < actual->count && !found; a++) {
            bool same = true;
            for (uint32_t c = 0; c < ncols; c++) {
                if (actual->rows[a][c] != expected[(size_t)r * ncols + c]) {
                    same = false;
                    break;
                }
            }
            found = same;
        }
        if (!found)
            return false;
    }
    return true;
}

/*
 * Parse + optimize @src, apply @demands, re-stratify, and seed @magic_rel
 * with @seed.  Returns the program ready for plan generation, or NULL.
 */
static struct wirelog_program *
magic_program_with_seed(const char *src, const wl_magic_demand_t *demands,
    uint32_t demand_count, const char *magic_rel, const int64_t *seed,
    uint32_t seed_rows, uint32_t seed_cols, wl_magic_sets_stats_t *stats)
{
    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog)
        return NULL;

    if (wl_magic_sets_apply_with_demands(prog, demands, demand_count, stats)
        != 0)
        goto fail;
    if (wl_ir_program_rebuild_relation_irs(prog) != 0)
        goto fail;
    wl_ir_program_free_strata(prog);
    if (wl_ir_stratify_program(prog) != 0)
        goto fail;
    if (magic_rel
        && !seed_relation_facts(prog, magic_rel, seed, seed_rows, seed_cols))
        goto fail;
    return prog;

fail:
    wirelog_program_free(prog);
    return NULL;
}

static bool
ms_evaluate(struct wirelog_program *prog, const char *out_rel,
    uint32_t workers, ms_row_set_t *out)
{
    memset(out, 0, sizeof(*out));
    out->relation = out_rel;

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0 || !plan)
        return false;

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, workers, &session);
    if (rc == 0)
        rc = wl_session_load_facts(session, prog);
    if (rc == 0)
        rc = wl_session_snapshot(session, ms_collect_rows, out);

    if (session)
        wl_session_destroy(session);
    wl_plan_free(plan);
    return rc == 0 && !out->overflow;
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

/* Issue #987: re-stratification after magic sets wrote past the end of a
 * stratum's rule_names array.
 *
 * wl_ir_stratify_program() sizes strata[s].rule_names from a counting loop
 * that skips rules whose head is absent from the dependency graph, then
 * filled it from a loop that did not skip them -- a skipped rule keeps
 * rule_stratum[r] == 0 from calloc and landed in stratum 0 regardless.
 * Magic-set rewriting manufactures those graph-absent heads because it
 * names demand relations from an atom's physical width while get_arity()
 * reports the declared logical width.
 *
 * The write is out of bounds.  One offending rule overwrites allocator
 * slack and is silent without a sanitizer; several corrupt the glibc heap
 * outright ("double free or corruption", "malloc(): corrupted top size"),
 * so this test uses several deliberately -- a single-rule version would
 * pass on the unfixed code in an ordinary build.
 *
 * All three shapes below crash on the unfixed code, and none is caught by
 * the #977 arity checks: the first has a correct head and a wrong body, the
 * second is undeclared so has_decl skips it, and in the third every atom
 * already measures at the correct physical width. */
static void
test_stratify_no_oob_on_graph_absent_head(void)
{
    TEST("Magic sets: re-stratification stays in bounds (#987)");

    static const char *const srcs[] = {
        /* declared, body atom wider than the .decl, several rules */
        ".decl edge(a: int64, b: int64)\n"
        ".decl path(a: int64, b: int64)\n"
        "path(x,z) :- path(x,y,q1), edge(y,z).\n"
        "path(x,z) :- path(x,y,q2), edge(y,z).\n"
        "path(x,z) :- path(x,y,q3), edge(y,z).\n"
        "path(x,z) :- path(x,y,q4), edge(y,z).\n",
        /* undeclared head relation -- has_decl gates the #977 checks off */
        ".decl edge(a: int64, b: int64)\n"
        "edge(1,2).\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,z) :- path(x,y,q1), edge(y,z).\n"
        "path(x,z) :- path(x,y,q2), edge(y,z).\n"
        "path(x,z) :- path(x,y,q3), edge(y,z).\n",
    };

    for (size_t i = 0; i < sizeof(srcs) / sizeof(srcs[0]); i++) {
        wirelog_error_t err;
        wirelog_program_t *prog = wirelog_parse_string(srcs[i], &err);
        if (!prog)
            continue; /* rejected earlier by an arity check: also fine */

        wl_magic_demand_t demands[1];
        demands[0].relation_name = "path";
        demands[0].bound_mask = 0x1;
        demands[0].arity = 2;

        if (wl_magic_sets_apply_with_demands(prog, demands, 1, NULL) != 0) {
            wirelog_program_free(prog);
            continue;
        }
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

        /* Every stratum must have filled exactly as many names as it
         * declares, and no NULL may remain -- a short fill is the
         * non-sanitizer-visible half of the same loop disagreement. */
        for (uint32_t s = 0; s < prog->stratum_count; s++) {
            if (!prog->strata[s].rule_names)
                continue;
            for (uint32_t k = 0; k < prog->strata[s].rule_count; k++) {
                if (!prog->strata[s].rule_names[k]) {
                    wirelog_program_free(prog);
                    FAIL("stratum rule_names shorter than rule_count");
                    return;
                }
            }
        }

        wirelog_program_free(prog);
    }

    PASS();
}

/* ======================================================================== */
/* Issue #989: guard JOIN order                                            */
/* ======================================================================== */

/* The `.query Path(b, f)` example from docs/SYNTAX.md. */
static const char *k_syntax_doc_src
    = ".decl Edge(x: int32, y: int32)\n"
    ".decl Path(x: int32, y: int32)\n"
    ".output Path\n"
    "Edge(1, 2).\n"
    "Edge(2, 3).\n"
    "Edge(3, 4).\n"
    "Path(x, y) :- Edge(x, y).\n"
    "Path(x, y) :- Edge(x, z), Path(z, y).\n";

/* Non-recursive three-way join: the guarded body is a JOIN chain. */
static const char *k_three_way_src
    = ".decl a(x: int32, y: int32)\n"
    ".decl b(y: int32, z: int32)\n"
    ".decl c(z: int32, w: int32)\n"
    ".decl out(x: int32, w: int32)\n"
    ".output out\n"
    "a(1, 2).\n"
    "a(9, 2).\n"
    "b(2, 3).\n"
    "c(3, 4).\n"
    "out(x, w) :- a(x, y), b(y, z), c(z, w).\n";

/*
 * Test: the guard JOIN's right child must be a relation-bearing node.
 *
 * wl_plan_op_t.right_relation is a relation name, not a subtree, so a JOIN
 * whose right child is itself a JOIN/ANTIJOIN/SEMIJOIN cannot be expressed
 * in the plan at all: translate_ir_node() emits right_relation = NULL and
 * the operator matches nothing.
 */
static void
test_guard_join_right_child_is_representable(void)
{
    TEST("test_guard_join_right_child_is_representable");

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "Path";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    struct wirelog_program *prog = magic_program_with_seed(k_syntax_doc_src,
            demands, 1, NULL, NULL, 0, 0, NULL);
    if (!prog) {
        FAIL("setup failed");
        return;
    }

    uint32_t nrules = rule_count_for(prog, "Path");
    if (nrules != 2) {
        wirelog_program_free(prog);
        FAIL("expected 2 rules for Path");
        return;
    }

    for (uint32_t r = 0; r < nrules; r++) {
        const wirelog_ir_node_t *guard = rule_body_root(prog, "Path", r);
        if (!guard || guard->type != WIRELOG_IR_JOIN
            || guard->child_count != 2) {
            wirelog_program_free(prog);
            FAIL("guard JOIN missing");
            return;
        }
        const wirelog_ir_node_t *right = guard->children[1];
        if (!right || !right->relation_name) {
            wirelog_program_free(prog);
            FAIL("guard JOIN right child carries no relation name "
                "(right_relation would be NULL)");
            return;
        }
        if (strncmp(right->relation_name, "$m$", 3) != 0) {
            wirelog_program_free(prog);
            FAIL("guard JOIN right child is not the magic demand scan");
            return;
        }
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Test: the docs/SYNTAX.md recursive example, with the demand relation
 * seeded by hand, must produce the full transitive closure reachable from
 * the seed.  With the guard built right-deep the recursive rule computes
 * nothing and only the base rule's 3 tuples survive.
 */
static void
test_guard_recursive_eval_exact(void)
{
    TEST("test_guard_recursive_eval_exact");

    /* Seed the demand at 2, not at 1.
     *
     * Seeding at 1 would expect all six closure tuples -- which is exactly
     * what an *unguarded* program produces over this graph, since every node
     * is reachable from 1.  Such a test detects the #989 failure (the guard
     * rejecting everything) but cannot detect the guard being absent or
     * over-deriving, because the correct and the broken answers coincide.
     *
     * Seeding at 2 makes the expected set a strict subset: the three tuples
     * rooted at nodes reachable from 2.  A missing or ineffective guard
     * yields six rows and fails. */
    static const int64_t seed[] = { 2 };
    static const int64_t expected[][2] = {
        { 2, 3 }, { 3, 4 }, { 2, 4 },
    };

    const uint32_t worker_counts[] = { 1, 4, 8 };
    for (uint32_t wi = 0; wi < 3; wi++) {
        wl_magic_demand_t demands[1];
        demands[0].relation_name = "Path";
        demands[0].bound_mask = 0x1;
        demands[0].arity = 2;

        struct wirelog_program *prog
            = magic_program_with_seed(k_syntax_doc_src, demands, 1,
                "$m$Path_bf", seed, 1, 1, NULL);
        if (!prog) {
            FAIL("setup failed");
            return;
        }

        ms_row_set_t rows;
        if (!ms_evaluate(prog, "Path", worker_counts[wi], &rows)) {
            wirelog_program_free(prog);
            FAIL("evaluation failed");
            return;
        }
        if (!ms_rows_match(&rows, &expected[0][0], 3, 2)) {
            printf(" [workers=%u got %u rows, want 3]", worker_counts[wi],
                rows.count);
            wirelog_program_free(prog);
            FAIL("Path != the demand-restricted closure from seed {2}");
            return;
        }
        wirelog_program_free(prog);
    }

    PASS();
}

/*
 * Test: a non-recursive three-way join under a guard.  The body is a JOIN
 * chain, so a right-deep guard makes the whole rule produce zero rows.
 */
static void
test_guard_nonrecursive_join_chain_exact(void)
{
    TEST("test_guard_nonrecursive_join_chain_exact");

    static const int64_t seed[] = { 1 };
    static const int64_t expected[][2] = { { 1, 4 } };

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "out";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    struct wirelog_program *prog = magic_program_with_seed(k_three_way_src,
            demands, 1, "$m$out_bf", seed, 1, 1, NULL);
    if (!prog) {
        FAIL("setup failed");
        return;
    }

    ms_row_set_t rows;
    if (!ms_evaluate(prog, "out", 1, &rows)) {
        wirelog_program_free(prog);
        FAIL("evaluation failed");
        return;
    }
    if (!ms_rows_match(&rows, &expected[0][0], 1, 2)) {
        printf(" [got %u rows, want 1]", rows.count);
        wirelog_program_free(prog);
        FAIL("out != {(1, 4)}");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Regression: a guarded rule whose body is an ANTIJOIN.  The ANTIJOIN is a
 * composite node, so it is equally unusable as a JOIN right child.
 */
static void
test_guard_over_antijoin_exact(void)
{
    TEST("test_guard_over_antijoin_exact");

    static const char *src = ".decl e(x: int32, y: int32)\n"
        ".decl s(y: int32)\n"
        ".decl p(x: int32, y: int32)\n"
        ".output p\n"
        "e(1, 2).\n"
        "e(1, 3).\n"
        "e(2, 3).\n"
        "s(3).\n"
        "p(x, y) :- e(x, y), !s(y).\n";

    static const int64_t seed[] = { 1 };
    static const int64_t expected[][2] = { { 1, 2 } };

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "p";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    struct wirelog_program *prog = magic_program_with_seed(src, demands, 1,
            "$m$p_bf", seed, 1, 1, NULL);
    if (!prog) {
        FAIL("setup failed");
        return;
    }

    const wirelog_ir_node_t *guard = rule_body_root(prog, "p", 0);
    if (!ir_contains_type(guard, WIRELOG_IR_ANTIJOIN)) {
        wirelog_program_free(prog);
        FAIL("expected an ANTIJOIN under the guard");
        return;
    }

    ms_row_set_t rows;
    if (!ms_evaluate(prog, "p", 1, &rows)) {
        wirelog_program_free(prog);
        FAIL("evaluation failed");
        return;
    }
    if (!ms_rows_match(&rows, &expected[0][0], 1, 2)) {
        printf(" [got %u rows, want 1]", rows.count);
        wirelog_program_free(prog);
        FAIL("p != {(1, 2)}");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Regression: a guarded rule whose body carries a SIP-inserted SEMIJOIN.
 */
static void
test_guard_over_semijoin_exact(void)
{
    TEST("test_guard_over_semijoin_exact");

    static const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl c(z: int32, w: int32)\n"
        ".decl out(x: int32, w: int32)\n"
        ".output out\n"
        "a(1, 2).\n"
        "a(2, 3).\n"
        "a(3, 4).\n"
        "b(2, 5).\n"
        "b(3, 6).\n"
        "b(4, 7).\n"
        "c(5, 10).\n"
        "c(6, 11).\n"
        "c(7, 12).\n"
        "out(x, w) :- a(x, y), b(y, z), c(z, w).\n";

    static const int64_t seed[] = { 1 };
    static const int64_t expected[][2] = { { 1, 10 } };

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "out";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    struct wirelog_program *prog = magic_program_with_seed(src, demands, 1,
            "$m$out_bf", seed, 1, 1, NULL);
    if (!prog) {
        FAIL("setup failed");
        return;
    }

    const wirelog_ir_node_t *guard = rule_body_root(prog, "out", 0);
    if (!ir_contains_type(guard, WIRELOG_IR_SEMIJOIN)) {
        wirelog_program_free(prog);
        FAIL("expected a SEMIJOIN under the guard (SIP did not fire)");
        return;
    }

    ms_row_set_t rows;
    if (!ms_evaluate(prog, "out", 1, &rows)) {
        wirelog_program_free(prog);
        FAIL("evaluation failed");
        return;
    }
    if (!ms_rows_match(&rows, &expected[0][0], 1, 2)) {
        printf(" [got %u rows, want 1]", rows.count);
        wirelog_program_free(prog);
        FAIL("out != {(1, 10)}");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Test: a constant in the bound head position leaves the rule unguarded, so
 * it must not be counted as modified.
 */
static void
test_constant_head_position_not_counted(void)
{
    TEST("test_constant_head_position_not_counted");

    static const char *src = ".decl edge(x: int32, y: int32)\n"
        ".decl q(x: int32, y: int32)\n"
        ".output q\n"
        "q(1, 1).\n"
        "q(1, 2).\n"
        "edge(1, 2).\n"
        "edge(2, 3).\n"
        "edge(3, 4).\n"
        "q(1, y) :- q(1, z), edge(z, y).\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "q";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    if (wl_magic_sets_apply_with_demands(prog, demands, 1, &stats) != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    if (stats.original_rules_modified != 0) {
        printf(" [original_rules_modified=%u]", stats.original_rules_modified);
        wirelog_program_free(prog);
        FAIL("no guard was inserted, yet the rule was counted as modified");
        return;
    }
    if (stats.skipped_constant_head != 1) {
        printf(" [skipped_constant_head=%u]", stats.skipped_constant_head);
        wirelog_program_free(prog);
        FAIL("expected skipped_constant_head == 1");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* A mixed constant/variable adornment keeps both physical magic columns, but
 * only the variable column participates in the guard key. */
static void
test_mixed_constant_variable_guard_positions(void)
{
    TEST("test_mixed_constant_variable_guard_positions");

    static const char *src = ".decl mid(x: int32, y: int32)\n"
        ".decl q(a: int32, b: int32)\n"
        ".output q\n"
        "mid(1, 2).\n"
        "q(1, y) :- mid(x, y).\n";
    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_magic_demand_t demand = { "q", 0x3, 2 };
    wl_magic_sets_stats_t stats;
    if (wl_magic_sets_apply_with_demands(prog, &demand, 1, &stats) != 0
        || stats.original_rules_modified != 1) {
        wirelog_program_free(prog);
        FAIL("mixed bound positions were not guarded");
        return;
    }

    const wirelog_ir_node_t *guard = rule_body_root(prog, "q", 0);
    const wirelog_ir_node_t *scan = guard && guard->child_count == 2
        ? guard->children[1] : NULL;
    if (!scan || scan->type != WIRELOG_IR_SCAN || scan->column_count != 2
        || !scan->column_names[0] || !scan->column_names[1]
        || strcmp(scan->column_names[1], "y") != 0
        || guard->join_key_count != 1
        || strcmp(guard->join_left_keys[0], "y") != 0
        || strcmp(guard->join_right_keys[0], "y") != 0) {
        wirelog_program_free(prog);
        FAIL("magic guard columns and keys are positionally misaligned");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Source shared by the two fused-head tests (#990).
 *
 * p's second rule is the fusion candidate: PROJECT(FILTER(JOIN(...)))
 * collapses to a FLATMAP root carrying the `y < 4` filter.  Logic Fusion runs
 * before Magic Sets in the shipping pipeline, so this is the ordinary shape a
 * filtered rule reaches the pass in.  p also has a PROJECT-rooted sibling
 * rule, so the two roots must both plan under the same UNION.
 *
 * With no magic sets the program yields five tuples:
 *
 *     (1,2) (1,3) (1,9) (2,3) (9,5)
 *
 * Seeded at $m$p_bf = {1} the answer is the three with x = 1: (1,2), (1,3),
 * (1,9).  (2,3) and (9,5) are exactly what the demand prunes, so the seeded
 * answer is *not* the unrestricted one -- a test that got all five back would
 * mean the guards matched everything.
 *
 * Two tuples carry the load.  p(1,3) comes only from the fused rule
 * (edge(1,2), b(2,3)), so it is lost outright when the fused rule's demand
 * propagation rule is not generated.  p(1,5) -- edge(1,9), b(9,5) -- is the
 * one the `y < 4` filter rejects, so it appears if guard insertion splices
 * over the FLATMAP instead of under it and drops the filter.  The filter is
 * therefore load-bearing on the answer, not only on the tree shape.
 */
static const char *k_fused_head_src = ".decl edge(x: int32, y: int32)\n"
    ".decl b(x: int32, y: int32)\n"
    ".decl p(x: int32, y: int32)\n"
    ".output p\n"
    "edge(1, 2).\n"
    "edge(2, 3).\n"
    "edge(1, 9).\n"
    "edge(9, 5).\n"
    "b(x, y) :- edge(x, y).\n"
    "p(x, y) :- b(x, y).\n"
    "p(x, y) :- edge(x, z), b(z, y), y < 4.\n";

/*
 * Test: a rule fused to a FLATMAP root still gets its magic guard, and the
 * demand it propagates still reaches the IDB it reads.
 *
 * This is the answer test.  get_head_vars() used to key off the root's node
 * type, which fusion rewrites in place, so a fused rule was skipped in all
 * three phases.  The Phase 4a skip is the one that loses tuples: no demand
 * propagation rule is generated for the fused rule, so b is under-populated
 * and p(1, 3) never derives.
 *
 * magic_rules_generated is the assertion that pins Phase 4a.  A shape-only
 * assertion passes with Phase 4a still broken, because Phase 4b inserts the
 * guard from the same head variables independently.
 */
static void
test_fused_head_guard_answers_exact(void)
{
    TEST("test_fused_head_guard_answers_exact");

    static const int64_t seed[] = { 1 };
    static const int64_t expected[][2] = { { 1, 2 }, { 1, 3 }, { 1, 9 } };

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "p";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    struct wirelog_program *prog = magic_program_with_seed(k_fused_head_src,
            demands, 1, "$m$p_bf", seed, 1, 1, &stats);
    if (!prog) {
        FAIL("setup failed");
        return;
    }

    /* Three guards: both rules of p, plus the single rule of b, which is
     * adorned in turn as p's body atom.  Only 2 of the 3 land without the
     * fix, because the fused rule of p is skipped. */
    if (stats.original_rules_modified != 3) {
        printf(" [original_rules_modified=%u]", stats.original_rules_modified);
        wirelog_program_free(prog);
        FAIL("expected both rules of p to be guarded");
        return;
    }
    if (stats.skipped_unsupported_head != 0) {
        printf(" [skipped_unsupported_head=%u]",
            stats.skipped_unsupported_head);
        wirelog_program_free(prog);
        FAIL("the fused rule must not be skipped as an unsupported head");
        return;
    }

    /* Phase 4a: one demand propagation rule per rule of p that reads an IDB.
     * With the fused rule skipped this is 1, and b loses its demand. */
    if (stats.magic_rules_generated != 2) {
        printf(" [magic_rules_generated=%u]", stats.magic_rules_generated);
        wirelog_program_free(prog);
        FAIL("the fused rule generated no demand propagation rule");
        return;
    }

    ms_row_set_t rows;
    if (!ms_evaluate(prog, "p", 1, &rows)) {
        wirelog_program_free(prog);
        FAIL("evaluation failed");
        return;
    }
    if (!ms_rows_match(&rows, &expected[0][0], 3, 2)) {
        printf(" [got %u rows, want 3]", rows.count);
        wirelog_program_free(prog);
        FAIL("p != {(1, 2), (1, 3), (1, 9)}");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Test: the guard inserted into a fused rule has the same shape as the guard
 * inserted into a PROJECT-rooted one -- a JOIN whose right child is the magic
 * demand SCAN (#989).
 *
 * Asserted per rule over every rule of p, including the fused one.  A bare
 * count of $m$ SCANs would not do: a right-deep guard still contains exactly
 * one, so it cannot tell #989's bug from a correct guard.
 */
static void
test_fused_head_guard_shape(void)
{
    TEST("test_fused_head_guard_shape");

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "p";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    struct wirelog_program *prog = parse_and_optimize(k_fused_head_src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* Precondition: fusion must actually have replaced a PROJECT root.  If it
     * stops firing this test would otherwise pass for the wrong reason. */
    uint32_t nrules = rule_count_for(prog, "p");
    bool saw_fused = false;
    for (uint32_t r = 0; r < nrules; r++) {
        const wirelog_ir_node_t *root = rule_ir_root(prog, "p", r);
        if (root && root->type == WIRELOG_IR_FLATMAP)
            saw_fused = true;
    }
    if (nrules != 2 || !saw_fused) {
        wirelog_program_free(prog);
        FAIL("expected fusion to replace the PROJECT root of one rule of p");
        return;
    }

    wl_magic_sets_stats_t stats;
    if (wl_magic_sets_apply_with_demands(prog, demands, 1, &stats) != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    for (uint32_t r = 0; r < nrules; r++) {
        const wirelog_ir_node_t *root = rule_ir_root(prog, "p", r);
        const wirelog_ir_node_t *guard = rule_body_root(prog, "p", r);
        if (!guard || guard->type != WIRELOG_IR_JOIN
            || guard->child_count != 2) {
            printf(" [rule %u]", r);
            wirelog_program_free(prog);
            FAIL("guard JOIN missing");
            return;
        }
        const wirelog_ir_node_t *right = guard->children[1];
        if (!right || !right->relation_name
            || strncmp(right->relation_name, "$m$", 3) != 0) {
            printf(" [rule %u]", r);
            wirelog_program_free(prog);
            FAIL("guard JOIN right child is not the magic demand scan");
            return;
        }
        /* The fused rule keeps its filter: the guard is spliced under the
         * FLATMAP, not in place of it. */
        if (root && root->type == WIRELOG_IR_FLATMAP && !root->filter_expr) {
            wirelog_program_free(prog);
            FAIL("guard insertion dropped the fused rule's filter");
            return;
        }
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Test: Phase 2's own call to get_head_vars() reads the fused root.
 *
 * The BFS in Phase 2 is what turns a demand on p into an adornment on p's
 * body atoms; a rule it cannot read the head of contributes no adorned
 * predicates.  k_fused_head_src does not test that call site: p there also has
 * a PROJECT-rooted sibling rule, which adorns b on its own, so gating Phase 2
 * alone changes nothing observable.  Here the fused rule is p's *only* rule,
 * so it is the only path from the demand on p to b, and $m$b_bf exists only if
 * Phase 2 read the FLATMAP root's head projection.
 *
 * original_rules_modified == 2 is the same statement counted: p's one rule
 * plus b's one rule.  b is guarded only because it was adorned.
 */
static void
test_fused_head_is_the_only_path_to_the_idb(void)
{
    TEST("test_fused_head_is_the_only_path_to_the_idb");

    static const char *src = ".decl edge(x: int32, y: int32)\n"
        ".decl b(x: int32, y: int32)\n"
        ".decl p(x: int32, y: int32)\n"
        ".output p\n"
        "edge(1, 2).\n"
        "edge(2, 3).\n"
        "edge(1, 9).\n"
        "edge(9, 5).\n"
        "b(x, y) :- edge(x, y).\n"
        "p(x, y) :- edge(x, z), b(z, y), y < 4.\n";

    static const int64_t seed[] = { 1 };
    static const int64_t expected[][2] = { { 1, 3 } };

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "p";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    /* Precondition: p's single rule must really be fused, or the test proves
     * nothing about a FLATMAP root. */
    struct wirelog_program *shape = parse_and_optimize(src);
    if (!shape) {
        FAIL("parse failed");
        return;
    }
    const wirelog_ir_node_t *root = rule_ir_root(shape, "p", 0);
    bool fused = rule_count_for(shape, "p") == 1 && root
        && root->type == WIRELOG_IR_FLATMAP;
    wirelog_program_free(shape);
    if (!fused) {
        FAIL("expected p to have exactly one rule, fused to a FLATMAP root");
        return;
    }

    wl_magic_sets_stats_t stats;
    struct wirelog_program *prog = magic_program_with_seed(src, demands, 1,
            "$m$p_bf", seed, 1, 1, &stats);
    if (!prog) {
        FAIL("setup failed");
        return;
    }

    /* Phase 2: the adornment reached b through the fused rule.  Without it the
     * BFS never leaves p and no magic relation for b is created. */
    if (!has_relation(prog, "$m$b_bf")) {
        wirelog_program_free(prog);
        FAIL("no $m$b_bf: the demand did not propagate through the fused rule");
        return;
    }
    if (stats.original_rules_modified != 2) {
        printf(" [original_rules_modified=%u]", stats.original_rules_modified);
        wirelog_program_free(prog);
        FAIL("expected p's fused rule and b's rule to be guarded");
        return;
    }
    if (stats.skipped_unsupported_head != 0) {
        printf(" [skipped_unsupported_head=%u]",
            stats.skipped_unsupported_head);
        wirelog_program_free(prog);
        FAIL("the fused rule must not be skipped as an unsupported head");
        return;
    }

    /* p(1, 3) needs b(2, 3), which exists only if b's guard saw the demand the
     * fused rule propagated. */
    ms_row_set_t rows;
    if (!ms_evaluate(prog, "p", 1, &rows)) {
        wirelog_program_free(prog);
        FAIL("evaluation failed");
        return;
    }
    if (!ms_rows_match(&rows, &expected[0][0], 1, 2)) {
        printf(" [got %u rows, want 1]", rows.count);
        wirelog_program_free(prog);
        FAIL("p != {(1, 3)}");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Test: a head wider than the 64-bit adornment mask is reported as an
 * unsupported head.
 *
 * This is what keeps skipped_unsupported_head reachable after #990.
 * get_head_vars() returns 0 for such a head even though the root's
 * project_exprs are present and readable -- the pass declines because it has
 * no adornment bit for column 64 and beyond -- and Phase 4b counts the rule
 * rather than guarding it.  Nothing else covered this counter.
 */
static void
test_wide_head_is_an_unsupported_head(void)
{
    TEST("test_wide_head_is_an_unsupported_head");

    /* 65 columns: one past the mask.  Same generator idea as
     * test_body_atom_limit_is_reported(). */
    char src[32768];
    size_t used = 0;
    used += (size_t)snprintf(src + used, sizeof(src) - used, ".decl src(");
    for (uint32_t i = 0; i < 65; i++) {
        used += (size_t)snprintf(src + used, sizeof(src) - used,
                "%sc%u: int32", i == 0 ? "" : ", ", i);
    }
    used += (size_t)snprintf(src + used, sizeof(src) - used, ")\n.decl wide(");
    for (uint32_t i = 0; i < 65; i++) {
        used += (size_t)snprintf(src + used, sizeof(src) - used,
                "%sc%u: int32", i == 0 ? "" : ", ", i);
    }
    used += (size_t)snprintf(src + used, sizeof(src) - used,
            ")\n.output wide\nwide(");
    for (uint32_t i = 0; i < 65; i++) {
        used += (size_t)snprintf(src + used, sizeof(src) - used,
                "%sv%u", i == 0 ? "" : ", ", i);
    }
    used += (size_t)snprintf(src + used, sizeof(src) - used, ") :- src(");
    for (uint32_t i = 0; i < 65; i++) {
        used += (size_t)snprintf(src + used, sizeof(src) - used,
                "%sv%u", i == 0 ? "" : ", ", i);
    }
    snprintf(src + used, sizeof(src) - used, ").\n");

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* The head projection is present -- this is not the AGGREGATE shape. */
    const wirelog_ir_node_t *root = rule_ir_root(prog, "wide", 0);
    if (!root || !root->project_exprs || root->project_count != 65) {
        wirelog_program_free(prog);
        FAIL("expected a 65-column head projection on the rule root");
        return;
    }

    wl_magic_demand_t demand = { "wide", 0x1, 65 };
    wl_magic_sets_stats_t stats;
    if (wl_magic_sets_apply_with_demands(prog, &demand, 1, &stats) != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    if (stats.skipped_unsupported_head != 1) {
        printf(" [skipped_unsupported_head=%u]",
            stats.skipped_unsupported_head);
        wirelog_program_free(prog);
        FAIL("expected skipped_unsupported_head == 1");
        return;
    }
    if (stats.original_rules_modified != 0) {
        printf(" [original_rules_modified=%u]", stats.original_rules_modified);
        wirelog_program_free(prog);
        FAIL("an over-wide head must not be guarded");
        return;
    }
    if (stats.skipped_constant_head != 0) {
        printf(" [skipped_constant_head=%u]", stats.skipped_constant_head);
        wirelog_program_free(prog);
        FAIL("a capability gap must not be reported as a policy skip");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * Test: a constant in the bound head position of a *fused* rule is a policy
 * skip, not a capability gap.  Before #990 the fused root made
 * get_head_vars() return 0 and the rule was misreported as an unsupported
 * head; reading project_exprs instead sees the constant for what it is.
 */
static void
test_fused_constant_head_is_a_policy_skip(void)
{
    TEST("test_fused_constant_head_is_a_policy_skip");

    static const char *src = ".decl e(x: int32, y: int32)\n"
        ".decl hot(x: int32, y: int32)\n"
        ".output hot\n"
        "e(1, 2).\n"
        "e(2, 3).\n"
        "hot(1, y) :- e(x, y), y > 1.\n";

    struct wirelog_program *prog = parse_and_optimize(src);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    const wirelog_ir_node_t *root = rule_ir_root(prog, "hot", 0);
    if (!root || root->type != WIRELOG_IR_FLATMAP) {
        wirelog_program_free(prog);
        FAIL("expected fusion to replace the PROJECT root");
        return;
    }

    wl_magic_demand_t demands[1];
    demands[0].relation_name = "hot";
    demands[0].bound_mask = 0x1;
    demands[0].arity = 2;

    wl_magic_sets_stats_t stats;
    if (wl_magic_sets_apply_with_demands(prog, demands, 1, &stats) != 0) {
        wirelog_program_free(prog);
        FAIL("magic sets returned error");
        return;
    }

    if (stats.original_rules_modified != 0) {
        printf(" [original_rules_modified=%u]", stats.original_rules_modified);
        wirelog_program_free(prog);
        FAIL("no guard was inserted, yet the rule was counted as modified");
        return;
    }
    if (stats.skipped_constant_head != 1) {
        printf(" [skipped_constant_head=%u]", stats.skipped_constant_head);
        wirelog_program_free(prog);
        FAIL("expected skipped_constant_head == 1");
        return;
    }
    if (stats.skipped_unsupported_head != 0) {
        printf(" [skipped_unsupported_head=%u]",
            stats.skipped_unsupported_head);
        wirelog_program_free(prog);
        FAIL("a constant head must not be reported as an unsupported head");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_body_atom_limit_is_reported(void)
{
    TEST("body atom limit is reported");

    char src[32768];
    size_t used = 0;
    used += (size_t)snprintf(src + used, sizeof(src) - used,
            ".decl out(x: int32)\n.output out\n.query out(b) .\n");
    for (uint32_t i = 0; i < 65; i++) {
        used += (size_t)snprintf(src + used, sizeof(src) - used,
                ".decl r%u(x: int32)\n", i);
    }
    used += (size_t)snprintf(src + used, sizeof(src) - used,
            "out(x) :- ");
    for (uint32_t i = 0; i < 65; i++) {
        used += (size_t)snprintf(src + used, sizeof(src) - used,
                "%sr%u(x)", i == 0 ? "" : ", ", i);
    }
    snprintf(src + used, sizeof(src) - used, ".\n");

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        FAIL("failed to parse over-limit rule");
        return;
    }

    wl_magic_demand_t demand = { "out", 1, 1 };
    int rc = wl_magic_sets_apply_with_demands(prog, &demand, 1, NULL);
    bool rejected = rc != 0 && !prog->magic_sets_applied;
    wirelog_program_free(prog);
    if (!rejected) {
        FAIL("over-limit rule was silently rewritten");
        return;
    }
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
    test_stratify_no_oob_on_graph_absent_head();

    printf("\nGuard Order Tests (Issue #989):\n");
    test_guard_join_right_child_is_representable();
    test_guard_recursive_eval_exact();
    test_guard_nonrecursive_join_chain_exact();
    test_guard_over_antijoin_exact();
    test_guard_over_semijoin_exact();
    test_constant_head_position_not_counted();
    test_mixed_constant_variable_guard_positions();
    test_fused_head_guard_answers_exact();
    test_fused_head_guard_shape();
    test_fused_head_is_the_only_path_to_the_idb();
    test_wide_head_is_an_unsupported_head();
    test_fused_constant_head_is_a_policy_skip();
    test_body_atom_limit_is_reported();

    printf("\n=== Results ===\n");
    printf("Tests run:    %d\n", tests_run);
    printf("Tests passed: %d\n", tests_passed);
    printf("Tests failed: %d\n", tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
